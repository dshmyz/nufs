package datanode

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// This file is Program 6 Phase F2: the EC self-healing scan. Shards of a 6+3
// stripe are lost when a disk/node degrades or ages out (a lost shard is
// tombstoned at its fixed generation and cannot be re-written in place).
// Without a scanning loop, a degraded stripe stays degraded until an operator
// notices — a degraded read reconstructs it only transiently for that one
// read. ECSelfHealer closes that gap: on a periodic sweep it discovers every
// chunk with shard(s) on this node, counts shard loss, and — when the loss is
// within §14's tolerance (1–3 of 9) and the stripe's original length is known
// — drives RepairChunkEC to rebuild the missing shards back onto healthy shard
// disks, restoring the full nine. It is idempotent (a full stripe is a no-op).

// ECSelfHealConfig tunes the self-healing scan.
type ECSelfHealConfig struct {
	// Interval between sweeps. 0 uses the default (30s).
	Interval time.Duration
}

const ecSelfHealDefaultInterval = 30 * time.Second

// EcOrphanDefaultAge is how long a stripe must stay rolled back before its
// partial shards are treated as reclaimable orphans (§14). It defers
// reclamation past any in-progress retry or salvage, matching the product
// expectation of "24h" in the F4 plan. Exported so runDataNodeV21 can feed it
// to SetOrphanResolver against the remote HTTP authority (Program 7).
const EcOrphanDefaultAge = 24 * time.Hour

// ECChunkResolver resolves an EC chunk's metadata (notably its Size, which is
// the only reliable source of the stripe's original pre-encoding length — the
// padding makes it unrecoverable from shard lengths alone, §14). Both the
// production *metadata.HTTPClient (GetChunk) and a test stub satisfy it.
type ECChunkResolver interface {
	GetChunk(ctx context.Context, chunkID metadata.ChunkID) (*metadata.ChunkMeta, error)
}

// ECLandingResolver resolves a chunk's authoritative per-shard landing (§14):
// the durable ECStripe.Shards recording which disk each shard originally landed
// on. The production *metadata.ECStore (ResolveStripeLanding) and a test stub
// satisfy it. When wired, the self-healer rebuilds a lost shard back onto its
// authoritative landing disk (RepairChunkECWithLanding) instead of a
// least-used disk, keeping the placement intact; absent, it falls back to
// least-used (RepairChunkEC's existing behavior).
type ECLandingResolver interface {
	ResolveStripeLanding(chunk *metadata.ChunkMeta) ([]metadata.ECShard, error)
}

// ECOrphanResolver answers whether a chunk's shards on this node are
// reclaimable orphans (§14): partial shards of a failed/rolled-back conversion,
// or leaked shards of a chunk whose metadata no longer references them. The
// production *metadata.ECStore (IsChunkShardsOrphaned) and a test stub satisfy
// it. When wired, the self-healer reclaims those shards via DeleteShard on a
// periodic orphan pass; absent (e.g. the V1 transport's HTTPClient, which lacks
// the local-only *metadata.ECStore method), orphan GC is disabled and the
// healer only repairs. On the production path (*metadata.HTTPClient satisfies
// this interface by structural typing since Program 7), orphan GC is LIVE over
// the metadata HTTP RPC — runDataNodeV21 wires SetOrphanResolver(metaStore, ...).
type ECOrphanResolver interface {
	IsChunkShardsOrphaned(ctx context.Context, chunkID metadata.ChunkID, olderThan time.Duration) (bool, error)
}

// ECSelfHealer is a background loop that repairs degraded 6+3 stripes on this
// datanode. It runs a periodic sweep (Enumerate) so an operator can also drive
// a single pass manually and assert on the result, or in tests.
type ECSelfHealer struct {
	v        *V2Store
	resolver ECChunkResolver
	landing  ECLandingResolver
	orphan   ECOrphanResolver
	orphanAge time.Duration
	interval time.Duration

	// peerDial makes repair cross-node (Program 13 / soak evidence): when set,
	// a degraded stripe is repaired from the cluster-wide shard view — reading
	// every shard from its owning node and pushing lost shards back to their
	// authoritative landing node — instead of the node-local view that can only
	// ever see this node's ~3 shards. nil keeps today's single-node behavior.
	peerDial func(addr string) *Client

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	scanned   atomic.Int64 // stripes considered this process lifetime
	repaired  atomic.Int64 // shards rebuilt
	skipped   atomic.Int64 // stripes skipped (loss beyond tolerance or no size)
	failed    atomic.Int64 // stripes whose repair errored
	reclaimed atomic.Int64 // orphan shards reclaimed
}

// NewECSelfHealer creates the self-healing scanner. resolver supplies the
// stripe's original length; pass nil to run discovery-only (repair skipped).
func NewECSelfHealer(v *V2Store, resolver ECChunkResolver, cfg ECSelfHealConfig) *ECSelfHealer {
	if cfg.Interval <= 0 {
		cfg.Interval = ecSelfHealDefaultInterval
	}
	return &ECSelfHealer{
		v: v, resolver: resolver,
		interval:  cfg.Interval,
		orphanAge: EcOrphanDefaultAge,
		stopCh:    make(chan struct{}),
	}
}

// SetLandingResolver wires an authoritative per-shard landing source (F3, §14):
// when set, a repaired shard is rebuilt back onto its originally-landed disk
// rather than a least-used one. Pass nil (default) to keep least-used fallback.
func (h *ECSelfHealer) SetLandingResolver(src ECLandingResolver) {
	if src != nil {
		h.landing = src
	}
}

// SetOrphanResolver wires an authoritative "are this chunk's shards orphans?"
// source (F4, §14). When set, the periodic ReclaimOrphans pass deletes shards
// the resolver judges reclaimable (rolled-back-and-aged or leaked) via
// DeleteShard, permanently freeing the datanode shard-store space. Pass nil
// (default) to disable orphan GC; olderThan <= 0 uses the default age.
func (h *ECSelfHealer) SetOrphanResolver(src ECOrphanResolver, olderThan time.Duration) {
	if src == nil {
		h.orphan = nil
		return
	}
	h.orphan = src
	if olderThan > 0 {
		h.orphanAge = olderThan
	}
}

// SetPeerDialer wires a cross-node datanode transport (Program 13 / soak) so the
// self-healer can rebuild shards that live on other nodes of the cluster. dial
// returns a client for addr (typically a lazily-connecting *Client derived from
// the metadata ListNodes topology, as ECService.SetCrossNode wires). Without it
// the healer keeps the single-node behavior: it only sees the ~3 shards this
// node holds locally, so on a multi-node cluster it cannot distinguish "my 3
// shards lost" from "other nodes' shards I never held" and never auto-repairs.
func (h *ECSelfHealer) SetPeerDialer(dial func(addr string) *Client) {
	h.peerDial = dial
}

// Start begins the periodic sweep loop.
func (h *ECSelfHealer) Start(ctx context.Context) {
	h.mu.Lock()
	if h.running {
		h.mu.Unlock()
		return
	}
	h.running = true
	h.mu.Unlock()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		ticker := time.NewTicker(h.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-h.stopCh:
				return
			case <-ticker.C:
				h.Enumerate(ctx)
				h.ReclaimOrphans(ctx)
			}
		}
	}()
	slog.Info("ec self-heal: worker started", "interval", h.interval)
}

// Stop halts the periodic sweep loop.
func (h *ECSelfHealer) Stop() {
	h.mu.Lock()
	if h.running {
		h.running = false
		close(h.stopCh)
	}
	h.mu.Unlock()
	h.wg.Wait()
}

// Stats returns (chunksScanned, shardsRepaired, skipped, failed) counters.
func (h *ECSelfHealer) Stats() (scanned, repaired, skipped, failed int64) {
	return h.scanned.Load(), h.repaired.Load(), h.skipped.Load(), h.failed.Load()
}

// Reclaimed returns the count of orphan shards reclaimed this process lifetime.
func (h *ECSelfHealer) Reclaimed() int64 { return h.reclaimed.Load() }

// Enumerate runs one full self-healing sweep: it discovers every chunk with
// shard(s) on this node and repairs any that are under-replicated within §14
// tolerance. It is safe to call concurrently with the background loop; callers
// that want one pass (tests, manual ops) call it directly.
func (h *ECSelfHealer) Enumerate(ctx context.Context) {
	if h.v == nil {
		return
	}
	chunks, err := h.v.ECShardChunks()
	if err != nil {
		slog.Warn("ec self-heal: discovery failed", "error", err)
		return
	}
	for cid := range chunks {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// A chunk whose shards are reclaimable orphans (F4, §14) must not be
		// repaired or re-referenced — skip it on this sweep; ReclaimOrphans
		// owns its lifecycle.
		if h.isOrphan(ctx, cid) {
			continue
		}
		h.repairChunk(ctx, cid)
	}
}

// isOrphan reports whether the chunk's shards are reclaimable orphans. With no
// orphan resolver wired it is always false (orphan GC disabled).
func (h *ECSelfHealer) isOrphan(ctx context.Context, cid metadata.ChunkID) bool {
	if h.orphan == nil {
		return false
	}
	orph, err := h.orphan.IsChunkShardsOrphaned(ctx, cid, h.orphanAge)
	if err != nil {
		slog.Warn("ec self-heal: orphan check failed, assuming live",
			"chunk", cid, "error", err)
		return false
	}
	return orph
}

// ReclaimOrphans runs one orphan-reclamation pass: it discovers every chunk
// with shard(s) on this node, and for any chunk the orphan resolver judges
// reclaimable (§14 rolled-back-and-aged or leaked), deletes every present
// shard via DeleteShard — permanently releasing the freed shard-store space.
// Idempotent: an already-reclaimed chunk has no present shards to delete. Safe
// to call concurrently with Enumerate; it is a no-op with no resolver wired.
func (h *ECSelfHealer) ReclaimOrphans(ctx context.Context) {
	if h.v == nil || h.orphan == nil {
		return
	}
	chunks, err := h.v.ECShardChunks()
	if err != nil {
		slog.Warn("ec self-heal: orphan discovery failed", "error", err)
		return
	}
	for cid := range chunks {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if !h.isOrphan(ctx, cid) {
			continue
		}
		h.reclaimChunkShards(ctx, cid)
	}
}

// reclaimChunkShards deletes every present shard of an orphaned chunk.
func (h *ECSelfHealer) reclaimChunkShards(ctx context.Context, cid metadata.ChunkID) {
	_, missing, err := h.v.readChunkECShards(cid)
	if err != nil {
		slog.Warn("ec self-heal: orphan shard occupancy read failed",
			"chunk", cid, "error", err)
		return
	}
	// readChunkECShards lists *missing* indices; the present ones are the
	// complement (0..8 minus missing). Reclaim those.
	present := make([]int, 0, ec63Shards-len(missing))
	presentMap := make(map[int]bool, ec63Shards)
	for _, m := range missing {
		presentMap[m] = true
	}
	for idx := 0; idx < ec63Shards; idx++ {
		if presentMap[idx] {
			continue
		}
		present = append(present, idx)
	}
	reclaimed := 0
	for _, idx := range present {
		if err := h.v.DeleteShard(cid, idx); err != nil {
			slog.Warn("ec self-heal: orphan shard delete failed",
				"chunk", cid, "shard", idx, "error", err)
			continue
		}
		reclaimed++
	}
	if reclaimed > 0 {
		h.reclaimed.Add(int64(reclaimed))
		slog.Info("ec self-heal: reclaimed orphan shards",
			"chunk", cid, "reclaimed", reclaimed)
	}
}

// repairChunk counts the shard loss for one chunk and repairs it if the loss
// is within tolerance and the original length is resolvable. On a multi-node
// cluster (peerDial wired) it uses the cluster-wide shard view so shards lost
// on *other* nodes are rebuilt; otherwise it stays node-local (single node).
func (h *ECSelfHealer) repairChunk(ctx context.Context, cid metadata.ChunkID) {
	chunk, ok := h.chunkMeta(ctx, cid)
	if !ok {
		// No authoritative original length (chunk metadata unreachable or gone):
		// we cannot safely decode/reconstruct, so skip rather than write garbage.
		h.skipped.Add(1)
		h.scanned.Add(1)
		slog.Warn("ec self-heal: no original length, skipping repair",
			"chunk", cid)
		return
	}
	h.scanned.Add(1)
	landing := h.landingFor(chunk)

	if h.peerDial != nil {
		h.repairChunkCrossNode(ctx, cid, chunk, landing)
		return
	}

	// Node-local path (single node): probe the nine shard indices across this
	// node's shard stores and report which are missing. A full stripe
	// short-circuits as a no-op.
	_, missing, err := h.v.readChunkECShards(cid)
	if err != nil {
		slog.Warn("ec self-heal: reading shard occupancy", "chunk", cid, "error", err)
		h.failed.Add(1)
		return
	}
	loss := len(missing)
	if loss == 0 {
		return // fully replicated, nothing to do
	}
	if loss > ec63Parity {
		// Beyond §14 tolerance: fewer than six shards survive, so the stripe
		// cannot be reconstructed. Leave it for a degraded-read / operator
		// intervention rather than fabricating shards we cannot verify.
		h.skipped.Add(1)
		slog.Warn("ec self-heal: stripe loss beyond §14 tolerance, skipping",
			"chunk", cid, "missing", loss)
		return
	}
	// Prefer rebuilding each lost shard back onto its authoritative landing
	// disk (F3, §14) when the durable stripe is resolvable; else least-used.
	rebuilt, err := h.v.RepairChunkECWithLanding(cid, int(chunk.Size), landing)
	if err != nil {
		h.failed.Add(1)
		slog.Warn("ec self-heal: repair failed", "chunk", cid, "missing", loss, "error", err)
		return
	}
	if rebuilt > 0 {
		h.repaired.Add(int64(rebuilt))
		slog.Info("ec self-heal: repaired degraded stripe",
			"chunk", cid, "rebuilt", rebuilt)
	}
}

// repairChunkCrossNode rebuilds a degraded 6+3 stripe whose shards are spread
// across multiple datanodes (Program 13 / soak evidence). A single node's
// node-local probe only ever sees the ~3 shards it holds locally, so on a
// multi-node cluster it cannot tell "my shards lost" from "other nodes' shards
// I never held" and would misreport loss > §14 tolerance forever. Here we take
// the cluster-wide view: read every shard from its owning node, and when the
// true loss is within tolerance, reconstruct the missing shards from the
// survivors and push each back to its authoritative landing node/disk.
//
// The owner of shard i is resolved from chunk.Replicas by ShardIndex (the
// authority kept the plan aligned, cf. PlanECWrite/RecordDirect), so the
// stripe's own metadata names every peer — no separate peer registry needed.
func (h *ECSelfHealer) repairChunkCrossNode(ctx context.Context, cid metadata.ChunkID, chunk *metadata.ChunkMeta, landing []metadata.ECShard) {
	// owner[i] = the ReplicaInfo that serves shard i (by ShardIndex).
	owner := make([]*metadata.ReplicaInfo, ec63Shards)
	for i := range chunk.Replicas {
		r := &chunk.Replicas[i]
		if r.ShardIndex >= 0 && r.ShardIndex < ec63Shards && owner[r.ShardIndex] == nil {
			owner[r.ShardIndex] = r
		}
	}

	// Read every shard from its owning node (cluster-wide view).
	shards := make([][]byte, ec63Shards)
	present := make([]bool, ec63Shards)
	var missing []int
	for i := 0; i < ec63Shards; i++ {
		ownerRep := owner[i]
		if ownerRep == nil || ownerRep.Addr == "" {
			missing = append(missing, i)
			continue
		}
		data, err := h.readShardFrom(ownerRep.Addr, cid, i)
		if err != nil {
			missing = append(missing, i)
			continue
		}
		shards[i] = data
		present[i] = true
	}
	loss := len(missing)
	if loss == 0 {
		return // fully replicated, nothing to do
	}
	if loss > ec63Parity {
		// Fewer than six surviving shards cluster-wide: cannot reconstruct.
		h.skipped.Add(1)
		slog.Warn("ec self-heal: stripe loss beyond §14 tolerance, skipping",
			"chunk", cid, "missing", loss)
		return
	}
	// Reconstruct the full nine-shard set (missing shards repopulated) from
	// the survivors, verifying the original payload against the shard checksums.
	// originalLen is clamped to the shard-derived padded length: a direct-EC
	// chunk's ChunkMeta.Size (MaxChunkSize) exceeds the true padded length and
	// would otherwise fail the reconstruction length check.
	originalLen := int(chunk.Size)
	if originalLen <= 0 || originalLen > ec63PaddedLen(shards) {
		originalLen = ec63PaddedLen(shards)
	}
	rebuilt, _, err := reconstructEC63(shards, originalLen)
	if err != nil {
		h.failed.Add(1)
		slog.Warn("ec self-heal: cross-node reconstruct failed", "chunk", cid, "missing", loss, "error", err)
		return
	}
	n := 0
	for _, idx := range missing {
		ownerRep := owner[idx]
		disk := -1
		if idx < len(landing) {
			disk = int(landing[idx].DiskID % 1000) // node-local shard-store index on the owner
		}
		if ownerRep == nil || ownerRep.Addr == "" {
			slog.Warn("ec self-heal: cannot restore shard, no owner addr",
				"chunk", cid, "shard", idx)
			continue
		}
		if err := h.pushShardTo(ownerRep.Addr, cid, idx, disk, rebuilt[idx]); err != nil {
			slog.Warn("ec self-heal: restore shard failed", "chunk", cid, "shard", idx, "error", err)
			continue
		}
		n++
	}
	if n > 0 {
		h.repaired.Add(int64(n))
		slog.Info("ec self-heal: cross-node repaired degraded stripe",
			"chunk", cid, "rebuilt", n)
	}
}

// readShardFrom reads one EC shard extent from addr (the node that owns it),
// using the same ReadECShard the serving read path uses for V2.1 converted
// stripes. Any error — unreachable node, absent extent, transient fault — is
// treated as "that shard is unavailable" so the caller counts it as missing.
func (h *ECSelfHealer) readShardFrom(addr string, cid metadata.ChunkID, idx int) ([]byte, error) {
	client, err := h.dial(addr)
	if err != nil {
		return nil, err
	}
	resp, err := client.ReadECShard(cid, idx)
	if err != nil {
		return nil, err
	}
	if resp.Status != StatusOK {
		return nil, fmt.Errorf("status=%d: %s", resp.Status, resp.Error)
	}
	return resp.Data, nil
}

// pushShardTo durably lands one rebuilt EC shard back onto its owning node's
// shard store (the authoritative landing disk when disk >= 0, else the peer's
// own routing). Matches the coordinator->peer push the direct EC write path
// uses (ReplicateECShard), so the peer's server writes it into its shard store.
func (h *ECSelfHealer) pushShardTo(addr string, cid metadata.ChunkID, idx, disk int, data []byte) error {
	client, err := h.dial(addr)
	if err != nil {
		return err
	}
	resp, err := client.ReplicateECShard(cid, idx, disk, data)
	if err != nil {
		return err
	}
	if resp.Status != StatusOK {
		return fmt.Errorf("status=%d: %s", resp.Status, resp.Error)
	}
	return nil
}

// dial returns a client for addr via the injected peer dialer (nil-safe: with
// no dialer wired the healer never reaches the cross-node path, so this is only
// called when h.peerDial != nil).
func (h *ECSelfHealer) dial(addr string) (*Client, error) {
	if h.peerDial == nil {
		return nil, fmt.Errorf("no peer dialer wired")
	}
	c := h.peerDial(addr)
	if c == nil {
		return nil, fmt.Errorf("dial %s: no client", addr)
	}
	return c, nil
}

// chunkMeta fetches the chunk's authoritative metadata (its Size is the
// stripe's original pre-encoding length, §14). Returns ok=false when the chunk
// metadata is unavailable (no resolver, or the chunk row no longer resolves).
func (h *ECSelfHealer) chunkMeta(ctx context.Context, cid metadata.ChunkID) (*metadata.ChunkMeta, bool) {
	if h.resolver == nil {
		return nil, false
	}
	chunk, err := h.resolver.GetChunk(ctx, cid)
	if err != nil || chunk == nil {
		return nil, false
	}
	if chunk.Size <= 0 {
		return nil, false
	}
	return chunk, true
}

// landingFor resolves the chunk's authoritative per-shard landing (the durable
// ECStripe.Shards, F3/§14) for the repair to prefer. Returns nil when no
// landing resolver is wired, or resolution fails — the repair then falls back
// to least-used disk placement.
func (h *ECSelfHealer) landingFor(chunk *metadata.ChunkMeta) []metadata.ECShard {
	if h.landing == nil {
		return nil
	}
	landing, err := h.landing.ResolveStripeLanding(chunk)
	if err != nil {
		slog.Warn("ec self-heal: landing resolution failed, using least-used",
			"chunk", chunk.ID, "error", err)
		return nil
	}
	return landing
}
