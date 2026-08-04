package datanode

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/dfs/metadata"
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

// ECSelfHealer is a background loop that repairs degraded 6+3 stripes on this
// datanode. It runs a periodic sweep (Enumerate) so an operator can also drive
// a single pass manually and assert on the result, or in tests.
type ECSelfHealer struct {
	v        *V2Store
	resolver ECChunkResolver
	landing  ECLandingResolver
	interval time.Duration

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	scanned   atomic.Int64 // stripes considered this process lifetime
	repaired  atomic.Int64 // shards rebuilt
	skipped   atomic.Int64 // stripes skipped (loss beyond tolerance or no size)
	failed    atomic.Int64 // stripes whose repair errored
}

// NewECSelfHealer creates the self-healing scanner. resolver supplies the
// stripe's original length; pass nil to run discovery-only (repair skipped).
func NewECSelfHealer(v *V2Store, resolver ECChunkResolver, cfg ECSelfHealConfig) *ECSelfHealer {
	if cfg.Interval <= 0 {
		cfg.Interval = ecSelfHealDefaultInterval
	}
	return &ECSelfHealer{v: v, resolver: resolver, interval: cfg.Interval, stopCh: make(chan struct{})}
}

// SetLandingResolver wires an authoritative per-shard landing source (F3, §14):
// when set, a repaired shard is rebuilt back onto its originally-landed disk
// rather than a least-used one. Pass nil (default) to keep least-used fallback.
func (h *ECSelfHealer) SetLandingResolver(src ECLandingResolver) {
	if src != nil {
		h.landing = src
	}
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
		h.repairChunk(ctx, cid)
	}
}

// repairChunk counts the shard loss for one chunk and repairs it if the loss
// is within tolerance and the original length is resolvable.
func (h *ECSelfHealer) repairChunk(ctx context.Context, cid metadata.ChunkID) {
	// readChunkECShards probes all nine shard indices across the node's shard
	// stores and reports which are missing (a present shard needs no repair; a
	// full stripe short-circuits here as a no-op).
	_, missing, err := h.v.readChunkECShards(cid)
	if err != nil {
		slog.Warn("ec self-heal: reading shard occupancy", "chunk", cid, "error", err)
		h.failed.Add(1)
		return
	}
	h.scanned.Add(1)
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
	chunk, ok := h.chunkMeta(ctx, cid)
	if !ok {
		// No authoritative original length (chunk metadata unreachable or gone):
		// we cannot safely decode/reconstruct, so skip rather than write garbage.
		h.skipped.Add(1)
		slog.Warn("ec self-heal: no original length, skipping repair",
			"chunk", cid, "missing", loss)
		return
	}
	// Prefer rebuilding each lost shard back onto its authoritative landing
	// disk (F3, §14) when the durable stripe is resolvable; else least-used.
	landing := h.landingFor(chunk)
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
