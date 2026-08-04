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

// ECSelfHealer is a background loop that repairs degraded 6+3 stripes on this
// datanode. It runs a periodic sweep (Enumerate) so an operator can also drive
// a single pass manually and assert on the result, or in tests.
type ECSelfHealer struct {
	v        *V2Store
	resolver ECChunkResolver
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
	originalLen, ok := h.originalLen(ctx, cid)
	if !ok {
		// No authoritative original length (chunk metadata unreachable or gone):
		// we cannot safely decode/reconstruct, so skip rather than write garbage.
		h.skipped.Add(1)
		slog.Warn("ec self-heal: no original length, skipping repair",
			"chunk", cid, "missing", loss)
		return
	}
	rebuilt, err := h.v.RepairChunkEC(cid, originalLen)
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

// originalLen resolves the stripe's authoritative pre-encoding length from the
// chunk's metadata Size. Returns ok=false when the chunk metadata is
// unavailable (no resolver, or the chunk row no longer resolves).
func (h *ECSelfHealer) originalLen(ctx context.Context, cid metadata.ChunkID) (int, bool) {
	if h.resolver == nil {
		return 0, false
	}
	chunk, err := h.resolver.GetChunk(ctx, cid)
	if err != nil || chunk == nil {
		return 0, false
	}
	if chunk.Size <= 0 {
		return 0, false
	}
	return int(chunk.Size), true
}
