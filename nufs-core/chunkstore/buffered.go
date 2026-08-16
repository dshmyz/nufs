// BufferedFile is a portable chunk-level write-back buffer.
//
// It owns the whole write-buffer lifecycle that previously lived in the FUSE
// gateway (gateway/fuse/inode.go): lazy 64 MiB per-chunk buffers, disk spill
// to a staging directory under a dirty-memory budget, hydration of committed
// bytes on partial overwrites, a dirty-over-committed read view, and a Flush
// that persists every dirty chunk through the shared ChunkWriter dispatch
// pipeline (allocate → write → commit(EC-skip) → seal) before wholesale
// replacing the inode's ChunkMap.
//
// BufferedFile is deliberately FUSE-agnostic: it has no knowledge of go-fuse,
// and all FUSE-only concerns (permission checks, advisory locks, op metrics,
// errno mapping) are injected as function values / narrow interfaces, so the
// file stays portable across build tags and the SDK never imports the
// gateway. The only gateway-facing policy is that Flush owns persistence
// (resolve policy → ChunkWriter → merge → UpdateInode → ledger → delete
// superseded refs → clear), returning FlushResult for observability.
//
// Not a vFS layer: callers drive their own concurrency (Write/ReadView take
// locks internally; Flush additionally serializes cross-instance flushes via
// Executor.LockInode). This mirrors the FUSE contract where concurrent
// DFSFile instances for the same inode must not interleave flushes.
package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ErrOutOfDirtyBudget is returned by Write/AppendWrite when the write would
// exceed the dirty-memory budget and no spill sink can free space (the staging
// directory is unset or already holds every dirty chunk). Callers map it to
// ENOSPC.
var ErrOutOfDirtyBudget = errors.New("chunkstore: out of dirty write budget")

// Executor wraps the individual metadata and chunk operations of a BufferedFile
// with reliability (retry + circuit breaker). FUSE passes its
// ReliabilityWrapper method values, which are nil-receiver-safe; a zero-value
// Executor is a plain direct call (used by tests and by S3-style consumers
// that do their own reliability). Every method is nil-safe.
type Executor struct {
	// DoMeta wraps metadata commands (GetInode/UpdateInode/GetChunk/...).
	DoMeta func(op string, fn func() error) error
	// DoChunk wraps chunk-store commands (ReadChunkRange/WriteChunk/...).
	DoChunk func(op string, fn func() error) error
	// LockInode serializes concurrent Flushes of the same inode across distinct
	// BufferedFile instances (go-fuse inode cache eviction + rebuild). It is
	// called inside Flush, after the dirty check, spanning GetInode→UpdateInode.
	// Returns an unlock function. Nil-safe (no-op).
	LockInode func(inodeID uint64) func()
}

func (e Executor) doMeta(op string, fn func() error) error {
	if e.DoMeta != nil {
		return e.DoMeta(op, fn)
	}
	return fn()
}

func (e Executor) doChunk(op string, fn func() error) error {
	if e.DoChunk != nil {
		return e.DoChunk(op, fn)
	}
	return fn()
}

func (e Executor) lockInode(id uint64) func() {
	if e.LockInode == nil {
		return func() {}
	}
	return e.LockInode(id)
}

// ReadCache is the narrow read-path cache consumed by ReadView. The FUSE
// ChunkCache satisfies it directly; tests may inject a fake.
type ReadCache interface {
	// Get returns the cached window starting at chunk offset off, if present.
	Get(chunkID uint64, off int64) ([]byte, bool)
	// Add stores a window at chunk offset off. Windows are independent, so a
	// partial window never masquerades as a whole chunk.
	Add(chunkID uint64, off int64, data []byte)
}

// SpillStats counts staging (disk spill) events. The FUSE MetricsRecorder
// satisfies it structurally; a nil value is a silent no-op.
type SpillStats interface {
	// IncStagingSpill counts a dirty chunk spilled to the staging directory.
	IncStagingSpill()
	// IncStagingLoad counts a dirty chunk reloaded from the staging directory.
	IncStagingLoad()
	// IncStagingSpillErr counts a failed spill.
	IncStagingSpillErr()
}

// WriteAttemptLedger records flush state transitions for observability and
// crash recovery. The recovery worker (shared with the S3 gateway) picks up
// incomplete attempts and cleans up orphaned chunks. Nil = disabled.
type WriteAttemptLedger interface {
	Record(ctx context.Context, attemptID string, inode *metadata.InodeMeta, state metadata.WriteAttemptState, lastErr string)
}

// Budget bounds how much dirty data a BufferedFile may hold in memory. A zero
// field means "unlimited" for that dimension.
type Budget struct {
	// MaxDirtyBytes is the per-file cap on in-memory dirty bytes. When a write
	// would exceed it, the oldest dirty chunk is spilled to StagingDir; if that
	// cannot free enough (or spill is disabled), Write returns
	// ErrOutOfDirtyBudget.
	MaxDirtyBytes int64
	// GlobalBudget is a cross-file cap shared through GlobalDirtyBytes. A write
	// is also rejected when the global counter would exceed it.
	GlobalBudget int64
	// GlobalDirtyBytes is the cross-file dirty counter shared by every
	// BufferedFile of one filesystem. It only tracks the spill↔load delta
	// (spill subtracts, load adds); the per-file dirtyBytes is authoritative
	// for the local budget. Nil-safe.
	GlobalDirtyBytes *atomic.Int64
	// StagingDir is where spilled chunks land. "" disables disk spill entirely
	// (out-of-budget writes then fail with ErrOutOfDirtyBudget).
	StagingDir string
}

// FlushResult is the outcome of a BufferedFile.Flush, for observability.
type FlushResult struct {
	// NewRefs is the wholesale-replaced ChunkMap the flush committed.
	NewRefs []metadata.ChunkRef
	// NewSize is the persisted file size (max of logical tail and committed
	// size), as written to the inode.
	NewSize int64
}

// BufferedFile is the FUSE write buffer, moved into the SDK. See the package
// doc for the full contract.
type BufferedFile struct {
	mu      sync.RWMutex
	meta    func() metadata.MetadataService // resolved per call — hot-swappable
	chunk   ChunkStore
	inodeID metadata.InodeID

	exec          Executor
	readCache     ReadCache
	spillStats    SpillStats
	ledger        WriteAttemptLedger
	budget        Budget
	defaultPolicy metadata.PlacementPolicy

	// dispatch is the Flush chunk pipeline, built lazily under b.mu (the meta
	// accessor must be resolved at call time, not construction).
	dispatch *ChunkWriter

	// Buffer image: chunkBufs holds a lazily-allocated 64 MiB buffer per dirty
	// chunk base; dirtyMap marks which bases are dirty; dirtyBytes sums
	// in-memory dirty bytes. spilledChunks maps bases that were evicted to the
	// staging dir (dirty stays, memory goes).
	dirty         bool
	chunkBufs     map[int64][]byte
	dirtyMap      map[int64]bool
	dirtyBytes    int64
	spilledChunks map[int64]string
	spilledBytes  int64
	// logicalSize is the high-water mark of writes since Clear, seeded with the
	// committed size at construction (so a partial in-place overwrite never
	// shrinks a file below its committed extent).
	logicalSize int64
}

// BufferedFileOption configures a BufferedFile at construction.
type BufferedFileOption func(*BufferedFile)

// WithExecutor injects the reliability executor (DoMeta/DoChunk/LockInode).
func WithExecutor(e Executor) BufferedFileOption {
	return func(b *BufferedFile) { b.exec = e }
}

// WithReadCache injects the read-path cache consumed by ReadView.
func WithReadCache(c ReadCache) BufferedFileOption {
	return func(b *BufferedFile) { b.readCache = c }
}

// WithSpillStats injects staging-event counters.
func WithSpillStats(s SpillStats) BufferedFileOption {
	return func(b *BufferedFile) { b.spillStats = s }
}

// WithFlushLedger injects the write-attempt ledger recorder.
func WithFlushLedger(l WriteAttemptLedger) BufferedFileOption {
	return func(b *BufferedFile) { b.ledger = l }
}

// WithBudget sets the dirty-memory budget (per-file cap, global cap + shared
// counter, staging dir).
func WithBudget(bud Budget) BufferedFileOption {
	return func(b *BufferedFile) { b.budget = bud }
}

// WithDefaultPolicy sets the placement policy used when Flush cannot resolve
// the containing bucket's policy (orphan inode or lookup failure).
func WithDefaultPolicy(p metadata.PlacementPolicy) BufferedFileOption {
	return func(b *BufferedFile) { b.defaultPolicy = p }
}

// bufferedDefaultPolicy is the fallback placement policy for Flush. FUSE
// overrides it with its own FUSE-named policy; this is only reached for orphan
// files or bucket-lookup failures.
var bufferedDefaultPolicy = metadata.PlacementPolicy{
	ID:                "buffered-default",
	ReplicationFactor: 1,
	TopologySpread:    metadata.SpreadNode,
}

// NewBufferedFile builds a BufferedFile for the given inode. meta is an
// accessor resolved on every call (so a hot-swappable MetadataService keeps
// working); committedSize seeds logicalSize so a partial overwrite never
// shrinks the file below its committed extent.
func NewBufferedFile(meta func() metadata.MetadataService, chunk ChunkStore, id metadata.InodeID, committedSize int64, opts ...BufferedFileOption) *BufferedFile {
	b := &BufferedFile{
		meta:          meta,
		chunk:         chunk,
		inodeID:       id,
		logicalSize:   committedSize,
		defaultPolicy: bufferedDefaultPolicy,
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// ============ buffer-image helpers ============

// getChunk returns the (lazily zero-allocated) 64 MiB buffer for a chunk base,
// charging it to the dirty-memory accounting. Caller holds b.mu.
func (b *BufferedFile) getChunk(base int64) []byte {
	if b.chunkBufs == nil {
		b.chunkBufs = make(map[int64][]byte)
	}
	c, ok := b.chunkBufs[base]
	if !ok {
		c = make([]byte, metadata.MaxChunkSize)
		b.chunkBufs[base] = c
		b.dirtyBytes += int64(metadata.MaxChunkSize)
	}
	return c
}

// markDirty flags a chunk base as dirty. Caller holds b.mu.
func (b *BufferedFile) markDirty(base int64) {
	if b.dirtyMap == nil {
		b.dirtyMap = make(map[int64]bool)
	}
	b.dirtyMap[base] = true
	b.dirty = true
}

// cleanupStaging removes every spill file and zeroes the spill accounting.
// Caller holds b.mu.
func (b *BufferedFile) cleanupStaging() {
	for base, path := range b.spilledChunks {
		_ = os.Remove(path)
		delete(b.spilledChunks, base)
	}
	b.spilledBytes = 0
}

// Clear resets the whole buffer image: drops in-memory buffers and spill
// files, zeroes counters, and marks the file clean. Caller holds b.mu.
func (b *BufferedFile) Clear() {
	b.cleanupStaging()
	b.chunkBufs = nil
	b.dirtyMap = nil
	b.dirty = false
	b.dirtyBytes = 0
	b.spilledBytes = 0
	b.logicalSize = 0
}

// stagingPath is the on-disk location of a spilled chunk. Caller holds b.mu.
func (b *BufferedFile) stagingPath(base int64) string {
	return fmt.Sprintf("%s/%d_%d.dat", b.budget.StagingDir, b.inodeID, base)
}

// spillToDisk evicts one chunk buffer to the staging directory, moving it from
// dirtyBytes to spilledBytes (and dropping it from the global counter). The
// chunk stays dirty. Caller holds b.mu.
func (b *BufferedFile) spillToDisk(base int64) error {
	if b.budget.StagingDir == "" {
		return fmt.Errorf("spill: staging dir not configured")
	}
	buf, ok := b.chunkBufs[base]
	if !ok {
		return nil
	}
	path := b.stagingPath(base)
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		return fmt.Errorf("spill chunk %d: %w", base, err)
	}
	delete(b.chunkBufs, base)
	b.dirtyBytes -= int64(len(buf))
	b.spilledBytes += int64(len(buf))
	if g := b.budget.GlobalDirtyBytes; g != nil {
		g.Add(-int64(len(buf)))
	}
	if b.spilledChunks == nil {
		b.spilledChunks = make(map[int64]string)
	}
	b.spilledChunks[base] = path
	return nil
}

// loadFromDisk pulls a spilled chunk back into memory (spill file removed).
// Caller holds b.mu.
func (b *BufferedFile) loadFromDisk(base int64) ([]byte, error) {
	path, ok := b.spilledChunks[base]
	if !ok {
		return nil, fmt.Errorf("loadFromDisk: base %d not in staging", base)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load staging %d: %w", base, err)
	}
	_ = os.Remove(path)
	delete(b.spilledChunks, base)
	b.spilledBytes -= int64(len(data))
	if b.chunkBufs == nil {
		b.chunkBufs = make(map[int64][]byte)
	}
	b.chunkBufs[base] = data
	b.dirtyBytes += int64(len(data))
	if g := b.budget.GlobalDirtyBytes; g != nil {
		g.Add(int64(len(data)))
	}
	return data, nil
}

// spillOldestChunk evicts the lowest dirty base to staging (best-effort; a
// missing staging dir is a no-op). Caller holds b.mu.
func (b *BufferedFile) spillOldestChunk() error {
	if b.budget.StagingDir == "" || len(b.chunkBufs) == 0 {
		return nil
	}
	var oldest int64 = -1
	for base := range b.dirtyMap {
		if oldest < 0 || base < oldest {
			oldest = base
		}
	}
	if oldest < 0 {
		return nil
	}
	return b.spillToDisk(oldest)
}

// ============ buffer-image invariant guard ============

var bufferImageInvariantOn atomic.Bool

// EnableBufferImageInvariant turns on the debug invariant assertion. Only
// tests should call it; production stays off for zero overhead.
func EnableBufferImageInvariant() { bufferImageInvariantOn.Store(true) }

// assertBufferImageLocked verifies that every dirty chunk base has either an
// in-memory buffer or a spill file backing it. This is the spill-aware
// strengthening of the original FUSE invariant (which only accepted
// chunkBufs and panicked after a spill evicted the buffer). No-op unless the
// guard is on. Caller holds b.mu.
func (b *BufferedFile) assertBufferImageLocked() {
	if !bufferImageInvariantOn.Load() || !b.dirty {
		return
	}
	for base := range b.dirtyMap {
		if _, ok := b.chunkBufs[base]; ok {
			continue
		}
		if _, spilled := b.spilledChunks[base]; spilled {
			continue
		}
		panic(fmt.Sprintf("chunkstore: BufferedFile invariant violated: dirty chunk at offset %d has neither buffer nor spill (inode %d)", base, b.inodeID))
	}
}

// ============ write path ============

// Write buffers data at the given offset until Flush. It supports random
// writes: a write at a nonzero offset grows the buffer, zero-filling any hole,
// and a write landing after a Flush at a nonzero offset is first hydrated from
// the committed file content so the next Flush's rebuild doesn't zero the
// committed prefix. Returns the number of bytes buffered, or
// ErrOutOfDirtyBudget when the dirty budget cannot fit the write (nothing was
// written in that case).
func (b *BufferedFile) Write(ctx context.Context, data []byte, off int64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.writeLocked(ctx, data, off); err != nil {
		return 0, err
	}
	return len(data), nil
}

// AppendWrite implements O_APPEND semantics: the write is placed at the file's
// current end (max of committed size and any already-buffered tail) regardless
// of any offset. The tail is computed under b.mu so concurrent appends to the
// same inode don't collide. Note: unlike Write, this path performs no
// permission check — callers that need one (FUSE Write does; AppendWrite's
// original FUSE glue did not) must enforce it beforehand.
func (b *BufferedFile) AppendWrite(ctx context.Context, data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	tail := b.logicalSize
	if tail == 0 {
		metaInode, err := b.meta().GetInode(ctx, b.inodeID)
		if err != nil {
			return 0, err
		}
		tail = int64(metaInode.Size)
	}
	if err := b.writeLocked(ctx, data, tail); err != nil {
		return 0, err
	}
	return len(data), nil
}

// writeLocked applies a buffered write at the given offset. When creating a
// new chunk buffer, committed data is loaded from the ChunkMap so partial
// writes preserve the committed prefix/suffix. Caller holds b.mu.
func (b *BufferedFile) writeLocked(ctx context.Context, data []byte, off int64) error {
	end := off + int64(len(data))

	// Check dirty-memory limits before allocating new chunk buffers. Count how
	// many new chunks this write would touch (bases not yet in chunkBufs). When
	// the per-file limit is exceeded, the oldest dirty chunk is spilled to disk
	// staging (if available) before returning ErrOutOfDirtyBudget.
	if max := b.budget.MaxDirtyBytes; max > 0 {
		newBases := make(map[int64]struct{})
		for pos := off; pos < end; {
			base := ChunkBase(pos)
			if _, exists := b.chunkBufs[base]; !exists {
				newBases[base] = struct{}{}
			}
			next := base + metadata.MaxChunkSize
			if next > end {
				next = end
			}
			pos = next
		}
		needed := int64(len(newBases)) * int64(metadata.MaxChunkSize)
		if b.dirtyBytes+needed > max {
			// Try spilling the oldest dirty chunk in this file to free memory.
			if err := b.spillOldestChunk(); err != nil {
				b.incSpillErr()
				slog.Warn("chunkstore: write: spill failed", "inode", b.inodeID, "error", err)
			} else if len(b.spilledChunks) > 0 {
				b.incSpill()
			}
			// Also try spilling if the global budget is exceeded.
			if g := b.budget.GlobalDirtyBytes; g != nil && b.budget.GlobalBudget > 0 && g.Load()+needed > b.budget.GlobalBudget {
				if err := b.spillOldestChunk(); err != nil {
					b.incSpillErr()
				} else if len(b.spilledChunks) > 0 {
					b.incSpill()
				}
			}
			if b.dirtyBytes+needed > max {
				return ErrOutOfDirtyBudget
			}
		}
	}

	// For each chunk touched by this write, ensure the buffer exists and has
	// committed data loaded (so partial writes don't zero committed bytes).
	for pos := off; pos < end; {
		base := ChunkBase(pos)
		if _, exists := b.chunkBufs[base]; !exists {
			b.loadCommittedChunkLocked(ctx, base)
		}
		pos = base + metadata.MaxChunkSize
		if pos > end {
			pos = end
		}
	}

	for pos := off; pos < end; {
		base := ChunkBase(pos)
		buf := b.getChunk(base)
		within := pos - base
		n := int64(len(data)) - (pos - off)
		if within+n > int64(len(buf)) {
			n = int64(len(buf)) - within
		}
		copy(buf[within:within+n], data[pos-off:pos-off+n])
		b.markDirty(base)
		pos += n
	}
	// Track the actual high-water mark of writes (not chunk-aligned).
	// logicalSize is seeded from the committed size at construction, so a
	// partial overwrite at a nonzero offset never shrinks the file below its
	// committed extent without a per-write metadata round-trip.
	if end > b.logicalSize {
		b.logicalSize = end
	}
	b.assertBufferImageLocked()
	return nil
}

// loadCommittedChunkLocked loads committed chunk data for the given chunk base
// into the chunk buffer, so partial writes preserve committed bytes that were
// already flushed. Caller holds b.mu.
func (b *BufferedFile) loadCommittedChunkLocked(ctx context.Context, base int64) {
	metaInode, err := b.meta().GetInode(ctx, b.inodeID)
	if err != nil {
		return // can't load — buffer stays zero-filled (safe for new files)
	}
	// Resolve the committed refs under either storage model (V1 ChunkMap or
	// V2 extent layout) so partial overwrites of a V2 file merge its committed
	// bytes instead of zero-filling the whole base.
	es, _ := b.meta().(metadata.ExtentInodeService)
	refs, err := metadata.ResolveFileChunks(ctx, es, metaInode)
	if err != nil {
		return
	}
	// Collect every committed chunk overlapping this base and merge them in
	// offset order. A single 64 MiB base can hold multiple committed refs
	// (small writes across flushes each become their own ref); stopping at the
	// first would zero-fill the others and lose committed data.
	overlaps := make([]metadata.ChunkRef, 0, 2)
	for _, cref := range refs {
		cEnd := cref.Offset + int64(cref.Length)
		if cEnd > base && cref.Offset < base+metadata.MaxChunkSize {
			overlaps = append(overlaps, cref)
		}
	}
	if len(overlaps) == 0 {
		return
	}
	// Sort by offset (then ID for determinism) so later refs overwrite earlier
	// ones at overlapping ranges, matching the latest-version-wins semantics.
	sort.Slice(overlaps, func(i, j int) bool {
		if overlaps[i].Offset != overlaps[j].Offset {
			return overlaps[i].Offset < overlaps[j].Offset
		}
		return overlaps[i].ID < overlaps[j].ID
	})
	for _, cref := range overlaps {
		buf := b.getChunk(base)
		rel := cref.Offset - base
		// Cap at the ref's declared live length: the payload may hold stale
		// bytes beyond cref.Length (e.g. a reused/legacy chunk), and copying
		// them would resurrect data outside the ref's extent.
		n := int64(cref.Length)
		if rel+n > int64(len(buf)) {
			n = int64(len(buf)) - rel
		}
		if n <= 0 {
			continue
		}
		// Water the committed bytes for this base in by reading the complete
		// [rel, rel+n) extent directly from the chunkstore. We deliberately do
		// NOT consult the sliced read cache here: ReadView caches partial
		// windows of a chunk, and treating such a short entry as the complete
		// committed image would silently zero-fill the remainder of the chunk's
		// real data on flush (its actual extent can exceed the cached window).
		// A full extent read is semantically identical to the old whole-chunk
		// hydration and guarantees no committed byte is lost.
		var chunk *metadata.ChunkMeta
		if err := b.exec.doMeta("read", func() error {
			var gerr error
			chunk, gerr = b.meta().GetChunk(ctx, cref.ID)
			return gerr
		}); err != nil {
			continue
		}
		var window []byte
		if err := b.exec.doChunk("read", func() error {
			var gerr error
			window, gerr = b.chunk.ReadChunkRange(ctx, chunk, rel, int32(n))
			return gerr
		}); err != nil {
			continue
		}
		if int64(len(window)) > n {
			window = window[:n]
		}
		if len(window) > 0 {
			copy(buf[rel:rel+int64(len(window))], window)
		}
	}
}

func (b *BufferedFile) incSpill() {
	if b.spillStats != nil {
		b.spillStats.IncStagingSpill()
	}
}

func (b *BufferedFile) incSpillErr() {
	if b.spillStats != nil {
		b.spillStats.IncStagingSpillErr()
	}
}

func (b *BufferedFile) incStagingLoad() {
	if b.spillStats != nil {
		b.spillStats.IncStagingLoad()
	}
}

// ============ read path ============

// ReadView returns the merged view of [off, off+n): the in-memory chunk
// buffers (dirty writes, authoritative over committed bytes) overlaid on the
// committed ChunkMap, with holes zero-filled. A nil or short result means
// fewer bytes are readable (EOF). Memory use is proportional to the number of
// accessed chunks (×64 MiB), not the total file size.
//
// Quirk preserved from the FUSE original: a chunk that was spilled to staging
// is not loaded back here, so a read of a spilled-but-not-yet-flushed range
// returns committed (stale) bytes until Flush reloads it. Flush always
// reloads spilled chunks before persisting, so the on-disk image is never
// corrupted — only a pre-flush read can see stale data under memory pressure.
func (b *BufferedFile) ReadView(ctx context.Context, off, n int64) ([]byte, error) {
	var metaInode *metadata.InodeMeta
	// chunks is the file's committed chunk references under either storage
	// model (V1 ChunkMap or V2 extent layout, roadmap §1.3b). Resolved in
	// the same doMeta critical section as GetInode so both see one snapshot.
	// The read discriminator probes the V2 extent surface only for inodes
	// with an empty ChunkMap, so V1 files pay nothing and metas without the
	// extent surface (es == nil) keep their unchanged V1 behavior. The probe
	// re-reads the inode row per call; amortize later by caching the resolved
	// view in BufferedFile and invalidating it on Flush (roadmap 1.3c+).
	var chunks []metadata.ChunkRef
	if err := b.exec.doMeta("read", func() error {
		var gerr error
		metaInode, gerr = b.meta().GetInode(ctx, b.inodeID)
		if gerr != nil {
			return gerr
		}
		es, _ := b.meta().(metadata.ExtentInodeService)
		chunks, gerr = metadata.ResolveFileChunks(ctx, es, metaInode)
		return gerr
	}); err != nil {
		return nil, err
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	// Effective file size accounts for un-flushed buffered tail that extends
	// past the committed size (e.g. after an O_APPEND write).
	size := int64(metaInode.Size)
	if b.logicalSize > size {
		size = b.logicalSize
	}
	if off >= size {
		return nil, nil
	}
	end := off + n
	if end > size {
		end = size
	}

	// Fast path: no committed chunks + no dirty chunks = every byte is a hole.
	if len(chunks) == 0 && len(b.chunkBufs) == 0 {
		return make([]byte, end-off), nil
	}

	// readChunkRange reads committed data for [start, end) from the resolved
	// chunk references. Returns the bytes and advances next. Caller holds b.mu.
	readChunkRange := func(start, end int64) []byte {
		result := make([]byte, 0, end-start)
		pos := start
		for pos < end {
			prevPos := pos
			// Find committed chunk overlapping [pos, end)
			found := false
			for _, cref := range chunks {
				cEnd := cref.Offset + int64(cref.Length)
				if cEnd <= pos || cref.Offset >= end {
					continue
				}
				// Compute the window of this cref that overlaps [pos, end).
				// Precompute it before fetching so a cache miss can issue an
				// exact range read instead of pulling the whole 64 MiB chunk.
				relStart := pos - cref.Offset
				if relStart < 0 {
					relStart = 0
				}
				relEnd := end - cref.Offset
				if relEnd > int64(cref.Length) {
					relEnd = int64(cref.Length)
				}
				if relEnd > relStart+int64(metadata.MaxChunkSize) {
					relEnd = relStart + int64(metadata.MaxChunkSize)
				}
				if relEnd <= relStart {
					// This cref is entirely behind pos; nothing to read here.
					found = true
					break
				}
				// Zero-fill any gap before this cref (in-memory only, never
				// fetched from the chunkstore).
				if cref.Offset > pos {
					gap := cref.Offset - pos
					if gap > end-pos {
						gap = end - pos
					}
					result = append(result, make([]byte, gap)...)
					pos += gap
				}
				// Fetch the needed window: prefer a cache hit, otherwise do a
				// precise range read of exactly [relStart, relEnd). The cache
				// is keyed by (chunkID, offset) so each window is cached
				// independently — a later read of the same window hits, and we
				// never let a partial window masquerade as the whole chunk.
				var window []byte
				if b.readCache != nil {
					if p, ok := b.readCache.Get(uint64(cref.ID), relStart); ok {
						if relEnd > relStart+int64(len(p)) {
							relEnd = relStart + int64(len(p))
						}
						if relEnd > relStart {
							window = p[:relEnd-relStart]
						}
					}
				}
				if window == nil {
					var chunk *metadata.ChunkMeta
					if err := b.exec.doMeta("read", func() error {
						var gerr error
						chunk, gerr = b.meta().GetChunk(ctx, cref.ID)
						return gerr
					}); err != nil {
						return nil
					}
					if err := b.exec.doChunk("read", func() error {
						var gerr error
						window, gerr = b.chunk.ReadChunkRange(ctx, chunk, relStart, int32(relEnd-relStart))
						return gerr
					}); err != nil {
						return nil
					}
					if b.readCache != nil && len(window) > 0 {
						b.readCache.Add(uint64(cref.ID), relStart, window)
					}
				}
				if len(window) > 0 {
					result = append(result, window...)
					pos += int64(len(window))
				}
				found = true
				break
			}
			if !found {
				// Hole: no chunk covers this region
				gap := end - pos
				result = append(result, make([]byte, gap)...)
				pos += gap
			}
			// Guard against infinite loop when pos didn't advance (empty range
			// read due to stale ref — cref.Length says data exists but the
			// actual stored extent is shorter). Break with a zero-fill so the
			// caller gets correct bytes without hanging.
			if pos == prevPos {
				remainder := end - pos
				result = append(result, make([]byte, remainder)...)
				pos = end
			}
		}
		return result
	}

	// Walk the read range in 64-MiB chunk steps. For each step, check
	// chunkBufs (dirty data) first, then the committed ChunkMap, then
	// zero-fill holes.
	out := make([]byte, 0, end-off)
	next := off
	for next < end {
		base := ChunkBase(next)
		n := end - next
		if n > metadata.MaxChunkSize {
			n = metadata.MaxChunkSize
		}

		// Check dirty chunk buffer first (authoritative for unflushed writes).
		// Buffers have committed data loaded, so they are complete.
		if b.chunkBufs != nil {
			if buf, ok := b.chunkBufs[base]; ok {
				within := next - base
				if within < 0 {
					within = 0
				}
				limit := int64(len(buf)) - within
				if n > limit {
					n = limit
				}
				if n > 0 {
					out = append(out, buf[within:within+n]...)
				}
				next += n
				continue
			}
		}

		// No dirty buffer — read committed data directly
		committed := readChunkRange(next, next+n)
		if committed != nil {
			out = append(out, committed...)
			next += int64(len(committed))
		} else {
			// Error or hole — zero fill
			out = append(out, make([]byte, n)...)
			next += n
		}
	}
	return out, nil
}

// ============ flush path ============

// Flush pushes the in-memory buffer to the chunk store and updates the inode's
// ChunkMap + size. It is idempotent: a clean BufferedFile flushes nothing.
//
// Only dirty chunks are written. Untouched committed chunks are preserved in
// the new ChunkMap without being loaded into memory — memory stays
// proportional to dirty data, not file size. The flush is serialized against
// concurrent flushes of the same inode (Executor.LockInode) spanning
// GetInode→UpdateInode, so two BufferedFile instances for one inode cannot
// interleave and lose refs.
//
// Flush owns the whole persistence tail: resolve policy, allocate+write via
// the shared ChunkWriter, record ledger transitions, wholesale-replace the
// ChunkMap, persist, delete the superseded chunk refs, and clear the buffer.
func (b *BufferedFile) Flush(ctx context.Context) (FlushResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.dirty {
		return FlushResult{}, nil
	}

	// Path lock: serialize concurrent Flushes of the same inode across distinct
	// BufferedFile instances (go-fuse inode cache eviction + rebuild).
	unlock := b.exec.lockInode(uint64(b.inodeID))
	defer unlock()

	// Read the current inode once and resolve its committed chunk refs under
	// whichever storage model it carries (V1 ChunkMap or V2 extent layout,
	// roadmap §1.3b/§1.3c). These are the OLD chunk refs this flush supersedes
	// at the end; a V2 row's ChunkMap is nil, so the refs must come from the
	// extent surface (ResolveFileChunks) rather than the inode row.
	var metaInode *metadata.InodeMeta
	var oldRefs []metadata.ChunkRef
	if err := b.exec.doMeta("flush", func() error {
		var gerr error
		metaInode, gerr = b.meta().GetInode(ctx, b.inodeID)
		if gerr != nil {
			return gerr
		}
		es, _ := b.meta().(metadata.ExtentInodeService)
		oldRefs, gerr = metadata.ResolveFileChunks(ctx, es, metaInode)
		return gerr
	}); err != nil {
		return FlushResult{}, err
	}
	// Surface the resolved refs on the projection so the write-attempt ledger
	// records them (recovery of a V2 file needs the chunk list even though the
	// row itself carries none). The commit path re-derives the storage model
	// from the serving surface, so this does not change its decision.
	metaInode.ChunkMap = oldRefs

	// Write-attempt ledger: record flush state transitions for observability
	// and crash recovery. The recovery worker (shared with S3) picks up
	// incomplete attempts and cleans up orphaned chunks.
	attemptID := fmt.Sprintf("%d-%d", b.inodeID, time.Now().UnixNano())
	b.recordFlushAttempt(ctx, attemptID, metaInode, metadata.WriteAttemptPending, "")

	policy := b.resolveChunkPolicy(ctx, metaInode)

	// Determine the file size to persist. logicalSize tracks the high-water
	// mark of writes since the last Clear() (which resets it to 0), so it can
	// be 0 (no dirty tail — use committed size), larger than committed (a write
	// extended the file — use logicalSize), OR smaller than committed (a
	// partial in-place overwrite that didn't reach the old EOF). Taking the max
	// prevents that last case from shrinking the file and silently dropping the
	// committed tail: e.g. pwrite(fd, 5B, offset=100) on a 200-byte file sets
	// logicalSize=105, and without the max the committed bytes [105,200) would
	// be truncated away. Legitimate truncation-down goes through Truncate/Setattr,
	// which commits the new smaller Size before the next Flush, so max() never
	// blocks it. This mirrors AppendWrite's own max(logicalSize, committed) rule.
	committedSize := int64(metaInode.Size)
	size := b.logicalSize
	if committedSize > size {
		size = committedSize
	}

	// Collect dirty chunk bases and sort them for deterministic allocation.
	dirtyBases := make([]int64, 0, len(b.dirtyMap))
	for base := range b.dirtyMap {
		dirtyBases = append(dirtyBases, base)
	}
	sort.Slice(dirtyBases, func(i, j int) bool { return dirtyBases[i] < dirtyBases[j] })

	// For each dirty chunk: merge dirty buffer with committed data (if any),
	// allocating a new chunk ID. Only chunks that are actually dirty are
	// loaded — untouched committed chunks are preserved as-is.
	type writtenRef struct {
		base int64
		ref  metadata.ChunkRef
	}
	written := make([]writtenRef, 0, len(dirtyBases))

	// Batch-allocate chunk IDs for all dirty chunks via the shared dispatch
	// pipeline (which sub-batches internally by metadata.MaxChunkAllocationBatch).
	chunks, err := b.writer().AllocateRanges(ctx, b.inodeID, policy, dirtyBases)
	if err != nil {
		return FlushResult{}, err
	}
	if len(chunks) > 0 {
		b.recordFlushAttempt(ctx, attemptID, metaInode, metadata.WriteAttemptChunksAllocated, "")

		// Write each dirty chunk. Buffers already have committed data loaded
		// (via loadCommittedChunkLocked), so they are complete images.
		for i, base := range dirtyBases {
			end := base + metadata.MaxChunkSize
			if end > size {
				end = size
			}
			chunkLen := int(end - base)

			// Use the dirty buffer directly — it has committed data merged in.
			// If the chunk was spilled to disk during a memory-pressure event,
			// load it back from the staging file. A load failure aborts the
			// entire flush — zero-filling here would silently corrupt data.
			var chunkData []byte
			if buf, ok := b.chunkBufs[base]; ok {
				n := chunkLen
				if n > len(buf) {
					n = len(buf)
				}
				chunkData = buf[:n]
			} else if _, spilled := b.spilledChunks[base]; spilled {
				loaded, err := b.loadFromDisk(base)
				if err != nil {
					return FlushResult{}, err
				}
				b.incStagingLoad()
				n := chunkLen
				if n > len(loaded) {
					n = len(loaded)
				}
				chunkData = loaded[:n]
			} else {
				chunkData = make([]byte, chunkLen) // hole: all zeros
			}

			// Write, commit (EC-skip), seal via the shared dispatch pipeline.
			chunk := chunks[i]
			newRef, err := b.writer().WriteAllocated(ctx, chunk, base, chunkData)
			if err != nil {
				return FlushResult{}, err
			}
			written = append(written, writtenRef{base: base, ref: newRef})
		}
	}

	// All chunks written to datanode (durable). Record before updating inode.
	b.recordFlushAttempt(ctx, attemptID, metaInode, metadata.WriteAttemptChunksDurable, "")

	// Build new ChunkMap: preserve untouched committed chunks + add written
	// chunks. Untouched committed chunks are NOT loaded into memory — just
	// copied as refs.
	writtenSet := make(map[int64]bool, len(written))
	for _, w := range written {
		writtenSet[w.base] = true
	}
	newRefs := make([]metadata.ChunkRef, 0, len(oldRefs)+len(written))
	// First: all untouched committed chunks (not in dirty set)
	for _, cref := range oldRefs {
		if !writtenSet[cref.Offset] {
			newRefs = append(newRefs, cref)
		}
	}
	// Second: all newly written chunks
	for _, w := range written {
		newRefs = append(newRefs, w.ref)
	}

	// Model-aware commit: land newRefs as the file's data under whichever
	// storage model the serving surface supports (roadmap §1.3c). With a V2
	// extent surface a single ≤ MaxInlineExtentSize chunk becomes an inline
	// extent and anything larger (or multi-chunk) spills to COW extent pages;
	// without the surface it falls back to the V1 ChunkMap update. The helper
	// sets Size/MTime/CTime itself, so the projection only carries newRefs for
	// the ledger's Committed record below.
	metaInode.ChunkMap = newRefs
	if err := b.exec.doMeta("flush", func() error {
		return metadata.CommitChunkRefsModelAware(ctx, b.meta(), metaInode, newRefs, size)
	}); err != nil {
		b.recordFlushAttempt(ctx, attemptID, metaInode, metadata.WriteAttemptRecoveryNeeded, err.Error())
		return FlushResult{}, err
	}

	// Flush succeeded: mark attempt committed so recovery worker knows this
	// inode's chunk references are authoritative.
	b.recordFlushAttempt(ctx, attemptID, metaInode, metadata.WriteAttemptCommitted, "")

	// Reclaim the superseded chunks.
	for _, cref := range oldRefs {
		if err := b.exec.doMeta("flush", func() error {
			return b.meta().DeleteChunk(ctx, cref.ID)
		}); err != nil {
			slog.Warn("chunkstore: flush: delete superseded chunk", "chunk_id", cref.ID, "error", err)
		}
	}

	b.Clear()
	return FlushResult{NewRefs: newRefs, NewSize: size}, nil
}

// writer returns the shared ChunkWriter dispatch pipeline, built lazily so the
// meta accessor is resolved at call time (hot-swap safe). Caller holds b.mu.
func (b *BufferedFile) writer() *ChunkWriter {
	if b.dispatch == nil {
		b.dispatch = NewChunkWriter(
			b.meta(),
			b.chunk,
			WithMetaAccessor(func() ChunkLifecycle { return b.meta() }),
			WithDoMeta(b.exec.DoMeta),
			WithDoChunk(b.exec.DoChunk),
		)
	}
	return b.dispatch
}

// resolveChunkPolicy picks the placement policy for this flush from the
// inode's containing bucket; an orphan inode (no bucket root) or a lookup
// failure falls back to the configured default policy.
func (b *BufferedFile) resolveChunkPolicy(ctx context.Context, metaInode *metadata.InodeMeta) metadata.PlacementPolicy {
	if metaInode.BucketRoot == 0 {
		return b.defaultPolicy
	}
	var bucket *metadata.BucketInfo
	if err := b.exec.doMeta("flush", func() error {
		var gerr error
		bucket, gerr = b.meta().GetBucketByRoot(ctx, metaInode.BucketRoot)
		return gerr
	}); err != nil {
		slog.Warn("chunkstore: flush: bucket policy lookup failed — using default", "bucket_root", metaInode.BucketRoot, "error", err)
		return b.defaultPolicy
	}
	return bucket.Policy
}

// recordFlushAttempt forwards a ledger transition (best-effort; ledger errors
// are swallowed — the write path must not fail on observability).
func (b *BufferedFile) recordFlushAttempt(ctx context.Context, attemptID string, meta *metadata.InodeMeta, state metadata.WriteAttemptState, lastErr string) {
	if b.ledger == nil {
		return
	}
	b.ledger.Record(ctx, attemptID, meta, state, lastErr)
}

// ============ truncate / preallocate paths ============

// Truncate adjusts the buffer image to newSize. committedSize is the inode's
// committed size (used as the pre-buffer end when the buffer has no tail).
//
//   - Down: buffers and spill files beyond the target base are dropped, the
//     last chunk is trimmed to the exact size, and the file is marked dirty.
//     Spill files beyond the new base are removed too, so truncation leaves no
//     orphaned staging files (previously a leak on O_TRUNC).
//   - Up: the new region is zeros (holes); nothing is hydrated — Flush creates
//     the appropriate chunks. The file is marked dirty.
//   - Equal: no buffer change.
func (b *BufferedFile) Truncate(newSize, committedSize int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	oldEnd := b.logicalSize
	if oldEnd == 0 {
		oldEnd = committedSize
	}

	if newSize < oldEnd {
		newBase := ChunkBase(newSize)
		for base := range b.chunkBufs {
			if base > newBase {
				delete(b.chunkBufs, base)
				delete(b.dirtyMap, base)
			} else if base == newBase && newSize%metadata.MaxChunkSize != 0 {
				// Trim the last chunk to the exact size.
				b.chunkBufs[base] = b.chunkBufs[base][:newSize%metadata.MaxChunkSize]
			}
		}
		// Also drop staging files for chunks beyond the new base, so a
		// truncate-down leaves no orphaned spilled chunks behind.
		for base, path := range b.spilledChunks {
			if base > newBase {
				_ = os.Remove(path)
				delete(b.spilledChunks, base)
			}
		}
		b.dirty = true
		b.logicalSize = newSize
	} else if newSize > oldEnd {
		// Extension: new region is zeros (holes). No need to load anything —
		// the flush will create appropriate chunks.
		b.dirty = true
		b.logicalSize = newSize
	}
	// newSize == oldEnd: no buffer change.
}

// TouchRange is the preallocate (posix_fallocate) default path: extend the
// buffered image to cover [off, end), loading committed data first so
// untouched regions preserve their committed content. When keepSize is false
// and the range runs past the current extent, metaInode.Size (and the logical
// tail) are grown to end. mtime is refreshed. Persistence of metaInode is the
// caller's job, after this returns (the buffer lock is released first).
func (b *BufferedFile) TouchRange(ctx context.Context, metaInode *metadata.InodeMeta, off, end int64, keepSize bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ef := b.logicalSize
	if int64(metaInode.Size) > ef {
		ef = int64(metaInode.Size)
	}
	// Extend chunkBufs to cover the new range, loading committed data first so
	// that untouched regions preserve their committed content.
	for base := ChunkBase(off); base < end; base += metadata.MaxChunkSize {
		if _, exists := b.chunkBufs[base]; !exists {
			b.loadCommittedChunkLocked(ctx, base)
		}
		_ = b.getChunk(base) // allocates zero-filled chunk if missing
	}
	b.dirty = true
	if !keepSize && end > ef {
		metaInode.Size = end
		b.logicalSize = end
	}
	metaInode.MTime = time.Now().UnixNano()
}

// ZeroRange zero-fills [off, off+size). If extend is true (default allocate or
// ZERO_RANGE without KEEP_SIZE) the file (and logical Size) is grown to cover
// the window; otherwise (PUNCH_HOLE, or ZERO_RANGE with KEEP_SIZE) the window
// is clamped to the current length and Size is untouched. Persistence of
// metaInode is the caller's job, after this returns.
func (b *BufferedFile) ZeroRange(ctx context.Context, metaInode *metadata.InodeMeta, off, size uint64, extend bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cur := b.logicalSize
	if int64(metaInode.Size) > cur {
		cur = int64(metaInode.Size)
	}
	end := off + size
	if !extend && int64(end) > cur {
		// Clamp to current committed + buffered length (never grow on punch).
		end = uint64(cur)
	}
	if end <= off {
		// Nothing to zero; still refresh mtime per POSIX and persist.
		metaInode.MTime = time.Now().UnixNano()
		return nil
	}
	// Load committed chunks overlapping the zero range so the non-zeroed
	// portion of each affected chunk is preserved. Only the affected chunks
	// are loaded — not the entire file. The refs resolve under either storage
	// model (V1 ChunkMap or V2 extent layout, roadmap §1.3b/§1.3c): a V2 row's
	// ChunkMap is nil, so its refs must come from the extent surface.
	es, _ := b.meta().(metadata.ExtentInodeService)
	refs, err := metadata.ResolveFileChunks(ctx, es, metaInode)
	if err != nil {
		return err
	}
	for base := ChunkBase(int64(off)); base < int64(end); base += metadata.MaxChunkSize {
		if _, ok := b.chunkBufs[base]; ok {
			continue // already in buffer (from prior write)
		}
		// Find committed chunk for this base.
		for _, cref := range refs {
			cEnd := cref.Offset + int64(cref.Length)
			if cEnd <= base || cref.Offset >= base+metadata.MaxChunkSize {
				continue
			}
			// Load the committed bytes of this chunk into the buffer so the
			// non-zeroed portion is preserved. We deliberately skip the sliced
			// read cache here: ReadView may have cached a short window as this
			// chunk's image, and treating it as complete would zero committed
			// data beyond the window, which a ZERO_RANGE/punch would then
			// persist as holes.
			buf := b.getChunk(base)
			rel := cref.Offset - base
			// Preserve the complete committed extent of this chunk by reading it
			// directly from the chunkstore.
			var chunk *metadata.ChunkMeta
			if err := b.exec.doMeta("flush", func() error {
				var gerr error
				chunk, gerr = b.meta().GetChunk(ctx, cref.ID)
				return gerr
			}); err != nil {
				return err
			}
			n := int64(len(buf)) - rel
			if n < 0 {
				n = 0
			}
			var window []byte
			if err := b.exec.doChunk("flush", func() error {
				var gerr error
				window, gerr = b.chunk.ReadChunkRange(ctx, chunk, rel, int32(n))
				return gerr
			}); err != nil {
				return err
			}
			if int64(len(window)) > n {
				window = window[:n]
			}
			if len(window) > 0 {
				copy(buf[rel:rel+int64(len(window))], window)
			}
			break
		}
	}
	// Zero within each affected chunk.
	for base := ChunkBase(int64(off)); base < int64(end); base += metadata.MaxChunkSize {
		buf := b.getChunk(base)
		zeroStart := int64(off) - base
		if zeroStart < 0 {
			zeroStart = 0
		}
		zeroEnd := int64(end) - base
		if zeroEnd > int64(len(buf)) {
			zeroEnd = int64(len(buf))
		}
		for i := zeroStart; i < zeroEnd; i++ {
			buf[i] = 0
		}
		b.markDirty(base)
	}
	// Grow the logical Size only when the window runs past the file's current
	// extent. `cur` (captured at the top, before the buffer was grown to the
	// window) is the pre-operation extent = max(buffered tail, committed size);
	// the committed Size lags the buffer until Flush, so comparing against it
	// alone would shrink the logical size below buffered data when zeroing an
	// in-range window (truncating on the next whole-file Flush rebuild).
	if extend && int64(end) > cur {
		metaInode.Size = int64(end)
		b.logicalSize = int64(end)
	}
	metaInode.MTime = time.Now().UnixNano()
	return nil
}

// ============ observability ============

// Size returns the buffered logical tail (high-water mark of writes since
// Clear). Getattr uses it to report un-flushed extensions.
func (b *BufferedFile) Size() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.logicalSize
}

// Dirty reports whether the buffer holds un-flushed changes.
func (b *BufferedFile) Dirty() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.dirty
}

// DirtyBytes returns the current in-memory dirty byte count.
func (b *BufferedFile) DirtyBytes() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.dirtyBytes
}

// BufferChunk returns the resident in-memory buffer for a chunk base, or nil if
// the base has no in-memory image (never written, or spilled to staging). It is
// a test-support accessor for verifying buffer-image extent (e.g. that a
// KEEP_SIZE preallocate physically extended the buffer without extending the
// logical Size); production code should use ReadView instead.
func (b *BufferedFile) BufferChunk(base int64) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.chunkBufs[base]
}

// MarkDirty flags the file dirty without touching buffers (used by the O_TRUNC
// open path, which clears the image then needs the truncation flushed).
func (b *BufferedFile) MarkDirty() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dirty = true
}

// SetReadCache installs (or replaces) the read-path cache.
func (b *BufferedFile) SetReadCache(c ReadCache) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.readCache = c
}

// CleanStagingDir removes all files in the staging directory. Called once at
// Mount time to clean up orphaned files left by previous runs or crashes.
// Files in active use belong to the current process (PID-unique path) or have
// been cleaned by Clear, so this only runs at startup.
func CleanStagingDir(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return // dir doesn't exist or can't be read — nothing to clean
	}
	for _, e := range entries {
		if !e.IsDir() {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}
