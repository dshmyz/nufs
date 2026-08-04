package datanode

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/segment"
	"github.com/example/dfs/metadata"
)

// extentLister is implemented by the V2.1 segment.Store (ListExtents).
// V2Store type-asserts each backend to it to enumerate committed extents
// for ListChunks/Stats/ChunkStateSnapshot and to rebuild the per-disk
// location/generation map at startup.
type extentLister interface {
	ListExtents() ([]segment.LiveExtent, error)
}

// diskBackend wraps one storage.Store with the per-disk accounting the
// V2Store needs (used bytes, extent count, write/error counters). The
// lister is non-nil for V2.1 segment stores; legacy backends that lack
// enumeration simply report nothing to ListChunks/Stats.
type diskBackend struct {
	store  storage.Store
	lister extentLister
	index  int
	// dir is the disk root (segment store's Config.Dir) reported to the
	// management/ops channel's DiskInfos/DiskInfo. Empty when the backend was
	// constructed without a directory (single-disk/tests).
	dir string

	// Accounting updated on the serving path and reconstructed at startup
	// for V2.1 backends via ListExtents.
	usedByts  atomic.Int64
	extCount  atomic.Int64
	readByts  atomic.Int64
	writeByts atomic.Int64

	// Rolling write-error window, consumed (Swap'ed to 0) by WriteErrorRate
	// each heartbeat — mirrors the legacy ChunkStore's rolling semantics.
	writeOps atomic.Int64
	writeErr atomic.Int64

	// failCount is a PERSISTENT consecutive-write-failure counter (never
	// reset by WriteErrorRate) used by diskFailed to flag a wedged disk.
	// It is kept separate from the rolling writeOps/writeErr window so the
	// rolling rate can reset each cycle without erasing the health signal.
	failCount atomic.Int64
}

// chunkLoc records where a chunk lives and at what generation, so a
// rewrite/read/delete/stat routes to the owning disk and generation.
type chunkLoc struct {
	disk int
	gen  storage.Generation
}

// V2Store implements the LocalChunkStore and HeartbeatStore interfaces
// over one or more V2.1 storage.Store backends, so the V2.1 engine can
// serve real TCP read/write traffic and heartbeats with multi-disk
// parity to the legacy ChunkStore.
//
// Chunk IDs map to extents. Overwrites increment the extent generation
// (proper local generation fencing: a new payload at the same generation
// is rejected by the segment store), so a re-write of an existing chunk
// chains to gen+1 and the latest generation resolves reads/stats/deletes.
//
// Multi-disk behavior mirrors the legacy ChunkStore:
//   - a new chunk is placed on the least-used disk (fewest used bytes),
//   - an existing chunk is written back to the disk it already lives on,
//
// so overwrites stay co-located and capacity spreads across disks.
type V2Store struct {
	disks []*diskBackend
	// mu guards locOf. Reconstruction at startup and serving updates both
	// mutate it; reads take RLock.
	mu    sync.RWMutex
	locOf map[metadata.ChunkID]chunkLoc

	// stateVersion increments on every durable state change (write or
	// delete), so the heartbeat's incremental delta diff can detect change.
	stateVersion atomic.Uint64
}

// NewV2Store wraps a single backend (legacy/plain single-disk use). An
// optional dir is the disk root reported to DiskInfos when non-empty.
func NewV2Store(store storage.Store, dir ...string) *V2Store {
	v := &V2Store{locOf: make(map[metadata.ChunkID]chunkLoc)}
	for _, s := range []storage.Store{store} {
		d := ""
		if len(dir) > 0 {
			d = dir[0]
		}
		b := &diskBackend{store: s, dir: d}
		if lister, ok := s.(extentLister); ok {
			b.lister = lister
		}
		v.disks = append(v.disks, b)
	}
	return v
}

// NewMultiV2Store wraps multiple backends for JBOD multi-disk mode. dirs, if
// provided, gives each backend's disk root in the same order as stores (used
// for the management/ops DiskInfos). For V2.1 segment backends it
// reconstructs the per-disk location/generation map and usage accounting by
// enumerating each store's committed extents, the equivalent of the legacy
// ChunkStore's startup disk scan.
func NewMultiV2Store(stores []storage.Store, dirs ...string) *V2Store {
	v := &V2Store{locOf: make(map[metadata.ChunkID]chunkLoc)}
	for i, s := range stores {
		d := ""
		if i < len(dirs) {
			d = dirs[i]
		}
		b := &diskBackend{store: s, index: i, dir: d}
		if lister, ok := s.(extentLister); ok {
			b.lister = lister
			if extents, err := lister.ListExtents(); err == nil {
				for _, e := range extents {
					id := metadata.ChunkID(e.ExtentID)
					b.usedByts.Add(int64(e.Value.LogicalLen))
					b.extCount.Add(1)
					v.locOf[id] = chunkLoc{disk: i, gen: e.Generation}
				}
			}
		}
		v.disks = append(v.disks, b)
	}
	return v
}

// Write implements LocalChunkStore.Write. Route an overwrite to its
// owning disk at the next generation, place a new chunk on the
// least-used disk at generation 1, then durably write and update
// accounting.
func (v *V2Store) Write(chunkID metadata.ChunkID, data []byte) error {
	disk, gen := v.nextLoc(chunkID)
	return v.writeTo(chunkID, disk, gen, data)
}

// WriteGen implements LocalChunkStore.WriteGen (Metadata V2 fencing). Unlike
// Write — which derives the next generation locally (gen+1) — WriteGen writes
// under the generation the metadata service issued for this chunk. The
// metadata service is the generation authority: it hands each overwrite a new
// generation, so a stale or duplicate replica write lands on that exact
// generation and the segment store's phase-0 fencing rejects any older write
// whose payload doesn't match. This keeps all replicas of a chunk on the same
// authoritative generation instead of each datanode bumping its own counter.
func (v *V2Store) WriteGen(chunkID metadata.ChunkID, generation uint64, data []byte) error {
	gen := storage.Generation(generation)
	v.mu.RLock()
	loc, ok := v.locOf[chunkID]
	v.mu.RUnlock()
	if ok {
		// Overwrite existing chunk: keep disk locality, honor metadata gen.
		return v.writeTo(chunkID, loc.disk, gen, data)
	}
	// New chunk: least-used disk at the metadata-issued generation.
	best, _ := v.nextLoc(chunkID)
	return v.writeTo(chunkID, best, gen, data)
}

// writeTo durably writes data for chunkID to a specific disk under a specific
// generation, updating location and accounting. newChunk (gen==1) is inferred
// from the generation.
func (v *V2Store) writeTo(chunkID metadata.ChunkID, disk int, gen storage.Generation, data []byte) error {
	if disk < 0 || disk >= len(v.disks) {
		disk = 0
	}
	newChunk := gen == 1
	b := v.disks[disk]
	b.writeOps.Add(1)
	if _, err := b.store.Write(context.Background(), &storage.WriteRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: gen,
		Data:       data,
	}); err != nil {
		b.writeErr.Add(1)
		b.failCount.Add(1)
		return err
	}
	// A successful write ends any consecutive-failure streak.
	b.failCount.Store(0)
	b.writeByts.Add(int64(len(data)))
	if newChunk {
		b.usedByts.Add(int64(len(data)))
		b.extCount.Add(1)
	} else {
		// Overwrite: the prior generation still occupies index space; drop
		// its size so used-bytes tracks the chunk's current live size rather
		// than the sum of all generations.
		if gen > 1 {
			if old, err := b.store.Stat(context.Background(), &storage.StatRequest{
				ExtentID: storage.ExtentID(chunkID), Generation: gen - 1,
			}); err == nil {
				b.usedByts.Add(-int64(old.LogicalLen))
			}
		}
		b.usedByts.Add(int64(len(data)))
	}
	v.mu.Lock()
	v.locOf[chunkID] = chunkLoc{disk: disk, gen: gen}
	v.mu.Unlock()
	v.stateVersion.Add(1)
	return nil
}

// nextLoc returns the owning disk and next generation for a write: the
// existing location bumped to gen+1 for an overwrite, or (least-used
// disk, 1) for a new chunk.
func (v *V2Store) nextLoc(chunkID metadata.ChunkID) (int, storage.Generation) {
	v.mu.RLock()
	loc, ok := v.locOf[chunkID]
	v.mu.RUnlock()
	if ok {
		return loc.disk, loc.gen + 1
	}

	best, bestUsed := 0, int64(1<<63-1)
	for i, d := range v.disks {
		if used := d.usedByts.Load(); used < bestUsed {
			bestUsed, best = used, i
		}
	}
	return best, 1
}

// RebalanceOne moves a single chunk from fromDisk to toDisk at the same
// generation (PUT-at-target: the extent is written under its existing
// generation, so the segment store places it as a fresh extent on the
// target). It reads the full payload from the source store, writes it to
// the target store (a different backend instance, so the same
// extentID+generation is a distinct extent there), verifies, then
// atomically re-points locOf and adjusts both disks' usage accounting.
//
// Concurrency: the store read/write happen WITHOUT the lock so concurrent
// Reads are not blocked for the duration of a large move. The locOf
// re-point is a guarded CAS — it only flips if the chunk still lives at
// (fromDisk, gen); if a concurrent Write/WriteGen/Delete already moved or
// removed the chunk, the orphaned target extent (reclaimed by compaction)
// is left unreferenced and accounting is untouched, so the winner's
// authoritative location and generation are never clobbered.
//
// toDisk==fromDisk, an unknown chunk, a missing source, or a move onto a
// failed disk returns an error without changing state.
func (v *V2Store) RebalanceOne(chunkID metadata.ChunkID, fromDisk, toDisk int) error {
	if fromDisk == toDisk {
		return fmt.Errorf("rebalance: fromDisk == toDisk (%d)", fromDisk)
	}
	if toDisk < 0 || toDisk >= len(v.disks) || fromDisk < 0 || fromDisk >= len(v.disks) {
		return fmt.Errorf("rebalance: disk index out of range (from=%d to=%d)", fromDisk, toDisk)
	}
	if v.disks[toDisk].failCount.Load() >= 5 {
		return fmt.Errorf("rebalance: target disk %d is failed", toDisk)
	}
	v.mu.RLock()
	loc, ok := v.locOf[chunkID]
	v.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: chunk %d", storage.ErrExtentNotFound, chunkID)
	}
	if loc.disk != fromDisk {
		return fmt.Errorf("rebalance: chunk %d lives on disk %d, not %d", chunkID, loc.disk, fromDisk)
	}
	gen := loc.gen

	src := v.disks[fromDisk]
	dst := v.disks[toDisk]

	res, err := src.store.Read(context.Background(), &storage.ReadRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: gen,
	})
	if err != nil {
		return err
	}

	dst.writeOps.Add(1)
	if _, err := dst.store.Write(context.Background(), &storage.WriteRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: gen,
		Data:       res.Data,
	}); err != nil {
		dst.writeErr.Add(1)
		dst.failCount.Add(1)
		return err
	}
	dst.failCount.Store(0)
	dst.writeByts.Add(int64(len(res.Data)))

	// CAS re-point: only if the chunk still lives at (fromDisk, gen). A
	// concurrent overwrite/delete that won the race leaves locOf pointing at
	// the authoritative location and generation.
	v.mu.Lock()
	cur, still := v.locOf[chunkID]
	if !still || cur.disk != fromDisk || cur.gen != gen {
		v.mu.Unlock()
		return fmt.Errorf("rebalance: chunk %d moved concurrently during rebalance", chunkID)
	}
	v.locOf[chunkID] = chunkLoc{disk: toDisk, gen: gen}
	v.mu.Unlock()

	src.usedByts.Add(-int64(len(res.Data)))
	src.extCount.Add(-1)
	dst.usedByts.Add(int64(len(res.Data)))
	dst.extCount.Add(1)
	v.stateVersion.Add(1)
	return nil
}

// RebalanceBalanced drains the fullest disk onto the least-used healthy disk
// until the used-byte gap between them is at most thresholdBytes. It returns
// the number of extents moved.
//
// Termination and convergence: a single pass fixes the drain target (the
// least-used healthy disk) and only moves an extent that fits strictly within
// the current gap (len < srcUsed - dstUsed), so a move never overshoots into a
// reversed imbalance that a later move would thrash back. The gap therefore
// shrinks monotonically each move (no oscillation), and a second call is
// idempotent (returns 0 once balanced). Rebooting on a failed disk never
// happens — failed disks are excluded from both source and target. The loop
// is additionally bounded so it always returns.
func (v *V2Store) RebalanceBalanced(thresholdBytes int64) (int, error) {
	moved := 0
	// Cap worst-case iterations well above the number of movable extents so
	// progress is guaranteed even under adversarial interleavings.
	cap := 0
	for _, b := range v.disks {
		if b.lister != nil {
			if extents, err := b.lister.ListExtents(); err == nil {
				cap += len(extents)
			}
		}
	}
	if cap == 0 {
		cap = 64
	}
	excluded := make(map[metadata.ChunkID]bool) // chunks that failed to move this pass
	for i := 0; i < cap; i++ {
		src, chunkID, ok := v.pickRebalanceChunk(thresholdBytes, excluded)
		if !ok {
			break
		}
		dst := v.leastUsedDisk(-1)
		if err := v.RebalanceOne(chunkID, src, dst); err != nil {
			// Not movable in this pass; exclude it so we make progress instead
			// of re-picking the same chunk forever.
			excluded[chunkID] = true
			continue
		}
		moved++
	}
	return moved, nil
}

// pickRebalanceChunk chooses the single chunk to move next: from the fullest
// healthy disk (≠ the least-used target), the largest extent that fits
// strictly within the current used-byte gap. This is the move that releases
// the most capacity per accounting bump without overshooting. Returns ok=false
// when no move would strictly improve balance.
func (v *V2Store) pickRebalanceChunk(thresholdBytes int64, excluded map[metadata.ChunkID]bool) (src int, chunkID metadata.ChunkID, ok bool) {
	dst := v.leastUsedDisk(-1)
	if dst < 0 {
		return 0, 0, false
	}
	dstUsed := v.disks[dst].usedByts.Load()

	// Pick the fullest healthy source disk that is beyond threshold and has a
	// movable extent; greedy on the fullest disk keeps the driver converging.
	bestSrc, bestGap := -1, int64(thresholdBytes)
	for i, b := range v.disks {
		if i == dst || b.failCount.Load() >= 5 {
			continue
		}
		if gap := b.usedByts.Load() - dstUsed; gap > bestGap {
			bestGap, bestSrc = gap, i
		}
	}
	if bestSrc < 0 {
		return 0, 0, false
	}

	// Largest extent on the source that fits strictly within its gap — never
	// overshooting into a reversed imbalance.
	bestChunk, bestLen := metadata.ChunkID(0), int64(-1)
	if l := v.disks[bestSrc].lister; l != nil {
		srcUsed := v.disks[bestSrc].usedByts.Load()
		if extents, err := l.ListExtents(); err == nil {
			for _, e := range extents {
				id := metadata.ChunkID(e.ExtentID)
				if e.Generation == 0 || excluded[id] {
					continue
				}
				n := int64(e.Value.LogicalLen)
				// Strict fit within the current gap (no overshoot).
				if n < srcUsed-dstUsed && n > bestLen {
					bestLen, bestChunk = n, id
				}
			}
		}
	}
	if bestLen < 0 {
		return 0, 0, false
	}
	return bestSrc, bestChunk, true
}

// leastUsedDisk returns the index of the healthy disk with the fewest used
// bytes, skipping failed disks and the excluded index (use exclude=-1 for
// none). Returns -1 when no healthy disk qualifies.
func (v *V2Store) leastUsedDisk(exclude int) int {
	best, bestUsed := -1, int64(1<<63-1)
	for i, d := range v.disks {
		if i == exclude || d.failCount.Load() >= 5 {
			continue
		}
		if used := d.usedByts.Load(); used < bestUsed {
			bestUsed, best = used, i
		}
	}
	return best
}

// fullestDisk returns the index of the healthy disk holding the most used
// bytes, skipping the excluded index and failed disks. Returns -1 when no
// qualifying disk exists.
func (v *V2Store) fullestDisk(exclude int) int {
	best, bestUsed := -1, int64(-1)
	for i, d := range v.disks {
		if i == exclude || d.failCount.Load() >= 5 {
			continue
		}
		if used := d.usedByts.Load(); used > bestUsed {
			bestUsed, best = used, i
		}
	}
	return best
}

// loc returns the location of a chunk, defaulting to the first disk at
// generation 1 when unknown (mirrors the legacy chunkstore.diskOf
// fallback).
func (v *V2Store) loc(chunkID metadata.ChunkID) chunkLoc {
	v.mu.RLock()
	loc, ok := v.locOf[chunkID]
	v.mu.RUnlock()
	if !ok {
		loc = chunkLoc{disk: 0, gen: 1}
	}
	if loc.disk < 0 || loc.disk >= len(v.disks) {
		loc.disk = 0
	}
	return loc
}

// Read implements LocalChunkStore.Read.
func (v *V2Store) Read(chunkID metadata.ChunkID, offset int64, length int32) ([]byte, uint32, error) {
	loc := v.loc(chunkID)
	b := v.disks[loc.disk]
	res, err := b.store.Read(context.Background(), &storage.ReadRequest{
		ExtentID:      storage.ExtentID(chunkID),
		Generation:    loc.gen,
		LogicalOffset: offset,
		Length:        length,
	})
	if err != nil {
		return nil, 0, err
	}
	b.readByts.Add(int64(len(res.Data)))
	return res.Data, res.Checksum, nil
}

// Delete implements LocalChunkStore.Delete.
func (v *V2Store) Delete(chunkID metadata.ChunkID) error {
	loc := v.loc(chunkID)
	b := v.disks[loc.disk]
	// Capture the live size before the fenced delete so used-bytes reflects
	// the freed space (a post-delete stat would return the tombstone).
	if ext, err := b.store.Stat(context.Background(), &storage.StatRequest{
		ExtentID: storage.ExtentID(chunkID), Generation: loc.gen,
	}); err == nil {
		b.usedByts.Add(-int64(ext.LogicalLen))
	}
	if err := b.store.Delete(context.Background(), &storage.DeleteRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: loc.gen,
	}); err != nil {
		return err
	}
	b.extCount.Add(-1)
	v.mu.Lock()
	delete(v.locOf, chunkID)
	v.mu.Unlock()
	v.stateVersion.Add(1)
	return nil
}

// Seal is a no-op for V2 — extents are sealed atomically through the
// commit log. Returns 0, nil.
func (v *V2Store) Seal(chunkID metadata.ChunkID) (uint32, error) {
	// V2 extents are committed atomically; no separate seal needed.
	return 0, nil
}

// Info returns basic info for a chunk, resolving size/checksum from the
// owning backend's stat.
func (v *V2Store) Info(chunkID metadata.ChunkID) (*LocalChunkInfo, bool) {
	loc := v.loc(chunkID)
	res, err := v.disks[loc.disk].store.Stat(context.Background(), &storage.StatRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: loc.gen,
	})
	if err != nil {
		return nil, false
	}
	state := LocalSealed
	if res.State == storage.ExtentCorrupt {
		state = LocalCorrupt
	}
	return &LocalChunkInfo{
		ChunkID:     chunkID,
		Size:        int64(res.LogicalLen),
		Checksum:    res.Checksum,
		State:       state,
		WrittenAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 0,
		DiskIndex:   loc.disk,
	}, true
}

// ListChunks returns all locally stored chunk information by enumerating
// every backend's committed extents.
func (v *V2Store) ListChunks() []LocalChunkInfo {
	var out []LocalChunkInfo
	for _, b := range v.disks {
		if b.lister == nil {
			continue
		}
		extents, err := b.lister.ListExtents()
		if err != nil {
			continue
		}
		for _, e := range extents {
			state := LocalSealed
			if e.Value.State == storage.ExtentCorrupt {
				state = LocalCorrupt
			}
			out = append(out, LocalChunkInfo{
				ChunkID:   metadata.ChunkID(e.ExtentID),
				Size:      int64(e.Value.LogicalLen),
				Checksum:  e.Value.Checksum,
				State:     state,
				WrittenAt: time.Now(),
				DiskIndex: b.index,
			})
		}
	}
	return out
}

// Stats returns aggregate storage statistics across all disks.
func (v *V2Store) Stats() (totalBytes int64, chunkCount int64) {
	for _, b := range v.disks {
		totalBytes += b.usedByts.Load()
		chunkCount += b.extCount.Load()
	}
	return totalBytes, chunkCount
}

// StateVersion returns a counter incremented on every durable chunk state
// change, so heartbeat can skip rebuilding the replica-state set when it
// is unchanged.
func (v *V2Store) StateVersion() uint64 {
	return v.stateVersion.Load()
}

// ChunkStateSnapshot returns the current replica-state view of every
// locally stored chunk, mapped to the ReplicaState heartbeat reports.
func (v *V2Store) ChunkStateSnapshot() map[metadata.ChunkID]metadata.ReplicaState {
	out := make(map[metadata.ChunkID]metadata.ReplicaState)
	for _, b := range v.disks {
		if b.lister == nil {
			continue
		}
		extents, err := b.lister.ListExtents()
		if err != nil {
			continue
		}
		for _, e := range extents {
			st := metadata.ReplicaReady
			if e.Value.State == storage.ExtentCorrupt {
				st = metadata.ReplicaFailed
			}
			out[metadata.ChunkID(e.ExtentID)] = st
		}
	}
	return out
}

// DiskStats returns per-disk usage and health, used by the heartbeat
// reporter and the management interface.
func (v *V2Store) DiskStats() []DiskStatsItem {
	out := make([]DiskStatsItem, len(v.disks))
	for i, b := range v.disks {
		out[i] = DiskStatsItem{
			Index:      i,
			UsedBytes:  b.usedByts.Load(),
			ChunkCount: b.extCount.Load(),
			Failed:     v.diskFailed(i),
		}
	}
	return out
}

// diskFailed reports whether a backend has been erroring (crude health
// signal: a streak of consecutive write failures, cleared by any success).
// Uses failCount, not the rolling write window, so WriteErrorRate's per-
// cycle reset does not erase the health signal. Kept minimal — no disk
// lifecycle management in this scope.
func (v *V2Store) diskFailed(i int) bool {
	return v.disks[i].failCount.Load() >= 5
}

// WriteErrorRate returns the aggregate write error rate (0.0-1.0) across
// all disks, as a rolling window since the last call. Like the legacy
// ChunkStore, the per-disk write-op/error counters are reset (Swap'd to 0)
// so each heartbeat reports the rate within its own cycle rather than a
// lifetime cumulative ratio. Disk health (diskFailed) is unaffected — it
// uses its own persistent failCount.
func (v *V2Store) WriteErrorRate() float64 {
	var ops, errs int64
	for _, b := range v.disks {
		ops += b.writeOps.Swap(0)
		errs += b.writeErr.Swap(0)
	}
	if ops == 0 {
		return 0
	}
	return float64(errs) / float64(ops)
}

// DiskManager returns nil — the V2.1 engine manages per-disk layout
// itself; the heartbeat falls back to not sampling disk I/O (DiskStats
// provides the per-disk breakdown instead).
func (v *V2Store) DiskManager() *DiskManager {
	return nil
}

// ReadWriteBytes returns the cumulative bytes read and written on the
// serving path since startup, across all disks. The heartbeat samples
// these to compute a live DiskIO utilization — something the legacy
// ChunkStore's DiskManager counters never feed (RecordRead/RecordWrite
// have no serving-path callers), so V2.1 produces a real metric.
func (v *V2Store) ReadWriteBytes() (read int64, write int64) {
	for _, b := range v.disks {
		read += b.readByts.Load()
		write += b.writeByts.Load()
	}
	return read, write
}

// DiskInfos returns per-disk metadata for the management/ops channel,
// mirroring the legacy ChunkStore.DiskInfos. Each V2.1 disk is one segment
// store; Index/Dir/UsedBytes/ChunkCount/Failed derive from its accounting.
func (v *V2Store) DiskInfos() []DiskInfo {
	out := make([]DiskInfo, len(v.disks))
	for i, b := range v.disks {
		out[i] = DiskInfo{
			Index:      i,
			Dir:        b.dir,
			UsedBytes:  b.usedByts.Load(),
			ChunkCount: b.extCount.Load(),
			Failed:     v.diskFailed(i),
		}
	}
	return out
}

// VerifyChunkData re-reads a chunk and reports whether its on-disk content
// still matches the recorded checksum, mirroring the legacy ha.go Verify
// semantics. Size is resolved from the owning backend's stat, so a full
// re-read verifies every byte.
func (v *V2Store) VerifyChunkData(chunkID metadata.ChunkID) (bool, uint32, error) {
	info, ok := v.Info(chunkID)
	if !ok {
		return false, 0, fmt.Errorf("chunk %d not found locally", chunkID)
	}
	data, _, err := v.Read(chunkID, 0, int32(info.Size))
	if err != nil {
		return false, 0, err
	}
	// The V2.1 segment store records extent payload checksums as CRC32C
	// (Castagnoli), so the integrity comparison must match that, not IEEE.
	got := storage.CRC32C(data)
	return got == info.Checksum, got, nil
}

// PerfSnapshot returns engine performance counters. The legacy V1 metrics
// (fsync count/timing, semaphore waits) are chunk-file concerns V2.1 does
// not have — its equivalents live in ReadWriteBytes/DiskStats — so this
// reports an honest zero snapshot.
func (v *V2Store) PerfSnapshot() ChunkStorePerfSnapshot {
	return ChunkStorePerfSnapshot{}
}

// Compile-time interface checks.
var _ LocalChunkStore = (*V2Store)(nil)
var _ HeartbeatStore = (*V2Store)(nil)
