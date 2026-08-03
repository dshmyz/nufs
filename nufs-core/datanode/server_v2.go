package datanode

import (
	"context"
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

	// Accounting updated on the serving path and reconstructed at startup
	// for V2.1 backends via ListExtents.
	usedByts atomic.Int64
	extCount atomic.Int64
	writeOps atomic.Int64
	writeErr atomic.Int64
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

// NewV2Store wraps a single backend (legacy/plain single-disk use).
func NewV2Store(store storage.Store) *V2Store {
	v := &V2Store{locOf: make(map[metadata.ChunkID]chunkLoc)}
	for _, s := range []storage.Store{store} {
		b := &diskBackend{store: s}
		if lister, ok := s.(extentLister); ok {
			b.lister = lister
		}
		v.disks = append(v.disks, b)
	}
	return v
}

// NewMultiV2Store wraps multiple backends for JBOD multi-disk mode. For
// V2.1 segment backends it reconstructs the per-disk location/generation
// map and usage accounting by enumerating each store's committed extents,
// the equivalent of the legacy ChunkStore's startup disk scan.
func NewMultiV2Store(stores []storage.Store) *V2Store {
	v := &V2Store{locOf: make(map[metadata.ChunkID]chunkLoc)}
	for i, s := range stores {
		b := &diskBackend{store: s, index: i}
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
	newChunk := gen == 1
	b := v.disks[disk]
	b.writeOps.Add(1)
	if _, err := b.store.Write(context.Background(), &storage.WriteRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: gen,
		Data:       data,
	}); err != nil {
		b.writeErr.Add(1)
		return err
	}
	if newChunk {
		b.usedByts.Add(int64(len(data)))
		b.extCount.Add(1)
	} else {
		// Overwrite: the prior generation still occupies index space; drop
		// its size so used-bytes tracks the chunk's current live size rather
		// than the sum of all generations.
		if old, err := b.store.Stat(context.Background(), &storage.StatRequest{
			ExtentID: storage.ExtentID(chunkID), Generation: gen - 1,
		}); err == nil {
			b.usedByts.Add(-int64(old.LogicalLen))
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
	res, err := v.disks[loc.disk].store.Read(context.Background(), &storage.ReadRequest{
		ExtentID:      storage.ExtentID(chunkID),
		Generation:    loc.gen,
		LogicalOffset: offset,
		Length:        length,
	})
	if err != nil {
		return nil, 0, err
	}
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
// signal: all recent writes failed). Kept minimal — no disk lifecycle
// management in this scope.
func (v *V2Store) diskFailed(i int) bool {
	b := v.disks[i]
	ops := b.writeOps.Load()
	return ops >= 5 && b.writeErr.Load() == ops
}

// WriteErrorRate returns the aggregate write error rate (0.0-1.0) across
// all disks, computed from write attempts and failures.
func (v *V2Store) WriteErrorRate() float64 {
	var ops, errs int64
	for _, b := range v.disks {
		ops += b.writeOps.Load()
		errs += b.writeErr.Load()
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

// Compile-time interface checks.
var _ LocalChunkStore = (*V2Store)(nil)
var _ HeartbeatStore = (*V2Store)(nil)
