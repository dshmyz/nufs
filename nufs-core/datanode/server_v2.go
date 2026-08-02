package datanode

import (
	"context"
	"time"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/metadata"
)

// V2Store wraps a storage.Store to implement the LocalChunkStore
// interface, so the V2.1 engine can serve through the existing TCP
// protocol. Each chunk ID maps to an extent with generation=1.
//
// This is a minimal adapter for the MVP. Production use should wire
// the full V2.1 metadata layer (extent pages, placements, etc.).
type V2Store struct {
	store storage.Store
}

// NewV2Store creates a V2 adapter around a storage.Store.
func NewV2Store(store storage.Store) *V2Store {
	return &V2Store{store: store}
}

// Write implements LocalChunkStore.Write. The chunk ID is used as the
// extent ID with generation=1.
func (v *V2Store) Write(chunkID metadata.ChunkID, data []byte) error {
	req := &storage.WriteRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: 1,
		Data:       data,
	}
	_, err := v.store.Write(context.Background(), req)
	return err
}

// Read implements LocalChunkStore.Read.
func (v *V2Store) Read(chunkID metadata.ChunkID, offset int64, length int32) ([]byte, uint32, error) {
	req := &storage.ReadRequest{
		ExtentID:      storage.ExtentID(chunkID),
		Generation:    1,
		LogicalOffset: offset,
		Length:        length,
	}
	res, err := v.store.Read(context.Background(), req)
	if err != nil {
		return nil, 0, err
	}
	return res.Data, res.Checksum, nil
}

// Delete implements LocalChunkStore.Delete.
func (v *V2Store) Delete(chunkID metadata.ChunkID) error {
	return v.store.Delete(context.Background(), &storage.DeleteRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: 1,
	})
}

// Seal is a no-op for V2 — extents are sealed atomically through the
// commit log. Returns 0, nil.
func (v *V2Store) Seal(chunkID metadata.ChunkID) (uint32, error) {
	// V2 extents are committed atomically; no separate seal needed.
	return 0, nil
}

// Info returns basic info. The V2 adapter does not track per-extent
// metadata in the old LocalChunkInfo format; for the MVP it returns a
// minimal info.
func (v *V2Store) Info(chunkID metadata.ChunkID) (*LocalChunkInfo, bool) {
	req := &storage.StatRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: 1,
	}
	res, err := v.store.Stat(context.Background(), req)
	if err != nil {
		return nil, false
	}
	return &LocalChunkInfo{
		ChunkID:     chunkID,
		Size:        int64(res.LogicalLen),
		State:       LocalSealed,
		WrittenAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 0,
		DiskIndex:   0,
	}, true
}

// ListChunks returns an empty list. The V2 engine does not expose a
// full chunk listing (it would be unbounded). Production uses the
// inventory/system for enumeration.
func (v *V2Store) ListChunks() []LocalChunkInfo {
	return nil
}

// Stats returns aggregate storage statistics.
func (v *V2Store) Stats() (totalBytes int64, chunkCount int64) {
	// The V2 engine tracks stats per stream; for the MVP we return
	// zeroed stats. Production should wire the real metrics.
	return 0, 0
}

// DiskManager returns nil — the V2 engine manages its own disk layout.
func (v *V2Store) DiskManager() *DiskManager {
	return nil
}

// ChunkStateSnapshot returns an empty snapshot. The V2 engine does not
// expose per-chunk replica state in the legacy format.
func (v *V2Store) ChunkStateSnapshot() map[metadata.ChunkID]metadata.ReplicaState {
	return map[metadata.ChunkID]metadata.ReplicaState{}
}

// StateVersion returns 0 — V2 chunks are durable atomically and there
// is no versioned chunk-state set to diff.
func (v *V2Store) StateVersion() uint64 {
	return 0
}

// DiskStats returns an empty per-disk breakdown.
func (v *V2Store) DiskStats() []DiskStatsItem {
	return nil
}

// WriteErrorRate returns 0 — the V2 engine reports per-stream error
// rates through its own metrics.
func (v *V2Store) WriteErrorRate() float64 {
	return 0
}

// Compile-time interface check.
var _ LocalChunkStore = (*V2Store)(nil)