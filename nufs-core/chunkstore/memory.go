package chunkstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/example/dfs/metadata"
)

// MemoryChunkStore is an in-process ChunkStore used by tests.
// It keeps chunk payloads in a map and does not require any datanode.
type MemoryChunkStore struct {
	mu     sync.RWMutex
	chunks map[metadata.ChunkID][]byte
	// WriteHook, if set, is called on every WriteChunk.
	WriteHook func(chunkID metadata.ChunkID, data []byte) error
	// ReadHook, if set, is called on every ReadChunk.
	ReadHook func(chunkID metadata.ChunkID) error
	// MinReplicasPerWrite mirrors DatanodeChunkStore.MinReplicasPerWrite.
	MinReplicasPerWrite int
}

// Compile-time interface check.
var _ ChunkStore = (*MemoryChunkStore)(nil)

// NewMemoryChunkStore returns an empty in-memory ChunkStore.
func NewMemoryChunkStore() *MemoryChunkStore {
	return &MemoryChunkStore{chunks: make(map[metadata.ChunkID][]byte)}
}

// WriteChunk stores data for the chunk.
func (m *MemoryChunkStore) WriteChunk(_ context.Context, chunk *metadata.ChunkMeta, data []byte) error {
	if m.MinReplicasPerWrite > 0 && m.WriteHook == nil {
		return fmt.Errorf("chunkstore: MinReplicasPerWrite=1 requires a non-nil WriteHook")
	}
	if m.WriteHook != nil {
		if err := m.WriteHook(chunk.ID, data); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	buf := make([]byte, len(data))
	copy(buf, data)
	m.chunks[chunk.ID] = buf
	return nil
}

// ReadChunk returns the previously written payload for chunk.ID.
func (m *MemoryChunkStore) ReadChunk(ctx context.Context, chunk *metadata.ChunkMeta) ([]byte, error) {
	return m.ReadChunkRange(ctx, chunk, 0, 0)
}

// ReadChunkRange reads a subrange from the in-memory chunk data.
func (m *MemoryChunkStore) ReadChunkRange(_ context.Context, chunk *metadata.ChunkMeta, offset int64, length int32) ([]byte, error) {
	if m.ReadHook != nil {
		if err := m.ReadHook(chunk.ID); err != nil {
			return nil, err
		}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.chunks[chunk.ID]
	if !ok {
		return nil, fmt.Errorf("chunkstore: chunk %d not found", chunk.ID)
	}
	if length <= 0 || offset < 0 || offset+int64(length) > int64(len(data)) {
		out := make([]byte, len(data))
		copy(out, data)
		return out, nil
	}
	out := make([]byte, length)
	copy(out, data[offset:offset+int64(length)])
	return out, nil
}

// Get returns the stored payload for a chunk (used by tests).
func (m *MemoryChunkStore) Get(chunkID metadata.ChunkID) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.chunks[chunkID]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, true
}
