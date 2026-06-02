package s3

import (
	"context"
	"errors"
	"testing"

	"github.com/example/dfs/metadata"
)

func TestMemoryChunkStore_WriteAndRead(t *testing.T) {
	store := NewMemoryChunkStore()
	chunk := &metadata.ChunkMeta{ID: 42, Replicas: []metadata.ReplicaInfo{{NodeID: 1}}}

	payload := []byte("hello world")
	if err := store.WriteChunk(context.Background(), chunk, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := store.ReadChunk(context.Background(), chunk)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload mismatch: got %q want %q", got, payload)
	}
}

func TestMemoryChunkStore_StoreCopiesData(t *testing.T) {
	store := NewMemoryChunkStore()
	chunk := &metadata.ChunkMeta{ID: 7}

	data := []byte("original")
	if err := store.WriteChunk(context.Background(), chunk, data); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Mutate the caller's slice and make sure the stored copy is intact.
	for i := range data {
		data[i] = 'X'
	}

	got, err := store.ReadChunk(context.Background(), chunk)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "original" {
		t.Fatalf("expected stored copy to be unaffected by caller mutation, got %q", got)
	}
}

func TestMemoryChunkStore_WriteHookError(t *testing.T) {
	hookErr := errors.New("boom")
	store := NewMemoryChunkStore()
	store.WriteHook = func(_ metadata.ChunkID, _ []byte) error { return hookErr }

	chunk := &metadata.ChunkMeta{ID: 1}
	err := store.WriteChunk(context.Background(), chunk, []byte("data"))
	if !errors.Is(err, hookErr) {
		t.Fatalf("expected hook error to propagate, got %v", err)
	}
	if _, ok := store.Get(chunk.ID); ok {
		t.Fatalf("data should not be stored when hook returns an error")
	}
}

func TestMemoryChunkStore_ReadMissing(t *testing.T) {
	store := NewMemoryChunkStore()
	_, err := store.ReadChunk(context.Background(), &metadata.ChunkMeta{ID: 99})
	if err == nil {
		t.Fatalf("expected error for missing chunk, got nil")
	}
}

func TestMemoryChunkStore_RequiresReplicas(t *testing.T) {
	// DatanodeChunkStore should refuse chunks with no replicas, since
	// there is no node to write to.
	store := NewDatanodeChunkStore()
	err := store.WriteChunk(context.Background(), &metadata.ChunkMeta{ID: 1}, []byte("x"))
	if err == nil {
		t.Fatalf("expected error for chunk with no replicas, got nil")
	}
	if _, err := store.ReadChunk(context.Background(), &metadata.ChunkMeta{ID: 1}); err == nil {
		t.Fatalf("expected read error for chunk with no replicas, got nil")
	}
}
