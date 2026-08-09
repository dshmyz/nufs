package chunkstore

import (
	"context"
	"errors"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
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

func TestSortReplicasForRead_HealthOrder(t *testing.T) {
	replicas := []metadata.ReplicaInfo{
		{NodeID: 1, State: metadata.ReplicaFailed},
		{NodeID: 2, State: metadata.ReplicaReady},
		{NodeID: 3, State: metadata.ReplicaStale},
		{NodeID: 4, State: metadata.ReplicaSyncing},
		{NodeID: 5, State: metadata.ReplicaReady},
	}

	sorted := sortReplicasForRead(replicas)

	// Expected order: Ready(2), Ready(5), Syncing(4), Stale(3), Failed(1)
	expected := []metadata.NodeID{2, 5, 4, 3, 1}
	if len(sorted) != len(expected) {
		t.Fatalf("len = %d, want %d", len(sorted), len(expected))
	}
	for i, want := range expected {
		if sorted[i].NodeID != want {
			t.Errorf("sorted[%d].NodeID = %d, want %d (state=%d)", i, sorted[i].NodeID, want, sorted[i].State)
		}
	}
}

func TestSortReplicasForRead_DoesNotMutateOriginal(t *testing.T) {
	replicas := []metadata.ReplicaInfo{
		{NodeID: 1, State: metadata.ReplicaFailed},
		{NodeID: 2, State: metadata.ReplicaReady},
	}
	_ = sortReplicasForRead(replicas)

	if replicas[0].NodeID != 1 {
		t.Errorf("original slice was mutated: first element NodeID = %d, want 1", replicas[0].NodeID)
	}
}

func TestSortReplicasForRead_AllSameState(t *testing.T) {
	replicas := []metadata.ReplicaInfo{
		{NodeID: 3, State: metadata.ReplicaReady},
		{NodeID: 1, State: metadata.ReplicaReady},
		{NodeID: 2, State: metadata.ReplicaReady},
	}
	sorted := sortReplicasForRead(replicas)

	// All same priority - stable sort preserves original order.
	for i, want := range []metadata.NodeID{3, 1, 2} {
		if sorted[i].NodeID != want {
			t.Errorf("sorted[%d].NodeID = %d, want %d", i, sorted[i].NodeID, want)
		}
	}
}

func TestReplicaReadPriority(t *testing.T) {
	tests := []struct {
		state    metadata.ReplicaState
		expected int
	}{
		{metadata.ReplicaReady, 0},
		{metadata.ReplicaSyncing, 1},
		{metadata.ReplicaStale, 2},
		{metadata.ReplicaFailed, 3},
	}
	for _, tt := range tests {
		got := replicaReadPriority(tt.state)
		if got != tt.expected {
			t.Errorf("replicaReadPriority(%v) = %d, want %d", tt.state, got, tt.expected)
		}
	}
}
