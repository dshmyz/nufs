package metadata

import (
	"context"
	"testing"
)

// TestPebbleStore_BatchUpdateChunkStates_Performance verifies that
// batchUpdateChunkStates correctly updates multiple chunks in a
// single batch without per-chunk lookups. We test correctness with
// a large number of chunks.
func TestPebbleStore_BatchUpdateChunkStates_LargeBatch(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()

	ctx := context.Background()

	// Register nodes
	for i := NodeID(1); i <= 5; i++ {
		node := makeTestNode(i, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline)
		store.RegisterNode(ctx, node)
	}

	// Create a bucket and file
	policy := PlacementPolicy{ReplicationFactor: 3, StorageTier: TierHot}
	if err := store.CreateBucket(ctx, "test-bucket", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := store.CreateFile(ctx, bucket.RootInode, "file.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	// Allocate 50 chunks
	chunkIDs := make([]ChunkID, 50)
	for i := range chunkIDs {
		chunk, err := store.AllocateChunk(ctx, inode.ID, int64(i*1024), policy)
		if err != nil {
			t.Fatalf("AllocateChunk %d: %v", i, err)
		}
		chunkIDs[i] = chunk.ID
	}

	// Batch update all chunks to ReplicaReady
	states := make(map[ChunkID]ReplicaState, len(chunkIDs))
	for _, cid := range chunkIDs {
		states[cid] = ReplicaReady
	}

	if err := store.batchUpdateChunkStates(NodeID(1), states); err != nil {
		t.Fatalf("batchUpdateChunkStates: %v", err)
	}

	// Verify all chunks have node 1 in ReplicaReady state
	for _, cid := range chunkIDs {
		chunk, err := store.GetChunk(ctx, cid)
		if err != nil {
			t.Fatalf("GetChunk %d: %v", cid, err)
		}
		found := false
		for _, r := range chunk.Replicas {
			if r.NodeID == NodeID(1) && r.State == ReplicaReady {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("chunk %d: node 1 not in ReplicaReady state", cid)
		}
	}
}

// TestPebbleStore_BatchUpdateChunkStates_PartialFailure verifies
// that if some chunks don't exist, they are silently skipped and
// the rest are still updated.
func TestPebbleStore_BatchUpdateChunkStates_PartialFailure(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()

	ctx := context.Background()

	for i := NodeID(1); i <= 5; i++ {
		node := makeTestNode(i, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline)
		store.RegisterNode(ctx, node)
	}

	policy := PlacementPolicy{ReplicationFactor: 3, StorageTier: TierHot}
	if err := store.CreateBucket(ctx, "test-bucket", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := store.CreateFile(ctx, bucket.RootInode, "file.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	// Allocate 2 real chunks
	chunk1, err := store.AllocateChunk(ctx, inode.ID, 0, policy)
	if err != nil {
		t.Fatalf("AllocateChunk 1: %v", err)
	}
	chunk2, err := store.AllocateChunk(ctx, inode.ID, 1024, policy)
	if err != nil {
		t.Fatalf("AllocateChunk 2: %v", err)
	}

	// Mix real and non-existent chunk IDs
	states := map[ChunkID]ReplicaState{
		chunk1.ID:        ReplicaReady,
		ChunkID(999999):  ReplicaReady, // doesn't exist
		chunk2.ID:        ReplicaReady,
		ChunkID(888888):  ReplicaFailed, // doesn't exist
	}

	if err := store.batchUpdateChunkStates(NodeID(1), states); err != nil {
		t.Fatalf("batchUpdateChunkStates: %v", err)
	}

	// Verify real chunks were updated
	for _, cid := range []ChunkID{chunk1.ID, chunk2.ID} {
		chunk, err := store.GetChunk(ctx, cid)
		if err != nil {
			t.Fatalf("GetChunk %d: %v", cid, err)
		}
		found := false
		for _, r := range chunk.Replicas {
			if r.NodeID == NodeID(1) && r.State == ReplicaReady {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("chunk %d: not updated", cid)
		}
	}
}
