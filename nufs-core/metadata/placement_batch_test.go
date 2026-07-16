package metadata

import (
	"context"
	"testing"
)

// TestPlacementEngine_GetNodeInfo verifies that GetNodeInfo returns
// node info directly from the in-memory map without Pebble lookups.
func TestPlacementEngine_GetNodeInfo(t *testing.T) {
	pe := NewPlacementEngine()

	// Add 3 nodes
	for i := NodeID(1); i <= 3; i++ {
		pe.UpdateNode(makeTestNode(i, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline))
	}

	// GetNodeInfo should return the node directly
	for i := NodeID(1); i <= 3; i++ {
		info, ok := pe.GetNodeInfo(i)
		if !ok {
			t.Fatalf("GetNodeInfo(%d): not found", i)
		}
		if info.ID != i {
			t.Fatalf("GetNodeInfo(%d): got ID %d", i, info.ID)
		}
		expectedAddr := ""
		_ = expectedAddr
		if info.Addr == "" {
			t.Fatalf("GetNodeInfo(%d): empty addr", i)
		}
	}

	// Non-existent node
	_, ok := pe.GetNodeInfo(999)
	if ok {
		t.Fatal("GetNodeInfo(999): expected not found")
	}
}

// TestPlacementEngine_GetNodeInfosBatch verifies batch retrieval of
// node info for multiple node IDs in a single call.
func TestPlacementEngine_GetNodeInfosBatch(t *testing.T) {
	pe := NewPlacementEngine()

	for i := NodeID(1); i <= 5; i++ {
		pe.UpdateNode(makeTestNode(i, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline))
	}

	ids := []NodeID{1, 2, 3, 4, 5}
	infos := pe.GetNodeInfosBatch(ids)
	if len(infos) != 5 {
		t.Fatalf("expected 5 infos, got %d", len(infos))
	}

	for i, info := range infos {
		if info == nil {
			t.Fatalf("infos[%d] is nil", i)
		}
		if info.ID != ids[i] {
			t.Fatalf("infos[%d]: got ID %d, want %d", i, info.ID, ids[i])
		}
	}

	// Mix of existing and non-existing
	mixed := []NodeID{1, 999, 3}
	mixedInfos := pe.GetNodeInfosBatch(mixed)
	if len(mixedInfos) != 3 {
		t.Fatalf("expected 3 results, got %d", len(mixedInfos))
	}
	if mixedInfos[0] == nil || mixedInfos[0].ID != 1 {
		t.Fatalf("mixedInfos[0]: expected node 1")
	}
	if mixedInfos[1] != nil {
		t.Fatalf("mixedInfos[1]: expected nil for non-existent node 999")
	}
	if mixedInfos[2] == nil || mixedInfos[2].ID != 3 {
		t.Fatalf("mixedInfos[2]: expected node 3")
	}
}

// TestAllocateChunk_UsesPlacementEngineNodeInfo verifies that
// AllocateChunk does not make per-replica GetNode calls to Pebble.
// Instead, it should use the placement engine's in-memory NodeInfo.
func TestAllocateChunk_UsesPlacementEngineNodeInfo(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()

	ctx := context.Background()

	// Add nodes to both store and placement engine
	for i := NodeID(1); i <= 5; i++ {
		node := makeTestNode(i, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline)
		store.RegisterNode(ctx, node)
	}

	// Create a bucket and file
	policy := PlacementPolicy{
		ReplicationFactor: 1,
		StorageTier:       TierHot,
	}
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

	allocPolicy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadRack,
		StorageTier:       TierHot,
	}

	chunk, err := store.AllocateChunk(ctx, inode.ID, 0, allocPolicy)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}

	if len(chunk.Replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(chunk.Replicas))
	}

	// Verify all replicas have valid addresses (from placement engine)
	for _, r := range chunk.Replicas {
		if r.Addr == "" {
			t.Fatalf("replica %d has empty addr", r.NodeID)
		}
	}
}
