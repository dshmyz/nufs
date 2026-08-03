package metadata

import (
	"context"
	"testing"
)

// newPGTestStore builds a PebbleStore with 5 online nodes registered on the
// shared placement engine, plus a bucket whose policy drives allocation.
func newPGTestStore(t *testing.T, bucketPolicy PlacementPolicy) (*PebbleStore, InodeID) {
	t.Helper()
	store := newTestPebbleStore(t)
	ctx := context.Background()
	for i := NodeID(1); i <= 5; i++ {
		store.RegisterNode(ctx, makeTestNode(i, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline))
	}
	if err := store.CreateBucket(ctx, "pg-bucket", bucketPolicy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "pg-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := store.CreateFile(ctx, bucket.RootInode, "f.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	return store, inode.ID
}

// TestAllocateChunkViaPG_RecordsPGOnChunkMeta proves that with placement
// groups enabled the chunk carries a real PGID/Epoch and its replicas are
// resolved from that PG's replica set.
func TestAllocateChunkViaPG_RecordsPGOnChunkMeta(t *testing.T) {
	store, inodeID := newPGTestStore(t, PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadRack,
		StorageTier:       TierHot,
	})
	ctx := context.Background()

	chunk, err := store.AllocateChunk(ctx, inodeID, 0, PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadRack,
		StorageTier:       TierHot,
		PlacementGroups:   true,
	})
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	if chunk.PGID == 0 {
		t.Fatal("expected a nonzero PGID on the chunk")
	}
	if chunk.Epoch != 1 {
		t.Fatalf("epoch=%d, want 1 (freshly created PG)", chunk.Epoch)
	}
	if len(chunk.Replicas) != 3 {
		t.Fatalf("replicas=%d, want 3", len(chunk.Replicas))
	}
	for _, r := range chunk.Replicas {
		if r.Addr == "" {
			t.Fatalf("replica %d has empty addr", r.NodeID)
		}
	}

	// The PG must exist and resolve to the same replica nodes the chunk
	// carries.
	pg, err := store.pgStore.Get(chunk.PGID)
	if err != nil || pg == nil {
		t.Fatalf("pgStore.Get(%d): err=%v pg=%v", chunk.PGID, err, pg)
	}
	if pg.Epoch != chunk.Epoch {
		t.Fatalf("pg.epoch=%d != chunk.epoch=%d", pg.Epoch, chunk.Epoch)
	}
	if len(pg.ReplicaNodes) != len(chunk.Replicas) {
		t.Fatalf("pg replica nodes=%d != chunk replicas=%d", len(pg.ReplicaNodes), len(chunk.Replicas))
	}
	for _, r := range chunk.Replicas {
		if !containsNodeID(pg.ReplicaNodes, r.NodeID) {
			t.Fatalf("chunk replica node %d not in pg set %v", r.NodeID, pg.ReplicaNodes)
		}
	}
}

// TestAllocateChunkViaPG_ConvergesAndReusesPG proves content-addressed PG
// selection is deterministic and convergent: the same replica set maps to
// the same PGID, and a second allocation reuses the existing PG (epoch stays
// at 1) rather than creating a new one.
func TestAllocateChunkViaPG_ConvergesAndReusesPG(t *testing.T) {
	store, inodeID := newPGTestStore(t, PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadRack,
		StorageTier:       TierHot,
	})
	ctx := context.Background()
	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadRack,
		StorageTier:       TierHot,
		PlacementGroups:   true,
	}

	c1, err := store.AllocateChunk(ctx, inodeID, 0, policy)
	if err != nil {
		t.Fatalf("alloc 1: %v", err)
	}
	c2, err := store.AllocateChunk(ctx, inodeID, 1, policy)
	if err != nil {
		t.Fatalf("alloc 2: %v", err)
	}

	// Deterministic placement: same node set both times.
	if c1.PGID != c2.PGID {
		t.Fatalf("pg1=%d != pg2=%d, want identical (content-addressed)", c1.PGID, c2.PGID)
	}
	// Reuse: the PG was not re-created, so both chunks sit at epoch 1.
	if c1.Epoch != 1 || c2.Epoch != 1 {
		t.Fatalf("epochs=%d,%d, want 1,1 (PG reused)", c1.Epoch, c2.Epoch)
	}

	// The PG store has exactly one PG.
	pg, err := store.pgStore.Get(c1.PGID)
	if err != nil || pg == nil {
		t.Fatalf("pgStore.Get: err=%v", err)
	}
	if len(pg.ReplicaNodes) != 3 {
		t.Fatalf("pg nodes=%d, want 3", len(pg.ReplicaNodes))
	}
}

// TestAllocateChunkLegacy_NoPGWhenDisabled proves the V1 default is untouched:
// with PlacementGroups false, chunks carry zero PGID/Epoch and replicas come
// straight from the PlacementEngine (the pre-existing behavior).
func TestAllocateChunkLegacy_NoPGWhenDisabled(t *testing.T) {
	store, inodeID := newPGTestStore(t, PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadRack,
		StorageTier:       TierHot,
	})
	ctx := context.Background()

	chunk, err := store.AllocateChunk(ctx, inodeID, 0, PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadRack,
		StorageTier:       TierHot,
	})
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	if chunk.PGID != 0 {
		t.Fatalf("pgid=%d, want 0 (placement groups disabled)", chunk.PGID)
	}
	if chunk.Epoch != 0 {
		t.Fatalf("epoch=%d, want 0 (placement groups disabled)", chunk.Epoch)
	}
	if len(chunk.Replicas) != 3 {
		t.Fatalf("replicas=%d, want 3 (legacy placement engine)", len(chunk.Replicas))
	}
}

// TestPlacementGroupIDForNodes_DeterministicAndOrderIndependent proves the
// PG ID derivation is a pure content hash: order-independent and stable.
func TestPlacementGroupIDForNodes_DeterministicAndOrderIndependent(t *testing.T) {
	a := placementGroupIDForNodes([]NodeID{3, 1, 2})
	b := placementGroupIDForNodes([]NodeID{1, 2, 3})
	c := placementGroupIDForNodes([]NodeID{2, 3, 1})
	if a != b || b != c {
		t.Fatalf("order-dependent PG id: %d %d %d", a, b, c)
	}
	d := placementGroupIDForNodes([]NodeID{1, 2, 4})
	if d == a {
		t.Fatalf("distinct node sets collided: %d", d)
	}
}

func containsNodeID(nodes []NodeID, want NodeID) bool {
	for _, id := range nodes {
		if id == want {
			return true
		}
	}
	return false
}
