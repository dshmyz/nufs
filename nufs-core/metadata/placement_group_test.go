package metadata

import (
	"testing"
)

func TestPlacementGroup_CreateAndResolve(t *testing.T) {
	store := newV2TestPebbleStore(t)
	pgs := NewPlacementGroupStore(store)

	pg, err := pgs.CreatePG(1, []NodeID{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	if pg.Epoch != 1 {
		t.Fatalf("epoch = %d, want 1", pg.Epoch)
	}
	replicas, migrating, err := pgs.ResolveReplicas(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if migrating || len(replicas) != 3 {
		t.Fatalf("resolve epoch 1: migrating=%v replicas=%v", migrating, replicas)
	}
}

func TestPlacementGroup_RebalanceOldEpochResolvable(t *testing.T) {
	store := newV2TestPebbleStore(t)
	pgs := NewPlacementGroupStore(store)

	if _, err := pgs.CreatePG(7, []NodeID{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	mig, err := pgs.Rebalance(7, []NodeID{4, 5, 6}, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if mig.SourceEpoch != 1 || mig.TargetEpoch != 2 {
		t.Fatalf("migration epochs: %+v", mig)
	}
	// Current epoch resolves to the new set.
	replicas, migrating, err := pgs.ResolveReplicas(7, 2)
	if err != nil {
		t.Fatal(err)
	}
	if migrating || replicas[0] != 4 {
		t.Fatalf("epoch 2 resolves to %v, want [4 5 6]", replicas)
	}
	// Old epoch still resolvable during migration.
	replicas, migrating, err = pgs.ResolveReplicas(7, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !migrating || replicas[0] != 1 {
		t.Fatalf("epoch 1 should resolve to source %v, migrating=%v", replicas, migrating)
	}
	// After completion, the old epoch is no longer resolvable.
	if err := pgs.CompleteMigration(7); err != nil {
		t.Fatal(err)
	}
	if _, _, err := pgs.ResolveReplicas(7, 1); err == nil {
		t.Fatal("old epoch should not resolve after migration completes")
	}
}

func TestEncodeExtentIDV2(t *testing.T) {
	id := EncodeExtentIDV2(0x1234, 0x0000FFFF00000001)
	if id.OwnerPartition() != 0x1234 {
		t.Fatalf("owner partition = %x, want 1234", id.OwnerPartition())
	}
}
