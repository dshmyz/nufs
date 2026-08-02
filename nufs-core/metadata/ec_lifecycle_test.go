package metadata

import (
	"testing"
	"time"
)

func TestECLifecycle_Conversion(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)

	st, err := ec.BeginConversion("stripe-1", 100, 1, 0xDEADBEEF)
	if err != nil {
		t.Fatal(err)
	}
	if st.State != ECConversionPreparing || st.OriginalChecksum != 0xDEADBEEF {
		t.Fatalf("begin: %+v", st)
	}
	if err := ec.MarkSyncing(st); err != nil {
		t.Fatal(err)
	}
	if err := ec.CompleteConversion(st, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, _ := ec.GetStripe("stripe-1")
	if got.State != ECConversionComplete || got.ConvertedAt == 0 {
		t.Fatalf("complete: %+v", got)
	}
}

func TestECLifecycle_RollbackPreservesReplicas(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)

	st, err := ec.BeginConversion("stripe-2", 200, 1, 0x1234)
	if err != nil {
		t.Fatal(err)
	}
	// Conversion fails (e.g. shard write error): roll back. Metadata
	// still points at the three replicas; partial EC shards are orphans.
	if err := ec.RollbackConversion(st, "shard write failed"); err != nil {
		t.Fatal(err)
	}
	got, _ := ec.GetStripe("stripe-2")
	if got.State != ECConversionRolledBack {
		t.Fatalf("rollback state = %s", got.State)
	}
}

func TestECPlacement_Diversity(t *testing.T) {
	v := &ECPlacementValidator{MinMachines: 3, MaxPerMachine: 3}
	// 9 shards across 3 machines, 3 each → valid.
	shards := []ECShard{}
	for i := 0; i < 9; i++ {
		shards = append(shards, ECShard{Index: i, NodeID: uint64(i / 3), DiskID: uint64(i)})
	}
	n, err := v.Validate(shards)
	if err != nil {
		t.Fatalf("valid placement rejected: %v", err)
	}
	if n != 3 {
		t.Fatalf("distinct machines = %d, want 3", n)
	}
	// A machine holding 4 shards violates the ≤3 bound.
	bad := []ECShard{}
	for i := 0; i < 9; i++ {
		node := uint64(1)
		if i >= 4 {
			node = uint64(i)
		}
		bad = append(bad, ECShard{Index: i, NodeID: node, DiskID: uint64(i)})
	}
	if _, err := v.Validate(bad); err == nil {
		t.Fatal("placement with 4 shards on one machine should be rejected")
	}
	// Fewer than 3 distinct machines is rejected.
	two := []ECShard{}
	for i := 0; i < 9; i++ {
		two = append(two, ECShard{Index: i, NodeID: uint64(i / 5), DiskID: uint64(i)})
	}
	if _, err := v.Validate(two); err == nil {
		t.Fatal("placement across 2 machines should be rejected")
	}
}

func TestECConversionCheck(t *testing.T) {
	c := ECConversionCheck{Immutable: true, Idle: true, NoConflict: true, HealthyReplicas: true, Scrubbed: true, Diverse: true}
	if !c.All() {
		t.Fatal("all preconditions met should pass")
	}
	c.Idle = false
	if c.All() {
		t.Fatal("missing idle should fail")
	}
}
