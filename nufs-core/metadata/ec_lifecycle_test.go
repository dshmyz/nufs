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

func testDisks(nodes []uint64, disksPerNode int) []ECDisk {
	var out []ECDisk
	for _, n := range nodes {
		for d := 0; d < disksPerNode; d++ {
			out = append(out, ECDisk{DiskID: n*100 + uint64(d), NodeID: n})
		}
	}
	return out
}

// TestECPlanShards_ThreeWayDistributes6Plus3 verifies the real planner fills
// all nine shards across three machines (3 each) with §14 fault-domain
// diversity, and persists the plan (state → Encoding).
func TestECPlanShards_ThreeMachine(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)

	st, err := ec.BeginConversion("stripe-plan-1", 100, 1, 0xABCD)
	if err != nil {
		t.Fatal(err)
	}
	disks := testDisks([]uint64{1, 2, 3}, 3) // 3 machines × 3 disks = 9
	if err := ec.PlanShards(st, disks); err != nil {
		t.Fatalf("plan shards: %v", err)
	}
	if st.State != ECConversionEncoding {
		t.Fatalf("state = %s, want encoding", st.State)
	}
	if len(st.Shards) != 9 {
		t.Fatalf("planned %d shards, want 9", len(st.Shards))
	}

	// §14 diversity: exactly 3 distinct machines, exactly 3 shards each.
	perMachine := make(map[uint64]int)
	for _, s := range st.Shards {
		if s.DiskID == 0 || s.NodeID == 0 {
			t.Fatalf("unplanned location in shard: %+v", s)
		}
		perMachine[s.NodeID]++
	}
	if len(perMachine) != 3 {
		t.Fatalf("distinct machines = %d, want 3", len(perMachine))
	}
	for n, cnt := range perMachine {
		if cnt != 3 {
			t.Fatalf("machine %d holds %d shards, want 3", n, cnt)
		}
	}

	// Persisted: reload and confirm the plan survives.
	got, _ := ec.GetStripe("stripe-plan-1")
	if got.State != ECConversionEncoding || len(got.Shards) != 9 {
		t.Fatalf("persisted plan: %+v", got)
	}
}

// TestECPlanShards_RejectsInsufficientDiversity: fewer than 3 machines (or
// too few disks) must fail without mutating the stripe.
func TestECPlanShards_RejectsInsufficientDiversity(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)

	// Only 2 machines (even with many disks) → insufficient diversity.
	st, _ := ec.BeginConversion("stripe-plan-bad", 200, 1, 0x1)
	if err := ec.PlanShards(st, testDisks([]uint64{1, 2}, 6)); err == nil {
		t.Fatal("planning across 2 machines should fail")
	}
	// Robust: failed plan must not advance the persisted state.
	got, _ := ec.GetStripe("stripe-plan-bad")
	if got.State != ECConversionPreparing || len(got.Shards) != 0 {
		t.Fatalf("failed plan mutated the stripe: %+v", got)
	}

	// Too few disks overall (3 machines but 2 disks each = 6 < 9).
	st2, _ := ec.BeginConversion("stripe-plan-fewdisks", 300, 1, 0x1)
	if err := ec.PlanShards(st2, testDisks([]uint64{1, 2, 3}, 2)); err == nil {
		t.Fatal("planning with only 6 disks for 9 shards should fail")
	}
}

// TestECPlanShards_MoreMachinesSpreadsEvenly: given more machines, the §14
// cap (≤3 per machine) is respected and all 9 shards are still placed.
func TestECPlanShards_MoreMachinesSpreadsEvenly(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)

	// 5 machines × 2 disks = 10 candidates → all 9 placed, none > 3.
	st, _ := ec.BeginConversion("stripe-plan-5m", 400, 1, 0x1)
	if err := ec.PlanShards(st, testDisks([]uint64{1, 2, 3, 4, 5}, 2)); err != nil {
		t.Fatalf("plan shards: %v", err)
	}
	if len(st.Shards) != 9 {
		t.Fatalf("placed %d shards, want 9", len(st.Shards))
	}
	perMachine := make(map[uint64]int)
	for _, s := range st.Shards {
		perMachine[s.NodeID]++
	}
	if len(perMachine) < 3 {
		t.Fatalf("distinct machines = %d, want >=3", len(perMachine))
	}
	for n, cnt := range perMachine {
		if cnt > 3 {
			t.Fatalf("machine %d holds %d shards, exceeds cap 3", n, cnt)
		}
	}
}

// TestECMarkExtentColdEC_WiresDormantFields verifies the dormant
// ExtentMetaV2 fields (Lifecycle/StorageClass/ECStripeID) are durably written
// through the inode store when an inline extent converts to 6+3.
func TestECMarkExtentColdEC_WiresDormantFields(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)
	ins := NewInodeStoreV2(store)

	if _, err := ins.CreateEmpty(100, FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}
	extID := ExtentIDV2(0x10000000001)
	if err := ins.SetInlineExtent(100, &ExtentMetaV2{ID: extID, Generation: 1, LogicalLen: 4096}, 4096); err != nil {
		t.Fatal(err)
	}

	if err := ec.MarkExtentColdEC(100, extID, "stripe-ec-9"); err != nil {
		t.Fatalf("mark cold EC: %v", err)
	}

	got, err := ins.Get(100)
	if err != nil {
		t.Fatal(err)
	}
	e := got.InlineExtent
	if e.StorageClass != StorageClassColdEC {
		t.Fatalf("storage class = %d, want ColdEC", e.StorageClass)
	}
	if e.Lifecycle != LifecycleECConverting {
		t.Fatalf("lifecycle = %d, want ECConverting", e.Lifecycle)
	}
	if e.ECStripeID != "stripe-ec-9" {
		t.Fatalf("ec stripe id = %q, want stripe-ec-9", e.ECStripeID)
	}

	// Mismatched extent / paged inode are rejected without mutation.
	if err := ec.MarkExtentColdEC(100, ExtentIDV2(0x999), "x"); err == nil {
		t.Fatal("marking a mismatched extent should fail")
	}
	if err := ec.MarkExtentColdEC(9999, extID, "x"); err != ErrInodeNotFound {
		t.Fatalf("missing inode error = %v, want ErrInodeNotFound", err)
	}
}
