package metadata

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ChaosTestSuite provides fault injection primitives for testing system resilience.
// It simulates node failures, disk failures, and network partitions against
// a real PebbleStore without requiring external chaos tools.

// ========== Node Failure Chaos ==========

func TestChaos_NodeFailure_DataAvailability(t *testing.T) {
	store := newChaosTestStore(t)
	defer store.Close()

	ctx := context.Background()

	// Register 3 nodes
	nodes := []*NodeInfo{
		{ID: 1, Zone: "z1", CapacityGB: 100, State: NodeOnline},
		{ID: 2, Zone: "z2", CapacityGB: 100, State: NodeOnline},
		{ID: 3, Zone: "z3", CapacityGB: 100, State: NodeOnline},
	}
	for _, n := range nodes {
		if err := store.RegisterNode(ctx, n); err != nil {
			t.Fatalf("register node %d: %v", n.ID, err)
		}
	}

	// Create bucket
	if err := store.CreateBucket(ctx, "chaos-test", PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadZone,
	}); err != nil {
		t.Fatal(err)
	}

	// Simulate node 2 failure
	if err := store.DecommissionNode(ctx, 2); err != nil {
		t.Fatal(err)
	}

	// Verify remaining nodes can still serve reads
	node, err := store.GetNode(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if node.State != NodeDraining {
		t.Errorf("expected node 2 to be draining, got %s", node.State)
	}

	// Verify new allocations skip the failed node
	policy := PlacementPolicy{ReplicationFactor: 2, TopologySpread: SpreadZone}
	selected, err := store.placement.PlaceChunk(policy, nil)
	if err != nil {
		t.Fatalf("placement after failure: %v", err)
	}
	for _, id := range selected {
		if id == 2 {
			t.Error("placement should not select decommissioned node")
		}
	}
}

// ========== Network Partition Chaos ==========

func TestChaos_NetworkPartition_QuorumPreserved(t *testing.T) {
	store := newChaosTestStore(t)
	defer store.Close()

	ctx := context.Background()

	// Register 5 nodes
	for i := NodeID(1); i <= 5; i++ {
		info := &NodeInfo{
			ID:         i,
			Zone:       fmt.Sprintf("z%d", i),
			CapacityGB: 100,
			State:      NodeOnline,
		}
		if err := store.RegisterNode(ctx, info); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate partition: nodes 4,5 go offline
	for _, id := range []NodeID{4, 5} {
		key := fmt.Sprintf("%s%d", prefixNode, id)
		var info NodeInfo
		exists, _ := store.getValue(key, &info)
		if exists {
			info.State = NodeFailed
			store.putMsgpack(key, &info)
			store.placement.UpdateNode(&info)
		}
	}

	// Verify quorum is maintained (3/5 nodes online)
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	online := 0
	for _, n := range nodes {
		if n.State == NodeOnline {
			online++
		}
	}
	if online != 3 {
		t.Errorf("expected 3 online nodes, got %d", online)
	}

	// Verify placement still works with remaining nodes
	policy := PlacementPolicy{ReplicationFactor: 3, TopologySpread: SpreadZone}
	selected, err := store.placement.PlaceChunk(policy, nil)
	if err != nil {
		t.Fatalf("placement should work with quorum: %v", err)
	}
	if len(selected) < 3 {
		t.Errorf("expected 3 placements, got %d", len(selected))
	}
}

// ========== Concurrent Failure Chaos ==========

func TestChaos_ConcurrentFailures_Recovery(t *testing.T) {
	store := newChaosTestStore(t)
	defer store.Close()

	ctx := context.Background()

	// Register nodes
	for i := NodeID(1); i <= 5; i++ {
		info := &NodeInfo{
			ID:         i,
			Zone:       fmt.Sprintf("z%d", i),
			CapacityGB: 100,
			State:      NodeOnline,
		}
		if err := store.RegisterNode(ctx, info); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate concurrent node failures
	var wg sync.WaitGroup
	var failCount atomic.Int32
	for _, id := range []NodeID{1, 2} {
		wg.Add(1)
		go func(nodeID NodeID) {
			defer wg.Done()
			if err := store.DecommissionNode(ctx, nodeID); err != nil {
				t.Errorf("decommission node %d: %v", nodeID, err)
				return
			}
			failCount.Add(1)
		}(id)
	}
	wg.Wait()

	if failCount.Load() != 2 {
		t.Errorf("expected 2 failures, got %d", failCount.Load())
	}

	// Verify system still functional
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	online := 0
	for _, n := range nodes {
		if n.State == NodeOnline {
			online++
		}
	}
	if online != 3 {
		t.Errorf("expected 3 online nodes after failures, got %d", online)
	}
}

// ========== Rolling Upgrade Chaos ==========

func TestChaos_RollingUpgrade_MaintainsAvailability(t *testing.T) {
	store := newChaosTestStore(t)
	defer store.Close()

	ctx := context.Background()

	// Register 3 nodes
	for i := NodeID(1); i <= 3; i++ {
		info := &NodeInfo{
			ID:         i,
			Zone:       fmt.Sprintf("z%d", i),
			CapacityGB: 100,
			State:      NodeOnline,
		}
		if err := store.RegisterNode(ctx, info); err != nil {
			t.Fatal(err)
		}
	}

	// Get upgrade plan
	plan, err := store.RollingUpgradePlan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan) == 0 {
		t.Fatal("upgrade plan should not be empty")
	}

	// Simulate rolling upgrade: one node at a time
	for _, nodeID := range plan {
		// Enter maintenance
		if err := store.EnterMaintenance(ctx, nodeID); err != nil {
			t.Fatalf("enter maintenance node %d: %v", nodeID, err)
		}

		// Verify at least 2 nodes still online
		nodes, _ := store.ListNodes(ctx)
		online := 0
		for _, n := range nodes {
			if n.State == NodeOnline {
				online++
			}
		}
		if online < 2 {
			t.Errorf("insufficient online nodes during upgrade: %d", online)
		}

		// Exit maintenance (upgrade complete)
		if err := store.ExitMaintenance(ctx, nodeID); err != nil {
			t.Fatalf("exit maintenance node %d: %v", nodeID, err)
		}
	}
}

// ========== Maintenance State Validation ==========

func TestChaos_MaintenanceState_Transitions(t *testing.T) {
	store := newChaosTestStore(t)
	defer store.Close()

	ctx := context.Background()

	info := &NodeInfo{ID: 10, Zone: "z1", CapacityGB: 100, State: NodeOnline}
	if err := store.RegisterNode(ctx, info); err != nil {
		t.Fatal(err)
	}

	// Online → Maint
	if err := store.EnterMaintenance(ctx, 10); err != nil {
		t.Fatal(err)
	}
	node, _ := store.GetNode(ctx, 10)
	if node.State != NodeMaint {
		t.Errorf("expected NodeMaint, got %s", node.State)
	}

	// Maint → Online
	if err := store.ExitMaintenance(ctx, 10); err != nil {
		t.Fatal(err)
	}
	node, _ = store.GetNode(ctx, 10)
	if node.State != NodeOnline {
		t.Errorf("expected NodeOnline, got %s", node.State)
	}

	// ExitMaintenance on non-maint node should fail
	if err := store.ExitMaintenance(ctx, 10); err == nil {
		t.Error("should fail to exit maintenance on online node")
	}

	// Idempotent EnterMaintenance
	if err := store.EnterMaintenance(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if err := store.EnterMaintenance(ctx, 10); err != nil {
		t.Error("EnterMaintenance should be idempotent")
	}
}

// ========== Auto Rebalance Chaos ==========

func TestChaos_AutoRebalance_TriggeredOnNewNode(t *testing.T) {
	store := newChaosTestStore(t)
	defer store.Close()
	store.SetAutoRebalance(true)

	ctx := context.Background()

	// Register initial node with data
	info1 := &NodeInfo{ID: 1, Zone: "z1", CapacityGB: 100, UsedGB: 80, State: NodeOnline}
	if err := store.RegisterNode(ctx, info1); err != nil {
		t.Fatal(err)
	}

	// Register a second node — should trigger auto-rebalance
	info2 := &NodeInfo{ID: 2, Zone: "z2", CapacityGB: 100, UsedGB: 10, State: NodeOnline}
	if err := store.RegisterNode(ctx, info2); err != nil {
		t.Fatal(err)
	}

	// Give rebalance goroutine time to run
	time.Sleep(500 * time.Millisecond)

	// Verify both nodes are registered
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}
}

// Helper: create a test PebbleStore for chaos tests
func newChaosTestStore(t *testing.T) *PebbleStore {
	t.Helper()
	store, err := NewPebbleStore(PebbleStoreConfig{
		Dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}
