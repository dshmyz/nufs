package metadata

import (
	"context"
	"testing"
	"time"
)

// ============================================================
// TDD: Auto Rebalance Scheduler
// ============================================================
// The AutoBalancer monitors cluster imbalance and automatically
// triggers rebalance when the imbalance exceeds a threshold.
// This replaces the current manual-only rebalance approach.

func TestAutoBalancer_DetectsImbalance(t *testing.T) {
	ab := NewAutoBalancer(AutoBalancerConfig{
		Threshold: 0.15, // 15% imbalance triggers rebalance
	})

	// Node A: 80% full, Node B: 20% full — clearly imbalanced
	nodes := []NodeInfo{
		{ID: 1, State: NodeOnline, CapacityGB: 1000, UsedGB: 800, ChunkCount: 800},
		{ID: 2, State: NodeOnline, CapacityGB: 1000, UsedGB: 200, ChunkCount: 200},
	}

	result := ab.Analyze(nodes)
	if result.Balanced {
		t.Error("expected imbalance to be detected")
	}
	if result.Imbalance < 0.15 {
		t.Errorf("imbalance = %.2f, expected >= 0.15", result.Imbalance)
	}
}

func TestAutoBalancer_BalancedCluster(t *testing.T) {
	ab := NewAutoBalancer(AutoBalancerConfig{
		Threshold: 0.15,
	})

	// Both nodes ~50% full
	nodes := []NodeInfo{
		{ID: 1, State: NodeOnline, CapacityGB: 1000, UsedGB: 500, ChunkCount: 500},
		{ID: 2, State: NodeOnline, CapacityGB: 1000, UsedGB: 510, ChunkCount: 510},
	}

	result := ab.Analyze(nodes)
	if !result.Balanced {
		t.Errorf("expected balanced, imbalance = %.2f", result.Imbalance)
	}
}

func TestAutoBalancer_GeneratesMigrationPlans(t *testing.T) {
	ab := NewAutoBalancer(AutoBalancerConfig{
		Threshold: 0.15,
	})

	nodes := []NodeInfo{
		{ID: 1, State: NodeOnline, CapacityGB: 1000, UsedGB: 800, ChunkCount: 800},
		{ID: 2, State: NodeOnline, CapacityGB: 1000, UsedGB: 200, ChunkCount: 200},
	}

	result := ab.Analyze(nodes)
	if len(result.Plans) == 0 {
		t.Error("expected migration plans to be generated")
	}

	// Plans should move chunks from overloaded to underloaded
	for _, plan := range result.Plans {
		if plan.SourceNode == plan.TargetNode {
			t.Error("source and target should be different nodes")
		}
	}
}

func TestAutoBalancer_SkipsOfflineNodes(t *testing.T) {
	ab := NewAutoBalancer(AutoBalancerConfig{
		Threshold: 0.15,
	})

	nodes := []NodeInfo{
		{ID: 1, State: NodeOnline, CapacityGB: 1000, UsedGB: 800, ChunkCount: 800},
		{ID: 2, State: NodeOffline, CapacityGB: 1000, UsedGB: 200, ChunkCount: 200},
		{ID: 3, State: NodeOnline, CapacityGB: 1000, UsedGB: 500, ChunkCount: 500},
	}

	result := ab.Analyze(nodes)
	// Should only consider online nodes (1 and 3)
	for _, plan := range result.Plans {
		if plan.TargetNode == 2 {
			t.Error("should not migrate to offline node")
		}
	}
}

func TestAutoBalancer_SingleNode_NoRebalance(t *testing.T) {
	ab := NewAutoBalancer(AutoBalancerConfig{
		Threshold: 0.15,
	})

	nodes := []NodeInfo{
		{ID: 1, State: NodeOnline, CapacityGB: 1000, UsedGB: 900, ChunkCount: 900},
	}

	result := ab.Analyze(nodes)
	if !result.Balanced {
		t.Error("single node should always be 'balanced' (nothing to rebalance)")
	}
}

func TestAutoBalancer_ConfigDefaults(t *testing.T) {
	cfg := AutoBalancerConfig{}
	cfg.ApplyDefaults()

	if cfg.Threshold != 0.15 {
		t.Errorf("default threshold = %.2f, want 0.15", cfg.Threshold)
	}
	if cfg.Interval != 5*time.Minute {
		t.Errorf("default interval = %v, want 5m", cfg.Interval)
	}
	if cfg.MaxConcurrentMigrations != 10 {
		t.Errorf("default max concurrent = %d, want 10", cfg.MaxConcurrentMigrations)
	}
}

func TestAutoBalancer_ShouldTrigger(t *testing.T) {
	ab := NewAutoBalancer(AutoBalancerConfig{
		Threshold: 0.15,
	})

	// Imbalanced
	nodes1 := []NodeInfo{
		{ID: 1, State: NodeOnline, CapacityGB: 1000, UsedGB: 800, ChunkCount: 800},
		{ID: 2, State: NodeOnline, CapacityGB: 1000, UsedGB: 200, ChunkCount: 200},
	}
	if !ab.ShouldTrigger(nodes1) {
		t.Error("should trigger for imbalanced cluster")
	}

	// Balanced
	nodes2 := []NodeInfo{
		{ID: 1, State: NodeOnline, CapacityGB: 1000, UsedGB: 500, ChunkCount: 500},
		{ID: 2, State: NodeOnline, CapacityGB: 1000, UsedGB: 510, ChunkCount: 510},
	}
	if ab.ShouldTrigger(nodes2) {
		t.Error("should not trigger for balanced cluster")
	}
}

// mockAutoBalanceStore for integration test
type mockAutoBalanceStore struct {
	nodes  []NodeInfo
	chunks []ChunkMeta
}

func (m *mockAutoBalanceStore) ListNodes(ctx context.Context) ([]NodeInfo, error) {
	return m.nodes, nil
}

func (m *mockAutoBalanceStore) ChunksByNode(ctx context.Context, nodeID NodeID) ([]ChunkMeta, error) {
	var result []ChunkMeta
	for _, chunk := range m.chunks {
		for _, replica := range chunk.Replicas {
			if replica.NodeID == nodeID {
				result = append(result, chunk)
				break
			}
		}
	}
	return result, nil
}

type mockAutoBalanceExecutor struct {
	plans []MigrationPlan
}

func (m *mockAutoBalanceExecutor) ExecutePlans(ctx context.Context, plans []MigrationPlan) error {
	m.plans = append(m.plans, plans...)
	return nil
}

func TestAutoBalancer_CheckExecutesConcreteMigrationPlans(t *testing.T) {
	store := &mockAutoBalanceStore{
		nodes: []NodeInfo{
			{ID: 1, State: NodeOnline, CapacityGB: 1000, UsedGB: 900, ChunkCount: 9},
			{ID: 2, State: NodeOnline, CapacityGB: 1000, UsedGB: 100, ChunkCount: 1},
		},
		chunks: []ChunkMeta{
			{ID: 101, Replicas: []ReplicaInfo{{NodeID: 1, State: ReplicaReady}}},
			{ID: 102, Replicas: []ReplicaInfo{{NodeID: 1, State: ReplicaReady}}},
			{ID: 103, Replicas: []ReplicaInfo{{NodeID: 1, State: ReplicaReady}}},
		},
	}
	executor := &mockAutoBalanceExecutor{}
	ab := NewAutoBalancer(AutoBalancerConfig{Threshold: 0.10})
	ab.SetStore(store)
	ab.SetExecutor(executor)

	ab.check()

	if len(executor.plans) == 0 {
		t.Fatal("expected auto-balancer to execute migration plans")
	}
	for _, plan := range executor.plans {
		if plan.ChunkID == 0 {
			t.Fatalf("expected concrete chunk IDs, got zero in plan %+v", plan)
		}
		if plan.SourceNode != 1 || plan.TargetNode != 2 {
			t.Fatalf("unexpected migration direction: %+v", plan)
		}
	}
}

func TestAutoBalancer_Integration_WithStore(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Register nodes with different loads
	for i := 0; i < 3; i++ {
		usedGB := int64(100)
		if i == 0 {
			usedGB = 800 // overloaded
		}
		node := &NodeInfo{
			ID:         NodeID(100 + i),
			Addr:       formatNodeAddr(i),
			CapacityGB: 1000,
			UsedGB:     usedGB,
			ChunkCount: usedGB, // 1 chunk per GB for simplicity
			State:      NodeOnline,
		}
		if err := store.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}

	ab := NewAutoBalancer(AutoBalancerConfig{
		Threshold: 0.15,
	})

	nodes, err := store.ListNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}

	result := ab.Analyze(nodes)
	if result.Balanced {
		t.Error("expected imbalance with 80%/10%/10% distribution")
	}
}
