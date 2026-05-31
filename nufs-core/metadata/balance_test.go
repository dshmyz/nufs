package metadata

import (
	"testing"
)

func TestRebalancePlanner_BalancedCluster(t *testing.T) {
	rp := &RebalancePlanner{}
	nodes := []NodeInfo{
		{ID: 1, ChunkCount: 100, CapacityGB: 1000, UsedGB: 100, State: NodeOnline},
		{ID: 2, ChunkCount: 100, CapacityGB: 1000, UsedGB: 100, State: NodeOnline},
		{ID: 3, ChunkCount: 100, CapacityGB: 1000, UsedGB: 100, State: NodeOnline},
	}

	result := rp.PlanRebalance(nodes, 0.1)
	if !result.Balanced {
		t.Error("expected balanced cluster")
	}
	if len(result.Plans) != 0 {
		t.Errorf("expected 0 migration plans, got %d", len(result.Plans))
	}
}

func TestRebalancePlanner_ImbalancedCluster(t *testing.T) {
	rp := &RebalancePlanner{}
	nodes := []NodeInfo{
		{ID: 1, ChunkCount: 500, CapacityGB: 1000, UsedGB: 500, State: NodeOnline},
		{ID: 2, ChunkCount: 50, CapacityGB: 1000, UsedGB: 50, State: NodeOnline},
		{ID: 3, ChunkCount: 50, CapacityGB: 1000, UsedGB: 50, State: NodeOnline},
	}

	result := rp.PlanRebalance(nodes, 0.1)
	if result.Balanced {
		t.Error("expected imbalanced cluster")
	}
	if len(result.Plans) == 0 {
		t.Error("expected migration plans for imbalanced cluster")
	}
	if result.Imbalance <= 0 {
		t.Error("expected positive imbalance value")
	}

	// Verify plans move from overloaded to underloaded
	for _, plan := range result.Plans {
		if plan.SourceNode != 1 {
			t.Errorf("expected source node 1, got %d", plan.SourceNode)
		}
		if plan.TargetNode == 1 {
			t.Error("should not migrate to overloaded node")
		}
	}
}

func TestRebalancePlanner_SingleNode(t *testing.T) {
	rp := &RebalancePlanner{}
	nodes := []NodeInfo{
		{ID: 1, ChunkCount: 100, CapacityGB: 1000, UsedGB: 100, State: NodeOnline},
	}

	result := rp.PlanRebalance(nodes, 0.1)
	if !result.Balanced {
		t.Error("single node should be considered balanced")
	}
}

func TestRebalancePlanner_OfflineNodes(t *testing.T) {
	rp := &RebalancePlanner{}
	nodes := []NodeInfo{
		{ID: 1, ChunkCount: 200, CapacityGB: 1000, UsedGB: 200, State: NodeOnline},
		{ID: 2, ChunkCount: 200, CapacityGB: 1000, UsedGB: 200, State: NodeOnline},
		{ID: 3, ChunkCount: 100, CapacityGB: 1000, UsedGB: 100, State: NodeOffline},
	}

	result := rp.PlanRebalance(nodes, 0.1)
	// Offline node should be excluded from balancing
	if !result.Balanced {
		t.Error("balanced online nodes should not trigger rebalance")
	}
}

func TestRebalancePlanner_Decommission(t *testing.T) {
	rp := &RebalancePlanner{}
	nodes := []NodeInfo{
		{ID: 1, ChunkCount: 100, CapacityGB: 1000, UsedGB: 100, State: NodeOnline},
		{ID: 2, ChunkCount: 50, CapacityGB: 1000, UsedGB: 50, State: NodeOnline},
		{ID: 3, ChunkCount: 50, CapacityGB: 1000, UsedGB: 50, State: NodeOnline},
	}

	plans, err := rp.PlanDecommission(1, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 100 {
		t.Errorf("expected 100 migration plans, got %d", len(plans))
	}
	for _, plan := range plans {
		if plan.SourceNode != 1 {
			t.Errorf("expected source node 1, got %d", plan.SourceNode)
		}
		if plan.TargetNode == 1 {
			t.Error("should not migrate to decommissioning node")
		}
	}
}

func TestRebalancePlanner_DecommissionNotFound(t *testing.T) {
	rp := &RebalancePlanner{}
	nodes := []NodeInfo{
		{ID: 1, ChunkCount: 100, State: NodeOnline},
	}

	_, err := rp.PlanDecommission(99, nodes)
	if err == nil {
		t.Error("expected error for nonexistent node")
	}
}

func TestRebalancePlanner_DecommissionNoTargets(t *testing.T) {
	rp := &RebalancePlanner{}
	nodes := []NodeInfo{
		{ID: 1, ChunkCount: 100, State: NodeOnline},
		{ID: 2, ChunkCount: 50, State: NodeOffline},
	}

	_, err := rp.PlanDecommission(1, nodes)
	if err == nil {
		t.Error("expected error when no target nodes available")
	}
}
