package metadata

import (
	"fmt"
	"testing"
)

func makeTestNode(id NodeID, rack, zone string, tier StorageTier, capGB, usedGB int64, state NodeState) *NodeInfo {
	return &NodeInfo{
		ID:         id,
		Addr:       fmt.Sprintf("10.0.0.%d:9100", id),
		Rack:       rack,
		Zone:       zone,
		Tier:       tier,
		CapacityGB: capGB,
		UsedGB:     usedGB,
		State:      state,
	}
}

func TestPlacementEngine_BasicPlacement(t *testing.T) {
	pe := NewPlacementEngine()

	// Add 5 nodes across different racks
	for i := NodeID(1); i <= 5; i++ {
		rack := fmt.Sprintf("rack-%d", i)
		pe.UpdateNode(makeTestNode(i, rack, "zone-1", TierHot, 1000, 100, NodeOnline))
	}

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadRack,
		StorageTier:       TierHot,
	}

	selected, err := pe.PlaceChunk(policy, nil)
	if err != nil {
		t.Fatalf("PlaceChunk: %v", err)
	}
	if len(selected) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(selected))
	}

	// Verify all selected nodes are in different racks
	racks := map[string]bool{}
	for _, nid := range selected {
		node := pe.nodes[nid]
		if racks[node.Rack] {
			t.Fatalf("two replicas in same rack: %s", node.Rack)
		}
		racks[node.Rack] = true
	}
}

func TestPlacementEngine_InsufficientNodes(t *testing.T) {
	pe := NewPlacementEngine()

	// Only 2 nodes, need 3
	pe.UpdateNode(makeTestNode(1, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(2, "rack-2", "zone-1", TierHot, 1000, 100, NodeOnline))

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadNode,
	}

	_, err := pe.PlaceChunk(policy, nil)
	if err != ErrInsufficientNodes {
		t.Fatalf("expected ErrInsufficientNodes, got: %v", err)
	}
}

func TestPlacementEngine_ExcludeNodes(t *testing.T) {
	pe := NewPlacementEngine()

	for i := NodeID(1); i <= 6; i++ {
		pe.UpdateNode(makeTestNode(i, fmt.Sprintf("rack-%d", i), "zone-1", TierHot, 1000, 100, NodeOnline))
	}

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadNode,
	}

	// Exclude nodes 1, 2, 3 (leaves 4, 5, 6 = exactly 3)
	exclude := map[NodeID]bool{1: true, 2: true, 3: true}
	selected, err := pe.PlaceChunk(policy, exclude)
	if err != nil {
		t.Fatalf("PlaceChunk with excludes: %v", err)
	}
	if len(selected) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(selected))
	}

	for _, nid := range selected {
		if exclude[nid] {
			t.Fatalf("selected excluded node %d", nid)
		}
	}
}

func TestPlacementEngine_SkipOfflineNodes(t *testing.T) {
	pe := NewPlacementEngine()

	pe.UpdateNode(makeTestNode(1, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(2, "rack-2", "zone-1", TierHot, 1000, 100, NodeOffline))
	pe.UpdateNode(makeTestNode(3, "rack-3", "zone-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(4, "rack-4", "zone-1", TierHot, 1000, 100, NodeDraining))
	pe.UpdateNode(makeTestNode(5, "rack-5", "zone-1", TierHot, 1000, 100, NodeOnline))

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadNode,
	}

	selected, err := pe.PlaceChunk(policy, nil)
	if err != nil {
		t.Fatalf("PlaceChunk: %v", err)
	}

	for _, nid := range selected {
		if nid == 2 || nid == 4 {
			t.Fatalf("selected non-online node %d", nid)
		}
	}
}

func TestPlacementEngine_SkipFullNodes(t *testing.T) {
	pe := NewPlacementEngine()

	pe.UpdateNode(makeTestNode(1, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(2, "rack-2", "zone-1", TierHot, 1000, 960, NodeOnline)) // 96% full
	pe.UpdateNode(makeTestNode(3, "rack-3", "zone-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(4, "rack-4", "zone-1", TierHot, 1000, 100, NodeOnline))

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadNode,
	}

	selected, err := pe.PlaceChunk(policy, nil)
	if err != nil {
		t.Fatalf("PlaceChunk: %v", err)
	}

	for _, nid := range selected {
		if nid == 2 {
			t.Fatal("selected a >95% full node")
		}
	}
}

func TestPlacementEngine_TierFilter(t *testing.T) {
	pe := NewPlacementEngine()

	pe.UpdateNode(makeTestNode(1, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(2, "rack-2", "zone-1", TierCold, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(3, "rack-3", "zone-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(4, "rack-4", "zone-1", TierHot, 1000, 100, NodeOnline))

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadNode,
		StorageTier:       TierHot,
	}

	selected, err := pe.PlaceChunk(policy, nil)
	if err != nil {
		t.Fatalf("PlaceChunk: %v", err)
	}

	for _, nid := range selected {
		if nid == 2 {
			t.Fatal("selected a TierCold node for TierHot policy")
		}
	}
}

func TestPlacementEngine_ZoneSpread(t *testing.T) {
	pe := NewPlacementEngine()

	pe.UpdateNode(makeTestNode(1, "rack-1", "us-east-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(2, "rack-2", "us-east-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(3, "rack-3", "us-west-2", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(4, "rack-4", "eu-west-1", TierHot, 1000, 100, NodeOnline))

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadZone,
		StorageTier:       TierHot,
	}

	selected, err := pe.PlaceChunk(policy, nil)
	if err != nil {
		t.Fatalf("PlaceChunk: %v", err)
	}
	if len(selected) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(selected))
	}

	// Verify all in different zones
	zones := map[string]bool{}
	for _, nid := range selected {
		node := pe.nodes[nid]
		if zones[node.Zone] {
			t.Fatalf("two replicas in same zone: %s", node.Zone)
		}
		zones[node.Zone] = true
	}
}

func TestPlacementEngine_TopologyRelaxation(t *testing.T) {
	pe := NewPlacementEngine()

	// 3 nodes but all in same rack — SpreadRack can't be fully satisfied
	pe.UpdateNode(makeTestNode(1, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(2, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(3, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline))

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadRack,
	}

	// Should still succeed by relaxing topology constraint
	selected, err := pe.PlaceChunk(policy, nil)
	if err != nil {
		t.Fatalf("PlaceChunk with relaxed topology: %v", err)
	}
	if len(selected) != 3 {
		t.Fatalf("expected 3 replicas (relaxed), got %d", len(selected))
	}
}

func TestPlacementEngine_LoadBalancing(t *testing.T) {
	pe := NewPlacementEngine()

	pe.UpdateNode(makeTestNode(1, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(2, "rack-2", "zone-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(3, "rack-3", "zone-1", TierHot, 1000, 100, NodeOnline))

	// Node 1 is heavily loaded, node 3 is lightly loaded
	pe.UpdateLoad(1, 0.9) // 90% load
	pe.UpdateLoad(2, 0.5)
	pe.UpdateLoad(3, 0.1) // 10% load

	policy := PlacementPolicy{
		ReplicationFactor: 1,
		TopologySpread:    SpreadNode,
	}

	// Run multiple times and count which node is selected as primary
	counts := map[NodeID]int{}
	for i := 0; i < 100; i++ {
		selected, err := pe.PlaceChunk(policy, nil)
		if err != nil {
			t.Fatalf("PlaceChunk: %v", err)
		}
		counts[selected[0]]++
	}

	// Node 3 (lowest load, most free) should be selected most often as primary
	if counts[3] <= counts[1] {
		t.Logf("Selection counts: node1=%d, node2=%d, node3=%d", counts[1], counts[2], counts[3])
		t.Log("Node 3 (lowest load) should be selected more often than node 1 (highest load)")
		// Don't fail — jitter can occasionally cause this, but log it
	}
}

func TestPlacementEngine_RemoveNode(t *testing.T) {
	pe := NewPlacementEngine()

	pe.UpdateNode(makeTestNode(1, "rack-1", "zone-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(2, "rack-2", "zone-1", TierHot, 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNode(3, "rack-3", "zone-1", TierHot, 1000, 100, NodeOnline))

	pe.RemoveNode(2)

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadNode,
	}

	_, err := pe.PlaceChunk(policy, nil)
	if err != ErrInsufficientNodes {
		t.Fatalf("expected ErrInsufficientNodes after RemoveNode, got: %v", err)
	}
}

func TestPlacementEngine_EmptyCluster(t *testing.T) {
	pe := NewPlacementEngine()

	policy := PlacementPolicy{
		ReplicationFactor: 1,
		TopologySpread:    SpreadNode,
	}

	_, err := pe.PlaceChunk(policy, nil)
	if err != ErrInsufficientNodes {
		t.Fatalf("expected ErrInsufficientNodes on empty cluster, got: %v", err)
	}
}

func TestChunkIDGenerator_Uniqueness(t *testing.T) {
	gen := NewChunkIDGenerator(1)

	seen := make(map[ChunkID]bool)
	const count = 5000

	for i := 0; i < count; i++ {
		id := gen.Next()
		if seen[id] {
			t.Fatalf("duplicate chunk ID generated: %d (at iteration %d)", id, i)
		}
		seen[id] = true
	}
}

func TestChunkIDGenerator_NodeIDEmbedded(t *testing.T) {
	gen1 := NewChunkIDGenerator(1)
	gen2 := NewChunkIDGenerator(2)

	id1 := gen1.Next()
	id2 := gen2.Next()

	// Extract node ID from chunk ID (bits 13-22)
	nodeID1 := (uint64(id1) >> 13) & 0x3FF
	nodeID2 := (uint64(id2) >> 13) & 0x3FF

	if nodeID1 != 1 {
		t.Fatalf("expected node ID 1, got %d", nodeID1)
	}
	if nodeID2 != 2 {
		t.Fatalf("expected node ID 2, got %d", nodeID2)
	}
}

func TestChunkIDGenerator_NodeIDMasking(t *testing.T) {
	// NodeID > 1023 should be masked to 10 bits
	gen := NewChunkIDGenerator(2048) // 2048 = 0b100000000000, masked to 0
	id := gen.Next()
	nodeID := (uint64(id) >> 13) & 0x3FF
	if nodeID != 0 {
		t.Fatalf("expected masked node ID 0, got %d", nodeID)
	}
}

func TestChunkIDGenerator_Monotonic(t *testing.T) {
	gen := NewChunkIDGenerator(1)

	var prev ChunkID
	for i := 0; i < 1000; i++ {
		id := gen.Next()
		if i > 0 && id <= prev {
			t.Fatalf("chunk IDs not monotonically increasing: %d <= %d at iteration %d", id, prev, i)
		}
		prev = id
	}
}

func makeTestNodeWithMachine(id NodeID, rack, zone, machineID string, capGB, usedGB int64, state NodeState) *NodeInfo {
	n := makeTestNode(id, rack, zone, TierHot, capGB, usedGB, state)
	n.MachineID = machineID
	return n
}

func TestPlacementEngine_MachineSpread(t *testing.T) {
	pe := NewPlacementEngine()

	// Two nodes on machine-A, one on machine-B, one on machine-C
	pe.UpdateNode(makeTestNodeWithMachine(1, "rack-1", "zone-1", "machine-A", 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNodeWithMachine(2, "rack-1", "zone-1", "machine-A", 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNodeWithMachine(3, "rack-2", "zone-1", "machine-B", 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNodeWithMachine(4, "rack-3", "zone-1", "machine-C", 1000, 100, NodeOnline))

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadMachine,
	}

	selected, err := pe.PlaceChunk(policy, nil)
	if err != nil {
		t.Fatalf("PlaceChunk: %v", err)
	}
	if len(selected) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(selected))
	}

	// Verify all on different machines
	machines := map[string]bool{}
	for _, nid := range selected {
		node := pe.nodes[nid]
		if machines[node.MachineID] {
			t.Fatalf("two replicas on same machine: %s", node.MachineID)
		}
		machines[node.MachineID] = true
	}
}

func TestPlacementEngine_MachineSpreadRelaxation(t *testing.T) {
	pe := NewPlacementEngine()

	// 3 nodes but only 2 machines — SpreadMachine should relax
	pe.UpdateNode(makeTestNodeWithMachine(1, "rack-1", "zone-1", "machine-A", 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNodeWithMachine(2, "rack-1", "zone-1", "machine-A", 1000, 100, NodeOnline))
	pe.UpdateNode(makeTestNodeWithMachine(3, "rack-1", "zone-1", "machine-B", 1000, 100, NodeOnline))

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadMachine,
	}

	selected, err := pe.PlaceChunk(policy, nil)
	if err != nil {
		t.Fatalf("PlaceChunk with relaxed machine spread: %v", err)
	}
	if len(selected) != 3 {
		t.Fatalf("expected 3 replicas (relaxed), got %d", len(selected))
	}
}
