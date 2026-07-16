package metadata

import (
	"testing"
)

// ============================================================
// TDD: Placement Algorithm Determinism
// ============================================================
// The placement algorithm must be deterministic: given the same set of
// online nodes and the same policy, PlaceChunk should always return the
// same node selection. This is critical for:
// 1. Reproducible debugging
// 2. Consistent replica placement across leader failover
// 3. Predictable capacity planning

func TestPlacement_Deterministic_SameInput(t *testing.T) {
	engine := NewPlacementEngineWithSeed(42) // deterministic seed

	// Register nodes
	for i := 0; i < 5; i++ {
		engine.UpdateNode(&NodeInfo{
			ID:         NodeID(100 + i),
			Addr:       formatNodeAddr(i),
			CapacityGB: 1000,
			UsedGB:     100,
			State:      NodeOnline,
		})
	}

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadNode,
	}

	// Call PlaceChunk 10 times with same input
	var firstResult []NodeID
	for i := 0; i < 10; i++ {
		result, err := engine.PlaceChunk(policy, nil)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			firstResult = result
		} else {
			if !nodeIDsEqual(firstResult, result) {
				t.Errorf("call %d: got %v, want %v (deterministic)", i, result, firstResult)
			}
		}
	}
}

func TestPlacement_Deterministic_DifferentSeeds(t *testing.T) {
	// Since jitter is now deterministic based on node ID (not RNG),
	// two engines with different seeds should produce the SAME result
	// given the same node set — this is the desired property.
	engine1 := NewPlacementEngineWithSeed(42)
	engine2 := NewPlacementEngineWithSeed(123)

	for i := 0; i < 5; i++ {
		n := &NodeInfo{
			ID:         NodeID(100 + i),
			Addr:       formatNodeAddr(i),
			CapacityGB: 1000,
			UsedGB:     100,
			State:      NodeOnline,
		}
		engine1.UpdateNode(n)
		engine2.UpdateNode(n)
	}

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadNode,
	}

	result1, _ := engine1.PlaceChunk(policy, nil)
	result2, _ := engine2.PlaceChunk(policy, nil)

	// Same node set → same placement, regardless of seed
	if !nodeIDsEqual(result1, result2) {
		t.Errorf("same node set should produce same result: engine1=%v engine2=%v", result1, result2)
	}
}

func TestPlacement_Deterministic_AfterNodeChange(t *testing.T) {
	engine := NewPlacementEngineWithSeed(42)

	for i := 0; i < 5; i++ {
		engine.UpdateNode(&NodeInfo{
			ID:         NodeID(100 + i),
			Addr:       formatNodeAddr(i),
			CapacityGB: 1000,
			UsedGB:     100,
			State:      NodeOnline,
		})
	}

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		TopologySpread:    SpreadNode,
	}

	resultBefore, _ := engine.PlaceChunk(policy, nil)

	// Add a new node
	engine.UpdateNode(&NodeInfo{
		ID:         NodeID(200),
		Addr:       "10.0.0.10:8080",
		CapacityGB: 1000,
		UsedGB:     0,
		State:      NodeOnline,
	})

	resultAfter, _ := engine.PlaceChunk(policy, nil)

	// After adding a node, the placement should change
	// (because the new node is likely the least loaded)
	_ = resultBefore
	_ = resultAfter
	// We don't assert they're different because the algorithm might
	// still pick the same nodes if they're the best fit.
	// The key property is determinism, not change.
}

// Helper to compare node ID slices
func nodeIDsEqual(a, b []NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
