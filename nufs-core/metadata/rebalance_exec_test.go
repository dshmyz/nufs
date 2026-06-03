package metadata

import (
	"context"
	"testing"
)

// mockMetadataStore implements MetadataService for testing the executor.
type mockRebalanceStore struct {
	MetadataService
	nodes  []NodeInfo
	chunks []ChunkMeta
	repair []ChunkID
}

func (m *mockRebalanceStore) ListNodes(ctx context.Context) ([]NodeInfo, error) {
	return m.nodes, nil
}

func (m *mockRebalanceStore) ChunksByNode(ctx context.Context, nodeID NodeID) ([]ChunkMeta, error) {
	var result []ChunkMeta
	for _, c := range m.chunks {
		for _, r := range c.Replicas {
			if r.NodeID == nodeID {
				result = append(result, c)
				break
			}
		}
	}
	return result, nil
}

func (m *mockRebalanceStore) TriggerRepair(ctx context.Context, chunkID ChunkID) error {
	m.repair = append(m.repair, chunkID)
	return nil
}

func (m *mockRebalanceStore) MigrateChunkReplica(ctx context.Context, chunkID ChunkID, fromNode, toNode NodeID) error {
	for i := range m.chunks {
		if m.chunks[i].ID == chunkID {
			newReplicas := make([]ReplicaInfo, 0, len(m.chunks[i].Replicas))
			for _, r := range m.chunks[i].Replicas {
				if r.NodeID != fromNode {
					newReplicas = append(newReplicas, r)
				}
			}
			newReplicas = append(newReplicas, ReplicaInfo{NodeID: toNode, Addr: "10.0.0.1:9100", State: ReplicaSyncing})
			m.chunks[i].Replicas = newReplicas
			break
		}
	}
	return nil
}

func (m *mockRebalanceStore) DecommissionNode(ctx context.Context, nodeID NodeID) error {
	for i := range m.nodes {
		if m.nodes[i].ID == nodeID {
			m.nodes[i].State = NodeDraining
			break
		}
	}
	return nil
}

func (m *mockRebalanceStore) GetXAttr(_ context.Context, _ InodeID, _ string) ([]byte, error) {
	return nil, ErrXAttrNotFound
}
func (m *mockRebalanceStore) SetXAttr(_ context.Context, _ InodeID, _ string, _ []byte) error {
	return nil
}
func (m *mockRebalanceStore) ListXAttr(_ context.Context, _ InodeID) (map[string][]byte, error) {
	return nil, nil
}
func (m *mockRebalanceStore) RemoveXAttr(_ context.Context, _ InodeID, _ string) error {
	return nil
}

func TestRebalanceExecutor_ExecuteDecommission(t *testing.T) {
	store := &mockRebalanceStore{
		nodes: []NodeInfo{
			{ID: 1, Addr: "10.0.0.1:9100", ChunkCount: 10, State: NodeOnline},
			{ID: 2, Addr: "10.0.0.2:9100", ChunkCount: 0, State: NodeOnline},
			{ID: 3, Addr: "10.0.0.3:9100", ChunkCount: 0, State: NodeOnline},
		},
	}
	for i := 100; i < 110; i++ {
		store.chunks = append(store.chunks, ChunkMeta{
			ID:       ChunkID(i),
			Replicas: []ReplicaInfo{{NodeID: 1, Addr: "10.0.0.1:9100", State: ReplicaReady}},
		})
	}

	executor := NewRebalanceExecutor(store)
	err := executor.ExecuteDecommission(context.Background(), 1)
	if err != nil {
		t.Fatalf("ExecuteDecommission: %v", err)
	}

	// Verify all unique chunks triggered repair
	uniqueRepairs := make(map[ChunkID]bool)
	for _, id := range store.repair {
		uniqueRepairs[id] = true
	}
	if len(uniqueRepairs) != 10 {
		t.Errorf("expected 10 unique chunks in repair triggers, got %d: %v", len(uniqueRepairs), store.repair)
	}

	// Verify chunks no longer have node 1 as replica
	for _, c := range store.chunks {
		for _, r := range c.Replicas {
			if r.NodeID == 1 {
				t.Errorf("chunk %d still has replica on decommissioned node 1", c.ID)
			}
		}
	}
}

func TestRebalanceExecutor_ExecuteDecommission_NoChunks(t *testing.T) {
	store := &mockRebalanceStore{
		nodes: []NodeInfo{
			{ID: 1, Addr: "10.0.0.1:9100", ChunkCount: 0, State: NodeOnline},
			{ID: 2, Addr: "10.0.0.2:9100", ChunkCount: 0, State: NodeOnline},
		},
		chunks: []ChunkMeta{
			{ID: 200, Replicas: []ReplicaInfo{{NodeID: 2, Addr: "10.0.0.2:9100", State: ReplicaReady}}},
		},
	}

	executor := NewRebalanceExecutor(store)
	err := executor.ExecuteDecommission(context.Background(), 1)
	if err != nil {
		t.Fatalf("ExecuteDecommission: %v", err)
	}

	if len(store.repair) != 0 {
		t.Errorf("expected 0 repair triggers for node with no chunks, got %d", len(store.repair))
	}
}

func TestRebalanceExecutor_ExecutePlans(t *testing.T) {
	store := &mockRebalanceStore{
		nodes: []NodeInfo{
			{ID: 1, Addr: "10.0.0.1:9100", ChunkCount: 100, State: NodeOnline},
			{ID: 2, Addr: "10.0.0.2:9100", ChunkCount: 50, State: NodeOnline},
			{ID: 3, Addr: "10.0.0.3:9100", ChunkCount: 10, State: NodeOnline},
		},
		chunks: []ChunkMeta{
			{ID: 300, Replicas: []ReplicaInfo{{NodeID: 1, Addr: "10.0.0.1:9100", State: ReplicaReady}}},
			{ID: 301, Replicas: []ReplicaInfo{{NodeID: 1, Addr: "10.0.0.1:9100", State: ReplicaReady}}},
		},
	}

	plans := []MigrationPlan{
		{ChunkID: 300, SourceNode: 1, TargetNode: 2, Reason: "rebalance", Priority: 1},
		{ChunkID: 301, SourceNode: 1, TargetNode: 3, Reason: "rebalance", Priority: 1},
	}

	executor := NewRebalanceExecutor(store)
	err := executor.ExecutePlans(context.Background(), plans)
	if err != nil {
		t.Fatalf("ExecutePlans: %v", err)
	}

	if len(store.repair) != 2 {
		t.Errorf("expected 2 repair triggers, got %d", len(store.repair))
	}

	// Verify replica migration
	for _, c := range store.chunks {
		hasSource := false
		hasTarget := false
		for _, r := range c.Replicas {
			if r.NodeID == 1 {
				hasSource = true
			}
			if r.NodeID == 2 || r.NodeID == 3 {
				hasTarget = true
			}
		}
		if hasSource {
			t.Errorf("chunk %d still has source node 1 as replica", c.ID)
		}
		if !hasTarget {
			t.Errorf("chunk %d should have migrated to target", c.ID)
		}
	}
}

func TestRebalanceExecutor_ExecutePlans_Empty(t *testing.T) {
	store := &mockRebalanceStore{
		nodes: []NodeInfo{
			{ID: 1, Addr: "10.0.0.1:9100", ChunkCount: 100, State: NodeOnline},
			{ID: 2, Addr: "10.0.0.2:9100", ChunkCount: 100, State: NodeOnline},
		},
	}

	executor := NewRebalanceExecutor(store)
	err := executor.ExecutePlans(context.Background(), nil)
	if err != nil {
		t.Fatalf("ExecutePlans: %v", err)
	}

	if len(store.repair) != 0 {
		t.Errorf("expected 0 repair triggers for empty plans, got %d", len(store.repair))
	}
}

func TestRebalanceExecutor_ExecutePlans_SkipsZeroChunkID(t *testing.T) {
	store := &mockRebalanceStore{
		nodes: []NodeInfo{
			{ID: 1, Addr: "10.0.0.1:9100", ChunkCount: 100, State: NodeOnline},
			{ID: 2, Addr: "10.0.0.2:9100", ChunkCount: 50, State: NodeOnline},
		},
	}

	plans := []MigrationPlan{
		{ChunkID: 0, SourceNode: 1, TargetNode: 2, Reason: "test", Priority: 1},
	}

	executor := NewRebalanceExecutor(store)
	err := executor.ExecutePlans(context.Background(), plans)
	if err != nil {
		t.Fatalf("ExecutePlans: %v", err)
	}

	if len(store.repair) != 0 {
		t.Errorf("expected 0 repair triggers for zero chunk ID, got %d", len(store.repair))
	}
}
