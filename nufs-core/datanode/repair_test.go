package datanode

import (
	"context"
	"sync"
	"testing"

	"github.com/example/dfs/metadata"
)

// mockMetadataService implements metadata.MetadataService for repair tests.
type mockMetadataService struct {
	mu            sync.Mutex
	chunks        map[metadata.ChunkID]*metadata.ChunkMeta
	nodes         []metadata.NodeInfo
	repairQueue   []metadata.RepairTask
	repairRemoved map[metadata.ChunkID]bool
}

func newMockMetadataService() *mockMetadataService {
	return &mockMetadataService{
		chunks:        make(map[metadata.ChunkID]*metadata.ChunkMeta),
		repairRemoved: make(map[metadata.ChunkID]bool),
	}
}

func (m *mockMetadataService) Close() error { return nil }

func (m *mockMetadataService) GetChunk(_ context.Context, chunkID metadata.ChunkID) (*metadata.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	chunk, ok := m.chunks[chunkID]
	if !ok {
		return nil, metadata.ErrChunkNotFound
	}
	return chunk, nil
}

func (m *mockMetadataService) UpdateChunk(_ context.Context, chunk *metadata.ChunkMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chunks[chunk.ID] = chunk
	return nil
}

func (m *mockMetadataService) ReportChunkState(_ context.Context, _ metadata.NodeID, states map[metadata.ChunkID]metadata.ReplicaState) error {
	return nil
}

func (m *mockMetadataService) ListNodes(_ context.Context) ([]metadata.NodeInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nodes, nil
}

func (m *mockMetadataService) GetRepairQueue(_ context.Context) ([]metadata.RepairTask, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.repairQueue, nil
}

func (m *mockMetadataService) TriggerRepair(_ context.Context, _ metadata.ChunkID) error {
	return nil
}

func (m *mockMetadataService) RemoveRepairTask(_ context.Context, chunkID metadata.ChunkID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.repairRemoved[chunkID] = true
	return nil
}

func (m *mockMetadataService) TriggerRebalance(_ context.Context) error { return nil }

// Unused interface methods
func (m *mockMetadataService) CreateBucket(_ context.Context, _ string, _ metadata.PlacementPolicy) error { return nil }
func (m *mockMetadataService) DeleteBucket(_ context.Context, _ string) error { return nil }
func (m *mockMetadataService) ListBuckets(_ context.Context) ([]metadata.BucketInfo, error) { return nil, nil }
func (m *mockMetadataService) GetBucket(_ context.Context, _ string) (*metadata.BucketInfo, error) { return nil, nil }
func (m *mockMetadataService) MkDir(_ context.Context, _ metadata.InodeID, _ string, _ uint32) (*metadata.InodeMeta, error) { return nil, nil }
func (m *mockMetadataService) RmDir(_ context.Context, _ metadata.InodeID, _ string) error { return nil }
func (m *mockMetadataService) ReadDir(_ context.Context, _ metadata.InodeID, _ int, _ int) ([]metadata.DirEntry, error) { return nil, nil }
func (m *mockMetadataService) CreateFile(_ context.Context, _ metadata.InodeID, _ string, _ uint32) (*metadata.InodeMeta, error) { return nil, nil }
func (m *mockMetadataService) Unlink(_ context.Context, _ metadata.InodeID, _ string) error { return nil }
func (m *mockMetadataService) Lookup(_ context.Context, _ metadata.InodeID, _ string) (*metadata.InodeMeta, error) { return nil, nil }
func (m *mockMetadataService) Rename(_ context.Context, _ metadata.InodeID, _ string, _ metadata.InodeID, _ string) error { return nil }
func (m *mockMetadataService) Symlink(_ context.Context, _ metadata.InodeID, _ string, _ string) (*metadata.InodeMeta, error) { return nil, nil }
func (m *mockMetadataService) Readlink(_ context.Context, _ metadata.InodeID) (string, error) { return "", nil }
func (m *mockMetadataService) Link(_ context.Context, _ metadata.InodeID, _ string, _ metadata.InodeID) (*metadata.InodeMeta, error) { return nil, nil }
func (m *mockMetadataService) GetInode(_ context.Context, _ metadata.InodeID) (*metadata.InodeMeta, error) { return nil, nil }
func (m *mockMetadataService) UpdateInode(_ context.Context, _ *metadata.InodeMeta) error { return nil }
func (m *mockMetadataService) AllocateChunk(_ context.Context, _ metadata.InodeID, _ int64, _ metadata.PlacementPolicy) (*metadata.ChunkMeta, error) { return nil, nil }
func (m *mockMetadataService) CommitChunk(_ context.Context, _ metadata.ChunkID, _ uint32) error { return nil }
func (m *mockMetadataService) SealChunk(_ context.Context, _ metadata.ChunkID) error { return nil }
func (m *mockMetadataService) ListChunks(_ context.Context, _ metadata.InodeID) ([]metadata.ChunkRef, error) { return nil, nil }
func (m *mockMetadataService) DeleteChunk(_ context.Context, _ metadata.ChunkID) error { return nil }
func (m *mockMetadataService) RegisterNode(_ context.Context, _ *metadata.NodeInfo) error { return nil }
func (m *mockMetadataService) Heartbeat(_ context.Context, _ metadata.NodeID, _ *metadata.NodeReport) error { return nil }
func (m *mockMetadataService) DecommissionNode(_ context.Context, _ metadata.NodeID) error { return nil }
func (m *mockMetadataService) GetNode(_ context.Context, _ metadata.NodeID) (*metadata.NodeInfo, error) { return nil, nil }
func (m *mockMetadataService) ChunksByNode(_ context.Context, nodeID metadata.NodeID) ([]metadata.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []metadata.ChunkMeta
	for _, chunk := range m.chunks {
		for _, r := range chunk.Replicas {
			if r.NodeID == nodeID {
				result = append(result, *chunk)
				break
			}
		}
	}
	return result, nil
}
func (m *mockMetadataService) MigrateChunkReplica(_ context.Context, chunkID metadata.ChunkID, fromNode, toNode metadata.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	chunk, ok := m.chunks[chunkID]
	if !ok {
		return metadata.ErrChunkNotFound
	}
	var newReplicas []metadata.ReplicaInfo
	for _, r := range chunk.Replicas {
		if r.NodeID != fromNode {
			newReplicas = append(newReplicas, r)
		}
	}
	toAddr := ""
	for _, n := range m.nodes {
		if n.ID == toNode {
			toAddr = n.Addr
			break
		}
	}
	newReplicas = append(newReplicas, metadata.ReplicaInfo{NodeID: toNode, Addr: toAddr, State: metadata.ReplicaSyncing})
	chunk.Replicas = newReplicas
	return nil
}

func TestRepairWorker_NeedsNewReplica(t *testing.T) {
	rw := &RepairWorker{}

	t.Run("no replicas does not need new (len=0)", func(t *testing.T) {
		chunk := &metadata.ChunkMeta{ID: 1}
		if rw.needsNewReplica(chunk) {
			t.Error("expected needsNewReplica=false when there are no replicas")
		}
	})

	t.Run("all ready does not need new", func(t *testing.T) {
		chunk := &metadata.ChunkMeta{
			ID: 1,
			Replicas: []metadata.ReplicaInfo{
				{NodeID: 1, State: metadata.ReplicaReady},
				{NodeID: 2, State: metadata.ReplicaReady},
			},
		}
		if rw.needsNewReplica(chunk) {
			t.Error("expected needsNewReplica=false when all replicas are ready")
		}
	})

	t.Run("some failed needs new", func(t *testing.T) {
		chunk := &metadata.ChunkMeta{
			ID: 1,
			Replicas: []metadata.ReplicaInfo{
				{NodeID: 1, State: metadata.ReplicaReady},
				{NodeID: 2, State: metadata.ReplicaFailed},
			},
		}
		if !rw.needsNewReplica(chunk) {
			t.Error("expected needsNewReplica=true when some replicas failed")
		}
	})
}

func TestRepairWorker_LocalReplicaCorrupt(t *testing.T) {
	rw := &RepairWorker{nodeID: 1}

	t.Run("local replica failed is corrupt", func(t *testing.T) {
		chunk := &metadata.ChunkMeta{
			Replicas: []metadata.ReplicaInfo{
				{NodeID: 1, State: metadata.ReplicaFailed},
			},
		}
		if !rw.localReplicaCorrupt(chunk) {
			t.Error("expected localReplicaCorrupt=true for failed local replica")
		}
	})

	t.Run("local replica ready is not corrupt", func(t *testing.T) {
		chunk := &metadata.ChunkMeta{
			Replicas: []metadata.ReplicaInfo{
				{NodeID: 1, State: metadata.ReplicaReady},
			},
		}
		if rw.localReplicaCorrupt(chunk) {
			t.Error("expected localReplicaCorrupt=false for ready local replica")
		}
	})

	t.Run("only remote failed is not local corrupt", func(t *testing.T) {
		chunk := &metadata.ChunkMeta{
			Replicas: []metadata.ReplicaInfo{
				{NodeID: 1, State: metadata.ReplicaReady},
				{NodeID: 2, State: metadata.ReplicaFailed},
			},
		}
		if rw.localReplicaCorrupt(chunk) {
			t.Error("expected localReplicaCorrupt=false when only remote replica is failed")
		}
	})
}

func TestRepairWorker_FindHealthySource(t *testing.T) {
	rw := &RepairWorker{}

	t.Run("finds ready replica", func(t *testing.T) {
		chunk := &metadata.ChunkMeta{
			Replicas: []metadata.ReplicaInfo{
				{NodeID: 1, State: metadata.ReplicaFailed},
				{NodeID: 2, State: metadata.ReplicaReady},
				{NodeID: 3, State: metadata.ReplicaStale},
			},
		}
		source := rw.findHealthySource(chunk)
		if source == nil || source.NodeID != 2 {
			t.Fatalf("expected node 2, got %+v", source)
		}
	})

	t.Run("no healthy source", func(t *testing.T) {
		chunk := &metadata.ChunkMeta{
			Replicas: []metadata.ReplicaInfo{
				{NodeID: 1, State: metadata.ReplicaFailed},
				{NodeID: 2, State: metadata.ReplicaStale},
			},
		}
		if source := rw.findHealthySource(chunk); source != nil {
			t.Fatalf("expected nil, got %+v", source)
		}
	})
}

func TestRepairWorker_FindTargetNode(t *testing.T) {
	meta := newMockMetadataService()
	meta.nodes = []metadata.NodeInfo{
		{ID: 1, State: metadata.NodeOnline, Addr: "host1:9100"},
		{ID: 2, State: metadata.NodeOnline, Addr: "host2:9100"},
		{ID: 3, State: metadata.NodeOffline, Addr: "host3:9100"},
	}
	rw := &RepairWorker{meta: meta}

	t.Run("skips existing nodes and offline nodes", func(t *testing.T) {
		chunk := &metadata.ChunkMeta{
			ID: 1,
			Replicas: []metadata.ReplicaInfo{
				{NodeID: 1, State: metadata.ReplicaReady},
			},
		}
		target, err := rw.findTargetNode(context.Background(), chunk)
		if err != nil {
			t.Fatalf("findTargetNode: %v", err)
		}
		if target.ID != 2 {
			t.Fatalf("expected node 2, got %d", target.ID)
		}
	})

	t.Run("no available target", func(t *testing.T) {
		chunk := &metadata.ChunkMeta{
			ID: 2,
			Replicas: []metadata.ReplicaInfo{
				{NodeID: 1, State: metadata.ReplicaReady},
				{NodeID: 2, State: metadata.ReplicaReady},
			},
		}
		_, err := rw.findTargetNode(context.Background(), chunk)
		if err == nil {
			t.Fatal("expected error for no available target")
		}
	})
}

func TestRepairWorker_ProcessRepairQueue_RemovesCompleted(t *testing.T) {
	meta := newMockMetadataService()

	chunk := &metadata.ChunkMeta{
		ID: 100,
		Replicas: []metadata.ReplicaInfo{
			{NodeID: 1, State: metadata.ReplicaReady},
			{NodeID: 2, State: metadata.ReplicaReady},
		},
	}
	meta.chunks[100] = chunk

	meta.repairQueue = []metadata.RepairTask{
		{ChunkID: 100, Reason: "test", Priority: 1},
	}

	rw := &RepairWorker{
		meta:   meta,
		nodeID: 1,
	}

	rw.processRepairQueue(context.Background())

	// All replicas ready → task is healthy → should be removed
	if !meta.repairRemoved[100] {
		t.Error("expected completed repair task to be removed from queue")
	}
}

func TestRepairWorker_ProcessRepairQueue_RemovesStaleFailed(t *testing.T) {
	meta := newMockMetadataService()

	// Chunk with no replicas at all → repair will fail
	meta.chunks[200] = &metadata.ChunkMeta{ID: 200}

	meta.repairQueue = []metadata.RepairTask{
		{ChunkID: 200, Reason: "test", Priority: 6}, // high priority = stale
	}

	rw := &RepairWorker{
		meta:   meta,
		nodeID: 1,
	}

	rw.processRepairQueue(context.Background())

	if !meta.repairRemoved[200] {
		t.Error("expected stale failed repair task to be removed")
	}
}
