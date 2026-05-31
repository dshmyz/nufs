package datanode

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/example/dfs/metadata"
)

// RepairWorker monitors chunk health and triggers repairs for degraded chunks.
type RepairWorker struct {
	meta     metadata.MetadataService
	client   *Client
	nodeID   metadata.NodeID

	mu       sync.Mutex
	running  bool
	stopCh   chan struct{}
	interval time.Duration

	// Stats
	repaired  int64
	failed    int64
}

// RepairConfig holds configuration for the repair worker.
type RepairConfig struct {
	Meta     metadata.MetadataService
	Client   *Client
	NodeID   metadata.NodeID
	Interval time.Duration // How often to scan for repairs
}

// NewRepairWorker creates a new chunk repair worker.
func NewRepairWorker(cfg RepairConfig) *RepairWorker {
	if cfg.Interval == 0 {
		cfg.Interval = 30 * time.Second
	}
	return &RepairWorker{
		meta:     cfg.Meta,
		client:   cfg.Client,
		nodeID:   cfg.NodeID,
		stopCh:   make(chan struct{}),
		interval: cfg.Interval,
	}
}

// Start begins the repair scan loop.
func (rw *RepairWorker) Start(ctx context.Context) {
	rw.mu.Lock()
	if rw.running {
		rw.mu.Unlock()
		return
	}
	rw.running = true
	rw.mu.Unlock()

	go rw.scanLoop(ctx)
	log.Printf("repair: worker started (interval=%v)", rw.interval)
}

// Stop halts the repair worker.
func (rw *RepairWorker) Stop() {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	if rw.running {
		close(rw.stopCh)
		rw.running = false
	}
}

// Stats returns repair statistics.
func (rw *RepairWorker) Stats() (repaired, failed int64) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.repaired, rw.failed
}

func (rw *RepairWorker) scanLoop(ctx context.Context) {
	ticker := time.NewTicker(rw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-rw.stopCh:
			return
		case <-ticker.C:
			rw.processRepairQueue(ctx)
		}
	}
}

func (rw *RepairWorker) processRepairQueue(ctx context.Context) {
	tasks, err := rw.meta.GetRepairQueue(ctx)
	if err != nil {
		log.Printf("repair: failed to get repair queue: %v", err)
		return
	}

	for _, task := range tasks {
		if err := rw.repairChunk(ctx, task); err != nil {
			log.Printf("repair: chunk %d repair failed: %v", task.ChunkID, err)
			rw.mu.Lock()
			rw.failed++
			rw.mu.Unlock()
		} else {
			rw.mu.Lock()
			rw.repaired++
			rw.mu.Unlock()
		}
	}
}

func (rw *RepairWorker) repairChunk(ctx context.Context, task metadata.RepairTask) error {
	// Get chunk metadata
	chunk, err := rw.meta.GetChunk(ctx, task.ChunkID)
	if err != nil {
		return fmt.Errorf("repair: get chunk %d: %w", task.ChunkID, err)
	}

	// Find a healthy replica to copy from
	var sourceReplica *metadata.ReplicaInfo
	for i := range chunk.Replicas {
		if chunk.Replicas[i].State == metadata.ReplicaReady {
			sourceReplica = &chunk.Replicas[i]
			break
		}
	}
	if sourceReplica == nil {
		return fmt.Errorf("repair: no healthy replica for chunk %d", task.ChunkID)
	}

	// Find a target node for the new replica
	nodes, err := rw.meta.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("repair: list nodes: %w", err)
	}

	var targetNode *metadata.NodeInfo
	existingNodes := make(map[metadata.NodeID]bool)
	for _, r := range chunk.Replicas {
		existingNodes[r.NodeID] = true
	}

	for i := range nodes {
		if nodes[i].State == metadata.NodeOnline && !existingNodes[nodes[i].ID] {
			targetNode = &nodes[i]
			break
		}
	}
	if targetNode == nil {
		return fmt.Errorf("repair: no available node for new replica")
	}

	// Replicate chunk data from source to target
	log.Printf("repair: replicating chunk %d from node %d to node %d",
		task.ChunkID, sourceReplica.NodeID, targetNode.ID)

	// In production: read chunk data from source node via TCP,
	// write to target node, update chunk metadata with new replica.
	// For now, we update the metadata to reflect the repair intent.

	// Report chunk state on the new node
	states := map[metadata.ChunkID]metadata.ReplicaState{
		task.ChunkID: metadata.ReplicaSyncing,
	}
	if err := rw.meta.ReportChunkState(ctx, targetNode.ID, states); err != nil {
		return fmt.Errorf("repair: report chunk state: %w", err)
	}

	// Trigger repair in metadata service
	if err := rw.meta.TriggerRepair(ctx, task.ChunkID); err != nil {
		return fmt.Errorf("repair: trigger repair: %w", err)
	}

	return nil
}
