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
	nodeID   metadata.NodeID

	mu       sync.Mutex
	running  bool
	stopCh   chan struct{}
	interval time.Duration

	// Stats
	repaired  int64
	failed    int64

	// Replicator for cross-node data copy
	replicator *Replicator
	localAddr  string
}

// RepairConfig holds configuration for the repair worker.
type RepairConfig struct {
	Meta       metadata.MetadataService
	NodeID     metadata.NodeID
	Interval   time.Duration // How often to scan for repairs
	Replicator *Replicator   // For cross-node chunk data copy
	LocalAddr  string        // Local data node address
}

// NewRepairWorker creates a new chunk repair worker.
func NewRepairWorker(cfg RepairConfig) *RepairWorker {
	if cfg.Interval == 0 {
		cfg.Interval = 30 * time.Second
	}
	return &RepairWorker{
		meta:       cfg.Meta,
		nodeID:     cfg.NodeID,
		stopCh:     make(chan struct{}),
		interval:   cfg.Interval,
		replicator: cfg.Replicator,
		localAddr:  cfg.LocalAddr,
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

			// Remove stale failed tasks (retried too many times)
			if task.Priority > 5 {
				log.Printf("repair: removing stale task for chunk %d (priority=%d)", task.ChunkID, task.Priority)
				if removeErr := rw.meta.RemoveRepairTask(ctx, task.ChunkID); removeErr != nil {
					log.Printf("repair: failed to remove stale task %d: %v", task.ChunkID, removeErr)
				}
			}
		} else {
			rw.mu.Lock()
			rw.repaired++
			rw.mu.Unlock()

			// Remove completed repair task from queue
			if removeErr := rw.meta.RemoveRepairTask(ctx, task.ChunkID); removeErr != nil {
				log.Printf("repair: failed to remove completed task %d: %v", task.ChunkID, removeErr)
			}
		}
	}
}

func (rw *RepairWorker) repairChunk(ctx context.Context, task metadata.RepairTask) error {
	// Get chunk metadata
	chunk, err := rw.meta.GetChunk(ctx, task.ChunkID)
	if err != nil {
		return fmt.Errorf("repair: get chunk %d: %w", task.ChunkID, err)
	}

	// Determine repair type
	switch {
	case len(chunk.Replicas) == 0:
		return fmt.Errorf("repair: chunk %d has no replicas at all", task.ChunkID)

	case rw.needsNewReplica(chunk):
		return rw.repairByAddingReplica(ctx, chunk)

	case rw.localReplicaCorrupt(chunk):
		return rw.repairByRefetchLocal(ctx, chunk)

	default:
		// Chunk is healthy — remove stale repair task
		log.Printf("repair: chunk %d is healthy, removing stale task", task.ChunkID)
		return nil
	}
}

// needsNewReplica checks if a chunk has fewer ready replicas than needed.
func (rw *RepairWorker) needsNewReplica(chunk *metadata.ChunkMeta) bool {
	readyCount := 0
	for _, r := range chunk.Replicas {
		if r.State == metadata.ReplicaReady {
			readyCount++
		}
	}
	return readyCount < len(chunk.Replicas)
}

// localReplicaCorrupt checks if the local node has a corrupt replica.
func (rw *RepairWorker) localReplicaCorrupt(chunk *metadata.ChunkMeta) bool {
	for _, r := range chunk.Replicas {
		if r.NodeID == rw.nodeID && (r.State == metadata.ReplicaFailed || r.State == metadata.ReplicaStale) {
			return true
		}
	}
	return false
}

// repairByAddingReplica finds a new target node and copies data from a healthy source.
func (rw *RepairWorker) repairByAddingReplica(ctx context.Context, chunk *metadata.ChunkMeta) error {
	sourceReplica := rw.findHealthySource(chunk)
	if sourceReplica == nil {
		return fmt.Errorf("repair: chunk %d has no healthy source replica", chunk.ID)
	}

	targetNode, err := rw.findTargetNode(ctx, chunk)
	if err != nil {
		return fmt.Errorf("repair: chunk %d: %w", chunk.ID, err)
	}

	log.Printf("repair: copying chunk %d from node %d (%s) to node %d (%s)",
		chunk.ID, sourceReplica.NodeID, sourceReplica.Addr, targetNode.ID, targetNode.Addr)

	// 1. Read chunk from source via TCP
	srcClient := NewClient(sourceReplica.Addr)
	if err := srcClient.Connect(); err != nil {
		return fmt.Errorf("repair: connect to source %s: %w", sourceReplica.Addr, err)
	}
	defer srcClient.Close()

	resp, err := srcClient.ReadChunk(chunk.ID, 0, 0)
	if err != nil {
		return fmt.Errorf("repair: read from source %s: %w", sourceReplica.Addr, err)
	}
	if resp.Status != StatusOK {
		return fmt.Errorf("repair: source read failed: status=%d err=%s", resp.Status, resp.Error)
	}
	data := resp.Data

	// 2. Write chunk to target via TCP
	if rw.replicator != nil {
		// Use replicator for async copy
		task := ReplicationTask{
			ChunkID:    chunk.ID,
			SourceAddr: sourceReplica.Addr,
			TargetAddr: targetNode.Addr,
			CreatedAt:  time.Now(),
		}
		if err := rw.replicator.Submit(task); err != nil {
			return fmt.Errorf("repair: submit replication: %w", err)
		}
		// Wait briefly for replication to complete
		time.Sleep(500 * time.Millisecond)
	} else {
		// Sync copy directly
		tgtClient := NewClient(targetNode.Addr)
		if err := tgtClient.Connect(); err != nil {
			return fmt.Errorf("repair: connect to target %s: %w", targetNode.Addr, err)
		}
		defer tgtClient.Close()

		replResp, err := tgtClient.ReplicateChunk(chunk.ID, data)
		if err != nil {
			return fmt.Errorf("repair: write to target %s: %w", targetNode.Addr, err)
		}
		if replResp.Status != StatusOK {
			return fmt.Errorf("repair: target write failed: status=%d err=%s", replResp.Status, replResp.Error)
		}
	}

	// 3. Update metadata: add new replica and mark it syncing
	chunk.Replicas = append(chunk.Replicas, metadata.ReplicaInfo{
		NodeID: targetNode.ID,
		Addr:   targetNode.Addr,
		State:  metadata.ReplicaSyncing,
	})
	if err := rw.meta.UpdateChunk(ctx, chunk); err != nil {
		return fmt.Errorf("repair: update chunk metadata: %w", err)
	}

	// 4. Report chunk state on the new node
	states := map[metadata.ChunkID]metadata.ReplicaState{
		chunk.ID: metadata.ReplicaReady,
	}
	if err := rw.meta.ReportChunkState(ctx, targetNode.ID, states); err != nil {
		return fmt.Errorf("repair: report replica ready: %w", err)
	}

	return nil
}

// repairByRefetchLocal fetches chunk data from a healthy peer and overwrites local copy.
func (rw *RepairWorker) repairByRefetchLocal(ctx context.Context, chunk *metadata.ChunkMeta) error {
	sourceReplica := rw.findHealthySource(chunk)
	if sourceReplica == nil {
		return fmt.Errorf("repair: no healthy source for local repair of chunk %d", chunk.ID)
	}

	log.Printf("repair: refetching chunk %d from node %d (%s) for local repair",
		chunk.ID, sourceReplica.NodeID, sourceReplica.Addr)

	// Read from source
	client := NewClient(sourceReplica.Addr)
	if err := client.Connect(); err != nil {
		return fmt.Errorf("repair: connect to source: %w", err)
	}
	defer client.Close()

	resp, err := client.ReadChunk(chunk.ID, 0, 0)
	if err != nil {
		return fmt.Errorf("repair: read from source: %w", err)
	}
	if resp.Status != StatusOK {
		return fmt.Errorf("repair: source read failed: status=%d", resp.Status)
	}

	// Overwrite local chunk via replicator
	if rw.replicator != nil {
		err = rw.replicator.Repair(ChunkRepairTask{
			ChunkID:       chunk.ID,
			SurvivingAddr: sourceReplica.Addr,
			NewTargetAddr: rw.localAddr,
		})
		if err != nil {
			return fmt.Errorf("repair: local chunk rewrite: %w", err)
		}
	}

	log.Printf("repair: chunk %d local repair complete", chunk.ID)
	return nil
}

func (rw *RepairWorker) findHealthySource(chunk *metadata.ChunkMeta) *metadata.ReplicaInfo {
	for i := range chunk.Replicas {
		if chunk.Replicas[i].State == metadata.ReplicaReady {
			return &chunk.Replicas[i]
		}
	}
	return nil
}

func (rw *RepairWorker) findTargetNode(ctx context.Context, chunk *metadata.ChunkMeta) (*metadata.NodeInfo, error) {
	nodes, err := rw.meta.ListNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	existingNodes := make(map[metadata.NodeID]bool)
	for _, r := range chunk.Replicas {
		existingNodes[r.NodeID] = true
	}

	for i := range nodes {
		if nodes[i].State == metadata.NodeOnline && !existingNodes[nodes[i].ID] {
			return &nodes[i], nil
		}
	}
	return nil, fmt.Errorf("no available target node for new replica")
}

// RepairChunksForDiskFailure triggers repair tasks for all chunks on a failed disk.
// This is called by DiskManager when a disk transitions to Failed state.
// It pushes each chunk on the failed disk into the metadata repair queue for re-replication.
func (rw *RepairWorker) RepairChunksForDiskFailure(ctx context.Context, failedDiskDir string) error {
	// List all local chunks (they will be repaired to other nodes)
	// The caller (DiskManager) or OpsServer should provide the store reference.
	// This function signs the chunks up for repair via metadata.
	chunks, err := rw.meta.ChunksByNode(ctx, rw.nodeID)
	if err != nil {
		return fmt.Errorf("list chunks for repair: %w", err)
	}

	triggered := 0
	for _, chunk := range chunks {
		// Only trigger repair for chunks that have this node as a replica
		hasLocal := false
		for _, r := range chunk.Replicas {
			if r.NodeID == rw.nodeID {
				hasLocal = true
				break
			}
		}
		if !hasLocal {
			continue
		}
		if err := rw.meta.TriggerRepair(ctx, chunk.ID); err != nil {
			log.Printf("repair: failed to trigger repair for chunk %d after disk failure: %v", chunk.ID, err)
			continue
		}
		triggered++
	}
	log.Printf("repair: triggered %d chunk repairs for failed disk %s", triggered, failedDiskDir)
	return nil
}
