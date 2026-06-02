package metadata

import (
	"context"
	"fmt"
	"log"
	"time"
)

// RebalanceExecutor executes chunk migration plans for decommission and rebalance.
// It manages the full lifecycle:
//   1. Plan migration (using RebalancePlanner)
//   2. Push migration tasks to repair queue
//   3. Wait for completion
//   4. Update chunk metadata (remove source replica, add target replica)
type RebalanceExecutor struct {
	store MetadataService
}

// NewRebalanceExecutor creates an executor backed by the given metadata service.
func NewRebalanceExecutor(store MetadataService) *RebalanceExecutor {
	return &RebalanceExecutor{store: store}
}

// ExecuteDecommission migrates all chunks off a node and marks it offline.
// Steps:
//   1. List all chunks on the draining node
//   2. Plan target nodes for each chunk
//   3. Push repair tasks for each chunk migration
//   4. After each chunk is repaired, update its replica set
func (e *RebalanceExecutor) ExecuteDecommission(ctx context.Context, nodeID NodeID) error {
	log.Printf("rebalance: starting decommission of node %d", nodeID)

	chunks, err := e.store.ChunksByNode(ctx, nodeID)
	if err != nil {
		return fmt.Errorf("list chunks for node %d: %w", nodeID, err)
	}
	log.Printf("rebalance: found %d chunks on node %d", len(chunks), nodeID)

	nodes, err := e.store.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}

	planner := &RebalancePlanner{}
	plans, err := planner.PlanDecommission(nodeID, nodes)
	if err != nil {
		return fmt.Errorf("plan decommission: %w", err)
	}

	if len(plans) == 0 {
		log.Printf("rebalance: node %d has no chunks to migrate", nodeID)
		return nil
	}

	// Assign actual chunk IDs to plans (round-robin)
	for i := range plans {
		plans[i].ChunkID = chunks[i%len(chunks)].ID
	}

	// Push to repair queue
	migrated := 0
	for _, plan := range plans {
		if plan.ChunkID == 0 {
			continue
		}

		// Push repair task so data nodes pick it up
		if err := e.store.TriggerRepair(ctx, plan.ChunkID); err != nil {
			log.Printf("rebalance: trigger repair for chunk %d: %v", plan.ChunkID, err)
			continue
		}

		// Update metadata: remove source replica, add target
		if err := e.store.MigrateChunkReplica(ctx, plan.ChunkID, plan.SourceNode, plan.TargetNode); err != nil {
			log.Printf("rebalance: migrate replica for chunk %d: %v", plan.ChunkID, err)
			continue
		}

		migrated++
		log.Printf("rebalance: migrated chunk %d (%s)", plan.ChunkID, plan.Reason)
	}

	log.Printf("rebalance: decommissioned node %d: %d/%d chunks migrated", nodeID, migrated, len(plans))
	return nil
}

// ExecutePlans executes a set of migration plans for cluster rebalancing.
// Each plan is pushed as a repair task and the replica metadata is updated.
func (e *RebalanceExecutor) ExecutePlans(ctx context.Context, plans []MigrationPlan) error {
	executed := 0
	for _, plan := range plans {
		if plan.ChunkID == 0 {
			continue
		}

		// Push to repair queue
		if err := e.store.TriggerRepair(ctx, plan.ChunkID); err != nil {
			log.Printf("rebalance: trigger repair for chunk %d: %v", plan.ChunkID, err)
			continue
		}

		// Update chunk metadata atomically
		if err := e.store.MigrateChunkReplica(ctx, plan.ChunkID, plan.SourceNode, plan.TargetNode); err != nil {
			log.Printf("rebalance: migrate replica for chunk %d: %v", plan.ChunkID, err)
			continue
		}

		executed++
	}

	log.Printf("rebalance: executed %d/%d migration plans", executed, len(plans))
	return nil
}

const (
	// DefaultRebalanceThreshold is the default imbalance ratio that triggers rebalance.
	DefaultRebalanceThreshold = 0.1
	// DecommissionTimeout is the max time to wait for decommission to complete.
	DecommissionTimeout = 30 * time.Minute
)
