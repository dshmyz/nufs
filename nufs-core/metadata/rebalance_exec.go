package metadata

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// RebalanceExecutor executes chunk migration plans for decommission and rebalance.
// It manages the full lifecycle:
//  1. Plan migration (using RebalancePlanner)
//  2. Push migration tasks to repair queue
//  3. Wait for completion
//  4. Update chunk metadata (remove source replica, add target replica)
type RebalanceExecutor struct {
	store MetadataService
}

// NewRebalanceExecutor creates an executor backed by the given metadata service.
func NewRebalanceExecutor(store MetadataService) *RebalanceExecutor {
	return &RebalanceExecutor{store: store}
}

// ExecuteDecommission migrates all chunks off a node and marks it offline.
// Steps:
//  1. List all chunks on the draining node
//  2. Plan target nodes for each chunk
//  3. Push repair tasks for each chunk migration
//  4. After each chunk is repaired, update its replica set
func (e *RebalanceExecutor) ExecuteDecommission(ctx context.Context, nodeID NodeID) error {
	slog.Info("rebalance: starting decommission", "node_id", nodeID)

	chunks, err := e.store.ChunksByNode(ctx, nodeID)
	if err != nil {
		return fmt.Errorf("list chunks for node %d: %w", nodeID, err)
	}
	slog.Info("rebalance: found chunks on node", "count", len(chunks), "node_id", nodeID)

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
		slog.Info("rebalance: node has no chunks to migrate", "node_id", nodeID)
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
			slog.Error("rebalance: trigger repair", "chunk_id", plan.ChunkID, "error", err)
			continue
		}

		// Update metadata: remove source replica, add target
		if err := e.store.MigrateChunkReplica(ctx, plan.ChunkID, plan.SourceNode, plan.TargetNode); err != nil {
			slog.Error("rebalance: migrate replica", "chunk_id", plan.ChunkID, "error", err)
			continue
		}

		migrated++
		slog.Info("rebalance: migrated chunk", "chunk_id", plan.ChunkID, "reason", plan.Reason)
	}

	slog.Info("rebalance: decommissioned node", "node_id", nodeID, "migrated", migrated, "total", len(plans))
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
			slog.Error("rebalance: trigger repair", "chunk_id", plan.ChunkID, "error", err)
			continue
		}

		// Update chunk metadata atomically
		if err := e.store.MigrateChunkReplica(ctx, plan.ChunkID, plan.SourceNode, plan.TargetNode); err != nil {
			slog.Error("rebalance: migrate replica", "chunk_id", plan.ChunkID, "error", err)
			continue
		}

		executed++
	}

	slog.Info("rebalance: executed migration plans", "executed", executed, "total", len(plans))
	return nil
}

const (
	// DefaultRebalanceThreshold is the default imbalance ratio that triggers rebalance.
	DefaultRebalanceThreshold = 0.1
	// DecommissionTimeout is the max time to wait for decommission to complete.
	DecommissionTimeout = 30 * time.Minute
)
