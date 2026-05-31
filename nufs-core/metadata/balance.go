package metadata

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// ========== Rebalance Engine ==========

// RebalancePlanner generates chunk migration plans for cluster rebalancing.
type RebalancePlanner struct {
	mu sync.Mutex
}

// MigrationPlan describes a chunk movement from source to target node.
type MigrationPlan struct {
	ChunkID    ChunkID `json:"chunk_id"`
	SourceNode NodeID  `json:"source_node"`
	TargetNode NodeID  `json:"target_node"`
	Reason     string  `json:"reason"`
	Priority   int     `json:"priority"`
}

// RebalanceResult holds the output of a rebalance planning run.
type RebalanceResult struct {
	Plans     []MigrationPlan `json:"plans"`
	Timestamp time.Time       `json:"timestamp"`
	Balanced  bool            `json:"balanced"`
	Imbalance float64         `json:"imbalance"`
}

// NodeLoad represents the load metrics for a node.
type NodeLoad struct {
	NodeID     NodeID    `json:"node_id"`
	ChunkCount int64     `json:"chunk_count"`
	UsedGB     int64     `json:"used_gb"`
	CapacityGB int64     `json:"capacity_gb"`
	UsageRatio float64   `json:"usage_ratio"`
	State      NodeState `json:"state"`
}

// PlanRebalance analyzes the cluster and generates migration plans.
// Note: this method produces plans with ChunkID=0 because it only has
// node-level aggregate stats. Use PlanRebalanceWithChunks for plans
// with concrete chunk IDs.
func (rp *RebalancePlanner) PlanRebalance(nodes []NodeInfo, threshold float64) *RebalanceResult {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if threshold <= 0 {
		threshold = 0.1
	}

	loads := make([]NodeLoad, 0, len(nodes))
	var totalChunks int64

	for _, n := range nodes {
		if n.State != NodeOnline {
			continue
		}
		usageRatio := 0.0
		if n.CapacityGB > 0 {
			usageRatio = float64(n.UsedGB) / float64(n.CapacityGB)
		}
		loads = append(loads, NodeLoad{
			NodeID:     n.ID,
			ChunkCount: n.ChunkCount,
			UsedGB:     n.UsedGB,
			CapacityGB: n.CapacityGB,
			UsageRatio: usageRatio,
			State:      n.State,
		})
		totalChunks += n.ChunkCount
	}

	if len(loads) < 2 {
		return &RebalanceResult{Timestamp: time.Now(), Balanced: true, Imbalance: 0}
	}

	avgUsage := float64(totalChunks) / float64(len(loads))
	var variance float64
	for _, l := range loads {
		diff := float64(l.ChunkCount) - avgUsage
		variance += diff * diff
	}
	variance /= float64(len(loads))
	stdDev := math.Sqrt(variance)
	imbalance := 0.0
	if avgUsage > 0 {
		imbalance = stdDev / avgUsage
	}

	result := &RebalanceResult{
		Timestamp: time.Now(),
		Imbalance: imbalance,
		Balanced:  imbalance <= threshold,
	}
	if result.Balanced {
		return result
	}

	sort.Slice(loads, func(i, j int) bool {
		return loads[i].ChunkCount > loads[j].ChunkCount
	})

	avgPerNode := totalChunks / int64(len(loads))
	maxMigrations := int(totalChunks / 10)
	if maxMigrations < 1 {
		maxMigrations = 1
	}

	overloaded := make([]NodeLoad, 0)
	underloaded := make([]NodeLoad, 0)
	for _, l := range loads {
		if l.ChunkCount > avgPerNode*110/100 {
			overloaded = append(overloaded, l)
		} else if l.ChunkCount < avgPerNode*90/100 {
			underloaded = append(underloaded, l)
		}
	}

	plans := make([]MigrationPlan, 0)
	underIdx := 0
	for _, over := range overloaded {
		excess := over.ChunkCount - avgPerNode
		for excess > 0 && underIdx < len(underloaded) && len(plans) < maxMigrations {
			target := &underloaded[underIdx]
			space := avgPerNode - target.ChunkCount
			if space <= 0 {
				underIdx++
				continue
			}
			move := excess
			if move > space {
				move = space
			}
			remaining := int64(maxMigrations - len(plans))
			if move > remaining {
				move = remaining
			}
			if move <= 0 {
				break
			}
			for i := int64(0); i < move; i++ {
				plans = append(plans, MigrationPlan{
					ChunkID:    ChunkID(0), // Caller must fill via PlanRebalanceWithChunks
					SourceNode: over.NodeID,
					TargetNode: target.NodeID,
					Reason:     fmt.Sprintf("rebalance: node %d overloaded (%d chunks)", over.NodeID, over.ChunkCount),
					Priority:   1,
				})
			}
			excess -= move
			target.ChunkCount += move
			over.ChunkCount -= move
		}
	}

	result.Plans = plans
	return result
}

// PlanRebalanceWithChunks generates migration plans with concrete chunk IDs.
// Requires a map of nodeID -> []ChunkID for the source nodes.
func (rp *RebalancePlanner) PlanRebalanceWithChunks(
	nodes []NodeInfo,
	nodeChunks map[NodeID][]ChunkID,
	threshold float64,
) *RebalanceResult {
	result := rp.PlanRebalance(nodes, threshold)
	if result.Balanced || len(result.Plans) == 0 {
		return result
	}

	// Assign real chunk IDs to plans
	for i := range result.Plans {
		plan := &result.Plans[i]
		chunks, ok := nodeChunks[plan.SourceNode]
		if !ok || len(chunks) == 0 {
			plan.ChunkID = ChunkID(0)
			plan.Reason = plan.Reason + "; no chunks assigned (node empty)"
			continue
		}
		// Round-robin assign chunk IDs from source node
		plan.ChunkID = chunks[i%len(chunks)]
	}

	return result
}

// PlanDecommission generates migration plans to move all chunks off a node.
// Note: produced plans have ChunkID=0. A separate phase should fill them from
// the actual chunk listing.
func (rp *RebalancePlanner) PlanDecommission(nodeID NodeID, nodes []NodeInfo) ([]MigrationPlan, error) {
	var source *NodeInfo
	for i := range nodes {
		if nodes[i].ID == nodeID {
			source = &nodes[i]
		}
	}
	if source == nil {
		return nil, fmt.Errorf("node %d not found", nodeID)
	}

	var candidates []NodeInfo
	for i := range nodes {
		if nodes[i].ID != nodeID && nodes[i].State == NodeOnline {
			candidates = append(candidates, nodes[i])
		}
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available target nodes")
	}

	sort.Slice(candidates, func(i, j int) bool {
		freeI := candidates[i].CapacityGB - candidates[i].UsedGB
		freeJ := candidates[j].CapacityGB - candidates[j].UsedGB
		return freeI > freeJ
	})

	// Distribute chunks across all available nodes, not just the most free
	plans := make([]MigrationPlan, 0)
	targetIdx := 0
	for i := int64(0); i < source.ChunkCount; i++ {
		target := &candidates[targetIdx%len(candidates)]
		plans = append(plans, MigrationPlan{
			ChunkID:    ChunkID(0), // Caller must fill with actual chunk IDs
			SourceNode: nodeID,
			TargetNode: target.ID,
			Reason:     fmt.Sprintf("decommission node %d", nodeID),
			Priority:   3,
		})
		targetIdx++
	}

	return plans, nil
}
