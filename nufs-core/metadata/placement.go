package metadata

import (
	"math/rand"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	// MaxChunkSize is the maximum chunk size (64MB).
	MaxChunkSize = 64 * 1024 * 1024
	// MaxNameLength is the maximum length for a file or directory name.
	MaxNameLength = 255
	// NodeHeartbeatTTL is the lease TTL for node registration.
	NodeHeartbeatTTL = 30 // seconds
	// RootInodeID is the inode ID of the root directory.
	RootInodeID InodeID = 1
)

// scoredNode pairs a node with its placement score.
type scoredNode struct {
	node  *NodeInfo
	score float64
}

// PlacementEngine selects optimal nodes for chunk placement based on policy.
type PlacementEngine struct {
	mu        sync.RWMutex
	nodes     map[NodeID]*NodeInfo
	loadIndex map[NodeID]float64 // 0.0 - 1.0, disk/IO load
	rng       *rand.Rand
}

// NewPlacementEngine creates a new placement engine instance.
func NewPlacementEngine() *PlacementEngine {
	return &PlacementEngine{
		nodes:     make(map[NodeID]*NodeInfo),
		loadIndex: make(map[NodeID]float64),
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// UpdateNode updates the placement engine's view of a node.
func (p *PlacementEngine) UpdateNode(info *NodeInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodes[info.ID] = info
}

// UpdateLoad updates the load index for a node.
func (p *PlacementEngine) UpdateLoad(nodeID NodeID, load float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.loadIndex[nodeID] = load
}

// RemoveNode removes a node from the placement engine.
func (p *PlacementEngine) RemoveNode(nodeID NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.nodes, nodeID)
	delete(p.loadIndex, nodeID)
}

// PlaceChunk selects optimal nodes for a new chunk based on the placement policy.
//
// Strategy:
//  1. Filter: remove offline, full, and draining nodes
//  2. Score: weight by (free_capacity * 0.4 + low_load * 0.3 + tier_match * 0.3)
//  3. Spread: enforce topology constraint (no 2 replicas in same rack/zone)
//  4. Return: ordered list [primary, secondary, tertiary]
func (p *PlacementEngine) PlaceChunk(
	policy PlacementPolicy,
	excludeNodes map[NodeID]bool,
) ([]NodeID, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// 1. Filter candidates
	var candidates []*NodeInfo
	for _, n := range p.nodes {
		if excludeNodes != nil && excludeNodes[n.ID] {
			continue
		}
		if n.State != NodeOnline {
			continue
		}
		if n.CapacityGB > 0 && n.UsedGB >= n.CapacityGB*95/100 {
			continue
		}
		// Tier filter: skip when policy tier is unset (zero value)
		if policy.StorageTier != StorageTierAny && n.Tier != policy.StorageTier {
			continue
		}
		candidates = append(candidates, n)
	}

	if len(candidates) < policy.ReplicationFactor {
		return nil, ErrInsufficientNodes
	}

	// 2. Score candidates
	scored := make([]scoredNode, 0, len(candidates))
	for _, n := range candidates {
		var freeCapacity float64
		if n.CapacityGB > 0 {
			freeCapacity = float64(n.CapacityGB-n.UsedGB) / float64(n.CapacityGB)
		}
		load := p.loadIndex[n.ID]
		lowLoad := 1.0 - load

		tierMatch := 0.3
		if policy.StorageTier == StorageTierAny || n.Tier == policy.StorageTier {
			tierMatch = 1.0
		}

		score := freeCapacity*0.4 + lowLoad*0.3 + tierMatch*0.3
		// Add jitter to avoid thundering herd
		score += p.rng.Float64() * 0.05

		scored = append(scored, scoredNode{node: n, score: score})
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	// 3. Topology-aware selection
	selected := p.spreadSelect(scored, policy.ReplicationFactor, policy.TopologySpread)

	if len(selected) < policy.ReplicationFactor {
		return nil, ErrPlacementFailed
	}

	return selected, nil
}

// spreadSelect picks nodes respecting topology spread constraints.
func (p *PlacementEngine) spreadSelect(
	scored []scoredNode,
	count int,
	spread TopologySpread,
) []NodeID {
	result := make([]NodeID, 0, count)
	usedDomains := make(map[string]bool)

	for _, s := range scored {
		if len(result) >= count {
			break
		}

		domain := p.getDomain(s.node, spread)
		if usedDomains[domain] {
			continue
		}

		result = append(result, s.node.ID)
		usedDomains[domain] = true
	}

	// If topology constraint cannot be fully satisfied, relax and fill remaining
	if len(result) < count {
		for _, s := range scored {
			if len(result) >= count {
				break
			}
			already := false
			for _, r := range result {
				if r == s.node.ID {
					already = true
					break
				}
			}
			if !already {
				result = append(result, s.node.ID)
			}
		}
	}

	return result
}

// getDomain returns the topology domain key for a node based on spread level.
func (p *PlacementEngine) getDomain(n *NodeInfo, spread TopologySpread) string {
	switch spread {
	case SpreadZone:
		return n.Zone
	case SpreadRack:
		return n.Rack
	default:
		// SpreadNode: each node is its own domain
		return fmt.Sprintf("node-%d", n.ID)
	}
}

// ========== Snowflake Chunk ID Generator ==========

// ChunkIDGenerator generates Snowflake-style 64-bit chunk IDs.
// Layout: [41-bit ms timestamp | 10-bit node | 13-bit sequence]
type ChunkIDGenerator struct {
	mu       sync.Mutex
	nodeID   uint64
	sequence uint64
	lastTS   int64
}

// NewChunkIDGenerator creates a new chunk ID generator for the given node.
func NewChunkIDGenerator(nodeID uint64) *ChunkIDGenerator {
	return &ChunkIDGenerator{
		nodeID: nodeID & 0x3FF, // 10-bit mask
	}
}

// Next generates the next chunk ID.
func (g *ChunkIDGenerator) Next() ChunkID {
	g.mu.Lock()
	defer g.mu.Unlock()

	ts := time.Now().UnixMilli()
	if ts == g.lastTS {
		g.sequence++
	} else {
		g.sequence = 0
		g.lastTS = ts
	}

	id := uint64(ts&0x1FFFFFFFFFF)<<23 |
		g.nodeID<<13 |
		(g.sequence & 0x1FFF)

	return ChunkID(id)
}
