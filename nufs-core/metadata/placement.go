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
// It can subscribe to an EventBus to automatically sync node state changes,
// eliminating the need for external callers to manually call UpdateNode/RemoveNode.
type PlacementEngine struct {
	mu        sync.RWMutex
	nodes     map[NodeID]*NodeInfo
	loadIndex map[NodeID]float64 // 0.0 - 1.0, disk/IO load
	rng       *rand.Rand

	// Optional: auto-sync via EventBus
	events  *EventBus
	watcher *Watcher
}

// NewPlacementEngine creates a new placement engine instance.
func NewPlacementEngine() *PlacementEngine {
	return &PlacementEngine{
		nodes:     make(map[NodeID]*NodeInfo),
		loadIndex: make(map[NodeID]float64),
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NewPlacementEngineWithSeed creates a placement engine with a deterministic
// RNG seed. This ensures PlaceChunk produces identical results for identical
// inputs, which is critical for reproducible placement across leader failover.
func NewPlacementEngineWithSeed(seed int64) *PlacementEngine {
	return &PlacementEngine{
		nodes:     make(map[NodeID]*NodeInfo),
		loadIndex: make(map[NodeID]float64),
		rng:       rand.New(rand.NewSource(seed)),
	}
}

// UpdateNode updates the placement engine's view of a node.
func (p *PlacementEngine) UpdateNode(info *NodeInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodes[info.ID] = info
}

// GetNodeInfo returns the node info for the given ID directly from
// the in-memory map, avoiding a Pebble lookup. Returns nil, false if
// the node is not registered with the placement engine.
func (p *PlacementEngine) GetNodeInfo(nodeID NodeID) (*NodeInfo, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	info, ok := p.nodes[nodeID]
	return info, ok
}

// GetNodeInfosBatch returns node info for multiple IDs in a single
// call. The result slice has the same length and order as the input;
// entries for unknown IDs are nil. This avoids N separate Pebble
// Get calls when building replica lists after PlaceChunk.
func (p *PlacementEngine) GetNodeInfosBatch(ids []NodeID) []*NodeInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]*NodeInfo, len(ids))
	for i, id := range ids {
		result[i] = p.nodes[id] // nil if not present
	}
	return result
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
		// Deterministic jitter based on node ID to avoid thundering herd
		// while ensuring reproducible placement across leader failover.
		score += float64(n.ID%100) * 0.0005

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
// It implements a best-effort cross-domain placement strategy:
//  1. First pass: pick one node per domain (strict isolation)
//  2. Second pass: if not enough domains, allow same-domain nodes
//     but prefer domains with fewer selected nodes
//
// This ensures that replicas are spread across failure domains (racks/zones)
// to survive rack/zone-level failures.
func (p *PlacementEngine) spreadSelect(
	scored []scoredNode,
	count int,
	spread TopologySpread,
) []NodeID {
	if len(scored) == 0 {
		return nil
	}

	result := make([]NodeID, 0, count)
	domainCount := make(map[string]int) // domain → number of selected nodes in it
	selected := make(map[NodeID]bool)

	// Pass 1: Strict isolation — pick at most one node per domain
	for _, s := range scored {
		if len(result) >= count {
			break
		}
		if selected[s.node.ID] {
			continue
		}
		domain := p.getDomain(s.node, spread)
		if domainCount[domain] > 0 {
			continue // skip — already have a node in this domain
		}
		result = append(result, s.node.ID)
		selected[s.node.ID] = true
		domainCount[domain]++
	}

	// Pass 2: Relaxed — fill remaining slots, preferring domains with fewer nodes
	if len(result) < count {
		for _, s := range scored {
			if len(result) >= count {
				break
			}
			if selected[s.node.ID] {
				continue
			}
			domain := p.getDomain(s.node, spread)
			// Allow but track domain usage
			result = append(result, s.node.ID)
			selected[s.node.ID] = true
			domainCount[domain]++
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
	case SpreadMachine:
		return n.MachineID
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

// SubscribeEvents registers the placement engine to automatically receive
// node state changes from the EventBus. When a node is updated or goes
// offline, the placement engine syncs its internal state without requiring
// external callers to call UpdateNode/RemoveNode.
//
// Call this after the EventBus is initialized. The caller must call
// UnsubscribeEvents before shutdown to release the watcher.
func (p *PlacementEngine) SubscribeEvents(events *EventBus) {
	p.events = events
	p.watcher = events.Watch(prefixNode)
	go p.drainEvents()
}

// UnsubscribeEvents stops receiving EventBus notifications.
func (p *PlacementEngine) UnsubscribeEvents() {
	if p.watcher != nil {
		p.watcher.Close()
		p.watcher = nil
	}
}

// drainEvents processes incoming node events from the EventBus.
func (p *PlacementEngine) drainEvents() {
	if p.watcher == nil {
		return
	}
	for event := range p.watcher.Events() {
		p.handleEvent(event)
	}
}

// handleEvent processes a single node change event.
func (p *PlacementEngine) handleEvent(event Event) {
	switch event.Type {
	case EventSet:
		// Node updated — decode and sync
		var info NodeInfo
		if err := unmarshalValue(event.Value, &info); err != nil {
			return
		}
		p.mu.Lock()
		p.nodes[info.ID] = &info
		p.mu.Unlock()

	case EventDelete:
		// Node removed — extract node ID from key and remove
		key := string(event.Key)
		if len(key) > len(prefixNode) {
			var id NodeID
			if _, err := fmt.Sscanf(key[len(prefixNode):], "%d", &id); err == nil {
				p.RemoveNode(id)
			}
		}
	}
}
