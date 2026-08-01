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
	mu         sync.RWMutex
	nodes      map[NodeID]*NodeInfo
	loadIndex  map[NodeID]float64 // 0.0 - 1.0, disk/IO load
	errorRates map[NodeID]float64 // 0.0 - 1.0, rolling write error rate
	rng        *rand.Rand

	// Dynamic config provider — called on each PlaceChunk for live thresholds.
	cfgFn func() *DynamicConfig

	// Optional: auto-sync via EventBus
	events  *EventBus
	watcher *Watcher
}

// NewPlacementEngine creates a new placement engine instance.
func NewPlacementEngine() *PlacementEngine {
	return &PlacementEngine{
		nodes:      make(map[NodeID]*NodeInfo),
		loadIndex:  make(map[NodeID]float64),
		errorRates: make(map[NodeID]float64),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NewPlacementEngineWithSeed creates a placement engine with a deterministic
// RNG seed. This ensures PlaceChunk produces identical results for identical
// inputs, which is critical for reproducible placement across leader failover.
func NewPlacementEngineWithSeed(seed int64) *PlacementEngine {
	return &PlacementEngine{
		nodes:      make(map[NodeID]*NodeInfo),
		loadIndex:  make(map[NodeID]float64),
		errorRates: make(map[NodeID]float64),
		rng:        rand.New(rand.NewSource(seed)),
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

// UpdateErrorRate updates the write error rate for a node.
// rate is 0.0 (no errors) to 1.0 (all writes fail).
func (p *PlacementEngine) UpdateErrorRate(nodeID NodeID, rate float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errorRates[nodeID] = rate
}

// SetConfigProvider sets a function that returns the current DynamicConfig.
// Called on each PlaceChunk to read live thresholds.
func (p *PlacementEngine) SetConfigProvider(fn func() *DynamicConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cfgFn = fn
}

// RemoveNode removes a node from the placement engine.
func (p *PlacementEngine) RemoveNode(nodeID NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.nodes, nodeID)
	delete(p.loadIndex, nodeID)
	delete(p.errorRates, nodeID)
}

// NodeMetrics holds per-node runtime metrics tracked by the placement engine.
type NodeMetrics struct {
	NodeID      NodeID
	ErrorRate   float64 // 0.0 - 1.0
	LoadIndex   float64 // 0.0 - 1.0
	CapacityGB  int64
	UsedGB      int64
	ChunkCount  int64
}

// GetNodeMetrics returns runtime metrics for all known nodes.
func (p *PlacementEngine) GetNodeMetrics() []NodeMetrics {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]NodeMetrics, 0, len(p.nodes))
	for id, n := range p.nodes {
		result = append(result, NodeMetrics{
			NodeID:     id,
			ErrorRate:  p.errorRates[id],
			LoadIndex:  p.loadIndex[id],
			CapacityGB: n.CapacityGB,
			UsedGB:     n.UsedGB,
			ChunkCount: n.ChunkCount,
		})
	}
	return result
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

	// Read live config thresholds.
	cfg := p.currentConfig()
	errFilter := cfg.PlacementErrorRateFilter
	wCapacity := cfg.PlacementWeightCapacity
	wLoad := cfg.PlacementWeightLoad
	wTier := cfg.PlacementWeightTier
	wHealth := cfg.PlacementWeightHealth

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
		// Skip nodes with write error rate above configurable threshold.
		if p.errorRates[n.ID] > errFilter {
			continue
		}
		// Tier: soft preference, not hard filter. All nodes are candidates;
		// tier match is scored as a gradient so the placement engine prefers
		// the requested tier but can degrade to adjacent tiers when the
		// preferred tier is full.
		candidates = append(candidates, n)
	}

	// For EC or high-RF where RF > node count, allow multiple shards
	// per node (up to maxPerNode). Each shard lands on a different disk
	// via the datanode's PickDisk (least-used selection).
	// Only enabled for EC configs (DataShards > 0); replication keeps
	// one-shard-per-node for maximum fault isolation.
	maxPerNode := 1
	if policy.ECConfig != nil && policy.ECConfig.DataShards > 0 && len(candidates) < policy.ReplicationFactor {
		maxPerNode = (policy.ReplicationFactor + len(candidates) - 1) / len(candidates)
	}
	if len(candidates)*maxPerNode < policy.ReplicationFactor {
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

		// Tier scoring: gradient match instead of hard filter.
		// Exact match = 1.0, adjacent tier = 0.7, two tiers = 0.4, far = 0.1.
		// TierAny = 1.0 for all (no preference).
		var tierMatch float64
		if policy.StorageTier == StorageTierAny {
			tierMatch = 1.0
		} else {
			diff := int(n.Tier) - int(policy.StorageTier)
			if diff < 0 {
				diff = -diff
			}
			switch diff {
			case 0:
				tierMatch = 1.0
			case 1:
				tierMatch = 0.7
			case 2:
				tierMatch = 0.4
			default:
				tierMatch = 0.1
			}
		}

		healthScore := 1.0 - p.errorRates[n.ID]

		score := freeCapacity*wCapacity + lowLoad*wLoad + tierMatch*wTier + healthScore*wHealth
		// Deterministic jitter based on node ID to avoid thundering herd
		// while ensuring reproducible placement across leader failover.
		score += float64(n.ID%100) * 0.0005

		scored = append(scored, scoredNode{node: n, score: score})
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	// 3. Topology-aware selection
	selected := p.spreadSelect(scored, policy.ReplicationFactor, policy.TopologySpread, maxPerNode)

	if len(selected) < policy.ReplicationFactor {
		return nil, ErrPlacementFailed
	}

	return selected, nil
}

// spreadSelect picks nodes respecting topology spread constraints.
// 1. Strict isolation: one node per domain
// 2. Relaxed: allow same-domain nodes
// 3. Multi-shard: allow same node up to maxPerNode (EC with few nodes)
func (p *PlacementEngine) spreadSelect(
	scored []scoredNode,
	count int,
	spread TopologySpread,
	maxPerNode int,
) []NodeID {
	if len(scored) == 0 {
		return nil
	}

	result := make([]NodeID, 0, count)
	domainCount := make(map[string]int)
	nodeCount := make(map[NodeID]int)

	// Pass 1: Strict isolation - pick at most one node per domain
	for _, s := range scored {
		if len(result) >= count {
			break
		}
		if nodeCount[s.node.ID] >= 1 {
			continue
		}
		domain := p.getDomain(s.node, spread)
		if domainCount[domain] > 0 {
			continue
		}
		result = append(result, s.node.ID)
		nodeCount[s.node.ID]++
		domainCount[domain]++
	}

	// Pass 2: Relaxed - fill remaining slots
	if len(result) < count {
		for _, s := range scored {
			if len(result) >= count {
				break
			}
			if nodeCount[s.node.ID] >= 1 {
				continue
			}
			result = append(result, s.node.ID)
			nodeCount[s.node.ID]++
			domain := p.getDomain(s.node, spread)
			domainCount[domain]++
		}
	}

	// Pass 3: Multi-shard per node (EC with fewer nodes than K+M)
	if len(result) < count && maxPerNode > 1 {
		for _, s := range scored {
			if len(result) >= count {
				break
			}
			if nodeCount[s.node.ID] >= maxPerNode {
				continue
			}
			result = append(result, s.node.ID)
			nodeCount[s.node.ID]++
		}
	}

	return result
}

// currentConfig returns the current DynamicConfig, or defaults if no provider is set.
// Must be called with p.mu held (at least RLock).
func (p *PlacementEngine) currentConfig() DynamicConfig {
	if p.cfgFn != nil {
		if cfg := p.cfgFn(); cfg != nil {
			return *cfg
		}
	}
	return DefaultDynamicConfig()
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
