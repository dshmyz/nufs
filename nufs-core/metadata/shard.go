package metadata

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// ============================================================
// ShardRouter — Consistent Hashing for Metadata Sharding
// ============================================================
//
// Distributes metadata across multiple PebbleStore instances
// using consistent hashing with virtual nodes.
//
// Key mapping:
//   hash(bucket + "/" + key) → virtual node → physical shard
//
// Each physical shard is a PebbleStore with its own Raft group.
// When a shard is added/removed, only ~1/N of keys migrate.

// ShardID identifies a metadata shard (0-65535).
type ShardID uint16

// ShardInfo describes a metadata shard's configuration.
type ShardInfo struct {
	ID       ShardID
	Nodes    []string // Raft group members (addresses)
	Leader   string   // Current leader address
	KeyCount int64    // Approximate key count
}

// HashRing implements consistent hashing with virtual nodes.
type HashRing struct {
	mu           sync.RWMutex
	nodes        map[ShardID]*ringNode
	sortedHashes []uint32    // sorted hash values
	ring         []ringEntry // parallel to sortedHashes
	vnodeCount   int         // virtual nodes per physical shard
}

type ringNode struct {
	info ShardInfo
}

type ringEntry struct {
	hash    uint32
	shardID ShardID
}

// NewHashRing creates a consistent hash ring.
func NewHashRing(vnodeCount int) *HashRing {
	if vnodeCount <= 0 {
		vnodeCount = 150 // Default: 150 virtual nodes per shard
	}
	return &HashRing{
		nodes:      make(map[ShardID]*ringNode),
		vnodeCount: vnodeCount,
	}
}

// AddShard adds a shard to the ring.
func (hr *HashRing) AddShard(info ShardInfo) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	hr.nodes[info.ID] = &ringNode{info: info}
	hr.rebuild()
}

// RemoveShard removes a shard from the ring.
func (hr *HashRing) RemoveShard(id ShardID) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	delete(hr.nodes, id)
	hr.rebuild()
}

// Route determines which shard a key belongs to.
func (hr *HashRing) Route(key string) ShardID {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.sortedHashes) == 0 {
		return 0
	}

	h := hashKey(key)
	idx := sort.Search(len(hr.sortedHashes), func(i int) bool {
		return hr.sortedHashes[i] >= h
	})
	if idx >= len(hr.sortedHashes) {
		idx = 0 // wrap around
	}
	return hr.ring[idx].shardID
}

// RouteN returns N distinct shards for replication (for cross-shard replication).
func (hr *HashRing) RouteN(key string, n int) []ShardID {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.sortedHashes) == 0 {
		return nil
	}
	if n > len(hr.nodes) {
		n = len(hr.nodes)
	}

	h := hashKey(key)
	idx := sort.Search(len(hr.sortedHashes), func(i int) bool {
		return hr.sortedHashes[i] >= h
	})
	if idx >= len(hr.sortedHashes) {
		idx = 0
	}

	seen := make(map[ShardID]bool)
	var result []ShardID
	for i := 0; i < len(hr.ring) && len(result) < n; i++ {
		pos := (idx + i) % len(hr.ring)
		sid := hr.ring[pos].shardID
		if !seen[sid] {
			seen[sid] = true
			result = append(result, sid)
		}
	}
	return result
}

// Shards returns all registered shards.
func (hr *HashRing) Shards() []ShardInfo {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	result := make([]ShardInfo, 0, len(hr.nodes))
	for _, n := range hr.nodes {
		result = append(result, n.info)
	}
	return result
}

// ShardCount returns the number of shards.
func (hr *HashRing) ShardCount() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return len(hr.nodes)
}

func (hr *HashRing) rebuild() {
	hr.sortedHashes = nil
	hr.ring = nil

	for id := range hr.nodes {
		for v := 0; v < hr.vnodeCount; v++ {
			vkey := fmt.Sprintf("shard-%d-vnode-%d", id, v)
			h := hashKey(vkey)
			hr.sortedHashes = append(hr.sortedHashes, h)
			hr.ring = append(hr.ring, ringEntry{hash: h, shardID: id})
		}
	}

	// Sort by hash value
	indices := make([]int, len(hr.sortedHashes))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(a, b int) bool {
		return hr.sortedHashes[indices[a]] < hr.sortedHashes[indices[b]]
	})

	sortedHashes := make([]uint32, len(hr.sortedHashes))
	sortedRing := make([]ringEntry, len(hr.ring))
	for i, idx := range indices {
		sortedHashes[i] = hr.sortedHashes[idx]
		sortedRing[i] = hr.ring[idx]
	}
	hr.sortedHashes = sortedHashes
	hr.ring = sortedRing
}

func hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// ============================================================
// ShardedStore — Routes operations to the correct shard
// ============================================================

// ShardedStore distributes metadata across multiple PebbleStore shards.
type ShardedStore struct {
	ring   *HashRing
	shards map[ShardID]*PebbleStore
	mu     sync.RWMutex
}

// NewShardedStore creates a sharded metadata store.
func NewShardedStore(ring *HashRing) *ShardedStore {
	return &ShardedStore{
		ring:   ring,
		shards: make(map[ShardID]*PebbleStore),
	}
}

// AddShard registers a PebbleStore for a given shard ID.
func (ss *ShardedStore) AddShard(id ShardID, store *PebbleStore) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.shards[id] = store
}

// RemoveShard unregisters a shard.
func (ss *ShardedStore) RemoveShard(id ShardID) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	delete(ss.shards, id)
}

// GetShard returns the PebbleStore for a given key.
func (ss *ShardedStore) GetShard(key string) (*PebbleStore, error) {
	sid := ss.ring.Route(key)
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	store, ok := ss.shards[sid]
	if !ok {
		return nil, fmt.Errorf("sharded store: shard %d not available", sid)
	}
	return store, nil
}

// ShardForKey returns the shard ID for a given key.
func (ss *ShardedStore) ShardForKey(key string) ShardID {
	return ss.ring.Route(key)
}

// AllShards returns all registered shard stores.
func (ss *ShardedStore) AllShards() map[ShardID]*PebbleStore {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	result := make(map[ShardID]*PebbleStore, len(ss.shards))
	for k, v := range ss.shards {
		result[k] = v
	}
	return result
}

// ScanAllShards iterates over all shards' chunk metadata.
func (ss *ShardedStore) ScanAllShards(fn func(*ChunkMeta) error) error {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	for _, store := range ss.shards {
		if err := store.ScanAllChunks(nil, fn); err != nil {
			return err
		}
	}
	return nil
}

// ShardStats returns per-shard statistics.
type ShardStats struct {
	ShardID  ShardID `json:"shard_id"`
	KeyCount int64   `json:"key_count"`
	Leader   string  `json:"leader"`
}

// Stats returns statistics for all shards.
func (ss *ShardedStore) Stats() []ShardStats {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	var stats []ShardStats
	for _, info := range ss.ring.Shards() {
		stats = append(stats, ShardStats{
			ShardID:  info.ID,
			KeyCount: info.KeyCount,
			Leader:   info.Leader,
		})
	}
	return stats
}

// Close shuts down all shard stores.
func (ss *ShardedStore) Close() error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	var errs []error
	for _, store := range ss.shards {
		if err := store.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("sharded store close: %v", errs)
	}
	return nil
}
