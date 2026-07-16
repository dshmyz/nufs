package metadata

import (
	"context"
	"fmt"
	"hash/crc32"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
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

	// Node throttling — evaluated at the sharded-store level so a
	// single incoming RegisterNode/Heartbeat doesn't consume N
	// tokens across N shards. Individual shard-level throttle is
	// not used; this is the single source of truth.
	throttle *NodeRegistrationThrottle
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

// SetQuotaManager propagates the quota manager to all shards.
func (ss *ShardedStore) SetQuotaManager(qm *QuotaManager) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	for _, store := range ss.shards {
		store.SetQuotaManager(qm)
	}
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

// ============================================================
// ShardManager — Automatic Shard Split & Merge
// ============================================================

// ShardManagerConfig configures auto-split/merge behavior.
type ShardManagerConfig struct {
	CheckInterval  time.Duration // How often to evaluate (default: 5min)
	SplitThreshold int64         // Min keys per shard to trigger split (default: 10M)
	MergeThreshold int64         // Max keys per shard to allow merge (default: 1M)
	MinShards      int           // Minimum shard count (default: 1)
	MaxShards      int           // Maximum shard count (default: 1024)
	Enabled        bool          // Master switch for auto-sharding
}

// ShardManager monitors shard sizes and triggers splits or merges.
type ShardManager struct {
	store   *ShardedStore
	ring    *HashRing
	cfg     ShardManagerConfig
	stopCh  chan struct{}
	running atomic.Bool
	wg      sync.WaitGroup

	// callbacks: set by the orchestrator to actually create/destroy shards
	OnSplit func(parent ShardID, childStore *PebbleStore) (ShardID, error)
	OnMerge func(left, right ShardID) error
}

// NewShardManager creates a shard manager with sensible defaults.
func NewShardManager(store *ShardedStore, ring *HashRing, cfg ShardManagerConfig) *ShardManager {
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 5 * time.Minute
	}
	if cfg.SplitThreshold <= 0 {
		cfg.SplitThreshold = 10_000_000 // 10M keys
	}
	if cfg.MergeThreshold <= 0 {
		cfg.MergeThreshold = 1_000_000 // 1M keys
	}
	if cfg.MinShards <= 0 {
		cfg.MinShards = 1
	}
	if cfg.MaxShards <= 0 {
		cfg.MaxShards = 1024
	}
	return &ShardManager{
		store:  store,
		ring:   ring,
		cfg:    cfg,
		stopCh: make(chan struct{}),
	}
}

// Start begins the auto-sharding loop.
func (sm *ShardManager) Start() {
	if sm.running.Swap(true) {
		return
	}
	sm.wg.Add(1)
	go sm.loop()
	slog.Info("shardmanager: started",
		"interval", sm.cfg.CheckInterval,
		"split_threshold", sm.cfg.SplitThreshold,
		"merge_threshold", sm.cfg.MergeThreshold)
}

// Stop gracefully stops auto-sharding.
func (sm *ShardManager) Stop() {
	if !sm.running.Swap(false) {
		return
	}
	close(sm.stopCh)
	sm.wg.Wait()
	slog.Info("shardmanager: stopped")
}

func (sm *ShardManager) loop() {
	defer sm.wg.Done()
	ticker := time.NewTicker(sm.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.stopCh:
			return
		case <-ticker.C:
			if sm.cfg.Enabled {
				sm.evaluate()
			}
		}
	}
}

func (sm *ShardManager) evaluate() {
	shards := sm.store.AllShards()
	currentCount := len(shards)

	// Check each shard for split
	for id, store := range shards {
		if currentCount >= sm.cfg.MaxShards {
			break
		}
		if store.closed.Load() {
			continue
		}

		approxKeys := getApproxKeyCount(store)
		if approxKeys >= sm.cfg.SplitThreshold && sm.OnSplit != nil {
			slog.Info("shardmanager: shard exceeds split threshold",
				"shard_id", id,
				"keys", approxKeys,
				"threshold", sm.cfg.SplitThreshold)
			newID, err := sm.OnSplit(id, store)
			if err != nil {
				slog.Error("shardmanager: split failed", "shard_id", id, "error", err)
				continue
			}
			currentCount++
			slog.Info("shardmanager: shard split completed", "old_shard", id, "new_shard", newID)
		}
	}

	// Check adjacent shards for merge
	if currentCount > sm.cfg.MinShards {
		mergePairs := sm.findMergeCandidates(shards)
		for _, pair := range mergePairs {
			if currentCount <= sm.cfg.MinShards {
				break
			}
			if sm.OnMerge != nil {
				slog.Info("shardmanager: merging shards below threshold", "shard1", pair[0], "shard2", pair[1])
				if err := sm.OnMerge(pair[0], pair[1]); err != nil {
					slog.Error("shardmanager: merge failed", "shard1", pair[0], "shard2", pair[1], "error", err)
					continue
				}
				currentCount--
			}
		}
	}
}

// findMergeCandidates finds pairs of adjacent shards both below merge threshold.
func (sm *ShardManager) findMergeCandidates(shards map[ShardID]*PebbleStore) [][2]ShardID {
	var candidates [][2]ShardID
	sortedIDs := make([]ShardID, 0, len(shards))
	for id := range shards {
		sortedIDs = append(sortedIDs, id)
	}
	sort.Slice(sortedIDs, func(i, j int) bool { return sortedIDs[i] < sortedIDs[j] })

	for i := 0; i < len(sortedIDs)-1; i++ {
		left := sortedIDs[i]
		right := sortedIDs[i+1]

		leftStore := shards[left]
		rightStore := shards[right]
		if leftStore == nil || rightStore == nil {
			continue
		}

		leftKeys := getApproxKeyCount(leftStore)
		rightKeys := getApproxKeyCount(rightStore)
		if leftKeys < sm.cfg.MergeThreshold && rightKeys < sm.cfg.MergeThreshold {
			candidates = append(candidates, [2]ShardID{left, right})
		}
	}
	return candidates
}

// getApproxKeyCount returns an approximate key count for a PebbleStore.
// Uses a full scan — accurate enough for split/merge decisions.
func getApproxKeyCount(s *PebbleStore) int64 {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return 0
	}
	defer iter.Close()

	var count int64
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	return count
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

// ============================================================
// ShardedStore implements MetadataService
// ============================================================
//
// ShardedStore routes each operation to the correct shard based on
// the key's hash. For operations that span all shards (e.g., ListBuckets),
// it aggregates results from every shard.
//
// Bucket and Node operations are broadcast to all shards since they
// need to be available on every shard for placement decisions.
// Namespace and Chunk operations are routed by inode/chunk ID.

// shardKeyForBucket returns the routing key for bucket operations.
// Buckets are broadcast to all shards for consistency.
func shardKeyForBucket(name string) string {
	return "bucket:" + name
}

// shardKeyForInode returns the routing key for inode operations.
func shardKeyForInode(id InodeID) string {
	return fmt.Sprintf("inode:%d", id)
}

// shardKeyForChunk returns the routing key for chunk operations.
func shardKeyForChunk(id ChunkID) string {
	return fmt.Sprintf("chunk:%d", id)
}

// shardKeyForNode returns the routing key for node operations.
func shardKeyForNode(id NodeID) string {
	return fmt.Sprintf("node:%d", id)
}

// routeToShard returns the shard store for the given routing key.
func (ss *ShardedStore) routeToShard(key string) (*PebbleStore, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	sid := ss.ring.Route(key)
	store, ok := ss.shards[sid]
	if !ok {
		return nil, fmt.Errorf("sharded store: shard %d not available", sid)
	}
	return store, nil
}

// forEachShard calls fn on every shard. Stops on first error.
func (ss *ShardedStore) forEachShard(fn func(*PebbleStore) error) error {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	for _, store := range ss.shards {
		if err := fn(store); err != nil {
			return err
		}
	}
	return nil
}

// --- BucketService ---

func (ss *ShardedStore) CreateBucket(ctx context.Context, name string, policy PlacementPolicy) error {
	// Broadcast: create on all shards so placement decisions work locally
	return ss.forEachShard(func(s *PebbleStore) error {
		return s.CreateBucket(ctx, name, policy)
	})
}

func (ss *ShardedStore) DeleteBucket(ctx context.Context, name string) error {
	return ss.forEachShard(func(s *PebbleStore) error {
		return s.DeleteBucket(ctx, name)
	})
}

func (ss *ShardedStore) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	// Read from first available shard (all shards have the same bucket list)
	ss.mu.RLock()
	for _, store := range ss.shards {
		ss.mu.RUnlock()
		return store.ListBuckets(ctx)
	}
	ss.mu.RUnlock()
	return nil, fmt.Errorf("sharded store: no shards available")
}

func (ss *ShardedStore) GetBucket(ctx context.Context, name string) (*BucketInfo, error) {
	ss.mu.RLock()
	for _, store := range ss.shards {
		ss.mu.RUnlock()
		return store.GetBucket(ctx, name)
	}
	ss.mu.RUnlock()
	return nil, fmt.Errorf("sharded store: no shards available")
}

func (ss *ShardedStore) GetBucketByRoot(ctx context.Context, rootInode InodeID) (*BucketInfo, error) {
	ss.mu.RLock()
	for _, store := range ss.shards {
		ss.mu.RUnlock()
		return store.GetBucketByRoot(ctx, rootInode)
	}
	ss.mu.RUnlock()
	return nil, fmt.Errorf("sharded store: no shards available")
}

// --- NamespaceService ---

func (ss *ShardedStore) MkDir(ctx context.Context, parent InodeID, name string, mode uint32) (*InodeMeta, error) {
	store, err := ss.routeToShard(shardKeyForInode(parent))
	if err != nil {
		return nil, err
	}
	return store.MkDir(ctx, parent, name, mode)
}

func (ss *ShardedStore) RmDir(ctx context.Context, parent InodeID, name string) error {
	store, err := ss.routeToShard(shardKeyForInode(parent))
	if err != nil {
		return err
	}
	return store.RmDir(ctx, parent, name)
}

func (ss *ShardedStore) ReadDir(ctx context.Context, parent InodeID, offset int, limit int) ([]DirEntry, error) {
	store, err := ss.routeToShard(shardKeyForInode(parent))
	if err != nil {
		return nil, err
	}
	return store.ReadDir(ctx, parent, offset, limit)
}

func (ss *ShardedStore) ReadDirFrom(ctx context.Context, parent InodeID, afterName string, limit int) ([]DirEntry, error) {
	store, err := ss.routeToShard(shardKeyForInode(parent))
	if err != nil {
		return nil, err
	}
	return store.ReadDirFrom(ctx, parent, afterName, limit)
}

func (ss *ShardedStore) CreateFile(ctx context.Context, parent InodeID, name string, mode uint32) (*InodeMeta, error) {
	store, err := ss.routeToShard(shardKeyForInode(parent))
	if err != nil {
		return nil, err
	}
	return store.CreateFile(ctx, parent, name, mode)
}

func (ss *ShardedStore) Unlink(ctx context.Context, parent InodeID, name string) error {
	store, err := ss.routeToShard(shardKeyForInode(parent))
	if err != nil {
		return err
	}
	return store.Unlink(ctx, parent, name)
}

func (ss *ShardedStore) Lookup(ctx context.Context, parent InodeID, name string) (*InodeMeta, error) {
	store, err := ss.routeToShard(shardKeyForInode(parent))
	if err != nil {
		return nil, err
	}
	return store.Lookup(ctx, parent, name)
}

func (ss *ShardedStore) Rename(ctx context.Context, oldParent InodeID, oldName string, newParent InodeID, newName string) error {
	// If old and new parent are on different shards, this is a cross-shard rename.
	// For simplicity, route to the old parent's shard and let it handle the operation.
	// A full implementation would need a two-phase commit for cross-shard renames.
	store, err := ss.routeToShard(shardKeyForInode(oldParent))
	if err != nil {
		return err
	}
	return store.Rename(ctx, oldParent, oldName, newParent, newName)
}

func (ss *ShardedStore) Symlink(ctx context.Context, parent InodeID, name string, target string) (*InodeMeta, error) {
	store, err := ss.routeToShard(shardKeyForInode(parent))
	if err != nil {
		return nil, err
	}
	return store.Symlink(ctx, parent, name, target)
}

func (ss *ShardedStore) Readlink(ctx context.Context, id InodeID) (string, error) {
	store, err := ss.routeToShard(shardKeyForInode(id))
	if err != nil {
		return "", err
	}
	return store.Readlink(ctx, id)
}

func (ss *ShardedStore) Link(ctx context.Context, parent InodeID, name string, target InodeID) (*InodeMeta, error) {
	store, err := ss.routeToShard(shardKeyForInode(parent))
	if err != nil {
		return nil, err
	}
	return store.Link(ctx, parent, name, target)
}

// --- InodeService ---

func (ss *ShardedStore) GetInode(ctx context.Context, id InodeID) (*InodeMeta, error) {
	store, err := ss.routeToShard(shardKeyForInode(id))
	if err != nil {
		return nil, err
	}
	return store.GetInode(ctx, id)
}

func (ss *ShardedStore) UpdateInode(ctx context.Context, meta *InodeMeta) error {
	store, err := ss.routeToShard(shardKeyForInode(meta.ID))
	if err != nil {
		return err
	}
	return store.UpdateInode(ctx, meta)
}

// --- ChunkService ---

func (ss *ShardedStore) AllocateChunk(ctx context.Context, inodeID InodeID, offset int64, policy PlacementPolicy) (*ChunkMeta, error) {
	store, err := ss.routeToShard(shardKeyForInode(inodeID))
	if err != nil {
		return nil, err
	}
	return store.AllocateChunk(ctx, inodeID, offset, policy)
}

func (ss *ShardedStore) AllocateChunksBatch(ctx context.Context, inodeID InodeID, offsets []int64, policy PlacementPolicy) ([]*ChunkMeta, error) {
	store, err := ss.routeToShard(shardKeyForInode(inodeID))
	if err != nil {
		return nil, err
	}
	return store.AllocateChunksBatch(ctx, inodeID, offsets, policy)
}

func (ss *ShardedStore) CommitChunk(ctx context.Context, chunkID ChunkID, checksum uint32) error {
	store, err := ss.routeToShard(shardKeyForChunk(chunkID))
	if err != nil {
		return err
	}
	return store.CommitChunk(ctx, chunkID, checksum)
}

func (ss *ShardedStore) GetChunk(ctx context.Context, chunkID ChunkID) (*ChunkMeta, error) {
	store, err := ss.routeToShard(shardKeyForChunk(chunkID))
	if err != nil {
		return nil, err
	}
	return store.GetChunk(ctx, chunkID)
}

func (ss *ShardedStore) UpdateChunk(ctx context.Context, chunk *ChunkMeta) error {
	store, err := ss.routeToShard(shardKeyForChunk(chunk.ID))
	if err != nil {
		return err
	}
	return store.UpdateChunk(ctx, chunk)
}

func (ss *ShardedStore) SealChunk(ctx context.Context, chunkID ChunkID) error {
	store, err := ss.routeToShard(shardKeyForChunk(chunkID))
	if err != nil {
		return err
	}
	return store.SealChunk(ctx, chunkID)
}

func (ss *ShardedStore) ListChunks(ctx context.Context, inodeID InodeID) ([]ChunkRef, error) {
	store, err := ss.routeToShard(shardKeyForInode(inodeID))
	if err != nil {
		return nil, err
	}
	return store.ListChunks(ctx, inodeID)
}

func (ss *ShardedStore) DeleteChunk(ctx context.Context, chunkID ChunkID) error {
	store, err := ss.routeToShard(shardKeyForChunk(chunkID))
	if err != nil {
		return err
	}
	return store.DeleteChunk(ctx, chunkID)
}

func (ss *ShardedStore) ReportChunkState(ctx context.Context, nodeID NodeID, states map[ChunkID]ReplicaState) error {
	// Route each chunk to its shard
	for chunkID, state := range states {
		store, err := ss.routeToShard(shardKeyForChunk(chunkID))
		if err != nil {
			return err
		}
		singleState := map[ChunkID]ReplicaState{chunkID: state}
		if err := store.ReportChunkState(ctx, nodeID, singleState); err != nil {
			return err
		}
	}
	return nil
}

// --- NodeService ---

func (ss *ShardedStore) RegisterNode(ctx context.Context, info *NodeInfo) error {
	if ss.throttle != nil && !ss.throttle.Allow(info.ID) {
		return ErrTooManyRequests
	}
	// Broadcast: nodes must be visible on all shards for placement
	return ss.forEachShard(func(s *PebbleStore) error {
		return s.RegisterNode(ctx, info)
	})
}

func (ss *ShardedStore) Heartbeat(ctx context.Context, nodeID NodeID, report *NodeReport) error {
	if ss.throttle != nil && !ss.throttle.Allow(nodeID) {
		return ErrTooManyRequests
	}
	// Broadcast to all shards
	return ss.forEachShard(func(s *PebbleStore) error {
		return s.Heartbeat(ctx, nodeID, report)
	})
}

// SetNodeThrottle installs or replaces the registration/heartbeat rate
// limiter at the sharded-store level. Passing nil disables throttling.
func (ss *ShardedStore) SetNodeThrottle(t *NodeRegistrationThrottle) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.throttle = t
}

// GetNodeThrottle returns the installed limiter (may be nil).
func (ss *ShardedStore) GetNodeThrottle() *NodeRegistrationThrottle {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.throttle
}

func (ss *ShardedStore) DecommissionNode(ctx context.Context, nodeID NodeID) error {
	return ss.forEachShard(func(s *PebbleStore) error {
		return s.DecommissionNode(ctx, nodeID)
	})
}

func (ss *ShardedStore) EnterMaintenance(ctx context.Context, nodeID NodeID) error {
	return ss.forEachShard(func(s *PebbleStore) error {
		return s.EnterMaintenance(ctx, nodeID)
	})
}

func (ss *ShardedStore) ExitMaintenance(ctx context.Context, nodeID NodeID) error {
	return ss.forEachShard(func(s *PebbleStore) error {
		return s.ExitMaintenance(ctx, nodeID)
	})
}

func (ss *ShardedStore) RollingUpgradePlan(ctx context.Context) ([]NodeID, error) {
	ss.mu.RLock()
	for _, store := range ss.shards {
		ss.mu.RUnlock()
		return store.RollingUpgradePlan(ctx)
	}
	ss.mu.RUnlock()
	return nil, fmt.Errorf("sharded store: no shards available")
}

func (ss *ShardedStore) ListNodes(ctx context.Context) ([]NodeInfo, error) {
	ss.mu.RLock()
	for _, store := range ss.shards {
		ss.mu.RUnlock()
		return store.ListNodes(ctx)
	}
	ss.mu.RUnlock()
	return nil, fmt.Errorf("sharded store: no shards available")
}

func (ss *ShardedStore) GetNode(ctx context.Context, nodeID NodeID) (*NodeInfo, error) {
	ss.mu.RLock()
	for _, store := range ss.shards {
		ss.mu.RUnlock()
		return store.GetNode(ctx, nodeID)
	}
	ss.mu.RUnlock()
	return nil, fmt.Errorf("sharded store: no shards available")
}

// --- RepairService ---

func (ss *ShardedStore) GetRepairQueue(ctx context.Context) ([]RepairTask, error) {
	var allTasks []RepairTask
	err := ss.forEachShard(func(s *PebbleStore) error {
		tasks, err := s.GetRepairQueue(ctx)
		if err != nil {
			return err
		}
		allTasks = append(allTasks, tasks...)
		return nil
	})
	return allTasks, err
}

func (ss *ShardedStore) TriggerRepair(ctx context.Context, chunkID ChunkID) error {
	store, err := ss.routeToShard(shardKeyForChunk(chunkID))
	if err != nil {
		return err
	}
	return store.TriggerRepair(ctx, chunkID)
}

func (ss *ShardedStore) RemoveRepairTask(ctx context.Context, chunkID ChunkID) error {
	store, err := ss.routeToShard(shardKeyForChunk(chunkID))
	if err != nil {
		return err
	}
	return store.RemoveRepairTask(ctx, chunkID)
}

func (ss *ShardedStore) TriggerRebalance(ctx context.Context) error {
	return ss.forEachShard(func(s *PebbleStore) error {
		return s.TriggerRebalance(ctx)
	})
}

func (ss *ShardedStore) ChunksByNode(ctx context.Context, nodeID NodeID) ([]ChunkMeta, error) {
	var allChunks []ChunkMeta
	err := ss.forEachShard(func(s *PebbleStore) error {
		chunks, err := s.ChunksByNode(ctx, nodeID)
		if err != nil {
			return err
		}
		allChunks = append(allChunks, chunks...)
		return nil
	})
	return allChunks, err
}

func (ss *ShardedStore) MigrateChunkReplica(ctx context.Context, chunkID ChunkID, fromNode, toNode NodeID) error {
	store, err := ss.routeToShard(shardKeyForChunk(chunkID))
	if err != nil {
		return err
	}
	return store.MigrateChunkReplica(ctx, chunkID, fromNode, toNode)
}

// --- LockService ---

func (ss *ShardedStore) AdvisoryLock(ctx context.Context, inode InodeID, owner string) error {
	store, err := ss.routeToShard(shardKeyForInode(inode))
	if err != nil {
		return err
	}
	return store.AdvisoryLock(ctx, inode, owner)
}

func (ss *ShardedStore) AdvisoryLockShared(ctx context.Context, inode InodeID, owner string) error {
	store, err := ss.routeToShard(shardKeyForInode(inode))
	if err != nil {
		return err
	}
	return store.AdvisoryLockShared(ctx, inode, owner)
}

func (ss *ShardedStore) AdvisoryUnlock(ctx context.Context, inode InodeID, owner string) error {
	store, err := ss.routeToShard(shardKeyForInode(inode))
	if err != nil {
		return err
	}
	return store.AdvisoryUnlock(ctx, inode, owner)
}

func (ss *ShardedStore) AdvisoryListLocks(ctx context.Context, inode InodeID) ([]LockInfo, error) {
	store, err := ss.routeToShard(shardKeyForInode(inode))
	if err != nil {
		return nil, err
	}
	return store.AdvisoryListLocks(ctx, inode)
}

// --- XAttrService ---

func (ss *ShardedStore) GetXAttr(ctx context.Context, id InodeID, name string) ([]byte, error) {
	store, err := ss.routeToShard(shardKeyForInode(id))
	if err != nil {
		return nil, err
	}
	return store.GetXAttr(ctx, id, name)
}

func (ss *ShardedStore) SetXAttr(ctx context.Context, id InodeID, name string, value []byte) error {
	store, err := ss.routeToShard(shardKeyForInode(id))
	if err != nil {
		return err
	}
	return store.SetXAttr(ctx, id, name, value)
}

func (ss *ShardedStore) ListXAttr(ctx context.Context, id InodeID) (map[string][]byte, error) {
	store, err := ss.routeToShard(shardKeyForInode(id))
	if err != nil {
		return nil, err
	}
	return store.ListXAttr(ctx, id)
}

func (ss *ShardedStore) RemoveXAttr(ctx context.Context, id InodeID, name string) error {
	store, err := ss.routeToShard(shardKeyForInode(id))
	if err != nil {
		return err
	}
	return store.RemoveXAttr(ctx, id, name)
}

// --- Admin ---

func (ss *ShardedStore) ComputeAllBucketUsage(ctx context.Context) ([]BucketUsage, error) {
	// Aggregate usage from all shards
	usageByName := make(map[string]*BucketUsage)
	err := ss.forEachShard(func(s *PebbleStore) error {
		usages, err := s.ComputeAllBucketUsage(ctx)
		if err != nil {
			return err
		}
		for _, u := range usages {
			if existing, ok := usageByName[u.Name]; ok {
				existing.UsedBytes += u.UsedBytes
				existing.Objects += u.Objects
			} else {
				usageByName[u.Name] = &BucketUsage{
					Name:      u.Name,
					UsedBytes: u.UsedBytes,
					Objects:   u.Objects,
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	result := make([]BucketUsage, 0, len(usageByName))
	for _, u := range usageByName {
		result = append(result, *u)
	}
	return result, nil
}

// ========== AccessControlService Implementation ==========

// SetBucketPolicy delegates to the shard that owns the bucket.
func (s *ShardedStore) SetBucketPolicy(ctx context.Context, bucket string, policy BucketPolicy) error {
	shard, err := s.GetShard(bucket)
	if err != nil {
		return err
	}
	return shard.SetBucketPolicy(ctx, bucket, policy)
}

// GetBucketPolicy delegates to the shard that owns the bucket.
func (s *ShardedStore) GetBucketPolicy(ctx context.Context, bucket string) (*BucketPolicy, error) {
	shard, err := s.GetShard(bucket)
	if err != nil {
		return nil, err
	}
	return shard.GetBucketPolicy(ctx, bucket)
}

// DeleteBucketPolicy delegates to the shard that owns the bucket.
func (s *ShardedStore) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	shard, err := s.GetShard(bucket)
	if err != nil {
		return err
	}
	return shard.DeleteBucketPolicy(ctx, bucket)
}

// Compile-time interface check
var _ MetadataService = (*ShardedStore)(nil)
