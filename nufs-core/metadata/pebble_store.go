package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	lru "github.com/hashicorp/golang-lru"
)

// ============================================================
// Graceful Degradation — Circuit breaker & read-only fallback
// ============================================================

// DegradationState represents the current operational mode of the store.
type DegradationState uint32

const (
	DegStateNormal      DegradationState = iota // Fully operational
	DegStateReadOnly                            // Accept reads only, reject writes
	DegStateDegraded                            // Partial functionality
	DegStateUnavailable                         // Completely unavailable
)

func (s DegradationState) String() string {
	switch s {
	case DegStateNormal:
		return "Normal"
	case DegStateReadOnly:
		return "ReadOnly"
	case DegStateDegraded:
		return "Degraded"
	case DegStateUnavailable:
		return "Unavailable"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

// DegradationManager monitors system health and automatically adjusts
// the operational mode to prevent cascading failures.
type DegradationManager struct {
	store     *PebbleStore
	state     atomic.Uint32
	raftAvail atomic.Bool
	dbAvail   atomic.Bool
	mu        sync.RWMutex

	// Consecutive error thresholds before triggering degradation
	maxConsecutiveWriteErrors int
	maxConsecutiveReadErrors  int
	maxConsecutiveRaftErrors  int

	// Current error counts (reset on success)
	writeErrors atomic.Int64
	readErrors  atomic.Int64
	raftErrors  atomic.Int64

	// Callback for state transitions
	onStateChange func(from, to DegradationState)
}

// NewDegradationManager creates a degradation manager.
func NewDegradationManager(store *PebbleStore) *DegradationManager {
	dm := &DegradationManager{
		store:                     store,
		maxConsecutiveWriteErrors: 10,
		maxConsecutiveReadErrors:  20,
		maxConsecutiveRaftErrors:  5,
	}
	dm.dbAvail.Store(true)
	dm.raftAvail.Store(true)
	dm.state.Store(uint32(DegStateNormal))
	return dm
}

// State returns the current degradation state.
func (dm *DegradationManager) State() DegradationState {
	return DegradationState(dm.state.Load())
}

// IsReadOnly returns true if the store is in read-only mode.
func (dm *DegradationManager) IsReadOnly() bool {
	return dm.State() >= DegStateReadOnly
}

// IsAvailable returns true if the store can serve requests.
func (dm *DegradationManager) IsAvailable() bool {
	return dm.State() < DegStateUnavailable
}

// RecordWriteError reports a write failure. Triggers read-only if threshold exceeded.
func (dm *DegradationManager) RecordWriteError() {
	n := dm.writeErrors.Add(1)
	if n >= int64(dm.maxConsecutiveWriteErrors) {
		dm.transition(DegStateReadOnly)
	}
}

// RecordWriteSuccess resets the write error counter.
func (dm *DegradationManager) RecordWriteSuccess() {
	dm.writeErrors.Store(0)
}

// RecordReadError reports a read failure.
func (dm *DegradationManager) RecordReadError() {
	n := dm.readErrors.Add(1)
	if n >= int64(dm.maxConsecutiveReadErrors) {
		dm.transition(DegStateDegraded)
	}
}

// RecordReadSuccess resets the read error counter.
func (dm *DegradationManager) RecordReadSuccess() {
	dm.readErrors.Store(0)
}

// RecordRaftError reports a Raft failure.
func (dm *DegradationManager) RecordRaftError() {
	n := dm.raftErrors.Add(1)
	if n >= int64(dm.maxConsecutiveRaftErrors) {
		dm.raftAvail.Store(false)
		dm.transition(DegStateReadOnly)
	}
}

// RecordRaftSuccess resets the Raft error counter.
func (dm *DegradationManager) RecordRaftSuccess() {
	dm.raftErrors.Store(0)
	dm.raftAvail.Store(true)
}

// SetOnStateChange registers a callback for degradation state transitions.
func (dm *DegradationManager) SetOnStateChange(fn func(from, to DegradationState)) {
	dm.mu.Lock()
	dm.onStateChange = fn
	dm.mu.Unlock()
}

// Recover forces the degradation state back to Normal.
// Intended for administrative use after the underlying issue has been resolved.
func (dm *DegradationManager) Recover() {
	dm.writeErrors.Store(0)
	dm.readErrors.Store(0)
	dm.raftErrors.Store(0)
	dm.transition(DegStateNormal)
}

func (dm *DegradationManager) transition(to DegradationState) {
	from := dm.State()
	if from == to {
		return
	}
	dm.state.Store(uint32(to))

	switch to {
	case DegStateReadOnly:
		slog.Warn("degradation: entering READ-ONLY mode", "from", from)
	case DegStateDegraded:
		slog.Warn("degradation: entering DEGRADED mode", "from", from)
	case DegStateUnavailable:
		slog.Error("degradation: UNAVAILABLE", "from", from)
	}

	dm.mu.RLock()
	fn := dm.onStateChange
	dm.mu.RUnlock()
	if fn != nil {
		fn(from, to)
	}
}

// AttemptRecovery tries to return to normal state.
func (dm *DegradationManager) AttemptRecovery() {
	current := dm.State()
	if current == DegStateUnavailable {
		return // Requires manual intervention
	}

	dm.writeErrors.Store(0)
	dm.readErrors.Store(0)
	dm.raftErrors.Store(0)
	dm.dbAvail.Store(true)
	dm.raftAvail.Store(true)

	if current != DegStateNormal {
		dm.transition(DegStateNormal)
		slog.Info("degradation: recovered to NORMAL mode")
	}
}

// PebbleStore implements MetadataService using Pebble (LSM-tree) as the
// primary storage engine. Designed for hundred-million to billion-scale metadata.
// When configured with Raft, all writes go through the Raft log for consistency.
type PebbleStore struct {
	db        *pebble.DB
	cache     *pebble.DB // Optional read cache (nil if disabled)
	placement *PlacementEngine
	chunkGen  *ChunkIDGenerator
	inodeSeq  atomic.Uint64
	closed    atomic.Bool
	mu        sync.RWMutex
	cfg       PebbleStoreConfig

	// Dynamic config — swapped atomically via atomic.Pointer so reads are lock-free.
	// Use GetDynamicConfig() / SetDynamicConfig() to access.
	dynCfg atomic.Pointer[DynamicConfig]

	// Degradation manager for graceful degradation
	degradation *DegradationManager

	// Metrics collector (populated by SetMetrics)
	metrics *Metrics

	// InodeID recycling: when files/dirs are deleted, their IDs can be
	// recycled to avoid unbounded growth of the inode counter. Recycled
	// IDs are returned before allocating new ones.
	inodeFreeList []InodeID
	inodeFreeMu   sync.Mutex

	// Raft integration: when set, all mutating operations are applied via Raft
	raft *RaftNode

	// advisoryLocks tracks the in-memory advisory file lock table.
	// See lock.go for the model. State is dropped on Close / restart.
	advisoryLocks *advisoryLockManager

	// inCache is a read-through cache for GetInode hot path.
	// Invalidated on UpdateInode and CASUpdateInode.
	inCache inodeCache

	// Optional EventBus for publishing change notifications.
	// Set by SetEventBus() after initialization.
	events *EventBus

	// Quota enforcement for bucket writes
	quota *QuotaManager

	// Auto-rebalance on node registration
	autoRebalance bool
	rebalanceMu   sync.Mutex // prevents concurrent rebalance runs
}

// inodeCache is a read-through cache for GetInode.
// Uses LRU eviction so that the hottest entries survive when the
// cache reaches capacity (no more "clear everything" stampede).
// Safe for concurrent use (hashicorp/golang-lru is internally locked).
type inodeCache struct {
	c *lru.Cache
}

const maxInodeCacheEntries = 100_000

func newInodeCache() inodeCache {
	c, _ := lru.New(maxInodeCacheEntries) // error only on size <= 0
	return inodeCache{c: c}
}

func (c *inodeCache) get(id InodeID) *InodeMeta {
	v, ok := c.c.Get(id)
	if !ok {
		return nil
	}
	return v.(*InodeMeta)
}

func (c *inodeCache) put(id InodeID, meta *InodeMeta) {
	c.c.Add(id, meta) // LRU evicts the least-recently-used entry automatically
}

func (c *inodeCache) del(id InodeID) {
	c.c.Remove(id)
}

func (c *inodeCache) clear() {
	c.c.Purge()
}

// // DynamicConfig holds all tunable parameters that can be changed while the
// system is running, without requiring a restart. These are typically adjusted
// by SREs in response to load changes, performance bottlenecks, or incidents.
type DynamicConfig struct {
	// GC
	GCEnabled          bool          `json:"gc_enabled"`          // Master switch for GC
	GCInterval         time.Duration `json:"gc_interval"`         // How often to run GC scans
	GCChunkBatchSize   int           `json:"gc_chunk_batch_size"` // Chunks per scan batch
	GCThresholdPercent float64       `json:"gc_threshold"`        // GC if orphaned > this %
	GCDryRun           bool          `json:"gc_dry_run"`          // Don't actually delete

	// Heartbeat / Lease
	HeartbeatTTLSeconds    int  `json:"heartbeat_ttl_seconds"`    // Node mark offline after this
	HeartbeatCheckInterval int  `json:"heartbeat_check_interval"` // How often to check
	AutoRepairEnabled      bool `json:"auto_repair_enabled"`      // Auto-trigger chunk repair

	// Write batching
	WriteBatchingEnabled bool          `json:"write_batching_enabled"`
	WriteBatchMaxSize    int           `json:"write_batch_max_size"`
	WriteBatchMaxWait    time.Duration `json:"write_batch_max_wait"`

	// Read cache
	CacheEnabled bool `json:"cache_enabled"`
	CacheMaxSize int  `json:"cache_max_size"` // Max entries before eviction

	// Raft
	RaftPreVoteEnabled bool `json:"raft_prevote_enabled"`

	// Placement
	PlacementSpreadEnabled  bool `json:"placement_spread_enabled"`
	PlacementWeightedChoice bool `json:"placement_weighted_choice"`
}

// DefaultDynamicConfig returns safe production defaults for all dynamic configs.
func DefaultDynamicConfig() DynamicConfig {
	return DynamicConfig{
		GCEnabled:               true,
		GCInterval:              15 * time.Minute,
		GCChunkBatchSize:        1000,
		GCThresholdPercent:      0.0, // GC if any orphaned chunk
		GCDryRun:                false,
		HeartbeatTTLSeconds:     30,
		HeartbeatCheckInterval:  5,
		AutoRepairEnabled:       true,
		WriteBatchingEnabled:    true,
		WriteBatchMaxSize:       256,
		WriteBatchMaxWait:       50 * time.Millisecond,
		CacheEnabled:            true,
		CacheMaxSize:            65536,
		RaftPreVoteEnabled:      true,
		PlacementSpreadEnabled:  true,
		PlacementWeightedChoice: false,
	}
}

// PebbleStoreConfig configures a PebbleStore instance.
type PebbleStoreConfig struct {
	// Dir is the directory for Pebble data files.
	Dir string
	// CacheDir enables a secondary Pebble instance for read caching (optional).
	CacheDir string
	// NodeID is used for chunk ID generation.
	NodeID uint64
	// MemTableSize is the size of each memtable in bytes (default 256MB).
	MemTableSize uint64
	// MaxOpenFiles limits the number of open SST files (default 16384).
	MaxOpenFiles int
	// UseInMemory uses an in-memory VFS (for testing only).
	UseInMemory bool
	// UseBucketStats enables per-bucket usage counters updated atomically
	// with mutations. When false, ComputeAllBucketUsage falls back to a
	// full namespace+inode scan. Enable after backfill (see migration docs).
	UseBucketStats bool
}

// batchJSONOp represents a single key-value write (JSON-serialized) in an atomic batch.
type batchJSONOp struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

// NewPebbleStore creates a new Pebble-backed metadata store.
func NewPebbleStore(cfg PebbleStoreConfig) (*PebbleStore, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("pebble store: Dir is required")
	}

	pebbleOpts := &pebble.Options{
		MemTableSize:                256 << 20, // 256MB
		MemTableStopWritesThreshold: 8,
		MaxOpenFiles:                16384,
		FormatMajorVersion:          pebble.FormatNewest,
	}
	if cfg.MemTableSize > 0 {
		pebbleOpts.MemTableSize = cfg.MemTableSize
	}
	if cfg.MaxOpenFiles > 0 {
		pebbleOpts.MaxOpenFiles = cfg.MaxOpenFiles
	}
	if cfg.UseInMemory {
		pebbleOpts.FS = vfs.NewMem()
	}

	db, err := pebble.Open(cfg.Dir, pebbleOpts)
	if err != nil {
		return nil, fmt.Errorf("pebble store: open db: %w", err)
	}

	s := &PebbleStore{
		db:            db,
		placement:     NewPlacementEngine(),
		chunkGen:      NewChunkIDGenerator(cfg.NodeID),
		cfg:           cfg,
		advisoryLocks: newAdvisoryLockManager(),
		inCache:       newInodeCache(),
	}
	s.inodeSeq.Store(uint64(RootInodeID))
	s.degradation = NewDegradationManager(s)
	dcfg := DefaultDynamicConfig()
	s.dynCfg.Store(&dcfg)

	if err := s.initRootInode(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close shuts down the store.
func (s *PebbleStore) Close() error {
	if s.closed.Swap(true) {
		return ErrServiceClosed
	}
	var errs []error
	if err := s.db.Close(); err != nil {
		errs = append(errs, err)
	}
	if s.cache != nil {
		if err := s.cache.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("pebble store: close errors: %v", errs)
	}
	return nil
}

// SetEventBus sets the event bus for publishing change notifications.
// This should be called after NewPebbleServiceBundle creates the EventBus.
func (s *PebbleStore) SetEventBus(events *EventBus) {
	s.events = events
}

// SetQuotaManager registers the quota manager for write admission control.
// When set, AllocateChunk checks bucket quotas before allocating new chunks.
// It also wires the PebbleStore as the QuotaStore backend so quota changes
// are persisted to Pebble, and loads any previously saved quota data.
func (s *PebbleStore) SetQuotaManager(qm *QuotaManager) {
	qm.SetStore(s)
	s.quota = qm
	s.loadQuotas(qm)
}

// SetAutoRebalance enables automatic rebalance when new nodes register.
// When enabled, RegisterNode triggers a rebalance if the cluster is imbalanced.
func (s *PebbleStore) SetAutoRebalance(enabled bool) {
	s.autoRebalance = enabled
}

// publishEvent publishes a change event if an EventBus is configured.
func (s *PebbleStore) publishEvent(event Event) {
	if s.events != nil {
		s.events.Publish(event)
	}
}

// publishNodeEvent publishes a node change event for PlacementEngine auto-sync.
func (s *PebbleStore) publishNodeEvent(key string, info *NodeInfo) {
	if s.events == nil {
		return
	}
	data, err := marshalValue(info, codecMsgpack)
	if err != nil {
		return
	}
	s.events.Publish(Event{
		Type:  EventSet,
		Key:   key,
		Value: data,
	})
}

// AdvisoryLock acquires an exclusive lock on inode for owner.
// See lock.go for the full model.
func (s *PebbleStore) AdvisoryLock(ctx context.Context, inode InodeID, owner string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	// Honour context cancellation before doing any work. The lock
	// itself is a short, in-memory operation, so cancellation can
	// only land at the entry point — once we are inside the mutex
	// the call completes in microseconds.
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.advisoryLocks.acquire(inode, owner, LockModeExclusive)
}

// AdvisoryLockShared is the read-side equivalent. See AdvisoryLock
// and lock.go.
func (s *PebbleStore) AdvisoryLockShared(ctx context.Context, inode InodeID, owner string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.advisoryLocks.acquire(inode, owner, LockModeShared)
}

// AdvisoryUnlock releases one acquisition of (inode, owner). A
// no-op for owners that do not hold the lock, matching flock(2).
func (s *PebbleStore) AdvisoryUnlock(ctx context.Context, inode InodeID, owner string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.advisoryLocks.release(inode, owner)
}

// AdvisoryListLocks returns a snapshot of every holder of inode.
// Used by `dfsctl locks <inode>` and the admin endpoint; the
// runtime path does not call it.
func (s *PebbleStore) AdvisoryListLocks(ctx context.Context, inode InodeID) ([]LockInfo, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.advisoryLocks.list(inode), nil
}

// ========== Extended Attributes (xattrs) ==========

// GetXAttr returns the value of the named xattr on the given inode.
// Returns ErrXAttrNotFound if the attribute does not exist.
func (s *PebbleStore) GetXAttr(ctx context.Context, id InodeID, name string) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	meta, err := s.GetInode(ctx, id)
	if err != nil {
		return nil, err
	}
	if meta.XAttrs == nil {
		return nil, ErrXAttrNotFound
	}
	val, ok := meta.XAttrs[name]
	if !ok {
		return nil, ErrXAttrNotFound
	}
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// SetXAttr sets the named xattr on the given inode. If the attribute
// already exists it is overwritten. An empty value removes the key.
func (s *PebbleStore) SetXAttr(ctx context.Context, id InodeID, name string, value []byte) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	meta, err := s.GetInode(ctx, id)
	if err != nil {
		return err
	}
	if meta.XAttrs == nil {
		meta.XAttrs = make(map[string][]byte)
	}
	v := make([]byte, len(value))
	copy(v, value)
	meta.XAttrs[name] = v
	return s.UpdateInode(ctx, meta)
}

// ListXAttr returns all xattrs on the given inode. The returned map
// is a copy; callers may mutate it freely.
func (s *PebbleStore) ListXAttr(ctx context.Context, id InodeID) (map[string][]byte, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	meta, err := s.GetInode(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(meta.XAttrs) == 0 {
		return nil, nil
	}
	out := make(map[string][]byte, len(meta.XAttrs))
	for k, v := range meta.XAttrs {
		cp := make([]byte, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out, nil
}

// RemoveXAttr deletes the named xattr from the given inode. Removing
// a non-existent attribute is a no-op (not an error).
func (s *PebbleStore) RemoveXAttr(ctx context.Context, id InodeID, name string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	meta, err := s.GetInode(ctx, id)
	if err != nil {
		return err
	}
	if meta.XAttrs == nil {
		return nil
	}
	delete(meta.XAttrs, name)
	return s.UpdateInode(ctx, meta)
}

// ========== Internal Helpers ==========

func (s *PebbleStore) initRootInode() error {
	key := fmt.Sprintf("%s%d", prefixInode, RootInodeID)
	_, closer, err := s.db.Get([]byte(key))
	if err == nil {
		closer.Close()
		return nil // Already initialized
	}
	if err != pebble.ErrNotFound {
		return fmt.Errorf("pebble store: init root inode: %w", err)
	}

	now := time.Now().UnixNano()
	root := &InodeMeta{
		ID:    RootInodeID,
		Type:  FileDirectory,
		Mode:  0755,
		NLink: 2,
		CTime: now,
		MTime: now,
		ATime: now,
	}
	return s.putJSON(key, root)
}

func (s *PebbleStore) nextInodeID() InodeID {
	// Try recycled IDs first
	s.inodeFreeMu.Lock()
	if len(s.inodeFreeList) > 0 {
		id := s.inodeFreeList[len(s.inodeFreeList)-1]
		s.inodeFreeList = s.inodeFreeList[:len(s.inodeFreeList)-1]
		s.inodeFreeMu.Unlock()
		return id
	}
	s.inodeFreeMu.Unlock()

	return InodeID(s.inodeSeq.Add(1))
}

// releaseInodeID returns an inode ID to the free list for recycling.
// Called when a file or directory is permanently deleted.
func (s *PebbleStore) releaseInodeID(id InodeID) {
	if id <= RootInodeID {
		return // Never recycle root or reserved IDs
	}
	s.inodeFreeMu.Lock()
	// Cap free list to prevent unbounded memory growth
	if len(s.inodeFreeList) < 65536 {
		s.inodeFreeList = append(s.inodeFreeList, id)
	}
	s.inodeFreeMu.Unlock()
}

func (s *PebbleStore) putMsgpack(key string, v interface{}) error {
	data, err := marshalValue(v, codecMsgpack)
	if err != nil {
		return fmt.Errorf("pebble store: marshal: %w", err)
	}
	return s.applyViaRaft(OpSet, key, data)
}

// putJSON writes a JSON-encoded value (cold path: admin/debug operations).
// Hot-path writes should use putMsgpack instead.
func (s *PebbleStore) putJSON(key string, v interface{}) error {
	data, err := marshalValue(v, codecJSON)
	if err != nil {
		return fmt.Errorf("pebble store: marshal: %w", err)
	}
	return s.applyViaRaft(OpSet, key, data)
}

// putMsgpackBatch writes a msgpack-encoded value to a Pebble batch (hot path).
func putMsgpackBatch(batch *pebble.Batch, key string, v interface{}) error {
	data, err := marshalValue(v, codecMsgpack)
	if err != nil {
		return fmt.Errorf("pebble store: marshal: %w", err)
	}
	return batch.Set([]byte(key), data, nil)
}

// putJSONBatch writes a JSON-encoded value to a Pebble batch (cold path).
func putJSONBatch(batch *pebble.Batch, key string, v interface{}) error {
	data, err := marshalValue(v, codecJSON)
	if err != nil {
		return fmt.Errorf("pebble store: marshal: %w", err)
	}
	return batch.Set([]byte(key), data, nil)
}

// getValue reads and auto-detects the encoding format (msgpack or JSON).
// Replaces the old getJSON with format-agnostic decoding.
func (s *PebbleStore) getValue(key string, v interface{}) (bool, error) {
	val, closer, err := s.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("pebble store: get %q: %w", key, err)
	}
	defer closer.Close()

	// Copy val before closer.Close()
	data := make([]byte, len(val))
	copy(data, val)

	if err := unmarshalValue(data, v); err != nil {
		return false, fmt.Errorf("pebble store: unmarshal %q: %w", key, err)
	}
	return true, nil
}

// getJSON is kept for backward compatibility; delegates to getValue.
// Deprecated: use getValue instead.
func (s *PebbleStore) getJSON(key string, v interface{}) (bool, error) {
	return s.getValue(key, v)
}

func (s *PebbleStore) deleteKey(key string) error {
	return s.applyViaRaft(OpDelete, key, nil)
}

// scanPrefix calls fn for each key-value pair matching the given prefix.
func (s *PebbleStore) scanPrefix(prefix string, fn func(key []byte, value []byte) error) error {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return err
		}
		if err := fn(iter.Key(), val); err != nil {
			return err
		}
	}
	return nil
}

// scanPrefixWithLimit returns up to `limit` entries matching prefix.
func (s *PebbleStore) scanPrefixWithLimit(prefix string, limit int) (keys [][]byte, vals [][]byte, err error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid() && count < limit; iter.Next() {
		k := make([]byte, len(iter.Key()))
		copy(k, iter.Key())
		v, err := iter.ValueAndErr()
		if err != nil {
			return nil, nil, err
		}
		vc := make([]byte, len(v))
		copy(vc, v)
		keys = append(keys, k)
		vals = append(vals, vc)
		count++
	}
	return keys, vals, nil
}

// ScanPageResult holds the results of a paginated scan.
type ScanPageResult struct {
	Keys    [][]byte
	Values  [][]byte
	NextKey []byte // nil if no more pages
	HasMore bool
}

// scanPrefixPaged returns a page of entries matching prefix, starting after
// the given cursor key. This avoids loading all matching entries into memory
// at once, which is critical for large datasets (millions of inodes/chunks).
//
// Usage:
//
//	cursor := []byte(nil) // start from beginning
//	for {
//	    page, err := store.scanPrefixPaged(prefix, cursor, pageSize)
//	    // process page.Keys, page.Values
//	    if !page.HasMore { break }
//	    cursor = page.NextKey
//	}
func (s *PebbleStore) scanPrefixPaged(prefix string, cursor []byte, pageSize int) (*ScanPageResult, error) {
	if pageSize <= 0 {
		pageSize = 1000
	}

	opts := &pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: prefixUpperBound(prefix),
	}

	// If cursor is provided, start after that key
	if cursor != nil {
		opts.LowerBound = cursor
	}

	iter, err := s.db.NewIter(opts)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	result := &ScanPageResult{}
	count := 0

	// Position iterator
	if cursor != nil {
		// Seek to cursor and skip past it
		iter.SeekGE(cursor)
		if iter.Valid() && bytes.Equal(iter.Key(), cursor) {
			iter.Next()
		}
	} else {
		iter.First()
	}

	for ; iter.Valid(); iter.Next() {
		// Check if we've exceeded the page size (fetch one extra to detect HasMore)
		if count >= pageSize+1 {
			break
		}

		k := make([]byte, len(iter.Key()))
		copy(k, iter.Key())
		v, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}
		vc := make([]byte, len(v))
		copy(vc, v)

		result.Keys = append(result.Keys, k)
		result.Values = append(result.Values, vc)
		count++
	}

	// If we got more than pageSize, there are more pages
	if len(result.Keys) > pageSize {
		result.NextKey = result.Keys[pageSize]
		result.Keys = result.Keys[:pageSize]
		result.Values = result.Values[:pageSize]
		result.HasMore = true
	}

	return result, nil
}

// prefixUpperBound returns the lexicographic upper bound for a prefix scan.
// e.g., "/ns/5/" → "/ns/50" (increment last byte)
func prefixUpperBound(prefix string) []byte {
	b := []byte(prefix)
	if len(b) == 0 {
		return nil
	}
	// Find last byte that can be incremented
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b = b[:i+1]
			b[i]++
			return b
		}
	}
	return nil // all 0xFF
}

// ========== BucketService Implementation ==========

func (s *PebbleStore) CreateBucket(ctx context.Context, name string, policy PlacementPolicy) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if len(name) == 0 || len(name) > MaxNameLength {
		return ErrInvalidArgument
	}

	bucketKey := prefixBucket + name
	var existing BucketInfo
	exists, err := s.getJSON(bucketKey, &existing)
	if err != nil {
		return err
	}
	if exists {
		return ErrBucketExists
	}

	rootID := s.nextInodeID()
	now := time.Now().UnixNano()
	root := &InodeMeta{
		ID:         rootID,
		Type:       FileDirectory,
		Mode:       0755,
		NLink:      2,
		BucketRoot: rootID,
		CTime:      now,
		MTime:      now,
		ATime:      now,
	}
	info := &BucketInfo{
		Name:         name,
		RootInode:    rootID,
		Policy:       policy,
		CreationDate: time.Now(),
	}
	ops := []batchJSONOp{
		{Key: fmt.Sprintf("%s%d", prefixInode, rootID), Value: root},
		{Key: bucketKey, Value: info},
		{Key: fmt.Sprintf("%s%s", prefixPolicy, name), Value: &policy},
	}
	if s.cfg.UseBucketStats {
		ops = append(ops, batchJSONOp{
			Key:   s.bucketStatsKey(rootID),
			Value: &BucketUsage{Name: name},
		})
	}
	return s.applyBatchJSON(ops, nil)
}

func (s *PebbleStore) DeleteBucket(ctx context.Context, name string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	bucketKey := prefixBucket + name
	var info BucketInfo
	exists, err := s.getJSON(bucketKey, &info)
	if err != nil {
		return err
	}
	if !exists {
		return ErrBucketNotFound
	}

	// Check if bucket root has children
	nsPrefix := fmt.Sprintf("%s%d/", prefixNS, info.RootInode)
	_, vals, err := s.scanPrefixWithLimit(nsPrefix, 1)
	if err != nil {
		return err
	}
	if len(vals) > 0 {
		return ErrBucketNotEmpty
	}

	deletes := []string{
		bucketKey,
		fmt.Sprintf("%s%d", prefixInode, info.RootInode),
		fmt.Sprintf("%s%s", prefixPolicy, name),
	}
	if s.cfg.UseBucketStats {
		deletes = append(deletes, s.bucketStatsKey(info.RootInode))
	}
	return s.applyBatchJSON(nil, deletes)
}

func (s *PebbleStore) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	var buckets []BucketInfo
	err := s.scanPrefix(prefixBucket, func(key, val []byte) error {
		var b BucketInfo
		if err := unmarshalValue(val, &b); err == nil {
			buckets = append(buckets, b)
		}
		return nil
	})
	return buckets, err
}

func (s *PebbleStore) GetBucket(ctx context.Context, name string) (*BucketInfo, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var info BucketInfo
	exists, err := s.getJSON(prefixBucket+name, &info)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrBucketNotFound
	}
	return &info, nil
}

// ========== NamespaceService Implementation ==========

func (s *PebbleStore) MkDir(ctx context.Context, parent InodeID, name string, mode uint32) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if len(name) > MaxNameLength {
		return nil, ErrNameTooLong
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var existing DirEntry
	exists, err := s.getJSON(nsKey, &existing)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrEntryExists
	}

	inodeID := s.nextInodeID()
	now := time.Now().UnixNano()
	meta := &InodeMeta{
		ID:    inodeID,
		Type:  FileDirectory,
		Mode:  mode,
		NLink: 2,
		CTime: now,
		MTime: now,
		ATime: now,
	}
	entry := &DirEntry{InodeID: inodeID, Type: FileDirectory, Name: name}

	// Update parent + inherit BucketRoot
	var parentMeta InodeMeta
	parentKey := fmt.Sprintf("%s%d", prefixInode, parent)
	pExists, _ := s.getJSON(parentKey, &parentMeta)
	if pExists {
		meta.BucketRoot = parentMeta.BucketRoot
		parentMeta.NLink++
		parentMeta.MTime = now
	}

	ops := []batchJSONOp{
		{Key: fmt.Sprintf("%s%d", prefixInode, inodeID), Value: meta},
		{Key: nsKey, Value: entry},
	}
	if pExists {
		ops = append(ops, batchJSONOp{Key: parentKey, Value: &parentMeta})
	}

	if err := s.applyBatchJSON(ops, nil); err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *PebbleStore) RmDir(ctx context.Context, parent InodeID, name string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var entry DirEntry
	exists, err := s.getJSON(nsKey, &entry)
	if err != nil {
		return err
	}
	if !exists {
		return ErrEntryNotFound
	}
	if entry.Type != FileDirectory {
		return ErrNotDirectory
	}

	// Check not empty
	childPrefix := fmt.Sprintf("%s%d/", prefixNS, entry.InodeID)
	_, vals, err := s.scanPrefixWithLimit(childPrefix, 1)
	if err != nil {
		return err
	}
	if len(vals) > 0 {
		return ErrDirNotEmpty
	}

	deletes := []string{
		fmt.Sprintf("%s%d", prefixInode, entry.InodeID),
		nsKey,
	}

	// Update parent nlink
	var parentMeta InodeMeta
	parentKey := fmt.Sprintf("%s%d", prefixInode, parent)
	pExists, _ := s.getJSON(parentKey, &parentMeta)
	if pExists {
		parentMeta.MTime = time.Now().UnixNano()
		if parentMeta.NLink > 0 {
			parentMeta.NLink--
		}
		ops := []batchJSONOp{
			{Key: parentKey, Value: &parentMeta},
		}
		s.releaseInodeID(entry.InodeID)
		return s.applyBatchJSON(ops, deletes)
	}
	s.releaseInodeID(entry.InodeID)
	return s.applyBatchJSON(nil, deletes)
}

func (s *PebbleStore) ReadDir(ctx context.Context, parent InodeID, offset int, limit int) ([]DirEntry, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	// Cap limit to prevent OOM on large directories.
	const maxReadDirEntries = 10_000
	if limit <= 0 || limit > maxReadDirEntries {
		limit = maxReadDirEntries
	}

	prefix := fmt.Sprintf("%s%d/", prefixNS, parent)
	var entries []DirEntry
	var skipped int

	// Paginate at scan level: skip |offset| entries, collect |limit| entries,
	// then stop scanning. This avoids loading the entire directory into memory.
	err := s.scanPrefix(prefix, func(key, val []byte) error {
		if len(entries) >= limit {
			return errStopIteration
		}
		name := strings.TrimPrefix(string(key), prefix)
		if name == "" {
			return nil
		}
		// Skip entries before the offset
		if skipped < offset {
			skipped++
			return nil
		}
		var entry DirEntry
		if err := unmarshalValue(val, &entry); err != nil {
			return nil
		}
		entry.Name = name
		entries = append(entries, entry)
		return nil
	})
	if err != nil && err != errStopIteration {
		return nil, err
	}

	return entries, nil
}

func (s *PebbleStore) CreateFile(ctx context.Context, parent InodeID, name string, mode uint32) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if len(name) > MaxNameLength {
		return nil, ErrNameTooLong
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var existing DirEntry
	exists, err := s.getJSON(nsKey, &existing)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrEntryExists
	}

	inodeID := s.nextInodeID()
	now := time.Now().UnixNano()
	bucketRoot := s.getBucketRoot(parent)
	meta := &InodeMeta{
		ID: inodeID, Type: FileRegular, Mode: mode, NLink: 1,
		BucketRoot: bucketRoot,
		CTime:      now, MTime: now, ATime: now,
	}
	entry := &DirEntry{InodeID: inodeID, Type: FileRegular, Name: name}
	ops := []batchJSONOp{
		{Key: fmt.Sprintf("%s%d", prefixInode, inodeID), Value: meta},
		{Key: nsKey, Value: entry},
	}
	s.addBucketStatsOp(bucketRoot, 0, 1, &ops)
	if err := s.applyBatchJSON(ops, nil); err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *PebbleStore) Unlink(ctx context.Context, parent InodeID, name string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var entry DirEntry
	exists, err := s.getJSON(nsKey, &entry)
	if err != nil {
		return err
	}
	if !exists {
		return ErrEntryNotFound
	}
	if entry.Type == FileDirectory {
		return ErrNotFile
	}

	var meta InodeMeta
	inodeKey := fmt.Sprintf("%s%d", prefixInode, entry.InodeID)
	pExists, _ := s.getJSON(inodeKey, &meta)
	ops := []batchJSONOp{}
	deletes := []string{nsKey}

	if pExists {
		meta.NLink--
		meta.MTime = time.Now().UnixNano()
		if meta.NLink <= 0 {
			s.addBucketStatsOp(meta.BucketRoot, -meta.Size, -1, &ops)
			deletes = append(deletes, inodeKey)
			s.releaseInodeID(meta.ID)
		} else {
			ops = append(ops, batchJSONOp{Key: inodeKey, Value: &meta})
		}
	}
	return s.applyBatchJSON(ops, deletes)
}

func (s *PebbleStore) Lookup(ctx context.Context, parent InodeID, name string) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	start := time.Now()

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var entry DirEntry
	exists, err := s.getJSON(nsKey, &entry)
	if err != nil {
		if s.metrics != nil {
			s.metrics.RecordError()
		}
		return nil, err
	}
	if !exists {
		if s.metrics != nil {
			s.metrics.RecordRead(time.Since(start))
		}
		return nil, ErrEntryNotFound
	}

	var meta InodeMeta
	exists, err = s.getJSON(fmt.Sprintf("%s%d", prefixInode, entry.InodeID), &meta)
	if err != nil {
		if s.metrics != nil {
			s.metrics.RecordError()
		}
		return nil, err
	}
	if !exists {
		if s.metrics != nil {
			s.metrics.RecordRead(time.Since(start))
		}
		return nil, ErrInodeNotFound
	}
	if s.metrics != nil {
		s.metrics.RecordRead(time.Since(start))
	}
	return &meta, nil
}

func (s *PebbleStore) GetInode(ctx context.Context, id InodeID) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	start := time.Now()

	if cached := s.inCache.get(id); cached != nil {
		if s.metrics != nil {
			s.metrics.RecordCacheHit()
			s.metrics.RecordRead(time.Since(start))
		}
		// Deep copy to prevent callers from mutating cached data.
		// ChunkMap and XAttrs are reference types that must be cloned.
		cp := *cached
		if len(cached.ChunkMap) > 0 {
			cp.ChunkMap = make([]ChunkRef, len(cached.ChunkMap))
			copy(cp.ChunkMap, cached.ChunkMap)
		}
		if len(cached.XAttrs) > 0 {
			cp.XAttrs = make(map[string][]byte, len(cached.XAttrs))
			for k, v := range cached.XAttrs {
				vc := make([]byte, len(v))
				copy(vc, v)
				cp.XAttrs[k] = vc
			}
		}
		return &cp, nil
	}
	if s.metrics != nil {
		s.metrics.RecordCacheMiss()
	}
	var meta InodeMeta
	exists, err := s.getJSON(fmt.Sprintf("%s%d", prefixInode, id), &meta)
	if err != nil {
		if s.metrics != nil {
			s.metrics.RecordError()
		}
		return nil, err
	}
	if !exists {
		if s.metrics != nil {
			s.metrics.RecordRead(time.Since(start))
		}
		return nil, ErrInodeNotFound
	}
	s.inCache.put(id, &meta)
	if s.metrics != nil {
		s.metrics.RecordRead(time.Since(start))
	}
	return &meta, nil
}

func (s *PebbleStore) UpdateInode(ctx context.Context, meta *InodeMeta) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if meta == nil {
		return ErrInvalidArgument
	}
	// Invalidate cache — next GetInode fetches fresh data
	s.inCache.del(meta.ID)

	meta.CTime = time.Now().UnixNano()
	ops := []batchJSONOp{
		{Key: fmt.Sprintf("%s%d", prefixInode, meta.ID), Value: meta},
	}

	// Only read old inode if bucket stats are enabled (avoid unnecessary Pebble I/O).
	// Most callers (e.g. SetXAttr/RemoveXAttr) don't change Size, so the delta read
	// is a no-op waste. This optimization saves a local Pebble + unmarshal per call.
	if s.cfg.UseBucketStats {
		var oldMeta InodeMeta
		if oldExists, _ := s.getJSON(fmt.Sprintf("%s%d", prefixInode, meta.ID), &oldMeta); oldExists {
			s.addBucketStatsOp(oldMeta.BucketRoot, meta.Size-oldMeta.Size, 0, &ops)
		}
	}
	return s.applyBatchJSON(ops, nil)
}

func (s *PebbleStore) Rename(ctx context.Context, oldParent InodeID, oldName string, newParent InodeID, newName string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	oldNSKey := fmt.Sprintf("%s%d/%s", prefixNS, oldParent, oldName)
	var entry DirEntry
	exists, err := s.getJSON(oldNSKey, &entry)
	if err != nil {
		return err
	}
	if !exists {
		return ErrEntryNotFound
	}

	// Check destination
	newNSKey := fmt.Sprintf("%s%d/%s", prefixNS, newParent, newName)
	var destEntry DirEntry
	destExists, _ := s.getJSON(newNSKey, &destEntry)
	if destExists {
		if destEntry.Type == FileDirectory {
			return ErrEntryExists
		}
		if err := s.Unlink(ctx, newParent, newName); err != nil {
			return err
		}
	}

	// Cross-bucket rename check
	if s.cfg.UseBucketStats {
		oldRoot := s.getBucketRoot(oldParent)
		newRoot := s.getBucketRoot(newParent)
		if oldRoot != newRoot {
			return ErrCrossBucketRename
		}
	}

	entry.Name = newName
	ops := []batchJSONOp{
		{Key: newNSKey, Value: &entry},
	}
	deletes := []string{oldNSKey}
	return s.applyBatchJSON(ops, deletes)
}

func (s *PebbleStore) Symlink(ctx context.Context, parent InodeID, name string, target string) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var existing DirEntry
	exists, err := s.getJSON(nsKey, &existing)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrEntryExists
	}

	inodeID := s.nextInodeID()
	now := time.Now().UnixNano()
	bucketRoot := s.getBucketRoot(parent)
	meta := &InodeMeta{
		ID: inodeID, Type: FileSymlink, Mode: 0777, NLink: 1, Symlink: target,
		BucketRoot: bucketRoot,
		CTime:      now, MTime: now, ATime: now,
	}
	entry := &DirEntry{InodeID: inodeID, Type: FileSymlink, Name: name}
	ops := []batchJSONOp{
		{Key: fmt.Sprintf("%s%d", prefixInode, inodeID), Value: meta},
		{Key: nsKey, Value: entry},
	}
	s.addBucketStatsOp(bucketRoot, int64(len(target)), 1, &ops)
	if err := s.applyBatchJSON(ops, nil); err != nil {
		return nil, err
	}
	return meta, nil
}
func (s *PebbleStore) Readlink(ctx context.Context, id InodeID) (string, error) {
	if s.closed.Load() {
		return "", ErrServiceClosed
	}
	var meta InodeMeta
	exists, err := s.getJSON(fmt.Sprintf("%s%d", prefixInode, id), &meta)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", ErrInodeNotFound
	}
	if meta.Type != FileSymlink {
		return "", ErrNotSymlink
	}
	return meta.Symlink, nil
}

func (s *PebbleStore) Link(ctx context.Context, parent InodeID, name string, target InodeID) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var existing DirEntry
	exists, err := s.getJSON(nsKey, &existing)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrEntryExists
	}

	var meta InodeMeta
	inodeKey := fmt.Sprintf("%s%d", prefixInode, target)
	exists, err = s.getJSON(inodeKey, &meta)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrInodeNotFound
	}
	if meta.Type == FileDirectory {
		return nil, ErrInvalidArgument
	}

	meta.NLink++
	meta.CTime = time.Now().UnixNano()
	entry := &DirEntry{InodeID: target, Type: meta.Type, Name: name}
	ops := []batchJSONOp{
		{Key: inodeKey, Value: &meta},
		{Key: nsKey, Value: entry},
	}
	if err := s.applyBatchJSON(ops, nil); err != nil {
		return nil, err
	}
	return &meta, nil
}

// ========== ChunkService Implementation ==========

func (s *PebbleStore) AllocateChunk(ctx context.Context, inodeID InodeID, offset int64, policy PlacementPolicy) (*ChunkMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	// Quota check: resolve bucket from inode and verify write is allowed
	if s.quota != nil {
		var meta InodeMeta
		inodeKey := fmt.Sprintf("%s%d", prefixInode, inodeID)
		exists, err := s.getJSON(inodeKey, &meta)
		if err != nil {
			return nil, err
		}
		if exists && meta.BucketRoot != 0 {
			bucketName := s.bucketNameByRoot(meta.BucketRoot)
			if bucketName != "" {
				if err := s.quota.CheckWrite(bucketName, MaxChunkSize); err != nil {
					return nil, err
				}
			}
		}
	}

	chunkID := s.chunkGen.Next()
	nodeIDs, err := s.placement.PlaceChunk(policy, nil)
	if err != nil {
		return nil, err
	}

	replicas := make([]ReplicaInfo, 0, len(nodeIDs))
	for _, nid := range nodeIDs {
		n, err := s.GetNode(ctx, nid)
		if err != nil {
			return nil, fmt.Errorf("allocate chunk: node %d not found: %w", nid, err)
		}
		replicas = append(replicas, ReplicaInfo{NodeID: nid, Addr: n.Addr, State: ReplicaSyncing})
	}

	chunk := &ChunkMeta{
		ID:         chunkID,
		Size:       MaxChunkSize,
		State:      ChunkCreated,
		Replicas:   replicas,
		Tier:       policy.StorageTier,
		CreateTime: time.Now().UnixNano(),
	}

	// Append to inode's chunk map
	var meta InodeMeta
	inodeKey := fmt.Sprintf("%s%d", prefixInode, inodeID)
	exists, err := s.getJSON(inodeKey, &meta)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrInodeNotFound
	}

	ref := ChunkRef{ID: chunkID, Offset: offset, Length: 0, Version: time.Now().UnixNano()}
	meta.ChunkMap = append(meta.ChunkMap, ref)
	meta.MTime = time.Now().UnixNano()
	ops := []batchJSONOp{
		{Key: fmt.Sprintf("%s%d", prefixChunk, chunkID), Value: chunk},
		{Key: inodeKey, Value: &meta},
	}
	if err := s.applyBatchJSON(ops, nil); err != nil {
		return nil, err
	}
	return chunk, nil
}

func (s *PebbleStore) CommitChunk(ctx context.Context, chunkID ChunkID, checksum uint32) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := fmt.Sprintf("%s%d", prefixChunk, chunkID)
	var chunk ChunkMeta
	exists, err := s.getJSON(key, &chunk)
	if err != nil {
		return err
	}
	if !exists {
		return ErrChunkNotFound
	}
	if chunk.State != ChunkCreated {
		return ErrChunkAlreadySealed
	}
	chunk.State = ChunkSealed
	chunk.Checksum = checksum
	return s.putMsgpack(key, &chunk)
}

func (s *PebbleStore) GetChunk(ctx context.Context, chunkID ChunkID) (*ChunkMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var chunk ChunkMeta
	exists, err := s.getJSON(fmt.Sprintf("%s%d", prefixChunk, chunkID), &chunk)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrChunkNotFound
	}
	return &chunk, nil
}

// UpdateChunk overwrites chunk metadata (e.g. to change tier or state).
func (s *PebbleStore) UpdateChunk(ctx context.Context, chunk *ChunkMeta) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := fmt.Sprintf("%s%d", prefixChunk, chunk.ID)
	// Verify chunk exists before update
	var existing ChunkMeta
	exists, err := s.getJSON(key, &existing)
	if err != nil {
		return err
	}
	if !exists {
		return ErrChunkNotFound
	}
	return s.putMsgpack(key, chunk)
}

func (s *PebbleStore) SealChunk(ctx context.Context, chunkID ChunkID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := fmt.Sprintf("%s%d", prefixChunk, chunkID)
	var chunk ChunkMeta
	exists, err := s.getJSON(key, &chunk)
	if err != nil {
		return err
	}
	if !exists {
		return ErrChunkNotFound
	}
	if chunk.State == ChunkReady {
		return nil
	}
	if chunk.State != ChunkSealed {
		return ErrChunkNotSealed
	}
	chunk.State = ChunkReady
	return s.putMsgpack(key, &chunk)
}

func (s *PebbleStore) ListChunks(ctx context.Context, inodeID InodeID) ([]ChunkRef, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var meta InodeMeta
	exists, err := s.getJSON(fmt.Sprintf("%s%d", prefixInode, inodeID), &meta)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrInodeNotFound
	}
	return meta.ChunkMap, nil
}

func (s *PebbleStore) DeleteChunk(ctx context.Context, chunkID ChunkID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.deleteKey(fmt.Sprintf("%s%d", prefixChunk, chunkID))
}

func (s *PebbleStore) ReportChunkState(ctx context.Context, nodeID NodeID, states map[ChunkID]ReplicaState) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if len(states) == 0 {
		return nil
	}
	return s.batchUpdateChunkStates(nodeID, states)
}

const maxBatchOps = 1000

// batchUpdateChunkStates updates replica states for multiple chunks in a single batch.
func (s *PebbleStore) batchUpdateChunkStates(nodeID NodeID, states map[ChunkID]ReplicaState) error {
	ops := make([]batchJSONOp, 0, len(states))
	deletes := make([]string, 0)

	for chunkID, state := range states {
		key := fmt.Sprintf("%s%d", prefixChunk, chunkID)
		var chunk ChunkMeta
		exists, err := s.getJSON(key, &chunk)
		if err != nil || !exists {
			continue
		}
		updated := false
		for i := range chunk.Replicas {
			if chunk.Replicas[i].NodeID == nodeID {
				chunk.Replicas[i].State = state
				updated = true
				break
			}
		}
		if !updated {
			chunk.Replicas = append(chunk.Replicas, ReplicaInfo{
				NodeID: nodeID,
				State:  state,
			})
		}
		ops = append(ops, batchJSONOp{Key: key, Value: &chunk})

		// Flush in batches to avoid oversized Raft entries
		if len(ops) >= maxBatchOps {
			if err := s.applyBatchJSON(ops, deletes); err != nil {
				return err
			}
			ops = ops[:0]
		}
	}

	if len(ops) > 0 {
		return s.applyBatchJSON(ops, deletes)
	}
	return nil
}

// ========== ClusterService Implementation ==========

func (s *PebbleStore) RegisterNode(ctx context.Context, info *NodeInfo) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := prefixNode + fmt.Sprintf("%d", info.ID)
	var existing NodeInfo
	exists, err := s.getJSON(key, &existing)
	if err != nil {
		return err
	}
	if exists {
		return ErrNodeAlreadyExists
	}
	info.State = NodeOnline
	info.LastSeen = time.Now().UnixNano()
	if err := s.putMsgpack(key, info); err != nil {
		return err
	}
	s.placement.UpdateNode(info)
	s.publishNodeEvent(key, info)

	// Auto-rebalance: trigger in background if cluster is imbalanced
	if s.autoRebalance {
		go s.triggerAutoRebalance()
	}

	return nil
}

// triggerAutoRebalance checks cluster balance and triggers rebalance if needed.
// It uses a mutex to prevent concurrent runs.
func (s *PebbleStore) triggerAutoRebalance() {
	if !s.rebalanceMu.TryLock() {
		slog.Info("auto-rebalance: already running, skipping")
		return
	}
	defer s.rebalanceMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	nodes, err := s.ListNodes(ctx)
	if err != nil {
		slog.Error("auto-rebalance: failed to list nodes", "error", err)
		return
	}

	if len(nodes) < 2 {
		return // nothing to rebalance
	}

	planner := &RebalancePlanner{}
	result := planner.PlanRebalance(nodes, 0.15) // 15% imbalance threshold
	if result == nil || len(result.Plans) == 0 {
		slog.Info("auto-rebalance: cluster is balanced, no action needed")
		return
	}

	slog.Info("auto-rebalance: triggering rebalance",
		"plans", len(result.Plans),
		"imbalance", fmt.Sprintf("%.1f%%", result.Imbalance*100))

	executor := NewRebalanceExecutor(s)
	if err := executor.ExecutePlans(ctx, result.Plans); err != nil {
		slog.Error("auto-rebalance: execution failed", "error", err)
		return
	}
	slog.Info("auto-rebalance: completed successfully")
}

func (s *PebbleStore) Heartbeat(ctx context.Context, nodeID NodeID, report *NodeReport) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := prefixNode + fmt.Sprintf("%d", nodeID)
	var info NodeInfo
	exists, err := s.getJSON(key, &info)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNodeNotFound
	}
	info.LastSeen = time.Now().UnixNano()
	info.State = NodeOnline
	if report != nil {
		info.UsedGB = report.UsedGB
		info.ChunkCount = report.ChunkCount
		s.placement.UpdateLoad(nodeID, report.DiskIO)
	}
	if err := s.putMsgpack(key, &info); err != nil {
		return err
	}
	s.placement.UpdateNode(&info)
	s.publishNodeEvent(key, &info)
	if report != nil && len(report.ChunkStates) > 0 {
		return s.ReportChunkState(ctx, nodeID, report.ChunkStates)
	}
	return nil
}

func (s *PebbleStore) DecommissionNode(ctx context.Context, nodeID NodeID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := prefixNode + fmt.Sprintf("%d", nodeID)
	var info NodeInfo
	exists, err := s.getJSON(key, &info)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNodeNotFound
	}
	info.State = NodeDraining
	if err := s.putMsgpack(key, &info); err != nil {
		return err
	}
	s.placement.UpdateNode(&info)
	return nil
}

// EnterMaintenance transitions a node to maintenance mode for rolling upgrades.
// The node stops receiving new chunk allocations but continues serving reads.
// Existing chunks are migrated off before the node is taken down.
func (s *PebbleStore) EnterMaintenance(ctx context.Context, nodeID NodeID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := prefixNode + fmt.Sprintf("%d", nodeID)
	var info NodeInfo
	exists, err := s.getJSON(key, &info)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNodeNotFound
	}
	if info.State == NodeMaint {
		return nil // already in maintenance
	}
	info.State = NodeMaint
	if err := s.putMsgpack(key, &info); err != nil {
		return err
	}
	s.placement.UpdateNode(&info)
	slog.Info("node entered maintenance mode", "nodeID", nodeID)
	return nil
}

// ExitMaintenance transitions a node back to online after a rolling upgrade.
func (s *PebbleStore) ExitMaintenance(ctx context.Context, nodeID NodeID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := prefixNode + fmt.Sprintf("%d", nodeID)
	var info NodeInfo
	exists, err := s.getJSON(key, &info)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNodeNotFound
	}
	if info.State != NodeMaint {
		return fmt.Errorf("node %d is not in maintenance state (current: %s)", nodeID, info.State)
	}
	info.State = NodeOnline
	info.LastSeen = time.Now().UnixNano()
	if err := s.putMsgpack(key, &info); err != nil {
		return err
	}
	s.placement.UpdateNode(&info)
	slog.Info("node exited maintenance mode", "nodeID", nodeID)
	return nil
}

// RollingUpgradePlan generates a node-by-node upgrade order that maintains
// quorum and replication health. It returns nodes sorted by least impact first.
func (s *PebbleStore) RollingUpgradePlan(ctx context.Context) ([]NodeID, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	nodes, err := s.ListNodes(ctx)
	if err != nil {
		return nil, err
	}

	// Filter to online nodes only, sort by used capacity ascending (least data first)
	var online []NodeInfo
	for _, n := range nodes {
		if n.State == NodeOnline {
			online = append(online, n)
		}
	}
	sort.Slice(online, func(i, j int) bool {
		return online[i].UsedGB < online[j].UsedGB
	})

	result := make([]NodeID, len(online))
	for i, n := range online {
		result[i] = n.ID
	}
	return result, nil
}

func (s *PebbleStore) ListNodes(ctx context.Context) ([]NodeInfo, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var nodes []NodeInfo
	err := s.scanPrefix(prefixNode, func(key, val []byte) error {
		var n NodeInfo
		if err := unmarshalValue(val, &n); err == nil {
			nodes = append(nodes, n)
		}
		return nil
	})
	return nodes, err
}

func (s *PebbleStore) GetNode(ctx context.Context, nodeID NodeID) (*NodeInfo, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var info NodeInfo
	exists, err := s.getJSON(prefixNode+fmt.Sprintf("%d", nodeID), &info)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNodeNotFound
	}
	return &info, nil
}

// GetDynamicConfig returns the current dynamic config atomically.
func (s *PebbleStore) GetDynamicConfig() *DynamicConfig {
	return s.dynCfg.Load()
}

// SetDynamicConfig atomically replaces the dynamic config.
func (s *PebbleStore) SetDynamicConfig(cfg *DynamicConfig) {
	s.dynCfg.Store(cfg)
}

// GetDegradationManager returns the degradation manager for this store.
func (s *PebbleStore) GetDegradationManager() *DegradationManager {
	return s.degradation
}

// SetMetrics attaches a metrics collector to this store.
func (s *PebbleStore) SetMetrics(m *Metrics) {
	s.metrics = m
}

// DynamicConfigHandler returns an HTTP handler for viewing and updating
// dynamic configuration at runtime. GET /config returns the current config;
// PUT /config accepts JSON to update it.
// A shared secret (admin_token) is required for write operations to prevent
// unauthorized configuration changes in production.
func (s *PebbleStore) DynamicConfigHandler(adminToken string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(s.GetDynamicConfig())
		case http.MethodPut, http.MethodPost:
			// Require authentication for writes
			token := r.Header.Get("X-Admin-Token")
			if token == "" {
				token = r.URL.Query().Get("token")
			}
			if adminToken != "" && token != adminToken {
				http.Error(w, "unauthorized: invalid or missing admin token", http.StatusUnauthorized)
				return
			}
			var cfg DynamicConfig
			if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
				http.Error(w, fmt.Sprintf("invalid config: %v", err), http.StatusBadRequest)
				return
			}
			s.SetDynamicConfig(&cfg)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
			slog.Info("dynamic config updated", "config", fmt.Sprintf("%+v", cfg))
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	return mux
}

// ============================================================
// Backup & Restore
// ============================================================

// CreateBackup creates a snapshot backup of the Pebble database to the given
// directory. The backup is a full checkpoint that can be restored later.
// Uses Pebble's native Checkpoint API for crash-consistent snapshots.
func (s *PebbleStore) CreateBackup(ctx context.Context, backupDir string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("backup: create dir %s: %w", backupDir, err)
	}

	// Pebble Checkpoint creates a crash-consistent snapshot without blocking writes
	if err := s.db.Checkpoint(backupDir); err != nil {
		return fmt.Errorf("backup: checkpoint to %s: %w", backupDir, err)
	}

	slog.Info("backup: created checkpoint", "path", backupDir)
	return nil
}

// RestoreBackup restores the database from a previously created backup directory.
// The current database is closed and replaced. The caller must re-initialize
// any dependent services (PlacementEngine, Raft, etc.) after restore.
func (s *PebbleStore) RestoreBackup(ctx context.Context, backupDir string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	// Verify backup exists and looks valid
	info, err := os.Stat(backupDir)
	if err != nil {
		return fmt.Errorf("restore: backup dir %s: %w", backupDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("restore: %s is not a directory", backupDir)
	}

	// Generate a timestamped backup of the current data before replacing
	currentBackup := filepath.Join(filepath.Dir(s.cfg.Dir), "pre-restore-"+fmt.Sprintf("%d", time.Now().Unix()))
	slog.Info("restore: backing up current data before restore", "path", currentBackup)
	if err := s.db.Checkpoint(currentBackup); err != nil {
		slog.Warn("restore: pre-restore backup failed", "error", err)
	}

	// Close current database
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("restore: close current db: %w", err)
	}

	// Replace data directory with backup
	dataDir := s.cfg.Dir
	tmpDir := dataDir + ".restore-tmp"

	// Move current data to temp
	if err := os.Rename(dataDir, tmpDir); err != nil {
		return fmt.Errorf("restore: rename current data: %w", err)
	}

	// Copy backup to data dir
	if err := copyDir(backupDir, dataDir); err != nil {
		// Try to revert
		os.Rename(tmpDir, dataDir)
		return fmt.Errorf("restore: copy backup to data dir: %w", err)
	}

	// Remove temp dir
	os.RemoveAll(tmpDir)

	// Re-open database
	pebbleOpts := &pebble.Options{
		MemTableSize:                256 << 20,
		MemTableStopWritesThreshold: 8,
		MaxOpenFiles:                16384,
		FormatMajorVersion:          pebble.FormatNewest,
	}
	if s.cfg.MemTableSize > 0 {
		pebbleOpts.MemTableSize = s.cfg.MemTableSize
	}
	if s.cfg.MaxOpenFiles > 0 {
		pebbleOpts.MaxOpenFiles = s.cfg.MaxOpenFiles
	}

	newDB, err := pebble.Open(dataDir, pebbleOpts)
	if err != nil {
		return fmt.Errorf("restore: re-open db: %w", err)
	}
	s.db = newDB

	slog.Info("restore: successfully restored", "path", backupDir)
	return nil
}

// copyDir recursively copies a directory.
func copyDir(src, dst string) error {
	if err := os.MkdirAll(dst, 0755); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			srcFile, err := os.Open(srcPath)
			if err != nil {
				return err
			}
			dstFile, err := os.Create(dstPath)
			if err != nil {
				srcFile.Close()
				return err
			}
			_, err = io.Copy(dstFile, srcFile)
			srcFile.Close()
			dstFile.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ========== Additional PebbleStore Methods ==========

// ScanAllChunks iterates over all chunk metadata (used by repair/rebalance).
func (s *PebbleStore) ScanAllChunks(ctx context.Context, fn func(*ChunkMeta) error) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.scanPrefix(prefixChunk, func(key, val []byte) error {
		var chunk ChunkMeta
		if err := unmarshalValue(val, &chunk); err != nil {
			// Log and skip corrupted entries
			slog.Warn("pebble store: corrupted chunk entry", "key", string(key), "error", err)
			return nil
		}
		return fn(&chunk)
	})
}

// DB returns the underlying Pebble instance (for Raft snapshot/restore).
func (s *PebbleStore) DB() *pebble.DB {
	return s.db
}

// ========== RepairService Implementation ==========

func (s *PebbleStore) GetRepairQueue(ctx context.Context) ([]RepairTask, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var tasks []RepairTask
	err := s.scanPrefix(prefixRepair, func(key, val []byte) error {
		var task RepairTask
		if err := unmarshalValue(val, &task); err == nil {
			tasks = append(tasks, task)
		}
		return nil
	})
	return tasks, err
}

func (s *PebbleStore) TriggerRepair(ctx context.Context, chunkID ChunkID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := fmt.Sprintf("%s%d", prefixRepair, chunkID)
	task := RepairTask{
		ChunkID:   chunkID,
		Reason:    "triggered",
		CreatedAt: time.Now(),
	}
	return s.putJSON(key, &task)
}

func (s *PebbleStore) RemoveRepairTask(ctx context.Context, chunkID ChunkID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := fmt.Sprintf("%s%d", prefixRepair, chunkID)
	return s.deleteKey(key)
}

// ========== Raft-Aware Write Path ==========

// SetRaftNode configures the PebbleStore to use Raft for consensus.
// When set, all mutating operations are proposed to the Raft log first.
func (s *PebbleStore) SetRaftNode(node *RaftNode) {
	s.raft = node
}

// IsLeader returns true if this node is the Raft leader.
func (s *PebbleStore) IsLeader() bool {
	if s.raft == nil {
		return true // No Raft = always leader (single-node mode)
	}
	return s.raft.IsLeader()
}

// LeaderAddr returns the address of the current Raft leader.
func (s *PebbleStore) LeaderAddr() string {
	if s.raft == nil {
		return ""
	}
	return s.raft.LeaderAddr()
}

// LeaderOpsAddr returns the HTTP ops URL of the current Raft leader,
// so followers can redirect mutating requests. Empty if no leader.
func (s *PebbleStore) LeaderOpsAddr() string {
	if s.raft == nil {
		return ""
	}
	return s.raft.LeaderOpsAddr()
}

// applyBatch commits multiple key-value pairs atomically via Raft or directly.
// Values are serialized using msgpack for better performance on hot paths.
func (s *PebbleStore) applyBatch(ops []batchJSONOp, deletes []string) error {
	raftOps := make([]BatchOp, 0, len(ops)+len(deletes))
	for _, op := range ops {
		data, err := marshalValue(op.Value, codecMsgpack)
		if err != nil {
			return fmt.Errorf("marshal batch value: %w", err)
		}
		raftOps = append(raftOps, BatchOp{Key: []byte(op.Key), Value: data})
	}
	for _, key := range deletes {
		raftOps = append(raftOps, BatchOp{Delete: true, Key: []byte(key)})
	}
	return s.applyBatchViaRaft(raftOps)
}

// applyBatchJSON commits multiple JSON-encoded key-value pairs atomically via Raft or directly.
// Deprecated: prefer applyBatch which uses msgpack for hot paths.
// Kept for backward compatibility with existing code that explicitly wants JSON.
func (s *PebbleStore) applyBatchJSON(ops []batchJSONOp, deletes []string) error {
	return s.applyBatch(ops, deletes)
}

// applyViaRaft proposes a write operation through the Raft log.
// If Raft is not configured, the write is applied directly.
func (s *PebbleStore) applyViaRaft(op RaftLogOp, key string, value []byte) error {
	// Check degradation state before all mutating writes
	if s.degradation.IsReadOnly() {
		return ErrServiceClosed
	}

	start := time.Now()

	if s.raft == nil {
		if op == OpDelete {
			err := s.db.Delete([]byte(key), pebble.Sync)
			if err == nil && s.metrics != nil {
				s.metrics.RecordWrite(time.Since(start))
			}
			return err
		}
		err := s.db.Set([]byte(key), value, pebble.Sync)
		if err == nil && s.metrics != nil {
			s.metrics.RecordWrite(time.Since(start))
		}
		return err
	}

	entry := &RaftLogEntry{
		Op:    op,
		Key:   []byte(key),
		Value: value,
	}
	err := s.raft.ApplyAutoForward(entry, 10*time.Second)
	if err == nil && s.metrics != nil {
		s.metrics.RecordWrite(time.Since(start))
	}
	return err
}

// applyBatchViaRaft proposes a batch of operations through the Raft log.
// When Raft is not configured, applies directly in a Pebble batch.
func (s *PebbleStore) applyBatchViaRaft(ops []BatchOp) error {
	if len(ops) == 0 {
		return nil
	}

	// Check degradation state before writes
	if s.degradation.IsReadOnly() {
		return ErrServiceClosed
	}

	start := time.Now()

	if s.raft == nil {
		batch := s.db.NewBatch()
		defer batch.Close()
		for _, op := range ops {
			if op.Delete {
				batch.Delete(op.Key, nil)
			} else {
				batch.Set(op.Key, op.Value, nil)
			}
		}
		err := batch.Commit(pebble.Sync)
		if err == nil && s.metrics != nil {
			s.metrics.RecordWrite(time.Since(start))
		}
		return err
	}

	entry := &RaftLogEntry{
		Op:    OpBatch,
		Batch: ops,
	}
	err := s.raft.ApplyAutoForward(entry, 10*time.Second)
	if err == nil && s.metrics != nil {
		s.metrics.RecordWrite(time.Since(start))
	}
	return err
}

// ========== RebalanceService Implementation ==========

func (s *PebbleStore) TriggerRebalance(ctx context.Context) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	nodes, err := s.ListNodes(ctx)
	if err != nil {
		return err
	}

	planner := &RebalancePlanner{}

	// Build node→chunks map for concrete chunk IDs
	chunkMap := make(map[NodeID][]ChunkID)
	s.scanPrefix(prefixChunk, func(key, val []byte) error {
		var chunk ChunkMeta
		if err := unmarshalValue(val, &chunk); err != nil {
			return nil
		}
		for _, r := range chunk.Replicas {
			chunkMap[r.NodeID] = append(chunkMap[r.NodeID], chunk.ID)
		}
		return nil
	})

	result := planner.PlanRebalanceWithChunks(nodes, chunkMap, 0.1)
	if result.Balanced || len(result.Plans) == 0 {
		return nil
	}

	for _, plan := range result.Plans {
		if plan.ChunkID == 0 {
			continue
		}
		key := fmt.Sprintf("%s%d", prefixRepair, plan.ChunkID)
		task := RepairTask{
			ChunkID:   plan.ChunkID,
			Reason:    fmt.Sprintf("rebalance: node %d → %d", plan.SourceNode, plan.TargetNode),
			Priority:  2,
			CreatedAt: time.Now(),
		}
		if err := s.putJSON(key, &task); err != nil {
			return err
		}
	}
	return nil
}

// ChunksByNode scans all chunks and returns those with a replica on the given node.
func (s *PebbleStore) ChunksByNode(ctx context.Context, nodeID NodeID) ([]ChunkMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var result []ChunkMeta
	err := s.scanPrefix(prefixChunk, func(key, val []byte) error {
		var chunk ChunkMeta
		if err := unmarshalValue(val, &chunk); err != nil {
			return nil
		}
		for _, r := range chunk.Replicas {
			if r.NodeID == nodeID {
				result = append(result, chunk)
				break
			}
		}
		return nil
	})
	return result, err
}

// MigrateChunkReplica removes a replica from fromNode and adds one on toNode.
// This updates the metadata record only; actual data transfer happens via the repair queue.
func (s *PebbleStore) MigrateChunkReplica(ctx context.Context, chunkID ChunkID, fromNode, toNode NodeID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	chunk, err := s.GetChunk(ctx, chunkID)
	if err != nil {
		return err
	}

	// Get target node address
	var toAddr string
	nodes, _ := s.ListNodes(ctx)
	for _, n := range nodes {
		if n.ID == toNode {
			toAddr = n.Addr
			break
		}
	}
	if toAddr == "" {
		return fmt.Errorf("target node %d not found", toNode)
	}

	// Remove old replica
	newReplicas := make([]ReplicaInfo, 0, len(chunk.Replicas))
	for _, r := range chunk.Replicas {
		if r.NodeID != fromNode {
			newReplicas = append(newReplicas, r)
		}
	}

	// Add new replica
	newReplicas = append(newReplicas, ReplicaInfo{
		NodeID: toNode,
		Addr:   toAddr,
		State:  ReplicaSyncing,
	})

	chunk.Replicas = newReplicas
	return s.UpdateChunk(ctx, chunk)
}

// ScanAllInodes iterates over all inode metadata (used by lifecycle engine).
func (s *PebbleStore) ScanAllInodes(ctx context.Context, fn func(*InodeMeta) error) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.scanPrefix(prefixInode, func(key, val []byte) error {
		var inode InodeMeta
		if err := unmarshalValue(val, &inode); err != nil {
			// Log and skip corrupted entries
			slog.Warn("pebble store: corrupted inode entry", "key", string(key), "error", err)
			return nil
		}
		return fn(&inode)
	})
}

// ============================================================
// Bucket Usage (Admin)
// ============================================================

// BucketUsage holds aggregate usage for a single bucket.
type BucketUsage struct {
	Name      string `json:"name"`
	UsedBytes int64  `json:"used_bytes"`
	Objects   int    `json:"objects"`
}

func (s *PebbleStore) bucketStatsKey(rootInode InodeID) string {
	return fmt.Sprintf("%s%d", prefixBucketStats, rootInode)
}

// bucketNameByRoot looks up the bucket name given a root inode ID.
// It scans the bucket prefix to find the matching RootInode.
// Returns empty string if not found.
func (s *PebbleStore) bucketNameByRoot(rootInode InodeID) string {
	prefix := []byte(prefixBucket)
	iter, _ := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var info BucketInfo
		if err := json.Unmarshal(iter.Value(), &info); err == nil {
			if info.RootInode == rootInode {
				return info.Name
			}
		}
	}
	return ""
}

// readBucketStats reads the current counter for a bucket. Returns zeros
// if the key does not exist (e.g., pre-migration).
func (s *PebbleStore) readBucketStats(rootInode InodeID) BucketUsage {
	var stats BucketUsage
	s.getJSON(s.bucketStatsKey(rootInode), &stats)
	return stats
}

// addBucketStatsOp adds a counter update to the pending batch ops.
// No-op when UseBucketStats is disabled. The delta values are applied
// to the current counter read at call time. Race-safe for admin-level
// precision (S3 usage metrics are eventually consistent).
func (s *PebbleStore) addBucketStatsOp(rootInode InodeID, deltaBytes int64, deltaObjects int, ops *[]batchJSONOp) {
	if !s.cfg.UseBucketStats || rootInode == 0 {
		return
	}
	stats := s.readBucketStats(rootInode)
	stats.UsedBytes += deltaBytes
	stats.Objects += deltaObjects
	*ops = append(*ops, batchJSONOp{Key: s.bucketStatsKey(rootInode), Value: &stats})
}

// getBucketRoot reads the parent inode and returns its BucketRoot.
// Used when creating new inodes to inherit the containing bucket.
func (s *PebbleStore) getBucketRoot(parent InodeID) InodeID {
	var parentMeta InodeMeta
	ok, _ := s.getJSON(fmt.Sprintf("%s%d", prefixInode, parent), &parentMeta)
	if !ok {
		return 0
	}
	return parentMeta.BucketRoot
}

// ComputeAllBucketUsage returns per-bucket usage. When UseBucketStats
// is enabled, reads counters directly. Otherwise falls back to a
// full namespace+inode prefix scan.
func (s *PebbleStore) ComputeAllBucketUsage(ctx context.Context) ([]BucketUsage, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	buckets, err := s.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	if len(buckets) == 0 {
		return nil, nil
	}

	if s.cfg.UseBucketStats {
		// Fast path: read per-bucket counters directly
		result := make([]BucketUsage, 0, len(buckets))
		for _, b := range buckets {
			stats := s.readBucketStats(b.RootInode)
			stats.Name = b.Name
			result = append(result, stats)
		}
		return result, nil
	}

	// Slow path: scan namespace + inodes (pre-migration fallback)
	children := make(map[InodeID][]DirEntry, len(buckets)*2)
	if err := s.scanPrefix(prefixNS, func(key, val []byte) error {
		parentStr, name := splitNSPath(string(key))
		if name == "" {
			return nil
		}
		parent, err := strconv.ParseUint(parentStr, 10, 64)
		if err != nil {
			return nil
		}
		var entry DirEntry
		if err := unmarshalValue(val, &entry); err != nil {
			return nil
		}
		entry.Name = name
		children[InodeID(parent)] = append(children[InodeID(parent)], entry)
		return nil
	}); err != nil {
		return nil, err
	}

	needed := make(map[InodeID]struct{})
	for _, b := range buckets {
		collectFileInodes(children, b.RootInode, needed)
	}

	sizes := make(map[InodeID]int64, len(needed))
	if err := s.scanPrefix(prefixInode, func(key, val []byte) error {
		idStr := strings.TrimPrefix(string(key), prefixInode)
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			return nil
		}
		if _, ok := needed[InodeID(id)]; ok {
			var meta InodeMeta
			if err := unmarshalValue(val, &meta); err == nil {
				sizes[InodeID(id)] = meta.Size
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	result := make([]BucketUsage, 0, len(buckets))
	for _, b := range buckets {
		used, objs := walkUsageTree(children, sizes, b.RootInode)
		result = append(result, BucketUsage{
			Name:      b.Name,
			UsedBytes: used,
			Objects:   objs,
		})
	}
	return result, nil
}

func collectFileInodes(children map[InodeID][]DirEntry, parent InodeID, needed map[InodeID]struct{}) {
	for _, e := range children[parent] {
		if e.Type == FileDirectory {
			collectFileInodes(children, e.InodeID, needed)
		} else {
			needed[e.InodeID] = struct{}{}
		}
	}
}

func walkUsageTree(children map[InodeID][]DirEntry, sizes map[InodeID]int64, dir InodeID) (usedBytes int64, objects int) {
	for _, e := range children[dir] {
		if e.Type == FileDirectory {
			ub, objs := walkUsageTree(children, sizes, e.InodeID)
			usedBytes += ub
			objects += objs
		} else {
			objects++
			usedBytes += sizes[e.InodeID]
		}
	}
	return
}

// ScrubAllChunks iterates all chunks and calls the callback with
// replica health information. Used by nufs-cli scrub command.
func (s *PebbleStore) ScrubAllChunks(fn func(chunkID ChunkID, replicaCount, healthyCount int)) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	prefix := prefixChunk
	return s.scanPrefix(prefix, func(key, val []byte) error {
		var chunk ChunkMeta
		if err := unmarshalValue(val, &chunk); err != nil {
			return nil
		}
		healthy := 0
		for _, replica := range chunk.Replicas {
			if replica.State == ReplicaReady {
				healthy++
			}
		}
		fn(chunk.ID, len(chunk.Replicas), healthy)
		return nil
	})
}

// splitNSPath splits "/ns/{parent}/{name}" into parent ID and name.
// Trailing slash on directory names is preserved.
func splitNSPath(key string) (parent, name string) {
	s := strings.TrimPrefix(key, prefixNS)
	slash := strings.IndexByte(s, '/')
	if slash < 0 {
		return s, ""
	}
	return s[:slash], s[slash+1:]
}

// ========== AccessControlService Implementation ==========

// SetBucketPolicy stores the access control policy for a bucket.
func (s *PebbleStore) SetBucketPolicy(_ context.Context, bucket string, policy BucketPolicy) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	policy.Bucket = bucket
	return s.applyBatchJSON([]batchJSONOp{
		{Key: prefixACL + bucket, Value: &policy},
	}, nil)
}

// GetBucketPolicy retrieves the access control policy for a bucket.
func (s *PebbleStore) GetBucketPolicy(_ context.Context, bucket string) (*BucketPolicy, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var policy BucketPolicy
	exists, err := s.getJSON(prefixACL+bucket, &policy)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrAccessDenied // no policy = default deny
	}
	return &policy, nil
}

// DeleteBucketPolicy removes the access control policy for a bucket.
func (s *PebbleStore) DeleteBucketPolicy(_ context.Context, bucket string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.applyBatchJSON(nil, []string{prefixACL + bucket})
}

// ============================================================
// QuotaStore implementation — persist quota data to Pebble
// ============================================================

// SaveQuota persists a bucket's quota configuration to Pebble.
func (s *PebbleStore) SaveQuota(bucket string, quota *BucketQuota) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.putMsgpack(prefixQuota+bucket, quota)
}

// SaveUsage persists a bucket's usage data to Pebble.
func (s *PebbleStore) SaveUsage(bucket string, usage *BucketUsage) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.putMsgpack(prefixQuotaUsage+bucket, usage)
}

// loadQuotas restores previously persisted quota and usage data from Pebble
// into the QuotaManager's in-memory maps. Called during SetQuotaManager.
func (s *PebbleStore) loadQuotas(qm *QuotaManager) {
	// Load quotas
	s.scanPrefix(prefixQuota, func(key, val []byte) error {
		var quota BucketQuota
		if err := json.Unmarshal(val, &quota); err != nil {
			slog.Warn("quota: failed to unmarshal quota entry", "key", string(key), "error", err)
			return nil
		}
		bucket := strings.TrimPrefix(string(key), prefixQuota)
		qm.LoadQuota(bucket, &quota)
		return nil
	})

	// Load usage
	s.scanPrefix(prefixQuotaUsage, func(key, val []byte) error {
		var usage BucketUsage
		if err := json.Unmarshal(val, &usage); err != nil {
			slog.Warn("quota: failed to unmarshal usage entry", "key", string(key), "error", err)
			return nil
		}
		bucket := strings.TrimPrefix(string(key), prefixQuotaUsage)
		qm.LoadUsage(bucket, &usage)
		return nil
	})
}
