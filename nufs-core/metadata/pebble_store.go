package metadata

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
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
	// pgStore is the placement-group authority for the Metadata V2 serving
	// path (Task #56 Phase A). It shares this store's DB and is raft-backed
	// like the other component stores. Built lazily in NewPebbleStore.
	pgStore  *PlacementGroupStore
	chunkGen *ChunkIDGenerator
	inodeSeq atomic.Uint64

	// chunkIDMax / chunkIDMaxInit bound chunk-ID minting to stay strictly above
	// the largest chunk ID already committed to this store (the cold-cache scan
	// in ensureChunkIDMax, kept current by advanceChunkIDMax). This closes the
	// cross-leader-failover and process-restart chunk-ID-reuse hole; see
	// buildAllocatedChunks.
	chunkIDMax     atomic.Uint64
	chunkIDMaxInit atomic.Bool

	// inodeIDMaxInit gates the one-time cold-cache scan in ensureInodeIDMax,
	// which raises inodeSeq strictly above the largest inode ID already
	// committed to this store. inodeSeq is a process-local counter reset to
	// RootInodeID at every store construction and never raft-replicated, so a
	// newly elected leader or restarted process could otherwise re-issue IDs
	// another leader already committed — silently overwriting an unrelated
	// inode row (the multi-metad leader-failover drill's 36/99 corruption).
	// The counter itself is the "largest minted" high mark once seeded; see
	// ensureInodeIDMax / nextInodeID.
	inodeIDMaxInit atomic.Bool

	closed atomic.Bool
	mu     sync.RWMutex
	cfg    PebbleStoreConfig

	// Dynamic config — swapped atomically via atomic.Pointer so reads are lock-free.
	// Use GetDynamicConfig() / SetDynamicConfig() to access.
	dynCfg atomic.Pointer[DynamicConfig]

	// Degradation manager for graceful degradation
	degradation *DegradationManager

	// Health checker (populated by ServiceBundle)
	health *HealthChecker

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

	// bucketCache caches GetBucket results to avoid repeated Pebble reads.
	// Key: bucket name, Value: *BucketInfo. Entries are invalidated on
	// CreateBucket / DeleteBucket.
	bucketCache sync.Map

	// Optional EventBus for publishing change notifications.
	// Set by SetEventBus() after initialization.
	events *EventBus

	// Quota enforcement for bucket writes
	quota *QuotaManager

	// Auto-rebalance on node registration
	autoRebalance bool
	rebalanceMu   sync.Mutex // prevents concurrent rebalance runs

	// Lease manager for node liveness detection
	lease *LeaseManager

	// Node throttling — protects RegisterNode/Heartbeat from
	// flooding during cluster topology changes. Nil-safe:
	// when nil, throttling is disabled (test-harness default).
	throttle *NodeRegistrationThrottle

	// Test-only deterministic interleaving points for tombstone conditionals.
	// They are nil in production and deliberately live beside the shared store
	// mutex so standalone race tests exercise the real mutation paths.
	chunkTombstoneBeforeConditional func()
	chunkPurgeBeforeConditional     func()
	chunkUpdateBeforeConditional    func()
	conditionalBatchBeforeCommit    func() error
	chunkIDNext                     func() ChunkID
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
	PlacementSpreadEnabled   bool    `json:"placement_spread_enabled"`
	PlacementWeightedChoice  bool    `json:"placement_weighted_choice"`
	PlacementErrorRateFilter float64 `json:"placement_error_rate_filter"` // Filter nodes above this error rate (0.0-1.0)
	PlacementWeightCapacity  float64 `json:"placement_weight_capacity"`   // Scoring weight for free capacity
	PlacementWeightLoad      float64 `json:"placement_weight_load"`       // Scoring weight for low load
	PlacementWeightTier      float64 `json:"placement_weight_tier"`       // Scoring weight for tier match
	PlacementWeightHealth    float64 `json:"placement_weight_health"`     // Scoring weight for low error rate

	// Readiness
	ReadinessRepairQueueThreshold int64 `json:"readiness_repair_queue_threshold"` // Repair backlog above this → degraded
}

// DefaultDynamicConfig returns safe production defaults for all dynamic configs.
func DefaultDynamicConfig() DynamicConfig {
	return DynamicConfig{
		GCEnabled:                     true,
		GCInterval:                    15 * time.Minute,
		GCChunkBatchSize:              1000,
		GCThresholdPercent:            0.0, // GC if any orphaned chunk
		GCDryRun:                      false,
		HeartbeatTTLSeconds:           30,
		HeartbeatCheckInterval:        5,
		AutoRepairEnabled:             true,
		WriteBatchingEnabled:          true,
		WriteBatchMaxSize:             256,
		WriteBatchMaxWait:             50 * time.Millisecond,
		CacheEnabled:                  true,
		CacheMaxSize:                  65536,
		RaftPreVoteEnabled:            true,
		PlacementSpreadEnabled:        true,
		PlacementWeightedChoice:       false,
		PlacementErrorRateFilter:      0.8,
		PlacementWeightCapacity:       0.4,
		PlacementWeightLoad:           0.25,
		PlacementWeightTier:           0.2,
		PlacementWeightHealth:         0.15,
		ReadinessRepairQueueThreshold: 1000,
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

// batchOp represents a single key-value write (msgpack-serialized) in an atomic batch.
type batchOp struct {
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
	s.placement.SetConfigProvider(s.GetDynamicConfig)
	// Placement-group authority for the Metadata V2 serving path.
	s.pgStore = NewPlacementGroupStore(s)

	if err := s.initRootInode(); err != nil {
		db.Close()
		return nil, err
	}
	s.restoreFreeList()
	if cfg.UseBucketStats {
		if err := s.ensureBucketStats(context.Background()); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("pebble store: initialize bucket stats: %w", err)
		}
	}
	return s, nil
}

// Close shuts down the store.
func (s *PebbleStore) Close() error {
	if s.closed.Swap(true) {
		return ErrServiceClosed
	}
	var errs []error
	if s.raft != nil {
		if err := s.raft.Shutdown(); err != nil {
			errs = append(errs, err)
		}
		s.raft = nil
	}
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

// Events returns the attached event bus; may be nil if watch is disabled.
func (s *PebbleStore) Events() *EventBus {
	return s.events
}

// SetLeaseManager registers the lease manager for node liveness detection.
func (s *PebbleStore) SetLeaseManager(lm *LeaseManager) {
	s.lease = lm
}

// SetNodeThrottle installs or replaces the registration/heartbeat
// rate limiter. Passing nil disables throttling.
func (s *PebbleStore) SetNodeThrottle(t *NodeRegistrationThrottle) {
	s.throttle = t
}

// GetNodeThrottle returns the installed limiter (may be nil).
func (s *PebbleStore) GetNodeThrottle() *NodeRegistrationThrottle {
	return s.throttle
}

// SetQuotaManager registers the quota manager and wires PebbleStore as its
// persistence backend. Gateway committers use the manager for write admission.
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
	return s.putMsgpack(key, root)
}

func (s *PebbleStore) nextInodeID() InodeID {
	// Try recycled IDs first
	s.inodeFreeMu.Lock()
	if len(s.inodeFreeList) > 0 {
		id := s.inodeFreeList[len(s.inodeFreeList)-1]
		s.inodeFreeList = s.inodeFreeList[:len(s.inodeFreeList)-1]
		s.inodeFreeMu.Unlock()
		// Remove from persistent store asynchronously — if we crash,
		// the ID may be re-issued, but the inode it belonged to is
		// already deleted, so the worst case is a harmless extra
		// ErrEntryExists on create.
		_ = s.deleteKey(fmt.Sprintf("%s%d", prefixFreeList, id))
		return id
	}
	s.inodeFreeMu.Unlock()

	// Seed the counter above the largest inode ID already committed to this
	// store before minting a fresh ID (cold-cache scan, once per process).
	// Without this, a newly elected raft leader or restarted process re-mints
	// from RootInodeID+1 and silently overwrites inode rows a previous leader
	// already committed — the multi-metad leader-failover drill's 36/99
	// corruption. The free-list path above does not need the scan: recycled
	// IDs are always ≤ the committed maximum and their rows are already
	// deleted (they only enter the list via a committed Unlink/RmDir).
	s.ensureInodeIDMax()
	return InodeID(s.inodeSeq.Add(1))
}

// releaseInodeID returns an inode ID to the free list for recycling.
// Called when a file or directory is permanently deleted.
// The recycled ID is persisted to Pebble so it survives restarts.
func (s *PebbleStore) releaseInodeID(id InodeID) {
	if id <= RootInodeID {
		return // Never recycle root or reserved IDs
	}
	s.inodeFreeMu.Lock()
	// Cap free list to prevent unbounded memory growth
	if len(s.inodeFreeList) < 65536 {
		s.inodeFreeList = append(s.inodeFreeList, id)
		s.inodeFreeMu.Unlock()
		// Persist to Pebble — best-effort, loss is acceptable
		_ = s.putMsgpack(fmt.Sprintf("%s%d", prefixFreeList, id), &id)
		return
	}
	s.inodeFreeMu.Unlock()
}

// restoreFreeList loads recycled inode IDs from Pebble on startup.
// This ensures that IDs released before a restart are still available
// for recycling, preventing inode ID space exhaustion.
func (s *PebbleStore) restoreFreeList() {
	s.inodeFreeMu.Lock()
	defer s.inodeFreeMu.Unlock()

	s.scanPrefix(prefixFreeList, func(key, val []byte) error {
		var id InodeID
		if err := unmarshalValue(val, &id); err != nil {
			return nil // skip malformed entries
		}
		s.inodeFreeList = append(s.inodeFreeList, id)
		return nil
	})

	if len(s.inodeFreeList) > 0 {
		slog.Info("pebble store: restored inode free list", "count", len(s.inodeFreeList))
	}
}

func (s *PebbleStore) putMsgpack(key string, v interface{}) error {
	data, err := marshalValue(v, codecMsgpack)
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
// getRawBytes returns the current raw bytes stored at key, or (nil, false)
// when absent. Unlike getValue it does not decode; it returns the exact on-disk
// getRaw fetches a single Pebble key and returns a copy of the value.
// Returns (true, value, nil) on hit; (false, nil, nil) on miss;
// (false, nil, err) on I/O error.  Callers that need richer error
// messages wrap the returned error themselves.
func (s *PebbleStore) getRaw(key string) (found bool, value []byte, err error) {
	val, closer, err := s.db.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	defer closer.Close()
	value = make([]byte, len(val))
	copy(value, val)
	return true, value, nil
}

// getRawBytes is a legacy alias kept for call-sites that expect the
// (value, found, err) return order.  It delegates to getRaw.
func (s *PebbleStore) getRawBytes(key string) ([]byte, bool, error) {
	found, value, err := s.getRaw(key)
	return value, found, err
}

// getValuesBatch fetches multiple keys in a single pass using a
// Pebble iterator. It returns a map of key → raw value bytes for
// all keys that exist. This avoids N independent Get calls (each
// of which does its own seek), replacing them with one sorted scan
// that seeks to each key in order (P2.10).
//
// The caller is responsible for unmarshalling the raw bytes.
func (s *PebbleStore) getValuesBatch(keys []string) (map[string][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// Sort keys so we can use SeekGE in ascending order, which is
	// the access pattern Pebble's iterator is optimized for.
	sortedKeys := make([]string, len(keys))
	copy(sortedKeys, keys)
	sort.Strings(sortedKeys)

	result := make(map[string][]byte, len(keys))
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return nil, fmt.Errorf("pebble store: new iter: %w", err)
	}
	defer iter.Close()

	for _, key := range sortedKeys {
		if !iter.SeekGE([]byte(key)) {
			break
		}
		if !iter.Valid() {
			break
		}
		// Pebble may position us at a key greater than what we asked
		// for; only consume if it's an exact match.
		if string(iter.Key()) != key {
			continue
		}
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("pebble store: iter value %q: %w", key, err)
		}
		// Copy val because the buffer is invalidated on Next/Seek.
		data := make([]byte, len(val))
		copy(data, val)
		result[key] = data
	}

	return result, nil
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

	// If cursor is provided, start after that key. Note: an empty (non-nil)
	// cursor must behave like no cursor — otherwise the empty LowerBound would
	// replace the prefix bound and the scan would start at the first key of
	// the whole keyspace.
	if len(cursor) > 0 {
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
	if len(cursor) > 0 {
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

	// If we got more than pageSize, there are more pages. NextKey is an
	// exclusive-start cursor, so it must be the last RETURNED key, not the
	// first unread one — the next page skips keys <= cursor.
	if len(result.Keys) > pageSize {
		result.Keys = result.Keys[:pageSize]
		result.Values = result.Values[:pageSize]
		result.NextKey = result.Keys[pageSize-1]
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
	if len(name) == 0 || len(name) > MaxNameLength || strings.Contains(name, "/") {
		return ErrInvalidArgument
	}

	bucketKey := prefixBucket + name
	var existing BucketInfo
	exists, err := s.getValue(bucketKey, &existing)
	if err != nil {
		return err
	}
	if exists {
		return ErrBucketExists
	}

	// The create is a conditional batch with ExpectAbsent preconditions on both
	// the bucket row and the freshly minted /inode/<id> row (the same guard the
	// namespace create paths use). The inode guard is defense-in-depth against
	// inode-ID reuse: under the under-seeded cold-scan window (a newly elected
	// raft leader whose FSM has not yet replayed a prior leader's inodes), a
	// stale mint would otherwise silently overwrite a live inode row with the
	// bucket root — the multi-metad leader-failover drill's 36/99 corruption.
	// A genuine same-name conflict (bucket-key precondition) fails again on the
	// re-seeded retry and maps to ErrBucketExists below; an inode-key conflict
	// only means the guard caught a stale mint and the retry re-mints above it.
	var info *BucketInfo
	if err := s.applyNamespaceCreate(ctx, ErrEntryExists, func() (*ConditionalBatch, error) {
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
		info = &BucketInfo{
			Name:         name,
			RootInode:    rootID,
			Policy:       policy,
			CreationDate: time.Now(),
		}
		ops := []batchOp{
			{Key: fmt.Sprintf("%s%d", prefixInode, rootID), Value: root},
			{Key: bucketKey, Value: info},
			{Key: fmt.Sprintf("%s%s", prefixPolicy, name), Value: &policy},
			// Reverse index: rootInode → bucket name, so FUSE can look up
			// a bucket's policy by inode.BucketRoot without scanning all
			// buckets (P1.5: avoid full ListBuckets in resolveChunkPolicy).
			{Key: fmt.Sprintf("%s%d", prefixBucketByRoot, rootID), Value: name},
		}
		if s.cfg.UseBucketStats {
			ops = append(ops, batchOp{
				Key:   s.bucketStatsKey(rootID),
				Value: &BucketUsage{Name: name},
			})
		}
		return buildNamespaceConditionalWithInodeGuard(bucketKey, fmt.Sprintf("%s%d", prefixInode, rootID), ops)
	}); err != nil {
		// If the bucket row exists now, the conflict was a genuine duplicate
		// (or a raced concurrent same-name create), not a stale inode mint.
		if exists, getErr := s.getValue(bucketKey, &existing); getErr != nil {
			return getErr
		} else if exists {
			return ErrBucketExists
		}
		return err
	}
	s.bucketCache.Store(name, info)
	s.publishEvent(Event{Type: EventSet, Key: fmt.Sprintf("bucket:%s", name)})
	return nil
}

func (s *PebbleStore) DeleteBucket(ctx context.Context, name string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	bucketKey := prefixBucket + name
	var info BucketInfo
	exists, err := s.getValue(bucketKey, &info)
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
		fmt.Sprintf("%s%d", prefixBucketByRoot, info.RootInode),
		prefixQuota + name,
		prefixQuotaUsage + name,
	}
	if s.cfg.UseBucketStats {
		deletes = append(deletes, s.bucketStatsKey(info.RootInode))
	}
	if err := s.applyBatchMsgpack(nil, deletes); err != nil {
		return err
	}
	s.bucketCache.Delete(name)
	if s.quota != nil {
		s.quota.LoadQuota(name, nil)
		s.quota.LoadUsage(name, nil)
	}
	s.publishEvent(Event{Type: EventDelete, Key: fmt.Sprintf("bucket:%s", name)})
	return nil
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
	// Check cache first (lock-free read).
	if cached, ok := s.bucketCache.Load(name); ok {
		b := *cached.(*BucketInfo) // copy to avoid sharing
		return &b, nil
	}
	var info BucketInfo
	exists, err := s.getValue(prefixBucket+name, &info)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrBucketNotFound
	}
	s.bucketCache.Store(name, &info)
	return &info, nil
}

// GetBucketByRoot looks up a bucket by its root inode ID using the
// reverse index written by CreateBucket. This avoids a full
// ListBuckets scan when FUSE needs to resolve a chunk policy from
// an inode's BucketRoot field (P1.5).
func (s *PebbleStore) GetBucketByRoot(ctx context.Context, rootInode InodeID) (*BucketInfo, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var name string
	key := fmt.Sprintf("%s%d", prefixBucketByRoot, rootInode)
	exists, err := s.getValue(key, &name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrBucketNotFound
	}
	return s.GetBucket(ctx, name)
}

// ========== NamespaceService Implementation ==========

// applyNamespaceConditional atomically applies a namespace-mutation batch
// guarded by an nsKey precondition, returning conflictErr when the
// precondition fails. It is used by both the create paths (ExpectAbsent →
// concurrent same-name create maps to ErrEntryExists) and the delete paths
// (CAS-on-value → concurrent same-name delete maps to ErrEntryNotFound).
//
// It fixes the non-atomic check-then-act race that both create and delete
// paths shared: two concurrent same-name operations both passed the plain
// getJSON(nsKey) check and both applied their mutations. Reusing
// OpConditionalBatch (via applyConditionalBatchWithHook, which is
// choke-point-generic) makes the check and the inode/nsKey/NLink mutations
// atomic in one step: the winner commits, the loser maps
// ErrRaftConditionalConflict to conflictErr.
func (s *PebbleStore) applyNamespaceConditional(ctx context.Context, conditional *ConditionalBatch, conflictErr error) error {
	if s.degradation.IsReadOnly() {
		return ErrServiceClosed
	}
	var err error
	if s.raft != nil {
		err = s.raft.applyConditionalAccepted(ctx, &RaftLogEntry{Op: OpConditionalBatch, Conditional: conditional}, 10*time.Second)
	} else {
		// Direct (non-raft) path mirrors applyBatchViaRaft: we do NOT gate the
		// commit on ctx cancellation. Recovery/cleanup writers (e.g. the S3
		// write-attempt recovery worker) rely on the write committing even when
		// a task context was canceled — ctx here only bounds raft waiting, not
		// whether the single-node batch is applied.
		s.mu.Lock()
		err = applyConditionalBatchWithHook(s.db, conditional, pebble.Sync, s.conditionalBatchBeforeCommit)
		s.mu.Unlock()
	}
	if errors.Is(err, ErrRaftConditionalConflict) {
		return conflictErr
	}
	return err
}

// applyNamespaceCreate runs a namespace conditional create up to twice: the
// first attempt, and — if it fails with conflictErr on the inode-key
// precondition — a re-seeded retry with a freshly minted ID. mkConditional
// must allocate a new inode ID (and rebuild the batch) on every call.
//
// The retry exists because ensureInodeIDMax's cold-cache scan reads the FSM's
// currently-applied maximum: a newly elected raft leader whose FSM has not yet
// replayed a prior leader's inodes can under-read it, mint a stale ID, and hit
// the inode-key ExpectAbsent guard — surfacing as ErrEntryExists even though
// the file name is new (the create-path inode-ID failover test flakes on
// exactly this window). Re-seeding after the conflict re-scans against the
// now-caught-up FSM, so the second attempt mints above the true committed
// maximum. A genuine same-name conflict (the nsKey precondition) fails again
// on retry and still returns conflictErr — the retry never masks a real
// ErrEntryExists.
func (s *PebbleStore) applyNamespaceCreate(ctx context.Context, conflictErr error, mkConditional func() (*ConditionalBatch, error)) error {
	// Gate the first mint on FSM catch-up: a newly elected leader whose FSM
	// has not yet replayed a prior leader's inodes under-reads the cold-cache
	// scan and mints a stale ID. The re-seed retry below only shrinks that
	// window (a retry can still race a multi-entry lag and surface a spurious
	// conflictErr for a brand-new name); waiting for the FSM to apply every
	// entry committed so far closes it. Zero cost when caught up (the fast
	// path has no raft round trips). The re-seed retry stays as defense in
	// depth against a commit that lands between the wait and the apply.
	if s.raft != nil {
		timeout, err := conditionalRaftApplyTimeout(ctx)
		if err != nil {
			return err
		}
		if err := s.raft.WaitCatchUp(ctx, timeout); err != nil {
			return fmt.Errorf("namespace create before FSM catch-up: %w", err)
		}
	}
	conditional, err := mkConditional()
	if err != nil {
		return err
	}
	if err := s.applyNamespaceConditional(ctx, conditional, conflictErr); err == nil || !errors.Is(err, conflictErr) {
		return err
	}
	s.reseedInodeIDMax()
	conditional, err = mkConditional()
	if err != nil {
		return err
	}
	return s.applyNamespaceConditional(ctx, conditional, conflictErr)
}

// reseedInodeIDMax forces a fresh cold-cache scan of the committed inode
// maximum. Used after an inode-key precondition conflict to pick up inode rows
// that were committed but not yet applied when the first scan ran. Safe under
// concurrency: ensureInodeIDMax raises inodeSeq via CompareAndSwap only, never
// lowering it, so a concurrent mint that already advanced the counter is
// untouched.
func (s *PebbleStore) reseedInodeIDMax() {
	s.inodeIDMaxInit.Store(false)
	s.ensureInodeIDMax()
}

// buildNamespaceConditional turns a set of msgpack-valued mutations (inode
// key, nsKey entry, parent NLink++) into an atomic ConditionalBatch whose
// single precondition requires nsKey to be absent. On conflict the mutation
// set is never applied, so no orphan inode is persisted and the parent NLink
// update is not partially lost.
func buildNamespaceConditional(nsKey string, ops []batchOp) (*ConditionalBatch, error) {
	return buildNamespaceConditionalOps(nsKey, ops, nil, nil)
}

// buildNamespaceConditionalWithInodeGuard is the create-path form of
// buildNamespaceConditional: same atomic nsKey-absence create semantics, plus
// a second precondition requiring the freshly minted /inode/<id> row to be
// absent. The inode guard is defense-in-depth against inode-ID reuse: if a
// stale ID ever collides with a live row (e.g. the cold-scan seeding window
// in ensureInodeIDMax), the create fails with ErrEntryExists instead of
// silently overwriting another object's inode row (the multi-metad
// leader-failover drill's 36/99 corruption). It never blocks a legitimate
// create: fresh IDs mint strictly above every committed row, and recycled
// free-list IDs only enter the list via a committed Unlink/RmDir that deleted
// their row. It must NOT be used on Link — a hard link's target inode is
// required to exist.
func buildNamespaceConditionalWithInodeGuard(nsKey, inodeKey string, ops []batchOp) (*ConditionalBatch, error) {
	mutations := make([]BatchOp, 0, len(ops))
	for _, op := range ops {
		data, err := marshalValue(op.Value, codecMsgpack)
		if err != nil {
			return nil, fmt.Errorf("marshal namespace mutation %q: %w", op.Key, err)
		}
		mutations = append(mutations, BatchOp{Key: []byte(op.Key), Value: data})
	}
	return &ConditionalBatch{
		Version: conditionalBatchVersion,
		Preconditions: []ConditionalPrecondition{
			{Key: []byte(nsKey), ExpectAbsent: true},
			{Key: []byte(inodeKey), ExpectAbsent: true},
		},
		Mutations: mutations,
	}, nil
}

// buildNamespaceConditionalOps is the general form: it encodes the mutations
// and (optionally) raw deletes, and attaches a single precondition on nsKey.
// When preCmp is ExpectAbsent the check is absence (create semantics); when
// preCmp specifies an ExpectedValue it is a compare-and-swap on the nsKey's
// current bytes (delete semantics). The precondition keys are caller-owned
// and must be sorted before commit (canonicalConditionalBatch sorts them).
func buildNamespaceConditionalOps(nsKey string, ops []batchOp, deletes []string, pre *ConditionalPrecondition) (*ConditionalBatch, error) {
	mutations := make([]BatchOp, 0, len(ops)+len(deletes))
	for _, op := range ops {
		data, err := marshalValue(op.Value, codecMsgpack)
		if err != nil {
			return nil, fmt.Errorf("marshal namespace mutation %q: %w", op.Key, err)
		}
		mutations = append(mutations, BatchOp{Key: []byte(op.Key), Value: data})
	}
	for _, del := range deletes {
		mutations = append(mutations, BatchOp{Key: []byte(del), Delete: true})
	}
	if pre == nil {
		pre = &ConditionalPrecondition{Key: []byte(nsKey), ExpectAbsent: true}
	}
	return &ConditionalBatch{
		Version:       conditionalBatchVersion,
		Preconditions: []ConditionalPrecondition{*pre},
		Mutations:     mutations,
	}, nil
}

func (s *PebbleStore) MkDir(ctx context.Context, parent InodeID, name string, mode uint32) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if len(name) > MaxNameLength {
		return nil, ErrNameTooLong
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var created *InodeMeta
	// All mutations ride a single conditional batch; the nsKey ExpectAbsent
	// precondition makes the existence check and the insert atomic, so a
	// concurrent same-name create fails with ErrEntryExists instead of
	// leaving an orphan inode / lost parent NLink update.
	if err := s.applyNamespaceCreate(ctx, ErrEntryExists, func() (*ConditionalBatch, error) {
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
		pExists, _ := s.getValue(parentKey, &parentMeta)
		if pExists {
			meta.BucketRoot = parentMeta.BucketRoot
			parentMeta.NLink++
			parentMeta.MTime = now
		}

		ops := []batchOp{
			{Key: fmt.Sprintf("%s%d", prefixInode, inodeID), Value: meta},
			{Key: nsKey, Value: entry},
		}
		if pExists {
			ops = append(ops, batchOp{Key: parentKey, Value: &parentMeta})
		}
		inodeKey := fmt.Sprintf("%s%d", prefixInode, inodeID)
		conditional, err := buildNamespaceConditionalWithInodeGuard(nsKey, inodeKey, ops)
		if err == nil {
			created = meta
		}
		return conditional, err
	}); err != nil {
		return nil, err
	}
	return created, nil
}

func (s *PebbleStore) RmDir(ctx context.Context, parent InodeID, name string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var entry DirEntry
	exists, err := s.getValue(nsKey, &entry)
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
	var ops []batchOp
	pExists, _ := s.getValue(parentKey, &parentMeta)
	if pExists {
		parentMeta.MTime = time.Now().UnixNano()
		if parentMeta.NLink > 0 {
			parentMeta.NLink--
		}
		ops = append(ops, batchOp{Key: parentKey, Value: &parentMeta})
	}

	// Atomically delete the entry + inode + parent-NLink. The precondition is a
	// compare-and-swap on nsKey's exact current bytes: if a concurrent
	// create/delete/overwrite of this name committed since we read it, the CAS
	// conflicts and we do NOT apply (mapping to ErrEntryNotFound) — preventing a
	// double NLink-- and a double releaseInodeID here and in Unlink.
	nsRaw, _, err := s.getRawBytes(nsKey)
	if err != nil {
		return err
	}
	conditional, err := buildNamespaceConditionalOps(nsKey, ops, deletes, &ConditionalPrecondition{
		Key:           []byte(nsKey),
		ExpectedValue: nsRaw,
	})
	if err != nil {
		return err
	}
	if err := s.applyNamespaceConditional(ctx, conditional, ErrEntryNotFound); err != nil {
		return err
	}
	s.releaseInodeID(entry.InodeID)
	return nil
}

func (s *PebbleStore) ReadDir(ctx context.Context, parent InodeID, offset int, limit int) ([]DirEntry, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if s.raft != nil {
		if err := s.raft.ReadIndex(5 * time.Second); err != nil {
			slog.Warn("read-index: falling back to local read", "error", err)
		}
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

// ReadDirFrom lists directory entries starting strictly after the
// given cursor (the name of the last entry returned by a previous
// call). An empty cursor starts from the beginning. This enables
// cursor-based pagination that is O(limit) regardless of how deep
// into the directory the cursor is, unlike ReadDir's offset-based
// approach which is O(offset+limit) (P1.7).
func (s *PebbleStore) ReadDirFrom(ctx context.Context, parent InodeID, afterName string, limit int) ([]DirEntry, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if s.raft != nil {
		if err := s.raft.ReadIndex(5 * time.Second); err != nil {
			slog.Warn("read-index: falling back to local read", "error", err)
		}
	}

	const maxReadDirEntries = 10_000
	if limit <= 0 || limit > maxReadDirEntries {
		limit = maxReadDirEntries
	}

	prefix := fmt.Sprintf("%s%d/", prefixNS, parent)

	// Construct the start key: prefix + afterName. The iterator will
	// SeekGE to this key and then skip past it (if it matches exactly),
	// so we get entries strictly after the cursor.
	var lowerBound []byte
	if afterName != "" {
		lowerBound = []byte(prefix + afterName)
	} else {
		lowerBound = []byte(prefix)
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	// Position the iterator
	if afterName != "" {
		// Seek to the cursor key and skip past it
		cursorKey := []byte(prefix + afterName)
		iter.SeekGE(cursorKey)
		if iter.Valid() && bytes.Equal(iter.Key(), cursorKey) {
			iter.Next()
		}
	} else {
		iter.First()
	}

	entries := make([]DirEntry, 0, limit)
	for ; iter.Valid(); iter.Next() {
		if len(entries) >= limit {
			break
		}
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}
		name := strings.TrimPrefix(string(iter.Key()), prefix)
		if name == "" {
			continue
		}
		var entry DirEntry
		if err := unmarshalValue(val, &entry); err != nil {
			continue
		}
		entry.Name = name
		entries = append(entries, entry)
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
	bucketRoot := s.getBucketRoot(parent)
	var created *InodeMeta
	if err := s.applyNamespaceCreate(ctx, ErrEntryExists, func() (*ConditionalBatch, error) {
		inodeID := s.nextInodeID()
		now := time.Now().UnixNano()
		meta := &InodeMeta{
			ID: inodeID, Type: FileRegular, Mode: mode, NLink: 1,
			BucketRoot: bucketRoot,
			CTime:      now, MTime: now, ATime: now,
		}
		entry := &DirEntry{InodeID: inodeID, Type: FileRegular, Name: name}
		ops := []batchOp{
			{Key: fmt.Sprintf("%s%d", prefixInode, inodeID), Value: meta},
			{Key: nsKey, Value: entry},
		}
		s.addBucketStatsOp(bucketRoot, 0, 1, &ops)
		inodeKey := fmt.Sprintf("%s%d", prefixInode, inodeID)
		conditional, err := buildNamespaceConditionalWithInodeGuard(nsKey, inodeKey, ops)
		if err == nil {
			created = meta
		}
		return conditional, err
	}); err != nil {
		return nil, err
	}
	s.publishEvent(Event{Type: EventSet, Key: fmt.Sprintf("inode:%d", created.ID)})
	return created, nil
}

// CreateNode creates a special (non-regular) namespace entry — FIFO, char or
// block device, or unix socket. It mirrors CreateFile's atomic namespace CAS
// (nextInodeID + bucket-root inheritance + buildNamespaceConditional +
// applyNamespaceConditional), differing only in the FileType and Rdev carried
// on the inode and its DirEntry. Special nodes never back real data, so their
// size stays 0 and they add one object to the bucket's object count.
func (s *PebbleStore) CreateNode(ctx context.Context, parent InodeID, name string, ftype FileType, mode uint32, rdev uint32) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if len(name) > MaxNameLength {
		return nil, ErrNameTooLong
	}
	// CreateNode is only for special nodes; regular files/dirs/symlinks have
	// their own dedicated primitives.
	switch ftype {
	case FileFIFO, FileCharDevice, FileBlockDevice, FileSocket:
	default:
		return nil, fmt.Errorf("metadata: CreateNode called with non-special type %v", ftype)
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	bucketRoot := s.getBucketRoot(parent)
	var created *InodeMeta
	if err := s.applyNamespaceCreate(ctx, ErrEntryExists, func() (*ConditionalBatch, error) {
		inodeID := s.nextInodeID()
		now := time.Now().UnixNano()
		meta := &InodeMeta{
			ID: inodeID, Type: ftype, Mode: mode, Rdev: rdev, NLink: 1,
			BucketRoot: bucketRoot,
			CTime:      now, MTime: now, ATime: now,
		}
		entry := &DirEntry{InodeID: inodeID, Type: ftype, Name: name}
		ops := []batchOp{
			{Key: fmt.Sprintf("%s%d", prefixInode, inodeID), Value: meta},
			{Key: nsKey, Value: entry},
		}
		s.addBucketStatsOp(bucketRoot, 0, 1, &ops)
		inodeKey := fmt.Sprintf("%s%d", prefixInode, inodeID)
		conditional, err := buildNamespaceConditionalWithInodeGuard(nsKey, inodeKey, ops)
		if err == nil {
			created = meta
		}
		return conditional, err
	}); err != nil {
		return nil, err
	}
	s.publishEvent(Event{Type: EventSet, Key: fmt.Sprintf("inode:%d", created.ID)})
	return created, nil
}
func (s *PebbleStore) Unlink(ctx context.Context, parent InodeID, name string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var entry DirEntry
	exists, err := s.getValue(nsKey, &entry)
	if err != nil {
		return err
	}
	if !exists {
		return ErrEntryNotFound
	}
	if entry.Type == FileDirectory {
		return ErrNotFile
	}

	// Model-aware inode handling (mirrors UpdateInode): the V1 and V2 inode
	// rows share the /inode/ prefix and differ only in layout fields, so the
	// read is model-agnostic but the rewrite must preserve the row's own
	// model. A V2-layout row re-encoded as InodeMeta would silently strip
	// Layout/InlineExtent/ExtentRoot — hard-linked V2 files would lose data.
	var v2 InodeMetaV2
	inodeKey := fmt.Sprintf("%s%d", prefixInode, entry.InodeID)
	found, raw, err := s.getRaw(inodeKey)
	if err != nil {
		return err
	}
	var ops []batchOp
	deletes := []string{nsKey}

	var deleteInode bool
	if found {
		if err := unmarshalValue(raw, &v2); err != nil {
			return err
		}
		v2.NLink--
		v2.MTime = time.Now().UnixNano()
		if v2.NLink <= 0 {
			s.addBucketStatsOp(v2.BucketRoot, -v2.Size, -1, &ops)
			deletes = append(deletes, inodeKey)
			deleteInode = true
		} else if v2.Layout != LayoutEmpty {
			ops = append(ops, batchOp{Key: inodeKey, Value: &v2})
		} else {
			var meta InodeMeta
			if err := unmarshalValue(raw, &meta); err != nil {
				return err
			}
			meta.NLink = v2.NLink
			meta.MTime = v2.MTime
			ops = append(ops, batchOp{Key: inodeKey, Value: &meta})
		}
	}

	// Atomically unlink the entry (+ inode, -NLink, bucket stats). The
	// precondition CASes on nsKey's exact current bytes: if a concurrent
	// create/delete/overwrite of this name committed since we read it, the CAS
	// conflicts and nothing applies (mapped to ErrEntryNotFound) — no double
	// NLink--, no double releaseInodeID.
	nsRaw, _, err := s.getRawBytes(nsKey)
	if err != nil {
		return err
	}
	conditional, err := buildNamespaceConditionalOps(nsKey, ops, deletes, &ConditionalPrecondition{
		Key:           []byte(nsKey),
		ExpectedValue: nsRaw,
	})
	if err != nil {
		return err
	}
	if err := s.applyNamespaceConditional(ctx, conditional, ErrEntryNotFound); err != nil {
		return err
	}
	if deleteInode {
		s.releaseInodeID(v2.ID)
	}
	// Invalidate cache for the deleted inode (or updated if still live)
	s.inCache.del(entry.InodeID)
	s.publishEvent(Event{Type: EventDelete, Key: fmt.Sprintf("inode:%d", entry.InodeID)})
	return nil
}

func (s *PebbleStore) Lookup(ctx context.Context, parent InodeID, name string) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if s.raft != nil {
		if err := s.raft.ReadIndex(5 * time.Second); err != nil {
			slog.Warn("read-index: falling back to local read", "error", err)
		}
	}
	start := time.Now()

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var entry DirEntry
	exists, err := s.getValue(nsKey, &entry)
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
	exists, err = s.getValue(fmt.Sprintf("%s%d", prefixInode, entry.InodeID), &meta)
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
	// Ensure linearizable read in Raft cluster: verify this node's log
	// is caught up to the leader before reading local data. Without this,
	// a follower may return stale data after a leader change.
	if s.raft != nil {
		if err := s.raft.ReadIndex(5 * time.Second); err != nil {
			slog.Warn("read-index: falling back to local read", "error", err)
		}
	}
	start := time.Now()

	if cached := s.inCache.get(id); cached != nil {
		if s.metrics != nil {
			s.metrics.RecordCacheHit()
			s.metrics.RecordRead(time.Since(start))
		}
		// Shallow-copy the struct (all scalar fields are isolated by
		// value semantics). ChunkMap and XAttrs are reference types
		// that need deep copying ONLY when non-empty — the common
		// case (directories, empty files) has both nil/empty, so we
		// skip the allocation entirely (P3.11).
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
	exists, err := s.getValue(fmt.Sprintf("%s%d", prefixInode, id), &meta)
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
	// Cache a deep copy so the returned pointer is independent of
	// the cached value. Without this, the caller could mutate the
	// cached entry via the returned pointer (P3.11).
	cached := &InodeMeta{
		ID: meta.ID, Type: meta.Type, Size: meta.Size, NLink: meta.NLink,
		BucketRoot: meta.BucketRoot, UID: meta.UID, GID: meta.GID, Mode: meta.Mode,
		CTime: meta.CTime, MTime: meta.MTime, ATime: meta.ATime, Symlink: meta.Symlink,
	}
	if len(meta.ChunkMap) > 0 {
		cached.ChunkMap = make([]ChunkRef, len(meta.ChunkMap))
		copy(cached.ChunkMap, meta.ChunkMap)
	}
	if len(meta.XAttrs) > 0 {
		cached.XAttrs = make(map[string][]byte, len(meta.XAttrs))
		for k, v := range meta.XAttrs {
			vc := make([]byte, len(v))
			copy(vc, v)
			cached.XAttrs[k] = vc
		}
	}
	s.inCache.put(id, cached)
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
	meta.CTime = time.Now().UnixNano()
	key := fmt.Sprintf("%s%d", prefixInode, meta.ID)
	var ops []batchOp

	// Model collision guard + bucket-stats delta share one raw read of the
	// existing row. V1 (InodeMeta, ChunkMap) and V2 (InodeMetaV2, layout)
	// coexist under the same key; a V1 data write carrying a ChunkMap would
	// silently wipe the V2 layout fields, so refuse it loudly instead of
	// corrupting. A metadata-only update (nil ChunkMap — GetInode on a V2
	// row returns no chunks) is merged into the V2 struct so the layout
	// fields survive chmod/chown/xattr/touch on V2 files. The same read
	// supplies the Size delta for bucket stats, so production
	// (UseBucketStats) pays no extra I/O for the guard.
	if found, raw, err := s.getRaw(key); err != nil {
		return err
	} else if found {
		var v2 InodeMetaV2
		if err := unmarshalValue(raw, &v2); err != nil {
			return err
		}
		if v2.Layout != LayoutEmpty {
			if len(meta.ChunkMap) > 0 {
				return fmt.Errorf("%w: inode %d carries layout %d", ErrInodeModelMismatch, meta.ID, v2.Layout)
			}
			// Metadata-only update on a V2-layout row: overlay the V1
			// projection's scalar/aux fields, keep the V2 layout fields.
			oldSize := v2.Size
			v2.Size = meta.Size
			v2.NLink = meta.NLink
			v2.BucketRoot = meta.BucketRoot
			v2.UID = meta.UID
			v2.GID = meta.GID
			v2.Mode = meta.Mode
			v2.MTime = meta.MTime
			v2.ATime = meta.ATime
			v2.Symlink = meta.Symlink
			v2.XAttrs = meta.XAttrs
			v2.CTime = meta.CTime // already bumped to now above
			if s.cfg.UseBucketStats {
				s.addBucketStatsOp(v2.BucketRoot, meta.Size-oldSize, 0, &ops)
			}
			ops = append(ops, batchOp{Key: key, Value: &v2})
		} else {
			if s.cfg.UseBucketStats {
				s.addBucketStatsOp(v2.BucketRoot, meta.Size-v2.Size, 0, &ops)
			}
			ops = append(ops, batchOp{Key: key, Value: meta})
		}
	} else {
		ops = append(ops, batchOp{Key: key, Value: meta})
	}
	if err := s.applyBatchMsgpack(ops, nil); err != nil {
		return err
	}
	// Invalidate only after the durable mutation succeeds.
	s.inCache.del(meta.ID)
	s.publishEvent(Event{Type: EventSet, Key: fmt.Sprintf("inode:%d", meta.ID)})
	return nil
}

func (s *PebbleStore) Rename(ctx context.Context, oldParent InodeID, oldName string, newParent InodeID, newName string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	oldNSKey := fmt.Sprintf("%s%d/%s", prefixNS, oldParent, oldName)
	var entry DirEntry
	exists, err := s.getValue(oldNSKey, &entry)
	if err != nil {
		return err
	}
	if !exists {
		return ErrEntryNotFound
	}

	// Check destination
	newNSKey := fmt.Sprintf("%s%d/%s", prefixNS, newParent, newName)
	var destEntry DirEntry
	destExists, _ := s.getValue(newNSKey, &destEntry)
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
	ops := []batchOp{
		{Key: newNSKey, Value: &entry},
	}
	deletes := []string{oldNSKey}
	return s.applyBatchMsgpack(ops, deletes)
}

func (s *PebbleStore) Symlink(ctx context.Context, parent InodeID, name string, target string) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	bucketRoot := s.getBucketRoot(parent)
	var created *InodeMeta
	if err := s.applyNamespaceCreate(ctx, ErrEntryExists, func() (*ConditionalBatch, error) {
		inodeID := s.nextInodeID()
		now := time.Now().UnixNano()
		meta := &InodeMeta{
			ID: inodeID, Type: FileSymlink, Mode: 0777, NLink: 1, Symlink: target,
			BucketRoot: bucketRoot,
			CTime:      now, MTime: now, ATime: now,
		}
		entry := &DirEntry{InodeID: inodeID, Type: FileSymlink, Name: name}
		ops := []batchOp{
			{Key: fmt.Sprintf("%s%d", prefixInode, inodeID), Value: meta},
			{Key: nsKey, Value: entry},
		}
		s.addBucketStatsOp(bucketRoot, int64(len(target)), 1, &ops)
		inodeKey := fmt.Sprintf("%s%d", prefixInode, inodeID)
		conditional, err := buildNamespaceConditionalWithInodeGuard(nsKey, inodeKey, ops)
		if err == nil {
			created = meta
		}
		return conditional, err
	}); err != nil {
		return nil, err
	}
	return created, nil
}
func (s *PebbleStore) Readlink(ctx context.Context, id InodeID) (string, error) {
	if s.closed.Load() {
		return "", ErrServiceClosed
	}
	var meta InodeMeta
	exists, err := s.getValue(fmt.Sprintf("%s%d", prefixInode, id), &meta)
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

	// Model-aware (mirrors UpdateInode/Unlink): preserve the inode row's own
	// model on rewrite — a V2-layout row re-encoded as InodeMeta would strip
	// Layout/InlineExtent/ExtentRoot. The return value is projected from the
	// raw V1 decode regardless (field-name aliasing makes the shared scalars
	// identical), so the *InodeMeta return contract is unchanged.
	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)

	inodeKey := fmt.Sprintf("%s%d", prefixInode, target)
	found, raw, err := s.getRaw(inodeKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrInodeNotFound
	}
	var v2 InodeMetaV2
	if err := unmarshalValue(raw, &v2); err != nil {
		return nil, err
	}
	if v2.Type == FileDirectory {
		return nil, ErrInvalidArgument
	}

	v2.NLink++
	v2.CTime = time.Now().UnixNano()
	entry := &DirEntry{InodeID: target, Type: v2.Type, Name: name}
	var ops []batchOp
	if v2.Layout != LayoutEmpty {
		ops = append(ops, batchOp{Key: inodeKey, Value: &v2})
	} else {
		var meta InodeMeta
		if err := unmarshalValue(raw, &meta); err != nil {
			return nil, err
		}
		meta.NLink = v2.NLink
		meta.CTime = v2.CTime
		ops = append(ops, batchOp{Key: inodeKey, Value: &meta})
	}
	ops = append(ops, batchOp{Key: nsKey, Value: entry})
	// Name-uniqueness is enforced atomically: a concurrent same-name link
	// fails with ErrEntryExists instead of persisting an orphan entry.
	conditional, err := buildNamespaceConditional(nsKey, ops)
	if err != nil {
		return nil, err
	}
	if err := s.applyNamespaceConditional(ctx, conditional, ErrEntryExists); err != nil {
		return nil, err
	}
	var meta InodeMeta
	if err := unmarshalValue(raw, &meta); err != nil {
		return nil, err
	}
	meta.NLink = v2.NLink
	meta.CTime = v2.CTime
	return &meta, nil
}

// ========== ChunkService Implementation ==========

func (s *PebbleStore) AllocateChunk(ctx context.Context, inodeID InodeID, offset int64, policy PlacementPolicy) (*ChunkMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	chunks, err := s.allocateChunksConditionally(ctx, inodeID, []int64{offset}, policy)
	if err != nil {
		return nil, err
	}
	return chunks[0], nil
}

// AllocateChunksBatch allocates multiple chunks at once and updates the inode once.
// This is more efficient for large files than calling AllocateChunk repeatedly,
// as it reduces the number of inode reads/writes and Raft round-trips.
// Returns the allocated chunk metadata (including replica addresses) in order.
func (s *PebbleStore) AllocateChunksBatch(ctx context.Context, inodeID InodeID, offsets []int64, policy PlacementPolicy) ([]*ChunkMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if len(offsets) == 0 {
		return nil, nil
	}
	if len(offsets) > MaxChunkAllocationBatch {
		return nil, fmt.Errorf("max chunk allocation batch is %d", MaxChunkAllocationBatch)
	}

	return s.allocateChunksConditionally(ctx, inodeID, offsets, policy)
}

const allocationConditionalAttempts = 8

func (s *PebbleStore) allocateChunksConditionally(ctx context.Context, inodeID InodeID, offsets []int64, policy PlacementPolicy) ([]*ChunkMeta, error) {
	if len(offsets) > MaxChunkAllocationBatch {
		return nil, fmt.Errorf("max chunk allocation batch is %d", MaxChunkAllocationBatch)
	}
	inodeKey := fmt.Sprintf("%s%d", prefixInode, inodeID)
	started := time.Now()
	// outcomeUnknownRetries bounds how often an outcome-unknown apply (each
	// one waits the full conditional window) is retried before the caller
	// learns the outcome is unknown. Unlike a fast collision retry, one retry
	// is enough for the FSM to catch up and reconcile to confirm; more would
	// multiply the wait by the conditional window for no extra correctness.
	const outcomeUnknownRetries = 1
	unknownRetries := 0
	for attempt := 0; attempt < allocationConditionalAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		inodeRaw, found, err := s.readChunkTombstoneRaw(inodeKey)
		if err != nil {
			return nil, fmt.Errorf("allocate chunks: read inode: %w", err)
		}
		if !found {
			return nil, ErrInodeNotFound
		}
		var inode InodeMeta
		if err := unmarshalValue(inodeRaw, &inode); err != nil {
			return nil, fmt.Errorf("allocate chunks: decode inode: %w", err)
		}
		if err := validateInodeKeyIdentity(inodeKey, inode.ID); err != nil {
			return nil, err
		}
		chunks, err := s.buildAllocatedChunks(ctx, offsets, policy)
		if err != nil {
			if errors.Is(err, ErrBackupMetadataConflict) {
				continue
			}
			return nil, err
		}
		next := inode
		now := time.Now().UnixNano()
		for i, chunk := range chunks {
			next.ChunkMap = append(next.ChunkMap, ChunkRef{ID: chunk.ID, Offset: offsets[i], Version: now})
		}
		next.MTime = now
		conditional, err := s.buildChunkAllocationConditional(inodeKey, inodeRaw, &next, chunks)
		if err != nil {
			return nil, err
		}
		err = s.applyAllocationConditional(ctx, conditional)
		if errors.Is(err, ErrRaftConditionalOutcomeUnknown) {
			// The batch may have committed while its FSM apply lagged past
			// the conditional wait. Reconcile against a CAUGHT-UP FSM so
			// "not committed" is a reliable verdict: without this, a
			// late-arriving apply of attempt 1 lands first (FSMs apply in
			// log-index order), the retry's precondition fails, and the next
			// retry appends on top of it — leaving two chunk refs at the
			// same offset, the first of whose replicas were never written
			// (ghost ref: the caller completes a 200 PUT whose object reads
			// the unwritten ghost first).
			if s.raft != nil {
				timeout, werr := conditionalRaftApplyTimeout(ctx)
				if werr != nil {
					return nil, werr
				}
				if werr := s.raft.WaitCatchUp(ctx, timeout); werr != nil {
					return nil, fmt.Errorf("allocate chunks: reconcile before FSM catch-up: %w", werr)
				}
			}
			committed, reconcileErr := s.reconcileAllocation(inodeKey, &next, chunks, conditional)
			if reconcileErr != nil {
				return nil, reconcileErr
			}
			if committed {
				if s.metrics != nil {
					s.metrics.RecordWrite(time.Since(started))
				}
				// The conditional batch writes the inode row straight through
				// the raft FSM, bypassing UpdateInode's cache invalidation —
				// drop the cached copy so a following GetInode re-reads the
				// freshly appended ChunkMap refs.
				s.inCache.del(inodeID)
				return chunks, nil
			}
			// The outcome is unknown AND not yet observable in the FSM (a
			// slow leader whose apply lagged past the conditional wait — e.g.
			// right after a failover under CPU contention). Retry once: the
			// next iteration re-reads the inode, so if the previous batch did
			// commit the fresh conditional appends new IDs on top of it, and
			// if it did not the retry just re-submits. The inode-key
			// precondition keeps a duplicate append from ever landing. A
			// second unknown means the leader cannot commit at all — surface
			// the outcome-unknown error for the caller's reconcile path.
			unknownRetries++
			if unknownRetries > outcomeUnknownRetries {
				return nil, err
			}
			continue
		}
		if !errors.Is(err, ErrBackupMetadataConflict) {
			if err == nil {
				if s.metrics != nil {
					s.metrics.RecordWrite(time.Since(started))
				}
				// Same cache-invalidation rationale as the reconcile path above.
				s.inCache.del(inodeID)
			}
			return chunks, err
		}
	}
	return nil, fmt.Errorf("allocate chunks: %w after %d collision retries", ErrBackupMetadataConflict, allocationConditionalAttempts)
}

func (s *PebbleStore) applyAllocationConditional(ctx context.Context, conditional *ConditionalBatch) error {
	if s.raft == nil {
		return s.applyChunkTombstoneConditional(ctx, conditional)
	}
	if s.degradation.IsReadOnly() {
		return ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.raft.applyConditionalAccepted(ctx, &RaftLogEntry{Op: OpConditionalBatch, Conditional: conditional}, 10*time.Second)
	if errors.Is(err, ErrRaftConditionalConflict) {
		return ErrBackupMetadataConflict
	}
	return err
}

func (s *PebbleStore) reconcileAllocation(inodeKey string, expected *InodeMeta, chunks []*ChunkMeta, conditional *ConditionalBatch) (bool, error) {
	inodeRaw, found, err := s.readChunkTombstoneRaw(inodeKey)
	if err != nil || !found {
		return false, err
	}
	var inode InodeMeta
	if err := unmarshalValue(inodeRaw, &inode); err != nil || !reflect.DeepEqual(inode.ChunkMap, expected.ChunkMap) {
		return false, nil
	}
	for _, chunk := range chunks {
		raw, found, err := s.readChunkTombstoneRaw(chunkMetadataKey(chunk.ID))
		if err != nil || !found {
			return false, err
		}
		want, _ := marshalValue(chunk, codecMsgpack)
		if !bytes.Equal(raw, want) {
			return false, nil
		}
		if _, tombstoneFound, err := s.readChunkTombstoneRaw(chunkTombstoneKey(chunk.ID)); err != nil || tombstoneFound {
			return false, err
		}
	}
	for _, mutation := range conditional.Mutations {
		if string(mutation.Key) == keyInodeReferenceEpoch {
			raw, found, err := s.readChunkTombstoneRaw(keyInodeReferenceEpoch)
			if err != nil || !found || len(raw) != 8 || binary.BigEndian.Uint64(raw) < binary.BigEndian.Uint64(mutation.Value) {
				return false, err
			}
		}
	}
	return true, nil
}

func (s *PebbleStore) buildAllocatedChunks(ctx context.Context, offsets []int64, policy PlacementPolicy) ([]*ChunkMeta, error) {
	replicaCount := policy.ReplicationFactor
	if policy.ECConfig != nil && policy.ECConfig.DataShards > 0 {
		replicaCount = policy.ECConfig.TotalShards()
	}
	if replicaCount <= 0 {
		return nil, fmt.Errorf("allocate chunk: invalid replica count (ReplicationFactor=%d, ECConfig=%v)", policy.ReplicationFactor, policy.ECConfig)
	}
	ecPolicy := policy
	ecPolicy.ReplicationFactor = replicaCount

	// ---- Chunk-ID uniqueness across leader failover ----
	// Chunk IDs are minted here, on whichever process is leader at request time,
	// from a per-process snowflake generator (chunkGen) *before* the raft
	// conditional apply. The generator is in-memory and uncoordinated, so a
	// newly elected leader or restarted process could otherwise re-issue an
	// ID another leader already committed — and since the datanode keys chunks
	// by their 64-bit ID, reuse silently overwrites another object's bytes (a
	// byte-exact durability break, reproduced by the multi-metad leader-
	// failover drill). Closing this: bump the generator strictly above the
	// largest chunk ID already committed to this store, so issuance is globally
	// monotonic across leadership changes and restarts.
	s.chunkGen.BumpAbove(s.ensureChunkIDMax())

	ids := make([]ChunkID, len(offsets))
	seenIDs := make(map[ChunkID]struct{}, len(offsets))
	for i := range ids {
		ids[i] = s.nextChunkID()
		if ids[i] == 0 {
			return nil, fmt.Errorf("allocate chunk: zero chunk id")
		}
		if _, duplicate := seenIDs[ids[i]]; duplicate {
			return nil, ErrBackupMetadataConflict
		}
		seenIDs[ids[i]] = struct{}{}
	}
	// The largest minted ID is the new in-process high mark, so later batches
	// stay monotonic without rescanning committed keys.
	var newHW ChunkID
	for _, id := range ids {
		if id > newHW {
			newHW = id
		}
	}
	s.advanceChunkIDMax(newHW)
	groupID := ""
	if policy.ECConfig != nil && policy.ECConfig.DataShards > 0 {
		groupID = fmt.Sprintf("ec-%d", ids[0])
		if len(ids) > 1 {
			groupID = fmt.Sprintf("ec-batch-%d", ids[0])
		}
	}
	chunks := make([]*ChunkMeta, len(ids))
	for i, id := range ids {
		var replicas []ReplicaInfo
		var pgID uint32
		var pgEpoch uint64
		if policy.PlacementGroups {
			if groupID != "" {
				return nil, fmt.Errorf("allocate chunk: placement groups do not apply to EC chunks")
			}
			var err error
			replicas, pgID, pgEpoch, err = s.allocateChunkViaPG(ctx, ecPolicy)
			if err != nil {
				return nil, err
			}
		} else {
			nodeIDs, err := s.placement.PlaceChunk(ecPolicy, nil)
			if err != nil {
				if errors.Is(err, ErrInsufficientNodes) {
					if herr := s.hydratePlacement(); herr != nil {
						return nil, herr
					}
					nodeIDs, err = s.placement.PlaceChunk(ecPolicy, nil)
				}
				if err != nil {
					return nil, err
				}
			}
			replicas, err = s.buildReplicas(ctx, nodeIDs, groupID)
			if err != nil {
				return nil, err
			}
		}
		chunk := &ChunkMeta{ID: id, Size: MaxChunkSize, State: ChunkCreated, Replicas: replicas, Tier: policy.StorageTier, CreateTime: time.Now().UnixNano(), PGID: pgID, Epoch: pgEpoch, Generation: 1}
		if groupID != "" {
			// Reference the shared ECProfile (config lives in the profile row,
			// not repeated per chunk) while retaining the embedded
			// DataShards/ParityShards for read-compatible consumers. The profile
			// derives from the bucket's ECConfig, NOT the canonical 6+3 default:
			// the codec must match the K/M the user configured. Previously a
			// non-6+3 ECConfig bucket wrote only the first K shards of the 6+3
			// codec (zero fault tolerance — one shard loss drops below K and
			// decode fails); sizing the group from the bucket's config makes the
			// configured scheme real. For 6+3 the derived profile equals
			// DefaultECProfile, so the canonical path is unchanged. No
			// ECStripeID yet: the durable stripe is only created at
			// conversion time (ECStore.BeginConversion), after which the
			// landing pointer is set by the publish step.
			chunk.ECGroup = ECGroupFromProfile(profileFromECConfig(policy.ECConfig), groupID)
		}
		chunks[i] = chunk
	}
	return chunks, nil
}

// ensureChunkIDMax returns a floor strictly above which the next chunk ID may
// safely be minted, derived from the true maximum chunk ID already committed to
// this store. The durable source of truth is the raft-replicated set of chunk-
// metadata keys, so this is authoritative across leader failover and process
// restart. The first call scans the committed chunk keys (cold cache); later
// calls are served from an in-memory high mark that advanceChunkIDMax keeps
// current, so steady-state allocation is O(1).
func (s *PebbleStore) ensureChunkIDMax() uint64 {
	if s.chunkIDMaxInit.Load() {
		return s.chunkIDMax.Load()
	}
	// Cold cache: this process (or this raft epoch) has not yet minted; scan the
	// committed chunk metadata for the largest existing chunk ID. Handles a
	// freshly elected leader that must not re-issue IDs a previous leader
	// already committed, and a restarted process that must not reuse its own
	// prior IDs.
	var max uint64
	_ = s.scanPrefix(prefixChunk, func(key, _ []byte) error {
		id, err := parseChunkTombstoneKey(string(key), prefixChunk)
		if err == nil && uint64(id) > max {
			max = uint64(id)
		}
		return nil
	})
	s.chunkIDMax.Store(max)
	s.chunkIDMaxInit.Store(true)
	return max
}

// parseInodeKeyID extracts the inode ID from a canonical /inode/<id> key.
// Mirrors parseChunkTombstoneKey: a strict round-trip check rejects any
// non-decimal or oddly-shaped key so a malformed row can never skew the
// cold-scan maximum.
func parseInodeKeyID(key string) (InodeID, error) {
	id, err := strconv.ParseUint(strings.TrimPrefix(key, prefixInode), 10, 64)
	if err != nil || id == 0 || prefixInode+strconv.FormatUint(id, 10) != key {
		return 0, fmt.Errorf("invalid inode key %q", key)
	}
	return InodeID(id), nil
}

// ensureInodeIDMax raises inodeSeq to at least the largest inode ID already
// committed to this store, derived from the raft-replicated /inode/ keys — the
// same durable source that chunk-ID minting uses. inodeSeq is process-local
// and reset to RootInodeID at every store construction, so without this a
// newly elected raft leader or restarted process re-mints IDs a previous
// leader already committed, silently overwriting unrelated inode rows (the
// multi-metad leader-failover drill's 36/99 corruption). The first call scans
// the committed inode keys (cold cache); later calls are served from the
// counter itself, which nextInodeID's Add(1) keeps current, so steady-state
// minting stays O(1).
//
// Lazy (first mint, not store construction): at construction time a raft FSM
// has replayed nothing, so an eager scan would permanently under-seed. By the
// first mint the election + leader-redirect + retry budget has given the FSM
// ample time to catch up (the same reasoning that makes ensureChunkIDMax's
// lazy scan the authoritative fix for chunk IDs).
//
// The raise is an upward-only CompareAndSwap loop, NOT an unconditional
// Store: inodeSeq is the "largest minted" counter, and a concurrent
// nextInodeID Add(1) may already have advanced it — Store would roll the
// counter back and mint a colliding ID in-process. (ensureChunkIDMax may
// Store unconditionally because chunkIDMax is only a comparison floor, never
// the live minting counter.)
func (s *PebbleStore) ensureInodeIDMax() uint64 {
	if s.inodeIDMaxInit.Load() {
		return s.inodeSeq.Load()
	}
	// Cold cache: scan the committed inode rows for the largest existing ID.
	var max uint64
	_ = s.scanPrefix(prefixInode, func(key, _ []byte) error {
		id, err := parseInodeKeyID(string(key))
		if err == nil && uint64(id) > max {
			max = uint64(id)
		}
		return nil
	})
	for {
		cur := s.inodeSeq.Load()
		if cur >= max {
			break
		}
		if s.inodeSeq.CompareAndSwap(cur, max) {
			break
		}
	}
	s.inodeIDMaxInit.Store(true)
	return s.inodeSeq.Load()
}

// advanceChunkIDMax raises the in-memory high mark when a batch mints IDs
// beyond what the cold-cache scan observed, so subsequent allocations stay
// monotonic without rescanning.
func (s *PebbleStore) advanceChunkIDMax(newMax ChunkID) {
	for {
		cur := s.chunkIDMax.Load()
		if uint64(newMax) <= cur {
			return
		}
		if s.chunkIDMax.CompareAndSwap(cur, uint64(newMax)) {
			return
		}
	}
}

// hydratePlacement syncs the in-memory PlacementEngine from the
// raft-replicated node store. A metad raft member that did not directly
// receive a datanode's RegisterNode/Heartbeat RPC (e.g. a newly elected
// leader right after a failover) has an empty placement view; rebuilding it
// from ListNodes lets allocation proceed immediately instead of failing
// until the datanodes happen to re-heartbeat to it.
func (s *PebbleStore) hydratePlacement() error {
	nodes, err := s.ListNodes(context.Background())
	if err != nil {
		return err
	}
	for i := range nodes {
		s.placement.UpdateNode(&nodes[i])
	}
	return nil
}

// buildReplicas resolves node IDs (from PlacementEngine.PlaceChunk) into
// ReplicaInfo entries with live addresses.
func (s *PebbleStore) buildReplicas(ctx context.Context, nodeIDs []NodeID, groupID string) ([]ReplicaInfo, error) {
	nodeInfos := s.placement.GetNodeInfosBatch(nodeIDs)
	replicas := make([]ReplicaInfo, 0, len(nodeIDs))
	for shard, nodeID := range nodeIDs {
		addr := ""
		if info := nodeInfos[shard]; info != nil {
			addr = info.Addr
		} else {
			node, err := s.GetNode(ctx, nodeID)
			if err != nil {
				return nil, fmt.Errorf("allocate chunk: node %d not found: %w", nodeID, err)
			}
			addr = node.Addr
		}
		replica := ReplicaInfo{NodeID: nodeID, Addr: addr, State: ReplicaSyncing}
		if groupID != "" {
			replica.ShardIndex = shard
		}
		replicas = append(replicas, replica)
	}
	return replicas, nil
}

// allocateChunkViaPG resolves a chunk's placement through the placement-group
// authority (Metadata V2 serving path). It selects the replica node set via
// the PlacementEngine (reusing scoring + topology spread), derives a stable
// PG ID from the sorted node set, and reuses / creates that PG as the durable
// placement authority. Returns the replica set resolved at the PG's current
// epoch plus the PGID/Epoch recorded on the ChunkMeta.
func (s *PebbleStore) allocateChunkViaPG(ctx context.Context, policy PlacementPolicy) ([]ReplicaInfo, uint32, uint64, error) {
	nodeIDs, err := s.placement.PlaceChunk(policy, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	pgID := placementGroupIDForNodes(nodeIDs)
	pg, err := s.pgStore.SelectOrCreatePG(pgID, nodeIDs)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("allocate chunk via PG: %w", err)
	}
	nodes, _, err := s.pgStore.ResolveReplicas(pg.ID, pg.Epoch)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("allocate chunk via PG: resolve: %w", err)
	}
	replicas, err := s.buildReplicas(ctx, nodes, "")
	if err != nil {
		return nil, 0, 0, err
	}
	return replicas, pg.ID, pg.Epoch, nil
}

func (s *PebbleStore) nextChunkID() ChunkID {
	if s.chunkIDNext != nil {
		return s.chunkIDNext()
	}
	return s.chunkGen.Next()
}

// buildChunkAllocationConditional builds the atomic chunk-allocation batch:
// create each chunk row (with per-chunk not-exists preconditions) against a
// precondition that the inode row is unchanged, then rewrite the inode row
// with the freshly allocated ChunkMap refs appended.
func (s *PebbleStore) buildChunkAllocationConditional(inodeKey string, inodeRaw []byte, next *InodeMeta, chunks []*ChunkMeta) (*ConditionalBatch, error) {
	encodedInode, err := marshalValue(next, codecMsgpack)
	if err != nil {
		return nil, err
	}
	ops := make([]BatchOp, 0, len(chunks)+1)
	for _, chunk := range chunks {
		encoded, err := marshalValue(chunk, codecMsgpack)
		if err != nil {
			return nil, err
		}
		ops = append(ops, BatchOp{Key: []byte(chunkMetadataKey(chunk.ID)), Value: encoded})
	}
	ops = append(ops, BatchOp{Key: []byte(inodeKey), Value: encodedInode})
	// Removed inodeReferenceEpoch: per-inode CAS (inodeKey precondition)
	// is sufficient for consistency. The global epoch caused CAS contention
	// storms under concurrency without meaningful safety benefit — stale
	// placement is already handled by the data-write failure path.
	prepared, err := prepareReferenceAwareBatch(s.db, ops)
	if err != nil {
		return nil, err
	}
	preconditions := make([]ConditionalPrecondition, 0, 1+len(chunks)*2)
	preconditions = append(preconditions, ConditionalPrecondition{Key: []byte(inodeKey), ExpectedValue: append([]byte(nil), inodeRaw...)})
	// Per-chunk existence checks: each chunk must not already exist.
	for _, chunk := range chunks {
		preconditions = append(preconditions,
			ConditionalPrecondition{Key: []byte(chunkMetadataKey(chunk.ID)), ExpectAbsent: true},
			ConditionalPrecondition{Key: []byte(chunkTombstoneKey(chunk.ID)), ExpectAbsent: true},
		)
	}
	return &ConditionalBatch{Version: chunkTombstoneFencedBatchVersion, Preconditions: preconditions, Mutations: prepared}, nil
}

func (s *PebbleStore) CommitChunk(ctx context.Context, chunkID ChunkID, checksum uint32) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := fmt.Sprintf("%s%d", prefixChunk, chunkID)
	raw, exists, err := s.readChunkTombstoneRaw(key)
	if err != nil {
		return err
	}
	if !exists {
		return ErrChunkNotFound
	}
	var chunk ChunkMeta
	if err := unmarshalValue(raw, &chunk); err != nil {
		return err
	}
	if chunk.State != ChunkCreated {
		return ErrChunkAlreadySealed
	}
	chunk.State = ChunkSealed
	chunk.Checksum = checksum
	if err := s.updateLiveChunkMetadata(ctx, raw, &chunk); err != nil {
		return err
	}
	s.publishEvent(Event{Type: EventSet, Key: fmt.Sprintf("chunk:%d", chunkID)})
	return nil
}

func (s *PebbleStore) GetChunk(ctx context.Context, chunkID ChunkID) (*ChunkMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var chunk ChunkMeta
	exists, err := s.getValue(fmt.Sprintf("%s%d", prefixChunk, chunkID), &chunk)
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
	if chunk == nil {
		return ErrInvalidArgument
	}
	key := fmt.Sprintf("%s%d", prefixChunk, chunk.ID)
	// Verify chunk exists before update
	raw, exists, err := s.readChunkTombstoneRaw(key)
	if err != nil {
		return err
	}
	if !exists {
		return ErrChunkNotFound
	}
	if hook := s.chunkUpdateBeforeConditional; hook != nil {
		hook()
	}
	if err := s.updateLiveChunkMetadata(ctx, raw, chunk); err != nil {
		return err
	}
	s.publishEvent(Event{Type: EventSet, Key: fmt.Sprintf("chunk:%d", chunk.ID)})
	return nil
}

func (s *PebbleStore) SealChunk(ctx context.Context, chunkID ChunkID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := fmt.Sprintf("%s%d", prefixChunk, chunkID)
	raw, exists, err := s.readChunkTombstoneRaw(key)
	if err != nil {
		return err
	}
	if !exists {
		return ErrChunkNotFound
	}
	var chunk ChunkMeta
	if err := unmarshalValue(raw, &chunk); err != nil {
		return err
	}
	if chunk.State == ChunkReady {
		return nil
	}
	if chunk.State != ChunkSealed {
		return ErrChunkNotSealed
	}
	chunk.State = ChunkReady
	if err := s.updateLiveChunkMetadata(ctx, raw, &chunk); err != nil {
		return err
	}
	s.publishEvent(Event{Type: EventSet, Key: fmt.Sprintf("chunk:%d", chunkID)})
	return nil
}

func (s *PebbleStore) ListChunks(ctx context.Context, inodeID InodeID) ([]ChunkRef, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var meta InodeMeta
	exists, err := s.getValue(fmt.Sprintf("%s%d", prefixInode, inodeID), &meta)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrInodeNotFound
	}
	return meta.ChunkMap, nil
}

func (s *PebbleStore) DeleteChunk(ctx context.Context, chunkID ChunkID) error {
	created, err := s.tombstoneChunk(ctx, chunkID, "deleted by metadata API")
	if err != nil {
		return err
	}
	if created {
		s.publishEvent(Event{Type: EventDelete, Key: fmt.Sprintf("chunk:%d", chunkID)})
	}
	return nil
}

func (s *PebbleStore) ReportChunkState(ctx context.Context, nodeID NodeID, states map[ChunkID]ReplicaState) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if len(states) == 0 {
		return nil
	}
	return s.batchUpdateChunkStatesCtx(ctx, nodeID, states)
}

const maxBatchOps = 1000

// batchUpdateChunkStates updates replica states for multiple chunks in a single batch.
// It pre-fetches all chunk metadata in a single iterator pass instead of doing
// one Get per chunk, which is O(N) seeks vs O(N log N) with sorted iteration (P2.10).
func (s *PebbleStore) batchUpdateChunkStates(nodeID NodeID, states map[ChunkID]ReplicaState) error {
	return s.batchUpdateChunkStatesCtx(context.Background(), nodeID, states)
}

func (s *PebbleStore) batchUpdateChunkStatesCtx(ctx context.Context, nodeID NodeID, states map[ChunkID]ReplicaState) error {
	if len(states) == 0 {
		return nil
	}

	// Build the list of chunk keys to fetch
	keys := make([]string, 0, len(states))
	for chunkID := range states {
		keys = append(keys, fmt.Sprintf("%s%d", prefixChunk, chunkID))
	}

	// Batch-fetch all chunk metadata in one iterator pass
	rawValues, err := s.getValuesBatch(keys)
	if err != nil {
		return fmt.Errorf("batch update: prefetch: %w", err)
	}

	for chunkID, state := range states {
		key := fmt.Sprintf("%s%d", prefixChunk, chunkID)
		raw, exists := rawValues[key]
		if !exists {
			continue
		}
		var chunk ChunkMeta
		if err := unmarshalValue(raw, &chunk); err != nil {
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
		// Reflect replica loss on the chunk's overall state so a data-carrying
		// chunk that has had a replica marked ReplicaFailed is surfaced as
		// ChunkDegraded. Upgrade back to ChunkReady is left to the scrubber /
		// anti-entropy which have full visibility into the expected replica set;
		// partial heartbeat batches may not include all nodes, so auto-upgrading
		// here would risk falsely marking a chunk as ready when some replicas
		// are still failed but unreported.
		prevState := chunk.State
		if state == ReplicaFailed && (chunk.State == ChunkSealed || chunk.State == ChunkReady) {
			chunk.State = ChunkDegraded
		}
		changed := chunk.State != prevState
		if err := s.updateLiveChunkMetadata(ctx, raw, &chunk); err != nil {
			if errors.Is(err, ErrBackupMetadataConflict) {
				continue
			}
			return err
		}
		// Mirror the chunk degrade onto its V2 extent (roadmap §1.4): the
		// /extent-meta/{id} row is co-located with the chunk row (both written
		// through the inode's shard), so the extent is reached locally. The call
		// is idempotent and keyed on the resulting state (not `changed`) so a
		// failed mirror is retried by the datanode's next heartbeat — the delta
		// is re-sent because lastKnownState only advances on success. The chunk
		// state is the durable truth; this is a derived mirror, fail-closed so a
		// transient write failure surfaces rather than silently skipping.
		if chunk.State == ChunkDegraded {
			if err := s.MarkExtentDegraded(ctx, ExtentIDV2(chunkID)); err != nil {
				return fmt.Errorf("batch update: mark extent %d degraded: %w", chunkID, err)
			}
		}
		if changed {
			s.publishEvent(Event{Type: EventSet, Key: fmt.Sprintf("chunk:%d", chunkID)})
		}
	}
	return nil
}

// ========== ClusterService Implementation ==========

func (s *PebbleStore) RegisterNode(ctx context.Context, info *NodeInfo) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if s.throttle != nil && !s.throttle.Allow(info.ID) {
		return ErrTooManyRequests
	}
	key := prefixNode + fmt.Sprintf("%d", info.ID)
	var existing NodeInfo
	exists, err := s.getValue(key, &existing)
	if err != nil {
		return err
	}
	if exists {
		// The same node is re-registering (process restart, container
		// recreation). Refresh its address, liveness, and EC shard-disk
		// count so peers can route to the current reachable endpoint and an
		// EC coordinator sees the node's true candidate-disk topology. The
		// address may have changed (e.g. new container hostname/port).
		changed := false
		if info.Addr != "" && info.Addr != existing.Addr {
			existing.Addr = info.Addr
			changed = true
		}
		if info.ShardDiskCount > 0 && info.ShardDiskCount != existing.ShardDiskCount {
			existing.ShardDiskCount = info.ShardDiskCount
			changed = true
		}
		// Refresh fault-domain identity (rack/zone/machine/tier) on restart so
		// the admin topology re-groups correctly even when the node was first
		// registered before these were reported.
		if info.Rack != "" && info.Rack != existing.Rack {
			existing.Rack = info.Rack
			changed = true
		}
		if info.Zone != "" && info.Zone != existing.Zone {
			existing.Zone = info.Zone
			changed = true
		}
		if info.MachineID != "" && info.MachineID != existing.MachineID {
			existing.MachineID = info.MachineID
			changed = true
		}
		if changed {
			if err := s.putMsgpack(key, &existing); err != nil {
				return err
			}
			s.placement.UpdateNode(&existing)
			s.publishNodeEvent(key, &existing)
		}
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
	chunkMap := make(map[NodeID][]ChunkID)
	for _, node := range nodes {
		chunks, err := s.ChunksByNode(ctx, node.ID)
		if err != nil {
			slog.Error("auto-rebalance: failed to list chunks by node", "node_id", node.ID, "error", err)
			return
		}
		for _, chunk := range chunks {
			chunkMap[node.ID] = append(chunkMap[node.ID], chunk.ID)
		}
	}

	result := planner.PlanRebalanceWithChunks(nodes, chunkMap, 0.15) // 15% imbalance threshold
	if result == nil || len(result.Plans) == 0 {
		slog.Info("auto-rebalance: cluster is balanced, no action needed")
		return
	}
	plans := result.Plans[:0]
	for _, plan := range result.Plans {
		if plan.ChunkID != 0 {
			plans = append(plans, plan)
		}
	}
	if len(plans) == 0 {
		slog.Warn("auto-rebalance: no concrete migration plans available")
		return
	}

	slog.Info("auto-rebalance: triggering rebalance",
		"plans", len(plans),
		"imbalance", fmt.Sprintf("%.1f%%", result.Imbalance*100))

	executor := NewRebalanceExecutor(s)
	if err := executor.ExecutePlans(ctx, plans); err != nil {
		slog.Error("auto-rebalance: execution failed", "error", err)
		return
	}
	slog.Info("auto-rebalance: completed successfully")
}

func (s *PebbleStore) Heartbeat(ctx context.Context, nodeID NodeID, report *NodeReport) error {
	if err := s.HeartbeatLiveness(ctx, nodeID, report); err != nil {
		return err
	}
	if report != nil && len(report.ChunkStates) > 0 {
		if err := s.ReportChunkState(ctx, nodeID, report.ChunkStates); err != nil {
			return err
		}
	}
	if report != nil && len(report.ChangeEvents) > 0 {
		if _, err := s.ReconcileChangeEvents(ctx, nodeID, report.ChangeEvents); err != nil {
			return err
		}
	}
	return nil
}

// ReconcileChangeEvents consumes the change-journal events a node shipped on
// its heartbeat (§12) and returns the highest sequence the metadata authority
// has fully processed — the watermark the node may Ack. It is called by the
// HTTP heartbeat handler after liveness/chunk-state processing so a batch
// that fails to reconcile is not acked (the node will reship it next round).
//
// Event → action mapping:
//   - corrupt  : the extent on this node failed a checksum/decrypt read. Mark
//     this node's replica of the chunk ReplicaFailed and trigger a repair so a
//     fresh copy replaces it.
//   - disk_lost / segment_lost : the node lost storage. Conservatively mark
//     every chunk this node reports as ReplicaFailed and trigger repairs. (The
//     event carries only the lost disk/segment, not its chunk list, so the
//     node-level sweep is the safe reconciliation.)
//   - informational kinds (relocated, third_replica_complete, repair_created,
//     scrub_finding, delete_complete) : logged; replica reconciliation for
//     these is driven by the repair/relocation/delete paths themselves.
//
// The returned watermark is the highest event sequence processed without
// error; on any reconciliation error the method returns (0, err) so the node
// does not advance past un-processed events.
func (s *PebbleStore) ReconcileChangeEvents(ctx context.Context, nodeID NodeID, events []ChangeEventRecord) (uint64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	var maxSeq uint64
	for _, ev := range events {
		if err := s.reconcileOneChangeEvent(ctx, nodeID, ev); err != nil {
			return 0, fmt.Errorf("reconcile %s: %w", ev.Kind, err)
		}
		if ev.Seq > maxSeq {
			maxSeq = ev.Seq
		}
	}
	// Persist the reconciled watermark so the node can safely advance its
	// local journal Ack once metadata has caught up (§12).
	if err := s.advanceChangeAck(ctx, nodeID, maxSeq); err != nil {
		return 0, err
	}
	return maxSeq, nil
}

// advanceChangeAck bumps the node's persisted change-journal watermark to seq
// (monotonic: never lowers it).
func (s *PebbleStore) advanceChangeAck(ctx context.Context, nodeID NodeID, seq uint64) error {
	key := prefixNode + fmt.Sprintf("%d", nodeID)
	var info NodeInfo
	exists, err := s.getValue(key, &info)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNodeNotFound
	}
	if seq <= info.ChangeAck {
		return nil // already advanced past this
	}
	info.ChangeAck = seq
	return s.putMsgpack(key, &info)
}

// GetChangeAck returns the highest change-journal sequence metadata has
// reconciled for this node — the watermark the node may safely Ack. 0 if the
// node is unknown or no events were ever reconciled.
func (s *PebbleStore) GetChangeAck(ctx context.Context, nodeID NodeID) (uint64, error) {
	key := prefixNode + fmt.Sprintf("%d", nodeID)
	var info NodeInfo
	exists, err := s.getValue(key, &info)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, ErrNodeNotFound
	}
	return info.ChangeAck, nil
}

// AckChangeEvents is the dedicated change-ack RPC (Option B transport). It
// returns the node's persisted reconciled watermark — the sequence the node
// may have the metadata authority confirm before advancing its local journal
// Ack. Metadata does not trust the node's assertion; it reports what it has in
// fact reconciled, so the node advances only past actually-processed events.
func (s *PebbleStore) AckChangeEvents(ctx context.Context, nodeID NodeID, _ uint64) (uint64, error) {
	if s.closed.Load() {
		return 0, ErrServiceClosed
	}
	return s.GetChangeAck(ctx, nodeID)
}

func (s *PebbleStore) reconcileOneChangeEvent(ctx context.Context, nodeID NodeID, ev ChangeEventRecord) error {
	switch ev.Kind {
	case ChangeCorrupt:
		if ev.ExtentID == 0 {
			return nil // no extent to reconcile against
		}
		return s.markReplicaFailedAndRepair(ctx, nodeID, ChunkID(ev.ExtentID))
	case ChangeDiskLost, ChangeSegmentLost:
		return s.markNodeReplicasFailedAndRepair(ctx, nodeID)
	case ChangeRelocated, ChangeThirdReplicaComplete, ChangeRepairCreated,
		ChangeScrubFinding, ChangeDeleteComplete:
		slog.Debug("metadata: info change event ignored", "kind", ev.Kind.String(), "node", nodeID)
		return nil
	default:
		return nil
	}
}

// markReplicaFailedAndRepair marks nodeID's replica of chunkID ReplicaFailed
// and triggers a repair for it (so a fresh copy is re-replicated elsewhere).
func (s *PebbleStore) markReplicaFailedAndRepair(ctx context.Context, nodeID NodeID, chunkID ChunkID) error {
	if err := s.ReportChunkState(ctx, nodeID, map[ChunkID]ReplicaState{chunkID: ReplicaFailed}); err != nil {
		return err
	}
	slog.Info("metadata: replica marked failed from change event",
		"node", nodeID, "chunk", chunkID)
	return s.TriggerRepair(ctx, chunkID)
}

// markNodeReplicasFailedAndRepair marks every chunk this node reports as
// ReplicaFailed (storage loss) and triggers repairs for them.
func (s *PebbleStore) markNodeReplicasFailedAndRepair(ctx context.Context, nodeID NodeID) error {
	chunks, err := s.ChunksByNode(context.Background(), nodeID)
	if err != nil {
		return err
	}
	states := make(map[ChunkID]ReplicaState, len(chunks))
	for _, c := range chunks {
		if c.ID != 0 {
			states[c.ID] = ReplicaFailed
		}
	}
	if len(states) == 0 {
		return nil
	}
	if err := s.ReportChunkState(ctx, nodeID, states); err != nil {
		return err
	}
	for id := range states {
		if err := s.TriggerRepair(ctx, id); err != nil {
			slog.Warn("metadata: trigger repair after storage loss", "chunk", id, "error", err)
		}
	}
	slog.Info("metadata: marked replicas failed after storage loss", "node", nodeID, "chunks", len(states))
	return nil
}

// HeartbeatLiveness updates node liveness, load, and placement state
// without touching chunk replica state. The ShardedStore splits a
// heartbeat into a liveness broadcast (all shards, for placement) and a
// per-shard chunk-state update, so it can call this directly.
func (s *PebbleStore) HeartbeatLiveness(ctx context.Context, nodeID NodeID, report *NodeReport) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if s.throttle != nil && !s.throttle.Allow(nodeID) {
		return ErrTooManyRequests
	}
	key := prefixNode + fmt.Sprintf("%d", nodeID)
	var info NodeInfo
	exists, err := s.getValue(key, &info)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNodeNotFound
	}
	info.LastSeen = time.Now().UnixNano()
	// A heartbeat proves the node is alive but must not undo an explicit
	// operator/admin state change on the same path that keeps it alive.
	// Decommission (NodeDraining) and maintenance (NodeMaint) are sticky:
	// decommissioning a node must keep excluding it from new placements
	// (PlaceChunk filters n.State != NodeOnline) even while the node keeps
	// heartbeating as its data drains away. Only promote a node that the
	// lease manager previously marked NodeOffline (heartbeat loss) back to
	// online when liveness resumes; leave draining/maintenance/failed as-is.
	if info.State == NodeOffline {
		info.State = NodeOnline
	}
	if report != nil {
		info.UsedGB = report.UsedGB
		info.UsedBytes = report.UsedBytes
		info.ChunkCount = report.ChunkCount
		// Persist the node's reported physical capacity (GB + exact bytes) so
		// the admin console can render honest per-node capacity/usage%. Only
		// nodes that report it get updated; legacy nodes keep their register.
		if report.TotalCapacityBytes > 0 {
			info.CapacityGB = report.TotalCapacityBytes / (1024 * 1024 * 1024)
			info.CapacityBytes = report.TotalCapacityBytes
		}
		// Persist the node's physical on-disk footprint (GB + exact bytes)
		// alongside logical UsedGB/UsedBytes so consoles can show both honestly
		// — they diverge under overwrite/delete until seal+compaction reclaims
		// superseded record generations.
		if report.OnDiskBytes > 0 {
			info.OnDiskGB = report.OnDiskBytes / (1024 * 1024 * 1024)
			info.OnDiskBytes = report.OnDiskBytes
		}
		s.placement.UpdateLoad(nodeID, report.DiskIO)
		s.placement.UpdateErrorRate(nodeID, report.WriteErrorRate)
	}
	if err := s.putMsgpack(key, &info); err != nil {
		return err
	}
	// Update lease manager index for efficient expiration checks
	if s.lease != nil {
		s.lease.UpdateNode(info.ID, key, info.LastSeen)
	}
	s.placement.UpdateNode(&info)
	s.publishNodeEvent(key, &info)
	return nil
}

func (s *PebbleStore) DecommissionNode(ctx context.Context, nodeID NodeID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := prefixNode + fmt.Sprintf("%d", nodeID)
	var info NodeInfo
	exists, err := s.getValue(key, &info)
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

// RestoreNode brings a node that was explicitly taken out of service back to
// online. It inverts DecommissionNode, EnterMaintenance, and the transient
// NodeOffline/NodeFailed states — including the terminal NodeDecommissioned
// state reached once a drained node holds zero replicas — making decommission
// reversible. This is the missing half of the sticky-decommission change
// (commit 346c884): heartbeats only promote NodeOffline → NodeOnline and never
// touch an operator-set NodeDraining, so the ONLY way a decommissioned node
// returns to service is this explicit call.
//
// It is a deliberate, operator-authenticated control-plane action, so it does
// not weaken decommission stickiness: an idle heartbeat still cannot silently
// resurrect a drained node. Placement immediately considers the node a target
// again on return, matching what DecommissionNode revoked.
//
// No-op if the node is already online. Fails if the node does not exist.
func (s *PebbleStore) RestoreNode(ctx context.Context, nodeID NodeID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := prefixNode + fmt.Sprintf("%d", nodeID)
	var info NodeInfo
	exists, err := s.getValue(key, &info)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNodeNotFound
	}
	if info.State == NodeOnline {
		return nil // already online
	}
	info.State = NodeOnline
	info.LastSeen = time.Now().UnixNano()
	if err := s.putMsgpack(key, &info); err != nil {
		return err
	}
	s.placement.UpdateNode(&info)
	slog.Info("node restored to online", "nodeID", nodeID)
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
	exists, err := s.getValue(key, &info)
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
	exists, err := s.getValue(key, &info)
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
	exists, err := s.getValue(prefixNode+fmt.Sprintf("%d", nodeID), &info)
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

// GetHealthChecker returns the health checker for this store, or nil if not configured.
func (s *PebbleStore) GetHealthChecker() *HealthChecker {
	return s.health
}

// NodeMetrics returns per-node runtime metrics (error rate, load, capacity)
// from the placement engine. Used by the Prometheus exporter.
func (s *PebbleStore) NodeMetrics() []NodeMetrics {
	return s.placement.GetNodeMetrics()
}

// SetMetrics attaches a metrics collector to this store.
func (s *PebbleStore) SetMetrics(m *Metrics) {
	s.metrics = m
}

// PebbleStats returns a snapshot of Pebble's internal metrics for
// diagnosing compaction stalls and write pressure.
type PebbleStatsSnapshot struct {
	L0Files           int64  `json:"l0_files"`
	L0Sublevels       int32  `json:"l0_sublevels"`
	CompactionDebt    uint64 `json:"compaction_debt_bytes"`
	CompactionPending int64  `json:"compaction_in_progress"`
	MemTableSize      uint64 `json:"memtable_bytes"`
	WALSize           uint64 `json:"wal_bytes"`
}

func (s *PebbleStore) PebbleStats() PebbleStatsSnapshot {
	m := s.db.Metrics()
	return PebbleStatsSnapshot{
		L0Files:           m.Levels[0].NumFiles,
		L0Sublevels:       m.Levels[0].Sublevels,
		CompactionDebt:    m.Compact.EstimatedDebt,
		CompactionPending: m.Compact.NumInProgress,
		MemTableSize:      m.MemTable.Size,
		WALSize:           m.WAL.Size,
	}
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

// ScanExtents iterates over all V2 extent metadata (used by the extent
// scrubber, roadmap §1.4). Corrupted /extent-meta rows are logged and
// skipped, mirroring ScanAllChunks.
func (s *PebbleStore) ScanExtents(ctx context.Context, fn func(*ExtentMetaV2) error) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.scanPrefix(prefixExtentMeta, func(key, val []byte) error {
		var ext ExtentMetaV2
		if err := unmarshalValue(val, &ext); err != nil {
			slog.Warn("pebble store: corrupted extent entry", "key", string(key), "error", err)
			return nil
		}
		return fn(&ext)
	})
}

// CountNodeReplicas scans every chunk and counts how many replicas (in any
// sync state) the given node currently hosts. The decommission drain-completion
// check uses this: a draining node is only "fully drained" once it holds zero
// replicas, at which point it may safely advance to the terminal
// NodeDecommissioned state.
func (s *PebbleStore) CountNodeReplicas(ctx context.Context, nodeID NodeID) (int, error) {
	if s.closed.Load() {
		return 0, ErrServiceClosed
	}
	count := 0
	err := s.scanPrefix(prefixChunk, func(key, val []byte) error {
		var chunk ChunkMeta
		if err := unmarshalValue(val, &chunk); err != nil {
			slog.Warn("pebble store: corrupted chunk entry", "key", string(key), "error", err)
			return nil
		}
		for _, r := range chunk.Replicas {
			if r.NodeID == nodeID {
				count++
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
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
	return s.putMsgpack(key, &task)
}

// TriggerExtentRepair enqueues a repair task for the chunk backing a V2
// extent (extent ID == chunk ID invariant). It is the extent-aware repair
// trigger (roadmap §1.4): the data-plane repair path (datanode RepairWorker)
// is shared with V1 chunks — only the trigger knows about the extent model.
// The extent row is validated first so a stale/nonexistent extent fails fast
// (ErrExtentNotFound) instead of queueing a repair for a chunk that may have
// been GC'd. The reason string is surfaced verbatim by /api/v1/repair/queue so
// extent-triggered repairs are distinguishable from heartbeat/rebalance ones.
func (s *PebbleStore) TriggerExtentRepair(ctx context.Context, extentID ExtentIDV2) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if _, err := s.GetExtentMeta(ctx, extentID); err != nil {
		return err // ErrExtentNotFound when the row is gone
	}
	key := fmt.Sprintf("%s%d", prefixRepair, uint64(extentID))
	task := RepairTask{
		ChunkID:   ChunkID(extentID),
		Reason:    "extent_unhealthy",
		Priority:  1,
		CreatedAt: time.Now(),
	}
	return s.putMsgpack(key, &task)
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

// TransferLeadership initiates a Raft leadership transfer. If targetID is
// empty, Raft automatically selects the best candidate. Returns once the
// transfer is initiated (the new leader may take 1-2 election timeouts).
func (s *PebbleStore) TransferLeadership(targetID string) error {
	if s.raft == nil {
		return fmt.Errorf("raft not enabled")
	}
	return s.raft.TransferLeadership(targetID)
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

// WaitReadFresh blocks until the local FSM has applied every entry committed
// as of the call — the linearizability gate for leader-served reads. A
// freshly elected leader can serve a local read while its FSM still lags
// committed entries by the apply-lag window; without this gate a client
// 307-redirected to the new leader reads stale rows. Returns immediately
// (zero raft round trips) when the FSM is already caught up; in the catch-up
// window it submits a raft Barrier. Non-raft (single-node) stores are always
// fresh. Returns an error (caller should 503/redirect) if the barrier fails,
// times out, or the context is cancelled.
func (s *PebbleStore) WaitReadFresh(ctx context.Context, timeout time.Duration) error {
	if s.raft == nil {
		return nil
	}
	return s.raft.WaitCatchUp(ctx, timeout)
}

// applyBatch commits multiple key-value pairs atomically via Raft or directly.
// Values are serialized using msgpack for better performance on hot paths.
func (s *PebbleStore) applyBatch(ops []batchOp, deletes []string) error {
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

// applyBatchMsgpack commits multiple msgpack-encoded key-value pairs atomically via Raft or directly.
func (s *PebbleStore) applyBatchMsgpack(ops []batchOp, deletes []string) error {
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
		s.mu.Lock()
		err := applyReferenceAwareBatch(s.db, []BatchOp{{Delete: op == OpDelete, Key: []byte(key), Value: value}}, pebble.NoSync)
		s.mu.Unlock()
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
	err := s.raft.applyTrustedAutoForward(entry, 10*time.Second)
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
		s.mu.Lock()
		err := applyReferenceAwareBatch(s.db, ops, pebble.NoSync)
		s.mu.Unlock()
		if err == nil && s.metrics != nil {
			s.metrics.RecordWrite(time.Since(start))
		}
		return err
	}

	entry := &RaftLogEntry{
		Op:    OpBatch,
		Batch: ops,
	}
	err := s.raft.applyTrustedAutoForward(entry, 10*time.Second)
	if err == nil && s.metrics != nil {
		s.metrics.RecordWrite(time.Since(start))
	}
	return err
}

// applyReferenceAwareBatch is the only non-conditional metadata mutation path.
// It keeps inode-reference changes and their epoch in the same Pebble commit.
func applyReferenceAwareBatch(db *pebble.DB, ops []BatchOp, sync *pebble.WriteOptions) error {
	prepared, err := prepareReferenceAwareBatch(db, ops)
	if err != nil {
		return err
	}
	batch := db.NewBatch()
	defer batch.Close()
	for _, op := range prepared {
		if op.Delete {
			if err := batch.Delete(op.Key, nil); err != nil {
				return err
			}
			continue
		}
		if err := batch.Set(op.Key, op.Value, nil); err != nil {
			return err
		}
	}
	return batch.Commit(sync)
}

func prepareReferenceAwareBatch(db *pebble.DB, ops []BatchOp) ([]BatchOp, error) {
	return prepareReferenceAwareBatchWithEpoch(db, ops, nil)
}

func prepareReferenceAwareBatchWithEpoch(db *pebble.DB, ops []BatchOp, epochOverride *rawReferenceValue) ([]BatchOp, error) {
	prepared := append([]BatchOp(nil), ops...)
	overlay := make(map[string]rawReferenceValue)
	referencesChanged := false

	readRaw := func(key string) (rawReferenceValue, error) {
		if value, ok := overlay[key]; ok {
			return value, nil
		}
		value, closer, err := db.Get([]byte(key))
		if errors.Is(err, pebble.ErrNotFound) {
			raw := rawReferenceValue{}
			overlay[key] = raw
			return raw, nil
		}
		if err != nil {
			return rawReferenceValue{}, err
		}
		raw := rawReferenceValue{found: true, value: append([]byte(nil), value...)}
		closer.Close()
		overlay[key] = raw
		return raw, nil
	}

	for _, op := range ops {
		key := string(op.Key)
		if key == keyInodeReferenceEpoch {
			return nil, fmt.Errorf("inode reference epoch is internal")
		}
		if isInodeMetadataKey(key) {
			oldRaw, err := readRaw(key)
			if err != nil {
				return nil, fmt.Errorf("inode reference epoch: read %q: %w", key, err)
			}
			oldMeta, oldFound, err := decodeReferencedInode(key, oldRaw)
			if err != nil {
				return nil, err
			}

			var newMeta InodeMeta
			newFound := !op.Delete
			if newFound {
				if err := unmarshalValue(op.Value, &newMeta); err != nil {
					return nil, fmt.Errorf("inode reference epoch: decode new inode %q: %w", key, err)
				}
				if err := validateInodeKeyIdentity(key, newMeta.ID); err != nil {
					return nil, err
				}
			}

			if chunkMapsDiffer(oldMeta, oldFound, newMeta, newFound) {
				referencesChanged = true
				for chunkID := range addedChunkReferences(oldMeta, oldFound, newMeta, newFound) {
					chunkRaw, err := readRaw(chunkMetadataKey(chunkID))
					if err != nil {
						return nil, fmt.Errorf("inode reference epoch: read chunk %d: %w", chunkID, err)
					}
					tombstoneRaw, err := readRaw(chunkTombstoneKey(chunkID))
					if err != nil {
						return nil, fmt.Errorf("inode reference epoch: read tombstone %d: %w", chunkID, err)
					}
					if !chunkRaw.found || tombstoneRaw.found {
						return nil, fmt.Errorf("inode reference epoch: chunk %d is unavailable for attachment", chunkID)
					}
				}
			}
			overlay[key] = rawReferenceValue{found: newFound, value: append([]byte(nil), op.Value...)}
			continue
		}

		if isChunkMetadataKey(key) && !op.Delete {
			chunkID, err := parseChunkTombstoneKey(key, prefixChunk)
			if err != nil {
				return nil, err
			}
			tombstoneRaw, err := readRaw(chunkTombstoneKey(chunkID))
			if err != nil {
				return nil, fmt.Errorf("chunk mutation: read tombstone %d: %w", chunkID, err)
			}
			if tombstoneRaw.found {
				return nil, fmt.Errorf("chunk mutation: chunk %d is tombstoned", chunkID)
			}
		}
		overlay[key] = rawReferenceValue{found: !op.Delete, value: append([]byte(nil), op.Value...)}
	}

	if !referencesChanged {
		return prepared, nil
	}
	// Epoch handling removed: per-inode CAS is sufficient for
	// allocation correctness; global epoch caused contention storms.
	return prepared, nil
}

type rawReferenceValue struct {
	found bool
	value []byte
}

func decodeReferencedInode(key string, raw rawReferenceValue) (InodeMeta, bool, error) {
	if !raw.found {
		return InodeMeta{}, false, nil
	}
	var meta InodeMeta
	if err := unmarshalValue(raw.value, &meta); err != nil {
		return InodeMeta{}, false, fmt.Errorf("inode reference epoch: decode stored inode %q: %w", key, err)
	}
	if err := validateInodeKeyIdentity(key, meta.ID); err != nil {
		return InodeMeta{}, false, err
	}
	return meta, true, nil
}

func isInodeMetadataKey(key string) bool {
	return strings.HasPrefix(key, prefixInode) && validInodeMetadataKey(key)
}

func validInodeMetadataKey(key string) bool {
	value := strings.TrimPrefix(key, prefixInode)
	if value == "" {
		return false
	}
	id, err := strconv.ParseUint(value, 10, 64)
	return err == nil && prefixInode+strconv.FormatUint(id, 10) == key
}

func validateInodeKeyIdentity(key string, inodeID InodeID) error {
	if !validInodeMetadataKey(key) {
		return fmt.Errorf("inode reference epoch: invalid inode key %q", key)
	}
	id, _ := strconv.ParseUint(strings.TrimPrefix(key, prefixInode), 10, 64)
	if inodeID != InodeID(id) {
		return fmt.Errorf("inode reference epoch: inode key and value identities differ for %q", key)
	}
	return nil
}

func isChunkMetadataKey(key string) bool {
	return strings.HasPrefix(key, prefixChunk) && validChunkMetadataKey(key)
}

func validChunkMetadataKey(key string) bool {
	_, err := parseChunkTombstoneKey(key, prefixChunk)
	return err == nil
}

func chunkMapsDiffer(old InodeMeta, oldFound bool, next InodeMeta, nextFound bool) bool {
	if oldFound != nextFound {
		return true
	}
	if !oldFound || len(old.ChunkMap) != len(next.ChunkMap) {
		return oldFound
	}
	for i := range old.ChunkMap {
		if old.ChunkMap[i] != next.ChunkMap[i] {
			return true
		}
	}
	return false
}

func addedChunkReferences(old InodeMeta, oldFound bool, next InodeMeta, nextFound bool) map[ChunkID]struct{} {
	added := make(map[ChunkID]struct{})
	if !nextFound {
		return added
	}
	previous := make(map[ChunkID]struct{}, len(old.ChunkMap))
	if oldFound {
		for _, ref := range old.ChunkMap {
			previous[ref.ID] = struct{}{}
		}
	}
	for _, ref := range next.ChunkMap {
		if _, found := previous[ref.ID]; !found {
			added[ref.ID] = struct{}{}
		}
	}
	return added
}

func encodeInodeReferenceEpoch(epoch uint64) []byte {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, epoch)
	return raw
}

func decodeInodeReferenceEpoch(raw rawReferenceValue) (uint64, error) {
	if !raw.found {
		return 0, nil
	}
	if len(raw.value) != 8 {
		return 0, fmt.Errorf("inode reference epoch is malformed")
	}
	return binary.BigEndian.Uint64(raw.value), nil
}

func inodeReferenceEpochPrecondition(raw rawReferenceValue) ConditionalPrecondition {
	precondition := ConditionalPrecondition{Key: []byte(keyInodeReferenceEpoch)}
	if raw.found {
		precondition.ExpectedValue = append([]byte(nil), raw.value...)
	} else {
		precondition.ExpectAbsent = true
	}
	return precondition
}

func rawReferenceValuesEqual(left, right rawReferenceValue) bool {
	return left.found == right.found && (!left.found || bytes.Equal(left.value, right.value))
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
		if err := s.putMsgpack(key, &task); err != nil {
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
		return fmt.Errorf("target node %d: %w", toNode, ErrNodeNotFound)
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

// readBucketStats reads the current counter for a bucket. Returns zeros
// if the key does not exist (e.g., pre-migration).
func (s *PebbleStore) readBucketStats(rootInode InodeID) BucketUsage {
	var stats BucketUsage
	s.getValue(s.bucketStatsKey(rootInode), &stats)
	return stats
}

func (s *PebbleStore) ensureBucketStats(ctx context.Context) error {
	buckets, err := s.ListBuckets(ctx)
	if err != nil || len(buckets) == 0 {
		return err
	}
	for _, bucket := range buckets {
		var stats BucketUsage
		exists, err := s.getValue(s.bucketStatsKey(bucket.RootInode), &stats)
		if err != nil {
			return err
		}
		if !exists {
			wasEnabled := s.cfg.UseBucketStats
			s.cfg.UseBucketStats = false
			usages, scanErr := s.ComputeAllBucketUsage(ctx)
			s.cfg.UseBucketStats = wasEnabled
			if scanErr != nil {
				return scanErr
			}
			roots := make(map[string]InodeID, len(buckets))
			for _, item := range buckets {
				roots[item.Name] = item.RootInode
			}
			ops := make([]batchOp, 0, len(usages))
			for i := range usages {
				root, ok := roots[usages[i].Name]
				if !ok {
					continue
				}
				usage := usages[i]
				ops = append(ops, batchOp{Key: s.bucketStatsKey(root), Value: &usage})
			}
			return s.applyBatchMsgpack(ops, nil)
		}
	}
	return nil
}

// addBucketStatsOp adds a counter update to the pending batch ops.
// No-op when UseBucketStats is disabled. The delta values are applied
// to the current counter read at call time. Race-safe for admin-level
// precision (S3 usage metrics are eventually consistent).
func (s *PebbleStore) addBucketStatsOp(rootInode InodeID, deltaBytes int64, deltaObjects int, ops *[]batchOp) {
	if !s.cfg.UseBucketStats || rootInode == 0 {
		return
	}
	stats := s.readBucketStats(rootInode)
	stats.UsedBytes += deltaBytes
	stats.Objects += deltaObjects
	*ops = append(*ops, batchOp{Key: s.bucketStatsKey(rootInode), Value: &stats})
}

// getBucketRoot reads the parent inode and returns its BucketRoot.
// Used when creating new inodes to inherit the containing bucket.
func (s *PebbleStore) getBucketRoot(parent InodeID) InodeID {
	var parentMeta InodeMeta
	ok, _ := s.getValue(fmt.Sprintf("%s%d", prefixInode, parent), &parentMeta)
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
			// Model-aware (roadmap §1.4 "bucket 配额按 extent 聚合"): a V2
			// layout inode aggregates its extent metadata rather than its
			// Size counter, which the inline writer keeps in sync but a
			// rebuild should verify from the extents themselves.
			var in InodeMetaV2
			if err := unmarshalValue(val, &in); err == nil {
				sizes[InodeID(id)] = s.extentBytesForInode(ctx, &in)
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

// extentBytesForInode returns the byte usage of an inode aggregated from its
// V2 extent metadata (roadmap §1.4 "bucket 配额按 extent 聚合"): the inline
// extent's LogicalLen, or the sum of its resolved extent pages' LogicalLen.
// A missing /extent-meta/ row (a dangling reference left by a torn write)
// falls back to the inode's Size so a rebuild never under-counts. V1 rows
// and layout-less V2 rows fall back to Size — for V1 the row's Size is
// authoritative and this is the legacy behavior.
func (s *PebbleStore) extentBytesForInode(ctx context.Context, in *InodeMetaV2) int64 {
	switch in.Layout {
	case LayoutInlineExtent:
		if in.InlineExtent != nil {
			return in.InlineExtent.LogicalLen
		}
	case LayoutExtentPages:
		refs, err := NewExtentPageStore(s).ResolveExtents(in)
		if err == nil {
			var total int64
			for _, r := range refs {
				ext, err := s.GetExtentMeta(ctx, r.ExtentID)
				if err != nil {
					return in.Size // dangling extent — never under-count
				}
				total += ext.LogicalLen
			}
			return total
		}
	}
	return in.Size
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

// ScrubExtents iterates all V2 extent metadata and reports each row's
// Lifecycle and backing-chunk health without mutating anything (recovery is
// the ExtentScrubber worker's job). Mirrors ScrubAllChunks for the
// /api/v1/scrub ops endpoint. orphan=true means the backing chunk row is
// gone (a dangling /extent-meta row); healthy is meaningless when orphan.
func (s *PebbleStore) ScrubExtents(fn func(extentID ExtentIDV2, lifecycle ExtentLifecycle, healthy, orphan bool)) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.scanPrefix(prefixExtentMeta, func(key, val []byte) error {
		var ext ExtentMetaV2
		if err := unmarshalValue(val, &ext); err != nil {
			return nil
		}
		exists, healthy := s.extentBackingChunkHealth(&ext)
		fn(ext.ID, ext.Lifecycle, healthy, !exists)
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
	return s.applyBatchMsgpack([]batchOp{
		{Key: prefixACL + bucket, Value: &policy},
	}, nil)
}

// GetBucketPolicy retrieves the access control policy for a bucket.
func (s *PebbleStore) GetBucketPolicy(_ context.Context, bucket string) (*BucketPolicy, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var policy BucketPolicy
	exists, err := s.getValue(prefixACL+bucket, &policy)
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
	return s.applyBatchMsgpack(nil, []string{prefixACL + bucket})
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

// GetBucketQuota returns the configured quota for an existing bucket.
func (s *PebbleStore) GetBucketQuota(ctx context.Context, bucket string) (*BucketQuota, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	var quota BucketQuota
	exists, err := s.getValue(prefixQuota+bucket, &quota)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return &quota, nil
}

// SetBucketQuota configures a quota for an existing bucket.
func (s *PebbleStore) SetBucketQuota(ctx context.Context, bucket string, quota *BucketQuota) error {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	}
	if err := quota.Validate(); err != nil {
		return err
	}
	if err := s.SaveQuota(bucket, quota); err != nil {
		return err
	}
	if s.quota != nil {
		s.quota.LoadQuota(bucket, quota)
	}
	return nil
}

// DeleteBucketQuota clears the configured quota for an existing bucket.
func (s *PebbleStore) DeleteBucketQuota(ctx context.Context, bucket string) error {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	}
	if err := s.DeleteQuota(bucket); err != nil {
		return err
	}
	if s.quota != nil {
		s.quota.LoadQuota(bucket, nil)
	}
	return nil
}

// DeleteQuota removes a persisted quota entry.
func (s *PebbleStore) DeleteQuota(bucket string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.applyBatchMsgpack(nil, []string{prefixQuota + bucket})
}

// CheckBucketQuota checks actual write deltas for an existing bucket.
func (s *PebbleStore) CheckBucketQuota(ctx context.Context, bucket string, additionalBytes int64, additionalObjects int64) error {
	quota, err := s.GetBucketQuota(ctx, bucket)
	if err != nil {
		return err
	}
	if quota == nil {
		return nil
	}
	usage, err := s.GetBucketUsage(ctx, bucket)
	if err != nil {
		return err
	}
	usedBytes := usage.UsedBytes
	objects := int64(usage.Objects)
	if quota.MaxSizeBytes > 0 && additionalBytes > 0 && usedBytes > quota.MaxSizeBytes-additionalBytes {
		return fmt.Errorf("%w: bucket %s would exceed size limit (%d + %d > %d)",
			ErrQuotaExceeded, bucket, usedBytes, additionalBytes, quota.MaxSizeBytes)
	}
	if quota.MaxObjects > 0 && additionalObjects > 0 && objects > quota.MaxObjects-additionalObjects {
		return fmt.Errorf("%w: bucket %s would exceed object limit (%d + %d > %d)",
			ErrQuotaExceeded, bucket, objects, additionalObjects, quota.MaxObjects)
	}
	return nil
}

// GetBucketUsage returns the current aggregate usage for an existing bucket.
func (s *PebbleStore) GetBucketUsage(ctx context.Context, bucket string) (*BucketUsage, error) {
	b, err := s.GetBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if s.cfg.UseBucketStats {
		stats := s.readBucketStats(b.RootInode)
		stats.Name = b.Name
		return &stats, nil
	}
	all, err := s.ComputeAllBucketUsage(ctx)
	if err != nil {
		return nil, err
	}
	for i := range all {
		if all[i].Name == bucket {
			return &all[i], nil
		}
	}
	return &BucketUsage{Name: bucket}, nil
}

// loadQuotas restores previously persisted quota and usage data from Pebble
// into the QuotaManager's in-memory maps. Called during SetQuotaManager.
func (s *PebbleStore) loadQuotas(qm *QuotaManager) {
	// Load quotas
	s.scanPrefix(prefixQuota, func(key, val []byte) error {
		var quota BucketQuota
		if err := unmarshalValue(val, &quota); err != nil {
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
		if err := unmarshalValue(val, &usage); err != nil {
			slog.Warn("quota: failed to unmarshal usage entry", "key", string(key), "error", err)
			return nil
		}
		bucket := strings.TrimPrefix(string(key), prefixQuotaUsage)
		qm.LoadUsage(bucket, &usage)
		return nil
	})
}
