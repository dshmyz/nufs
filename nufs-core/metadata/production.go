package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

// ============================================================
// Watch/Notify — Change notification for Pebble
// ============================================================

// EventType represents the type of metadata change.
type EventType uint8

const (
	EventSet    EventType = iota // Key created or updated
	EventDelete                  // Key deleted
)

// Event represents a metadata change event.
type Event struct {
	Type  EventType
	Key   string
	Value []byte // nil for Delete events
	Time  time.Time
}

// Watcher is a subscription to metadata change events.
type Watcher struct {
	id     uint64
	prefix string
	ch     chan Event
	done   chan struct{}
}

// Events returns the channel for receiving events.
func (w *Watcher) Events() <-chan Event {
	return w.ch
}

// Close unsubscribes the watcher.
func (w *Watcher) Close() {
	close(w.done)
}

// EventBus is a publish-subscribe system for metadata change notifications.
type EventBus struct {
	mu       sync.RWMutex
	watchers map[uint64]*Watcher
	nextID   atomic.Uint64
	buffer   int // Channel buffer size
}

// NewEventBus creates a new event bus.
func NewEventBus(bufferSize int) *EventBus {
	if bufferSize <= 0 {
		bufferSize = 256
	}
	return &EventBus{
		watchers: make(map[uint64]*Watcher),
		buffer:   bufferSize,
	}
}

// Watch subscribes to events matching a key prefix.
func (eb *EventBus) Watch(prefix string) *Watcher {
	w := &Watcher{
		id:     eb.nextID.Add(1),
		prefix: prefix,
		ch:     make(chan Event, eb.buffer),
		done:   make(chan struct{}),
	}
	eb.mu.Lock()
	eb.watchers[w.id] = w
	eb.mu.Unlock()

	// Auto-cleanup when done
	go func() {
		<-w.done
		eb.mu.Lock()
		delete(eb.watchers, w.id)
		eb.mu.Unlock()
	}()

	return w
}

// Publish sends an event to all matching watchers.
func (eb *EventBus) Publish(event Event) {
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	for _, w := range eb.watchers {
		if hasPrefix(event.Key, w.prefix) {
			select {
			case w.ch <- event:
			default:
				// Drop event if buffer full (slow consumer)
			}
		}
	}
}

// WatcherCount returns the number of active watchers.
func (eb *EventBus) WatcherCount() int {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return len(eb.watchers)
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// ============================================================
// MVCC — Optimistic Locking for Concurrent Inode Updates
// ============================================================

// MVCCVersion tracks the version of an inode for optimistic locking.
// Each write increments the version; concurrent writers detect conflicts.
type MVCCVersion uint64

// InodeWithVersion wraps InodeMeta with a version for CAS operations.
type InodeWithVersion struct {
	InodeMeta
	Version MVCCVersion `json:"_v"`
}

// CASUpdateInode performs a compare-and-swap update on an inode.
// Returns ErrVersionConflict if the inode was modified since the read.
func (s *PebbleStore) CASUpdateInode(ctx context.Context, expected MVCCVersion, meta *InodeMeta) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if meta == nil {
		return ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%s%d", prefixInode, meta.ID)

	// Read current version
	var current InodeWithVersion
	val, closer, err := s.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return ErrInodeNotFound
	}
	if err != nil {
		return fmt.Errorf("cas get: %w", err)
	}
	data := make([]byte, len(val))
	copy(data, val)
	closer.Close()

	if err := json.Unmarshal(data, &current); err != nil {
		return fmt.Errorf("cas unmarshal: %w", err)
	}

	// Check version
	if current.Version != expected {
		return ErrVersionConflict
	}

	// Apply update
	meta.CTime = time.Now().UnixNano()
	wrapper := InodeWithVersion{
		InodeMeta: *meta,
		Version:   current.Version + 1,
	}
	newData, err := json.Marshal(&wrapper)
	if err != nil {
		return fmt.Errorf("cas marshal: %w", err)
	}

	return s.db.Set([]byte(key), newData, pebble.Sync)
}

// GetInodeWithVersion reads an inode along with its MVCC version.
func (s *PebbleStore) GetInodeWithVersion(ctx context.Context, id InodeID) (*InodeMeta, MVCCVersion, error) {
	if s.closed.Load() {
		return nil, 0, ErrServiceClosed
	}

	key := fmt.Sprintf("%s%d", prefixInode, id)
	val, closer, err := s.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return nil, 0, ErrInodeNotFound
	}
	if err != nil {
		return nil, 0, err
	}
	data := make([]byte, len(val))
	copy(data, val)
	closer.Close()

	var wrapper InodeWithVersion
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return nil, 0, err
	}
	return &wrapper.InodeMeta, wrapper.Version, nil
}

// ============================================================
// Lease — Automatic Node Expiry
// ============================================================

// LeaseManager monitors node heartbeats and marks offline nodes.
type LeaseManager struct {
	store         *PebbleStore
	events        *EventBus
	ttl           time.Duration // Heartbeat TTL (default: 30s)
	checkInterval time.Duration
	stopCh        chan struct{}
	running       atomic.Bool
}

// NewLeaseManager creates a lease manager for node liveness detection.
func NewLeaseManager(store *PebbleStore, events *EventBus, ttl time.Duration) *LeaseManager {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &LeaseManager{
		store:         store,
		events:        events,
		ttl:           ttl,
		checkInterval: ttl / 3,
		stopCh:        make(chan struct{}),
	}
}

// Start begins the lease monitoring loop.
func (lm *LeaseManager) Start() {
	if lm.running.Swap(true) {
		return
	}
	go lm.loop()
}

// Stop terminates the lease monitoring loop.
func (lm *LeaseManager) Stop() {
	if lm.running.Swap(false) {
		close(lm.stopCh)
	}
}

func (lm *LeaseManager) loop() {
	ticker := time.NewTicker(lm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			lm.checkExpiredNodes()
		case <-lm.stopCh:
			return
		}
	}
}

func (lm *LeaseManager) checkExpiredNodes() {
	now := time.Now().UnixNano()
	deadline := now - lm.ttl.Nanoseconds()

	lm.store.scanPrefix(prefixNode, func(key, val []byte) error {
		var info NodeInfo
		if err := json.Unmarshal(val, &info); err != nil {
			return nil
		}
		if info.State == NodeOnline && info.LastSeen < deadline {
			// Node missed heartbeat — mark offline via Raft
			info.State = NodeOffline
			info.LastSeen = now
			data, err := json.Marshal(&info)
			if err != nil {
				slog.Error("lease: marshal node", "node_id", info.ID, "error", err)
				return nil
			}
			if err := lm.store.applyViaRaft(OpSet, string(key), data); err != nil {
				slog.Error("lease: failed to mark node offline via raft", "node_id", info.ID, "error", err)
				return nil
			}
			slog.Warn("lease: node marked offline", "node_id", info.ID, "offline_since", time.Since(time.Unix(0, info.LastSeen)))

			// Publish event for repair worker
			if lm.events != nil {
				eventData, _ := json.Marshal(map[string]interface{}{
					"node_id": info.ID,
					"state":   "offline",
				})
				lm.events.Publish(Event{
					Type:  EventSet,
					Key:   string(key),
					Value: eventData,
				})
			}
		}
		return nil
	})
}

// ============================================================
// Orphan Chunk GC — Reclaim Unreferenced Storage
// ============================================================

// ChunkGC scans for chunks that have no inode references and deletes them.
type ChunkGC struct {
	store   *PebbleStore
	events  *EventBus
	dryRun  bool
	stopCh  chan struct{}
	running atomic.Bool
}

// NewChunkGC creates a chunk garbage collector.
func NewChunkGC(store *PebbleStore, events *EventBus, dryRun bool) *ChunkGC {
	return &ChunkGC{
		store:  store,
		events: events,
		dryRun: dryRun,
		stopCh: make(chan struct{}),
	}
}

// ScanResult holds the result of a GC scan.
type GCScanResult struct {
	TotalChunks   int
	OrphanChunks  int
	DeletedChunks int
	FreedBytes    int64
	ScanDuration  time.Duration
}

// Scan performs one pass of orphan chunk detection and cleanup.
func (gc *ChunkGC) Scan(ctx context.Context) (*GCScanResult, error) {
	start := time.Now()
	result := &GCScanResult{}

	// Phase 1: Collect all chunk IDs referenced by inodes
	referencedChunks := make(map[ChunkID]bool)
	err := gc.store.scanPrefix(prefixInode, func(key, val []byte) error {
		var meta InodeMeta
		if err := json.Unmarshal(val, &meta); err != nil {
			return nil
		}
		for _, ref := range meta.ChunkMap {
			referencedChunks[ref.ID] = true
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("gc scan inodes: %w", err)
	}

	// Phase 2: Find orphan chunks
	var orphans []ChunkID
	err = gc.store.scanPrefix(prefixChunk, func(key, val []byte) error {
		result.TotalChunks++
		var chunk ChunkMeta
		if err := json.Unmarshal(val, &chunk); err != nil {
			return nil
		}
		if !referencedChunks[chunk.ID] {
			result.OrphanChunks++
			orphans = append(orphans, chunk.ID)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("gc scan chunks: %w", err)
	}

	// Phase 3: Delete orphans
	for _, id := range orphans {
		if ctx.Err() != nil {
			break
		}
		chunk, err := gc.store.GetChunk(ctx, id)
		if err != nil {
			continue
		}
		if !gc.dryRun {
			gc.store.DeleteChunk(ctx, id)
			result.DeletedChunks++
			result.FreedBytes += int64(chunk.Size)
		} else {
			result.DeletedChunks++
			result.FreedBytes += int64(chunk.Size)
		}
	}

	result.ScanDuration = time.Since(start)
	return result, nil
}

// Start runs GC periodically.
func (gc *ChunkGC) Start(interval time.Duration) {
	if gc.running.Swap(true) {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				result, err := gc.Scan(context.Background())
				if err != nil {
					slog.Error("gc: scan error", "error", err)
				} else if result.OrphanChunks > 0 {
					slog.Info("gc: found orphans",
						"orphans", result.OrphanChunks,
						"deleted", result.DeletedChunks,
						"freed_bytes", result.FreedBytes,
						"duration", result.ScanDuration)
				}
			case <-gc.stopCh:
				return
			}
		}
	}()
}

// Stop terminates periodic GC.
func (gc *ChunkGC) Stop() {
	if gc.running.Swap(false) {
		close(gc.stopCh)
	}
}

// ============================================================
// Data Scrubber — Silent Corruption Detection
// ============================================================

// Scrubber periodically verifies chunk checksums against stored metadata.
type Scrubber struct {
	store   *PebbleStore
	events  *EventBus
	stopCh  chan struct{}
	running atomic.Bool
}

// NewScrubber creates a data scrubber.
func NewScrubber(store *PebbleStore, events *EventBus) *Scrubber {
	return &Scrubber{
		store:  store,
		events: events,
		stopCh: make(chan struct{}),
	}
}

// ScrubResult holds the result of a scrub pass.
type ScrubResult struct {
	ChunksScanned   int
	ChunksCorrupted int
	ChunksDegraded  int
	RepairTriggered int
	ScanDuration    time.Duration
}

// VerifyChunkChecksum checks if a chunk's reported checksum matches expectations.
// In production, this would read actual data from datanodes and compute CRC32C.
// Here we verify metadata consistency.
func (s *Scrubber) VerifyChunkChecksum(ctx context.Context, chunk *ChunkMeta) error {
	// Check: chunk has replicas
	if len(chunk.Replicas) == 0 {
		return fmt.Errorf("chunk %d has no replicas", chunk.ID)
	}

	// Check: at least one replica is healthy
	healthyCount := 0
	for _, r := range chunk.Replicas {
		if r.State == ReplicaReady || r.State == ReplicaSyncing {
			healthyCount++
		}
	}
	if healthyCount == 0 {
		return fmt.Errorf("chunk %d has no healthy replicas", chunk.ID)
	}

	// Check: sealed chunks must have a checksum
	if chunk.State == ChunkSealed || chunk.State == ChunkReady {
		if chunk.Checksum == 0 {
			return fmt.Errorf("chunk %d is sealed but has no checksum", chunk.ID)
		}
	}

	return nil
}

// Scan performs one full scrub pass.
func (s *Scrubber) Scan(ctx context.Context) (*ScrubResult, error) {
	start := time.Now()
	result := &ScrubResult{}

	err := s.store.ScanAllChunks(ctx, func(chunk *ChunkMeta) error {
		result.ChunksScanned++

		if err := s.VerifyChunkChecksum(ctx, chunk); err != nil {
			result.ChunksCorrupted++
			slog.Error("scrub: chunk corrupted", "chunk_id", chunk.ID, "error", err)

			// Trigger repair
			if s.events != nil {
				data, _ := json.Marshal(map[string]interface{}{
					"chunk_id": chunk.ID,
					"reason":   err.Error(),
				})
				s.events.Publish(Event{
					Type:  EventSet,
					Key:   fmt.Sprintf("/repair/%d", chunk.ID),
					Value: data,
				})
				result.RepairTriggered++
			}
		}

		// Check for degraded chunks
		if chunk.State == ChunkDegraded {
			result.ChunksDegraded++
		}

		return nil
	})

	result.ScanDuration = time.Since(start)
	return result, err
}

// Start runs the scrubber periodically.
func (s *Scrubber) Start(interval time.Duration) {
	if s.running.Swap(true) {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				result, err := s.Scan(context.Background())
				if err != nil {
					slog.Error("scrub: error", "error", err)
				} else {
					slog.Info("scrub: completed",
						"scanned", result.ChunksScanned,
						"corrupted", result.ChunksCorrupted,
						"degraded", result.ChunksDegraded,
						"repairs", result.RepairTriggered,
						"duration", result.ScanDuration)
				}
			case <-s.stopCh:
				return
			}
		}
	}()
}

// Stop terminates the scrubber.
func (s *Scrubber) Stop() {
	if s.running.Swap(false) {
		close(s.stopCh)
	}
}

// ============================================================
// CRC32C helper for data verification
// ============================================================

// CRC32C computes CRC32 Castagnoli checksum (same as used by iSCSI, ext4, Btrfs).
func CRC32C(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
}
