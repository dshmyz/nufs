package metadata

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ========== EventBus Tests ==========

func TestEventBus_PublishSubscribe(t *testing.T) {
	bus := NewEventBus(16)

	w1 := bus.Watch("/chunk/")
	w2 := bus.Watch("/inode/")
	defer w1.Close()
	defer w2.Close()

	bus.Publish(Event{Type: EventSet, Key: "/chunk/123", Value: []byte("data")})
	bus.Publish(Event{Type: EventSet, Key: "/inode/456", Value: []byte("meta")})
	bus.Publish(Event{Type: EventDelete, Key: "/chunk/789"})

	// w1 should receive 2 chunk events
	select {
	case e := <-w1.Events():
		if e.Key != "/chunk/123" {
			t.Fatalf("expected /chunk/123, got %s", e.Key)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
	select {
	case e := <-w1.Events():
		if e.Key != "/chunk/789" || e.Type != EventDelete {
			t.Fatalf("unexpected event: %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	// w2 should receive 1 inode event
	select {
	case e := <-w2.Events():
		if e.Key != "/inode/456" {
			t.Fatalf("expected /inode/456, got %s", e.Key)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestEventBus_WatcherCount(t *testing.T) {
	bus := NewEventBus(4)
	w1 := bus.Watch("/a/")
	w2 := bus.Watch("/b/")
	if bus.WatcherCount() != 2 {
		t.Fatalf("expected 2 watchers, got %d", bus.WatcherCount())
	}
	w1.Close()
	time.Sleep(10 * time.Millisecond)
	if bus.WatcherCount() != 1 {
		t.Fatalf("expected 1 watcher after close, got %d", bus.WatcherCount())
	}
	w2.Close()
}

// ========== MVCC Tests ==========

func TestMVCC_CASUpdate(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	store.CreateBucket(ctx, "fs", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "fs")
	meta, _ := store.CreateFile(ctx, bucket.RootInode, "test.txt", 0644)

	// Read with version
	inode, ver, err := store.GetInodeWithVersion(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetInodeWithVersion: %v", err)
	}

	// CAS update succeeds
	inode.Size = 2048
	err = store.CASUpdateInode(ctx, ver, inode)
	if err != nil {
		t.Fatalf("CAS update: %v", err)
	}

	// Read again — version should be incremented
	_, ver2, _ := store.GetInodeWithVersion(ctx, meta.ID)
	if ver2 <= ver {
		t.Fatalf("version not incremented: %d <= %d", ver2, ver)
	}

	// CAS with stale version should fail
	inode.Size = 4096
	err = store.CASUpdateInode(ctx, ver, inode) // old version!
	if err != ErrVersionConflict {
		t.Fatalf("expected ErrVersionConflict, got: %v", err)
	}
}

// ========== Lease Manager Tests ==========

func TestLeaseManager_ExpiresOfflineNodes(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	bus := NewEventBus(4)

	// Register a node with old LastSeen
	info := &NodeInfo{
		ID: 1, Addr: "node1:9001", CapacityGB: 100,
		State: NodeOnline, LastSeen: time.Now().Add(-60 * time.Second).UnixNano(),
	}
	store.putJSON(prefixNode+"1", info)

	// Start lease manager with 10s TTL
	lm := NewLeaseManager(store, bus, 10*time.Second)
	lm.Start()
	defer lm.Stop()

	// Wait for check cycle
	time.Sleep(5 * time.Second)

	// Node should be marked offline
	node, _ := store.GetNode(ctx, 1)
	if node.State != NodeOffline {
		t.Fatalf("expected NodeOffline, got %d", node.State)
	}
}

// ========== Chunk GC Tests ==========

func TestChunkGC_FindsOrphans(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Register nodes
	for i := 1; i <= 3; i++ {
		store.RegisterNode(ctx, &NodeInfo{
			ID: NodeID(i), Addr: fmt.Sprintf("node%d:9001", i),
			CapacityGB: 100, Rack: fmt.Sprintf("rack%d", i), Tier: TierHot,
		})
	}

	store.CreateBucket(ctx, "fs", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "fs")
	file, _ := store.CreateFile(ctx, bucket.RootInode, "data.bin", 0644)

	// Create 3 chunks, 1 referenced by file
	c1, _ := store.AllocateChunk(ctx, file.ID, 0, PlacementPolicy{ReplicationFactor: 3, TopologySpread: SpreadRack})
	store.AllocateChunk(ctx, file.ID, MaxChunkSize, PlacementPolicy{ReplicationFactor: 3, TopologySpread: SpreadRack})

	// Create orphan chunks (directly in Pebble, no inode reference)
	store.putJSON(fmt.Sprintf("%s%d", prefixChunk, ChunkID(99901)), &ChunkMeta{
		ID: ChunkID(99901), Size: 1000, State: ChunkReady,
		Replicas: []ReplicaInfo{{NodeID: 1, State: ReplicaReady}},
	})
	store.putJSON(fmt.Sprintf("%s%d", prefixChunk, ChunkID(99902)), &ChunkMeta{
		ID: ChunkID(99902), Size: 2000, State: ChunkReady,
		Replicas: []ReplicaInfo{{NodeID: 1, State: ReplicaReady}},
	})

	// Run GC
	gc := NewChunkGC(store, nil, nil, false)
	result, err := gc.Scan(ctx)
	if err != nil {
		t.Fatalf("GC scan: %v", err)
	}

	// Should find 2 orphans (99901, 99902)
	if result.OrphanChunks != 2 {
		t.Fatalf("expected 2 orphans, got %d (total: %d)", result.OrphanChunks, result.TotalChunks)
	}
	if result.TombstonesCreated != 2 || result.DeletedChunks != 0 || result.ChunksPurged != 0 {
		t.Fatalf("expected 2 tombstones and no physical purge, got %+v", result)
	}

	// Verify referenced chunk still exists
	_, err = store.GetChunk(ctx, c1.ID)
	if err != nil {
		t.Fatalf("referenced chunk should still exist: %v", err)
	}

	// Verify orphan metadata remains readable through quarantine.
	if _, err = store.GetChunk(ctx, ChunkID(99901)); err != nil {
		t.Fatalf("orphan must remain through quarantine: %v", err)
	}
	tombstones, err := store.ListChunkTombstones(ctx, 0)
	if err != nil || len(tombstones) != 2 {
		t.Fatalf("tombstones = (%v, %v), want two", tombstones, err)
	}
}

// ========== Scrubber Tests ==========

func TestScrubber_DetectsCorruption(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	bus := NewEventBus(4)

	// Create a sealed chunk with no replicas (corrupted)
	store.putJSON(fmt.Sprintf("%s%d", prefixChunk, ChunkID(88001)), &ChunkMeta{
		ID: ChunkID(88001), Size: 1000, State: ChunkReady,
		Replicas: []ReplicaInfo{}, // no replicas!
		Checksum: 0xDEAD,
	})

	// Create a healthy chunk
	store.putJSON(fmt.Sprintf("%s%d", prefixChunk, ChunkID(88002)), &ChunkMeta{
		ID: ChunkID(88002), Size: 1000, State: ChunkReady,
		Replicas: []ReplicaInfo{{NodeID: 1, State: ReplicaReady}},
		Checksum: 0xBEEF,
	})

	scrubber := NewScrubber(store, bus)
	result, err := scrubber.Scan(ctx)
	if err != nil {
		t.Fatalf("scrub scan: %v", err)
	}

	if result.ChunksScanned != 2 {
		t.Fatalf("expected 2 scanned, got %d", result.ChunksScanned)
	}
	if result.ChunksCorrupted != 1 {
		t.Fatalf("expected 1 corrupted, got %d", result.ChunksCorrupted)
	}
}

// ========== Health Check Tests ==========

func TestHealthCheck_HTTP(t *testing.T) {
	store := newTestPebbleStore(t)
	metrics := NewMetrics()
	hc := NewHealthChecker(store, nil, metrics, "1.0.0-test")

	// Record some metrics
	metrics.RecordRead(100 * time.Microsecond)
	metrics.RecordWrite(200 * time.Microsecond)

	handler := hc.HTTPHandler()

	// /health
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("/health status: %d", w.Code)
	}

	// /ready
	req = httptest.NewRequest("GET", "/ready", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("/ready status: %d", w.Code)
	}

	// /metrics
	req = httptest.NewRequest("GET", "/metrics", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("/metrics status: %d", w.Code)
	}
}

// ========== Metrics Tests ==========

func TestMetrics_Latency(t *testing.T) {
	m := NewMetrics()
	for i := 0; i < 100; i++ {
		m.RecordRead(time.Duration(i+1) * time.Millisecond)
		m.RecordWrite(time.Duration(i+1) * time.Millisecond)
	}

	snap := m.Snapshot()
	if snap.ReadOps != 100 || snap.WriteOps != 100 {
		t.Fatalf("ops mismatch: read=%d write=%d", snap.ReadOps, snap.WriteOps)
	}
	if snap.ReadP50us == 0 || snap.ReadP99us == 0 {
		t.Fatalf("latency should be non-zero: p50=%d p99=%d", snap.ReadP50us, snap.ReadP99us)
	}
	if snap.OpsTotal != 200 {
		t.Fatalf("expected 200 total ops, got %d", snap.OpsTotal)
	}
}

// ========== ServiceBundle Tests ==========

func TestServiceBundle_Interface(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Verify PebbleStore implements MetadataService
	var svc MetadataService = store
	err := svc.CreateBucket(ctx, "test", PlacementPolicy{ID: "p", ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("CreateBucket via interface: %v", err)
	}

	buckets, err := svc.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets via interface: %v", err)
	}
	if len(buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(buckets))
	}
}

func TestServiceBundle_StartsAutoBalancerWhenConfigured(t *testing.T) {
	store := newTestPebbleStore(t)
	bundle, err := NewPebbleServiceBundle(
		store,
		WithLeaseTTL(0),
		WithGCInterval(0),
		WithScrubInterval(0),
		WithAutoBalanceInterval(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewPebbleServiceBundle: %v", err)
	}
	if bundle.AutoBalancer == nil {
		t.Fatal("expected AutoBalancer to be configured")
	}
	if !bundle.AutoBalancer.running.Load() {
		t.Fatal("expected AutoBalancer to be running")
	}
	if err := bundle.Close(); err != nil {
		t.Fatalf("bundle close: %v", err)
	}
	if bundle.AutoBalancer.running.Load() {
		t.Fatal("expected AutoBalancer to stop during bundle close")
	}
}

func TestCRC32C(t *testing.T) {
	data := []byte("hello world")
	crc := CRC32C(data)
	if crc == 0 {
		t.Fatal("CRC32C should not be zero")
	}
	// Same data → same CRC
	if CRC32C(data) != crc {
		t.Fatal("CRC32C not deterministic")
	}
	// Different data → different CRC
	if CRC32C([]byte("goodbye")) == crc {
		t.Fatal("CRC32C collision")
	}
}
