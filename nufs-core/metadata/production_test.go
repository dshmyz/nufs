package metadata

import (
	"context"
	"fmt"
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

// TestLeaseManager_PreservesOperatorStates proves that lease expiry never
// clobbers an operator-set state with NodeOffline. Decommission, maintenance,
// and failed are sticky human actions: if the lease manager overwrote a
// draining node with NodeOffline, a later heartbeat (HeartbeatLiveness, which
// promotes only offline → online) would silently resurrect a node the operator
// deliberately took out of service. This is the regression that made
// "decommission then restore" fail on a live cluster whose datanode heartbeat
// lapsed momentarily.
func TestLeaseManager_PreservesOperatorStates(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		prep func(store *PebbleStore, id NodeID) error
		want NodeState
	}{
		{
			// Decommissioned node with no replicas left advances to the terminal
			// NodeDecommissioned state via the lease-sweep drain-completion check
			// (see TestLeaseManager_DrainingTerminalState). This is still sticky —
			// the sweep never demotes it to NodeOffline, which is the property
			// under test here.
			name: "decommissioned",
			prep: func(store *PebbleStore, id NodeID) error { return store.DecommissionNode(ctx, id) },
			want: NodeDecommissioned,
		},
		{
			name: "maintenance",
			prep: func(store *PebbleStore, id NodeID) error { return store.EnterMaintenance(ctx, id) },
			want: NodeMaint,
		},
		{
			name: "failed",
			prep: func(store *PebbleStore, id NodeID) error {
				// No public "mark failed" surface; a failed state arises from the
				// disaster drill path, so write it directly like the lease test.
				var cur NodeInfo
				key := prefixNode + fmt.Sprintf("%d", id)
				if _, err := store.getJSON(key, &cur); err != nil {
					return err
				}
				cur.State = NodeFailed
				return store.putJSON(key, &cur)
			},
			want: NodeFailed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestPebbleStore(t)
			bus := NewEventBus(4)

			const id = NodeID(77)
			if err := store.RegisterNode(ctx, &NodeInfo{
				ID: id, Addr: "n77:9001", CapacityGB: 100,
				State: NodeOnline, LastSeen: time.Now().UnixNano(),
			}); err != nil {
				t.Fatalf("RegisterNode: %v", err)
			}

			// 1. Lease manager with a short TTL, started first.
			lm := NewLeaseManager(store, bus, 2*time.Second)
			lm.Start()
			defer lm.Stop()

			// 2. The node heartbeats normally — this keeps it in the lease
			//    expiry heap with a fresh LastSeen (mirrors live: a draining node
			//    still heartbeats while its data drains away).
			hb := func() {
				if err := store.HeartbeatLiveness(ctx, id, nil); err != nil {
					t.Fatalf("HeartbeatLiveness: %v", err)
				}
			}
			hb()
			hb()

			// 3. Operator action mid-run, then the heartbeat stops so LastSeen
			//    goes stale and the next expiry sweep would try to mark it
			//    offline — the exact condition that used to clobber NodeDraining.
			if err := tc.prep(store, id); err != nil {
				t.Fatalf("prep: %v", err)
			}

			time.Sleep(2500 * time.Millisecond)

			n, err := store.GetNode(ctx, id)
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			if n.State != tc.want {
				t.Fatalf("state after lease expiry = %d, want %d (operator state must survive)", n.State, tc.want)
			}
		})
	}
}

// TestLeaseManager_DrainingTerminalState locks the "draining terminal state"
// semantics the operator chose: a decommissioned (draining) node advances to
// the terminal NodeDecommissioned state automatically once it holds zero
// replicas, but stays draining while it still hosts data. Combined with
// PreservesOperatorStates, this makes decommission a true lifecycle:
//
//	decommission → draining → (data fully migrated away) → decommissioned
//
// The transition is driven here in the lease sweep because the drain worker
// (rebalance_exec) migrates chunks independently of heartbeats, and only the
// metadata authority can observe "last replica left". It is still sticky (the
// sweep never marks a draining node NodeOffline), so a heartbeat lapse can not
// resurrect a node mid-drain, and only RestoreNode brings a decommissioned
// node back to service.
func TestLeaseManager_DrainingTerminalState(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(4)

	// Two nodes. Both hold operator-set states whose records we will advance.
	// Node 1 additionally hosts a live replica (so it must STAY draining); node
	// 2 is empty (so it must reach the terminal decommissioned state).
	store := newTestPebbleStore(t)
	for _, id := range []NodeID{1, 2} {
		if err := store.RegisterNode(ctx, &NodeInfo{
			ID: id, Addr: fmt.Sprintf("n%d:9001", id),
			CapacityGB: 100, State: NodeOnline, LastSeen: time.Now().UnixNano(),
		}); err != nil {
			t.Fatalf("RegisterNode %d: %v", id, err)
		}
	}
	// Node 1 hosts one live replica.
	n1 := ChunkID(1001)
	if err := store.putJSON(prefixChunk+fmt.Sprint(n1), &ChunkMeta{
		ID: n1, Size: 64, State: ChunkReady,
		Replicas: []ReplicaInfo{{NodeID: 1, Addr: "n1:9001", State: ReplicaReady}},
	}); err != nil {
		t.Fatalf("seed chunk on node 1: %v", err)
	}

	lm := NewLeaseManager(store, bus, 2*time.Second)
	lm.Start()
	defer lm.Stop()

	// Decommission both nodes, then stop heartbeats so LastSeen goes stale and
	// the next expiry sweep runs the drain-completion check.
	for _, id := range []NodeID{1, 2} {
		if err := store.DecommissionNode(ctx, id); err != nil {
			t.Fatalf("DecommissionNode %d: %v", id, err)
		}
	}

	time.Sleep(2500 * time.Millisecond)

	// Node 1 still hosts a replica → must remain draining (not decommissioned).
	n1n, err := store.GetNode(ctx, 1)
	if err != nil {
		t.Fatalf("GetNode 1: %v", err)
	}
	if n1n.State != NodeDraining {
		t.Fatalf("node 1 (still hosting a replica) state = %d, want NodeDraining", n1n.State)
	}

	// Node 2 holds no replicas → must advance to the terminal NodeDecommissioned.
	n2n, err := store.GetNode(ctx, 2)
	if err != nil {
		t.Fatalf("GetNode 2: %v", err)
	}
	if n2n.State != NodeDecommissioned {
		t.Fatalf("node 2 (fully drained) state = %d, want NodeDecommissioned", n2n.State)
	}

	// Restore brings a decommissioned node back to service.
	if err := store.RestoreNode(ctx, 2); err != nil {
		t.Fatalf("RestoreNode 2: %v", err)
	}
	n2n, _ = store.GetNode(ctx, 2)
	if n2n.State != NodeOnline {
		t.Fatalf("node 2 after restore = %d, want NodeOnline", n2n.State)
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
