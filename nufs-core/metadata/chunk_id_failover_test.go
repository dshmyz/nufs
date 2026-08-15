package metadata

import (
	"context"
	"testing"
	"time"
)

// TestChunkIDNoReuseAfterStoreReopen is the decisive regression test for the
// chunk-ID-reuse durability bug (multi-metad leader-failover drill): chunk IDs
// were minted by a per-process, in-memory snowflake generator with no memory of
// IDs already committed to the store. After a process restart that reuses the
// same node ID, the fresh generator could re-issue an ID the earlier process
// had already committed — and since the datanode keys chunks by that 64-bit ID,
// reuse silently overwrote another object's bytes (byte-exact durability break).
//
// The fix bumps the generator strictly above the largest chunk ID already
// committed to the store before minting (cold-cache scan on restart). This
// non-raft test reproduces the exact mechanism: allocate under a node, close and
// reopen the store (fresh generator on the same node ID), allocate again, and
// assert zero reuse with strict monotonicity.
func TestChunkIDNoReuseAfterStoreReopen(t *testing.T) {
	dir := t.TempDir()
	fresh := func() *PebbleStore {
		st, err := NewPebbleStore(PebbleStoreConfig{Dir: dir, NodeID: 1})
		if err != nil {
			t.Fatalf("store: %v", err)
		}
		return st
	}

	ctx := context.Background()
	setupStore := fresh()
	if err := setupStore.CreateBucket(ctx, "fs", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		setupStore.Close()
		t.Fatalf("create bucket: %v", err)
	}
	bucket, err := setupStore.GetBucket(ctx, "fs")
	if err != nil {
		setupStore.Close()
		t.Fatalf("get bucket: %v", err)
	}
	if _, err := setupStore.CreateFile(ctx, bucket.RootInode, "f.bin", 0o644); err != nil {
		setupStore.Close()
		t.Fatalf("create file: %v", err)
	}
	if err := setupStore.RegisterNode(ctx, &NodeInfo{ID: 1, Zone: "z", CapacityGB: 1000, State: NodeOnline}); err != nil {
		setupStore.Close()
		t.Fatalf("register node: %v", err)
	}
	if err := setupStore.Close(); err != nil {
		t.Fatalf("close setup store: %v", err)
	}

	policy := PlacementPolicy{ReplicationFactor: 1}
	allocate := func(st *PebbleStore, fileID InodeID, n int) []ChunkID {
		t.Helper()
		offsets := make([]int64, n)
		for i := range offsets {
			offsets[i] = int64(i) * MaxChunkSize
		}
		chunks, err := st.AllocateChunksBatch(ctx, fileID, offsets, policy)
		if err != nil {
			t.Fatalf("AllocateChunksBatch: %v", err)
		}
		ids := make([]ChunkID, len(chunks))
		for i, c := range chunks {
			ids[i] = c.ID
		}
		return ids
	}

	// First life: allocate chunks and commit them durably to Pebble, then also
	// seed a committed chunk whose ID sits in the FUTURE relative to wall clock.
	// This models the real hazard deterministically: without the fix, a freshly
	// restarted generator mints at ~now and can land at or below a committed
	// chunk ID (which, when millis/node/sequence align, is an exact collision
	// that overwrites another object's bytes). With the fix, the restarted
	// generator scans committed keys and bumps strictly above the future floor.
	firstStore := fresh()
	b2, err := firstStore.GetBucket(ctx, "fs")
	if err != nil {
		firstStore.Close()
		t.Fatalf("GetBucket: %v", err)
	}
	file, err := firstStore.Lookup(ctx, b2.RootInode, "f.bin")
	if err != nil {
		firstStore.Close()
		t.Fatalf("Lookup: %v", err)
	}
	first := allocate(firstStore, file.ID, 2)

	// Future floor: a committed chunk ID whose 41-bit millisecond field lies
	// 60s ahead of now, on this same node (node=1, seq=0) — the exact tuple a
	// naive fresh generator would re-issue the moment the wall clock catches up.
	futureMS := uint64(time.Now().UnixMilli()+60000) & 0x1FFFFFFFFFF
	floorID := ChunkID(futureMS<<23 | (uint64(1)&0x3FF)<<13)
	if err := firstStore.putMsgpack(chunkMetadataKey(floorID), &ChunkMeta{ID: floorID, Size: 1}); err != nil {
		firstStore.Close()
		t.Fatalf("seed future floor: %v", err)
	}
	committedMax := floorID
	for _, id := range first {
		if id > committedMax {
			committedMax = id
		}
	}
	if err := firstStore.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	// Restart: fresh process, same node ID, fresh generator + cold cache.
	spotChunks := func(n int) []ChunkID {
		t.Helper()
		st := fresh()
		bx, err := st.GetBucket(ctx, "fs")
		if err != nil {
			st.Close()
			t.Fatalf("GetBucket after reopen: %v", err)
		}
		f2, err := st.Lookup(ctx, bx.RootInode, "f.bin")
		if err != nil {
			st.Close()
			t.Fatalf("Lookup after reopen: %v", err)
		}
		ids := allocate(st, f2.ID, n)
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
		return ids
	}

	second := spotChunks(6)
	for _, id := range second {
		if id == floorID {
			t.Fatalf("chunk ID %d reused after store reopen (same node): the datanode would overwrite another object's bytes", id)
		}
		if id <= committedMax {
			t.Fatalf("post-reopen chunk ID %d is not strictly greater than the committed max %d", id, committedMax)
		}
	}
	t.Logf("OK: %d first-life + seeded floor %d -> %d restarted-life chunk IDs, strictly monotonic", len(first), floorID, second[len(second)-1])
}

// TestRaftClusterChunkIDNoReuseAcrossFailover is the distributed counterpart of
// TestChunkIDNoReuseAfterStoreReopen: after a raft leader fails over, the new
// leader's allocations must never reuse a previous leader's committed chunk ID
// and must be strictly monotonic (the cross-leader extension of the same fix).
func TestRaftClusterChunkIDNoReuseAcrossFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("real raft integration test")
	}

	cluster := startRealRaftTestCluster(t, 3)
	defer cluster.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Register datanode candidates through the leader so placement has nodes to
	// pick once either leader allocates (registration replicates via raft, so a
	// failover keeps them visible).
	leader := cluster.CreateBucketOnLeader(t, ctx, "fs", PlacementPolicy{ReplicationFactor: 1})
	for i := NodeID(1); i <= 3; i++ {
		if err := leader.Store.RegisterNode(ctx, &NodeInfo{
			ID:         i,
			Zone:       "z",
			CapacityGB: 1000,
			State:      NodeOnline,
		}); err != nil {
			t.Fatalf("register node %d: %v", i, err)
		}
	}

	bucket, err := leader.Store.GetBucket(ctx, "fs")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	file, err := leader.Store.CreateFile(ctx, bucket.RootInode, "f.bin", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	fileID := file.ID

	policy := PlacementPolicy{ReplicationFactor: 1}

	// Allocate n chunks on the given live store; returns the minted IDs.
	spotChunks := func(st *PebbleStore, n int) []ChunkID {
		t.Helper()
		offsets := make([]int64, n)
		for i := range offsets {
			offsets[i] = int64(i) * MaxChunkSize
		}
		chunks, err := st.AllocateChunksBatch(ctx, fileID, offsets, policy)
		if err != nil {
			t.Fatalf("AllocateChunksBatch: %v", err)
		}
		ids := make([]ChunkID, len(chunks))
		for i, c := range chunks {
			ids[i] = c.ID
		}
		return ids
	}

	// Phase 1: allocate a batch of chunks under the initial leader (A).
	preFailover := spotChunks(leader.Store, 8)
	leaderID := leader.ID
	t.Logf("allocated %d chunk IDs under leader %s", len(preFailover), leaderID)

	// Phase 2: kill the current leader and fail over to a new one (B).
	cluster.StopNode(t, leaderID)
	failoverCtx, cancelFO := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelFO()
	newLeader := cluster.WaitForLeader(t, failoverCtx)
	if newLeader.ID == leaderID {
		t.Fatal("expected a new leader after failover")
	}
	t.Logf("failover: leader %s -> %s", leaderID, newLeader.ID)

	// Phase 3: allocate more chunks under the new leader and assert strict,
	// collision-free monotonicity across the leadership boundary.
	postFailover := spotChunks(newLeader.Store, 6)
	for _, id := range postFailover {
		for _, prev := range preFailover {
			if id == prev {
				t.Fatalf("chunk ID %d reused across leader failover (%s -> %s): the datanode would overwrite another object's bytes", id, leaderID, newLeader.ID)
			}
			if id <= prev {
				t.Fatalf("post-failover chunk ID %d is not strictly greater than pre-failover ID %d", id, prev)
			}
		}
	}

	t.Logf("OK: %d pre-failover + %d post-failover chunk IDs, zero reuse, strictly monotonic", len(preFailover), len(postFailover))

	// The in-memory high mark must now reflect the most recent allocation on
	// the surviving leaders (proving committed state drives the floor, not just
	// local process memory).
	hwm := uint64(0)
	for _, n := range cluster.Nodes {
		if n.Store == nil {
			continue
		}
		if v := n.Store.ensureChunkIDMax(); v > hwm {
			hwm = v
		}
	}
	if hwm == 0 {
		t.Fatal("chunk-ID high mark never advanced across the raft cluster")
	}
	for _, prev := range preFailover {
		if ChunkID(hwm) <= prev {
			t.Fatalf("high mark %d is not above pre-failover ID %d", hwm, prev)
		}
	}
	t.Logf("chunk-ID high mark across raft cluster = %d", hwm)
}
