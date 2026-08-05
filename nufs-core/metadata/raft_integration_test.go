package metadata

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func TestRaftClusterLeaderFailoverPreservesCommittedBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("real raft integration test")
	}

	cluster := startRealRaftTestCluster(t, 3)
	defer cluster.Stop()

	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelSetup()
	leader := cluster.CreateBucketOnLeader(t, setupCtx, "prod-check", PlacementPolicy{ReplicationFactor: 2})
	cluster.WaitForBucketOnFollowers(t, setupCtx, leader.ID, "prod-check")

	cluster.StopNode(t, leader.ID)

	failoverCtx, cancelFailover := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelFailover()
	newLeader := cluster.WaitForLeader(t, failoverCtx)
	bucket := waitForBucket(t, failoverCtx, newLeader.Store, "prod-check")
	if bucket.Name != "prod-check" {
		t.Fatalf("bucket name = %q", bucket.Name)
	}
}

// TestRaftClusterConcurrentSameNameMkDir verifies the distributed (Raft)
// path of atomic namespace creation: concurrent same-name MkDir through the
// leader must yield exactly one winner and ErrEntryExists for every loser,
// with no orphan inode or parent-NLink drift. This is the multi-node
// counterpart of TestPebbleStore_ConcurrentSameNameMkDir and is the real
// hazard behind "concurrent directory creation in a distributed scenario".
func TestRaftClusterConcurrentSameNameMkDir(t *testing.T) {
	if testing.Short() {
		t.Skip("real raft integration test")
	}

	cluster := startRealRaftTestCluster(t, 3)
	defer cluster.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_ = cluster.CreateBucketOnLeader(t, ctx, "fs", PlacementPolicy{ReplicationFactor: 2})
	leader := cluster.WaitForLeader(t, ctx)
	bucket, err := leader.Store.GetBucket(ctx, "fs")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	parent := bucket.RootInode

	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = leader.Store.MkDir(ctx, parent, "race-dir", 0755)
		}(i)
	}
	wg.Wait()

	var succeeded, conflicts int
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case err == ErrEntryExists:
			conflicts++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("expected exactly 1 successful MkDir, got %d (conflicts=%d)", succeeded, conflicts)
	}
	if conflicts != n-1 {
		t.Fatalf("expected %d ErrEntryExists, got %d", n-1, conflicts)
	}

	entries, err := leader.Store.ReadDir(ctx, parent, 0, 100)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	parentMeta, err := leader.Store.GetInode(ctx, parent)
	if err != nil {
		t.Fatalf("GetInode(parent): %v", err)
	}
	if parentMeta.NLink != 3 {
		t.Fatalf("parent NLink = %d, want 3 (no lost/double update)", parentMeta.NLink)
	}
}

type realRaftTestCluster struct {
	Nodes []*realRaftTestNode
}

type realRaftTestNode struct {
	ID    string
	Store *PebbleStore
}

func startRealRaftTestCluster(t *testing.T, n int) *realRaftTestCluster {
	t.Helper()

	peers := make([]RaftPeer, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = reserveLocalTCPAddr(t)
		peers[i] = RaftPeer{
			ID:      fmt.Sprintf("node-%d", i+1),
			Address: addrs[i],
		}
	}

	cluster := &realRaftTestCluster{
		Nodes: make([]*realRaftTestNode, 0, n),
	}
	for i := 0; i < n; i++ {
		store, err := NewPebbleStore(PebbleStoreConfig{
			Dir:    t.TempDir(),
			NodeID: uint64(i + 1),
		})
		if err != nil {
			cluster.Stop()
			t.Fatalf("create pebble store %d: %v", i+1, err)
		}

		_, err = NewRaftNode(store, RaftNodeConfig{
			NodeID:             peers[i].ID,
			BindAddr:           addrs[i],
			AdvertiseAddr:      addrs[i],
			RaftDir:            t.TempDir(),
			Bootstrap:          true,
			BootstrapPeers:     peers,
			HeartbeatTimeout:   100 * time.Millisecond,
			ElectionTimeout:    100 * time.Millisecond,
			LeaderLeaseTimeout: 50 * time.Millisecond,
			SnapshotThreshold:  64,
			SnapshotInterval:   time.Second,
			TrailingLogs:       128,
		})
		if err != nil {
			_ = store.Close()
			cluster.Stop()
			t.Fatalf("create raft node %d: %v", i+1, err)
		}

		cluster.Nodes = append(cluster.Nodes, &realRaftTestNode{
			ID:    peers[i].ID,
			Store: store,
		})
	}
	return cluster
}

func reserveLocalTCPAddr(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve tcp addr: %v", err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func (c *realRaftTestCluster) CreateBucketOnLeader(t *testing.T, ctx context.Context, name string, policy PlacementPolicy) *realRaftTestNode {
	t.Helper()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		leader := c.WaitForLeader(t, ctx)
		err := leader.Store.CreateBucket(ctx, name, policy)
		if err == nil || err == ErrBucketExists {
			return leader
		}
		select {
		case <-ctx.Done():
			t.Fatalf("create bucket on leader: %v", err)
		case <-ticker.C:
		}
	}
}

func (c *realRaftTestCluster) Stop() {
	for _, node := range c.Nodes {
		if node.Store != nil {
			_ = node.Store.Close()
			node.Store = nil
		}
	}
}

func (c *realRaftTestCluster) StopNode(t *testing.T, id string) {
	t.Helper()

	for _, node := range c.Nodes {
		if node.ID == id {
			if node.Store == nil {
				return
			}
			if err := node.Store.Close(); err != nil && err != ErrServiceClosed {
				t.Fatalf("stop node %s: %v", id, err)
			}
			node.Store = nil
			return
		}
	}
	t.Fatalf("node %s not found", id)
}

func (c *realRaftTestCluster) WaitForLeader(t *testing.T, ctx context.Context) *realRaftTestNode {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		for _, node := range c.Nodes {
			if node.Store != nil && node.Store.IsLeader() {
				return node
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("wait for leader: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (c *realRaftTestCluster) WaitForBucketOnFollowers(t *testing.T, ctx context.Context, leaderID string, name string) {
	t.Helper()

	for _, node := range c.Nodes {
		if node.Store == nil || node.ID == leaderID {
			continue
		}
		waitForBucket(t, ctx, node.Store, name)
	}
}

func waitForBucket(t *testing.T, ctx context.Context, store *PebbleStore, name string) *BucketInfo {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		bucket, err := store.GetBucket(ctx, name)
		if err == nil {
			return bucket
		}

		select {
		case <-ctx.Done():
			t.Fatalf("wait for bucket %q: %v", name, ctx.Err())
		case <-ticker.C:
		}
	}
}
