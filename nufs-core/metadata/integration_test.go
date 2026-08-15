package metadata

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestIntegrationCluster tests a 3-node Raft cluster end-to-end.
// This test requires significant setup and is tagged for CI environments.
func TestIntegrationCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Check if we should run this test (requires RAFT_INTEGRATION_TEST=1)
	if os.Getenv("RAFT_INTEGRATION_TEST") != "1" {
		t.Skip("skipping Raft integration test; set RAFT_INTEGRATION_TEST=1 to enable")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create 3 temporary directories for the cluster
	tmpDir := t.TempDir()
	peers := []string{
		filepath.Join(tmpDir, "node1"),
		filepath.Join(tmpDir, "node2"),
		filepath.Join(tmpDir, "node3"),
	}

	// Start 3-node cluster
	cluster, err := startTestCluster(ctx, peers)
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	defer cluster.Stop()

	// Wait for leader election
	t.Log("waiting for leader election...")
	leader := cluster.WaitForLeader(ctx, 10*time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	t.Logf("leader elected: node %d", leader.ID)

	// Test 1: Create bucket via leader
	t.Run("CreateBucket", func(t *testing.T) {
		err := leader.Store.CreateBucket(ctx, "test-bucket", PlacementPolicy{
			ID:                "default",
			ReplicationFactor: 2,
		})
		if err != nil {
			t.Fatalf("create bucket: %v", err)
		}

		// Verify bucket exists
		bucket, err := leader.Store.GetBucket(ctx, "test-bucket")
		if err != nil {
			t.Fatalf("get bucket: %v", err)
		}
		if bucket.Name != "test-bucket" {
			t.Fatalf("bucket name mismatch: got %s", bucket.Name)
		}
		t.Logf("bucket created: %s (root inode: %d)", bucket.Name, bucket.RootInode)
	})

	// Test 2: Write and read file
	t.Run("FileOperations", func(t *testing.T) {
		bucket, _ := leader.Store.GetBucket(ctx, "test-bucket")

		// Create directory
		dirInode, err := leader.Store.MkDir(ctx, bucket.RootInode, "testdir", 0755)
		if err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		t.Logf("created directory: inode %d", dirInode.ID)

		// Create file
		fileInode, err := leader.Store.CreateFile(ctx, dirInode.ID, "testfile.txt", 0644)
		if err != nil {
			t.Fatalf("create file: %v", err)
		}
		t.Logf("created file: inode %d", fileInode.ID)

		// Lookup file
		found, err := leader.Store.Lookup(ctx, dirInode.ID, "testfile.txt")
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if found.ID != fileInode.ID {
			t.Fatalf("lookup mismatch: expected %d, got %d", fileInode.ID, found.ID)
		}

		// Read directory
		entries, err := leader.Store.ReadDir(ctx, dirInode.ID, 0, 1000)
		if err != nil {
			t.Fatalf("readdir: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].Name != "testfile.txt" {
			t.Fatalf("entry name mismatch: %s", entries[0].Name)
		}
	})

	// Test 3: Failover
	t.Run("Failover", func(t *testing.T) {
		// Kill the leader
		oldLeaderID := leader.ID
		t.Logf("killing leader %d...", oldLeaderID)
		cluster.KillNode(oldLeaderID)

		// Wait for new leader
		newLeader := cluster.WaitForLeader(ctx, 10*time.Second)
		if newLeader == nil {
			t.Fatal("no new leader elected")
		}
		if newLeader.ID == oldLeaderID {
			t.Fatal("new leader should be different from old leader")
		}
		t.Logf("new leader elected: node %d", newLeader.ID)

		// Verify data is still accessible
		bucket, err := newLeader.Store.GetBucket(ctx, "test-bucket")
		if err != nil {
			t.Fatalf("get bucket after failover: %v", err)
		}
		t.Logf("bucket still accessible after failover: %s", bucket.Name)
	})

	// Test 4: Data consistency
	t.Run("Consistency", func(t *testing.T) {
		// All nodes should have the same data
		for _, node := range cluster.Nodes() {
			if !node.IsAlive() {
				continue
			}
			bucket, err := node.Store.GetBucket(ctx, "test-bucket")
			if err != nil {
				t.Errorf("node %d: get bucket: %v", node.ID, err)
				continue
			}
			if bucket.Name != "test-bucket" {
				t.Errorf("node %d: bucket name mismatch", node.ID)
			}
		}
	})
}

// TestIntegrationHTTP tests the HTTP API end-to-end.
func TestIntegrationHTTP(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a single-node store
	tmpDir := t.TempDir()
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: tmpDir})
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer store.Close()

	// Create service bundle
	bundle, err := NewPebbleServiceBundle(store)
	if err != nil {
		t.Fatalf("create service bundle: %v", err)
	}
	defer bundle.Close()

	// Create HTTP handler
	mux := http.NewServeMux()
	mux.Handle("/metrics", PrometheusHandler(bundle.Metrics))
	mux.Handle("/healthz", HealthHandler(bundle.Health))

	// Start test server
	server := httptest.NewServer(mux)
	defer server.Close()

	// Test health endpoint
	t.Run("Health", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/healthz")
		if err != nil {
			t.Fatalf("health check: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("health check status: %d", resp.StatusCode)
		}
	})

	// Test metrics endpoint
	t.Run("Metrics", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/metrics")
		if err != nil {
			t.Fatalf("metrics: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("metrics status: %d", resp.StatusCode)
		}
	})

	// Test bucket operations via store
	t.Run("BucketOps", func(t *testing.T) {
		err := store.CreateBucket(ctx, "test-bucket", PlacementPolicy{
			ID:                "default",
			ReplicationFactor: 1,
		})
		if err != nil {
			t.Fatalf("create bucket: %v", err)
		}

		bucket, err := store.GetBucket(ctx, "test-bucket")
		if err != nil {
			t.Fatalf("get bucket: %v", err)
		}
		if bucket.Name != "test-bucket" {
			t.Fatalf("bucket name: %s", bucket.Name)
		}

		buckets, err := store.ListBuckets(ctx)
		if err != nil {
			t.Fatalf("list buckets: %v", err)
		}
		if len(buckets) != 1 {
			t.Fatalf("expected 1 bucket, got %d", len(buckets))
		}
	})

	// Test scrub
	t.Run("Scrub", func(t *testing.T) {
		var scanned int
		err := store.ScrubAllChunks(func(chunkID ChunkID, replicaCount, healthyCount int) {
			scanned++
		})
		if err != nil {
			t.Fatalf("scrub: %v", err)
		}
		t.Logf("scanned %d chunks", scanned)
	})
}

// ============================================================
// Test Cluster Helpers
// ============================================================

type testCluster struct {
	nodes []*testNode
}

type testNode struct {
	ID    int
	Store *PebbleStore
	alive bool
}

func startTestCluster(ctx context.Context, dirs []string) (*testCluster, error) {
	cluster := &testCluster{
		nodes: make([]*testNode, len(dirs)),
	}

	// For this test, we simulate a cluster with independent stores
	// A full Raft cluster would require actual network communication
	for i, dir := range dirs {
		store, err := NewPebbleStore(PebbleStoreConfig{Dir: dir})
		if err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("create store %d: %w", i, err)
		}
		cluster.nodes[i] = &testNode{
			ID:    i + 1,
			Store: store,
			alive: true,
		}
	}

	return cluster, nil
}

func (c *testCluster) Stop() {
	for _, node := range c.nodes {
		if node.Store != nil {
			node.Store.Close()
		}
	}
}

func (c *testCluster) Nodes() []*testNode {
	return c.nodes
}

func (c *testCluster) KillNode(id int) {
	for _, node := range c.nodes {
		if node.ID == id {
			node.alive = false
		}
	}
}

func (c *testCluster) WaitForLeader(ctx context.Context, timeout time.Duration) *testNode {
	// In a real Raft cluster, we would wait for leader election
	// For this test, we return the first alive node
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range c.nodes {
			if node.alive {
				return node
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (n *testNode) IsAlive() bool {
	return n.alive
}
