package smoke

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/gateway/s3"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func TestProductionSmokePutFailoverGet(t *testing.T) {
	if os.Getenv("NUFS_RUN_SMOKE") != "1" {
		t.Skip("set NUFS_RUN_SMOKE=1 to run local multi-node smoke test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cluster := startRaftSmokeCluster(t, 3)
	defer cluster.Stop()

	leader := cluster.WaitForLeader(t, ctx)
	for i := 1; i <= 3; i++ {
		if err := leader.Store.RegisterNode(ctx, &metadata.NodeInfo{
			ID:         metadata.NodeID(i),
			Addr:       fmt.Sprintf("node-%d:9100", i),
			Zone:       "zone-a",
			Rack:       fmt.Sprintf("rack-%d", i),
			MachineID:  fmt.Sprintf("machine-%d", i),
			Tier:       metadata.TierHot,
			CapacityGB: 100,
			State:      metadata.NodeOnline,
		}); err != nil {
			t.Fatalf("register datanode %d: %v", i, err)
		}
	}

	chunks := chunkstore.NewMemoryChunkStore()
	putGateway := httptest.NewServer(s3.NewGateway(s3.GatewayConfig{
		MetaService: leader.Store,
		ChunkStore:  chunks,
	}).Handler())
	defer putGateway.Close()

	doSmokeRequest(t, http.MethodPut, putGateway.URL+"/prod", nil, http.StatusOK)
	doSmokeRequest(t, http.MethodPut, putGateway.URL+"/prod/object.txt", bytes.NewReader([]byte("smoke payload")), http.StatusOK)

	cluster.WaitForObjectOnFollowers(t, ctx, leader.ID, "prod", "object.txt")
	cluster.StopNode(t, leader.ID)

	newLeader := cluster.WaitForLeader(t, ctx)
	getGateway := httptest.NewServer(s3.NewGateway(s3.GatewayConfig{
		MetaService: newLeader.Store,
		ChunkStore:  chunks,
	}).Handler())
	defer getGateway.Close()

	resp := doSmokeRequest(t, http.MethodGet, getGateway.URL+"/prod/object.txt", nil, http.StatusOK)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read get body: %v", err)
	}
	if string(body) != "smoke payload" {
		t.Fatalf("body = %q, want %q", string(body), "smoke payload")
	}
}

type raftSmokeCluster struct {
	Nodes []*raftSmokeNode
}

type raftSmokeNode struct {
	ID    string
	Store *metadata.PebbleStore
}

func startRaftSmokeCluster(t *testing.T, n int) *raftSmokeCluster {
	t.Helper()

	peers := make([]metadata.RaftPeer, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = reserveLocalTCPAddr(t)
		peers[i] = metadata.RaftPeer{
			ID:      fmt.Sprintf("node-%d", i+1),
			Address: addrs[i],
		}
	}

	cluster := &raftSmokeCluster{Nodes: make([]*raftSmokeNode, 0, n)}
	for i := 0; i < n; i++ {
		store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
			Dir:    t.TempDir(),
			NodeID: uint64(i + 1),
		})
		if err != nil {
			cluster.Stop()
			t.Fatalf("create store %d: %v", i+1, err)
		}
		_, err = metadata.NewRaftNode(store, metadata.RaftNodeConfig{
			NodeID:                peers[i].ID,
			BindAddr:              addrs[i],
			AdvertiseAddr:         addrs[i],
			RaftDir:               t.TempDir(),
			Bootstrap:             true,
			BootstrapPeers:        peers,
			PreSeedBootstrapPeers: true, // in-process: all peers start together, form quorum at boot
			HeartbeatTimeout:      100 * time.Millisecond,
			ElectionTimeout:       100 * time.Millisecond,
			LeaderLeaseTimeout:    50 * time.Millisecond,
			SnapshotThreshold:     64,
			SnapshotInterval:      time.Second,
			TrailingLogs:          128,
		})
		if err != nil {
			_ = store.Close()
			cluster.Stop()
			t.Fatalf("create raft node %d: %v", i+1, err)
		}
		cluster.Nodes = append(cluster.Nodes, &raftSmokeNode{ID: peers[i].ID, Store: store})
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

func (c *raftSmokeCluster) Stop() {
	for _, node := range c.Nodes {
		if node.Store != nil {
			_ = node.Store.Close()
			node.Store = nil
		}
	}
}

func (c *raftSmokeCluster) StopNode(t *testing.T, id string) {
	t.Helper()

	for _, node := range c.Nodes {
		if node.ID != id {
			continue
		}
		if node.Store == nil {
			return
		}
		if err := node.Store.Close(); err != nil && err != metadata.ErrServiceClosed {
			t.Fatalf("stop node %s: %v", id, err)
		}
		node.Store = nil
		return
	}
	t.Fatalf("node %s not found", id)
}

func (c *raftSmokeCluster) WaitForLeader(t *testing.T, ctx context.Context) *raftSmokeNode {
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

func (c *raftSmokeCluster) WaitForObjectOnFollowers(t *testing.T, ctx context.Context, leaderID string, bucketName string, key string) {
	t.Helper()

	for _, node := range c.Nodes {
		if node.Store == nil || node.ID == leaderID {
			continue
		}
		waitForObject(t, ctx, node.Store, bucketName, key)
	}
}

func waitForObject(t *testing.T, ctx context.Context, store *metadata.PebbleStore, bucketName string, key string) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		bucket, err := store.GetBucket(ctx, bucketName)
		if err == nil {
			if inode, err := store.Lookup(ctx, bucket.RootInode, key); err == nil && len(inode.ChunkMap) > 0 {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for object %s/%s: %v", bucketName, key, ctx.Err())
		case <-ticker.C:
		}
	}
}

func doSmokeRequest(t *testing.T, method string, url string, body io.Reader, wantStatus int) *http.Response {
	t.Helper()

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	if resp.StatusCode != wantStatus {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("%s %s status = %d, want %d, body=%s", method, url, resp.StatusCode, wantStatus, string(data))
	}
	return resp
}
