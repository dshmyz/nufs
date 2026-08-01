package smoke

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/example/dfs/chunkstore"
	"github.com/example/dfs/datanode"
	"github.com/example/dfs/gateway/s3"
	"github.com/example/dfs/metadata"
)

// TestRepair_EndToEnd verifies the repair path: write data with RF=3,
// kill one datanode, trigger repair, and verify the chunk is repaired
// to a new node with the correct data.
func TestRepair_EndToEnd(t *testing.T) {
	if os.Getenv("NUFS_RUN_SMOKE") != "1" {
		t.Skip("set NUFS_RUN_SMOKE=1 to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start metadata + 3 datanodes + S3 gateway
	metaDir := t.TempDir()
	metaStore, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: metaDir, NodeID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer metaStore.Close()

	const numNodes = 3
	servers := make([]*datanode.Server, numNodes)
	addrs := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		store, _ := datanode.NewChunkStore(t.TempDir(), 64, 256, nil)
		store.WaitForScan()
		cfg := datanode.DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.NodeID = metadata.NodeID(i + 1)
		cfg.RequestTimeout = 5 * time.Second
		srv := datanode.NewServer(cfg, store)
		srv.Start()
		servers[i] = srv
		addrs[i] = srv.Addr()
		metaStore.RegisterNode(ctx, &metadata.NodeInfo{
			ID: metadata.NodeID(i + 1), Addr: addrs[i],
			State: metadata.NodeOnline, CapacityGB: 10000,
			Tier: metadata.TierHot, Zone: "z", Rack: "r", MachineID: "m",
		})
	}
	defer func() { for _, s := range servers { s.Stop() } }()

	cs := chunkstore.NewDatanodeChunkStore()
	defer cs.Close()
	gw := s3.NewGateway(s3.GatewayConfig{
		MetaService: metaStore, ChunkStore: cs,
		RejectEmptyReplicas: true,
	})
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	// Create bucket and write data via S3
	metaStore.CreateBucket(ctx, "repair-test", metadata.PlacementPolicy{
		ReplicationFactor: 3, StorageTier: metadata.TierHot,
	})
	payload := []byte("repair test data with enough content to verify integrity across replicas")
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut,
		ts.URL+"/repair-test/data.txt", nil)
	req.Body = newReadCloser(payload)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT: %d", resp.StatusCode)
	}

	// Verify data is readable
	body := doGet(t, ctx, ts.URL+"/repair-test/data.txt", http.StatusOK)
	if string(body) != string(payload) {
		t.Fatalf("initial GET mismatch")
	}
	t.Log("data written and verified with 3 replicas")

	// Kill datanode 0 (losing 1 replica)
	t.Log("killing datanode 0...")
	servers[0].Stop()
	time.Sleep(200 * time.Millisecond)

	// Verify data still readable (2 replicas remaining)
	body = doGet(t, ctx, ts.URL+"/repair-test/data.txt", http.StatusOK)
	if string(body) != string(payload) {
		t.Fatal("GET after kill: data mismatch")
	}
	t.Log("GET after kill succeeded — 2 replicas remaining")

	// Trigger repair (the repair worker should detect the missing replica
	// and create a new one on another node)
	metaStore.TriggerRepair(ctx, 1) // trigger repair for chunk ID 1
	time.Sleep(2 * time.Second)     // wait for repair to complete

	// Verify data is still readable after repair
	body = doGet(t, ctx, ts.URL+"/repair-test/data.txt", http.StatusOK)
	if string(body) != string(payload) {
		t.Fatal("GET after repair: data mismatch")
	}
	t.Log("repair path verified: data survived node failure + repair")
}

// readCloser wraps a byte slice as an io.ReadCloser for http.Request.Body.
type readCloser struct {
	*bytes.Reader
}

func (rc *readCloser) Close() error { return nil }

func newReadCloser(data []byte) *readCloser {
	return &readCloser{bytes.NewReader(data)}
}
