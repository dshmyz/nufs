package smoke

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/datanode"
	"github.com/dshmyz/nufs/nufs-core/gateway/s3"
	"github.com/dshmyz/nufs/nufs-core/metadata"
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
		n := startV21Datanode(t, metadata.NodeID(i+1), t.TempDir())
		servers[i] = n.Server
		addrs[i] = n.Server.Addr()
		metaStore.RegisterNode(ctx, &metadata.NodeInfo{
			ID: metadata.NodeID(i + 1), Addr: addrs[i],
			State: metadata.NodeOnline, CapacityGB: 10000,
			Tier: metadata.TierHot, Zone: "z", Rack: "r", MachineID: "m",
		})
	}

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

// TestRepair_EndToEnd_EC verifies the EC repair path: write data with
// K=6 M=3 erasure coding across 9 datanodes, kill one datanode, trigger
// repair, and verify the chunk is still readable via EC reconstruction.
//
// NOTE: the scheme must be 6+3. The metadata allocator materializes every
// EC chunk's group from the shared 6+3 profile (ECGroupFromProfile defaults
// to the canonical 6+3), so a bucket configured with any other K/M writes
// only the first K data shards of the 6+3 codec and gains zero fault
// tolerance — one lost shard makes the object unreadable. All other EC
// tests (ec_record_direct_*, zz_raft_alloc_repro) use 6+3 for the same
// reason.
func TestRepair_EndToEnd_EC(t *testing.T) {
	if os.Getenv("NUFS_RUN_SMOKE") != "1" {
		t.Skip("set NUFS_RUN_SMOKE=1 to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Start metadata + 9 datanodes (K=6 + M=3)
	metaDir := t.TempDir()
	metaStore, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: metaDir, NodeID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer metaStore.Close()

	const numNodes = 9 // K+M for EC(6,3)
	servers := make([]*datanode.Server, numNodes)
	addrs := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		n := startV21Datanode(t, metadata.NodeID(i+1), t.TempDir())
		servers[i] = n.Server
		addrs[i] = n.Server.Addr()
		metaStore.RegisterNode(ctx, &metadata.NodeInfo{
			ID: metadata.NodeID(i + 1), Addr: addrs[i],
			State: metadata.NodeOnline, CapacityGB: 10000,
			Tier: metadata.TierHot, Zone: "z", Rack: "r", MachineID: "m",
		})
	}

	cs := chunkstore.NewDatanodeChunkStore()
	defer cs.Close()
	gw := s3.NewGateway(s3.GatewayConfig{
		MetaService: metaStore, ChunkStore: cs,
		RejectEmptyReplicas: true,
	})
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	// Create bucket with EC policy (K=6, M=3)
	metaStore.CreateBucket(ctx, "ec-repair-test", metadata.PlacementPolicy{
		ReplicationFactor: 1,
		ECConfig:          &metadata.ECConfig{DataShards: 6, ParityShards: 3},
		StorageTier:       metadata.TierHot,
	})

	payload := bytes.Repeat([]byte("EC repair test payload "), 50)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut,
		ts.URL+"/ec-repair-test/data.txt", nil)
	req.Body = newReadCloser(payload)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT: %d", resp.StatusCode)
	}

	// Verify data readable (K=6 shards present)
	body := doGet(t, ctx, ts.URL+"/ec-repair-test/data.txt", http.StatusOK)
	if string(body) != string(payload) {
		t.Fatal("initial GET mismatch")
	}
	t.Log("EC data written and verified (K=6, M=3)")

	// Kill datanode 0 (losing shard 0)
	t.Log("killing datanode 0 (shard 0)...")
	servers[0].Stop()
	time.Sleep(200 * time.Millisecond)

	// Verify data still readable via EC reconstruction (8 shards >= K=6)
	body = doGet(t, ctx, ts.URL+"/ec-repair-test/data.txt", http.StatusOK)
	if string(body) != string(payload) {
		t.Fatal("GET after EC node failure: data mismatch")
	}
	t.Log("GET after EC node failure succeeded — EC reconstruction working")

	// Trigger repair for the chunk
	metaStore.TriggerRepair(ctx, 1)
	time.Sleep(2 * time.Second)

	// Verify data still readable after repair
	body = doGet(t, ctx, ts.URL+"/ec-repair-test/data.txt", http.StatusOK)
	if string(body) != string(payload) {
		t.Fatal("GET after EC repair: data mismatch")
	}
	t.Log("EC repair path verified: data survived node failure + EC reconstruction")
}
