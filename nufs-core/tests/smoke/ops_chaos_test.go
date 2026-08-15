package smoke

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/datanode"
	"github.com/dshmyz/nufs/nufs-core/gateway/s3"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// countChunksPerDisk counts chunks on each disk from ListChunks.
func countChunksPerDisk(cs datanode.OpsStore) []int64 {
	counts := make([]int64, len(cs.DiskInfos()))
	for _, info := range cs.ListChunks() {
		if info.DiskIndex >= 0 && info.DiskIndex < len(counts) {
			counts[info.DiskIndex]++
		}
	}
	return counts
}

// TestOpsFlow_AdoptMigrateDecommissionRestart exercises the full disk
// lifecycle: adopt → write → migrate → remove → verify data.
func TestOpsFlow_AdoptMigrateDecommissionRestart(t *testing.T) {
	if os.Getenv("NUFS_RUN_SMOKE") != "1" {
		t.Skip("set NUFS_RUN_SMOKE=1")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Phase 1: Start a V2.1 node with 2 disks
	disk0Dir := t.TempDir()
	disk1Dir := t.TempDir()
	n := startV21Datanode(t, 1, disk0Dir, disk1Dir)
	cs := n.Store

	metaStore, _ := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: t.TempDir(), NodeID: 1})
	defer metaStore.Close()
	metaStore.RegisterNode(ctx, &metadata.NodeInfo{
		ID: 1, Addr: n.Server.Addr(), State: metadata.NodeOnline,
		CapacityGB: 10000, Tier: metadata.TierHot,
		Zone: "z1", Rack: "r1", MachineID: "m1",
	})

	gw := s3.NewGateway(s3.GatewayConfig{
		MetaService: metaStore, ChunkStore: chunkstore.NewDatanodeChunkStore(),
		RejectEmptyReplicas: true,
	})
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	// Create bucket with RF=1 BEFORE any object PUTs
	metaStore.CreateBucket(ctx, "ops-bucket", metadata.PlacementPolicy{
		ReplicationFactor: 1, StorageTier: metadata.TierHot,
	})

	// Phase 2: Write 20 objects
	for i := 0; i < 20; i++ {
		doPut(t, ctx, ts.URL+"/ops-bucket/key"+string(rune('a'+i)),
			bytes.NewReader(bytes.Repeat([]byte("x"), 100)), http.StatusOK)
	}
	counts := countChunksPerDisk(cs)
	t.Logf("phase 2: disk0=%d, disk1=%d", counts[0], counts[1])

	// Phase 3: Adopt a 3rd disk
	disk2Dir := t.TempDir()
	idx, err := cs.AddDisk(disk2Dir, 8, 8)
	if err != nil {
		t.Fatalf("AddDisk: %v", err)
	}
	t.Logf("phase 3: adopted disk index=%d", idx)
	if len(cs.DiskInfos()) != 3 {
		t.Fatalf("expected 3 disks, got %d", len(cs.DiskInfos()))
	}

	// Phase 4: Write 10 more objects — should spread to new disk
	for i := 0; i < 10; i++ {
		doPut(t, ctx, ts.URL+"/ops-bucket/new"+string(rune('a'+i)),
			bytes.NewReader(bytes.Repeat([]byte("y"), 100)), http.StatusOK)
	}
	counts = countChunksPerDisk(cs)
	t.Logf("phase 4: disk0=%d, disk1=%d, disk2=%d", counts[0], counts[1], counts[2])
	if counts[2] == 0 {
		t.Fatal("new disk got no chunks — least-used selection not working")
	}

	// Phase 5: Migrate disk 0 to other disks
	t.Log("phase 5: migrating disk 0...")
	start := time.Now()
	if _, err := cs.MigrateDisk(0); err != nil {
		t.Fatalf("MigrateDisk(0): %v", err)
	}
	elapsed := time.Since(start)
	counts = countChunksPerDisk(cs)
	t.Logf("phase 5: disk0=%d after migration (took %v)", counts[0], elapsed.Round(time.Millisecond))
	if counts[0] != 0 {
		t.Fatalf("disk 0 still has %d chunks after migration", counts[0])
	}

	// All data should still be readable
	for i := 0; i < 20; i++ {
		doGet(t, ctx, ts.URL+"/ops-bucket/key"+string(rune('a'+i)), http.StatusOK)
	}
	t.Log("phase 5: all data readable after migration")

	// Phase 6: Remove disk 1
	if err := cs.RemoveDisk(1); err != nil {
		t.Fatalf("RemoveDisk(1): %v", err)
	}
	status := cs.DiskInfos()
	if !status[1].Failed {
		t.Fatal("disk 1 should be failed after removal")
	}
	t.Log("phase 6: disk 1 removed")

	// Phase 7: Write more — goes to remaining healthy disks
	for i := 0; i < 5; i++ {
		doPut(t, ctx, ts.URL+"/ops-bucket/after-remove"+string(rune('a'+i)),
			bytes.NewReader(bytes.Repeat([]byte("z"), 100)), http.StatusOK)
	}
	t.Log("phase 7: post-removal writes succeeded")

	t.Log("=== ops flow test complete ===")
}

// TestChaos_RandomDiskNodeKill runs S3 operations while killing datanodes,
// verifying known data survives all chaos.
func TestChaos_RandomDiskNodeKill(t *testing.T) {
	if os.Getenv("NUFS_RUN_SMOKE") != "1" {
		t.Skip("set NUFS_RUN_SMOKE=1")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	metaStore, _ := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: t.TempDir(), NodeID: 1})
	defer metaStore.Close()

	const numNodes = 3
	nodes := make([]*v21Datanode, numNodes)
	servers := make([]*datanode.Server, numNodes)
	for i := 0; i < numNodes; i++ {
		n := startV21Datanode(t, metadata.NodeID(i+1), t.TempDir())
		nodes[i] = n
		servers[i] = n.Server
		metaStore.RegisterNode(ctx, &metadata.NodeInfo{
			ID: metadata.NodeID(i + 1), Addr: n.Server.Addr(),
			State: metadata.NodeOnline, CapacityGB: 10000,
			Tier: metadata.TierHot, Zone: "z", Rack: "r", MachineID: "m",
		})
	}
	// Stop/close every node at test end. t.Cleanup (registered by
	// startV21Datanode) covers the originals; Stop and Close are both
	// idempotent, so this only additionally cleans up the mid-run restart.
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.Server.Stop()
				n.Close()
			}
		}
	}()

	cs := chunkstore.NewDatanodeChunkStore()
	defer cs.Close()
	gw := s3.NewGateway(s3.GatewayConfig{
		MetaService: metaStore, ChunkStore: cs, RejectEmptyReplicas: true,
	})
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	// Pre-seed known data. RF=3 so known.txt is replicated on every node: with
	// RF=1 and 2 of 3 nodes killed (or restarted fresh), the single replica
	// could land on a dead node and the final assertion becomes probabilistic.
	metaStore.CreateBucket(ctx, "chaos-bucket", metadata.PlacementPolicy{
		ReplicationFactor: 3, StorageTier: metadata.TierHot,
	})
	knownPayload := bytes.Repeat([]byte("known-data"), 1000)
	doPut(t, ctx, ts.URL+"/chaos-bucket/known.txt", bytes.NewReader(knownPayload), http.StatusOK)
	body := doGet(t, ctx, ts.URL+"/chaos-bucket/known.txt", http.StatusOK)
	if !bytes.Equal(body, knownPayload) {
		t.Fatal("known data corrupted before chaos")
	}

	// Chaos: kill datanodes during operations
	var opsDone, errorsHit, readsOK atomic.Int64
	chaosDone := make(chan struct{})
	go func() {
		defer close(chaosDone)
		time.Sleep(10 * time.Second)
		t.Log("chaos: kill datanode 0")
		servers[0].Stop()
		time.Sleep(8 * time.Second)
		t.Log("chaos: kill datanode 1")
		servers[1].Stop()
		time.Sleep(10 * time.Second)
		t.Log("chaos: restart datanode 0")
		// Build via newV21Node (no t.Fatalf): this goroutine is not the test
		// goroutine, and FailNow from here would panic the test binary.
		nn, err := newV21Node(1, t.TempDir())
		if err != nil {
			t.Errorf("restart datanode 0: %v", err)
			return
		}
		servers[0] = nn.Server
		nodes[0] = nn
		metaStore.RegisterNode(ctx, &metadata.NodeInfo{
			ID: 1, Addr: nn.Server.Addr(), State: metadata.NodeOnline,
			CapacityGB: 10000, Tier: metadata.TierHot,
			Zone: "z", Rack: "r", MachineID: "m",
		})
	}()

	client := &http.Client{Timeout: 10 * time.Second}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(wID int) {
			defer wg.Done()
			end := time.Now().Add(70 * time.Second)
			for time.Now().Before(end) {
				key := fmt.Sprintf("/chaos-bucket/c%d-%d", wID, opsDone.Add(1))
				payload := bytes.Repeat([]byte("x"), 4096)
				req, _ := http.NewRequestWithContext(ctx, http.MethodPut, ts.URL+key, bytes.NewReader(payload))
				resp, err := client.Do(req)
				if err != nil {
					errorsHit.Add(1)
					continue
				}
				resp.Body.Close()
				req2, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+key, nil)
				resp2, err := client.Do(req2)
				if err == nil {
					resp2.Body.Close()
					readsOK.Add(1)
				} else {
					errorsHit.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	// Join the chaos goroutine before the deferred cleanup so the restarted
	// node's stop/close has a happens-before edge (no data race on nodes[0]).
	<-chaosDone

	// Verify known data survived
	finalBody := doGet(t, ctx, ts.URL+"/chaos-bucket/known.txt", http.StatusOK)
	if !bytes.Equal(finalBody, knownPayload) {
		t.Fatal("known data CORRUPTED after chaos — data loss!")
	}
	t.Logf("chaos: ops=%d errors=%d readsOK=%d — known data survived",
		opsDone.Load(), errorsHit.Load(), readsOK.Load())
}
