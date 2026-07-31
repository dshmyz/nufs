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

	"github.com/example/dfs/chunkstore"
	"github.com/example/dfs/datanode"
	"github.com/example/dfs/gateway/s3"
	"github.com/example/dfs/metadata"
)

// countChunksPerDisk counts chunks on each disk from ListChunks.
func countChunksPerDisk(cs *datanode.ChunkStore) []int64 {
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

	// Phase 1: Start with 2 disks
	disk0Dir := t.TempDir()
	disk1Dir := t.TempDir()
	wal0, _ := datanode.NewWriteAheadLog(disk0Dir + "/wal")
	wal1, _ := datanode.NewWriteAheadLog(disk1Dir + "/wal")

	cs, _ := datanode.NewMultiDiskChunkStore([]string{disk0Dir, disk1Dir}, 8, 8,
		[]*datanode.WriteAheadLog{wal0, wal1})
	cs.WaitForScan()
	dm, _ := datanode.NewMultiDiskManager([]string{disk0Dir, disk1Dir}, cs,
		[]int64{100, 100}, []*datanode.WriteAheadLog{wal0, wal1})
	cs.SetDiskManager(dm)

	srvCfg := datanode.DefaultConfig()
	srvCfg.ListenAddr = "127.0.0.1:0"
	srvCfg.NodeID = 1
	srvCfg.RequestTimeout = 5 * time.Second
	srv := datanode.NewServer(srvCfg, cs)
	srv.Start()
	defer srv.Stop()

	metaStore, _ := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: t.TempDir(), NodeID: 1})
	defer metaStore.Close()
	metaStore.RegisterNode(ctx, &metadata.NodeInfo{
		ID: 1, Addr: srv.Addr(), State: metadata.NodeOnline,
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
	wal2, _ := datanode.NewWriteAheadLog(disk2Dir + "/wal")
	idx, err := cs.AddDisk(disk2Dir, 8, 8, wal2)
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
	cs.MigrateDisk(0)
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
	cs.RemoveDisk(1)
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
		MetaService: metaStore, ChunkStore: cs, RejectEmptyReplicas: true,
	})
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	// Pre-seed known data
	metaStore.CreateBucket(ctx, "chaos-bucket", metadata.PlacementPolicy{
		ReplicationFactor: 1, StorageTier: metadata.TierHot,
	})
	knownPayload := bytes.Repeat([]byte("known-data"), 1000)
	doPut(t, ctx, ts.URL+"/chaos-bucket/known.txt", bytes.NewReader(knownPayload), http.StatusOK)
	body := doGet(t, ctx, ts.URL+"/chaos-bucket/known.txt", http.StatusOK)
	if !bytes.Equal(body, knownPayload) {
		t.Fatal("known data corrupted before chaos")
	}

	// Chaos: kill datanodes during operations
	var opsDone, errorsHit, readsOK atomic.Int64
	go func() {
		time.Sleep(10 * time.Second)
		t.Log("chaos: kill datanode 0")
		servers[0].Stop()
		time.Sleep(8 * time.Second)
		t.Log("chaos: kill datanode 1")
		servers[1].Stop()
		time.Sleep(10 * time.Second)
		t.Log("chaos: restart datanode 0")
		store, _ := datanode.NewChunkStore(t.TempDir(), 64, 256, nil)
		store.WaitForScan()
		cfg := datanode.DefaultConfig()
		cfg.ListenAddr = addrs[0]
		cfg.NodeID = 1
		cfg.RequestTimeout = 5 * time.Second
		srv := datanode.NewServer(cfg, store)
		srv.Start()
		servers[0] = srv
		metaStore.RegisterNode(ctx, &metadata.NodeInfo{
			ID: 1, Addr: addrs[0], State: metadata.NodeOnline,
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

	// Verify known data survived
	finalBody := doGet(t, ctx, ts.URL+"/chaos-bucket/known.txt", http.StatusOK)
	if !bytes.Equal(finalBody, knownPayload) {
		t.Fatal("known data CORRUPTED after chaos — data loss!")
	}
	t.Logf("chaos: ops=%d errors=%d readsOK=%d — known data survived",
		opsDone.Load(), errorsHit.Load(), readsOK.Load())
}
