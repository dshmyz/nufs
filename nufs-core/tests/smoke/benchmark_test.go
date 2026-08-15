package smoke

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/gateway/s3"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// TestBenchmark_S3Workload runs a realistic S3 workload benchmark with
// proper client configuration, mixed read/write ratio, and varying object
// sizes. This is the scientific test for production readiness assessment.
//
// Workload: 90% GET / 10% PUT, objects 4KB-256KB, proper connection pool.
// Measures: latency percentiles, throughput, error rate, resource usage.
func TestBenchmark_S3Workload(t *testing.T) {
	if os.Getenv("NUFS_RUN_SMOKE") != "1" {
		t.Skip("set NUFS_RUN_SMOKE=1 to run benchmark")
	}

	const (
		duration     = 60 * time.Second
		workers      = 8
		writeRatio   = 0.10 // 10% writes, 90% reads
		minObjSize   = 4096
		maxObjSize   = 256 * 1024
		numObjects   = 200  // pre-seed object pool for reads
	)

	ctx := context.Background()

	// === Start cluster ===
	metaStore, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: t.TempDir(), NodeID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer metaStore.Close()

	const numDatanodes = 3
	addrs := make([]string, numDatanodes)
	for i := 0; i < numDatanodes; i++ {
		n := startV21Datanode(t, metadata.NodeID(i+1), t.TempDir())
		addrs[i] = n.Server.Addr()
		metaStore.RegisterNode(ctx, &metadata.NodeInfo{
			ID: metadata.NodeID(i + 1), Addr: addrs[i],
			State: metadata.NodeOnline, CapacityGB: 10000,
			Tier: metadata.TierHot, Zone: "test", Rack: "r1", MachineID: "m1",
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

	// === Configure HTTP client properly (like production S3 clients) ===
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
			WriteBufferSize:     64 * 1024,
			ReadBufferSize:      64 * 1024,
		},
		Timeout: 30 * time.Second,
	}

	// === Pre-seed object pool for reads ===
	metaStore.CreateBucket(ctx, "bench-bucket", metadata.PlacementPolicy{
		ReplicationFactor: 1, StorageTier: metadata.TierHot,
	})

	t.Logf("pre-seeding %d objects...", numObjects)
	objectPool := make([]string, numObjects)
	for i := 0; i < numObjects; i++ {
		size := minObjSize + (i*173)%((maxObjSize - minObjSize))
		data := make([]byte, size)
		rand.Read(data)
		key := fmt.Sprintf("bench/obj-%04d", i)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, ts.URL+"/bench-bucket/"+key, bytes.NewReader(data))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("seed PUT %s: %v", key, err)
		}
		resp.Body.Close()
		objectPool[i] = key
	}
	t.Logf("pre-seeded %d objects", numObjects)

	// === Warm up connection pool ===
	for i := 0; i < 50; i++ {
		key := objectPool[i%numObjects]
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/bench-bucket/"+key, nil)
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}

	// === Collect baseline ===
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	pebbleBefore := metaStore.PebbleStats()

	goroutinesBefore := runtime.NumGoroutine()

	// === Run benchmark ===
	t.Logf("benchmark: %d workers, %v duration, %.0f%% writes, %d-%dKB objects",
		workers, duration, writeRatio*100, minObjSize/1024, maxObjSize/1024)

	var (
		totalOps     atomic.Int64
		totalErrors  atomic.Int64
		getOps       atomic.Int64
		putOps       atomic.Int64
		totalBytes   atomic.Int64
		allLatencies []time.Duration
		latMu        sync.Mutex
	)

	// Memory sampler
	stopSampler := make(chan struct{})
	var memSamples []uint64
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				memSamples = append(memSamples, m.Sys)
			case <-stopSampler:
				return
			}
		}
	}()

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := int64(workerID*1000 + 1)
			for time.Since(start) < duration {
				// Decide: read or write?
				rng = (rng*6364136223846793005 + 1) & 0x7FFFFFFFFFFFFFFF
				isWrite := (rng % 100) < int64(writeRatio*100)

				if isWrite {
					// PUT: random size, random data
					size := minObjSize + int(rng%int64(maxObjSize-minObjSize))
					data := make([]byte, size)
					rand.Read(data)
					key := fmt.Sprintf("bench/w%d-op%d", workerID, totalOps.Add(1)-1)
					objURL := ts.URL + "/bench-bucket/" + key

					opStart := time.Now()
					req, _ := http.NewRequestWithContext(ctx, http.MethodPut, objURL, bytes.NewReader(data))
					resp, err := client.Do(req)
					latency := time.Since(opStart)
					if err != nil || resp.StatusCode != http.StatusOK {
						totalErrors.Add(1)
						putOps.Add(1)
						if resp != nil { resp.Body.Close() }
						continue
					}
					resp.Body.Close()
					totalOps.Add(1)
					putOps.Add(1)
					totalBytes.Add(int64(len(data)))
					latMu.Lock()
					allLatencies = append(allLatencies, latency)
					latMu.Unlock()
				} else {
					// GET: random existing object
					objIdx := int(rng % int64(numObjects))
					key := objectPool[objIdx]
					objURL := ts.URL + "/bench-bucket/" + key

					opStart := time.Now()
					req, _ := http.NewRequestWithContext(ctx, http.MethodGet, objURL, nil)
					resp, err := client.Do(req)
					latency := time.Since(opStart)
					if err != nil || resp.StatusCode != http.StatusOK {
						totalErrors.Add(1)
						getOps.Add(1)
						if resp != nil { resp.Body.Close() }
						continue
					}
					body, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					totalOps.Add(1)
					getOps.Add(1)
					totalBytes.Add(int64(len(body)))
					latMu.Lock()
					allLatencies = append(allLatencies, latency)
					latMu.Unlock()
				}
			}
		}(w)
	}

	wg.Wait()
	close(stopSampler)
	elapsed := time.Since(start)

	// === Collect final metrics ===
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	pebbleAfter := metaStore.PebbleStats()

	// === Compute results ===
	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
	totalOpsVal := totalOps.Load()
	totalErrorsVal := totalErrors.Load()
	getOpsVal := getOps.Load()
	putOpsVal := putOps.Load()
	totalBytesVal := totalBytes.Load()

	ops := float64(totalOpsVal) / elapsed.Seconds()
	throughput := float64(totalBytesVal) / elapsed.Seconds() / 1024 / 1024

	p50 := percentile(allLatencies, 0.50)
	p90 := percentile(allLatencies, 0.90)
	p95 := percentile(allLatencies, 0.95)
	p99 := percentile(allLatencies, 0.99)
	p999 := percentile(allLatencies, 0.999)

	// === Report ===
	t.Log("")
	t.Log("========================================")
	t.Log("  S3 BENCHMARK RESULTS")
	t.Log("========================================")
	t.Logf("Duration:         %v", elapsed.Round(time.Second))
	t.Logf("Workers:          %d", workers)
	t.Logf("Write ratio:      %.0f%%", writeRatio*100)
	t.Logf("Object size:      %d-%d KB", minObjSize/1024, maxObjSize/1024)
	t.Log("")
	t.Logf("Total ops:        %d", totalOpsVal)
	t.Logf("  GET:            %d", getOpsVal)
	t.Logf("  PUT:            %d", putOpsVal)
	t.Logf("Errors:           %d (%.2f%%)", totalErrorsVal, float64(totalErrorsVal)/float64(max(1, int(totalOpsVal)))*100)
	t.Logf("Throughput:       %.2f MB/s", throughput)
	t.Logf("Ops/sec:          %.0f", ops)
	t.Log("")
	t.Logf("Latency P50:      %v", p50)
	t.Logf("Latency P90:      %v", p90)
	t.Logf("Latency P95:      %v", p95)
	t.Logf("Latency P99:      %v", p99)
	t.Logf("Latency P99.9:    %v", p999)
	t.Log("")
	t.Logf("Memory RSS:       %d MB → %d MB (delta: %+d MB)",
		memBefore.Sys/1024/1024, memAfter.Sys/1024/1024,
		(int64(memAfter.Sys)-int64(memBefore.Sys))/1024/1024)
	t.Logf("Goroutines:       %d → %d (delta: %+d)",
		goroutinesBefore, runtime.NumGoroutine(),
		runtime.NumGoroutine()-goroutinesBefore)
	t.Logf("Pebble L0:        %d → %d", pebbleBefore.L0Files, pebbleAfter.L0Files)
	t.Logf("Pebble debt:      %d → %d bytes", pebbleBefore.CompactionDebt, pebbleAfter.CompactionDebt)
	t.Log("========================================")
}
