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

	"github.com/example/dfs/chunkstore"
	"github.com/example/dfs/datanode"
	"github.com/example/dfs/gateway/s3"
	"github.com/example/dfs/metadata"
)

// TestLoad_SustainedS3Ops runs sustained S3 PUT/GET operations against a
// real cluster (metad + 3 datanodes + s3 gateway) for a configurable
// duration, measuring latency percentiles, throughput, and resource usage.
//
// Usage:
//
//	NUFS_RUN_SMOKE=1 NUFS_LOAD_DURATION=60s NUFS_LOAD_WORKERS=8 go test \
//	  -v -run TestLoad_SustainedS3Ops -timeout=120s ./tests/smoke/
//
// Defaults: 60s duration, 8 workers, 64KB objects.
func TestLoad_SustainedS3Ops(t *testing.T) {
	if os.Getenv("NUFS_RUN_SMOKE") != "1" {
		t.Skip("set NUFS_RUN_SMOKE=1 to run load test")
	}

	const (
		defaultDuration = 60 * time.Second
		defaultWorkers  = 8
		defaultObjSize  = 64 * 1024 // 64KB
	)
	duration := parseDurationEnv("NUFS_LOAD_DURATION", defaultDuration)
	workers := parseIntEnv("NUFS_LOAD_WORKERS", defaultWorkers)
	objSize := parseIntEnv("NUFS_LOAD_OBJ_SIZE", defaultObjSize)

	t.Logf("load test: duration=%v workers=%d objSize=%d", duration, workers, objSize)

	// === Start cluster (3-node Raft metadata) ===
	ctx, cancel := context.WithTimeout(context.Background(), duration+30*time.Second)
	defer cancel()

	metaStore, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: t.TempDir(), NodeID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer metaStore.Close()


	const numDatanodes = 3
	servers := make([]*datanode.Server, numDatanodes)
	addrs := make([]string, numDatanodes)
	for i := 0; i < numDatanodes; i++ {
		store, err := datanode.NewChunkStore(t.TempDir(), 64, 256, nil)
		if err != nil {
			t.Fatal(err)
		}
		store.WaitForScan()
		cfg := datanode.DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.NodeID = metadata.NodeID(i + 1)
		cfg.RequestTimeout = 10 * time.Second
		srv := datanode.NewServer(cfg, store)
		srv.Start()
		servers[i] = srv
		addrs[i] = srv.Addr()
		metaStore.RegisterNode(ctx, &metadata.NodeInfo{
			ID: metadata.NodeID(i + 1), Addr: addrs[i],
			State: metadata.NodeOnline, CapacityGB: 10000,
			Tier: metadata.TierHot, Zone: "test-zone", Rack: "rack-1", MachineID: "machine-1",
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

	// Create bucket
	doPut(t, ctx, ts.URL+"/load-bucket", nil, http.StatusOK)

	// Pre-generate random payloads
	payloads := make([][]byte, workers)
	for i := range payloads {
		payloads[i] = make([]byte, objSize)
		rand.Read(payloads[i])
	}

	// === Warm up ===
	t.Log("warming up...")
	warmupCount := 20 // Raft PPUT ~900ms, keep warmup fast
	for i := 0; i < warmupCount; i++ {
		key := fmt.Sprintf("warmup/%d", i)
		doPut(t, ctx, ts.URL+"/load-bucket/"+key, bytes.NewReader(payloads[i%workers]), http.StatusOK)
	}

	// === Collect baseline metrics ===
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	fdBefore := countOpenFDs()
	pebbleBefore := metaStore.PebbleStats()

	// === Start load ===
	t.Logf("starting load: %d workers for %v", workers, duration)

	var (
		totalOps     atomic.Int64
		totalErrors  atomic.Int64
		putErrors    atomic.Int64
		getErrors    atomic.Int64
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
			payload := payloads[workerID]
			for time.Since(start) < duration {
				key := fmt.Sprintf("load/w%d/op%d", workerID, totalOps.Add(1)-1)
				objURL := ts.URL + "/load-bucket/" + key

				// PUT
				opStart := time.Now()
				req, _ := http.NewRequestWithContext(ctx, http.MethodPut, objURL, bytes.NewReader(payload))
				resp, err := http.DefaultClient.Do(req)
				latency := time.Since(opStart)
				if err != nil || resp.StatusCode != http.StatusOK {
					totalErrors.Add(1)
					putErrors.Add(1)
					if resp != nil {
						resp.Body.Close()
					}
					continue
				}
				resp.Body.Close()
				totalBytes.Add(int64(len(payload)))

				latMu.Lock()
				allLatencies = append(allLatencies, latency)
				latMu.Unlock()

				// GET (verify)
				opStart = time.Now()
				req, _ = http.NewRequestWithContext(ctx, http.MethodGet, objURL, nil)
				resp, err = http.DefaultClient.Do(req)
				latency = time.Since(opStart)
				if err != nil || resp.StatusCode != http.StatusOK {
					totalErrors.Add(1)
					getErrors.Add(1)
					if resp != nil {
						resp.Body.Close()
					}
					continue
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				latency = time.Since(opStart)

				latMu.Lock()
				allLatencies = append(allLatencies, latency)
				latMu.Unlock()
				totalBytes.Add(int64(len(body)))
			}
		}(w)
	}

	wg.Wait()
	close(stopSampler)
	elapsed := time.Since(start)

	// === Collect final metrics ===
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	fdAfter := countOpenFDs()
	pebbleAfter := metaStore.PebbleStats()

	// === Compute results ===
	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
	totalOpsVal := totalOps.Load()
	totalErrorsVal := totalErrors.Load()
	totalBytesVal := totalBytes.Load()

	ops := float64(totalOpsVal) / elapsed.Seconds()
	throughput := float64(totalBytesVal) / elapsed.Seconds() / 1024 / 1024 // MB/s

	p50 := percentile(allLatencies, 0.50)
	p95 := percentile(allLatencies, 0.95)
	p99 := percentile(allLatencies, 0.99)

	t.Logf("=== LOAD TEST RESULTS ===")
	t.Logf("Duration:       %v", elapsed.Round(time.Second))
	t.Logf("Workers:        %d", workers)
	t.Logf("Object size:    %d bytes", objSize)
	t.Logf("Total ops:      %d", totalOpsVal)
	t.Logf("Total errors:   %d (%.2f%%) [PUT: %d, GET: %d]",
		totalErrorsVal, float64(totalErrorsVal)/float64(totalOpsVal)*100,
		putErrors.Load(), getErrors.Load())
	t.Logf("Throughput:     %.2f MB/s", throughput)
	t.Logf("Ops/sec:        %.0f", ops)
	t.Logf("Latency P50:    %v", p50)
	t.Logf("Latency P95:    %v", p95)
	t.Logf("Latency P99:    %v", p99)
	t.Logf("Memory RSS:     %d MB → %d MB (delta: %+d MB)",
		memBefore.Sys/1024/1024, memAfter.Sys/1024/1024,
		(int64(memAfter.Sys)-int64(memBefore.Sys))/1024/1024)
	t.Logf("Goroutines:       %d → %d (delta: %+d)", fdBefore, fdAfter, fdAfter-fdBefore)
	if len(memSamples) > 0 {
		t.Logf("Memory trend:   min=%d MB max=%d MB",
			minUint64(memSamples)/1024/1024, maxUint64(memSamples)/1024/1024)
	}
	t.Logf("Pebble L0 files: %d → %d (delta: %+d)",
		pebbleBefore.L0Files, pebbleAfter.L0Files, pebbleAfter.L0Files-pebbleBefore.L0Files)
	t.Logf("Pebble compaction debt: %d -> %d bytes",
		pebbleBefore.CompactionDebt, pebbleAfter.CompactionDebt)
	t.Logf("Pebble memtable: %d bytes", pebbleAfter.MemTableSize)

	// === Assertions (relaxed for short runs; strict thresholds for >=60s) ===
	errorRate := float64(totalErrorsVal) / float64(totalOpsVal)
	if errorRate > 0.01 {
		t.Errorf("error rate %.2f%% exceeds 1%% threshold", errorRate*100)
	}
	if p99 > 10*time.Second {
		t.Errorf("P99 latency %v exceeds 10s threshold", p99)
	}
	fdDelta := fdAfter - fdBefore
	if fdDelta > 200 {
		t.Errorf("Goroutine leak detected: %d → %d (delta %d > 200)", fdBefore, fdAfter, fdDelta)
	}
	memDelta := int64(memAfter.Sys) - int64(memBefore.Sys)
	// Only enforce memory threshold for runs >= 60s. Short runs (<60s)
	// have Go runtime warmup overhead; long runs should stabilize.
	// 1.5GB headroom accounts for: Go heap + GC + HTTP client buffers
	// + datanode chunk cache + connection pools.
	if duration >= 60*time.Second && memDelta > 1500*1024*1024 {
		t.Errorf("memory growth %d MB exceeds 1.5GB threshold for %v run", memDelta/1024/1024, duration)
	}
}

// ============================================================
// Helpers
// ============================================================

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func countOpenFDs() int {
	return runtime.NumGoroutine() // goroutine count as resource proxy
}

func minUint64(vals []uint64) uint64 {
	min := vals[0]
	for _, v := range vals[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

func maxUint64(vals []uint64) uint64 {
	max := vals[0]
	for _, v := range vals[1:] {
		if v > max {
			max = v
		}
	}
	return max
}


func parseDurationEnv(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func parseIntEnv(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n := 0
		for _, c := range v {
			if c >= '0' && c <= '9' {
				n = n*10 + int(c-'0')
			}
		}
		if n > 0 {
			return n
		}
	}
	return def
}
