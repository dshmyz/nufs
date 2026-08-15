// Command s3bench runs a standalone S3 benchmark against a remote NUFS S3
// gateway. It measures PUT/GET latency percentiles, throughput, IOPS, and
// error rate over a configurable duration.
//
// Usage:
//
//	s3bench -endpoint http://gateway:8180 -duration 60s -workers 8 -obj-size 65536
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	mrand "math/rand"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	endpoint   string
	bucket     string
	duration   time.Duration
	workers    int
	objSize    int
	writeRatio float64
	numObjects int
	jsonOut    bool
	authToken  string
}

type result struct {
	Duration      string  `json:"duration"`
	Workers       int     `json:"workers"`
	ObjSize       int     `json:"obj_size_bytes"`
	WriteRatio    float64 `json:"write_ratio"`
	TotalOps      int64   `json:"total_ops"`
	PutOps        int64   `json:"put_ops"`
	GetOps        int64   `json:"get_ops"`
	Errors        int64   `json:"errors"`
	ErrorRate     float64 `json:"error_rate_pct"`
	ThroughputMBs float64 `json:"throughput_mbps"`
	IOPS          float64 `json:"iops"`
	LatP50        string  `json:"latency_p50"`
	LatP90        string  `json:"latency_p90"`
	LatP95        string  `json:"latency_p95"`
	LatP99        string  `json:"latency_p99"`
	LatP999       string  `json:"latency_p999"`
	Goroutines    int     `json:"goroutines_delta"`
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.endpoint, "endpoint", "http://127.0.0.1:8180", "S3 gateway endpoint URL")
	flag.StringVar(&cfg.bucket, "bucket", "s3bench", "bucket name (auto-created)")
	flag.DurationVar(&cfg.duration, "duration", 60*time.Second, "benchmark duration")
	flag.IntVar(&cfg.workers, "workers", 8, "concurrent workers")
	flag.IntVar(&cfg.objSize, "obj-size", 65536, "object size in bytes (default 64KB)")
	flag.Float64Var(&cfg.writeRatio, "write-ratio", 0.10, "fraction of ops that are PUTs (0.0-1.0)")
	flag.IntVar(&cfg.numObjects, "num-objects", 200, "pre-seeded object pool for reads")
	flag.BoolVar(&cfg.jsonOut, "json", false, "output results as JSON")
	flag.StringVar(&cfg.authToken, "auth-token", "", "ops auth token (optional)")
	flag.Parse()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
		Timeout: 120 * time.Second,
	}

	// Create bucket
	if err := doRequest(client, cfg, http.MethodPut, "/"+cfg.bucket, nil); err != nil {
		fmt.Fprintf(os.Stderr, "create bucket: %v\n", err)
		os.Exit(1)
	}

	// Pre-seed object pool
	objectPool := make([]string, cfg.numObjects)
	if !cfg.jsonOut {
		fmt.Fprintf(os.Stderr, "pre-seeding %d objects (%d bytes each)...\n", cfg.numObjects, cfg.objSize)
	}
	for i := 0; i < cfg.numObjects; i++ {
		data := make([]byte, cfg.objSize)
		rand.Read(data)
		key := fmt.Sprintf("pool/obj-%06d", i)
		if err := doRequest(client, cfg, http.MethodPut, "/"+cfg.bucket+"/"+key, data); err != nil {
			fmt.Fprintf(os.Stderr, "seed PUT %s: %v\n", key, err)
			os.Exit(1)
		}
		objectPool[i] = key
	}

	// Warm up connection pool
	for i := 0; i < 50; i++ {
		key := objectPool[i%len(objectPool)]
		resp, err := client.Do(mustReq(cfg, http.MethodGet, "/"+cfg.bucket+"/"+key, nil))
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}

	goroutinesBefore := numGoroutines()

	// Run benchmark
	if !cfg.jsonOut {
		fmt.Fprintf(os.Stderr, "benchmark: %d workers, %v, %.0f%% writes, %d-byte objects\n",
			cfg.workers, cfg.duration, cfg.writeRatio*100, cfg.objSize)
	}

	var (
		totalOps   atomic.Int64
		putOps     atomic.Int64
		getOps     atomic.Int64
		totalErrs  atomic.Int64
		totalBytes atomic.Int64
		latencies  []time.Duration
		latMu      sync.Mutex
	)

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			rng := mrand.New(mrand.NewSource(int64(wid*1000 + 1)))
			for time.Since(start) < cfg.duration {
				isWrite := rng.Float64() < cfg.writeRatio
				if isWrite {
					data := make([]byte, cfg.objSize)
					rand.Read(data)
					key := fmt.Sprintf("load/w%d-op%d", wid, totalOps.Add(1)-1)
					opStart := time.Now()
					err := doRequest(client, cfg, http.MethodPut, "/"+cfg.bucket+"/"+key, data)
					lat := time.Since(opStart)
					putOps.Add(1)
					if err != nil {
						totalErrs.Add(1)
						continue
					}
					totalBytes.Add(int64(len(data)))
					latMu.Lock()
					latencies = append(latencies, lat)
					latMu.Unlock()
				} else {
					key := objectPool[rng.Intn(len(objectPool))]
					opStart := time.Now()
					resp, err := client.Do(mustReq(cfg, http.MethodGet, "/"+cfg.bucket+"/"+key, nil))
					lat := time.Since(opStart)
					getOps.Add(1)
					if err != nil {
						totalErrs.Add(1)
						continue
					}
					body, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					totalBytes.Add(int64(len(body)))
					latMu.Lock()
					latencies = append(latencies, lat)
					latMu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Compute results
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	opsVal := totalOps.Load()
	errVal := totalErrs.Load()
	bytesVal := totalBytes.Load()

	res := result{
		Duration:      elapsed.Round(time.Second).String(),
		Workers:       cfg.workers,
		ObjSize:       cfg.objSize,
		WriteRatio:    cfg.writeRatio,
		TotalOps:      opsVal,
		PutOps:        putOps.Load(),
		GetOps:        getOps.Load(),
		Errors:        errVal,
		ErrorRate:     float64(errVal) / float64(max(1, opsVal)) * 100,
		ThroughputMBs: float64(bytesVal) / elapsed.Seconds() / 1024 / 1024,
		IOPS:          float64(opsVal) / elapsed.Seconds(),
		LatP50:        pctl(latencies, 0.50).String(),
		LatP90:        pctl(latencies, 0.90).String(),
		LatP95:        pctl(latencies, 0.95).String(),
		LatP99:        pctl(latencies, 0.99).String(),
		LatP999:       pctl(latencies, 0.999).String(),
		Goroutines:    numGoroutines() - goroutinesBefore,
	}

	if cfg.jsonOut {
		json.NewEncoder(os.Stdout).Encode(res)
	} else {
		fmt.Println("========================================")
		fmt.Println("  S3 BENCHMARK RESULTS")
		fmt.Println("========================================")
		fmt.Printf("Duration:         %s\n", res.Duration)
		fmt.Printf("Workers:          %d\n", res.Workers)
		fmt.Printf("Write ratio:      %.0f%%\n", res.WriteRatio*100)
		fmt.Printf("Object size:      %d bytes\n", res.ObjSize)
		fmt.Println()
		fmt.Printf("Total ops:        %d\n", res.TotalOps)
		fmt.Printf("  PUT:            %d\n", res.PutOps)
		fmt.Printf("  GET:            %d\n", res.GetOps)
		fmt.Printf("Errors:           %d (%.2f%%)\n", res.Errors, res.ErrorRate)
		fmt.Printf("Throughput:       %.2f MB/s\n", res.ThroughputMBs)
		fmt.Printf("IOPS:             %.0f\n", res.IOPS)
		fmt.Println()
		fmt.Printf("Latency P50:      %s\n", res.LatP50)
		fmt.Printf("Latency P90:      %s\n", res.LatP90)
		fmt.Printf("Latency P95:      %s\n", res.LatP95)
		fmt.Printf("Latency P99:      %s\n", res.LatP99)
		fmt.Printf("Latency P99.9:    %s\n", res.LatP999)
		fmt.Printf("Goroutine delta:  %+d\n", res.Goroutines)
		fmt.Println("========================================")
	}
}

func doRequest(client *http.Client, cfg config, method, path string, body []byte) error {
	req, err := http.NewRequest(method, cfg.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if cfg.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.authToken)
	}
	if method == http.MethodPut && body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

func mustReq(cfg config, method, path string, body io.Reader) *http.Request {
	req, _ := http.NewRequest(method, cfg.endpoint+path, body)
	if cfg.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.authToken)
	}
	return req
}

func pctl(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*p)]
}

func numGoroutines() int {
	return runtime.NumGoroutine()
}
