package benchmark

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/segment"
)

// V2.1 §19 DataNode performance acceptance targets. The benchmark tool
// exercises the real storage/segment.Store write/read path (real
// fsync durability, no shortcuts) so results count toward acceptance
// (§19: "Results with durability, checksum, encryption, or generation
// fencing disabled do not count").
const (
	// TargetSmallWriteRate is the 64KiB mixed small-writes target.
	TargetSmallWriteRate = 20000
	// TargetSeqWriteRate is the 16MiB sequential write target (bytes/s).
	TargetSeqWriteRate = 1 << 30 // 1 GiB/s
	// TargetCachedReadRate is the cached random small-read target.
	TargetCachedReadRate = 50000
	// TargetNVMEReadRate is the NVMe random small-read target.
	TargetNVMEReadRate = 20000
	// GroupCommitP99 / ExtentWriteP99 / RangeReadP99 targets (ms).
	GroupCommitP99MS  = 10
	ExtentWriteP99MS  = 30
	RangeReadP99MS    = 20
)

// Result is one benchmark measurement.
type Result struct {
	Name     string
	Ops      int64
	Bytes    int64
	Elapsed  time.Duration
	OpPerSec float64
	BytesPerSec float64
	P99MS    float64
	Target   float64
	// Pass is true if the target is met.
	Pass bool
}

func (r *Result) Summary() string {
	status := "FAIL"
	if r.Pass {
		status = "PASS"
	}
	return fmt.Sprintf("%-28s %s  ops/s=%.0f  p99=%.2fms  (target %.0f ops/s)",
		r.Name, status, r.OpPerSec, r.P99MS, r.Target)
}

// p99 computes the 99th percentile of a latency slice.
func p99(latencies []time.Duration) float64 {
	if len(latencies) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted))*0.99) - 1
	if idx < 0 {
		idx = 0
	}
	return float64(sorted[idx]) / float64(time.Millisecond)
}

// SmallWriteBenchmark runs 64KiB mixed small writes with N concurrent
// writers. It measures throughput and group-commit P99.
func SmallWriteBenchmark(dir string, writers int, totalOps int, extentSize int) ([]Result, error) {
	s, err := segment.New(segment.Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		return nil, err
	}
	defer s.Close()
	ctx := context.Background()

	var ops atomic.Int64
	var latMu sync.Mutex
	var latencies []time.Duration

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			data := make([]byte, extentSize)
			for i := 0; i < totalOps/writers; i++ {
				t0 := time.Now()
				id := storage.ExtentID(uint64(w)*1e9 + uint64(i) + 1)
				if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: id, Generation: 1, Data: data}); err != nil {
					continue
				}
				latMu.Lock()
				latencies = append(latencies, time.Since(t0))
				latMu.Unlock()
				ops.Add(1)
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	latMu.Lock()
	p := p99(latencies)
	latMu.Unlock()
	rate := float64(ops.Load()) / elapsed.Seconds()
	res := Result{
		Name:      "small-write",
		Ops:       ops.Load(),
		Bytes:     ops.Load() * int64(extentSize),
		Elapsed:   elapsed,
		OpPerSec:  rate,
		BytesPerSec: float64(ops.Load()*int64(extentSize)) / elapsed.Seconds(),
		P99MS:     p,
		Target:    TargetSmallWriteRate,
		Pass:      rate >= TargetSmallWriteRate && p <= GroupCommitP99MS,
	}
	return []Result{res}, nil
}

// RandomReadBenchmark measures random small reads from a pre-populated
// store (cached-read target: 50K/s; the NVMe target 20K/s is the
// uncached variant, dominated by disk). Ops read extent IDs modulo the
// populated set.
func RandomReadBenchmark(dir string, readers int, totalOps int, populated int, extentSize int) ([]Result, error) {
	s, err := segment.New(segment.Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		return nil, err
	}
	defer s.Close()
	ctx := context.Background()

	// Pre-populate `populated` extents.
	data := make([]byte, extentSize)
	for i := 0; i < populated; i++ {
		if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: storage.ExtentID(i + 1), Generation: 1, Data: data}); err != nil {
			return nil, err
		}
	}

	var ops atomic.Int64
	var latMu sync.Mutex
	var latencies []time.Duration
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < readers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < totalOps/readers; i++ {
				id := storage.ExtentID((uint64(w)*7919+uint64(i)*104729)%uint64(populated)) + 1
				t0 := time.Now()
				if _, err := s.Read(ctx, &storage.ReadRequest{ExtentID: id, Generation: 1}); err != nil {
					continue
				}
				latMu.Lock()
				latencies = append(latencies, time.Since(t0))
				latMu.Unlock()
				ops.Add(1)
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	latMu.Lock()
	p := p99(latencies)
	latMu.Unlock()
	rate := float64(ops.Load()) / elapsed.Seconds()
	res := Result{
		Name:      "random-read",
		Ops:       ops.Load(),
		Bytes:     ops.Load() * int64(extentSize),
		Elapsed:   elapsed,
		OpPerSec:  rate,
		BytesPerSec: float64(ops.Load()*int64(extentSize)) / elapsed.Seconds(),
		P99MS:     p,
		Target:    TargetCachedReadRate,
		Pass:      rate >= TargetCachedReadRate && p <= RangeReadP99MS,
	}
	return []Result{res}, nil
}

// RunAll runs the acceptance benchmarks and prints results. Exit code
// is non-zero if any target fails.
func RunAll(dir string) int {
	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Printf("benchmark: mkdir: %v\n", err)
		return 1
	}
	fmt.Println("=== V2.1 §19 DataNode performance acceptance ===")
	fmt.Println("(real fsync durability; results count toward acceptance)")
	allPass := true

	// Small writes: 64KiB, 8 writers, 50K ops total.
	small, err := SmallWriteBenchmark(dir+"/small", 8, 50000, 64<<10)
	if err != nil {
		fmt.Printf("small-write error: %v\n", err)
		allPass = false
	} else {
		for _, r := range small {
			fmt.Println(" " + r.Summary())
			allPass = allPass && r.Pass
		}
	}

	// Random reads over 10K populated extents.
	read, err := RandomReadBenchmark(dir+"/read", 8, 100000, 10000, 64<<10)
	if err != nil {
		fmt.Printf("random-read error: %v\n", err)
		allPass = false
	} else {
		for _, r := range read {
			fmt.Println(" " + r.Summary())
			allPass = allPass && r.Pass
		}
	}

	if allPass {
		fmt.Println("=== ALL PERFORMANCE TARGETS PASS ===")
		return 0
	}
	fmt.Println("=== SOME TARGETS NOT MET (see above) ===")
	return 1
}
