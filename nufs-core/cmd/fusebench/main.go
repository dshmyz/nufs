// Command fusebench runs file I/O benchmarks against a mounted NUFS filesystem.
// It measures sequential/random read/write throughput and metadata (stat, readdir)
// latency on the live mount point.
//
// Usage:
//
//	fusebench -dir /mnt/nufs/bench -duration 60s -workers 8 -block-size 65536
package main

import (
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	mrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	dir       string
	duration  time.Duration
	workers   int
	blockSize int
	fileSize  int
	numFiles  int
	jsonOut   bool
	randSeed  int64
	directIO  bool
}

type result struct {
	Duration      string  `json:"duration"`
	Workers       int     `json:"workers"`
	BlockSize     int     `json:"block_size_bytes"`
	FileSize      int     `json:"file_size_bytes"`
	NumFiles      int     `json:"num_files"`
	SeqWriteMBs   float64 `json:"seq_write_mbps"`
	SeqWriteIOPS  float64 `json:"seq_write_iops"`
	SeqReadMBs    float64 `json:"seq_read_mbps"`
	SeqReadIOPS   float64 `json:"seq_read_iops"`
	RandWriteMBs  float64 `json:"rand_write_mbps"`
	RandWriteIOPS float64 `json:"rand_write_iops"`
	RandReadMBs   float64 `json:"rand_read_mbps"`
	RandReadIOPS  float64 `json:"rand_read_iops"`
	StatIOPS      float64 `json:"stat_iops"`
	ReaddirIOPS   float64 `json:"readdir_iops"`
	StatLatP50    string  `json:"stat_latency_p50"`
	StatLatP99    string  `json:"stat_latency_p99"`
	ReaddirP50    string  `json:"readdir_latency_p50"`
	ReaddirP99    string  `json:"readdir_latency_p99"`
	WriteErrors   int64   `json:"write_errors"`
	ReadErrors    int64   `json:"read_errors"`
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.dir, "dir", "/mnt/nufs/bench", "mount point or test directory")
	flag.DurationVar(&cfg.duration, "duration", 10*time.Second, "duration per phase")
	flag.IntVar(&cfg.workers, "workers", 4, "concurrent workers per phase")
	flag.IntVar(&cfg.blockSize, "block-size", 65536, "I/O block size in bytes")
	flag.IntVar(&cfg.fileSize, "file-size", 4*1024*1024, "file size in bytes per worker")
	flag.IntVar(&cfg.numFiles, "num-files", 100, "number of files for metadata tests")
	flag.BoolVar(&cfg.jsonOut, "json", false, "output results as JSON")
	flag.BoolVar(&cfg.directIO, "direct-io", false, "use O_DIRECT (bypass page cache, block-size must be 512-aligned)")
	flag.Parse()

	if cfg.directIO && cfg.blockSize%512 != 0 {
		fmt.Fprintf(os.Stderr, "error: -direct-io requires block-size to be 512-byte aligned (got %d)\n", cfg.blockSize)
		os.Exit(1)
	}

	benchDir := filepath.Join(cfg.dir, "_fusebench")
	os.MkdirAll(benchDir, 0755)
	defer os.RemoveAll(benchDir)

	var res result
	res.Duration = cfg.duration.String()
	res.Workers = cfg.workers
	res.BlockSize = cfg.blockSize
	res.FileSize = cfg.fileSize
	res.NumFiles = cfg.numFiles

	// Phase 1: Sequential write
	fmt.Fprintf(os.Stderr, "phase 1: sequential write (%d workers x %d bytes)...\n", cfg.workers, cfg.fileSize)
	res.SeqWriteMBs, res.SeqWriteIOPS, res.WriteErrors = benchSeqWrite(cfg, benchDir)

	// Phase 2: Sequential read
	fmt.Fprintf(os.Stderr, "phase 2: sequential read...\n")
	res.SeqReadMBs, res.SeqReadIOPS, res.ReadErrors = benchSeqRead(cfg, benchDir)

	// Phase 3: Random write
	fmt.Fprintf(os.Stderr, "phase 3: random write...\n")
	rwMBs, rwIOPS, rwErrs := benchRandWrite(cfg, benchDir)
	res.RandWriteMBs, res.RandWriteIOPS = rwMBs, rwIOPS
	res.WriteErrors += rwErrs

	// Phase 4: Random read
	fmt.Fprintf(os.Stderr, "phase 4: random read...\n")
	rrMBs, rrIOPS, rrErrs := benchRandRead(cfg, benchDir)
	res.RandReadMBs, res.RandReadIOPS = rrMBs, rrIOPS
	res.ReadErrors += rrErrs

	// Phase 5: Metadata — stat
	fmt.Fprintf(os.Stderr, "phase 5: metadata stat (%d files)...\n", cfg.numFiles)
	res.StatIOPS, res.StatLatP50, res.StatLatP99 = benchStat(cfg, benchDir)

	// Phase 6: Metadata — readdir
	fmt.Fprintf(os.Stderr, "phase 6: metadata readdir...\n")
	res.ReaddirIOPS, res.ReaddirP50, res.ReaddirP99 = benchReaddir(cfg, benchDir)

	if cfg.jsonOut {
		json.NewEncoder(os.Stdout).Encode(res)
	} else {
		fmt.Println("========================================")
		fmt.Println("  FUSE MOUNT BENCHMARK RESULTS")
		fmt.Println("========================================")
		fmt.Printf("Duration/phase:   %s\n", res.Duration)
		fmt.Printf("Workers:          %d\n", res.Workers)
		fmt.Printf("Block size:       %d bytes\n", res.BlockSize)
		fmt.Printf("File size:        %d bytes\n", res.FileSize)
		fmt.Println()
		fmt.Printf("Seq write:        %.2f MB/s  (%.0f IOPS)\n", res.SeqWriteMBs, res.SeqWriteIOPS)
		fmt.Printf("Seq read:         %.2f MB/s  (%.0f IOPS)\n", res.SeqReadMBs, res.SeqReadIOPS)
		fmt.Printf("Rand write:       %.2f MB/s  (%.0f IOPS)\n", res.RandWriteMBs, res.RandWriteIOPS)
		fmt.Printf("Rand read:        %.2f MB/s  (%.0f IOPS)\n", res.RandReadMBs, res.RandReadIOPS)
		fmt.Println()
		fmt.Printf("Stat IOPS:        %.0f\n", res.StatIOPS)
		fmt.Printf("  P50:            %s\n", res.StatLatP50)
		fmt.Printf("  P99:            %s\n", res.StatLatP99)
		fmt.Printf("Readdir IOPS:     %.0f\n", res.ReaddirIOPS)
		fmt.Printf("  P50:            %s\n", res.ReaddirP50)
		fmt.Printf("  P99:            %s\n", res.ReaddirP99)
		fmt.Println()
		fmt.Printf("Write errors:     %d\n", res.WriteErrors)
		fmt.Printf("Read errors:      %d\n", res.ReadErrors)
		fmt.Println("========================================")
	}
}

// ============================================================
// Sequential write: each worker writes its file sequentially
// ============================================================

func benchSeqWrite(cfg config, dir string) (mbps float64, iops float64, errs int64) {
	// For O_DIRECT, buffer must be aligned; use syscall.Open for direct control.
	block := make([]byte, cfg.blockSize)
	rand.Read(block)

	var totalBytes, totalOps atomic.Int64
	var errCount atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			path := filepath.Join(dir, fmt.Sprintf("seqw-%d", wid))
			var f *os.File
			if cfg.directIO {
				fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_WRONLY|syscall.O_TRUNC, 0644)
				if err != nil {
					errCount.Add(1)
					return
				}
				f = os.NewFile(uintptr(fd), path)
			} else {
				var err error
				f, err = os.Create(path)
				if err != nil {
					errCount.Add(1)
					return
				}
			}
			defer f.Close()
			written := 0
			for written < cfg.fileSize {
				n, err := f.Write(block)
				totalBytes.Add(int64(n))
				totalOps.Add(1)
				written += n
				if err != nil {
					errCount.Add(1)
					return
				}
			}
			f.Sync()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	return throughputMBs(totalBytes.Load(), elapsed), float64(totalOps.Load()) / elapsed.Seconds(), errCount.Load()
}

// ============================================================
// Sequential read: each worker reads its file sequentially
// ============================================================

func benchSeqRead(cfg config, dir string) (mbps float64, iops float64, errs int64) {
	var totalBytes, totalOps atomic.Int64
	var errCount atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			path := filepath.Join(dir, fmt.Sprintf("seqw-%d", wid))
			var f *os.File
			if cfg.directIO {
				fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
				if err != nil {
					errCount.Add(1)
					return
				}
				f = os.NewFile(uintptr(fd), path)
			} else {
				var err error
				f, err = os.Open(path)
				if err != nil {
					errCount.Add(1)
					return
				}
			}
			defer f.Close()
			buf := make([]byte, cfg.blockSize)
			for {
				n, err := f.Read(buf)
				totalBytes.Add(int64(n))
				if n > 0 {
					totalOps.Add(1)
				}
				if err != nil {
					break
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	return throughputMBs(totalBytes.Load(), elapsed), float64(totalOps.Load()) / elapsed.Seconds(), errCount.Load()
}

// ============================================================
// Random write: each worker writes random offsets
// ============================================================

func benchRandWrite(cfg config, dir string) (mbps float64, iops float64, errs int64) {
	block := make([]byte, cfg.blockSize)
	rand.Read(block)

	// Create files first
	for w := 0; w < cfg.workers; w++ {
		p := filepath.Join(dir, fmt.Sprintf("randw-%d", w))
		f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			f.Truncate(int64(cfg.fileSize))
			f.Close()
		}
	}

	var totalBytes, totalOps atomic.Int64
	var errCount atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			rng := mrand.New(mrand.NewSource(int64(wid*1000 + 1)))
			path := filepath.Join(dir, fmt.Sprintf("randw-%d", wid))
			var f *os.File
			if cfg.directIO {
				fd, err := syscall.Open(path, syscall.O_WRONLY, 0)
				if err != nil {
					errCount.Add(1)
					return
				}
				f = os.NewFile(uintptr(fd), path)
			} else {
				var err error
				f, err = os.OpenFile(path, os.O_WRONLY, 0644)
				if err != nil {
					errCount.Add(1)
					return
				}
			}
			defer f.Close()
			writes := 0
			for time.Since(start) < cfg.duration {
				maxOff := int64(cfg.fileSize - cfg.blockSize)
				if maxOff <= 0 {
					maxOff = 0
				}
				off := int64(rng.Intn(int(maxOff/int64(cfg.blockSize))+1)) * int64(cfg.blockSize)
				if off > maxOff {
					off = maxOff
				}
				_, err := f.WriteAt(block, off)
				totalBytes.Add(int64(cfg.blockSize))
				totalOps.Add(1)
				writes++
				if err != nil {
					errCount.Add(1)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	return throughputMBs(totalBytes.Load(), elapsed), float64(totalOps.Load()) / elapsed.Seconds(), errCount.Load()
}

// ============================================================
// Random read: each worker reads random offsets
// ============================================================

func benchRandRead(cfg config, dir string) (mbps float64, iops float64, errs int64) {
	var totalBytes, totalOps atomic.Int64
	var errCount atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			rng := mrand.New(mrand.NewSource(int64(wid*2000 + 1)))
			path := filepath.Join(dir, fmt.Sprintf("seqw-%d", wid))
			var f *os.File
			if cfg.directIO {
				fd, err := syscall.Open(path, syscall.O_RDONLY, 0)
				if err != nil {
					errCount.Add(1)
					return
				}
				f = os.NewFile(uintptr(fd), path)
			} else {
				var err error
				f, err = os.Open(path)
				if err != nil {
					errCount.Add(1)
					return
				}
			}
			defer f.Close()
			buf := make([]byte, cfg.blockSize)
			for time.Since(start) < cfg.duration {
				maxOff := int64(cfg.fileSize - cfg.blockSize)
				if maxOff <= 0 {
					maxOff = 0
				}
				off := int64(rng.Intn(int(maxOff/int64(cfg.blockSize))+1)) * int64(cfg.blockSize)
				if off > maxOff {
					off = maxOff
				}
				_, err := f.ReadAt(buf, off)
				totalBytes.Add(int64(cfg.blockSize))
				totalOps.Add(1)
				if err != nil {
					errCount.Add(1)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	return throughputMBs(totalBytes.Load(), elapsed), float64(totalOps.Load()) / elapsed.Seconds(), errCount.Load()
}

// ============================================================
// Metadata: stat on many files
// ============================================================

func benchStat(cfg config, dir string) (iops float64, p50 string, p99 string) {
	// Create test files
	subdir := filepath.Join(dir, "_meta")
	os.MkdirAll(subdir, 0755)
	for i := 0; i < cfg.numFiles; i++ {
		p := filepath.Join(subdir, fmt.Sprintf("f%06d", i))
		os.WriteFile(p, []byte("x"), 0644)
	}

	var totalOps atomic.Int64
	var lats []time.Duration
	var latMu sync.Mutex
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			rng := mrand.New(mrand.NewSource(int64(wid*3000 + 1)))
			for time.Since(start) < cfg.duration {
				name := fmt.Sprintf("f%06d", rng.Intn(cfg.numFiles))
				t := time.Now()
				_, err := os.Stat(filepath.Join(subdir, name))
				lat := time.Since(t)
				totalOps.Add(1)
				if err == nil {
					latMu.Lock()
					lats = append(lats, lat)
					latMu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	return float64(totalOps.Load()) / elapsed.Seconds(), pctlStr(lats, 0.50), pctlStr(lats, 0.99)
}

// ============================================================
// Metadata: readdir on the test directory
// ============================================================

func benchReaddir(cfg config, dir string) (iops float64, p50 string, p99 string) {
	subdir := filepath.Join(dir, "_meta")
	var totalOps atomic.Int64
	var lats []time.Duration
	var latMu sync.Mutex
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Since(start) < cfg.duration {
				t := time.Now()
				_, err := os.ReadDir(subdir)
				lat := time.Since(t)
				totalOps.Add(1)
				if err == nil {
					latMu.Lock()
					lats = append(lats, lat)
					latMu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	return float64(totalOps.Load()) / elapsed.Seconds(), pctlStr(lats, 0.50), pctlStr(lats, 0.99)
}

// ============================================================
// Helpers
// ============================================================

func throughputMBs(bytes int64, elapsed time.Duration) float64 {
	return float64(bytes) / elapsed.Seconds() / 1024 / 1024
}

func pctl(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*p)]
}

func pctlStr(sorted []time.Duration, p float64) string {
	return pctl(sorted, p).String()
}
