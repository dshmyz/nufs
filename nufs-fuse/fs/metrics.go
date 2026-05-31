// Copyright (c) 2021 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package minfs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// --- Latency Histogram (lock-free for hot path) ---

// histogram tracks latency distribution with fixed exponential buckets.
// Each bucket counts observations <= its upper bound.
type histogram struct {
	mu      sync.Mutex
	count   uint64
	sum     float64 // total seconds
	buckets []histBucket
}

type histBucket struct {
	le    float64 // upper bound in seconds
	count uint64
}

// defaultLatencyBuckets covers 1ms to 60s, suitable for S3/FUSE operations.
var defaultLatencyBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
}

func newHistogram(buckets []float64) *histogram {
	h := &histogram{buckets: make([]histBucket, len(buckets))}
	for i, b := range buckets {
		h.buckets[i].le = b
	}
	return h
}

func (h *histogram) observe(seconds float64) {
	h.mu.Lock()
	h.count++
	h.sum += seconds
	for i := range h.buckets {
		if seconds <= h.buckets[i].le {
			h.buckets[i].count++
		}
	}
	h.mu.Unlock()
}

type histSnapshot struct {
	count   uint64
	sum     float64
	buckets []histBucket
}

func (h *histogram) snapshot() histSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := histSnapshot{count: h.count, sum: h.sum, buckets: make([]histBucket, len(h.buckets))}
	copy(s.buckets, h.buckets)
	return s
}

// --- Global Metrics ---

// Metrics holds all runtime counters for MinFS operations.
// All fields are accessed via sync/atomic and are safe for concurrent use.
type Metrics struct {
	// FUSE operations
	OpsOpen    uint64
	OpsRead    uint64
	OpsWrite   uint64
	OpsRelease uint64
	OpsLookup  uint64
	OpsReadDir uint64
	OpsMkdir   uint64
	OpsRemove  uint64
	OpsRename  uint64
	OpsCreate  uint64
	OpsFlush   uint64

	// S3 backend operations
	S3Get    uint64
	S3Put    uint64
	S3Copy   uint64
	S3Remove uint64
	S3List   uint64

	// Errors
	S3Errors  uint64
	S3Retries uint64

	// Active state
	ActiveHandles uint64

	// Latency histograms
	HistS3Get  *histogram
	HistS3Put  *histogram
	HistS3List *histogram
	HistRead   *histogram
	HistWrite  *histogram

	// Circuit breaker state (updated by breaker.State())
	CircuitBreakerState string

	// Uptime
	startTime time.Time
}

// global metrics instance, initialised once.
var globalMetrics = &Metrics{
	startTime:  time.Now(),
	HistS3Get:  newHistogram(defaultLatencyBuckets),
	HistS3Put:  newHistogram(defaultLatencyBuckets),
	HistS3List: newHistogram(defaultLatencyBuckets),
	HistRead:   newHistogram(defaultLatencyBuckets),
	HistWrite:  newHistogram(defaultLatencyBuckets),
}

// --- Increment helpers (all use atomic) ---

func metricsIncOpen()    { atomic.AddUint64(&globalMetrics.OpsOpen, 1) }
func metricsIncRead()    { atomic.AddUint64(&globalMetrics.OpsRead, 1) }
func metricsIncWrite()   { atomic.AddUint64(&globalMetrics.OpsWrite, 1) }
func metricsIncRelease() { atomic.AddUint64(&globalMetrics.OpsRelease, 1) }
func metricsIncLookup()  { atomic.AddUint64(&globalMetrics.OpsLookup, 1) }
func metricsIncReadDir() { atomic.AddUint64(&globalMetrics.OpsReadDir, 1) }
func metricsIncMkdir()   { atomic.AddUint64(&globalMetrics.OpsMkdir, 1) }
func metricsIncRemove()  { atomic.AddUint64(&globalMetrics.OpsRemove, 1) }
func metricsIncRename()  { atomic.AddUint64(&globalMetrics.OpsRename, 1) }
func metricsIncCreate()  { atomic.AddUint64(&globalMetrics.OpsCreate, 1) }
func metricsIncFlush()   { atomic.AddUint64(&globalMetrics.OpsFlush, 1) }

func metricsIncS3Get()    { atomic.AddUint64(&globalMetrics.S3Get, 1) }
func metricsIncS3Put()    { atomic.AddUint64(&globalMetrics.S3Put, 1) }
func metricsIncS3Copy()   { atomic.AddUint64(&globalMetrics.S3Copy, 1) }
func metricsIncS3Remove() { atomic.AddUint64(&globalMetrics.S3Remove, 1) }
func metricsIncS3List()   { atomic.AddUint64(&globalMetrics.S3List, 1) }
func metricsIncS3Error()  { atomic.AddUint64(&globalMetrics.S3Errors, 1) }
func metricsIncS3Retry()  { atomic.AddUint64(&globalMetrics.S3Retries, 1) }

func metricsIncActiveHandles() { atomic.AddUint64(&globalMetrics.ActiveHandles, 1) }
func metricsDecActiveHandles() { atomic.AddUint64(&globalMetrics.ActiveHandles, ^uint64(0)) }

// --- Latency observation helpers ---

func metricsObserveS3Get(seconds float64)  { globalMetrics.HistS3Get.observe(seconds) }
func metricsObserveS3Put(seconds float64)  { globalMetrics.HistS3Put.observe(seconds) }
func metricsObserveS3List(seconds float64) { globalMetrics.HistS3List.observe(seconds) }
func metricsObserveRead(seconds float64)   { globalMetrics.HistRead.observe(seconds) }
func metricsObserveWrite(seconds float64)  { globalMetrics.HistWrite.observe(seconds) }

// Snapshot returns a JSON-serialisable snapshot of all current metrics.
func (m *Metrics) Snapshot() map[string]interface{} {
	return map[string]interface{}{
		"uptime_seconds":  time.Since(m.startTime).Seconds(),
		"active_handles":  atomic.LoadUint64(&m.ActiveHandles),
		"circuit_breaker": m.CircuitBreakerState,
		"ops": map[string]uint64{
			"open":    atomic.LoadUint64(&m.OpsOpen),
			"read":    atomic.LoadUint64(&m.OpsRead),
			"write":   atomic.LoadUint64(&m.OpsWrite),
			"release": atomic.LoadUint64(&m.OpsRelease),
			"lookup":  atomic.LoadUint64(&m.OpsLookup),
			"readdir": atomic.LoadUint64(&m.OpsReadDir),
			"mkdir":   atomic.LoadUint64(&m.OpsMkdir),
			"remove":  atomic.LoadUint64(&m.OpsRemove),
			"rename":  atomic.LoadUint64(&m.OpsRename),
			"create":  atomic.LoadUint64(&m.OpsCreate),
			"flush":   atomic.LoadUint64(&m.OpsFlush),
		},
		"s3": map[string]uint64{
			"get":     atomic.LoadUint64(&m.S3Get),
			"put":     atomic.LoadUint64(&m.S3Put),
			"copy":    atomic.LoadUint64(&m.S3Copy),
			"remove":  atomic.LoadUint64(&m.S3Remove),
			"list":    atomic.LoadUint64(&m.S3List),
			"errors":  atomic.LoadUint64(&m.S3Errors),
			"retries": atomic.LoadUint64(&m.S3Retries),
		},
	}
}

// --- Prometheus text exposition format ---

func writeHistogramProm(sb *strings.Builder, name, help string, snap histSnapshot) {
	fmt.Fprintf(sb, "# HELP %s %s\n", name, help)
	fmt.Fprintf(sb, "# TYPE %s histogram\n", name)
	for _, b := range snap.buckets {
		fmt.Fprintf(sb, "%s_bucket{le=\"%g\"} %d\n", name, b.le, b.count)
	}
	fmt.Fprintf(sb, "%s_bucket{le=\"+Inf\"} %d\n", name, snap.count)
	fmt.Fprintf(sb, "%s_sum %g\n", name, snap.sum)
	fmt.Fprintf(sb, "%s_count %d\n", name, snap.count)
}

func prometheusHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var sb strings.Builder

		// Uptime
		fmt.Fprintf(&sb, "# HELP minfs_uptime_seconds Time since MinFS started.\n")
		fmt.Fprintf(&sb, "# TYPE minfs_uptime_seconds gauge\n")
		fmt.Fprintf(&sb, "minfs_uptime_seconds %g\n\n", time.Since(globalMetrics.startTime).Seconds())

		// Active handles
		fmt.Fprintf(&sb, "# HELP minfs_active_handles Number of currently open file handles.\n")
		fmt.Fprintf(&sb, "# TYPE minfs_active_handles gauge\n")
		fmt.Fprintf(&sb, "minfs_active_handles %d\n\n", atomic.LoadUint64(&globalMetrics.ActiveHandles))

		// FUSE operation counters
		fmt.Fprintf(&sb, "# HELP minfs_ops_total Total FUSE operations by type.\n")
		fmt.Fprintf(&sb, "# TYPE minfs_ops_total counter\n")
		ops := []struct {
			name string
			val  uint64
		}{
			{"open", atomic.LoadUint64(&globalMetrics.OpsOpen)},
			{"read", atomic.LoadUint64(&globalMetrics.OpsRead)},
			{"write", atomic.LoadUint64(&globalMetrics.OpsWrite)},
			{"release", atomic.LoadUint64(&globalMetrics.OpsRelease)},
			{"lookup", atomic.LoadUint64(&globalMetrics.OpsLookup)},
			{"readdir", atomic.LoadUint64(&globalMetrics.OpsReadDir)},
			{"mkdir", atomic.LoadUint64(&globalMetrics.OpsMkdir)},
			{"remove", atomic.LoadUint64(&globalMetrics.OpsRemove)},
			{"rename", atomic.LoadUint64(&globalMetrics.OpsRename)},
			{"create", atomic.LoadUint64(&globalMetrics.OpsCreate)},
			{"flush", atomic.LoadUint64(&globalMetrics.OpsFlush)},
		}
		for _, o := range ops {
			fmt.Fprintf(&sb, "minfs_ops_total{op=\"%s\"} %d\n", o.name, o.val)
		}
		sb.WriteString("\n")

		// S3 operation counters
		fmt.Fprintf(&sb, "# HELP minfs_s3_ops_total Total S3 backend operations by type.\n")
		fmt.Fprintf(&sb, "# TYPE minfs_s3_ops_total counter\n")
		s3ops := []struct {
			name string
			val  uint64
		}{
			{"get", atomic.LoadUint64(&globalMetrics.S3Get)},
			{"put", atomic.LoadUint64(&globalMetrics.S3Put)},
			{"copy", atomic.LoadUint64(&globalMetrics.S3Copy)},
			{"remove", atomic.LoadUint64(&globalMetrics.S3Remove)},
			{"list", atomic.LoadUint64(&globalMetrics.S3List)},
			{"errors", atomic.LoadUint64(&globalMetrics.S3Errors)},
			{"retries", atomic.LoadUint64(&globalMetrics.S3Retries)},
		}
		for _, o := range s3ops {
			fmt.Fprintf(&sb, "minfs_s3_ops_total{op=\"%s\"} %d\n", o.name, o.val)
		}
		sb.WriteString("\n")

		// Latency histograms
		writeHistogramProm(&sb, "minfs_s3_get_duration_seconds", "S3 GetObject latency.", globalMetrics.HistS3Get.snapshot())
		sb.WriteString("\n")
		writeHistogramProm(&sb, "minfs_s3_put_duration_seconds", "S3 PutObject latency.", globalMetrics.HistS3Put.snapshot())
		sb.WriteString("\n")
		writeHistogramProm(&sb, "minfs_s3_list_duration_seconds", "S3 ListObjects latency.", globalMetrics.HistS3List.snapshot())
		sb.WriteString("\n")
		writeHistogramProm(&sb, "minfs_read_duration_seconds", "FUSE Read latency.", globalMetrics.HistRead.snapshot())
		sb.WriteString("\n")
		writeHistogramProm(&sb, "minfs_write_duration_seconds", "FUSE Write latency.", globalMetrics.HistWrite.snapshot())
		sb.WriteString("\n")

		// Circuit breaker state
		fmt.Fprintf(&sb, "# HELP minfs_circuit_breaker_state S3 circuit breaker state (0=closed, 1=open, 2=half-open).\n")
		fmt.Fprintf(&sb, "# TYPE minfs_circuit_breaker_state gauge\n")
		stateVal := 0.0
		switch globalMetrics.CircuitBreakerState {
		case "open":
			stateVal = 1.0
		case "half-open":
			stateVal = 2.0
		}
		fmt.Fprintf(&sb, "minfs_circuit_breaker_state %g\n", stateVal)

		w.Write([]byte(sb.String()))
	}
}

// metricsHandler returns an http.HandlerFunc that serves the current metrics
// snapshot as JSON. Suitable for ad-hoc debugging.
func metricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		snap := globalMetrics.Snapshot()
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(snap); err != nil {
			http.Error(w, fmt.Sprintf("metrics encode error: %v", err), http.StatusInternalServerError)
		}
	}
}

// startMetricsServer launches a non-blocking HTTP server on the given address
// (e.g. ":9900") exposing /metrics (Prometheus), /metrics/json, and /healthz endpoints.
// Returns the server so it can be shut down gracefully.
func startMetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", prometheusHandler())
	mux.HandleFunc("/metrics/json", metricsHandler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			_ = err
		}
	}()
	return srv
}
