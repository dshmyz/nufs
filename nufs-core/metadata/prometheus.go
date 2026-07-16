package metadata

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
)

// ============================================================
// Prometheus Metrics Exporter — /metrics endpoint
// ============================================================

// PrometheusHandler returns an HTTP handler that serves metrics in
// Prometheus text exposition format at /metrics.
func PrometheusHandler(m *Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m == nil {
			http.Error(w, "metrics not available", http.StatusServiceUnavailable)
			return
		}
		snap := m.Snapshot()

		sb := strings.Builder{}

		// Counters
		sb.WriteString("# HELP nufs_ops_total Total operations\n")
		sb.WriteString("# TYPE nufs_ops_total counter\n")
		sb.WriteString(fmt.Sprintf("nufs_ops_total %d\n", snap.OpsTotal))

		sb.WriteString("# HELP nufs_read_ops_total Total read operations\n")
		sb.WriteString("# TYPE nufs_read_ops_total counter\n")
		sb.WriteString(fmt.Sprintf("nufs_read_ops_total %d\n", snap.ReadOps))

		sb.WriteString("# HELP nufs_write_ops_total Total write operations\n")
		sb.WriteString("# TYPE nufs_write_ops_total counter\n")
		sb.WriteString(fmt.Sprintf("nufs_write_ops_total %d\n", snap.WriteOps))

		sb.WriteString("# HELP nufs_errors_total Total errors\n")
		sb.WriteString("# TYPE nufs_errors_total counter\n")
		sb.WriteString(fmt.Sprintf("nufs_errors_total %d\n", snap.ErrorsTotal))

		sb.WriteString("# HELP nufs_cache_hits_total Cache hits\n")
		sb.WriteString("# TYPE nufs_cache_hits_total counter\n")
		sb.WriteString(fmt.Sprintf("nufs_cache_hits_total %d\n", snap.CacheHits))

		sb.WriteString("# HELP nufs_cache_misses_total Cache misses\n")
		sb.WriteString("# TYPE nufs_cache_misses_total counter\n")
		sb.WriteString(fmt.Sprintf("nufs_cache_misses_total %d\n", snap.CacheMisses))

		// Gauges
		sb.WriteString("# HELP nufs_keys_total Total keys in metadata store\n")
		sb.WriteString("# TYPE nufs_keys_total gauge\n")
		sb.WriteString(fmt.Sprintf("nufs_keys_total %d\n", snap.KeysTotal))

		sb.WriteString("# HELP nufs_chunks_total Total chunks\n")
		sb.WriteString("# TYPE nufs_chunks_total gauge\n")
		sb.WriteString(fmt.Sprintf("nufs_chunks_total %d\n", snap.ChunksTotal))

		sb.WriteString("# HELP nufs_nodes_online Online nodes\n")
		sb.WriteString("# TYPE nufs_nodes_online gauge\n")
		sb.WriteString(fmt.Sprintf("nufs_nodes_online %d\n", snap.NodesOnline))

		sb.WriteString("# HELP nufs_nodes_total Total nodes\n")
		sb.WriteString("# TYPE nufs_nodes_total gauge\n")
		sb.WriteString(fmt.Sprintf("nufs_nodes_total %d\n", snap.NodesTotal))

		sb.WriteString("# HELP nufs_buckets_total Total buckets\n")
		sb.WriteString("# TYPE nufs_buckets_total gauge\n")
		sb.WriteString(fmt.Sprintf("nufs_buckets_total %d\n", snap.BucketsTotal))

		sb.WriteString("# HELP nufs_cache_size Cache size\n")
		sb.WriteString("# TYPE nufs_cache_size gauge\n")
		sb.WriteString(fmt.Sprintf("nufs_cache_size %d\n", snap.CacheSize))

		// Disk I/O
		sb.WriteString("# HELP nufs_disk_read_bytes_total Disk bytes read\n")
		sb.WriteString("# TYPE nufs_disk_read_bytes_total counter\n")
		sb.WriteString(fmt.Sprintf("nufs_disk_read_bytes_total %d\n", snap.DiskReadBytes))

		sb.WriteString("# HELP nufs_disk_write_bytes_total Disk bytes written\n")
		sb.WriteString("# TYPE nufs_disk_write_bytes_total counter\n")
		sb.WriteString(fmt.Sprintf("nufs_disk_write_bytes_total %d\n", snap.DiskWriteBytes))

		// WAL
		sb.WriteString("# HELP nufs_wal_bytes_written_total WAL bytes written\n")
		sb.WriteString("# TYPE nufs_wal_bytes_written_total counter\n")
		sb.WriteString(fmt.Sprintf("nufs_wal_bytes_written_total %d\n", snap.WALBytesWritten))

		sb.WriteString("# HELP nufs_wal_fsync_total WAL fsync count\n")
		sb.WriteString("# TYPE nufs_wal_fsync_total counter\n")
		sb.WriteString(fmt.Sprintf("nufs_wal_fsync_total %d\n", snap.WALFsyncCount))

		// GC
		sb.WriteString("# HELP nufs_gc_scanned_chunks GC scanned chunks\n")
		sb.WriteString("# TYPE nufs_gc_scanned_chunks counter\n")
		sb.WriteString(fmt.Sprintf("nufs_gc_scanned_chunks %d\n", snap.GCScannedChunks))

		sb.WriteString("# HELP nufs_gc_deleted_chunks GC deleted chunks\n")
		sb.WriteString("# TYPE nufs_gc_deleted_chunks counter\n")
		sb.WriteString(fmt.Sprintf("nufs_gc_deleted_chunks %d\n", snap.GCDeletedChunks))

		sb.WriteString("# HELP nufs_gc_orphan_chunks Orphan chunks (no valid owner inode)\n")
		sb.WriteString("# TYPE nufs_gc_orphan_chunks gauge\n")
		sb.WriteString(fmt.Sprintf("nufs_gc_orphan_chunks %d\n", snap.GCOrphanChunks))

		// Raft
		sb.WriteString("# HELP nufs_raft_state Raft state (0=follower, 1=candidate, 2=leader)\n")
		sb.WriteString("# TYPE nufs_raft_state gauge\n")
		sb.WriteString(fmt.Sprintf("nufs_raft_state %d\n", snap.RaftState))

		sb.WriteString("# HELP nufs_raft_term Raft term\n")
		sb.WriteString("# TYPE nufs_raft_term gauge\n")
		sb.WriteString(fmt.Sprintf("nufs_raft_term %d\n", snap.RaftTerm))

		sb.WriteString("# HELP nufs_raft_log_index Raft log index\n")
		sb.WriteString("# TYPE nufs_raft_log_index gauge\n")
		sb.WriteString(fmt.Sprintf("nufs_raft_log_index %d\n", snap.RaftLogIndex))

		// Latency histograms (as summary percentiles)
		sb.WriteString("# HELP nufs_read_latency_us Read latency in microseconds\n")
		sb.WriteString("# TYPE nufs_read_latency_us summary\n")
		sb.WriteString(fmt.Sprintf("nufs_read_latency_us{quantile=\"0.5\"} %d\n", snap.ReadP50us))
		sb.WriteString(fmt.Sprintf("nufs_read_latency_us{quantile=\"0.99\"} %d\n", snap.ReadP99us))

		sb.WriteString("# HELP nufs_write_latency_us Write latency in microseconds\n")
		sb.WriteString("# TYPE nufs_write_latency_us summary\n")
		sb.WriteString(fmt.Sprintf("nufs_write_latency_us{quantile=\"0.5\"} %d\n", snap.WriteP50us))
		sb.WriteString(fmt.Sprintf("nufs_write_latency_us{quantile=\"0.99\"} %d\n", snap.WriteP99us))

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		io.WriteString(w, sb.String())
	})
}

// ============================================================
// HealthCheck HTTP Handler — /healthz
// ============================================================

// HealthHandler returns an HTTP handler for health checks.
func HealthHandler(hc *HealthChecker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hc == nil {
			http.Error(w, "health checker not available", http.StatusServiceUnavailable)
			return
		}
		status := hc.Check()

		w.Header().Set("Content-Type", "application/json")
		if status.Status != "healthy" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		fmt.Fprintf(w, `{"status":"%s","version":"%s","uptime":"%s"}`,
			status.Status, status.Version, status.Uptime)
	})
}

// ============================================================
// Atomic Counter/Summary helpers (used by health.go)
// ============================================================

// atomicCounter is a simple atomic counter for metrics.
type atomicCounter struct {
	value atomic.Int64
}

func (c *atomicCounter) Add(n int64) { c.value.Add(n) }
func (c *atomicCounter) Load() int64 { return c.value.Load() }

// atomicGauge is a simple atomic gauge for metrics.
type atomicGauge struct {
	value atomic.Int64
}

func (g *atomicGauge) Set(n int64) { g.value.Store(n) }
func (g *atomicGauge) Load() int64 { return g.value.Load() }
