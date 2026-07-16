package metadata

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"net/http"
	"sync/atomic"
	"time"
)

// ============================================================
// Metrics — Production observability
// ============================================================

// Metrics holds all operational metrics for the metadata service.
type Metrics struct {
	// Operation counters
	OpsTotal    atomic.Int64
	ReadOps     atomic.Int64
	WriteOps    atomic.Int64
	ErrorsTotal atomic.Int64

	// Latency (microseconds, ring buffer for p99)
	ReadLatency  *LatencyHistogram
	WriteLatency *LatencyHistogram

	// Per-operation-type latency histograms for granular observability
	CreateFileLatency *LatencyHistogram
	MkDirLatency      *LatencyHistogram
	LookupLatency     *LatencyHistogram
	ReadDirLatency    *LatencyHistogram
	GetInodeLatency   *LatencyHistogram
	AllocateChunkLat  *LatencyHistogram

	// Cache
	CacheHits   atomic.Int64
	CacheMisses atomic.Int64
	CacheSize   atomic.Int64

	// Storage
	KeysTotal    atomic.Int64
	ChunksTotal  atomic.Int64
	NodesOnline  atomic.Int64
	NodesTotal   atomic.Int64
	BucketsTotal atomic.Int64

	// Disk I/O (datanode side)
	DiskReadBytes  atomic.Int64
	DiskWriteBytes atomic.Int64
	DiskReadOps    atomic.Int64
	DiskWriteOps   atomic.Int64

	// WAL
	WALBytesWritten atomic.Int64
	WALFsyncCount   atomic.Int64
	WALLatency      *LatencyHistogram

	// GC
	GCScannedChunks  atomic.Int64
	GCDeletedChunks  atomic.Int64
	GCScannedBytes   atomic.Int64
	GCFreedBytes     atomic.Int64
	GCLastDurationMs atomic.Int64
	GCOrphanChunks   atomic.Int64 // Orphan chunks detected (no valid owner inode)

	// Repair / Rebalance
	RepairTasksQueued   atomic.Int64
	RepairTasksCompleted atomic.Int64
	RebalanceBytesMoved atomic.Int64

	// Raft (populated when Raft is active)
	RaftState     atomic.Int32 // 0=follower, 1=candidate, 2=leader
	RaftTerm      atomic.Int64
	RaftLogIndex  atomic.Int64
	SnapshotsDone atomic.Int64

	// Uptime
	startTime time.Time
}

// NewMetrics creates a new metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{
		ReadLatency:       NewLatencyHistogram(),
		WriteLatency:      NewLatencyHistogram(),
		CreateFileLatency: NewLatencyHistogram(),
		MkDirLatency:      NewLatencyHistogram(),
		LookupLatency:     NewLatencyHistogram(),
		ReadDirLatency:    NewLatencyHistogram(),
		GetInodeLatency:   NewLatencyHistogram(),
		AllocateChunkLat:  NewLatencyHistogram(),
		WALLatency:        NewLatencyHistogram(),
		startTime:         time.Now(),
	}
}

// RecordRead records a read operation.
func (m *Metrics) RecordRead(latency time.Duration) {
	m.OpsTotal.Add(1)
	m.ReadOps.Add(1)
	m.ReadLatency.Observe(latency)
}

// RecordCacheHit increments the cache hit counter.
func (m *Metrics) RecordCacheHit() {
	m.CacheHits.Add(1)
}

// RecordCacheMiss increments the cache miss counter.
func (m *Metrics) RecordCacheMiss() {
	m.CacheMisses.Add(1)
}

// RecordWrite records a write operation.
func (m *Metrics) RecordWrite(latency time.Duration) {
	m.OpsTotal.Add(1)
	m.WriteOps.Add(1)
	m.WriteLatency.Observe(latency)
}

// RecordError increments the error counter.
func (m *Metrics) RecordError() {
	m.ErrorsTotal.Add(1)
}

// Snapshot returns a point-in-time snapshot of all metrics.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		Uptime:        time.Since(m.startTime).String(),
		UptimeSeconds: int64(time.Since(m.startTime).Seconds()),
		OpsTotal:      m.OpsTotal.Load(),
		ReadOps:       m.ReadOps.Load(),
		WriteOps:      m.WriteOps.Load(),
		ErrorsTotal:   m.ErrorsTotal.Load(),
		ReadP50us:     m.ReadLatency.Percentile(50),
		ReadP99us:     m.ReadLatency.Percentile(99),
		WriteP50us:    m.WriteLatency.Percentile(50),
		WriteP99us:    m.WriteLatency.Percentile(99),
		KeysTotal:     m.KeysTotal.Load(),
		ChunksTotal:   m.ChunksTotal.Load(),
		NodesOnline:   m.NodesOnline.Load(),
		NodesTotal:    m.NodesTotal.Load(),
		BucketsTotal:  m.BucketsTotal.Load(),
		CacheHits:     m.CacheHits.Load(),
		CacheMisses:   m.CacheMisses.Load(),
		CacheSize:     m.CacheSize.Load(),
		DiskReadBytes:  m.DiskReadBytes.Load(),
		DiskWriteBytes: m.DiskWriteBytes.Load(),
		DiskReadOps:    m.DiskReadOps.Load(),
		DiskWriteOps:   m.DiskWriteOps.Load(),
		WALBytesWritten: m.WALBytesWritten.Load(),
		WALFsyncCount:   m.WALFsyncCount.Load(),
		GCScannedChunks:  m.GCScannedChunks.Load(),
		GCDeletedChunks:  m.GCDeletedChunks.Load(),
		GCScannedBytes:   m.GCScannedBytes.Load(),
		GCFreedBytes:     m.GCFreedBytes.Load(),
		GCLastDurationMs: m.GCLastDurationMs.Load(),
		GCOrphanChunks:   m.GCOrphanChunks.Load(),
		RepairTasksQueued:    m.RepairTasksQueued.Load(),
		RepairTasksCompleted: m.RepairTasksCompleted.Load(),
		RebalanceBytesMoved:  m.RebalanceBytesMoved.Load(),
		RaftState:     int(m.RaftState.Load()),
		RaftTerm:      m.RaftTerm.Load(),
		RaftLogIndex:  m.RaftLogIndex.Load(),
		SnapshotsDone: m.SnapshotsDone.Load(),
	}
}

// MetricsSnapshot is a JSON-serializable metrics report.
type MetricsSnapshot struct {
	Uptime        string `json:"uptime"`
	UptimeSeconds int64  `json:"uptime_seconds"`
	OpsTotal      int64  `json:"ops_total"`
	ReadOps       int64  `json:"read_ops"`
	WriteOps      int64  `json:"write_ops"`
	ErrorsTotal   int64  `json:"errors_total"`
	ReadP50us     int64  `json:"read_p50_us"`
	ReadP99us     int64  `json:"read_p99_us"`
	WriteP50us    int64  `json:"write_p50_us"`
	WriteP99us    int64  `json:"write_p99_us"`
	KeysTotal     int64  `json:"keys_total"`
	ChunksTotal   int64  `json:"chunks_total"`
	NodesOnline   int64  `json:"nodes_online"`
	NodesTotal    int64  `json:"nodes_total"`
	BucketsTotal  int64  `json:"buckets_total"`

	// Cache
	CacheHits   int64 `json:"cache_hits"`
	CacheMisses int64 `json:"cache_misses"`
	CacheSize   int64 `json:"cache_size"`

	// Disk I/O
	DiskReadBytes  int64 `json:"disk_read_bytes"`
	DiskWriteBytes int64 `json:"disk_write_bytes"`
	DiskReadOps    int64 `json:"disk_read_ops"`
	DiskWriteOps   int64 `json:"disk_write_ops"`

	// WAL
	WALBytesWritten int64 `json:"wal_bytes_written"`
	WALFsyncCount   int64 `json:"wal_fsync_count"`

	// GC
	GCScannedChunks  int64 `json:"gc_scanned_chunks"`
	GCDeletedChunks  int64 `json:"gc_deleted_chunks"`
	GCScannedBytes   int64 `json:"gc_scanned_bytes"`
	GCFreedBytes     int64 `json:"gc_freed_bytes"`
	GCLastDurationMs int64 `json:"gc_last_duration_ms"`
	GCOrphanChunks   int64 `json:"gc_orphan_chunks"`

	// Repair / Rebalance
	RepairTasksQueued    int64 `json:"repair_tasks_queued"`
	RepairTasksCompleted int64 `json:"repair_tasks_completed"`
	RebalanceBytesMoved  int64 `json:"rebalance_bytes_moved"`

	RaftState     int   `json:"raft_state"` // 0=follower, 1=candidate, 2=leader
	RaftTerm      int64 `json:"raft_term"`
	RaftLogIndex  int64 `json:"raft_log_index"`
	SnapshotsDone int64 `json:"snapshots_done"`
}

// ============================================================
// LatencyHistogram — Lock-free latency tracking
// ============================================================

// LatencyHistogram tracks latency percentiles using a lock-free
// ring buffer backed by atomic operations.  O(1) Observe with no
// mutex; O(N log N) Percentile on a snapshot copy.
type LatencyHistogram struct {
	// ring is the fixed-size circular buffer.  Written by Observe
	// via atomic.Store; read by Percentile via atomic.Load.
	ring []atomic.Int64

	// head is the next slot index, advanced by atomic.Add.
	head atomic.Uint64

	// count records the total number of observations (never wraps).
	count atomic.Uint64

	// cap is the ring size (constant after construction).
	cap int
}

// NewLatencyHistogram creates a histogram with a 10K-sample lock-free ring buffer.
func NewLatencyHistogram() *LatencyHistogram {
	const size = 10_000
	h := &LatencyHistogram{
		ring: make([]atomic.Int64, size),
		cap:  size,
	}
	return h
}

// Observe records a latency measurement. Lock-free.
func (h *LatencyHistogram) Observe(d time.Duration) {
	us := d.Microseconds()
	idx := h.head.Add(1) - 1 // unique slot per goroutine
	h.ring[idx%uint64(h.cap)].Store(us)
	h.count.Add(1)
}

// Percentile returns the p-th percentile (0-100) in microseconds.
func (h *LatencyHistogram) Percentile(p int) int64 {
	n := h.count.Load()
	if n == 0 {
		return 0
	}
	// Only sample up to cap entries (ring wraps).
	sampleSize := int(n)
	if sampleSize > h.cap {
		sampleSize = h.cap
	}
	cp := make([]int64, sampleSize)
	for i := 0; i < sampleSize; i++ {
		cp[i] = h.ring[i].Load()
	}

	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := (p * sampleSize) / 100
	if idx >= sampleSize {
		idx = sampleSize - 1
	}
	return cp[idx]
}

// ============================================================
// Health Check — HTTP probe for k8s/load balancer
// ============================================================

// HealthStatus represents the health of the metadata service.
type HealthStatus struct {
	Status    string            `json:"status"`  // "healthy", "degraded", "unhealthy"
	Role      string            `json:"role"`    // "leader", "follower", "standalone"
	Version   string            `json:"version"` // Service version
	Uptime    string            `json:"uptime"`
	Checks    map[string]string `json:"checks"` // Individual check results
	Timestamp time.Time         `json:"timestamp"`
}

// HealthChecker performs health checks on the metadata service.
type HealthChecker struct {
	store    *PebbleStore
	raftNode *RaftNode
	metrics  *Metrics
	version  string
}

// NewHealthChecker creates a health checker for a PebbleStore.
func NewHealthChecker(store *PebbleStore, raftNode *RaftNode, metrics *Metrics, version string) *HealthChecker {
	return &HealthChecker{
		store:    store,
		raftNode: raftNode,
		metrics:  metrics,
		version:  version,
	}
}

// Check performs all health checks and returns the status.
func (hc *HealthChecker) Check() HealthStatus {
	status := HealthStatus{
		Status:    "healthy",
		Version:   hc.version,
		Checks:    make(map[string]string),
		Timestamp: time.Now(),
	}

	if hc.metrics != nil {
		status.Uptime = time.Since(hc.metrics.startTime).String()
	}

	// Check 1: Pebble readable
	func() {
		iter, err := hc.store.db.NewIter(nil)
		if err != nil {
			status.Checks["pebble"] = fmt.Sprintf("error: %v", err)
			status.Status = "unhealthy"
			return
		}
		iter.Close()
		status.Checks["pebble"] = "ok"
	}()

	// Check 2: Root inode exists
	func() {
		key := fmt.Sprintf("%s%d", prefixInode, RootInodeID)
		_, closer, err := hc.store.db.Get([]byte(key))
		if err != nil {
			status.Checks["root_inode"] = "missing"
			status.Status = "unhealthy"
			return
		}
		closer.Close()
		status.Checks["root_inode"] = "ok"
	}()

	// Check 3: Raft status (if applicable)
	if hc.raftNode != nil {
		if hc.raftNode.IsLeader() {
			status.Role = "leader"
		} else {
			status.Role = "follower"
		}
		stats := hc.raftNode.Stats()
		if state, ok := stats["state"]; ok {
			status.Checks["raft_state"] = state
		}
		if leader, ok := stats["last_contact"]; ok {
			status.Checks["raft_last_contact"] = leader
		}
	} else {
		status.Role = "standalone"
		status.Checks["raft"] = "disabled"
	}

	// Check 4: Error rate
	if hc.metrics != nil {
		total := hc.metrics.OpsTotal.Load()
		errors := hc.metrics.ErrorsTotal.Load()
		if total > 100 && float64(errors)/float64(total) > 0.05 {
			status.Checks["error_rate"] = fmt.Sprintf("%.2f%% (degraded)", float64(errors)/float64(total)*100)
			if status.Status == "healthy" {
				status.Status = "degraded"
			}
		} else {
			status.Checks["error_rate"] = "ok"
		}
	}

	return status
}

// HTTPHandler returns an http.Handler for health check endpoints.
// Mount at /health and /ready for k8s liveness/readiness probes.
func (hc *HealthChecker) HTTPHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		status := hc.Check()
		w.Header().Set("Content-Type", "application/json")
		if status.Status == "unhealthy" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(status)
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		status := hc.Check()
		w.Header().Set("Content-Type", "application/json")
		if status.Status != "healthy" {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "not ready"})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if hc.metrics == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		accept := r.Header.Get("Accept")
		if accept == "application/openmetrics-text" || r.URL.Query().Get("format") == "openmetrics" {
			w.Header().Set("Content-Type", "application/openmetrics-text; version=1.0.0; charset=utf-8")
			hc.writeOpenMetrics(w)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		hc.writePrometheus(w)
	})

	return mux
}

func (hc *HealthChecker) writePrometheus(w io.Writer) {
	s := hc.metrics.Snapshot()

	fmt.Fprintf(w, "# HELP metad_uptime_seconds Uptime.\n# TYPE metad_uptime_seconds gauge\nmetad_uptime_seconds %d\n\n", s.UptimeSeconds)
	fmt.Fprintf(w, "# HELP metad_ops_total Total operations.\n# TYPE metad_ops_total counter\nmetad_ops_total %d\n", s.OpsTotal)
	fmt.Fprintf(w, "# HELP metad_read_ops Read operations.\n# TYPE metad_read_ops counter\nmetad_read_ops %d\n", s.ReadOps)
	fmt.Fprintf(w, "# HELP metad_write_ops Write operations.\n# TYPE metad_write_ops counter\nmetad_write_ops %d\n", s.WriteOps)
	fmt.Fprintf(w, "# HELP metad_errors_total Total errors.\n# TYPE metad_errors_total counter\nmetad_errors_total %d\n\n", s.ErrorsTotal)
	fmt.Fprintf(w, "# HELP metad_read_latency_us Latency percentiles.\n# TYPE metad_read_latency_us gauge\n")
	fmt.Fprintf(w, "metad_read_latency_us{quantile=\"0.5\"} %d\nmetad_read_latency_us{quantile=\"0.99\"} %d\n\n", s.ReadP50us, s.ReadP99us)
	fmt.Fprintf(w, "# HELP metad_write_latency_us Latency percentiles.\n# TYPE metad_write_latency_us gauge\n")
	fmt.Fprintf(w, "metad_write_latency_us{quantile=\"0.5\"} %d\nmetad_write_latency_us{quantile=\"0.99\"} %d\n\n", s.WriteP50us, s.WriteP99us)
	fmt.Fprintf(w, "# HELP metad_keys_total Total keys.\n# TYPE metad_keys_total gauge\nmetad_keys_total %d\n", s.KeysTotal)
	fmt.Fprintf(w, "# HELP metad_chunks_total Total chunks.\n# TYPE metad_chunks_total gauge\nmetad_chunks_total %d\n", s.ChunksTotal)
	fmt.Fprintf(w, "# HELP metad_nodes_online Nodes online.\n# TYPE metad_nodes_online gauge\nmetad_nodes_online %d\n", s.NodesOnline)
	fmt.Fprintf(w, "# HELP metad_nodes_total Total registered nodes.\n# TYPE metad_nodes_total gauge\nmetad_nodes_total %d\n", s.NodesTotal)
	fmt.Fprintf(w, "# HELP metad_buckets_total Total buckets.\n# TYPE metad_buckets_total gauge\nmetad_buckets_total %d\n\n", s.BucketsTotal)

	// Cache
	fmt.Fprintf(w, "# HELP metad_cache_hits Cache hit count.\n# TYPE metad_cache_hits counter\nmetad_cache_hits %d\n", s.CacheHits)
	fmt.Fprintf(w, "# HELP metad_cache_misses Cache miss count.\n# TYPE metad_cache_misses counter\nmetad_cache_misses %d\n", s.CacheMisses)
	fmt.Fprintf(w, "# HELP metad_cache_size Current cache entry count.\n# TYPE metad_cache_size gauge\nmetad_cache_size %d\n\n", s.CacheSize)

	// Disk I/O
	fmt.Fprintf(w, "# HELP metad_disk_read_bytes Total bytes read from disk.\n# TYPE metad_disk_read_bytes counter\nmetad_disk_read_bytes %d\n", s.DiskReadBytes)
	fmt.Fprintf(w, "# HELP metad_disk_write_bytes Total bytes written to disk.\n# TYPE metad_disk_write_bytes counter\nmetad_disk_write_bytes %d\n", s.DiskWriteBytes)
	fmt.Fprintf(w, "# HELP metad_disk_read_ops Total disk read operations.\n# TYPE metad_disk_read_ops counter\nmetad_disk_read_ops %d\n", s.DiskReadOps)
	fmt.Fprintf(w, "# HELP metad_disk_write_ops Total disk write operations.\n# TYPE metad_disk_write_ops counter\nmetad_disk_write_ops %d\n\n", s.DiskWriteOps)

	// WAL
	fmt.Fprintf(w, "# HELP metad_wal_bytes_written Total bytes written to WAL.\n# TYPE metad_wal_bytes_written counter\nmetad_wal_bytes_written %d\n", s.WALBytesWritten)
	fmt.Fprintf(w, "# HELP metad_wal_fsync_count Total WAL fsync calls.\n# TYPE metad_wal_fsync_count counter\nmetad_wal_fsync_count %d\n\n", s.WALFsyncCount)

	// GC
	fmt.Fprintf(w, "# HELP metad_gc_scanned_chunks Chunks scanned by GC.\n# TYPE metad_gc_scanned_chunks counter\nmetad_gc_scanned_chunks %d\n", s.GCScannedChunks)
	fmt.Fprintf(w, "# HELP metad_gc_deleted_chunks Chunks deleted by GC.\n# TYPE metad_gc_deleted_chunks counter\nmetad_gc_deleted_chunks %d\n", s.GCDeletedChunks)
	fmt.Fprintf(w, "# HELP metad_gc_scanned_bytes Bytes scanned by GC.\n# TYPE metad_gc_scanned_bytes counter\nmetad_gc_scanned_bytes %d\n", s.GCScannedBytes)
	fmt.Fprintf(w, "# HELP metad_gc_freed_bytes Bytes freed by GC.\n# TYPE metad_gc_freed_bytes counter\nmetad_gc_freed_bytes %d\n", s.GCFreedBytes)
	fmt.Fprintf(w, "# HELP metad_gc_last_duration_ms Last GC duration.\n# TYPE metad_gc_last_duration_ms gauge\nmetad_gc_last_duration_ms %d\n\n", s.GCLastDurationMs)

	// Repair
	fmt.Fprintf(w, "# HELP metad_repair_tasks_queued Repair tasks queued.\n# TYPE metad_repair_tasks_queued gauge\nmetad_repair_tasks_queued %d\n", s.RepairTasksQueued)
	fmt.Fprintf(w, "# HELP metad_repair_tasks_completed Repair tasks completed.\n# TYPE metad_repair_tasks_completed counter\nmetad_repair_tasks_completed %d\n", s.RepairTasksCompleted)
	fmt.Fprintf(w, "# HELP metad_rebalance_bytes_moved Bytes moved by rebalance.\n# TYPE metad_rebalance_bytes_moved counter\nmetad_rebalance_bytes_moved %d\n\n", s.RebalanceBytesMoved)

	fmt.Fprintf(w, "# HELP metad_raft_info Raft metadata.\n# TYPE metad_raft_info gauge\nmetad_raft_info{state=\"%s\",term=\"%d\",log_index=\"%d\",snapshots=\"%d\"} 1\n",
		raftStateLabel(s.RaftState), s.RaftTerm, s.RaftLogIndex, s.SnapshotsDone)
}

func (hc *HealthChecker) writeOpenMetrics(w io.Writer) {
	s := hc.metrics.Snapshot()

	fmt.Fprintf(w, "# TYPE metad_uptime_seconds gauge\n# UNIT metad_uptime_seconds seconds\nmetad_uptime_seconds %d\n\n", s.UptimeSeconds)
	fmt.Fprintf(w, "# TYPE metad_ops_total counter\n# UNIT metad_ops_total operations\nmetad_ops_total %d\n", s.OpsTotal)
	fmt.Fprintf(w, "# TYPE metad_read_ops counter\n# UNIT metad_read_ops operations\nmetad_read_ops %d\n", s.ReadOps)
	fmt.Fprintf(w, "# TYPE metad_write_ops counter\n# UNIT metad_write_ops operations\nmetad_write_ops %d\n", s.WriteOps)
	fmt.Fprintf(w, "# TYPE metad_errors_total counter\n# UNIT metad_errors_total errors\nmetad_errors_total %d\n\n", s.ErrorsTotal)
	fmt.Fprintf(w, "# TYPE metad_read_latency_us gauge\n# UNIT metad_read_latency_us microseconds\n")
	fmt.Fprintf(w, "metad_read_latency_us{quantile=\"0.5\"} %d\nmetad_read_latency_us{quantile=\"0.99\"} %d\n\n", s.ReadP50us, s.ReadP99us)
	fmt.Fprintf(w, "# TYPE metad_write_latency_us gauge\n# UNIT metad_write_latency_us microseconds\n")
	fmt.Fprintf(w, "metad_write_latency_us{quantile=\"0.5\"} %d\nmetad_write_latency_us{quantile=\"0.99\"} %d\n\n", s.WriteP50us, s.WriteP99us)
	fmt.Fprintf(w, "# TYPE metad_keys_total gauge\nmetad_keys_total %d\n", s.KeysTotal)
	fmt.Fprintf(w, "# TYPE metad_chunks_total gauge\nmetad_chunks_total %d\n", s.ChunksTotal)
	fmt.Fprintf(w, "# TYPE metad_nodes_online gauge\nmetad_nodes_online %d\n", s.NodesOnline)
	fmt.Fprintf(w, "# TYPE metad_nodes_total gauge\nmetad_nodes_total %d\n", s.NodesTotal)
	fmt.Fprintf(w, "# TYPE metad_buckets_total gauge\nmetad_buckets_total %d\n\n", s.BucketsTotal)

	// Cache
	fmt.Fprintf(w, "# TYPE metad_cache_hits counter\nmetad_cache_hits %d\n", s.CacheHits)
	fmt.Fprintf(w, "# TYPE metad_cache_misses counter\nmetad_cache_misses %d\n", s.CacheMisses)
	fmt.Fprintf(w, "# TYPE metad_cache_size gauge\nmetad_cache_size %d\n\n", s.CacheSize)

	// Disk I/O
	fmt.Fprintf(w, "# TYPE metad_disk_read_bytes counter\n# UNIT metad_disk_read_bytes bytes\nmetad_disk_read_bytes %d\n", s.DiskReadBytes)
	fmt.Fprintf(w, "# TYPE metad_disk_write_bytes counter\n# UNIT metad_disk_write_bytes bytes\nmetad_disk_write_bytes %d\n", s.DiskWriteBytes)
	fmt.Fprintf(w, "# TYPE metad_disk_read_ops counter\nmetad_disk_read_ops %d\n", s.DiskReadOps)
	fmt.Fprintf(w, "# TYPE metad_disk_write_ops counter\nmetad_disk_write_ops %d\n\n", s.DiskWriteOps)

	// WAL
	fmt.Fprintf(w, "# TYPE metad_wal_bytes_written counter\n# UNIT metad_wal_bytes_written bytes\nmetad_wal_bytes_written %d\n", s.WALBytesWritten)
	fmt.Fprintf(w, "# TYPE metad_wal_fsync_count counter\nmetad_wal_fsync_count %d\n\n", s.WALFsyncCount)

	// GC
	fmt.Fprintf(w, "# TYPE metad_gc_scanned_chunks counter\nmetad_gc_scanned_chunks %d\n", s.GCScannedChunks)
	fmt.Fprintf(w, "# TYPE metad_gc_deleted_chunks counter\nmetad_gc_deleted_chunks %d\n", s.GCDeletedChunks)
	fmt.Fprintf(w, "# TYPE metad_gc_scanned_bytes counter\n# UNIT metad_gc_scanned_bytes bytes\nmetad_gc_scanned_bytes %d\n", s.GCScannedBytes)
	fmt.Fprintf(w, "# TYPE metad_gc_freed_bytes counter\n# UNIT metad_gc_freed_bytes bytes\nmetad_gc_freed_bytes %d\n", s.GCFreedBytes)
	fmt.Fprintf(w, "# TYPE metad_gc_last_duration_ms gauge\n# UNIT metad_gc_last_duration_ms milliseconds\nmetad_gc_last_duration_ms %d\n\n", s.GCLastDurationMs)

	// Repair
	fmt.Fprintf(w, "# TYPE metad_repair_tasks_queued gauge\nmetad_repair_tasks_queued %d\n", s.RepairTasksQueued)
	fmt.Fprintf(w, "# TYPE metad_repair_tasks_completed counter\nmetad_repair_tasks_completed %d\n", s.RepairTasksCompleted)
	fmt.Fprintf(w, "# TYPE metad_rebalance_bytes_moved counter\n# UNIT metad_rebalance_bytes_moved bytes\nmetad_rebalance_bytes_moved %d\n\n", s.RebalanceBytesMoved)

	fmt.Fprintf(w, "# TYPE metad_raft_info gauge\nmetad_raft_info{state=\"%s\",term=\"%d\",log_index=\"%d\",snapshots=\"%d\"} 1\n",
		raftStateLabel(s.RaftState), s.RaftTerm, s.RaftLogIndex, s.SnapshotsDone)
	fmt.Fprintln(w, "# EOF")
}

func raftStateLabel(state int) string {
	switch state {
	case 1:
		return "candidate"
	case 2:
		return "leader"
	default:
		return "follower"
	}
}
