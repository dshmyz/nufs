package metadata

import (
	"context"
	"fmt"
	"sort"
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
	RepairTasksQueued    atomic.Int64
	RepairTasksCompleted atomic.Int64
	RebalanceBytesMoved  atomic.Int64

	// Raft (populated when Raft is active)
	RaftState     atomic.Int32 // 0=follower, 1=candidate, 2=leader
	RaftTerm      atomic.Int64
	RaftLogIndex  atomic.Int64
	SnapshotsDone atomic.Int64

	// HTTP (metad ops API) - request counts by status class. These back the
	// metad_availability SLI (1 - 5xx/total). Store-layer OpsTotal/ErrorsTotal
	// above only see Pebble paths, not the HTTP surface, so they under-count
	// failures (raft-not-leader 503, validation 400, auth 401, rate-limit 429).
	HTTPReq2xx atomic.Int64
	HTTPReq4xx atomic.Int64
	HTTPReq5xx atomic.Int64

	// LeaderFailoverRTO is the wall-clock seconds the most recent raft leader
	// failover took (old leader's last contact -> this node winning leadership).
	// Populated by cmd/metad's leaderRTOTracker; only the current leader holds a
	// non-zero value (followers reset to 0 on step-down) so the RTO alert
	// evaluates only on the node that just took over. Backs
	// metad_leader_failover_rto_seconds + the failover-RTO SLO.
	LeaderFailoverRTO atomic.Int64

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

// RecordHTTPStatus increments the ops-API request counter for the HTTP status
// code's class (2xx/4xx/5xx). 3xx (redirects used for leader forwarding) and
// 1xx are folded into 2xx/none respectively; only 2xx/4xx/5xx are tracked
// since those are what the availability SLI uses.
func (m *Metrics) RecordHTTPStatus(code int) {
	switch {
	case code >= 200 && code < 300:
		m.HTTPReq2xx.Add(1)
	case code >= 300 && code < 400:
		// 3xx (leader-forward 307s) are not failures; count as success.
		m.HTTPReq2xx.Add(1)
	case code >= 400 && code < 500:
		m.HTTPReq4xx.Add(1)
	case code >= 500:
		m.HTTPReq5xx.Add(1)
	}
}

// Snapshot returns a point-in-time snapshot of all metrics.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		Uptime:               time.Since(m.startTime).String(),
		UptimeSeconds:        int64(time.Since(m.startTime).Seconds()),
		OpsTotal:             m.OpsTotal.Load(),
		ReadOps:              m.ReadOps.Load(),
		WriteOps:             m.WriteOps.Load(),
		ErrorsTotal:          m.ErrorsTotal.Load(),
		ReadP50us:            m.ReadLatency.Percentile(50),
		ReadP99us:            m.ReadLatency.Percentile(99),
		WriteP50us:           m.WriteLatency.Percentile(50),
		WriteP99us:           m.WriteLatency.Percentile(99),
		KeysTotal:            m.KeysTotal.Load(),
		ChunksTotal:          m.ChunksTotal.Load(),
		NodesOnline:          m.NodesOnline.Load(),
		NodesTotal:           m.NodesTotal.Load(),
		BucketsTotal:         m.BucketsTotal.Load(),
		CacheHits:            m.CacheHits.Load(),
		CacheMisses:          m.CacheMisses.Load(),
		CacheSize:            m.CacheSize.Load(),
		DiskReadBytes:        m.DiskReadBytes.Load(),
		DiskWriteBytes:       m.DiskWriteBytes.Load(),
		DiskReadOps:          m.DiskReadOps.Load(),
		DiskWriteOps:         m.DiskWriteOps.Load(),
		WALBytesWritten:      m.WALBytesWritten.Load(),
		WALFsyncCount:        m.WALFsyncCount.Load(),
		GCScannedChunks:      m.GCScannedChunks.Load(),
		GCDeletedChunks:      m.GCDeletedChunks.Load(),
		GCScannedBytes:       m.GCScannedBytes.Load(),
		GCFreedBytes:         m.GCFreedBytes.Load(),
		GCLastDurationMs:     m.GCLastDurationMs.Load(),
		GCOrphanChunks:       m.GCOrphanChunks.Load(),
		RepairTasksQueued:    m.RepairTasksQueued.Load(),
		RepairTasksCompleted: m.RepairTasksCompleted.Load(),
		RebalanceBytesMoved:  m.RebalanceBytesMoved.Load(),
		RaftState:            int(m.RaftState.Load()),
		RaftTerm:             m.RaftTerm.Load(),
		RaftLogIndex:         m.RaftLogIndex.Load(),
		SnapshotsDone:        m.SnapshotsDone.Load(),
		HTTPReq2xx:           m.HTTPReq2xx.Load(),
		HTTPReq4xx:           m.HTTPReq4xx.Load(),
		HTTPReq5xx:           m.HTTPReq5xx.Load(),
		LeaderFailoverRTO:    m.LeaderFailoverRTO.Load(),
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

	// HTTP (metad ops API) request counts by status class
	HTTPReq2xx int64 `json:"http_req_2xx"`
	HTTPReq4xx int64 `json:"http_req_4xx"`
	HTTPReq5xx int64 `json:"http_req_5xx"`

	// LeaderFailoverRTO (seconds) of the most recent failover; 0 on followers.
	LeaderFailoverRTO int64 `json:"leader_failover_rto_seconds"`
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

// ============================================================
// Cluster Readiness — aggregated cluster health
// ============================================================

// ClusterReadiness describes the cluster's overall ability to serve requests.
type ClusterReadiness struct {
	// Status is "ready", "degraded", or "not_ready".
	Status string `json:"status"`

	// CanWriteRF is the maximum replication factor the cluster can
	// currently satisfy (number of online nodes).
	CanWriteRF int `json:"can_write_rf"`

	// Node counts.
	NodesOnline int `json:"nodes_online"`
	NodesTotal  int `json:"nodes_total"`

	// LeaderStable is true when a Raft leader is elected and reachable.
	LeaderStable bool `json:"leader_stable"`

	// DegradationState reports the PebbleStore's operational mode
	// (Normal, ReadOnly, Degraded, Unavailable).
	DegradationState string `json:"degradation_state"`

	// ChunksTotal is the number of tracked chunks.
	ChunksTotal int64 `json:"chunks_total"`

	// ChunksUnderReplicated counts chunks with fewer Ready replicas
	// than replicas in the set.
	ChunksUnderReplicated int64 `json:"chunks_under_replicated"`

	// RepairQueueDepth is the number of pending repair tasks.
	RepairQueueDepth int64 `json:"repair_queue_depth"`

	// Checks holds per-subsystem verdict strings.
	Checks map[string]string `json:"checks"`

	Timestamp time.Time `json:"timestamp"`
}

// ComputeClusterReadiness evaluates the cluster's overall health.
// It is safe to call from an HTTP handler but may take a few hundred
// milliseconds on large clusters due to the chunk scan.
func (hc *HealthChecker) ComputeClusterReadiness() ClusterReadiness {
	r := ClusterReadiness{
		Status:    "ready",
		Checks:    make(map[string]string),
		Timestamp: time.Now(),
	}

	// --- Node counts and quorum ---
	if hc.store != nil {
		nodes, err := hc.store.ListNodes(context.Background())
		if err != nil {
			r.Checks["nodes"] = fmt.Sprintf("error: %v", err)
			r.Status = "not_ready"
			// Surface the failure explicitly: 0/0 would otherwise report a
			// (false) "everything online" for the availability SLI.
			if hc.metrics != nil {
				hc.metrics.NodesOnline.Store(0)
				hc.metrics.NodesTotal.Store(0)
			}
		} else {
			r.NodesTotal = len(nodes)
			for _, n := range nodes {
				if n.State == NodeOnline {
					r.NodesOnline++
				}
			}
			if hc.metrics != nil {
				hc.metrics.NodesOnline.Store(int64(r.NodesOnline))
				hc.metrics.NodesTotal.Store(int64(r.NodesTotal))
			}
			r.CanWriteRF = r.NodesOnline
			if r.NodesOnline < (r.NodesTotal/2)+1 {
				r.Checks["quorum"] = fmt.Sprintf("at risk: %d/%d online", r.NodesOnline, r.NodesTotal)
				r.Status = "not_ready"
			} else {
				r.Checks["quorum"] = "ok"
			}
		}
	}

	// --- Raft leader stability ---
	if hc.raftNode != nil {
		if hc.raftNode.IsLeader() {
			r.LeaderStable = true
			r.Checks["leader"] = "stable"
		} else {
			addr := hc.raftNode.LeaderAddr()
			if addr == "" {
				r.LeaderStable = false
				r.Checks["leader"] = "no leader"
				if r.Status != "not_ready" {
					r.Status = "degraded"
				}
			} else {
				r.LeaderStable = true
				r.Checks["leader"] = "follower (leader at " + addr + ")"
			}
		}
	} else {
		r.LeaderStable = true // standalone mode — no Raft
		r.Checks["leader"] = "standalone"
	}

	// --- Degradation state ---
	if hc.store != nil {
		dm := hc.store.GetDegradationManager()
		r.DegradationState = dm.State().String()
		if dm.State() == DegStateUnavailable {
			r.Status = "not_ready"
			r.Checks["degradation"] = "unavailable"
		} else if dm.State() != DegStateNormal {
			if r.Status != "not_ready" {
				r.Status = "degraded"
			}
			r.Checks["degradation"] = dm.State().String()
		} else {
			r.Checks["degradation"] = "normal"
		}
	}

	// --- Chunk replication health (lightweight scan) ---
	if hc.store != nil {
		var total, underReplicated int64
		err := hc.store.ScrubAllChunks(func(_ ChunkID, replicaCount, healthyCount int) {
			total++
			if healthyCount < replicaCount {
				underReplicated++
			}
		})
		if err != nil {
			r.Checks["replication"] = fmt.Sprintf("scan error: %v", err)
			if hc.metrics != nil {
				hc.metrics.ChunksTotal.Store(0)
			}
		} else {
			r.ChunksTotal = total
			r.ChunksUnderReplicated = underReplicated
			if hc.metrics != nil {
				hc.metrics.ChunksTotal.Store(total)
			}
			if underReplicated > 0 {
				r.Checks["replication"] = fmt.Sprintf("%d under-replicated / %d total", underReplicated, total)
				if r.Status != "not_ready" {
					r.Status = "degraded"
				}
			} else {
				r.Checks["replication"] = "ok"
			}
		}
	}

	// --- Repair queue ---
	if hc.metrics != nil {
		r.RepairQueueDepth = hc.metrics.RepairTasksQueued.Load()
		repairThreshold := int64(1000) // fallback default
		if hc.store != nil {
			if cfg := hc.store.GetDynamicConfig(); cfg != nil {
				repairThreshold = cfg.ReadinessRepairQueueThreshold
			}
		}
		if r.RepairQueueDepth > repairThreshold {
			r.Checks["repair_queue"] = fmt.Sprintf("%d tasks queued (threshold %d)", r.RepairQueueDepth, repairThreshold)
			if r.Status == "ready" {
				r.Status = "degraded"
			}
		} else {
			r.Checks["repair_queue"] = "ok"
		}
	}

	return r
}

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
