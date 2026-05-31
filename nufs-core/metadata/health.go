package metadata

import (
	"encoding/json"
	"fmt"
	"sort"
	"net/http"
	"sync"
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

	// Storage
	KeysTotal    atomic.Int64
	ChunksTotal  atomic.Int64
	NodesOnline  atomic.Int64
	BucketsTotal atomic.Int64

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
		ReadLatency:  NewLatencyHistogram(),
		WriteLatency: NewLatencyHistogram(),
		startTime:    time.Now(),
	}
}

// RecordRead records a read operation.
func (m *Metrics) RecordRead(latency time.Duration) {
	m.OpsTotal.Add(1)
	m.ReadOps.Add(1)
	m.ReadLatency.Observe(latency)
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
		BucketsTotal:  m.BucketsTotal.Load(),
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
	BucketsTotal  int64  `json:"buckets_total"`
	RaftState     int    `json:"raft_state"` // 0=follower, 1=candidate, 2=leader
	RaftTerm      int64  `json:"raft_term"`
	RaftLogIndex  int64  `json:"raft_log_index"`
	SnapshotsDone int64  `json:"snapshots_done"`
}

// ============================================================
// LatencyHistogram — Lock-free latency tracking
// ============================================================

// LatencyHistogram tracks latency percentiles using a fixed-size ring buffer.
// Suitable for millions of observations with O(1) write and O(N log N) percentile.
type LatencyHistogram struct {
	mu      sync.Mutex
	values  []int64 // microseconds
	count   int
	maxSize int
}

// NewLatencyHistogram creates a histogram with a 10K sample ring buffer.
func NewLatencyHistogram() *LatencyHistogram {
	return &LatencyHistogram{
		values:  make([]int64, 0, 10000),
		maxSize: 10000,
	}
}

// Observe records a latency measurement.
func (h *LatencyHistogram) Observe(d time.Duration) {
	us := d.Microseconds()
	h.mu.Lock()
	if len(h.values) < h.maxSize {
		h.values = append(h.values, us)
	} else {
		h.values[h.count%h.maxSize] = us
	}
	h.count++
	h.mu.Unlock()
}

// Percentile returns the p-th percentile (0-100) in microseconds.
func (h *LatencyHistogram) Percentile(p int) int64 {
	h.mu.Lock()
	n := len(h.values)
	if n == 0 {
		h.mu.Unlock()
		return 0
	}
	// Copy to avoid holding lock during sort
	cp := make([]int64, n)
	copy(cp, h.values)
	h.mu.Unlock()

	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := (p * n) / 100
	if idx >= n {
		idx = n - 1
	}
	return cp[idx]
}

// sortInt64s is a simple insertion sort (fine for 10K elements).
func sortInt64s(a []int64) {
	for i := 1; i < len(a); i++ {
		key := a[i]
		j := i - 1
		for j >= 0 && a[j] > key {
			a[j+1] = a[j]
			j--
		}
		a[j+1] = key
	}
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(hc.metrics.Snapshot())
	})

	return mux
}
