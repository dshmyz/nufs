package s3fs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// --- Histogram ---

type histogram struct {
	mu      sync.Mutex
	count   uint64
	sum     float64
	buckets []histBucket
}

type histBucket struct {
	le    float64
	count uint64
}

var defaultBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
}

func newHistogram() *histogram {
	h := &histogram{buckets: make([]histBucket, len(defaultBuckets))}
	for i, b := range defaultBuckets {
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

// Metrics holds all runtime counters.
type Metrics struct {
	OpsOpen, OpsRead, OpsWrite, OpsRelease uint64
	OpsLookup, OpsReadDir, OpsMkdir        uint64
	OpsRemove, OpsRename, OpsCreate        uint64
	OpsFlush                               uint64

	S3Get, S3Put, S3Copy, S3Remove, S3List uint64
	S3Errors, S3Retries                     uint64

	ActiveHandles uint64

	HistS3Get  *histogram
	HistS3Put  *histogram
	HistS3List *histogram
	HistRead   *histogram
	HistWrite  *histogram

	CircuitBreakerState string
	startTime           time.Time
}

var globalMetrics = &Metrics{
	startTime:  time.Now(),
	HistS3Get:  newHistogram(),
	HistS3Put:  newHistogram(),
	HistS3List: newHistogram(),
	HistRead:   newHistogram(),
	HistWrite:  newHistogram(),
}

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

func metricsObserveS3Get(s float64)  { globalMetrics.HistS3Get.observe(s) }
func metricsObserveS3Put(s float64)  { globalMetrics.HistS3Put.observe(s) }
func metricsObserveS3List(s float64) { globalMetrics.HistS3List.observe(s) }
func metricsObserveRead(s float64)   { globalMetrics.HistRead.observe(s) }
func metricsObserveWrite(s float64)  { globalMetrics.HistWrite.observe(s) }

// Snapshot returns a JSON-serialisable snapshot.
func (m *Metrics) Snapshot() map[string]interface{} {
	return map[string]interface{}{
		"uptime_seconds":  time.Since(m.startTime).Seconds(),
		"active_handles":  atomic.LoadUint64(&m.ActiveHandles),
		"circuit_breaker": m.CircuitBreakerState,
		"ops": map[string]uint64{
			"open": atomic.LoadUint64(&m.OpsOpen), "read": atomic.LoadUint64(&m.OpsRead),
			"write": atomic.LoadUint64(&m.OpsWrite), "release": atomic.LoadUint64(&m.OpsRelease),
			"lookup": atomic.LoadUint64(&m.OpsLookup), "readdir": atomic.LoadUint64(&m.OpsReadDir),
			"mkdir": atomic.LoadUint64(&m.OpsMkdir), "remove": atomic.LoadUint64(&m.OpsRemove),
			"rename": atomic.LoadUint64(&m.OpsRename), "create": atomic.LoadUint64(&m.OpsCreate),
			"flush": atomic.LoadUint64(&m.OpsFlush),
		},
		"s3": map[string]uint64{
			"get": atomic.LoadUint64(&m.S3Get), "put": atomic.LoadUint64(&m.S3Put),
			"copy": atomic.LoadUint64(&m.S3Copy), "remove": atomic.LoadUint64(&m.S3Remove),
			"list": atomic.LoadUint64(&m.S3List), "errors": atomic.LoadUint64(&m.S3Errors),
			"retries": atomic.LoadUint64(&m.S3Retries),
		},
	}
}

func writeHistogramProm(sb *strings.Builder, name, help string, snap histSnapshot) {
	fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	for _, b := range snap.buckets {
		fmt.Fprintf(sb, "%s_bucket{le=\"%g\"} %d\n", name, b.le, b.count)
	}
	fmt.Fprintf(sb, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n",
		name, snap.count, name, snap.sum, name, snap.count)
}

func prometheusHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var sb strings.Builder

		fmt.Fprintf(&sb, "# HELP s3fs_uptime_seconds Uptime.\n# TYPE s3fs_uptime_seconds gauge\ns3fs_uptime_seconds %g\n\n",
			time.Since(globalMetrics.startTime).Seconds())
		fmt.Fprintf(&sb, "# HELP s3fs_active_handles Open handles.\n# TYPE s3fs_active_handles gauge\ns3fs_active_handles %d\n\n",
			atomic.LoadUint64(&globalMetrics.ActiveHandles))

		fmt.Fprintf(&sb, "# TYPE s3fs_ops_total counter\n")
		for _, o := range []struct{ n string; v *uint64 }{
			{"open", &globalMetrics.OpsOpen}, {"read", &globalMetrics.OpsRead},
			{"write", &globalMetrics.OpsWrite}, {"release", &globalMetrics.OpsRelease},
			{"lookup", &globalMetrics.OpsLookup}, {"readdir", &globalMetrics.OpsReadDir},
			{"mkdir", &globalMetrics.OpsMkdir}, {"remove", &globalMetrics.OpsRemove},
			{"rename", &globalMetrics.OpsRename}, {"create", &globalMetrics.OpsCreate},
			{"flush", &globalMetrics.OpsFlush},
		} {
			fmt.Fprintf(&sb, "s3fs_ops_total{op=\"%s\"} %d\n", o.n, atomic.LoadUint64(o.v))
		}
		sb.WriteString("\n")

		fmt.Fprintf(&sb, "# TYPE s3fs_s3_ops_total counter\n")
		for _, o := range []struct{ n string; v *uint64 }{
			{"get", &globalMetrics.S3Get}, {"put", &globalMetrics.S3Put},
			{"copy", &globalMetrics.S3Copy}, {"remove", &globalMetrics.S3Remove},
			{"list", &globalMetrics.S3List}, {"errors", &globalMetrics.S3Errors},
			{"retries", &globalMetrics.S3Retries},
		} {
			fmt.Fprintf(&sb, "s3fs_s3_ops_total{op=\"%s\"} %d\n", o.n, atomic.LoadUint64(o.v))
		}
		sb.WriteString("\n")

		writeHistogramProm(&sb, "s3fs_s3_get_duration_seconds", "S3 GetObject latency.", globalMetrics.HistS3Get.snapshot())
		sb.WriteString("\n")
		writeHistogramProm(&sb, "s3fs_s3_put_duration_seconds", "S3 PutObject latency.", globalMetrics.HistS3Put.snapshot())
		sb.WriteString("\n")
		writeHistogramProm(&sb, "s3fs_read_duration_seconds", "FUSE Read latency.", globalMetrics.HistRead.snapshot())

		w.Write([]byte(sb.String()))
	}
}

func metricsJSONHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(globalMetrics.Snapshot())
	}
}

func healthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	}
}

// StartMetricsServer launches the metrics/health HTTP server.
func StartMetricsServer(addr string) *http.Server {
	if addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", prometheusHandler())
	mux.HandleFunc("/metrics/json", metricsJSONHandler())
	mux.HandleFunc("/healthz", healthHandler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		_ = srv.ListenAndServe()
	}()
	return srv
}
