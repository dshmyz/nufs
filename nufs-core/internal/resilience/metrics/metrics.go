// Package metrics provides a small, dependency-free Prometheus
// exposition layer. It is lifted from the legacy MinFS code path
// and re-shaped to be service-agnostic: instead of hard-coded
// FUSE/S3 counter names, callers declare the counters they want
// on a Registry and reuse the writer.
//
// The package also exposes a JSON snapshot helper, useful for
// ad-hoc debugging and for admin endpoints that want to embed
// metrics in HTML.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Histogram tracks latency distribution with fixed exponential
// buckets. Each bucket counts observations whose value is <= the
// bucket's upper bound.
type Histogram struct {
	name string
	help string

	mu      sync.Mutex
	count   uint64
	sum     float64 // total seconds
	buckets []histBucket
}

type histBucket struct {
	le    float64 // upper bound in seconds
	count uint64
}

// NewHistogram returns a Histogram with the given bucket upper bounds
// (in seconds). DefaultLatencyBuckets is a good starting point. The
// name and help strings are used by WriteProm to label the output;
// they can be set with SetMeta after construction.
func NewHistogram(buckets []float64) *Histogram {
	h := &Histogram{buckets: make([]histBucket, len(buckets))}
	for i, b := range buckets {
		h.buckets[i].le = b
	}
	return h
}

// SetMeta records the metric name and HELP text used by WriteProm.
func (h *Histogram) SetMeta(name, help string) {
	h.name = name
	h.help = help
}

// Name returns the metric name.
func (h *Histogram) Name() string { return h.name }

// DefaultLatencyBuckets covers 1ms to 60s, suitable for typical
// S3 / datanode / FUSE operations.
var DefaultLatencyBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
}

// Observe records a single observation in seconds.
func (h *Histogram) Observe(seconds float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += seconds
	for i := range h.buckets {
		if seconds <= h.buckets[i].le {
			h.buckets[i].count++
		}
	}
}

// Count returns the number of observations recorded.
func (h *Histogram) Count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

// Sum returns the total of all observations, in seconds.
func (h *Histogram) Sum() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sum
}

// HistogramSnapshot is a point-in-time view of a Histogram.
type HistogramSnapshot struct {
	Count   uint64
	Sum     float64
	Buckets []struct {
		LE    float64
		Count uint64
	}
}

// Snapshot returns a copy of the histogram state.
func (h *Histogram) Snapshot() HistogramSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := HistogramSnapshot{Count: h.count, Sum: h.sum}
	s.Buckets = make([]struct {
		LE    float64
		Count uint64
	}, len(h.buckets))
	for i, b := range h.buckets {
		s.Buckets[i].LE = b.le
		s.Buckets[i].Count = b.count
	}
	return s
}

// WriteProm writes the histogram in Prometheus text exposition format
// to w, prefixed with HELP and TYPE lines. The metric name and HELP
// text come from SetMeta.
func (h *Histogram) WriteProm(w io.Writer) {
	snap := h.Snapshot()
	fmt.Fprintf(w, "# HELP %s %s\n", h.name, h.help)
	fmt.Fprintf(w, "# TYPE %s histogram\n", h.name)
	for _, b := range snap.Buckets {
		fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", h.name, b.LE, b.Count)
	}
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", h.name, snap.Count)
	fmt.Fprintf(w, "%s_sum %g\n", h.name, snap.Sum)
	fmt.Fprintf(w, "%s_count %d\n", h.name, snap.Count)
}

// Counter is an atomic uint64 with a name and help string for
// exposition. Counters are declared on a Registry so they are
// exported via the registry's Prometheus / JSON handlers.
type Counter struct {
	name  string
	help  string
	value uint64
}

// NewCounter constructs a Counter. Use Registry.Counter to make a
// counter that the registry knows about.
func NewCounter(name, help string) *Counter {
	return &Counter{name: name, help: help}
}

// Inc adds 1 to the counter.
func (c *Counter) Inc() { atomic.AddUint64(&c.value, 1) }

// Add adds delta to the counter.
func (c *Counter) Add(delta uint64) { atomic.AddUint64(&c.value, delta) }

// Value returns the current counter value.
func (c *Counter) Value() uint64 { return atomic.LoadUint64(&c.value) }

// Name returns the metric name.
func (c *Counter) Name() string { return c.name }

// Gauge is an atomic int64 with a name and help string.
type Gauge struct {
	name  string
	help  string
	value int64
}

// NewGauge constructs a Gauge.
func NewGauge(name, help string) *Gauge {
	return &Gauge{name: name, help: help}
}

// Set replaces the gauge value.
func (g *Gauge) Set(v int64) { atomic.StoreInt64(&g.value, v) }

// Inc adds 1 to the gauge.
func (g *Gauge) Inc() { atomic.AddInt64(&g.value, 1) }

// Dec subtracts 1 from the gauge.
func (g *Gauge) Dec() { atomic.AddInt64(&g.value, -1) }

// Value returns the current gauge value.
func (g *Gauge) Value() int64 { return atomic.LoadInt64(&g.value) }

// Name returns the metric name.
func (g *Gauge) Name() string { return g.name }

// Registry holds counters, gauges and histograms and exposes them in
// Prometheus and JSON formats. Use NewRegistry to construct one and
// call Counter / Gauge / Histogram factories to register metrics.
type Registry struct {
	mu         sync.RWMutex
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram
	startTime  time.Time
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]*Counter),
		gauges:     make(map[string]*Gauge),
		histograms: make(map[string]*Histogram),
		startTime:  time.Now(),
	}
}

// Counter registers (or returns the existing) counter with the given
// name.
func (r *Registry) Counter(name, help string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		return c
	}
	c := NewCounter(name, help)
	r.counters[name] = c
	return c
}

// Gauge registers (or returns the existing) gauge with the given name.
func (r *Registry) Gauge(name, help string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.gauges[name]; ok {
		return g
	}
	g := NewGauge(name, help)
	r.gauges[name] = g
	return g
}

// Histogram registers (or returns the existing) histogram with the
// given name, using DefaultLatencyBuckets.
func (r *Registry) Histogram(name, help string) *Histogram {
	return r.HistogramWithBuckets(name, help, DefaultLatencyBuckets)
}

// HistogramWithBuckets is like Histogram but lets the caller supply
// custom bucket upper bounds.
func (r *Registry) HistogramWithBuckets(name, help string, buckets []float64) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.histograms[name]; ok {
		return h
	}
	h := NewHistogram(buckets)
	h.SetMeta(name, help)
	r.histograms[name] = h
	return h
}

// UptimeSeconds returns the time since the registry was created.
func (r *Registry) UptimeSeconds() float64 {
	return time.Since(r.startTime).Seconds()
}

// WriteProm writes all registered metrics in Prometheus text format.
func (r *Registry) WriteProm(w io.Writer) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, c := range r.counters {
		fmt.Fprintf(w, "# HELP %s %s\n", c.name, c.help)
		fmt.Fprintf(w, "# TYPE %s counter\n", c.name)
		fmt.Fprintf(w, "%s %d\n", c.name, c.Value())
	}
	for _, g := range r.gauges {
		fmt.Fprintf(w, "# HELP %s %s\n", g.name, g.help)
		fmt.Fprintf(w, "# TYPE %s gauge\n", g.name)
		fmt.Fprintf(w, "%s %d\n", g.name, g.Value())
	}
	for _, h := range r.histograms {
		// Histograms already write their own HELP/TYPE prefix.
		h.WriteProm(w)
	}
}

// Snapshot returns a JSON-serialisable map of all current metrics.
func (r *Registry) Snapshot() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snap := map[string]interface{}{
		"uptime_seconds": r.UptimeSeconds(),
		"counters":       make(map[string]uint64, len(r.counters)),
		"gauges":         make(map[string]int64, len(r.gauges)),
		"histograms":     make(map[string]HistogramSnapshot, len(r.histograms)),
	}
	for name, c := range r.counters {
		snap["counters"].(map[string]uint64)[name] = c.Value()
	}
	for name, g := range r.gauges {
		snap["gauges"].(map[string]int64)[name] = g.Value()
	}
	for name, h := range r.histograms {
		snap["histograms"].(map[string]HistogramSnapshot)[name] = h.Snapshot()
	}
	return snap
}

// PrometheusHandler returns an http.Handler that serves the registry in
// Prometheus text format.
func (r *Registry) PrometheusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.WriteProm(w)
	})
}

// JSONHandler returns an http.Handler that serves a JSON snapshot of
// the registry. Useful for ad-hoc debugging and embedded dashboards.
func (r *Registry) JSONHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Build the response manually so that a missing field does
		// not break older clients.
		var sb strings.Builder
		sb.WriteString("{")
		sb.WriteString(`"uptime_seconds":`)
		fmt.Fprintf(&sb, "%g", r.UptimeSeconds())
		sb.WriteString(",")
		sb.WriteString(`"counters":{`)
		first := true
		r.mu.RLock()
		for name, c := range r.counters {
			if !first {
				sb.WriteString(",")
			}
			first = false
			fmt.Fprintf(&sb, "%q:%d", name, c.Value())
		}
		r.mu.RUnlock()
		sb.WriteString("}}")
		_, _ = w.Write([]byte(sb.String()))
	})
}
