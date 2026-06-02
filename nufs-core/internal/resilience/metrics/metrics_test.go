package metrics

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCounter_IncAdd(t *testing.T) {
	c := NewCounter("c1", "test counter")
	if got := c.Value(); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
	c.Inc()
	c.Inc()
	c.Add(10)
	if got := c.Value(); got != 12 {
		t.Fatalf("expected 12, got %d", got)
	}
}

func TestGauge_SetIncDec(t *testing.T) {
	g := NewGauge("g1", "test gauge")
	g.Set(5)
	if got := g.Value(); got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}
	g.Inc()
	g.Inc()
	g.Dec()
	if got := g.Value(); got != 6 {
		t.Fatalf("expected 6, got %d", got)
	}
}

func TestHistogram_ObserveAndSnapshot(t *testing.T) {
	h := NewHistogram([]float64{0.1, 0.5, 1.0})
	h.Observe(0.05) // <= 0.1, 0.5, 1.0
	h.Observe(0.3)  // <= 0.5, 1.0
	h.Observe(2.0)  // <= none
	if got := h.Count(); got != 3 {
		t.Fatalf("expected count 3, got %d", got)
	}
	if got := h.Sum(); got != 2.35 {
		t.Fatalf("expected sum 2.35, got %g", got)
	}
	snap := h.Snapshot()
	if snap.Buckets[0].Count != 1 {
		t.Errorf("expected bucket 0.1 count=1, got %d", snap.Buckets[0].Count)
	}
	if snap.Buckets[1].Count != 2 {
		t.Errorf("expected bucket 0.5 count=2, got %d", snap.Buckets[1].Count)
	}
	// 0.05 and 0.3 are both <= 1.0; 2.0 is not.
	if snap.Buckets[2].Count != 2 {
		t.Errorf("expected bucket 1.0 count=2, got %d", snap.Buckets[2].Count)
	}
}

func TestRegistry_PrometheusExposition(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("requests_total", "Total requests")
	c.Add(7)
	g := r.Gauge("active_handles", "Currently open handles")
	g.Set(3)
	// Use a small, fixed set of buckets so the assertions stay readable.
	h := r.HistogramWithBuckets("request_duration_seconds", "Request latency",
		[]float64{0.1, 0.5, 1.0})
	h.Observe(0.05) // <= 0.1, 0.5, 1.0
	h.Observe(0.3)  // <= 0.5, 1.0

	var buf bytes.Buffer
	r.WriteProm(&buf)
	out := buf.String()

	mustContain := []string{
		"# TYPE requests_total counter",
		"requests_total 7",
		"# TYPE active_handles gauge",
		"active_handles 3",
		"# TYPE request_duration_seconds histogram",
		`request_duration_seconds_bucket{le="0.1"} 1`,
		`request_duration_seconds_bucket{le="0.5"} 2`,
		`request_duration_seconds_bucket{le="1"} 2`,
		`request_duration_seconds_bucket{le="+Inf"} 2`,
		"request_duration_seconds_count 2",
		"request_duration_seconds_sum 0.35",
	}
	for _, s := range mustContain {
		if !strings.Contains(out, s) {
			t.Errorf("Prometheus output missing %q\n--- output ---\n%s", s, out)
		}
	}
}

func TestRegistry_PrometheusHandler(t *testing.T) {
	r := NewRegistry()
	r.Counter("hits_total", "").Inc()
	srv := httptest.NewServer(r.PrometheusHandler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/plain") {
		t.Fatalf("expected text/plain, got %q", got)
	}
}

func TestRegistry_JSONHandler(t *testing.T) {
	r := NewRegistry()
	r.Counter("hits_total", "").Add(42)
	srv := httptest.NewServer(r.JSONHandler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var got map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	counters, ok := got["counters"].(map[string]interface{})
	if !ok {
		t.Fatalf("counters missing or wrong type: %+v", got)
	}
	if v, ok := counters["hits_total"].(float64); !ok || v != 42 {
		t.Errorf("expected hits_total=42, got %v", counters["hits_total"])
	}
}

func TestRegistry_DuplicateRegistrationReturnsSame(t *testing.T) {
	r := NewRegistry()
	c1 := r.Counter("foo", "bar")
	c1.Inc()
	c2 := r.Counter("foo", "different help")
	if c1 != c2 {
		t.Fatal("duplicate registration should return the same counter")
	}
	if c2.Value() != 1 {
		t.Fatalf("expected value 1 preserved across re-registration, got %d", c2.Value())
	}
}
