package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// TestPrometheusEmitsHTTPRequestMetric asserts the ops /metrics endpoint
// emits nufs_metad_http_requests_total{status=2xx|4xx|5xx} backed by the
// instrumentMiddleware counters. This metric backs the metad_availability SLI.
func TestPrometheusEmitsHTTPRequestMetric(t *testing.T) {
	m := metadata.NewMetrics()
	m.HTTPReq2xx.Add(12)
	m.HTTPReq4xx.Add(3)
	m.HTTPReq5xx.Add(1)

	store, _ := newOpsTestStore(t)
	defer store.Close()
	rr := httptest.NewRecorder()
	prometheusMetricsHandler(store, m).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	body := rr.Body.String()
	for _, want := range []string{
		`nufs_metad_http_requests_total{status="2xx"} 12`,
		`nufs_metad_http_requests_total{status="4xx"} 3`,
		`nufs_metad_http_requests_total{status="5xx"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics body missing %q", want)
		}
	}
}
