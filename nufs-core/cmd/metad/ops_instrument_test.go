package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func TestInstrumentMiddlewareCountsByStatusClass(t *testing.T) {
	m := metadata.NewMetrics()
	public := map[string]struct{}{"/health": {}, "/metrics": {}}

	serve := func(status int, path string) {
		h := instrumentMiddleware(m, public, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		}))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}

	serve(200, "/api/v1/buckets")
	serve(307, "/api/v1/buckets") // leader-forward 3xx folds into 2xx (not a failure)
	serve(400, "/api/v1/buckets") // client error
	serve(429, "/api/v1/buckets") // rate limited
	serve(500, "/api/v1/buckets") // server error
	serve(503, "/api/v1/buckets") // leader unavailable

	// Public probe paths are skipped entirely (no count, even on 500).
	serve(500, "/health")
	serve(500, "/metrics")
	// Non-api path skipped.
	serve(500, "/version")

	snap := m.Snapshot()
	if snap.HTTPReq2xx != 2 {
		t.Errorf("2xx = %d, want 2 (200 + 307)", snap.HTTPReq2xx)
	}
	if snap.HTTPReq4xx != 2 {
		t.Errorf("4xx = %d, want 2 (400 + 429)", snap.HTTPReq4xx)
	}
	if snap.HTTPReq5xx != 2 {
		t.Errorf("5xx = %d, want 2 (500 + 503)", snap.HTTPReq5xx)
	}
}
