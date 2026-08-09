package main

import (
	"net/http"
	"strings"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ============================================================
// Ops-API HTTP instrumentation.
//
// A status-capturing middleware that counts every ops-API request by HTTP
// status class into metadata.Metrics, emitted as
// nufs_metad_http_requests_total{status="2xx|4xx|5xx"}. This backs the
// metad_availability SLI (1 - 5xx/total).
//
// It wraps the OUTERMOST ops handler so failures from every layer are counted:
// rate-limit 429, bearer-auth 401, drain 503, leader-forward 307, and handler
// 5xxs alike. Health/metrics/version probes (the public path set) are skipped
// so polling and scrapes don't dilute the availability denominator.
// ============================================================

// statusRecorder captures the status code of the first WriteHeader call.
// 200 is the default when a handler writes a body without calling
// WriteHeader (per net/http semantics).
type statusRecorder struct {
	http.ResponseWriter
	code    int
	written bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.written {
		r.code = code
		r.written = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.written {
		r.code = http.StatusOK
		r.written = true
	}
	return r.ResponseWriter.Write(b)
}

// instrumentMiddleware counts ops-API requests by status class. Public probe
// paths (health/metrics/version) are skipped.
func instrumentMiddleware(metrics *metadata.Metrics, public map[string]struct{}, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, skip := public[r.URL.Path]; skip {
			next.ServeHTTP(w, r)
			return
		}
		// Only the real ops API is an availability signal; static/site paths
		// outside /api/v1/ (if any) are not ops calls.
		if !strings.HasPrefix(r.URL.Path, "/api/v1/") {
			next.ServeHTTP(w, r)
			return
		}
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		metrics.RecordHTTPStatus(rec.code)
	})
}
