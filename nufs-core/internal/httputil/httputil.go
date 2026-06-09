// Package httputil provides HTTP middleware utilities for NUFS services.
package httputil

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// BearerTokenOK verifies an Authorization header against the expected bearer
// token using a constant-time comparison. Empty expected tokens never match.
func BearerTokenOK(header, expected string) bool {
	if expected == "" {
		return false
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	got := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if len(got) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}

// BearerAuth wraps next with bearer-token authentication. Requests whose path
// is listed in publicPaths bypass authentication.
func BearerAuth(token string, publicPaths map[string]struct{}, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := publicPaths[r.URL.Path]; ok {
			next.ServeHTTP(w, r)
			return
		}
		if !BearerTokenOK(r.Header.Get("Authorization"), token) {
			slog.Warn("auth rejected", "remote", r.RemoteAddr, "path", r.URL.Path)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// threshold. It wraps the next handler and emits a structured warning when a
// request takes longer than threshold.
//
// Usage:
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("/api/buckets", handler)
//	srv := &http.Server{Handler: httputil.SlowRequestLogger(mux, 200*time.Millisecond)}
func SlowRequestLogger(next http.Handler, threshold time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		elapsed := time.Since(start)

		if elapsed > threshold {
			slog.Warn("http: slow request",
				"method", r.Method,
				"path", r.URL.Path,
				"remote", r.RemoteAddr,
				"duration", elapsed,
				"threshold", threshold,
			)
		}
	})
}
