package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"
)

func TestSPAFallbackServesIndexForNonAPIPath(t *testing.T) {
	static := fstest.MapFS{
		"index.html": {Data: []byte("<html>nufs admin</html>")},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := withSPAFallback(mux, static)
	req := httptest.NewRequest(http.MethodGet, "/clusters/manage", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if rec.Body.String() != "<html>nufs admin</html>" {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func TestSPAFallbackDoesNotHandleAPIPath(t *testing.T) {
	static := fstest.MapFS{
		"index.html": {Data: []byte("<html>nufs admin</html>")},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := withSPAFallback(mux, static)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}
