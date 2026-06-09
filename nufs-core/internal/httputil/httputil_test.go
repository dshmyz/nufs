package httputil

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBearerTokenOK(t *testing.T) {
	if !BearerTokenOK("Bearer secret", "secret") {
		t.Fatal("expected valid bearer token")
	}
	if BearerTokenOK("Bearer wrong", "secret") {
		t.Fatal("expected wrong token to fail")
	}
	if BearerTokenOK("Basic secret", "secret") {
		t.Fatal("expected wrong scheme to fail")
	}
	if BearerTokenOK("Bearer secret", "") {
		t.Fatal("empty expected token should not match")
	}
}

func TestBearerAuthPublicPath(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	h := BearerAuth("secret", map[string]struct{}{"/healthz": {}}, next)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected public path through, got %d", rr.Code)
	}
}

func TestBearerAuthProtectedPath(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	h := BearerAuth("secret", nil, next)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api", nil))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized without token, got %d", rr.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/api", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected authorized request through, got %d", rr.Code)
	}
}
