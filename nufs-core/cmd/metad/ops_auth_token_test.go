package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// TestHandleAuthToken_PerIPThrottle verifies the token endpoint's dedicated
// per-IP limiter gates credential-guess attempts before any credential check
// runs. With a burst of 10 and refresh rate of 2/s, the first 10 requests from
// a source IP pass the limiter (and, since signingKey is empty here, fall
// through to the "disabled" path); the 11th is rejected with 429 and a
// Retry-After header so a brute-force attempt backs off instead of firing fast
// HMAC checks in a tight loop.
func TestHandleAuthToken_PerIPThrottle(t *testing.T) {
	h := &opsAuthHandlers{
		// Empty signingKey routes every allowed request straight to the
		// "token_signing_disabled" check after the limiter passes, so we don't
		// need a real credential store to exercise the throttle.
		signingKey:   "",
		tokenLimiter: metadata.NewRateLimiter(2, 10),
	}

	const fromIP = "192.0.2.10:1234"
	req := func() *http.Request {
		body := []byte(`{"access_key":"ak","secret_key":"sk"}`)
		r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/token", bytes.NewReader(body))
		r.RemoteAddr = fromIP
		return r
	}

	for i := 0; i < 10; i++ {
		rec := httptest.NewRecorder()
		h.handleAuthToken(rec, req())
		if rec.Code == http.StatusTooManyRequests {
			t.Fatalf("request %d: unexpected throttle before burst exhausted", i)
		}
	}

	// Burst exhausted; next request from the same IP is throttled.
	rec := httptest.NewRecorder()
	h.handleAuthToken(rec, req())
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after burst exhausted, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header on throttled response")
	}

	// A different source IP has its own fresh bucket and is not throttled.
	another := httptest.NewRequest(http.MethodPost, "/api/v1/auth/token", bytes.NewReader([]byte(`{"access_key":"b","secret_key":"c"}`)))
	another.RemoteAddr = "198.51.100.7:9999"
	rec2 := httptest.NewRecorder()
	h.handleAuthToken(rec2, another)
	if rec2.Code == http.StatusTooManyRequests {
		t.Fatal("a different source IP must have an independent bucket")
	}
}
