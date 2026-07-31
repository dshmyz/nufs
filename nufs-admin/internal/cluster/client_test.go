package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDoJSON_RetryOn5xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer srv.Close()

	c := NewClient("test", srv.URL)
	var result map[string]string
	if err := c.Get(context.Background(), "/api/v1/test", &result); err != nil {
		t.Fatalf("expected success after retries, got: %v", err)
	}
	if result["status"] != "ok" {
		t.Fatalf("expected status=ok, got %v", result)
	}
	if n := int(calls.Load()); n != 3 {
		t.Fatalf("expected 3 attempts, got %d", n)
	}
}

func TestDoJSON_NoRetryOn4xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer srv.Close()

	c := NewClient("test", srv.URL)
	err := c.Get(context.Background(), "/api/v1/test", nil)
	if err == nil {
		t.Fatal("expected error on 400")
	}
	upErr, ok := err.(*UpstreamHTTPError)
	if !ok {
		t.Fatalf("expected UpstreamHTTPError, got %T: %v", err, err)
	}
	if upErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", upErr.StatusCode)
	}
	if n := int(calls.Load()); n != 1 {
		t.Fatalf("expected 1 attempt (no retry on 4xx), got %d", n)
	}
}

func TestDoJSON_Follows307Redirect(t *testing.T) {
	// Leader server — responds with success.
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"leader": "true"})
	}))
	defer leader.Close()

	// Follower server — redirects to leader.
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, leader.URL+"/api/v1/test", http.StatusTemporaryRedirect)
	}))
	defer follower.Close()

	c := NewClient("test", follower.URL)
	var result map[string]string
	if err := c.Get(context.Background(), "/api/v1/test", &result); err != nil {
		t.Fatalf("expected success after redirect, got: %v", err)
	}
	if result["leader"] != "true" {
		t.Fatalf("expected leader=true, got %v", result)
	}
}

func TestDoJSON_RedirectLoopExhausts(t *testing.T) {
	// Two servers that redirect to each other, creating an infinite loop.
	// With maxRedirectHops=5, this should exhaust and fail.
	var srvB *httptest.Server
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, srvB.URL+"/loop", http.StatusTemporaryRedirect)
	}))
	defer srvA.Close()

	srvB = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, srvA.URL+"/loop", http.StatusTemporaryRedirect)
	}))
	defer srvB.Close()

	c := NewClient("test", srvA.URL)
	err := c.Get(context.Background(), "/api/v1/test", nil)
	if err == nil {
		t.Fatal("expected error from redirect loop")
	}
	if !strings.Contains(err.Error(), "redirect") {
		t.Fatalf("expected redirect-related error, got: %v", err)
	}
}

func TestDoJSON_RetryExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	c := NewClient("test", srv.URL)
	// Use a short timeout to speed up the test.
	c.http.Timeout = 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := c.Get(ctx, "/api/v1/test", nil)
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	upErr, ok := err.(*UpstreamHTTPError)
	if !ok {
		t.Fatalf("expected UpstreamHTTPError, got %T: %v", err, err)
	}
	if upErr.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d", upErr.StatusCode)
	}
}

func TestDoJSON_RetryOn429(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	defer srv.Close()

	c := NewClient("test", srv.URL)
	var result map[string]bool
	if err := c.Get(context.Background(), "/api/v1/test", &result); err != nil {
		t.Fatalf("expected success after 429 retry, got: %v", err)
	}
	if !result["ok"] {
		t.Fatal("expected ok=true")
	}
}

func TestDoJSON_ContextCancelledDuringRetry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient("test", srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after first failure.
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := c.Get(ctx, "/api/v1/test", nil)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestDoJSON_307RedirectWithBody(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		json.NewEncoder(w).Encode(body)
	}))
	defer leader.Close()

	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, leader.URL+"/api/v1/test", http.StatusTemporaryRedirect)
	}))
	defer follower.Close()

	c := NewClient("test", follower.URL)
	var result map[string]string
	body := `{"key":"value"}`
	if err := c.Post(context.Background(), "/api/v1/test", strings.NewReader(body), &result); err != nil {
		t.Fatalf("expected success after redirect with body, got: %v", err)
	}
	if result["key"] != "value" {
		t.Fatalf("expected key=value, got %v", result)
	}
}

func TestDoJSON_RedirectThen5xx_RetriesToLeader(t *testing.T) {
	var leaderCalls atomic.Int32
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := leaderCalls.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"leader": "ok"})
	}))
	defer leader.Close()

	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, leader.URL+"/api/v1/test", http.StatusTemporaryRedirect)
	}))
	defer follower.Close()

	c := NewClient("test", follower.URL)
	var result map[string]string
	if err := c.Get(context.Background(), "/api/v1/test", &result); err != nil {
		t.Fatalf("expected success after redirect+retry, got: %v", err)
	}
	if result["leader"] != "ok" {
		t.Fatalf("expected leader=ok, got %v", result)
	}
	// First attempt redirects then gets 503, second attempt redirects then succeeds = 2 leader calls
	if n := int(leaderCalls.Load()); n != 2 {
		t.Fatalf("expected 2 leader calls, got %d", n)
	}
}

func TestCheckHealth_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient("test", srv.URL)
	if err := c.CheckHealth(context.Background()); err != nil {
		t.Fatalf("expected healthy, got: %v", err)
	}
}

func TestCheckHealth_307TreatedAsHealthy(t *testing.T) {
	// Follower returns 307 — should be treated as healthy.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://leader:9000/health", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	c := NewClient("test", srv.URL)
	if err := c.CheckHealth(context.Background()); err != nil {
		t.Fatalf("expected 307 to be treated as healthy, got: %v", err)
	}
}

func TestCheckHealth_500TreatedAsUnhealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient("test", srv.URL)
	if err := c.CheckHealth(context.Background()); err == nil {
		t.Fatal("expected error on 500")
	}
}

func TestCheckHealth_Unreachable(t *testing.T) {
	c := NewClient("test", "http://127.0.0.1:1")
	if err := c.CheckHealth(context.Background()); err == nil {
		t.Fatal("expected error for unreachable host")
	}
}

func TestClientOptions_CustomRetryCount(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	c := NewClient("test", srv.URL, WithMaxRetries(1), WithRetryBaseDelay(10*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = c.Get(ctx, "/test", nil)
	// 1 initial + 1 retry = 2 calls
	if n := int(calls.Load()); n != 2 {
		t.Fatalf("expected 2 calls with maxRetries=1, got %d", n)
	}
}

func TestClientOptions_ZeroRetries(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient("test", srv.URL, WithMaxRetries(0))
	_ = c.Get(context.Background(), "/test", nil)
	if n := int(calls.Load()); n != 1 {
		t.Fatalf("expected 1 call with maxRetries=0, got %d", n)
	}
}
