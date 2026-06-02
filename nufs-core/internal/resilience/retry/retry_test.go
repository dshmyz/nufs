package retry

import (
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestDo_SuccessFirstTry(t *testing.T) {
	calls := 0
	err, attempts := Do(Config{}, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 1 || attempts != 1 {
		t.Fatalf("expected 1 call / 1 attempt, got %d / %d", calls, attempts)
	}
}

func TestDo_NonRetryableError(t *testing.T) {
	perm := errors.New("permission denied")
	calls := 0
	err, _ := Do(Config{}, func() error {
		calls++
		return perm
	})
	if !errors.Is(err, perm) {
		t.Fatalf("expected perm, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected no retries, got %d calls", calls)
	}
}

func TestDo_RetryableErrorEventualSuccess(t *testing.T) {
	calls := 0
	err, attempts := Do(Config{
		BaseDelay: 1 * time.Millisecond,
		MaxDelay:  5 * time.Millisecond,
	}, func() error {
		calls++
		if calls < 3 {
			return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 3 || attempts != 3 {
		t.Fatalf("expected 3 calls / 3 attempts, got %d / %d", calls, attempts)
	}
}

func TestDo_ExhaustsRetries(t *testing.T) {
	retryable := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")}
	calls := 0
	err, attempts := Do(Config{
		BaseDelay: 1 * time.Millisecond,
		MaxDelay:  2 * time.Millisecond,
	}, func() error {
		calls++
		return retryable
	})
	if !errors.Is(err, retryable) {
		t.Fatalf("expected retryable, got %v", err)
	}
	if calls != DefaultMaxRetries+1 || attempts != DefaultMaxRetries+1 {
		t.Fatalf("expected %d calls, got %d", DefaultMaxRetries+1, calls)
	}
}

func TestDo_OnRetryHook(t *testing.T) {
	hookCalls := 0
	_, _ = Do(Config{
		BaseDelay: 1 * time.Millisecond,
		MaxDelay:  2 * time.Millisecond,
		OnRetry:   func(_ int, _ error, _ time.Duration) { hookCalls++ },
	}, func() error {
		return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")}
	})
	if hookCalls != DefaultMaxRetries {
		t.Fatalf("expected OnRetry called %d times, got %d", DefaultMaxRetries, hookCalls)
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"generic", errors.New("random"), false},
		{"net.OpError", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("reset")}, true},
		{"net.Error timeout", &timeoutErr{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRetryable(tt.err); got != tt.want {
				t.Fatalf("IsRetryable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

type httpErr struct{ code int }

func (e httpErr) Error() string       { return http.StatusText(e.code) }
func (e httpErr) HTTPStatusCode() int { return e.code }

func TestIsRetryable_HTTP(t *testing.T) {
	retryable := []int{429, 500, 502, 503, 504}
	notRetryable := []int{400, 401, 403, 404, 409}
	for _, code := range retryable {
		if !IsRetryable(httpErr{code: code}) {
			t.Errorf("HTTP %d should be retryable", code)
		}
	}
	for _, code := range notRetryable {
		if IsRetryable(httpErr{code: code}) {
			t.Errorf("HTTP %d should NOT be retryable", code)
		}
	}
}
