// Package retry executes fallible operations with exponential backoff
// and jitter. It is lifted from the legacy MinFS code path and made
// package-agnostic; the original implementation came from MinIO Inc.
//
// The package exposes a small, opinionated retry policy plus a helper
// (IsRetryable) that classifies common transient errors. Callers that
// need richer policies can roll their own loop and reuse IsRetryable.
package retry

import (
	"errors"
	mathrand "math/rand"
	"net"
	"net/http"
	"time"
)

// Default limits used by the helper.
const (
	DefaultMaxRetries   = 3
	DefaultBaseDelay    = 500 * time.Millisecond
	DefaultMaxDelay     = 5 * time.Second
)

// Config controls Do.
type Config struct {
	// MaxAttempts is the total number of attempts including the
	// initial one. Default: 4 (1 initial + 3 retries).
	MaxAttempts int
	// BaseDelay is the initial backoff. Default: 500ms.
	BaseDelay time.Duration
	// MaxDelay caps the exponential growth. Default: 5s.
	MaxDelay time.Duration
	// IsRetryable, if non-nil, overrides the default classifier.
	IsRetryable func(error) bool
	// OnRetry, if non-nil, is called before each backoff sleep. The
	// attempt argument is the 1-based index of the attempt that just
	// failed (so 1 is the initial attempt).
	OnRetry func(attempt int, err error, nextDelay time.Duration)
}

func (c Config) withDefaults() Config {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = DefaultMaxRetries + 1
	}
	if c.BaseDelay <= 0 {
		c.BaseDelay = DefaultBaseDelay
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = DefaultMaxDelay
	}
	if c.IsRetryable == nil {
		c.IsRetryable = IsRetryable
	}
	return c
}

// Do executes fn with the given retry policy. It returns the final
// error (or nil) along with the number of attempts performed.
//
// Non-retryable errors are returned immediately. The first call counts
// as attempt 1, so with MaxAttempts=4 we get one initial call plus up
// to three retries.
func Do(cfg Config, fn func() error) (error, int) {
	cfg = cfg.withDefaults()
	var lastErr error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil, attempt
		}
		if !cfg.IsRetryable(lastErr) {
			return lastErr, attempt
		}
		if attempt == cfg.MaxAttempts {
			break
		}
		// Exponential backoff: base * 2^(attempt-1), capped at MaxDelay.
		delay := cfg.BaseDelay * time.Duration(1<<uint(attempt-1))
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
		// Jitter: ±25% of the delay.
		jitter := time.Duration(mathrand.Int63n(int64(delay/2))) - delay/4
		delay += jitter
		if cfg.OnRetry != nil {
			cfg.OnRetry(attempt, lastErr, delay)
		}
		time.Sleep(delay)
	}
	return lastErr, cfg.MaxAttempts
}

// HTTPStatusError is implemented by errors that carry an HTTP status
// code (e.g. *minio.ErrorResponse, *awserr.Error, or any wrapper).
type HTTPStatusError interface {
	error
	HTTPStatusCode() int
}

// IsRetryable returns true for transient errors: network timeouts,
// connection-level errors, and HTTP 429 / 5xx responses.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Network-level errors (timeouts, DNS failures, broken pipe).
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Connection refused / broken pipe.
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}

	// HTTP-level errors: inspect status code if available.
	var httpErr HTTPStatusError
	if errors.As(err, &httpErr) {
		switch httpErr.HTTPStatusCode() {
		case http.StatusTooManyRequests,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			return true
		}
	}

	return false
}
