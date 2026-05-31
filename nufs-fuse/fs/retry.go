// Copyright (c) 2021 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package minfs

import (
	"errors"
	mathrand "math/rand"
	"net"
	"net/http"
	"time"
)

const (
	// maxRetries is the maximum number of retries for transient S3 errors.
	maxRetries = 3

	// retryBaseDelay is the initial backoff delay.
	retryBaseDelay = 500 * time.Millisecond

	// retryMaxDelay caps the exponential growth.
	retryMaxDelay = 5 * time.Second
)

// retryWithBackoff executes fn with exponential backoff and jitter.
// It retries up to maxRetries times for transient/retryable errors.
// Non-retryable errors are returned immediately.
func retryWithBackoff(fn func() error) error {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if !isRetryableError(lastErr) {
			return lastErr
		}
		if attempt == maxRetries {
			break
		}
		metricsIncS3Retry()
		// Exponential backoff: base * 2^attempt, capped at retryMaxDelay.
		delay := retryBaseDelay * time.Duration(1<<uint(attempt))
		if delay > retryMaxDelay {
			delay = retryMaxDelay
		}
		// Add jitter: ±25% of the delay.
		jitter := time.Duration(mathrand.Int63n(int64(delay/2))) - delay/4
		delay += jitter
		time.Sleep(delay)
	}
	return lastErr
}

// isRetryableError returns true for transient errors that are worth retrying:
// network timeouts, temporary server errors (5xx), and rate limiting (429).
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Network-level errors (timeouts, connection reset, DNS failures).
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
	type httpStatusError interface {
		HTTPStatusCode() int
	}
	var httpErr httpStatusError
	if errors.As(err, &httpErr) {
		code := httpErr.HTTPStatusCode()
		// Retry on 429 (rate limit), 500, 502, 503, 504.
		if code == http.StatusTooManyRequests ||
			code == http.StatusInternalServerError ||
			code == http.StatusBadGateway ||
			code == http.StatusServiceUnavailable ||
			code == http.StatusGatewayTimeout {
			return true
		}
	}

	return false
}
