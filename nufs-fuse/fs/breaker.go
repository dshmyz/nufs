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
	"sync"
	"sync/atomic"
	"time"
)

// Circuit breaker states.
const (
	cbClosed   = 0 // Normal operation; requests pass through.
	cbOpen     = 1 // Failing; requests are rejected immediately.
	cbHalfOpen = 2 // Probing; one request is allowed to test recovery.
)

// Sentinel error returned when the circuit breaker is open.
var errCircuitOpen = errors.New("minfs: S3 circuit breaker is open")

// circuitBreaker implements a simple three-state circuit breaker for S3
// backend calls. When consecutive failures exceed the threshold, the
// circuit opens and all subsequent calls fail fast. After a timeout the
// circuit transitions to half-open, allowing a single probe call. If that
// call succeeds the circuit closes; otherwise it re-opens.
type circuitBreaker struct {
	// Accessed via sync/atomic — must be first field for alignment on 32-bit.
	state        int64
	failures     int64
	lastFailTime int64 // unix-nano

	threshold int64         // consecutive failures before opening
	timeout   time.Duration // how long to wait before half-open probe

	mu sync.Mutex
}

// newCircuitBreaker creates a circuit breaker with the given threshold and
// recovery timeout.
func newCircuitBreaker(threshold int64, timeout time.Duration) *circuitBreaker {
	return &circuitBreaker{
		threshold: threshold,
		timeout:   timeout,
	}
}

// Execute runs fn through the circuit breaker.
//   - Closed: fn is called. On success, failure counter resets. On failure,
//     the counter increments and the circuit may open.
//   - Open: if the timeout has elapsed, transitions to half-open and allows
//     one probe call. Otherwise returns errCircuitOpen immediately.
//   - Half-open: if fn succeeds, closes the circuit. If it fails, re-opens.
func (cb *circuitBreaker) Execute(fn func() error) error {
	// Fast path: check state without locking.
	state := atomic.LoadInt64(&cb.state)

	if state == cbOpen {
		lastFail := time.Unix(0, atomic.LoadInt64(&cb.lastFailTime))
		if time.Since(lastFail) < cb.timeout {
			metricsIncS3Error()
			return errCircuitOpen
		}
		// Timeout elapsed — try half-open.
		cb.mu.Lock()
		// Double-check after acquiring lock.
		if atomic.LoadInt64(&cb.state) == cbOpen {
			atomic.StoreInt64(&cb.state, cbHalfOpen)
		}
		cb.mu.Unlock()
	}

	// Execute the actual operation.
	err := fn()

	if err != nil {
		cb.onFailure()
		return err
	}

	cb.onSuccess()
	return nil
}

// onSuccess resets the circuit after a successful call.
func (cb *circuitBreaker) onSuccess() {
	atomic.StoreInt64(&cb.failures, 0)
	atomic.StoreInt64(&cb.state, cbClosed)
}

// onFailure records a failure and potentially opens the circuit.
func (cb *circuitBreaker) onFailure() {
	n := atomic.AddInt64(&cb.failures, 1)
	atomic.StoreInt64(&cb.lastFailTime, time.Now().UnixNano())

	if n >= cb.threshold {
		atomic.StoreInt64(&cb.state, cbOpen)
	}
}

// IsOpen returns true if the circuit is currently open.
func (cb *circuitBreaker) IsOpen() bool {
	return atomic.LoadInt64(&cb.state) == cbOpen
}

// State returns a human-readable state name (for metrics/debugging).
func (cb *circuitBreaker) State() string {
	switch atomic.LoadInt64(&cb.state) {
	case cbClosed:
		return "closed"
	case cbOpen:
		return "open"
	case cbHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}
