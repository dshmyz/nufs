// Package breaker implements a three-state circuit breaker for outbound
// RPC calls (typically S3 / datanode clients). It is lifted from the
// legacy MinFS code path and re-licensed under the project's normal
// terms; the original implementation came from MinIO Inc. (AGPLv3).
//
// Lifted from nufs-fuse/fs/breaker.go and made package-agnostic:
//   - dropped the metricsIncS3Error() global side effect;
//   - added an optional OnStateChange hook so callers can wire
//     metrics / logging however they like.
package breaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// State represents the current state of a circuit breaker.
type State int64

const (
	// StateClosed is the normal operating state: requests pass through.
	StateClosed State = 0
	// StateOpen rejects all requests immediately; entered once the
	// consecutive-failure counter reaches the configured threshold.
	StateOpen State = 1
	// StateHalfOpen allows a single probe request through. If that
	// probe succeeds the circuit closes; if it fails it re-opens.
	StateHalfOpen State = 2
)

// String returns a human-readable name for the state.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// ErrOpen is returned by Execute when the circuit is open and the
// recovery timeout has not elapsed.
var ErrOpen = errors.New("breaker: circuit is open")

// Config configures a Breaker. Zero values are replaced with sensible
// defaults by New.
type Config struct {
	// Threshold is the number of consecutive failures that opens the
	// circuit. Default: 5.
	Threshold int64
	// Timeout is how long the breaker stays open before transitioning
	// to half-open. Default: 30s.
	Timeout time.Duration
	// OnStateChange, if non-nil, is invoked whenever the breaker
	// transitions to a new state. It is called synchronously and
	// should not block for long.
	OnStateChange func(name string, from, to State)
}

// Breaker is a circuit breaker for a single named dependency.
type Breaker struct {
	name string

	threshold int64
	timeout   time.Duration
	onChange  func(name string, from, to State)

	// Accessed via sync/atomic — must be first field for alignment on 32-bit.
	state        int64
	failures     int64
	lastFailTime int64 // unix-nano

	mu sync.Mutex
}

// New constructs a Breaker with the given config. The name is used in
// callbacks and error messages.
func New(name string, cfg Config) *Breaker {
	if cfg.Threshold <= 0 {
		cfg.Threshold = 5
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &Breaker{
		name:      name,
		threshold: cfg.Threshold,
		timeout:   cfg.Timeout,
		onChange:  cfg.OnStateChange,
	}
}

// Execute runs fn through the circuit breaker.
//
//   - StateClosed: fn is called. On success the failure counter resets.
//     On failure the counter increments and the circuit may open.
//   - StateOpen: if the recovery timeout has elapsed the breaker
//     transitions to half-open and one probe call is allowed through.
//     Otherwise ErrOpen is returned immediately.
//   - StateHalfOpen: the single in-flight probe fn runs. Success
//     closes the circuit; failure re-opens it.
func (b *Breaker) Execute(fn func() error) error {
	state := State(atomic.LoadInt64(&b.state))

	if state == StateOpen {
		lastFail := time.Unix(0, atomic.LoadInt64(&b.lastFailTime))
		if time.Since(lastFail) < b.timeout {
			return ErrOpen
		}
		// Timeout elapsed — try half-open.
		b.mu.Lock()
		if State(atomic.LoadInt64(&b.state)) == StateOpen {
			b.transitionLocked(StateHalfOpen)
		}
		b.mu.Unlock()
	}

	err := fn()
	if err != nil {
		b.onFailure()
		return err
	}
	b.onSuccess()
	return nil
}

// onSuccess resets the failure counter and closes the circuit.
func (b *Breaker) onSuccess() {
	atomic.StoreInt64(&b.failures, 0)
	b.transition(StateClosed)
}

// onFailure records a failure and may open the circuit.
func (b *Breaker) onFailure() {
	n := atomic.AddInt64(&b.failures, 1)
	atomic.StoreInt64(&b.lastFailTime, time.Now().UnixNano())

	if n >= b.threshold {
		b.transition(StateOpen)
	}
}

// transition moves the breaker to a new state and invokes the
// OnStateChange callback when the state actually changes.
func (b *Breaker) transition(to State) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.transitionLocked(to)
}

// transitionLocked must be called with b.mu held.
func (b *Breaker) transitionLocked(to State) {
	from := State(atomic.LoadInt64(&b.state))
	if from == to {
		return
	}
	atomic.StoreInt64(&b.state, int64(to))
	if b.onChange != nil {
		b.onChange(b.name, from, to)
	}
}

// IsOpen reports whether the circuit is currently open.
func (b *Breaker) IsOpen() bool {
	return State(atomic.LoadInt64(&b.state)) == StateOpen
}

// State returns the current state of the breaker.
func (b *Breaker) State() State {
	return State(atomic.LoadInt64(&b.state))
}

// Failures returns the current consecutive-failure counter. Useful for
// tests and for /metrics handlers.
func (b *Breaker) Failures() int64 {
	return atomic.LoadInt64(&b.failures)
}
