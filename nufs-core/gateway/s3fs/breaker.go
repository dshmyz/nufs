package s3fs

import (
	"sync"
	"time"
)

type circuitBreakerState int

const (
	stateClosed   circuitBreakerState = iota
	stateOpen
	stateHalfOpen
)

type circuitBreaker struct {
	mu           sync.Mutex
	state        circuitBreakerState
	failures     int
	threshold    int
	resetTimeout time.Duration
	lastFailure  time.Time
}

func newCircuitBreaker(threshold int, resetTimeout time.Duration) *circuitBreaker {
	return &circuitBreaker{
		state:        stateClosed,
		threshold:    threshold,
		resetTimeout: resetTimeout,
	}
}

func (cb *circuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	if cb.state == stateOpen {
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.state = stateHalfOpen
		} else {
			cb.mu.Unlock()
			return errCircuitOpen
		}
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()
	if err != nil {
		cb.failures++
		cb.lastFailure = time.Now()
		if cb.failures >= cb.threshold {
			cb.state = stateOpen
		}
		return err
	}
	cb.failures = 0
	cb.state = stateClosed
	return nil
}

func (cb *circuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case stateClosed:
		return "closed"
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half-open"
	}
	return "unknown"
}

var errCircuitOpen = &circuitOpenError{}

type circuitOpenError struct{}

func (e *circuitOpenError) Error() string {
	return "circuit breaker is open"
}
