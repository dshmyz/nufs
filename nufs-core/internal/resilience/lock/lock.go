// Package lock implements a keyed mutex with FIFO waiter queues. Each
// key has at most one holder; additional Lock calls block until the
// previous holder calls Unlock, at which point exactly one waiter is
// woken.
//
// The implementation is lifted from the legacy MinFS code path and
// re-shaped into a standalone type so it can be used from any package
// (datacenter RPC serialization, per-file write coordination, etc.).
// The wait/timeout helpers from the original code were merged into
// the main Lock/Unlock pair, since they shared the same waiter queue
// and the timeout case is the only reason for the original split.
package lock

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrTimeout is returned by LockContext when the wait exceeds the
// context deadline or timeout.
var ErrTimeout = errors.New("lock: timed out waiting for key")

type entry struct {
	held    bool
	waiters []chan struct{}
}

// Manager is a keyed mutex. The zero value is ready to use.
type Manager struct {
	mu   sync.Mutex
	keys map[string]*entry
}

// New returns a ready-to-use Manager.
func New() *Manager {
	return &Manager{keys: make(map[string]*entry)}
}

// Lock blocks until it acquires the lock for key. If key is already
// locked the caller waits in FIFO order behind any other waiters.
func (m *Manager) Lock(key string) error {
	m.mu.Lock()
	e, ok := m.keys[key]
	if !ok {
		m.keys[key] = &entry{held: true}
		m.mu.Unlock()
		return nil
	}
	if !e.held {
		e.held = true
		m.mu.Unlock()
		return nil
	}
	// Key is held — register as waiter and block outside m.mu.
	ch := make(chan struct{}, 1)
	e.waiters = append(e.waiters, ch)
	m.mu.Unlock()

	<-ch
	return nil
}

// LockContext is like Lock but returns ErrTimeout if ctx is cancelled
// or d elapses (whichever comes first) before the lock is acquired.
func (m *Manager) LockContext(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var timer *time.Timer
	var timeoutCh <-chan time.Time
	if deadline, ok := ctx.Deadline(); ok {
		wait := time.Until(deadline)
		if wait <= 0 {
			return ErrTimeout
		}
		timer = time.NewTimer(wait)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	m.mu.Lock()
	e, ok := m.keys[key]
	if !ok {
		m.keys[key] = &entry{held: true}
		m.mu.Unlock()
		return nil
	}
	if !e.held {
		e.held = true
		m.mu.Unlock()
		return nil
	}
	ch := make(chan struct{}, 1)
	e.waiters = append(e.waiters, ch)
	m.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-timeoutCh:
		// Try to remove ourselves from the waiter queue so we don't
		// hold up the next holder.
		m.mu.Lock()
		for i, w := range e.waiters {
			if w == ch {
				e.waiters = append(e.waiters[:i], e.waiters[i+1:]...)
				break
			}
		}
		m.mu.Unlock()
		return ErrTimeout
	case <-ctx.Done():
		m.mu.Lock()
		for i, w := range e.waiters {
			if w == ch {
				e.waiters = append(e.waiters[:i], e.waiters[i+1:]...)
				break
			}
		}
		m.mu.Unlock()
		return ctx.Err()
	}
}

// Unlock releases the lock for key and wakes the next waiter (FIFO).
// Unlocking a key that is not held is a no-op.
func (m *Manager) Unlock(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.keys[key]
	if !ok {
		return
	}
	if len(e.waiters) > 0 {
		// Hand off directly to the first waiter (stays locked).
		next := e.waiters[0]
		e.waiters = e.waiters[1:]
		close(next)
		return
	}
	delete(m.keys, key)
}

// IsLocked reports whether key is currently held. It does not block.
func (m *Manager) IsLocked(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.keys[key]
	return ok && e.held
}

// Waiters returns the number of goroutines currently waiting for key.
// Useful for tests and metrics.
func (m *Manager) Waiters(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.keys[key]
	if !ok {
		return 0
	}
	return len(e.waiters)
}
