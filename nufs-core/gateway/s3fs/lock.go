package s3fs

import (
	"time"
)

const defaultLockWaitTimeout = 5 * time.Second

// pathLock represents a lock on a specific file path.
// Lock and Wait use separate waiter queues so that Unlock can transfer
// ownership to the next Lock waiter (FIFO) while still notifying Wait
// callers that the path was released.
type pathLock struct {
	locked      bool
	lockWaiters []chan struct{} // FIFO queue for Lock callers
	waitWaiters []chan struct{} // notification queue for Wait callers
}

// Lock acquires a path-level lock. Blocks if already locked, queuing
// in FIFO order. When Unlock transfers ownership, the first Lock waiter
// receives the lock directly (no race with other goroutines).
func (fs *S3FileSystem) Lock(path string) error {
	fs.mu.Lock()
	pl, ok := fs.locks[path]
	if !ok {
		fs.locks[path] = &pathLock{locked: true}
		fs.mu.Unlock()
		return nil
	}
	if !pl.locked && len(pl.lockWaiters) == 0 {
		pl.locked = true
		fs.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	pl.lockWaiters = append(pl.lockWaiters, ch)
	fs.mu.Unlock()
	<-ch
	return nil
}

// Unlock releases a path-level lock. If Lock waiters are queued the
// lock is transferred directly to the first waiter (FIFO). All Wait
// callers are notified (they do not take ownership).
func (fs *S3FileSystem) Unlock(path string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	pl, ok := fs.locks[path]
	if !ok {
		return
	}

	// Notify all Wait callers that the lock status changed.
	for _, ch := range pl.waitWaiters {
		close(ch)
	}
	pl.waitWaiters = nil

	if len(pl.lockWaiters) > 0 {
		// Transfer lock to the first FIFO waiter.
		next := pl.lockWaiters[0]
		pl.lockWaiters = pl.lockWaiters[1:]
		// locked stays true — the waiter inherits the lock.
		close(next)
	} else {
		pl.locked = false
		fs.locks[path] = pl
	}
}

// Wait blocks until the path is no longer locked, with a configurable
// timeout. Unlike Lock, the caller does not hold the path lock after
// Wait returns.
func (fs *S3FileSystem) Wait(path string) error {
	timeout := fs.lockWait
	if timeout <= 0 {
		timeout = defaultLockWaitTimeout
	}

	fs.mu.Lock()
	pl, ok := fs.locks[path]
	if !ok || !pl.locked {
		fs.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	pl.waitWaiters = append(pl.waitWaiters, ch)
	fs.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case <-timer.C:
		return errTimeout
	}
}

// IsLocked returns true if the path is currently locked.
func (fs *S3FileSystem) IsLocked(path string) bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	pl, ok := fs.locks[path]
	return ok && pl.locked
}

// newLockMap creates an initialized lock map.
func newLockMap() map[string]*pathLock {
	return make(map[string]*pathLock)
}
