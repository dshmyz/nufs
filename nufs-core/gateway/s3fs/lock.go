package s3fs

import (
	"time"
)

// pathLock represents a lock on a specific file path.
type pathLock struct {
	locked  bool
	waiters []chan struct{}
}

// Lock acquires a path-level lock. Blocks if already locked.
func (fs *S3FileSystem) Lock(path string) error {
	for {
		fs.mu.Lock()
		pl, ok := fs.locks[path]
		if !ok {
			fs.locks[path] = &pathLock{locked: true}
			fs.mu.Unlock()
			return nil
		}
		if !pl.locked {
			pl.locked = true
			fs.mu.Unlock()
			return nil
		}
		ch := make(chan struct{}, 1)
		pl.waiters = append(pl.waiters, ch)
		fs.mu.Unlock()
		<-ch
	}
}

// Unlock releases a path-level lock and wakes the next waiter.
func (fs *S3FileSystem) Unlock(path string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	pl, ok := fs.locks[path]
	if !ok {
		return
	}
	if len(pl.waiters) > 0 {
		next := pl.waiters[0]
		pl.waiters = pl.waiters[1:]
		close(next)
	} else {
		delete(fs.locks, path)
	}
}

// Wait blocks until the path is unlocked, with a 5-second timeout.
func (fs *S3FileSystem) Wait(path string) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		fs.mu.Lock()
		pl, ok := fs.locks[path]
		if !ok || !pl.locked {
			fs.mu.Unlock()
			return nil
		}
		ch := make(chan struct{}, 1)
		pl.waiters = append(pl.waiters, ch)
		fs.mu.Unlock()

		timeout := time.Until(deadline)
		if timeout <= 0 {
			return errTimeout
		}
		timer := time.NewTimer(timeout)
		select {
		case <-ch:
			timer.Stop()
		case <-timer.C:
			return errTimeout
		}
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


