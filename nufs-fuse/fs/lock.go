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
	"time"

	"bazil.org/fuse"
)

// pathLock represents a lock on a specific file path.
type pathLock struct {
	locked  bool
	waiters []chan struct{}
}

// Lock acquires a lock for the given path. If the path is already locked,
// Lock blocks until the lock becomes available.
func (mfs *MinFS) Lock(path string) error {
	for {
		mfs.m.Lock()
		pl, ok := mfs.locks[path]
		if !ok {
			mfs.locks[path] = &pathLock{locked: true}
			mfs.m.Unlock()
			return nil
		}
		if !pl.locked {
			pl.locked = true
			mfs.m.Unlock()
			return nil
		}
		// Path is currently locked — register as waiter and block outside m.
		ch := make(chan struct{}, 1)
		pl.waiters = append(pl.waiters, ch)
		mfs.m.Unlock()

		<-ch
		// Woken up: loop back and try to acquire.
	}
}

// Unlock releases the lock for the given path and wakes the next waiter.
func (mfs *MinFS) Unlock(path string) error {
	mfs.m.Lock()
	defer mfs.m.Unlock()

	pl, ok := mfs.locks[path]
	if !ok {
		return nil
	}

	if len(pl.waiters) > 0 {
		// Hand off directly to the first waiter (stays locked).
		next := pl.waiters[0]
		pl.waiters = pl.waiters[1:]
		close(next)
	} else {
		delete(mfs.locks, path)
	}
	return nil
}

// IsLocked returns true if the path is currently locked.
func (mfs *MinFS) IsLocked(path string) bool {
	mfs.m.Lock()
	defer mfs.m.Unlock()

	pl, ok := mfs.locks[path]
	return ok && pl.locked
}

// wait blocks until the given path is unlocked, with a maximum timeout of 5 seconds.
func (mfs *MinFS) wait(path string) error {
	deadline := time.Now().Add(5 * time.Second)

	for {
		mfs.m.Lock()
		pl, ok := mfs.locks[path]
		if !ok || !pl.locked {
			mfs.m.Unlock()
			return nil // not locked, proceed
		}

		// Register as waiter before releasing mfs.m to avoid missed-wakeup race.
		ch := make(chan struct{}, 1)
		pl.waiters = append(pl.waiters, ch)
		mfs.m.Unlock()

		timeout := time.Until(deadline)
		if timeout <= 0 {
			return fuse.EPERM
		}

		timer := time.NewTimer(timeout)
		select {
		case <-ch:
			timer.Stop()
			// Woken up — re-check in next iteration.
		case <-timer.C:
			return fuse.EPERM
		}
	}
}
