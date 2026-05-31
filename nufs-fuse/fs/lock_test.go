// Copyright (c) 2021 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// +build linux

package minfs

import (
	"sync"
	"testing"
	"time"
)

// newTestMinFS returns a minimal MinFS struct for lock tests.
func newTestMinFS() *MinFS {
	return &MinFS{
		locks: map[string]*pathLock{},
	}
}

func TestLockUnlock(t *testing.T) {
	mfs := newTestMinFS()

	if err := mfs.Lock("/file1"); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	if !mfs.IsLocked("/file1") {
		t.Fatal("expected /file1 to be locked")
	}
	if err := mfs.Unlock("/file1"); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
	if mfs.IsLocked("/file1") {
		t.Fatal("expected /file1 to be unlocked")
	}
}

func TestLockBlocksConcurrentAccess(t *testing.T) {
	mfs := newTestMinFS()

	// Acquire lock in main goroutine.
	if err := mfs.Lock("/shared"); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	acquired := make(chan bool, 1)
	go func() {
		// This should block until Lock is released.
		mfs.Lock("/shared")
		acquired <- true
		mfs.Unlock("/shared")
	}()

	// Verify the goroutine is blocked.
	select {
	case <-acquired:
		t.Fatal("second Lock should have blocked")
	case <-time.After(100 * time.Millisecond):
		// Expected: still blocked.
	}

	// Release and verify the goroutine proceeds.
	mfs.Unlock("/shared")
	select {
	case <-acquired:
		// OK
	case <-time.After(1 * time.Second):
		t.Fatal("second Lock should have been unblocked after Unlock")
	}
}

func TestLockDifferentPaths(t *testing.T) {
	mfs := newTestMinFS()

	if err := mfs.Lock("/a"); err != nil {
		t.Fatalf("Lock /a failed: %v", err)
	}
	// Lock on a different path should succeed immediately.
	if err := mfs.Lock("/b"); err != nil {
		t.Fatalf("Lock /b should succeed: %v", err)
	}
	mfs.Unlock("/a")
	mfs.Unlock("/b")
}

func TestWaitTimeout(t *testing.T) {
	mfs := newTestMinFS()

	// Hold the lock for the entire test.
	mfs.Lock("/held")

	// wait() should time out (reduced timeout for test speed).
	done := make(chan error, 1)
	go func() {
		done <- mfs.wait("/held")
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("wait should have timed out")
		}
	case <-time.After(7 * time.Second):
		t.Fatal("wait should have returned within 5s timeout")
	}

	mfs.Unlock("/held")
}

func TestLockMultipleWaiters(t *testing.T) {
	mfs := newTestMinFS()
	mfs.Lock("/resource")

	var mu sync.Mutex
	order := []int{}

	for i := 0; i < 3; i++ {
		i := i
		go func() {
			mfs.Lock("/resource")
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			mfs.Unlock("/resource")
		}()
		// Small delay to ensure ordering of waiter registration.
		time.Sleep(10 * time.Millisecond)
	}

	// Release the lock — all 3 waiters should eventually acquire and release.
	mfs.Unlock("/resource")
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 {
		t.Fatalf("expected 3 waiters to complete, got %d", len(order))
	}
}
