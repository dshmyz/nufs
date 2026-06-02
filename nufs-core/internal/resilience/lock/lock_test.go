package lock

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLockUnlock(t *testing.T) {
	m := New()
	if err := m.Lock("a"); err != nil {
		t.Fatalf("Lock a: %v", err)
	}
	if !m.IsLocked("a") {
		t.Fatal("a should be locked")
	}
	m.Unlock("a")
	if m.IsLocked("a") {
		t.Fatal("a should be unlocked")
	}
}

func TestLockBlocksConcurrent(t *testing.T) {
	m := New()
	if err := m.Lock("shared"); err != nil {
		t.Fatalf("Lock: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		_ = m.Lock("shared")
		close(acquired)
		m.Unlock("shared")
	}()

	select {
	case <-acquired:
		t.Fatal("Lock should have blocked")
	case <-time.After(50 * time.Millisecond):
	}

	m.Unlock("shared")
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("Lock should have unblocked")
	}
}

func TestLockDifferentKeys(t *testing.T) {
	m := New()
	if err := m.Lock("a"); err != nil {
		t.Fatalf("Lock a: %v", err)
	}
	// b should be lockable while a is held.
	done := make(chan struct{})
	go func() {
		_ = m.Lock("b")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Lock b should have succeeded while a is held")
	}
	m.Unlock("a")
	m.Unlock("b")
}

func TestLockFIFO(t *testing.T) {
	m := New()
	_ = m.Lock("key")

	const n = 3
	var order []int32
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(n)
	for i := int32(0); i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			_ = m.Lock("key")
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			m.Unlock("key")
		}()
		time.Sleep(5 * time.Millisecond) // ensure registration order
	}
	m.Unlock("key")
	wg.Wait()

	if len(order) != n {
		t.Fatalf("expected %d entries, got %d", n, len(order))
	}
	// All entries should be unique.
	seen := make(map[int32]bool, n)
	for _, v := range order {
		if seen[v] {
			t.Fatalf("duplicate waiter %d in order %v", v, order)
		}
		seen[v] = true
	}
}

func TestLockContext_Timeout(t *testing.T) {
	m := New()
	_ = m.Lock("held")
	defer m.Unlock("held")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := m.LockContext(ctx, "held")
	if err != ErrTimeout {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
	// The original holder is still in possession of the lock; the
	// timed-out caller must NOT have acquired it, and must have been
	// removed from the waiter queue.
	if !m.IsLocked("held") {
		t.Fatal("held should still be locked by the original holder")
	}
	if got := m.Waiters("held"); got != 0 {
		t.Fatalf("expected timed-out waiter to be removed, got %d waiters", got)
	}
}

func TestLockContext_DeadlineZero(t *testing.T) {
	m := New()
	_ = m.Lock("held")
	defer m.Unlock("held")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := m.LockContext(ctx, "held")
	if err == nil {
		t.Fatal("expected error from already-cancelled context")
	}
}

func TestUnlockUnheld(t *testing.T) {
	m := New()
	// Should not panic.
	m.Unlock("never-held")
	m.Unlock("never-held")
}

func TestWaitersCounter(t *testing.T) {
	m := New()
	_ = m.Lock("k")
	var wg sync.WaitGroup
	var observed atomic.Int32
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.Lock("k")
			observed.Add(1)
			m.Unlock("k")
		}()
	}
	// Give waiters time to register. All 5 spawned goroutines should
	// be parked in the waiter queue (the main holder doesn't count).
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) && m.Waiters("k") != 5 {
		time.Sleep(2 * time.Millisecond)
	}
	if got := m.Waiters("k"); got != 5 {
		t.Fatalf("expected 5 waiters, got %d", got)
	}
	m.Unlock("k")
	wg.Wait()
	if got := observed.Load(); got != 5 {
		t.Fatalf("expected 5 successful acquirers, got %d", got)
	}
}
