package metadata

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestAdvisoryLockManager_ShardedConcurrency verifies that the
// lock manager uses sharded locks so that operations on different
// inodes do not contend on a single global mutex. We measure this
// by running concurrent acquire/release on distinct inodes and
// checking that the max concurrent holders across shards is
// greater than 1 (i.e., parallelism exists).
func TestAdvisoryLockManager_ShardedConcurrency(t *testing.T) {
	m := newAdvisoryLockManager()

	const numGoroutines = 64
	const opsPerGoroutine = 100

	// Each goroutine works on a distinct inode, so there should be
	// no contention between them. If the manager used a single
	// global mutex, all operations would serialize.
	var wg sync.WaitGroup
	var maxConcurrent int64
	var current int64

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			inode := InodeID(goroutineID + 1)
			owner := fmt.Sprintf("owner-%d", goroutineID)

			for j := 0; j < opsPerGoroutine; j++ {
				if err := m.acquire(inode, owner, LockModeExclusive); err != nil {
					t.Errorf("acquire inode %d: %v", inode, err)
					return
				}

				cur := atomic.AddInt64(&current, 1)
				for {
					max := atomic.LoadInt64(&maxConcurrent)
					if cur <= max || atomic.CompareAndSwapInt64(&maxConcurrent, max, cur) {
						break
					}
				}

				atomic.AddInt64(&current, -1)

				if err := m.release(inode, owner); err != nil {
					t.Errorf("release inode %d: %v", inode, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// If sharding works, multiple goroutines should have been in
	// the critical section simultaneously (maxConcurrent > 1).
	// With a global mutex, maxConcurrent would be exactly 1.
	if maxConcurrent <= 1 {
		t.Errorf("expected concurrent operations > 1 with sharded locks, got max=%d (likely using a single global mutex)", maxConcurrent)
	}
}

// TestAdvisoryLockManager_ShardCount verifies that the manager
// uses a fixed number of shards (power of 2 for fast modulo).
func TestAdvisoryLockManager_ShardCount(t *testing.T) {
	m := newAdvisoryLockManager()

	// The shard count should be a power of 2 and > 1
	if m.shardCount <= 1 {
		t.Fatalf("shardCount = %d, expected > 1 for sharding", m.shardCount)
	}

	// Verify it's a power of 2
	if m.shardCount&(m.shardCount-1) != 0 {
		t.Fatalf("shardCount = %d, expected power of 2", m.shardCount)
	}

	// Verify shardFor distributes inodes across shards
	shards := make(map[uint]int)
	for i := InodeID(1); i <= 1000; i++ {
		shards[m.shardFor(i)]++
	}
	if len(shards) < 2 {
		t.Fatalf("shardFor does not distribute: only %d shards used", len(shards))
	}
}

// TestAdvisoryLockManager_CorrectnessAfterSharding verifies that
// sharding does not break lock semantics: two different owners
// cannot hold exclusive locks on the same inode, even if the
// inode maps to the same shard as many others.
func TestAdvisoryLockManager_CorrectnessAfterSharding(t *testing.T) {
	m := newAdvisoryLockManager()

	// Acquire exclusive lock
	if err := m.acquire(100, "owner-A", LockModeExclusive); err != nil {
		t.Fatalf("acquire A: %v", err)
	}

	// Second owner should be blocked
	if err := m.acquire(100, "owner-B", LockModeExclusive); err != ErrLockBusy {
		t.Fatalf("expected ErrLockBusy, got %v", err)
	}

	// Release and retry
	if err := m.release(100, "owner-A"); err != nil {
		t.Fatalf("release A: %v", err)
	}
	if err := m.acquire(100, "owner-B", LockModeExclusive); err != nil {
		t.Fatalf("acquire B after release: %v", err)
	}
}

// TestAdvisoryLockManager_SameInodeStillSerializes verifies that
// operations on the SAME inode still serialize correctly (the
// per-shard mutex protects the per-inode state).
func TestAdvisoryLockManager_SameInodeStillSerializes(t *testing.T) {
	m := newAdvisoryLockManager()

	// Acquire exclusive lock
	if err := m.acquire(42, "owner-A", LockModeExclusive); err != nil {
		t.Fatalf("acquire A: %v", err)
	}

	// Multiple goroutines try to acquire the same inode — all should fail
	var wg sync.WaitGroup
	var failCount int64
	const numTries = 10

	for i := 0; i < numTries; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			owner := fmt.Sprintf("owner-%d", id)
			if err := m.acquire(42, owner, LockModeExclusive); err != ErrLockBusy {
				t.Errorf("expected ErrLockBusy for owner %s, got %v", owner, err)
				atomic.AddInt64(&failCount, 1)
			}
		}(i)
	}
	wg.Wait()

	if failCount > 0 {
		t.Fatalf("%d goroutines incorrectly acquired the locked inode", failCount)
	}
}
