package metadata

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- happy path ---

func TestAdvisoryLock_Exclusive_AcquireAndRelease(t *testing.T) {
	m := newAdvisoryLockManager()

	if err := m.acquire(42, "fusegw-1", LockModeExclusive); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if err := m.release(42, "fusegw-1"); err != nil {
		t.Fatalf("release: %v", err)
	}
	// After release the lock should be free; a second owner can take it.
	if err := m.acquire(42, "s3gw-1", LockModeExclusive); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
}

func TestAdvisoryLock_Shared_MultipleHoldersCoexist(t *testing.T) {
	m := newAdvisoryLockManager()
	for _, owner := range []string{"fusegw-A", "fusegw-B", "dfsctl-X"} {
		if err := m.acquire(7, owner, LockModeShared); err != nil {
			t.Fatalf("acquire shared %s: %v", owner, err)
		}
	}
	holders := m.list(7)
	if len(holders) != 3 {
		t.Fatalf("expected 3 holders, got %d", len(holders))
	}
	// All must be shared.
	for _, h := range holders {
		if h.Mode != LockModeShared {
			t.Errorf("holder %s has mode %d, want shared (%d)", h.Owner, h.Mode, LockModeShared)
		}
	}
}

func TestAdvisoryLock_Reentrant_SameOwnerBumpsRefcount(t *testing.T) {
	m := newAdvisoryLockManager()

	// Take the same lock 3 times.
	for i := 0; i < 3; i++ {
		if err := m.acquire(99, "fusegw-1", LockModeExclusive); err != nil {
			t.Fatalf("acquire #%d: %v", i, err)
		}
	}
	// Release twice — should still be held.
	m.release(99, "fusegw-1")
	m.release(99, "fusegw-1")
	// Another owner should still see it busy.
	if err := m.acquire(99, "s3gw-1", LockModeExclusive); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("expected ErrLockBusy while same owner still holds, got %v", err)
	}
	// Final release frees the lock.
	m.release(99, "fusegw-1")
	if err := m.acquire(99, "s3gw-1", LockModeExclusive); err != nil {
		t.Fatalf("acquire after final release: %v", err)
	}
}

// --- conflict matrix ---

func TestAdvisoryLock_ExclusiveBlocksExclusive(t *testing.T) {
	m := newAdvisoryLockManager()
	if err := m.acquire(1, "A", LockModeExclusive); err != nil {
		t.Fatal(err)
	}
	if err := m.acquire(1, "B", LockModeExclusive); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("expected ErrLockBusy, got %v", err)
	}
}

func TestAdvisoryLock_ExclusiveBlocksShared(t *testing.T) {
	m := newAdvisoryLockManager()
	if err := m.acquire(1, "A", LockModeExclusive); err != nil {
		t.Fatal(err)
	}
	if err := m.acquire(1, "B", LockModeShared); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("expected ErrLockBusy, got %v", err)
	}
}

func TestAdvisoryLock_SharedBlocksExclusive(t *testing.T) {
	m := newAdvisoryLockManager()
	if err := m.acquire(1, "A", LockModeShared); err != nil {
		t.Fatal(err)
	}
	if err := m.acquire(1, "B", LockModeExclusive); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("expected ErrLockBusy, got %v", err)
	}
}

func TestAdvisoryLock_OwnerMismatchReleaseIsNoOp(t *testing.T) {
	m := newAdvisoryLockManager()
	if err := m.acquire(1, "A", LockModeExclusive); err != nil {
		t.Fatal(err)
	}
	// B is not a holder — release must succeed silently, not free A's lock.
	if err := m.release(1, "B"); err != nil {
		t.Fatalf("non-holder release: %v", err)
	}
	// A's lock is still in place; the inode should still appear locked.
	if err := m.acquire(1, "C", LockModeExclusive); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("expected ErrLockBusy, A's lock was stolen by B's release: %v", err)
	}
}

func TestAdvisoryLock_EmptyOwnerRejected(t *testing.T) {
	m := newAdvisoryLockManager()
	if err := m.acquire(1, "", LockModeExclusive); !errors.Is(err, ErrInvalidOwner) {
		t.Fatalf("acquire: expected ErrInvalidOwner, got %v", err)
	}
	if err := m.acquire(1, "", LockModeShared); !errors.Is(err, ErrInvalidOwner) {
		t.Fatalf("acquire shared: expected ErrInvalidOwner, got %v", err)
	}
	if err := m.release(1, ""); !errors.Is(err, ErrInvalidOwner) {
		t.Fatalf("release: expected ErrInvalidOwner, got %v", err)
	}
}

func TestAdvisoryLock_ListSortedAndEmpty(t *testing.T) {
	m := newAdvisoryLockManager()
	// No holders — list returns nil, not a zero-length slice.
	if got := m.list(1); got != nil {
		t.Errorf("list on empty inode: expected nil, got %v", got)
	}
	// Add a few and confirm sort order.
	for _, owner := range []string{"charlie", "alpha", "bravo"} {
		_ = m.acquire(1, owner, LockModeShared)
	}
	got := m.list(1)
	if len(got) != 3 {
		t.Fatalf("expected 3 holders, got %d", len(got))
	}
	want := []string{"alpha", "bravo", "charlie"}
	for i, h := range got {
		if h.Owner != want[i] {
			t.Errorf("holder %d: got %q, want %q", i, h.Owner, want[i])
		}
	}
}

// --- PebbleStore integration ---

func TestPebbleStore_AdvisoryLock_HTTPPath(t *testing.T) {
	// We don't need a real PebbleStore to exercise the lock methods —
	// the lock table is held by the struct but its acquire/release
	// functions are called directly. Using a real store keeps the
	// test honest about Close() behaviour.
	store := newTestStore(t)
	defer store.Close()

	ctx := context.Background()
	if err := store.AdvisoryLock(ctx, 100, "fusegw-1"); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := store.AdvisoryLock(ctx, 100, "s3gw-1"); !errors.Is(err, ErrLockBusy) {
		t.Fatalf("second acquire: expected ErrLockBusy, got %v", err)
	}
	if err := store.AdvisoryUnlock(ctx, 100, "fusegw-1"); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if err := store.AdvisoryLock(ctx, 100, "s3gw-1"); err != nil {
		t.Fatalf("acquire after unlock: %v", err)
	}

	// List should see s3gw-1 holding shared (we'll switch modes).
	holders, err := store.AdvisoryListLocks(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(holders) != 1 || holders[0].Owner != "s3gw-1" {
		t.Errorf("unexpected holders: %+v", holders)
	}
}

func TestPebbleStore_AdvisoryLock_CloseRejects(t *testing.T) {
	store := newTestStore(t)
	store.Close()

	err := store.AdvisoryLock(context.Background(), 1, "x")
	if !errors.Is(err, ErrServiceClosed) {
		t.Fatalf("expected ErrServiceClosed, got %v", err)
	}
}

func TestPebbleStore_AdvisoryLock_ContextCancel(t *testing.T) {
	store := newTestStore(t)
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.AdvisoryLock(ctx, 1, "x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// --- concurrency / stress ---

// TestAdvisoryLock_ConcurrentSameInode exercises the lock manager
// from many goroutines on the same inode. The invariant is that
// exactly one acquirer at a time observes the critical section;
// ErrLockBusy is fine for everyone else.
func TestAdvisoryLock_ConcurrentSameInode(t *testing.T) {
	m := newAdvisoryLockManager()
	var (
		holdings   atomic.Int32
		maxHolding atomic.Int32
		wg         sync.WaitGroup
	)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Unique owner per goroutine — otherwise re-entrancy
			// would let multiple "owners" of the same name run the
			// critical section concurrently, which is correct
			// behaviour but defeats the test invariant.
			owner := "client-" + itoa(id)
			for j := 0; j < 200; j++ {
				if err := m.acquire(1, owner, LockModeExclusive); err == nil {
					cur := holdings.Add(1)
					for {
						prev := maxHolding.Load()
						if cur <= prev || maxHolding.CompareAndSwap(prev, cur) {
							break
						}
					}
					// Hold for a tiny moment to amplify the race.
					time.Sleep(50 * time.Microsecond)
					holdings.Add(-1)
					m.release(1, owner)
				} else if !errors.Is(err, ErrLockBusy) {
					t.Errorf("unexpected error: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	if maxHolding.Load() != 1 {
		t.Errorf("max concurrent holders = %d, want exactly 1 (mutex invariant broken)", maxHolding.Load())
	}
}

// TestAdvisoryLock_DeadlockFree runs 200 random acquire/release
// sequences across 5 inodes and 5 owners in parallel. With the
// strict compatibility rules, no pair of operations can ever
// deadlock because every lock is independent of every other; this
// test just guards against future refactors that introduce
// cross-inode lock ordering.
func TestAdvisoryLock_DeadlockFree(t *testing.T) {
	m := newAdvisoryLockManager()
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := uint64(seed*997 + 1)
			for i := 0; i < 500; i++ {
				// Simple xorshift for deterministic-but-varied
				// pseudo-randomness without pulling math/rand.
				rng ^= rng << 13
				rng ^= rng >> 7
				rng ^= rng << 17
				inode := InodeID(rng % 5)
				owner := [3]string{"fusegw", "s3gw", "dfsctl"}[rng%3]
				mode := LockMode(rng % 2)
				if err := m.acquire(inode, owner, mode); err == nil {
					_ = m.release(inode, owner)
				}
			}
		}(w)
	}
	// Use a watchdog: if the test hangs, the test framework will
	// time out and report a hang at this line.
	wg.Wait()
}

// --- helpers ---

// newTestStore builds a PebbleStore against a temp directory. It's
// used by the integration tests above that need a real store but
// don't care about persistence between runs.
func newTestStore(t *testing.T) *PebbleStore {
	t.Helper()
	dir := t.TempDir()
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: dir, UseInMemory: true})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	return store
}

// itoa is a tiny integer-to-string helper to avoid pulling in
// strconv just for goroutine labels in the concurrency test.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
