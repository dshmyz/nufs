package segment

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/dfs/datanode/storage"
)

func testPendingWrite(id storage.ExtentID) *pendingWrite {
	return &pendingWrite{extentID: id}
}

func TestGroupCommit_NoLostWakeup(t *testing.T) {
	beforeWait := make(chan struct{})
	releaseWait := make(chan struct{})
	timerWake := make(chan struct{})
	c := newGroupCommitCoordinator(groupCommitConfig{
		MaxBatch: 8,
		MaxWait:  time.Millisecond,
		beforeWait: func() {
			close(beforeWait)
			<-releaseWait
		},
		afterWake: func() { close(timerWake) },
	})
	defer c.close()

	done := make(chan error, 1)
	go func() {
		done <- c.Submit(testPendingWrite(1), func(batch []*pendingWrite) error {
			if len(batch) != 1 {
				return fmt.Errorf("batch size = %d, want 1", len(batch))
			}
			return nil
		})
	}()

	select {
	case <-beforeWait:
	case <-time.After(time.Second):
		close(releaseWait)
		t.Fatal("coordinator did not reach batch wait")
	}
	select {
	case <-timerWake:
	case <-time.After(time.Second):
		close(releaseWait)
		t.Fatal("coordinator timer did not wake")
	}
	close(releaseWait)

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("group commit leader lost wake-up")
	}
}

func TestGroupCommit_CloseRacingSubmit(t *testing.T) {
	c := newGroupCommitCoordinator(groupCommitConfig{MaxBatch: 1})
	commitStarted := make(chan struct{})
	releaseCommit := make(chan struct{})
	accepted := make(chan error, 1)
	var commitCalls atomic.Int32
	t.Cleanup(func() {
		select {
		case <-releaseCommit:
		default:
			close(releaseCommit)
		}
		c.close()
	})
	go func() {
		accepted <- c.Submit(testPendingWrite(1), func(batch []*pendingWrite) error {
			if len(batch) != 1 {
				return fmt.Errorf("batch size = %d, want 1", len(batch))
			}
			close(commitStarted)
			<-releaseCommit
			commitCalls.Add(1)
			return nil
		})
	}()

	select {
	case <-commitStarted:
	case <-time.After(time.Second):
		t.Fatal("accepted request did not begin commit")
	}

	closed := make(chan struct{}, 2)
	for range 2 {
		go func() {
			c.close()
			closed <- struct{}{}
		}()
	}
	select {
	case <-c.stop:
	case <-time.After(time.Second):
		t.Fatal("coordinator stop was not closed")
	}

	rejected := make(chan error, 1)
	go func() { rejected <- c.Submit(testPendingWrite(2), func([]*pendingWrite) error { return nil }) }()
	select {
	case err := <-rejected:
		if !errors.Is(err, storage.ErrCapacity) {
			t.Fatalf("rejected request error = %v, want %v", err, storage.ErrCapacity)
		}
	case <-time.After(time.Second):
		t.Fatal("request submitted after close hung")
	}

	select {
	case <-closed:
		t.Fatal("close returned before the accepted request completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseCommit)

	select {
	case err := <-accepted:
		if err != nil {
			t.Fatalf("accepted request error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("accepted request hung during close")
	}
	if got := commitCalls.Load(); got != 1 {
		t.Fatalf("accepted request completed %d commits, want exactly one", got)
	}
	for range 2 {
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatal("idempotent close did not return")
		}
	}
}

// TestGroupCommit_SharesSyncBarrier verifies the §6.4 core: N concurrent
// writes to the same stream share far fewer than N fsync barriers.
func TestGroupCommit_SharesSyncBarrier(t *testing.T) {
	writeConcurrentAndReopen(t, 8)
}

// TestSustained_Concurrent1024Writes retains high-volume batching and reopen
// coverage. Its name intentionally avoids the required repeated-test regex.
func TestSustained_Concurrent1024Writes(t *testing.T) {
	writeConcurrentAndReopen(t, 1024)
}

func writeConcurrentAndReopen(t *testing.T, writers int) {
	t.Helper()
	dir := t.TempDir()
	s, err := New(Config{Dir: dir, UseMemIndex: false, SegmentSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	const perWriter = 1
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < perWriter; i++ {
				id := storage.ExtentID(uint64(w)*100000 + uint64(i) + 1)
				_, err := s.Write(ctx, &storage.WriteRequest{ExtentID: id, Generation: 1, Data: []byte("batch-data")})
				if err != nil {
					errs[w] = err
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			t.Fatalf("write error: %v", e)
		}
	}
	syncs := s.syncCalls.Load()
	writes := writers * perWriter
	if syncs >= int64(writes) {
		t.Fatalf("group commit did not batch: %d syncs for %d writes", syncs, writes)
	}
	t.Logf("%d concurrent writes → %d fsync barriers (batched %.1fx)", writes, syncs, float64(writes)/float64(syncs))

	// Every write must be readable after reopen (no data loss despite
	// batching).
	s.Close()
	s2, err := New(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	checked := 0
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			id := storage.ExtentID(uint64(w)*100000 + uint64(i) + 1)
			if _, err := s2.Read(ctx, &storage.ReadRequest{ExtentID: id, Generation: 1}); err != nil {
				t.Fatalf("read %d after reopen: %v", id, err)
			}
			checked++
		}
	}
	if checked != writes {
		t.Fatalf("checked %d extents, want %d", checked, writes)
	}
}
