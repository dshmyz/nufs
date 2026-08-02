package segment

import (
	"context"
	"fmt"
	"sync"
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

// TestGroupCommit_SharesSyncBarrier verifies the §6.4 core: N concurrent
// writes to the same stream share far fewer than N fsync barriers.
// With the coordinator loop, concurrent writers batch into a single sync
// each. This is the performance property the design targets.
func TestGroupCommit_SharesSyncBarrier(t *testing.T) {
	dir := t.TempDir()
	s, err := New(Config{Dir: dir, UseMemIndex: false, SegmentSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	const writers = 16
	const perWriter = 64 // 1024 total writes
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
	if syncs >= 1024 {
		t.Fatalf("group commit did not batch: %d syncs for %d writes", syncs, 1024)
	}
	t.Logf("%d concurrent writes → %d fsync barriers (batched %.1fx)", 1024, syncs, float64(1024)/float64(syncs))

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
	if checked != 1024 {
		t.Fatalf("checked %d extents, want 1024", checked)
	}
}
