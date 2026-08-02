package segment

import (
	"context"
	"sync"
	"testing"

	"github.com/example/dfs/datanode/storage"
)

// TestGroupCommit_SharesSyncBarrier verifies the §6.4 core: N concurrent
// writes to the same stream share far fewer than N fsync barriers.
// With the leader-follower coordinator, concurrent writers batch into a
// single sync each. This is the performance property the design targets.
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
