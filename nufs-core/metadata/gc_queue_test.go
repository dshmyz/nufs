package metadata

import (
	"testing"
	"time"
)

func TestGCQueue_EnqueueAndExpire(t *testing.T) {
	store := newV2TestPebbleStore(t)
	gc := NewGCQueue(store)

	now := time.Now()
	extents := []GCExtent{{ExtentID: 1, Generation: 1}, {ExtentID: 2, Generation: 1}}
	if err := gc.Enqueue(now, 0x0001, 5001, GCRetentionOverwriteDelete, extents); err != nil {
		t.Fatal(err)
	}
	// Not yet expired.
	expired, err := gc.ExpiredBatches(now.Add(time.Minute), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 0 {
		t.Fatalf("batch should not be expired yet, got %d", len(expired))
	}
	// After retention passes, it is expired.
	expired, err = gc.ExpiredBatches(now.Add(GCRetentionOverwriteDelete+time.Minute), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired batch, got %d", len(expired))
	}
	if expired[0].BatchID != 5001 || len(expired[0].Extents) != 2 {
		t.Fatalf("batch mismatch: %+v", expired[0])
	}
}

func TestGCQueue_PageAndProgress(t *testing.T) {
	store := newV2TestPebbleStore(t)
	gc := NewGCQueue(store)

	now := time.Now()
	var extents []GCExtent
	for i := 0; i < GCBatchPageSize+100; i++ {
		extents = append(extents, GCExtent{ExtentID: uint64(i + 1), Generation: 1})
	}
	if err := gc.Enqueue(now, 0x0002, 6001, GCRetentionAbandonedWrite, extents); err != nil {
		t.Fatal(err)
	}
	hour := HourBucket(now)
	b, err := gc.Get(hour, 0x0002, 6001)
	if err != nil || b == nil {
		t.Fatalf("get batch: %v %+v", err, b)
	}
	// First page: 512 entries, more remain.
	page, more, err := gc.NextPage(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != GCBatchPageSize || !more {
		t.Fatalf("first page len=%d more=%v", len(page), more)
	}
	// Second page: remainder, done.
	page, more, err = gc.NextPage(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 100 || more {
		t.Fatalf("second page len=%d more=%v", len(page), more)
	}
	// Progress is durable.
	b2, _ := gc.Get(hour, 0x0002, 6001)
	if b2.Cursor != GCBatchPageSize+100 {
		t.Fatalf("cursor = %d, want %d", b2.Cursor, GCBatchPageSize+100)
	}
}

func TestGCQueue_DeletedExcluded(t *testing.T) {
	store := newV2TestPebbleStore(t)
	gc := NewGCQueue(store)

	now := time.Now()
	if err := gc.Enqueue(now.Add(-2*GCRetentionFailedRepair), 0x0003, 7001, GCRetentionFailedRepair, []GCExtent{{ExtentID: 1, Generation: 1}}); err != nil {
		t.Fatal(err)
	}
	hour := HourBucket(now.Add(-2 * GCRetentionFailedRepair))
	b, _ := gc.Get(hour, 0x0003, 7001)
	if err := gc.MarkDeleted(b); err != nil {
		t.Fatal(err)
	}
	expired, _ := gc.ExpiredBatches(now, 0, 100)
	if len(expired) != 0 {
		t.Fatalf("deleted batch should be excluded, got %d", len(expired))
	}
}
