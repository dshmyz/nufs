package metadata

import (
	"testing"
	"time"
)

func TestRepairBatch_Lifecycle(t *testing.T) {
	store := newV2TestPebbleStore(t)
	rb := NewRepairBatchStore(store)

	if _, err := rb.CreateBatch(3, 1, 2, 0, RepairPriorityOneReplica, 1001); err != nil {
		t.Fatal(err)
	}
	// Lease acquires.
	task, err := rb.Lease(1001, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if task == nil || task.State != RepairLeased {
		t.Fatalf("lease: %+v", task)
	}
	// A second lease attempt while held must fail.
	again, err := rb.Lease(1001, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if again != nil {
		t.Fatal("lease should be held")
	}
	// Advance → copying → verifying → complete.
	if err := rb.Advance(1001, 0, 100, 4096); err != nil {
		t.Fatal(err)
	}
	if err := rb.MarkVerifying(1001); err != nil {
		t.Fatal(err)
	}
	if err := rb.Complete(1001); err != nil {
		t.Fatal(err)
	}
	got, _ := rb.Get(1001)
	if got.State != RepairCommitted {
		t.Fatalf("state = %s, want committed", got.State)
	}
	if got.CopiedBytes != 4096 {
		t.Fatalf("copied bytes = %d, want 4096", got.CopiedBytes)
	}
}

func TestRepairBatch_LeaseExpiryAllowsReacquisition(t *testing.T) {
	store := newV2TestPebbleStore(t)
	rb := NewRepairBatchStore(store)

	if _, err := rb.CreateBatch(4, 1, 2, 0, RepairPriorityMissingReplica, 2002); err != nil {
		t.Fatal(err)
	}
	if _, err := rb.Lease(2002, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// After the lease expires, another worker can acquire it.
	time.Sleep(80 * time.Millisecond)
	task, err := rb.Lease(2002, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if task == nil {
		t.Fatal("expected reacquisition after lease expiry")
	}
	if task.Attempts < 2 {
		t.Fatalf("attempts = %d, want >= 2", task.Attempts)
	}
}

func TestRepairBatch_PermanentFailure(t *testing.T) {
	store := newV2TestPebbleStore(t)
	rb := NewRepairBatchStore(store)

	if _, err := rb.CreateBatch(5, 1, 2, 0, RepairPriorityChecksum, 3003); err != nil {
		t.Fatal(err)
	}
	if err := rb.Fail(3003, true, "unrecoverable"); err != nil {
		t.Fatal(err)
	}
	got, _ := rb.Get(3003)
	if got.State != RepairPermanentFailure || got.PermanentError != "unrecoverable" {
		t.Fatalf("permanent failure: %+v", got)
	}
}

func TestRepairBatch_ListActivePaginated(t *testing.T) {
	store := newV2TestPebbleStore(t)
	rb := NewRepairBatchStore(store)
	for i := 0; i < 5; i++ {
		if _, err := rb.CreateBatch(uint32(i), 1, 2, 0, RepairPriorityNodeDrain, uint64(4000+i)); err != nil {
			t.Fatal(err)
		}
	}
	page0, err := rb.ListActive(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page0) != 2 {
		t.Fatalf("page 0 has %d, want 2", len(page0))
	}
	page2, err := rb.ListActive(2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 1 {
		t.Fatalf("page 2 has %d, want 1", len(page2))
	}
}
