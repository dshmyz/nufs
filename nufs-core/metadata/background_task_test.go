package metadata

import (
	"context"
	"testing"
	"time"
)

func TestBackgroundTaskLeaseLifecycle(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	task := &BackgroundTask{
		ID:             "task-1",
		Type:           TaskRepair,
		State:          TaskQueued,
		Target:         "chunk:10",
		IdempotencyKey: "repair/chunk/10",
	}
	if err := store.PutBackgroundTask(ctx, task); err != nil {
		t.Fatalf("put task: %v", err)
	}

	leased, err := store.LeaseBackgroundTask(ctx, TaskRepair, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("lease task: %v", err)
	}
	if leased.ID != "task-1" || leased.State != TaskLeased || leased.LeaseOwner != "worker-1" {
		t.Fatalf("leased = %+v", leased)
	}

	if err := store.CompleteBackgroundTask(ctx, "task-1"); err != nil {
		t.Fatalf("complete task: %v", err)
	}

	got, err := store.GetBackgroundTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.State != TaskSucceeded {
		t.Fatalf("state = %s, want %s", got.State, TaskSucceeded)
	}
}

func TestBackgroundTaskFailureEventuallyDeadLetters(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	task := &BackgroundTask{ID: "task-2", Type: TaskRepair, State: TaskQueued, Target: "chunk:20"}
	_ = store.PutBackgroundTask(ctx, task)

	for i := 0; i < 4; i++ {
		if err := store.FailBackgroundTask(ctx, "task-2", "boom", 3); err != nil {
			t.Fatalf("fail task %d: %v", i, err)
		}
	}

	got, err := store.GetBackgroundTask(ctx, "task-2")
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.State != TaskDeadLetter {
		t.Fatalf("state = %s, want %s", got.State, TaskDeadLetter)
	}
}

func TestLeaseBackgroundTaskForNode_OwnerFiltering(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// A task whose OwnerNodes include node 2 must only be leasable by node 2.
	owned := &BackgroundTask{
		ID: "conv-1", Type: TaskECConvert, State: TaskQueued,
		Target: "ec-convert-100", OwnerNodes: []uint64{2, 3},
	}
	if err := store.PutBackgroundTask(ctx, owned); err != nil {
		t.Fatalf("put owned task: %v", err)
	}

	// Non-owner lease: no eligible task (owner nodes exclude node 1).
	if _, err := store.LeaseBackgroundTaskForNode(ctx, TaskECConvert, 1, "ec-convert-worker-1", time.Minute); err != ErrEntryNotFound {
		t.Fatalf("non-owner lease err = %v, want ErrEntryNotFound", err)
	}

	// Owner lease: node 2 leases it and the task flips to leased.
	leased, err := store.LeaseBackgroundTaskForNode(ctx, TaskECConvert, 2, "ec-convert-worker-2", time.Minute)
	if err != nil {
		t.Fatalf("owner lease: %v", err)
	}
	if leased.ID != "conv-1" || leased.State != TaskLeased || leased.LeaseOwner != "ec-convert-worker-2" {
		t.Fatalf("leased = %+v", leased)
	}

	// Complete it so the queue is empty again, then verify a task with NO
	// OwnerNodes is leasable by any node (legacy/operator-created tasks).
	if err := store.CompleteBackgroundTask(ctx, "conv-1"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	legacy := &BackgroundTask{
		ID: "conv-2", Type: TaskECConvert, State: TaskQueued, Target: "ec-convert-200",
	}
	if err := store.PutBackgroundTask(ctx, legacy); err != nil {
		t.Fatalf("put legacy task: %v", err)
	}
	any, err := store.LeaseBackgroundTaskForNode(ctx, TaskECConvert, 7, "ec-convert-worker-7", time.Minute)
	if err != nil {
		t.Fatalf("legacy any-node lease: %v", err)
	}
	if any.ID != "conv-2" {
		t.Fatalf("legacy lease got %s, want conv-2", any.ID)
	}

	// An owner-routed task must not be leased by the node-agnostic
	// LeaseBackgroundTask either? No — the node-agnostic variant deliberately
	// treats OwnerNodes as advisory (repair/gc/scrub never set it). Assert the
	// documented behavior: it leases regardless.
	if err := store.CompleteBackgroundTask(ctx, "conv-2"); err != nil {
		t.Fatalf("complete conv-2: %v", err)
	}
	routed := &BackgroundTask{
		ID: "conv-3", Type: TaskECConvert, State: TaskQueued,
		Target: "ec-convert-300", OwnerNodes: []uint64{5},
	}
	if err := store.PutBackgroundTask(ctx, routed); err != nil {
		t.Fatalf("put routed task: %v", err)
	}
	agnostic, err := store.LeaseBackgroundTask(ctx, TaskECConvert, "any-worker", time.Minute)
	if err != nil {
		t.Fatalf("node-agnostic lease: %v", err)
	}
	if agnostic.ID != "conv-3" {
		t.Fatalf("node-agnostic lease got %s, want conv-3", agnostic.ID)
	}
}

func TestBackgroundTaskRetryingIsReLeasable(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// A failed task moves to TaskRetrying with a short backoff. Once that
	// NextRunAt has elapsed it must be re-leasable — otherwise a single
	// transient failure strands the task forever (silent dead end).
	task := &BackgroundTask{
		ID: "retry-1", Type: TaskRepair, State: TaskQueued, Target: "chunk:30",
	}
	if err := store.PutBackgroundTask(ctx, task); err != nil {
		t.Fatalf("put task: %v", err)
	}
	if err := store.FailBackgroundTask(ctx, "retry-1", "transient", 3); err != nil {
		t.Fatalf("fail task: %v", err)
	}
	got, err := store.GetBackgroundTask(ctx, "retry-1")
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.State != TaskRetrying {
		t.Fatalf("state = %s, want %s", got.State, TaskRetrying)
	}
	// The just-failed task's backoff is in the future: not yet re-leasable.
	if _, err := store.LeaseBackgroundTask(ctx, TaskRepair, "worker-1", time.Minute); err != ErrEntryNotFound {
		t.Fatalf("early re-lease err = %v, want ErrEntryNotFound", err)
	}

	// Rewind the retry to the past → re-leasable.
	got.State = TaskRetrying
	got.NextRunAt = time.Now().Add(-time.Second).UnixNano()
	if err := store.PutBackgroundTask(ctx, got); err != nil {
		t.Fatalf("rewind retry: %v", err)
	}
	released, err := store.LeaseBackgroundTask(ctx, TaskRepair, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("re-lease due retry: %v", err)
	}
	if released.ID != "retry-1" || released.State != TaskLeased {
		t.Fatalf("re-leased = %+v", released)
	}
	// AttemptCount survives the re-lease so repeated failures still dead-letter.
	if released.AttemptCount != 1 {
		t.Fatalf("attempt count = %d, want 1", released.AttemptCount)
	}
}
