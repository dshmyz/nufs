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
