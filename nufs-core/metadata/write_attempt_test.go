package metadata

import (
	"context"
	"testing"
)

func TestPebbleStoreWriteAttemptLifecycle(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	attempt := &ObjectWriteAttempt{
		ID:             "attempt-1",
		Bucket:         "bucket",
		Key:            "object.txt",
		InodeID:        42,
		InodeCTime:     12345,
		RecoveryIntent: WriteAttemptRecoveryCleanup,
		CleanupParent:  2,
		RollbackInode: &InodeMeta{
			ID:       42,
			CTime:    12345,
			Size:     3,
			ChunkMap: []ChunkRef{{ID: 9, Offset: 0, Length: 3, Version: 1}},
		},
		State: WriteAttemptPending,
	}
	if err := store.PutWriteAttempt(ctx, attempt); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	attempt.State = WriteAttemptChunksDurable
	attempt.Chunks = []ChunkRef{{ID: 10, Offset: 0, Length: 7, Version: 1}}
	if err := store.PutWriteAttempt(ctx, attempt); err != nil {
		t.Fatalf("update attempt: %v", err)
	}

	got, err := store.GetWriteAttempt(ctx, "attempt-1")
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if got.State != WriteAttemptChunksDurable || len(got.Chunks) != 1 {
		t.Fatalf("unexpected attempt: %+v", got)
	}
	if got.RecoveryIntent != WriteAttemptRecoveryCleanup || got.InodeCTime != attempt.InodeCTime || got.CleanupParent != attempt.CleanupParent {
		t.Fatalf("cleanup identity did not round trip: %+v", got)
	}
	if got.RollbackInode == nil || got.RollbackInode.ID != 42 || got.RollbackInode.CTime != 12345 ||
		len(got.RollbackInode.ChunkMap) != 1 || got.RollbackInode.ChunkMap[0].ID != 9 {
		t.Fatalf("rollback inode did not round trip: %+v", got.RollbackInode)
	}
}

func TestPebbleStoreListRecoverableWriteAttempts(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	_ = store.PutWriteAttempt(ctx, &ObjectWriteAttempt{ID: "recover", State: WriteAttemptRecoveryNeeded})
	_ = store.PutWriteAttempt(ctx, &ObjectWriteAttempt{ID: "committed", State: WriteAttemptCommitted})

	attempts, err := store.ListWriteAttemptsByState(ctx, WriteAttemptRecoveryNeeded, 100)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].ID != "recover" {
		t.Fatalf("attempts = %+v", attempts)
	}
}

func TestPebbleStoreWriteAttemptEmptyRecoveryIntentRemainsCommitCompatible(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	legacy := &ObjectWriteAttempt{
		ID:      "legacy-attempt",
		InodeID: 42,
		Chunks:  []ChunkRef{{ID: 10, Offset: 0, Length: 7, Version: 1}},
		State:   WriteAttemptRecoveryNeeded,
	}
	if err := store.applyBatchMsgpack([]batchOp{{Key: writeAttemptKey(legacy.ID), Value: legacy}}, nil); err != nil {
		t.Fatalf("persist legacy attempt: %v", err)
	}

	got, err := store.GetWriteAttempt(ctx, "legacy-attempt")
	if err != nil {
		t.Fatalf("get legacy attempt: %v", err)
	}
	if got.RecoveryIntent != WriteAttemptRecoveryCommit {
		t.Fatalf("recovery intent = %q, want %q", got.RecoveryIntent, WriteAttemptRecoveryCommit)
	}
}
