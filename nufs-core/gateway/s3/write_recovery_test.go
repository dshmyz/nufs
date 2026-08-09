package s3

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func TestObjectWriteRecoveryWorkerCommitsDurableAttempt(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()

	if err := meta.CreateBucket(ctx, "bucket", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	bucket, err := meta.GetBucket(ctx, "bucket")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	inode, err := meta.CreateFile(ctx, bucket.RootInode, "object.txt", 0644)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	chunk, err := meta.AllocateChunk(ctx, inode.ID, 0, metadata.PlacementPolicy{ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("allocate chunk: %v", err)
	}
	if err := meta.CommitChunk(ctx, chunk.ID, 123); err != nil {
		t.Fatalf("commit chunk: %v", err)
	}
	chunks := []metadata.ChunkRef{{ID: chunk.ID, Offset: 0, Length: 7, Version: 1}}
	if err := attempts.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:         "attempt-1",
		Bucket:     "bucket",
		Key:        "object.txt",
		InodeID:    inode.ID,
		InodeCTime: inode.CTime,
		Chunks:     chunks,
		State:      metadata.WriteAttemptChunksDurable,
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}
	worker := NewObjectWriteRecoveryWorker(&recoveryMetaService{
		mockMetaService: meta,
		attemptStore:    attempts,
	})
	result, err := worker.RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("recover once: %v", err)
	}
	if result.Committed != 1 {
		t.Fatalf("committed = %d, want 1", result.Committed)
	}

	gotInode, err := meta.GetInode(ctx, inode.ID)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	if gotInode.Size != 7 {
		t.Fatalf("inode size = %d, want 7", gotInode.Size)
	}
	if len(gotInode.ChunkMap) != 1 || gotInode.ChunkMap[0].ID != chunk.ID {
		t.Fatalf("inode chunk map = %+v, want chunk %d", gotInode.ChunkMap, chunk.ID)
	}
	gotAttempt, err := attempts.GetWriteAttempt(ctx, "attempt-1")
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if gotAttempt.State != metadata.WriteAttemptCommitted {
		t.Fatalf("attempt state = %s, want %s", gotAttempt.State, metadata.WriteAttemptCommitted)
	}
}

func TestObjectWriteRecoveryWorkerRecognizesAlreadyAppliedCommit(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()
	if err := meta.CreateBucket(ctx, "bucket", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	bucket, err := meta.GetBucket(ctx, "bucket")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	inode, err := meta.CreateFile(ctx, bucket.RootInode, "object.txt", 0644)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	chunk, err := meta.AllocateChunk(ctx, inode.ID, 0, bucket.Policy)
	if err != nil {
		t.Fatalf("allocate chunk: %v", err)
	}
	if err := meta.CommitChunk(ctx, chunk.ID, 123); err != nil {
		t.Fatalf("commit chunk: %v", err)
	}
	chunks := []metadata.ChunkRef{{ID: chunk.ID, Offset: 0, Length: 7, Version: 1}}
	if err := attempts.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:         "already-applied",
		Bucket:     "bucket",
		Key:        "object.txt",
		InodeID:    inode.ID,
		InodeCTime: inode.CTime,
		Chunks:     chunks,
		State:      metadata.WriteAttemptChunksDurable,
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	committedInode := cloneInodeMeta(inode)
	committedInode.CTime++
	committedInode.Size = 7
	committedInode.ChunkMap = cloneChunkRefs(chunks)
	if err := meta.UpdateInode(ctx, committedInode); err != nil {
		t.Fatalf("apply inode update: %v", err)
	}

	result, err := NewObjectWriteRecoveryWorker(&recoveryMetaService{
		mockMetaService: meta,
		attemptStore:    attempts,
	}).RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("recover once: %v", err)
	}
	if result.Committed != 1 || result.Failed != 0 {
		t.Fatalf("result = %+v, want one committed and no failures", result)
	}
	if _, err := meta.GetChunk(ctx, chunk.ID); err != nil {
		t.Fatalf("already committed chunk was deleted: %v", err)
	}
	gotAttempt, err := attempts.GetWriteAttempt(ctx, "already-applied")
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if gotAttempt.State != metadata.WriteAttemptCommitted {
		t.Fatalf("attempt state = %s, want %s", gotAttempt.State, metadata.WriteAttemptCommitted)
	}
}

func TestObjectWriteRecoveryWorkerDoesNotOverwriteNewerObjectVersion(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()
	if err := meta.CreateBucket(ctx, "bucket", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	bucket, err := meta.GetBucket(ctx, "bucket")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	inode, err := meta.CreateFile(ctx, bucket.RootInode, "object.txt", 0644)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	staleChunk, err := meta.AllocateChunk(ctx, inode.ID, 0, bucket.Policy)
	if err != nil {
		t.Fatalf("allocate stale chunk: %v", err)
	}
	if err := meta.CommitChunk(ctx, staleChunk.ID, 111); err != nil {
		t.Fatalf("commit stale chunk: %v", err)
	}
	attempt := &metadata.ObjectWriteAttempt{
		ID:         "stale-attempt",
		Bucket:     "bucket",
		Key:        "object.txt",
		InodeID:    inode.ID,
		InodeCTime: inode.CTime,
		Chunks:     []metadata.ChunkRef{{ID: staleChunk.ID, Offset: 0, Length: 5, Version: 1}},
		State:      metadata.WriteAttemptChunksDurable,
	}
	if err := attempts.PutWriteAttempt(ctx, attempt); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	replacement := cloneInodeMeta(inode)
	replacement.CTime++
	replacement.Size = 3
	replacement.ChunkMap = []metadata.ChunkRef{{ID: 999, Offset: 0, Length: 3, Version: 1}}
	if err := meta.UpdateInode(ctx, replacement); err != nil {
		t.Fatalf("update replacement: %v", err)
	}

	recoveryMeta := &recoveryMetaService{mockMetaService: meta, attemptStore: attempts}
	result, err := NewObjectWriteRecoveryWorker(recoveryMeta).RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("recover once: %v", err)
	}
	if result.Failed != 1 || result.Committed != 0 {
		t.Fatalf("result = %+v, want one failed and no commit", result)
	}
	got, err := meta.Lookup(ctx, bucket.RootInode, "object.txt")
	if err != nil {
		t.Fatalf("lookup replacement: %v", err)
	}
	if got.CTime != replacement.CTime || got.Size != replacement.Size || !equalChunkRefs(got.ChunkMap, replacement.ChunkMap) {
		t.Fatalf("replacement was overwritten: got=%+v want=%+v", got, replacement)
	}
	if recoveryMeta.lockInode != inode.ID || recoveryMeta.lockOwner != attempt.ID {
		t.Fatalf("recovery lock = (%d, %q), want (%d, %q)", recoveryMeta.lockInode, recoveryMeta.lockOwner, inode.ID, attempt.ID)
	}
}

func TestObjectWriteRecoveryWorkerRechecksQuotaBeforeCommit(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "bucket")
	bucket, err := store.GetBucket(ctx, "bucket")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	inode, err := store.CreateFile(ctx, bucket.RootInode, "object.txt", 0644)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	chunk, err := store.AllocateChunk(ctx, inode.ID, 0, bucket.Policy)
	if err != nil {
		t.Fatalf("allocate chunk: %v", err)
	}
	if err := store.CommitChunk(ctx, chunk.ID, 123); err != nil {
		t.Fatalf("commit chunk: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "bucket", &metadata.BucketQuota{MaxSizeBytes: 3}); err != nil {
		t.Fatalf("set quota: %v", err)
	}
	before, err := store.Lookup(ctx, bucket.RootInode, "object.txt")
	if err != nil {
		t.Fatalf("lookup before recovery: %v", err)
	}
	attempt := &metadata.ObjectWriteAttempt{
		ID:         "quota-recovery",
		Bucket:     "bucket",
		Key:        "object.txt",
		InodeID:    inode.ID,
		InodeCTime: inode.CTime,
		Chunks:     []metadata.ChunkRef{{ID: chunk.ID, Offset: 0, Length: 4, Version: 1}},
		State:      metadata.WriteAttemptChunksDurable,
	}
	if err := store.PutWriteAttempt(ctx, attempt); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	result, err := NewObjectWriteRecoveryWorker(store).RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("recover once: %v", err)
	}
	if result.Failed != 1 || result.Committed != 0 {
		t.Fatalf("result = %+v, want one failed and no commit", result)
	}
	got, err := store.Lookup(ctx, bucket.RootInode, "object.txt")
	if err != nil {
		t.Fatalf("lookup object: %v", err)
	}
	if got.Size != before.Size || !equalChunkRefs(got.ChunkMap, before.ChunkMap) {
		t.Fatalf("quota-rejected recovery updated inode: got=%+v want=%+v", got, before)
	}
	if _, err := store.GetChunk(ctx, chunk.ID); err != nil {
		t.Fatalf("GetChunk after quota rejection = %v, want retained metadata", err)
	}
}

func TestObjectWriteRecoveryWorkerFailsAttemptWithMissingChunk(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()

	if err := attempts.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:      "attempt-2",
		Bucket:  "bucket",
		Key:     "object.txt",
		InodeID: 1,
		Chunks:  []metadata.ChunkRef{{ID: 99, Offset: 0, Length: 7, Version: 1}},
		State:   metadata.WriteAttemptRecoveryNeeded,
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}
	worker := NewObjectWriteRecoveryWorker(&recoveryMetaService{
		mockMetaService: meta,
		attemptStore:    attempts,
	})
	result, err := worker.RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("recover once: %v", err)
	}
	if result.Failed != 1 {
		t.Fatalf("failed = %d, want 1", result.Failed)
	}
	gotAttempt, err := attempts.GetWriteAttempt(ctx, "attempt-2")
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if gotAttempt.State != metadata.WriteAttemptFailed {
		t.Fatalf("attempt state = %s, want %s", gotAttempt.State, metadata.WriteAttemptFailed)
	}
	if gotAttempt.LastError == "" {
		t.Fatal("expected failed attempt to record last error")
	}
}

func TestObjectWriteRecoveryWorkerCleansRejectedNewObjectWithoutCommitting(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()

	if err := meta.CreateBucket(ctx, "bucket", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	bucket, err := meta.GetBucket(ctx, "bucket")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	inode, err := meta.CreateFile(ctx, bucket.RootInode, "rejected.txt", 0644)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	chunk, err := meta.AllocateChunk(ctx, inode.ID, 0, bucket.Policy)
	if err != nil {
		t.Fatalf("allocate chunk: %v", err)
	}
	if err := meta.CommitChunk(ctx, chunk.ID, 123); err != nil {
		t.Fatalf("commit chunk: %v", err)
	}
	chunks := []metadata.ChunkRef{{ID: chunk.ID, Offset: 0, Length: 7, Version: 1}}
	if err := attempts.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:               "cleanup-new",
		Bucket:           "bucket",
		Key:              "rejected.txt",
		InodeID:          inode.ID,
		InodeCTime:       inode.CTime,
		Chunks:           chunks,
		State:            metadata.WriteAttemptRecoveryNeeded,
		RecoveryIntent:   metadata.WriteAttemptRecoveryCleanup,
		CleanupParent:    bucket.RootInode,
		CleanupNewObject: true,
		LastError:        "bucket quota exceeded",
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	worker := NewObjectWriteRecoveryWorker(&recoveryMetaService{
		mockMetaService: meta,
		attemptStore:    attempts,
	})
	result, err := worker.RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("recover once: %v", err)
	}
	if result.Committed != 0 || result.Cleaned != 1 || result.Failed != 0 {
		t.Fatalf("result = %+v, want one cleaned and no committed/failed", result)
	}
	if _, err := meta.Lookup(ctx, bucket.RootInode, "rejected.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("lookup rejected object = %v, want %v", err, metadata.ErrEntryNotFound)
	}
	if _, err := meta.GetChunk(ctx, chunk.ID); !errors.Is(err, metadata.ErrChunkNotFound) {
		t.Fatalf("get rejected chunk = %v, want %v", err, metadata.ErrChunkNotFound)
	}
	gotAttempt, err := attempts.GetWriteAttempt(ctx, "cleanup-new")
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if gotAttempt.State != metadata.WriteAttemptFailed {
		t.Fatalf("attempt state = %s, want %s", gotAttempt.State, metadata.WriteAttemptFailed)
	}
	if !strings.Contains(gotAttempt.LastError, "quota") {
		t.Fatalf("last error = %q, want original quota context", gotAttempt.LastError)
	}
}

func TestObjectWriteRecoveryWorkerRestoresRejectedOverwriteSnapshot(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()

	if err := meta.CreateBucket(ctx, "bucket", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	bucket, err := meta.GetBucket(ctx, "bucket")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	inode, err := meta.CreateFile(ctx, bucket.RootInode, "object.txt", 0644)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	oldChunk, err := meta.AllocateChunk(ctx, inode.ID, 0, bucket.Policy)
	if err != nil {
		t.Fatalf("allocate old chunk: %v", err)
	}
	if err := meta.CommitChunk(ctx, oldChunk.ID, 111); err != nil {
		t.Fatalf("commit old chunk: %v", err)
	}
	inode.Size = 3
	inode.ChunkMap = []metadata.ChunkRef{{ID: oldChunk.ID, Offset: 0, Length: 3, Version: 1}}
	if err := meta.UpdateInode(ctx, inode); err != nil {
		t.Fatalf("update old inode: %v", err)
	}
	rollback := cloneInodeMeta(inode)

	rejectedChunk, err := meta.AllocateChunk(ctx, inode.ID, 0, bucket.Policy)
	if err != nil {
		t.Fatalf("allocate rejected chunk: %v", err)
	}
	if err := meta.CommitChunk(ctx, rejectedChunk.ID, 222); err != nil {
		t.Fatalf("commit rejected chunk: %v", err)
	}
	inode.Size = 7
	inode.ChunkMap = []metadata.ChunkRef{{ID: rejectedChunk.ID, Offset: 0, Length: 7, Version: 1}}
	if err := meta.UpdateInode(ctx, inode); err != nil {
		t.Fatalf("install rejected inode state: %v", err)
	}
	if err := attempts.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:             "cleanup-overwrite",
		Bucket:         "bucket",
		Key:            "object.txt",
		InodeID:        inode.ID,
		InodeCTime:     inode.CTime,
		Chunks:         cloneChunkRefs(inode.ChunkMap),
		State:          metadata.WriteAttemptRecoveryNeeded,
		RecoveryIntent: metadata.WriteAttemptRecoveryCleanup,
		CleanupParent:  bucket.RootInode,
		RollbackInode:  rollback,
		LastError:      "bucket quota exceeded",
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}
	storedAttempt, err := attempts.GetWriteAttempt(ctx, "cleanup-overwrite")
	if err != nil {
		t.Fatalf("get cleanup attempt: %v", err)
	}
	current, err := meta.Lookup(ctx, bucket.RootInode, "object.txt")
	if err != nil {
		t.Fatalf("lookup cleanup target: %v", err)
	}
	if current.ID != storedAttempt.InodeID || current.CTime != storedAttempt.InodeCTime {
		t.Fatalf("cleanup identity = (%d, %d), want (%d, %d)", current.ID, current.CTime, storedAttempt.InodeID, storedAttempt.InodeCTime)
	}

	worker := NewObjectWriteRecoveryWorker(&recoveryMetaService{
		mockMetaService: meta,
		attemptStore:    attempts,
	})
	result, err := worker.RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("recover once: %v", err)
	}
	if result.Committed != 0 || result.Cleaned != 1 {
		t.Fatalf("result = %+v, want one cleaned and no committed", result)
	}
	got, err := meta.Lookup(ctx, bucket.RootInode, "object.txt")
	if err != nil {
		t.Fatalf("lookup restored object: %v", err)
	}
	if got.Size != rollback.Size || len(got.ChunkMap) != 1 || got.ChunkMap[0].ID != oldChunk.ID {
		t.Fatalf("restored inode = %+v, want snapshot %+v", got, rollback)
	}
	if _, err := meta.GetChunk(ctx, rejectedChunk.ID); !errors.Is(err, metadata.ErrChunkNotFound) {
		t.Fatalf("get rejected chunk = %v, want %v", err, metadata.ErrChunkNotFound)
	}
	if _, err := meta.GetChunk(ctx, oldChunk.ID); err != nil {
		t.Fatalf("old chunk was deleted: %v", err)
	}
}

func TestObjectWriteRecoveryWorkerKeepsCleanupFailureRetryable(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()

	if err := meta.CreateBucket(ctx, "bucket", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	bucket, err := meta.GetBucket(ctx, "bucket")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	inode, err := meta.CreateFile(ctx, bucket.RootInode, "rejected.txt", 0644)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	chunk, err := meta.AllocateChunk(ctx, inode.ID, 0, bucket.Policy)
	if err != nil {
		t.Fatalf("allocate chunk: %v", err)
	}
	if err := meta.CommitChunk(ctx, chunk.ID, 123); err != nil {
		t.Fatalf("commit chunk: %v", err)
	}
	if err := attempts.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:               "cleanup-retry",
		Bucket:           "bucket",
		Key:              "rejected.txt",
		InodeID:          inode.ID,
		InodeCTime:       inode.CTime,
		Chunks:           []metadata.ChunkRef{{ID: chunk.ID, Offset: 0, Length: 7, Version: 1}},
		State:            metadata.WriteAttemptRecoveryNeeded,
		RecoveryIntent:   metadata.WriteAttemptRecoveryCleanup,
		CleanupParent:    bucket.RootInode,
		CleanupNewObject: true,
		LastError:        "bucket quota exceeded",
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	worker := NewObjectWriteRecoveryWorker(&recoveryMetaService{
		mockMetaService: meta,
		attemptStore:    attempts,
		deleteChunkErr:  errors.New("injected delete failure"),
	})
	result, err := worker.RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("recover once: %v", err)
	}
	if result.Committed != 0 || result.Cleaned != 0 || result.Failed != 1 {
		t.Fatalf("result = %+v, want one retryable cleanup failure", result)
	}
	gotAttempt, err := attempts.GetWriteAttempt(ctx, "cleanup-retry")
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if gotAttempt.State != metadata.WriteAttemptRecoveryNeeded {
		t.Fatalf("attempt state = %s, want %s", gotAttempt.State, metadata.WriteAttemptRecoveryNeeded)
	}
	if !strings.Contains(gotAttempt.LastError, "injected delete failure") {
		t.Fatalf("last error = %q, want current cleanup failure", gotAttempt.LastError)
	}

	worker.meta.(*recoveryMetaService).deleteChunkErr = nil
	result, err = worker.RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("retry cleanup: %v", err)
	}
	if result.Cleaned != 1 || result.Failed != 0 || result.Committed != 0 {
		t.Fatalf("retry result = %+v, want one cleaned and no failed/committed", result)
	}
	gotAttempt, err = attempts.GetWriteAttempt(ctx, "cleanup-retry")
	if err != nil {
		t.Fatalf("get retried attempt: %v", err)
	}
	if gotAttempt.State != metadata.WriteAttemptFailed {
		t.Fatalf("retried attempt state = %s, want %s", gotAttempt.State, metadata.WriteAttemptFailed)
	}
}

func TestObjectWriteRecoveryWorkerTreatsMissingCleanupChunkAsSuccess(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()

	if err := meta.CreateBucket(ctx, "bucket", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	bucket, err := meta.GetBucket(ctx, "bucket")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	inode, err := meta.CreateFile(ctx, bucket.RootInode, "rejected.txt", 0644)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	chunk, err := meta.AllocateChunk(ctx, inode.ID, 0, bucket.Policy)
	if err != nil {
		t.Fatalf("allocate chunk: %v", err)
	}
	if err := meta.DeleteChunk(ctx, chunk.ID); err != nil {
		t.Fatalf("pre-delete chunk: %v", err)
	}
	if err := attempts.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:               "cleanup-missing-chunk",
		Bucket:           "bucket",
		Key:              "rejected.txt",
		InodeID:          inode.ID,
		InodeCTime:       inode.CTime,
		Chunks:           []metadata.ChunkRef{{ID: chunk.ID, Offset: 0, Length: 7, Version: 1}},
		State:            metadata.WriteAttemptRecoveryNeeded,
		RecoveryIntent:   metadata.WriteAttemptRecoveryCleanup,
		CleanupParent:    bucket.RootInode,
		CleanupNewObject: true,
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	worker := NewObjectWriteRecoveryWorker(&recoveryMetaService{mockMetaService: meta, attemptStore: attempts})
	result, err := worker.RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("recover once: %v", err)
	}
	if result.Cleaned != 1 || result.Failed != 0 || result.Committed != 0 {
		t.Fatalf("result = %+v, want one cleaned and no failed/committed", result)
	}
	if _, err := meta.Lookup(ctx, bucket.RootInode, "rejected.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("lookup rejected object = %v, want %v", err, metadata.ErrEntryNotFound)
	}
}

func TestObjectWriteRecoveryWorkerCleanupIdentityMismatchLeavesReplacementObject(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()

	if err := meta.CreateBucket(ctx, "bucket", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	bucket, err := meta.GetBucket(ctx, "bucket")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	replacement, err := meta.CreateFile(ctx, bucket.RootInode, "object.txt", 0644)
	if err != nil {
		t.Fatalf("create replacement: %v", err)
	}
	chunk, err := meta.AllocateChunk(ctx, replacement.ID, 0, bucket.Policy)
	if err != nil {
		t.Fatalf("allocate chunk: %v", err)
	}
	if err := meta.CommitChunk(ctx, chunk.ID, 123); err != nil {
		t.Fatalf("commit chunk: %v", err)
	}

	if err := attempts.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:               "cleanup-mismatch",
		Bucket:           "bucket",
		Key:              "object.txt",
		InodeID:          replacement.ID,
		InodeCTime:       replacement.CTime - 1,
		Chunks:           []metadata.ChunkRef{{ID: chunk.ID, Offset: 0, Length: 7, Version: 1}},
		State:            metadata.WriteAttemptRecoveryNeeded,
		RecoveryIntent:   metadata.WriteAttemptRecoveryCleanup,
		CleanupParent:    bucket.RootInode,
		CleanupNewObject: true,
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	recoveryMeta := &recoveryMetaService{mockMetaService: meta, attemptStore: attempts}
	worker := NewObjectWriteRecoveryWorker(recoveryMeta)
	result, err := worker.RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("recover once: %v", err)
	}
	if result.Committed != 0 || result.Cleaned != 1 || result.Failed != 0 {
		t.Fatalf("result = %+v, want one cleaned and no committed/failed", result)
	}
	if recoveryMeta.lockInode != replacement.ID || recoveryMeta.lockOwner != "cleanup-mismatch" {
		t.Fatalf("cleanup lock = inode %d owner %q, want inode %d owner %q", recoveryMeta.lockInode, recoveryMeta.lockOwner, replacement.ID, "cleanup-mismatch")
	}
	got, err := meta.Lookup(ctx, bucket.RootInode, "object.txt")
	if err != nil {
		t.Fatalf("lookup replacement object: %v", err)
	}
	if got.ID != replacement.ID || got.CTime != replacement.CTime {
		t.Fatalf("replacement object changed: %+v", got)
	}
	if _, err := meta.GetChunk(ctx, chunk.ID); !errors.Is(err, metadata.ErrChunkNotFound) {
		t.Fatalf("get cleanup chunk = %v, want %v", err, metadata.ErrChunkNotFound)
	}
}

func TestObjectWriteRecoveryWorkerRunsUnifiedBackgroundTask(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()

	if err := meta.PutBackgroundTask(ctx, &metadata.BackgroundTask{
		ID:     "write-recovery-1",
		Type:   metadata.TaskWriteRecovery,
		State:  metadata.TaskQueued,
		Target: "object-write-recovery",
	}); err != nil {
		t.Fatalf("put background task: %v", err)
	}

	worker := NewObjectWriteRecoveryWorker(&recoveryMetaService{
		mockMetaService: meta,
		attemptStore:    attempts,
	})
	if _, err := worker.RunBackgroundTaskOnce(ctx, "worker-1", time.Minute, 10); err != nil {
		t.Fatalf("run background task: %v", err)
	}
	task, err := meta.GetBackgroundTask(ctx, "write-recovery-1")
	if err != nil {
		t.Fatalf("get background task: %v", err)
	}
	if task.State != metadata.TaskSucceeded {
		t.Fatalf("task state = %s, want %s", task.State, metadata.TaskSucceeded)
	}
}

type recoveryMetaService struct {
	*mockMetaService
	attemptStore   *memoryAttemptStore
	deleteChunkErr error
	lockInode      metadata.InodeID
	lockOwner      string
}

func (m *recoveryMetaService) PutWriteAttempt(ctx context.Context, attempt *metadata.ObjectWriteAttempt) error {
	return m.attemptStore.PutWriteAttempt(ctx, attempt)
}

func (m *recoveryMetaService) ListWriteAttemptsByState(ctx context.Context, state metadata.WriteAttemptState, limit int) ([]metadata.ObjectWriteAttempt, error) {
	return m.attemptStore.ListWriteAttemptsByState(ctx, state, limit)
}

func (m *recoveryMetaService) DeleteChunk(ctx context.Context, chunkID metadata.ChunkID) error {
	if m.deleteChunkErr != nil {
		return m.deleteChunkErr
	}
	return m.mockMetaService.DeleteChunk(ctx, chunkID)
}

func (m *recoveryMetaService) AdvisoryLock(ctx context.Context, inodeID metadata.InodeID, owner string) error {
	m.lockInode = inodeID
	m.lockOwner = owner
	return m.mockMetaService.AdvisoryLock(ctx, inodeID, owner)
}

func (m *recoveryMetaService) AdvisoryUnlock(ctx context.Context, inodeID metadata.InodeID, owner string) error {
	return m.mockMetaService.AdvisoryUnlock(ctx, inodeID, owner)
}
