package s3

import (
	"context"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func TestObjectWriteGCWorkerDeletesFailedOrphanChunks(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()

	inode := &metadata.InodeMeta{ID: 1, Type: metadata.FileRegular}
	meta.inodes[inode.ID] = inode
	chunk, err := meta.AllocateChunk(ctx, inode.ID, 0, metadata.PlacementPolicy{ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("allocate chunk: %v", err)
	}
	if err := attempts.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:      "failed-orphan",
		InodeID: inode.ID,
		Chunks:  []metadata.ChunkRef{{ID: chunk.ID, Offset: 0, Length: 7}},
		State:   metadata.WriteAttemptFailed,
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	worker := NewObjectWriteGCWorker(&writeGCMetaService{
		mockMetaService: meta,
		attemptStore:    attempts,
	})
	result, err := worker.SweepOnce(ctx, ObjectWriteGCSweepOptions{Limit: 10})
	if err != nil {
		t.Fatalf("sweep once: %v", err)
	}
	if result.DeletedChunks != 1 {
		t.Fatalf("deleted chunks = %d, want 1", result.DeletedChunks)
	}
	if _, err := meta.GetChunk(ctx, chunk.ID); err != metadata.ErrChunkNotFound {
		t.Fatalf("get chunk err = %v, want %v", err, metadata.ErrChunkNotFound)
	}
	if _, err := attempts.GetWriteAttempt(ctx, "failed-orphan"); err != metadata.ErrEntryNotFound {
		t.Fatalf("get attempt err = %v, want %v", err, metadata.ErrEntryNotFound)
	}
}

func TestObjectWriteGCWorkerDoesNotDeleteReferencedChunk(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()

	inode := &metadata.InodeMeta{ID: 1, Type: metadata.FileRegular}
	meta.inodes[inode.ID] = inode
	chunk, err := meta.AllocateChunk(ctx, inode.ID, 0, metadata.PlacementPolicy{ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("allocate chunk: %v", err)
	}
	inode.ChunkMap = []metadata.ChunkRef{{ID: chunk.ID, Offset: 0, Length: 7}}
	if err := attempts.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:      "failed-referenced",
		InodeID: inode.ID,
		Chunks:  []metadata.ChunkRef{{ID: chunk.ID, Offset: 0, Length: 7}},
		State:   metadata.WriteAttemptFailed,
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	worker := NewObjectWriteGCWorker(&writeGCMetaService{
		mockMetaService: meta,
		attemptStore:    attempts,
	})
	result, err := worker.SweepOnce(ctx, ObjectWriteGCSweepOptions{Limit: 10})
	if err != nil {
		t.Fatalf("sweep once: %v", err)
	}
	if result.DeletedChunks != 0 {
		t.Fatalf("deleted chunks = %d, want 0", result.DeletedChunks)
	}
	if _, err := meta.GetChunk(ctx, chunk.ID); err != nil {
		t.Fatalf("get chunk: %v", err)
	}
}

func TestObjectWriteGCWorkerSkipsRecentPendingAttempt(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()

	now := time.Now()
	if err := attempts.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:        "recent-pending",
		State:     metadata.WriteAttemptPending,
		UpdatedAt: now.UnixNano(),
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	worker := NewObjectWriteGCWorker(&writeGCMetaService{
		mockMetaService: meta,
		attemptStore:    attempts,
	})
	result, err := worker.SweepOnce(ctx, ObjectWriteGCSweepOptions{
		Limit:       10,
		AbandonAge:  time.Hour,
		CurrentTime: now,
	})
	if err != nil {
		t.Fatalf("sweep once: %v", err)
	}
	if result.Scanned != 0 {
		t.Fatalf("scanned = %d, want 0", result.Scanned)
	}
	if got, err := attempts.GetWriteAttempt(ctx, "recent-pending"); err != nil || got.State != metadata.WriteAttemptPending {
		t.Fatalf("pending attempt = %+v, err=%v", got, err)
	}
}

func TestObjectWriteGCWorkerDeletesAbandonedAllocatedChunks(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()

	now := time.Now()
	inode := &metadata.InodeMeta{ID: 1, Type: metadata.FileRegular}
	meta.inodes[inode.ID] = inode
	chunk, err := meta.AllocateChunk(ctx, inode.ID, 0, metadata.PlacementPolicy{ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("allocate chunk: %v", err)
	}
	if err := attempts.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:        "old-allocated",
		InodeID:   inode.ID,
		Chunks:    []metadata.ChunkRef{{ID: chunk.ID, Offset: 0, Length: 7}},
		State:     metadata.WriteAttemptChunksAllocated,
		UpdatedAt: now.Add(-2 * time.Hour).UnixNano(),
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	worker := NewObjectWriteGCWorker(&writeGCMetaService{
		mockMetaService: meta,
		attemptStore:    attempts,
	})
	result, err := worker.SweepOnce(ctx, ObjectWriteGCSweepOptions{
		Limit:       10,
		AbandonAge:  time.Hour,
		CurrentTime: now,
	})
	if err != nil {
		t.Fatalf("sweep once: %v", err)
	}
	if result.DeletedChunks != 1 {
		t.Fatalf("deleted chunks = %d, want 1", result.DeletedChunks)
	}
	if _, err := attempts.GetWriteAttempt(ctx, "old-allocated"); err != metadata.ErrEntryNotFound {
		t.Fatalf("get attempt err = %v, want %v", err, metadata.ErrEntryNotFound)
	}
}

func TestObjectWriteGCWorkerRunsUnifiedBackgroundTask(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	attempts := newMemoryAttemptStore()

	if err := meta.PutBackgroundTask(ctx, &metadata.BackgroundTask{
		ID:     "object-write-gc-1",
		Type:   metadata.TaskWriteGC,
		State:  metadata.TaskQueued,
		Target: "object-write-gc",
	}); err != nil {
		t.Fatalf("put background task: %v", err)
	}

	worker := NewObjectWriteGCWorker(&writeGCMetaService{
		mockMetaService: meta,
		attemptStore:    attempts,
	})
	if _, err := worker.RunBackgroundTaskOnce(ctx, "worker-1", time.Minute, ObjectWriteGCSweepOptions{Limit: 10}); err != nil {
		t.Fatalf("run background task: %v", err)
	}
	task, err := meta.GetBackgroundTask(ctx, "object-write-gc-1")
	if err != nil {
		t.Fatalf("get background task: %v", err)
	}
	if task.State != metadata.TaskSucceeded {
		t.Fatalf("task state = %s, want %s", task.State, metadata.TaskSucceeded)
	}
}

type writeGCMetaService struct {
	*mockMetaService
	attemptStore *memoryAttemptStore
}

func (m *writeGCMetaService) PutWriteAttempt(ctx context.Context, attempt *metadata.ObjectWriteAttempt) error {
	return m.attemptStore.PutWriteAttempt(ctx, attempt)
}

func (m *writeGCMetaService) GetWriteAttempt(ctx context.Context, id string) (*metadata.ObjectWriteAttempt, error) {
	return m.attemptStore.GetWriteAttempt(ctx, id)
}

func (m *writeGCMetaService) ListWriteAttemptsByState(ctx context.Context, state metadata.WriteAttemptState, limit int) ([]metadata.ObjectWriteAttempt, error) {
	return m.attemptStore.ListWriteAttemptsByState(ctx, state, limit)
}

func (m *writeGCMetaService) DeleteWriteAttempt(ctx context.Context, id string) error {
	return m.attemptStore.DeleteWriteAttempt(ctx, id)
}
