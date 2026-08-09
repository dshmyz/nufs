package s3

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

type writeAttemptRecoveryStore interface {
	PutWriteAttempt(context.Context, *metadata.ObjectWriteAttempt) error
	ListWriteAttemptsByState(context.Context, metadata.WriteAttemptState, int) ([]metadata.ObjectWriteAttempt, error)
}

type writeRecoveryMeta interface {
	writeAttemptRecoveryStore
	metadata.BucketService
	metadata.BucketQuotaService
	metadata.NamespaceService
	metadata.InodeService
	metadata.ChunkService
	metadata.LockService
}

type writeRecoveryTaskMeta interface {
	writeRecoveryMeta
	metadata.BackgroundTaskService
}

type ObjectWriteRecoveryWorker struct {
	meta writeRecoveryMeta
}

type ObjectWriteRecoveryResult struct {
	Scanned   int
	Committed int
	Cleaned   int
	Failed    int
}

func NewObjectWriteRecoveryWorker(meta writeRecoveryMeta) *ObjectWriteRecoveryWorker {
	return &ObjectWriteRecoveryWorker{meta: meta}
}

func (w *ObjectWriteRecoveryWorker) RecoverOnce(ctx context.Context, limit int) (ObjectWriteRecoveryResult, error) {
	var result ObjectWriteRecoveryResult
	for _, state := range []metadata.WriteAttemptState{
		metadata.WriteAttemptChunksDurable,
		metadata.WriteAttemptRecoveryNeeded,
	} {
		if limit > 0 && result.Scanned >= limit {
			break
		}
		remaining := limit - result.Scanned
		if limit <= 0 {
			remaining = 100
		}
		attempts, err := w.meta.ListWriteAttemptsByState(ctx, state, remaining)
		if err != nil {
			return result, err
		}
		for i := range attempts {
			if limit > 0 && result.Scanned >= limit {
				break
			}
			result.Scanned++
			if attempts[i].RecoveryIntent == metadata.WriteAttemptRecoveryCleanup {
				if err := w.cleanupAttempt(ctx, &attempts[i]); err != nil {
					result.Failed++
					w.markCleanupRetryable(ctx, &attempts[i], err)
					continue
				}
				if err := w.markCleanupComplete(ctx, &attempts[i]); err != nil {
					result.Failed++
					w.markCleanupRetryable(ctx, &attempts[i], err)
					continue
				}
				result.Cleaned++
				continue
			}
			if err := w.recoverAttempt(ctx, &attempts[i]); err != nil {
				result.Failed++
				w.markFailed(ctx, &attempts[i], err)
				continue
			}
			result.Committed++
		}
	}
	return result, nil
}

func (w *ObjectWriteRecoveryWorker) cleanupAttempt(ctx context.Context, attempt *metadata.ObjectWriteAttempt) error {
	if attempt == nil || attempt.ID == "" {
		return fmt.Errorf("cleanup attempt is missing id")
	}
	if attempt.InodeID == 0 {
		return w.deleteChunks(ctx, attempt.Chunks)
	}

	if err := w.meta.AdvisoryLock(ctx, attempt.InodeID, attempt.ID); err != nil {
		return fmt.Errorf("lock inode %d: %w", attempt.InodeID, err)
	}
	defer func() {
		if err := releaseAdvisoryLock(ctx, w.meta, attempt.InodeID, attempt.ID); err != nil {
			log.Printf("s3gw: release recovery lock for attempt %s: %v", attempt.ID, err)
		}
	}()

	inode, err := w.meta.Lookup(ctx, attempt.CleanupParent, attempt.Key)
	if err != nil && !errors.Is(err, metadata.ErrEntryNotFound) && !errors.Is(err, metadata.ErrInodeNotFound) {
		return fmt.Errorf("lookup cleanup target: %w", err)
	}
	if err := w.deleteChunks(ctx, attempt.Chunks); err != nil {
		return err
	}
	if err != nil {
		return nil
	}
	if inode.ID != attempt.InodeID || inode.CTime != attempt.InodeCTime {
		return nil
	}

	if attempt.CleanupNewObject {
		if err := w.meta.Unlink(ctx, attempt.CleanupParent, attempt.Key); err != nil && !errors.Is(err, metadata.ErrEntryNotFound) {
			return fmt.Errorf("unlink cleanup target: %w", err)
		}
		return nil
	}
	if attempt.RollbackInode == nil {
		return nil
	}
	rollback := cloneInodeMeta(attempt.RollbackInode)
	if err := w.meta.UpdateInode(ctx, rollback); err != nil {
		return fmt.Errorf("restore inode %d: %w", rollback.ID, err)
	}
	return nil
}

func (w *ObjectWriteRecoveryWorker) deleteChunks(ctx context.Context, chunks []metadata.ChunkRef) error {
	for _, ref := range chunks {
		if err := w.meta.DeleteChunk(ctx, ref.ID); err != nil && !errors.Is(err, metadata.ErrChunkNotFound) {
			return fmt.Errorf("delete chunk %d: %w", ref.ID, err)
		}
	}
	return nil
}

func (w *ObjectWriteRecoveryWorker) RunBackgroundTaskOnce(ctx context.Context, owner string, lease time.Duration, limit int) (ObjectWriteRecoveryResult, error) {
	meta, ok := w.meta.(writeRecoveryTaskMeta)
	if !ok {
		return ObjectWriteRecoveryResult{}, fmt.Errorf("metadata service does not support background tasks")
	}
	task, err := meta.LeaseBackgroundTask(ctx, metadata.TaskWriteRecovery, owner, lease)
	if err == metadata.ErrEntryNotFound {
		return ObjectWriteRecoveryResult{}, nil
	}
	if err != nil {
		return ObjectWriteRecoveryResult{}, err
	}
	result, err := w.RecoverOnce(ctx, limit)
	if err != nil {
		_ = meta.FailBackgroundTask(ctx, task.ID, err.Error(), 3)
		return result, err
	}
	if err := meta.CompleteBackgroundTask(ctx, task.ID); err != nil {
		return result, err
	}
	return result, nil
}

func (w *ObjectWriteRecoveryWorker) recoverAttempt(ctx context.Context, attempt *metadata.ObjectWriteAttempt) error {
	if attempt == nil || attempt.ID == "" || attempt.Bucket == "" || attempt.Key == "" ||
		attempt.InodeID == 0 || attempt.InodeCTime == 0 || len(attempt.Chunks) == 0 {
		return fmt.Errorf("write attempt is missing object identity, inode, or chunks")
	}
	if err := w.meta.AdvisoryLock(ctx, attempt.InodeID, attempt.ID); err != nil {
		return fmt.Errorf("lock inode %d: %w", attempt.InodeID, err)
	}
	defer func() {
		if err := releaseAdvisoryLock(ctx, w.meta, attempt.InodeID, attempt.ID); err != nil {
			log.Printf("s3gw: release commit recovery lock for attempt %s: %v", attempt.ID, err)
		}
	}()

	bucket, err := w.meta.GetBucket(ctx, attempt.Bucket)
	if err != nil {
		return fmt.Errorf("get bucket %s: %w", attempt.Bucket, err)
	}
	inode, err := w.meta.Lookup(ctx, bucket.RootInode, attempt.Key)
	if err != nil {
		return fmt.Errorf("lookup recovery target: %w", err)
	}
	if inode.ID != attempt.InodeID {
		_ = w.deleteChunks(ctx, attempt.Chunks)
		return fmt.Errorf("stale write attempt for %s/%s", attempt.Bucket, attempt.Key)
	}
	if equalRecoveryChunkRefs(inode.ChunkMap, attempt.Chunks) {
		committed := *attempt
		committed.State = metadata.WriteAttemptCommitted
		committed.LastError = ""
		return w.meta.PutWriteAttempt(ctx, &committed)
	}
	if inode.CTime != attempt.InodeCTime {
		_ = w.deleteChunks(ctx, attempt.Chunks)
		return fmt.Errorf("stale write attempt for %s/%s", attempt.Bucket, attempt.Key)
	}
	newSize := chunkRefsSize(attempt.Chunks)
	if err := w.meta.CheckBucketQuota(ctx, attempt.Bucket, newSize-inode.Size, 0); err != nil {
		_ = w.deleteChunks(ctx, attempt.Chunks)
		return fmt.Errorf("check recovery quota: %w", err)
	}
	if err := w.ensureChunksDurable(ctx, attempt.Chunks); err != nil {
		return err
	}
	inode.ChunkMap = cloneChunkRefs(attempt.Chunks)
	inode.Size = newSize
	now := time.Now().UnixNano()
	inode.CTime = now
	inode.MTime = now
	if err := w.meta.UpdateInode(ctx, inode); err != nil {
		return fmt.Errorf("update inode %d: %w", attempt.InodeID, err)
	}
	committed := *attempt
	committed.State = metadata.WriteAttemptCommitted
	committed.LastError = ""
	return w.meta.PutWriteAttempt(ctx, &committed)
}

func equalRecoveryChunkRefs(left, right []metadata.ChunkRef) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (w *ObjectWriteRecoveryWorker) ensureChunksDurable(ctx context.Context, chunks []metadata.ChunkRef) error {
	for _, ref := range chunks {
		chunk, err := w.meta.GetChunk(ctx, ref.ID)
		if err != nil {
			return fmt.Errorf("get chunk %d: %w", ref.ID, err)
		}
		switch chunk.State {
		case metadata.ChunkReady:
			continue
		case metadata.ChunkSealed:
			if err := w.meta.SealChunk(ctx, ref.ID); err != nil {
				return fmt.Errorf("seal chunk %d: %w", ref.ID, err)
			}
		default:
			return fmt.Errorf("chunk %d is not durable: state=%v", ref.ID, chunk.State)
		}
	}
	return nil
}

func (w *ObjectWriteRecoveryWorker) markFailed(ctx context.Context, attempt *metadata.ObjectWriteAttempt, err error) {
	failed := *attempt
	failed.State = metadata.WriteAttemptFailed
	failed.LastError = err.Error()
	_ = w.meta.PutWriteAttempt(ctx, &failed)
}

func (w *ObjectWriteRecoveryWorker) markCleanupRetryable(ctx context.Context, attempt *metadata.ObjectWriteAttempt, err error) {
	retryable := *attempt
	retryable.State = metadata.WriteAttemptRecoveryNeeded
	retryable.RecoveryIntent = metadata.WriteAttemptRecoveryCleanup
	retryable.LastError = err.Error()
	_ = w.meta.PutWriteAttempt(ctx, &retryable)
}

func (w *ObjectWriteRecoveryWorker) markCleanupComplete(ctx context.Context, attempt *metadata.ObjectWriteAttempt) error {
	completed := *attempt
	completed.State = metadata.WriteAttemptFailed
	return w.meta.PutWriteAttempt(ctx, &completed)
}

func cloneChunkRefs(chunks []metadata.ChunkRef) []metadata.ChunkRef {
	if len(chunks) == 0 {
		return nil
	}
	cp := make([]metadata.ChunkRef, len(chunks))
	copy(cp, chunks)
	return cp
}

func chunkRefsSize(chunks []metadata.ChunkRef) int64 {
	var size int64
	for _, chunk := range chunks {
		end := chunk.Offset + int64(chunk.Length)
		if end > size {
			size = end
		}
	}
	return size
}

func NewBackgroundObjectWriteRecoveryTask(id string, nextRunAt time.Time) metadata.BackgroundTask {
	return metadata.BackgroundTask{
		ID:        id,
		Type:      metadata.TaskWriteRecovery,
		State:     metadata.TaskQueued,
		Target:    "object-write-recovery",
		NextRunAt: nextRunAt.UnixNano(),
	}
}
