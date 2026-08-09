package s3

import (
	"context"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

type writeGCMeta interface {
	metadata.WriteAttemptService
	metadata.InodeService
	metadata.ChunkService
}

type writeGCTaskMeta interface {
	writeGCMeta
	metadata.BackgroundTaskService
}

type ObjectWriteGCWorker struct {
	meta writeGCMeta
}

type ObjectWriteGCSweepOptions struct {
	Limit       int
	AbandonAge  time.Duration
	CurrentTime time.Time
}

type ObjectWriteGCSweepResult struct {
	Scanned           int
	DeletedChunks     int
	DeletedAttempts   int
	SkippedReferenced int
}

func NewObjectWriteGCWorker(meta writeGCMeta) *ObjectWriteGCWorker {
	return &ObjectWriteGCWorker{meta: meta}
}

func (w *ObjectWriteGCWorker) RunBackgroundTaskOnce(ctx context.Context, owner string, lease time.Duration, opts ObjectWriteGCSweepOptions) (ObjectWriteGCSweepResult, error) {
	meta, ok := w.meta.(writeGCTaskMeta)
	if !ok {
		return ObjectWriteGCSweepResult{}, ErrObjectMetadataFailed
	}
	task, err := meta.LeaseBackgroundTask(ctx, metadata.TaskWriteGC, owner, lease)
	if err == metadata.ErrEntryNotFound {
		return ObjectWriteGCSweepResult{}, nil
	}
	if err != nil {
		return ObjectWriteGCSweepResult{}, err
	}
	result, err := w.SweepOnce(ctx, opts)
	if err != nil {
		_ = meta.FailBackgroundTask(ctx, task.ID, err.Error(), 3)
		return result, err
	}
	if err := meta.CompleteBackgroundTask(ctx, task.ID); err != nil {
		return result, err
	}
	return result, nil
}

func (w *ObjectWriteGCWorker) SweepOnce(ctx context.Context, opts ObjectWriteGCSweepOptions) (ObjectWriteGCSweepResult, error) {
	if opts.Limit <= 0 {
		opts.Limit = 100
	}
	if opts.AbandonAge <= 0 {
		opts.AbandonAge = time.Hour
	}
	if opts.CurrentTime.IsZero() {
		opts.CurrentTime = time.Now()
	}

	var result ObjectWriteGCSweepResult
	for _, state := range []metadata.WriteAttemptState{
		metadata.WriteAttemptFailed,
		metadata.WriteAttemptPending,
		metadata.WriteAttemptChunksAllocated,
	} {
		if result.Scanned >= opts.Limit {
			break
		}
		attempts, err := w.meta.ListWriteAttemptsByState(ctx, state, opts.Limit-result.Scanned)
		if err != nil {
			return result, err
		}
		for i := range attempts {
			attempt := &attempts[i]
			if state != metadata.WriteAttemptFailed && !attemptIsAbandoned(attempt, opts.CurrentTime, opts.AbandonAge) {
				continue
			}
			if result.Scanned >= opts.Limit {
				break
			}
			result.Scanned++
			chunkResult, err := w.sweepAttempt(ctx, attempt)
			if err != nil {
				return result, err
			}
			result.DeletedChunks += chunkResult.deletedChunks
			result.SkippedReferenced += chunkResult.skippedReferenced
			if chunkResult.skippedReferenced == 0 {
				if err := w.meta.DeleteWriteAttempt(ctx, attempt.ID); err != nil {
					return result, err
				}
				result.DeletedAttempts++
			}
		}
	}
	return result, nil
}

type objectWriteGCChunkResult struct {
	deletedChunks     int
	skippedReferenced int
}

func (w *ObjectWriteGCWorker) sweepAttempt(ctx context.Context, attempt *metadata.ObjectWriteAttempt) (objectWriteGCChunkResult, error) {
	var result objectWriteGCChunkResult
	for _, ref := range attempt.Chunks {
		if w.chunkReferencedByAttemptInode(ctx, attempt.InodeID, ref.ID) {
			result.skippedReferenced++
			continue
		}
		err := w.meta.DeleteChunk(ctx, ref.ID)
		if err == metadata.ErrChunkNotFound {
			continue
		}
		if err != nil {
			return result, err
		}
		result.deletedChunks++
	}
	return result, nil
}

func (w *ObjectWriteGCWorker) chunkReferencedByAttemptInode(ctx context.Context, inodeID metadata.InodeID, chunkID metadata.ChunkID) bool {
	if inodeID == 0 {
		return false
	}
	inode, err := w.meta.GetInode(ctx, inodeID)
	if err != nil {
		return false
	}
	for _, ref := range inode.ChunkMap {
		if ref.ID == chunkID {
			return true
		}
	}
	return false
}

func attemptIsAbandoned(attempt *metadata.ObjectWriteAttempt, now time.Time, age time.Duration) bool {
	if attempt == nil || attempt.UpdatedAt == 0 {
		return false
	}
	return now.Sub(time.Unix(0, attempt.UpdatedAt)) >= age
}

func NewBackgroundObjectWriteGCTask(id string, nextRunAt time.Time) metadata.BackgroundTask {
	return metadata.BackgroundTask{
		ID:        id,
		Type:      metadata.TaskWriteGC,
		State:     metadata.TaskQueued,
		Target:    "object-write-gc",
		NextRunAt: nextRunAt.UnixNano(),
	}
}
