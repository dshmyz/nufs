package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

type writeAttemptStore interface {
	PutWriteAttempt(context.Context, *metadata.ObjectWriteAttempt) error
}

type metadataObjectCommitter struct {
	meta                metadata.MetadataService
	chunkStore          chunkstore.ChunkStore
	rejectEmptyReplicas bool
	// writer is the shared chunk dispatch pipeline (allocate→write→commit→
	// seal→ref). It runs with passthrough executors, preserving the S3
	// reliability path exactly as before.
	writer *chunkstore.ChunkWriter
}

const maxBatchAllocationChunks int64 = 1024

func newMetadataObjectCommitter(meta metadata.MetadataService, chunkStore chunkstore.ChunkStore, rejectEmptyReplicas bool) *metadataObjectCommitter {
	return &metadataObjectCommitter{
		meta:                meta,
		chunkStore:          chunkStore,
		rejectEmptyReplicas: rejectEmptyReplicas,
		writer:              chunkstore.NewChunkWriter(meta, chunkStore),
	}
}

func (c *metadataObjectCommitter) Put(ctx context.Context, req PutObjectRequest) (PutObjectResult, error) {
	maxObjectSize := req.MaxObjectSize
	if maxObjectSize <= 0 {
		maxObjectSize = DefaultMaxObjectSize
	}
	if req.ContentLength > maxObjectSize {
		return PutObjectResult{}, fmt.Errorf(
			"%w: content length %d exceeds maximum %d",
			ErrObjectBodyTooLarge,
			req.ContentLength,
			maxObjectSize,
		)
	}

	// Per-step latency tracking for debugging slow PUTs.
	var t0, tBucket, tLookup, tLock, tAllocate time.Time
	t0 = time.Now()
	defer func() {
		total := time.Since(t0)
		if total > 500*time.Millisecond {
			log.Printf("s3gw: SLOW PUT /%s/%s total=%v [bucket:%v lookup:%v lock:%v alloc:%v write:%v]",
				req.Bucket, req.Key, total.Round(time.Microsecond),
				tBucket.Sub(t0).Round(time.Microsecond),
				tLookup.Sub(tBucket).Round(time.Microsecond),
				tLock.Sub(tLookup).Round(time.Microsecond),
				tAllocate.Sub(tLock).Round(time.Microsecond),
				time.Since(tAllocate).Round(time.Microsecond),
			)
		}
	}()

	attemptID := fmt.Sprintf("%s/%s/%d", req.Bucket, req.Key, time.Now().UnixNano())
	c.recordAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:     attemptID,
		Bucket: req.Bucket,
		Key:    req.Key,
		State:  metadata.WriteAttemptPending,
	})

	b, err := c.meta.GetBucket(ctx, req.Bucket)
	tBucket = time.Now()
	if err != nil {
		c.recordAttempt(ctx, &metadata.ObjectWriteAttempt{
			ID:        attemptID,
			Bucket:    req.Bucket,
			Key:       req.Key,
			State:     metadata.WriteAttemptFailed,
			LastError: err.Error(),
		})
		if errors.Is(err, metadata.ErrBucketNotFound) {
			return PutObjectResult{}, fmt.Errorf("%w: %v", ErrObjectBucketNotFound, err)
		}
		return PutObjectResult{}, fmt.Errorf("%w: %v", ErrObjectMetadataFailed, err)
	}

	var (
		inode            *metadata.InodeMeta
		oldInodeSnapshot *metadata.InodeMeta
		oldChunks        []metadata.ChunkRef
		oldSize          int64
		newObject        = true
		knownLength      = req.ContentLength >= 0
	)
	if inode, err = c.meta.Lookup(ctx, b.RootInode, req.Key); err == nil {
		oldInodeSnapshot = cloneInodeMeta(inode)
		oldChunks, _ = resolveCommittedChunks(ctx, c.meta, inode)
		oldSize = inode.Size
		newObject = false
	} else if !errors.Is(err, metadata.ErrEntryNotFound) && !errors.Is(err, metadata.ErrInodeNotFound) {
		c.recordAttempt(ctx, &metadata.ObjectWriteAttempt{
			ID:        attemptID,
			Bucket:    req.Bucket,
			Key:       req.Key,
			State:     metadata.WriteAttemptFailed,
			LastError: err.Error(),
		})
		return PutObjectResult{}, fmt.Errorf("%w: %v", ErrObjectMetadataFailed, err)
	}
	tLookup = time.Now() // set after Lookup (covers both hit and miss)

	objectDelta := int64(0)
	contentLength := req.ContentLength

	if newObject {
		objectDelta = 1
	}
	if knownLength || newObject {
		additionalBytes := int64(0)
		if knownLength {
			additionalBytes = req.ContentLength - oldSize
		}
		if err := c.meta.CheckBucketQuota(ctx, req.Bucket, additionalBytes, objectDelta); err != nil {
			quotaErr := classifyQuotaCheckError(err)
			c.recordAttempt(ctx, &metadata.ObjectWriteAttempt{
				ID:        attemptID,
				Bucket:    req.Bucket,
				Key:       req.Key,
				State:     metadata.WriteAttemptFailed,
				LastError: quotaErr.Error(),
			})
			return PutObjectResult{}, quotaErr
		}
	}

	if newObject {
		inode, err = c.meta.CreateFile(ctx, b.RootInode, req.Key, 0644)
		if errors.Is(err, metadata.ErrEntryExists) {
			// Another writer created the key after the initial lookup.
			// Continue as an overwrite, matching the pre-existing behavior.
			inode, err = c.meta.Lookup(ctx, b.RootInode, req.Key)
			if err == nil {
				oldInodeSnapshot = cloneInodeMeta(inode)
				oldChunks, _ = resolveCommittedChunks(ctx, c.meta, inode)
				oldSize = inode.Size
				newObject = false
			}
		}
		if err != nil {
			c.recordAttempt(ctx, &metadata.ObjectWriteAttempt{
				ID: attemptID, Bucket: req.Bucket, Key: req.Key,
				State: metadata.WriteAttemptFailed, LastError: err.Error(),
			})
			return PutObjectResult{}, fmt.Errorf("%w: %v", ErrObjectMetadataFailed, err)
		}
	}
	c.recordAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:         attemptID,
		Bucket:     req.Bucket,
		Key:        req.Key,
		InodeID:    inode.ID,
		InodeCTime: inode.CTime,
		State:      metadata.WriteAttemptChunksAllocated,
	})

	lockedInodeID := inode.ID
	lockedInodeCTime := inode.CTime
	lockOwner := fmt.Sprintf("s3gw-%s-%s-%d", req.Bucket, req.Key, time.Now().UnixNano())
	if err := c.meta.AdvisoryLock(ctx, lockedInodeID, lockOwner); err != nil {
		lockErr := fmt.Errorf("%w: %v", ErrObjectLocked, err)
		if newObject {
			// We do not own the inode lock, so leave unlinking to the
			// recovery worker after the cleanup plan is durable.
			plan := newCleanupAttempt(
				attemptID, req, inode, b.RootInode, true, nil, nil, lockErr,
			)
			_, persistErr := c.persistCleanupAttempt(ctx, plan)
			return PutObjectResult{}, errors.Join(lockErr, persistErr)
		}
		c.recordAttemptFailure(ctx, attemptID, req, inode, nil, metadata.WriteAttemptFailed, lockErr)
		return PutObjectResult{}, lockErr
	}
	defer func() {
		if err := releaseAdvisoryLock(ctx, c.meta, lockedInodeID, lockOwner); err != nil {
			log.Printf("s3gw: release PUT lock for /%s/%s: %v", req.Bucket, req.Key, err)
		}
	}()
	tLock = time.Now() // after AdvisoryLock succeeds

	currentInode, err := c.meta.Lookup(ctx, b.RootInode, req.Key)
	if err != nil {
		var refreshErr error
		if errors.Is(err, metadata.ErrEntryNotFound) || errors.Is(err, metadata.ErrInodeNotFound) {
			refreshErr = fmt.Errorf("%w: key changed while acquiring inode lock: %v", ErrObjectLocked, err)
		} else {
			refreshErr = fmt.Errorf("%w: refresh inode after lock: %v", ErrObjectMetadataFailed, err)
		}
		if newObject {
			refreshErr = c.compensateFailedWrite(
				ctx, attemptID, req, inode, b.RootInode, true, nil, nil, refreshErr,
			)
		} else {
			c.recordAttemptFailure(ctx, attemptID, req, inode, nil, metadata.WriteAttemptFailed, refreshErr)
		}
		return PutObjectResult{}, refreshErr
	}
	if currentInode.ID != lockedInodeID || currentInode.CTime != lockedInodeCTime {
		identityErr := fmt.Errorf("key changed inode identity from (%d, %d) to (%d, %d) while acquiring lock",
			lockedInodeID, lockedInodeCTime, currentInode.ID, currentInode.CTime)
		lockErr := fmt.Errorf("%w: %v", ErrObjectLocked, identityErr)
		if newObject {
			lockErr = c.compensateFailedWrite(
				ctx, attemptID, req, inode, b.RootInode, true, nil, nil, lockErr,
			)
		} else {
			c.recordAttemptFailure(ctx, attemptID, req, inode, nil, metadata.WriteAttemptFailed, lockErr)
		}
		return PutObjectResult{}, lockErr
	}
	inode = currentInode

	if newObject {
		oldInodeSnapshot = nil
		oldChunks = nil
		oldSize = 0
	} else {
		oldInodeSnapshot = cloneInodeMeta(inode)
		oldChunks, _ = resolveCommittedChunks(ctx, c.meta, inode)
		oldSize = inode.Size
	}

	if knownLength || newObject {
		additionalBytes := int64(0)
		if knownLength {
			additionalBytes = req.ContentLength - oldSize
		}
		if err := c.meta.CheckBucketQuota(ctx, req.Bucket, additionalBytes, 0); err != nil {
			quotaErr := classifyQuotaCheckError(err)
			if newObject {
				quotaErr = c.compensateFailedWrite(
					ctx, attemptID, req, inode, b.RootInode, true, nil, nil, quotaErr,
				)
			} else {
				c.recordAttemptFailure(ctx, attemptID, req, inode, nil, metadata.WriteAttemptFailed, quotaErr)
			}
			return PutObjectResult{}, quotaErr
		}
	}

	var (
		newChunkRefs             []metadata.ChunkRef
		allAllocatedChunkRefs    []metadata.ChunkRef
		unusedAllocatedChunkRefs []metadata.ChunkRef
		totalSize                int64
		hash                     = sha256.New()
	)

	// Batch allocation: AllocateRanges atomically allocates up to
	// MaxChunkAllocationBatch chunks for the object; the overwrite path falls
	// through to per-range allocation below.

	if numChunks, useBatch := batchAllocationChunkCount(contentLength); useBatch {
		offsets := make([]int64, numChunks)
		for i := 0; i < numChunks; i++ {
			offsets[i] = int64(i) * metadata.MaxChunkSize
		}

		preAllocChunks, err := c.writer.AllocateRanges(ctx, inode.ID, b.Policy, offsets)
		tAllocate = time.Now()
		if err != nil {
			primaryErr := fmt.Errorf("%w: failed to batch allocate chunks: %v", ErrObjectMetadataFailed, err)
			return PutObjectResult{}, c.compensateFailedWrite(
				ctx, attemptID, req, inode, b.RootInode, newObject, oldInodeSnapshot, allAllocatedChunkRefs, primaryErr,
			)
		}
		tAllocate = time.Now()
		for i, chunk := range preAllocChunks {
			allAllocatedChunkRefs = append(allAllocatedChunkRefs, metadata.ChunkRef{
				ID:      chunk.ID,
				Offset:  offsets[i],
				Version: 1,
			})
		}

		if c.rejectEmptyReplicas {
			for _, ch := range preAllocChunks {
				if len(ch.Replicas) == 0 {
					return PutObjectResult{}, c.compensateFailedWrite(
						ctx, attemptID, req, inode, b.RootInode, newObject, oldInodeSnapshot,
						allAllocatedChunkRefs, ErrObjectNoReplicas,
					)
				}
			}
		}

		buf := make([]byte, metadata.MaxChunkSize)
		chunkIdx := 0
		remaining := contentLength

		for remaining > 0 && chunkIdx < len(preAllocChunks) {
			readSize := int64(metadata.MaxChunkSize)
			if remaining < readSize {
				readSize = remaining
			}

			n, err := io.ReadFull(req.Body, buf[:readSize])
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				return PutObjectResult{}, c.compensateFailedWrite(
					ctx, attemptID, req, inode, b.RootInode, newObject, oldInodeSnapshot,
					allAllocatedChunkRefs, classifyBodyReadError(err),
				)
			}
			if n == 0 {
				break
			}

			chunkData := buf[:n]
			chunk := preAllocChunks[chunkIdx]

			newRef, err := c.writer.WriteAllocated(ctx, chunk, offsets[chunkIdx], chunkData)
			if err != nil {
				var primaryErr error
				switch {
				case errors.Is(err, chunkstore.ErrChunkWriteFailed):
					primaryErr = fmt.Errorf("%w: failed to write chunk data: %v", ErrObjectWriteFailed, err)
				case errors.Is(err, chunkstore.ErrChunkCommitFailed):
					primaryErr = fmt.Errorf("%w: failed to commit chunk: %v", ErrObjectCommitFailed, err)
				default:
					primaryErr = fmt.Errorf("%w: failed to write chunk data: %v", ErrObjectWriteFailed, err)
				}
				return PutObjectResult{}, c.compensateFailedWrite(
					ctx, attemptID, req, inode, b.RootInode, newObject, oldInodeSnapshot,
					allAllocatedChunkRefs, primaryErr,
				)
			}
			newChunkRefs = append(newChunkRefs, newRef)
			allAllocatedChunkRefs[chunkIdx] = newRef
			_, _ = hash.Write(chunkData)
			totalSize += int64(n)
			remaining -= int64(n)
			chunkIdx++
		}
		unusedAllocatedChunkRefs = append(unusedAllocatedChunkRefs, allAllocatedChunkRefs[chunkIdx:]...)
	} else {
		buf := make([]byte, metadata.MaxChunkSize)
		for {
			n, err := io.ReadFull(req.Body, buf)
			if err != nil && err != io.ErrUnexpectedEOF {
				if err == io.EOF {
					break
				}
				return PutObjectResult{}, c.compensateFailedWrite(
					ctx, attemptID, req, inode, b.RootInode, newObject, oldInodeSnapshot,
					allAllocatedChunkRefs, classifyBodyReadError(err),
				)
			}
			if n == 0 {
				break
			}

			chunkData := buf[:n]
			chunk, err := c.meta.AllocateChunk(ctx, inode.ID, totalSize, b.Policy)
			if err != nil {
				primaryErr := fmt.Errorf("%w: failed to allocate chunk: %v", ErrObjectMetadataFailed, err)
				return PutObjectResult{}, c.compensateFailedWrite(
					ctx, attemptID, req, inode, b.RootInode, newObject, oldInodeSnapshot,
					allAllocatedChunkRefs, primaryErr,
				)
			}
			newRef := metadata.ChunkRef{
				ID: chunk.ID, Offset: totalSize, Length: int32(n), Version: 1,
			}
			allAllocatedChunkRefs = append(allAllocatedChunkRefs, newRef)

			if c.rejectEmptyReplicas && len(chunk.Replicas) == 0 {
				return PutObjectResult{}, c.compensateFailedWrite(
					ctx, attemptID, req, inode, b.RootInode, newObject, oldInodeSnapshot,
					allAllocatedChunkRefs, ErrObjectNoReplicas,
				)
			}

			committedRef, err := c.writer.WriteAllocated(ctx, chunk, totalSize, chunkData)
			if err != nil {
				var primaryErr error
				switch {
				case errors.Is(err, chunkstore.ErrChunkWriteFailed):
					primaryErr = fmt.Errorf("%w: failed to write data to enough datanodes: %v", ErrObjectWriteFailed, err)
				case errors.Is(err, chunkstore.ErrChunkCommitFailed):
					primaryErr = fmt.Errorf("%w: failed to commit chunk: %v", ErrObjectCommitFailed, err)
				default:
					primaryErr = fmt.Errorf("%w: failed to write data to enough datanodes: %v", ErrObjectWriteFailed, err)
				}
				return PutObjectResult{}, c.compensateFailedWrite(
					ctx, attemptID, req, inode, b.RootInode, newObject, oldInodeSnapshot,
					allAllocatedChunkRefs, primaryErr,
				)
			}

			newChunkRefs = append(newChunkRefs, committedRef)
			_, _ = hash.Write(chunkData)
			totalSize += int64(n)
		}
	}

	// CreateFile already accounts for a new object's object count.
	if err := c.meta.CheckBucketQuota(ctx, req.Bucket, totalSize-oldSize, 0); err != nil {
		quotaErr := classifyQuotaCheckError(err)
		return PutObjectResult{}, c.compensateFailedWrite(
			ctx, attemptID, req, inode, b.RootInode, newObject, oldInodeSnapshot,
			allAllocatedChunkRefs, quotaErr,
		)
	}
	if cleanupErr := c.deleteChunkRefs(ctx, unusedAllocatedChunkRefs); cleanupErr != nil {
		primaryErr := fmt.Errorf("%w: delete unused allocated chunks: %v", ErrObjectMetadataFailed, cleanupErr)
		return PutObjectResult{}, c.compensateFailedWrite(
			ctx, attemptID, req, inode, b.RootInode, newObject, oldInodeSnapshot,
			allAllocatedChunkRefs, primaryErr,
		)
	}
	recoveryInodeCTime := inode.CTime
	c.recordAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:         attemptID,
		Bucket:     req.Bucket,
		Key:        req.Key,
		InodeID:    inode.ID,
		InodeCTime: recoveryInodeCTime,
		Chunks:     newChunkRefs,
		State:      metadata.WriteAttemptChunksDurable,
	})
	// Model-aware commit (roadmap §1.3c): a V2 extent surface lands the
	// ref set as V2 layout (single ≤16MiB chunk → inline extent, else
	// extent pages); without the surface it falls back to the V1 ChunkMap
	// update. inode is still the pre-allocation projection (nil ChunkMap
	// on V2 rows) so the helper's model probe sees the decoded layout; the
	// serving writer sets CTime/MTime, so the committer no longer touches
	// them (V1 UpdateInode bumps CTime on the server).
	if err := metadata.CommitChunkRefsModelAware(ctx, c.meta, inode, newChunkRefs, totalSize); err != nil {
		c.recordAttempt(ctx, &metadata.ObjectWriteAttempt{
			ID:         attemptID,
			Bucket:     req.Bucket,
			Key:        req.Key,
			InodeID:    inode.ID,
			InodeCTime: recoveryInodeCTime,
			Chunks:     cloneChunkRefs(newChunkRefs),
			State:      metadata.WriteAttemptRecoveryNeeded,
			LastError:  err.Error(),
		})
		return PutObjectResult{}, fmt.Errorf("%w: update inode %d: %v", ErrObjectMetadataFailed, inode.ID, err)
	}
	c.recordAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:         attemptID,
		Bucket:     req.Bucket,
		Key:        req.Key,
		InodeID:    inode.ID,
		InodeCTime: inode.CTime,
		Chunks:     newChunkRefs,
		State:      metadata.WriteAttemptCommitted,
	})

	for _, cref := range oldChunks {
		_ = c.meta.DeleteChunk(ctx, cref.ID)
	}

	return PutObjectResult{
		ETag: "\"" + hex.EncodeToString(hash.Sum(nil)[:8]) + "\"",
		Size: totalSize,
	}, nil
}

func (c *metadataObjectCommitter) Get(ctx context.Context, req GetObjectRequest) (ObjectReader, error) {
	return nil, ErrObjectMetadataFailed
}

func batchAllocationChunkCount(contentLength int64) (int, bool) {
	if contentLength <= metadata.MaxChunkSize {
		return 0, false
	}
	chunks := (contentLength-1)/metadata.MaxChunkSize + 1
	if chunks > maxBatchAllocationChunks {
		return 0, false
	}
	return int(chunks), true
}

func classifyBodyReadError(err error) error {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		return fmt.Errorf("%w: %v", ErrObjectBodyTooLarge, err)
	}
	return fmt.Errorf("%w: failed to read request body: %v", ErrObjectMetadataFailed, err)
}

func classifyQuotaCheckError(err error) error {
	if errors.Is(err, metadata.ErrQuotaExceeded) {
		return fmt.Errorf("%w: %v", ErrObjectQuotaExceeded, err)
	}
	return fmt.Errorf("%w: check bucket quota: %v", ErrObjectMetadataFailed, err)
}

func (c *metadataObjectCommitter) putAttempt(ctx context.Context, attempt *metadata.ObjectWriteAttempt) (bool, error) {
	store, ok := c.meta.(writeAttemptStore)
	if !ok {
		return false, nil
	}
	return true, store.PutWriteAttempt(ctx, attempt)
}

func (c *metadataObjectCommitter) recordAttempt(ctx context.Context, attempt *metadata.ObjectWriteAttempt) {
	if _, err := c.putAttempt(ctx, attempt); err != nil {
		log.Printf("s3gw: record write attempt %s: %v", attempt.ID, err)
	}
}

func (c *metadataObjectCommitter) recordAttemptFailure(ctx context.Context, attemptID string, req PutObjectRequest, inode *metadata.InodeMeta, chunks []metadata.ChunkRef, state metadata.WriteAttemptState, err error) {
	var inodeID metadata.InodeID
	var inodeCTime int64
	if inode != nil {
		inodeID = inode.ID
		inodeCTime = inode.CTime
	}
	c.recordAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:         attemptID,
		Bucket:     req.Bucket,
		Key:        req.Key,
		InodeID:    inodeID,
		InodeCTime: inodeCTime,
		Chunks:     cloneChunkRefs(chunks),
		State:      state,
		LastError:  err.Error(),
	})
}

func newCleanupAttempt(
	attemptID string,
	req PutObjectRequest,
	inode *metadata.InodeMeta,
	parent metadata.InodeID,
	newObject bool,
	rollbackInode *metadata.InodeMeta,
	chunks []metadata.ChunkRef,
	err error,
) *metadata.ObjectWriteAttempt {
	return &metadata.ObjectWriteAttempt{
		ID:               attemptID,
		Bucket:           req.Bucket,
		Key:              req.Key,
		InodeID:          inode.ID,
		InodeCTime:       inode.CTime,
		RecoveryIntent:   metadata.WriteAttemptRecoveryCleanup,
		CleanupParent:    parent,
		CleanupNewObject: newObject,
		RollbackInode:    cloneInodeMeta(rollbackInode),
		Chunks:           cloneChunkRefs(chunks),
		State:            metadata.WriteAttemptRecoveryNeeded,
		LastError:        err.Error(),
	}
}

func (c *metadataObjectCommitter) persistCleanupAttempt(
	ctx context.Context,
	attempt *metadata.ObjectWriteAttempt,
) (bool, error) {
	persistCtx, cancel := detachedMetadataContext(ctx)
	defer cancel()
	supported, err := c.putAttempt(persistCtx, attempt)
	if err != nil {
		return supported, fmt.Errorf("persist cleanup plan %s: %w", attempt.ID, err)
	}
	return supported, nil
}

func (c *metadataObjectCommitter) compensateFailedWrite(
	ctx context.Context,
	attemptID string,
	req PutObjectRequest,
	inode *metadata.InodeMeta,
	parent metadata.InodeID,
	newObject bool,
	rollbackInode *metadata.InodeMeta,
	allocations []metadata.ChunkRef,
	primaryErr error,
) error {
	plan := newCleanupAttempt(
		attemptID,
		req,
		inode,
		parent,
		newObject,
		rollbackInode,
		allocations,
		primaryErr,
	)
	supported, persistErr := c.persistCleanupAttempt(ctx, plan)
	resultErr := errors.Join(primaryErr, persistErr)

	cleanupCtx, cancel := detachedMetadataContext(ctx)
	cleanupErr := c.cleanupQuotaRejectedWrite(
		cleanupCtx,
		parent,
		req.Key,
		inode,
		newObject,
		rollbackInode,
		allocations,
	)
	cancel()
	if cleanupErr != nil {
		resultErr = errors.Join(resultErr, fmt.Errorf("cleanup failed write: %w", cleanupErr))
		plan.LastError = resultErr.Error()
		if supported {
			if _, err := c.persistCleanupAttempt(ctx, plan); err != nil {
				resultErr = errors.Join(resultErr, err)
			}
		}
		return resultErr
	}

	plan.State = metadata.WriteAttemptFailed
	plan.LastError = resultErr.Error()
	if supported {
		if _, err := c.persistCleanupAttempt(ctx, plan); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
	}
	return resultErr
}

func cloneInodeMeta(inode *metadata.InodeMeta) *metadata.InodeMeta {
	if inode == nil {
		return nil
	}
	clone := *inode
	clone.ChunkMap = append([]metadata.ChunkRef(nil), inode.ChunkMap...)
	if inode.XAttrs != nil {
		clone.XAttrs = make(map[string][]byte, len(inode.XAttrs))
		for name, value := range inode.XAttrs {
			clone.XAttrs[name] = append([]byte(nil), value...)
		}
	}
	return &clone
}

// resolveCommittedChunks returns the chunk references a row currently
// points at, regardless of storage model (roadmap §1.3c): V1 rows return
// ChunkMap verbatim; V2 rows (nil ChunkMap) are resolved through the
// extent surface (extent ID == chunk ID). It is the committer's oldChunks
// probe (so an overwrite of a V2 object tombstones the superseded
// extents' chunks) and the recovery worker's "already applied" probe. On
// a resolution error it returns the error so callers can decide whether
// to degrade (tombstone leak, no data loss) or fail loud. meta is passed
// as any because only the extent-surface assertion is used — the caller
// may hold a full MetadataService or a narrower composition such as the
// recovery worker's writeRecoveryMeta.
func resolveCommittedChunks(ctx context.Context, meta any, inode *metadata.InodeMeta) ([]metadata.ChunkRef, error) {
	if inode == nil {
		return nil, nil
	}
	if len(inode.ChunkMap) > 0 {
		return append([]metadata.ChunkRef(nil), inode.ChunkMap...), nil
	}
	es, ok := meta.(metadata.ExtentInodeService)
	if !ok {
		return nil, nil
	}
	return metadata.ResolveFileChunks(ctx, es, inode)
}

func (c *metadataObjectCommitter) deleteChunkRefs(ctx context.Context, chunks []metadata.ChunkRef) error {
	var cleanupErrs []error
	for _, ref := range chunks {
		if err := c.meta.DeleteChunk(ctx, ref.ID); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("delete chunk %d: %w", ref.ID, err))
		}
	}
	return errors.Join(cleanupErrs...)
}

func (c *metadataObjectCommitter) cleanupQuotaRejectedWrite(
	ctx context.Context,
	parent metadata.InodeID,
	key string,
	inode *metadata.InodeMeta,
	newObject bool,
	oldInode *metadata.InodeMeta,
	chunks []metadata.ChunkRef,
) error {
	var cleanupErrs []error
	current, lookupErr := c.meta.Lookup(ctx, parent, key)
	if err := c.deleteChunkRefs(ctx, chunks); err != nil {
		cleanupErrs = append(cleanupErrs, err)
	}
	if lookupErr != nil {
		if !errors.Is(lookupErr, metadata.ErrEntryNotFound) && !errors.Is(lookupErr, metadata.ErrInodeNotFound) {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("lookup cleanup target: %w", lookupErr))
		}
		return errors.Join(cleanupErrs...)
	}
	if current.ID != inode.ID || current.CTime != inode.CTime {
		return errors.Join(cleanupErrs...)
	}

	if newObject {
		if err := c.meta.Unlink(ctx, parent, key); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("unlink inode: %w", err))
		}
	} else if oldInode != nil {
		// NOTE (§1.4): for a V2 object the rollback snapshot is a V1
		// projection (nil ChunkMap — the V2 layout lives outside the
		// row). Allocating for the overwrite transiently clobbers the
		// row to V1 (see CommitChunkRefsModelAware), so this V1 restore
		// lands a layout-less row and the V2 object's extents become
		// unreachable. Mitigating the loss requires either preserving the
		// layout through allocation or a V2-aware rollback snapshot;
		// deferred with bucket-stats aggregation to roadmap §1.4.
		if err := c.meta.UpdateInode(ctx, cloneInodeMeta(oldInode)); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("restore inode %d: %w", oldInode.ID, err))
		}
	}
	return errors.Join(cleanupErrs...)
}
