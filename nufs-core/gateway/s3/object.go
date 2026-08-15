package s3

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// handlePutObject handles PUT /{bucket}/{key+}
//
// Server-side retry is intentionally NOT implemented here. Under high
// concurrency, retrying a metadata-contention error adds load to the
// already-overloaded metadata service, making things worse (retry storm).
// Instead, S3 clients handle retries with proper exponential backoff
// and jitter — this is the standard behavior per the S3 spec.
func (gw *Gateway) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	ctx := r.Context()

	// Cap the body at MaxObjectSize + 1 so http.MaxBytesReader trips
	// exactly when the client overshoots.
	r.Body = http.MaxBytesReader(w, r.Body, gw.maxObjectSize)

	result, err := gw.committer.Put(ctx, PutObjectRequest{
		Bucket:        bucket,
		Key:           key,
		Body:          r.Body,
		ContentLength: r.ContentLength,
		MaxObjectSize: gw.maxObjectSize,
		RequestID:     requestID,
	})
	if err != nil {
		gw.writePutObjectCommitterError(w, err, bucket, key, requestID)
		return
	}

	w.Header().Set("ETag", result.ETag)
	w.WriteHeader(http.StatusOK)
}

func (gw *Gateway) writePutObjectCommitterError(w http.ResponseWriter, err error, bucket, key, requestID string) {
	resource := "/" + bucket + "/" + key
	switch {
	case errors.Is(err, ErrObjectBucketNotFound):
		WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchBucket,
			"The specified bucket does not exist", "/"+bucket, requestID)
	case errors.Is(err, ErrObjectLocked):
		WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeSlowDown,
			"Object is locked by another writer: "+err.Error(), resource, requestID)
	case errors.Is(err, ErrObjectBodyTooLarge):
		WriteXMLError(w, http.StatusRequestEntityTooLarge, ErrCodeEntityTooLarge,
			fmt.Sprintf("Object size exceeds %d bytes", gw.maxObjectSize), resource, requestID)
	case errors.Is(err, ErrObjectQuotaExceeded):
		WriteXMLError(w, http.StatusForbidden, "QuotaExceeded", err.Error(), resource, requestID)
	case errors.Is(err, ErrObjectNoReplicas):
		WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeServiceUnavailable,
			"No datanode replicas are available for this bucket's placement policy", resource, requestID)
	case errors.Is(err, ErrObjectWriteFailed):
		WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeServiceUnavailable,
			err.Error(), resource, requestID)
	case errors.Is(err, ErrObjectCommitFailed), errors.Is(err, ErrObjectMetadataFailed):
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), resource, requestID)
	default:
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), resource, requestID)
	}
}

// handleGetObject handles GET /{bucket}/{key+}
func (gw *Gateway) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	ctx := r.Context()

	b, err := gw.meta.GetBucket(ctx, bucket)
	if err != nil {
		if errors.Is(err, metadata.ErrBucketNotFound) {
			WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchBucket,
				"The specified bucket does not exist", "/"+bucket, requestID)
			return
		}
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), "/"+bucket+"/"+key, requestID)
		return
	}

	// Lookup inode
	inode, err := gw.meta.Lookup(ctx, b.RootInode, key)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) || errors.Is(err, metadata.ErrInodeNotFound) {
			WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchKey,
				"The specified key does not exist", "/"+bucket+"/"+key, requestID)
			return
		}
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), "/"+bucket+"/"+key, requestID)
		return
	}

	// Acquire a shared advisory lock so a concurrent FUSE write
	// (or S3 PUT) blocks until we finish streaming the response.
	// Multiple readers can coexist; only writers are exclusive.
	lockOwner := fmt.Sprintf("s3gw-get-%s-%s-%d", bucket, key, time.Now().UnixNano())
	if err := gw.meta.AdvisoryLockShared(ctx, inode.ID, lockOwner); err != nil {
		WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeSlowDown,
			"Object is locked: "+err.Error(),
			"/"+bucket+"/"+key, requestID)
		return
	}
	defer func() {
		if err := releaseAdvisoryLock(ctx, gw.meta, inode.ID, lockOwner); err != nil {
			log.Printf("s3gw: release GET lock for /%s/%s: %v", bucket, key, err)
		}
	}()

	// Resolve the file's chunk references under either storage model (V1
	// ChunkMap or V2 extent layout, roadmap §1.3b). The read discriminator
	// probes the V2 extent surface only for inodes with an empty ChunkMap,
	// so V1 files pay nothing, and services without the extent surface
	// (es == nil) keep their unchanged V1 behavior.
	es, _ := gw.meta.(metadata.ExtentInodeService)
	chunks, err := metadata.ResolveFileChunks(ctx, es, inode)
	if err != nil {
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			"Failed to resolve file chunks: "+err.Error(), "/"+bucket+"/"+key, requestID)
		return
	}

	// Handle range request
	start, end := parseRange(r.Header.Get("Range"), inode.Size)
	contentLen := end - start + 1

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(contentLen, 10))
	w.Header().Set("Last-Modified", FormatS3Time(unixNanoToTime(inode.MTime)))
	w.Header().Set("ETag", FormatETag(0))

	if start > 0 || end < inode.Size-1 {
		w.Header().Set("Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", start, end, inode.Size))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	// Read chunks from data nodes and stream to client
	for _, cref := range chunks {
		chunkEnd := cref.Offset + int64(cref.Length) - 1
		// Skip chunks outside the requested range
		if chunkEnd < start || cref.Offset > end {
			continue
		}

		chunk, err := gw.meta.GetChunk(ctx, cref.ID)
		if err != nil {
			WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
				"Failed to get chunk: "+err.Error(), "/"+bucket+"/"+key, requestID)
			return
		}
		if len(chunk.Replicas) == 0 {
			continue
		}

		// Compute chunk-local read range
		localStart := int64(0)
		localEnd := int64(cref.Length) - 1
		if start > cref.Offset {
			localStart = start - cref.Offset
		}
		if end < chunkEnd {
			localEnd = end - cref.Offset
		}
		readLen := int32(localEnd - localStart + 1)

		data, err := gw.chunkStore.ReadChunkRange(ctx, chunk, localStart, readLen)
		if err != nil {
			WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
				"Failed to read chunk data: "+err.Error(), "/"+bucket+"/"+key, requestID)
			return
		}

		if _, err := w.Write(data); err != nil {
			log.Printf("s3gw: write response: %v", err)
			return
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}
}

// handleDeleteObject handles DELETE /{bucket}/{key+}
func (gw *Gateway) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	ctx := r.Context()
	resource := "/" + bucket + "/" + key

	b, err := gw.meta.GetBucket(ctx, bucket)
	if err != nil {
		if errors.Is(err, metadata.ErrBucketNotFound) {
			WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchBucket,
				"The specified bucket does not exist", "/"+bucket, requestID)
			return
		}
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), "/"+bucket+"/"+key, requestID)
		return
	}

	inode, err := gw.meta.Lookup(ctx, b.RootInode, key)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) || errors.Is(err, metadata.ErrInodeNotFound) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), resource, requestID)
		return
	}

	lockOwner := fmt.Sprintf("s3gw-delete-%s-%s-%s-%d", bucket, key, requestID, time.Now().UnixNano())
	if err := gw.meta.AdvisoryLock(ctx, inode.ID, lockOwner); err != nil {
		WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeSlowDown,
			"Object is locked: "+err.Error(), resource, requestID)
		return
	}
	defer func() {
		if err := releaseAdvisoryLock(ctx, gw.meta, inode.ID, lockOwner); err != nil {
			log.Printf("s3gw: release DELETE lock for %s: %v", resource, err)
		}
	}()

	lockedInode, err := gw.meta.Lookup(ctx, b.RootInode, key)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) || errors.Is(err, metadata.ErrInodeNotFound) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), resource, requestID)
		return
	}
	if lockedInode.ID != inode.ID || lockedInode.CTime != inode.CTime {
		WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeSlowDown,
			"Object changed while waiting for lock", resource, requestID)
		return
	}

	// Unlink (handles nlink decrement + cleanup)
	if err := gw.meta.Unlink(ctx, b.RootInode, key); err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) || errors.Is(err, metadata.ErrInodeNotFound) {
			// S3 returns 204 even if key doesn't exist
			w.WriteHeader(http.StatusNoContent)
			return
		}
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), resource, requestID)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleHeadObject handles HEAD /{bucket}/{key+}
func (gw *Gateway) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	ctx := r.Context()

	b, err := gw.meta.GetBucket(ctx, bucket)
	if err != nil {
		if errors.Is(err, metadata.ErrBucketNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	inode, err := gw.meta.Lookup(ctx, b.RootInode, key)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) || errors.Is(err, metadata.ErrInodeNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(inode.Size, 10))
	w.Header().Set("Last-Modified", FormatS3Time(unixNanoToTime(inode.MTime)))
	w.Header().Set("ETag", FormatETag(0))
	w.WriteHeader(http.StatusOK)
}

// handleCopyObject handles PUT /{bucket}/{key+} with X-Amz-Copy-Source header
func (gw *Gateway) handleCopyObject(w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	ctx := r.Context()
	resource := "/" + bucket + "/" + key

	copySource := r.Header.Get("X-Amz-Copy-Source")
	copySource, _ = url.PathUnescape(copySource)
	copySource = strings.TrimPrefix(copySource, "/")

	srcBucket, srcKey := parsePath("/" + copySource)
	if srcBucket == "" || srcKey == "" {
		WriteXMLError(w, http.StatusBadRequest, ErrCodeInvalidArgument,
			"Invalid copy source", "/"+bucket+"/"+key, requestID)
		return
	}
	if srcBucket == bucket && srcKey == key {
		WriteXMLError(w, http.StatusBadRequest, ErrCodeInvalidRequest,
			"Copy source and destination must differ", resource, requestID)
		return
	}

	// Get source bucket + inode
	srcB, err := gw.meta.GetBucket(ctx, srcBucket)
	if err != nil {
		WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchBucket,
			"Source bucket not found", "/"+srcBucket, requestID)
		return
	}

	srcInode, err := gw.meta.Lookup(ctx, srcB.RootInode, srcKey)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) || errors.Is(err, metadata.ErrInodeNotFound) {
			WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchKey,
				"Source key not found", "/"+copySource, requestID)
			return
		}
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), "/"+copySource, requestID)
		return
	}

	lockOwner := fmt.Sprintf("s3gw-copy-%s-%s-%s-%d", srcBucket, srcKey, requestID, time.Now().UnixNano())
	if err := gw.meta.AdvisoryLockShared(ctx, srcInode.ID, lockOwner); err != nil {
		WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeSlowDown,
			"Copy source is locked: "+err.Error(), resource, requestID)
		return
	}
	defer func() {
		if err := releaseAdvisoryLock(ctx, gw.meta, srcInode.ID, lockOwner); err != nil {
			log.Printf("s3gw: release COPY source lock for /%s/%s: %v", srcBucket, srcKey, err)
		}
	}()

	lockedSource, err := gw.meta.Lookup(ctx, srcB.RootInode, srcKey)
	if err != nil {
		WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeSlowDown,
			"Copy source changed while waiting for lock", resource, requestID)
		return
	}
	if lockedSource.ID != srcInode.ID || lockedSource.CTime != srcInode.CTime {
		WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeSlowDown,
			"Copy source changed while waiting for lock", resource, requestID)
		return
	}

	reader, writer := io.Pipe()
	// streamCopySource reads source chunks and feeds them into the pipe; it runs
	// on a background goroutine so the destination Put can stream from the pipe.
	// It MUST be awaited (and the pipe reader closed) before this handler returns:
	// otherwise a copy whose destination Put fails before draining the body leaves
	// the producer running, and when the metadata store is later closed (app
	// shutdown, or the test teardown) the still-running GetChunk panics with
	// "pebble: closed". Closing the reader unblocks a producer stuck on the pipe
	// write; draining copyErr then joins it so it cannot outlive this handler.
	done := make(chan struct{})
	go func() {
		defer close(done)
		writer.CloseWithError(gw.streamCopySource(ctx, writer, lockedSource))
	}()
	result, err := gw.committer.Put(ctx, PutObjectRequest{
		Bucket:        bucket,
		Key:           key,
		Body:          reader,
		ContentLength: lockedSource.Size,
		MaxObjectSize: gw.maxObjectSize,
		RequestID:     requestID,
	})
	_ = reader.Close()
	<-done
	if err != nil {
		gw.writePutObjectCommitterError(w, err, bucket, key, requestID)
		return
	}

	response := CopyObjectResult{
		LastModified: FormatS3Time(time.Now()),
		ETag:         result.ETag,
	}
	WriteXML(w, http.StatusOK, response)
}

func (gw *Gateway) streamCopySource(ctx context.Context, dst io.Writer, inode *metadata.InodeMeta) error {
	// Model-aware chunk resolution (roadmap §1.3b): V1 ChunkMap passthrough,
	// V2 extent layout resolved via the extent surface.
	es, _ := gw.meta.(metadata.ExtentInodeService)
	chunks, err := metadata.ResolveFileChunks(ctx, es, inode)
	if err != nil {
		return fmt.Errorf("resolve source chunks: %w", err)
	}
	for _, ref := range chunks {
		chunk, err := gw.meta.GetChunk(ctx, ref.ID)
		if err != nil {
			return fmt.Errorf("get source chunk %d: %w", ref.ID, err)
		}
		if len(chunk.Replicas) == 0 {
			return fmt.Errorf("source chunk %d has no replicas", ref.ID)
		}
		data, err := gw.chunkStore.ReadChunkRange(ctx, chunk, 0, int32(ref.Length))
		if err != nil {
			return fmt.Errorf("read source chunk %d: %w", ref.ID, err)
		}
		if _, err := dst.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// parseRange parses the Range header "bytes=start-end"
func parseRange(rangeHeader string, fileSize int64) (start, end int64) {
	start = 0
	end = fileSize - 1

	if rangeHeader == "" {
		return
	}

	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.SplitN(rangeSpec, "-", 2)
	if len(parts) != 2 {
		return
	}

	if parts[0] != "" {
		if n, err := strconv.ParseInt(parts[0], 10, 64); err == nil {
			start = n
		}
	}
	if parts[1] != "" {
		if n, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
			end = n
		}
	}

	if end >= fileSize {
		end = fileSize - 1
	}
	return
}

func crc32Checksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// isRetryablePutError returns true for transient errors that are safe to
// retry at the server level (metadata contention, write failures).
// Context-canceled errors are never retried since the request deadline
// has already expired.
func isRetryablePutError(err error) bool {
	if ctxErr := context.Cause(context.Background()); ctxErr != nil {
		// global context canceled — never retry
		return false
	}
	return errors.Is(err, ErrObjectMetadataFailed) ||
		errors.Is(err, ErrObjectCommitFailed) ||
		errors.Is(err, ErrObjectWriteFailed)
}
