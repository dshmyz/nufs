package s3

import (
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/example/dfs/metadata"
)

// handlePutObject handles PUT /{bucket}/{key+}
func (gw *Gateway) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	ctx := r.Context()

	// Get bucket info
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

	// Cap the body at MaxObjectSize + 1 so http.MaxBytesReader trips
	// exactly when the client overshoots. ReadAll will then return
	// *http.MaxBytesError and we can return 413 instead of buffering
	// gigabytes into memory.
	r.Body = http.MaxBytesReader(w, r.Body, gw.maxObjectSize)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			WriteXMLError(w, http.StatusRequestEntityTooLarge, ErrCodeEntityTooLarge,
				fmt.Sprintf("Object size exceeds %d bytes", gw.maxObjectSize),
				"/"+bucket+"/"+key, requestID)
			return
		}
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			"Failed to read request body: "+err.Error(), "/"+bucket+"/"+key, requestID)
		return
	}

	// Try to create file; if exists, look up the existing inode and
	// update it in place.  This avoids the Unlink+CreateFile window
	// where the key temporarily disappears.
	var oldChunks []metadata.ChunkRef
	inode, err := gw.meta.CreateFile(ctx, b.RootInode, key, 0644)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			inode, err = gw.meta.Lookup(ctx, b.RootInode, key)
			if err != nil {
				WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
					err.Error(), "/"+bucket+"/"+key, requestID)
				return
			}
			oldChunks = inode.ChunkMap
		} else {
			WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
				err.Error(), "/"+bucket+"/"+key, requestID)
			return
		}
	}

	// Acquire an exclusive advisory lock on the inode so a
	// concurrent FUSE writer (or another S3 PUT to the same key)
	// sees ErrLockBusy rather than a silent overwrite.  The lock
	// owner is unique per-request so we don't hold our own lock.
	lockOwner := fmt.Sprintf("s3gw-%s-%s-%d", bucket, key, time.Now().UnixNano())
	if err := gw.meta.AdvisoryLock(ctx, inode.ID, lockOwner); err != nil {
		WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeSlowDown,
			"Object is locked by another writer: "+err.Error(),
			"/"+bucket+"/"+key, requestID)
		return
	}
	defer gw.meta.AdvisoryUnlock(ctx, inode.ID, lockOwner)

	// Allocate a chunk for the data
	chunk, err := gw.meta.AllocateChunk(ctx, inode.ID, 0, b.Policy)
	if err != nil {
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			"Failed to allocate chunk: "+err.Error(), "/"+bucket+"/"+key, requestID)
		return
	}

	// Reject the request up front if the placement policy produced no
	// replicas. This is a clearer 503 than waiting for the chunk store
	// to fail with "no replicas", and it stops a misconfigured cluster
	// from accepting writes that cannot satisfy the durability contract.
	// Only enforced when the gateway is configured to do so (production
	// sets it; in-memory tests do not).
	if gw.rejectEmptyReplicas && len(chunk.Replicas) == 0 {
		WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeServiceUnavailable,
			"No datanode replicas are available for this bucket's placement policy",
			"/"+bucket+"/"+key, requestID)
		return
	}

	// Write data to each replica via the chunk store (TCP for prod,
	// in-memory for tests).
	checksum := crc32Checksum(data)
	if err := gw.chunkStore.WriteChunk(ctx, chunk, data); err != nil {
		WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeServiceUnavailable,
			"Failed to write data to enough datanodes: "+err.Error(),
			"/"+bucket+"/"+key, requestID)
		return
	}

	// Commit chunk with checksum
	if err := gw.meta.CommitChunk(ctx, chunk.ID, checksum); err != nil {
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			"Failed to commit chunk: "+err.Error(), "/"+bucket+"/"+key, requestID)
		return
	}

	// Seal chunk
	if err := gw.meta.SealChunk(ctx, chunk.ID); err != nil {
		log.Printf("s3gw: seal chunk %d: %v", chunk.ID, err)
	}

	// Update inode with size and chunk reference
	inode.Size = int64(len(data))
	inode.ChunkMap = []metadata.ChunkRef{
		{ID: chunk.ID, Offset: 0, Length: int32(len(data)), Version: 1},
	}
	if err := gw.meta.UpdateInode(ctx, inode); err != nil {
		log.Printf("s3gw: update inode %d: %v", inode.ID, err)
	}

	// Clean up old chunks (async — readers still holding in-flight
	// references will finish before the chunk data is removed).
	for _, cref := range oldChunks {
		_ = gw.meta.DeleteChunk(ctx, cref.ID)
	}

	// Compute ETag from content hash
	hash := sha256.Sum256(data)
	etag := "\"" + hex.EncodeToString(hash[:8]) + "\""

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
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
	defer gw.meta.AdvisoryUnlock(ctx, inode.ID, lockOwner)

	// Handle range request
	start, end := parseRange(r.Header.Get("Range"), inode.Size)
	contentLen := end - start + 1

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(contentLen, 10))
	w.Header().Set("Last-Modified", FormatS3Time(unixNanoToTime(inode.MTime)))
	w.Header().Set("ETag", FormatETag(0))
	w.Header().Set("Accept-Ranges", "bytes")

	if start > 0 || end < inode.Size-1 {
		w.Header().Set("Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", start, end, inode.Size))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	// Read chunks from data nodes and stream to client
	for _, cref := range inode.ChunkMap {
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

	// Unlink (handles nlink decrement + cleanup)
	if err := gw.meta.Unlink(ctx, b.RootInode, key); err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) {
			// S3 returns 204 even if key doesn't exist
			w.WriteHeader(http.StatusNoContent)
			return
		}
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), "/"+bucket+"/"+key, requestID)
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
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
}

// handleCopyObject handles PUT /{bucket}/{key+} with X-Amz-Copy-Source header
func (gw *Gateway) handleCopyObject(w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	ctx := r.Context()

	copySource := r.Header.Get("X-Amz-Copy-Source")
	copySource, _ = url.PathUnescape(copySource)
	copySource = strings.TrimPrefix(copySource, "/")

	srcBucket, srcKey := parsePath("/" + copySource)
	if srcBucket == "" || srcKey == "" {
		WriteXMLError(w, http.StatusBadRequest, ErrCodeInvalidArgument,
			"Invalid copy source", "/"+bucket+"/"+key, requestID)
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

	// Get dest bucket
	dstB, err := gw.meta.GetBucket(ctx, bucket)
	if err != nil {
		WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchBucket,
			"Destination bucket not found", "/"+bucket, requestID)
		return
	}

	// Create hard link to same inode (server-side copy via shared chunks)
	_, err = gw.meta.Link(ctx, dstB.RootInode, key, srcInode.ID)
	if err != nil {
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), "/"+bucket+"/"+key, requestID)
		return
	}

	result := CopyObjectResult{
		LastModified: FormatS3Time(unixNanoToTime(srcInode.MTime)),
		ETag:         FormatETag(0),
	}
	WriteXML(w, http.StatusOK, result)
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
