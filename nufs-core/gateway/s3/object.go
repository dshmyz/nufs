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
	// exactly when the client overshoots.
	r.Body = http.MaxBytesReader(w, r.Body, gw.maxObjectSize)

	// Try to create file; if exists, look up the existing inode and
	// update it in place.
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

	// Acquire an exclusive advisory lock on the inode
	lockOwner := fmt.Sprintf("s3gw-%s-%s-%d", bucket, key, time.Now().UnixNano())
	if err := gw.meta.AdvisoryLock(ctx, inode.ID, lockOwner); err != nil {
		WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeSlowDown,
			"Object is locked by another writer: "+err.Error(),
			"/"+bucket+"/"+key, requestID)
		return
	}
	defer gw.meta.AdvisoryUnlock(ctx, inode.ID, lockOwner)

	// Determine content length if known (Content-Length header)
	contentLength := int64(-1)
	if r.ContentLength > 0 {
		contentLength = r.ContentLength
	}

	// --- Streaming chunked write ---
	// Read body in MaxChunkSize pieces, allocating and writing chunks
	// one at a time. If Content-Length is known and requires multiple
	// chunks, use batch allocation to reduce metadata round trips.
	var (
		newChunkRefs []metadata.ChunkRef
		totalSize    int64
		hash         = sha256.New()
	)

	// Pre-allocate chunks if Content-Length is known and large enough
	if contentLength > metadata.MaxChunkSize {
		numChunks := int((contentLength + metadata.MaxChunkSize - 1) / metadata.MaxChunkSize)
		offsets := make([]int64, numChunks)
		for i := 0; i < numChunks; i++ {
			offsets[i] = int64(i) * metadata.MaxChunkSize
		}

		preAllocChunks, err := gw.meta.AllocateChunksBatch(ctx, inode.ID, offsets, b.Policy)
		if err != nil {
			WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
				"Failed to batch allocate chunks: "+err.Error(), "/"+bucket+"/"+key, requestID)
			return
		}

		// Validate replica availability
		if gw.rejectEmptyReplicas {
			for _, ch := range preAllocChunks {
				if len(ch.Replicas) == 0 {
					WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeServiceUnavailable,
						"No datanode replicas are available for this bucket's placement policy",
						"/"+bucket+"/"+key, requestID)
					return
				}
			}
		}

		// Stream data through pre-allocated chunks
		buf := make([]byte, metadata.MaxChunkSize)
		chunkIdx := 0
		remaining := contentLength

		for remaining > 0 && chunkIdx < len(preAllocChunks) {
			readSize := int64(metadata.MaxChunkSize)
			if remaining < readSize {
				readSize = remaining
			}

			// Read exactly readSize bytes into the buffer
			n, err := io.ReadFull(r.Body, buf[:readSize])
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				// MaxBytesReader trips here if the client exceeds the limit
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
			if n == 0 {
				break
			}

			chunkData := buf[:n]
			chunk := preAllocChunks[chunkIdx]

			checksum := crc32Checksum(chunkData)
			if err := gw.chunkStore.WriteChunk(ctx, chunk, chunkData); err != nil {
				WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
					"Failed to write chunk data: "+err.Error(), "/"+bucket+"/"+key, requestID)
				return
			}

			if err := gw.meta.CommitChunk(ctx, chunk.ID, checksum); err != nil {
				WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
					"Failed to commit chunk: "+err.Error(), "/"+bucket+"/"+key, requestID)
				return
			}

			if err := gw.meta.SealChunk(ctx, chunk.ID); err != nil {
				log.Printf("s3gw: seal chunk %d: %v", chunk.ID, err)
			}

			newChunkRefs = append(newChunkRefs, metadata.ChunkRef{
				ID:     chunk.ID,
				Offset: int64(chunkIdx) * metadata.MaxChunkSize,
				Length: int32(n),
				Version: 1,
			})
			_, _ = hash.Write(chunkData)
			totalSize += int64(n)
			remaining -= int64(n)
			chunkIdx++
		}
	} else {
		// Small or unknown size: allocate and write single chunks on the fly
		buf := make([]byte, metadata.MaxChunkSize)
		for {
			n, err := io.ReadFull(r.Body, buf)
			if n == 0 || err == io.EOF {
				break
			}
			if err != nil && err != io.ErrUnexpectedEOF {
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

			chunkData := buf[:n]
			chunk, err := gw.meta.AllocateChunk(ctx, inode.ID, totalSize, b.Policy)
			if err != nil {
				WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
					"Failed to allocate chunk: "+err.Error(), "/"+bucket+"/"+key, requestID)
				return
			}

			if gw.rejectEmptyReplicas && len(chunk.Replicas) == 0 {
				WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeServiceUnavailable,
					"No datanode replicas are available for this bucket's placement policy",
					"/"+bucket+"/"+key, requestID)
				return
			}

			checksum := crc32Checksum(chunkData)
			if err := gw.chunkStore.WriteChunk(ctx, chunk, chunkData); err != nil {
				WriteXMLError(w, http.StatusServiceUnavailable, ErrCodeServiceUnavailable,
					"Failed to write data to enough datanodes: "+err.Error(),
					"/"+bucket+"/"+key, requestID)
				return
			}

			if err := gw.meta.CommitChunk(ctx, chunk.ID, checksum); err != nil {
				WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
					"Failed to commit chunk: "+err.Error(), "/"+bucket+"/"+key, requestID)
				return
			}

			if err := gw.meta.SealChunk(ctx, chunk.ID); err != nil {
				log.Printf("s3gw: seal chunk %d: %v", chunk.ID, err)
			}

			newChunkRefs = append(newChunkRefs, metadata.ChunkRef{
				ID: chunk.ID, Offset: totalSize, Length: int32(n), Version: 1,
			})
			_, _ = hash.Write(chunkData)
			totalSize += int64(n)
		}
	}

	// Update inode with final size and all chunk references
	inode.Size = totalSize
	inode.ChunkMap = newChunkRefs
	if err := gw.meta.UpdateInode(ctx, inode); err != nil {
		log.Printf("s3gw: update inode %d: %v", inode.ID, err)
	}

	// Clean up old chunks (async)
	for _, cref := range oldChunks {
		_ = gw.meta.DeleteChunk(ctx, cref.ID)
	}

	// ETag from hash (truncated to 16 hex chars for consistency)
	etag := "\"" + hex.EncodeToString(hash.Sum(nil)[:8]) + "\""

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
