package s3

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

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

	// Read body
	data, err := io.ReadAll(io.LimitReader(r.Body, 5*1024*1024*1024)) // 5GB limit
	if err != nil {
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			"Failed to read request body: "+err.Error(), "/"+bucket+"/"+key, requestID)
		return
	}

	// Try to create file; if exists, unlink old and recreate
	inode, err := gw.meta.CreateFile(ctx, b.RootInode, key, 0644)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			// Overwrite: unlink existing entry
			_ = gw.meta.Unlink(ctx, b.RootInode, key)
			inode, err = gw.meta.CreateFile(ctx, b.RootInode, key, 0644)
			if err != nil {
				WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
					err.Error(), "/"+bucket+"/"+key, requestID)
				return
			}
		} else {
			WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
				err.Error(), "/"+bucket+"/"+key, requestID)
			return
		}
	}

	// Allocate a chunk for the data
	chunk, err := gw.meta.AllocateChunk(ctx, inode.ID, 0, b.Policy)
	if err != nil {
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			"Failed to allocate chunk: "+err.Error(), "/"+bucket+"/"+key, requestID)
		return
	}

	// In production: write data to primary replica data node via TCP,
	// then commit chunk with checksum. For now, commit directly.
	checksum := crc32Checksum(data)
	err = gw.meta.CommitChunk(ctx, chunk.ID, checksum)
	if err != nil {
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			"Failed to commit chunk: "+err.Error(), "/"+bucket+"/"+key, requestID)
		return
	}

	// Update inode with size and chunk reference
	inode.Size = int64(len(data))
	inode.ChunkMap = []metadata.ChunkRef{
		{ID: chunk.ID, Offset: 0, Length: int32(len(data)), Version: 1},
	}
	err = gw.meta.UpdateInode(ctx, inode)
	if err != nil {
		// Non-fatal: data is written, metadata update may lag
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

	// TODO: In production, read chunks from data nodes and stream to client
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
	var crc uint32 = 0xFFFFFFFF
	for _, b := range data {
		crc ^= uint32(b)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xEDB88320
			} else {
				crc >>= 1
			}
		}
	}
	return crc ^ 0xFFFFFFFF
}
