package s3

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"
)

var (
	activeUploads = &uploadTracker{
		uploads: make(map[string]*multipartUpload),
	}
)

type uploadTracker struct {
	mu      sync.RWMutex
	uploads map[string]*multipartUpload

	partDir string // temp directory for part data on disk
}

func (t *uploadTracker) init(partDir string) error {
	t.partDir = partDir
	if partDir != "" {
		return os.MkdirAll(partDir, 0700)
	}
	return nil
}

type multipartUpload struct {
	UploadID  string
	Bucket    string
	Key       string
	CreatedAt time.Time
	Parts     map[int]*uploadPart
	partDir   string
	mu        sync.Mutex
}

type uploadPart struct {
	PartNumber int
	Size       int64
	ETag       string
	partPath   string // empty if data is in memory
	Data       []byte // in-memory fallback (only used when partDir is empty)
}

func (t *uploadTracker) create(bucket, key string) string {
	t.mu.Lock()
	defer t.mu.Unlock()

	uploadID := generateRequestID()
	t.uploads[uploadID] = &multipartUpload{
		UploadID:  uploadID,
		Bucket:    bucket,
		Key:       key,
		CreatedAt: time.Now(),
		Parts:     make(map[int]*uploadPart),
		partDir:   t.partDir,
	}
	return uploadID
}

func (t *uploadTracker) get(uploadID string) (*multipartUpload, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	u, ok := t.uploads[uploadID]
	return u, ok
}

func (t *uploadTracker) remove(uploadID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.uploads, uploadID)
}

func (t *uploadTracker) list(bucket string) []*multipartUpload {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var result []*multipartUpload
	for _, u := range t.uploads {
		if u.Bucket == bucket {
			result = append(result, u)
		}
	}
	return result
}

// cleanupPart removes the temp file for a part, if any.
func (p *uploadPart) cleanup() {
	if p.partPath != "" {
		os.Remove(p.partPath)
	}
}

// readAll returns the full part data, reading from disk if needed.
func (p *uploadPart) readAll() ([]byte, error) {
	if p.partPath != "" {
		return os.ReadFile(p.partPath)
	}
	return p.Data, nil
}

// writePart stores part data to disk when partDir is configured, or in memory otherwise.
func (t *uploadTracker) writePart(upload *multipartUpload, partNum int, data []byte, etag string) error {
	p := &uploadPart{
		PartNumber: partNum,
		Size:       int64(len(data)),
		ETag:       etag,
	}
	if t.partDir != "" {
		partPath := path.Join(t.partDir, fmt.Sprintf("%s-%05d", upload.UploadID, partNum))
		if err := os.WriteFile(partPath, data, 0600); err != nil {
			return err
		}
		p.partPath = partPath
	} else {
		buf := make([]byte, len(data))
		copy(buf, data)
		p.Data = buf
	}

	upload.mu.Lock()
	// Clean up previous part data for this part number
	if old, ok := upload.Parts[partNum]; ok {
		old.cleanup()
	}
	upload.Parts[partNum] = p
	upload.mu.Unlock()
	return nil
}

// cleanupUpload removes all temp files for a multipart upload.
func cleanupUpload(upload *multipartUpload) {
	for _, p := range upload.Parts {
		p.cleanup()
	}
}

// handleInitiateMultipartUpload handles POST /{bucket}/{key}?uploads
func (gw *Gateway) handleInitiateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	uploadID := activeUploads.create(bucket, key)

	result := InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}
	WriteXML(w, http.StatusOK, result)
}

// handleUploadPart handles PUT /{bucket}/{key}?partNumber=N&uploadId=xxx
func (gw *Gateway) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	q := r.URL.Query()
	uploadID := q.Get("uploadId")
	partNumStr := q.Get("partNumber")

	partNum, err := strconv.Atoi(partNumStr)
	if err != nil || partNum < 1 {
		WriteXMLError(w, http.StatusBadRequest, ErrCodeInvalidArgument,
			"Invalid part number", "/"+bucket+"/"+key, requestID)
		return
	}

	upload, ok := activeUploads.get(uploadID)
	if !ok {
		WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchUpload,
			"The specified multipart upload does not exist", "/"+bucket+"/"+key, requestID)
		return
	}

	// Read part data. The 5 GiB per-part S3 limit is also our cap; we
	// rely on the gateway-wide MaxObjectSize to reject anything larger.
	r.Body = http.MaxBytesReader(w, r.Body, gw.maxObjectSize)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			WriteXMLError(w, http.StatusRequestEntityTooLarge, ErrCodeEntityTooLarge,
				fmt.Sprintf("Part exceeds %d bytes", gw.maxObjectSize),
				"/"+bucket+"/"+key, requestID)
			return
		}
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			"Failed to read part data", "/"+bucket+"/"+key, requestID)
		return
	}

	etag := fmt.Sprintf("\"%08x\"", crc32Checksum(data))

	if err := activeUploads.writePart(upload, partNum, data, etag); err != nil {
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			"Failed to store part data", "/"+bucket+"/"+key, requestID)
		return
	}

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

// handleCompleteMultipartUpload handles POST /{bucket}/{key}?uploadId=xxx
func (gw *Gateway) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	uploadID := r.URL.Query().Get("uploadId")

	upload, ok := activeUploads.get(uploadID)
	if !ok {
		WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchUpload,
			"The specified multipart upload does not exist", "/"+bucket+"/"+key, requestID)
		return
	}

	// Parse completion request
	body, err := io.ReadAll(r.Body)
	if err != nil {
		WriteXMLError(w, http.StatusBadRequest, ErrCodeInvalidArgument,
			"Failed to read request body", "/"+bucket+"/"+key, requestID)
		return
	}

	var completeReq CompleteMultipartUpload
	if err := xml.Unmarshal(body, &completeReq); err != nil {
		WriteXMLError(w, http.StatusBadRequest, ErrCodeInvalidArgument,
			"Invalid XML in request", "/"+bucket+"/"+key, requestID)
		return
	}

	// Verify all parts exist
	upload.mu.Lock()
	defer upload.mu.Unlock()

	var totalSize int64
	sortedParts := make([]*uploadPart, 0, len(completeReq.Parts))
	for _, cp := range completeReq.Parts {
		part, ok := upload.Parts[cp.PartNumber]
		if !ok {
			upload.mu.Unlock()
			WriteXMLError(w, http.StatusBadRequest, ErrCodeInvalidPart,
				fmt.Sprintf("Part %d not found", cp.PartNumber), "/"+bucket+"/"+key, requestID)
			upload.mu.Lock()
			return
		}
		sortedParts = append(sortedParts, part)
		totalSize += part.Size
	}

	// Sort by part number
	sort.Slice(sortedParts, func(i, j int) bool {
		return sortedParts[i].PartNumber < sortedParts[j].PartNumber
	})

	// In production: merge all part data into chunks, write to data nodes,
	// register chunks in metadata, and create the file inode.
	// For now, we just clean up the upload tracker.

	activeUploads.remove(uploadID)

	result := CompleteMultipartUploadResult{
		Location: fmt.Sprintf("/%s/%s", bucket, key),
		Bucket:   bucket,
		Key:      key,
		ETag:     FormatETag(crc32Checksum([]byte(uploadID))),
	}
	WriteXML(w, http.StatusOK, result)
}

// handleAbortMultipartUpload handles DELETE /{bucket}/{key}?uploadId=xxx
func (gw *Gateway) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	uploadID := r.URL.Query().Get("uploadId")

	upload, ok := activeUploads.get(uploadID)
	if !ok {
		WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchUpload,
			"The specified multipart upload does not exist", "/"+bucket+"/"+key, requestID)
		return
	}

	cleanupUpload(upload)
	activeUploads.remove(uploadID)
	w.WriteHeader(http.StatusNoContent)
}

// handleListParts handles GET /{bucket}/{key}?uploadId=xxx
func (gw *Gateway) handleListParts(w http.ResponseWriter, r *http.Request, bucket, key, requestID string) {
	uploadID := r.URL.Query().Get("uploadId")

	upload, ok := activeUploads.get(uploadID)
	if !ok {
		WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchUpload,
			"The specified multipart upload does not exist", "/"+bucket+"/"+key, requestID)
		return
	}

	upload.mu.Lock()
	defer upload.mu.Unlock()

	result := ListPartsResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
		MaxParts: 1000,
	}

	for _, part := range upload.Parts {
		result.Parts = append(result.Parts, PartEntry{
			PartNumber:   part.PartNumber,
			LastModified: FormatS3Time(upload.CreatedAt),
			ETag:         part.ETag,
			Size:         part.Size,
		})
	}

	sort.Slice(result.Parts, func(i, j int) bool {
		return result.Parts[i].PartNumber < result.Parts[j].PartNumber
	})

	WriteXML(w, http.StatusOK, result)
}

// handleListMultipartUploads handles GET /{bucket}?uploads
func (gw *Gateway) handleListMultipartUploads(w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	uploads := activeUploads.list(bucket)

	// Build response as a simplified XML
	type UploadEntry struct {
		XMLName   xml.Name `xml:"Upload"`
		Key       string   `xml:"Key"`
		UploadID  string   `xml:"UploadId"`
		Initiated string   `xml:"Initiated"`
	}
	type ListMultipartUploadsResult struct {
		XMLName xml.Name      `xml:"ListMultipartUploadsResult"`
		Bucket  string        `xml:"Bucket"`
		Uploads []UploadEntry `xml:"Upload,omitempty"`
	}

	result := ListMultipartUploadsResult{
		Bucket: bucket,
	}
	for _, u := range uploads {
		result.Uploads = append(result.Uploads, UploadEntry{
			Key:       u.Key,
			UploadID:  u.UploadID,
			Initiated: FormatS3Time(u.CreatedAt),
		})
	}
	WriteXML(w, http.StatusOK, result)
}
