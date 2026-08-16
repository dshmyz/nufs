package s3

import (
	"bytes"
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

// errUploadFinished is returned by writePart when the upload has already been
// completed. The newly written part's staged data is discarded; the caller maps
// it to NoSuchUpload — a completed upload no longer exists as a target for
// further parts.
var errUploadFinished = errors.New("multipart: upload already completed")

var (
	activeUploads = &uploadTracker{
		uploads: make(map[string]*multipartUpload),
	}
)

const defaultUploadTTL = 24 * time.Hour

type uploadTracker struct {
	mu      sync.RWMutex
	uploads map[string]*multipartUpload

	partDir string // temp directory for part data on disk
	stopCh  chan struct{}
	stopped chan struct{}
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
	// finished marks a successfully completed upload. Complete is a terminal
	// operation: once set, any late UploadPart that already holds the upload
	// pointer is rejected and any second Complete fails with NoSuchUpload
	// (the tracker entry is removed on success, but a concurrent request may
	// have fetched the pointer before that removal).
	finished bool
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

// startCleanup launches a background goroutine that removes incomplete
// multipart uploads older than maxAge. Safe to call multiple times;
// any prior cleanup goroutine is stopped first.
func (t *uploadTracker) startCleanup(maxAge time.Duration) {
	// Stop any prior cleanup goroutine to avoid leaks.
	t.stopCleanup()
	t.stopCh = make(chan struct{})
	t.stopped = make(chan struct{})
	go func() {
		defer close(t.stopped)
		ticker := time.NewTicker(maxAge / 2)
		defer ticker.Stop()
		for {
			select {
			case <-t.stopCh:
				return
			case <-ticker.C:
				t.cleanupExpired(maxAge)
			}
		}
	}()
}

// stopCleanup terminates the background cleanup goroutine and waits for it.
// No-op if no cleanup goroutine is running.
func (t *uploadTracker) stopCleanup() {
	if t.stopCh != nil {
		close(t.stopCh)
	}
	if t.stopped != nil {
		<-t.stopped
	}
}

// cleanupExpired removes uploads older than maxAge and their part data.
//
// Locking: the tracker map is guarded by t.mu, while each upload's Parts map is
// guarded by that upload's own upload.mu (see writePart/cleanupUpload). We must
// never iterate or delete part data while holding only t.mu, nor run disk I/O
// (os.Remove) under the shared tracker lock — a slow disk would stall every
// concurrent writePart for the duration. So this method does two passes:
//
//  1. Under t.mu, collect the expired upload IDs and drop them from the map so
//     no new part PUT can reach them (get() returns false afterwards).
//  2. Outside t.mu, for each evicted upload, take its own upload.mu while
//     deleting its part files. This serializes against an in-flight writePart
//     that already holds the upload pointer, and keeps disk deletion off the
//     shared tracker lock.
func (t *uploadTracker) cleanupExpired(maxAge time.Duration) {
	cutoff := time.Now().Add(-maxAge)
	t.mu.Lock()
	expired := make([]*multipartUpload, 0, 4)
	for id, u := range t.uploads {
		if u.CreatedAt.Before(cutoff) {
			delete(t.uploads, id)
			expired = append(expired, u)
		}
	}
	t.mu.Unlock()

	for _, u := range expired {
		u.mu.Lock()
		cleanupUpload(u)
		u.mu.Unlock()
	}
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
	if upload.finished {
		// The upload was completed while this part was being read/written.
		// Discard the part we just stored — re-adding it would orphan the
		// temp file (or resurrect deleted data), as complete's cleanup already
		// removed it. Complete is terminal: the part cannot join the object.
		upload.mu.Unlock()
		if p.partPath != "" {
			os.Remove(p.partPath)
		}
		return errUploadFinished
	}
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
		if errors.Is(err, errUploadFinished) {
			WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchUpload,
				"The specified multipart upload has already been completed",
				"/"+bucket+"/"+key, requestID)
			return
		}
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

	// The merge and the entire object commit run under the upload's own mutex.
	// A concurrent UploadPart replaces a part (and deletes the old part file)
	// through writePart, so reads of the parts must serialize against it:
	// holding the lock keeps the part files we open from being unlinked
	// mid-merge. This also makes Complete a terminal operation — after it
	// succeeds, writePart rejects new parts and a second Complete fails with
	// NoSuchUpload.
	upload.mu.Lock()
	defer upload.mu.Unlock()

	if upload.finished {
		WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchUpload,
			"The specified multipart upload does not exist", "/"+bucket+"/"+key, requestID)
		return
	}
	if upload.Bucket != bucket || upload.Key != key {
		WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchUpload,
			"The specified multipart upload does not exist", "/"+bucket+"/"+key, requestID)
		return
	}
	if len(completeReq.Parts) == 0 {
		WriteXMLError(w, http.StatusBadRequest, ErrCodeInvalidRequest,
			"You must specify at least one part", "/"+bucket+"/"+key, requestID)
		return
	}

	// Assemble the object as the concatenation of the parts in the order the
	// caller listed them (the complete request, not the part numbers, defines
	// the byte order). Enforce S3's request invariants: strictly ascending
	// unique part numbers, ETags matching the staged parts, and non-empty
	// parts — a zero-length mid-list part would be silently absorbed by the
	// chunk-write loop (ReadFull n==0 → break) and truncate the object.
	var (
		mergedSize int64
		readers    []io.Reader
		openFiles  []*os.File
	)
	lastPartNumber := 0
	for _, cp := range completeReq.Parts {
		if cp.PartNumber <= lastPartNumber {
			WriteXMLError(w, http.StatusBadRequest, ErrCodeInvalidPart,
				"Part numbers must be listed in ascending order without duplicates",
				"/"+bucket+"/"+key, requestID)
			return
		}
		lastPartNumber = cp.PartNumber
		p, present := upload.Parts[cp.PartNumber]
		if !present {
			WriteXMLError(w, http.StatusBadRequest, ErrCodeInvalidPart,
				fmt.Sprintf("Part %d does not exist", cp.PartNumber), "/"+bucket+"/"+key, requestID)
			return
		}
		if cp.ETag != p.ETag {
			WriteXMLError(w, http.StatusBadRequest, ErrCodeInvalidPart,
				fmt.Sprintf("ETag for part %d does not match the uploaded part", cp.PartNumber),
				"/"+bucket+"/"+key, requestID)
			return
		}
		if p.Size == 0 {
			WriteXMLError(w, http.StatusBadRequest, ErrCodeInvalidPart,
				fmt.Sprintf("Part %d is empty", cp.PartNumber), "/"+bucket+"/"+key, requestID)
			return
		}
		if p.partPath != "" {
			f, err := os.Open(p.partPath)
			if err != nil {
				WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
					fmt.Sprintf("Failed to read part %d data", cp.PartNumber), "/"+bucket+"/"+key, requestID)
				return
			}
			openFiles = append(openFiles, f)
			readers = append(readers, f)
		} else {
			readers = append(readers, bytes.NewReader(p.Data))
		}
		mergedSize += p.Size
	}
	defer func() {
		for _, f := range openFiles {
			_ = f.Close()
		}
	}()

	// Commit the concatenated staged parts through the same orchestration as a
	// single PUT (metadataObjectCommitter.Put): the object lands via
	// CommitChunkRefsModelAware — inline extent (≤16MiB single ref) or COW
	// extent pages — with quota admission, advisory locking, overwrite
	// supersede + tombstone, and ECConfig ColdEC marking all inherited from the
	// single-PUT path. Roadmap §1.4: complete must no longer be able to produce
	// a V1 ChunkMap object.
	result, err := gw.committer.Put(r.Context(), PutObjectRequest{
		Bucket:        bucket,
		Key:           key,
		Body:          io.MultiReader(readers...),
		ContentLength: mergedSize,
		MaxObjectSize: gw.maxObjectSize,
		RequestID:     requestID,
	})
	if err != nil {
		gw.writePutObjectCommitterError(w, err, bucket, key, requestID)
		return
	}
	if result.Size != mergedSize {
		// A staged part file shorter than its recorded size would be silently
		// absorbed as a truncated object by the chunk loop; surface it so the
		// upload stays staged for a corrected Complete (or an abort) instead.
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			"merged object size does not match the staged part sizes",
			"/"+bucket+"/"+key, requestID)
		return
	}

	// Success: the upload is terminal. Drop its staged part data and evict it
	// from the tracker while still holding the upload lock, so a concurrent
	// writePart that already holds the pointer sees finished and cannot
	// resurrect a deleted part file. remove() takes the tracker lock under the
	// upload lock — a new upload.mu→tracker.mu nesting — but no path ever holds
	// the tracker lock while taking an upload lock, so there is no cycle.
	upload.finished = true
	cleanupUpload(upload)
	activeUploads.remove(uploadID)

	WriteXML(w, http.StatusOK, CompleteMultipartUploadResult{
		Bucket: bucket,
		Key:    key,
		ETag:   result.ETag,
	})
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

	// Remove the upload from the tracker while holding its own mutex so we
	// serialize against a concurrent writePart (which also holds upload.mu)
	// that may already be writing part data into this upload's Parts map.
	upload.mu.Lock()
	cleanupUpload(upload)
	upload.mu.Unlock()
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
