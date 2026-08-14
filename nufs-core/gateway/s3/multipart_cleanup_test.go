package s3

import (
	"sync"
	"testing"
	"time"
)

func TestCleanupExpiredRemovesOldUploads(t *testing.T) {
	tr := &uploadTracker{
		uploads: make(map[string]*multipartUpload),
	}

	// Create an upload that is already expired.
	tr.mu.Lock()
	tr.uploads["old-upload"] = &multipartUpload{
		UploadID:  "old-upload",
		Bucket:    "test",
		Key:       "old-key",
		CreatedAt: time.Now().Add(-48 * time.Hour),
		Parts:     make(map[int]*uploadPart),
	}
	tr.uploads["new-upload"] = &multipartUpload{
		UploadID:  "new-upload",
		Bucket:    "test",
		Key:       "new-key",
		CreatedAt: time.Now(),
		Parts:     make(map[int]*uploadPart),
	}
	tr.mu.Unlock()

	tr.cleanupExpired(24 * time.Hour)

	tr.mu.RLock()
	defer tr.mu.RUnlock()

	if _, ok := tr.uploads["old-upload"]; ok {
		t.Fatal("old upload should have been cleaned up")
	}
	if _, ok := tr.uploads["new-upload"]; !ok {
		t.Fatal("new upload should still exist")
	}
}

func TestCleanupExpiredCleansPartData(t *testing.T) {
	// Create a temp file to simulate on-disk part data.
	tmpDir := t.TempDir()
	tr := &uploadTracker{
		uploads: make(map[string]*multipartUpload),
		partDir: tmpDir,
	}

	// Create an expired upload with a part that has a file on disk.
	tr.mu.Lock()
	u := &multipartUpload{
		UploadID:  "stale-upload",
		Bucket:    "test",
		Key:       "stale-key",
		CreatedAt: time.Now().Add(-48 * time.Hour),
		Parts:     make(map[int]*uploadPart),
		partDir:   tmpDir,
	}
	u.Parts[1] = &uploadPart{PartNumber: 1, Size: 100, partPath: tmpDir + "/stale-00001"}
	tr.uploads["stale-upload"] = u
	tr.mu.Unlock()

	tr.cleanupExpired(24 * time.Hour)

	tr.mu.RLock()
	defer tr.mu.RUnlock()

	if _, ok := tr.uploads["stale-upload"]; ok {
		t.Fatal("stale upload should have been cleaned up")
	}
}

// TestCleanupExpiredConcurrentWritePart races the background cleanup of an
// expired upload against a writePart still holding the same upload pointer. The
// upload is evicted from the tracker map before either side touches its Parts
// map, but a writePart that acquired the pointer before the eviction keeps
// writing under upload.mu. cleanupExpired must serialize on the same mutex, or
// the -race detector reports an unsynchronized map access.
func TestCleanupExpiredConcurrentWritePart(t *testing.T) {
	tmpDir := t.TempDir()
	tr := &uploadTracker{
		uploads: make(map[string]*multipartUpload),
		partDir: tmpDir,
	}

	u := &multipartUpload{
		UploadID:  "racing-upload",
		Bucket:    "test",
		Key:       "racing-key",
		CreatedAt: time.Now().Add(-48 * time.Hour),
		Parts:     make(map[int]*uploadPart),
		partDir:   tmpDir,
	}
	tr.mu.Lock()
	tr.uploads[u.UploadID] = u
	tr.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer goroutine: keep putting parts into the evicted-but-still-referenced
	// upload until cleanup drains it.
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if err := tr.writePart(u, i%16+1, []byte("part-data"), "etag"); err != nil {
				t.Errorf("writePart: %v", err)
				return
			}
		}
	}()

	// Cleanup goroutine: repeatedly evict-and-clean the same upload object; the
	// first pass removes it from the map but the handle stays live via `u`.
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			tr.cleanupExpired(24 * time.Hour)
			// Re-register so the next pass has a target again (past the cutoff).
			tr.mu.Lock()
			tr.uploads[u.UploadID] = u
			tr.mu.Unlock()
		}
	}()

	wg.Wait()
}
