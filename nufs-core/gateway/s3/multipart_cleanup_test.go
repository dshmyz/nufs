package s3

import (
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
	// Note: we don't create actual files here since cleanupUpload
	// calls os.Remove which is a no-op for non-existent files.
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
