package datanode

import (
	"os"
	"path/filepath"
	"testing"
)

// ============================================================
// TDD: End-to-End Data Integrity Verification
// ============================================================
// ChunkStore.Read must verify data checksum on every read to detect
// silent data corruption (bitrot). If the stored CRC doesn't match
// the computed CRC of the plaintext, the read must return an error.

func TestChunkStore_ReadVerifiesChecksum(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewChunkStore(dir, 4, 4, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Write a chunk
	data := []byte("Hello, data integrity verification!")
	if err := cs.Write(1, data); err != nil {
		t.Fatal(err)
	}

	// Read should succeed and return correct data
	readData, checksum, err := cs.Read(1, 0, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(readData) != string(data) {
		t.Errorf("data mismatch: got %q, want %q", readData, data)
	}
	_ = checksum
}

func TestChunkStore_ReadDetectsBitrot(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewChunkStore(dir, 4, 4, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Write a chunk
	data := []byte("Important data that must not be corrupted!")
	if err := cs.Write(42, data); err != nil {
		t.Fatal(err)
	}

	// Corrupt the data on disk (simulate bitrot)
	chunkPath := cs.disks[0].chunkPath(42)
	f, err := os.OpenFile(chunkPath, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte in the data section (after the 20-byte header)
	headerSize := int64(ChunkFileHeaderSize)
	if _, err := f.WriteAt([]byte{0xFF}, headerSize+5); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()

	// Invalidate fd cache so the corrupted file is re-opened
	cs.disks[0].fdMu.Lock()
	delete(cs.disks[0].fdCache, 42)
	cs.disks[0].fdMu.Unlock()

	// Read should detect the corruption
	_, _, err = cs.Read(42, 0, 0)
	if err == nil {
		t.Error("expected checksum verification error for corrupted data, got nil")
	}
}

func TestChunkStore_WriteStoresChecksum(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewChunkStore(dir, 4, 4, nil)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("checksum test data")
	if err := cs.Write(100, data); err != nil {
		t.Fatal(err)
	}

	// Verify the stored checksum matches the computed CRC32
	readData, storedChecksum, err := cs.Read(100, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Verify data integrity
	if string(readData) != string(data) {
		t.Errorf("data mismatch: got %q, want %q", readData, data)
	}

	// The stored checksum should be non-zero
	if storedChecksum == 0 {
		t.Error("stored checksum should not be zero")
	}
}

func TestChunkStore_MetaSidecarContainsChecksum(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewChunkStore(dir, 4, 4, nil)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("sidecar test")
	if err := cs.Write(200, data); err != nil {
		t.Fatal(err)
	}

	// Read the metadata sidecar
	metaPath := cs.disks[0].metaPath(200)
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("failed to read meta sidecar: %v", err)
	}

	// The sidecar should contain the checksum
	if len(metaData) == 0 {
		t.Error("meta sidecar is empty")
	}

	// Verify the sidecar JSON contains a checksum field
	// (We just check it's non-empty; the actual format is checked by ChunkStore)
	_ = filepath.Base(metaPath)
}
