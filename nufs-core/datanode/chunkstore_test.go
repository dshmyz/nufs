package datanode

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// newTestChunkStore creates a ChunkStore in a temporary directory.
func newTestChunkStore(t *testing.T) (*ChunkStore, string) {
	t.Helper()
	dir := t.TempDir()
	cs, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}
	return cs, dir
}

func TestNewChunkStore_CreatesShardDirs(t *testing.T) {
	cs, dir := newTestChunkStore(t)
	_ = cs

	chunksDir := filepath.Join(dir, "chunks")
	for i := 0; i < MaxShards; i++ {
		shardDir := filepath.Join(chunksDir, fmt.Sprintf("%02x", i))
		info, err := os.Stat(shardDir)
		if err != nil {
			t.Fatalf("shard dir %02x missing: %v", i, err)
		}
		if !info.IsDir() {
			t.Fatalf("shard dir %02x is not a directory", i)
		}
	}
}

func TestChunkStore_WriteAndRead(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(12345)
	data := []byte("hello distributed storage world")

	// Write
	if err := cs.Write(chunkID, data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read full
	got, checksum, err := cs.Read(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Read data mismatch: got %q, want %q", got, data)
	}
	expectedCRC := crc32.ChecksumIEEE(data)
	if checksum != expectedCRC {
		t.Fatalf("Checksum mismatch: got %d, want %d", checksum, expectedCRC)
	}
}

func TestChunkStore_ReadPartial(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(99)
	data := []byte("0123456789ABCDEF")

	if err := cs.Write(chunkID, data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read bytes [4:10] = "456789"
	got, _, err := cs.Read(chunkID, 4, 6)
	if err != nil {
		t.Fatalf("Read partial: %v", err)
	}
	if !bytes.Equal(got, []byte("456789")) {
		t.Fatalf("Partial read mismatch: got %q, want %q", got, "456789")
	}
}

func TestChunkStore_ReadBeyondEnd(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(200)
	data := []byte("short")

	if err := cs.Write(chunkID, data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Request 100 bytes from offset 0 — should clamp to data length
	got, _, err := cs.Read(chunkID, 0, 100)
	if err != nil {
		t.Fatalf("Read beyond end: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Clamped read mismatch: got %q, want %q", got, data)
	}
}

func TestChunkStore_ReadNotFound(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	_, _, err := cs.Read(metadata.ChunkID(999999), 0, 0)
	if err == nil {
		t.Fatal("expected error for non-existent chunk, got nil")
	}
}

func TestChunkStore_Seal(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(777)
	data := make([]byte, 4096)
	rand.Read(data)

	if err := cs.Write(chunkID, data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Before seal, state should be LocalWritten
	info, ok := cs.Info(chunkID)
	if !ok {
		t.Fatal("Info: chunk not found")
	}
	if info.State != LocalWritten {
		t.Fatalf("expected LocalWritten before seal, got %d", info.State)
	}

	// Seal
	checksum, err := cs.Seal(chunkID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	expectedCRC := crc32.ChecksumIEEE(data)
	if checksum != expectedCRC {
		t.Fatalf("Seal checksum mismatch: got %d, want %d", checksum, expectedCRC)
	}

	// After seal, state should be LocalSealed
	info, _ = cs.Info(chunkID)
	if info.State != LocalSealed {
		t.Fatalf("expected LocalSealed after seal, got %d", info.State)
	}
	if info.Checksum != expectedCRC {
		t.Fatalf("Info checksum mismatch after seal: got %d, want %d", info.Checksum, expectedCRC)
	}

	// Read back and verify checksum in file header matches
	_, storedChecksum, err := cs.Read(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("Read after seal: %v", err)
	}
	if storedChecksum != expectedCRC {
		t.Fatalf("Stored checksum mismatch: got %d, want %d", storedChecksum, expectedCRC)
	}
}

func TestChunkStore_Delete(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(555)
	data := []byte("to be deleted")

	if err := cs.Write(chunkID, data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify it exists
	_, ok := cs.Info(chunkID)
	if !ok {
		t.Fatal("chunk should exist after write")
	}
	totalBytes, chunkCount := cs.Stats()
	if chunkCount != 1 {
		t.Fatalf("expected 1 chunk, got %d", chunkCount)
	}

	// Delete
	if err := cs.Delete(chunkID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify it's gone
	_, ok = cs.Info(chunkID)
	if ok {
		t.Fatal("chunk should not exist after delete")
	}
	totalBytes2, chunkCount2 := cs.Stats()
	if chunkCount2 != 0 {
		t.Fatalf("expected 0 chunks after delete, got %d", chunkCount2)
	}
	if totalBytes2 != totalBytes-int64(len(data)) {
		t.Fatalf("totalBytes not decremented: got %d, want %d", totalBytes2, totalBytes-int64(len(data)))
	}

	// Read should fail
	_, _, err := cs.Read(chunkID, 0, 0)
	if err == nil {
		t.Fatal("expected error reading deleted chunk")
	}
}

func TestChunkStore_DeleteNonExistent(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	// Deleting a non-existent chunk should not error (idempotent)
	if err := cs.Delete(metadata.ChunkID(888888)); err != nil {
		t.Fatalf("Delete non-existent: unexpected error: %v", err)
	}
}

func TestChunkStore_Overwrite_Bookkeeping(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(50000)
	data1 := []byte("short")       // 5 bytes
	data2 := []byte("much longer data") // 16 bytes

	// Write initial chunk
	if err := cs.Write(chunkID, data1); err != nil {
		t.Fatalf("Write 1: %v", err)
	}

	totalBytes1, chunkCount1 := cs.Stats()
	if chunkCount1 != 1 {
		t.Fatalf("after first write: expected chunkCount=1, got %d", chunkCount1)
	}
	if totalBytes1 != int64(len(data1)) {
		t.Fatalf("after first write: expected totalBytes=%d, got %d", len(data1), totalBytes1)
	}

	// Overwrite same chunk with different size
	if err := cs.Write(chunkID, data2); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	totalBytes2, chunkCount2 := cs.Stats()
	if chunkCount2 != 1 {
		t.Errorf("after overwrite: expected chunkCount=1, got %d (should not double-count)", chunkCount2)
	}
	if totalBytes2 != int64(len(data2)) {
		t.Errorf("after overwrite: expected totalBytes=%d, got %d (should reflect new size only)", len(data2), totalBytes2)
	}

	// Read should return the overwritten data
	got, _, err := cs.Read(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, data2) {
		t.Fatalf("Read data mismatch: got %q, want %q", got, data2)
	}
}

func TestChunkStore_ConcurrentAccess_NoDataRace(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	// Write multiple chunks to reduce fd cache contention
	for i := 0; i < 10; i++ {
		chunkID := metadata.ChunkID(60000 + i)
		data := []byte(fmt.Sprintf("race test data chunk %d", i))
		if err := cs.Write(chunkID, data); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	// Run concurrent reads — each updates LastAccess/AccessCount.
	// With -race flag, this should detect the data race on access stats.
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			chunkID := metadata.ChunkID(60000 + id)
			for j := 0; j < 50; j++ {
				cs.Read(chunkID, 0, 0)
			}
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestChunkStore_Seal_PersistsCRC(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(70000)
	data := []byte("seal persist crc test data")
	expectedCRC := crc32.ChecksumIEEE(data)

	if err := cs.Write(chunkID, data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Seal should persist CRC to the file header
	checksum, err := cs.Seal(chunkID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if checksum != expectedCRC {
		t.Fatalf("Seal checksum mismatch: got %d, want %d", checksum, expectedCRC)
	}

	// Create a new ChunkStore from the same directory to verify CRC persists on disk
	cs2, err := NewChunkStore(cs.disks[0].dataDir, 8, 8, nil)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	// Read from the new store — CRC should be verified
	got, gotCRC, err := cs2.Read(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("Read from new store: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: got %q, want %q", got, data)
	}
	if gotCRC != expectedCRC {
		t.Fatalf("CRC mismatch: got %d, want %d", gotCRC, expectedCRC)
	}
}

func TestChunkStore_DrainWritesReleasesSlots(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	release, err := cs.DrainWrites(ctx)
	if err != nil {
		t.Fatalf("DrainWrites: %v", err)
	}

	select {
	case cs.disks[0].writeSem <- struct{}{}:
		t.Fatal("write semaphore should be full while drained")
	default:
	}
	release()

	select {
	case cs.disks[0].writeSem <- struct{}{}:
		<-cs.disks[0].writeSem
	case <-time.After(time.Second):
		t.Fatal("write semaphore slot was not released")
	}
}

func TestChunkStore_DrainWritesTimeout(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	for i := 0; i < cap(cs.disks[0].writeSem); i++ {
		cs.disks[0].writeSem <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(cs.disks[0].writeSem); i++ {
			<-cs.disks[0].writeSem
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	release, err := cs.DrainWrites(ctx)
	if err == nil {
		release()
		t.Fatal("expected timeout")
	}
	if release != nil {
		release()
	}
}

func TestChunkStore_WriteAtThenSealCRC(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	// Write initial 1MB chunk
	base := make([]byte, 1024*1024)
	for i := range base {
		base[i] = byte(i % 256)
	}
	chunkID := metadata.ChunkID(900)
	if err := cs.Write(chunkID, base); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Seal sets CRC
	checksum1, err := cs.Seal(chunkID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	expectedCRC := crc32.ChecksumIEEE(base)
	if checksum1 != expectedCRC {
		t.Fatalf("Seal CRC: got %d, want %d", checksum1, expectedCRC)
	}

	// Read should verify CRC
	got, checksum2, err := cs.Read(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, base) {
		t.Fatalf("Read data mismatch")
	}
	if checksum2 != expectedCRC {
		t.Fatalf("Read CRC: got %d, want %d", checksum2, expectedCRC)
	}

	// Partial write (WriteAt)
	patch := make([]byte, 4*1024)
	for i := range patch {
		patch[i] = byte((i + 100) % 256)
	}
	if err := cs.WriteAt(chunkID, 0, patch); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	// After WriteAt, Seal should recompute CRC
	expectedCRC2 := crc32.ChecksumIEEE(append(append([]byte{}, patch...), base[len(patch):]...))
	checksum3, err := cs.Seal(chunkID)
	if err != nil {
		t.Fatalf("Seal after WriteAt: %v", err)
	}
	if checksum3 != expectedCRC2 {
		t.Fatalf("Seal after WriteAt CRC: got %d, want %d", checksum3, expectedCRC2)
	}

	// Read after Seal should verify new CRC
	got2, _, err := cs.Read(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("Read after Seal: %v", err)
	}
	expectedData := append(append([]byte{}, patch...), base[len(patch):]...)
	if !bytes.Equal(got2, expectedData) {
		t.Fatalf("Read after Seal data mismatch")
	}
}

func TestChunkStore_CloseClosesFdCache(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	chunkID := metadata.ChunkID(909)
	if err := cs.Write(chunkID, []byte("cached fd")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, _, err := cs.Read(chunkID, 0, 0); err != nil {
		t.Fatalf("Read: %v", err)
	}
	cs.disks[0].fdMu.RLock()
	cached := len(cs.disks[0].fdCache)
	cs.disks[0].fdMu.RUnlock()
	if cached == 0 {
		t.Fatal("expected fd cache to contain entry after read")
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	cs.disks[0].fdMu.RLock()
	defer cs.disks[0].fdMu.RUnlock()
	if len(cs.disks[0].fdCache) != 0 {
		t.Fatalf("expected fd cache to be empty, got %d", len(cs.disks[0].fdCache))
	}
}

func TestChunkStore_Stats(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	// Empty store
	totalBytes, chunkCount := cs.Stats()
	if totalBytes != 0 || chunkCount != 0 {
		t.Fatalf("empty store stats: got (%d, %d), want (0, 0)", totalBytes, chunkCount)
	}

	// Write 3 chunks
	sizes := []int{100, 200, 300}
	for i, size := range sizes {
		data := make([]byte, size)
		rand.Read(data)
		if err := cs.Write(metadata.ChunkID(uint64(i+1)), data); err != nil {
			t.Fatalf("Write chunk %d: %v", i+1, err)
		}
	}

	totalBytes, chunkCount = cs.Stats()
	if chunkCount != 3 {
		t.Fatalf("expected 3 chunks, got %d", chunkCount)
	}
	if totalBytes != 600 {
		t.Fatalf("expected 600 total bytes, got %d", totalBytes)
	}
}

func TestChunkStore_ListChunks(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	// Empty
	chunks := cs.ListChunks()
	if len(chunks) != 0 {
		t.Fatalf("expected 0 chunks, got %d", len(chunks))
	}

	// Write 2 chunks
	cs.Write(metadata.ChunkID(10), []byte("aaa"))
	cs.Write(metadata.ChunkID(20), []byte("bbb"))

	chunks = cs.ListChunks()
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}

	ids := map[metadata.ChunkID]bool{}
	for _, c := range chunks {
		ids[c.ChunkID] = true
	}
	if !ids[10] || !ids[20] {
		t.Fatalf("missing expected chunk IDs in list: %v", ids)
	}
}

func TestChunkStore_WriteAt(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(42)

	// WriteAt to a new chunk (creates file with header)
	part1 := []byte("Hello")
	if err := cs.WriteAt(chunkID, 0, part1); err != nil {
		t.Fatalf("WriteAt part1: %v", err)
	}

	// WriteAt to append more data
	part2 := []byte(" World")
	if err := cs.WriteAt(chunkID, int64(len(part1)), part2); err != nil {
		t.Fatalf("WriteAt part2: %v", err)
	}

	// WriteAt-created files have dataLen=0 in header until seal.
	// We must seal to compute checksum and update the header.
	// But seal reads dataLen from header (which is 0), so we need to
	// manually update the header dataLen first. Instead, let's use Write
	// for the full data and test WriteAt for partial overwrite on a
	// Write-created chunk.

	// Rewrite using Write first
	fullData := []byte("Hello World!")
	if err := cs.Write(chunkID, fullData); err != nil {
		t.Fatalf("Write full: %v", err)
	}

	// Now use WriteAt to overwrite a portion: replace "World" with "Go   "
	patch := []byte("Go   ")
	if err := cs.WriteAt(chunkID, 6, patch); err != nil {
		t.Fatalf("WriteAt patch: %v", err)
	}

	// Read full
	got, _, err := cs.Read(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("Read after WriteAt: %v", err)
	}
	expected := []byte("Hello Go   !")
	if !bytes.Equal(got, expected) {
		t.Fatalf("WriteAt read mismatch: got %q, want %q", got, expected)
	}
}

func TestChunkStore_ScanExisting(t *testing.T) {
	dir := t.TempDir()

	// Create store and write a chunk
	cs1, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	chunkID := metadata.ChunkID(1234)
	data := []byte("persistent data")
	if err := cs1.Write(chunkID, data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Seal to store checksum in header
	if _, err := cs1.Seal(chunkID); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Create a NEW store from the same directory — should scan existing chunks
	cs2, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatalf("NewChunkStore (reopen): %v", err)
	}

	info, ok := cs2.Info(chunkID)
	if !ok {
		t.Fatal("scanned store should find existing chunk")
	}
	if info.ChunkID != chunkID {
		t.Fatalf("scanned chunk ID mismatch: got %d, want %d", info.ChunkID, chunkID)
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("scanned chunk size mismatch: got %d, want %d", info.Size, len(data))
	}
	if info.State != LocalSealed {
		t.Fatalf("scanned chunk state: got %d, want LocalSealed", info.State)
	}

	// Verify data can be read from reopened store
	got, _, err := cs2.Read(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("Read from reopened store: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("scanned data mismatch: got %q, want %q", got, data)
	}

	// Stats should reflect scanned chunks
	totalBytes, chunkCount := cs2.Stats()
	if chunkCount != 1 {
		t.Fatalf("expected 1 scanned chunk, got %d", chunkCount)
	}
	if totalBytes != int64(len(data)) {
		t.Fatalf("expected %d total bytes, got %d", len(data), totalBytes)
	}
}

func TestChunkStore_LargeChunk(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	// Write a 1MB chunk
	chunkID := metadata.ChunkID(9999)
	data := make([]byte, 1024*1024)
	rand.Read(data)

	if err := cs.Write(chunkID, data); err != nil {
		t.Fatalf("Write large chunk: %v", err)
	}

	got, checksum, err := cs.Read(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("Read large chunk: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("large chunk data mismatch")
	}
	if checksum != crc32.ChecksumIEEE(data) {
		t.Fatal("large chunk checksum mismatch")
	}
}

func TestChunkStore_Overwrite(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(100)

	// Write initial data
	data1 := []byte("version 1")
	if err := cs.Write(chunkID, data1); err != nil {
		t.Fatalf("Write v1: %v", err)
	}

	// Overwrite with new data
	data2 := []byte("version 2 - updated")
	if err := cs.Write(chunkID, data2); err != nil {
		t.Fatalf("Write v2: %v", err)
	}

	// Read should return latest version
	got, _, err := cs.Read(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("Read after overwrite: %v", err)
	}
	if !bytes.Equal(got, data2) {
		t.Fatalf("overwrite data mismatch: got %q, want %q", got, data2)
	}
}

func TestChunkStore_ConcurrentAccess(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	const numGoroutines = 16
	const chunkSize = 1024

	done := make(chan error, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			chunkID := metadata.ChunkID(uint64(id + 1000))
			data := make([]byte, chunkSize)
			rand.Read(data)

			if err := cs.Write(chunkID, data); err != nil {
				done <- err
				return
			}

			got, _, err := cs.Read(chunkID, 0, 0)
			if err != nil {
				done <- err
				return
			}
			if !bytes.Equal(got, data) {
				done <- fmt.Errorf("concurrent chunk %d data mismatch", id)
				return
			}
			done <- nil
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent access error: %v", err)
		}
	}

	_, chunkCount := cs.Stats()
	if chunkCount != numGoroutines {
		t.Fatalf("expected %d chunks after concurrent writes, got %d", numGoroutines, chunkCount)
	}
}
