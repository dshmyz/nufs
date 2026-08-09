package datanode

import (
	"bytes"
	"hash/crc32"
	"os"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// openFileForCorruption opens a chunk file for read/write so tests
// can flip bytes to simulate bitrot.
func openFileForCorruption(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR, 0o644)
}

// TestChunkStore_RangeReadSealedNoAmplification verifies that a
// range read on a sealed chunk (with non-zero CRC) does NOT read
// the entire chunk into memory. The readAmplifiedBytes metric
// should be close to the requested bytes, not the full chunk size.
func TestChunkStore_RangeReadSealedNoAmplification(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(500)
	// Write 64KB of data
	fullData := make([]byte, 64*1024)
	for i := range fullData {
		fullData[i] = byte(i)
	}
	if err := cs.Write(chunkID, fullData); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Seal the chunk so CRC is set (non-zero)
	if _, err := cs.Seal(chunkID); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Reset perf counters by snapshotting before
	before := cs.PerfSnapshot()

	// Read only 4KB from offset 1024
	offset := int64(1024)
	wantLen := int32(4096)
	got, checksum, err := cs.Read(chunkID, offset, wantLen)
	if err != nil {
		t.Fatalf("Read range: %v", err)
	}

	if len(got) != int(wantLen) {
		t.Fatalf("read length: got %d, want %d", len(got), wantLen)
	}

	// Verify data correctness
	expected := fullData[offset : offset+int64(wantLen)]
	if !bytes.Equal(got, expected) {
		t.Fatalf("range read data mismatch")
	}

	// Verify checksum is returned (the chunk's stored CRC)
	if checksum == 0 {
		t.Fatal("expected non-zero checksum for sealed chunk")
	}
	if checksum != crc32.ChecksumIEEE(fullData) {
		t.Fatal("returned checksum does not match full data CRC")
	}

	// Verify read amplification is minimal: amplified bytes should
	// be close to requested bytes (4KB), not the full chunk (64KB).
	after := cs.PerfSnapshot()
	amplified := after.ReadAmplifiedBytes - before.ReadAmplifiedBytes
	requested := after.ReadRequestedBytes - before.ReadRequestedBytes

	if amplified > int64(wantLen)+64 { // allow small overhead for header
		t.Errorf("read amplification too high: amplified=%d bytes, requested=%d bytes (full chunk=%d bytes)",
			amplified, requested, len(fullData))
	}
}

// TestChunkStore_RangeReadSealedCorrectness verifies that range
// reads on sealed chunks return correct data at various offsets.
func TestChunkStore_RangeReadSealedCorrectness(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(501)
	fullData := make([]byte, 32*1024)
	for i := range fullData {
		fullData[i] = byte(i % 256)
	}
	if err := cs.Write(chunkID, fullData); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := cs.Seal(chunkID); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	cases := []struct {
		name   string
		offset int64
		length int32
	}{
		{"start", 0, 1024},
		{"middle", 8192, 4096},
		{"end", 30 * 1024, 2048},
		{"single_byte", 100, 1},
		{"full_read", 0, 0}, // full read, should still work
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := cs.Read(chunkID, tc.offset, tc.length)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}

			var expected []byte
			if tc.length == 0 {
				expected = fullData[tc.offset:]
			} else {
				end := tc.offset + int64(tc.length)
				if end > int64(len(fullData)) {
					end = int64(len(fullData))
				}
				expected = fullData[tc.offset:end]
			}

			if !bytes.Equal(got, expected) {
				t.Fatalf("data mismatch at offset=%d len=%d: got %d bytes, want %d bytes",
					tc.offset, tc.length, len(got), len(expected))
			}
		})
	}
}

// TestChunkStore_FullReadSealedVerifiesCRC verifies that a full
// read (offset=0, length=0) on a sealed chunk still verifies CRC
// and detects bitrot.
func TestChunkStore_FullReadSealedVerifiesCRC(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(502)
	fullData := []byte("data that will be corrupted after sealing")
	if err := cs.Write(chunkID, fullData); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := cs.Seal(chunkID); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Corrupt the chunk file on disk (flip a byte in the data region)
	path := cs.disks[0].chunkPath(chunkID)
	f, err := openFileForCorruption(path)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	// Write a corrupt byte at offset ChunkFileHeaderSize + 5
	corruptByte := []byte{0xFF}
	if _, err := f.WriteAt(corruptByte, int64(ChunkFileHeaderSize+5)); err != nil {
		f.Close()
		t.Fatalf("corrupt: %v", err)
	}
	f.Close()

	// Clear FD cache so the corrupted file is re-opened
	cs.mu.Lock()
	delete(cs.disks[0].fdCache, chunkID)
	cs.mu.Unlock()

	// Full read should detect CRC mismatch
	_, _, err = cs.Read(chunkID, 0, 0)
	if err == nil {
		t.Fatal("expected CRC mismatch error on corrupted sealed chunk, got nil")
	}
}

// TestChunkStore_RangeReadSealedSkipsCRCVerification verifies that
// a range read on a sealed chunk does NOT verify CRC (since it
// only reads partial data). This is the performance trade-off:
// range reads are fast but don't catch bitrot in the unread portion.
func TestChunkStore_RangeReadSealedSkipsCRCVerification(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(503)
	fullData := make([]byte, 8192)
	for i := range fullData {
		fullData[i] = byte(i)
	}
	if err := cs.Write(chunkID, fullData); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := cs.Seal(chunkID); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Corrupt a byte at offset 7000 (outside the range we'll read)
	path := cs.disks[0].chunkPath(chunkID)
	f, err := openFileForCorruption(path)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	corruptByte := []byte{0xFF}
	f.WriteAt(corruptByte, int64(ChunkFileHeaderSize+7000))
	f.Close()

	cs.mu.Lock()
	delete(cs.disks[0].fdCache, chunkID)
	cs.mu.Unlock()

	// Range read [0:1024] should succeed despite corruption at 7000
	// because range reads skip full CRC verification.
	got, _, err := cs.Read(chunkID, 0, 1024)
	if err != nil {
		t.Fatalf("range read should succeed despite out-of-range corruption: %v", err)
	}
	expected := fullData[0:1024]
	if !bytes.Equal(got, expected) {
		t.Fatal("range read data mismatch")
	}

	// But full read should detect the corruption
	_, _, err = cs.Read(chunkID, 0, 0)
	if err == nil {
		t.Fatal("full read should detect corruption, but got nil error")
	}
}
