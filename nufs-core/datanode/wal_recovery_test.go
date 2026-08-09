package datanode

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// TestWAL_RecoveryCleansOrphans simulates a crash between Write and
// LogCommit: a chunk .dat file exists on disk but the WAL has no
// corresponding commit entry. On restart, WAL.Recover() must clean
// up the orphan file, and scanExisting must not include it in the index.
func TestWAL_RecoveryCleansOrphans(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWriteAheadLog(dir + "/wal")
	if err != nil {
		t.Fatal(err)
	}

	// Phase 1: create a ChunkStore and write some committed chunks.
	cs, err := NewChunkStore(dir, 8, 8, wal)
	if err != nil {
		t.Fatal(err)
	}
	cs.WaitForScan()

	committed := metadata.ChunkID(100)
	if err := cs.Write(committed, []byte("committed data")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify the committed chunk exists on disk.
	committedPath := testChunkPath(dir, committed)
	if _, err := os.Stat(committedPath); err != nil {
		t.Fatalf("committed chunk file not found: %v", err)
	}

	// Phase 2: simulate crash — create an orphan .dat file and a WAL
	// write-without-commit entry. We write the WAL entry directly to
	// the file (bypassing the buffered LogWrite) to guarantee it's on
	// disk before close.
	orphanID := metadata.ChunkID(200)
	orphanPath := testChunkPath(dir, orphanID)
	os.MkdirAll(filepath.Dir(orphanPath), 0755)
	if err := os.WriteFile(orphanPath, []byte("orphan data"), 0644); err != nil {
		t.Fatalf("create orphan file: %v", err)
	}

	// Write WAL entry directly to ensure it's flushed.
	wal.SetDataDir(dir)
	walFile := filepath.Join(dir, "wal", "wal.log")
	f, _ := os.OpenFile(walFile, os.O_APPEND|os.O_WRONLY, 0644)
	walEntry := make([]byte, 21)
	copy(walEntry[0:4], walMagic)
	binary.BigEndian.PutUint32(walEntry[4:8], uint32(len("orphan data")))
	binary.BigEndian.PutUint64(walEntry[8:16], uint64(orphanID))
	walEntry[16] = walOpWrite // write intent, no commit
	crc := crc32.ChecksumIEEE(walEntry[:17])
	binary.BigEndian.PutUint32(walEntry[17:21], crc)
	f.Write(walEntry)
	f.Sync()
	f.Close()

	cs.Close()
	wal.Close()

	// Phase 3: reopen — WAL.Recover should clean the orphan, scanExisting
	// should not include it in the index.
	wal2, _ := NewWriteAheadLog(dir + "/wal")
	cs2, err := NewChunkStore(dir, 8, 8, wal2)
	if err != nil {
		t.Fatal(err)
	}
	cs2.WaitForScan()

	// Orphan file should be gone.
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("orphan file still exists after WAL recovery")
	}

	// Committed chunk should still be in the index.
	info, ok := cs2.Info(committed)
	if !ok {
		t.Fatalf("committed chunk %d not in index after recovery", committed)
	}
	if info.Size != int64(len("committed data")) {
		t.Fatalf("committed chunk size = %d, want %d", info.Size, len("committed data"))
	}

	// Orphan chunk should NOT be in the index.
	if _, ok := cs2.Info(orphanID); ok {
		t.Fatalf("orphan chunk %d is in the index after recovery (should be cleaned)", orphanID)
	}

	cs2.Close()
	wal2.Close()
}

// TestWAL_RecoveryMultiDisk verifies WAL recovery across multiple disks.
func TestWAL_RecoveryMultiDisk(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir()}
	wals := make([]*WriteAheadLog, 2)
	for i, d := range dirs {
		w, _ := NewWriteAheadLog(d + "/wal")
		wals[i] = w
	}
	cs, err := NewMultiDiskChunkStore(dirs, 8, 8, wals)
	if err != nil {
		t.Fatal(err)
	}
	cs.WaitForScan()

	// Write on each disk.
	cs.Write(300, []byte("disk0 data"))
	cs.Write(301, []byte("disk1 data"))

	// Simulate orphan on disk 1: write .dat file + WAL entry directly.
	orphanID := metadata.ChunkID(310)
	d1Path := testChunkPath(dirs[1], orphanID)
	os.MkdirAll(filepath.Dir(d1Path), 0755)
	os.WriteFile(d1Path, []byte("orphan"), 0644)

	wals[1].SetDataDir(dirs[1])
	walFile := filepath.Join(dirs[1], "wal", "wal.log")
	f, _ := os.OpenFile(walFile, os.O_APPEND|os.O_WRONLY, 0644)
	entry := make([]byte, 21)
	copy(entry[0:4], walMagic)
	binary.BigEndian.PutUint32(entry[4:8], 6)
	binary.BigEndian.PutUint64(entry[8:16], uint64(orphanID))
	entry[16] = walOpWrite
	crc := crc32.ChecksumIEEE(entry[:17])
	binary.BigEndian.PutUint32(entry[17:21], crc)
	f.Write(entry)
	f.Sync()
	f.Close()

	cs.Close()
	for _, w := range wals {
		w.Close()
	}

	// Reopen.
	wals2 := make([]*WriteAheadLog, 2)
	for i, d := range dirs {
		wals2[i], _ = NewWriteAheadLog(d + "/wal")
	}
	cs2, _ := NewMultiDiskChunkStore(dirs, 8, 8, wals2)
	cs2.WaitForScan()

	// Orphan cleaned.
	if _, err := os.Stat(d1Path); !os.IsNotExist(err) {
		t.Fatalf("orphan still exists on disk 1")
	}

	// Committed chunks survive.
	for _, id := range []metadata.ChunkID{300, 301} {
		if _, ok := cs2.Info(id); !ok {
			t.Fatalf("committed chunk %d missing after recovery", id)
		}
	}

	cs2.Close()
	for _, w := range wals2 {
		w.Close()
	}
}

// testChunkPath returns the file path for a chunk, matching the
// actual chunkPath implementation.
func testChunkPath(dir string, id metadata.ChunkID) string {
	shard := uint64(id) % MaxShards
	return filepath.Join(dir, "chunks",
		fmt.Sprintf("%02x", shard),
		fmt.Sprintf("%d.dat", id))
}
