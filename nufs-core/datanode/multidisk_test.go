package datanode

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

// TestMultiDisk_AdoptHotAdd verifies that adding a new disk at runtime
// scans pre-existing chunk files, merges them into the global index, and
// distributes subsequent writes across all disks including the adopted one.
func TestMultiDisk_AdoptHotAdd(t *testing.T) {
	// Phase 1: populate a "spare" directory with chunks, as if it was
	// a disk removed from another node and now being adopted.
	spareDir := t.TempDir()
	wal0, _ := NewWriteAheadLog(spareDir + "/wal")
	bootstrapper, _ := NewChunkStore(spareDir, 8, 8, wal0)
	if err := bootstrapper.WaitForScan(); err != nil {
		t.Fatal(err)
	}
	preExisting := map[metadata.ChunkID][]byte{
		900: []byte("legacy-chunk-A"),
		901: []byte("legacy-chunk-B"),
		902: []byte("legacy-chunk-C"),
	}
	for id, data := range preExisting {
		if err := bootstrapper.Write(id, data); err != nil {
			t.Fatalf("bootstrap write %d: %v", id, err)
		}
	}
	bootstrapper.Close()

	// Phase 2: start a 1-disk ChunkStore (disk 0) with a separate dir.
	disk0Dir := t.TempDir()
	wal1, _ := NewWriteAheadLog(disk0Dir + "/wal")
	cs, err := NewMultiDiskChunkStore([]string{disk0Dir}, 8, 8, []*WriteAheadLog{wal1})
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.WaitForScan(); err != nil {
		t.Fatal(err)
	}
	dm, _ := NewMultiDiskManager([]string{disk0Dir}, cs, []int64{1}, []*WriteAheadLog{wal1})
	cs.SetDiskManager(dm)

	// Write some chunks on disk 0.
	for i := 0; i < 5; i++ {
		id := metadata.ChunkID(100 + i)
		cs.Write(id, []byte("disk0-chunk"))
	}

	// Phase 3: hot-add the spare directory (contains pre-existing chunks).
	idx, err := cs.AddDisk(spareDir, 8, 8, wal0)
	if err != nil {
		t.Fatalf("AddDisk: %v", err)
	}
	if idx != 1 {
		t.Fatalf("expected new disk index 1, got %d", idx)
	}

	// Phase 4: verify the adopted chunks are in the global index.
	for id, want := range preExisting {
		info, ok := cs.Info(id)
		if !ok {
			t.Fatalf("adopted chunk %d not in index", id)
		}
		if info.DiskIndex != 1 {
			t.Fatalf("adopted chunk %d on disk %d, want 1", id, info.DiskIndex)
		}
		got, _, err := cs.Read(id, 0, 0)
		if err != nil {
			t.Fatalf("Read adopted chunk %d: %v", id, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("adopted chunk %d: got %q, want %q", id, got, want)
		}
	}

	// Phase 5: new writes should go to the least-used disk.
	// Disk 0 has ~60 bytes (5 chunks), disk 1 (adopted) has ~39 bytes
	// (3 legacy chunks), so new writes go to disk 1.
	for i := 0; i < 5; i++ {
		id := metadata.ChunkID(200 + i)
		cs.Write(id, []byte("post-adopt"))
		info, _ := cs.Info(id)
		if info.DiskIndex != 1 {
			t.Fatalf("post-adopt write %d on disk %d, want 1 (least-used)", id, info.DiskIndex)
		}
		got, _, err := cs.Read(id, 0, 0)
		if err != nil {
			t.Fatalf("Read post-adopt %d: %v", id, err)
		}
		if string(got) != "post-adopt" {
			t.Fatalf("post-adopt %d: got %q", id, got)
		}
	}

	// Verify aggregate stats include adopted + post-adopt chunks.
	totalBytes, chunkCount := cs.Stats()
	if chunkCount != 13 { // 5 disk0 + 3 adopted + 5 post-adopt
		t.Fatalf("chunkCount = %d, want 13", chunkCount)
	}
	if totalBytes == 0 {
		t.Fatal("totalBytes should be non-zero")
	}

	cs.Close()
	dm.Stop()
}

// ============================================================
// Previous multi-disk tests (restored after accidental overwrite)
// ============================================================

func newMultiDiskTestStore(t *testing.T, n int, capacityGB int64) (*ChunkStore, *DiskManager, []string) {
	t.Helper()
	dirs := make([]string, n)
	wals := make([]*WriteAheadLog, n)
	for i := 0; i < n; i++ {
		dirs[i] = t.TempDir()
		w, err := NewWriteAheadLog(dirs[i] + "/wal")
		if err != nil {
			t.Fatalf("NewWriteAheadLog: %v", err)
		}
		wals[i] = w
	}
	cs, err := NewMultiDiskChunkStore(dirs, 8, 8, wals)
	if err != nil {
		t.Fatalf("NewMultiDiskChunkStore: %v", err)
	}
	if err := cs.WaitForScan(); err != nil {
		t.Fatalf("WaitForScan: %v", err)
	}
	caps := make([]int64, n)
	for i := range caps {
		caps[i] = capacityGB
	}
	dm, err := NewMultiDiskManager(dirs, cs, caps, wals)
	if err != nil {
		t.Fatalf("NewMultiDiskManager: %v", err)
	}
	cs.SetDiskManager(dm)
	return cs, dm, dirs
}

func TestMultiDisk_WriteSpreading(t *testing.T) {
	cs, dm, _ := newMultiDiskTestStore(t, 2, 1)
	defer cs.Close()
	defer dm.Stop()

	seen := make(map[int]bool)
	for i := 0; i < 10; i++ {
		chunkID := metadata.ChunkID(100 + i)
		if err := cs.Write(chunkID, []byte("data")); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		info, ok := cs.Info(chunkID)
		if !ok {
			t.Fatalf("chunk %d not in index", chunkID)
		}
		seen[info.DiskIndex] = true
	}
	if len(seen) < 2 {
		t.Fatalf("expected writes to spread across >=2 disks, got %v", seen)
	}
}

func TestMultiDisk_FailureIsolation(t *testing.T) {
	cs, dm, _ := newMultiDiskTestStore(t, 2, 1)
	defer cs.Close()
	defer dm.Stop()

	cs.Write(1, []byte("on-disk-0-or-1"))
	cs.Write(2, []byte("another"))

	dm.MarkDiskFailed(1)

	for i := 0; i < 5; i++ {
		chunkID := metadata.ChunkID(200 + i)
		if err := cs.Write(chunkID, []byte("after-fail")); err != nil {
			t.Fatalf("Write %d after failure: %v", i, err)
		}
		info, _ := cs.Info(chunkID)
		if info.DiskIndex != 0 {
			t.Fatalf("chunk %d landed on failed disk %d, want 0", chunkID, info.DiskIndex)
		}
	}

	if _, _, err := cs.Read(200, 0, 0); err != nil {
		t.Fatalf("Read on healthy disk 0: %v", err)
	}
}

func TestMultiDisk_LeastUsedSelection(t *testing.T) {
	cs, dm, _ := newMultiDiskTestStore(t, 2, 1)
	defer cs.Close()
	defer dm.Stop()

	cs.disks[0].usedBytes.Store(80 * 1024 * 1024 * 1024)

	if err := cs.Write(42, []byte("least-used")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	info, _ := cs.Info(42)
	if info.DiskIndex != 1 {
		t.Fatalf("expected write on disk 1 (least used), got disk %d", info.DiskIndex)
	}
}

func TestMultiDisk_ScanRecovery(t *testing.T) {
	cs1, dm1, dirs := newMultiDiskTestStore(t, 2, 1)

	for i := 0; i < 10; i++ {
		if err := cs1.Write(metadata.ChunkID(500+i), bytes.Repeat([]byte("x"), 100)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	locations := make(map[metadata.ChunkID]int)
	for i := 0; i < 10; i++ {
		info, _ := cs1.Info(metadata.ChunkID(500 + i))
		locations[metadata.ChunkID(500+i)] = info.DiskIndex
	}
	cs1.Close()
	dm1.Stop()

	wals := make([]*WriteAheadLog, len(dirs))
	for i, d := range dirs {
		wals[i], _ = NewWriteAheadLog(d + "/wal")
	}
	cs2, err := NewMultiDiskChunkStore(dirs, 8, 8, wals)
	if err != nil {
		t.Fatalf("recreate: %v", err)
	}
	defer cs2.Close()
	if err := cs2.WaitForScan(); err != nil {
		t.Fatalf("WaitForScan: %v", err)
	}

	for id, wantDisk := range locations {
		info, ok := cs2.Info(id)
		if !ok {
			t.Fatalf("chunk %d not recovered", id)
		}
		if info.DiskIndex != wantDisk {
			t.Fatalf("chunk %d DiskIndex = %d, want %d", id, info.DiskIndex, wantDisk)
		}
		if _, _, err := cs2.Read(id, 0, 0); err != nil {
			t.Fatalf("Read recovered chunk %d: %v", id, err)
		}
	}
}

func TestMultiDisk_AggregateStats(t *testing.T) {
	cs, dm, _ := newMultiDiskTestStore(t, 3, 1)
	defer cs.Close()
	defer dm.Stop()

	payload := bytes.Repeat([]byte("z"), 1024)
	const n = 15
	for i := 0; i < n; i++ {
		if err := cs.Write(metadata.ChunkID(700+i), payload); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	totalBytes, chunkCount := cs.Stats()
	if chunkCount != int64(n) {
		t.Fatalf("chunkCount = %d, want %d", chunkCount, n)
	}
	wantBytes := int64(n) * int64(len(payload))
	if totalBytes != wantBytes {
		t.Fatalf("totalBytes = %d, want %d", totalBytes, wantBytes)
	}

	listed := cs.ListChunks()
	if len(listed) != n {
		t.Fatalf("ListChunks returned %d, want %d", len(listed), n)
	}

	seen := make(map[int]bool)
	for _, info := range listed {
		seen[info.DiskIndex] = true
	}
	if len(seen) < 2 {
		t.Fatalf("expected spread across >=2 disks, got %v", seen)
	}
}

func TestMultiDisk_DrainWritesAcrossDisks(t *testing.T) {
	cs, dm, _ := newMultiDiskTestStore(t, 2, 1)
	defer cs.Close()
	defer dm.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	release, err := cs.DrainWrites(ctx)
	if err != nil {
		t.Fatalf("DrainWrites: %v", err)
	}
	defer release()

	for i, d := range cs.disks {
		select {
		case d.writeSem <- struct{}{}:
			t.Fatalf("disk %d writeSem was not drained", i)
		default:
		}
	}
}


func TestMultiDisk_MigrateDisk(t *testing.T) {
	cs, dm, _ := newMultiDiskTestStore(t, 2, 1)
	defer cs.Close()
	defer dm.Stop()

	// Write enough to spread across both disks.
	for i := 0; i < 20; i++ {
		cs.Write(metadata.ChunkID(800+i), []byte("migrate-me"))
	}

	// Count chunks on each disk before migration.
	disk0Before, disk1Before := int64(0), int64(0)
	for _, info := range cs.ListChunks() {
		if info.DiskIndex == 0 { disk0Before++ } else { disk1Before++ }
	}
	if disk1Before == 0 {
		t.Fatal("no chunks on disk 1 to migrate")
	}

	// Migrate all chunks off disk 1 to disk 0.
	migrated, err := cs.MigrateDisk(1)
	if err != nil {
		t.Fatalf("MigrateDisk: %v", err)
	}
	if migrated != int(disk1Before) {
		t.Fatalf("migrated %d, want %d", migrated, disk1Before)
	}

	// All chunks should now be on disk 0.
	for _, info := range cs.ListChunks() {
		if info.DiskIndex != 0 {
			t.Fatalf("chunk %d still on disk %d after migration", info.ChunkID, info.DiskIndex)
		}
	}

	// All migrated chunks must still be readable.
	for i := 0; i < 20; i++ {
		id := metadata.ChunkID(800 + i)
		got, _, err := cs.Read(id, 0, 0)
		if err != nil {
			t.Fatalf("Read after migration %d: %v", id, err)
		}
		if string(got) != "migrate-me" {
			t.Fatalf("data mismatch after migration %d: got %q", id, got)
		}
	}
}
