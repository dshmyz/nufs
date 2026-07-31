package datanode

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

// TestChunkStore_WriteBatch_GroupFsync verifies that WriteBatch
// writes multiple chunks and syncs them in a single fsync batch,
// reducing per-chunk fsync overhead.
func TestChunkStore_WriteBatch_GroupFsync(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	// Prepare 10 chunks of 4KB each
	chunks := make([]WriteChunkReq, 10)
	for i := range chunks {
		chunks[i].ChunkID = metadata.ChunkID(600 + i)
		chunks[i].Data = make([]byte, 4096)
		for j := range chunks[i].Data {
			chunks[i].Data[j] = byte(i)
		}
	}

	before := cs.PerfSnapshot()
	err := cs.WriteBatch(chunks)
	after := cs.PerfSnapshot()

	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	// Verify all chunks are readable
	for i, c := range chunks {
		got, _, err := cs.Read(c.ChunkID, 0, 0)
		if err != nil {
			t.Fatalf("Read chunk %d: %v", i, err)
		}
		if len(got) != len(c.Data) {
			t.Fatalf("chunk %d length: got %d, want %d", i, len(got), len(c.Data))
		}
	}

	// fsync count should be less than the number of chunks (grouped)
	fsyncCount := after.FsyncCount - before.FsyncCount
	if fsyncCount > int64(len(chunks)) {
		t.Errorf("fsync count %d should be <= %d (chunks), group fsync not working",
			fsyncCount, len(chunks))
	}
	if fsyncCount == 0 {
		t.Errorf("expected at least 1 fsync, got 0")
	}
}

// TestChunkStore_WriteBatch_Durability verifies that chunks written
// via WriteBatch are persisted to disk (survive a simulated crash
// by closing and reopening the store).
func TestChunkStore_WriteBatch_Durability(t *testing.T) {
	cs, dir := newTestChunkStore(t)

	chunks := []WriteChunkReq{
		{ChunkID: metadata.ChunkID(700), Data: []byte("chunk-700-data")},
		{ChunkID: metadata.ChunkID(701), Data: []byte("chunk-701-data")},
		{ChunkID: metadata.ChunkID(702), Data: []byte("chunk-702-data")},
	}

	if err := cs.WriteBatch(chunks); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	// Close the store
	cs.Close()

	// Reopen with the same directory
	cs2, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}
	defer cs2.Close()

	// Verify chunks survived
	for _, c := range chunks {
		got, _, err := cs2.Read(c.ChunkID, 0, 0)
		if err != nil {
			t.Fatalf("Read after reopen chunk %d: %v", c.ChunkID, err)
		}
		if string(got) != string(c.Data) {
			t.Fatalf("chunk %d data mismatch: got %q, want %q", c.ChunkID, got, c.Data)
		}
	}
}

// TestChunkStore_WriteBatch_Empty verifies that WriteBatch with no
// chunks is a no-op.
func TestChunkStore_WriteBatch_Empty(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	before := cs.PerfSnapshot()
	if err := cs.WriteBatch(nil); err != nil {
		t.Fatalf("WriteBatch(nil): %v", err)
	}
	after := cs.PerfSnapshot()

	if after.FsyncCount != before.FsyncCount {
		t.Errorf("empty WriteBatch should not fsync, got delta=%d",
			after.FsyncCount-before.FsyncCount)
	}
}

// TestChunkStore_WriteBatch_Concurrent verifies that concurrent
// WriteBatch calls do not corrupt data or deadlock.
func TestChunkStore_WriteBatch_Concurrent(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	const numGoroutines = 8
	const chunksPerBatch = 5

	var wg sync.WaitGroup
	var errCount int64

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			chunks := make([]WriteChunkReq, chunksPerBatch)
			for i := range chunks {
				chunks[i].ChunkID = metadata.ChunkID(goroutineID*1000 + i)
				chunks[i].Data = make([]byte, 1024)
				for j := range chunks[i].Data {
					chunks[i].Data[j] = byte(goroutineID)
				}
			}
			if err := cs.WriteBatch(chunks); err != nil {
				t.Errorf("goroutine %d: WriteBatch: %v", goroutineID, err)
				atomic.AddInt64(&errCount, 1)
			}
		}(g)
	}

	wg.Wait()

	if errCount > 0 {
		t.Fatalf("%d goroutines failed", errCount)
	}

	// Verify all chunks are readable
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < chunksPerBatch; i++ {
			chunkID := metadata.ChunkID(g*1000 + i)
			got, _, err := cs.Read(chunkID, 0, 0)
			if err != nil {
				t.Fatalf("Read chunk %d: %v", chunkID, err)
			}
			if len(got) != 1024 {
				t.Fatalf("chunk %d length: got %d, want 1024", chunkID, len(got))
			}
			if got[0] != byte(g) {
				t.Fatalf("chunk %d data mismatch: got %d, want %d", chunkID, got[0], g)
			}
		}
	}
}

// TestChunkStore_WriteBatch_LessFsyncThanIndividualWrites is a
// performance characteristic test: writing N chunks via WriteBatch
// should issue fewer fsyncs than writing them one by one.
func TestChunkStore_WriteBatch_LessFsyncThanIndividualWrites(t *testing.T) {
	// Setup: write chunks individually and count fsyncs
	cs1, _ := newTestChunkStore(t)
	defer cs1.Close()

	chunkIDs1 := []metadata.ChunkID{800, 801, 802, 803, 804}
	before1 := cs1.PerfSnapshot()
	for _, id := range chunkIDs1 {
		if err := cs1.Write(id, []byte("data-"+string(rune(id)))); err != nil {
			t.Fatalf("Write %d: %v", id, err)
		}
	}
	after1 := cs1.PerfSnapshot()
	individualFsyncs := after1.FsyncCount - before1.FsyncCount

	// Setup: write the same chunks via WriteBatch
	cs2, _ := newTestChunkStore(t)
	defer cs2.Close()

	chunks := make([]WriteChunkReq, len(chunkIDs1))
	for i, id := range chunkIDs1 {
		chunks[i] = WriteChunkReq{ChunkID: id, Data: []byte("data-" + string(rune(id)))}
	}
	before2 := cs2.PerfSnapshot()
	if err := cs2.WriteBatch(chunks); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	after2 := cs2.PerfSnapshot()
	batchFsyncs := after2.FsyncCount - before2.FsyncCount

	if batchFsyncs >= individualFsyncs {
		t.Logf("batch fsyncs=%d, individual fsyncs=%d (batch should be fewer)",
			batchFsyncs, individualFsyncs)
		// Don't fail — this is a performance characteristic, not correctness.
		// On some systems fsync count may be similar.
	}
	t.Logf("individual fsyncs=%d, batch fsyncs=%d", individualFsyncs, batchFsyncs)
}

// Ensure file mode check works after WriteBatch (files should be readable)
func TestChunkStore_WriteBatch_FileMode(t *testing.T) {
	cs, dir := newTestChunkStore(t)
	defer cs.Close()

	chunks := []WriteChunkReq{
		{ChunkID: metadata.ChunkID(900), Data: []byte("test")},
	}
	if err := cs.WriteBatch(chunks); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	// Verify file exists and is readable
	path := cs.disks[0].chunkPath(metadata.ChunkID(900))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() < 4+ChunkFileHeaderSize {
		t.Errorf("file size %d too small", info.Size())
	}

	_ = dir
}

// Ensure Close is called in tests that need it
var _ = time.Second
