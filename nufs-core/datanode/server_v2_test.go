package datanode

import (
	"testing"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/segment"
	"github.com/example/dfs/metadata"
)

// newTestMultiStore builds a V2Store over n in-memory segment stores
// (one per disk), returning the store and a cleanup that closes each
// backend. Data topics use StreamID 1.
func newTestMultiStore(t *testing.T, n int) (*V2Store, []*segment.Store) {
	t.Helper()
	dirs := make([]string, n)
	for i := 0; i < n; i++ {
		dirs[i] = t.TempDir()
	}
	backends := make([]*segment.Store, n)
	for i := 0; i < n; i++ {
		s, err := segment.New(segment.Config{
			Dir:         dirs[i],
			UseMemIndex: true,
			StreamID:    1,
		})
		if err != nil {
			t.Fatalf("segment.New disk %d: %v", i, err)
		}
		backends[i] = s
	}
	stores := make([]storage.Store, n)
	for i := 0; i < n; i++ {
		stores[i] = backends[i]
	}
	v := NewMultiV2Store(stores)
	t.Cleanup(func() {
		for _, s := range backends {
			s.Close()
		}
	})
	return v, backends
}

func TestV2StoreMultiDiskPlacement(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)

	// First write lands on disk 0 (least-used tie broken by index).
	if err := v.Write(metadata.ChunkID(101), []byte("first-on-disk-zero")); err != nil {
		t.Fatalf("write 101: %v", err)
	}
	// After disk 0 has bytes, the next new chunk goes to the empty disk 1.
	if err := v.Write(metadata.ChunkID(202), []byte("second-on-disk-one")); err != nil {
		t.Fatalf("write 202: %v", err)
	}

	v.mu.RLock()
	d101, ok101 := v.locOf[metadata.ChunkID(101)]
	d202, ok202 := v.locOf[metadata.ChunkID(202)]
	v.mu.RUnlock()
	if !ok101 || d101.disk != 0 {
		t.Fatalf("chunk 101 on disk %d (ok=%v), want 0", d101.disk, ok101)
	}
	if !ok202 || d202.disk != 1 {
		t.Fatalf("chunk 202 on disk %d (ok=%v), want 1", d202.disk, ok202)
	}

	// A rewrite of an existing chunk stays on its owning disk (here 0)
	// and advances to a fresh generation (it no longer overwrites at gen 1,
	// which the segment store rejects as stale).
	beforeGen := d101.gen
	if err := v.Write(metadata.ChunkID(101), []byte("rewrite-101-longer-payload")); err != nil {
		t.Fatalf("rewrite 101: %v", err)
	}
	v.mu.RLock()
	d101 = v.locOf[metadata.ChunkID(101)]
	v.mu.RUnlock()
	if d101.disk != 0 {
		t.Fatalf("rewritten chunk 101 on disk %d, want 0 (owning disk)", d101.disk)
	}
	if d101.gen != beforeGen+1 {
		t.Fatalf("rewritten chunk 101 gen=%d, want %d", d101.gen, beforeGen+1)
	}

	// The data actually landed on the expected backends (read through).
	if _, _, err := v.Read(metadata.ChunkID(202), 0, 0); err != nil {
		t.Fatalf("read 202 from disk 1: %v", err)
	}

	// Per-disk accounting reflects the spread.
	ds := v.DiskStats()
	if len(ds) != 2 {
		t.Fatalf("DiskStats len=%d, want 2", len(ds))
	}
	if ds[0].ChunkCount == 0 || ds[1].ChunkCount == 0 {
		t.Fatalf("expected chunks on both disks, got disk0=%d disk1=%d", ds[0].ChunkCount, ds[1].ChunkCount)
	}
}

func TestV2StoreStatsListSnapshot(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)

	// Two distinct chunks.
	if err := v.Write(metadata.ChunkID(1), []byte("aaaaaaaa")); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	if err := v.Write(metadata.ChunkID(2), []byte("bbbb")); err != nil {
		t.Fatalf("write 2: %v", err)
	}

	totalBytes, chunkCount := v.Stats()
	if chunkCount != 2 {
		t.Fatalf("Stats chunkCount=%d, want 2", chunkCount)
	}
	// Sum of logical payloads (12 bytes).
	if totalBytes != 12 {
		t.Fatalf("Stats totalBytes=%d, want 12", totalBytes)
	}

	chunks := v.ListChunks()
	if len(chunks) != 2 {
		t.Fatalf("ListChunks len=%d, want 2", len(chunks))
	}
	sizes := map[metadata.ChunkID]int64{}
	for _, c := range chunks {
		sizes[c.ChunkID] = c.Size
	}
	if sizes[metadata.ChunkID(1)] != 8 || sizes[metadata.ChunkID(2)] != 4 {
		t.Fatalf("unexpected sizes: %v", sizes)
	}

	snap := v.ChunkStateSnapshot()
	if len(snap) != 2 {
		t.Fatalf("ChunkStateSnapshot len=%d, want 2", len(snap))
	}
	for id, st := range snap {
		if st != metadata.ReplicaReady {
			t.Fatalf("chunk %d state=%v, want ReplicaReady", id, st)
		}
	}

	// Delete drops it from stats/snapshot and bumps the version.
	v0 := v.StateVersion()
	if err := v.Delete(metadata.ChunkID(1)); err != nil {
		t.Fatalf("delete 1: %v", err)
	}
	if v.StateVersion() <= v0 {
		t.Fatalf("StateVersion did not advance after delete")
	}
	_, chunkCount = v.Stats()
	if chunkCount != 1 {
		t.Fatalf("Stats chunkCount after delete=%d, want 1", chunkCount)
	}
	if _, ok := v.ChunkStateSnapshot()[metadata.ChunkID(1)]; ok {
		t.Fatalf("deleted chunk 1 still in snapshot")
	}
}

func TestV2StoreWriteErrorRateAndInterface(t *testing.T) {
	v, _ := newTestMultiStore(t, 1)
	if rate := v.WriteErrorRate(); rate != 0 {
		t.Fatalf("WriteErrorRate on succcess=%v, want 0", rate)
	}
	// Compile-time: V2Store satisfies both serving interfaces.
	var _ LocalChunkStore = v
	var _ HeartbeatStore = v
}

func TestV2StoreOverwriteAccounting(t *testing.T) {
	v, _ := newTestMultiStore(t, 1)

	// Write 4 bytes, then overwrite with 12 bytes. used-bytes must track
	// the LIVE size (12), not the sum of both generations, and the count
	// stays one chunk.
	if err := v.Write(metadata.ChunkID(10), []byte("abcd")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := v.Write(metadata.ChunkID(10), []byte("efghijklmnop")); err != nil {
		t.Fatalf("overwrite: %v", err)
	}

	total, count := v.Stats()
	if count != 1 {
		t.Fatalf("count=%d, want 1", count)
	}
	if total != 12 {
		t.Fatalf("totalBytes=%d, want 12 (live size, not 16)", total)
	}

	// The read must return the latest payload (8+12 = the overwrite).
	data, _, err := v.Read(metadata.ChunkID(10), 0, 0)
	if err != nil {
		t.Fatalf("read after overwrite: %v", err)
	}
	if string(data) != "efghijklmnop" {
		t.Fatalf("read returned %q, want %q", data, "efghijklmnop")
	}

	// Delete frees the live size; the chunk disappears from enumeration.
	if err := v.Delete(metadata.ChunkID(10)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, count := v.Stats(); count != 0 {
		t.Fatalf("count after delete=%d, want 0", count)
	}
	if chunks := v.ListChunks(); len(chunks) != 0 {
		t.Fatalf("ListChunks after delete=%d, want 0", len(chunks))
	}
}
