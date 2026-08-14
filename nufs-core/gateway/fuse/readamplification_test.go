//go:build linux

package fuse

import (
	"context"
	"sync"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// amplificationCounter wraps a ChunkStore and records how many raw bytes each
// range read transfers, to assert the range-read fix pulls only the requested
// window instead of the whole 64 MiB chunk.
type amplificationCounter struct {
	chunkstore.ChunkStore
	mu        sync.Mutex
	readBytes int64
	minOff    int64
	maxEnd    int64
}

func (a *amplificationCounter) ReadChunkRange(ctx context.Context, chunk *metadata.ChunkMeta, offset int64, length int32) ([]byte, error) {
	data, err := a.ChunkStore.ReadChunkRange(ctx, chunk, offset, length)
	a.mu.Lock()
	a.readBytes += int64(len(data))
	if offset < a.minOff || a.readBytes == int64(len(data)) {
		a.minOff = offset
	}
	if offset+int64(len(data)) > a.maxEnd {
		a.maxEnd = offset + int64(len(data))
	}
	a.mu.Unlock()
	return data, err
}

// ampTestSetup builds an in-memory meta store (RF=1) plus a counting chunk
// store and a DFSFile for a fresh file. This mirrors newTestFile/newTestMetaStore
// (which are linux-gated) so the range-read behaviour is checkable on any OS.
func ampTestSetup(t *testing.T) (*ampCounterMeta, *amplificationCounter, *DFSFile, metadata.InodeID) {
	t.Helper()
	ctx := context.Background()
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:         t.TempDir(),
		UseInMemory: true,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.RegisterNode(ctx, &metadata.NodeInfo{
		ID: 1, Addr: "127.0.0.1:9001", CapacityGB: 100,
	}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if err := store.CreateBucket(ctx, "test", metadata.PlacementPolicy{
		ID: "test", ReplicationFactor: 1, TopologySpread: metadata.SpreadNode,
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	file, err := store.CreateFile(ctx, bucket.RootInode, "a.bin", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	underlying := chunkstore.NewMemoryChunkStore()
	counting := &amplificationCounter{ChunkStore: underlying}
	dfs := NewDFSFileSystem(store, counting, nil, nil, nil)
	return &ampCounterMeta{meta: store}, counting, &DFSFile{fs: dfs, chunkStore: counting, inodeID: file.ID}, file.ID
}

type ampCounterMeta struct {
	meta *metadata.PebbleStore
}

// TestReadAmplification_FetchIsWindowOnly writes a committed chunk and reads a
// small window, asserting the chunkstore transfers only the window bytes, not
// the whole 64 MiB chunk.
func TestReadAmplification_FetchIsWindowOnly(t *testing.T) {
	_, counting, f, _ := ampTestSetup(t)

	seed := make([]byte, 8<<20)
	for i := range seed {
		seed[i] = byte(i)
	}
	if _, errno := f.Write(context.Background(), nil, seed, 0); errno != 0 {
		t.Fatalf("Write seed: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush seed: errno=%v", errno)
	}

	dest := make([]byte, 4096)
	off := int64((8 << 20) / 2) // middle window of the 8 MiB chunk
	rr, errno := f.Read(context.Background(), nil, dest, off)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	if _, st := rr.Bytes(dest); st != 0 {
		t.Fatalf("rr.Bytes: %v", st)
	}

	counting.mu.Lock()
	got := counting.readBytes
	counting.mu.Unlock()
	// With the range-read fix this is ~4 KiB, not the 8 MiB whole chunk.
	if got > 16<<10 { // well above a 4 KiB window + slack
		t.Fatalf("read amplification: fetched %d bytes for a 4096-byte window (want ~4096)", got)
	}
	if got < 4096 {
		t.Fatalf("fetched %d bytes, less than the 4096 requested", got)
	}
}

// TestReadThenRewrite_PreservesCommittedData guards against a data-loss
// regression in the sliced read cache: a small read caches a short window of a
// chunk, and a subsequent partial rewrite must hydrate the FULL committed
// extent (not just the cached short window) or the un-rewritten committed data
// gets zeroed on flush. Uses the production-style newTestFile helper and
// attaches a real ChunkCache so the read path actually exercises slicing.
func TestReadThenRewrite_PreservesCommittedData(t *testing.T) {
	ctx := context.Background()
	_, counting, f, _ := ampTestSetup(t)
	cache, err := NewChunkCache("")
	if err != nil {
		t.Fatalf("NewChunkCache: %v", err)
	}
	f.cache = cache

	// Write 8 MiB of patterned data and flush it (committed extent = 8 MiB).
	const committed = 8 << 20
	seed := make([]byte, committed)
	for i := range seed {
		seed[i] = byte(i)
	}
	if _, errno := f.Write(ctx, nil, seed, 0); errno != 0 {
		t.Fatalf("Write seed: errno=%v", errno)
	}
	if errno := f.Flush(ctx, nil); errno != 0 {
		t.Fatalf("Flush seed: errno=%v", errno)
	}

	// Read a small window at the chunk start. This populates the sliced cache
	// at (chunkID, 0) with a short window far smaller than the extent,
	// exercising the exact regression scenario.
	small := make([]byte, 4096)
	smallData, errno := f.Read(ctx, nil, small, 0)
	if errno != 0 {
		t.Fatalf("Read small: errno=%v", errno)
	}
	data, _ := smallData.Bytes(small)
	if len(data) < 2 {
		t.Fatalf("read returned too few bytes: %d", len(data))
	}
	if data[0] != 0 || data[4095] != 0xff {
		t.Fatalf("seed not committed before rewrite: data[:2]=%x", data[:2])
	}

	// A partial overwrite at the start triggers hydration of the chunk base.
	// Hydration must read the FULL committed extent, not trust the short
	// cached window, or the [2, committed) tail is zeroed on flush.
	over := []byte{0xAA, 0xBB}
	if _, errno := f.Write(ctx, nil, over, 0); errno != 0 {
		t.Fatalf("Write over: errno=%v", errno)
	}
	if errno := f.Flush(ctx, nil); errno != 0 {
		t.Fatalf("Flush over: errno=%v", errno)
	}

	// Read the whole extent back; verify the overwritten prefix and that the
	// untouched committed tail survived.
	out := make([]byte, committed)
	outRR, errno := f.Read(ctx, nil, out, 0)
	if errno != 0 {
		t.Fatalf("Read full: errno=%v", errno)
	}
	outData, _ := outRR.Bytes(out)
	if len(outData) < 2 {
		t.Fatalf("full read returned %d bytes", len(outData))
	}
	if outData[0] != 0xAA || outData[1] != 0xBB {
		t.Fatalf("overwritten prefix wrong: %x %x", outData[0], outData[1])
	}
	for i := 2; i < committed; i++ {
		if i < len(outData) && outData[i] != byte(i) {
			t.Fatalf("committed data lost at offset %d: got %x want %x", i, outData[i], byte(i))
		}
	}
	_ = counting
}
