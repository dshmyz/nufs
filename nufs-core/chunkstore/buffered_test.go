package chunkstore

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// stagingFileCount returns the number of files in the staging dir.
func stagingFileCount(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return -1
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() {
			n++
		}
	}
	return n
}

// ========== Test doubles ==========

// countingSpillStats counts staging events for spill/reload assertions.
type countingSpillStats struct {
	spill, load, spillErr atomic.Int64
}

func (c *countingSpillStats) IncStagingSpill()    { c.spill.Add(1) }
func (c *countingSpillStats) IncStagingLoad()     { c.load.Add(1) }
func (c *countingSpillStats) IncStagingSpillErr() { c.spillErr.Add(1) }

// recordingLedger records flush attempt transitions.
type recordingLedger struct {
	mu       sync.Mutex
	attempts int
	states   []metadata.WriteAttemptState
}

func (r *recordingLedger) Record(_ context.Context, _ string, _ *metadata.InodeMeta, state metadata.WriteAttemptState, _ string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts++
	r.states = append(r.states, state)
}

func (r *recordingLedger) snapshot() []metadata.WriteAttemptState {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]metadata.WriteAttemptState, len(r.states))
	copy(out, r.states)
	return out
}

// garbageCache returns wrong data on every Get. Used to prove that hydration
// (loadCommittedChunkLocked) reads committed bytes straight from the chunkstore
// and never consults the read cache — a partial cached window masquerading as
// the complete image would silently corrupt a partial overwrite.
type garbageCache struct{}

func (garbageCache) Get(uint64, int64) ([]byte, bool) { return []byte("GARBAGE-GARBAGE-GARBAGE"), true }
func (garbageCache) Add(uint64, int64, []byte)        {}

// ========== Fixtures ==========

// newBufferedTestMeta is newWriterTestMeta without the policy (buffered tests
// don't need to name it).
func newBufferedTestMeta(t testing.TB) (*metadata.PebbleStore, metadata.InodeID) {
	t.Helper()
	pb, inodeID, _ := newWriterTestMeta(t)
	return pb, inodeID
}

// newTestBuffered builds a BufferedFile over the given meta+chunkStore with
// committedSize=0 (fresh file). Executor is zero (passthrough) unless a test
// injects one.
func newTestBuffered(pb *metadata.PebbleStore, cs ChunkStore, inodeID metadata.InodeID, opts ...BufferedFileOption) *BufferedFile {
	return NewBufferedFile(
		func() metadata.MetadataService { return pb },
		cs,
		inodeID,
		0,
		opts...,
	)
}

// withInvariantOn enables the buffer-image invariant for the duration of a
// test, restoring the prior state afterwards.
func withInvariantOn(t *testing.T) {
	t.Helper()
	prev := bufferImageInvariantOn.Load()
	bufferImageInvariantOn.Store(true)
	t.Cleanup(func() { bufferImageInvariantOn.Store(prev) })
}

// ========== Round-trip / flush ==========

func TestBufferedFile_WriteReadRoundtrip(t *testing.T) {
	pb, inodeID := newBufferedTestMeta(t)
	b := newTestBuffered(pb, NewMemoryChunkStore(), inodeID)
	ctx := context.Background()

	want := []byte("hello, world!")
	if _, err := b.Write(ctx, want, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !b.Dirty() {
		t.Error("Dirty()=false after write, want true")
	}

	got, err := b.ReadView(ctx, 0, 100)
	if err != nil {
		t.Fatalf("ReadView: %v", err)
	}
	if string(got[:len(want)]) != string(want) {
		t.Errorf("ReadView prefix=%q, want %q", got[:len(want)], want)
	}
	for _, z := range got[len(want):] {
		if z != 0 {
			t.Errorf("ReadView tail contains nonzero byte %d at %d", z, len(want))
			break
		}
	}
}

func TestBufferedFile_FlushCommitsAndClears(t *testing.T) {
	pb, inodeID := newBufferedTestMeta(t)
	cs := NewMemoryChunkStore()
	b := newTestBuffered(pb, cs, inodeID)
	ctx := context.Background()

	data := []byte("flush-me")
	if _, err := b.Write(ctx, data, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	res, err := b.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if res.NewSize != int64(len(data)) {
		t.Errorf("NewSize=%d, want %d", res.NewSize, len(data))
	}
	if len(res.NewRefs) != 1 {
		t.Fatalf("NewRefs len=%d, want 1", len(res.NewRefs))
	}
	ref := res.NewRefs[0]
	if ref.Offset != 0 || ref.Length != int32(len(data)) {
		t.Errorf("ref=%+v, want Offset=0 Length=%d", ref, len(data))
	}

	// Inode was updated: ChunkMap + size persisted.
	inode, err := pb.GetInode(ctx, inodeID)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != int64(len(data)) {
		t.Errorf("inode.Size=%d, want %d", inode.Size, len(data))
	}
	if len(inode.ChunkMap) != 1 || inode.ChunkMap[0].ID != ref.ID {
		t.Errorf("ChunkMap=%+v, want single ref %d", inode.ChunkMap, ref.ID)
	}

	// Chunk payload landed byte-exact in the chunk store.
	chunk, err := pb.GetChunk(ctx, ref.ID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	stored, err := cs.ReadChunk(ctx, chunk)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if string(stored) != string(data) {
		t.Errorf("stored=%q, want %q", stored, data)
	}

	// Buffer cleared: file is clean, a second Flush writes nothing.
	if b.Dirty() {
		t.Error("Dirty()=true after Flush, want false")
	}
	res2, err := b.Flush(ctx)
	if err != nil {
		t.Fatalf("second Flush: %v", err)
	}
	if len(res2.NewRefs) != 0 {
		t.Errorf("second Flush NewRefs=%d, want 0 (idempotent)", len(res2.NewRefs))
	}
}

// TestBufferedFile_FlushPersistsMergedView verifies that after a write +
// flush + partial overwrite + flush, the committed image is the merged view
// (committed prefix preserved, new bytes on top, holes zero-filled).
func TestBufferedFile_FlushPersistsMergedView(t *testing.T) {
	pb, inodeID := newBufferedTestMeta(t)
	cs := NewMemoryChunkStore()
	b := newTestBuffered(pb, cs, inodeID)
	ctx := context.Background()

	if _, err := b.Write(ctx, []byte("committed"), 0); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if _, err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush 1: %v", err)
	}

	// Partial overwrite at 50: the committed prefix must survive hydration.
	if _, err := b.Write(ctx, []byte("XYZ"), 50); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	// The file's effective size is now 53 bytes (max committed 9, tail 53).
	want := []byte("committed")
	want = append(want, make([]byte, 41)...) // [9,50) holes
	want = append(want, []byte("XYZ")...)    // total 53
	got, err := b.ReadView(ctx, 0, 60)
	if err != nil {
		t.Fatalf("ReadView pre-flush: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("pre-flush view=%q, want %q", got, want)
	}

	if _, err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush 2: %v", err)
	}

	// Fresh reader on the SAME chunk store: committed image is the merged view.
	b2 := newTestBuffered(pb, cs, inodeID)
	got2, err := b2.ReadView(ctx, 0, 60)
	if err != nil {
		t.Fatalf("ReadView post-flush: %v", err)
	}
	if string(got2) != string(want) {
		t.Errorf("post-flush view=%q, want %q", got2, want)
	}
}

// TestBufferedFile_HydrationBypassesReadCache proves loadCommittedChunkLocked
// reads committed bytes from the chunkstore directly, never the read cache —
// a partial cached window would otherwise corrupt the merged image.
func TestBufferedFile_HydrationBypassesReadCache(t *testing.T) {
	pb, inodeID := newBufferedTestMeta(t)
	cs := NewMemoryChunkStore()
	b := newTestBuffered(pb, cs, inodeID)
	ctx := context.Background()

	if _, err := b.Write(ctx, []byte("committed"), 0); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if _, err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// New buffered file with a cache that would return garbage if consulted.
	b2 := newTestBuffered(pb, cs, inodeID, WithReadCache(garbageCache{}))
	if _, err := b2.Write(ctx, []byte("XYZ"), 50); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	got, err := b2.ReadView(ctx, 0, 60)
	if err != nil {
		t.Fatalf("ReadView: %v", err)
	}
	// If hydration had consulted the cache, [0,9) would be GARBAGE.
	if string(got[:9]) != "committed" {
		t.Errorf("hydrated prefix=%q, want %q (cache must be bypassed)", got[:9], "committed")
	}
}

// ========== Budget / spill ==========

func TestBufferedFile_BudgetSpillReload(t *testing.T) {
	withInvariantOn(t)
	pb, inodeID := newBufferedTestMeta(t)
	cs := NewMemoryChunkStore()
	stats := &countingSpillStats{}
	dir := t.TempDir()
	b := newTestBuffered(pb, cs, inodeID,
		WithSpillStats(stats),
		WithBudget(Budget{
			MaxDirtyBytes: int64(metadata.MaxChunkSize), // one in-memory chunk max
			StagingDir:    dir,
		}),
	)
	ctx := context.Background()

	dataA := []byte("chunk-a-data")
	dataB := []byte("chunk-b-data")
	if _, err := b.Write(ctx, dataA, 0); err != nil {
		t.Fatalf("Write A: %v", err)
	}
	// Second chunk base: exceeds the 1-chunk budget → chunk 0 must spill.
	if _, err := b.Write(ctx, dataB, metadata.MaxChunkSize); err != nil {
		t.Fatalf("Write B: %v", err)
	}
	if stats.spill.Load() != 1 {
		t.Errorf("spills=%d, want 1", stats.spill.Load())
	}

	// Flush reloads the spilled chunk from staging before persisting.
	res, err := b.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if stats.load.Load() != 1 {
		t.Errorf("staging loads=%d, want 1", stats.load.Load())
	}
	if len(res.NewRefs) != 2 {
		t.Fatalf("NewRefs len=%d, want 2", len(res.NewRefs))
	}

	// Both chunks readable after flush, byte-exact. Chunk A lives at offset 0,
	// chunk B at offset MaxChunkSize.
	gotA, err := b.ReadView(ctx, 0, int64(len(dataA)))
	if err != nil {
		t.Fatalf("ReadView A: %v", err)
	}
	if string(gotA) != string(dataA) {
		t.Errorf("chunk A=%q, want %q", gotA, dataA)
	}
	gotB, err := b.ReadView(ctx, metadata.MaxChunkSize, int64(len(dataB)))
	if err != nil {
		t.Fatalf("ReadView B: %v", err)
	}
	if string(gotB) != string(dataB) {
		t.Errorf("chunk B=%q, want %q", gotB, dataB)
	}

	// Staging dir emptied: loadFromDisk removed chunk 0's file, Clear removed
	// nothing further (no leftovers).
	if n := stagingFileCount(dir); n != 0 {
		t.Errorf("staging dir has %d leftover files after flush, want 0", n)
	}
}

func TestBufferedFile_OutOfBudgetNoSink(t *testing.T) {
	pb, inodeID := newBufferedTestMeta(t)
	b := newTestBuffered(pb, NewMemoryChunkStore(), inodeID,
		WithBudget(Budget{MaxDirtyBytes: int64(metadata.MaxChunkSize)}), // no StagingDir
	)
	ctx := context.Background()

	if _, err := b.Write(ctx, []byte("first"), 0); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	// Second chunk base cannot fit and there is no sink to spill to.
	n, err := b.Write(ctx, []byte("second"), metadata.MaxChunkSize)
	if !errors.Is(err, ErrOutOfDirtyBudget) {
		t.Fatalf("Write 2 err=%v, want ErrOutOfDirtyBudget", err)
	}
	if n != 0 {
		t.Errorf("Write 2 returned %d bytes, want 0 (nothing written)", n)
	}
	if b.DirtyBytes() != int64(metadata.MaxChunkSize) {
		t.Errorf("DirtyBytes=%d, want %d (first chunk untouched)", b.DirtyBytes(), int64(metadata.MaxChunkSize))
	}
	// The original write still flushes fine.
	if _, err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// ========== Truncate / holes / concurrency ==========

func TestBufferedFile_TruncateDown(t *testing.T) {
	pb, inodeID := newBufferedTestMeta(t)
	b := newTestBuffered(pb, NewMemoryChunkStore(), inodeID)
	ctx := context.Background()

	if _, err := b.Write(ctx, []byte("abcdefghij"), 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	b.Truncate(4, 0)
	if b.Size() != 4 {
		t.Errorf("Size()=%d, want 4", b.Size())
	}
	if !b.Dirty() {
		t.Error("Dirty()=false after truncate, want true")
	}

	res, err := b.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if res.NewSize != 4 {
		t.Errorf("NewSize=%d, want 4", res.NewSize)
	}
	// Committed image is the truncated 4 bytes, nothing beyond.
	got, err := b.ReadView(ctx, 0, 10)
	if err != nil {
		t.Fatalf("ReadView: %v", err)
	}
	if string(got) != "abcd" {
		t.Errorf("view=%q, want %q", got, "abcd")
	}
}

// TestBufferedFile_ClearEmptiesStaging covers the O_TRUNC open path, which
// clears the whole buffer image including spill files — truncation must leave
// no orphaned staging files behind (a pre-move leak).
func TestBufferedFile_ClearEmptiesStaging(t *testing.T) {
	pb, inodeID := newBufferedTestMeta(t)
	dir := t.TempDir()
	b := newTestBuffered(pb, NewMemoryChunkStore(), inodeID,
		WithBudget(Budget{MaxDirtyBytes: int64(metadata.MaxChunkSize), StagingDir: dir}),
	)
	ctx := context.Background()

	if _, err := b.Write(ctx, []byte("a"), 0); err != nil {
		t.Fatalf("Write A: %v", err)
	}
	if _, err := b.Write(ctx, []byte("b"), metadata.MaxChunkSize); err != nil {
		t.Fatalf("Write B: %v", err)
	}
	if n := stagingFileCount(dir); n != 1 {
		t.Fatalf("staging files before clear=%d, want 1 (chunk 0 spilled)", n)
	}
	_ = ctx
	b.Clear()
	if n := stagingFileCount(dir); n != 0 {
		t.Errorf("staging files after clear=%d, want 0 (no orphaned spills)", n)
	}
}

func TestBufferedFile_SparseHoleZeroFill(t *testing.T) {
	pb, inodeID := newBufferedTestMeta(t)
	b := newTestBuffered(pb, NewMemoryChunkStore(), inodeID)
	ctx := context.Background()

	if _, err := b.Write(ctx, []byte("hi"), 100); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// File is 102 bytes: [0,100) hole, [100,102) data.
	got, err := b.ReadView(ctx, 0, 105)
	if err != nil {
		t.Fatalf("ReadView: %v", err)
	}
	if len(got) != 102 {
		t.Fatalf("read %d bytes, want 102 (file size)", len(got))
	}
	for i := 0; i < 100; i++ {
		if got[i] != 0 {
			t.Fatalf("hole byte %d=%d, want 0", i, got[i])
		}
	}
	if string(got[100:102]) != "hi" {
		t.Errorf("data=%q, want %q", got[100:102], "hi")
	}
}

func TestBufferedFile_ConcurrentAppend(t *testing.T) {
	pb, inodeID := newBufferedTestMeta(t)
	b := newTestBuffered(pb, NewMemoryChunkStore(), inodeID)
	ctx := context.Background()

	const (
		workers = 8
		per     = 16
	)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				if _, err := b.AppendWrite(ctx, []byte("x")); err != nil {
					t.Errorf("AppendWrite: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	res, err := b.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	wantSize := int64(workers * per)
	if res.NewSize != wantSize {
		t.Errorf("NewSize=%d, want %d", res.NewSize, wantSize)
	}
	got, err := b.ReadView(ctx, 0, wantSize)
	if err != nil {
		t.Fatalf("ReadView: %v", err)
	}
	if len(got) != int(wantSize) {
		t.Fatalf("read %d bytes, want %d", len(got), wantSize)
	}
	if strings.Count(string(got), "x") != int(wantSize) {
		t.Errorf("read %d 'x' bytes, want %d (no lost/overwritten appends)", strings.Count(string(got), "x"), wantSize)
	}
}

// ========== Ledger ==========

func TestBufferedFile_FlushLedgerTransitions(t *testing.T) {
	pb, inodeID := newBufferedTestMeta(t)
	ledger := &recordingLedger{}
	b := newTestBuffered(pb, NewMemoryChunkStore(), inodeID, WithFlushLedger(ledger))
	ctx := context.Background()

	if _, err := b.Write(ctx, []byte("ledger-data"), 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	want := []metadata.WriteAttemptState{
		metadata.WriteAttemptPending,
		metadata.WriteAttemptChunksAllocated,
		metadata.WriteAttemptChunksDurable,
		metadata.WriteAttemptCommitted,
	}
	states := ledger.snapshot()
	if len(states) != len(want) {
		t.Fatalf("ledger transitions=%v, want %v", states, want)
	}
	for i := range want {
		if states[i] != want[i] {
			t.Errorf("transition[%d]=%v, want %v", i, states[i], want[i])
		}
	}

	// A clean flush records nothing further.
	if _, err := b.Flush(ctx); err != nil {
		t.Fatalf("clean Flush: %v", err)
	}
	if got := len(ledger.snapshot()); got != len(want) {
		t.Errorf("ledger transitions after clean flush=%d, want %d (no-op)", got, len(want))
	}
}

// TestBufferedFile_InvariantSpillAware verifies the buffer-image invariant
// accepts a spilled chunk (buffer evicted to staging) as a valid image — the
// original FUSE assertion only accepted an in-memory buffer and would have
// panicked here.
func TestBufferedFile_InvariantSpillAware(t *testing.T) {
	withInvariantOn(t)
	pb, inodeID := newBufferedTestMeta(t)
	dir := t.TempDir()
	b := newTestBuffered(pb, NewMemoryChunkStore(), inodeID,
		WithBudget(Budget{MaxDirtyBytes: int64(metadata.MaxChunkSize), StagingDir: dir}),
	)
	ctx := context.Background()

	if _, err := b.Write(ctx, []byte("a"), 0); err != nil {
		t.Fatalf("Write A: %v", err)
	}
	// This forces a spill of chunk 0; with the invariant on, a panic here would
	// mean the assertion is not spill-aware.
	if _, err := b.Write(ctx, []byte("b"), metadata.MaxChunkSize); err != nil {
		t.Fatalf("Write B: %v", err)
	}
}

// TestBufferedFile_ReadViewV2ExtentLayout drives the read dual-model
// (roadmap §1.3b): a file whose inode is a V2 inline extent (no ChunkMap)
// must still be readable. The file is seeded the way 1.3c will produce it —
// AllocateChunk for a real chunk row + payload, then SetInlineExtent to
// promote the inode, with the extent ID mirroring the chunk ID.
func TestBufferedFile_ReadViewV2ExtentLayout(t *testing.T) {
	pb, inodeID, policy := newWriterTestMeta(t)
	mem := NewMemoryChunkStore()
	ctx := context.Background()

	payload := []byte("v2 fuse extent read")
	chunk, err := pb.AllocateChunk(ctx, inodeID, 0, policy)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	if err := mem.WriteChunk(ctx, chunk, payload); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	ext := &metadata.ExtentMetaV2{
		ID:         metadata.ExtentIDV2(chunk.ID),
		Generation: 1,
		LogicalLen: int64(len(payload)),
		PGID:       1,
	}
	if err := pb.SetInlineExtent(ctx, inodeID, ext, int64(len(payload))); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}

	b := newTestBuffered(pb, mem, inodeID)

	// Full read: the resolver maps the extent back to the chunk and the
	// buffered reader streams the committed payload.
	got, err := b.ReadView(ctx, 0, int64(len(payload)))
	if err != nil {
		t.Fatalf("ReadView: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("ReadView body = %q, want %q", got, payload)
	}

	// A windowed read lands on the same extent-backed chunk.
	want := payload[4:14]
	got, err = b.ReadView(ctx, 4, int64(len(want)))
	if err != nil {
		t.Fatalf("ReadView window: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("ReadView window = %q, want %q", got, want)
	}
}
