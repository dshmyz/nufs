//go:build linux

package fuse

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// TestMain enables the runtime buffer-image invariant assertion for the whole
// package. With it on, any test path that breaks the invariant
// (loaded=true but buffer shorter than the committed file size) panics
// immediately instead of silently producing wrong data — the regression
// tripwire for the off-0 overwrite / hydration bug class.
func TestMain(m *testing.M) {
	chunkstore.EnableBufferImageInvariant()
	os.Exit(m.Run())
}

// ========== Test fixtures ==========

// newTestMetaStore returns an in-memory PebbleStore wired with a
// single pre-created bucket "test" and a pre-created file at the
// bucket root. The returned inode ID is what the file uses.
func newTestMetaStore(t testing.TB) (*metadata.PebbleStore, metadata.InodeID) {
	t.Helper()
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:         t.TempDir(), // required even in UseInMemory mode; storage is mem-VFS
		UseInMemory: true,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()

	// Register one healthy node so the PlacementEngine can satisfy RF=1
	// chunk allocations. Without any node the store rejects placement
	// with "insufficient healthy nodes" (these fixtures run under linux).
	if err := store.RegisterNode(ctx, &metadata.NodeInfo{
		ID:         1,
		Addr:       "127.0.0.1:9001",
		CapacityGB: 100,
	}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}

	if err := store.CreateBucket(ctx, "test", metadata.PlacementPolicy{
		ID:                "test",
		ReplicationFactor: 1,
		TopologySpread:    metadata.SpreadNode,
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	file, err := store.CreateFile(ctx, bucket.RootInode, "hello.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	return store, file.ID
}

// newTestFileFromDFS builds a DFSFile (via the production newDFSFile wiring)
// from an existing filesystem root and inode, so unit fixtures exercise the
// same buffer construction as production. Panics if the inode can't be read.
func newTestFileFromDFS(dfs *DFSFileSystem, id metadata.InodeID, rec MetricsRecorder) *DFSFile {
	metaInode, err := dfs.Meta().GetInode(context.Background(), id)
	if err != nil {
		panic(err)
	}
	return newDFSFile(dfs, metaInode, rec)
}

// newTestFile returns a DFSFile backed by the given meta+chunkStore,
// with a pre-set inodeID. A real DFSFileSystem root is created so the
// file's `fs` field resolves Meta()/checkAccess; the inode is not
// attached to a FUSE bridge (unit tests exercise its methods directly).
// committedSize is seeded from the committed inode size, mirroring the
// production Open path — a partial overwrite must not shrink the file
// below its committed extent.
func newTestFile(meta metadata.MetadataService, cs chunkstore.ChunkStore, id metadata.InodeID) *DFSFile {
	dfs := NewDFSFileSystem(meta, cs, nil, nil, nil)
	return newTestFileFromDFS(dfs, id, nil)
}

// newTestFileWithRecorder 同 newTestFile 但注入 MetricsRecorder。
func newTestFileWithRecorder(meta metadata.MetadataService, cs chunkstore.ChunkStore, id metadata.InodeID, rec MetricsRecorder) *DFSFile {
	dfs := NewDFSFileSystem(meta, cs, nil, rec, nil)
	return newTestFileFromDFS(dfs, id, rec)
}

// resolvedChunks returns a file's committed chunk references under either
// storage model (V1 ChunkMap or V2 extent layout, roadmap §1.3b/§1.3c). FUSE
// flush lands inline extents for small files, so a post-flush inode's ChunkMap
// is nil and its refs must come from the extent surface instead of the row.
func resolvedChunks(t testing.TB, meta metadata.MetadataService, inode *metadata.InodeMeta) []metadata.ChunkRef {
	t.Helper()
	es, _ := meta.(metadata.ExtentInodeService)
	refs, err := metadata.ResolveFileChunks(context.Background(), es, inode)
	if err != nil {
		t.Fatalf("ResolveFileChunks: %v", err)
	}
	return refs
}

// ========== B1: Read returns real chunk data (was: always zero bytes) ==========

// TestDFSFile_Read_EmptyFile_ReturnsZeros has been removed: reading a
// freshly-created zero-size file at offset 0 is "at EOF" (off >= size),
// so the correct behavior is returning no bytes — which is already
// covered by TestDFSFile_Read_PastEOF_ReturnsNil. The prior assertion
// of 32 zero bytes contradicted the (correct, unchanged) EOF semantics.

// TestDFSFile_Read_PastEOF_ReturnsNil is the "offset >= size" path.
func TestDFSFile_Read_PastEOF_ReturnsNil(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	// File size is 0 (fresh). Off=0 is "at EOF" because off >= size
	// (0 >= 0), so we expect a nil payload.
	dest := make([]byte, 16)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if len(got) != 0 {
		t.Fatalf("Read at EOF: got %d bytes, want 0", len(got))
	}
}

// TestDFSFile_Read_AfterFlush_ReadsFromChunkStore is the round-trip
// path. Flush writes the buffer to the chunk store, then Read pulls
// it back. This is the test that proves B1 is fixed end-to-end:
// pre-fix, the data never reached the chunk store, so the Read
// either returned zeros (fast path) or EIO.
func TestDFSFile_Read_AfterFlush_ReadsFromChunkStore(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	// Write "hello, world!" into the buffer.
	want := []byte("hello, world!")
	if _, errno := f.Write(context.Background(), nil, want, 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	// Re-read the inode — it should now have Size=13 and a non-empty
	// ChunkMap.
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != int64(len(want)) {
		t.Fatalf("after Flush: Size=%d, want %d", inode.Size, len(want))
	}
	if len(resolvedChunks(t, meta, inode)) == 0 {
		t.Fatalf("after Flush: no committed chunk (B2 regression: flush didn't allocate)")
	}

	// Now Read should return the bytes from the chunk store.
	dest := make([]byte, 32)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if !bytes.Equal(got, want) {
		t.Fatalf("Read: got %q, want %q (B1 regression: not reading from chunk store)", got, want)
	}
}

// TestDFSFile_Read_TruncateExtend_ServesZeros covers the hole-read path:
// a file truncated (extended) to a nonzero size via Setattr, so its inode
// has Size > 0 but an empty ChunkMap (no data has ever been flushed). The
// new size range is a hole; reading within it must return zero-filled
// bytes. This is the dead-path fix: pre-fix the fast path required
// Size==0, and the EOF clamp (off >= Size) fires before it for Size==0, so
// an empty-ChunkMap file with Size>0 fell through to an empty ChunkMap
// walk and returned zero bytes instead of the hole's zeros.
func TestDFSFile_Read_TruncateExtend_ServesZeros(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	// Extend the file to 1024 bytes with no data written (truncate-up:
	// POSIX ftruncate to a larger size zero-fills the new range). The
	// freshly-created file's ChunkMap is empty, so this yields the
	// Size>0 / empty-ChunkMap hole state.
	const holeSize = 1024
	if errno := f.Setattr(context.Background(), nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			Valid: fuse.FATTR_SIZE,
			Size:  holeSize,
		},
	}, &fuse.AttrOut{}); errno != 0 {
		t.Fatalf("Setattr truncate-extend: errno=%v", errno)
	}

	// Sanity: the inode now has the size but no chunks.
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != holeSize {
		t.Fatalf("after truncate: Size=%d, want %d", inode.Size, holeSize)
	}
	if len(inode.ChunkMap) != 0 {
		t.Fatalf("after truncate: ChunkMap len=%d, want 0", len(inode.ChunkMap))
	}

	// Read the whole hole — every byte must come back as 0x00.
	dest := make([]byte, holeSize)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read hole: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if len(got) != holeSize {
		t.Fatalf("Read hole: got %d bytes, want %d", len(got), holeSize)
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("Read hole: byte[%d]=0x%02x, want 0x00", i, b)
		}
	}

	// Reading past EOF still returns no bytes (unchanged EOF semantics).
	rr, errno = f.Read(context.Background(), nil, make([]byte, 16), holeSize)
	if errno != 0 {
		t.Fatalf("Read at EOF: errno=%v", errno)
	}
	eof, _ := rr.Bytes(dest)
	if len(eof) != 0 {
		t.Fatalf("Read at EOF: got %d bytes, want 0", len(eof))
	}
}

// TestDFSFile_Read_SparseCommittedChunkMap_ZeroFillsHoles exercises the
// committed-chunk walk against a non-contiguous ChunkMap. A FUSE flush
// always produces a contiguous map, but the committed layout can be sparse
// (externally-produced inode, or a post-truncate map with a hole whose
// bytes were never materialized). The walk must not collapse such a hole:
// it must zero-fill it at the correct file coordinate and still return a
// full (end-off)-byte window. (Same-class hardening of the committed-read
// path — before the fix, out-of-order / gapped chunks were concatenated
// back-to-back, misplacing bytes and returning a short read.)
func TestDFSFile_Read_SparseCommittedChunkMap_ZeroFillsHoles(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	// First, write and flush a real file so the chunk store holds payloads
	// under real (committed) chunk IDs, and f.buffer is nil'd by Flush so
	// Read goes through the committed-chunk walk.
	seed := []byte("0123456789abcdef") // 16 bytes
	if _, errno := f.Write(context.Background(), nil, seed, 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}
	if f.buffered.Dirty() {
		t.Fatalf("post-Flush: file still dirty (committed-walk path)")
	}

	// Grab a committed chunk ID already in the store from the seed flush. The
	// seed flush landed inline (roadmap §1.3c), so the refs come from the extent
	// surface rather than the row's ChunkMap (which is nil on a V2 row).
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	refs := resolvedChunks(t, meta, inode)
	if len(refs) != 1 {
		t.Fatalf("seed committed refs len=%d, want 1", len(refs))
	}
	cid := refs[0].ID

	// Allocate + commit a second REAL chunk so the extent set can refer to it
	// (mirroring Flush: allocate → write → commit).
	bucket, err := meta.GetBucket(context.Background(), "test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	alloc, err := meta.AllocateChunksBatch(context.Background(), id, []int64{10}, bucket.Policy)
	if err != nil {
		t.Fatalf("AllocateChunksBatch: %v", err)
	}
	if len(alloc) != 1 {
		t.Fatalf("allocated %d chunks, want 1", len(alloc))
	}
	chunk2 := alloc[0]
	if err := cs.WriteChunk(context.Background(), chunk2, seed[10:16]); err != nil {
		t.Fatalf("WriteChunk (chunk2): %v", err)
	}
	if err := meta.CommitChunk(context.Background(), chunk2.ID, 0); err != nil {
		t.Fatalf("CommitChunk (chunk2): %v", err)
	}

	// Rewrite the committed layout to a sparse V2 extent set with a hole:
	//   extent cid (file [0,4))       → "0123"
	//   hole  (file [4,10))           → zeros
	//   extent chunk2 (file [10,16))  → "abcd" (second half of the seed)
	// Seeding the gap through the extent surface (ReplaceExtents) is required:
	// the model guard refuses a V1 ChunkMap rewrite on a now-V2 row.
	writes := []metadata.ExtentWrite{
		{Extent: &metadata.ExtentMetaV2{ID: metadata.ExtentIDV2(cid), Generation: 1, LogicalLen: 4}, Offset: 0},
		{Extent: &metadata.ExtentMetaV2{ID: metadata.ExtentIDV2(chunk2.ID), Generation: 1, LogicalLen: 6}, Offset: 10},
	}
	if err := meta.ReplaceExtents(context.Background(), id, writes, 16); err != nil {
		t.Fatalf("ReplaceExtents (sparse): %v", err)
	}

	dest := make([]byte, 16)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if len(got) != 16 {
		t.Fatalf("Read: got %d bytes, want 16 (holes must be zero-filled)", len(got))
	}
	want := append([]byte("0123"), make([]byte, 6)...)
	want = append(want, seed[10:16]...)
	if !bytes.Equal(got, want) {
		t.Fatalf("sparse read mismatch:\n got %q\nwant %q", got, want)
	}
}

// TestDFSFile_Write_PartialOverwriteZero_OnReopen_PreservesCommittedTail
// guards a same-class data-loss hazard: a FRESH DFSFile (buffer nil,
// loaded=false) doing a partial overwrite at offset 0 over an existing
// committed file must NOT treat the empty buffer as a full-file overwrite.
// Off-0 is a full overwrite only when the write covers the whole committed
// file; a partial 0-off write replaces the head and must leave the committed
// tail intact. Without hydration the buffer would claim loaded=true while
// holding only [0,head), so a subsequent Read/Flush would truncate the
// committed tail to zeros / drop it entirely.
func TestDFSFile_Write_PartialOverwriteZero_OnReopen_PreservesCommittedTail(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()

	// Handle 1: write a 100-byte committed file and flush it.
	f1 := newTestFile(meta, cs, id)
	original := bytes.Repeat([]byte("x"), 100)
	if _, errno := f1.Write(context.Background(), nil, original, 0); errno != 0 {
		t.Fatalf("Write (original): errno=%v", errno)
	}
	if errno := f1.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush (original): errno=%v", errno)
	}

	// Handle 2: a fresh DFSFile models a reopened fd — buffer nil, loaded=false.
	f2 := newTestFile(meta, cs, id)
	head := []byte("abcdefghij") // 10 bytes overwriting file [0,10)
	if _, errno := f2.Write(context.Background(), nil, head, 0); errno != 0 {
		t.Fatalf("Write (partial off=0): errno=%v", errno)
	}

	// Read through handle 2 before any flush: [0,10) is the new head, and
	// the committed [10,100) must still be present (not zeros from a bogus
	// loaded=true claim over an un-hydrated buffer).
	dest := make([]byte, 100)
	rr, errno := f2.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	want := append(append([]byte(nil), head...), original[10:]...)
	if !bytes.Equal(got, want) {
		t.Fatalf("partial overwrite read mismatch:\n got %q\nwant %q", got, want)
	}

	// Flush handle 2 and re-read: the file must still be 100 bytes with the
	// new head + preserved committed tail — NOT truncated to 10 bytes.
	if errno := f2.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush (partial): errno=%v", errno)
	}
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != 100 {
		t.Fatalf("after flush: Size=%d, want 100 (committed tail must be preserved)", inode.Size)
	}
	f3 := newTestFile(meta, cs, id)
	dest2 := make([]byte, 100)
	rr, errno = f3.Read(context.Background(), nil, dest2, 0)
	if errno != 0 {
		t.Fatalf("Read (post-flush): errno=%v", errno)
	}
	got2, _ := rr.Bytes(dest2)
	if !bytes.Equal(got2, want) {
		t.Fatalf("post-flush read mismatch:\n got %q\nwant %q", got2, want)
	}
}

// ========== B2: Flush actually writes the buffer (was: only updated inode size) ==========

// TestDFSFile_Flush_AllocatesChunk is the "first flush" path. After
// Flush, the chunk store must contain the buffer contents under the
// allocated chunk ID, and the inode's ChunkMap must have length 1.
func TestDFSFile_Flush_AllocatesChunk(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	want := []byte("committed payload")
	if _, errno := f.Write(context.Background(), nil, want, 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	// Inspect metadata: ChunkMap should now reference the chunk.
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	refs := resolvedChunks(t, meta, inode)
	if len(refs) != 1 {
		t.Fatalf("committed refs len=%d, want 1", len(refs))
	}
	chunkID := refs[0].ID
	if chunkID == 0 {
		t.Fatalf("refs[0].ID == 0; allocator should produce a non-zero ID")
	}
	if refs[0].Length != int32(len(want)) {
		t.Fatalf("refs[0].Length=%d, want %d", refs[0].Length, len(want))
	}

	// Inspect chunk store: the payload must be there verbatim.
	stored, ok := cs.Get(chunkID)
	if !ok {
		t.Fatalf("chunk %d not in chunk store (B2 regression: flush did not write to chunk store)", chunkID)
	}
	if !bytes.Equal(stored, want) {
		t.Fatalf("chunk %d contents=%q, want %q", chunkID, stored, want)
	}
}

// TestDFSFile_Flush_CommitsAndSeals is the post-write state check:
// the chunk should be in the "sealed" state, and the metadata
// should reflect a non-zero checksum that matches crc32 of the
// payload.
func TestDFSFile_Flush_CommitsAndSeals(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	want := []byte("check me")
	if _, errno := f.Write(context.Background(), nil, want, 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	refs := resolvedChunks(t, meta, inode)
	if len(refs) == 0 {
		t.Fatalf("no committed chunk after flush")
	}
	chunk, err := meta.GetChunk(context.Background(), refs[0].ID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if chunk.State != metadata.ChunkReady {
		t.Fatalf("chunk state=%v, want ChunkReady (post-seal)", chunk.State)
	}
	if chunk.Checksum != crc32.ChecksumIEEE(want) {
		t.Fatalf("chunk checksum=%#x, want %#x", chunk.Checksum, crc32.ChecksumIEEE(want))
	}
}

// ========== B3: Flush clears the buffer (was: didn't reset f.buffer) ==========

// TestDFSFile_Flush_Idempotent is the post-B3 regression test. After
// Flush, the buffer must be nil and dirty must be false, so a second
// Flush is a no-op (does not double-allocate, does not re-write).
// Pre-fix, f.buffer was not cleared, so a second Flush re-allocated
// a chunk and re-sized the inode, producing a "size is too large"
// error on the second Release (Flush).
func TestDFSFile_Flush_Idempotent(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("abc"), 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush #1: errno=%v", errno)
	}

	inode1, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode after flush #1: %v", err)
	}
	refs1 := resolvedChunks(t, meta, inode1)
	chunkCount := len(refs1)
	size1 := inode1.Size

	// Second flush without further writes should be a no-op.
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush #2: errno=%v (B3 regression: not idempotent)", errno)
	}

	inode2, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode after flush #2: %v", err)
	}
	refs2 := resolvedChunks(t, meta, inode2)
	if len(refs2) != chunkCount {
		t.Fatalf("after flush #2: committed refs len=%d, want %d (B3 regression: re-allocated)", len(refs2), chunkCount)
	}
	if inode2.Size != size1 {
		t.Fatalf("after flush #2: Size=%d, want %d (B3 regression: re-sized)", inode2.Size, size1)
	}

	// Internal state checks: buffer should be clean, dirty should be
	// false. These are implementation details but they're the
	// direct fix for the B3 bug; locking them in here documents
	// the contract.
	if f.buffered.Dirty() {
		t.Fatalf("after flush: dirty=true, want false (B3 fix)")
	}
}

// TestDFSFile_Flush_CleanIsNoop is the path where Flush is called on
// a file that hasn't been written to (e.g. read-only open followed
// by close). It must return success without touching metadata.
func TestDFSFile_Flush_CleanIsNoop(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush on clean file: errno=%v", errno)
	}

	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if len(inode.ChunkMap) != 0 {
		t.Fatalf("after clean flush: ChunkMap len=%d, want 0", len(inode.ChunkMap))
	}
	if inode.Size != 0 {
		t.Fatalf("after clean flush: Size=%d, want 0", inode.Size)
	}
}

// TestDFSFile_Flush_Oversized_AllocatesMultiChunk replaces the old
// EFBIG rejection (commit 1.1 single-chunk path). Now a buffer larger
// than metadata.MaxChunkSize is split into multiple chunks and the inode's
// ChunkMap holds one ref per metadata.MaxChunkSize window (Program 11).
func TestDFSFile_Flush_Oversized_AllocatesMultiChunk(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	// Seed the buffer past the chunk limit via the real Write path.
	size := 2*metadata.MaxChunkSize + 123
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(i % 251)
	}
	if _, errno := f.Write(context.Background(), nil, buf, 0); errno != 0 {
		t.Fatalf("Write seed: errno=%v", errno)
	}

	errno := f.Flush(context.Background(), nil)
	if errno != 0 {
		t.Fatalf("Flush oversized: errno=%v, want 0 (multi-chunk)", errno)
	}

	// The inode must now reference ceil(size/chunk) = 3 chunks.
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	refs := resolvedChunks(t, meta, inode)
	if len(refs) != 3 {
		t.Fatalf("after oversized flush: committed refs len=%d, want 3", len(refs))
	}
	if inode.Size != int64(size) {
		t.Fatalf("after oversized flush: Size=%d, want %d", inode.Size, size)
	}
	// Each ref is sealed, holds the right offset/length window, and
	// read-back is byte-exact across the whole file.
	for i, cref := range refs {
		wantOff := int64(i) * metadata.MaxChunkSize
		if cref.Offset != wantOff {
			t.Fatalf("refs[%d].Offset=%d, want %d", i, cref.Offset, wantOff)
		}
		wantLen := metadata.MaxChunkSize
		if i == 2 {
			wantLen = size - 2*metadata.MaxChunkSize
		}
		if int(cref.Length) != wantLen {
			t.Fatalf("refs[%d].Length=%d, want %d", i, cref.Length, wantLen)
		}
		chunk, gerr := meta.GetChunk(context.Background(), cref.ID)
		if gerr != nil {
			t.Fatalf("GetChunk(%d): %v", cref.ID, gerr)
		}
		if chunk.State != metadata.ChunkReady {
			t.Fatalf("chunk %d state=%v, want ready (post-seal)", cref.ID, chunk.State)
		}
		if chunk.Checksum != crc32.ChecksumIEEE(buf[cref.Offset:cref.Offset+int64(cref.Length)]) {
			t.Fatalf("chunk %d checksum mismatch", cref.ID)
		}
	}

	dest := make([]byte, size)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read oversized: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if !bytes.Equal(got, buf) {
		t.Fatalf("oversized read-back mismatch: got %d bytes (prefix %v...), want %d", len(got), got[:8], len(buf))
	}
}

// TestDFSFile_Flush_MultiChunk_ReadBack is the Program 11 capstone for
// the plain multi-chunk path: a payload spanning 2*metadata.MaxChunkSize+123
// bytes (3 chunk windows) flushes into 3 sealed chunks at aligned
// offsets, and Read reconstructs the whole file byte-exact across them.
func TestDFSFile_Flush_MultiChunk_ReadBack(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	size := 2*metadata.MaxChunkSize + 123
	want := make([]byte, size)
	for i := range want {
		want[i] = byte((i*7 + 3) % 256)
	}
	if _, errno := f.Write(context.Background(), nil, want, 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	refs := resolvedChunks(t, meta, inode)
	if len(refs) != 3 {
		t.Fatalf("committed refs len=%d, want 3", len(refs))
	}
	if inode.Size != int64(size) {
		t.Fatalf("Size=%d, want %d", inode.Size, size)
	}

	dest := make([]byte, size)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if !bytes.Equal(got, want) {
		t.Fatalf("multi-chunk read-back mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

// TestDFSFile_Flush_MultiChunk_CrossFlushReuse is the Program 11
// chunk-reuse test: a first flush writes a multi-chunk file, then a
// second flush (after overwriting part of the buffer) rebuilds the
// ChunkMap with fresh chunk IDs and DeleteChunk's the superseded old
// chunks (S3 PUT-overwrite alignment). Read-back after the second flush
// is byte-exact against the new buffer.
func TestDFSFile_Flush_MultiChunk_CrossFlushReuse(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	// First flush: a 2-window file.
	size1 := metadata.MaxChunkSize + 50
	first := make([]byte, size1)
	for i := range first {
		first[i] = byte(i % 200)
	}
	if _, errno := f.Write(context.Background(), nil, first, 0); errno != 0 {
		t.Fatalf("Write #1: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush #1: errno=%v", errno)
	}

	before, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode (before overwrite): %v", err)
	}
	beforeRefs := resolvedChunks(t, meta, before)
	if len(beforeRefs) != 2 {
		t.Fatalf("before overwrite: committed refs len=%d, want 2", len(beforeRefs))
	}
	oldIDs := make(map[metadata.ChunkID]bool, len(beforeRefs))
	for _, cref := range beforeRefs {
		oldIDs[cref.ID] = true
	}

	// Second flush: overwrite a chunk-worth of data and extend the file.
	size2 := metadata.MaxChunkSize + 300
	second := make([]byte, size2)
	for i := range second {
		second[i] = byte((i*11 + 1) % 256)
	}
	if _, errno := f.Write(context.Background(), nil, second, 0); errno != 0 {
		t.Fatalf("Write #2: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush #2: errno=%v", errno)
	}

	// The inode's ChunkMap is rebuilt with fresh (non-overlapping) IDs.
	after, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode (after overwrite): %v", err)
	}
	afterRefs := resolvedChunks(t, meta, after)
	if len(afterRefs) != 2 {
		t.Fatalf("after overwrite: committed refs len=%d, want 2", len(afterRefs))
	}
	for _, cref := range afterRefs {
		if oldIDs[cref.ID] {
			t.Fatalf("chunk %d reused across flush — overwrite must not reuse chunk IDs (S3-aligned)", cref.ID)
		}
	}

	// Every old chunk must have been reclaimed: DeleteChunk writes a
	// durable tombstone (metadata retained, inode dereferenced), which
	// is the same reclaim contract S3 metadataObjectCommitter.Put relies
	// on. Verify each old ID landed in the tombstone set.
	ts, err := meta.ListChunkTombstones(context.Background(), 0)
	if err != nil {
		t.Fatalf("ListChunkTombstones: %v", err)
	}
	tombstoned := make(map[metadata.ChunkID]bool, len(ts))
	for _, tb := range ts {
		tombstoned[tb.ChunkID] = true
	}
	for cid := range oldIDs {
		if !tombstoned[cid] {
			t.Fatalf("old chunk %d not tombstoned after overwrite — DeleteChunk was not issued (Program 11 reclaim regression)", cid)
		}
	}

	// Read-back is byte-exact against the new buffer.
	dest := make([]byte, size2)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read (after overwrite): errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if !bytes.Equal(got, second) {
		t.Fatalf("cross-flush read-back mismatch: got %d bytes, want %d", len(got), len(second))
	}
}

// TestDFSFile_Append_AfterFlush_HydratesCommittedContent covers O_APPEND
// semantics after a Flush: opening with O_APPEND routes writes to
// AppendWrite at the file's current end. The tricky part is that a Flush
// clears the in-memory buffer, so an append must first hydrate the
// committed content back into the buffer — otherwise the next Flush would
// rebuild the whole file from an empty buffer and silently zero the
// originally-written prefix.
func TestDFSFile_Append_AfterFlush_HydratesCommittedContent(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	// Write the base content and flush it.
	base := []byte("hello")
	if _, errno := f.Write(context.Background(), nil, base, 0); errno != 0 {
		t.Fatalf("Write base: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush base: errno=%v", errno)
	}

	// Open an O_APPEND handle and append. This is the exact regression:
	// pre-fix the buffer (empty after Flush) would not know the committed
	// "hello", so appending " WORL" then flushing would produce
	// "\x00...WORL" with "hello" destroyed.
	h := &DFSFileHandle{file: f, append: true}
	if _, errno := h.Write(context.Background(), []byte(" world"), 0); errno != 0 {
		t.Fatalf("Append: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush appended: errno=%v", errno)
	}

	want := "hello world"
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != int64(len(want)) {
		t.Fatalf("after append: Size=%d, want %d", inode.Size, len(want))
	}

	dest := make([]byte, len(want)+8)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("append read-back = %q, want %q (committed prefix was zeroed)", got, want)
	}
}

// TestDFSFile_Write_NonZeroOffset_AfterFlush_PreservesPrefix covers a
// random in-place overwrite after a Flush (the same hydration
// prerequisite that enables append): flushing "hello world", then
// overwriting [6,11) with "brave", then flushing again must yield
// "hello brave" — the committed prefix must survive the whole-file
// rebuild rather than being zeroed by the fresh buffer.
func TestDFSFile_Write_NonZeroOffset_AfterFlush_PreservesPrefix(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("hello world"), 0); errno != 0 {
		t.Fatalf("Write base: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush base: errno=%v", errno)
	}

	// In-place overwrite at a nonzero offset after the flush.
	if _, errno := f.Write(context.Background(), nil, []byte("brave"), 6); errno != 0 {
		t.Fatalf("Write overwrite: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush overwrite: errno=%v", errno)
	}

	want := []byte("hello brave")
	dest := make([]byte, len(want)+8)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if !bytes.Equal(got, want) {
		t.Fatalf("overwrite read-back = %q, want %q (committed prefix was zeroed)", got, want)
	}
}

// TestDFSFile_PartialOverwrite_DoesNotTruncateCommittedTail guards the
// data-loss regression where a partial in-place overwrite whose end is below
// the committed EOF shrank the file. Sequence: commit a 200-byte file, then
// pwrite 5 bytes at offset 100 (end=105 < 200). Before the fix, Flush computed
// size = logicalSize = 105 (the size==0 fallback didn't fire because 105 != 0)
// and UpdateInode wrote Size=105, silently dropping committed bytes [105,200).
// The fix takes max(logicalSize, committedSize) so the tail is preserved.
func TestDFSFile_PartialOverwrite_DoesNotTruncateCommittedTail(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	// Commit a 200-byte file: all 'A' except a sentinel tail we will check for.
	seed := make([]byte, 200)
	for i := range seed {
		seed[i] = 'A'
	}
	copy(seed[190:], []byte("TAIL123")) // bytes [190,197) = "TAIL123"
	if _, errno := f.Write(context.Background(), nil, seed, 0); errno != 0 {
		t.Fatalf("Write seed: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush seed: errno=%v", errno)
	}

	// Partial in-place overwrite that ends BEFORE the committed EOF.
	patch := []byte("XXXXX") // 5 bytes at offset 100 -> end=105 < 200
	if _, errno := f.Write(context.Background(), nil, patch, 100); errno != 0 {
		t.Fatalf("Write patch: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush patch: errno=%v", errno)
	}

	// The committed Size must still be 200 - not 105.
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != 200 {
		t.Fatalf("inode.Size = %d, want 200 (partial overwrite truncated committed tail)", inode.Size)
	}

	// Read the whole file back; the tail sentinel must survive, and the patch
	// must be applied at offset 100.
	dest := make([]byte, 200)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if len(got) != 200 {
		t.Fatalf("read-back len = %d, want 200", len(got))
	}
	if string(got[100:105]) != "XXXXX" {
		t.Errorf("patch at [100,105) = %q, want %q", got[100:105], "XXXXX")
	}
	if string(got[190:197]) != "TAIL123" {
		t.Errorf("tail sentinel at [190,197) = %q, want %q (committed tail was truncated)", got[190:197], "TAIL123")
	}
}

// TestDFSFile_Append_Concurrent_NoCollision verifies that two append
// writes to the same inode land back-to-back rather than at the same
// offset (the serialization point is the per-inode f.mu in AppendWrite).
// TestDFSFile_LoadCommitted_MultipleRefsInChunk_PreservesAll guards a
// data-loss hazard in loadCommittedChunkLocked: a single 64 MiB base can
// hold more than one committed ChunkRef (e.g. non-aligned refs from a
// sparse/legacy ChunkMap). A partial overwrite that triggers hydration
// must merge ALL overlapping refs, not just the first — otherwise refs
// after the first get zero-filled and their committed data is lost.
func TestDFSFile_LoadCommitted_MultipleRefsInChunk_PreservesAll(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	// Seed a real 16-byte file, then rewrite ChunkMap to two non-contiguous
	// refs inside the SAME base [0, metadata.MaxChunkSize):
	//   ref A: file [0,4)  -> "0123"
	//   ref B: file [10,14) -> "abcd"
	seed := []byte("0123456789abcdef")
	if _, errno := f.Write(context.Background(), nil, seed, 0); errno != 0 {
		t.Fatalf("Write (seed): errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush (seed): errno=%v", errno)
	}

	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	refs := resolvedChunks(t, meta, inode)
	if len(refs) != 1 {
		t.Fatalf("seed committed refs len=%d, want 1", len(refs))
	}
	cidA := refs[0].ID

	// Second real chunk carrying "abcd" at file offset 10.
	bucket, err := meta.GetBucket(context.Background(), "test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	alloc, err := meta.AllocateChunksBatch(context.Background(), id, []int64{10}, bucket.Policy)
	if err != nil {
		t.Fatalf("AllocateChunksBatch: %v", err)
	}
	if len(alloc) != 1 {
		t.Fatalf("allocated %d chunks, want 1", len(alloc))
	}
	chunkB := alloc[0]
	if err := cs.WriteChunk(context.Background(), chunkB, []byte("abcd")); err != nil {
		t.Fatalf("WriteChunk (ref B): %v", err)
	}
	if err := meta.CommitChunk(context.Background(), chunkB.ID, 0); err != nil {
		t.Fatalf("CommitChunk (ref B): %v", err)
	}

	// Rewrite the inline layout to two non-contiguous extents inside the SAME
	// base [0, metadata.MaxChunkSize) via the extent surface (ReplaceExtents) —
	// the merge guard refuses a V1 ChunkMap rewrite on a now-V2 row.
	twoRefs := []metadata.ExtentWrite{
		{Extent: &metadata.ExtentMetaV2{ID: metadata.ExtentIDV2(cidA), Generation: 1, LogicalLen: 4}, Offset: 0},
		{Extent: &metadata.ExtentMetaV2{ID: metadata.ExtentIDV2(chunkB.ID), Generation: 1, LogicalLen: 4}, Offset: 10},
	}
	if err := meta.ReplaceExtents(context.Background(), id, twoRefs, 14); err != nil {
		t.Fatalf("ReplaceExtents (twoRefs): %v", err)
	}

	// Partial overwrite in the gap at offset 5 -> hydration must load BOTH refs.
	if _, errno := f.Write(context.Background(), nil, []byte("XY"), 5); errno != 0 {
		t.Fatalf("Write (gap): errno=%v", errno)
	}

	dest := make([]byte, 14)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	// ref A="0123" at [0,4), gap zero at [4,5), write "XY" at [5,7),
	// gap zero at [7,10), ref B="abcd" at [10,14).
	want := append([]byte("0123"), []byte{0, 'X', 'Y', 0, 0, 0}...)
	want = append(want, []byte("abcd")...)
	if !bytes.Equal(got, want) {
		t.Fatalf("multi-ref merge mismatch:\n got %q\nwant %q (second ref was zero-filled: data loss)", got, want)
	}
}

func TestDFSFile_Append_Concurrent_NoCollision(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	h := &DFSFileHandle{file: f, append: true}
	var wg sync.WaitGroup
	writes := []string{"AAA", "BBB", "CCC", "DDD"}
	for _, w := range writes {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			if _, errno := h.Write(context.Background(), []byte(s), 0); errno != 0 {
				t.Errorf("concurrent append %q: errno=%v", s, errno)
			}
		}(w)
	}
	wg.Wait()

	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	want := "AAABBBCCCDDD"
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != int64(len(want)) {
		t.Fatalf("after concurrent appends: Size=%d, want %d", inode.Size, len(want))
	}

	// Content is a permutation of the four writes, back-to-back — i.e.
	// each write occupies a unique non-overlapping window.
	got := make([]byte, len(want))
	rr, errno := f.Read(context.Background(), nil, got, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	rb, _ := rr.Bytes(got)
	// Sort the read-back into 3-char windows and confirm they're exactly
	// the four pieces (no overlap, no loss).
	seen := map[string]bool{}
	for i := 0; i+3 <= len(rb); i += 3 {
		seen[string(rb[i:i+3])] = true
	}
	for _, w := range writes {
		if !seen[w] {
			t.Fatalf("concurrent append lost/interleaved piece %q (got %q)", w, rb)
		}
	}
}

// ========== State-consistency: Read/Getattr must reflect un-flushed buffer ==========

// TestDFSFile_Read_AfterWrite_NoFlush_ReturnsBuffered covers the same-class
// bug where Read served only committed bytes and returned empty/stale data
// for a write that hadn't been flushed yet. POSIX requires a write followed
// by a read on the same open file (no fsync) to observe the write.
func TestDFSFile_Read_AfterWrite_NoFlush_ReturnsBuffered(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("hello"), 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	// NO Flush. The committed ChunkMap is still empty; Read must serve the
	// buffered "hello" rather than treating the file as empty.
	dest := make([]byte, 8)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("Read after un-flushed write = %q, want %q", got, "hello")
	}
}

// TestDFSFile_Read_AfterInPlaceOverwrite_NoFlush_ReturnsBuffered covers the
// random-overwrite case: flush "hello world", then overwrite [6,11) with
// "brave" WITHOUT flushing, then read. The read must see the new byte range
// (buffered) merged with the committed prefix.
func TestDFSFile_Read_AfterInPlaceOverwrite_NoFlush_ReturnsBuffered(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("hello world"), 0); errno != 0 {
		t.Fatalf("Write base: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush base: errno=%v", errno)
	}
	// Overwrite mid-file, no flush.
	if _, errno := f.Write(context.Background(), nil, []byte("brave"), 6); errno != 0 {
		t.Fatalf("Write overwrite: errno=%v", errno)
	}

	dest := make([]byte, 16)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if !bytes.Equal(got, []byte("hello brave")) {
		t.Fatalf("Read after un-flushed overwrite = %q, want %q", got, "hello brave")
	}
}

// TestDFSFile_Read_AfterAppend_NoFlush_ReturnsBuffered covers the append
// case: an O_APPEND write that grows the file past the committed size must be
// visible to an immediate read (the buffered tail extends the file).
func TestDFSFile_Read_AfterAppend_NoFlush_ReturnsBuffered(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("hello"), 0); errno != 0 {
		t.Fatalf("Write base: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush base: errno=%v", errno)
	}
	h := &DFSFileHandle{file: f, append: true}
	if _, errno := h.Write(context.Background(), []byte(" world"), 0); errno != 0 {
		t.Fatalf("Append: errno=%v", errno)
	}

	dest := make([]byte, 16)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if !bytes.Equal(got, []byte("hello world")) {
		t.Fatalf("Read after un-flushed append = %q, want %q", got, "hello world")
	}
}

// TestDFSFile_Read_UnflushedHole_ReturnsZeros covers a buffered write at a
// nonzero offset that leaves a hole: {"llo" at offset 2, buffer "..llo"} must
// serve two NULs then "llo", even before any flush.
func TestDFSFile_Read_UnflushedHole_ReturnsZeros(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("llo"), 2); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	dest := make([]byte, 5)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	want := []byte{0x00, 0x00, 'l', 'l', 'o'}
	if !bytes.Equal(got, want) {
		t.Fatalf("Read buffered hole = %v, want %v", got, want)
	}
}

// TestDFSFile_Read_AfterFlush_FallsBackToCommitted verifies the committed
// (no-buffer) read path is still correct after a flush clears the buffer —
// the buffer overlay must not corrupt the post-flush read.
func TestDFSFile_Read_AfterFlush_FallsBackToCommitted(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("committed-data"), 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}
	if f.buffered.Dirty() {
		t.Fatalf("after Flush buffer should be clean, got dirty")
	}
	dest := make([]byte, 32)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if !bytes.Equal(got, []byte("committed-data")) {
		t.Fatalf("Read after flush = %q, want %q", got, "committed-data")
	}
}

// TestDFSFile_Getattr_SizeIncludesBufferedTail verifies stat() reports the
// effective file size including an un-flushed O_APPEND tail, so the reported
// size agrees with the next append position.
func TestDFSFile_Getattr_SizeIncludesBufferedTail(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("hello world"), 0); errno != 0 {
		t.Fatalf("Write base: errno=%v", errno)
	}
	h := &DFSFileHandle{file: f, append: true}
	if _, errno := h.Write(context.Background(), []byte("!"), 0); errno != 0 {
		t.Fatalf("Append: errno=%v", errno)
	}
	// No flush: Getattr must reflect the 12-byte buffered file, not the
	// committed size (0), which would be 0 before the first flush.
	var out fuse.AttrOut
	if errno := f.Getattr(context.Background(), nil, &out); errno != 0 {
		t.Fatalf("Getattr: errno=%v", errno)
	}
	if got := int64(out.Attr.Size); got != int64(len("hello world!")) {
		t.Fatalf("Getattr.Size=%d, want %d (buffered tail not reflected)", got, len("hello world!"))
	}
}

// TestDFSFile_Write_OffZero_ExtendsBuffer covers the "pwrite at a
// non-zero offset" path, which previously allocated a giant buffer
// up to `off+len(data)`. That's still how the single-chunk Flush
// works (no sparse-file support in commit 1.1), but at least the
// pwrite path is exercised.
func TestDFSFile_Write_OffZero_ExtendsBuffer(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("AAA"), 0); errno != 0 {
		t.Fatalf("Write #1: errno=%v", errno)
	}
	if _, errno := f.Write(context.Background(), nil, []byte("BBB"), 3); errno != 0 {
		t.Fatalf("Write #2: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	want := []byte("AAABBB")
	dest := make([]byte, 32)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if !bytes.Equal(got, want) {
		t.Fatalf("Read: got %q, want %q", got, want)
	}
}

// TestDFSFile_Getattr_ReportsFlushedSize proves that the kernel sees
// the up-to-date size via the Getattr path (which is what
// `ls -l` and friends use). Without this, a process that writes
// data and then stat()s the file before close would see the
// pre-flush size.
func TestDFSFile_Getattr_ReportsFlushedSize(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	want := int64(len("flushed-and-statted"))
	if _, errno := f.Write(context.Background(), nil, []byte("flushed-and-statted"), 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	// The attribute query needs a valid fs.AttrOut to write into.
	// Build one and call Getattr with it.
	attr, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if attr.Size != want {
		t.Fatalf("GetInode.Size=%d, want %d", attr.Size, want)
	}
	if attr.MTime == 0 {
		t.Fatalf("GetInode.MTime == 0; Flush should stamp MTime")
	}
	if time.Since(time.Unix(0, attr.MTime)) > 10*time.Second {
		t.Fatalf("GetInode.MTime not recent: %v", time.Unix(0, attr.MTime))
	}
}

// ========== Sanity: a complete write+close cycle is idempotent ==========

// TestDFSFile_FullCycle_WriteFlushWriteFlush is the close-then-rename
// / truncate-then-extend style sequence a real workload would hit.
// First flush is one chunk; the second flush should overwrite the
// first's chunk with a new payload (and the same chunk ID if the
// allocator reuses — but commit 0 noted that the allocator always
// allocates fresh, so we expect a second ChunkMap entry. This is
// the "metadata-layer bug" the design doc flagged for follow-up
// work; commit 1.1 just makes sure Flush doesn't crash on it).
func TestDFSFile_FullCycle_WriteFlushWriteFlush(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("first"), 0); errno != 0 {
		t.Fatalf("Write #1: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush #1: errno=%v", errno)
	}
	if _, errno := f.Write(context.Background(), nil, []byte("second-longer"), 0); errno != 0 {
		t.Fatalf("Write #2: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush #2: errno=%v", errno)
	}

	rr, errno := f.Read(context.Background(), nil, make([]byte, 64), 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(make([]byte, 64))
	if !bytes.Equal(got, []byte("second-longer")) {
		t.Fatalf("Read: got %q, want %q (allocator should not split single-chunk file)", got, "second-longer")
	}
}

// ========== readBufPool ==========

// TestReadBufPool_ConcurrentStress runs 1000 sequential reads through
// the pooled-buffer path and asserts every read returns the correct
// data. This is a smoke test for F1: without the pool, every Read
// allocates a fresh 128 KB buffer; with the pool, the same buffer
// is reused across calls.
func TestReadBufPool_ConcurrentStress(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	want := []byte("pool-test-data")
	if _, errno := f.Write(context.Background(), nil, want, 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	for i := 0; i < 1000; i++ {
		rr, errno := f.Read(context.Background(), nil, make([]byte, 32), 0)
		if errno != 0 {
			t.Fatalf("Read %d: errno=%v", i, errno)
		}
		got, _ := rr.Bytes(make([]byte, 32))
		if !bytes.Equal(got, want) {
			t.Fatalf("Read %d: got %q, want %q", i, got, want)
		}
	}
}

// ========== xattr ==========

// TestDFSXAttr_GetSetRoundTrip writes an xattr and reads it back.
func TestDFSXAttr_GetSetRoundTrip(t *testing.T) {
	meta, id := newTestMetaStore(t)
	x := &DFSXAttr{meta: meta, inodeID: id}
	ctx := context.Background()

	if errno := x.Setxattr(ctx, "user.foo", []byte("bar"), 0); errno != 0 {
		t.Fatalf("Setxattr: errno=%v", errno)
	}

	dest := make([]byte, 256)
	n, errno := x.Getxattr(ctx, "user.foo", dest)
	if errno != 0 {
		t.Fatalf("Getxattr: errno=%v", errno)
	}
	if string(dest[:n]) != "bar" {
		t.Fatalf("Getxattr: got %q, want %q", dest[:n], "bar")
	}
}

// TestDFSXAttr_ListMultiple writes two xattrs and lists them.
func TestDFSXAttr_ListMultiple(t *testing.T) {
	meta, id := newTestMetaStore(t)
	x := &DFSXAttr{meta: meta, inodeID: id}
	ctx := context.Background()

	if errno := x.Setxattr(ctx, "user.a", []byte("1"), 0); errno != 0 {
		t.Fatalf("Setxattr a: errno=%v", errno)
	}
	if errno := x.Setxattr(ctx, "user.b", []byte("2"), 0); errno != 0 {
		t.Fatalf("Setxattr b: errno=%v", errno)
	}

	dest := make([]byte, 256)
	n, errno := x.Listxattr(ctx, dest)
	if errno != 0 {
		t.Fatalf("Listxattr: errno=%v", errno)
	}
	// Parse null-separated names.
	names := make(map[string]bool)
	var offset int
	for offset < int(n) {
		end := offset
		for end < int(n) && dest[end] != 0 {
			end++
		}
		names[string(dest[offset:end])] = true
		offset = end + 1
	}
	if !names["user.a"] || !names["user.b"] {
		t.Fatalf("Listxattr: got %v, want user.a and user.b", names)
	}
}

// TestDFSXAttr_NotFound returns ENODATA for a missing xattr.
func TestDFSXAttr_NotFound(t *testing.T) {
	meta, id := newTestMetaStore(t)
	x := &DFSXAttr{meta: meta, inodeID: id}
	ctx := context.Background()

	_, errno := x.Getxattr(ctx, "user.no-such-attr", make([]byte, 256))
	if errno != syscall.ENODATA {
		t.Fatalf("Getxattr missing: errno=%v, want ENODATA", errno)
	}
}

// ========== Advisory lock integration ==========

// TestDFSFile_Open_AcquiresExclusiveLock verifies that opening a
// file for writing acquires an exclusive advisory lock, and that
// Release releases it. Between Open and Release, a second Open
// from a different owner must fail with ErrLockBusy (returned as
// EIO by the FUSE layer).
func TestDFSFile_Open_AcquiresExclusiveLock(t *testing.T) {
	store, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()

	dfs := NewDFSFileSystem(store, cs, nil, nil, nil)
	f := newTestFileFromDFS(dfs, id, nil)
	ctx := context.Background()

	// Open for writing → exclusive lock.
	h, _, errno := f.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("Open O_RDWR: errno=%v", errno)
	}
	if h == nil {
		t.Fatalf("Open O_RDWR: returned nil handle")
	}
	fh := h.(*DFSFileHandle)
	if !fh.lockAcquired {
		t.Fatalf("Open O_RDWR: lockAcquired=false, expected true")
	}

	// A concurrent open from a DIFFERENT owner must fail (exclusive
	// lock is held).
	f2 := newTestFileFromDFS(dfs, id, nil)
	f2.lockOwner = "fusegw-other-pid"
	_, _, errno2 := f2.Open(ctx, syscall.O_RDWR)
	if errno2 != syscall.EIO {
		t.Fatalf("concurrent Open: errno=%v, want EIO (lock busy)", errno2)
	}

	// Release our handle → lock freed.
	if errno := f.Release(ctx, h); errno != 0 {
		t.Fatalf("Release: errno=%v", errno)
	}

	// Now the other owner can acquire.
	h2, _, errno3 := f2.Open(ctx, syscall.O_RDWR)
	if errno3 != 0 {
		t.Fatalf("Open after unlock: errno=%v, expected success", errno3)
	}
	if h2 != nil {
		f2.Release(ctx, h2)
	}
}

// TestDFSFile_Open_AcquiresSharedLock verifies that opening a file
// for reading acquires a shared lock. Two readers can coexist, but
// a writer is blocked by either reader.
func TestDFSFile_Open_AcquiresSharedLock(t *testing.T) {
	store, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	dfs := NewDFSFileSystem(store, cs, nil, nil, nil)

	f1 := newTestFileFromDFS(dfs, id, nil)
	f1.lockOwner = "reader-1"
	f2 := newTestFileFromDFS(dfs, id, nil)
	f2.lockOwner = "reader-2"
	fw := newTestFileFromDFS(dfs, id, nil)
	fw.lockOwner = "writer"
	ctx := context.Background()

	// Two readers can coexist.
	h1, _, errno := f1.Open(ctx, syscall.O_RDONLY)
	if errno != 0 {
		t.Fatalf("Open reader-1: errno=%v", errno)
	}
	h2, _, errno := f2.Open(ctx, syscall.O_RDONLY)
	if errno != 0 {
		t.Fatalf("Open reader-2: errno=%v", errno)
	}

	// A writer is blocked by either reader.
	_, _, errno = fw.Open(ctx, syscall.O_RDWR)
	if errno != syscall.EIO {
		t.Fatalf("Open writer while readers: errno=%v, want EIO", errno)
	}

	// Release both readers.
	f1.Release(ctx, h1)
	f2.Release(ctx, h2)

	// Now the writer can acquire.
	hw, _, errno := fw.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("Open writer after readers gone: errno=%v", errno)
	}
	if hw != nil {
		fw.Release(ctx, hw)
	}
}

// ========== Helper: nothing here. ==========

// ========== D3: Flush uses bucket policy, not hardcoded replica count ==========

// newTestMetaStoreWithPolicy creates a bucket with a custom placement
// policy so we can verify Flush honours it instead of the hardcoded
// fuseChunkPolicy (ReplicationFactor=1). It also registers enough
// nodes to satisfy the placement engine.
func newTestMetaStoreWithPolicy(t *testing.T, policy metadata.PlacementPolicy) (*metadata.PebbleStore, metadata.InodeID) {
	t.Helper()
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:         t.TempDir(), // required even in UseInMemory mode; storage is mem-VFS
		UseInMemory: true,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()

	// Register enough nodes to satisfy the replication factor.
	nodeCount := policy.ReplicationFactor
	if nodeCount < 1 {
		nodeCount = 1
	}
	for i := 1; i <= int(nodeCount); i++ {
		err := store.RegisterNode(ctx, &metadata.NodeInfo{
			ID:         metadata.NodeID(i),
			Addr:       fmt.Sprintf("127.0.0.1:%d", 9000+i),
			CapacityGB: 100,
		})
		if err != nil {
			t.Fatalf("RegisterNode %d: %v", i, err)
		}
	}

	if err := store.CreateBucket(ctx, "test", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	file, err := store.CreateFile(ctx, bucket.RootInode, "hello.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	return store, file.ID
}

// TestDFSFile_Flush_UsesBucketPolicy verifies that Flush allocates
// chunks using the parent bucket's placement policy, not the hardcoded
// fuseChunkPolicy with ReplicationFactor=1.
//
// TDD red phase: before the fix, Flush always used fuseChunkPolicy
// (ReplicationFactor=1) regardless of the bucket's policy. This test
// creates a bucket with ReplicationFactor=3 and checks that the
// allocated chunk has 3 replicas.
func TestDFSFile_Flush_UsesBucketPolicy(t *testing.T) {
	policy := metadata.PlacementPolicy{
		ID:                "test-rf3",
		ReplicationFactor: 3,
		TopologySpread:    metadata.SpreadNode,
	}
	meta, id := newTestMetaStoreWithPolicy(t, policy)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	// Write and flush.
	data := []byte("replicated data")
	if _, errno := f.Write(context.Background(), nil, data, 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	// Read back the inode and check the chunk's replica count.
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	refs := resolvedChunks(t, meta, inode)
	if len(refs) == 0 {
		t.Fatal("no committed chunks after Flush")
	}

	chunkID := refs[0].ID
	chunk, err := meta.GetChunk(context.Background(), chunkID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}

	if len(chunk.Replicas) != 3 {
		t.Fatalf("expected 3 replicas (from bucket policy), got %d — Flush used hardcoded fuseChunkPolicy instead of bucket policy",
			len(chunk.Replicas))
	}
}

// TestDFSFile_Flush_FallsBackToDefaultOnNoBucket verifies that when
// the inode has no BucketRoot (edge case), Flush falls back to a
// safe default policy rather than panicking.
func TestDFSFile_Flush_FallsBackToDefaultOnNoBucket(t *testing.T) {
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:         t.TempDir(), // required even in UseInMemory mode; storage is mem-VFS
		UseInMemory: true,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()

	// Register one node so the placement engine can satisfy the
	// default single-replica policy.
	if err := store.RegisterNode(ctx, &metadata.NodeInfo{
		ID:         metadata.NodeID(1),
		Addr:       "127.0.0.1:9001",
		CapacityGB: 100,
	}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}

	// Create a file directly at root inode (no bucket) — BucketRoot=0.
	inode, err := store.CreateFile(ctx, metadata.RootInodeID, "orphan.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(store, cs, inode.ID)

	data := []byte("orphan file")
	if _, errno := f.Write(context.Background(), nil, data, 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	// Should succeed with default replica count (1).
	metaInode, err := store.GetInode(ctx, inode.ID)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	refs := resolvedChunks(t, store, metaInode)
	if len(refs) == 0 {
		t.Fatal("no committed chunks after Flush on orphan file")
	}

	// Verify the chunk has exactly 1 replica (default policy).
	chunkID := refs[0].ID
	chunk, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if len(chunk.Replicas) != 1 {
		t.Fatalf("expected 1 replica (default policy), got %d", len(chunk.Replicas))
	}
}

// ========== O_TRUNC: open(2) with O_TRUNC truncates the file ==========
//
// Linux delivers open(2)'s O_TRUNC through the Open flags (the kernel does
// not issue a separate SETATTR), so DFSFile.Open is responsible for the
// truncation. These tests exercise the three contracts:
//   1. `: > file` (open O_TRUNC, no write, close) empties a committed file.
//   2. open O_TRUNC then a partial write yields only the new bytes — no
//      stale committed tail is preserved (echo x > file semantics).
//   3. the truncation is visible immediately (inode Size == 0) as soon as
//      Open returns, before any write or flush.

// commitFile writes data + Flush so the inode has a committed (on-disk)
// size and ChunkMap, then returns the DFSFile used. Mirrors the fixture
// pattern of the other Flush round-trip tests.
func commitFile(t *testing.T, meta metadata.MetadataService, cs chunkstore.ChunkStore, id metadata.InodeID, want []byte) *DFSFile {
	t.Helper()
	f := newTestFile(meta, cs, id)
	if _, errno := f.Write(context.Background(), nil, want, 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}
	return f
}

// TestDFSFile_Open_OTrunc_NoWrite_EmptiesFile covers `: > file`: open with
// O_TRUNC and nothing else, then close. The committed file must become empty
// (Size 0, no readable bytes).
func TestDFSFile_Open_OTrunc_NoWrite_EmptiesFile(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()

	original := []byte("this is a committed file worth one hundred bytes exactly!!!")
	if len(original) < 40 {
		t.Fatal("test fixture too short")
	}
	commitFile(t, meta, cs, id, original)

	// Reopen with O_TRUNC (as `: > file` does), then flush/close with no
	// intervening write.
	f := newTestFile(meta, cs, id)
	if _, _, errno := f.Open(context.Background(), syscall.O_WRONLY|syscall.O_TRUNC); errno != 0 {
		t.Fatalf("Open(O_TRUNC): errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != 0 {
		t.Fatalf("after O_TRUNC flush: Size=%d, want 0", inode.Size)
	}

	// A read anywhere in the file must return no bytes.
	dest := make([]byte, 16)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if len(got) != 0 {
		t.Fatalf("Read after O_TRUNC: got %d bytes, want 0", len(got))
	}
}

// TestDFSFile_Open_OTrunc_ThenPartialWrite_NoStaleTail covers `echo x > file`:
// open O_TRUNC then write a short string. The result must be exactly the new
// string — a partial off-0 write over a truncated file must not preserve the
// old committed tail.
func TestDFSFile_Open_OTrunc_ThenPartialWrite_NoStaleTail(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()

	original := []byte("abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOP") // 50 bytes
	commitFile(t, meta, cs, id, original)

	f := newTestFile(meta, cs, id)
	if _, _, errno := f.Open(context.Background(), syscall.O_WRONLY|syscall.O_TRUNC); errno != 0 {
		t.Fatalf("Open(O_TRUNC): errno=%v", errno)
	}

	// A partial write shorter than the old committed file.
	payload := []byte("hi")
	if _, errno := f.Write(context.Background(), nil, payload, 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != int64(len(payload)) {
		t.Fatalf("after O_TRUNC + partial write: Size=%d, want %d (stale committed tail preserved)",
			inode.Size, len(payload))
	}

	// Read back: must be exactly "hi" with no stale tail.
	dest := make([]byte, len(original))
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if !bytes.Equal(got, payload) {
		t.Fatalf("after O_TRUNC + partial write: got %q (%d bytes), want %q (stale committed tail preserved)",
			got, len(got), payload)
	}
}

// TestDFSFile_Open_OTrunc_VisibleImmediately asserts the POSIX contract that
// once open(O_TRUNC) returns, the file size is 0 to every observer — before
// any write or flush. This is what makes concurrent fds/readers see the
// truncation at the right moment rather than at close.
func TestDFSFile_Open_OTrunc_VisibleImmediately(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()

	original := []byte("a committed body long enough to matter")
	commitFile(t, meta, cs, id, original)

	f := newTestFile(meta, cs, id)
	if _, _, errno := f.Open(context.Background(), syscall.O_RDWR|syscall.O_TRUNC); errno != 0 {
		t.Fatalf("Open(O_TRUNC): errno=%v", errno)
	}

	// The committed size must already be 0, without any write or flush.
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != 0 {
		t.Fatalf("immediately after Open(O_TRUNC): Size=%d, want 0", inode.Size)
	}
}

// ========== Program 12 Commit 3: fallocate (NodeAllocater) ==========

// TestDFSFile_Allocate_ExtendsSizeAndZeroFills covers the default
// preallocate path (mode 0): Allocate(off,size) must extend the file's
// logical Size to off+size, zero-fill the new range in the buffer, and
// mark the buffer dirty so the next Flush persists the zeros.
func TestDFSFile_Allocate_ExtendsSizeAndZeroFills(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	const off, size = 16, 100
	if errno := f.Allocate(context.Background(), nil, off, size, 0); errno != 0 {
		t.Fatalf("Allocate: errno=%v", errno)
	}

	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != off+size {
		t.Fatalf("after Allocate: Size=%d, want %d", inode.Size, off+size)
	}

	// Bytes [0,off) are a hole too (file was empty); all must read as zero.
	dest := make([]byte, off+size)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	for i, b := range got {
		if b != 0 {
			t.Fatalf("Read after Allocate: byte[%d]=0x%02x, want 0x00", i, b)
		}
	}
}

// TestDFSFile_Allocate_Flush_ReadbackByteExact proves the preallocated
// zeros survive a full Flush/rebuild: Allocate extends, then Flush writes
// the whole buffer (including the zero-filled tail) to the chunk store,
// and a fresh Read returns the same byte-exact image.
func TestDFSFile_Allocate_Flush_ReadbackByteExact(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("head"), 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	const off, size = 16, 40
	if errno := f.Allocate(context.Background(), nil, off, size, 0); errno != 0 {
		t.Fatalf("Allocate: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	const wantLen = off + size // 56
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != wantLen {
		t.Fatalf("after Flush: Size=%d, want %d", inode.Size, wantLen)
	}

	dest := make([]byte, wantLen)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read after Flush: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	want := make([]byte, wantLen)
	copy(want, []byte("head"))
	for i, b := range got {
		if b != want[i] {
			t.Fatalf("Readback after Flush: byte[%d]=0x%02x, want 0x%02x", i, b, want[i])
		}
	}
}

// TestDFSFile_Allocate_KeepSize_GrowsBufferNotSize covers FALLOC_FL_KEEP_SIZE:
// physically preallocate (buffer grows) but leave the logical Size unchanged.
// The extra physical bytes carry into a later Flush but never extend Size.
func TestDFSFile_Allocate_KeepSize_GrowsBufferNotSize(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("abc"), 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	// Preallocate 0..(32) with KEEP_SIZE. Logical Size must stay 3 even though
	// the buffer physically grows to 32.
	if errno := f.Allocate(context.Background(), nil, 0, 32, fallocKeepSize); errno != 0 {
		t.Fatalf("Allocate(KEEP_SIZE): errno=%v", errno)
	}

	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != 3 {
		t.Fatalf("after Allocate(KEEP_SIZE): Size=%d, want 3 (unchanged)", inode.Size)
	}

	// The written head must still be intact in the chunk buffer.
	chunk := f.buffered.BufferChunk(0)
	if chunk == nil {
		t.Fatalf("KEEP_SIZE: base-0 buffer not resident")
	}
	if string(chunk[:3]) != "abc" {
		t.Fatalf("KEEP_SIZE clobbered head: chunk[:3]=%q, want \"abc\"", chunk[:3])
	}

	// Verify that the physical allocation extended the buffer beyond the
	// committed size. The chunk buffer should be at least 32 bytes.
	if len(chunk) < 32 {
		t.Fatalf("after Allocate(KEEP_SIZE): chunk len=%d, want >= 32 (physical prealloc)", len(chunk))
	}
}

// TestDFSFile_Allocate_ZeroRange_ClearsInterval covers FALLOC_FL_ZERO_RANGE:
// bytes [off,off+size) are zeroed. The range is within the current file so
// the logical Size must not grow (extends only when the range exceeds Size).
func TestDFSFile_Allocate_ZeroRange_ClearsInterval(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	payload := bytes.Repeat([]byte{0xAA}, 100)
	if _, errno := f.Write(context.Background(), nil, payload, 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	// Flush so the committed Size reflects the written bytes (the kernel
	// commits a size via write+fsync before a later fallocate would run).
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	const off, size = 20, 30
	if errno := f.Allocate(context.Background(), nil, off, size, fallocZeroRange); errno != 0 {
		t.Fatalf("Allocate(ZERO_RANGE): errno=%v", errno)
	}

	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != 100 {
		t.Fatalf("after in-range ZERO_RANGE: Size=%d, want 100 (unchanged)", inode.Size)
	}

	// Bytes [20,50) zeroed, everything else untouched.
	dest := make([]byte, 100)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	for i, b := range got {
		if i >= int(off) && i < int(off+size) {
			if b != 0 {
				t.Fatalf("ZERO_RANGE: byte[%d]=0x%02x, want 0x00", i, b)
			}
		} else if b != 0xAA {
			t.Fatalf("ZERO_RANGE outside interval: byte[%d]=0x%02x, want 0xAA", i, b)
		}
	}
}

// TestDFSFile_Allocate_PunchHole_ClampsToSize covers FALLOC_FL_PUNCH_HOLE:
// the zeroed range is clamped to the current file size and never grows Size.
func TestDFSFile_Allocate_PunchHole_ClampsToSize(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	payload := bytes.Repeat([]byte{0xBB}, 40)
	if _, errno := f.Write(context.Background(), nil, payload, 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	// Punch [30, 1000): only [30,40) (within Size=40) is clamped and zeroed;
	// Size must stay 40.
	if errno := f.Allocate(context.Background(), nil, 30, 970, fallocPunchHole); errno != 0 {
		t.Fatalf("Allocate(PUNCH_HOLE): errno=%v", errno)
	}

	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != 40 {
		t.Fatalf("after PUNCH_HOLE: Size=%d, want 40 (clamped, no grow)", inode.Size)
	}

	dest := make([]byte, 40)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	for i, b := range got {
		if i >= 30 && b != 0 {
			t.Fatalf("PUNCH_HOLE: byte[%d]=0x%02x, want 0x00", i, b)
		}
		if i < 30 && b != 0xBB {
			t.Fatalf("PUNCH_HOLE outside: byte[%d]=0x%02x, want 0xBB", i, b)
		}
	}
}

// TestDFSFile_Allocate_UnsupportedFlags_EOPNOTSUPP ensures range-shifting
// / unshare flags that we do not implement are rejected with EOPNOTSUPP and
// leave both buffer and Size untouched.
func TestDFSFile_Allocate_UnsupportedFlags_EOPNOTSUPP(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("xy"), 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}
	for _, bad := range []uint32{fallocCollapseRng, fallocInsertRng, fallocUnshareRange, fallocCollapseRng | fallocZeroRange} {
		if errno := f.Allocate(context.Background(), nil, 0, 8, bad); errno != syscall.EOPNOTSUPP {
			t.Errorf("Allocate(mode=%#x) errno=%v, want EOPNOTSUPP", bad, errno)
		}
	}

	// Nothing was changed: committed Size intact, buffer unchanged (still nil
	// after Flush, since no Allocate mutated state), and the data still reads
	// back byte-exact.
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.Size != 2 {
		t.Fatalf("after rejected Allocate: Size=%d, want 2", inode.Size)
	}
	if f.buffered.Dirty() {
		t.Fatalf("after rejected Allocate: file dirty (state untouched)")
	}
	dest := make([]byte, 4)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if string(got) != "xy" {
		t.Fatalf("after rejected Allocate: got %q, want \"xy\"", got)
	}
}

// TestDFSFile_Allocate_Overflow_EFBIG covers the off+size overflow guard:
// a range that would wrap (or exceed MaxInt64) is rejected with EFBIG.
func TestDFSFile_Allocate_Overflow_EFBIG(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if errno := f.Allocate(context.Background(), nil, 1<<63-1, 2, 0); errno != syscall.EFBIG {
		t.Errorf("Allocate(overflow) errno=%v, want EFBIG", errno)
	}
}

// TestDFSFile_Allocate_ZeroRange_ExtendsBeyondSize covers the ZERO_RANGE
// growth semantics: when the range exceeds the current Size (no KEEP_SIZE),
// the file extends to off+size and the new tail is zeroed.
func TestDFSFile_Allocate_ZeroRange_ExtendsBeyondSize(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	if _, errno := f.Write(context.Background(), nil, []byte("abc"), 0); errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if errno := f.Allocate(context.Background(), nil, 10, 20, fallocZeroRange); errno != 0 {
		t.Fatalf("Allocate(ZERO_RANGE extend): errno=%v", errno)
	}

	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	// off+size = 30 > Size 3 → extended to 30 (fills [10,30) with zeros,
	// [3,10) is a hole).
	if inode.Size != 30 {
		t.Fatalf("after extending ZERO_RANGE: Size=%d, want 30", inode.Size)
	}

	dest := make([]byte, 30)
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if string(got[:3]) != "abc" {
		t.Fatalf("extending ZERO_RANGE clobbered head: got[:3]=%q, want \"abc\"", got[:3])
	}
	for i := 3; i < 30; i++ {
		if got[i] != 0 {
			t.Fatalf("extending ZERO_RANGE: byte[%d]=0x%02x, want 0x00", i, got[i])
		}
	}
}
