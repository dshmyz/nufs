//go:build linux

package fuse

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/example/dfs/chunkstore"
	"github.com/example/dfs/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// ========== Test fixtures ==========

// newTestMetaStore returns an in-memory PebbleStore wired with a
// single pre-created bucket "test" and a pre-created file at the
// bucket root. The returned inode ID is what the file uses.
func newTestMetaStore(t *testing.T) (*metadata.PebbleStore, metadata.InodeID) {
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

// newTestFile returns a DFSFile backed by the given meta+chunkStore,
// with a pre-set inodeID. The test does not have a real FUSE server
// so the fs.Inode embedded field stays zero-value; we exercise the
// Read/Write/Flush methods directly.
func newTestFile(meta metadata.MetadataService, cs chunkstore.ChunkStore, id metadata.InodeID) *DFSFile {
	return &DFSFile{
		meta:       meta,
		chunkStore: cs,
		inodeID:    id,
	}
}

// newTestFileWithRecorder 同 newTestFile 但注入 MetricsRecorder。
func newTestFileWithRecorder(meta metadata.MetadataService, cs chunkstore.ChunkStore, id metadata.InodeID, rec MetricsRecorder) *DFSFile {
	return &DFSFile{
		meta:       meta,
		chunkStore: cs,
		inodeID:    id,
		recorder:   rec,
	}
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
	if len(inode.ChunkMap) == 0 {
		t.Fatalf("after Flush: ChunkMap is empty (B2 regression: flush didn't allocate)")
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
	if len(inode.ChunkMap) != 1 {
		t.Fatalf("ChunkMap len=%d, want 1", len(inode.ChunkMap))
	}
	chunkID := inode.ChunkMap[0].ID
	if chunkID == 0 {
		t.Fatalf("ChunkMap[0].ID == 0; allocator should produce a non-zero ID")
	}
	if inode.ChunkMap[0].Length != int32(len(want)) {
		t.Fatalf("ChunkMap[0].Length=%d, want %d", inode.ChunkMap[0].Length, len(want))
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
	if len(inode.ChunkMap) == 0 {
		t.Fatalf("ChunkMap empty")
	}
	chunk, err := meta.GetChunk(context.Background(), inode.ChunkMap[0].ID)
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
	chunkCount := len(inode1.ChunkMap)
	size1 := inode1.Size

	// Second flush without further writes should be a no-op.
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush #2: errno=%v (B3 regression: not idempotent)", errno)
	}

	inode2, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode after flush #2: %v", err)
	}
	if len(inode2.ChunkMap) != chunkCount {
		t.Fatalf("after flush #2: ChunkMap len=%d, want %d (B3 regression: re-allocated)", len(inode2.ChunkMap), chunkCount)
	}
	if inode2.Size != size1 {
		t.Fatalf("after flush #2: Size=%d, want %d (B3 regression: re-sized)", inode2.Size, size1)
	}

	// Internal state checks: buffer should be nil, dirty should be
	// false. These are implementation details but they're the
	// direct fix for the B3 bug; locking them in here documents
	// the contract.
	if f.buffer != nil {
		t.Fatalf("after flush: buffer=%v, want nil (B3 fix)", f.buffer)
	}
	if f.dirty {
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
// than MaxChunkPayload is split into multiple chunks and the inode's
// ChunkMap holds one ref per MaxChunkPayload window (Program 11).
func TestDFSFile_Flush_Oversized_AllocatesMultiChunk(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	// Directly seed the in-memory buffer past the chunk limit, as the
	// old EFBIG test did, so we don't copy ~128MiB through Write.
	size := 2*MaxChunkPayload + 123
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(i % 251)
	}
	f.mu.Lock()
	f.buffer = buf
	f.dirty = true
	f.mu.Unlock()

	errno := f.Flush(context.Background(), nil)
	if errno != 0 {
		t.Fatalf("Flush oversized: errno=%v, want 0 (multi-chunk)", errno)
	}

	// The inode must now reference ceil(size/chunk) = 3 chunks.
	inode, err := meta.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if len(inode.ChunkMap) != 3 {
		t.Fatalf("after oversized flush: ChunkMap len=%d, want 3", len(inode.ChunkMap))
	}
	if inode.Size != int64(size) {
		t.Fatalf("after oversized flush: Size=%d, want %d", inode.Size, size)
	}
	// Each ref is sealed, holds the right offset/length window, and
	// read-back is byte-exact across the whole file.
	for i, cref := range inode.ChunkMap {
		wantOff := int64(i) * MaxChunkPayload
		if cref.Offset != wantOff {
			t.Fatalf("ChunkMap[%d].Offset=%d, want %d", i, cref.Offset, wantOff)
		}
		wantLen := MaxChunkPayload
		if i == 2 {
			wantLen = size - 2*MaxChunkPayload
		}
		if int(cref.Length) != wantLen {
			t.Fatalf("ChunkMap[%d].Length=%d, want %d", i, cref.Length, wantLen)
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
// the plain multi-chunk path: a payload spanning 2*MaxChunkPayload+123
// bytes (3 chunk windows) flushes into 3 sealed chunks at aligned
// offsets, and Read reconstructs the whole file byte-exact across them.
func TestDFSFile_Flush_MultiChunk_ReadBack(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	size := 2*MaxChunkPayload + 123
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
	if len(inode.ChunkMap) != 3 {
		t.Fatalf("ChunkMap len=%d, want 3", len(inode.ChunkMap))
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
	size1 := MaxChunkPayload + 50
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
	if len(before.ChunkMap) != 2 {
		t.Fatalf("before overwrite: ChunkMap len=%d, want 2", len(before.ChunkMap))
	}
	oldIDs := make(map[metadata.ChunkID]bool, len(before.ChunkMap))
	for _, cref := range before.ChunkMap {
		oldIDs[cref.ID] = true
	}

	// Second flush: overwrite a chunk-worth of data and extend the file.
	size2 := MaxChunkPayload + 300
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
	if len(after.ChunkMap) != 2 {
		t.Fatalf("after overwrite: ChunkMap len=%d, want 2", len(after.ChunkMap))
	}
	for _, cref := range after.ChunkMap {
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

// TestDFSFile_Append_Concurrent_NoCollision verifies that two append
// writes to the same inode land back-to-back rather than at the same
// offset (the serialization point is the per-inode f.mu in AppendWrite).
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
	if f.buffer != nil {
		t.Fatalf("after Flush buffer should be nil, got %q", f.buffer)
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

// ========== Root inode: bucket-as-shared-root ==========

// TestDFSFileSystem_Readdir_ListsBuckets is the "ls /mnt/dfs" path.
// The root inode (RootInodeID) must list every bucket as a directory
// entry with its RootInode as the inode number. Pre-fix, the root
// was a bare fs.Inode with no Readdir/Lookup, so "ls /mnt/dfs"
// returned ENOTDIR.
func TestDFSFileSystem_Readdir_ListsBuckets(t *testing.T) {
	store, _ := newTestMetaStore(t)
	dfs := NewDFSFileSystem(store, chunkstore.NewMemoryChunkStore(), nil, nil, nil)
	ctx := context.Background()

	// Create a second bucket so we have two entries.
	if err := store.CreateBucket(ctx, "another", metadata.PlacementPolicy{
		ID: "another", ReplicationFactor: 1, TopologySpread: metadata.SpreadNode,
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	ds, errno := dfs.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("Readdir: errno=%v", errno)
	}
	var names []string
	for ds.HasNext() {
		entry, _ := ds.Next()
		names = append(names, entry.Name)
	}
	if len(names) != 2 {
		t.Fatalf("Readdir: got %d entries %v, want 2", len(names), names)
	}
	// Entries should include both "another" and "test".
	found := map[string]bool{}
	for _, n := range names {
		found[n] = true
	}
	if !found["test"] || !found["another"] {
		t.Fatalf("Readdir: missing expected buckets, got %v", names)
	}
}

// TestDFSFileSystem_Lookup_ExistingBucket resolves "test" to a
// DFSDir with inodeID == bucket.RootInode. The returned *fs.Inode
// must have StableAttr.Ino equal to the bucket's RootInode so the
// kernel caches it correctly.
func TestDFSFileSystem_Lookup_ExistingBucket(t *testing.T) {
	store, _ := newTestMetaStore(t)
	dfs := NewDFSFileSystem(store, chunkstore.NewMemoryChunkStore(), nil, nil, nil)
	ctx := context.Background()

	bucket, err := store.GetBucket(ctx, "test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	var out fuse.EntryOut
	inode, errno := dfs.Lookup(ctx, "test", &out)
	if errno != 0 {
		t.Fatalf("Lookup: errno=%v", errno)
	}
	if inode == nil {
		t.Fatalf("Lookup: returned nil inode for existing bucket")
	}
	if out.Attr.Ino != uint64(bucket.RootInode) {
		t.Fatalf("Lookup: Ino=%d, want %d (bucket.RootInode)", out.Attr.Ino, bucket.RootInode)
	}
}

// TestDFSFileSystem_Lookup_MissingBucket returns ENOENT for a bucket
// that doesn't exist.
func TestDFSFileSystem_Lookup_MissingBucket(t *testing.T) {
	store, _ := newTestMetaStore(t)
	dfs := NewDFSFileSystem(store, chunkstore.NewMemoryChunkStore(), nil, nil, nil)
	ctx := context.Background()

	var out fuse.EntryOut
	_, errno := dfs.Lookup(ctx, "no-such-bucket", &out)
	if errno != syscall.ENOENT {
		t.Fatalf("Lookup missing bucket: errno=%v, want ENOENT", errno)
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
	f := &DFSFile{
		meta:       store,
		chunkStore: cs,
		inodeID:    id,
		lockOwner:  dfs.lockOwner,
	}
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
	f2 := &DFSFile{
		meta:       store,
		chunkStore: cs,
		inodeID:    id,
		lockOwner:  "fusegw-other-pid",
	}
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

	f1 := &DFSFile{meta: store, chunkStore: cs, inodeID: id, lockOwner: "reader-1"}
	f2 := &DFSFile{meta: store, chunkStore: cs, inodeID: id, lockOwner: "reader-2"}
	fw := &DFSFile{meta: store, chunkStore: cs, inodeID: id, lockOwner: "writer"}
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
	if len(inode.ChunkMap) == 0 {
		t.Fatal("ChunkMap is empty after Flush")
	}

	chunkID := inode.ChunkMap[0].ID
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
	if len(metaInode.ChunkMap) == 0 {
		t.Fatal("ChunkMap is empty after Flush on orphan file")
	}

	// Verify the chunk has exactly 1 replica (default policy).
	chunkID := metaInode.ChunkMap[0].ID
	chunk, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if len(chunk.Replicas) != 1 {
		t.Fatalf("expected 1 replica (default policy), got %d", len(chunk.Replicas))
	}
}
