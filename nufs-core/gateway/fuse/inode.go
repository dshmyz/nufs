//go:build linux

package fuse

import (
	"context"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/example/dfs/chunkstore"
	"github.com/example/dfs/metadata"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// readBufPool reuses 128 KB buffers for Read calls. The go-fuse
// framework copies the returned []byte into kernel space before
// returning from the read(2) syscall, so it is safe to return the
// same pooled buffer to the pool after the call. This avoids a
// fresh 128 KB heap allocation on every Read that crosses a chunk
// boundary.
var readBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 128*1024)
		return &buf
	},
}

// ========== DFSFile: regular file inode ==========

// MaxChunkPayload is the largest single-chunk payload we hand to
// ChunkStore.WriteChunk. Flush is multi-chunk (Program 11): a file
// larger than this is split into one chunk per MaxChunkPayload window
// and the inode's ChunkMap holds one ref per window.
const MaxChunkPayload = 64 * 1024 * 1024 // 64 MiB

// fuseDefaultPolicy is the fallback placement policy used when Flush
// cannot determine the parent bucket's policy (e.g., the inode has no
// BucketRoot). This is a safe single-replica default for orphan files.
var fuseDefaultPolicy = metadata.PlacementPolicy{
	ID:                "fuse-default",
	ReplicationFactor: 1,
	TopologySpread:    metadata.SpreadNode,
}

// resolveChunkPolicy looks up the placement policy for the file's
// containing bucket. It uses the inode's BucketRoot field to look up
// the bucket directly via GetBucketByRoot (reverse index), avoiding
// a full ListBuckets scan. If the inode has no BucketRoot (orphan
// file) or the bucket cannot be found, it falls back to
// fuseDefaultPolicy.
//
// This fixes D3: previously Flush used a hardcoded fuseChunkPolicy
// with ReplicationFactor=1, ignoring the bucket's configured policy.
// Now a bucket with ReplicationFactor=3 will actually get 3 replicas.
//
// P1.5: replaced ListBuckets + linear scan with O(1) GetBucketByRoot.
func (f *DFSFile) resolveChunkPolicy(ctx context.Context, inode *metadata.InodeMeta) metadata.PlacementPolicy {
	if inode.BucketRoot == 0 {
		return fuseDefaultPolicy
	}
	var bucket *metadata.BucketInfo
	if err := f.reliability.DoMeta("flush", func() error {
		var gerr error
		bucket, gerr = f.meta.GetBucketByRoot(ctx, inode.BucketRoot)
		return gerr
	}); err != nil {
		logf("flush: get bucket by root %d for policy lookup: %v — using default", inode.BucketRoot, err)
		return fuseDefaultPolicy
	}
	return bucket.Policy
}

// DFSFile represents a regular file in the DFS FUSE filesystem.
type DFSFile struct {
	fs.Inode

	meta       metadata.MetadataService
	chunkStore chunkstore.ChunkStore
	cache      *ChunkCache
	inodeID    metadata.InodeID

	// lockOwner is the per-process owner string used when acquiring
	// advisory file locks; see commit 0 (metadata: add advisory
	// file lock service). The Open call fills it in so Release /
	// Flush know what to unlock. Empty means "do not take locks"
	// — used in unit tests that have no lock manager.
	lockOwner string

	// recorder 记录 FUSE 操作指标。nil 时不打点（兼容旧测试）。
	recorder MetricsRecorder

	// reliability 包装 retry + circuit breaker + 路径锁。
	// nil 时为 passthrough 模式（直接调用 fn），兼容旧测试。
	reliability *ReliabilityWrapper

	// Write buffer for small writes before flush
	mu     sync.Mutex
	dirty  bool
	buffer []byte

	// loaded reports whether buffer, when non-nil, holds a faithful image
	// of the file's *committed* prefix [0, committedSize). It is false right
	// after a Flush (buffer reset) and true once the buffer has either been
	// hydrated from the chunk store or fully overwritten from offset 0.
	// Guards against a write/append at a nonzero offset rebuilding the whole
	// file from an empty buffer and zeroing the committed prefix.
	loaded bool
}

var _ = (fs.NodeOpener)((*DFSFile)(nil))
var _ = (fs.NodeReader)((*DFSFile)(nil))
var _ = (fs.NodeWriter)((*DFSFile)(nil))
var _ = (fs.NodeGetattrer)((*DFSFile)(nil))
var _ = (fs.NodeSetattrer)((*DFSFile)(nil))
var _ = (fs.NodeFsyncer)((*DFSFile)(nil))
var _ = (fs.NodeFlusher)((*DFSFile)(nil))
var _ = (fs.NodeReleaser)((*DFSFile)(nil))
var _ = (fs.NodeAccesser)((*DFSFile)(nil))

// DFSFileHandle wraps DFSFile for per-open-file state.
type DFSFileHandle struct {
	file *DFSFile
	// append is true when the file was opened with O_APPEND. Every write
	// through this handle is then placed at the file's current end
	// (O_APPEND semantics) rather than at the fd's file position.
	append bool
	// lockAcquired is true when Open acquired an advisory lock on
	// this file descriptor. Release must call AdvisoryUnlock exactly
	// once per acquisition (POSIX-flock semantics).
	lockAcquired bool
}

// Open opens the file.
func (f *DFSFile) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	rec := recorderFor(f.recorder)
	rec.IncOp("open")

	h := &DFSFileHandle{file: f, append: flags&syscall.O_APPEND != 0}

	// Acquire an advisory file lock.  O_WRONLY|O_RDWR → exclusive;
	// O_RDONLY → shared.  An empty lockOwner (unit tests that supply
	// no lock manager) skips locking entirely.
	if f.lockOwner != "" && f.meta != nil {
		isWrite := (flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0)
		if isWrite {
			if err := f.meta.AdvisoryLock(ctx, f.inodeID, f.lockOwner); err != nil {
				logf("open: advisory lock %d: %v", f.inodeID, err)
				rec.IncOpError("open")
				return nil, 0, syscall.EIO
			}
		} else {
			if err := f.meta.AdvisoryLockShared(ctx, f.inodeID, f.lockOwner); err != nil {
				logf("open: advisory shared lock %d: %v", f.inodeID, err)
				rec.IncOpError("open")
				return nil, 0, syscall.EIO
			}
		}
		h.lockAcquired = true
	}

	return h, 0, 0
}

// Read reads data from the file. The returned window is the merged view of
// the committed chunk store and any un-flushed buffered writes: a buffered
// (dirty) region is authoritative over the committed bytes it overlaps, so a
// read immediately after a write (before Flush) sees the new data rather than
// stale committed content. (Same-class fix as the buffer hydration work.)
func (f *DFSFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	rec := recorderFor(f.recorder)
	rec.IncOp("read")

	var metaInode *metadata.InodeMeta
	if err := f.reliability.DoMeta("read", func() error {
		var gerr error
		metaInode, gerr = f.meta.GetInode(ctx, f.inodeID)
		return gerr
	}); err != nil {
		rec.IncOpError("read")
		return nil, syscall.EIO
	}

	// f.mu guards the dirty buffer. A read races a concurrent Write/Flush
	// on f.buffer otherwise, so the whole window resolution runs under it.
	f.mu.Lock()
	defer f.mu.Unlock()

	// Effective file size accounts for an un-flushed buffered tail that
	// extends past the committed size (e.g. after an O_APPEND write). A read
	// must be able to see bytes a process wrote but hasn't fsync/flushed.
	size := metaInode.Size
	if int64(len(f.buffer)) > size {
		size = int64(len(f.buffer))
	}
	if off >= size {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > size {
		end = size
	}

	// Dirty buffer present: it is a faithful image of the committed prefix
	// [0, committed) (writeLocked / ensureHydratedLocked keep loaded=true
	// whenever buffer is non-nil) plus any dirty tail, so it is authoritative
	// for the whole window up to its end. Anything past the buffer is a hole.
	// ensureHydratedLocked is a defensive re-check of the invariant: it is a
	// no-op when the buffer already covers committed content.
	if len(f.buffer) > 0 {
		if err := f.ensureHydratedLocked(ctx, rec); err != nil {
			rec.IncOpError("read")
			return nil, syscall.EIO
		}
		out := make([]byte, end-off)
		bufStart := int(off)
		if bufStart < 0 {
			bufStart = 0
		}
		if bufStart < len(f.buffer) {
			n := len(f.buffer) - bufStart
			if n > len(out) {
				n = len(out)
			}
			copy(out, f.buffer[bufStart:bufStart+n])
		}
		return fuse.ReadResultData(out), 0
	}
	if off >= metaInode.Size {
		return fuse.ReadResultData(nil), 0
	}
	end = off + int64(len(dest))
	if end > metaInode.Size {
		end = metaInode.Size
	}

	// Fast path: empty ChunkMap means the file has no on-disk chunks at
	// all — every byte is a hole. This covers the freshly-created /
	// never-flushed state and, more importantly, a truncate-extended file
	// (Size > 0 with no data written into the new range). Serve zeros for
	// the requested window rather than round-tripping through an empty
	// ChunkMap and returning zero bytes. (B1 fix.)
	//
	// Note the Size==0 case needs no special handling: the EOF clamp above
	// (off >= Size, i.e. off >= 0) already returns nil for it, so a
	// zero-size file never reaches here.
	if len(metaInode.ChunkMap) == 0 {
		size := end - off
		return fuse.ReadResultData(make([]byte, size)), 0
	}

	// Grab a reusable 128 KB buffer from the pool. The pool buffer
	// is used as scratch space for individual chunk reads; the final
	// result is assembled in a separate (right-sized) output slice
	// so the pool buffer can be returned immediately.
	bufp := readBufPool.Get().(*[]byte)
	defer readBufPool.Put(bufp)

	// Walk the ChunkMap and pick out the bytes that overlap the requested
	// window. Each chunk owns [cref.Offset, +cref.Length). The window is in
	// (off, end] in file coordinates; we trim each chunk payload to that
	// range and concatenate.
	//
	// (Same-class hardening) The ChunkMap is length-ordered, but it is not
	// guaranteed contiguous: after a truncate or an externally-produced
	// sparse layout there can be a hole between chunks whose bytes are never
	// materialized. Collapsing such holes would both misplace every
	// subsequent chunk at the wrong file coordinate and return a short read.
	// So the walk zero-fills any gap before each overlapping chunk, and
	// always returns a full (end-off)-byte window at correct coordinates.
	out := make([]byte, 0, end-off)
	next := off // next file offset already filled, in [off, end]
	for _, cref := range metaInode.ChunkMap {
		chunkStart := cref.Offset
		chunkEnd := cref.Offset + int64(cref.Length)
		if chunkEnd <= next || chunkStart >= end {
			// chunk entirely outside the remaining window
			continue
		}
		var payload []byte
		if f.cache != nil {
			if p, ok := f.cache.Get(uint64(cref.ID)); ok {
				payload = p
			}
		}
		if payload == nil {
			var chunk *metadata.ChunkMeta
			if err := f.reliability.DoMeta("read", func() error {
				var gerr error
				chunk, gerr = f.meta.GetChunk(ctx, cref.ID)
				return gerr
			}); err != nil {
				rec.IncOpError("read")
				return nil, syscall.EIO
			}
			if err := f.reliability.DoChunk("read", func() error {
				var gerr error
				payload, gerr = f.chunkStore.ReadChunk(ctx, chunk)
				return gerr
			}); err != nil {
				rec.IncOpError("read")
				return nil, syscall.EIO
			}
			if f.cache != nil {
				f.cache.Add(uint64(cref.ID), payload)
			}
		}
		// Zero-fill any hole between the last filled offset and this chunk.
		if chunkStart > next {
			gap := chunkStart - next
			if gap > end-next {
				gap = end - next
			}
			out = append(out, make([]byte, gap)...)
			next += gap
		}
		// Map file-coordinates [off,end) to chunk-local coordinates. Bound
		// by BOTH the chunk's declared file extent (cref.Length) and its
		// actual payload length, so we never serve bytes past the file
		// coordinate that this chunk genuinely owns.
		relStart := next - chunkStart
		relEnd := end - chunkStart
		if relEnd > int64(cref.Length) {
			relEnd = int64(cref.Length)
		}
		if relEnd > int64(len(payload)) {
			relEnd = int64(len(payload))
		}
		if relStart >= relEnd {
			continue
		}
		// If the chunk slice fits in the pool buffer, copy it there
		// first so we avoid growing `out` with tiny appends.
		chunkLen := int(relEnd - relStart)
		if chunkLen <= cap(*bufp) {
			buf := (*bufp)[:chunkLen]
			copy(buf, payload[relStart:relEnd])
			out = append(out, buf...)
		} else {
			out = append(out, payload[relStart:relEnd]...)
		}
		next += int64(chunkLen)
		if next >= end {
			break
		}
	}
	// Zero-fill a trailing hole after the last chunk (before 'end').
	if next < end {
		out = append(out, make([]byte, end-next)...)
	}
	return fuse.ReadResultData(out), 0
}

// Write buffers data at the given offset until Flush; the actual datanode
// round-trip happens in Flush, not here, so the kernel can coalesce many small
// pwrite(2) calls into one chunk allocation. It supports random writes: a write
// at a nonzero offset grows the buffer, zero-filling any hole. When the write
// lands after a Flush (buffer reset) but at a nonzero offset, the buffer is
// first hydrated from the committed file content so the next Flush's whole-file
// rebuild doesn't zero the committed prefix.
func (f *DFSFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	rec := recorderFor(f.recorder)
	rec.IncOp("write")

	f.mu.Lock()
	defer f.mu.Unlock()

	// Hydrate the committed prefix whenever the buffer isn't loaded. This
	// must happen for ANY write offset, including 0: a write at offset 0 is
	// only a full-file overwrite if it covers the whole committed file. A
	// partial off-0 overwrite (e.g. pwrite(fd, buf, 20, 0) over a 100-byte
	// committed file on a freshly re-opened handle) replaces the head and
	// must preserve the committed [off+len, committed) tail — otherwise the
	// next Flush rebuilds the file from an under-faithful buffer and silently
	// truncates/drops it. (Same-class fix; ensureHydratedLocked is a cheap
	// no-op for a fresh/empty file, writing at 0 into one stays untouched.)
	if !f.loaded {
		if err := f.ensureHydratedLocked(ctx, rec); err != nil {
			rec.IncOpError("write")
			return 0, syscall.EIO
		}
	}
	return f.writeLocked(ctx, data, off, rec)
}

// AppendWrite implements O_APPEND semantics: the write is placed at the
// file's current end (max of committed size and any already-buffered tail)
// regardless of the offset the kernel would otherwise pass from the fd's
// file position. This is the correct serialization point — the tail is
// computed under f.mu so concurrent appends to the same inode don't collide.
func (f *DFSFile) AppendWrite(ctx context.Context, fh fs.FileHandle, data []byte) (uint32, syscall.Errno) {
	rec := recorderFor(f.recorder)
	rec.IncOp("write")

	f.mu.Lock()
	defer f.mu.Unlock()

	// Resolve the true end of file: hydrate so the buffered tail reflects
	// committed content, then append after whatever is already buffered.
	if err := f.ensureHydratedLocked(ctx, rec); err != nil {
		rec.IncOpError("write")
		return 0, syscall.EIO
	}
	tail := int64(len(f.buffer))
	return f.writeLocked(ctx, data, tail, rec)
}

// =====================================================================
// Buffer-image invariant guard
// =====================================================================
//
// DFSFile's core state invariant is:
//
//	loaded == true  ⟹  f.buffer is a faithful image of the committed
//	                   prefix [0, committedSize), i.e. len(buffer) >= committed
//
// Every path that sets loaded=true must uphold it. Production never enforces
// this (it only reads a package-level atomic flag and costs nothing when
// off); tests turn it on so a future violation panics at the point of the
// write, instead of surfacing much later as silent data loss on Flush / a
// stale Read. The same-class bugs fixed in this series (most notably the
// off-0 partial-overwrite data loss) were all violations of this invariant,
// so it doubles as a regression tripwire.
var bufferImageInvariantOn atomic.Bool

// EnableBufferImageInvariant turns on the debug invariant assertion. Only
// tests should call it; production stays off for zero overhead.
func EnableBufferImageInvariant() { bufferImageInvariantOn.Store(true) }

// assertBufferImageLocked verifies that, when loaded, the buffer is at least
// as large as the currently committed file size. Must be called with f.mu
// held. It is a no-op when the invariant guard is off, or when a metadata
// read fails (an I/O failure is a real error path, not an invariant break).
func (f *DFSFile) assertBufferImageLocked(ctx context.Context) {
	if !bufferImageInvariantOn.Load() || !f.loaded {
		return
	}
	meta, err := f.meta.GetInode(ctx, f.inodeID)
	if err != nil {
		return
	}
	if int64(len(f.buffer)) < meta.Size {
		panic(fmt.Sprintf("DFSFile invariant violated: loaded=true but buffer len=%d < committed size=%d (inode %d)",
			len(f.buffer), meta.Size, f.inodeID))
	}
}

// writeLocked applies a buffered write at the given offset. Must be called
// with f.mu held and the buffer hydrated to cover the committed prefix
// (callers — Write, AppendWrite — hydrate whenever !loaded regardless of
// offset, so a partial off-0 overwrite cannot drop the committed tail).
func (f *DFSFile) writeLocked(ctx context.Context, data []byte, off int64, rec MetricsRecorder) (uint32, syscall.Errno) {
	// Buffer write data locally until flush
	needed := int(off) + len(data)
	if needed > len(f.buffer) {
		newBuf := make([]byte, needed)
		copy(newBuf, f.buffer)
		f.buffer = newBuf
	}
	copy(f.buffer[off:], data)
	f.dirty = true
	f.loaded = true
	f.assertBufferImageLocked(ctx)

	return uint32(len(data)), 0
}

// ensureHydratedLocked guarantees that f.buffer is a faithful image of the
// file's committed prefix [0, committedSize), zero-filling holes. This is
// required because Flush rebuilds the whole file from f.buffer: if the
// buffer started empty and a write landed at a nonzero offset, the
// committed [0, off) prefix must be loaded from the chunk store or the next
// Flush would write zeros over it (data loss for append /
// random-overwrite-after-Flush). Any already-buffered (un-flushed) bytes
// that extend past the committed size are preserved on top. Must be called
// with f.mu held.
func (f *DFSFile) ensureHydratedLocked(ctx context.Context, rec MetricsRecorder) error {
	if f.loaded {
		return nil
	}
	var metaInode *metadata.InodeMeta
	if err := f.reliability.DoMeta("read", func() error {
		var gerr error
		metaInode, gerr = f.meta.GetInode(ctx, f.inodeID)
		return gerr
	}); err != nil {
		return err
	}
	committed := int(metaInode.Size)
	if committed <= 0 {
		f.loaded = true
		f.assertBufferImageLocked(ctx)
		return nil
	}

	// Preserve any already-buffered tail that extends past the committed
	// size (un-flushed bytes written while the buffer wasn't loaded).
	var tail []byte
	if len(f.buffer) > committed {
		tail = f.buffer[committed:]
	}
	buf := make([]byte, committed+len(tail))
	f.buffer = buf
	if err := f.loadChunkRangeLocked(ctx, rec, metaInode); err != nil {
		return err
	}
	if tail != nil {
		copy(f.buffer[committed:], tail)
	}
	f.loaded = true
	f.assertBufferImageLocked(ctx)
	return nil
}

// loadChunkRangeLocked fills f.buffer's [0, committed) region from the
// inode's committed ChunkMap. Must be called with f.mu held and f.buffer
// already sized to at least the committed file size.
func (f *DFSFile) loadChunkRangeLocked(ctx context.Context, rec MetricsRecorder, metaInode *metadata.InodeMeta) error {
	for _, cref := range metaInode.ChunkMap {
		chunkStart := int(cref.Offset)
		if chunkStart < 0 {
			continue
		}
		chunkEnd := chunkStart + int(cref.Length)
		if chunkStart >= len(f.buffer) {
			break
		}
		if chunkEnd > len(f.buffer) {
			chunkEnd = len(f.buffer)
		}
		var payload []byte
		if f.cache != nil {
			if p, ok := f.cache.Get(uint64(cref.ID)); ok {
				payload = p
			}
		}
		if payload == nil {
			var chunk *metadata.ChunkMeta
			if err := f.reliability.DoMeta("read", func() error {
				var gerr error
				chunk, gerr = f.meta.GetChunk(ctx, cref.ID)
				return gerr
			}); err != nil {
				return err
			}
			if err := f.reliability.DoChunk("read", func() error {
				var gerr error
				payload, gerr = f.chunkStore.ReadChunk(ctx, chunk)
				return gerr
			}); err != nil {
				return err
			}
			if f.cache != nil {
				f.cache.Add(uint64(cref.ID), payload)
			}
		}
		rel := chunkStart - int(cref.Offset) // == 0 normally
		n := chunkEnd - chunkStart
		if rel < 0 {
			rel = 0
		}
		if rel >= len(payload) {
			continue
		}
		if rel+n > len(payload) {
			n = len(payload) - rel
		}
		copy(f.buffer[chunkStart:chunkStart+n], payload[rel:rel+n])
	}
	return nil
}

// Flush pushes the in-memory buffer to the chunk store and updates
// the inode's ChunkMap + size. It is idempotent: a second Flush on
// the same dirty buffer is a no-op (a clean Flush never re-writes).
//
// Multi-chunk (Program 11), aligned with the production S3 PUT
// overwrite path (metadataObjectCommitter.Put): the buffer is the
// whole new file content, so Flush cuts it into MaxChunkPayload
// windows, AllocateChunksBatch'es fresh chunk IDs at offsets
// [0, MaxChunkPayload, 2*MaxChunkPayload, ...), writes/commits/seals
// each, then wholesale-replaces the inode's ChunkMap with the new
// refs and DeleteChunk's the superseded old chunks. Fresh allocation
// + supersede + delete sidesteps the ErrChunkAlreadySealed invariant
// (a sealed chunk can never be re-committed) with zero metadata
// changes — exactly how S3 already reuses across PUTs.
func (f *DFSFile) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	rec := recorderFor(f.recorder)
	rec.IncOp("flush")

	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.dirty {
		return 0
	}

	// 路径锁：串行化同一 inode 的并发 Flush，防止不同 DFSFile
	// 实例（go-fuse inode cache eviction 后重建）交叉写入。
	// nil receiver 时为 no-op（passthrough 模式）。
	unlock := f.reliability.LockInode(uint64(f.inodeID))
	defer unlock()

	// Read the current inode once. Its ChunkMap holds the OLD chunk
	// refs we will supersede at the end of this flush.
	var metaInode *metadata.InodeMeta
	if err := f.reliability.DoMeta("flush", func() error {
		var gerr error
		metaInode, gerr = f.meta.GetInode(ctx, f.inodeID)
		return gerr
	}); err != nil {
		rec.IncOpError("flush")
		return syscall.EIO
	}
	oldRefs := make([]metadata.ChunkRef, len(metaInode.ChunkMap))
	copy(oldRefs, metaInode.ChunkMap)

	// Resolve the placement policy from the file's containing bucket
	// instead of using a hardcoded fuseChunkPolicy. This ensures a
	// bucket configured with ReplicationFactor=3 actually gets 3
	// replicas for FUSE writes (fixes D3).
	policy := f.resolveChunkPolicy(ctx, metaInode)

	size := len(f.buffer)
	nChunks := (size + MaxChunkPayload - 1) / int(MaxChunkPayload)
	if nChunks == 0 {
		nChunks = 1 // an empty buffer still allocates one chunk at offset 0
	}
	offsets := make([]int64, nChunks)
	segEnd := make([]int, nChunks)
	for i := 0; i < nChunks; i++ {
		offsets[i] = int64(i) * MaxChunkPayload
		end := (i + 1) * MaxChunkPayload
		if end > size {
			end = size
		}
		segEnd[i] = end
	}

	// AllocateChunksBatch is capped at metadata.MaxChunkAllocationBatch
	// (1024) chunks per request. A single flush beyond that (>64 GiB) is
	// unrealistic, but loop defensively so the batch allocation never
	// trips the per-request cap. Each batch offsets are already sorted
	// ascending, so the concatenated result preserves file order.
	chunks := make([]*metadata.ChunkMeta, 0, nChunks)
	for start := 0; start < nChunks; {
		end := start + metadata.MaxChunkAllocationBatch
		if end > nChunks {
			end = nChunks
		}
		var batch []*metadata.ChunkMeta
		if err := f.reliability.DoMeta("flush", func() error {
			var gerr error
			batch, gerr = f.meta.AllocateChunksBatch(ctx, f.inodeID, offsets[start:end], policy)
			return gerr
		}); err != nil {
			logf("flush: allocate chunk batch [%d:%d): %v", start, end, err)
			rec.IncOpError("flush")
			return syscall.EIO
		}
		chunks = append(chunks, batch...)
		start = end
	}

	// Write, commit, and seal each chunk, then assemble the new
	// ChunkMap refs. Mirrors S3 metadataObjectCommitter.Put: one
	// ChunkRef{ID, Offset, Length, Version} per chunk at its
	// MaxChunkPayload-aligned offset.
	newRefs := make([]metadata.ChunkRef, 0, nChunks)
	for i, chunk := range chunks {
		data := f.buffer[i*MaxChunkPayload:segEnd[i]]
		if err := f.reliability.DoChunk("flush", func() error {
			return f.chunkStore.WriteChunk(ctx, chunk, data)
		}); err != nil {
			logf("flush: write chunk %d: %v", chunk.ID, err)
			rec.IncOpError("flush")
			return syscall.EIO
		}
		checksum := crc32.ChecksumIEEE(data)
		if err := f.reliability.DoMeta("flush", func() error {
			return f.meta.CommitChunk(ctx, chunk.ID, checksum)
		}); err != nil {
			logf("flush: commit chunk %d: %v", chunk.ID, err)
			rec.IncOpError("flush")
			return syscall.EIO
		}
		if err := f.reliability.DoMeta("flush", func() error {
			return f.meta.SealChunk(ctx, chunk.ID)
		}); err != nil {
			logf("flush: seal chunk %d: %v", chunk.ID, err)
			// Not fatal: a sealed chunk can already be read.
		}
		newRefs = append(newRefs, metadata.ChunkRef{
			ID:      chunk.ID,
			Offset:  offsets[i],
			Length:  int32(len(data)),
			Version: 1,
		})
	}

	// Wholesale-replace the ChunkMap + size + mtime (S3 overwrite
	// semantics), so Read walks the fresh refs across every chunk.
	metaInode.ChunkMap = newRefs
	metaInode.Size = int64(size)
	metaInode.MTime = time.Now().UnixNano()

	if err := f.reliability.DoMeta("flush", func() error {
		return f.meta.UpdateInode(ctx, metaInode)
	}); err != nil {
		rec.IncOpError("flush")
		return syscall.EIO
	}

	// Reclaim the superseded chunks. Fresh-allocation guarantees none
	// of the new IDs overlap the old refs, so deleting by ID is safe.
	for _, cref := range oldRefs {
		if err := f.reliability.DoMeta("flush", func() error {
			return f.meta.DeleteChunk(ctx, cref.ID)
		}); err != nil {
			logf("flush: delete old chunk %d: %v", cref.ID, err)
		}
	}

	f.buffer = nil
	f.dirty = false
	f.loaded = false
	return 0
}

// Fsync syncs file data to persistent storage.
func (f *DFSFile) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	return f.Flush(ctx, fh)
}

// Release is called when the last reference to the file handle is dropped.
func (f *DFSFile) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	rec := recorderFor(f.recorder)
	rec.IncOp("release")

	// Flush first so buffered data hits the chunk store before we
	// release the advisory lock.
	if err := f.Flush(ctx, fh); err != 0 {
		return err
	}

	// Release the advisory lock that Open acquired.  If no lock was
	// acquired (unit tests / empty lockOwner) this is a no-op.
	if h, ok := fh.(*DFSFileHandle); ok && h.lockAcquired && f.lockOwner != "" {
		if err := f.meta.AdvisoryUnlock(ctx, f.inodeID, f.lockOwner); err != nil {
			logf("release: advisory unlock %d: %v", f.inodeID, err)
			// POSIX-flock: unlock failure is non-fatal; log only.
		}
	}

	return 0
}

// Getattr returns file attributes.
func (f *DFSFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	rec := recorderFor(f.recorder)
	metaInode, err := f.meta.GetInode(ctx, f.inodeID)
	if err != nil {
		rec.IncOpError("getattr")
		return syscall.EIO
	}
	out.Attr = inodeMetaToAttr(metaInode)

	// Report the effective file size including any un-flushed buffered tail
	// (e.g. after an O_APPEND write), so stat() agrees with where the next
	// write/append will land rather than lagging behind on committed size.
	// Same-class fix: the buffered image must be reflected in the view.
	f.mu.Lock()
	if int64(len(f.buffer)) > int64(out.Attr.Size) {
		out.Attr.Size = uint64(len(f.buffer))
	}
	f.mu.Unlock()
	return 0
}

// Setattr sets file attributes (truncate, chmod, etc.).
func (f *DFSFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	rec := recorderFor(f.recorder)
	metaInode, err := f.meta.GetInode(ctx, f.inodeID)
	if err != nil {
		rec.IncOpError("setattr")
		return syscall.EIO
	}

	if size, ok := in.GetSize(); ok {
		f.mu.Lock()
		if int(size) >= len(f.buffer) && !f.loaded {
			// Truncating up (or to the same size) over a buffer that
			// doesn't yet hold the committed prefix: hydrate it first so
			// the extension zero-fills only the new range and preserves
			// the committed content (truncate-extend), and so a later
			// Flush's whole-file rebuild doesn't zero committed bytes.
			if err := f.ensureHydratedLocked(ctx, rec); err != nil {
				rec.IncOpError("setattr")
				f.mu.Unlock()
				return syscall.EIO
			}
		}
		if int(size) < len(f.buffer) {
			f.buffer = f.buffer[:size]
			// Truncating down: the file is genuinely smaller now, so the
			// buffer is a faithful image of the new file. Committed bytes
			// beyond `size` are intentionally dropped on the next Flush.
			f.dirty = true
		} else if int(size) > len(f.buffer) {
			newBuf := make([]byte, size)
			copy(newBuf, f.buffer)
			f.buffer = newBuf
			f.dirty = true
		} else {
			// size == len(f.buffer): no buffer change, but the attr still
			// applies below (mtime/ctime).
		}
		metaInode.Size = int64(size)
		f.mu.Unlock()
	}

	if mode, ok := in.GetMode(); ok {
		metaInode.Mode = mode
	}
	if uid, ok := in.GetUID(); ok {
		metaInode.UID = uid
	}
	if gid, ok := in.GetGID(); ok {
		metaInode.GID = gid
	}
	metaInode.MTime = time.Now().UnixNano()
	metaInode.CTime = time.Now().UnixNano()

	if err := f.meta.UpdateInode(ctx, metaInode); err != nil {
		rec.IncOpError("setattr")
		return syscall.EIO
	}

	out.Attr = inodeMetaToAttr(metaInode)
	return 0
}

// Access always returns 0 (allow-all).
func (f *DFSFile) Access(ctx context.Context, mask uint32) syscall.Errno {
	return 0
}

// OpenXAttr returns an xattr handle for this file.
func (f *DFSFile) OpenXAttr() *DFSXAttr {
	return &DFSXAttr{meta: f.meta, inodeID: f.inodeID}
}

// ========== DFSFileHandle methods ==========

var _ = (fs.FileReader)((*DFSFileHandle)(nil))
var _ = (fs.FileWriter)((*DFSFileHandle)(nil))

func (h *DFSFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	return h.file.Read(ctx, h, dest, off)
}

func (h *DFSFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	// O_APPEND: ignore the fd's file position and write at the current end.
	if h.append {
		return h.file.AppendWrite(ctx, h, data)
	}
	return h.file.Write(ctx, h, data, off)
}

// DFSSymlink is defined in symlink.go.
