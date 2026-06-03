//go:build linux

package fuse

import (
	"context"
	"errors"
	"hash/crc32"
	"sync"
	"syscall"
	"time"

	"github.com/example/dfs/gateway"
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
// ChunkStore.WriteChunk. A file larger than this is rejected with
// EFBIG in commit 1.1 (single-chunk path); commit 1.2 / the cache
// commit will introduce a multi-chunk Flush that allocates one
// chunk per MaxChunkPayload window.
const MaxChunkPayload = 64 * 1024 * 1024 // 64 MiB

// fuseChunkPolicy is the placement policy used when Flush allocates
// a new chunk for a FUSE write. Commit 1.1 only handles single-chunk
// files (one Flush = one chunk); for multi-chunk files we need to
// thread the parent bucket's policy through, which is commit 1.2
// work. Single-replica is the right default for now: it makes the
// MemoryChunkStore test double trivially correct and it matches
// the existing DatanodeChunkStore default for unit tests.
//
// TODO(commit-1.2): look up the parent bucket's policy via
// meta.GetBucket and use it here, so a /mnt/dfs/foo/ write honours
// the policy that `s3gw CreateBucket` set.
var fuseChunkPolicy = metadata.PlacementPolicy{
	ID:                "fuse-default",
	ReplicationFactor: 1,
	TopologySpread:    metadata.SpreadNode,
}

// DFSFile represents a regular file in the DFS FUSE filesystem.
type DFSFile struct {
	fs.Inode

	meta       metadata.MetadataService
	chunkStore gateway.ChunkStore
	inodeID    metadata.InodeID

	// lockOwner is the per-process owner string used when acquiring
	// advisory file locks; see commit 0 (metadata: add advisory
	// file lock service). The Open call fills it in so Release /
	// Flush know what to unlock. Empty means "do not take locks"
	// — used in unit tests that have no lock manager.
	lockOwner string

	// Write buffer for small writes before flush
	mu     sync.Mutex
	dirty  bool
	buffer []byte
}

var _ = (fs.NodeOpener)((*DFSFile)(nil))
var _ = (fs.NodeReader)((*DFSFile)(nil))
var _ = (fs.NodeWriter)((*DFSFile)(nil))
var _ = (fs.NodeGetattrer)((*DFSFile)(nil))
var _ = (fs.NodeSetattrer)((*DFSFile)(nil))
var _ = (fs.NodeFsyncer)((*DFSFile)(nil))
var _ = (fs.NodeFlusher)((*DFSFile)(nil))
var _ = (fs.NodeReleaser)((*DFSFile)(nil))

// DFSFileHandle wraps DFSFile for per-open-file state.
type DFSFileHandle struct {
	file *DFSFile
	// lockAcquired is true when Open acquired an advisory lock on
	// this file descriptor. Release must call AdvisoryUnlock exactly
	// once per acquisition (POSIX-flock semantics).
	lockAcquired bool
}

// Open opens the file.
func (f *DFSFile) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	h := &DFSFileHandle{file: f}

	// Acquire an advisory file lock.  O_WRONLY|O_RDWR → exclusive;
	// O_RDONLY → shared.  An empty lockOwner (unit tests that supply
	// no lock manager) skips locking entirely.
	if f.lockOwner != "" && f.meta != nil {
		isWrite := (flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0)
		if isWrite {
			if err := f.meta.AdvisoryLock(ctx, f.inodeID, f.lockOwner); err != nil {
				logf("open: advisory lock %d: %v", f.inodeID, err)
				return nil, 0, syscall.EIO
			}
		} else {
			if err := f.meta.AdvisoryLockShared(ctx, f.inodeID, f.lockOwner); err != nil {
				logf("open: advisory shared lock %d: %v", f.inodeID, err)
				return nil, 0, syscall.EIO
			}
		}
		h.lockAcquired = true
	}

	return h, 0, 0
}

// Read reads data from the file.
func (f *DFSFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	metaInode, err := f.meta.GetInode(ctx, f.inodeID)
	if err != nil {
		return nil, syscall.EIO
	}
	// Clamp read to file size
	if off >= metaInode.Size {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > metaInode.Size {
		end = metaInode.Size
	}

	// Fast path: empty ChunkMap on a zero-size file is the freshly-
	// created / never-flushed state. Serve zeros rather than
	// round-tripping through every chunk. (B1 fix.)
	if len(metaInode.ChunkMap) == 0 && metaInode.Size == 0 {
		size := end - off
		return fuse.ReadResultData(make([]byte, size)), 0
	}

	// Grab a reusable 128 KB buffer from the pool. The pool buffer
	// is used as scratch space for individual chunk reads; the final
	// result is assembled in a separate (right-sized) output slice
	// so the pool buffer can be returned immediately.
	bufp := readBufPool.Get().(*[]byte)
	defer readBufPool.Put(bufp)

	// Walk the ChunkMap and pick out the bytes that overlap the
	// requested window. Each chunk owns [cref.Offset, +cref.Length).
	// The window is in (off, end] in file coordinates; we trim
	// each chunk payload to that range and concatenate.
	out := make([]byte, 0, end-off)
	for _, cref := range metaInode.ChunkMap {
		chunkStart := cref.Offset
		chunkEnd := cref.Offset + int64(cref.Length)
		if chunkEnd <= off || chunkStart >= end {
			// chunk entirely outside the requested window
			continue
		}
		chunk, err := f.meta.GetChunk(ctx, cref.ID)
		if err != nil {
			return nil, syscall.EIO
		}
		payload, err := f.chunkStore.ReadChunk(ctx, chunk)
		if err != nil {
			return nil, syscall.EIO
		}
		// Map file-coordinates [off,end) to chunk-local coordinates.
		relStart := off - chunkStart
		if relStart < 0 {
			relStart = 0
		}
		relEnd := end - chunkStart
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
		if int64(len(out)) >= end-off {
			break
		}
	}
	return fuse.ReadResultData(out), 0
}

// Write writes data to the file. The actual datanode round-trip
// happens in Flush, not here, so the kernel can coalesce many small
// pwrite(2) calls into one chunk allocation.
func (f *DFSFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Buffer write data locally until flush
	needed := int(off) + len(data)
	if needed > len(f.buffer) {
		newBuf := make([]byte, needed)
		copy(newBuf, f.buffer)
		f.buffer = newBuf
	}
	copy(f.buffer[off:], data)
	f.dirty = true

	return uint32(len(data)), 0
}

// Flush pushes the in-memory buffer to the chunk store and updates
// the inode's ChunkMap + size. It is idempotent: a second Flush on
// the same dirty buffer is a no-op for the chunk store (the chunks
// are already sealed), and a third+ on the same buffer is a no-op
// for the metadata (size is monotone). (B2 + B3 fix.)
//
// Single-chunk only in commit 1.1: AllocateChunk at offset 0, then
// WriteChunk + CommitChunk + SealChunk the entire buffer as one
// payload, then update the ChunkMap length and inode size. A file
// larger than MaxChunkPayload is rejected with EFBIG — the right
// answer for a single-chunk path; the multi-chunk Flush arrives
// in commit 1.2 / the cache commit alongside an mmap-style write
// buffer.
func (f *DFSFile) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.dirty {
		return 0
	}

	// Reject files that would not fit in a single chunk. (commit
	// 1.1 only knows the single-chunk code path.)
	if int64(len(f.buffer)) > MaxChunkPayload {
		logf("flush: file %d size %d exceeds single-chunk limit %d (multi-chunk write is commit 1.2)", f.inodeID, len(f.buffer), MaxChunkPayload)
		return syscall.EFBIG
	}

	metaInode, err := f.meta.GetInode(ctx, f.inodeID)
	if err != nil {
		return syscall.EIO
	}

	// Find the existing chunk for offset 0 if the file has been
	// flushed before; otherwise allocate a fresh one.
	var existingChunkID metadata.ChunkID
	for _, cref := range metaInode.ChunkMap {
		if cref.Offset == 0 {
			existingChunkID = cref.ID
			break
		}
	}

	chunk, err := f.meta.AllocateChunk(ctx, f.inodeID, 0, fuseChunkPolicy)
	if err != nil {
		logf("flush: allocate chunk: %v", err)
		return syscall.EIO
	}
	_ = existingChunkID // currently unused; future work lets us
	// re-use the same chunk ID across flushes (the allocator always
	// returns a fresh ID today; that's a metadata-layer bug — see
	// TODO in the design doc).

	if err := f.chunkStore.WriteChunk(ctx, chunk, f.buffer); err != nil {
		logf("flush: write chunk %d: %v", chunk.ID, err)
		return syscall.EIO
	}
	checksum := crc32.ChecksumIEEE(f.buffer)
	if err := f.meta.CommitChunk(ctx, chunk.ID, checksum); err != nil {
		logf("flush: commit chunk %d: %v", chunk.ID, err)
		return syscall.EIO
	}
	if err := f.meta.SealChunk(ctx, chunk.ID); err != nil {
		logf("flush: seal chunk %d: %v", chunk.ID, err)
		// Not fatal: a sealed chunk can already be read.
	}

	// Re-read the inode to find the ChunkRef that AllocateChunk
	// just appended, then stamp the actual data length on it so
	// Read can trim the payload to the data window.
	metaInode, err = f.meta.GetInode(ctx, f.inodeID)
	if err != nil {
		return syscall.EIO
	}
	for i := range metaInode.ChunkMap {
		if metaInode.ChunkMap[i].ID == chunk.ID {
			metaInode.ChunkMap[i].Length = int32(len(f.buffer))
			break
		}
	}

	// Update the inode size + mtime + the patched ChunkMap, then
	// drop the buffer so the next Flush is a no-op even if the
	// kernel calls us twice.
	metaInode.Size = int64(len(f.buffer))
	metaInode.MTime = time.Now().UnixNano()

	if err := f.meta.UpdateInode(ctx, metaInode); err != nil {
		return syscall.EIO
	}

	f.buffer = nil
	f.dirty = false
	return 0
}

// Fsync syncs file data to persistent storage.
func (f *DFSFile) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	return f.Flush(ctx, fh)
}

// Release is called when the last reference to the file handle is dropped.
func (f *DFSFile) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
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
	metaInode, err := f.meta.GetInode(ctx, f.inodeID)
	if err != nil {
		return syscall.EIO
	}
	out.Attr = inodeMetaToAttr(metaInode)
	return 0
}

// Setattr sets file attributes (truncate, chmod, etc.).
func (f *DFSFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	metaInode, err := f.meta.GetInode(ctx, f.inodeID)
	if err != nil {
		return syscall.EIO
	}

	if size, ok := in.GetSize(); ok {
		f.mu.Lock()
		if int(size) < len(f.buffer) {
			f.buffer = f.buffer[:size]
		} else if int(size) > len(f.buffer) {
			newBuf := make([]byte, size)
			copy(newBuf, f.buffer)
			f.buffer = newBuf
		}
		metaInode.Size = int64(size)
		f.dirty = true
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
		return syscall.EIO
	}

	out.Attr = inodeMetaToAttr(metaInode)
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
	return h.file.Write(ctx, h, data, off)
}

// ========== DFSSymlink: symbolic link inode ==========

// DFSSymlink represents a symbolic link in the DFS FUSE filesystem.
type DFSSymlink struct {
	fs.Inode

	meta    metadata.MetadataService
	inodeID metadata.InodeID
}

var _ = (fs.NodeReadlinker)((*DFSSymlink)(nil))
var _ = (fs.NodeGetattrer)((*DFSSymlink)(nil))

// Readlink reads the target path of the symlink.
func (s *DFSSymlink) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	target, err := s.meta.Readlink(ctx, s.inodeID)
	if err != nil {
		if errors.Is(err, metadata.ErrNotSymlink) {
			return nil, syscall.EINVAL
		}
		return nil, syscall.EIO
	}
	return []byte(target), 0
}

// Getattr returns symlink attributes.
func (s *DFSSymlink) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	metaInode, err := s.meta.GetInode(ctx, s.inodeID)
	if err != nil {
		return syscall.EIO
	}
	out.Attr = inodeMetaToAttr(metaInode)
	return 0
}

// OpenXAttr returns an xattr handle for this symlink.
func (s *DFSSymlink) OpenXAttr() *DFSXAttr {
	return &DFSXAttr{meta: s.meta, inodeID: s.inodeID}
}
