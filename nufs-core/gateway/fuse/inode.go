//go:build linux

package fuse

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
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

// directIOEnabled controls whether Open returns FOPEN_DIRECT_IO,
// bypassing the kernel page cache. Set via SetDirectIO before mounting.
var directIOEnabled atomic.Bool

// SetDirectIO enables or disables DirectIO for all subsequent Open calls.
func SetDirectIO(enabled bool) { directIOEnabled.Store(enabled) }

// ========== DFSFile: regular file inode ==========

// fuseDefaultPolicy is the fallback placement policy used when Flush
// cannot determine the parent bucket's policy (e.g., the inode has no
// BucketRoot). This is a safe single-replica default for orphan files.
var fuseDefaultPolicy = metadata.PlacementPolicy{
	ID:                "fuse-default",
	ReplicationFactor: 1,
	TopologySpread:    metadata.SpreadNode,
}

// DFSFile represents a regular file in the DFS FUSE filesystem.
type DFSFile struct {
	fs.Inode

	// fs is the owning filesystem root. It is a stable pointer across
	// metadata hot-swaps (SwapMetadata), so per-inode methods resolve the
	// current metadata service via fs.Meta() rather than caching it.
	fs *DFSFileSystem

	// inodeID is the metadata inode this file maps to.
	inodeID metadata.InodeID

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

	// buffered owns the entire chunk write-buffer state machine — lazy
	// 64 MiB chunk buffers, dirty tracking, disk spill under the dirty
	// budget, hydration of committed bytes, the logical size tail, and
	// Flush persistence (policy → ChunkWriter → merge → UpdateInode →
	// ledger → delete superseded refs). DFSFile is the go-fuse glue layer
	// around it: permissions, advisory locks, op metrics, errno mapping.
	// See chunkstore.BufferedFile.
	buffered *chunkstore.BufferedFile
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
var _ = (fs.NodeAllocater)((*DFSFile)(nil))

// flushLedger adapts BufferedFile flush state transitions to the write-attempt
// ledger shared with the S3 gateway (observability + crash recovery). The
// recovery worker picks up incomplete attempts and cleans up orphaned chunks.
type flushLedger struct {
	dfs     *DFSFileSystem
	inodeID metadata.InodeID
}

func (l flushLedger) Record(ctx context.Context, attemptID string, meta *metadata.InodeMeta, state metadata.WriteAttemptState, lastErr string) {
	chunks := make([]metadata.ChunkRef, len(meta.ChunkMap))
	copy(chunks, meta.ChunkMap)
	_ = l.dfs.Meta().PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:         attemptID,
		Bucket:     l.dfs.bucketName,
		InodeID:    l.inodeID,
		InodeCTime: meta.CTime,
		Chunks:     chunks,
		State:      state,
		LastError:  lastErr,
	})
}

// =====================================================================
// Chunk-level buffer helpers
// =====================================================================

// The chunk-level buffer helpers (getChunk / markDirty / clearChunks /
// stagingPath / spillToDisk / loadFromDisk / cleanupStaging / spillOldestChunk /
// recordFlushAttempt / cleanStagingDir / assertBufferImageLocked / writeLocked /
// loadCommittedChunkLocked) moved to chunkstore.BufferedFile when the write
// buffer was pulled into the SDK. DFSFile methods delegate to f.buffered;
// see chunkstore/buffered.go.



// FALLOC_FL_* mode bits for NodeAllocater.Allocate. The syscall package on
// Linux does not export these (they live in golang.org/x/sys/unix), so we
// define the ones we honor here, matching <linux/falloc.h>. Unsupported
// range-manipulation flags (COLLAPSE_RANGE/INSERT_RANGE/UNSHARE_RANGE) return
// EOPNOTSUPP.
const (
	fallocKeepSize     = 0x01 // FALLOC_FL_KEEP_SIZE
	fallocPunchHole    = 0x02 // FALLOC_FL_PUNCH_HOLE
	fallocCollapseRng  = 0x08 // FALLOC_FL_COLLAPSE_RANGE (unsupported)
	fallocZeroRange    = 0x10 // FALLOC_FL_ZERO_RANGE
	fallocInsertRng    = 0x20 // FALLOC_FL_INSERT_RANGE (unsupported)
	fallocUnshareRange = 0x40 // FALLOC_FL_UNSHARE_RANGE (unsupported)
)

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

// checkOpenAccess verifies that the caller's uid/gid has the POSIX permission
// bits required by the open flags (read/write), mirroring the implicit
// permission check the kernel performs for access(2) but not for open(2) when
// a filesystem implements NodeAccesser.
func (f *DFSFile) checkOpenAccess(ctx context.Context, flags uint32) syscall.Errno {
	caller, ok := ctx.(*fuse.Context)
	if !ok {
		return 0
	}
	metaInode, err := f.fs.Meta().GetInode(ctx, f.inodeID)
	if err != nil {
		return syscall.EIO
	}
	perm := metaInode.Mode & 0o7777 // mask to permission + sticky/setuid/setgid bits

	// Map open flags to the 3-bit R/W/X mask that hasPOSIXAccess expects
	// (it applies the owner/group/other shift internally).
	var mask uint32
	switch flags & (syscall.O_RDONLY | syscall.O_WRONLY | syscall.O_RDWR) {
	case syscall.O_WRONLY, syscall.O_RDWR:
		mask = 0o2 // W_OK
	case syscall.O_RDONLY:
		mask = 0o4 // R_OK
	}
	if mask == 0 {
		return 0
	}
	if hasPOSIXAccess(caller.Uid, caller.Gid, metaInode.UID, metaInode.GID, perm, mask) {
		return 0
	}
	return syscall.EACCES
}

// Open opens the file.
func (f *DFSFile) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	rec := recorderFor(f.recorder)
	rec.IncOp("open")
	start := time.Now()
	defer func() { rec.ObserveOpLatency("open", time.Since(start)) }()

	// POSIX requires open(2) to check the requested access mode (read/write)
	// against the file's permission bits before allowing the file descriptor to
	// be returned.  This is the same check access(2) would perform for the
	// corresponding R_OK / W_OK probe, but enforced synchronously on open so
	// that a mode-0000 file cannot be opened for reading or writing.
	if errno := f.checkOpenAccess(ctx, flags); errno != 0 {
		rec.IncOpError("open")
		return nil, 0, errno
	}

	// Linux delivers open(2)'s O_TRUNC via the Open flags (the kernel does not
	// issue a separate SETATTR for it), so the filesystem must perform the
	// truncation itself. Do it before anything else so the truncation is
	// visible immediately to every other open fd of this inode.
	if flags&syscall.O_TRUNC != 0 {
		if e := f.truncateOnOpen(ctx, rec); e != 0 {
			rec.IncOpError("open")
			return nil, 0, e
		}
	}

	h := &DFSFileHandle{file: f, append: flags&syscall.O_APPEND != 0}

	// Acquire an advisory file lock.  O_WRONLY|O_RDWR → exclusive;
	// O_RDONLY → shared.  An empty lockOwner (unit tests that supply
	// no lock manager) skips locking entirely.
	if f.lockOwner != "" && f.fs != nil {
		isWrite := (flags&(syscall.O_WRONLY|syscall.O_RDWR) != 0)
		if isWrite {
			if err := f.fs.Meta().AdvisoryLock(ctx, f.inodeID, f.lockOwner); err != nil {
				logf("open: advisory lock %d: %v", f.inodeID, err)
				rec.IncOpError("open")
				return nil, 0, syscall.EIO
			}
		} else {
			if err := f.fs.Meta().AdvisoryLockShared(ctx, f.inodeID, f.lockOwner); err != nil {
				logf("open: advisory shared lock %d: %v", f.inodeID, err)
				rec.IncOpError("open")
				return nil, 0, syscall.EIO
			}
		}
		h.lockAcquired = true
	}

	if directIOEnabled.Load() {
		fuseFlags = fuse.FOPEN_DIRECT_IO
	}
	return h, fuseFlags, 0
}

// truncateOnOpen implements O_TRUNC semantics for Open: the file becomes
// empty immediately, visible to every other open fd of the inode. A
// subsequent write in this same open produces only the new bytes (a fresh
// empty file), never a stale committed tail.
//
// It clears the whole buffer image (in-memory chunks AND spilled staging
// files) and marks it dirty: the (empty) buffer is the faithful image of the
// now-empty file, which is what stops hydration from restoring committed
// bytes on a later partial write — truncation wins over hydration. The
// inode's committed Size is set to 0 right away; its old ChunkMap refs are
// left untouched and get reclaimed by the supersede+delete in Flush.
func (f *DFSFile) truncateOnOpen(ctx context.Context, rec MetricsRecorder) syscall.Errno {
	var metaInode *metadata.InodeMeta
	if err := f.reliability.DoMeta("truncate", func() error {
		var gerr error
		metaInode, gerr = f.fs.Meta().GetInode(ctx, f.inodeID)
		return gerr
	}); err != nil {
		logf("open otrunc: get inode %d: %v", f.inodeID, err)
		return syscall.EIO
	}

	f.buffered.Clear() // also removes any spilled staging files (orphan leak fix)
	f.buffered.MarkDirty()
	metaInode.Size = 0
	metaInode.MTime = time.Now().UnixNano()
	metaInode.CTime = time.Now().UnixNano()

	if err := f.reliability.DoMeta("truncate", func() error {
		return f.fs.Meta().UpdateInode(ctx, metaInode)
	}); err != nil {
		logf("open otrunc: update inode %d: %v", f.inodeID, err)
		return syscall.EIO
	}
	return 0
}

// Read reads data from the file. The returned window is the merged view of
// the in-memory chunk buffers (dirty writes) and the committed chunk store:
// a dirty chunk is authoritative over the committed bytes it overlaps, so a
// read immediately after a write (before Flush) sees the new data rather than
// stale committed content. The whole walk lives in chunkstore.BufferedFile.
func (f *DFSFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	rec := recorderFor(f.recorder)
	rec.IncOp("read")
	start := time.Now()
	defer func() { rec.ObserveOpLatency("read", time.Since(start)) }()

	data, err := f.buffered.ReadView(ctx, off, int64(len(dest)))
	if err != nil {
		rec.IncOpError("read")
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(data), 0
}

// Write buffers data at the given offset until Flush; the actual datanode
// round-trip happens in Flush, not here, so the kernel can coalesce many small
// pwrite(2) calls into one chunk allocation. It supports random writes: a write
// at a nonzero offset grows the buffer, zero-filling any hole. When the write
// lands after a Flush (buffer reset) but at a nonzero offset, the buffer is
// first hydrated from the committed file content so the next Flush's whole-file
// rebuild doesn't zero the committed prefix.
func (f *DFSFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	dfs := f.fs
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return 0, toErrno(err)
	}
	rec := recorderFor(f.recorder)
	rec.IncOp("write")
	start := time.Now()
	defer func() { rec.ObserveOpLatency("write", time.Since(start)) }()

	n, err := f.buffered.Write(ctx, data, off)
	if err != nil {
		rec.IncOpError("write")
		if errors.Is(err, chunkstore.ErrOutOfDirtyBudget) {
			return 0, syscall.ENOSPC
		}
		return 0, syscall.EIO
	}
	return uint32(n), 0
}

// AppendWrite implements O_APPEND semantics: the write is placed at the
// file's current end (max of committed size and any already-buffered tail)
// regardless of the offset the kernel would otherwise pass from the fd's
// file position. The tail computation and serialization live in
// chunkstore.BufferedFile.AppendWrite (a lazy GetInode under the buffer lock).
func (f *DFSFile) AppendWrite(ctx context.Context, fh fs.FileHandle, data []byte) (uint32, syscall.Errno) {
	rec := recorderFor(f.recorder)
	rec.IncOp("write")
	start := time.Now()
	defer func() { rec.ObserveOpLatency("write", time.Since(start)) }()

	n, err := f.buffered.AppendWrite(ctx, data)
	if err != nil {
		rec.IncOpError("write")
		if errors.Is(err, chunkstore.ErrOutOfDirtyBudget) {
			return 0, syscall.ENOSPC
		}
		return 0, syscall.EIO
	}
	return uint32(n), 0
}

// The buffer-image invariant guard (assertBufferImageLocked + the debug
// EnableBufferImageInvariant switch) and the write-path internals
// (writeLocked / loadCommittedChunkLocked / ensureHydratedLocked /
// loadChunkRangeLocked) moved to chunkstore.BufferedFile.

// Flush pushes the in-memory buffer to the chunk store and updates
// the inode's ChunkMap + size. It is idempotent: a second Flush on
// the same dirty buffer is a no-op (a clean Flush never re-writes).
//
// Only dirty chunks are written. Untouched committed chunks are preserved
// in the new ChunkMap without being loaded into memory — this keeps memory
// proportional to dirty data, not file size.
//
// The full persistence — policy resolution, chunk dispatch, merge,
// UpdateInode, write-attempt ledger transitions, superseded-chunk reclaim,
// buffer clear — lives in chunkstore.BufferedFile.Flush, which also takes
// the inode path lock (LockInode) so concurrent Flushes of the same inode
// from distinct DFSFile instances serialize.
func (f *DFSFile) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	dfs := f.fs
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return toErrno(err)
	}
	rec := recorderFor(f.recorder)
	rec.IncOp("flush")
	start := time.Now()
	defer func() { rec.ObserveOpLatency("flush", time.Since(start)) }()

	if _, err := f.buffered.Flush(ctx); err != nil {
		rec.IncOpError("flush")
		return syscall.EIO
	}
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
	start := time.Now()
	defer func() { rec.ObserveOpLatency("release", time.Since(start)) }()

	// Flush first so buffered data hits the chunk store before we
	// release the advisory lock.
	if err := f.Flush(ctx, fh); err != 0 {
		return err
	}

	// Release the advisory lock that Open acquired.  If no lock was
	// acquired (unit tests / empty lockOwner) this is a no-op.
	if h, ok := fh.(*DFSFileHandle); ok && h.lockAcquired && f.lockOwner != "" {
		if err := f.fs.Meta().AdvisoryUnlock(ctx, f.inodeID, f.lockOwner); err != nil {
			logf("release: advisory unlock %d: %v", f.inodeID, err)
			// POSIX-flock: unlock failure is non-fatal; log only.
		}
	}

	return 0
}

// Getattr returns file attributes.
func (f *DFSFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	rec := recorderFor(f.recorder)
	metaInode, err := f.fs.Meta().GetInode(ctx, f.inodeID)
	if err != nil {
		rec.IncOpError("getattr")
		return syscall.EIO
	}
	out.Attr = inodeMetaToAttr(metaInode)

	// Report the effective file size including any un-flushed buffered tail
	// (e.g. after an O_APPEND write), so stat() agrees with where the next
	// write/append will land rather than lagging behind on committed size.
	// Same-class fix: the buffered image must be reflected in the view.
	if s := f.buffered.Size(); s > int64(out.Attr.Size) {
		out.Attr.Size = uint64(s)
	}
	return 0
}

// Setattr sets file attributes (truncate, chmod, etc.).
func (f *DFSFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	rec := recorderFor(f.recorder)

	// Block truncation in read-only mode; allow chmod/chown (metadata-only).
	if _, ok := in.GetSize(); ok && f.fs.readOnly {
		return syscall.EROFS
	}

	metaInode, err := f.fs.Meta().GetInode(ctx, f.inodeID)
	if err != nil {
		rec.IncOpError("setattr")
		return syscall.EIO
	}

	if size, ok := in.GetSize(); ok {
		newSize := int64(size)
		// BufferedFile.Truncate trims the buffer to newSize (dropping buffers
		// and spill files beyond the new base, trimming the last chunk) and
		// grows it with holes when extending. The committed inode Size is
		// updated right away so the next Flush persists the truncated size.
		f.buffered.Truncate(newSize, metaInode.Size)
		metaInode.Size = newSize
	}

	if mode, ok := in.GetMode(); ok {
		metaInode.Mode = mode
	}
	var chown bool
	if uid, ok := in.GetUID(); ok {
		metaInode.UID = uid
		chown = true
	}
	if gid, ok := in.GetGID(); ok {
		metaInode.GID = gid
		chown = true
	}
	// POSIX: chown always clears setuid/setgid bits (security requirement
	// to prevent privilege escalation).
	if chown && metaInode.Mode&(sIsuid|sIsgid) != 0 {
		metaInode.Mode &^= sIsuid | sIsgid
	}
	metaInode.MTime = time.Now().UnixNano()
	metaInode.CTime = time.Now().UnixNano()

	if err := f.fs.Meta().UpdateInode(ctx, metaInode); err != nil {
		rec.IncOpError("setattr")
		return syscall.EIO
	}

	out.Attr = inodeMetaToAttr(metaInode)
	return 0
}

// Allocate implements posix_fallocate(3) and friends (NodeAllocater). In the
// object-store model there are no physical blocks to reserve, so allocation is
// expressed purely through the byte-range semantics the kernel/userland
// expects:
//
//   - default (mode 0): extend the logical file to cover [off, off+size),
//     zero-filling the new region (same truncate-extend path as Setattr).
//   - FALLOC_FL_ZERO_RANGE: zero [off, off+size) in place, extending the file
//     (and logical Size) if the range runs past the current end.
//   - FALLOC_FL_PUNCH_HOLE: zero [off, off+size) clamped to the current file
//     length (no true holes exist in an object store; zero-filling yields the
//     POSIX-visible "reads as zero" result without shrinking Size).
//   - FALLOC_FL_KEEP_SIZE is honored with the default path: the physical
//     (buffered) length grows to off+size while the logical Size stays at its
//     committed value. Extra physical bytes beyond Size are harmless on Flush
//     (the multi-chunk ChunkMap covers only the logical length).
//
// Range-manipulation flags (COLLAPSE/INSERT/UNSHARE) are not supported and
// return EOPNOTSUPP. The byte-range state machine lives in
// chunkstore.BufferedFile (TouchRange / ZeroRange), which owns the buffer
// lock, hydration, dirty marking, mtime bumping, and Size growth; this glue
// only fetches the inode, picks the mode path, and persists via UpdateInode.
func (f *DFSFile) Allocate(ctx context.Context, fh fs.FileHandle, off uint64, size uint64, mode uint32) syscall.Errno {
	if f.fs.readOnly {
		return syscall.EROFS
	}
	rec := recorderFor(f.recorder)
	rec.IncOp("allocate")

	// The kernel passes off/size as unsigned; reject anything that overflows
	// the int64 file model or the int buffer indexing.
	if off > uint64(1<<63-1) || size > uint64(1<<63-1) || off+size > uint64(1<<63-1) {
		return syscall.EFBIG
	}

	// Reject unsupported range-manipulation modes outright before touching any
	// state.
	if mode&^(fallocKeepSize|fallocZeroRange|fallocPunchHole) != 0 {
		return syscall.EOPNOTSUPP
	}

	metaInode, err := f.fs.Meta().GetInode(ctx, f.inodeID)
	if err != nil {
		rec.IncOpError("allocate")
		return syscall.EIO
	}

	switch {
	case mode&fallocPunchHole != 0:
		// PUNCH_HOLE never extends: clamp the window to the current file length,
		// zero-fill in place, and never touch Size.
		if err := f.buffered.ZeroRange(ctx, metaInode, off, size, false); err != nil {
			rec.IncOpError("allocate")
			return syscall.EIO
		}
	case mode&fallocZeroRange != 0:
		// ZERO_RANGE zeroes in place and grows the file (and, absent KEEP_SIZE,
		// the logical size) when the window runs past the current end.
		if err := f.buffered.ZeroRange(ctx, metaInode, off, size, mode&fallocKeepSize == 0); err != nil {
			rec.IncOpError("allocate")
			return syscall.EIO
		}
	default:
		// Default (preallocate) path: extend the buffered image to cover
		// [off, off+size), honoring KEEP_SIZE by leaving the logical Size
		// untouched.
		f.buffered.TouchRange(ctx, metaInode, int64(off), int64(off+size), mode&fallocKeepSize != 0)
	}

	if err := f.fs.Meta().UpdateInode(ctx, metaInode); err != nil {
		rec.IncOpError("allocate")
		return syscall.EIO
	}
	return 0
}

// Access evaluates the file's POSIX mode bits against the requesting caller.
func (f *DFSFile) Access(ctx context.Context, mask uint32) syscall.Errno {
	return checkPOSIXAccess(ctx, f.fs, f.inodeID, mask)
}

// OpenXAttr returns an xattr handle for this file.
func (f *DFSFile) OpenXAttr() *DFSXAttr {
	return &DFSXAttr{meta: f.fs.Meta(), inodeID: f.inodeID}
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
