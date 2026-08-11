//go:build linux

package fuse

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
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
		bucket, gerr = f.fs.Meta().GetBucketByRoot(ctx, inode.BucketRoot)
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

	// fs is the owning filesystem root. It is a stable pointer across
	// metadata hot-swaps (SwapMetadata), so per-inode methods resolve the
	// current metadata service via fs.Meta() rather than caching it.
	fs         *DFSFileSystem
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

	// Write buffer for small writes before flush. Instead of one flat
	// []byte covering the whole file (OOM-prone for large files),
	// chunkBufs holds only the 64-MiB chunks that have actually been
	// written to. Memory use = accessed_chunks × 64 MiB, not file_size.
	// Flush merges dirty chunks with committed data and preserves
	// untouched committed chunks in the ChunkMap.
	mu        sync.RWMutex
	dirty     bool
	chunkBufs map[int64][]byte // chunkBaseOffset → data buffer
	dirtyMap  map[int64]bool   // chunkBaseOffset → has unflushed writes
	// dirtyBytes tracks the current memory held by chunkBufs.
	// Checked against f.fs.maxDirtyBytes before allocating new buffers.
	dirtyBytes int64
	// spilledChunks tracks dirty chunks that have been spilled from memory
	// to disk staging files. Key is chunk base offset, value is staging file path.
	spilledChunks map[int64]string
	// spilledBytes tracks total bytes held by staging files on disk.
	spilledBytes int64
	// logicalSize tracks the actual high-water mark of all writes (not
	// chunk-aligned). This is the true file size when dirty, independent
	// of the 64-MiB chunk buffer allocation. Must be called with f.mu held.
	logicalSize int64
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

// =====================================================================
// Chunk-level buffer helpers
// =====================================================================

// effectiveSize returns the file's logical size, accounting for any
// un-flushed tail that extends past the committed size. Must be called
// with f.mu held.
func (f *DFSFile) effectiveSize() int64 {
	maxEnd := int64(0)
	for base := range f.chunkBufs {
		end := base + int64(len(f.chunkBufs[base]))
		if end > maxEnd {
			maxEnd = end
		}
	}
	return maxEnd
}

// chunkBase returns the base offset of the 64-MiB chunk that contains off.
func chunkBase(off int64) int64 {
	return (off / MaxChunkPayload) * MaxChunkPayload
}

// getChunk returns the chunk buffer at base, allocating a zero-filled
// 64-MiB buffer if it doesn't exist yet. Must be called with f.mu held.
func (f *DFSFile) getChunk(base int64) []byte {
	if f.chunkBufs == nil {
		f.chunkBufs = make(map[int64][]byte)
	}
	c, ok := f.chunkBufs[base]
	if !ok {
		c = make([]byte, MaxChunkPayload)
		f.chunkBufs[base] = c
		f.dirtyBytes += int64(MaxChunkPayload)
	}
	return c
}

// markDirty marks the chunk at base as having unflushed writes.
func (f *DFSFile) markDirty(base int64) {
	if f.dirtyMap == nil {
		f.dirtyMap = make(map[int64]bool)
	}
	f.dirtyMap[base] = true
	f.dirty = true
}

// clearChunks releases all chunk buffers and dirty state. Called after
// a successful Flush or on Release. Also cleans up any staging files
// left by disk spill.
func (f *DFSFile) clearChunks() {
	f.cleanupStaging()
	f.chunkBufs = nil
	f.dirtyMap = nil
	f.dirty = false
	f.dirtyBytes = 0
	f.spilledBytes = 0
	f.logicalSize = 0
}

// stagingPath returns the staging file path for a spilled chunk.
func (f *DFSFile) stagingPath(base int64) string {
	return fmt.Sprintf("%s/%d_%d.dat", f.fs.writeStagingDir, f.inodeID, base)
}

// spillToDisk moves the dirty chunk buffer at base from memory to a disk
// staging file. The caller must hold f.mu. The chunk is removed from
// chunkBufs and the global dirty counter is decremented.
func (f *DFSFile) spillToDisk(base int64) error {
	if f.fs.writeStagingDir == "" {
		return fmt.Errorf("spill: staging dir not configured")
	}
	buf, ok := f.chunkBufs[base]
	if !ok {
		return nil // nothing to spill
	}
	path := f.stagingPath(base)
	if err := os.WriteFile(path, buf, 0600); err != nil {
		return fmt.Errorf("spill chunk %d: %w", base, err)
	}
	delete(f.chunkBufs, base)
	f.dirtyBytes -= int64(len(buf))
	f.spilledBytes += int64(len(buf))
	f.fs.globalDirtyBytes.Add(-int64(len(buf)))
	if f.spilledChunks == nil {
		f.spilledChunks = make(map[int64]string)
	}
	f.spilledChunks[base] = path
	return nil
}

// loadFromDisk loads a spilled chunk from its staging file back into memory.
// The caller must hold f.mu. Returns the loaded buffer.
func (f *DFSFile) loadFromDisk(base int64) ([]byte, error) {
	path, ok := f.spilledChunks[base]
	if !ok {
		return nil, fmt.Errorf("loadFromDisk: base %d not in staging", base)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("load staging %d: %w", base, err)
	}
	// Remove staging file and tracking state.
	_ = os.Remove(path)
	delete(f.spilledChunks, base)
	f.spilledBytes -= int64(len(data))
	// Put back into memory — use getChunk's raw allocation to avoid
	// triggering another spill loop.
	if f.chunkBufs == nil {
		f.chunkBufs = make(map[int64][]byte)
	}
	f.chunkBufs[base] = data
	f.dirtyBytes += int64(len(data))
	f.fs.globalDirtyBytes.Add(int64(len(data)))
	return data, nil
}

// cleanupStaging removes all staging files for this inode. Called on
// clearChunks (flush/release) to ensure no orphans are left on disk.
// MUST be called with f.mu held (all callers go through Flush which
// acquires f.mu).
func (f *DFSFile) cleanupStaging() {
	for base, path := range f.spilledChunks {
		_ = os.Remove(path)
		delete(f.spilledChunks, base)
	}
	f.spilledBytes = 0
}

// spillOldestChunk spills the dirty chunk with the lowest base offset
// from memory to disk staging. Returns nil if no spill was needed.
// No-op if staging is not configured or no chunks can be spilled.
// Must be called with f.mu held.
//
// DESIGN NOTE: when the global dirty budget is exceeded, we only spill
// from the current file (the one pushing over the limit), not from
// other files. This matches the Linux kernel's vm.dirty_ratio behavior:
// the writing task that pushes over the limit is throttled, not other
// tasks' dirty pages. Cross-file eviction is possible but adds lock
// ordering complexity with minimal benefit — the current-file approach
// is sufficient backpressure.
func (f *DFSFile) spillOldestChunk() error {
	if f.fs.writeStagingDir == "" || len(f.chunkBufs) == 0 {
		return nil
	}
	// Find the dirty chunk with the lowest base offset.
	var oldest int64 = -1
	for base := range f.dirtyMap {
		if oldest < 0 || base < oldest {
			oldest = base
		}
	}
	if oldest < 0 {
		return nil // no dirty chunks to spill
	}
	return f.spillToDisk(oldest)
}

// recordFlushAttempt records a flush state transition in the write-attempt
// ledger. This is best-effort: failure is logged but does not fail the flush,
// because the ledger is for observability and crash recovery — not for
// correctness (the inode update is the correctness boundary).
func (f *DFSFile) recordFlushAttempt(ctx context.Context, attemptID string, meta *metadata.InodeMeta, state metadata.WriteAttemptState, lastErr string) {
	chunks := make([]metadata.ChunkRef, len(meta.ChunkMap))
	copy(chunks, meta.ChunkMap)
	_ = f.fs.Meta().PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:         attemptID,
		Bucket:     f.fs.bucketName,
		InodeID:    f.inodeID,
		InodeCTime: meta.CTime,
		Chunks:     chunks,
		State:      state,
		LastError:  lastErr,
	})
}

// cleanStagingDir removes all files in the staging directory. Called once
// at Mount time to clean up orphaned files left by previous runs or crashes.
// Files in active use belong to the current process (PID-unique path) or
// have been cleaned by clearChunks, so this only runs at startup.
func cleanStagingDir(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return // dir doesn't exist or can't be read — nothing to clean
	}
	for _, e := range entries {
		if !e.IsDir() {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

// ensureChunksLoadedInRange is DEPRECATED — writes no longer need pre-loading.
// Flush handles merging dirty data with committed chunks.
func (f *DFSFile) ensureChunksLoadedInRange(ctx context.Context, start, end int64, rec MetricsRecorder) error {
	return nil // no-op: flush handles merge
}

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
// It clears the buffer AND marks it loaded=true. loaded=true means the
// (empty) buffer is the faithful image of the now-empty file, which is what
// stops ensureHydratedLocked from restoring committed bytes on a later
// partial write — truncation wins over hydration. The inode's committed Size
// is set to 0 right away; its old ChunkMap refs are left untouched and get
// reclaimed by the supersede+delete in Flush.
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

	f.mu.Lock()
	f.chunkBufs = nil
	f.dirtyMap = nil
	f.dirty = true // truncation needs to be flushed
	f.logicalSize = 0
	metaInode.Size = 0
	metaInode.MTime = time.Now().UnixNano()
	metaInode.CTime = time.Now().UnixNano()
	f.mu.Unlock()

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
// stale committed content. Memory use is proportional to the number of
// accessed chunks (×64 MiB), not the total file size.
func (f *DFSFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	rec := recorderFor(f.recorder)
	rec.IncOp("read")
	start := time.Now()
	defer func() { rec.ObserveOpLatency("read", time.Since(start)) }()

	var metaInode *metadata.InodeMeta
	if err := f.reliability.DoMeta("read", func() error {
		var gerr error
		metaInode, gerr = f.fs.Meta().GetInode(ctx, f.inodeID)
		return gerr
	}); err != nil {
		rec.IncOpError("read")
		return nil, syscall.EIO
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	// Effective file size accounts for un-flushed buffered tail that
	// extends past the committed size (e.g. after an O_APPEND write).
	size := int64(metaInode.Size)
	if f.logicalSize > size {
		size = f.logicalSize
	}
	if off >= size {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > size {
		end = size
	}

	// Fast path: empty ChunkMap + no dirty chunks = every byte is a hole.
	if len(metaInode.ChunkMap) == 0 && len(f.chunkBufs) == 0 {
		return fuse.ReadResultData(make([]byte, end-off)), 0
	}

	// readChunkRange reads committed data for [start, end) from the ChunkMap.
	// Returns the bytes and advances next. Caller holds f.mu.
	readChunkRange := func(start, end int64) []byte {
		result := make([]byte, 0, end-start)
		pos := start
		for pos < end {
			// Find committed chunk overlapping [pos, end)
			found := false
			for _, cref := range metaInode.ChunkMap {
				cEnd := cref.Offset + int64(cref.Length)
				if cEnd <= pos || cref.Offset >= end {
					continue
				}
				// Fetch committed payload
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
						chunk, gerr = f.fs.Meta().GetChunk(ctx, cref.ID)
						return gerr
					}); err != nil {
						return nil
					}
					if err := f.reliability.DoChunk("read", func() error {
						var gerr error
						payload, gerr = f.chunkStore.ReadChunk(ctx, chunk)
						return gerr
					}); err != nil {
						return nil
					}
					if f.cache != nil {
						f.cache.Add(uint64(cref.ID), payload)
					}
				}
				// Zero-fill gap before this chunk
				if cref.Offset > pos {
					gap := cref.Offset - pos
					if gap > end-pos {
						gap = end - pos
					}
					result = append(result, make([]byte, gap)...)
					pos += gap
				}
				relStart := pos - cref.Offset
				relEnd := end - cref.Offset
				if relEnd > int64(cref.Length) {
					relEnd = int64(cref.Length)
				}
				if relEnd > int64(len(payload)) {
					relEnd = int64(len(payload))
				}
				if relStart < relEnd {
					result = append(result, payload[relStart:relEnd]...)
					pos += relEnd - relStart
				}
				found = true
				break
			}
			if !found {
				// Hole: no chunk covers this region
				gap := end - pos
				result = append(result, make([]byte, gap)...)
				pos += gap
			}
		}
		return result
	}

	// Walk the read range in 64-MiB chunk steps. For each step, check
	// chunkBufs (dirty data) first, then the committed ChunkMap, then
	// zero-fill holes.
	out := make([]byte, 0, end-off)
	next := off
	for next < end {
		base := chunkBase(next)
		n := end - next
		if n > MaxChunkPayload {
			n = MaxChunkPayload
		}

		// Check dirty chunk buffer first (authoritative for unflushed writes).
		// Buffers have committed data loaded, so they are complete.
		if f.chunkBufs != nil {
			if buf, ok := f.chunkBufs[base]; ok {
				within := next - base
				if within < 0 {
					within = 0
				}
				limit := int64(len(buf)) - within
				if n > limit {
					n = limit
				}
				if n > 0 {
					out = append(out, buf[within:within+n]...)
				}
				next += n
				continue
			}
		}

		// No dirty buffer — read committed data directly
		committed := readChunkRange(next, next+n)
		if committed != nil {
			out = append(out, committed...)
			next += int64(len(committed))
		} else {
			// Error or hole — zero fill
			out = append(out, make([]byte, n)...)
			next += n
		}
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
	dfs := f.fs
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return 0, toErrno(err)
	}
	rec := recorderFor(f.recorder)
	rec.IncOp("write")
	start := time.Now()
	defer func() { rec.ObserveOpLatency("write", time.Since(start)) }()

	f.mu.Lock()
	defer f.mu.Unlock()
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
	start := time.Now()
	defer func() { rec.ObserveOpLatency("write", time.Since(start)) }()

	f.mu.Lock()
	defer f.mu.Unlock()

	// Append position = max of logical dirty size and committed size.
	// When logicalSize is 0 (after flush or fresh open), we need to
	// fetch the committed size from metadata to know where to append.
	tail := f.logicalSize
	if tail == 0 {
		metaInode, err := f.fs.Meta().GetInode(ctx, f.inodeID)
		if err != nil {
			return 0, syscall.EIO
		}
		tail = int64(metaInode.Size)
	}
	return f.writeLocked(ctx, data, tail, rec)
}

// =====================================================================
// Buffer-image invariant guard
// =====================================================================
//
// DFSFile's core state invariant is:
//
//	loaded == true  ⟹  chunkBufs contains all committed chunks
//
// Every path that sets loaded=true must uphold it.

var bufferImageInvariantOn atomic.Bool

// EnableBufferImageInvariant turns on the debug invariant assertion. Only
// tests should call it; production stays off for zero overhead.
func EnableBufferImageInvariant() { bufferImageInvariantOn.Store(true) }

// assertBufferImageLocked verifies that every chunk marked dirty in dirtyMap
// has a corresponding buffer in chunkBufs. Must be called with f.mu held.
// It is a no-op when the invariant guard is off.
func (f *DFSFile) assertBufferImageLocked(ctx context.Context) {
	if !bufferImageInvariantOn.Load() || !f.dirty {
		return
	}
	for base := range f.dirtyMap {
		if _, ok := f.chunkBufs[base]; !ok {
			panic(fmt.Sprintf("DFSFile invariant violated: dirty chunk at offset %d has no buffer (inode %d)",
				base, f.inodeID))
		}
	}
}

// writeLocked applies a buffered write at the given offset. Must be called
// with f.mu held. When creating a new chunk buffer, committed data is loaded
// from the ChunkMap so that partial writes preserve the committed prefix/suffix.
func (f *DFSFile) writeLocked(ctx context.Context, data []byte, off int64, rec MetricsRecorder) (uint32, syscall.Errno) {
	end := off + int64(len(data))

	// Check dirty-memory limit before allocating new chunk buffers.
	// Count how many new chunks this write would touch (bases not yet in
	// chunkBufs). When the limit is exceeded, the oldest dirty chunk is
	// spilled to disk staging (if available) before returning ENOSPC.
	if max := f.fs.maxDirtyBytes; max > 0 {
		newBases := make(map[int64]struct{})
		for pos := off; pos < end; {
			base := chunkBase(pos)
			if _, exists := f.chunkBufs[base]; !exists {
				newBases[base] = struct{}{}
			}
			next := base + MaxChunkPayload
			if next > end {
				next = end
			}
			pos = next
		}
		needed := int64(len(newBases)) * int64(MaxChunkPayload)
		if f.dirtyBytes+needed > max {
			// Try spilling the oldest dirty chunk in this file to free memory.
			if err := f.spillOldestChunk(); err != nil {
				rec.IncStagingSpillErr()
				logf("write: spill failed: %v", err)
			} else if len(f.spilledChunks) > 0 {
				rec.IncStagingSpill()
			}
			// Also try spilling if global budget is exceeded.
			if f.fs.globalDirtyBudget > 0 && f.fs.globalDirtyBytes.Load()+needed > f.fs.globalDirtyBudget {
				if err := f.spillOldestChunk(); err != nil {
					rec.IncStagingSpillErr()
				} else if len(f.spilledChunks) > 0 {
					rec.IncStagingSpill()
				}
			}
			if f.dirtyBytes+needed > max {
				return 0, syscall.ENOSPC
			}
		}
	}

	// For each chunk touched by this write, ensure the buffer exists and
	// has committed data loaded (so partial writes don't zero committed bytes).
	for pos := off; pos < end; {
		base := chunkBase(pos)
		if _, exists := f.chunkBufs[base]; !exists {
			// New buffer — load committed data for this chunk first
			f.loadCommittedChunkLocked(ctx, base, rec)
		}
		pos = (base + MaxChunkPayload)
		if pos > end {
			pos = end
		}
	}

	for pos := off; pos < end; {
		base := chunkBase(pos)
		buf := f.getChunk(base)
		within := pos - base
		n := int64(len(data)) - (pos - off)
		if within+n > int64(len(buf)) {
			n = int64(len(buf)) - within
		}
		copy(buf[within:within+n], data[pos-off:pos-off+n])
		f.markDirty(base)
		pos += n
	}
	// Track the actual high-water mark of writes (not chunk-aligned).
	// logicalSize is initialized to the committed size at Open, so a partial
	// overwrite at a nonzero offset never shrinks the file below its
	// committed extent without a per-write metadata round-trip.
	if end > f.logicalSize {
		f.logicalSize = end
	}
	f.assertBufferImageLocked(ctx)
	return uint32(len(data)), 0
}

// loadCommittedChunkLocked loads committed chunk data for the given chunk base
// into the chunk buffer. This ensures that partial writes preserve committed
// bytes that were already flushed. Must be called with f.mu held.
func (f *DFSFile) loadCommittedChunkLocked(ctx context.Context, base int64, rec MetricsRecorder) {
	metaInode, err := f.fs.Meta().GetInode(ctx, f.inodeID)
	if err != nil {
		return // can't load — buffer stays zero-filled (safe for new files)
	}
	// Collect every committed chunk overlapping this base and merge them in
	// offset order. A single 64 MiB base can hold multiple committed refs
	// (small writes across flushes each become their own ref); stopping at the
	// first would zero-fill the others and lose committed data.
	overlaps := make([]metadata.ChunkRef, 0, 2)
	for _, cref := range metaInode.ChunkMap {
		cEnd := cref.Offset + int64(cref.Length)
		if cEnd > base && cref.Offset < base+MaxChunkPayload {
			overlaps = append(overlaps, cref)
		}
	}
	if len(overlaps) == 0 {
		return
	}
	// Sort by offset (then ID for determinism) so later refs overwrite earlier
	// ones at overlapping ranges, matching the latest-version-wins semantics.
	sort.Slice(overlaps, func(i, j int) bool {
		if overlaps[i].Offset != overlaps[j].Offset {
			return overlaps[i].Offset < overlaps[j].Offset
		}
		return overlaps[i].ID < overlaps[j].ID
	})
	for _, cref := range overlaps {
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
				chunk, gerr = f.fs.Meta().GetChunk(ctx, cref.ID)
				return gerr
			}); err != nil {
				continue
			}
			if err := f.reliability.DoChunk("read", func() error {
				var gerr error
				payload, gerr = f.chunkStore.ReadChunk(ctx, chunk)
				return gerr
			}); err != nil {
				continue
			}
			if f.cache != nil {
				f.cache.Add(uint64(cref.ID), payload)
			}
		}
		buf := f.getChunk(base)
		rel := cref.Offset - base
		// Cap at the ref's declared live length: the payload may hold stale
		// bytes beyond cref.Length (e.g. a reused/legacy chunk), and copying
		// them would resurrect data outside the ref's extent.
		n := int64(cref.Length)
		if n > int64(len(payload)) {
			n = int64(len(payload))
		}
		if rel+n > int64(len(buf)) {
			n = int64(len(buf)) - rel
		}
		copy(buf[rel:rel+n], payload[:n])
	}
}

// ensureHydratedLocked is DEPRECATED — flush handles merge.
func (f *DFSFile) ensureHydratedLocked(ctx context.Context, rec MetricsRecorder) error {
	return nil
}

// loadChunkRangeLocked is DEPRECATED — flush handles merge.
func (f *DFSFile) loadChunkRangeLocked(ctx context.Context, rec MetricsRecorder, metaInode *metadata.InodeMeta) error {
	return nil
}

// Flush pushes the in-memory buffer to the chunk store and updates
// the inode's ChunkMap + size. It is idempotent: a second Flush on
// the same dirty buffer is a no-op (a clean Flush never re-writes).
//
// Only dirty chunks are written. Untouched committed chunks are preserved
// in the new ChunkMap without being loaded into memory — this keeps memory
// proportional to dirty data, not file size.
func (f *DFSFile) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	dfs := f.fs
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return toErrno(err)
	}
	rec := recorderFor(f.recorder)
	rec.IncOp("flush")
	start := time.Now()
	defer func() { rec.ObserveOpLatency("flush", time.Since(start)) }()

	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.dirty {
		return 0
	}

	// 路径锁：串行化同一 inode 的并发 Flush，防止不同 DFSFile
	// 实例（go-fuse inode cache eviction 后重建）交叉写入。
	unlock := f.reliability.LockInode(uint64(f.inodeID))
	defer unlock()

	// Read the current inode once. Its ChunkMap holds the OLD chunk
	// refs we will supersede at the end of this flush.
	var metaInode *metadata.InodeMeta
	if err := f.reliability.DoMeta("flush", func() error {
		var gerr error
		metaInode, gerr = f.fs.Meta().GetInode(ctx, f.inodeID)
		return gerr
	}); err != nil {
		rec.IncOpError("flush")
		return syscall.EIO
	}
	oldRefs := make([]metadata.ChunkRef, len(metaInode.ChunkMap))
	copy(oldRefs, metaInode.ChunkMap)

	// Write-attempt ledger: record flush state transitions for
	// observability and crash recovery. The ObjectWriteRecoveryWorker
	// (shared with S3) picks up incomplete attempts and cleans up
	// orphaned chunks.
	attemptID := fmt.Sprintf("fuse-%d-%d", f.inodeID, time.Now().UnixNano())
	f.recordFlushAttempt(ctx, attemptID, metaInode, metadata.WriteAttemptPending, "")

	policy := f.resolveChunkPolicy(ctx, metaInode)

	// Determine the file size to persist. logicalSize tracks the high-water
	// mark of writes since the last clearChunks() (which resets it to 0), so it
	// can be 0 (no dirty tail - use committed size), larger than committed (a
	// write extended the file - use logicalSize), OR smaller than committed (a
	// partial in-place overwrite that didn't reach the old EOF). Taking the max
	// prevents that last case from shrinking the file and silently dropping the
	// committed tail: e.g. pwrite(fd, 5B, offset=100) on a 200-byte file sets
	// logicalSize=105, and without the max the committed bytes [105,200) would
	// be truncated away. Legitimate truncation-down goes through Setattr, which
	// commits the new smaller Size before the next Flush, so max() never blocks
	// it. This mirrors AppendWrite's own max(logicalSize, committed) rule.
	committedSize := int64(metaInode.Size)
	size := f.logicalSize
	if committedSize > size {
		size = committedSize
	}

	// Collect dirty chunk bases and sort them for deterministic allocation.
	dirtyBases := make([]int64, 0, len(f.dirtyMap))
	for base := range f.dirtyMap {
		dirtyBases = append(dirtyBases, base)
	}
	sort.Slice(dirtyBases, func(i, j int) bool { return dirtyBases[i] < dirtyBases[j] })

	// For each dirty chunk: merge dirty buffer with committed data (if any),
	// allocating a new chunk ID. Only chunks that are actually dirty are
	// loaded — untouched committed chunks are preserved as-is.
	type writtenRef struct {
		base    int64
		data    []byte
		chunkID metadata.ChunkID
	}
	written := make([]writtenRef, 0, len(dirtyBases))

	// Batch-allocate chunk IDs for all dirty chunks.
	offsets := make([]int64, len(dirtyBases))
	for i, base := range dirtyBases {
		end := base + MaxChunkPayload
		if end > size {
			end = size
		}
		offsets[i] = base
		_ = end // chunk length derived later from actual data
	}
	if len(offsets) > 0 {
		chunks := make([]*metadata.ChunkMeta, 0, len(offsets))
		for start := 0; start < len(offsets); {
			end := start + metadata.MaxChunkAllocationBatch
			if end > len(offsets) {
				end = len(offsets)
			}
			var batch []*metadata.ChunkMeta
			if err := f.reliability.DoMeta("flush", func() error {
				var gerr error
				batch, gerr = f.fs.Meta().AllocateChunksBatch(ctx, f.inodeID, offsets[start:end], policy)
				return gerr
			}); err != nil {
				logf("flush: allocate chunk batch [%d:%d): %v", start, end, err)
				rec.IncOpError("flush")
				return syscall.EIO
			}
			chunks = append(chunks, batch...)
			start = end
		}
		f.recordFlushAttempt(ctx, attemptID, metaInode, metadata.WriteAttemptChunksAllocated, "")

		// Write each dirty chunk. Buffers already have committed data loaded
		// (via loadCommittedChunkLocked), so they are complete images.
		for i, base := range dirtyBases {
			end := base + MaxChunkPayload
			if end > size {
				end = size
			}
			chunkLen := int(end - base)

			// Use the dirty buffer directly — it has committed data merged in.
			// If the chunk was spilled to disk during a memory-pressure event,
			// load it back from the staging file. A load failure aborts the
			// entire flush — zero-filling here would silently corrupt data.
			var chunkData []byte
			if buf, ok := f.chunkBufs[base]; ok {
				n := chunkLen
				if n > len(buf) {
					n = len(buf)
				}
				chunkData = buf[:n]
			} else if _, spilled := f.spilledChunks[base]; spilled {
				loaded, err := f.loadFromDisk(base)
				if err != nil {
					logf("flush: load spilled chunk %d: %v — aborting", base, err)
					rec.IncOpError("flush")
					return syscall.EIO
				}
				rec.IncStagingLoad()
				n := chunkLen
				if n > len(loaded) {
					n = len(loaded)
				}
				chunkData = loaded[:n]
			} else {
				chunkData = make([]byte, chunkLen) // hole: all zeros
			}

			// Write, commit, seal the new chunk.
			chunk := chunks[i]
			if err := f.reliability.DoChunk("flush", func() error {
				return f.chunkStore.WriteChunk(ctx, chunk, chunkData)
			}); err != nil {
				logf("flush: write chunk %d: %v", chunk.ID, err)
				rec.IncOpError("flush")
				return syscall.EIO
			}
			checksum := crc32.ChecksumIEEE(chunkData)
			if err := f.reliability.DoMeta("flush", func() error {
				return f.fs.Meta().CommitChunk(ctx, chunk.ID, checksum)
			}); err != nil {
				logf("flush: commit chunk %d: %v", chunk.ID, err)
				rec.IncOpError("flush")
				return syscall.EIO
			}
			if err := f.reliability.DoMeta("flush", func() error {
				return f.fs.Meta().SealChunk(ctx, chunk.ID)
			}); err != nil {
				logf("flush: seal chunk %d: %v", chunk.ID, err)
			}
			written = append(written, writtenRef{
				base:    base,
				data:    chunkData,
				chunkID: chunk.ID,
			})
		}
	}

	// All chunks written to datanode (durable). Record before updating inode.
	f.recordFlushAttempt(ctx, attemptID, metaInode, metadata.WriteAttemptChunksDurable, "")

	// Build new ChunkMap: preserve untouched committed chunks + add written chunks.
	// Untouched committed chunks are NOT loaded into memory — just copied as refs.
	writtenSet := make(map[int64]bool, len(written))
	for _, w := range written {
		writtenSet[w.base] = true
	}
	newRefs := make([]metadata.ChunkRef, 0, len(oldRefs)+len(written))
	// First: all untouched committed chunks (not in dirty set)
	for _, cref := range oldRefs {
		if !writtenSet[cref.Offset] {
			newRefs = append(newRefs, cref)
		}
	}
	// Second: all newly written chunks
	for _, w := range written {
		newRefs = append(newRefs, metadata.ChunkRef{
			ID:      w.chunkID,
			Offset:  w.base,
			Length:  int32(len(w.data)),
			Version: 1,
		})
	}

	// Wholesale-replace the ChunkMap + size + mtime.
	metaInode.ChunkMap = newRefs
	metaInode.Size = int64(size)
	metaInode.MTime = time.Now().UnixNano()

	if err := f.reliability.DoMeta("flush", func() error {
		return f.fs.Meta().UpdateInode(ctx, metaInode)
	}); err != nil {
		f.recordFlushAttempt(ctx, attemptID, metaInode, metadata.WriteAttemptRecoveryNeeded, err.Error())
		rec.IncOpError("flush")
		return syscall.EIO
	}

	// Flush succeeded: mark attempt committed so recovery worker knows
	// this inode's chunk references are authoritative.
	f.recordFlushAttempt(ctx, attemptID, metaInode, metadata.WriteAttemptCommitted, "")

	// Reclaim the superseded chunks.
	for _, cref := range oldRefs {
		if err := f.reliability.DoMeta("flush", func() error {
			return f.fs.Meta().DeleteChunk(ctx, cref.ID)
		}); err != nil {
			logf("flush: delete old chunk %d: %v", cref.ID, err)
		}
	}

	f.clearChunks()
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
	f.mu.Lock()
	if f.logicalSize > int64(out.Attr.Size) {
		out.Attr.Size = uint64(f.logicalSize)
	}
	f.mu.Unlock()
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
		f.mu.Lock()
		newSize := int64(size)
		oldEnd := f.logicalSize
		if oldEnd == 0 {
			oldEnd = metaInode.Size
		}

		if newSize < oldEnd {
			// Truncation DOWN: remove chunks beyond newSize, trim the last chunk.
			newBase := chunkBase(newSize)
			for base := range f.chunkBufs {
				if base > newBase {
					delete(f.chunkBufs, base)
					delete(f.dirtyMap, base)
				} else if base == newBase && newSize%MaxChunkPayload != 0 {
					// Trim the last chunk to the exact size
					f.chunkBufs[base] = f.chunkBufs[base][:newSize%MaxChunkPayload]
				}
			}
			f.dirty = true
			f.logicalSize = newSize
		} else if newSize > oldEnd {
			// Extension: new region is zeros (holes). No need to load
			// anything — the flush will create appropriate chunks.
			f.dirty = true
			f.logicalSize = newSize
		}
		// newSize == oldEnd: no buffer change
		metaInode.Size = newSize
		f.mu.Unlock()
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
// return EOPNOTSUPP. All paths take f.mu, hydrate a committed prefix when the
// buffer doesn't yet reflect it (so an extension never zeroes committed
// bytes), mark the file dirty, bump mtime, and persist Size when it changes.
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

	// PUNCH_HOLE never extends: clamp the window to the current file length,
	// zero-fill in place, and never touch Size.
	if mode&fallocPunchHole != 0 {
		return f.zeroRange(ctx, rec, metaInode, off, size, false)
	}
	// ZERO_RANGE zeroes in place and grows the file (and, absent KEEP_SIZE, the
	// logical size) when the window runs past the current end.
	if mode&fallocZeroRange != 0 {
		return f.zeroRange(ctx, rec, metaInode, off, size, mode&fallocKeepSize == 0)
	}

	// Default (preallocate) path: extend the file to cover [off, off+size),
	// honoring KEEP_SIZE by leaving the logical Size untouched.
	f.mu.Lock()
	end := off + size
	ef := f.logicalSize
	if metaInode.Size > ef {
		ef = metaInode.Size
	}
	// Extend chunkBufs to cover the new range, loading committed data first
	// so that untouched regions preserve their committed content.
	for base := chunkBase(int64(off)); base < int64(end); base += MaxChunkPayload {
		if _, exists := f.chunkBufs[base]; !exists {
			f.loadCommittedChunkLocked(ctx, base, rec)
		}
		_ = f.getChunk(base) // allocates zero-filled chunk if missing
	}
	f.dirty = true
	if mode&fallocKeepSize == 0 && int64(end) > ef {
		metaInode.Size = int64(end)
		f.logicalSize = int64(end)
	}
	metaInode.MTime = time.Now().UnixNano()
	f.mu.Unlock()

	if err := f.fs.Meta().UpdateInode(ctx, metaInode); err != nil {
		rec.IncOpError("allocate")
		return syscall.EIO
	}
	return 0
}

// zeroRange zero-fills [off, off+size). If extend is true (default allocate or
// ZERO_RANGE without KEEP_SIZE) the file (and logical Size) is grown to cover
// the window; otherwise (PUNCH_HOLE, or ZERO_RANGE with KEEP_SIZE) the window
// is clamped to the current length and Size is untouched. Returns the errno
// (0 on success). Callers must pass metaInode fetched under the inode's meta
// contract; this helper manages f.mu itself so the UpdateInode lands after the
// lock is released.
func (f *DFSFile) zeroRange(ctx context.Context, rec MetricsRecorder, metaInode *metadata.InodeMeta, off uint64, size uint64, extend bool) syscall.Errno {
	f.mu.Lock()
	cur := f.logicalSize
	if metaInode.Size > cur {
		cur = metaInode.Size
	}
	end := off + size
	if !extend && int64(end) > cur {
		// Clamp to current committed + buffered length (never grow on punch).
		end = uint64(cur)
	}
	if end <= off {
		// Nothing to zero; still refresh mtime per POSIX and persist.
		metaInode.MTime = time.Now().UnixNano()
		f.mu.Unlock()
		if err := f.fs.Meta().UpdateInode(ctx, metaInode); err != nil {
			rec.IncOpError("allocate")
			return syscall.EIO
		}
		return 0
	}
	// Load committed chunks overlapping the zero range so the non-zeroed
	// portion of each affected chunk is preserved. Only the affected chunks
	// are loaded — not the entire file.
	for base := chunkBase(int64(off)); base < int64(end); base += MaxChunkPayload {
		if _, ok := f.chunkBufs[base]; ok {
			continue // already in buffer (from prior write)
		}
		// Find committed chunk for this base
		for _, cref := range metaInode.ChunkMap {
			cEnd := cref.Offset + int64(cref.Length)
			if cEnd <= base || cref.Offset >= base+MaxChunkPayload {
				continue
			}
			// Load this committed chunk into the buffer
			var payload []byte
			if f.cache != nil {
				if p, ok := f.cache.Get(uint64(cref.ID)); ok {
					payload = p
				}
			}
			if payload == nil {
				var chunk *metadata.ChunkMeta
				if err := f.reliability.DoMeta("flush", func() error {
					var gerr error
					chunk, gerr = f.fs.Meta().GetChunk(ctx, cref.ID)
					return gerr
				}); err != nil {
					f.mu.Unlock()
					rec.IncOpError("allocate")
					return syscall.EIO
				}
				if err := f.reliability.DoChunk("flush", func() error {
					var gerr error
					payload, gerr = f.chunkStore.ReadChunk(ctx, chunk)
					return gerr
				}); err != nil {
					f.mu.Unlock()
					rec.IncOpError("allocate")
					return syscall.EIO
				}
				if f.cache != nil {
					f.cache.Add(uint64(cref.ID), payload)
				}
			}
			buf := f.getChunk(base)
			rel := cref.Offset - base
			n := int64(len(payload))
			if rel+n > int64(len(buf)) {
				n = int64(len(buf)) - rel
			}
			copy(buf[rel:rel+n], payload[:n])
			break
		}
	}
	// Zero within each affected chunk.
	for base := chunkBase(int64(off)); base < int64(end); base += MaxChunkPayload {
		buf := f.getChunk(base)
		zeroStart := int64(off) - base
		if zeroStart < 0 {
			zeroStart = 0
		}
		zeroEnd := int64(end) - base
		if zeroEnd > int64(len(buf)) {
			zeroEnd = int64(len(buf))
		}
		for i := zeroStart; i < zeroEnd; i++ {
			buf[i] = 0
		}
		f.markDirty(base)
	}
	// Grow the logical Size only when the window runs past the file's current
	// extent. `cur` (captured at the top, before the buffer was grown to the
	// window) is the pre-operation extent = max(buffered tail, committed size);
	// the committed Size lags the buffer until Flush, so comparing against it
	// alone would shrink the logical size below buffered data when zeroing an
	// in-range window (truncating on the next whole-file Flush rebuild).
	if extend && int64(end) > cur {
		metaInode.Size = int64(end)
		f.logicalSize = int64(end)
	}
	metaInode.MTime = time.Now().UnixNano()
	f.mu.Unlock()

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
