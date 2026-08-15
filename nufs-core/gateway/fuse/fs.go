//go:build linux

// Package fuse provides a FUSE filesystem gateway for DFS.
package fuse

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// DFSFileSystem is the root filesystem backed by the DFS metadata service.
type DFSFileSystem struct {
	fs.Inode

	// metaAtomic holds the current metadata service. Use SwapMetadata to
	// hot-swap the backend (e.g. during remount) without unmounting FUSE.
	// All child inodes resolve meta via their stable fs pointer → fs.Meta()
	// → metaAtomic.Load(), so a swap is visible to every inode immediately.
	metaAtomic atomic.Pointer[metadataService]

	chunkStore chunkstore.ChunkStore

	// lockOwner is the per-process string used when acquiring advisory
	// file locks (commit 0: metadata: add advisory file lock service).
	// Each Open that returns a write handle acquires an exclusive lock
	// under this owner; Release drops it. Empty means "no locks" (used
	// in unit tests that have no lock manager).
	lockOwner string

	// chunkCache caches chunk payloads to avoid datanode round-trips.
	chunkCache *ChunkCache

	// recorder 记录 FUSE 操作指标。nil 时使用 noopMetricsRecorder。
	// 子 inode (DFSFile/DFSDir/DFSSymlink/DFSFifo) 通过稳定的 fs 指针取 Meta()。
	recorder MetricsRecorder

	// reliability 包装 retry + circuit breaker + 路径锁。
	// nil 时为 passthrough 模式（直接调用 fn），兼容旧测试。
	reliability *ReliabilityWrapper

	// bucketName scopes the mount to a single bucket. Root Readdir/Lookup and
	// all root mutating ops operate inside the bucket directly.
	bucketName string
	bucketRoot metadata.InodeID
	// owner is the RBAC principal for FUSE permission checks, derived from
	// the verified mount token (or --owner dev fallback).
	owner string

	// mountUID/mountGID override the UID/GID on newly created inodes.
	// 0 means "use the caller's uid/gid from the kernel" (no override).
	mountUID uint32
	mountGID uint32

	// readOnly when true denies all mutating FUSE operations with EROFS.
	// This is a client-side mount-level enforcement, independent of bucket
	// policy — useful for read-only mounts of writable buckets.
	readOnly bool

	// maxDirtyBytes limits the total memory used by per-file dirty write
	// buffers (the 64-MiB chunkBufs). When a file's dirty buffers would
	// exceed this threshold, new writes return ENOSPC to prevent OOM from
	// pathological sparse-write patterns (e.g. 100 × 64 MiB for 100 × 4K
	// writes into different chunks of the same file). 0 means no limit.
	maxDirtyBytes int64

	// globalDirtyBudget limits total dirty memory across ALL open files.
	// When exceeded, files spill oldest dirty chunks to writeStagingDir
	// (disk spill) before allocating new memory. 0 means no global limit.
	globalDirtyBudget int64
	// globalDirtyBytes tracks total dirty memory held by all DFSFiles.
	// Must only be accessed via atomic ops.
	globalDirtyBytes atomic.Int64
	// writeStagingDir is the directory for spilled dirty chunk files.
	// Files are named {inodeID}_{chunkBase}.dat. Empty means disk
	// spill is disabled (fallback to ENOSPC when per-file memory full).
	writeStagingDir string

	// Inode cache: metadata.InodeID -> *fs.Inode
	mu       sync.RWMutex
	inodeMap map[metadata.InodeID]*fs.Inode
}

// metadataService is an interface alias so atomic.Pointer can be used.
type metadataService = metadata.MetadataService

// chunkEventWatcher is the minimal interface that DFSFileSystem needs from
// its metadata client to subscribe to change events. If the implementation
// does not provide streaming (e.g. a local PebbleStore in tests), the
// filesystem runs without event-driven cache invalidation — a safe fallback.
type chunkEventWatcher interface {
	WatchEventsStream(ctx context.Context, prefix string) <-chan metadata.WatchEvent
}

// NewDFSFileSystem creates a new FUSE filesystem root.
// If cache is provided, it will be invalidated on chunk events received
// from the metadata service; otherwise the gateway relies on TTL-based
// expiry at the datanode layer.
// recorder 用于指标打点，nil 时使用 noopMetricsRecorder（关闭指标）。
// reliability 注入 retry+breaker+pathlock 能力，nil 时为 passthrough 模式。
func NewDFSFileSystem(meta metadata.MetadataService, chunkStore chunkstore.ChunkStore, cache *ChunkCache, recorder MetricsRecorder, reliability *ReliabilityWrapper) *DFSFileSystem {
	if recorder == nil {
		recorder = noopMetricsRecorder{}
	}
	fsys := &DFSFileSystem{
		chunkStore:  chunkStore,
		chunkCache:  cache,
		lockOwner:   fmt.Sprintf("fusegw-%d", os.Getpid()),
		inodeMap:    make(map[metadata.InodeID]*fs.Inode),
		recorder:    recorder,
		reliability: reliability,
	}
	fsys.metaAtomic.Store(&meta)
	// 把 recorder 注入到 chunkCache，统一缓存命中/未命中计数。
	if cache != nil {
		cache.recorder = recorder
	}
	// Kick off the event-driven cache invalidation loop if the metadata
	// service supports streaming watches.
	if cache != nil {
		if w, ok := meta.(chunkEventWatcher); ok {
			ctx := context.Background()
			go fsys.runCacheInvalidationLoop(ctx, w)
		}
	}
	return fsys
}

// Meta returns the current metadata service (atomic load, safe for concurrent use).
func (dfs *DFSFileSystem) Meta() metadata.MetadataService {
	return *dfs.metaAtomic.Load()
}

// SwapMetadata atomically replaces the metadata service and closes the old one.
// The swap is instantaneous, but the old client is closed synchronously within
// the swap, so the caller (remount handler) must ensure no in-flight I/O still
// references the old client when Close runs — any request that read the old
// pointer via Meta() before the swap and is still in flight could be severed.
func (dfs *DFSFileSystem) SwapMetadata(newMeta metadata.MetadataService) {
	old := *dfs.metaAtomic.Swap(&newMeta)
	if old != nil {
		old.Close()
	}
}

// checkAccess verifies the mount principal is allowed to perform perm on
// the given bucket. Returns nil when allowed. RBAC is only enforced when the
// mount carries a principal (owner); an empty owner means dev/local mounting
// with no authorization boundary (kept to preserve non-RBAC test setups).
//
// No policy is NOT open: GetBucketPolicy reports no-policy as access-denied,
// and IsAllowed defaults to deny, so a bucket with no explicit policy denies
// the mount rather than silently granting it. The bucket owner always has
// access once a policy exists (IsAllowed's owner shortcut).
func (dfs *DFSFileSystem) checkAccess(bucket string, perm metadata.Permission) error {
	if dfs.readOnly && perm == metadata.PermWrite {
		return syscall.EROFS
	}
	if dfs.owner == "" || bucket == "" {
		return nil
	}
	policy, err := dfs.Meta().GetBucketPolicy(context.Background(), bucket)
	if err != nil || policy == nil {
		logf("access denied: bucket=%s principal=%s perm=%s (no policy)", bucket, dfs.owner, perm)
		return syscall.EACCES
	}
	if policy.IsAllowed(metadata.Principal(dfs.owner), perm) {
		return nil
	}
	logf("access denied: bucket=%s principal=%s perm=%s", bucket, dfs.owner, perm)
	return syscall.EACCES
}

// toErrno converts a Go error to a FUSE errno. syscall errors pass through;
// anything else maps to EIO.
func toErrno(err error) syscall.Errno {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	return syscall.EIO
}

// runCacheInvalidationLoop consumes chunk-scoped events from the metadata
// service and removes the corresponding cache entries. It also watches
// inode-level events to evict stale size/mtime info held in the OS page
// cache. The loop is best-effort: if the stream ends (e.g. server restart)
// it simply returns and the cache eventually expires by itself.
func (fsys *DFSFileSystem) runCacheInvalidationLoop(ctx context.Context, w chunkEventWatcher) {
	for e := range w.WatchEventsStream(ctx, "chunk:") {
		chunkID, err := parseChunkID(e.Key)
		if err == nil {
			fsys.chunkCache.RemoveChunk(chunkID)
		}
	}
}

// parseChunkID parses "chunk:1234" into uint64(1234). Returns an error if
// the key format is unexpected; callers should just ignore bad keys.
func parseChunkID(key string) (uint64, error) {
	const prefix = "chunk:"
	if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
		return 0, fmt.Errorf("invalid chunk event key: %s", key)
	}
	var id uint64
	if _, err := fmt.Sscanf(key[len(prefix):], "%d", &id); err != nil {
		return 0, err
	}
	return id, nil
}

// MountOptions configures a DFS FUSE mount.
type MountOptions struct {
	Mountpoint  string
	Meta        metadata.MetadataService
	ChunkStore  chunkstore.ChunkStore
	Cache       *ChunkCache
	Recorder    MetricsRecorder
	Reliability *ReliabilityWrapper
	FUSEOpts    *fuse.MountOptions
	BucketName  string
	Owner       string // RBAC principal from verified token; empty = no RBAC boundary
	MountUID    uint32 // override UID on new inodes; 0 = use caller's uid
	MountGID    uint32 // override GID on new inodes; 0 = use caller's gid
	ReadOnly    bool   // deny all mutating operations (EROFS)
	// MaxDirtyBytes limits per-file dirty write buffer memory (default 1 GiB).
	// Exceeding this limit causes writes to return ENOSPC. 0 means no limit.
	MaxDirtyBytes int64
	// GlobalDirtyBudget limits total dirty memory across all open files.
	// 0 means no global limit (only per-file limits apply).
	GlobalDirtyBudget int64
	// WriteStagingDir is the directory for spilling dirty chunks to disk
	// when memory pressure exceeds GlobalDirtyBudget. Empty disables spill.
	WriteStagingDir string
}

// Mount mounts the DFS filesystem at the given mountpoint, scoped to the
// bucket (which must be non-empty). Returns the server and the filesystem
// root (for hot-swap operations).
func Mount(opts MountOptions) (*fuse.Server, *DFSFileSystem, error) {
	root := NewDFSFileSystem(opts.Meta, opts.ChunkStore, opts.Cache, opts.Recorder, opts.Reliability)
	root.owner = opts.Owner
	root.mountUID = opts.MountUID
	root.mountGID = opts.MountGID
	root.readOnly = opts.ReadOnly
	root.maxDirtyBytes = opts.MaxDirtyBytes
	if root.maxDirtyBytes <= 0 {
		root.maxDirtyBytes = 1 << 30 // 1 GiB default
	}
	root.globalDirtyBudget = opts.GlobalDirtyBudget
	if opts.WriteStagingDir != "" {
		if err := os.MkdirAll(opts.WriteStagingDir, 0700); err != nil {
			return nil, nil, fmt.Errorf("create write staging dir: %w", err)
		}
		root.writeStagingDir = opts.WriteStagingDir
	} else if root.globalDirtyBudget > 0 {
		// Global budget configured but no explicit staging dir: auto-create
		// a default so disk spill is enabled without extra configuration
		// (matches JuiceFS --cache-size behavior).
		root.writeStagingDir = filepath.Join(os.TempDir(), fmt.Sprintf("nufs-fuse-staging-%d", os.Getpid()))
		if err := os.MkdirAll(root.writeStagingDir, 0700); err != nil {
			return nil, nil, fmt.Errorf("create default write staging dir: %w", err)
		}
	}
	// Clean up orphaned staging files from previous runs or crashes.
	if root.writeStagingDir != "" {
		chunkstore.CleanStagingDir(root.writeStagingDir)
	}

	// Single-bucket mode: resolve bucket root inode.
	if opts.BucketName == "" {
		return nil, nil, fmt.Errorf("bucket is required")
	}
	bucket, err := opts.Meta.GetBucket(context.Background(), opts.BucketName)
	if err != nil {
		return nil, nil, fmt.Errorf("bucket %q: %w", opts.BucketName, err)
	}
	root.bucketName = opts.BucketName
	root.bucketRoot = bucket.RootInode

	fuseOpts := opts.FUSEOpts
	if fuseOpts == nil {
		fuseOpts = &fuse.MountOptions{
			AllowOther: false,
			Name:       "dfs",
			FsName:     "dfs",
		}
	}

	server, err := fs.Mount(opts.Mountpoint, root, &fs.Options{
		MountOptions: *fuseOpts,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("fuse mount: %w", err)
	}

	return server, root, nil
}

// applyMountOwner overrides UID/GID on a freshly created InodeMeta with the
// mount-level defaults (if set), then applies POSIX setgid inheritance: when
// the parent directory has S_ISGID, the child inherits the parent's gid
// (overriding the mount default), finally persisting the resulting inode.
// Called after MkDir/CreateFile/Symlink/Mknod, which create inodes with zero
// UID/GID. parent must be the immediate parent directory (fetched by callers);
// pass nil when the caller should NOT inherit setgid (e.g. symlinks) or when
// the parent could not be read.
func (dfs *DFSFileSystem) applyMountOwner(ctx context.Context, metaInode *metadata.InodeMeta, parent *metadata.InodeMeta) {
	inheritSetgid := parent != nil && parent.Mode&sIsgid != 0
	if dfs.mountUID == 0 && dfs.mountGID == 0 && !inheritSetgid {
		return
	}
	if dfs.mountUID != 0 {
		metaInode.UID = dfs.mountUID
	}
	if inheritSetgid {
		metaInode.GID = parent.GID
	} else if dfs.mountGID != 0 {
		metaInode.GID = dfs.mountGID
	}
	dfs.Meta().UpdateInode(ctx, metaInode)
}

// registerInode registers a metadata inode in the cache.
func (dfs *DFSFileSystem) registerInode(metaID metadata.InodeID, inode *fs.Inode) {
	dfs.mu.Lock()
	defer dfs.mu.Unlock()
	dfs.inodeMap[metaID] = inode
}

// getInode returns the cached FUSE inode for a metadata inode ID.
func (dfs *DFSFileSystem) getInode(metaID metadata.InodeID) *fs.Inode {
	dfs.mu.RLock()
	defer dfs.mu.RUnlock()
	return dfs.inodeMap[metaID]
}

// ========== Root inode: bucket-as-shared-root ==========

// Compile-time guards for root inode operations.
var _ = (fs.NodeReaddirer)((*DFSFileSystem)(nil))
var _ = (fs.NodeLookuper)((*DFSFileSystem)(nil))
var _ = (fs.NodeStatfser)((*DFSFileSystem)(nil))
var _ = (fs.NodeMkdirer)((*DFSFileSystem)(nil))
var _ = (fs.NodeRmdirer)((*DFSFileSystem)(nil))
var _ = (fs.NodeCreater)((*DFSFileSystem)(nil))
var _ = (fs.NodeUnlinker)((*DFSFileSystem)(nil))
var _ = (fs.NodeRenamer)((*DFSFileSystem)(nil))
var _ = (fs.NodeSymlinker)((*DFSFileSystem)(nil))
var _ = (fs.NodeLinker)((*DFSFileSystem)(nil))
var _ = (fs.NodeMknoder)((*DFSFileSystem)(nil))

// Mkdir creates a directory under the bucket root.
func (dfs *DFSFileSystem) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return nil, toErrno(err)
	}
	rec := recorderFor(dfs.recorder)
	rec.IncOp("mkdir")

	metaInode, err := dfs.Meta().MkDir(ctx, dfs.bucketRoot, name, mode)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			return nil, syscall.EEXIST
		}
		logf("mkdir error: %v", err)
		rec.IncOpError("mkdir")
		return nil, syscall.EIO
	}

	// setgid inheritance: when the parent directory (the bucket root) has
	// S_ISGID set, the new child inherits the parent's gid (POSIX semantics),
	// overriding the mount default. applyMountOwner applies it and persists.
	rootMeta, _ := dfs.Meta().GetInode(ctx, dfs.bucketRoot)
	dfs.applyMountOwner(ctx, metaInode, rootMeta)

	child := &DFSDir{fs: dfs, inodeID: metaInode.ID, recorder: rec}
	attr := inodeMetaToAttr(metaInode)
	inode := dfs.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFDIR, Ino: uint64(metaInode.ID)})
	out.Attr = attr
	return inode, 0
}

// Rmdir removes an empty directory under the bucket root.
func (dfs *DFSFileSystem) Rmdir(ctx context.Context, name string) syscall.Errno {
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return toErrno(err)
	}
	rec := recorderFor(dfs.recorder)
	rec.IncOp("rmdir")

	err := dfs.Meta().RmDir(ctx, dfs.bucketRoot, name)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) {
			return syscall.ENOENT
		}
		if errors.Is(err, metadata.ErrDirNotEmpty) {
			return syscall.ENOTEMPTY
		}
		logf("rmdir error: %v", err)
		rec.IncOpError("rmdir")
		return syscall.EIO
	}
	return 0
}

// Create creates a file under the bucket root.
func (dfs *DFSFileSystem) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return nil, nil, 0, toErrno(err)
	}
	rec := recorderFor(dfs.recorder)
	rec.IncOp("create")

	metaInode, err := dfs.Meta().CreateFile(ctx, dfs.bucketRoot, name, mode)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			metaInode, err = dfs.Meta().Lookup(ctx, dfs.bucketRoot, name)
			if err != nil {
				rec.IncOpError("create")
				return nil, nil, 0, syscall.EIO
			}
		} else {
			logf("create error: %v", err)
			rec.IncOpError("create")
			return nil, nil, 0, syscall.EIO
		}
	} else {
		// setgid inheritance: when the parent directory (the bucket root) has
		// S_ISGID set, the new child inherits the parent's gid (POSIX semantics),
		// overriding the mount default, and is persisted.
		rootMeta, _ := dfs.Meta().GetInode(ctx, dfs.bucketRoot)
		dfs.applyMountOwner(ctx, metaInode, rootMeta)
	}

	file := newDFSFile(dfs, metaInode, rec)
	attr := inodeMetaToAttr(metaInode)
	inode := dfs.NewInode(ctx, file, fs.StableAttr{Mode: fuse.S_IFREG, Ino: uint64(metaInode.ID)})
	out.Attr = attr
	if directIOEnabled.Load() {
		fuseFlags = fuse.FOPEN_DIRECT_IO
	}
	return inode, file, fuseFlags, 0
}

// Mknod creates a special node under the bucket root.
func (dfs *DFSFileSystem) Mknod(ctx context.Context, name string, mode uint32, dev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return nil, toErrno(err)
	}
	rec := recorderFor(dfs.recorder)
	rec.IncOp("mknod")

	var ftype metadata.FileType
	var rdev uint32
	switch mode & syscall.S_IFMT {
	case syscall.S_IFIFO:
		ftype = metadata.FileFIFO
	case syscall.S_IFCHR:
		ftype = metadata.FileCharDevice
		rdev = dev
	case syscall.S_IFBLK:
		ftype = metadata.FileBlockDevice
		rdev = dev
	case syscall.S_IFSOCK:
		ftype = metadata.FileSocket
	default:
		return nil, syscall.EINVAL
	}

	metaInode, err := dfs.Meta().CreateNode(ctx, dfs.bucketRoot, name, ftype, mode&07777, rdev)
	if errno := errnoForCreateNode(err); errno != 0 {
		if errno == syscall.EIO {
			logf("mknod error: %v", err)
			rec.IncOpError("mknod")
		}
		return nil, errno
	}
	// setgid inheritance: when the parent directory (the bucket root) has
	// S_ISGID set, the new child inherits the parent's gid (POSIX semantics),
	// overriding the mount default, and is persisted.
	rootMeta, _ := dfs.Meta().GetInode(ctx, dfs.bucketRoot)
	dfs.applyMountOwner(ctx, metaInode, rootMeta)
	attr := inodeMetaToAttr(metaInode)
	inode := dfs.NewInode(ctx, &DFSFifo{fs: dfs, inodeID: metaInode.ID, recorder: rec}, fs.StableAttr{
		Mode: attr.Mode & syscall.S_IFMT,
		Ino:  uint64(metaInode.ID),
	})
	out.Attr = attr
	return inode, 0
}

// Unlink removes an entry under the bucket root.
func (dfs *DFSFileSystem) Unlink(ctx context.Context, name string) syscall.Errno {
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return toErrno(err)
	}
	rec := recorderFor(dfs.recorder)
	rec.IncOp("unlink")

	err := dfs.Meta().Unlink(ctx, dfs.bucketRoot, name)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) {
			return syscall.ENOENT
		}
		logf("unlink error: %v", err)
		rec.IncOpError("unlink")
		return syscall.EIO
	}
	return 0
}

// Rename renames/moves an entry under the bucket root. newParent is always a
// DFSDir (created under this root), so we derive its inode ID from the embedder.
func (dfs *DFSFileSystem) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return toErrno(err)
	}
	rec := recorderFor(dfs.recorder)
	rec.IncOp("rename")

	newParentInode := dfs.bucketRoot
	switch p := newParent.(type) {
	case *DFSDir:
		newParentInode = p.inodeID
	case *DFSFileSystem:
		// renaming within the root itself
	}

	err := dfs.Meta().Rename(ctx, dfs.bucketRoot, name, newParentInode, newName)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) {
			return syscall.ENOENT
		}
		logf("rename error: %v", err)
		rec.IncOpError("rename")
		return syscall.EIO
	}
	return 0
}

// Symlink creates a symlink under the bucket root.
func (dfs *DFSFileSystem) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return nil, toErrno(err)
	}
	rec := recorderFor(dfs.recorder)
	rec.IncOp("symlink")

	metaInode, err := dfs.Meta().Symlink(ctx, dfs.bucketRoot, name, target)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			return nil, syscall.EEXIST
		}
		logf("symlink error: %v", err)
		rec.IncOpError("symlink")
		return nil, syscall.EIO
	}

	// Symlinks do not inherit a setgid parent's gid (POSIX: the link's group is
	// the caller's egid), so pass a nil parent to skip inheritance.
	dfs.applyMountOwner(ctx, metaInode, nil)
	child := &DFSSymlink{fs: dfs, inodeID: metaInode.ID, recorder: rec}
	attr := inodeMetaToAttr(metaInode)
	inode := dfs.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFLNK, Ino: uint64(metaInode.ID)})
	out.Attr = attr
	return inode, 0
}

// Link creates a hard link under the bucket root.
func (dfs *DFSFileSystem) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return nil, toErrno(err)
	}
	rec := recorderFor(dfs.recorder)
	rec.IncOp("link")

	var targetID metadata.InodeID
	switch t := target.(type) {
	case *DFSFile:
		targetID = t.inodeID
	case *DFSDir:
		targetID = t.inodeID
	default:
		rec.IncOpError("link")
		return nil, syscall.EINVAL
	}

	metaInode, err := dfs.Meta().Link(ctx, dfs.bucketRoot, name, targetID)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			return nil, syscall.EEXIST
		}
		logf("link error: %v", err)
		rec.IncOpError("link")
		return nil, syscall.EIO
	}

	var child fs.InodeEmbedder
	switch metaInode.Type {
	case metadata.FileDirectory:
		child = &DFSDir{fs: dfs, inodeID: metaInode.ID, recorder: rec}
	default:
		child = newDFSFile(dfs, metaInode, rec)
	}
	attr := inodeMetaToAttr(metaInode)
	inode := dfs.NewInode(ctx, child, fs.StableAttr{Mode: attr.Mode, Ino: uint64(metaInode.ID)})
	out.Attr = attr
	return inode, 0
}

// Readdir on the root inode lists the bucket root's children directly. dfs is
// the connected root inode in single-bucket mode; Readdir itself needs no
// parent inode.
func (dfs *DFSFileSystem) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	rec := recorderFor(dfs.recorder)
	rec.IncOp("readdir")

	if err := dfs.checkAccess(dfs.bucketName, metadata.PermList); err != nil {
		return nil, toErrno(err)
	}
	meta := dfs.Meta()
	var entries []fuse.DirEntry
	cursor := ""
	for {
		page, err := meta.ReadDirFrom(ctx, dfs.bucketRoot, cursor, readdirPageSize)
		if err != nil {
			logf("root readdir: %v", err)
			rec.IncOpError("readdir")
			return nil, syscall.EIO
		}
		for _, e := range page {
			entries = append(entries, fuse.DirEntry{
				Name: e.Name,
				Ino:  uint64(e.InodeID),
				Mode: typeFromReaddir(e.Type),
			})
		}
		if len(page) < readdirPageSize {
			break
		}
		cursor = page[len(page)-1].Name
	}
	return fs.NewListDirStream(entries), 0
}

// Lookup resolves a name under the bucket root. dfs (root inode 1) is the
// tree-connected node in single-bucket mode, so children are created against
// it — not against a transient, disconnected DFSDir.
func (dfs *DFSFileSystem) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	rec := recorderFor(dfs.recorder)
	rec.IncOp("lookup")

	if err := dfs.checkAccess(dfs.bucketName, metadata.PermRead); err != nil {
		return nil, toErrno(err)
	}
	metaInode, err := dfs.Meta().Lookup(ctx, dfs.bucketRoot, name)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) || errors.Is(err, metadata.ErrInodeNotFound) {
			return nil, syscall.ENOENT
		}
		logf("root lookup %q: %v", name, err)
		rec.IncOpError("lookup")
		return nil, syscall.EIO
	}

	child := newChildInode(dfs, metaInode)
	attr := inodeMetaToAttr(metaInode)
	inode := dfs.NewInode(ctx, child, fs.StableAttr{
		Mode: attr.Mode,
		Ino:  attr.Ino,
	})

	out.Attr = attr
	return inode, 0
}

// Getattr on the root inode returns the attributes of the bucket root inode.
// It is always a directory.
func (dfs *DFSFileSystem) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	rootInode, err := dfs.Meta().GetInode(ctx, dfs.bucketRoot)
	if err != nil {
		return syscall.EIO
	}
	out.Attr = inodeMetaToAttr(rootInode)
	return 0
}

// Statfs returns filesystem statistics, reporting the bucket's quota (if set)
// so `df` reflects the user's actual limit. Falls back to cluster-wide totals.
func (dfs *DFSFileSystem) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	rec := recorderFor(dfs.recorder)
	rec.IncOp("statfs")

	meta := dfs.Meta()

	const blockSize = uint32(4096)
	out.Bsize = blockSize
	out.Frsize = blockSize
	out.NameLen = 255

	// Report the mounted bucket's quota when set. Free space is the quota
	// minus the bucket's actual usage (otherwise df would always show 0%
	// used even for a full bucket). If usage can't be read we fall back to
	// reporting the full quota as free rather than skewing the numbers.
	q, err := meta.GetBucketQuota(ctx, dfs.bucketName)
	if err == nil && q != nil && q.MaxSizeBytes > 0 {
		totalBytes := uint64(q.MaxSizeBytes)
		usedBytes := uint64(0)
		if usage, uerr := meta.GetBucketUsage(ctx, dfs.bucketName); uerr == nil && usage != nil && usage.UsedBytes > 0 {
			usedBytes = uint64(usage.UsedBytes)
			if usedBytes > totalBytes {
				usedBytes = totalBytes
			}
		}
		freeBytes := totalBytes - usedBytes
		out.Blocks = totalBytes / uint64(blockSize)
		out.Bfree = freeBytes / uint64(blockSize)
		out.Bavail = out.Bfree
		// Inode estimate, same heuristic as the fallback branch: 1 per 64 KB.
		totalInodes := totalBytes / 65536
		usedInodes := usedBytes / 65536
		out.Files = totalInodes
		out.Ffree = totalInodes - usedInodes
		return 0
	}

	// Fallback: cluster-wide node totals.
	nodes, err := meta.ListNodes(ctx)
	if err != nil {
		logf("statfs: %v", err)
		return 0
	}

	var totalCap, usedCap int64
	for _, n := range nodes {
		totalCap += n.CapacityGB
		usedCap += n.UsedGB
	}

	totalBytes := uint64(totalCap) * 1024 * 1024 * 1024
	usedBytes := uint64(usedCap) * 1024 * 1024 * 1024

	out.Blocks = totalBytes / uint64(blockSize)
	out.Bfree = (totalBytes - usedBytes) / uint64(blockSize)
	out.Bavail = out.Bfree

	// Estimate inodes: 1 per 64 KB of capacity.
	totalInodes := totalBytes / 65536
	usedInodes := usedBytes / 65536
	out.Files = totalInodes
	out.Ffree = totalInodes - usedInodes

	return 0
}

// ========== Inode type resolution ==========

// newDFSFile constructs a DFSFile (go-fuse glue) over a fresh
// chunkstore.BufferedFile. The buffer is wired to the filesystem's metadata
// accessor (hot-swappable via SwapMetadata), reliability executor, read
// cache, spill stats, write-attempt ledger, dirty budget, and default
// placement policy. committedSize seeds the buffer's logical tail from the
// inode's current committed Size (a partial overwrite must not shrink the
// file below its committed extent).
func newDFSFile(dfs *DFSFileSystem, metaInode *metadata.InodeMeta, rec MetricsRecorder) *DFSFile {
	opts := []chunkstore.BufferedFileOption{
		chunkstore.WithExecutor(chunkstore.Executor{
			DoMeta:    dfs.reliability.DoMeta,
			DoChunk:   dfs.reliability.DoChunk,
			LockInode: dfs.reliability.LockInode,
		}),
		chunkstore.WithSpillStats(dfs.recorder),
		chunkstore.WithFlushLedger(flushLedger{dfs: dfs, inodeID: metaInode.ID}),
		chunkstore.WithBudget(chunkstore.Budget{
			MaxDirtyBytes:    dfs.maxDirtyBytes,
			GlobalBudget:     dfs.globalDirtyBudget,
			GlobalDirtyBytes: &dfs.globalDirtyBytes,
			StagingDir:       dfs.writeStagingDir,
		}),
		chunkstore.WithDefaultPolicy(fuseDefaultPolicy),
	}
	// Only install a cache when one is actually configured: a typed-nil
	// *ChunkCache wrapped in the ReadCache interface is != nil to Go, which
	// would make BufferedFile call Get on a nil pointer (unit tests and
	// cache-less mounts).
	if dfs.chunkCache != nil {
		opts = append(opts, chunkstore.WithReadCache(dfs.chunkCache))
	}

	return &DFSFile{
		fs:          dfs,
		inodeID:     metaInode.ID,
		lockOwner:   dfs.lockOwner,
		recorder:    rec,
		reliability: dfs.reliability,
		buffered: chunkstore.NewBufferedFile(
			func() metadata.MetadataService { return dfs.Meta() },
			dfs.chunkStore,
			metaInode.ID,
			metaInode.Size,
			opts...,
		),
	}
}

// newChildInode creates the appropriate InodeEmbedder based on file type.
func newChildInode(dfs *DFSFileSystem, metaInode *metadata.InodeMeta) fs.InodeEmbedder {
	switch metaInode.Type {
	case metadata.FileDirectory:
		return &DFSDir{fs: dfs, inodeID: metaInode.ID, recorder: dfs.recorder}
	case metadata.FileRegular:
		return newDFSFile(dfs, metaInode, dfs.recorder)
	case metadata.FileSymlink:
		return &DFSSymlink{fs: dfs, inodeID: metaInode.ID, recorder: dfs.recorder}
	case metadata.FileFIFO, metadata.FileCharDevice, metadata.FileBlockDevice, metadata.FileSocket:
		return &DFSFifo{fs: dfs, inodeID: metaInode.ID, recorder: dfs.recorder}
	default:
		return newDFSFile(dfs, metaInode, dfs.recorder)
	}
}

// inodeMetaToAttr converts InodeMeta to FUSE Attr.
func inodeMetaToAttr(m *metadata.InodeMeta) fuse.Attr {
	attr := fuse.Attr{
		Ino:   uint64(m.ID),
		Size:  uint64(m.Size),
		Nlink: m.NLink,
		Mode:  m.Mode,
	}
	attr.Owner.Uid = m.UID
	attr.Owner.Gid = m.GID

	// Set file type bits
	switch m.Type {
	case metadata.FileDirectory:
		attr.Mode |= fuse.S_IFDIR
	case metadata.FileRegular:
		attr.Mode |= fuse.S_IFREG
	case metadata.FileSymlink:
		attr.Mode |= fuse.S_IFLNK
	case metadata.FileFIFO:
		attr.Mode |= fuse.S_IFIFO
	case metadata.FileCharDevice:
		attr.Mode |= syscall.S_IFCHR
	case metadata.FileBlockDevice:
		attr.Mode |= syscall.S_IFBLK
	case metadata.FileSocket:
		attr.Mode |= syscall.S_IFSOCK
	}

	// Rdev carries the device number on char/block devices (fuse.Attr exposes
	// it as a uint32); zero for every other type.
	attr.Rdev = m.Rdev

	// Convert nanosecond timestamps
	if m.MTime > 0 {
		t := time.Unix(0, m.MTime)
		attr.Mtime = uint64(t.Unix())
		attr.Mtimensec = uint32(t.Nanosecond())
	}
	if m.CTime > 0 {
		t := time.Unix(0, m.CTime)
		attr.Ctime = uint64(t.Unix())
		attr.Ctimensec = uint32(t.Nanosecond())
	}

	return attr
}

// logf is a helper for FUSE debug logging.
func logf(format string, args ...interface{}) {
	log.Printf("[fuse] "+format, args...)
}
