//go:build linux

// Package fuse provides a FUSE filesystem gateway for DFS.
package fuse

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/example/dfs/chunkstore"
	"github.com/example/dfs/metadata"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// DFSFileSystem is the root filesystem backed by the DFS metadata service.
type DFSFileSystem struct {
	fs.Inode

	meta       metadata.MetadataService
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
	// 子 inode (DFSFile/DFSDir/DFSSymlink) 通过 rootFromInode 获取引用。
	recorder MetricsRecorder

	// reliability 包装 retry + circuit breaker + 路径锁。
	// nil 时为 passthrough 模式（直接调用 fn），兼容旧测试。
	reliability *ReliabilityWrapper

	// Inode cache: metadata.InodeID -> *fs.Inode
	mu       sync.RWMutex
	inodeMap map[metadata.InodeID]*fs.Inode
}

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
		meta:        meta,
		chunkStore:  chunkStore,
		chunkCache:  cache,
		lockOwner:   fmt.Sprintf("fusegw-%d", os.Getpid()),
		inodeMap:    make(map[metadata.InodeID]*fs.Inode),
		recorder:    recorder,
		reliability: reliability,
	}
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

// runCacheInvalidationLoop consumes chunk-scoped events from the metadata
// service and removes the corresponding cache entries. It also watches
// inode-level events to evict stale size/mtime info held in the OS page
// cache. The loop is best-effort: if the stream ends (e.g. server restart)
// it simply returns and the cache eventually expires by itself.
func (fsys *DFSFileSystem) runCacheInvalidationLoop(ctx context.Context, w chunkEventWatcher) {
	for e := range w.WatchEventsStream(ctx, "chunk:") {
		chunkID, err := parseChunkID(e.Key)
		if err == nil {
			fsys.chunkCache.Remove(chunkID)
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

// Mount mounts the DFS filesystem at the given mountpoint.
// recorder 用于指标打点，nil 时使用 noopMetricsRecorder。
// reliability 注入 retry+breaker+pathlock 能力，nil 时为 passthrough 模式。
func Mount(mountpoint string, meta metadata.MetadataService, chunkStore chunkstore.ChunkStore, cache *ChunkCache, recorder MetricsRecorder, reliability *ReliabilityWrapper, opts *fuse.MountOptions) (*fuse.Server, error) {
	root := NewDFSFileSystem(meta, chunkStore, cache, recorder, reliability)

	if opts == nil {
		opts = &fuse.MountOptions{
			AllowOther: false,
			Name:       "dfs",
			FsName:     "dfs",
		}
	}

	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: *opts,
	})
	if err != nil {
		return nil, fmt.Errorf("fuse mount: %w", err)
	}

	return server, nil
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

// Readdir on the root inode lists all bucket names as directory
// entries. This is what makes `ls /mnt/dfs` show the bucket list
// without requiring a separate mount per bucket (compare: s3gw
// uses per-bucket RootInode; fusegw shares RootInodeID=1).
func (dfs *DFSFileSystem) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	rec := recorderFor(dfs.recorder)
	rec.IncOp("readdir")

	buckets, err := dfs.meta.ListBuckets(ctx)
	if err != nil {
		logf("root readdir: %v", err)
		rec.IncOpError("readdir")
		return nil, syscall.EIO
	}

	var entries []fuse.DirEntry
	for _, b := range buckets {
		entries = append(entries, fuse.DirEntry{
			Name: b.Name,
			Ino:  uint64(b.RootInode),
			Mode: fuse.S_IFDIR,
		})
	}

	return fs.NewListDirStream(entries), 0
}

// Lookup resolves a bucket name at the root level. "foo" returns the
// DFSDir that wraps bucket "foo" RootInode, so all file operations
// under /mnt/dfs/foo go through the same metadata path that s3gw
// would use for bucket "foo".
func (dfs *DFSFileSystem) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	rec := recorderFor(dfs.recorder)
	rec.IncOp("lookup")

	bucket, err := dfs.meta.GetBucket(ctx, name)
	if err != nil {
		if errors.Is(err, metadata.ErrBucketNotFound) {
			return nil, syscall.ENOENT
		}
		logf("root lookup %q: %v", name, err)
		rec.IncOpError("lookup")
		return nil, syscall.EIO
	}

	child := &DFSDir{meta: dfs.meta, inodeID: bucket.RootInode, recorder: dfs.recorder}
	attr := fuse.Attr{
		Ino:  uint64(bucket.RootInode),
		Mode: fuse.S_IFDIR,
	}
	inode := dfs.NewInode(ctx, child, fs.StableAttr{
		Mode: fuse.S_IFDIR,
		Ino:  uint64(bucket.RootInode),
	})

	out.Attr = attr
	return inode, 0
}

// Getattr on the root inode returns the attributes of the global
// root (inode 1). The root is always a directory; its size is 0
// (no content); timestamps are not tracked.
func (dfs *DFSFileSystem) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	rootInode, err := dfs.meta.GetInode(ctx, metadata.RootInodeID)
	if err != nil {
		return syscall.EIO
	}
	out.Attr = inodeMetaToAttr(rootInode)
	return 0
}

// Statfs returns filesystem statistics from cluster-wide node capacity.
func (dfs *DFSFileSystem) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	rec := recorderFor(dfs.recorder)
	rec.IncOp("statfs")

	nodes, err := dfs.meta.ListNodes(ctx)
	if err != nil {
		logf("statfs: %v", err)
		// 保留原行为：statfs 错误不向调用方报错，仅 log
		return 0
	}

	var totalCap, usedCap int64
	for _, n := range nodes {
		totalCap += n.CapacityGB
		usedCap += n.UsedGB
	}

	totalBytes := uint64(totalCap) * 1024 * 1024 * 1024
	usedBytes := uint64(usedCap) * 1024 * 1024 * 1024

	const blockSize = uint32(4096)
	out.Bsize = blockSize
	out.Frsize = blockSize
	out.NameLen = 255

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

// newChildInode creates the appropriate InodeEmbedder based on file type.
func newChildInode(dfs *DFSFileSystem, metaInode *metadata.InodeMeta) fs.InodeEmbedder {
	switch metaInode.Type {
	case metadata.FileDirectory:
		return &DFSDir{meta: dfs.meta, inodeID: metaInode.ID, recorder: dfs.recorder}
	case metadata.FileRegular:
		return &DFSFile{meta: dfs.meta, chunkStore: dfs.chunkStore, cache: dfs.chunkCache, inodeID: metaInode.ID, lockOwner: dfs.lockOwner, recorder: dfs.recorder, reliability: dfs.reliability}
	case metadata.FileSymlink:
		return &DFSSymlink{meta: dfs.meta, inodeID: metaInode.ID, recorder: dfs.recorder}
	default:
		return &DFSFile{meta: dfs.meta, chunkStore: dfs.chunkStore, cache: dfs.chunkCache, inodeID: metaInode.ID, lockOwner: dfs.lockOwner, recorder: dfs.recorder, reliability: dfs.reliability}
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
	}

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
