//go:build linux

// Package fuse provides a FUSE filesystem gateway for DFS.
package fuse

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"syscall"
	"sync"
	"time"

	"github.com/example/dfs/gateway"
	"github.com/example/dfs/metadata"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// DFSFileSystem is the root filesystem backed by the DFS metadata service.
type DFSFileSystem struct {
	fs.Inode

	meta       metadata.MetadataService
	chunkStore gateway.ChunkStore

	// lockOwner is the per-process string used when acquiring advisory
	// file locks (commit 0: metadata: add advisory file lock service).
	// Each Open that returns a write handle acquires an exclusive lock
	// under this owner; Release drops it. Empty means "no locks" (used
	// in unit tests that have no lock manager).
	lockOwner string

	// chunkCache caches chunk payloads to avoid datanode round-trips.
	chunkCache *ChunkCache

	// Inode cache: metadata.InodeID -> *fs.Inode
	mu       sync.RWMutex
	inodeMap map[metadata.InodeID]*fs.Inode
}

// NewDFSFileSystem creates a new FUSE filesystem root.
func NewDFSFileSystem(meta metadata.MetadataService, chunkStore gateway.ChunkStore, cache *ChunkCache) *DFSFileSystem {
	return &DFSFileSystem{
		meta:       meta,
		chunkStore: chunkStore,
		chunkCache: cache,
		lockOwner:  fmt.Sprintf("fusegw-%d", os.Getpid()),
		inodeMap:   make(map[metadata.InodeID]*fs.Inode),
	}
}

// Mount mounts the DFS filesystem at the given mountpoint.
func Mount(mountpoint string, meta metadata.MetadataService, chunkStore gateway.ChunkStore, cache *ChunkCache, opts *fuse.MountOptions) (*fuse.Server, error) {
	root := NewDFSFileSystem(meta, chunkStore, cache)

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

// Readdir on the root inode lists all bucket names as directory
// entries. This is what makes `ls /mnt/dfs` show the bucket list
// without requiring a separate mount per bucket (compare: s3gw
// uses per-bucket RootInode; fusegw shares RootInodeID=1).
func (dfs *DFSFileSystem) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	buckets, err := dfs.meta.ListBuckets(ctx)
	if err != nil {
		logf("root readdir: %v", err)
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
	bucket, err := dfs.meta.GetBucket(ctx, name)
	if err != nil {
		if errors.Is(err, metadata.ErrBucketNotFound) {
			return nil, syscall.ENOENT
		}
		logf("root lookup %q: %v", name, err)
		return nil, syscall.EIO
	}

	child := &DFSDir{meta: dfs.meta, inodeID: bucket.RootInode}
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

// ========== Inode type resolution ==========

// newChildInode creates the appropriate InodeEmbedder based on file type.
func newChildInode(dfs *DFSFileSystem, metaInode *metadata.InodeMeta) fs.InodeEmbedder {
	switch metaInode.Type {
	case metadata.FileDirectory:
		return &DFSDir{meta: dfs.meta, inodeID: metaInode.ID}
	case metadata.FileRegular:
		return &DFSFile{meta: dfs.meta, chunkStore: dfs.chunkStore, cache: dfs.chunkCache, inodeID: metaInode.ID, lockOwner: dfs.lockOwner}
	case metadata.FileSymlink:
		return &DFSSymlink{meta: dfs.meta, inodeID: metaInode.ID}
	default:
		return &DFSFile{meta: dfs.meta, chunkStore: dfs.chunkStore, cache: dfs.chunkCache, inodeID: metaInode.ID, lockOwner: dfs.lockOwner}
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
