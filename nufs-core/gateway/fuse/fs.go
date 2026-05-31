//go:build linux

// Package fuse provides a FUSE filesystem gateway for DFS.
package fuse

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/example/dfs/metadata"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// DFSFileSystem is the root filesystem backed by the DFS metadata service.
type DFSFileSystem struct {
	fs.Inode

	meta metadata.MetadataService

	// Inode cache: metadata.InodeID -> *fs.Inode
	mu       sync.RWMutex
	inodeMap map[metadata.InodeID]*fs.Inode
}

// NewDFSFileSystem creates a new FUSE filesystem root.
func NewDFSFileSystem(meta metadata.MetadataService) *DFSFileSystem {
	return &DFSFileSystem{
		meta:     meta,
		inodeMap: make(map[metadata.InodeID]*fs.Inode),
	}
}

// Mount mounts the DFS filesystem at the given mountpoint.
func Mount(mountpoint string, meta metadata.MetadataService, opts *fuse.MountOptions) (*fuse.Server, error) {
	root := NewDFSFileSystem(meta)

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

// ========== Inode type resolution ==========

// newChildInode creates the appropriate InodeEmbedder based on file type.
func newChildInode(meta metadata.MetadataService, metaInode *metadata.InodeMeta) fs.InodeEmbedder {
	switch metaInode.Type {
	case metadata.FileDirectory:
		return &DFSDir{meta: meta, inodeID: metaInode.ID}
	case metadata.FileRegular:
		return &DFSFile{meta: meta, inodeID: metaInode.ID}
	case metadata.FileSymlink:
		return &DFSSymlink{meta: meta, inodeID: metaInode.ID}
	default:
		return &DFSFile{meta: meta, inodeID: metaInode.ID}
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
