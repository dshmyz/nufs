//go:build linux

package fuse

import (
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/example/dfs/metadata"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// DFSSymlink represents a symbolic link in the DFS FUSE filesystem.
type DFSSymlink struct {
	fs.Inode

	meta    metadata.MetadataService
	inodeID metadata.InodeID

	// recorder 记录 FUSE 操作指标。nil 时不打点。
	recorder MetricsRecorder
}

var _ = (fs.NodeReadlinker)((*DFSSymlink)(nil))
var _ = (fs.NodeGetattrer)((*DFSSymlink)(nil))
var _ = (fs.NodeAccesser)((*DFSSymlink)(nil))
var _ = (fs.NodeSetattrer)((*DFSSymlink)(nil))

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

// Access always returns 0 (allow-all).
func (s *DFSSymlink) Access(ctx context.Context, mask uint32) syscall.Errno {
	return 0
}

// Setattr updates symlink metadata (uid, gid, mode).
func (s *DFSSymlink) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	metaInode, err := s.meta.GetInode(ctx, s.inodeID)
	if err != nil {
		return syscall.EIO
	}

	changed := false
	if mode, ok := in.GetMode(); ok {
		metaInode.Mode = mode
		changed = true
	}
	if uid, ok := in.GetUID(); ok {
		metaInode.UID = uid
		changed = true
	}
	if gid, ok := in.GetGID(); ok {
		metaInode.GID = gid
		changed = true
	}
	if changed {
		metaInode.CTime = time.Now().UnixNano()
		if err := s.meta.UpdateInode(ctx, metaInode); err != nil {
			return syscall.EIO
		}
	}

	out.Attr = inodeMetaToAttr(metaInode)
	return 0
}

// OpenXAttr returns an xattr handle for this symlink.
func (s *DFSSymlink) OpenXAttr() *DFSXAttr {
	return &DFSXAttr{meta: s.meta, inodeID: s.inodeID}
}
