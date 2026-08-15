package s3fs

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// S3Symlink represents a symbolic link.
type S3Symlink struct {
	fs.Inode

	mfs     *S3FileSystem
	dir     *S3Dir
	Path    string
	Target  string
	InodeID uint64
	Mode    os.FileMode
	UID     uint32
	GID     uint32
}

var (
	_ fs.NodeReadlinker = (*S3Symlink)(nil)
	_ fs.NodeGetattrer  = (*S3Symlink)(nil)
	_ fs.NodeSetattrer  = (*S3Symlink)(nil)
	_ fs.NodeAccesser   = (*S3Symlink)(nil)
)

// Readlink returns the symlink target.
func (s *S3Symlink) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return []byte(s.Target), 0
}

// Getattr returns symlink attributes.
func (s *S3Symlink) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr = fuse.Attr{
		Ino:   s.InodeID,
		Size:  uint64(len(s.Target)),
		Mode:  uint32(os.ModeSymlink | s.Mode),
		Owner: fuse.Owner{Uid: s.UID, Gid: s.GID},
		Nlink: 1,
	}
	return 0
}

// Setattr updates symlink attributes.
func (s *S3Symlink) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if uid, ok := in.GetUID(); ok {
		s.UID = uid
	}
	if gid, ok := in.GetGID(); ok {
		s.GID = gid
	}
	if _, ok := in.GetMTime(); ok {
		// mtime update
	}

	_ = s.mfs.cache.PutInode(&CacheInode{
		ID:            s.InodeID,
		Name:          s.Path,
		Mode:          uint32(os.ModeSymlink | s.Mode),
		UID:           s.UID,
		GID:           s.GID,
		SymlinkTarget: s.Target,
		Mtime:         time.Now().UnixNano(),
		Ctime:         time.Now().UnixNano(),
		Atime:         time.Now().UnixNano(),
	})

	out.Attr = fuse.Attr{
		Ino:   s.InodeID,
		Size:  uint64(len(s.Target)),
		Mode:  uint32(os.ModeSymlink | s.Mode),
		Owner: fuse.Owner{Uid: s.UID, Gid: s.GID},
		Nlink: 1,
	}
	return 0
}

// Access always returns 0.
func (s *S3Symlink) Access(ctx context.Context, mask uint32) syscall.Errno {
	return 0
}
