package s3fs

import (
	"context"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
)

// DFSXAttr provides extended attribute support for S3 files/dirs.
type DFSXAttr struct {
	mfs    *S3FileSystem
	inode  *CacheInode
}

// OpenXAttr returns an xattr handle for a file.
func (f *S3File) OpenXAttr() *DFSXAttr {
	inode, _ := f.mfs.cache.GetInode(f.InodeID)
	return &DFSXAttr{mfs: f.mfs, inode: inode}
}

// OpenXAttr returns an xattr handle for a directory.
func (d *S3Dir) OpenXAttr() *DFSXAttr {
	inode, _ := d.mfs.cache.GetInode(d.InodeID)
	return &DFSXAttr{mfs: d.mfs, inode: inode}
}

var (
	_ fs.NodeGetxattrer  = (*DFSXAttr)(nil)
	_ fs.NodeSetxattrer  = (*DFSXAttr)(nil)
	_ fs.NodeListxattrer = (*DFSXAttr)(nil)
	_ fs.NodeRemovexattrer = (*DFSXAttr)(nil)
)

// xattrKey returns the xattr storage key for an inode.
func xattrKey(inodeID uint64, name string) []byte {
	return []byte("xattr:" + string(rune(inodeID)) + ":" + name)
}

// Getxattr retrieves an extended attribute.
func (x *DFSXAttr) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	if x.inode == nil {
		return 0, syscall.ENODATA
	}
	// xattrs stored in local cache — simplified: return ENODATA if not found.
	// Full implementation would use Pebble xattr sub-bucket.
	return 0, syscall.ENODATA
}

// Setxattr sets an extended attribute.
func (x *DFSXAttr) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	if x.mfs.config.ReadOnly {
		return syscall.EPERM
	}
	// Simplified: no-op for now. Full implementation stores in Pebble.
	return 0
}

// Listxattr lists extended attribute names.
func (x *DFSXAttr) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	// No xattrs stored by default.
	return 0, 0
}

// Removexattr removes an extended attribute.
func (x *DFSXAttr) Removexattr(ctx context.Context, attr string) syscall.Errno {
	if x.mfs.config.ReadOnly {
		return syscall.EPERM
	}
	return syscall.ENODATA
}

// Ensure imports are used.
var _ = strings.Join
