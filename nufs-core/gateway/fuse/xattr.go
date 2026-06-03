//go:build linux

package fuse

import (
	"context"
	"syscall"

	"github.com/example/dfs/metadata"
	"github.com/hanwen/go-fuse/v2/fs"
)

// DFSXAttr wraps an inode so that getxattr/setxattr/listxattr/
// removexattr syscalls can reach the metadata service. Every DFSFile,
// DFSDir and DFSSymlink exposes an OpenXAttr method that returns
// one of these; the go-fuse framework routes xattr calls to it
// automatically.
type DFSXAttr struct {
	meta    metadata.MetadataService
	inodeID metadata.InodeID
}

var _ = (fs.NodeGetxattrer)((*DFSXAttr)(nil))
var _ = (fs.NodeSetxattrer)((*DFSXAttr)(nil))
var _ = (fs.NodeListxattrer)((*DFSXAttr)(nil))
var _ = (fs.NodeRemovexattrer)((*DFSXAttr)(nil))

// Getxattr reads a single extended attribute. If dest is too small
// it returns ERANGE together with the required size.
func (x *DFSXAttr) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	val, err := x.meta.GetXAttr(ctx, x.inodeID, attr)
	if err != nil {
		if err == metadata.ErrXAttrNotFound {
			return 0, syscall.ENODATA
		}
		return 0, syscall.EIO
	}
	if len(val) > len(dest) {
		return uint32(len(val)), syscall.ERANGE
	}
	copy(dest, val)
	return uint32(len(val)), 0
}

// Setxattr stores a single extended attribute.
func (x *DFSXAttr) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	if err := x.meta.SetXAttr(ctx, x.inodeID, attr, data); err != nil {
		return syscall.EIO
	}
	return 0
}

// Listxattr returns all attribute names as a null-separated string
// in dest. Returns ERANGE if dest is too small.
func (x *DFSXAttr) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	attrs, err := x.meta.ListXAttr(ctx, x.inodeID)
	if err != nil {
		return 0, syscall.EIO
	}
	if len(attrs) == 0 {
		return 0, 0
	}
	// Build a null-separated list of names.
	var total int
	for name := range attrs {
		total += len(name) + 1 // +1 for the null terminator
	}
	if total > len(dest) {
		return uint32(total), syscall.ERANGE
	}
	var offset int
	for name := range attrs {
		n := copy(dest[offset:], name)
		dest[offset+n] = 0
		offset += n + 1
	}
	return uint32(total), 0
}

// Removexattr deletes a single extended attribute.
func (x *DFSXAttr) Removexattr(ctx context.Context, attr string) syscall.Errno {
	if err := x.meta.RemoveXAttr(ctx, x.inodeID, attr); err != nil {
		return syscall.EIO
	}
	return 0
}
