//go:build linux

package fuse

import (
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// DFSDir represents a directory inode in the DFS FUSE filesystem.
type DFSDir struct {
	fs.Inode

	// fs is the owning filesystem root; stable across SwapMetadata swaps.
	fs      *DFSFileSystem
	inodeID metadata.InodeID

	// recorder 记录 FUSE 操作指标。nil 时不打点。
	recorder MetricsRecorder
}

var _ = (fs.NodeReaddirer)((*DFSDir)(nil))
var _ = (fs.NodeLookuper)((*DFSDir)(nil))
var _ = (fs.NodeMkdirer)((*DFSDir)(nil))
var _ = (fs.NodeRmdirer)((*DFSDir)(nil))
var _ = (fs.NodeCreater)((*DFSDir)(nil))
var _ = (fs.NodeMknoder)((*DFSDir)(nil))
var _ = (fs.NodeUnlinker)((*DFSDir)(nil))
var _ = (fs.NodeRenamer)((*DFSDir)(nil))
var _ = (fs.NodeSymlinker)((*DFSDir)(nil))
var _ = (fs.NodeLinker)((*DFSDir)(nil))
var _ = (fs.NodeGetattrer)((*DFSDir)(nil))
var _ = (fs.NodeSetattrer)((*DFSDir)(nil))
var _ = (fs.NodeAccesser)((*DFSDir)(nil))

// readdirPageSize is the number of entries fetched per ReadDirFrom page.
// The metadata layer caps a single read at maxReadDirEntries (10k); paging in
// smaller chunks via the O(limit) cursor API keeps per-call latency bounded and
// lets directories of arbitrary size enumerate fully (no silent truncation).
const readdirPageSize = 4096

// Readdir lists directory entries.
//
// Directories can exceed the 10k-entry cap a single ReadDir call imposes, so we
// page through the O(limit) cursor-based ReadDirFrom until a page comes back
// short (i.e. we hit the end). This guarantees every entry is listed.
func (d *DFSDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	dfs := d.fs
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermList); err != nil {
		return nil, toErrno(err)
	}
	rec := recorderFor(d.recorder)
	rec.IncOp("readdir")

	var fuseEntries []fuse.DirEntry
	cursor := ""
	for {
		entries, err := d.fs.Meta().ReadDirFrom(ctx, d.inodeID, cursor, readdirPageSize)
		if err != nil {
			logf("readdir error: %v", err)
			rec.IncOpError("readdir")
			return nil, syscall.EIO
		}
		for _, e := range entries {
			fuseEntries = append(fuseEntries, fuse.DirEntry{
				Name: e.Name,
				Ino:  uint64(e.InodeID),
				Mode: typeFromReaddir(e.Type),
			})
		}
		// A short page means we've reached the end of the directory.
		if len(entries) < readdirPageSize {
			break
		}
		// Advance the cursor past the last entry of this page. ReadDirFrom
		// returns entries strictly after the cursor name, so using the final
		// entry's name makes the next page start exactly at the next entry.
		cursor = entries[len(entries)-1].Name
	}

	return fs.NewListDirStream(fuseEntries), 0
}

// typeFromReaddir maps a metadata FileType to the FUSE S_IF* mode bit used in
// directory listings. Unknown types map to 0 (treated as a regular file by the
// caller's caller); see inodeMetaToAttr for the full getattr mapping.
func typeFromReaddir(t metadata.FileType) uint32 {
	switch t {
	case metadata.FileDirectory:
		return fuse.S_IFDIR
	case metadata.FileRegular:
		return fuse.S_IFREG
	case metadata.FileSymlink:
		return fuse.S_IFLNK
	case metadata.FileFIFO:
		return fuse.S_IFIFO
	case metadata.FileCharDevice:
		return syscall.S_IFCHR
	case metadata.FileBlockDevice:
		return syscall.S_IFBLK
	case metadata.FileSocket:
		return syscall.S_IFSOCK
	default:
		return 0
	}
}

// Lookup looks up a child entry by name.
func (d *DFSDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	dfs := d.fs
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermRead); err != nil {
		return nil, toErrno(err)
	}
	rec := recorderFor(d.recorder)
	rec.IncOp("lookup")

	metaInode, err := d.fs.Meta().Lookup(ctx, d.inodeID, name)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) || errors.Is(err, metadata.ErrInodeNotFound) {
			// ENOENT 不是错误（合法的"不存在"响应），不计入 errors
			return nil, syscall.ENOENT
		}
		logf("lookup error: %v", err)
		rec.IncOpError("lookup")
		return nil, syscall.EIO
	}

	child := newChildInode(dfs, metaInode)
	attr := inodeMetaToAttr(metaInode)

	inode := d.NewInode(ctx, child, fs.StableAttr{
		Mode: attr.Mode,
		Ino:  attr.Ino,
	})

	out.Attr = attr
	return inode, 0
}

// Mkdir creates a new directory.
func (d *DFSDir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	dfs := d.fs
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return nil, toErrno(err)
	}
	rec := recorderFor(d.recorder)
	rec.IncOp("mkdir")

	metaInode, err := d.fs.Meta().MkDir(ctx, d.inodeID, name, mode)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			// EEXIST 不是错误（合法的"已存在"响应），不计入 errors
			return nil, syscall.EEXIST
		}
		logf("mkdir error: %v", err)
		rec.IncOpError("mkdir")
		return nil, syscall.EIO
	}

	// setgid inheritance: when the parent directory has S_ISGID set, new child
	// directories inherit the parent's gid (POSIX semantics), overriding the
	// mount default; applyMountOwner applies and persists it.
	dirMeta, _ := d.fs.Meta().GetInode(ctx, d.inodeID)
	dfs.applyMountOwner(ctx, metaInode, dirMeta)

	child := &DFSDir{fs: dfs, inodeID: metaInode.ID, recorder: rec}
	attr := inodeMetaToAttr(metaInode)

	inode := d.NewInode(ctx, child, fs.StableAttr{
		Mode: fuse.S_IFDIR,
		Ino:  uint64(metaInode.ID),
	})

	out.Attr = attr
	return inode, 0
}

// Rmdir removes an empty directory.
func (d *DFSDir) Rmdir(ctx context.Context, name string) syscall.Errno {
	dfs := d.fs
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return toErrno(err)
	}
	rec := recorderFor(d.recorder)
	rec.IncOp("rmdir")

	err := d.fs.Meta().RmDir(ctx, d.inodeID, name)
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

// Create creates and opens a new file.
func (d *DFSDir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	dfs := d.fs
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return nil, nil, 0, toErrno(err)
	}
	rec := recorderFor(d.recorder)
	rec.IncOp("create")

	metaInode, err := d.fs.Meta().CreateFile(ctx, d.inodeID, name, mode)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			// File exists — lookup and open it
			metaInode, err = d.fs.Meta().Lookup(ctx, d.inodeID, name)
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
		// New file created — apply mount-level uid/gid override plus setgid
		// inheritance from the parent, persisted together.
		dirMeta, _ := d.fs.Meta().GetInode(ctx, d.inodeID)
		dfs.applyMountOwner(ctx, metaInode, dirMeta)
	}

	file := newDFSFile(dfs, metaInode, rec)
	attr := inodeMetaToAttr(metaInode)

	inode := d.NewInode(ctx, file, fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  uint64(metaInode.ID),
	})

	out.Attr = attr
	if directIOEnabled.Load() {
		fuseFlags = fuse.FOPEN_DIRECT_IO
	}
	return inode, file, fuseFlags, 0
}

// Mknod creates a special (non-regular) node: FIFO, char/block device or
// unix socket. The kernel fully owns FIFO pipe semantics (this fs only
// provides node identity + attrs via the DFSFifo thin node); devices and
// sockets exist as identity-only stubs and open() is refused (EOPNOTSUPP).
func (d *DFSDir) Mknod(ctx context.Context, name string, mode uint32, dev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	rec := recorderFor(d.recorder)
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

	metaInode, err := d.fs.Meta().CreateNode(ctx, d.inodeID, name, ftype, mode&07777, rdev)
	if errno := errnoForCreateNode(err); errno != 0 {
		if errno == syscall.EIO {
			logf("mknod error: %v", err)
			rec.IncOpError("mknod")
		}
		return nil, errno
	}

	// setgid inheritance: when the parent directory has S_ISGID set, new child
	// nodes inherit the parent's gid (POSIX semantics), overriding the mount
	// default; applyMountOwner applies and persists it.
	dirMeta, _ := d.fs.Meta().GetInode(ctx, d.inodeID)
	d.fs.applyMountOwner(ctx, metaInode, dirMeta)
	attr := inodeMetaToAttr(metaInode)
	inode := d.NewInode(ctx, &DFSFifo{fs: d.fs, inodeID: metaInode.ID, recorder: rec}, fs.StableAttr{
		Mode: attr.Mode & syscall.S_IFMT,
		Ino:  uint64(metaInode.ID),
	})
	out.Attr = attr
	return inode, 0
}

// checkStickyDir returns EACCES when the directory dirID has the sticky bit
// (S_ISVTX) set and the caller is neither the owner of fileUid nor root.  This
// mirrors the kernel's own sticky-directory enforcement for unlink(2) and
// rename(2), which is bypassed when FUSE handles NodeUnlinker/NodeRenamer.
func (d *DFSDir) checkStickyDir(ctx context.Context, dirID metadata.InodeID, fileUid uint32) error {
	dirMeta, err := d.fs.Meta().GetInode(ctx, dirID)
	if err != nil {
		return syscall.EIO
	}
	if dirMeta.Mode&sIsvtx == 0 {
		return nil // no sticky bit → no restriction
	}
	caller, ok := ctx.(*fuse.Context)
	if !ok || caller.Uid == 0 {
		return nil // root bypass
	}
	if caller.Uid == fileUid {
		return nil
	}
	return syscall.EACCES
}

// checkStickyBit returns EACCES when the parent directory (this dir) has the
// sticky bit set and the caller is neither the named child's owner nor root.
func (d *DFSDir) checkStickyBit(ctx context.Context, name string) error {
	childMeta, err := d.fs.Meta().Lookup(ctx, d.inodeID, name)
	if err != nil {
		return syscall.EIO
	}
	return d.checkStickyDir(ctx, d.inodeID, childMeta.UID)
}

// Unlink removes a file entry.
func (d *DFSDir) Unlink(ctx context.Context, name string) syscall.Errno {
	dfs := d.fs
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return toErrno(err)
	}
	rec := recorderFor(d.recorder)
	rec.IncOp("unlink")

	// Sticky-bit check: when the parent directory has S_ISVTX set, only
	// the file owner (or root) may unlink.  This mirrors the kernel's own
	// sticky-directory check for unlink(2) which is NOT performed by FUSE
	// when the filesystem implements NodeUnlinker.
	if err := d.checkStickyBit(ctx, name); err != nil {
		return toErrno(err)
	}

	err := d.fs.Meta().Unlink(ctx, d.inodeID, name)
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

// Rename renames/moves a file or directory.
func (d *DFSDir) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	dfs := d.fs
	if err := dfs.checkAccess(dfs.bucketName, metadata.PermWrite); err != nil {
		return toErrno(err)
	}
	rec := recorderFor(d.recorder)
	rec.IncOp("rename")

	var newParentDir *DFSDir
	switch p := newParent.(type) {
	case *DFSDir:
		newParentDir = p
	default:
		rec.IncOpError("rename")
		return syscall.EINVAL
	}

	// Sticky-bit checks apply on BOTH the source and the destination directory
	// (POSIX): renaming a file out of a sticky directory OR into a sticky
	// directory both require the caller to own the file being moved (or be
	// root). Fetch the moved file's uid once and check each directory.
	srcChild, err := d.fs.Meta().Lookup(ctx, d.inodeID, name)
	if err != nil {
		rec.IncOpError("rename")
		return toErrno(err)
	}
	if err := d.checkStickyDir(ctx, d.inodeID, srcChild.UID); err != nil {
		return toErrno(err)
	}
	if err := d.checkStickyDir(ctx, newParentDir.inodeID, srcChild.UID); err != nil {
		return toErrno(err)
	}

	err = d.fs.Meta().Rename(ctx, d.inodeID, name, newParentDir.inodeID, newName)
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

// Symlink creates a symbolic link.
func (d *DFSDir) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	rec := recorderFor(d.recorder)
	rec.IncOp("symlink")

	metaInode, err := d.fs.Meta().Symlink(ctx, d.inodeID, name, target)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			return nil, syscall.EEXIST
		}
		logf("symlink error: %v", err)
		rec.IncOpError("symlink")
		return nil, syscall.EIO
	}

	// Symlinks do not inherit a setgid parent's gid (POSIX), so pass a nil parent.
	d.fs.applyMountOwner(ctx, metaInode, nil)
	child := &DFSSymlink{fs: d.fs, inodeID: metaInode.ID, recorder: rec}
	attr := inodeMetaToAttr(metaInode)

	inode := d.NewInode(ctx, child, fs.StableAttr{
		Mode: fuse.S_IFLNK,
		Ino:  uint64(metaInode.ID),
	})

	out.Attr = attr
	return inode, 0
}

// Link creates a hard link.
func (d *DFSDir) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	rec := recorderFor(d.recorder)
	rec.IncOp("link")

	var targetFile *DFSFile
	switch t := target.(type) {
	case *DFSFile:
		targetFile = t
	default:
		rec.IncOpError("link")
		return nil, syscall.EINVAL
	}

	metaInode, err := d.fs.Meta().Link(ctx, d.inodeID, name, targetFile.inodeID)
	if err != nil {
		logf("link error: %v", err)
		rec.IncOpError("link")
		return nil, syscall.EIO
	}

	dfs := d.fs
	child := newDFSFile(dfs, metaInode, rec)
	attr := inodeMetaToAttr(metaInode)

	inode := d.NewInode(ctx, child, fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  uint64(metaInode.ID),
	})

	out.Attr = attr
	return inode, 0
}

// Getattr returns the attributes of this directory inode.
func (d *DFSDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	rec := recorderFor(d.recorder)
	metaInode, err := d.fs.Meta().GetInode(ctx, d.inodeID)
	if err != nil {
		rec.IncOpError("getattr")
		return syscall.EIO
	}
	out.Attr = inodeMetaToAttr(metaInode)
	return 0
}

// Setattr sets attributes on this directory inode.
func (d *DFSDir) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	rec := recorderFor(d.recorder)
	metaInode, err := d.fs.Meta().GetInode(ctx, d.inodeID)
	if err != nil {
		rec.IncOpError("setattr")
		return syscall.EIO
	}

	if mode, ok := in.GetMode(); ok {
		metaInode.Mode = mode
	}
	if uid, ok := in.GetUID(); ok {
		metaInode.UID = uid
	}
	if gid, ok := in.GetGID(); ok {
		metaInode.GID = gid
	}
	metaInode.CTime = time.Now().UnixNano()

	if err := d.fs.Meta().UpdateInode(ctx, metaInode); err != nil {
		rec.IncOpError("setattr")
		return syscall.EIO
	}

	out.Attr = inodeMetaToAttr(metaInode)
	return 0
}

// Access evaluates the directory's POSIX mode bits against the requesting caller.
func (d *DFSDir) Access(ctx context.Context, mask uint32) syscall.Errno {
	return checkPOSIXAccess(ctx, d.fs, d.inodeID, mask)
}

// OpenXAttr returns an xattr handle for this directory.
func (d *DFSDir) OpenXAttr() *DFSXAttr {
	return &DFSXAttr{meta: d.fs.Meta(), inodeID: d.inodeID}
}
