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

// DFSDir represents a directory inode in the DFS FUSE filesystem.
type DFSDir struct {
	fs.Inode

	meta    metadata.MetadataService
	inodeID metadata.InodeID
}

var _ = (fs.NodeReaddirer)((*DFSDir)(nil))
var _ = (fs.NodeLookuper)((*DFSDir)(nil))
var _ = (fs.NodeMkdirer)((*DFSDir)(nil))
var _ = (fs.NodeRmdirer)((*DFSDir)(nil))
var _ = (fs.NodeCreater)((*DFSDir)(nil))
var _ = (fs.NodeUnlinker)((*DFSDir)(nil))
var _ = (fs.NodeRenamer)((*DFSDir)(nil))
var _ = (fs.NodeSymlinker)((*DFSDir)(nil))
var _ = (fs.NodeLinker)((*DFSDir)(nil))
var _ = (fs.NodeGetattrer)((*DFSDir)(nil))
var _ = (fs.NodeSetattrer)((*DFSDir)(nil))

// Readdir lists directory entries.
func (d *DFSDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := d.meta.ReadDir(ctx, d.inodeID, 0, 10000)
	if err != nil {
		logf("readdir error: %v", err)
		return nil, syscall.EIO
	}

	var fuseEntries []fuse.DirEntry
	for _, e := range entries {
		var mode uint32
		switch e.Type {
		case metadata.FileDirectory:
			mode = fuse.S_IFDIR
		case metadata.FileRegular:
			mode = fuse.S_IFREG
		case metadata.FileSymlink:
			mode = fuse.S_IFLNK
		}
		fuseEntries = append(fuseEntries, fuse.DirEntry{
			Name: e.Name,
			Ino:  uint64(e.InodeID),
			Mode: mode,
		})
	}

	return fs.NewListDirStream(fuseEntries), 0
}

// Lookup looks up a child entry by name.
func (d *DFSDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	metaInode, err := d.meta.Lookup(ctx, d.inodeID, name)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) || errors.Is(err, metadata.ErrInodeNotFound) {
			return nil, syscall.ENOENT
		}
		logf("lookup error: %v", err)
		return nil, syscall.EIO
	}

	dfs := rootFromInode(&d.Inode)
	child := newChildInode(dfs, metaInode)
	attr := inodeMetaToAttr(metaInode)

	inode := d.NewInode(ctx, child, fs.StableAttr{
		Mode: attr.Mode,
		Ino:  attr.Ino,
	})

	out.Attr = attr
	return inode, 0
}

// rootFromInode walks up to the DFSFileSystem root so newChildInode
// can read the chunk store. go-fuse exposes the embedder tree via
// Inode.Root().Operations(), which is the DFSFileSystem instance we
// constructed in NewDFSFileSystem. The argument is a pointer so
// the embedded fs.Inode (which contains a sync.Mutex) is not copied.
func rootFromInode(inode *fs.Inode) *DFSFileSystem {
	root := inode.Root().Operations()
	if r, ok := root.(*DFSFileSystem); ok {
		return r
	}
	return nil
}

// Mkdir creates a new directory.
func (d *DFSDir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	metaInode, err := d.meta.MkDir(ctx, d.inodeID, name, mode)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			return nil, syscall.EEXIST
		}
		logf("mkdir error: %v", err)
		return nil, syscall.EIO
	}

	child := &DFSDir{meta: d.meta, inodeID: metaInode.ID}
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
	err := d.meta.RmDir(ctx, d.inodeID, name)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) {
			return syscall.ENOENT
		}
		if errors.Is(err, metadata.ErrDirNotEmpty) {
			return syscall.ENOTEMPTY
		}
		logf("rmdir error: %v", err)
		return syscall.EIO
	}
	return 0
}

// Create creates and opens a new file.
func (d *DFSDir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	metaInode, err := d.meta.CreateFile(ctx, d.inodeID, name, mode)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			// File exists — lookup and open it
			metaInode, err = d.meta.Lookup(ctx, d.inodeID, name)
			if err != nil {
				return nil, nil, 0, syscall.EIO
			}
		} else {
			logf("create error: %v", err)
			return nil, nil, 0, syscall.EIO
		}
	}

	dfs := rootFromInode(&d.Inode)
	file := &DFSFile{meta: d.meta, chunkStore: dfs.chunkStore, inodeID: metaInode.ID, lockOwner: dfs.lockOwner}
	attr := inodeMetaToAttr(metaInode)

	inode := d.NewInode(ctx, file, fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  uint64(metaInode.ID),
	})

	out.Attr = attr
	return inode, file, 0, 0
}

// Unlink removes a file entry.
func (d *DFSDir) Unlink(ctx context.Context, name string) syscall.Errno {
	err := d.meta.Unlink(ctx, d.inodeID, name)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) {
			return syscall.ENOENT
		}
		logf("unlink error: %v", err)
		return syscall.EIO
	}
	return 0
}

// Rename renames/moves a file or directory.
func (d *DFSDir) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	var newParentDir *DFSDir
	switch p := newParent.(type) {
	case *DFSDir:
		newParentDir = p
	default:
		return syscall.EINVAL
	}

	err := d.meta.Rename(ctx, d.inodeID, name, newParentDir.inodeID, newName)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) {
			return syscall.ENOENT
		}
		logf("rename error: %v", err)
		return syscall.EIO
	}
	return 0
}

// Symlink creates a symbolic link.
func (d *DFSDir) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	metaInode, err := d.meta.Symlink(ctx, d.inodeID, name, target)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			return nil, syscall.EEXIST
		}
		logf("symlink error: %v", err)
		return nil, syscall.EIO
	}

	child := &DFSSymlink{meta: d.meta, inodeID: metaInode.ID}
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
	var targetFile *DFSFile
	switch t := target.(type) {
	case *DFSFile:
		targetFile = t
	default:
		return nil, syscall.EINVAL
	}

	metaInode, err := d.meta.Link(ctx, d.inodeID, name, targetFile.inodeID)
	if err != nil {
		logf("link error: %v", err)
		return nil, syscall.EIO
	}

	dfs := rootFromInode(&d.Inode)
	child := &DFSFile{meta: d.meta, chunkStore: dfs.chunkStore, inodeID: metaInode.ID, lockOwner: dfs.lockOwner}
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
	metaInode, err := d.meta.GetInode(ctx, d.inodeID)
	if err != nil {
		return syscall.EIO
	}
	out.Attr = inodeMetaToAttr(metaInode)
	return 0
}

// Setattr sets attributes on this directory inode.
func (d *DFSDir) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	metaInode, err := d.meta.GetInode(ctx, d.inodeID)
	if err != nil {
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

	if err := d.meta.UpdateInode(ctx, metaInode); err != nil {
		return syscall.EIO
	}

	out.Attr = inodeMetaToAttr(metaInode)
	return 0
}
