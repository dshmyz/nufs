//go:build linux

package fuse

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"time"

	"github.com/example/dfs/metadata"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// ========== DFSFile: regular file inode ==========

// DFSFile represents a regular file in the DFS FUSE filesystem.
type DFSFile struct {
	fs.Inode

	meta    metadata.MetadataService
	inodeID metadata.InodeID

	// Write buffer for small writes before flush
	mu     sync.Mutex
	dirty  bool
	buffer []byte
}

var _ = (fs.NodeOpener)((*DFSFile)(nil))
var _ = (fs.NodeReader)((*DFSFile)(nil))
var _ = (fs.NodeWriter)((*DFSFile)(nil))
var _ = (fs.NodeGetattrer)((*DFSFile)(nil))
var _ = (fs.NodeSetattrer)((*DFSFile)(nil))
var _ = (fs.NodeFsyncer)((*DFSFile)(nil))
var _ = (fs.NodeFlusher)((*DFSFile)(nil))
var _ = (fs.NodeReleaser)((*DFSFile)(nil))

// DFSFileHandle wraps DFSFile for per-open-file state.
type DFSFileHandle struct {
	file *DFSFile
}

// Open opens the file.
func (f *DFSFile) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return &DFSFileHandle{file: f}, 0, 0
}

// Read reads data from the file.
func (f *DFSFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	metaInode, err := f.meta.GetInode(ctx, f.inodeID)
	if err != nil {
		return nil, syscall.EIO
	}

	// Clamp read to file size
	if off >= metaInode.Size {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > metaInode.Size {
		end = metaInode.Size
	}

	// In production: read from data nodes via chunk map
	// For now, return zeros for unwritten regions
	size := end - off
	data := make([]byte, size)
	return fuse.ReadResultData(data), 0
}

// Write writes data to the file.
func (f *DFSFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Buffer write data locally until flush
	needed := int(off) + len(data)
	if needed > len(f.buffer) {
		newBuf := make([]byte, needed)
		copy(newBuf, f.buffer)
		f.buffer = newBuf
	}
	copy(f.buffer[off:], data)
	f.dirty = true

	return uint32(len(data)), 0
}

// Flush flushes buffered writes.
func (f *DFSFile) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.dirty {
		return 0
	}

	// In production: write buffer to data nodes, allocate chunks,
	// register chunk metadata, update inode size.
	// For now, just update the inode size.
	metaInode, err := f.meta.GetInode(ctx, f.inodeID)
	if err != nil {
		return syscall.EIO
	}

	newSize := int64(len(f.buffer))
	if newSize > metaInode.Size {
		metaInode.Size = newSize
	}
	metaInode.MTime = time.Now().UnixNano()

	if err := f.meta.UpdateInode(ctx, metaInode); err != nil {
		return syscall.EIO
	}

	f.dirty = false
	return 0
}

// Fsync syncs file data to persistent storage.
func (f *DFSFile) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	return f.Flush(ctx, fh)
}

// Release is called when the last reference to the file handle is dropped.
func (f *DFSFile) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	return f.Flush(ctx, fh)
}

// Getattr returns file attributes.
func (f *DFSFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	metaInode, err := f.meta.GetInode(ctx, f.inodeID)
	if err != nil {
		return syscall.EIO
	}
	out.Attr = inodeMetaToAttr(metaInode)
	return 0
}

// Setattr sets file attributes (truncate, chmod, etc.).
func (f *DFSFile) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	metaInode, err := f.meta.GetInode(ctx, f.inodeID)
	if err != nil {
		return syscall.EIO
	}

	if size, ok := in.GetSize(); ok {
		f.mu.Lock()
		if int(size) < len(f.buffer) {
			f.buffer = f.buffer[:size]
		} else if int(size) > len(f.buffer) {
			newBuf := make([]byte, size)
			copy(newBuf, f.buffer)
			f.buffer = newBuf
		}
		metaInode.Size = int64(size)
		f.dirty = true
		f.mu.Unlock()
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
	metaInode.MTime = time.Now().UnixNano()
	metaInode.CTime = time.Now().UnixNano()

	if err := f.meta.UpdateInode(ctx, metaInode); err != nil {
		return syscall.EIO
	}

	out.Attr = inodeMetaToAttr(metaInode)
	return 0
}

// ========== DFSFileHandle methods ==========

var _ = (fs.FileReader)((*DFSFileHandle)(nil))
var _ = (fs.FileWriter)((*DFSFileHandle)(nil))

func (h *DFSFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	return h.file.Read(ctx, h, dest, off)
}

func (h *DFSFileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	return h.file.Write(ctx, h, data, off)
}

// ========== DFSSymlink: symbolic link inode ==========

// DFSSymlink represents a symbolic link in the DFS FUSE filesystem.
type DFSSymlink struct {
	fs.Inode

	meta    metadata.MetadataService
	inodeID metadata.InodeID
}

var _ = (fs.NodeReadlinker)((*DFSSymlink)(nil))
var _ = (fs.NodeGetattrer)((*DFSSymlink)(nil))

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
