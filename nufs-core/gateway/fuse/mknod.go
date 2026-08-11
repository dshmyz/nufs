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

// DFSFifo is the thin inode backing special nodes created by mknod: FIFOs,
// char/block devices and unix sockets. These carry no payload in the
// object-store model — the kernel pipe fully owns FIFO read/write (the fs only
// needs node identity + correct attributes, as go-fuse's loopback Mknod shows),
// and device/socket I/O cannot be routed by a userspace FUSE fs. So this node
// answers only Getattr/Setattr(chmod,chown)/Open.
type DFSFifo struct {
	fs.Inode

	// fs is the owning filesystem root; stable across SwapMetadata swaps.
	fs      *DFSFileSystem
	inodeID metadata.InodeID

	// recorder 记录 FUSE 操作指标。nil 时不打点。
	recorder MetricsRecorder
}

var _ = (fs.NodeOpener)((*DFSFifo)(nil))
var _ = (fs.NodeReleaser)((*DFSFifo)(nil))
var _ = (fs.NodeGetattrer)((*DFSFifo)(nil))
var _ = (fs.NodeSetattrer)((*DFSFifo)(nil))
var _ = (fs.NodeAccesser)((*DFSFifo)(nil))

// Open returns a non-nil handle for a FIFO so the kernel performs the
// read-end/write-end rendezvous itself (FOPEN_NONSEEKABLE tells the VFS the
// fd is a pipe, not a seekable file). Char/block devices and sockets cannot be
// serviced by a userspace FUSE fs, so opening them is refused with EOPNOTSUPP.
func (n *DFSFifo) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	rec := recorderFor(n.recorder)
	rec.IncOp("open")

	metaInode, err := n.fs.Meta().GetInode(ctx, n.inodeID)
	if err != nil {
		rec.IncOpError("open")
		return nil, 0, syscall.EIO
	}
	if metaInode.Type != metadata.FileFIFO {
		// Device / socket nodes exist only for identity; there is no way to
		// attach real device or socket I/O to a userspace FUSE fd.
		return nil, 0, syscall.EOPNOTSUPP
	}
	return n, fuse.FOPEN_NONSEEKABLE, 0
}

// Release is a no-op: FIFO fds hold no filesystem-side state to drop.
func (n *DFSFifo) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	return 0
}

// Getattr returns the special node's attributes — size always 0, nlink 1, the
// S_IF* type bit and (for devices) Rdev.
func (n *DFSFifo) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	rec := recorderFor(n.recorder)
	metaInode, err := n.fs.Meta().GetInode(ctx, n.inodeID)
	if err != nil {
		rec.IncOpError("getattr")
		return syscall.EIO
	}
	out.Attr = inodeMetaToAttr(metaInode)
	return 0
}

// Setattr updates chmod/chown only. Size is not applicable to special nodes.
func (n *DFSFifo) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	rec := recorderFor(n.recorder)
	metaInode, err := n.fs.Meta().GetInode(ctx, n.inodeID)
	if err != nil {
		rec.IncOpError("setattr")
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
		if err := n.fs.Meta().UpdateInode(ctx, metaInode); err != nil {
			rec.IncOpError("setattr")
			return syscall.EIO
		}
	}

	out.Attr = inodeMetaToAttr(metaInode)
	return 0
}

// Access evaluates the fifo's POSIX mode bits against the requesting caller.
func (n *DFSFifo) Access(ctx context.Context, mask uint32) syscall.Errno {
	return checkPOSIXAccess(ctx, n.fs, n.inodeID, mask)
}

// errnoForCreateNode maps a CreateNode error to a FUSE errno (nil → 0).
func errnoForCreateNode(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errors.Is(err, metadata.ErrEntryExists) {
		return syscall.EEXIST
	}
	return syscall.EIO
}
