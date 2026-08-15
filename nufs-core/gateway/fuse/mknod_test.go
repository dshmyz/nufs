//go:build linux

package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// typeFromReaddir must map the four special node types to the right S_IF*
// mode bits so Readdir shows fifos/devices/sockets with their true file type
// (else `ls -l` reports them as regular files).
func TestTypeFromReaddir_SpecialNodes(t *testing.T) {
	cases := []struct {
		ftype metadata.FileType
		mode  uint32
	}{
		{metadata.FileFIFO, fuse.S_IFIFO},
		{metadata.FileCharDevice, syscall.S_IFCHR},
		{metadata.FileBlockDevice, syscall.S_IFBLK},
		{metadata.FileSocket, syscall.S_IFSOCK},
	}
	for _, c := range cases {
		if mode := typeFromReaddir(c.ftype); mode != c.mode {
			t.Errorf("typeFromReaddir(%v) = %#x, want %#x", c.ftype, mode, c.mode)
		}
	}
}

// inodeMetaToAttr must stamp the S_IF* type bit and carry Rdev for devices so
// the kernel sees a real chardev/blkdev/sock/fifo in stat (Rdev used by mknod).
func TestInodeMetaToAttr_SpecialNodes(t *testing.T) {
	cases := []struct {
		ftype metadata.FileType
		mode  uint32
		rdev  uint32
	}{
		{metadata.FileFIFO, fuse.S_IFIFO, 0},
		{metadata.FileCharDevice, syscall.S_IFCHR, 0x0103},  // mknod c 1 3
		{metadata.FileBlockDevice, syscall.S_IFBLK, 0x0801}, // mknod b 8 1
		{metadata.FileSocket, syscall.S_IFSOCK, 0},
	}
	for _, c := range cases {
		m := &metadata.InodeMeta{ID: 7, Type: c.ftype, Mode: 0600, Rdev: c.rdev}
		attr := inodeMetaToAttr(m)
		if attr.Mode&c.mode == 0 {
			t.Errorf("inodeMetaToAttr(%v) missing type bit %#x, Mode=%#x", c.ftype, c.mode, attr.Mode)
		}
		// Type bit must be exclusive with the other S_IF* bits.
		typ := attr.Mode & syscall.S_IFMT
		if typ != c.mode {
			t.Errorf("inodeMetaToAttr(%v) S_IFMT=%#x, want %#x", c.ftype, typ, c.mode)
		}
		if got := uint32(attr.Rdev); got != c.rdev {
			t.Errorf("inodeMetaToAttr(%v) Rdev=%d, want %d", c.ftype, got, c.rdev)
		}
	}
}

// errnoForCreateNode maps metadata create errors to the FUSE errno, with
// EEXIST for a duplicate-name creation (the "not an error" case) and EIO for
// everything else.
func TestErrnoForCreateNode(t *testing.T) {
	if errnoForCreateNode(nil) != 0 {
		t.Error("nil error should map to 0")
	}
	if errnoForCreateNode(metadata.ErrEntryExists) != syscall.EEXIST {
		t.Error("ErrEntryExists should map to EEXIST")
	}
	if errnoForCreateNode(metadata.ErrInodeNotFound) != syscall.EIO {
		t.Error("other errors should map to EIO")
	}
}

// TestDFSFifo_Node is the thin-node behavior for special nodes created by
// mknod: Getattr returns the S_IF* type + Rdev with size 0, Setattr applies
// chmod/chown, Open returns a NONSEEKABLE handle for a FIFO (the kernel pipe
// owns the bytes) and refuses devices/sockets with EOPNOTSUPP, Release is a
// no-op.
func TestDFSFifo_Node(t *testing.T) {
	ctx := context.Background()

	t.Run("fifo_full_node", func(t *testing.T) {
		store, dir := newTestDir(t)
		metaInode, err := store.CreateNode(ctx, dir.inodeID, "pipe0", metadata.FileFIFO, 0o600, 0)
		if err != nil {
			t.Fatalf("CreateNode fifo: %v", err)
		}
		n := &DFSFifo{fs: dir.fs, inodeID: metaInode.ID}

		var out fuse.AttrOut
		if errno := n.Getattr(ctx, nil, &out); errno != 0 {
			t.Fatalf("Getattr: errno=%v", errno)
		}
		if out.Mode&fuse.S_IFIFO == 0 {
			t.Errorf("fifo Getattr missing S_IFIFO, Mode=%#x", out.Mode)
		}
		if out.Size != 0 {
			t.Errorf("fifo Getattr Size=%d, want 0", out.Size)
		}

		fh, flags, errno := n.Open(ctx, syscall.O_RDWR)
		if errno != 0 {
			t.Fatalf("fifo Open: errno=%v", errno)
		}
		if fh == nil && flags&fuse.FOPEN_NONSEEKABLE == 0 {
			t.Error("fifo Open should return NONSEEKABLE handle")
		}
		if n.Release(ctx, fh) != 0 {
			t.Error("fifo Release should be no-op errno=0")
		}
	})

	t.Run("device_open_refused", func(t *testing.T) {
		store, dir := newTestDir(t)
		metaInode, err := store.CreateNode(ctx, dir.inodeID, "null", metadata.FileCharDevice, 0o666, 0x0103)
		if err != nil {
			t.Fatalf("CreateNode chardev: %v", err)
		}
		n := &DFSFifo{fs: dir.fs, inodeID: metaInode.ID}

		var out fuse.AttrOut
		if errno := n.Getattr(ctx, nil, &out); errno != 0 {
			t.Fatalf("Getattr: errno=%v", errno)
		}
		if out.Mode&syscall.S_IFCHR == 0 {
			t.Errorf("chardev Getattr missing S_IFCHR, Mode=%#x", out.Mode)
		}
		if uint32(out.Rdev) != 0x0103 {
			t.Errorf("chardev Getattr Rdev=%d, want 0x0103", out.Rdev)
		}

		if _, _, errno := n.Open(ctx, syscall.O_RDWR); errno != syscall.EOPNOTSUPP {
			t.Errorf("chardev Open errno=%v, want EOPNOTSUPP", errno)
		}
	})

	t.Run("setattr_chmod_chown", func(t *testing.T) {
		store, dir := newTestDir(t)
		metaInode, err := store.CreateNode(ctx, dir.inodeID, "sock0", metadata.FileSocket, 0o600, 0)
		if err != nil {
			t.Fatalf("CreateNode sock: %v", err)
		}
		n := &DFSFifo{fs: dir.fs, inodeID: metaInode.ID}

		var in fuse.SetAttrIn
		in.Valid = fuse.FATTR_MODE
		in.Mode = 0o777
		var out fuse.AttrOut
		if errno := n.Setattr(ctx, nil, &in, &out); errno != 0 {
			t.Fatalf("Setattr chmod: errno=%v", errno)
		}
		if out.Mode&0777 != 0777 {
			t.Errorf("Setattr mode=%#o, want 0777", out.Mode&0777)
		}
	})

	t.Run("mknod_invalid_mode_einval", func(t *testing.T) {
		_, dir := newTestDir(t)
		var out fuse.EntryOut
		// No recognized S_IFMT type bit → Mknod rejects with EINVAL before any
		// metadata write (so no bridge/NewInode is needed for this path).
		if _, errno := dir.Mknod(ctx, "bad", 0o600&^uint32(syscall.S_IFMT), 0, &out); errno != syscall.EINVAL {
			t.Errorf("Mknod with no S_IFMT errno=%v, want EINVAL", errno)
		}
	})
}

// TestDFSFifo_Open_MissingNode ensures a GetInode failure surfaces as EIO
// rather than a panic.
func TestDFSFifo_Open_MissingNode(t *testing.T) {
	store, _ := newTestMetaStore(t)
	dfs := NewDFSFileSystem(store, nil, nil, nil, nil)
	ctx := context.Background()
	n := &DFSFifo{fs: dfs, inodeID: 999999}
	if _, _, errno := n.Open(ctx, syscall.O_RDONLY); errno != syscall.EIO {
		t.Errorf("Open missing node errno=%v, want EIO", errno)
	}
}
