//go:build linux

package fuse

import (
	"testing"
	"time"

	"github.com/example/dfs/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestInodeMetaToAttr_RegularFile(t *testing.T) {
	now := time.Now().UnixNano()
	m := &metadata.InodeMeta{
		ID:    42,
		Type:  metadata.FileRegular,
		Size:  1024,
		NLink: 1,
		Mode:  0644,
		UID:   1000,
		GID:   1000,
		MTime: now,
		CTime: now,
	}

	attr := inodeMetaToAttr(m)

	if attr.Ino != 42 {
		t.Errorf("expected Ino 42, got %d", attr.Ino)
	}
	if attr.Size != 1024 {
		t.Errorf("expected Size 1024, got %d", attr.Size)
	}
	if attr.Nlink != 1 {
		t.Errorf("expected Nlink 1, got %d", attr.Nlink)
	}
	if attr.Mode&fuse.S_IFREG == 0 {
		t.Error("expected S_IFREG bit set")
	}
	if attr.Owner.Uid != 1000 {
		t.Errorf("expected Uid 1000, got %d", attr.Owner.Uid)
	}
}

func TestInodeMetaToAttr_Directory(t *testing.T) {
	m := &metadata.InodeMeta{
		ID:   10,
		Type: metadata.FileDirectory,
		Mode: 0755,
	}

	attr := inodeMetaToAttr(m)
	if attr.Mode&fuse.S_IFDIR == 0 {
		t.Error("expected S_IFDIR bit set")
	}
}

func TestInodeMetaToAttr_Symlink(t *testing.T) {
	m := &metadata.InodeMeta{
		ID:      20,
		Type:    metadata.FileSymlink,
		Mode:    0777,
		Symlink: "/target/path",
	}

	attr := inodeMetaToAttr(m)
	if attr.Mode&fuse.S_IFLNK == 0 {
		t.Error("expected S_IFLNK bit set")
	}
}

func TestNewChildInode(t *testing.T) {
	// A minimal, real DFSFileSystem so newChildInode can read its meta/chunkStore
	// fields without a nil deref. The children's fields are irrelevant here (we
	// only assert the returned node type). (The previously-passed nil pointer
	// dereferenced dfs.meta/chunkStore and panicked for every file type.)
	dfs := &DFSFileSystem{}

	tests := []struct {
		fileType metadata.FileType
		name     string
	}{
		{metadata.FileRegular, "file"},
		{metadata.FileDirectory, "dir"},
		{metadata.FileSymlink, "symlink"},
		{metadata.FileFIFO, "fifo"},
		{metadata.FileCharDevice, "chardev"},
		{metadata.FileBlockDevice, "blockdev"},
		{metadata.FileSocket, "socket"},
	}

	for _, tt := range tests {
		m := &metadata.InodeMeta{ID: 1, Type: tt.fileType}
		child := newChildInode(dfs, m)
		var want string
		switch tt.fileType {
		case metadata.FileRegular:
			if _, ok := child.(*DFSFile); ok {
				want = "file"
			}
		case metadata.FileDirectory:
			if _, ok := child.(*DFSDir); ok {
				want = "dir"
			}
		case metadata.FileSymlink:
			if _, ok := child.(*DFSSymlink); ok {
				want = "symlink"
			}
		case metadata.FileFIFO, metadata.FileCharDevice, metadata.FileBlockDevice, metadata.FileSocket:
			if _, ok := child.(*DFSFifo); ok {
				want = "special"
			}
		}
		if want == "" {
			t.Errorf("newChildInode(%v) returned unexpected node type %T", tt.fileType, child)
		}
	}
}
