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
	tests := []struct {
		fileType metadata.FileType
		name     string
	}{
		{metadata.FileRegular, "file"},
		{metadata.FileDirectory, "dir"},
		{metadata.FileSymlink, "symlink"},
	}

	for _, tt := range tests {
		m := &metadata.InodeMeta{ID: 1, Type: tt.fileType}
		child := newChildInode(nil, m)
		switch tt.name {
		case "file":
			if _, ok := child.(*DFSFile); !ok {
				t.Errorf("expected *DFSFile for %s", tt.name)
			}
		case "dir":
			if _, ok := child.(*DFSDir); !ok {
				t.Errorf("expected *DFSDir for %s", tt.name)
			}
		case "symlink":
			if _, ok := child.(*DFSSymlink); !ok {
				t.Errorf("expected *DFSSymlink for %s", tt.name)
			}
		}
	}
}
