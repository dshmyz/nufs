//go:build linux

package fuse

import (
	"context"
	"fmt"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// newTestDir returns a DFSDir rooted at the "test" bucket's root inode,
// backed by the in-memory PebbleStore. The bucket already contains the
// pre-created "hello.txt" file (see newTestMetaStore).
func newTestDir(t *testing.T) (*metadata.PebbleStore, *DFSDir) {
	t.Helper()
	store, _ := newTestMetaStore(t)
	bucket, err := store.GetBucket(context.Background(), "test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	dir := &DFSDir{meta: store, inodeID: bucket.RootInode}
	return store, dir
}

// TestDFSDir_Readdir_LargeDirectory_NoTruncation is the regression test for
// the Readdir silent-truncation bug. The old implementation fetched the
// directory with a single ReadDir(offset=0, limit=10000); the metadata layer
// caps a single read at 10k entries, so any directory with more than 10000
// entries was silently listed short. Readdir now pages through the O(limit)
// cursor-based ReadDirFrom, so arbitrarily large directories enumerate fully.
//
// We create >10k entries (crossing both the per-page size and the old hard
// 10k cap) and assert every one appears exactly once in the DirStream.
func TestDFSDir_Readdir_LargeDirectory_NoTruncation(t *testing.T) {
	store, dir := newTestDir(t)
	ctx := context.Background()

	// Create enough entries to exceed the old 10k hard cap by a clear
	// margin. 10050 total (10,050 + the pre-created hello.txt = 10,051).
	const n = 10050
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("file-%05d", i)
		if _, err := store.CreateFile(ctx, dir.inodeID, name, 0o644); err != nil {
			t.Fatalf("CreateFile %q: %v", name, err)
		}
	}
	expected := n + 1 // files we added + the pre-created hello.txt

	ds, errno := dir.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("Readdir: errno=%v", errno)
	}

	seen := make(map[string]bool, expected)
	var names []string
	for ds.HasNext() {
		entry, e := ds.Next()
		if e != 0 {
			t.Fatalf("DirStream.Next: errno=%v", e)
		}
		if seen[entry.Name] {
			t.Fatalf("duplicate entry %q in Readdir", entry.Name)
		}
		seen[entry.Name] = true
		names = append(names, entry.Name)
	}

	if len(names) != expected {
		t.Fatalf("Readdir returned %d entries, want %d (>10k: truncation regression)",
			len(names), expected)
	}

	// Sanity: the pre-created file and at least the boundary entries are all
	// present — no page got dropped and the pagination cursor never skipped.
	for _, probe := range []string{"hello.txt", "file-00000", "file-04095", "file-04096", "file-09999", "file-10049"} {
		if !seen[probe] {
			t.Fatalf("Readdir missing expected entry %q", probe)
		}
	}
}
