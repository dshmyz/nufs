package metadata

import (
	"context"
	"testing"
)

// TestPebbleStore_ReadDirFrom_CursorPagination verifies that
// ReadDirFrom returns entries after the given cursor (last entry
// name from the previous page), enabling O(1) pagination instead
// of O(offset) skip.
func TestPebbleStore_ReadDirFrom_CursorPagination(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()

	ctx := context.Background()

	// Create a bucket and directory
	policy := PlacementPolicy{ReplicationFactor: 1, StorageTier: TierHot}
	if err := store.CreateBucket(ctx, "test-bucket", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	// Create 10 files with sortable names
	for i := 0; i < 10; i++ {
		name := string(rune('a'+i)) + ".txt"
		if _, err := store.CreateFile(ctx, bucket.RootInode, name, 0o644); err != nil {
			t.Fatalf("CreateFile %s: %v", name, err)
		}
	}

	// Page 1: read first 3 entries (cursor = "")
	page1, err := store.ReadDirFrom(ctx, bucket.RootInode, "", 3)
	if err != nil {
		t.Fatalf("ReadDirFrom page1: %v", err)
	}
	if len(page1) != 3 {
		t.Fatalf("page1: expected 3 entries, got %d", len(page1))
	}

	// Page 2: read next 3 entries (cursor = last name from page1)
	page2, err := store.ReadDirFrom(ctx, bucket.RootInode, page1[2].Name, 3)
	if err != nil {
		t.Fatalf("ReadDirFrom page2: %v", err)
	}
	if len(page2) != 3 {
		t.Fatalf("page2: expected 3 entries, got %d", len(page2))
	}

	// Verify no overlap between pages
	if page1[2].Name == page2[0].Name {
		t.Fatalf("cursor entry should not appear in next page: %s", page1[2].Name)
	}

	// Page 3: read next 3 entries
	page3, err := store.ReadDirFrom(ctx, bucket.RootInode, page2[2].Name, 3)
	if err != nil {
		t.Fatalf("ReadDirFrom page3: %v", err)
	}
	if len(page3) != 3 {
		t.Fatalf("page3: expected 3 entries, got %d", len(page3))
	}

	// Page 4: read remaining 1 entry
	page4, err := store.ReadDirFrom(ctx, bucket.RootInode, page3[2].Name, 3)
	if err != nil {
		t.Fatalf("ReadDirFrom page4: %v", err)
	}
	if len(page4) != 1 {
		t.Fatalf("page4: expected 1 entry, got %d", len(page4))
	}

	// Verify all 10 entries are unique and cover all files
	all := append(append(append(page1, page2...), page3...), page4...)
	if len(all) != 10 {
		t.Fatalf("total entries: expected 10, got %d", len(all))
	}
	seen := make(map[string]bool)
	for _, e := range all {
		if seen[e.Name] {
			t.Fatalf("duplicate entry: %s", e.Name)
		}
		seen[e.Name] = true
	}
}

// TestPebbleStore_ReadDirFrom_EmptyCursor verifies that an empty
// cursor returns entries from the beginning (same as offset=0).
func TestPebbleStore_ReadDirFrom_EmptyCursor(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()

	ctx := context.Background()

	policy := PlacementPolicy{ReplicationFactor: 1, StorageTier: TierHot}
	if err := store.CreateBucket(ctx, "test-bucket", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	store.CreateFile(ctx, bucket.RootInode, "x.txt", 0o644)
	store.CreateFile(ctx, bucket.RootInode, "y.txt", 0o644)

	// Empty cursor = start from beginning
	entries, err := store.ReadDirFrom(ctx, bucket.RootInode, "", 100)
	if err != nil {
		t.Fatalf("ReadDirFrom: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

// TestPebbleStore_ReadDirFrom_NonExistentCursor verifies that a
// cursor that doesn't match any entry still works: it returns
// entries strictly after the cursor in lexicographic order.
func TestPebbleStore_ReadDirFrom_NonExistentCursor(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()

	ctx := context.Background()

	policy := PlacementPolicy{ReplicationFactor: 1, StorageTier: TierHot}
	if err := store.CreateBucket(ctx, "test-bucket", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	store.CreateFile(ctx, bucket.RootInode, "a.txt", 0o644)
	store.CreateFile(ctx, bucket.RootInode, "b.txt", 0o644)
	store.CreateFile(ctx, bucket.RootInode, "c.txt", 0o644)

	// Cursor "b.txt" should return only "c.txt"
	entries, err := store.ReadDirFrom(ctx, bucket.RootInode, "b.txt", 100)
	if err != nil {
		t.Fatalf("ReadDirFrom: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after 'b.txt', got %d", len(entries))
	}
	if entries[0].Name != "c.txt" {
		t.Fatalf("expected 'c.txt', got %q", entries[0].Name)
	}
}

// TestPebbleStore_ReadDirFrom_EmptyDir verifies behavior on an
// empty directory.
func TestPebbleStore_ReadDirFrom_EmptyDir(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()

	ctx := context.Background()

	policy := PlacementPolicy{ReplicationFactor: 1, StorageTier: TierHot}
	if err := store.CreateBucket(ctx, "test-bucket", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	entries, err := store.ReadDirFrom(ctx, bucket.RootInode, "", 100)
	if err != nil {
		t.Fatalf("ReadDirFrom empty dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}
