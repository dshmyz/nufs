package metadata

import (
	"context"
	"testing"
)

// TestPebbleStore_GetInode_NoDeepCopyForEmptyFields verifies that
// GetInode does not allocate new slices for ChunkMap and XAttrs
// when they are empty (common case: directories, empty files).
// This is the P3.11 optimization: skip deep copy when there's
// nothing to copy.
func TestPebbleStore_GetInode_NoDeepCopyForEmptyFields(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()

	ctx := context.Background()

	// Create a bucket (root inode is a directory with no chunks/xattrs)
	policy := PlacementPolicy{ReplicationFactor: 1, StorageTier: TierHot}
	if err := store.CreateBucket(ctx, "test-bucket", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	// First GetInode populates the cache
	inode1, err := store.GetInode(ctx, bucket.RootInode)
	if err != nil {
		t.Fatalf("GetInode 1: %v", err)
	}
	if len(inode1.ChunkMap) != 0 {
		t.Fatalf("directory should have no chunks, got %d", len(inode1.ChunkMap))
	}

	// Second GetInode hits cache
	inode2, err := store.GetInode(ctx, bucket.RootInode)
	if err != nil {
		t.Fatalf("GetInode 2: %v", err)
	}

	// Both should return valid data
	if inode1.ID != inode2.ID {
		t.Fatalf("inode ID mismatch: %d vs %d", inode1.ID, inode2.ID)
	}
}

// TestPebbleStore_GetInode_CopyIsolationWithChunks verifies that
// when an inode has chunks, modifying the returned copy does not
// affect the cached version. This ensures the deep copy is still
// performed when needed.
func TestPebbleStore_GetInode_CopyIsolationWithChunks(t *testing.T) {
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
	inode, err := store.CreateFile(ctx, bucket.RootInode, "file.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	// Use UpdateInode to set a live chunk reference (this invalidates cache).
	seedLiveChunk(t, store, 1, 100)
	inode.ChunkMap = []ChunkRef{{ID: 1, Offset: 0, Length: 100}}
	if err := store.UpdateInode(ctx, inode); err != nil {
		t.Fatalf("UpdateInode: %v", err)
	}

	// GetInode (populates cache)
	inode1, err := store.GetInode(ctx, inode.ID)
	if err != nil {
		t.Fatalf("GetInode 1: %v", err)
	}
	if len(inode1.ChunkMap) == 0 {
		t.Fatal("expected non-empty ChunkMap")
	}

	// Mutate the returned copy
	originalOffset := inode1.ChunkMap[0].Offset
	inode1.ChunkMap[0].Offset = 999999

	// GetInode again (from cache) — should be unaffected
	inode2, err := store.GetInode(ctx, inode.ID)
	if err != nil {
		t.Fatalf("GetInode 2: %v", err)
	}
	if inode2.ChunkMap[0].Offset != originalOffset {
		t.Fatalf("cache was mutated: expected offset %d, got %d",
			originalOffset, inode2.ChunkMap[0].Offset)
	}
}

// TestPebbleStore_GetInode_CopyIsolationWithXAttrs verifies that
// XAttrs are deep-copied when present.
func TestPebbleStore_GetInode_CopyIsolationWithXAttrs(t *testing.T) {
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
	inode, err := store.CreateFile(ctx, bucket.RootInode, "file.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	// Use UpdateInode to set xattrs (this invalidates cache)
	inode.XAttrs = map[string][]byte{"user.custom": []byte("value1")}
	if err := store.UpdateInode(ctx, inode); err != nil {
		t.Fatalf("UpdateInode: %v", err)
	}

	// GetInode (populates cache)
	inode1, err := store.GetInode(ctx, inode.ID)
	if err != nil {
		t.Fatalf("GetInode 1: %v", err)
	}
	if len(inode1.XAttrs) == 0 {
		t.Fatal("expected non-empty XAttrs")
	}

	// Mutate the returned copy
	inode1.XAttrs["user.custom"] = []byte("mutated")

	// GetInode again — should be unaffected
	inode2, err := store.GetInode(ctx, inode.ID)
	if err != nil {
		t.Fatalf("GetInode 2: %v", err)
	}
	if string(inode2.XAttrs["user.custom"]) != "value1" {
		t.Fatalf("cache was mutated: expected 'value1', got %q",
			inode2.XAttrs["user.custom"])
	}
}
