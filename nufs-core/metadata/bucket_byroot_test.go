package metadata

import (
	"context"
	"testing"
)

// TestPebbleStore_GetBucketByRoot verifies that a bucket can be
// looked up directly by its root inode ID, avoiding a full
// ListBuckets scan. This is the P1.5 optimization for FUSE
// resolveChunkPolicy.
func TestPebbleStore_GetBucketByRoot(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()

	ctx := context.Background()

	policy := PlacementPolicy{
		ReplicationFactor: 3,
		StorageTier:       TierHot,
	}
	if err := store.CreateBucket(ctx, "bucket-a", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucketA, err := store.GetBucket(ctx, "bucket-a")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	// Create a second bucket to ensure we don't accidentally match
	// the wrong one.
	policy2 := PlacementPolicy{
		ReplicationFactor: 2,
		StorageTier:       TierCold,
	}
	if err := store.CreateBucket(ctx, "bucket-b", policy2); err != nil {
		t.Fatalf("CreateBucket b: %v", err)
	}
	bucketB, err := store.GetBucket(ctx, "bucket-b")
	if err != nil {
		t.Fatalf("GetBucket b: %v", err)
	}

	// Lookup by root inode should return the correct bucket
	got, err := store.GetBucketByRoot(ctx, bucketA.RootInode)
	if err != nil {
		t.Fatalf("GetBucketByRoot: %v", err)
	}
	if got.Name != "bucket-a" {
		t.Fatalf("got name %q, want bucket-a", got.Name)
	}
	if got.Policy.ReplicationFactor != 3 {
		t.Fatalf("got RF %d, want 3", got.Policy.ReplicationFactor)
	}

	gotB, err := store.GetBucketByRoot(ctx, bucketB.RootInode)
	if err != nil {
		t.Fatalf("GetBucketByRoot b: %v", err)
	}
	if gotB.Name != "bucket-b" {
		t.Fatalf("got name %q, want bucket-b", gotB.Name)
	}

	// Non-existent root
	_, err = store.GetBucketByRoot(ctx, 999999)
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got %v", err)
	}
}

// TestPebbleStore_GetBucketByRoot_AfterDelete verifies that the
// reverse index is cleaned up when a bucket is deleted, so a
// subsequent lookup returns ErrBucketNotFound.
func TestPebbleStore_GetBucketByRoot_AfterDelete(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()

	ctx := context.Background()

	policy := PlacementPolicy{ReplicationFactor: 1, StorageTier: TierHot}
	if err := store.CreateBucket(ctx, "temp-bucket", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "temp-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	// Lookup works before delete
	_, err = store.GetBucketByRoot(ctx, bucket.RootInode)
	if err != nil {
		t.Fatalf("GetBucketByRoot before delete: %v", err)
	}

	// Delete the bucket
	if err := store.DeleteBucket(ctx, "temp-bucket"); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	// Lookup should fail after delete
	_, err = store.GetBucketByRoot(ctx, bucket.RootInode)
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound after delete, got %v", err)
	}
}
