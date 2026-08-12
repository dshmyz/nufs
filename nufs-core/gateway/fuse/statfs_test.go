//go:build linux

package fuse

import (
	"context"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// TestStatfs_QuotaBranch pins the quota path of Statfs: when a bucket has a
// byte quota, `df` reports Blocks == quota (not cluster-wide totals, not zero),
// and the whole quota as free when the bucket holds no data yet. This guards
// against the previous behavior where Bfree was hardcoded to Blocks regardless
// of the bucket, and against the quota branch being dropped entirely.
func TestStatfs_QuotaBranch(t *testing.T) {
	store, _ := newTestMetaStore(t)
	ctx := context.Background()

	bucket, err := store.GetBucket(ctx, "test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	dfs := NewDFSFileSystem(store, nil, nil, nil, nil)
	dfs.bucketName = "test"
	dfs.bucketRoot = bucket.RootInode

	const quotaBytes = uint64(8 << 30) // 8 GiB
	if err := store.SetBucketQuota(ctx, "test", &metadata.BucketQuota{MaxSizeBytes: int64(quotaBytes)}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}

	var out fuse.StatfsOut
	if errno := dfs.Statfs(ctx, &out); errno != 0 {
		t.Fatalf("Statfs errno = %d, want 0", errno)
	}

	const blockSize = uint64(4096)
	wantBlocks := quotaBytes / blockSize
	if got := uint64(out.Blocks); got != wantBlocks {
		t.Errorf("Blocks = %d, want %d (quota/4096)", got, wantBlocks)
	}
	// Empty bucket: the whole quota is free.
	if got := uint64(out.Bfree); got != wantBlocks {
		t.Errorf("Bfree = %d, want %d (full quota free on empty bucket)", got, wantBlocks)
	}
	if uint64(out.Bsize) != blockSize {
		t.Errorf("Bsize = %d, want %d", out.Bsize, blockSize)
	}
}
