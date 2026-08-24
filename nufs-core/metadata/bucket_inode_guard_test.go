package metadata

import (
	"context"
	"fmt"
	"testing"
)

// TestCreateBucketDoesNotOverwriteUnderSeededInodeRow is the bucket-path
// regression test for the inode-ID-reuse corruption class (multi-metad
// leader-failover drill: 36/99 objects corrupted). The namespace create paths
// guard the freshly minted /inode/<id> row with an ExpectAbsent precondition
// (buildNamespaceConditionalWithInodeGuard) and re-seed on conflict, but
// CreateBucket writes its bucket-root inode row through a plain non-conditional
// batch: under the exact under-seeded cold-scan window this series fixes (a
// newly elected leader whose FSM has not yet replayed a prior leader's inodes),
// a stale mint collides with a committed row and silently overwrites it.
//
// This non-raft test reproduces the mechanism deterministically: a committed
// inode row exists at ID 999, and inodeSeq is artificially lowered to 998 so
// the next mint lands on 999 (the same way an apply-lag window makes the
// cold-cache scan under-read the committed maximum). Without the guard the
// bucket root silently replaces the live row; with it, the create must either
// fail or re-seed and mint strictly above 999 — never clobbering the row.
func TestCreateBucketDoesNotOverwriteUnderSeededInodeRow(t *testing.T) {
	dir := t.TempDir()
	st, err := NewPebbleStore(PebbleStoreConfig{Dir: dir, NodeID: 1})
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()

	ctx := context.Background()
	policy := PlacementPolicy{ReplicationFactor: 1}
	if err := st.CreateBucket(ctx, "fs", policy); err != nil {
		t.Fatalf("create baseline bucket: %v", err)
	}

	// Seed a committed inode row whose ID lies beyond the under-seeded counter.
	// In production this row was committed by a previous leader but not yet
	// replayed into this store's FSM when the cold-cache scan ran.
	liveID := InodeID(999)
	if err := st.putMsgpack(fmt.Sprintf("%s%d", prefixInode, liveID),
		&InodeMeta{ID: liveID, Type: FileRegular, Mode: 0o644, NLink: 1}); err != nil {
		t.Fatalf("seed live inode row: %v", err)
	}

	// Under-seed the counter so the next mint collides with the live row —
	// the deterministic stand-in for the apply-lag cold-scan window.
	st.inodeSeq.Store(uint64(liveID) - 1)

	if err := st.CreateBucket(ctx, "other", policy); err != nil {
		t.Fatalf("create bucket under under-seeded counter: %v", err)
	}

	// The live row must be untouched — the bucket root must NOT have replaced it.
	var live InodeMeta
	if _, err := st.getValue(fmt.Sprintf("%s%d", prefixInode, liveID), &live); err != nil {
		t.Fatalf("read live inode row: %v", err)
	}
	if live.ID != liveID || live.Type != FileRegular || live.NLink != 1 {
		t.Fatalf("live inode row %d was overwritten by CreateBucket: got %+v (want ID=%d Type=FileRegular NLink=1)", liveID, live, liveID)
	}

	// The bucket must have re-seeded and minted strictly above the live row.
	bucket, err := st.GetBucket(ctx, "other")
	if err != nil {
		t.Fatalf("get new bucket: %v", err)
	}
	if bucket.RootInode <= liveID {
		t.Fatalf("bucket root inode %d collides with live row %d (reuse would overwrite it)", bucket.RootInode, liveID)
	}
	t.Logf("OK: bucket %q rooted at inode %d, live row %d untouched", bucket.Name, bucket.RootInode, liveID)
}
