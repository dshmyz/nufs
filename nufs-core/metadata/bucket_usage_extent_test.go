package metadata

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// ========== Roadmap §1.4: bucket 配额按 extent 聚合 ==========

// newBucketUsageStore builds an in-memory store with bucket-stats counters
// either enabled (fast-path deltas) or disabled (slow-path rebuild).
func newBucketUsageStore(t *testing.T, useBucketStats bool) *PebbleStore {
	t.Helper()
	store, err := NewPebbleStore(PebbleStoreConfig{
		Dir:            fmt.Sprintf("bucket-usage-%d", time.Now().UnixNano()),
		UseInMemory:    true,
		NodeID:         1,
		UseBucketStats: useBucketStats,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// newUsageBucket creates a bucket and returns its root inode.
func newUsageBucket(t *testing.T, store *PebbleStore, name string) InodeID {
	t.Helper()
	if err := store.CreateBucket(context.Background(), name, PlacementPolicy{
		ID: "default", ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	b, err := store.GetBucket(context.Background(), name)
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	return b.RootInode
}

// TestComputeAllBucketUsage_AggregatesByExtent is the core slow-path test
// (UseBucketStats=false forces the rebuild): a V1 file sizes by its inode
// Size, a V2 inline file by its inline extent's LogicalLen, and a V2 pages
// file by the sum of its resolved extent pages' LogicalLen. The V2 inode
// Size counters are seeded deliberately out of sync with the extents (9000 /
// 9999 vs 4096 / 3072) to prove the rebuild aggregates from the extents
// themselves rather than the Size counter — before this knife the scan decoded
// only InodeMeta and read meta.Size off every row, so a drifted counter would
// mis-report.
func TestComputeAllBucketUsage_AggregatesByExtent(t *testing.T) {
	ctx := context.Background()
	store := newBucketUsageStore(t, false)
	root := newUsageBucket(t, store, "b")

	// V1 file: ChunkMap model, inode Size authoritative.
	v1, err := store.CreateFile(ctx, root, "v1.bin", 0644)
	if err != nil {
		t.Fatalf("CreateFile v1: %v", err)
	}
	v1.Size = 512
	if err := store.UpdateInode(ctx, v1); err != nil {
		t.Fatalf("UpdateInode v1: %v", err)
	}

	// V2 inline file: inline extent's LogicalLen (4096), Size deliberately
	// drifted to 9000 to prove the rebuild reads the extent.
	vi, err := store.CreateFile(ctx, root, "v2i.bin", 0644)
	if err != nil {
		t.Fatalf("CreateFile v2i: %v", err)
	}
	inlineExt := &ExtentMetaV2{ID: ExtentIDV2(60001), Generation: 1, LogicalLen: 4096}
	if err := store.putExtentMeta(inlineExt); err != nil {
		t.Fatalf("putExtentMeta inline: %v", err)
	}
	if err := NewInodeStoreV2(store).SetInlineExtent(vi.ID, inlineExt, 9000); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}

	// V2 pages file: two extents (1024 + 2048), Size deliberately drifted to
	// 9999 to prove the rebuild sums the resolved pages.
	vp, err := store.CreateFile(ctx, root, "v2p.bin", 0644)
	if err != nil {
		t.Fatalf("CreateFile v2p: %v", err)
	}
	for _, id := range []uint64{60002, 60003} {
		ext := &ExtentMetaV2{ID: ExtentIDV2(id), Generation: 1, LogicalLen: 1024}
		if id == 60003 {
			ext.LogicalLen = 2048
		}
		if err := store.putExtentMeta(ext); err != nil {
			t.Fatalf("putExtentMeta pages %d: %v", id, err)
		}
	}
	writes := []ExtentWrite{
		{Extent: &ExtentMetaV2{ID: ExtentIDV2(60002), LogicalLen: 1024}, Offset: 0},
		{Extent: &ExtentMetaV2{ID: ExtentIDV2(60003), LogicalLen: 2048}, Offset: 1024},
	}
	if err := NewInodeStoreV2(store).ReplaceExtents(vp.ID, writes, 9999); err != nil {
		t.Fatalf("ReplaceExtents: %v", err)
	}

	usages, err := store.ComputeAllBucketUsage(ctx)
	if err != nil {
		t.Fatalf("ComputeAllBucketUsage: %v", err)
	}
	if len(usages) != 1 {
		t.Fatalf("usage buckets = %d, want 1", len(usages))
	}
	got := usages[0]
	if got.UsedBytes != 7680 {
		t.Fatalf("UsedBytes = %d, want 7680 (512 + 4096 + 3072, from extents not Size)", got.UsedBytes)
	}
	if got.Objects != 3 {
		t.Fatalf("Objects = %d, want 3", got.Objects)
	}
}

// TestComputeAllBucketUsage_FallsBackOnDanglingExtent verifies a pages file
// with a missing /extent-meta/ row (dangling reference from a torn write)
// does not fail the rebuild and never under-counts: the inode Size (which
// includes all extents) is used as the fallback. The Size is deliberately
// drifted (9999) so the assertion proves the fallback returns the full inode
// Size, not the partial sum of present extents (1024).
func TestComputeAllBucketUsage_FallsBackOnDanglingExtent(t *testing.T) {
	ctx := context.Background()
	store := newBucketUsageStore(t, false)
	root := newUsageBucket(t, store, "b")

	vp, err := store.CreateFile(ctx, root, "vp.bin", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	// 60012 row exists; 60013 row deliberately absent (dangling reference).
	if err := store.putExtentMeta(&ExtentMetaV2{ID: ExtentIDV2(60012), Generation: 1, LogicalLen: 1024}); err != nil {
		t.Fatalf("putExtentMeta: %v", err)
	}
	writes := []ExtentWrite{
		{Extent: &ExtentMetaV2{ID: ExtentIDV2(60012), LogicalLen: 1024}, Offset: 0},
		{Extent: &ExtentMetaV2{ID: ExtentIDV2(60013), LogicalLen: 2048}, Offset: 1024},
	}
	if err := NewInodeStoreV2(store).ReplaceExtents(vp.ID, writes, 9999); err != nil {
		t.Fatalf("ReplaceExtents: %v", err)
	}

	usages, err := store.ComputeAllBucketUsage(ctx)
	if err != nil {
		t.Fatalf("ComputeAllBucketUsage: %v", err)
	}
	if len(usages) != 1 {
		t.Fatalf("usage buckets = %d, want 1", len(usages))
	}
	if usages[0].UsedBytes != 9999 {
		t.Fatalf("UsedBytes = %d, want 9999 (dangling extent falls back to inode Size, not partial sum)", usages[0].UsedBytes)
	}
}

// TestEnsureBucketStats_SeedsUsageForExistingV2Bucket verifies the migration
// seed: a store opened with bucket-stats counters disabled writes a V2 inline
// file, and reopening the same directory with counters enabled seeds the
// /bucket-stats/ row from the model-aware slow-path rebuild.
func TestEnsureBucketStats_SeedsUsageForExistingV2Bucket(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	s1, err := NewPebbleStore(PebbleStoreConfig{Dir: dir, NodeID: 1, UseBucketStats: false})
	if err != nil {
		t.Fatalf("NewPebbleStore (seed): %v", err)
	}
	root := newUsageBucket(t, s1, "b")
	vi, err := s1.CreateFile(ctx, root, "v2i.bin", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	ext := &ExtentMetaV2{ID: ExtentIDV2(70001), Generation: 1, LogicalLen: 4096}
	if err := s1.putExtentMeta(ext); err != nil {
		t.Fatalf("putExtentMeta: %v", err)
	}
	if err := NewInodeStoreV2(s1).SetInlineExtent(vi.ID, ext, 4096); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close (seed): %v", err)
	}

	s2, err := NewPebbleStore(PebbleStoreConfig{Dir: dir, NodeID: 1, UseBucketStats: true})
	if err != nil {
		t.Fatalf("NewPebbleStore (reopen): %v", err)
	}
	defer s2.Close()
	usage, err := s2.GetBucketUsage(ctx, "b")
	if err != nil {
		t.Fatalf("GetBucketUsage: %v", err)
	}
	if usage.UsedBytes != 4096 {
		t.Fatalf("seeded UsedBytes = %d, want 4096", usage.UsedBytes)
	}
	if usage.Objects != 1 {
		t.Fatalf("seeded Objects = %d, want 1", usage.Objects)
	}
}

// TestUnlink_DecrementsV2BucketUsage verifies the fast-path counter decrement
// on a V2 inline file: the inode row and the /bucket-stats/ row share the
// same raft batch, so deleting the file drops usage to zero.
func TestUnlink_DecrementsV2BucketUsage(t *testing.T) {
	ctx := context.Background()
	store := newBucketUsageStore(t, true)
	root := newUsageBucket(t, store, "b")

	vi, err := store.CreateFile(ctx, root, "v2i.bin", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	ext := &ExtentMetaV2{ID: ExtentIDV2(70002), Generation: 1, LogicalLen: 4096}
	if err := store.putExtentMeta(ext); err != nil {
		t.Fatalf("putExtentMeta: %v", err)
	}
	if err := NewInodeStoreV2(store).SetInlineExtent(vi.ID, ext, 4096); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}

	usage, err := store.GetBucketUsage(ctx, "b")
	if err != nil {
		t.Fatalf("GetBucketUsage before: %v", err)
	}
	if usage.UsedBytes != 4096 || usage.Objects != 1 {
		t.Fatalf("usage before = %d bytes / %d objects, want 4096/1", usage.UsedBytes, usage.Objects)
	}

	if err := store.Unlink(ctx, root, "v2i.bin"); err != nil {
		t.Fatalf("Unlink: %v", err)
	}
	usage, err = store.GetBucketUsage(ctx, "b")
	if err != nil {
		t.Fatalf("GetBucketUsage after: %v", err)
	}
	if usage.UsedBytes != 0 || usage.Objects != 0 {
		t.Fatalf("usage after = %d bytes / %d objects, want 0/0", usage.UsedBytes, usage.Objects)
	}
}

// TestLinkAndUnlink_PreserveV2LayoutAcrossHardLinks pins the corruption bug
// fixed this knife: Link and Unlink's NLink>1 branch used to re-encode the
// shared /inode/ row as V1 InodeMeta, silently stripping Layout/InlineExtent/
// ExtentRoot from a V2-layout file. Now both preserve the row's own model, so
// a hard-linked V2 file keeps its layout, stays resolvable, and only returns
// to zero usage when the last link is unlinked.
func TestLinkAndUnlink_PreserveV2LayoutAcrossHardLinks(t *testing.T) {
	ctx := context.Background()
	store := newBucketUsageStore(t, true)
	root := newUsageBucket(t, store, "b")

	vi, err := store.CreateFile(ctx, root, "a.bin", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	ext := &ExtentMetaV2{ID: ExtentIDV2(70003), Generation: 1, LogicalLen: 4096}
	if err := store.putExtentMeta(ext); err != nil {
		t.Fatalf("putExtentMeta: %v", err)
	}
	if err := NewInodeStoreV2(store).SetInlineExtent(vi.ID, ext, 4096); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}

	// Link adds a name; usage must not change (objects are unique-inode).
	linked, err := store.Link(ctx, root, "a-hard.bin", vi.ID)
	if err != nil {
		t.Fatalf("Link: %v", err)
	}
	if linked.NLink != 2 {
		t.Fatalf("linked NLink = %d, want 2", linked.NLink)
	}
	assertV2LayoutPreserved(t, store, vi.ID)
	usage, err := store.GetBucketUsage(ctx, "b")
	if err != nil {
		t.Fatalf("GetBucketUsage after link: %v", err)
	}
	if usage.UsedBytes != 4096 || usage.Objects != 1 {
		t.Fatalf("usage after link = %d/%d, want 4096/1", usage.UsedBytes, usage.Objects)
	}

	// Unlink one name: NLink 2→1, layout preserved, usage untouched.
	if err := store.Unlink(ctx, root, "a.bin"); err != nil {
		t.Fatalf("Unlink: %v", err)
	}
	in, err := NewInodeStoreV2(store).Get(vi.ID)
	if err != nil {
		t.Fatalf("Get after unlink: %v", err)
	}
	if in.NLink != 1 {
		t.Fatalf("NLink after unlink = %d, want 1", in.NLink)
	}
	assertV2LayoutPreserved(t, store, vi.ID)
	usage, err = store.GetBucketUsage(ctx, "b")
	if err != nil {
		t.Fatalf("GetBucketUsage after unlink: %v", err)
	}
	if usage.UsedBytes != 4096 || usage.Objects != 1 {
		t.Fatalf("usage after unlink = %d/%d, want 4096/1 (hard link keeps data alive)", usage.UsedBytes, usage.Objects)
	}

	// Unlink last name: inode deleted, usage → 0.
	if err := store.Unlink(ctx, root, "a-hard.bin"); err != nil {
		t.Fatalf("Unlink last: %v", err)
	}
	in, err = NewInodeStoreV2(store).Get(vi.ID)
	if err != nil {
		t.Fatalf("Get after final unlink: %v", err)
	}
	if in != nil {
		t.Fatalf("inode still present after final unlink: %+v", in)
	}
	usage, err = store.GetBucketUsage(ctx, "b")
	if err != nil {
		t.Fatalf("GetBucketUsage after final unlink: %v", err)
	}
	if usage.UsedBytes != 0 || usage.Objects != 0 {
		t.Fatalf("usage after final unlink = %d/%d, want 0/0", usage.UsedBytes, usage.Objects)
	}
}

// assertV2LayoutPreserved asserts the inline layout survived a Link/Unlink
// rewrite: layout field, extent reference, and resolvability all intact.
func assertV2LayoutPreserved(t *testing.T, store *PebbleStore, id InodeID) {
	t.Helper()
	in, err := NewInodeStoreV2(store).Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if in == nil {
		t.Fatalf("inode %d missing", id)
	}
	if in.Layout != LayoutInlineExtent {
		t.Fatalf("layout = %d, want LayoutInlineExtent (V2 fields stripped?)", in.Layout)
	}
	if in.InlineExtent == nil || in.InlineExtent.ID != ExtentIDV2(70003) {
		t.Fatalf("inline extent lost: %+v", in.InlineExtent)
	}
	refs, err := NewInodeStoreV2(store).ResolveExtents(id)
	if err != nil {
		t.Fatalf("ResolveExtents: %v", err)
	}
	if len(refs) != 1 || refs[0].ExtentID != ExtentIDV2(70003) {
		t.Fatalf("resolved extents = %+v, want [70003]", refs)
	}
}
