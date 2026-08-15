package metadata

import (
	"context"
	"errors"
	"testing"
)

// Tests for the PebbleStore serving surface of the V2.1 extent-layout inode
// model (ExtentInodeService in inode_v2_serving.go, roadmap stage 1 §1.3a):
// inline round-trip, promote+append pages round-trip, the V1 UpdateInode
// collision guard, and the error sentinels.

func TestExtentInodeService_InlineRoundTrip(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()
	id := InodeID(1001)

	if _, err := NewInodeStoreV2(store).CreateEmpty(id, FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}
	ext := &ExtentMetaV2{
		ID:           ExtentIDV2(0x10000001001),
		Generation:   1,
		LogicalLen:   4096,
		Checksum:     0xdeadbeef,
		PGID:         7,
		Lifecycle:    LifecycleReady,
		StorageClass: StorageClassHotReplica,
	}
	if err := store.SetInlineExtent(ctx, id, ext, 4096); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}

	refs, err := store.ResolveExtents(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].ExtentID != ext.ID {
		t.Fatalf("resolve inline refs = %+v, want [%d]", refs, ext.ID)
	}

	got, err := store.GetExtentMeta(ctx, ext.ID)
	if err != nil {
		t.Fatalf("GetExtentMeta: %v", err)
	}
	if got.PGID != ext.PGID || got.LogicalLen != ext.LogicalLen ||
		got.StorageClass != ext.StorageClass || got.Checksum != ext.Checksum ||
		got.Lifecycle != ext.Lifecycle {
		t.Fatalf("extent meta round-trip mismatch:\n got %+v\nwant %+v", got, ext)
	}
}

func TestExtentInodeService_PagesRoundTrip(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()
	id := InodeID(1002)

	if _, err := NewInodeStoreV2(store).CreateEmpty(id, FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}
	first := &ExtentMetaV2{ID: ExtentIDV2(0x20000002001), Generation: 1, LogicalLen: 4096, PGID: 1}
	if err := store.SetInlineExtent(ctx, id, first, 4096); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}
	if err := store.PromoteToPages(ctx, id); err != nil {
		t.Fatalf("PromoteToPages: %v", err)
	}
	second := &ExtentMetaV2{ID: ExtentIDV2(0x20000002002), Generation: 1, LogicalLen: 8192, PGID: 2}
	if _, err := store.AppendExtent(ctx, id, second, 4096); err != nil {
		t.Fatalf("AppendExtent: %v", err)
	}

	refs, err := store.ResolveExtents(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 || refs[0].ExtentID != first.ID || refs[1].ExtentID != second.ID {
		t.Fatalf("resolved refs = %+v, want [%d %d]", refs, first.ID, second.ID)
	}

	// Both extents' metadata rows survived the COW page migration.
	if _, err := store.GetExtentMeta(ctx, first.ID); err != nil {
		t.Fatalf("GetExtentMeta(first): %v", err)
	}
	m2, err := store.GetExtentMeta(ctx, second.ID)
	if err != nil {
		t.Fatalf("GetExtentMeta(second): %v", err)
	}
	if m2.LogicalLen != 8192 || m2.PGID != 2 {
		t.Fatalf("second extent meta = %+v", m2)
	}

	in, err := NewInodeStoreV2(store).Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if in.Layout != LayoutExtentPages {
		t.Fatalf("layout = %d, want ExtentPages", in.Layout)
	}
}

func TestUpdateInode_RefusesV2Layout(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()
	id := InodeID(1003)

	if _, err := NewInodeStoreV2(store).CreateEmpty(id, FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}

	// A LayoutEmpty row is not yet owned by the V2 model: a V1 UpdateInode
	// still succeeds (there are no V2 layout fields to clobber).
	if err := store.UpdateInode(ctx, &InodeMeta{ID: id, Size: 1234}); err != nil {
		t.Fatalf("V1 update on LayoutEmpty row should succeed: %v", err)
	}

	// After SetInlineExtent the row carries a V2 layout; V1 overwrite is refused.
	ext := &ExtentMetaV2{ID: ExtentIDV2(0x30000003001), LogicalLen: 4096, PGID: 1}
	if err := store.SetInlineExtent(ctx, id, ext, 4096); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}
	err := store.UpdateInode(ctx, &InodeMeta{ID: id, Size: 9999})
	if !errors.Is(err, ErrInodeModelMismatch) {
		t.Fatalf("UpdateInode on inline row err = %v, want ErrInodeModelMismatch", err)
	}

	// PromoteToPages: pages layout is protected the same way.
	if err := store.PromoteToPages(ctx, id); err != nil {
		t.Fatalf("PromoteToPages: %v", err)
	}
	err = store.UpdateInode(ctx, &InodeMeta{ID: id, Size: 9999})
	if !errors.Is(err, ErrInodeModelMismatch) {
		t.Fatalf("UpdateInode on pages row err = %v, want ErrInodeModelMismatch", err)
	}

	// The V2 layout fields survived both rejected updates.
	in, err := NewInodeStoreV2(store).Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if in.Layout != LayoutExtentPages {
		t.Fatalf("layout = %d, want ExtentPages after rejected V1 updates", in.Layout)
	}
}

func TestExtentInodeService_Errors(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()

	if _, err := store.GetExtentMeta(ctx, ExtentIDV2(9999)); !errors.Is(err, ErrExtentNotFound) {
		t.Fatalf("GetExtentMeta missing = %v, want ErrExtentNotFound", err)
	}
	if err := store.SetInlineExtent(ctx, 1, nil, 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("SetInlineExtent nil extent = %v, want ErrInvalidArgument", err)
	}
	if _, err := store.AppendExtent(ctx, 1, nil, 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("AppendExtent nil extent = %v, want ErrInvalidArgument", err)
	}
	if _, err := store.ResolveExtents(ctx, InodeID(777)); !errors.Is(err, ErrInodeNotFound) {
		t.Fatalf("ResolveExtents missing inode = %v, want ErrInodeNotFound", err)
	}
}

// TestExtentInodeService_ShardedGetExtentMeta covers the --shards N mode: the
// /extent-meta/{id} row is written as a side effect of SetInlineExtent, which
// routes by the *inode*, so it lands in the inode's shard. ShardedStore's
// GetExtentMeta has only the extent ID, so it must scan shards rather than
// route by an extent key — this test forces the cross-shard case where the
// naive extent-key route would miss the row.
func TestExtentInodeService_ShardedGetExtentMeta(t *testing.T) {
	ctx := context.Background()
	ring := NewHashRing(1)
	ring.AddShard(ShardInfo{ID: 1})
	ring.AddShard(ShardInfo{ID: 2})
	sharded := NewShardedStore(ring)
	sharded.AddShard(1, newV2TestPebbleStore(t))
	sharded.AddShard(2, newV2TestPebbleStore(t))

	id := InodeID(7)
	// Pick an extent id whose naive extent-key route differs from the inode's
	// shard, so the inode-shard write and an extent-key single-shard read
	// would land on different shards (the scan must bridge the gap).
	extID := ExtentIDV2(0)
	for n := uint64(1); n < 10000 && extID == 0; n++ {
		if ring.Route(shardKeyForInode(id)) != ring.Route(extentMetaKey(ExtentIDV2(n))) {
			extID = ExtentIDV2(n)
		}
	}
	if extID == 0 {
		t.Fatal("could not find an extent id routing to a different shard than inode 7")
	}

	// Seed the empty inode on its routed shard, then promote through the
	// ShardedStore serving surface (which routes by inode).
	inodeShard, err := sharded.routeToShard(shardKeyForInode(id))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewInodeStoreV2(inodeShard).CreateEmpty(id, FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}
	ext := &ExtentMetaV2{ID: extID, Generation: 1, LogicalLen: 4096, PGID: 9, StorageClass: StorageClassHotReplica}
	if err := sharded.SetInlineExtent(ctx, id, ext, 4096); err != nil {
		t.Fatalf("ShardedStore.SetInlineExtent: %v", err)
	}

	refs, err := sharded.ResolveExtents(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].ExtentID != extID {
		t.Fatalf("sharded resolve = %+v, want [%d]", refs, extID)
	}

	m, err := sharded.GetExtentMeta(ctx, extID)
	if err != nil {
		t.Fatalf("ShardedStore.GetExtentMeta: %v", err)
	}
	if m.PGID != 9 || m.LogicalLen != 4096 || m.StorageClass != StorageClassHotReplica {
		t.Fatalf("sharded extent meta = %+v", m)
	}
}
