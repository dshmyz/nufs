package metadata

import (
	"context"
	"errors"
	"testing"
)

// Tests for the PebbleStore serving surface of the V2.1 extent-layout inode
// model (ExtentInodeService in inode_v2_serving.go, roadmap stage 1 §1.3a):
// inline round-trip, promote+append pages round-trip, the V1 UpdateInode
// collision guard, the error sentinels, and the read dual-model resolver
// ResolveFileChunks (§1.3b).

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

// ========== Read dual-model resolver (§1.3b) ==========

// v2ResolveTestPolicy is a single-replica policy for in-memory allocation.
var v2ResolveTestPolicy = PlacementPolicy{
	ID:                "resolve-test",
	ReplicationFactor: 1,
	TopologySpread:    SpreadNode,
}

// newResolveTestStore creates an in-memory PebbleStore with one healthy
// node, ready for AllocateChunk.
func newResolveTestStore(t *testing.T) *PebbleStore {
	t.Helper()
	store := newV2TestPebbleStore(t)
	if err := store.RegisterNode(context.Background(), &NodeInfo{ID: 1, Addr: "127.0.0.1:9001", CapacityGB: 100}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	return store
}

// newResolveTestInode creates an empty regular inode on the store.
func newResolveTestInode(t *testing.T, store *PebbleStore, id InodeID) {
	t.Helper()
	if _, err := NewInodeStoreV2(store).CreateEmpty(id, FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatalf("CreateEmpty: %v", err)
	}
}

func TestResolveFileChunks_V1Passthrough(t *testing.T) {
	store := newResolveTestStore(t)
	ctx := context.Background()
	id := InodeID(2001)
	newResolveTestInode(t, store, id)

	chunk, err := store.AllocateChunk(ctx, id, 0, v2ResolveTestPolicy)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	inode, err := store.GetInode(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(inode.ChunkMap) == 0 {
		t.Fatal("fixture: expected a V1 ChunkMap after AllocateChunk")
	}

	// A V1 inode with chunks is returned verbatim — the extent surface is
	// never probed.
	got, err := ResolveFileChunks(ctx, store, inode)
	if err != nil {
		t.Fatalf("ResolveFileChunks: %v", err)
	}
	if len(got) != 1 || got[0].ID != chunk.ID {
		t.Fatalf("resolved chunks = %+v, want [id=%d]", got, chunk.ID)
	}
}

func TestResolveFileChunks_V2Inline(t *testing.T) {
	store := newResolveTestStore(t)
	ctx := context.Background()
	id := InodeID(2002)
	newResolveTestInode(t, store, id)

	// AllocateChunk writes a real chunk row; SetInlineExtent then promotes
	// the inode to V2 inline layout (drops the V1 ChunkMap). The extent ID
	// mirrors the chunk ID, honoring the extent==chunk-ID invariant.
	chunk, err := store.AllocateChunk(ctx, id, 0, v2ResolveTestPolicy)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	ext := &ExtentMetaV2{ID: ExtentIDV2(chunk.ID), Generation: 1, LogicalLen: 4096, PGID: 2}
	if err := store.SetInlineExtent(ctx, id, ext, 4096); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}

	inode, err := store.GetInode(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(inode.ChunkMap) != 0 {
		t.Fatalf("fixture: expected ChunkMap to be dropped, got %d refs", len(inode.ChunkMap))
	}

	got, err := ResolveFileChunks(ctx, store, inode)
	if err != nil {
		t.Fatalf("ResolveFileChunks: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("resolved chunks = %+v, want 1", got)
	}
	if got[0].ID != chunk.ID || got[0].Offset != 0 || got[0].Length != 4096 {
		t.Fatalf("resolved inline ref = %+v, want id=%d offset=0 length=4096", got[0], chunk.ID)
	}
}

func TestResolveFileChunks_V2Pages(t *testing.T) {
	store := newResolveTestStore(t)
	ctx := context.Background()
	id := InodeID(2003)
	newResolveTestInode(t, store, id)

	chunk1, err := store.AllocateChunk(ctx, id, 0, v2ResolveTestPolicy)
	if err != nil {
		t.Fatalf("AllocateChunk(1): %v", err)
	}
	first := &ExtentMetaV2{ID: ExtentIDV2(chunk1.ID), Generation: 1, LogicalLen: 4096, PGID: 1}
	if err := store.SetInlineExtent(ctx, id, first, 4096); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}
	if err := store.PromoteToPages(ctx, id); err != nil {
		t.Fatalf("PromoteToPages: %v", err)
	}
	// The second extent is synthetic: ResolveFileChunks only reads the extent
	// surface (page refs + /extent-meta), never the chunk row, so no second
	// AllocateChunk is needed (and one would clobber the pages layout, since
	// AllocateChunk is a V1-model write).
	second := &ExtentMetaV2{ID: ExtentIDV2(0x8000000000000002), Generation: 1, LogicalLen: 8192, PGID: 2}
	if _, err := store.AppendExtent(ctx, id, second, MaxChunkSize); err != nil {
		t.Fatalf("AppendExtent: %v", err)
	}

	inode, err := store.GetInode(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ResolveFileChunks(ctx, store, inode)
	if err != nil {
		t.Fatalf("ResolveFileChunks: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("resolved chunks = %+v, want 2", got)
	}
	if got[0].ID != chunk1.ID || got[0].Offset != 0 || got[0].Length != 4096 {
		t.Fatalf("first page ref = %+v", got[0])
	}
	if got[1].ID != ChunkID(second.ID) || got[1].Offset != MaxChunkSize || got[1].Length != 8192 {
		t.Fatalf("second page ref = %+v", got[1])
	}
}

func TestResolveFileChunks_V1EmptyAndNilSurface(t *testing.T) {
	store := newResolveTestStore(t)
	ctx := context.Background()

	// Empty inode: no chunks, and the extent probe correctly finds no V2
	// layout either. (Allocation and the V1 check below use a separate
	// inode: AllocateChunk does not invalidate the inode cache, so a cached
	// pre-allocation GetInode would shadow the new ChunkMap.)
	newResolveTestInode(t, store, InodeID(2004))
	inode, err := store.GetInode(ctx, InodeID(2004))
	if err != nil {
		t.Fatal(err)
	}
	got, err := ResolveFileChunks(ctx, store, inode)
	if err != nil {
		t.Fatalf("ResolveFileChunks: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty file resolved = %+v, want none", got)
	}
	if got, err := ResolveFileChunks(ctx, nil, inode); err != nil || len(got) != 0 {
		t.Fatalf("nil surface empty file = %+v, %v, want none/nil", got, err)
	}

	// A nil surface on a V1 file with chunks must fall back to the V1
	// verdict (passthrough), not panic and not lose the chunks.
	newResolveTestInode(t, store, InodeID(2005))
	chunk, err := store.AllocateChunk(ctx, InodeID(2005), 0, v2ResolveTestPolicy)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	v1inode, err := store.GetInode(ctx, InodeID(2005))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ResolveFileChunks(ctx, nil, v1inode); err != nil || len(got) != 1 || got[0].ID != chunk.ID {
		t.Fatalf("nil surface V1 file = %+v, %v, want [%d]/nil", got, err, chunk.ID)
	}
}

// TestResolveFileChunks_ErrorPropagation uses the internal InodeStoreV2
// SetInlineExtent (which, unlike the serving surface, does NOT persist the
// /extent-meta row) to fabricate a V2 inline inode whose extent has no
// metadata row. Resolving must surface ErrExtentNotFound, proving the probe
// fails loudly rather than silently reading the file as empty.
func TestResolveFileChunks_ErrorPropagation(t *testing.T) {
	store := newResolveTestStore(t)
	ctx := context.Background()
	id := InodeID(2005)
	newResolveTestInode(t, store, id)

	chunk, err := store.AllocateChunk(ctx, id, 0, v2ResolveTestPolicy)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	ext := &ExtentMetaV2{ID: ExtentIDV2(chunk.ID), Generation: 1, LogicalLen: 4096, PGID: 2}
	if err := NewInodeStoreV2(store).SetInlineExtent(id, ext, 4096); err != nil {
		t.Fatalf("internal SetInlineExtent: %v", err)
	}

	inode, err := store.GetInode(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveFileChunks(ctx, store, inode); !errors.Is(err, ErrExtentNotFound) {
		t.Fatalf("ResolveFileChunks missing extent meta = %v, want ErrExtentNotFound", err)
	}
}
