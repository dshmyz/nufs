package metadata

import (
	"context"
	"errors"
	"testing"
)

// Tests for ReplaceExtents — the whole-set extent-pages writer added in
// roadmap stage 1 §1.3c (write-side dual-model). It rewrites a file's
// entire extent set as COW pages under a fresh root, replacing whatever
// model the row previously had (empty, inline, or earlier pages), and is
// the commit path for gateway overwrites (which have already rewritten the
// old extent's data into a new chunk set).

// extWrite is a shorthand for building ExtentWrite fixtures.
func extWrite(id ExtentIDV2, offset int64) ExtentWrite {
	return ExtentWrite{Extent: &ExtentMetaV2{ID: id, Generation: 1, LogicalLen: 4096, PGID: 1, Lifecycle: LifecycleReady}, Offset: offset}
}

func TestReplaceExtents_MultiExtentRoundTrip(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()
	id := InodeID(3001)
	if _, err := NewInodeStoreV2(store).CreateEmpty(id, FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}

	writes := []ExtentWrite{extWrite(ExtentIDV2(0x900000000000001), 0), extWrite(ExtentIDV2(0x900000000000002), MaxChunkSize)}
	if err := store.ReplaceExtents(ctx, id, writes, MaxChunkSize+4096); err != nil {
		t.Fatalf("ReplaceExtents: %v", err)
	}

	refs, err := store.ResolveExtents(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 || refs[0].ExtentID != writes[0].Extent.ID || refs[1].ExtentID != writes[1].Extent.ID {
		t.Fatalf("resolved refs = %+v, want [%d@0, %d@MaxChunkSize]", refs, writes[0].Extent.ID, writes[1].Extent.ID)
	}
	if refs[0].LogicalOffset != 0 || refs[1].LogicalOffset != MaxChunkSize {
		t.Fatalf("logical offsets = [%d, %d], want [0, %d]", refs[0].LogicalOffset, refs[1].LogicalOffset, MaxChunkSize)
	}

	// Both extents' metadata rows survived.
	for _, w := range writes {
		if _, err := store.GetExtentMeta(ctx, w.Extent.ID); err != nil {
			t.Fatalf("GetExtentMeta(%d): %v", w.Extent.ID, err)
		}
	}

	in, err := NewInodeStoreV2(store).Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if in.Layout != LayoutExtentPages || in.InlineExtent != nil || in.ExtentPageCount != 1 {
		t.Fatalf("inode = %+v, want ExtentPages 1 page", in)
	}
	if in.Size != MaxChunkSize+4096 {
		t.Fatalf("size = %d, want %d", in.Size, MaxChunkSize+4096)
	}
	if in.CTime == 0 || in.MTime == 0 {
		t.Fatalf("CTime/MTime not bumped: %+v", in)
	}
}

func TestReplaceExtents_EmptyRowToPages(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()
	id := InodeID(3002)
	if _, err := NewInodeStoreV2(store).CreateEmpty(id, FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}
	// A LayoutEmpty (V1-fresh) row converts straight to pages under root 1.
	if err := store.ReplaceExtents(ctx, id, []ExtentWrite{extWrite(ExtentIDV2(0x900000000000003), 0)}, 4096); err != nil {
		t.Fatalf("ReplaceExtents on empty row: %v", err)
	}
	in, err := NewInodeStoreV2(store).Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if in.Layout != LayoutExtentPages || in.ExtentRoot != 1 || in.ExtentPageCount != 1 {
		t.Fatalf("empty→pages inode = %+v", in)
	}
}

func TestReplaceExtents_InlineRowDropsOldExtent(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()
	id := InodeID(3003)
	if _, err := NewInodeStoreV2(store).CreateEmpty(id, FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}
	oldExt := &ExtentMetaV2{ID: ExtentIDV2(0x900000000000004), LogicalLen: 4096, PGID: 1}
	if err := store.SetInlineExtent(ctx, id, oldExt, 4096); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}
	// Overwrite: unlike PromoteToPages, the old inline extent must NOT be
	// preserved — its data was rewritten into the new chunk set.
	newWrite := extWrite(ExtentIDV2(0x900000000000005), 0)
	if err := store.ReplaceExtents(ctx, id, []ExtentWrite{newWrite}, 8192); err != nil {
		t.Fatalf("ReplaceExtents on inline row: %v", err)
	}
	refs, err := store.ResolveExtents(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].ExtentID != newWrite.Extent.ID {
		t.Fatalf("resolved = %+v, want only the new extent %d", refs, newWrite.Extent.ID)
	}
	in, err := NewInodeStoreV2(store).Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if in.Layout != LayoutExtentPages || in.InlineExtent != nil || in.Size != 8192 {
		t.Fatalf("inline→pages inode = %+v", in)
	}
}

func TestReplaceExtents_PagesOverwriteGCsOldRoot(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()
	id := InodeID(3004)
	if _, err := NewInodeStoreV2(store).CreateEmpty(id, FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}
	first := extWrite(ExtentIDV2(0x900000000000006), 0)
	if err := store.ReplaceExtents(ctx, id, []ExtentWrite{first}, 4096); err != nil {
		t.Fatalf("first ReplaceExtents: %v", err)
	}
	in1, _ := NewInodeStoreV2(store).Get(id)
	firstRoot := in1.ExtentRoot

	// Second overwrite switches to a fresh root; the old root's page must be
	// gone (GC) and resolve must return only the new set.
	second := extWrite(ExtentIDV2(0x900000000000007), 0)
	if err := store.ReplaceExtents(ctx, id, []ExtentWrite{second}, 8192); err != nil {
		t.Fatalf("second ReplaceExtents: %v", err)
	}
	in2, err := NewInodeStoreV2(store).Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if in2.ExtentRoot != firstRoot+1 || in2.ExtentRootVersion != in1.ExtentRootVersion+1 {
		t.Fatalf("root switch: first root=%d version=%d, second root=%d version=%d", firstRoot, in1.ExtentRootVersion, in2.ExtentRoot, in2.ExtentRootVersion)
	}
	// Old root's page 0 is deleted.
	oldPage, err := NewExtentPageStore(store).GetPage(id, firstRoot, 0)
	if err != nil {
		t.Fatal(err)
	}
	if oldPage != nil {
		t.Fatalf("old root page still present: %+v", oldPage)
	}
	refs, err := store.ResolveExtents(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].ExtentID != second.Extent.ID {
		t.Fatalf("resolved = %+v, want only the second extent", refs)
	}
}

func TestReplaceExtents_EmptyWrites(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()
	id := InodeID(3005)
	if _, err := NewInodeStoreV2(store).CreateEmpty(id, FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}
	if err := store.SetInlineExtent(ctx, id, &ExtentMetaV2{ID: ExtentIDV2(0x900000000000008), LogicalLen: 4096, PGID: 1}, 4096); err != nil {
		t.Fatal(err)
	}
	// Zero-byte overwrite: no extents, but the (previously V2) row must not
	// be left with stale extents.
	if err := store.ReplaceExtents(ctx, id, nil, 0); err != nil {
		t.Fatalf("ReplaceExtents empty: %v", err)
	}
	refs, err := store.ResolveExtents(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Fatalf("empty overwrite resolved = %+v, want none", refs)
	}
	in, err := NewInodeStoreV2(store).Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if in.Layout != LayoutExtentPages || in.Size != 0 || in.ExtentPageCount != 0 {
		t.Fatalf("empty pages inode = %+v", in)
	}
}

func TestReplaceExtents_Errors(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()
	if err := store.ReplaceExtents(ctx, InodeID(9999), []ExtentWrite{extWrite(ExtentIDV2(1), 0)}, 1); !errors.Is(err, ErrInodeNotFound) {
		t.Fatalf("ReplaceExtents missing inode = %v, want ErrInodeNotFound", err)
	}
	// A nil extent must be rejected before any mutation.
	id := InodeID(3006)
	if _, err := NewInodeStoreV2(store).CreateEmpty(id, FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceExtents(ctx, id, []ExtentWrite{{Extent: nil, Offset: 0}}, 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ReplaceExtents nil extent = %v, want ErrInvalidArgument", err)
	}
}

// ========== CommitChunkRefsModelAware (write dual-model decision) ==========

// TestCommitChunkRefsModelAware covers the shared commit decision used by
// both gateways and the recovery worker: inline for a single small chunk,
// pages for anything larger or multi-chunk, V1 fallback without the extent
// surface, and empty-ref commits that either empty an existing V2 file or
// keep a brand-new zero-ref file on the V1 path.
func TestCommitChunkRefsModelAware(t *testing.T) {
	ctx := context.Background()
	var nextID InodeID = 4000

	newStore := func(t *testing.T) (*PebbleStore, InodeID) {
		t.Helper()
		nextID++
		store := newResolveTestStore(t)
		id := nextID
		if _, err := NewInodeStoreV2(store).CreateEmpty(id, FileRegular, 1, 0, 0, 0644); err != nil {
			t.Fatal(err)
		}
		return store, id
	}
	proj := func(t *testing.T, store *PebbleStore, id InodeID) *InodeMeta {
		t.Helper()
		in, err := store.GetInode(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return in
	}
	resolve := func(t *testing.T, store *PebbleStore, id InodeID) []ExtentRef {
		t.Helper()
		refs, err := store.ResolveExtents(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return refs
	}

	t.Run("inline", func(t *testing.T) {
		store, id := newStore(t)
		refs := []ChunkRef{{ID: 5001, Offset: 0, Length: 4096}}
		if err := CommitChunkRefsModelAware(ctx, store, proj(t, store, id), refs, 4096); err != nil {
			t.Fatalf("commit: %v", err)
		}
		in, _ := NewInodeStoreV2(store).Get(id)
		if in.Layout != LayoutInlineExtent || in.InlineExtent == nil || in.InlineExtent.ID != ExtentIDV2(5001) {
			t.Fatalf("inline commit inode = %+v", in)
		}
		if len(resolve(t, store, id)) != 1 {
			t.Fatalf("resolve = %+v, want 1", resolve(t, store, id))
		}
	})

	t.Run("pages single large", func(t *testing.T) {
		store, id := newStore(t)
		refs := []ChunkRef{{ID: 5002, Offset: 0, Length: MaxInlineExtentSize + 1}}
		if err := CommitChunkRefsModelAware(ctx, store, proj(t, store, id), refs, MaxInlineExtentSize+1); err != nil {
			t.Fatalf("commit: %v", err)
		}
		in, _ := NewInodeStoreV2(store).Get(id)
		if in.Layout != LayoutExtentPages {
			t.Fatalf("large single chunk must go pages, got layout %d", in.Layout)
		}
	})

	t.Run("pages multi chunk", func(t *testing.T) {
		store, id := newStore(t)
		refs := []ChunkRef{{ID: 5003, Offset: 0, Length: 4096}, {ID: 5004, Offset: MaxChunkSize, Length: 4096}}
		if err := CommitChunkRefsModelAware(ctx, store, proj(t, store, id), refs, MaxChunkSize+4096); err != nil {
			t.Fatalf("commit: %v", err)
		}
		in, _ := NewInodeStoreV2(store).Get(id)
		if in.Layout != LayoutExtentPages || in.ExtentPageCount != 1 {
			t.Fatalf("multi chunk commit inode = %+v", in)
		}
		got := resolve(t, store, id)
		if len(got) != 2 {
			t.Fatalf("resolve = %+v, want 2", got)
		}
	})

	t.Run("empty refs empties V2 file", func(t *testing.T) {
		store, id := newStore(t)
		refs := []ChunkRef{{ID: 5005, Offset: 0, Length: 4096}}
		if err := CommitChunkRefsModelAware(ctx, store, proj(t, store, id), refs, 4096); err != nil {
			t.Fatal(err)
		}
		// Now a zero-byte overwrite of the V2 file: must empty it.
		if err := CommitChunkRefsModelAware(ctx, store, proj(t, store, id), nil, 0); err != nil {
			t.Fatalf("empty overwrite: %v", err)
		}
		if len(resolve(t, store, id)) != 0 {
			t.Fatalf("resolve after empty overwrite = %+v, want none", resolve(t, store, id))
		}
		in, _ := NewInodeStoreV2(store).Get(id)
		if in.Size != 0 {
			t.Fatalf("size after empty overwrite = %d", in.Size)
		}
	})

	t.Run("empty refs keeps new empty file V1", func(t *testing.T) {
		store, id := newStore(t)
		if err := CommitChunkRefsModelAware(ctx, store, proj(t, store, id), nil, 0); err != nil {
			t.Fatalf("commit empty new file: %v", err)
		}
		in, _ := NewInodeStoreV2(store).Get(id)
		if in.Layout != LayoutEmpty {
			t.Fatalf("new empty file must stay V1, got layout %d", in.Layout)
		}
	})
}
