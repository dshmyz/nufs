package metadata

import (
	"testing"
)

// newV2TestPebbleStore creates an in-memory Pebble store for V2 metadata
// tests.
func newV2TestPebbleStore(t *testing.T) *PebbleStore {
	t.Helper()
	store, err := NewPebbleStore(PebbleStoreConfig{
		Dir:            t.TempDir(),
		UseInMemory:    true,
		UseBucketStats: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestInodeV2_InlineExtent(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ins := NewInodeStoreV2(store)

	in, err := ins.CreateEmpty(100, FileRegular, 1, 0, 0, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if in.Layout != LayoutEmpty {
		t.Fatalf("new inode layout = %d, want Empty", in.Layout)
	}

	ext := &ExtentMetaV2{ID: 0x10000000001, Generation: 1, LogicalLen: 4096, PGID: 1}
	if err := ins.SetInlineExtent(100, ext, 4096); err != nil {
		t.Fatal(err)
	}
	got, err := ins.Get(100)
	if err != nil {
		t.Fatal(err)
	}
	if got.Layout != LayoutInlineExtent || got.InlineExtent == nil {
		t.Fatalf("inline extent not set: %+v", got)
	}

	refs, err := ins.ResolveExtents(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].ExtentID != 0x10000000001 {
		t.Fatalf("resolve inline: %+v", refs)
	}
}

func TestInodeV2_ExtentPages(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ins := NewInodeStoreV2(store)

	if _, err := ins.CreateEmpty(200, FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}
	// Append many extents to cross the page boundary.
	for i := 0; i < MaxExtentsPerPage+5; i++ {
		ref := ExtentRef{ExtentID: ExtentIDV2(0x20000000000 + uint64(i)), LogicalOffset: int64(i * (16 << 20))}
		if _, err := ins.AppendExtent(200, ref, 16<<20); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	got, err := ins.Get(200)
	if err != nil {
		t.Fatal(err)
	}
	if got.Layout != LayoutExtentPages {
		t.Fatalf("layout = %d, want ExtentPages", got.Layout)
	}
	if got.ExtentPageCount != 2 {
		t.Fatalf("page count = %d, want 2", got.ExtentPageCount)
	}
	refs, err := ins.ResolveExtents(200)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != MaxExtentsPerPage+5 {
		t.Fatalf("resolved %d refs, want %d", len(refs), MaxExtentsPerPage+5)
	}
	// Verify ordering and content.
	for i, r := range refs {
		if r.ExtentID != ExtentIDV2(0x20000000000+uint64(i)) {
			t.Fatalf("ref %d id = %x, want %x", i, r.ExtentID, uint64(0x20000000000+i))
		}
	}
}

func TestExtentPage_COW_OldRootPreserved(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ps := NewExtentPageStore(store)

	// Write a page under root 1, then update it under root 2.
	base := &ExtentPage{InodeID: 300, PageNo: 0, Extents: []ExtentRef{{ExtentID: 1}}}
	if err := ps.writePage(base, 1); err != nil {
		t.Fatal(err)
	}
	updated, err := ps.UpdatePage(300, 1, 2, 0, func(p *ExtentPage) error {
		p.Extents = append(p.Extents, ExtentRef{ExtentID: 2})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(updated.Extents) != 2 {
		t.Fatalf("updated page has %d extents, want 2", len(updated.Extents))
	}
	// Old root still has the original 1 extent (COW preserved it).
	old, err := ps.GetPage(300, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if old == nil || len(old.Extents) != 1 {
		t.Fatalf("old root page corrupted: %+v", old)
	}
}
