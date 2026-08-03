package index

import (
	"testing"

	"github.com/example/dfs/datanode/storage"
)

func openMemIndex(t *testing.T) *Index {
	t.Helper()
	ix, err := Open(Options{Dir: t.TempDir(), UseInMemory: true})
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	t.Cleanup(func() { ix.Close() })
	return ix
}

// TestIterateLatestGeneration verifies Iterate yields exactly one entry
// per extent with the value of its highest generation, ordered correctly
// across generations and extents.
func TestIterateLatestGeneration(t *testing.T) {
	ix := openMemIndex(t)

	b := ix.NewBatch()
	muts := []struct {
		id  storage.ExtentID
		gen storage.Generation
		sz  uint32
	}{
		{1, 1, 100},
		{1, 2, 150}, // newer generation of extent 1
		{1, 3, 200}, // newest generation of extent 1
		{2, 1, 50},
		{3, 1, 25},
		{3, 2, 30},
	}
	for _, m := range muts {
		if err := ix.PutBatch(b, m.id, m.gen, &Value{
			SegmentID:  storage.SegmentID(m.id),
			LogicalLen: m.sz,
			State:      storage.ExtentDurable,
		}); err != nil {
			t.Fatalf("PutBatch: %v", err)
		}
	}
	if err := ix.ApplyBatch(b); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	got := map[storage.ExtentID]struct{ gen storage.Generation; sz uint32 }{}
	if err := ix.Iterate(func(id storage.ExtentID, gen storage.Generation, v Value) error {
		got[id] = struct {
			gen storage.Generation
			sz  uint32
		}{gen, v.LogicalLen}
		return nil
	}); err != nil {
		t.Fatalf("Iterate: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("Iterate produced %d extents, want 3: %+v", len(got), got)
	}
	// Extent 1 must resolve to gen 3 (the latest), not gen 1 or 2.
	if g := got[storage.ExtentID(1)]; g.gen != 3 || g.sz != 200 {
		t.Fatalf("extent 1 = %+v, want gen=3 sz=200", g)
	}
	if g := got[storage.ExtentID(2)]; g.gen != 1 || g.sz != 50 {
		t.Fatalf("extent 2 = %+v, want gen=1 sz=50", g)
	}
	if g := got[storage.ExtentID(3)]; g.gen != 2 || g.sz != 30 {
		t.Fatalf("extent 3 = %+v, want gen=2 sz=30", g)
	}
}

// TestIterateTombstoneStateSurface verifies Iterate exposes a tombstoned
// latest generation (state + generation) so the caller can filter it,
// rather than silently returning the older live generation.
func TestIterateTombstoneStateSurface(t *testing.T) {
	ix := openMemIndex(t)

	b := ix.NewBatch()
	// Extent 9: gen 1 live, gen 2 tombstoned (overwrite-then-delete).
	if err := ix.PutBatch(b, 9, 1, &Value{LogicalLen: 100, State: storage.ExtentDurable}); err != nil {
		t.Fatalf("put gen1: %v", err)
	}
	if err := ix.PutBatch(b, 9, 2, &Value{LogicalLen: 0, State: storage.ExtentTombstoned}); err != nil {
		t.Fatalf("put gen2 tombstone: %v", err)
	}
	if err := ix.ApplyBatch(b); err != nil {
		t.Fatalf("apply: %v", err)
	}

	var found *Value
	if err := ix.Iterate(func(id storage.ExtentID, gen storage.Generation, v Value) error {
		if id == storage.ExtentID(9) {
			c := v
			found = &c
		}
		return nil
	}); err != nil {
		t.Fatalf("Iterate: %v", err)
	}
	if found == nil {
		t.Fatalf("extent 9 not enumerated")
	}
	// The latest generation is the tombstone — caller sees and filters it.
	if found.State != storage.ExtentTombstoned {
		t.Fatalf("extent 9 state=%v, want ExtentTombstoned (latest gen)", found.State)
	}
}
