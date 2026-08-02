package metadata

import (
	"testing"
)

func TestInventoryPartition_Commutative(t *testing.T) {
	// Adding the same set in different orders yields the same summary.
	p1 := &InventoryPartition{}
	p1.Add(1, 1, 100)
	p1.Add(2, 3, 200)
	p1.Add(3, 5, 300)

	p2 := &InventoryPartition{}
	p2.Add(3, 5, 300)
	p2.Add(1, 1, 100)
	p2.Add(2, 3, 200)

	if !p1.Equal(p2) {
		t.Fatalf("commutative summaries differ: %+v vs %+v", p1, p2)
	}
	if p1.Count != 3 || p1.LiveBytes != 600 {
		t.Fatalf("count/bytes: %d/%d", p1.Count, p1.LiveBytes)
	}
}

func TestInventoryPartition_Remove(t *testing.T) {
	p := &InventoryPartition{}
	p.Add(1, 1, 100)
	p.Add(2, 2, 200)
	p.Remove(2, 2, 200)
	if p.Count != 1 || p.LiveBytes != 100 {
		t.Fatalf("after remove: count=%d bytes=%d", p.Count, p.LiveBytes)
	}
}

func TestInventoryStore_RecordAndGlobal(t *testing.T) {
	store := newV2TestPebbleStore(t)
	inv := NewInventoryStore(store)

	if err := inv.RecordAdd(100, 1, 4096); err != nil {
		t.Fatal(err)
	}
	if err := inv.RecordAdd(200, 2, 8192); err != nil {
		t.Fatal(err)
	}
	d, err := inv.Global()
	if err != nil {
		t.Fatal(err)
	}
	if d.Count != 2 || d.LiveBytes != 4096+8192 {
		t.Fatalf("global digest: count=%d bytes=%d", d.Count, d.LiveBytes)
	}
	// Partition routing is deterministic.
	if PartitionFor(100) != PartitionFor(100) {
		t.Fatal("PartitionFor must be deterministic")
	}
}

func TestMerkle_DiffNarrowsToChangedPages(t *testing.T) {
	// Two sorted lists differing only in one extent near the end.
	var a, b []ExtentKey
	for i := uint64(0); i < 10000; i++ {
		a = append(a, ExtentKey{ExtentID: i, Generation: 1})
		b = append(b, ExtentKey{ExtentID: i, Generation: 1})
	}
	// Change one extent's generation in b.
	b[9999].Generation = 2

	ta := BuildMerkle(a)
	tb := BuildMerkle(b)
	diffs := ta.DiffNodes(tb)
	if len(diffs) == 0 {
		t.Fatal("expected at least one differing leaf")
	}
	// The differing leaf must be small (≤ page size) and cover the change.
	total := 0
	for _, d := range diffs {
		if len(d.Leaf) > MerklePageSize {
			t.Fatalf("leaf exceeds page size: %d", len(d.Leaf))
		}
		total += len(d.Leaf)
	}
	if total >= 10000 {
		t.Fatalf("diff should narrow, not return everything: %d", total)
	}
}
