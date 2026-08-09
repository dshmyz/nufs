package maintenance

import (
	"context"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
)

// TestCompactor_WithRealStore verifies the compactor works through the
// StoreSink interface against the real segment.Store (§10.3). It writes
// extents, simulates a dead segment by making some extents dead (their
// index points elsewhere), and compacts live ones into a fresh segment.
func TestCompactor_WithRealStore(t *testing.T) {
	dir := t.TempDir()
	s, err := segment.New(segment.Config{Dir: dir, UseMemIndex: false, SegmentSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	// Write 5 extents.
	for i := 0; i < 5; i++ {
		if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: storage.ExtentID(i + 1), Generation: 1, Data: []byte("live-data")}); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate: extents 1, 3 are dead (their index no longer points at
	// the source segment). The compactor only copies live ones.
	live := map[storage.ExtentID]struct{}{2: {}, 4: {}, 5: {}}
	isLive := func(id storage.ExtentID, gen storage.Generation) bool {
		_, ok := live[id]
		return ok
	}
	// Build scanned records (simulating a sealed segment scan).
	records := []ScannedRecord{
		{ExtentID: 1, Generation: 1, LogicalLen: 9, ReadPayload: func() ([]byte, error) { return []byte("live-data"), nil }},
		{ExtentID: 2, Generation: 1, LogicalLen: 9, ReadPayload: func() ([]byte, error) { return []byte("live-data"), nil }},
		{ExtentID: 3, Generation: 1, LogicalLen: 9, ReadPayload: func() ([]byte, error) { return []byte("live-data"), nil }},
		{ExtentID: 4, Generation: 1, LogicalLen: 9, ReadPayload: func() ([]byte, error) { return []byte("live-data"), nil }},
		{ExtentID: 5, Generation: 1, LogicalLen: 9, ReadPayload: func() ([]byte, error) { return []byte("live-data"), nil }},
	}

	c := NewCompactor(s, nil)
	copied, err := c.Compact(1, records, isLive)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if copied != 3 {
		t.Fatalf("expected 3 live records copied, got %d", copied)
	}

	// The P0 relocation-checksum regression: after compaction moves a live
	// extent, reading it back through the real store must yield the
	// byte-exact payload AND its real logical checksum (not the 0 that the
	// old Relocate shadowed in). This is the property read/repair/verify
	// rely on for integrity validation.
	relocated, err := s.Read(ctx, &storage.ReadRequest{ExtentID: 2, Generation: 1})
	if err != nil {
		t.Fatalf("read relocated live extent: %v", err)
	}
	if string(relocated.Data) != "live-data" {
		t.Fatalf("relocated data = %q, want %q", relocated.Data, "live-data")
	}
	if want := storage.CRC32C([]byte("live-data")); relocated.Checksum != want {
		t.Fatalf("relocated checksum = %d, want %d (was shadowed to 0 before the fix)", relocated.Checksum, want)
	}
}