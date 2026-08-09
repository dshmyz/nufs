package maintenance

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
)

// fillPayload returns n bytes of incompressible pseudo-random data so a
// small segment actually fills and rolls (segment roll triggers on stored
// length, so compressible data never seals).
func fillPayload(seed uint64, n int) []byte {
	state := seed | 1
	b := make([]byte, n)
	for i := range b {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		b[i] = byte(state)
	}
	return b
}

// TestCompressionWorker_ReclaimsDeadSegment runs the real compaction
// worker against a real segment.Store: it writes extents whose old
// generations are later superseded so the early sealed segments hold only
// dead bytes, then verifies the worker compacts a source away (the file
// is removed) while every live generation still reads back byte-exact.
func TestCompressionWorker_ReclaimsDeadSegment(t *testing.T) {
	dir := t.TempDir()
	s, err := segment.New(segment.Config{Dir: dir, UseMemIndex: false, SegmentSize: 256 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	extentIDs := []storage.ExtentID{1, 2, 3, 4, 5}
	payload := fillPayload(7, 64*1024)

	// Write many generations (each a distinct gen so every write appends a
	// new record). The newest generation per extent is live; older
	// generations scattered across earlier sealed segments are dead.
	for i := 0; i < 20; i++ {
		for _, e := range extentIDs {
			if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: e, Generation: storage.Generation(1000 + i), Data: payload}); err != nil {
				t.Fatal(err)
			}
		}
	}

	sealedBefore, err := s.SealedSegments()
	if err != nil {
		t.Fatal(err)
	}
	if len(sealedBefore) == 0 {
		t.Fatalf("expected sealed segments before compaction, got none")
	}

	w := NewCompressionWorker([]storage.Store{s})
	w.RunOnce()

	// At least one sealed source must have been reclaimed (removed) so the
	// physical footprint shrinks.
	sealedAfter, err := s.SealedSegments()
	if err != nil {
		t.Fatal(err)
	}
	if len(sealedAfter) >= len(sealedBefore) {
		t.Fatalf("compaction reclaimed nothing: sealed before=%d after=%d", len(sealedBefore), len(sealedAfter))
	}

	// The authoritative (newest) generation must still read back byte-exact.
	for _, e := range extentIDs {
		got, err := s.Read(ctx, &storage.ReadRequest{ExtentID: e, Generation: 1019})
		if err != nil {
			t.Fatalf("read live gen 1019 extent %d: %v", e, err)
		}
		if !bytes.Equal(got.Data, payload) {
			t.Fatalf("extent %d payload mismatch after compaction", e)
		}
	}
}

// TestCompressionWorker_RemovesSourceFile confirms the reclaimed source
// .seg file is actually deleted from the active directory.
func TestCompressionWorker_RemovesSourceFile(t *testing.T) {
	dir := t.TempDir()
	s, err := segment.New(segment.Config{Dir: dir, UseMemIndex: false, SegmentSize: 256 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	payload := fillPayload(11, 64*1024)
	for i := 0; i < 20; i++ {
		for e := storage.ExtentID(1); e <= 5; e++ {
			if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: e, Generation: storage.Generation(1000 + i), Data: payload}); err != nil {
				t.Fatal(err)
			}
		}
	}

	sealedBefore, err := s.SealedSegments()
	if err != nil {
		t.Fatal(err)
	}
	pathsBefore := map[string]bool{}
	for _, seg := range sealedBefore {
		pathsBefore[seg.Path] = true
		if _, err := os.Stat(seg.Path); err != nil {
			t.Fatalf("sealed source path missing: %v", err)
		}
	}
	if len(pathsBefore) == 0 {
		t.Fatal("no sealed segments")
	}

	w := NewCompressionWorker([]storage.Store{s})
	w.RunOnce()

	gone := 0
	for path := range pathsBefore {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			gone++
		}
	}
	if gone == 0 {
		t.Fatalf("compaction did not remove any source .seg file")
	}
}
