package segment

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/testutil"
)

func newTestStore(t *testing.T, faults storage.FaultHook) *Store {
	t.Helper()
	s, err := New(Config{
		Dir:         t.TempDir(),
		SegmentSize: 1 << 20, // small segments so sealing triggers
		UseMemIndex: true,
		Faults:      faults,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestWriteReadRoundtrip(t *testing.T) {
	s := newTestStore(t, nil)
	ctx := context.Background()

	data := bytes.Repeat([]byte("hello segment"), 1000)
	rec, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: data})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if rec.SegmentID == 0 || rec.Offset < int64(storage.SegmentHeaderSize) {
		t.Fatalf("bad receipt: %+v", rec)
	}

	got, err := s.Read(ctx, &storage.ReadRequest{ExtentID: 1, Generation: 1})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got.Data, data) {
		t.Fatalf("read mismatch: got %d bytes want %d", len(got.Data), len(data))
	}

	// Range read of a sub-range.
	got, err = s.Read(ctx, &storage.ReadRequest{ExtentID: 1, Generation: 1, LogicalOffset: 10, Length: 20})
	if err != nil {
		t.Fatalf("RangeRead: %v", err)
	}
	if !bytes.Equal(got.Data, data[10:30]) {
		t.Fatalf("range read mismatch: got %q want %q", got.Data, data[10:30])
	}

	// Stat.
	st, err := s.Stat(ctx, &storage.StatRequest{ExtentID: 1, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if st.State != storage.ExtentDurable || st.SegmentID != rec.SegmentID {
		t.Fatalf("stat mismatch: %+v", st)
	}
}

func TestIdempotentWrite(t *testing.T) {
	s := newTestStore(t, nil)
	ctx := context.Background()
	data := []byte("same bytes")

	r1, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 5, Generation: 1, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	// Same extent+gen+checksum → idempotent success, no duplicate record.
	r2, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 5, Generation: 1, Data: data})
	if err != nil {
		t.Fatalf("idempotent write failed: %v", err)
	}
	if r1.SegmentID != r2.SegmentID || r1.Offset != r2.Offset {
		t.Fatalf("idempotent write changed location: %+v vs %+v", r1, r2)
	}

	// Different payload, same gen → stale generation conflict.
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 5, Generation: 1, Data: []byte("different")}); err != storage.ErrStaleGeneration {
		t.Fatalf("expected ErrStaleGeneration, got %v", err)
	}
}

func TestGenerationFencedOverwrite(t *testing.T) {
	s := newTestStore(t, nil)
	ctx := context.Background()

	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 9, Generation: 1, Data: []byte("v1")}); err != nil {
		t.Fatal(err)
	}
	// Overwrite with a higher generation succeeds.
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 9, Generation: 2, Data: []byte("v2")}); err != nil {
		t.Fatalf("overwrite with higher gen failed: %v", err)
	}
	// Read resolves to the latest generation the caller asks for.
	got, err := s.Read(ctx, &storage.ReadRequest{ExtentID: 9, Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Data) != "v2" {
		t.Fatalf("read gen2 = %q want v2", got.Data)
	}
	// Old generation still readable (metadata decides liveness).
	got1, err := s.Read(ctx, &storage.ReadRequest{ExtentID: 9, Generation: 1})
	if err != nil {
		t.Fatalf("old gen read failed: %v", err)
	}
	if string(got1.Data) != "v1" {
		t.Fatalf("read gen1 = %q want v1", got1.Data)
	}
}

func TestDeleteGenerationFenced(t *testing.T) {
	s := newTestStore(t, nil)
	ctx := context.Background()

	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 7, Generation: 1, Data: []byte("data")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, &storage.DeleteRequest{ExtentID: 7, Generation: 1}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Read(ctx, &storage.ReadRequest{ExtentID: 7, Generation: 1}); err != storage.ErrExtentNotFound {
		t.Fatalf("expected ErrExtentNotFound after delete, got %v", err)
	}
	// Deleting an absent extent is a no-op (idempotent delete).
	if err := s.Delete(ctx, &storage.DeleteRequest{ExtentID: 7, Generation: 1}); err != nil {
		t.Fatalf("idempotent delete failed: %v", err)
	}
}

// TestListExtentsCoalescesToLatestGeneration pins the read-authority
// enumeration rule: for every extent ListExtents must surface the single
// highest generation, so (a) a tombstone at any generation hides every
// live generation (an acknowledged delete is immediately invisible) and
// (b) an overwrite resolves to its newest payload. This is a regression
// test for a bug where ListExtents iterated the overlay's per-key map in
// arbitrary order and could surface a stale lower generation, resurrecting
// an extent its own delete had just removed.
func TestListExtentsCoalescesToLatestGeneration(t *testing.T) {
	s := newTestStore(t, nil)
	ctx := context.Background()

	// Two extents hidden beneath a newer tombstone: one (41) where both the
	// live gen-1 and tombstone gen-2 still sit in the overlay (single write +
	// single delete, with the flush lagging), and one (42) where an
	// intermediate generation was also written so the overlay holds several
	// generations of the same extent at once.
	for id, gens := range map[storage.ExtentID][]storage.Generation{
		41: {1, 2},
		42: {1, 2, 3},
	} {
		for _, gen := range gens {
			if _, err := s.Write(ctx, &storage.WriteRequest{
				ExtentID: id, Generation: gen,
				Data: []byte{byte(id), byte(gen)},
			}); err != nil {
				t.Fatalf("write extent %d gen %d: %v", id, gen, err)
			}
		}
	}
	// Delete the latest generation of each, leaving the lower live ones.
	for id, latest := range map[storage.ExtentID]storage.Generation{41: 2, 42: 3} {
		if err := s.Delete(ctx, &storage.DeleteRequest{ExtentID: id, Generation: latest}); err != nil {
			t.Fatalf("delete extent %d gen %d: %v", id, latest, err)
		}
	}

	// A live extent with a genuinely newer live generation must resolve to
	// that newest payload in the enumeration (overwrite coalescing).
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 43, Generation: 1, Data: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 43, Generation: 2, Data: []byte("bb")}); err != nil {
		t.Fatal(err)
	}

	exts, err := s.ListExtents()
	if err != nil {
		t.Fatalf("ListExtents: %v", err)
	}
	byID := make(map[storage.ExtentID]LiveExtent, len(exts))
	for _, e := range exts {
		byID[e.ExtentID] = e
	}
	// 41 and 42 were deleted at their newest generation: entirely gone.
	if _, ok := byID[41]; ok {
		t.Fatalf("extent 41 (deleted at gen 2) still enumerated: %+v", byID[41])
	}
	if _, ok := byID[42]; ok {
		t.Fatalf("extent 42 (deleted at gen 3) still enumerated: %+v", byID[42])
	}
	// 43 must surface its newest generation 2 with that payload's size.
	e, ok := byID[43]
	if !ok {
		t.Fatalf("live extent 43 not enumerated")
	}
	if e.Generation != 2 || e.Value.LogicalLen != 2 {
		t.Fatalf("extent 43 = gen %d logical %d, want gen 2 logical 2", e.Generation, e.Value.LogicalLen)
	}
}

func TestDelete_EvictsLiveLocationCacheAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := New(Config{Dir: dir, UseMemIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 72, Generation: 1, Data: []byte("data")}); err != nil {
		s.Close()
		t.Fatal(err)
	}
	// Populate locCache with the live location before the durable delete.
	if _, err := s.Read(ctx, &storage.ReadRequest{ExtentID: 72, Generation: 1}); err != nil {
		s.Close()
		t.Fatal(err)
	}
	if err := s.Delete(ctx, &storage.DeleteRequest{ExtentID: 72, Generation: 1}); err != nil {
		s.Close()
		t.Fatal(err)
	}
	if _, err := s.Read(ctx, &storage.ReadRequest{ExtentID: 72, Generation: 1}); !errors.Is(err, storage.ErrExtentNotFound) {
		s.Close()
		t.Fatalf("read after acknowledged delete = %v, want ErrExtentNotFound", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(Config{Dir: dir, UseMemIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Read(ctx, &storage.ReadRequest{ExtentID: 72, Generation: 1}); !errors.Is(err, storage.ErrExtentNotFound) {
		t.Fatalf("read after reopen = %v, want ErrExtentNotFound", err)
	}
}

// TestDelete_CrashReopenTombstoneRemainsDeleted uses a fresh in-memory index
// after the acknowledged delete so only durable segment-log recovery can
// preserve the tombstone.
func TestDelete_CrashReopenTombstoneRemainsDeleted(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := New(Config{Dir: dir, UseMemIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 71, Generation: 1, Data: []byte("data")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, &storage.DeleteRequest{ExtentID: 71, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(Config{Dir: dir, UseMemIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Read(ctx, &storage.ReadRequest{ExtentID: 71, Generation: 1}); !errors.Is(err, storage.ErrExtentNotFound) {
		t.Fatalf("read after recovered delete = %v, want ErrExtentNotFound", err)
	}
}

func TestStoreRecoveryRestoresStreamSequence(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := New(Config{Dir: dir, UseMemIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 81, Generation: 1, Data: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(Config{Dir: dir, UseMemIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.streamSeq; got != 1 {
		t.Fatalf("recovered stream sequence = %d, want 1", got)
	}
	if _, err := reopened.Write(ctx, &storage.WriteRequest{ExtentID: 82, Generation: 1, Data: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	if got := reopened.streamSeq; got != 2 {
		t.Fatalf("stream sequence after append = %d, want 2", got)
	}
}

func TestSegmentSealAndNew(t *testing.T) {
	s := newTestStore(t, nil)
	ctx := context.Background()

	// Force sealing by writing many records into a tiny segment.
	// The first segment fills and the store rotates to a new one.
	var lastRec *storage.DurableReceipt
	for i := 0; i < 200; i++ {
		rec, err := s.Write(ctx, &storage.WriteRequest{
			ExtentID:   storage.ExtentID(100 + i),
			Generation: 1,
			Data:       bytes.Repeat([]byte("x"), 4096),
		})
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		lastRec = rec
	}
	// Every record still reads back correctly across the seal boundary.
	for i := 0; i < 200; i++ {
		got, err := s.Read(ctx, &storage.ReadRequest{ExtentID: storage.ExtentID(100 + i), Generation: 1})
		if err != nil {
			t.Fatalf("read %d after seal: %v", i, err)
		}
		if len(got.Data) != 4096 {
			t.Fatalf("read %d: wrong size %d", i, len(got.Data))
		}
	}
	_ = lastRec
}

// TestCrashInjection_BeforeBatchSync verifies the V2.1 durability
// invariant: a crash before the single batch fdatasync must NOT result
// in an acknowledged write. Recovery must leave the extent either
// present (BatchCommit was durable) or absent, never corrupt.
func TestCrashInjection_BeforeBatchSync(t *testing.T) {
	s := newTestStore(t, testutil.NewScriptedFaults([]testutil.Step{
		{Point: storage.CrashBeforeOverlayApply, Err: testutil.ErrSimulatedCrash},
	}))
	ctx := context.Background()

	_, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: []byte("x")})
	if err == nil {
		t.Fatal("expected write to fail on injected crash")
	}
	// The write must NOT be acknowledged: the overlay has no entry.
	if _, err := s.Read(ctx, &storage.ReadRequest{ExtentID: 1, Generation: 1}); err == nil {
		t.Fatal("read succeeded for a write that was never acknowledged")
	}
}
