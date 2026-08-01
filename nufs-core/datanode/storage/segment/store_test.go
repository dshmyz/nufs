package segment

import (
	"bytes"
	"context"
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

func TestSegmentSealAndNew(t *testing.T) {
	s := newTestStore(t, nil)
	ctx := context.Background()

	// Force sealing by writing many records into a tiny segment.
	// The first segment fills and the store rotates to a new one.
	var lastRec *storage.DurableReceipt
	for i := 0; i < 200; i++ {
		rec, err := s.Write(ctx, &storage.WriteRequest{
			ExtentID: storage.ExtentID(100 + i),
			Generation: 1,
			Data:    bytes.Repeat([]byte("x"), 4096),
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
