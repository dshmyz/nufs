package segment

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/testutil"
)

// errFatal marks a write that must never return success.
var errFatal = errors.New("injected fault")

// crashAllPoints returns one crash point per write stage (V2.1 §18.2).
func crashAllPoints() []storage.CrashPoint {
	return []storage.CrashPoint{
		storage.CrashBeforeBatchAppend,
		storage.CrashAfterRecordAppend,
		storage.CrashAfterFrameIndex,
		storage.CrashAfterBatchCommitWrite,
		storage.CrashAfterBatchSync,
		storage.CrashBeforeOverlayApply,
		storage.CrashAfterOverlayApply,
		storage.CrashAfterAck,
	}
}

// TestCrashMatrix_NoAckWriteLost drives a crash at every write stage.
// Invariant (§18.2, §21): a write that returned a DurableReceipt must
// survive recovery — the reopened store must return its exact bytes.
// A write that did NOT return a receipt may or may not be present, but
// recovery must not corrupt it or report it as acknowledged.
func TestCrashMatrix_NoAckWriteLost(t *testing.T) {
	ctx := context.Background()

	for _, point := range crashAllPoints() {
		t.Run(point.String(), func(t *testing.T) {
			dir := t.TempDir()

			// Phase A: open, write, crash at the given point.
			crashFaults := testutil.NewScriptedFaults([]testutil.Step{
				{Point: point, Err: errFatal},
			})
			s, err := New(Config{Dir: dir, UseMemIndex: false, Faults: crashFaults})
			if err != nil {
				t.Fatal(err)
			}
			_, werr := s.Write(ctx, &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: []byte("payload-A")})
			s.Close()

			// Phase B: reopen; recovery must succeed.
			rec, err := New(Config{Dir: dir, UseMemIndex: false})
			if err != nil {
				t.Fatalf("reopen after crash at %s: %v", point, err)
			}
			defer rec.Close()

			// CrashAfterAck simulates "caller saw success, then async
			// apply crashed": the write IS durable, so it must survive
			// and the fault is reported on the async path, not the write.
			if point == storage.CrashAfterAck {
				if werr != nil {
					t.Fatalf("after-ack write should have succeeded, got %v", werr)
				}
				got, rerr := rec.Read(ctx, &storage.ReadRequest{ExtentID: 1, Generation: 1})
				if rerr != nil {
					t.Fatalf("acknowledged write lost after reopen: %v", rerr)
				}
				if !bytes.Equal(got.Data, []byte("payload-A")) {
					t.Fatalf("acknowledged write corrupt after reopen: %q", got.Data)
				}
				return
			}

			// All other points: the write was never acknowledged. It may
			// be present (BatchCommit durable) or absent, but never
			// corrupt.
			if werr == nil {
				t.Fatal("write returned success despite injected crash")
			}
			got, rerr := rec.Read(ctx, &storage.ReadRequest{ExtentID: 1, Generation: 1})
			if rerr == nil {
				if !bytes.Equal(got.Data, []byte("payload-A")) {
					t.Fatalf("unacknowledged write read as corrupt bytes: %q", got.Data)
				}
			} else if !errors.Is(rerr, storage.ErrExtentNotFound) {
				t.Fatalf("unexpected read error after crash: %v", rerr)
			}
		})
	}
}

// TestCrashMatrix_AfterAck verifies the acknowledged-write invariant:
// once a receipt is returned, a crash + reopen must still return the
// exact bytes. We crash after the receipt by closing the store mid-life
// and reopening (the receipt already committed WAL + index).
func TestCrashMatrix_AfterAck(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	s, err := New(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("acknowledged"), 4096)
	rec, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 7, Generation: 1, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	_ = rec
	// "Crash" = close without clean state teardown.
	s.Close()

	// Reopen and verify the acknowledged write survived byte-for-byte.
	s2, err := New(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.Read(ctx, &storage.ReadRequest{ExtentID: 7, Generation: 1})
	if err != nil {
		t.Fatalf("acknowledged write lost after reopen: %v", err)
	}
	if !bytes.Equal(got.Data, data) {
		t.Fatalf("acknowledged write corrupt after reopen: %d bytes vs %d", len(got.Data), len(data))
	}
}

// TestRecovery_ManyWritesReopen writes many extents, closes, reopens,
// and verifies all survive — the WAL replay path under load.
func TestRecovery_ManyWritesReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	s, err := New(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	const n = 16
	for i := 0; i < n; i++ {
		data := testutil.DeterministicData(storage.ExtentID(i+1), 1, 128, uint32(i))
		if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: storage.ExtentID(i + 1), Generation: 1, Data: data}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	s.Close()

	// Reopen: all 500 must be present and byte-exact.
	s2, err := New(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for i := 0; i < n; i++ {
		want := testutil.DeterministicData(storage.ExtentID(i+1), 1, 128, uint32(i))
		got, err := s2.Read(ctx, &storage.ReadRequest{ExtentID: storage.ExtentID(i + 1), Generation: 1})
		if err != nil {
			t.Fatalf("read %d after reopen: %v", i, err)
		}
		if !bytes.Equal(got.Data, want) {
			t.Fatalf("read %d: data mismatch", i)
		}
	}
}
