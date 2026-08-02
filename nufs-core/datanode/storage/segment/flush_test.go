package segment

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/example/dfs/datanode/storage"
)

// TestFlush_IndexSafePersistsSequence verifies §7.4: after writes commit
// and a flush fires, an INDEX_SAFE record is written carrying the safe
// sequence, and SafeSeq reflects it. The flush persists the overlay to
// Pebble and records the safe sequence.
func TestFlush_IndexSafePersistsSequence(t *testing.T) {
	dir := t.TempDir()
	// Short flush interval so the interval trigger fires.
	s, err := New(Config{Dir: dir, UseMemIndex: false, FlushInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Write a few extents.
	for i := 0; i < 5; i++ {
		if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: storage.ExtentID(i + 1), Generation: 1, Data: []byte("data")}); err != nil {
			t.Fatal(err)
		}
	}
	// Wait for the flush loop to fire (interval trigger).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.SafeSeq() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if s.SafeSeq() == 0 {
		t.Fatal("flush did not publish an INDEX_SAFE safe sequence")
	}
	t.Logf("safe sequence after flush = %d", s.SafeSeq())
	s.Close()
}

// TestFlush_RecoveryWithIndexSafeMarker verifies that an INDEX_SAFE
// marker in the segment log does not break recovery: after a flush and a
// reopen, all acknowledged writes still read back byte-exact.
func TestFlush_RecoveryWithIndexSafeMarker(t *testing.T) {
	dir := t.TempDir()
	s, err := New(Config{Dir: dir, UseMemIndex: false, FlushInterval: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	data := bytes.Repeat([]byte("flush-recovery"), 100)
	for i := 0; i < 20; i++ {
		if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: storage.ExtentID(i + 1), Generation: 1, Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	// Wait for at least one flush (INDEX_SAFE marker written).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.SafeSeq() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.Close()

	// Reopen: recovery must handle the INDEX_SAFE marker and all 20
	// extents must read back.
	s2, err := New(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	for i := 0; i < 20; i++ {
		got, err := s2.Read(ctx, &storage.ReadRequest{ExtentID: storage.ExtentID(i + 1), Generation: 1})
		if err != nil {
			t.Fatalf("read %d after reopen: %v", i+1, err)
		}
		if !bytes.Equal(got.Data, data) {
			t.Fatalf("extent %d data mismatch after reopen", i+1)
		}
	}
}
