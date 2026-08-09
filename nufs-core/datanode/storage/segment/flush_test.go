package segment

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/index"
)

func TestFlush_CheckpointExcludesCommitPublication(t *testing.T) {
	dir := t.TempDir()
	entered := make(chan struct{})
	release := make(chan struct{})
	s, err := New(Config{
		Dir:               dir,
		FlushInterval:     time.Hour,
		disableAsyncApply: true,
		flushCheckpointHook: func() {
			close(entered)
			<-release
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: []byte("before checkpoint")}); err != nil {
		t.Fatal(err)
	}
	commitStarted := make(chan struct{})
	allowCommit := make(chan struct{})
	s.beforeCommitLock = func() {
		close(commitStarted)
		<-allowCommit
	}

	flushDone := make(chan error, 1)
	go func() { flushDone <- s.flush() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("flush did not reach checkpoint boundary")
	}

	commitDone := make(chan error, 1)
	go func() {
		_, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 2, Generation: 1, Data: []byte("after checkpoint")})
		commitDone <- err
	}()
	select {
	case <-commitStarted:
	case <-time.After(time.Second):
		t.Fatal("commit did not reach the segment-lock barrier")
	}
	close(allowCommit)
	select {
	case err := <-commitDone:
		t.Fatalf("commit acknowledged during checkpoint transaction: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-flushDone; err != nil {
		t.Fatalf("flush: %v", err)
	}
	select {
	case err := <-commitDone:
		if err != nil {
			t.Fatalf("commit after checkpoint: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("commit remained blocked after checkpoint")
	}

	checkpointSafeSeq := s.SafeSeq()
	if checkpointSafeSeq != 1 {
		t.Fatalf("checkpoint safe sequence = %d, want 1", checkpointSafeSeq)
	}
	if pending := s.flushMutations.Load(); pending != 1 {
		t.Fatalf("post-checkpoint pending mutations = %d, want 1", pending)
	}
	crashStoreForTest(t, s)

	recovered, err := New(Config{Dir: dir, FlushInterval: time.Hour, disableAsyncApply: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer recovered.Close()
	if got := recovered.RecoveryResult(); got.SafeSeq != checkpointSafeSeq || got.Applied != 1 {
		t.Fatalf("recovery result = %+v, want checkpoint suffix replay of one record", got)
	}
	for id, want := range map[storage.ExtentID][]byte{
		1: []byte("before checkpoint"),
		2: []byte("after checkpoint"),
	} {
		got, err := recovered.Read(ctx, &storage.ReadRequest{ExtentID: id, Generation: 1})
		if err != nil {
			t.Fatalf("read extent %d after crash/reopen: %v", id, err)
		}
		if !bytes.Equal(got.Data, want) {
			t.Fatalf("extent %d = %q, want %q", id, got.Data, want)
		}
	}
}

func TestFlush_ErrorReleasesCheckpointTransaction(t *testing.T) {
	flushErr := errors.New("injected flush apply failure")
	s, err := New(Config{
		Dir:               t.TempDir(),
		FlushInterval:     time.Hour,
		disableAsyncApply: true,
		flushApply: func([]index.Mutation) error {
			return flushErr
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: []byte("flush error")}); err != nil {
		t.Fatal(err)
	}
	if err := s.flush(); !errors.Is(err, flushErr) {
		t.Fatalf("flush error = %v, want %v", err, flushErr)
	}
	if s.SafeSeq() != 0 || s.Overlay().Len() != 1 {
		t.Fatalf("failed flush advanced checkpoint or lost overlay: safe=%d overlay=%d", s.SafeSeq(), s.Overlay().Len())
	}

	commitDone := make(chan error, 1)
	go func() {
		_, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 2, Generation: 1, Data: []byte("after flush error")})
		commitDone <- err
	}()
	select {
	case err := <-commitDone:
		if err != nil {
			t.Fatalf("commit after failed flush: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("failed flush leaked checkpoint lock")
	}
	s.flushApply = nil
	if err := s.flush(); err != nil {
		t.Fatalf("flush after error: %v", err)
	}
}

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
