package segment

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/example/dfs/datanode/storage"
)

// TestStoreClose_IsIdempotent proves repeated Close() calls neither panic
// nor return divergent results. Before the one-shot shutdown the second
// call panicked with "close of closed channel" because Close() closed
// s.stopCh unconditionally.
func TestStoreClose_IsIdempotent(t *testing.T) {
	s := openCloseTestStore(t)

	first := s.Close()
	if first != nil {
		t.Fatalf("first close: %v", first)
	}
	for i := 0; i < 4; i++ {
		if err := s.Close(); err != first {
			t.Fatalf("close #%d returned %v, want %v", i+2, err, first)
		}
	}
}

// TestStoreClose_ConcurrentCallersAgree runs many concurrent Close() calls
// and requires one shutdown execution with a single shared result.
func TestStoreClose_ConcurrentCallersAgree(t *testing.T) {
	s := openCloseTestStore(t)

	const callers = 16
	errs := make([]error, callers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // maximise contention on the once
			errs[i] = s.Close()
		}(i)
	}
	close(start)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent Close() deadlocked")
	}

	for i, err := range errs {
		if err != errs[0] {
			t.Fatalf("caller %d got %v, caller 0 got %v: callers disagree", i, err, errs[0])
		}
	}
	if errs[0] != nil {
		t.Fatalf("close: %v", errs[0])
	}
}

// TestStoreClose_FlushesAcknowledgedWrites guards the durability barrier
// while making shutdown idempotent: data acknowledged before Close must
// still be readable after reopening, and the extra Close calls must not
// disturb that.
func TestStoreClose_FlushesAcknowledgedWrites(t *testing.T) {
	dir := t.TempDir()
	s, err := New(Config{Dir: dir, StreamID: 1})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx := context.Background()
	payload := []byte("acknowledged-before-close")
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 5, Generation: 1, Data: payload}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	reopened, err := New(Config{Dir: dir, StreamID: 1})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.Read(ctx, &storage.ReadRequest{ExtentID: 5, Generation: 1})
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if string(got.Data) != string(payload) {
		t.Fatalf("read %q, want %q", got.Data, payload)
	}
}

// TestStoreClose_ConcurrentWithWrites closes the store while writers are
// still in flight. Writers must either succeed or fail cleanly; no call
// may panic or hang. Before the admission gate a writer that reached the
// index after index.Close() panicked the process with "pebble: closed",
// which crashes the datanode whenever a request is in flight at shutdown.
func TestStoreClose_ConcurrentWithWrites(t *testing.T) {
	s := openCloseTestStore(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 16; j++ {
				// Errors are expected once the store closes; a panic is not.
				_, err := s.Write(ctx, &storage.WriteRequest{
					ExtentID:   storage.ExtentID(i*100 + j),
					Generation: 1,
					Data:       []byte("concurrent"),
				})
				if err != nil && !errors.Is(err, storage.ErrStoreClosed) && !errors.Is(err, storage.ErrCapacity) {
					t.Errorf("write %d/%d: unexpected error %v", i, j, err)
					return
				}
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = s.Close() }()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("Close() concurrent with writes deadlocked")
	}
}

// TestStoreClose_RejectsRequestsAfterClose proves every mutating and
// reading entry point fails with ErrStoreClosed once the store is shut
// down, rather than reaching a closed Pebble handle and panicking.
func TestStoreClose_RejectsRequestsAfterClose(t *testing.T) {
	s := openCloseTestStore(t)
	ctx := context.Background()
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	cases := []struct {
		name string
		call func() error
	}{
		{"Write", func() error {
			_, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: []byte("x")})
			return err
		}},
		{"Read", func() error {
			_, err := s.Read(ctx, &storage.ReadRequest{ExtentID: 1, Generation: 1})
			return err
		}},
		{"Delete", func() error {
			return s.Delete(ctx, &storage.DeleteRequest{ExtentID: 1, Generation: 1})
		}},
		{"Stat", func() error {
			_, err := s.Stat(ctx, &storage.StatRequest{ExtentID: 1, Generation: 1})
			return err
		}},
		{"AppendRecord", func() error {
			_, err := s.AppendRecord(1, 1, []byte("x"), storage.CompressionNone)
			return err
		}},
		{"Relocate", func() error {
			return s.Relocate([]storage.Reloc{{ExtentID: 1, Generation: 1}})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, storage.ErrStoreClosed) {
				t.Fatalf("after close got %v, want ErrStoreClosed", err)
			}
		})
	}
}

func openCloseTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(Config{Dir: t.TempDir(), StreamID: 1})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return s
}
