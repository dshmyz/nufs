package segment

import (
	"context"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/index"
)

// overlayTestKey builds an extent index key at generation 1.
func overlayTestKey(id uint64) []byte {
	return index.Key(storage.ExtentID(id), 1)
}

// indexValueDurable builds a live extent value at the given offset.
func indexValueDurable(offset int64) index.Value {
	return index.Value{
		SegmentID:  1,
		Offset:     offset,
		StoredLen:  32,
		LogicalLen: 16,
		State:      storage.ExtentDurable,
	}
}

// TestFlush_CommittedExtentVisibleDuringDrainWindow pins the drain-window
// visibility invariant: a committed extent must never be absent from BOTH the
// overlay and the derived index.
//
// flush() drains the overlay and only then applies the mutations to Pebble.
// Both steps run under s.mu, but the read path (lookup) does not take s.mu --
// it consults the overlay's own lock, then Pebble. A destructive Drain
// therefore opened a window in which an acknowledged extent was in neither
// place, and every reader in that window got ErrExtentNotFound.
//
// That was not merely a stale read. Delete() resolves its target through the
// same lookup and treats ErrExtentNotFound as "already gone", returning nil
// WITHOUT appending a tombstone -- so the caller received a successful delete
// ack that was never recorded in the segment log, and recovery brought the
// extent back live. That end-to-end consequence is what
// TestProcessCrash_AcknowledgedMutationsRecover was failing intermittently;
// this test pins the underlying window deterministically via
// flushCheckpointHook, which fires after Drain and before the apply.
func TestFlush_CommittedExtentVisibleDuringDrainWindow(t *testing.T) {
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
	defer s.Close()

	ctx := context.Background()
	if _, err := s.Write(ctx, &storage.WriteRequest{
		ExtentID: 7, Generation: 1, Data: []byte("committed before flush"),
	}); err != nil {
		t.Fatal(err)
	}

	flushDone := make(chan error, 1)
	go func() { flushDone <- s.flush() }()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not reach the checkpoint hook")
	}

	// The overlay has been drained and Pebble has NOT been written yet. The
	// extent is durable in the segment log, so every read path must still
	// resolve it.
	//
	// Only read paths are exercised inside the window: flush() holds s.mu
	// while parked in the hook, and commitBatch needs that same lock, so a
	// Delete issued here would deadlock against the paused flush rather than
	// test anything. The consequence for Delete -- that a not-found lookup
	// makes it ack without appending a tombstone -- is covered end to end by
	// TestProcessCrash_AcknowledgedMutationsRecover, which is exactly the
	// gate this bug was failing.
	st, err := s.Stat(ctx, &storage.StatRequest{ExtentID: 7, Generation: 1})
	if err != nil {
		t.Fatalf("Stat during drain window: %v (want the committed extent)", err)
	}
	if st.State != storage.ExtentDurable {
		t.Errorf("state = %d, want ExtentDurable(%d)", st.State, storage.ExtentDurable)
	}

	// Read resolves through cachedLookup, a different path than Stat: it
	// consults locCache before the overlay. Both must see the extent.
	if _, err := s.Read(ctx, &storage.ReadRequest{ExtentID: 7, Generation: 1}); err != nil {
		t.Errorf("Read during drain window: %v (want the committed payload)", err)
	}

	close(release)
	if err := <-flushDone; err != nil {
		t.Fatalf("flush: %v", err)
	}

	// After the flush completes the extent is still resolvable, now from
	// Pebble rather than the overlay.
	st, err = s.Stat(ctx, &storage.StatRequest{ExtentID: 7, Generation: 1})
	if err != nil {
		t.Fatalf("Stat after flush: %v", err)
	}
	if st.State != storage.ExtentDurable {
		t.Errorf("post-flush state = %d, want ExtentDurable(%d)", st.State, storage.ExtentDurable)
	}

	// The drained entry must not linger in the overlay once Pebble owns it,
	// or the overlay is no longer bounded by the flush delta.
	if _, ok := s.Overlay().Get(overlayTestKey(7)); ok {
		t.Error("overlay still holds the entry after a successful flush; the delta is unbounded")
	}
}

// TestOverlay_DrainKeepsEntriesReadableUntilResolved covers the Overlay
// contract directly: drained entries stay visible to Get until the flush
// resolves them, DiscardDrained releases them once Pebble owns them, and
// RestoreDrained puts them back in the delta after a failed flush without
// clobbering a newer commit that raced the drain.
func TestOverlay_DrainKeepsEntriesReadableUntilResolved(t *testing.T) {
	key := func(id uint64) []byte { return overlayTestKey(id) }

	t.Run("visible_after_drain", func(t *testing.T) {
		o := NewOverlay()
		o.Put(key(1), indexValueDurable(10))
		if got := o.Drain(); len(got) != 1 {
			t.Fatalf("Drain returned %d mutations, want 1", len(got))
		}
		if o.Len() != 0 {
			t.Errorf("Len() = %d after Drain, want 0 (drained entries are not delta)", o.Len())
		}
		v, ok := o.Get(key(1))
		if !ok {
			t.Fatal("Get after Drain: entry invisible; a committed extent would read as not-found")
		}
		if v.Offset != 10 {
			t.Errorf("Offset = %d, want 10", v.Offset)
		}
	})

	t.Run("discard_releases", func(t *testing.T) {
		o := NewOverlay()
		o.Put(key(1), indexValueDurable(10))
		o.Drain()
		o.DiscardDrained()
		if _, ok := o.Get(key(1)); ok {
			t.Error("Get after DiscardDrained: entry still present; the overlay is unbounded")
		}
	})

	t.Run("restore_returns_to_delta", func(t *testing.T) {
		o := NewOverlay()
		o.Put(key(1), indexValueDurable(10))
		o.Drain()
		o.RestoreDrained()
		if o.Len() != 1 {
			t.Errorf("Len() = %d after RestoreDrained, want 1", o.Len())
		}
		if _, ok := o.Get(key(1)); !ok {
			t.Error("entry lost after RestoreDrained")
		}
	})

	t.Run("restore_does_not_clobber_newer_commit", func(t *testing.T) {
		o := NewOverlay()
		o.Put(key(1), indexValueDurable(10))
		o.Drain()
		// A commit lands after the drain: newer than the staged copy.
		o.Put(key(1), indexValueDurable(99))
		o.RestoreDrained()
		v, ok := o.Get(key(1))
		if !ok {
			t.Fatal("entry missing")
		}
		if v.Offset != 99 {
			t.Errorf("Offset = %d, want 99: RestoreDrained overwrote a newer commit", v.Offset)
		}
	})

	t.Run("discard_keeps_newer_commit", func(t *testing.T) {
		o := NewOverlay()
		o.Put(key(1), indexValueDurable(10))
		o.Drain()
		o.Put(key(1), indexValueDurable(99))
		o.DiscardDrained()
		v, ok := o.Get(key(1))
		if !ok {
			t.Fatal("DiscardDrained dropped a commit that raced the flush")
		}
		if v.Offset != 99 {
			t.Errorf("Offset = %d, want 99", v.Offset)
		}
	})
}
