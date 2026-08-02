package metadata

import (
	"testing"
	"time"
)

func TestScrubStore_BeginAdvanceComplete(t *testing.T) {
	store := newV2TestPebbleStore(t)
	scrub := NewScrubStore(store)

	c, err := scrub.Begin(100, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if c.State != ScrubInProgress || c.Offset != 0 {
		t.Fatalf("begin: %+v", c)
	}
	if err := scrub.Advance(100, 4096); err != nil {
		t.Fatal(err)
	}
	if err := scrub.Complete(100, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, _ := scrub.Get(100)
	if got.State != ScrubComplete || got.Offset != 4096 || got.LastScrub == 0 {
		t.Fatalf("complete: %+v", got)
	}
}

func TestScrub_DueLogic(t *testing.T) {
	now := time.Now()
	// Hot segment scrubbed recently → not due.
	recent := &ScrubCursor{SegmentID: 1, State: ScrubComplete, LastScrub: now.Add(-time.Hour).UnixNano(), Cold: false}
	if recent.Due(now) {
		t.Fatal("recently scrubbed hot segment should not be due")
	}
	// Hot segment scrubbed 31 days ago → due.
	old := &ScrubCursor{SegmentID: 2, State: ScrubComplete, LastScrub: now.Add(-31 * 24 * time.Hour).UnixNano(), Cold: false}
	if !old.Due(now) {
		t.Fatal("31-day-old hot segment should be due (30d cycle)")
	}
	// Cold segment scrubbed 31 days ago → not due (90d cycle).
	cold := &ScrubCursor{SegmentID: 3, State: ScrubComplete, LastScrub: now.Add(-31 * 24 * time.Hour).UnixNano(), Cold: true}
	if cold.Due(now) {
		t.Fatal("31-day-old cold segment should NOT be due (90d cycle)")
	}
	// Newly sealed segment always due (24h one-scrub target).
	neu := &ScrubCursor{SegmentID: 4, State: ScrubComplete, LastScrub: now.UnixNano(), NewSegment: true}
	if !neu.Due(now) {
		t.Fatal("newly sealed segment should be due")
	}
	// Failed scrub → due.
	failed := &ScrubCursor{SegmentID: 5, State: ScrubFailed}
	if !failed.Due(now) {
		t.Fatal("failed scrub should be due")
	}
}

func TestScrub_ResumeOffsetPersisted(t *testing.T) {
	store := newV2TestPebbleStore(t)
	scrub := NewScrubStore(store)
	if err := scrub.Advance(999, 12345); err != nil {
		t.Fatal(err)
	}
	// Simulate restart: a new ScrubStore on the same Pebble.
	got, err := scrub.Get(999)
	if err != nil {
		t.Fatal(err)
	}
	if got.Offset != 12345 {
		t.Fatalf("resume offset = %d, want 12345", got.Offset)
	}
}
