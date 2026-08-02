package metadata

import (
	"fmt"
	"time"
)

// Scrub cursor (V2.1 §13.2): background segment scrub sequentially
// validates sealed segments with a persistent, resumable cursor. Hot
// segments complete a cycle within 30 days; cold EC segments within 90
// days; newly sealed segments get one scrub within 24 hours.
const (
	// ScrubHotCycle is the hot-segment scrub target (§13.2).
	ScrubHotCycle = 30 * 24 * time.Hour
	// ScrubColdCycle is the cold-EC-segment scrub target (§13.2).
	ScrubColdCycle = 90 * 24 * time.Hour
	// ScrubNewSegmentDelay is the one-scrub-within-24h target.
	ScrubNewSegmentDelay = 24 * time.Hour
	// ScrubCheckEvery is how often the scrubber considers a segment.
	ScrubCheckEvery = 7 * 24 * time.Hour
)

// ScrubState is the scrub lifecycle for a segment.
type ScrubState uint8

const (
	ScrubPending ScrubState = iota
	ScrubInProgress
	ScrubComplete
	ScrubFailed
)

// ScrubCursor is the persistent, resumable position for one segment's
// scrub (§13.2: "scrub cursors are persistent and resumable").
type ScrubCursor struct {
	// SegmentID identifies the segment being scrubbed.
	SegmentID uint64 `json:"segment_id"`
	// State is the scrub lifecycle.
	State ScrubState `json:"state"`
	// Offset is the resumable byte position within the segment.
	Offset int64 `json:"offset"`
	// LastScrub is when the segment last completed a scrub.
	LastScrub int64 `json:"last_scrub"`
	// Cold is true for cold EC segments (90-day cycle).
	Cold bool `json:"cold"`
	// NewSegment is true for recently sealed segments needing a 24h
	// scrub.
	NewSegment bool `json:"new_segment"`
}

// ScrubStore persists scrub cursors.
type ScrubStore struct {
	store *PebbleStore
}

// NewScrubStore creates the scrub store.
func NewScrubStore(store *PebbleStore) *ScrubStore {
	return &ScrubStore{store: store}
}

// scrubKey formats a scrub cursor key.
func scrubKey(segmentID uint64) string {
	return fmt.Sprintf("scrub/%020d", segmentID)
}

// Get reads a scrub cursor (zero-value if none).
func (s *ScrubStore) Get(segmentID uint64) (*ScrubCursor, error) {
	var c ScrubCursor
	exists, err := s.store.getValue(scrubKey(segmentID), &c)
	if err != nil {
		return nil, err
	}
	if !exists {
		return &ScrubCursor{SegmentID: segmentID, State: ScrubPending}, nil
	}
	return &c, nil
}

// Put writes a scrub cursor.
func (s *ScrubStore) Put(c *ScrubCursor) error {
	return s.store.putMsgpack(scrubKey(c.SegmentID), c)
}

// Begin starts (or resumes) a segment scrub, returning the resumable
// offset.
func (s *ScrubStore) Begin(segmentID uint64, cold, newSegment bool) (*ScrubCursor, error) {
	c, err := s.Get(segmentID)
	if err != nil {
		return nil, err
	}
	c.Cold = cold
	c.NewSegment = newSegment
	c.State = ScrubInProgress
	if err := s.Put(c); err != nil {
		return nil, err
	}
	return c, nil
}

// Advance persists scrub progress after validating a chunk of bytes.
func (s *ScrubStore) Advance(segmentID uint64, offset int64) error {
	c, err := s.Get(segmentID)
	if err != nil {
		return err
	}
	c.Offset = offset
	return s.Put(c)
}

// Complete marks a segment's scrub finished and records the time.
func (s *ScrubStore) Complete(segmentID uint64, at time.Time) error {
	c, err := s.Get(segmentID)
	if err != nil {
		return err
	}
	c.State = ScrubComplete
	c.LastScrub = at.UnixNano()
	c.NewSegment = false
	return s.Put(c)
}

// Fail marks a scrub as failed (corruption found → quarantine + repair).
func (s *ScrubStore) Fail(segmentID uint64, reason string) error {
	c, err := s.Get(segmentID)
	if err != nil {
		return err
	}
	c.State = ScrubFailed
	return s.Put(c)
}

// Due reports whether a segment is due for a scrub based on its class
// and last-scrub time (§13.2 cycle targets).
func (c *ScrubCursor) Due(now time.Time) bool {
	if c.State == ScrubPending || c.State == ScrubFailed {
		return true
	}
	if c.NewSegment {
		// Newly sealed segments need one scrub within 24h.
		return true
	}
	if c.LastScrub == 0 {
		return true
	}
	cycle := ScrubHotCycle
	if c.Cold {
		cycle = ScrubColdCycle
	}
	return now.Sub(time.Unix(0, c.LastScrub)) >= cycle
}
