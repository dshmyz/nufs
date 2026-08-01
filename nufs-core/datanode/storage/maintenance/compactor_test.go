package maintenance

import (
	"testing"

	"github.com/example/dfs/datanode/storage"
)

// fakeSink records appended relocations and relocated entries.
type fakeSink struct {
	appends  []storage.Reloc
	relocs   []storage.Reloc
	relocErr error
}

func (f *fakeSink) AppendRecord(extentID storage.ExtentID, gen storage.Generation, data []byte, codec storage.CompressionCodec) (*storage.Reloc, error) {
	r := &storage.Reloc{
		ExtentID:   extentID,
		Generation: gen,
		SegmentID:  99,
		Offset:     int64(len(f.appends) * 100),
		StoredLen:  uint32(len(data)),
		LogicalLen: uint32(len(data)),
	}
	f.appends = append(f.appends, *r)
	return r, nil
}

func (f *fakeSink) Relocate(relocs []storage.Reloc) error {
	if f.relocErr != nil {
		return f.relocErr
	}
	f.relocs = append(f.relocs, relocs...)
	return nil
}

func TestCompactor_MovesOnlyLive(t *testing.T) {
	sink := &fakeSink{}
	c := NewCompactor(sink, nil)
	records := []ScannedRecord{
		{ExtentID: 1, Generation: 1, LogicalLen: 10, ReadPayload: func() ([]byte, error) { return []byte("record-1"), nil }},
		{ExtentID: 2, Generation: 1, LogicalLen: 10, ReadPayload: func() ([]byte, error) { return []byte("record-2"), nil }},
		{ExtentID: 3, Generation: 1, LogicalLen: 10, ReadPayload: func() ([]byte, error) { return []byte("record-3"), nil }},
	}
	// Only extents 1 and 3 are live (2's index no longer points at the
	// source segment, so it is dead).
	live := map[storage.ExtentID]struct{}{1: {}, 3: {}}
	isLive := func(id storage.ExtentID, gen storage.Generation) bool {
		_, ok := live[id]
		return ok
	}
	copied, err := c.Compact(5, records, isLive)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if copied != 2 {
		t.Fatalf("expected 2 live records copied, got %d", copied)
	}
	if len(sink.appends) != 2 {
		t.Fatalf("expected 2 appends, got %d", len(sink.appends))
	}
	if sink.appends[0].ExtentID != 1 || sink.appends[1].ExtentID != 3 {
		t.Fatalf("appended wrong extents: %+v", sink.appends)
	}
	if len(sink.relocs) != 2 {
		t.Fatalf("expected 2 relocations, got %d", len(sink.relocs))
	}
}

func TestCompactor_RelocateFailure(t *testing.T) {
	sink := &fakeSink{relocErr: errSimulated}
	c := NewCompactor(sink, nil)
	records := []ScannedRecord{
		{ExtentID: 1, Generation: 1, LogicalLen: 5, ReadPayload: func() ([]byte, error) { return []byte("data"), nil }},
	}
	_, err := c.Compact(5, records, func(id storage.ExtentID, gen storage.Generation) bool { return true })
	if err == nil {
		t.Fatal("expected relocate failure to propagate")
	}
}

func TestCompactorCandidateScoring(t *testing.T) {
	c := CompactionCandidate{SegmentID: 1, DeadBytes: 400, LiveBytes: 100}
	if !c.Eligible() {
		t.Fatal("40% dead bytes should be eligible")
	}
	c.ScoreWith(1.0, 1.0, 1.0)
	if c.Score <= 0 {
		t.Fatalf("expected positive score, got %v", c.Score)
	}
	// A segment with no dead bytes is not eligible.
	clean := CompactionCandidate{SegmentID: 2, DeadBytes: 0, LiveBytes: 500}
	if clean.Eligible() {
		t.Fatal("clean segment should not be eligible")
	}
}

func TestSchedulerSelect(t *testing.T) {
	s := NewScheduler()
	cands := []CompactionCandidate{
		{SegmentID: 1, DeadBytes: 500, LiveBytes: 100}, // 83% dead, high value
		{SegmentID: 2, DeadBytes: 30, LiveBytes: 70},   // 30% dead, borderline
		{SegmentID: 3, DeadBytes: 0, LiveBytes: 500},   // not eligible
	}
	s.Track(1) // segment 1 already queued
	sel := s.Select(cands, 5, 1.0, 1.0, 1.0)
	if len(sel) != 1 {
		t.Fatalf("expected 1 eligible unqueued candidate, got %d: %+v", len(sel), sel)
	}
	if sel[0].SegmentID != 2 {
		t.Fatalf("expected segment 2 selected, got %d", sel[0].SegmentID)
	}
	s.Release(1)
	sel2 := s.Select(cands, 5, 1.0, 1.0, 1.0)
	if len(sel2) != 2 {
		t.Fatalf("after release expected 2 candidates, got %d", len(sel2))
	}
}

var errSimulated = &simErr{}

type simErr struct{}

func (*simErr) Error() string { return "simulated relocate failure" }
