package journal

import (
	"testing"

	"github.com/example/dfs/datanode/storage"
)

func TestChangeJournal_AppendAndResume(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenChangeJournal(JournalOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	s1, err := j.Append(EventCorrupt, 1, 2, 0, "checksum_mismatch")
	if err != nil {
		t.Fatal(err)
	}
	if s1 != 1 {
		t.Fatalf("first seq = %d, want 1", s1)
	}
	if _, err := j.Append(EventDiskLost, 0, 0, 7, "disk_failed"); err != nil {
		t.Fatal(err)
	}
	j.Close()

	// Reopen: sequence must resume and events must decode.
	j2, err := OpenChangeJournal(JournalOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	if j2.Seq() != 2 {
		t.Fatalf("seq after reopen = %d, want 2", j2.Seq())
	}
	evs, nextAck := j2.Pending(100, 1<<20)
	if len(evs) != 2 {
		t.Fatalf("expected 2 pending events, got %d", len(evs))
	}
	if evs[0].Kind != EventCorrupt || evs[0].ExtentID != 1 || evs[0].Generation != 2 {
		t.Fatalf("event 0 mismatch: %+v", evs[0])
	}
	if evs[1].Kind != EventDiskLost || evs[1].SegmentID != 7 {
		t.Fatalf("event 1 mismatch: %+v", evs[1])
	}
	_ = nextAck
}

func TestChangeJournal_AckFilters(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenChangeJournal(JournalOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for i := 0; i < 5; i++ {
		if _, err := j.Append(EventCorrupt, storage.ExtentID(i+1), 1, 0, "x"); err != nil {
			t.Fatal(err)
		}
	}
	// Ack seq 2: only events 3-5 remain pending.
	j.Ack(2)
	evs, _ := j.Pending(100, 1<<20)
	if len(evs) != 3 {
		t.Fatalf("expected 3 pending after ack 2, got %d", len(evs))
	}
	if evs[0].Seq != 3 {
		t.Fatalf("first pending seq = %d, want 3", evs[0].Seq)
	}
}

func TestChangeJournal_HeartbeatBounds(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenChangeJournal(JournalOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for i := 0; i < 10; i++ {
		if _, err := j.Append(EventCorrupt, storage.ExtentID(i+1), 1, 0, "reason"); err != nil {
			t.Fatal(err)
		}
	}
	// Bound to 3 events per heartbeat (§12: at most 10000 events / 4 MiB).
	evs, _ := j.Pending(3, 1<<20)
	if len(evs) != 3 {
		t.Fatalf("expected 3 bounded events, got %d", len(evs))
	}
}
