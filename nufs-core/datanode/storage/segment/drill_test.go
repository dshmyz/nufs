package segment

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/journal"
)

// Failure drills (§18.3) as automated tests. The kill-9/power-loss
// scenarios are simulated by reopen-after-close (process crash without
// graceful state flush) — the crash matrix already covers every crash
// point; these drills cover the disk-loss and ENOSPC control-plane
// responses.

// TestDrill_DiskLossEmitsEvent verifies that a disk loss is recorded in
// the change journal as an async event (§12: EventDiskLost), which
// drives repair-batch creation.
func TestDrill_DiskLossEmitsEvent(t *testing.T) {
	dir := t.TempDir()
	j, err := journal.OpenChangeJournal(journal.JournalOptions{Dir: dir + "/journal"})
	if err != nil {
		t.Fatal(err)
	}
	seq, err := j.Append(journal.EventDiskLost, 0, 0, 42, "nvme_removed")
	if err != nil {
		t.Fatal(err)
	}
	// Heartbeat picks up the event (≤10000 events / 4MiB per §12).
	events, _ := j.Pending(100, 1<<20)
	if len(events) != 1 || events[0].Kind != journal.EventDiskLost {
		t.Fatalf("pending events: %+v", events)
	}
	if events[0].SegmentID != 42 || events[0].Reason != "nvme_removed" {
		t.Fatalf("disk lost event: %+v", events[0])
	}
	// Ack clears it from the pending set.
	j.Ack(seq)
	events2, _ := j.Pending(100, 1<<20)
	if len(events2) != 0 {
		t.Fatalf("after ack, pending should be empty, got %d", len(events2))
	}
}

// TestDrill_CorruptReadNeverSucceeds verifies the §21 gate "corrupt or
// unverifiable data is never returned successfully": a bit-flipped
// record fails the read rather than returning bad bytes.
func TestDrill_CorruptReadNeverSucceeds(t *testing.T) {
	dir := t.TempDir()
	s, err := New(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	data := deterministicDrillPayload(16 << 10)
	receipt, err := s.Write(context.Background(), &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Flip a byte in the segment file's payload region to simulate
	// bitrot (§18.3 "bit flip"). Default StreamID=0 → small/active.
	corruptSegmentPayload(t, dir+"/segments/small/active/1.seg", receipt.Offset)

	s2, err := New(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatalf("payload corruption must not mask an unrelated startup failure: %v", err)
	}
	defer s2.Close()
	got, err := s2.Read(context.Background(), &storage.ReadRequest{ExtentID: 1, Generation: 1})
	if err == nil {
		// If the read succeeded, the data must be byte-exact (the
		// corruption must not be silently returned).
		if !bytes.Equal(got.Data, data) {
			t.Fatal("corrupt bytes returned as a successful read — §21 gate violated")
		}
		t.Fatal("corrupt read succeeded unchanged — §21 gate violated")
	}
}

// corruptSegmentPayload flips a byte in a known record payload region to
// simulate bitrot
// (§18.3 "bit flip").
func corruptSegmentPayload(t *testing.T, path string, offset int64) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pos := int(offset) + RecordHeaderSize + FrameIndexEntrySize + 100
	if pos >= len(data) {
		t.Fatalf("segment too small to corrupt: %d", len(data))
	}
	data[pos] ^= 0xFF
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}

func deterministicDrillPayload(n int) []byte {
	payload := make([]byte, n)
	var state uint32 = 0x6d2b79f5
	for i := range payload {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		payload[i] = byte(state)
	}
	return payload
}
