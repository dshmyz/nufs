package datanode

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/journal"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ackRecord records AckChangeEvents calls with the delivered seq.
type ackRecord struct {
	seq uint64
}

// mockHeartbeatMetaChange ships to a buffer and records change-ack calls.
type mockHeartbeatMetaChange struct {
	mu      sync.Mutex
	reports []*metadata.NodeReport
	acks    []uint64
	ackRet  uint64
}

func (m *mockHeartbeatMetaChange) Heartbeat(_ context.Context, _ metadata.NodeID, report *metadata.NodeReport) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// capture a copy so the reporter's reuse of the slice can't leak
	r := *report
	r.ChunkStates = report.ChunkStates
	r.ChangeEvents = append([]metadata.ChangeEventRecord(nil), report.ChangeEvents...)
	m.reports = append(m.reports, &r)
	return nil
}

func (m *mockHeartbeatMetaChange) AckChangeEvents(_ context.Context, _ metadata.NodeID, seq uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acks = append(m.acks, seq)
	return m.ackRet, nil
}

func (m *mockHeartbeatMetaChange) lastEvents() []metadata.ChangeEventRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.reports) == 0 {
		return nil
	}
	return m.reports[len(m.reports)-1].ChangeEvents
}

func (m *mockHeartbeatMetaChange) ackCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.acks)
}

// TestHeartbeat_ShipsChangeJournalEventsAndAcks proves the Phase B flow: a
// corrupt event appended to the node's change journal rides on the next
// heartbeat's NodeReport, and once metadata reports a reconcilable watermark
// the journal Ack advances past the shipped events.
func TestHeartbeat_ShipsChangeJournalEventsAndAcks(t *testing.T) {
	j, err := journal.OpenChangeJournal(journal.JournalOptions{
		Dir:               t.TempDir(),
		MaxPerHeartbeat:   100,
		MaxHeartbeatBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("OpenChangeJournal: %v", err)
	}
	t.Cleanup(func() { j.Close() })

	// Append one corrupt event (seq 1) and one disk-lost event (seq 2).
	if _, err := j.Append(journal.EventCorrupt, storage.ExtentID(42), 1, 0, "checksum mismatch"); err != nil {
		t.Fatalf("Append corrupt: %v", err)
	}
	if _, err := j.Append(journal.EventDiskLost, 0, 0, 3, "disk io error"); err != nil {
		t.Fatalf("Append disk lost: %v", err)
	}

	meta := &mockHeartbeatMetaChange{ackRet: 2}
	h := NewHeartbeatReporter(Config{NodeID: 7, HeartbeatInterval: time.Second}, meta, &emptyHeartbeatStore{})
	h.SetChangeJournal(j)

	// Drive a single send directly (no loop).
	h.send()

	evs := meta.lastEvents()
	if len(evs) != 2 {
		t.Fatalf("shipped %d change events, want 2 (%+v)", len(evs), evs)
	}
	if evs[0].Kind != metadata.ChangeCorrupt || evs[0].ExtentID != 42 {
		t.Fatalf("event[0]=%+v, want corrupt extent 42", evs[0])
	}
	if evs[1].Kind != metadata.ChangeDiskLost {
		t.Fatalf("event[1]=%+v, want disk_lost", evs[1])
	}

	// After metadata confirms watermark 2, the journal is acked: Pending now
	// returns nothing.
	if p, _ := j.Pending(100, 1<<20); len(p) != 0 {
		t.Fatalf("journal not acked after heartbeat; still pending: %+v", p)
	}
}

// emptyHeartbeatStore satisfies HeartbeatStore with no chunks.
type emptyHeartbeatStore struct{}

func (e *emptyHeartbeatStore) Stats() (int64, int64) { return 0, 0 }
func (e *emptyHeartbeatStore) ChunkStateSnapshot() map[metadata.ChunkID]metadata.ReplicaState {
	return map[metadata.ChunkID]metadata.ReplicaState{}
}
func (e *emptyHeartbeatStore) StateVersion() uint64 { return 0 }
func (e *emptyHeartbeatStore) DiskStats() []DiskStatsItem {
	return nil
}
func (e *emptyHeartbeatStore) WriteErrorRate() float64 { return 0 }

var _ HeartbeatStore = (*emptyHeartbeatStore)(nil)
