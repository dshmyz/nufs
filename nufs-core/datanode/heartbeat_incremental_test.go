package datanode

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// mockHeartbeatMeta captures heartbeat reports for inspection.
type mockHeartbeatMeta struct {
	mu      sync.Mutex
	reports []*metadata.NodeReport
	count   int
	nodeIDs []metadata.NodeID
}

func (m *mockHeartbeatMeta) Heartbeat(_ context.Context, nodeID metadata.NodeID, report *metadata.NodeReport) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reports = append(m.reports, report)
	m.count++
	m.nodeIDs = append(m.nodeIDs, nodeID)
	return nil
}

func (m *mockHeartbeatMeta) AckChangeEvents(_ context.Context, _ metadata.NodeID, _ uint64) (uint64, error) {
	return 0, nil
}

func (m *mockHeartbeatMeta) getReports() []*metadata.NodeReport {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*metadata.NodeReport, len(m.reports))
	copy(out, m.reports)
	return out
}

// TestHeartbeat_IncrementalChunkStates verifies that the heartbeat
// reporter only sends chunk state changes (deltas) instead of the
// full chunk state map on every heartbeat.
func TestHeartbeat_IncrementalChunkStates(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	defer cs.Close()

	// Write 5 chunks
	for i := 0; i < 5; i++ {
		if err := cs.Write(metadata.ChunkID(100+i), []byte("data")); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	mock := &mockHeartbeatMeta{}
	cfg := Config{
		NodeID:            1,
		HeartbeatInterval: 50 * time.Millisecond,
	}
	reporter := NewHeartbeatReporter(cfg, mock, cs)

	// First heartbeat sends all 5 chunks (full sync)
	reporter.Start()
	time.Sleep(100 * time.Millisecond)

	reports := mock.getReports()
	if len(reports) == 0 {
		reporter.Stop()
		t.Fatal("no heartbeat reports received")
	}

	// First report should have all 5 chunks
	firstReport := reports[0]
	if len(firstReport.ChunkStates) != 5 {
		t.Fatalf("first report: expected 5 chunk states, got %d", len(firstReport.ChunkStates))
	}

	// Wait for second heartbeat — no changes, should be empty (delta)
	time.Sleep(100 * time.Millisecond)
	reports = mock.getReports()
	if len(reports) < 2 {
		reporter.Stop()
		t.Fatalf("expected at least 2 reports, got %d", len(reports))
	}

	// Second report should have 0 chunk states (no changes)
	secondReport := reports[1]
	if len(secondReport.ChunkStates) != 0 {
		t.Errorf("second report: expected 0 chunk states (no changes), got %d", len(secondReport.ChunkStates))
	}

	// Seal a chunk — state change should be reported as delta
	if _, err := cs.Seal(metadata.ChunkID(100)); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	reports = mock.getReports()

	// Find the report after the seal — should only contain the changed chunk
	foundDelta := false
	for i := 2; i < len(reports); i++ {
		if len(reports[i].ChunkStates) > 0 {
			foundDelta = true
			if len(reports[i].ChunkStates) != 1 {
				t.Errorf("report %d: expected 1 changed chunk, got %d", i, len(reports[i].ChunkStates))
			}
			if _, ok := reports[i].ChunkStates[metadata.ChunkID(100)]; !ok {
				t.Errorf("report %d: expected chunk 100 in delta, got %v", i, reports[i].ChunkStates)
			}
			break
		}
	}
	if !foundDelta {
		t.Error("no delta report found after Seal")
	}

	reporter.Stop()
}

// TestHeartbeat_ForceFullSync verifies that the reporter can be
// forced to do a full sync (e.g., on reconnect after network
// partition).
func TestHeartbeat_ForceFullSync(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	defer cs.Close()

	for i := 0; i < 3; i++ {
		if err := cs.Write(metadata.ChunkID(200+i), []byte("data")); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	mock := &mockHeartbeatMeta{}
	cfg := Config{
		NodeID:            1,
		HeartbeatInterval: 50 * time.Millisecond,
	}
	reporter := NewHeartbeatReporter(cfg, mock, cs)
	reporter.Start()

	// Wait for initial full sync
	time.Sleep(100 * time.Millisecond)
	reports := mock.getReports()
	if len(reports) == 0 {
		reporter.Stop()
		t.Fatal("no reports")
	}

	// Wait for a delta (empty) report
	time.Sleep(100 * time.Millisecond)
	reportsBeforeForce := len(mock.getReports())

	// Force full sync
	reporter.ForceFullSync()

	// Wait for at least one more heartbeat after the force
	time.Sleep(150 * time.Millisecond)
	reportsAfter := mock.getReports()

	// Find the first report after ForceFullSync — it should be a
	// full sync with all 3 chunks.
	foundFullSync := false
	for i := reportsBeforeForce; i < len(reportsAfter); i++ {
		if len(reportsAfter[i].ChunkStates) == 3 {
			foundFullSync = true
			break
		}
	}
	if !foundFullSync {
		t.Errorf("forced full sync: no report with 3 chunk states found after force (reports after force: %d)",
			len(reportsAfter)-reportsBeforeForce)
		for i := reportsBeforeForce; i < len(reportsAfter); i++ {
			t.Logf("  report %d: %d states", i, len(reportsAfter[i].ChunkStates))
		}
	}

	reporter.Stop()
}
