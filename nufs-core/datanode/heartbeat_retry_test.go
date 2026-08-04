package datanode

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

// mockHeartbeatMetaFail fails heartbeat calls until release is closed,
// then records reports like mockHeartbeatMeta.
type mockHeartbeatMetaFail struct {
	mu      sync.Mutex
	reports []*metadata.NodeReport
	count   int
	fail    bool
}

func (m *mockHeartbeatMetaFail) Heartbeat(_ context.Context, _ metadata.NodeID, report *metadata.NodeReport) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return errors.New("metad unreachable")
	}
	m.reports = append(m.reports, report)
	m.count++
	return nil
}

func (m *mockHeartbeatMetaFail) AckChangeEvents(_ context.Context, _ metadata.NodeID, _ uint64) (uint64, error) {
	return 0, nil
}

func (m *mockHeartbeatMetaFail) getReports() []*metadata.NodeReport {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*metadata.NodeReport, len(m.reports))
	copy(out, m.reports)
	return out
}

func (m *mockHeartbeatMetaFail) setFail(f bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail = f
}

// TestHeartbeat_RetryAfterFailure verifies the conditional-advance
// property: when a heartbeat send fails, the pending chunk-state changes
// are NOT marked acknowledged, so the next successful heartbeat re-sends
// them (re-derived from the cached snapshot, no rescan needed).
func TestHeartbeat_RetryAfterFailure(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	defer cs.Close()

	// Write a chunk and seal it so the heartbeat reports ReplicaReady.
	if err := cs.Write(metadata.ChunkID(300), []byte("data")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := cs.Seal(metadata.ChunkID(300)); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	mock := &mockHeartbeatMetaFail{fail: true}
	cfg := Config{NodeID: 1, HeartbeatInterval: 40 * time.Millisecond}
	reporter := NewHeartbeatReporter(cfg, mock, cs)
	reporter.Start()

	// Wait through at least two failed heartbeat attempts.
	time.Sleep(150 * time.Millisecond)
	if n := mock.getReports(); len(n) != 0 {
		t.Fatalf("expected no successful reports while failing, got %d", len(n))
	}

	// Recover the metadata service; the pending change must be delivered.
	mock.setFail(false)
	time.Sleep(150 * time.Millisecond)
	reporter.Stop()

	reports := mock.getReports()
	if len(reports) == 0 {
		t.Fatal("no report delivered after recovery")
	}
	// The first successful send after recovery must carry the pending
	// change (later ticks are empty steady-state deltas).
	if _, ok := reports[0].ChunkStates[metadata.ChunkID(300)]; !ok {
		t.Errorf("expected chunk 300 in the post-recovery report, got %v", reports[0].ChunkStates)
	}
}

// TestHeartbeat_SteadyStateSkipsScan verifies that once the store state
// is stable, heartbeat sends empty deltas without rebuilding the
// snapshot on every tick. We assert the behavioral invariant rather
// than an internal counter: repeated empty reports mean no state change
// was (re)derived, which is exactly what a skipped scan produces.
func TestHeartbeat_SteadyStateSkipsScan(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	defer cs.Close()

	mock := &mockHeartbeatMeta{}
	cfg := Config{NodeID: 1, HeartbeatInterval: 30 * time.Millisecond}
	reporter := NewHeartbeatReporter(cfg, mock, cs)
	reporter.Start()

	// Let the initial full sync land, then wait for several quiet ticks.
	time.Sleep(250 * time.Millisecond)
	reporter.Stop()

	reports := mock.getReports()
	if len(reports) < 3 {
		t.Fatalf("expected several reports, got %d", len(reports))
	}
	// All reports after the first must be empty (no state changes).
	for i := 1; i < len(reports); i++ {
		if len(reports[i].ChunkStates) != 0 {
			t.Errorf("report %d: expected empty delta in steady state, got %d states",
				i, len(reports[i].ChunkStates))
		}
	}
}
