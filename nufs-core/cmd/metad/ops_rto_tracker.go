package main

import (
	"sync/atomic"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ============================================================
// Leader-failover RTO tracker.
//
// metad_leader_failover_rto was previously a drill-only measurement: the
// failover script (scripts/soak/run-v21-leader-failover.sh) recorded RTO
// client-side (kill old leader -> first successful write served by the new
// leader) into REPORT.txt, but no exporter emitted it at runtime, so
// NUFSLeaderFailoverRTOExceeded could not be a deployed alert.
//
// This tracker derives a runtime RTO from the raft layer: while this node is
// a follower/candidate, it records the wall-clock time it last had contact
// with a leader (now - stats["last_contact"]). When this node then wins
// leadership, RTO = now - lastLeaderContact, i.e. the gap from the old
// leader's last heartbeat to this node's election win. That closely matches
// the client-side drill RTO (within a heartbeat interval).
//
// Only the current leader holds a non-zero value; on step-down the gauge
// resets to 0, so the RTO alert evaluates only on the node that just took
// over and a stale value on an old leader cannot false-fire. Precision is
// bounded by the poll interval (500ms) -- immaterial for a 15s budget.
//
// Initial elections (no prior leader, last_contact="never") are not failovers
// and produce RTO=0.
// ============================================================

// raftStatSource is the subset of *metadata.RaftNode the tracker reads, as an
// interface so the tick logic is unit-testable with a fake.
type raftStatSource interface {
	IsLeader() bool
	Stats() map[string]string
}

// leaderRTODecayAfter bounds how long the RTO gauge holds a failover's value
// before decaying to 0. Without it the wasLeader latch means one overtime
// failover pins nufs_leader_failover_rto_seconds above budget indefinitely —
// re-evaluating NUFSLeaderFailoverRTOExceeded every `for: 1m` window until the
// next step-down, so a single old breach keeps the alert stuck with no way to
// clear but another election. The window is deliberately much longer than the
// alert's `for` period, so a real breach still fires and stays visible for
// operator action, and only then clears automatically instead of sticking.
const leaderRTODecayAfter = 10 * time.Minute

type leaderRTOTracker struct {
	raft    raftStatSource
	metrics *metadata.Metrics

	// lastContactWall is the wall-clock unix-nano this node last had contact
	// with a leader (updated while following); 0 if it has never followed a
	// leader. Frozen while a candidate (no leader to hear from), so it stays
	// pinned at the old leader's last heartbeat until a new leader wins.
	lastContactWall atomic.Int64
	wasLeader       atomic.Bool
	// rtoSetAt is the wall-clock unix-nano the current RTO gauge value was
	// written. 0 when no RTO is being held. Used to decay a stale breach.
	rtoSetAt atomic.Int64
}

// tick performs one polling pass. Extracted from run for testing.
func (t *leaderRTOTracker) tick(now time.Time) {
	isLeader := t.raft.IsLeader()
	if !isLeader {
		// Track the wall-clock time of last leader contact (as a follower or
		// candidate). last_contact is the duration since the last heartbeat
		// from a leader; "never" means no leader was ever known.
		if d, ok := parseLastContact(t.raft.Stats()["last_contact"]); ok {
			t.lastContactWall.Store(now.Add(-d).UnixNano())
		}
		// Non-leaders report 0 so the alert only fires on the current leader.
		if t.wasLeader.Load() {
			t.metrics.LeaderFailoverRTO.Store(0)
			t.rtoSetAt.Store(0)
			t.wasLeader.Store(false)
		}
		return
	}
	// Leader: on the fresh transition into leadership, compute RTO from the
	// last known leader contact. Subsequent ticks while still leader no-op so
	// the gauge holds the failover's RTO — until it outlives leaderRTODecayAfter,
	// at which point it decays to 0 so an old breach doesn't pin the alert.
	if !t.wasLeader.Load() {
		if lc := t.lastContactWall.Load(); lc > 0 {
			if rto := now.UnixNano() - lc; rto > 0 {
				// Round UP to the next whole second rather than truncating: the
				// alert is `nufs_leader_failover_rto_seconds > 15`, and a floor
				// (rto/time.Second) would under-report a 15.0-15.99s failover as
				// "15" and silently miss the budget breach right at the boundary.
				t.metrics.LeaderFailoverRTO.Store((rto + int64(time.Second) - 1) / int64(time.Second))
				t.rtoSetAt.Store(now.UnixNano())
			}
		}
		t.wasLeader.Store(true)
		return
	}
	// Already leader holding a stored RTO: decay it once it has outlived
	// leaderRTODecayAfter so a stale breach clears itself (the next real
	// failover — a fresh !wasLeader transition — recomputes and repins it).
	if t.rtoSetAt.Load() > 0 && now.UnixNano()-t.rtoSetAt.Load() > int64(leaderRTODecayAfter) {
		t.metrics.LeaderFailoverRTO.Store(0)
		t.rtoSetAt.Store(0)
	}
}
}

func (t *leaderRTOTracker) run(stop <-chan struct{}) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			t.tick(time.Now())
		}
	}
}

// parseLastContact parses raft Stats()["last_contact"]. Per hashicorp/raft:
// "never" = no leader ever contacted; "0" = this node is the leader; otherwise
// a duration string like "1.234s". Returns ok=false for "never"/"0"/unparseable.
func parseLastContact(s string) (time.Duration, bool) {
	if s == "" || s == "never" || s == "0" {
		return 0, false
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, false
	}
	return d, true
}
