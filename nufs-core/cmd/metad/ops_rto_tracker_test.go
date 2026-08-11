package main

import (
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

type fakeRaftStats struct {
	leader bool
	stats  map[string]string
}

func (f *fakeRaftStats) IsLeader() bool           { return f.leader }
func (f *fakeRaftStats) Stats() map[string]string { return f.stats }

// TestLeaderRTOTracker_FailoverMeasuresFromLastContact: a follower that last
// heard from its leader 2s ago, then wins leadership, should record an RTO
// close to 2s (the gap from the old leader's last heartbeat to winning).
func TestLeaderRTOTracker_FailoverMeasuresFromLastContact(t *testing.T) {
	m := metadata.NewMetrics()
	tr := &leaderRTOTracker{raft: &fakeRaftStats{}, metrics: m}

	// Follower: last leader contact was 2s ago.
	t0 := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	tr.raft = &fakeRaftStats{leader: false, stats: map[string]string{"last_contact": "2s"}}
	tr.tick(t0)
	// lastContactWall should be t0 - 2s.
	if got := tr.lastContactWall.Load(); got != t0.Add(-2*time.Second).UnixNano() {
		t.Errorf("lastContactWall = %d, want %d", got, t0.Add(-2*time.Second).UnixNano())
	}

	// 3s later (so 5s after the old leader's last contact), this node wins.
	t1 := t0.Add(3 * time.Second)
	tr.raft = &fakeRaftStats{leader: true, stats: map[string]string{"last_contact": "0"}}
	tr.tick(t1)
	rto := m.LeaderFailoverRTO.Load()
	// RTO = t1 - (t0 - 2s) = 5s.
	if rto != 5 {
		t.Errorf("RTO = %d, want 5", rto)
	}

	// Subsequent ticks while still leader hold the value (no recompute).
	tr.tick(t1.Add(time.Second))
	if m.LeaderFailoverRTO.Load() != 5 {
		t.Errorf("RTO changed on stable-leader tick = %d, want 5", m.LeaderFailoverRTO.Load())
	}

	// Step down -> gauge resets to 0 so the alert only fires on the leader.
	tr.raft = &fakeRaftStats{leader: false, stats: map[string]string{"last_contact": "0.5s"}}
	tr.tick(t1.Add(2 * time.Second))
	if m.LeaderFailoverRTO.Load() != 0 {
		t.Errorf("RTO after step-down = %d, want 0", m.LeaderFailoverRTO.Load())
	}
}

// TestLeaderRTOTracker_RoundsUpRTOAboveBoundary: a failover just over an
// integer-second budget boundary must report the ceiling, not the floor, so
// the `nufs_leader_failover_rto_seconds > 15` alert cannot miss a 15.0-15.99s
// failover by truncating it to "15". A 15.5s RTO must export 16.
func TestLeaderRTOTracker_RoundsUpRTOAboveBoundary(t *testing.T) {
	m := metadata.NewMetrics()
	tr := &leaderRTOTracker{raft: &fakeRaftStats{}, metrics: m}

	t0 := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	// Follower last heard from the leader ~0.5s before t0.
	tr.raft = &fakeRaftStats{leader: false, stats: map[string]string{"last_contact": "500ms"}}
	tr.tick(t0)
	if got := tr.lastContactWall.Load(); got != t0.Add(-500*time.Millisecond).UnixNano() {
		t.Fatalf("lastContactWall = %d, want %d", got, t0.Add(-500*time.Millisecond).UnixNano())
	}

	// 15s later: RTO = t1 - (t0 - 500ms) = 15.5s. Floor would report 15 (missing
	// the >15 alert); ceil must report 16.
	t1 := t0.Add(15 * time.Second)
	tr.raft = &fakeRaftStats{leader: true, stats: map[string]string{"last_contact": "0"}}
	tr.tick(t1)
	if got := m.LeaderFailoverRTO.Load(); got != 16 {
		t.Errorf("RTO for 15.5s failover = %d, want 16 (ceil, not floor 15)", got)
	}
}

// TestLeaderRTOTracker_InitialElectionNoRTO: winning leadership without
// having followed a leader (fresh start) is not a failover -> RTO stays 0.
func TestLeaderRTOTracker_InitialElectionNoRTO(t *testing.T) {
	m := metadata.NewMetrics()
	tr := &leaderRTOTracker{raft: &fakeRaftStats{leader: true, stats: map[string]string{"last_contact": "0"}}, metrics: m}
	tr.tick(time.Now())
	if m.LeaderFailoverRTO.Load() != 0 {
		t.Errorf("initial-election RTO = %d, want 0", m.LeaderFailoverRTO.Load())
	}
}

// TestLeaderRTOTracker_DecaysStaleRTO: a failover's RTO must hold long enough
// for the alert to fire (its `for` period is 1m) but then decay to 0 once it
// outlives leaderRTODecayAfter, so an old breach doesn't pin the gauge above
// budget indefinitely (re-alerting every window until the next election). A
// later real failover (step-down then re-election) recomputes a fresh RTO.
func TestLeaderRTOTracker_DecaysStaleRTO(t *testing.T) {
	m := metadata.NewMetrics()
	tr := &leaderRTOTracker{raft: &fakeRaftStats{}, metrics: m}

	t0 := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	// Follower last heard from the leader 20s ago; win leadership 1s later ->
	// RTO = 21s (breaches the 15s budget).
	tr.raft = &fakeRaftStats{leader: false, stats: map[string]string{"last_contact": "20s"}}
	tr.tick(t0)
	tr.raft = &fakeRaftStats{leader: true, stats: map[string]string{"last_contact": "0"}}
	t1 := t0.Add(time.Second)
	tr.tick(t1)
	if got := m.LeaderFailoverRTO.Load(); got != 21 {
		t.Fatalf("RTO after 21s failover = %d, want 21", got)
	}

	// A tick shortly after (still within the alert window) holds the value so
	// the alert can stay FIRING.
	tr.tick(t1.Add(time.Minute))
	if got := m.LeaderFailoverRTO.Load(); got != 21 {
		t.Fatalf("RTO before decay = %d, want 21 (should hold for the alert window)", got)
	}

	// Once past leaderRTODecayAfter, the stale breach decays to 0 instead of
	// pinning the alert forever.
	tr.tick(t1.Add(leaderRTODecayAfter + time.Minute))
	if got := m.LeaderFailoverRTO.Load(); got != 0 {
		t.Errorf("RTO after decay window = %d, want 0 (stale breach should clear)", got)
	}

	// A fresh failover (step down, then re-elect after another 25s gap)
	// recomputes a new RTO rather than staying cleared.
	tr.raft = &fakeRaftStats{leader: false, stats: map[string]string{"last_contact": "25s"}}
	tr.tick(t1.Add(leaderRTODecayAfter + 2*time.Minute))
	tr.raft = &fakeRaftStats{leader: true, stats: map[string]string{"last_contact": "0"}}
	t2 := t1.Add(leaderRTODecayAfter + 2*time.Minute + time.Second)
	tr.tick(t2)
	if got := m.LeaderFailoverRTO.Load(); got != 26 {
		t.Errorf("RTO after re-failover = %d, want 26", got)
	}
}

func TestParseLastContact(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"1.234s", 1234 * time.Millisecond, true},
		{"0s", 0, true},     // follower just heard from leader (duration rounds to 0)
		{"0", 0, false},     // leader (raft emits the literal "0")
		{"never", 0, false}, // never contacted
		{"", 0, false},
		{"bogus", 0, false},
	}
	for _, c := range cases {
		got, ok := parseLastContact(c.in)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("parseLastContact(%q) = (%v, %v), want (%v, %v)", c.in, got, ok, c.want, c.ok)
		}
	}
}
