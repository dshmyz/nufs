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

// TestLeaderRTOTracker_InitialElectionNoRTO: winning leadership without ever
// having followed a leader (fresh start) is not a failover -> RTO stays 0.
func TestLeaderRTOTracker_InitialElectionNoRTO(t *testing.T) {
	m := metadata.NewMetrics()
	tr := &leaderRTOTracker{raft: &fakeRaftStats{leader: true, stats: map[string]string{"last_contact": "0"}}, metrics: m}
	tr.tick(time.Now())
	if m.LeaderFailoverRTO.Load() != 0 {
		t.Errorf("initial-election RTO = %d, want 0", m.LeaderFailoverRTO.Load())
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
