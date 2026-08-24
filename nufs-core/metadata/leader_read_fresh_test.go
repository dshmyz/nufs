package metadata

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestRaftWaitCatchUp pins the linearizability gate for leader-served reads:
// a freshly elected leader's FSM can lag committed entries by the apply-lag
// window (the same window that made chunk-allocation outcome-unknown retries
// necessary), so a leader must wait until its FSM has applied every entry
// committed before the read. Without the gate, a client 307-redirected to the
// new leader reads stale rows — the follower-read fix merely moved the window
// from "follower" to "new leader during catch-up".
//
// The fast path (already caught up) must return immediately with zero raft
// round trips; the slow path (FSM artificially lagged, standing in for the
// catch-up window) must barrier until the FSM is caught up; and a lagged
// follower must fail the call (only the leader can force the barrier).
func TestRaftWaitCatchUp(t *testing.T) {
	if testing.Short() {
		t.Skip("real raft integration test")
	}

	cluster := startRealRaftTestCluster(t, 3)
	defer cluster.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	leader := cluster.CreateBucketOnLeader(t, ctx, "fs", PlacementPolicy{ReplicationFactor: 1})
	bucket, err := leader.Store.GetBucket(ctx, "fs")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := leader.Store.CreateFile(ctx, bucket.RootInode, fmt.Sprintf("f%d.bin", i), 0o644); err != nil {
			t.Fatalf("create file %d: %v", i, err)
		}
	}

	node := leader.Store.raft
	if node == nil {
		t.Fatal("leader store must expose its raft node")
	}

	// Fast path: already caught up — no raft round trip.
	start := time.Now()
	if err := node.WaitCatchUp(ctx, 5*time.Second); err != nil {
		t.Fatalf("caught-up WaitCatchUp: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("fast-path WaitCatchUp took %v — it must return immediately when the FSM is caught up, not poll on every read", elapsed)
	}

	// Slow path without progress: a lagged FSM with nothing arriving must
	// time out (the poll loop is what enforces freshness — with no apply
	// progress there is nothing to wait for, so the gate must fail closed).
	node.fsm.snapshotMu.Lock()
	node.fsm.lastConsumedIndex = 0
	node.fsm.snapshotMu.Unlock()
	if node.CaughtUp() {
		t.Fatal("lagged FSM must report not caught up")
	}
	if err := node.WaitCatchUp(ctx, 300*time.Millisecond); err == nil {
		t.Fatal("WaitCatchUp on a lagged FSM with no apply progress must time out, not return nil")
	}

	// Slow path with progress: the FSM catches up as committed entries are
	// applied (the real post-failover window: the background apply loop
	// replays the prior leader's entries) — WaitCatchUp must return once the
	// FSM is caught up, and a subsequent read sees fresh data. The probe
	// entry is submitted directly (not through a store method) so it drives
	// the apply loop without itself blocking on the catch-up gate.
	node.fsm.snapshotMu.Lock()
	node.fsm.lastConsumedIndex = 0
	node.fsm.snapshotMu.Unlock()
	probe := &RaftLogEntry{Op: OpSet, Key: []byte("probe:catchup"), Value: []byte("1")}
	if future := node.raft.Apply(probe.Encode(), 5*time.Second); future.Error() != nil {
		t.Fatalf("submit probe entry: %v", future.Error())
	}
	if err := node.WaitCatchUp(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitCatchUp on lagged leader with apply progress: %v", err)
	}
	if !node.CaughtUp() {
		t.Fatal("after WaitCatchUp the FSM must be caught up")
	}

	// A lagged follower must fail: only the leader can force the barrier.
	for _, n := range cluster.Nodes {
		if n.ID == leader.ID {
			continue
		}
		if n.Store.raft == nil {
			t.Fatalf("node %s has no raft node", n.ID)
		}
		n.Store.raft.fsm.snapshotMu.Lock()
		n.Store.raft.fsm.lastConsumedIndex = 0
		n.Store.raft.fsm.snapshotMu.Unlock()
		if err := n.Store.raft.WaitCatchUp(ctx, time.Second); err == nil {
			t.Fatalf("WaitCatchUp on lagged follower %s must fail (only the leader can barrier)", n.ID)
		}
	}
}
