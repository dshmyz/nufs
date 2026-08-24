package metadata

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestNamespaceCreateWaitsForFSMCatchUp pins the mint-before-catch-up gate:
// applyNamespaceCreate must not mint inode IDs against a lagging FSM. The
// re-seed retry (a8d16d8) only shrinks the under-seeded cold-scan window —
// a retry can still run against an FSM that lags multiple committed entries
// and surface a spurious ErrEntryExists for a brand-new name. With the gate,
// the first mint waits until the FSM has applied every entry committed so
// far (the background apply loop provides the progress; zero cost when
// already caught up).
//
// Deterministic construction: a real single-node raft cluster whose FSM is
// artificially lagged (lastAppliedIndex=0) with NO apply progress. Before
// the fix, CreateFile succeeds against the lagging FSM; with the gate it
// must fail (the 500ms context deadline outlives the un-applied window)
// rather than mint against a stale view.
func TestNamespaceCreateWaitsForFSMCatchUp(t *testing.T) {
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
	for i := 0; i < 3; i++ {
		if _, err := leader.Store.CreateFile(ctx, bucket.RootInode, fmt.Sprintf("f%d.bin", i), 0o644); err != nil {
			t.Fatalf("create file %d: %v", i, err)
		}
	}

	// Lag the FSM with no apply progress on the horizon: the mint must not
	// proceed against this stale view.
	node := leader.Store.raft
	node.fsm.snapshotMu.Lock()
	node.fsm.lastAppliedIndex = 0
	node.fsm.snapshotMu.Unlock()
	if node.CaughtUp() {
		t.Fatal("lagged FSM must report not caught up")
	}

	bounded, cancelBounded := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelBounded()
	if _, err := leader.Store.CreateFile(bounded, bucket.RootInode, "post-failover.bin", 0o644); err == nil {
		t.Fatal("CreateFile must not succeed against a lagging FSM — the mint would be based on a stale view of committed inodes")
	}

	// With apply progress, the gate must let the create through once caught
	// up: drive the FSM forward with a directly-submitted entry (the stand-in
	// for the pending apply queue a real post-failover leader replays — the
	// background apply loop advances lastAppliedIndex) and confirm the create
	// succeeds on the now-caught-up FSM.
	node.fsm.snapshotMu.Lock()
	node.fsm.lastAppliedIndex = 0
	node.fsm.snapshotMu.Unlock()
	probe := &RaftLogEntry{Op: OpSet, Key: []byte("probe:catchup"), Value: []byte("1")}
	if future := node.raft.Apply(probe.Encode(), 5*time.Second); future.Error() != nil {
		t.Fatalf("submit probe entry: %v", future.Error())
	}
	if _, err := leader.Store.CreateFile(ctx, bucket.RootInode, "post-apply.bin", 0o644); err != nil {
		t.Fatalf("CreateFile after FSM catch-up: %v", err)
	}
	if !node.CaughtUp() {
		t.Fatal("FSM must be caught up after driven apply")
	}
}
