package smoke

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// TestRaft_LeadershipTransfer verifies that leadership can be transferred
// to another node and the new leader can serve writes.
func TestRaft_LeadershipTransfer(t *testing.T) {
	if os.Getenv("NUFS_RUN_SMOKE") != "1" {
		t.Skip("set NUFS_RUN_SMOKE=1 to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cluster := startRaftSmokeCluster(t, 3)
	defer cluster.Stop()

	leader := cluster.WaitForLeader(t, ctx)
	t.Logf("initial leader: %s", leader.ID)

	// Create bucket and write data via leader
	store := leader.Store
	if err := store.CreateBucket(ctx, "transfer-test", metadata.PlacementPolicy{
		ReplicationFactor: 1, StorageTier: metadata.TierHot,
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, _ := store.GetBucket(ctx, "transfer-test")
	if _, err := store.CreateFile(ctx, bucket.RootInode, "key", 0644); err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	t.Logf("initial write via leader %s", leader.ID)

	// Transfer leadership to node 2
	targetID := "node-2"
	t.Logf("transferring leadership to %s", targetID)
	start := time.Now()
	if err := store.TransferLeadership(targetID); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}
	t.Logf("transfer completed in %v", time.Since(start).Round(time.Millisecond))

	// Wait for leadership to actually switch to the new node.
	// TransferLeadership returns when the transfer is initiated;
	// the actual leader change happens after an election timeout (~100ms).
	oldLeader := leader.ID
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	newLeader := leader
	for {
		select {
		case <-waitCtx.Done():
			t.Fatalf("leader did not change from %s within 5s", oldLeader)
		case <-ticker.C:
			for _, candidate := range cluster.Nodes {
				if candidate.Store != nil && candidate.Store.IsLeader() && candidate.ID != oldLeader {
					newLeader = candidate
					goto done
				}
			}
		}
	}
done:
	t.Logf("new leader: %s (transferred from %s)", newLeader.ID, oldLeader)
	if newLeader.ID != targetID {
		t.Fatalf("expected leader %s, got %s", targetID, newLeader.ID)
	}

	// Verify new leader can serve writes
	newStore := newLeader.Store
	if _, err := newStore.CreateFile(ctx, bucket.RootInode, "after", 0644); err != nil {
		t.Fatalf("CreateFile on new leader: %v", err)
	}
	t.Log("leadership transfer verified: new leader serves writes")
}
