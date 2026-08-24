package metadata

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// TestAllocateChunks_GhostRefOnLateApply reproduces the harmful ordering the
// outcome-unknown retry loop (0ce26aa) can produce: attempt 1 commits to
// raft but its FSM apply lags past the 10s conditional wait; reconciliation
// sees "not committed" and retries; attempt 1's apply then lands first (the
// FSM applies in log-index order), attempt 2's precondition fails, and the
// next retry appends on top of attempt 1 — leaving BOTH chunk refs at the
// same offset in the inode, while only the retried chunk's replicas were
// ever written. The caller (V1 write path) completes a 200 PUT whose object
// reads the unwritten ghost first.
//
// Deterministic construction: a real single-node raft cluster whose FSM
// apply is blocked on the first data entry after the allocation submits, so
// the batch is truly committed but not applied when the 10s wait expires.
// The fix must reconcile against a caught-up FSM: "committed" is then a
// reliable verdict and the late-arriving attempt 1 is returned instead of a
// second append.
func TestAllocateChunks_GhostRefOnLateApply(t *testing.T) {
	if testing.Short() {
		t.Skip("real raft integration test")
	}

	cluster := startRealRaftTestCluster(t, 1)
	defer cluster.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	store := cluster.WaitForLeader(t, ctx).Store
	if err := store.RegisterNode(ctx, &NodeInfo{ID: 1, Addr: "node:9001", CapacityGB: 10, Tier: TierHot}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if err := store.CreateBucket(ctx, "fs", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "fs")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	file, err := store.CreateFile(ctx, bucket.RootInode, "f.bin", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	// Block the FSM apply of the first data entry after this point — the
	// allocation batch commits to raft but never reaches the FSM within the
	// conditional wait window.
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store.raft.fsm.applyDelayHook = func(*raft.Log) {
		once.Do(func() {
			close(entered)
			<-release
		})
	}

	type result struct {
		chunks []*ChunkMeta
		err    error
	}
	done := make(chan result, 1)
	go func() {
		chunks, err := store.AllocateChunksBatch(ctx, file.ID, []int64{0}, PlacementPolicy{ReplicationFactor: 1})
		done <- result{chunks, err}
	}()

	// Wait until attempt 1 is committed to raft but blocked in FSM apply.
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("allocation never reached the blocked FSM apply")
	}

	// Let the conditional wait (10s) expire as outcome-unknown, then release
	// the apply so attempt 1 lands — the exact late-apply ordering.
	time.Sleep(10*time.Second + 500*time.Millisecond)
	close(release)

	var res result
	select {
	case res = <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("AllocateChunksBatch never returned")
	}
	if res.err != nil {
		t.Fatalf("AllocateChunksBatch: %v", res.err)
	}

	// The returned chunks must be exactly what the inode references — a
	// ghost ref (a chunk the caller was never told about, from the
	// late-arriving attempt) breaks the read path: the object reads the
	// unwritten ghost first.
	raw, found, err := store.readChunkTombstoneRaw(fmt.Sprintf("%s%d", prefixInode, file.ID))
	if err != nil || !found {
		t.Fatalf("read inode: found=%v err=%v", found, err)
	}
	var inode InodeMeta
	if err := unmarshalValue(raw, &inode); err != nil {
		t.Fatalf("decode inode: %v", err)
	}
	returned := make(map[ChunkID]bool, len(res.chunks))
	for _, c := range res.chunks {
		returned[c.ID] = true
	}
	seen := make(map[int64]ChunkID)
	for _, ref := range inode.ChunkMap {
		if prev, dup := seen[ref.Offset]; dup {
			t.Fatalf("ghost ref: inode has TWO chunk refs at offset %d (%d, %d) — the late-arriving attempt was appended on top of instead of reconciled; returned chunks %v",
				ref.Offset, prev, ref.ID, res.chunks)
		}
		seen[ref.Offset] = ref.ID
		if !returned[ref.ID] {
			t.Fatalf("ghost ref: inode references chunk %d (offset %d) that AllocateChunksBatch never returned — its replicas were never written",
				ref.ID, ref.Offset)
		}
	}
	t.Logf("OK: inode chunk map matches returned chunks (%d refs, no ghost)", len(inode.ChunkMap))
}
