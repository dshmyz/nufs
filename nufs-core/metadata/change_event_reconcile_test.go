package metadata

import (
	"context"
	"testing"
)

// newTestCluster registers two online nodes and an RF=2 placement-group
// bucket with one file whose chunk is allocated to both nodes — the shape a
// real V2.1 multi-node deployment presents to the change-journal reconciler.
func newTestCluster(t *testing.T) (*PebbleStore, NodeID, NodeID, *ChunkMeta) {
	t.Helper()
	s := newTestPebbleStore(t)
	ctx := context.Background()

	var n1, n2 NodeID
	for i, id := range []NodeID{1, 2} {
		if err := s.RegisterNode(ctx, &NodeInfo{
			ID:   id,
			Addr: "127.0.0.1:7000",
			Tier: TierHot,
		}); err != nil {
			t.Fatalf("RegisterNode %d: %v", id, err)
		}
		if i == 0 {
			n1 = id
		} else {
			n2 = id
		}
	}
	if err := s.CreateBucket(ctx, "reconcile", PlacementPolicy{
		ID:                "default",
		ReplicationFactor: 2,
		TopologySpread:    SpreadNode,
		StorageTier:       TierHot,
		PlacementGroups:   true,
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := s.GetBucket(ctx, "reconcile")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := s.CreateFile(ctx, bucket.RootInode, "obj.bin", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	alloc, err := s.AllocateChunksBatch(ctx, inode.ID, []int64{0}, bucket.Policy)
	if err != nil || len(alloc) != 1 {
		t.Fatalf("AllocateChunksBatch: alloc=%d err=%v", len(alloc), err)
	}
	// Both replicas are durably present and ready (production heartbeat marks
	// them Ready after the fan-out lands).
	if err := s.ReportChunkState(ctx, n1, map[ChunkID]ReplicaState{alloc[0].ID: ReplicaReady}); err != nil {
		t.Fatalf("ReportChunkState n1: %v", err)
	}
	if err := s.ReportChunkState(ctx, n2, map[ChunkID]ReplicaState{alloc[0].ID: ReplicaReady}); err != nil {
		t.Fatalf("ReportChunkState n2: %v", err)
	}
	return s, n1, n2, alloc[0]
}

// TestReconcileChangeEvents_CorruptMarkFailedAndRepair proves that a corrupt
// change-journal event shipped on a node's heartbeat makes the metadata
// authority mark that node's replica of the chunk ReplicaFailed and enqueue a
// repair — the reconciliation the flat ChunkStates delta misses (a corrupt but
// still-present extent looks "present").
func TestReconcileChangeEvents_CorruptMarkFailedAndRepair(t *testing.T) {
	s, n1, _, chunk := newTestCluster(t)
	ctx := context.Background()

	// Sanity: chunk is fully replicated (both replicas ready) before the event.
	cur, err := s.GetChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if len(cur.Replicas) != 2 {
		t.Fatalf("want 2 replicas, got %d", len(cur.Replicas))
	}

	evs := []ChangeEventRecord{{
		Seq:      1,
		Kind:     ChangeCorrupt,
		ExtentID: uint64(chunk.ID),
	}}
	ack, err := s.ReconcileChangeEvents(ctx, n1, evs)
	if err != nil {
		t.Fatalf("ReconcileChangeEvents: %v", err)
	}
	if ack != 1 {
		t.Fatalf("ack=%d, want 1", ack)
	}

	// n1's replica is now failed and a repair task exists for the chunk.
	cur, err = s.GetChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("GetChunk after: %v", err)
	}
	var n1State ReplicaState
	for i := range cur.Replicas {
		if cur.Replicas[i].NodeID == n1 {
			n1State = cur.Replicas[i].State
		}
	}
	if n1State != ReplicaFailed {
		t.Fatalf("node %d state=%v, want ReplicaFailed", n1, n1State)
	}
	tasks, err := s.GetRepairQueue(ctx)
	if err != nil {
		t.Fatalf("GetRepairQueue: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ChunkID != chunk.ID {
		t.Fatalf("want 1 repair task for chunk %d, got %+v", chunk.ID, tasks)
	}
}

// TestReconcileChangeEvents_DiskLostSweepsNodeReplicas proves a disk/segment
// loss event conservatively marks every chunk this node reports as failed and
// triggers repairs.
func TestReconcileChangeEvents_DiskLostSweepsNodeReplicas(t *testing.T) {
	s, n1, _, _ := newTestCluster(t)
	ctx := context.Background()

	evs := []ChangeEventRecord{{
		Seq:  1,
		Kind: ChangeDiskLost,
	}}
	if _, err := s.ReconcileChangeEvents(ctx, n1, evs); err != nil {
		t.Fatalf("ReconcileChangeEvents: %v", err)
	}
	tasks, err := s.GetRepairQueue(ctx)
	if err != nil {
		t.Fatalf("GetRepairQueue: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatalf("no repair tasks triggered after storage loss")
	}
}

// TestAckChangeEvents_ReturnsReconciledWatermark proves the dedicated ack RPC
// reports the persisted reconciled watermark so the node advances its local
// journal Ack only past actually-consumed sequences.
func TestAckChangeEvents_ReturnsReconciledWatermark(t *testing.T) {
	s, n1, _, chunk := newTestCluster(t)
	ctx := context.Background()

	// No events reconciled yet → watermark 0.
	ack, err := s.AckChangeEvents(ctx, n1, 0)
	if err != nil {
		t.Fatalf("AckChangeEvents: %v", err)
	}
	if ack != 0 {
		t.Fatalf("initial ack=%d, want 0", ack)
	}

	if _, err := s.ReconcileChangeEvents(ctx, n1, []ChangeEventRecord{{
		Seq: 7, Kind: ChangeCorrupt, ExtentID: uint64(chunk.ID),
	}}); err != nil {
		t.Fatalf("ReconcileChangeEvents: %v", err)
	}

	ack, err = s.AckChangeEvents(ctx, n1, 7)
	if err != nil {
		t.Fatalf("AckChangeEvents after: %v", err)
	}
	if ack != 7 {
		t.Fatalf("ack=%d, want 7 (reconciled watermark)", ack)
	}
}
