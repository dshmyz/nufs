package metadata

import (
	"context"
	"testing"
	"time"
)

// Decisive reproduction: does a REAL single-node Raft leader complete a
// conditional AllocateChunksBatch when the store is on-disk (as in prod)?
// Mirrors cmd/metad with --raft --raft-bootstrap (the default --raft=true path).
func TestReproRealRaftAllocateChunks(t *testing.T) {
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: t.TempDir(), NodeID: 1})
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer store.Close()

	node, err := NewRaftNode(store, RaftNodeConfig{
		NodeID:             "repro",
		BindAddr:           unusedRaftAddress(t),
		RaftDir:            t.TempDir(),
		Bootstrap:          true,
		HeartbeatTimeout:   1 * time.Second,  // cmd/metad default
		ElectionTimeout:    1 * time.Second,  // cmd/metad default
		LeaderLeaseTimeout: 500 * time.Millisecond,
		SnapshotThreshold:  8192,
		SnapshotInterval:   2 * time.Minute,
		TrailingLogs:       10240,
	})
	if err != nil {
		t.Fatalf("raft: %v", err)
	}
	defer func() {
		_ = node.Shutdown()
		store.raft = nil
	}()
	store.SetRaftNode(node)

	if ld := waitForLeader([]*RaftNode{node}, 5*time.Second); ld == nil {
		t.Fatal("no raft leader")
	}
	t.Log("raft leader elected")

	ctx := context.Background()
	if err := store.CreateBucket(ctx, "repro-bucket", PlacementPolicy{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	b, err := store.GetBucket(ctx, "repro-bucket")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	file, err := store.CreateFile(ctx, b.RootInode, "f.bin", 0o644)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	fileID := file.ID

	// Register 3 candidate nodes so placement for EC 6+3 (RF=9) can spread 9
	// shards across them (3 per node). Mirrors the harness's ≥3 online nodes.
	for i := NodeID(1); i <= 3; i++ {
		if err := store.RegisterNode(ctx, &NodeInfo{
			ID:         i,
			Zone:       "z",
			CapacityGB: 1000,
			State:      NodeOnline,
		}); err != nil {
			t.Fatalf("register node %d: %v", i, err)
		}
	}

	// Drive a real allocation to completion with a bounded timeout.
	done := make(chan error, 1)
	go func() {
		_, err := store.AllocateChunksBatch(ctx, fileID, []int64{0}, PlacementPolicy{
			ReplicationFactor: 9,
			ECConfig:          &ECConfig{DataShards: 6, ParityShards: 3},
		})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AllocateChunksBatch error: %v", err)
		}
		t.Log("AllocateChunksBatch SUCCEEDED over real raft")
	case <-time.After(15 * time.Second):
		t.Fatal("AllocateChunksBatch hung on real raft (never committed)")
	}
}
