package chunkstore

import (
	"context"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// TestEC_RangeRead_SingleShardWindow verifies that a V2.1 EC range read
// overlapping only one data shard completes without deadlock or ErrShardSize.
// Before the fix, the collector loop iterated len(Replicas) times but the
// wantWindow path skipped goroutines for non-overlapping shards, causing the
// collector to block forever on channel receives that never arrived.
// Additionally, partial data shards (< shardSize) and full parity shards
// (== shardSize) caused reedsolomon.ErrShardSize on decode.
func TestEC_RangeRead_SingleShardWindow(t *testing.T) {
	const (
		nNodes   = 3
		disksPer = 3
	)
	nodes, pb := buildV21GatewayCluster(t, nNodes, disksPer)
	nodesByID := map[uint64]*v21Node{}
	for i, nd := range nodes {
		nodesByID[uint64(i+1)] = nd
	}

	// Write a payload small enough that it fits in a single shard.
	cid := metadata.ChunkID(99001)
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i)
	}

	// Replicated write then convert to EC.
	coordStore := nodes[0].v2
	if err := coordStore.Write(cid, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	auth := metadata.NewECStore(pb)
	svc := datanode.NewECService(coordStore, auth)
	svc.SetCrossNode(1, func(nodeID uint64) (*datanode.Client, bool) {
		nd, ok := nodesByID[nodeID]
		if !ok {
			return nil, false
		}
		return datanode.NewClient(nd.srv.Addr()), true
	})
	svc.SetCandidateDisks(func() []metadata.ECDisk { return v21Topology(nNodes, disksPer) })
	st, err := svc.ConvertToEC(context.Background(), cid, 1)
	if err != nil {
		t.Fatalf("ConvertToEC: %v", err)
	}

	// Build V2.1 chunk metadata with ECStripeID — triggers wantWindow path.
	chunk := &metadata.ChunkMeta{
		ID:         cid,
		Size:       int32(len(payload)),
		State:      metadata.ChunkReady,
		ECStripeID: st.StripeID,
		ECGroup: &metadata.ECGroupInfo{
			GroupID:      st.StripeID,
			ProfileID:    metadata.DefaultECProfileID,
			DataShards:   metadata.ECDataShards,
			ParityShards: metadata.ECParityShards,
		},
	}
	for _, sh := range st.Shards {
		chunk.Replicas = append(chunk.Replicas, metadata.ReplicaInfo{
			NodeID:     metadata.NodeID(sh.NodeID),
			Addr:       nodesByID[sh.NodeID].srv.Addr(),
			State:      metadata.ReplicaReady,
			ShardIndex: sh.Index,
		})
	}

	cs := NewDatanodeChunkStore()
	defer cs.Close()

	// Range read of just the first 16 bytes. With K=6, shardSize ≈ 10.7 MiB,
	// so only shard 0 overlaps [0, 16). This triggers wantWindow where most
	// data shards are skipped — the exact scenario that caused the collector
	// deadlock and ErrShardSize before the fix.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got, err := cs.ReadChunkRange(ctx, chunk, 0, 16)
	if err != nil {
		// A context timeout means the collector deadlocked.
		t.Fatalf("ReadChunkRange deadlock or error: %v", err)
	}
	want := payload[:16]
	if len(got) != len(want) {
		t.Fatalf("length mismatch: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("data mismatch at byte %d: got %x, want %x", i, got[i], want[i])
		}
	}
}
