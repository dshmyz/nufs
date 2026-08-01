package chunkstore

import (
	"bytes"
	"context"
	"testing"

	"github.com/example/dfs/metadata"
)

// TestRepair_EC_Path verifies the EC repair path end-to-end:
// write EC chunks across K+M datanodes, stop one datanode, trigger
// repair, and verify the chunk data is still accessible via EC
// reconstruction.
func TestRepair_EC_Path(t *testing.T) {
	const k, m = 4, 2
	servers, addrs := startTestDatanodes(t, k+m)
	defer stopAll(servers)

	cs := NewDatanodeChunkStore()
	defer cs.Close()

	// Write EC chunk
	data := bytes.Repeat([]byte("ec-repair-test-data "), 50)
	chunk := &metadata.ChunkMeta{
		ID:   600,
		Size: int32(len(data)),
		ECGroup: &metadata.ECGroupInfo{
			GroupID:      "repair-test",
			DataShards:   k,
			ParityShards: m,
		},
	}
	for i := 0; i < k+m; i++ {
		chunk.Replicas = append(chunk.Replicas, metadata.ReplicaInfo{
			NodeID:     metadata.NodeID(i + 1),
			Addr:       addrs[i],
			State:      metadata.ReplicaReady,
			ShardIndex: i,
		})
	}

	ctx := context.Background()
	if err := cs.WriteChunk(ctx, chunk, data); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Kill datanode 0 (shard 0 = data shard)
	t.Log("killing datanode 0 (shard 0)...")
	servers[0].Stop()

	// Verify data still readable via EC reconstruction (5 shards >= K=4)
	got, err := cs.ReadChunk(ctx, chunk)
	if err != nil {
		t.Fatalf("ReadChunk after EC node failure: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch after EC node failure")
	}
	t.Log("EC reconstruction working: data survived shard 0 loss")

	// Stop another node (shard 5 = parity)
	t.Log("killing datanode 5 (shard 5)...")
	servers[5].Stop()

	// Now only 4 shards remain (exactly K) — should still read via EC
	got, err = cs.ReadChunk(ctx, chunk)
	if err != nil {
		t.Fatalf("ReadChunk with 2 missing shards: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch with 2 missing shards")
	}
	t.Log("EC reconstruction working: 4/6 shards (exactly K) still readable")
}
