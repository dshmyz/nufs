package chunkstore
import (
	"bytes"
	"context"
	"testing"
	"time"
	
	"github.com/dshmyz/nufs/nufs-core/metadata"
)
// TestEC_EndToEnd_PlaceWriteRead verifies the complete EC data path:
// K+M datanodes online → ChunkMeta built with ECGroup →
// ChunkStore.WriteChunk (EC encode + TCP fan-out) →
// ChunkStore.ReadChunk (TCP fetch + EC decode).
func TestEC_EndToEnd_PlaceWriteRead(t *testing.T) {
	const k, m = 4, 2
	servers, addrs := startTestDatanodes(t, k+m)
	defer stopAll(servers)
	data := []byte("end-to-end erasure coding through full stack")
	chunk := &metadata.ChunkMeta{
		ID:   1000,
		Size: int32(len(data)),
		ECGroup: &metadata.ECGroupInfo{
			GroupID:      "e2e-test",
			DataShards:   k,
			ParityShards: m,
		},
		Replicas: ecReplicasWithAddrs(addrs, k, m),
	}
	cs := NewDatanodeChunkStore()
	defer cs.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Write: EC encode → fan-out to K+M datanodes.
	if err := cs.WriteChunk(ctx, chunk, data); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	// Read: fetch from datanodes → EC decode.
	got, err := cs.ReadChunk(ctx, chunk)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch: got %d bytes, want %d", len(got), len(data))
	}
}
// TestEC_EndToEnd_WithNodeFailure tests the full EC path with a real
// datanode failure: write → stop one datanode → read still succeeds
// via EC reconstruction from remaining K shards.
func TestEC_EndToEnd_WithNodeFailure(t *testing.T) {
	const k, m = 4, 2
	servers, addrs := startTestDatanodes(t, k+m)
	defer stopAll(servers)
	data := bytes.Repeat([]byte("NODE-FAILURE-E2E "), 30)
	chunk := &metadata.ChunkMeta{
		ID:   1100,
		Size: int32(len(data)),
		ECGroup: &metadata.ECGroupInfo{
			GroupID:      "fail-test",
			DataShards:   k,
			ParityShards: m,
		},
		Replicas: ecReplicasWithAddrs(addrs, k, m),
	}
	cs := NewDatanodeChunkStore()
	defer cs.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := cs.WriteChunk(ctx, chunk, data); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	// Hard-stop the node holding shard 0 (a data shard).
	stopStart := time.Now()
	servers[0].Stop()
	if d := time.Since(stopStart); d > 2*time.Second {
		t.Fatalf("Stop took %v", d)
	}
	// Read must succeed: 5 shards >= K=4 → EC reconstructs shard 0.
	got, err := cs.ReadChunk(ctx, chunk)
	if err != nil {
		t.Fatalf("ReadChunk after node failure: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch after reconstruction")
	}
}
// ecReplicasWithAddrs builds a K+M replica list with shard indices.
func ecReplicasWithAddrs(addrs []string, k, m int) []metadata.ReplicaInfo {
	reps := make([]metadata.ReplicaInfo, k+m)
	for i := 0; i < k+m; i++ {
		reps[i] = metadata.ReplicaInfo{
			NodeID:     metadata.NodeID(i + 1),
			Addr:       addrs[i],
			State:      metadata.ReplicaReady,
			ShardIndex: i,
		}
	}
	return reps
}
