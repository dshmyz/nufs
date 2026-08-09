package chunkstore

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// startTestDatanodes spins up n in-process datanode TCP servers on
// ephemeral ports, each backed by its own on-disk ChunkStore under a
// temp dir. Returns the servers (so individual nodes can be stopped to
// simulate failure) and their listen addresses.
func startTestDatanodes(t *testing.T, n int) ([]*datanode.Server, []string) {
	t.Helper()
	servers := make([]*datanode.Server, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		store, err := datanode.NewChunkStore(t.TempDir(), 8, 8, nil)
		if err != nil {
			t.Fatalf("NewChunkStore: %v", err)
		}
		if err := store.WaitForScan(); err != nil {
			t.Fatalf("WaitForScan: %v", err)
		}
		cfg := datanode.DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.NodeID = metadata.NodeID(i + 1)
		// Short request timeout so idle pooled connections are reaped
		// quickly; this lets Server.Stop() return promptly instead of
		// blocking for the 30s default. Local ops complete in <1ms so
		// 200ms is ample headroom.
		cfg.RequestTimeout = 200 * time.Millisecond
		srv := datanode.NewServer(cfg, store)
		if err := srv.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		servers[i] = srv
		addrs[i] = srv.Addr()
	}
	return servers, addrs
}

// stopAll stops every server. Safe to call on already-stopped servers.
func stopAll(servers []*datanode.Server) {
	for _, s := range servers {
		s.Stop()
	}
}

// ecReplicas builds a K+M replica set: replica i holds shard i and
// lives at addrs[i]. All replicas start in the Ready state.
func ecReplicas(addrs []string) []metadata.ReplicaInfo {
	reps := make([]metadata.ReplicaInfo, len(addrs))
	for i, addr := range addrs {
		reps[i] = metadata.ReplicaInfo{
			NodeID:     metadata.NodeID(i + 1),
			Addr:       addr,
			State:      metadata.ReplicaReady,
			ShardIndex: i,
		}
	}
	return reps
}

// ecChunk builds a ChunkMeta for an erasure-coded chunk of the given
// dimensions, with replicas spread across addrs.
func ecChunk(id metadata.ChunkID, size int, addrs []string, k, m int) *metadata.ChunkMeta {
	return &metadata.ChunkMeta{
		ID:       id,
		Size:     int32(size),
		Replicas: ecReplicas(addrs),
		ECGroup: &metadata.ECGroupInfo{
			GroupID:      "test-group",
			DataShards:   k,
			ParityShards: m,
		},
	}
}

// unreachableAddr is a TCP address with no listener, so dials fail fast
// with connection refused. Used to simulate a down node.
const unreachableAddr = "127.0.0.1:1"

// TestEC_WriteReadRoundTrip is the basic EC data-path smoke test: encode
// -> fan-out write -> concurrent read -> decode must return the original
// payload exactly, including the padding/trim path (payload length is not
// a multiple of K).
func TestEC_WriteReadRoundTrip(t *testing.T) {
	const k, m = 4, 2
	servers, addrs := startTestDatanodes(t, k+m)
	defer stopAll(servers)

	cs := NewDatanodeChunkStore()
	defer cs.Close()
	// 54 bytes is not a multiple of K=4, so the encoder pads to 56 and
	// the decoder must trim back to 54.
	data := bytes.Repeat([]byte("abcdef"), 9)
	chunk := ecChunk(100, len(data), addrs, k, m)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := cs.WriteChunk(ctx, chunk, data); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	got, err := cs.ReadChunk(ctx, chunk)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("payload mismatch: got %d bytes %q, want %d bytes %q", len(got), got, len(data), data)
	}
}

// TestEC_ReadReconstructsMissingDataShard verifies the core EC durability
// guarantee: with K=4 data + M=2 parity, losing one DATA shard and one
// parity shard still allows a full read, because the missing data shard
// is reconstructed from parity.
//
// Node failure at read time is simulated by pointing the down shard's
// replica at a closed port (connection refused) rather than stopping the
// server - this exercises the same failed-read path in readECChunk
// without racing the connection pool's idle reaping.
func TestEC_ReadReconstructsMissingDataShard(t *testing.T) {
	const k, m = 4, 2
	servers, addrs := startTestDatanodes(t, k+m)
	defer stopAll(servers)

	cs := NewDatanodeChunkStore()
	defer cs.Close()
	data := bytes.Repeat([]byte("X"), 200) // multiple of K
	chunk := ecChunk(200, len(data), addrs, k, m)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Write all 6 shards to healthy nodes.
	if err := cs.WriteChunk(ctx, chunk, data); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Simulate shard 0 (data) and shard 5 (parity) being down at read
	// time. 4 of 6 shards remain (= K), so decode must reconstruct the
	// missing data shard from parity.
	readChunk := withUnreachableShards(chunk, 0, 5)
	got, err := cs.ReadChunk(ctx, readChunk)
	if err != nil {
		t.Fatalf("ReadChunk after losing 1 data + 1 parity shard: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("payload mismatch after reconstruction: got %d bytes, want %d", len(got), len(data))
	}
}

// TestEC_ReadToleratesParityLoss verifies that losing only parity shards
// (up to M) never affects readability.
func TestEC_ReadToleratesParityLoss(t *testing.T) {
	const k, m = 4, 2
	servers, addrs := startTestDatanodes(t, k+m)
	defer stopAll(servers)

	cs := NewDatanodeChunkStore()
	defer cs.Close()
	data := []byte("parity shards are redundant by definition")
	chunk := ecChunk(300, len(data), addrs, k, m)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := cs.WriteChunk(ctx, chunk, data); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Both parity shards (indices 4 and 5) are down at read time.
	// All K data shards remain, so decode succeeds trivially.
	readChunk := withUnreachableShards(chunk, 4, 5)
	got, err := cs.ReadChunk(ctx, readChunk)
	if err != nil {
		t.Fatalf("ReadChunk after losing both parity shards: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("payload mismatch: got %q, want %q", got, data)
	}
}

// withUnreachableShards returns a shallow copy of chunk with the replicas
// at the given shard indices repointed at a closed port, simulating nodes
// that are down at read time.
func withUnreachableShards(chunk *metadata.ChunkMeta, shards ...int) *metadata.ChunkMeta {
	reps := make([]metadata.ReplicaInfo, len(chunk.Replicas))
	copy(reps, chunk.Replicas)
	down := make(map[int]bool, len(shards))
	for _, s := range shards {
		down[s] = true
	}
	for i := range reps {
		if down[reps[i].ShardIndex] {
			reps[i].Addr = unreachableAddr
		}
	}
	cp := *chunk
	cp.Replicas = reps
	return &cp
}

// TestEC_WriteQuorumFailure verifies that a write is rejected when fewer
// than K shards can be persisted. With K=4 we make only 3 shards
// reachable; the other 3 point at a closed port, so only 3 writes succeed
// (< quorum 4) and WriteChunk must error.
func TestEC_WriteQuorumFailure(t *testing.T) {
	const k, m = 4, 2
	servers, reachable := startTestDatanodes(t, k-1) // 3 reachable nodes
	defer stopAll(servers)

	cs := NewDatanodeChunkStore()
	defer cs.Close()
	data := []byte("this write should not reach quorum")

	// 6 replicas: 3 reachable (shards 0..2) + 3 unreachable (shards 3..5).
	reps := make([]metadata.ReplicaInfo, k+m)
	for i := 0; i < k+m; i++ {
		addr := unreachableAddr
		if i < len(reachable) {
			addr = reachable[i]
		}
		reps[i] = metadata.ReplicaInfo{
			NodeID:     metadata.NodeID(i + 1),
			Addr:       addr,
			State:      metadata.ReplicaReady,
			ShardIndex: i,
		}
	}
	chunk := &metadata.ChunkMeta{
		ID:       400,
		Size:     int32(len(data)),
		Replicas: reps,
		ECGroup: &metadata.ECGroupInfo{
			GroupID:      "quorum-fail",
			DataShards:   k,
			ParityShards: m,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := cs.WriteChunk(ctx, chunk, data)
	if err == nil {
		t.Fatal("expected WriteChunk to fail with insufficient quorum, got nil")
	}
}

// TestEC_RangeRead verifies that ReadChunkRange returns the correct
// sub-slice of the decoded payload.
func TestEC_RangeRead(t *testing.T) {
	const k, m = 4, 2
	servers, addrs := startTestDatanodes(t, k+m)
	defer stopAll(servers)

	cs := NewDatanodeChunkStore()
	defer cs.Close()
	// 40 bytes, multiple of K.
	data := bytes.Repeat([]byte("0123456789"), 4)
	chunk := ecChunk(500, len(data), addrs, k, m)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := cs.WriteChunk(ctx, chunk, data); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Read [10, 30) -> 20 bytes.
	got, err := cs.ReadChunkRange(ctx, chunk, 10, 20)
	if err != nil {
		t.Fatalf("ReadChunkRange: %v", err)
	}
	want := data[10:30]
	if !bytes.Equal(got, want) {
		t.Fatalf("range mismatch: got %q, want %q", got, want)
	}

	// Full read via offset=0, length=0.
	gotFull, err := cs.ReadChunkRange(ctx, chunk, 0, 0)
	if err != nil {
		t.Fatalf("ReadChunkRange full: %v", err)
	}
	if !bytes.Equal(gotFull, data) {
		t.Fatalf("full read mismatch: got %d bytes, want %d", len(gotFull), len(data))
	}
}

// TestEC_ReadAfterRealNodeFailure stops an actual datanode server after
// the write completes and verifies the read still succeeds via EC
// reconstruction. This exercises the full connection-lifecycle chain:
//   - Server.Stop() returns promptly (graceful shutdown closes the
//     pooled connection instead of blocking on reqTimeout).
//   - The dead pooled connection to the stopped node is detected on use;
//     the client redials, that dial fails fast (connection refused), and
//     the shard read is treated as failed.
//   - readECChunk collects the surviving K+ shards and decodes, with the
//     missing data shard reconstructed from parity.
//
// Losing one data shard (shard 0) leaves K+1 shards, so decode must
// reconstruct it.
func TestEC_ReadAfterRealNodeFailure(t *testing.T) {
	const k, m = 4, 2
	servers, addrs := startTestDatanodes(t, k+m)
	defer stopAll(servers)

	cs := NewDatanodeChunkStore()
	defer cs.Close()
	data := bytes.Repeat([]byte("R"), 256) // multiple of K
	chunk := ecChunk(600, len(data), addrs, k, m)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := cs.WriteChunk(ctx, chunk, data); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// Hard-stop the node holding data shard 0. Stop() must return quickly
	// (graceful shutdown), and the read must tolerate the lost shard.
	stopStart := time.Now()
	servers[0].Stop()
	if d := time.Since(stopStart); d > 2*time.Second {
		t.Fatalf("Stop took %v, want < 2s", d)
	}

	got, err := cs.ReadChunk(ctx, chunk)
	if err != nil {
		t.Fatalf("ReadChunk after real node failure: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("payload mismatch after node failure: got %d bytes, want %d", len(got), len(data))
	}
}
