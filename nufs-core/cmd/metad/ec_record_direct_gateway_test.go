package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/example/dfs/chunkstore"
	"github.com/example/dfs/metadata"
)

// TestGatewayDirectECWrite_ReadBack is the Program 10 capstone: the gateway's
// production write path — a real DatanodeChunkStore whose ECWriteAuthority is
// the metad HTTPClient — encodes a 6+3 chunk and pushes each shard *directly*
// to its owning node's shard store (no intermediate replica), then serves a
// byte-exact read back through the same store. It mirrors V1 direct-write
// semantics but lands shards in the V2.1 shard store (§14) and lifts the chunk
// into durable EC (ECStripeID) so read/self-heal/orphan-GC recognize it.
func TestGatewayDirectECWrite_ReadBack(t *testing.T) {
	const (
		nNodes   = 6 // PlacementEngine places 9 EC shards across >=5 nodes (§14)
		disksPer = 3
	)
	cl := buildProdCluster(t, nNodes, disksPer)
	meta := cl.nodes[0].meta
	pb := cl.pb

	// Allocate a 6+3 ECConfig chunk through the authority — the exact
	// production allocation a gateway PUT to an ECConfig bucket produces: 9
	// replicas, each carrying the owning node's live Addr + ShardIndex + ECGroup.
	ctx := context.Background()
	if err := pb.CreateBucket(ctx, "gw-direct-ec", metadata.PlacementPolicy{
		ReplicationFactor: 1,
		ECConfig:          &metadata.ECConfig{DataShards: 6, ParityShards: 3},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := pb.GetBucket(ctx, "gw-direct-ec")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := pb.CreateFile(ctx, bucket.RootInode, "obj", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	alloc, err := pb.AllocateChunk(ctx, inode.ID, 0, metadata.PlacementPolicy{
		ReplicationFactor: 1,
		ECConfig:          &metadata.ECConfig{DataShards: 6, ParityShards: 3},
	})
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	if alloc.ECGroup == nil || len(alloc.Replicas) != 9 {
		t.Fatalf("allocation lacks 6+3 EC layout: group=%v replicas=%d", alloc.ECGroup, len(alloc.Replicas))
	}

	// The gateway store: a real DatanodeChunkStore whose direct-EC authority is
	// the metad HTTPClient (wired in gateway/s3/handler.go via SetECWriteAuthority).
	cs := chunkstore.NewDatanodeChunkStore()
	cs.SetECWriteAuthority(meta)
	defer cs.Close()

	// Payload length a multiple of 6 so the EC pad rounds back to the exact
	// original (read path trims to paddedLen <= MaxChunkSize).
	payload := make([]byte, 0, 720)
	for i := 0; i < 720; i++ {
		payload = append(payload, byte(i*7+1))
	}

	if err := cs.WriteChunk(ctx, alloc, payload); err != nil {
		t.Fatalf("WriteChunk (direct EC): %v", err)
	}

	// The authority lifted the chunk into durable EC (ECStripeID set).
	lifted, err := pb.GetChunk(ctx, alloc.ID)
	if err != nil {
		t.Fatalf("GetChunk(post): %v", err)
	}
	if lifted.ECStripeID == "" {
		t.Fatalf("chunk not lifted into EC after direct write (ECStripeID empty)")
	}

	// Read back through the gateway store: ECStripeID set -> ReadECShard path,
	// aggregates 9 shards -> decodes the exact original.
	got, err := cs.ReadChunk(ctx, lifted)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("read-back mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

// TestGatewayDirectECWrite_SingleNode exercises the single-node V2.1 topology
// (user-selected): all 9 shards land on the one node's shard store (local 6+3,
// mirroring V1 single-node direct EC) and read back byte-exact.
func TestGatewayDirectECWrite_SingleNode(t *testing.T) {
	const (
		nNodes   = 1
		disksPer = 9 // 9 candidate shard stores on one node
	)
	cl := buildProdCluster(t, nNodes, disksPer)
	meta := cl.nodes[0].meta
	pb := cl.pb

	ctx := context.Background()
	if err := pb.CreateBucket(ctx, "gw-single-ec", metadata.PlacementPolicy{
		ReplicationFactor: 1,
		ECConfig:          &metadata.ECConfig{DataShards: 6, ParityShards: 3},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, _ := pb.GetBucket(ctx, "gw-single-ec")
	inode, _ := pb.CreateFile(ctx, bucket.RootInode, "obj", 0644)
	alloc, err := pb.AllocateChunk(ctx, inode.ID, 0, metadata.PlacementPolicy{
		ReplicationFactor: 1,
		ECConfig:          &metadata.ECConfig{DataShards: 6, ParityShards: 3},
	})
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}

	cs := chunkstore.NewDatanodeChunkStore()
	cs.SetECWriteAuthority(meta)
	defer cs.Close()

	payload := make([]byte, 0, 600)
	for i := 0; i < 600; i++ {
		payload = append(payload, byte(i*3))
	}
	if err := cs.WriteChunk(ctx, alloc, payload); err != nil {
		t.Fatalf("WriteChunk single-node direct EC: %v", err)
	}
	lifted, err := pb.GetChunk(ctx, alloc.ID)
	if err != nil || lifted.ECStripeID == "" {
		t.Fatalf("single-node chunk not lifted to EC: err=%v stripe=%q", err, lifted.ECStripeID)
	}
	got, err := cs.ReadChunk(ctx, lifted)
	if err != nil {
		t.Fatalf("ReadChunk single-node: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("single-node read-back mismatch: got %d, want %d", len(got), len(payload))
	}
}

// TestGatewayDirectECWrite_DegradedRead proves the direct-written chunk serves
// a degraded read after losing <=3 shards (the gateway store reads the 9 shards
// over TCP from their owning nodes, decodes from the surviving >=6). A node
// with ~2 shards is stopped; the rest still decode the original.
func TestGatewayDirectECWrite_DegradedRead(t *testing.T) {
	const (
		nNodes   = 6
		disksPer = 3
	)
	cl := buildProdCluster(t, nNodes, disksPer)
	meta := cl.nodes[0].meta
	pb := cl.pb

	ctx := context.Background()
	if err := pb.CreateBucket(ctx, "gw-degraded-ec", metadata.PlacementPolicy{
		ReplicationFactor: 1,
		ECConfig:          &metadata.ECConfig{DataShards: 6, ParityShards: 3},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, _ := pb.GetBucket(ctx, "gw-degraded-ec")
	inode, _ := pb.CreateFile(ctx, bucket.RootInode, "obj", 0644)
	alloc, err := pb.AllocateChunk(ctx, inode.ID, 0, metadata.PlacementPolicy{
		ReplicationFactor: 1,
		ECConfig:          &metadata.ECConfig{DataShards: 6, ParityShards: 3},
	})
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}

	cs := chunkstore.NewDatanodeChunkStore()
	cs.SetECWriteAuthority(meta)
	defer cs.Close()

	payload := make([]byte, 0, 900)
	for i := 0; i < 900; i++ {
		payload = append(payload, byte((i*11)&0xff))
	}
	if err := cs.WriteChunk(ctx, alloc, payload); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	lifted, err := pb.GetChunk(ctx, alloc.ID)
	if err != nil {
		t.Fatalf("GetChunk(post): %v", err)
	}

	// Stop a node that owns shards of this chunk, so its ~2 shards vanish
	// (still >=6 survive -> degraded decode). We don't know the placement
	// offhand, so pick the node that owns the fewest shards and stop it if
	// that loses <=3 shards; otherwise stop any single node (<=3 shards each).
	perNode := map[metadata.NodeID]int{}
	for _, r := range lifted.Replicas {
		perNode[r.NodeID]++
	}
	victim := metadata.NodeID(0)
	for id, cnt := range perNode {
		if cnt <= 3 && (victim == 0 || cnt < perNode[victim]) {
			victim = id
		}
	}
	if victim == 0 {
		t.Fatal("no node owns <=3 shards of the direct-written chunk")
	}
	cl.nodes[int(victim)-1].srv.Stop()

	got, err := cs.ReadChunk(ctx, lifted)
	if err != nil {
		t.Fatalf("degraded ReadChunk after losing node %d: %v", victim, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("degraded read-back mismatch: got %d, want %d", len(got), len(payload))
	}
}

// TestGatewayDirectECWrite_V1Fallback proves a DatanodeChunkStore without a
// wired ECWriteAuthority keeps V1 fallback semantics: an ECGroup chunk routes to
// the V1 writeECChunk path (ReplicateECShard not involved), unchanged.
func TestGatewayDirectECWrite_V1Fallback(t *testing.T) {
	const (
		nNodes   = 6
		disksPer = 3
	)
	cl := buildProdCluster(t, nNodes, disksPer)
	pb := cl.pb

	ctx := context.Background()
	if err := pb.CreateBucket(ctx, "gw-v1fallback", metadata.PlacementPolicy{
		ReplicationFactor: 1,
		ECConfig:          &metadata.ECConfig{DataShards: 6, ParityShards: 3},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, _ := pb.GetBucket(ctx, "gw-v1fallback")
	inode, _ := pb.CreateFile(ctx, bucket.RootInode, "obj", 0644)
	alloc, err := pb.AllocateChunk(ctx, inode.ID, 0, metadata.PlacementPolicy{
		ReplicationFactor: 1,
		ECConfig:          &metadata.ECConfig{DataShards: 6, ParityShards: 3},
	})
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}

	// No ECWriteAuthority wired — WriteChunk must take the V1 writeECChunk branch.
	cs := chunkstore.NewDatanodeChunkStore()
	defer cs.Close()

	payload := make([]byte, 0, 600)
	for i := 0; i < 600; i++ {
		payload = append(payload, byte(i*5))
	}
	if err := cs.WriteChunk(ctx, alloc, payload); err != nil {
		t.Fatalf("WriteChunk (V1 fallback): %v", err)
	}
}
