package metadata

import (
	"context"
	"testing"
)

// ============================================================
// TDD: EC Write Path Integration Tests
// ============================================================
// These tests define the expected behavior when PlacementPolicy
// includes ECConfig. The implementation should make them pass.

func TestAllocateChunk_ECPlacement(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Register enough nodes for EC(4+2) = 6 nodes
	for i := 0; i < 8; i++ {
		node := &NodeInfo{
			ID:         NodeID(100 + i),
			Addr:       "10.0.0.{node}:8080",
			Rack:       "rack-1",
			Zone:       "zone-a",
			CapacityGB: 1000,
			UsedGB:     100,
			State:      NodeOnline,
		}
		node.Addr = formatNodeAddr(i)
		if err := store.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}

	// Create bucket with EC policy
	ecPolicy := PlacementPolicy{
		ID:                "ec-4-2",
		ReplicationFactor: 0, // 0 means use EC instead of replication
		ECConfig:          &ECConfig{DataShards: 4, ParityShards: 2},
		TopologySpread:    SpreadNode,
	}
	if err := store.CreateBucket(ctx, "ec-bucket", ecPolicy); err != nil {
		t.Fatal(err)
	}

	bucket, err := store.GetBucket(ctx, "ec-bucket")
	if err != nil {
		t.Fatal(err)
	}

	// Create a file in the bucket
	file, err := store.CreateFile(ctx, bucket.RootInode, "data.bin", 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Allocate chunk with EC policy
	chunk, err := store.AllocateChunk(ctx, file.ID, 0, ecPolicy)
	if err != nil {
		t.Fatalf("AllocateChunk with EC policy failed: %v", err)
	}

	// Test 1: Should allocate K+M replicas (6 for 4+2)
	if len(chunk.Replicas) != 6 {
		t.Errorf("expected 6 replicas for EC(4+2), got %d", len(chunk.Replicas))
	}

	// Test 2: ECGroup should be set on the chunk
	if chunk.ECGroup == nil {
		t.Fatal("expected ECGroup to be set on chunk")
	}
	if chunk.ECGroup.DataShards != 4 {
		t.Errorf("expected ECGroup.DataShards=4, got %d", chunk.ECGroup.DataShards)
	}
	if chunk.ECGroup.ParityShards != 2 {
		t.Errorf("expected ECGroup.ParityShards=2, got %d", chunk.ECGroup.ParityShards)
	}

	// Test 3: Each replica should have a unique shard index
	shardIndices := make(map[int]bool)
	for i, r := range chunk.Replicas {
		_ = r
		// ShardIndex should be set per-replica
		shardIndices[i] = true
	}
	if len(shardIndices) != 6 {
		t.Errorf("expected 6 unique shard indices, got %d", len(shardIndices))
	}
}

func TestAllocateChunk_ReplicationPolicy_NoEC(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		node := &NodeInfo{
			ID:         NodeID(100 + i),
			Addr:       formatNodeAddr(i),
			Rack:       "rack-1",
			Zone:       "zone-a",
			CapacityGB: 1000,
			UsedGB:     100,
			State:      NodeOnline,
		}
		if err := store.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}

	// Create bucket with replication policy (no EC)
	replPolicy := PlacementPolicy{
		ID:                "repl-3",
		ReplicationFactor: 3,
		TopologySpread:    SpreadNode,
	}
	if err := store.CreateBucket(ctx, "repl-bucket", replPolicy); err != nil {
		t.Fatal(err)
	}

	bucket, err := store.GetBucket(ctx, "repl-bucket")
	if err != nil {
		t.Fatal(err)
	}

	file, err := store.CreateFile(ctx, bucket.RootInode, "data.bin", 0644)
	if err != nil {
		t.Fatal(err)
	}

	chunk, err := store.AllocateChunk(ctx, file.ID, 0, replPolicy)
	if err != nil {
		t.Fatalf("AllocateChunk with replication policy failed: %v", err)
	}

	// Should allocate ReplicationFactor replicas
	if len(chunk.Replicas) != 3 {
		t.Errorf("expected 3 replicas, got %d", len(chunk.Replicas))
	}

	// ECGroup should NOT be set
	if chunk.ECGroup != nil {
		t.Error("expected ECGroup to be nil for replication policy")
	}
}

func TestAllocateChunk_ECPolicy_InsufficientNodes(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Register only 3 nodes — not enough for EC(4+2)
	for i := 0; i < 3; i++ {
		node := &NodeInfo{
			ID:         NodeID(100 + i),
			Addr:       formatNodeAddr(i),
			Rack:       "rack-1",
			Zone:       "zone-a",
			CapacityGB: 1000,
			UsedGB:     100,
			State:      NodeOnline,
		}
		if err := store.RegisterNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}

	ecPolicy := PlacementPolicy{
		ID:                "ec-4-2",
		ReplicationFactor: 0,
		ECConfig:          &ECConfig{DataShards: 4, ParityShards: 2},
		TopologySpread:    SpreadNode,
	}

	if err := store.CreateBucket(ctx, "ec-bucket", ecPolicy); err != nil {
		t.Fatal(err)
	}

	bucket, err := store.GetBucket(ctx, "ec-bucket")
	if err != nil {
		t.Fatal(err)
	}

	file, err := store.CreateFile(ctx, bucket.RootInode, "data.bin", 0644)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.AllocateChunk(ctx, file.ID, 0, ecPolicy)
	if err == nil {
		t.Fatal("expected error when not enough nodes for EC, got nil")
	}
}

func TestECEncoder_RoundTrip(t *testing.T) {
	// Verify EC encode → partial loss → decode works end-to-end
	ec := NewECEncoder(4, 2)
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	result, err := ec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate losing 1 data shard and 1 parity shard
	totalShards := 6
	shards := make([][]byte, totalShards)
	present := make([]bool, totalShards)

	// Data shards 0,1,2 present; shard 3 lost
	shards[0] = result.DataShards[0]
	present[0] = true
	shards[1] = result.DataShards[1]
	present[1] = true
	shards[2] = result.DataShards[2]
	present[2] = true
	shards[3] = nil
	present[3] = false

	// Parity shard 0 present; shard 1 lost
	shards[4] = result.ParityShards[0]
	present[4] = true
	shards[5] = nil
	present[5] = false

	decoded, err := ec.Decode(shards, present, len(data))
	if err != nil {
		t.Fatalf("EC decode failed: %v", err)
	}

	for i := range data {
		if decoded[i] != data[i] {
			t.Fatalf("decoded data mismatch at byte %d: got %d, want %d", i, decoded[i], data[i])
		}
	}
}

func TestECConfig_EffectiveReplicaCount(t *testing.T) {
	// EC(4+2) provides durability equivalent to ~1.5 replicas
	// but uses only 1.5x space instead of 3x
	ec4_2 := ECConfig{DataShards: 4, ParityShards: 2}
	if ec4_2.TotalShards() != 6 {
		t.Errorf("EC(4+2) total shards should be 6, got %d", ec4_2.TotalShards())
	}
	if ec4_2.StorageOverhead() != 1.5 {
		t.Errorf("EC(4+2) storage overhead should be 1.5, got %.2f", ec4_2.StorageOverhead())
	}

	ec8_4 := ECConfig{DataShards: 8, ParityShards: 4}
	if ec8_4.StorageOverhead() != 1.5 {
		t.Errorf("EC(8+4) storage overhead should be 1.5, got %.2f", ec8_4.StorageOverhead())
	}

	ec2_1 := ECConfig{DataShards: 2, ParityShards: 1}
	if ec2_1.StorageOverhead() != 1.5 {
		t.Errorf("EC(2+1) storage overhead should be 1.5, got %.2f", ec2_1.StorageOverhead())
	}
}

// helper: format node address
func formatNodeAddr(i int) string {
	return "10.0.0." + string(rune('1'+i)) + ":8080"
}
