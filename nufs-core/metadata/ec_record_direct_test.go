package metadata

import (
	"context"
	"testing"
)

// buildDirectChunk seeds a chunk exactly as an ECConfig allocation does
// (buildAllocatedChunks): ECGroup referencing the 6+3 profile with the group ID
// "ec-<chunkID>", and nine Replicas each carrying the owning node's Addr +
// ShardIndex. Returns the seeded chunk.
func buildDirectChunk(t *testing.T, store *PebbleStore, cid ChunkID, nodes []NodeID) *ChunkMeta {
	t.Helper()
	seed := &ChunkMeta{
		ID:         cid,
		Size:       4096,
		State:      ChunkCreated,
		Tier:       TierCold,
		CreateTime: 1,
		Generation: 1,
		ECGroup:    ECGroupFromProfile(nil, groupIDFor(cid)),
	}
	for i, n := range nodes {
		seed.Replicas = append(seed.Replicas, ReplicaInfo{
			NodeID: n, Addr: "node-" + itoa(int(n)) + ":12345",
			State: ReplicaSyncing, ShardIndex: i,
		})
	}
	if err := store.putMsgpack(chunkMetadataKey(cid), seed); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	return seed
}

func groupIDFor(cid ChunkID) string { return "ec-" + itoa(int(cid)) }

// TestECRecordDirect_LiftsAllocatedChunkToEC verifies the write-path direct-EC
// authority registers a directly-written chunk: it records a durably Complete
// ECStripe (keyed by the chunk's allocation group ID) and atomically joins
// ChunkMeta.ECStripeID + State while preserving the allocated Replicas (with
// live Addr) the gateway read path dials.
func TestECRecordDirect_LiftsAllocatedChunkToEC(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)

	const cid = ChunkID(9001)
	nodes := []NodeID{1, 2, 3}
	chunk := buildDirectChunk(t, store, cid, []NodeID{1, 2, 3, 1, 2, 3, 1, 2, 3})
	_ = chunk

	// A §14-diverse plan aligned to the allocation: shard i -> Replicas[i].NodeID
	// with a disk per node (DiskID = NodeID*1000 + disk, §14).
	plan := make([]ECShard, 0, 9)
	diskFor := map[NodeID]int{1: 0, 2: 0, 3: 0}
	want := []NodeID{1, 2, 3, 1, 2, 3, 1, 2, 3}
	for i, n := range want {
		plan = append(plan, ECShard{Index: i, NodeID: uint64(n), DiskID: uint64(n)*1000 + uint64(diskFor[n])})
		diskFor[n]++
	}
	for i := range nodes {
		if plan[i].NodeID != uint64(want[i]) {
			t.Fatalf("plan node mismatch at %d", i)
		}
	}

	layout, st, err := ec.RecordDirect(context.Background(), cid, plan, 0xCAFEBABE)
	if err != nil {
		t.Fatalf("RecordDirect: %v", err)
	}
	if layout.ECStripeID != "ec-9001" {
		t.Fatalf("ECStripeID = %q, want ec-9001", layout.ECStripeID)
	}
	if layout.State != ChunkReady {
		t.Fatalf("state = %v, want ready", layout.State)
	}
	if layout.Checksum != 0xCAFEBABE {
		t.Fatalf("checksum = %#x, want cafebabe", layout.Checksum)
	}
	// Replicas preserved with Addr (gateway read dials them).
	for i, r := range layout.Replicas {
		if r.Addr == "" || r.NodeID != want[i] || r.ShardIndex != i {
			t.Fatalf("replica %d preserved incorrectly: %+v", i, r)
		}
	}
	// Durable stripe is Complete with the same landing.
	if st.State != ECConversionComplete || st.OriginalChecksum != 0xCAFEBABE {
		t.Fatalf("stripe = state %s csum %#x", st.State, st.OriginalChecksum)
	}
	durable, err := ec.GetStripe("ec-9001")
	if err != nil || durable == nil {
		t.Fatalf("GetStripe: %v (nil=%v)", err, durable == nil)
	}
	if durable.State != ECConversionComplete || len(durable.Shards) != 9 {
		t.Fatalf("durable stripe = state %s shards %d", durable.State, len(durable.Shards))
	}
	// ResolveStripeLanding now returns the authoritative landing (self-heal path).
	resolved, err := ec.ResolveStripeLanding(layout)
	if err != nil || len(resolved) != 9 {
		t.Fatalf("resolve landing after direct write: n=%d err=%v", len(resolved), err)
	}
}

// TestECRecordDirect_RejectsNodeMismatch verifies the authority refuses to
// record a plan whose per-shard node disagrees with the allocated Replicas —
// the materialized landing must never diverge from where the gateway actually
// pushed the shards.
func TestECRecordDirect_RejectsNodeMismatch(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)

	const cid = ChunkID(9002)
	buildDirectChunk(t, store, cid, []NodeID{1, 2, 3, 1, 2, 3, 1, 2, 3})

	// Shard 0 planned on node 2 but Replicas[0].NodeID is 1 -> mismatch.
	badPlan := make([]ECShard, 0, 9)
	for i := 0; i < 9; i++ {
		badPlan = append(badPlan, ECShard{Index: i, NodeID: 2, DiskID: 2000 + uint64(i)})
	}
	if _, _, err := ec.RecordDirect(context.Background(), cid, badPlan, 1); err == nil {
		t.Fatal("RecordDirect accepted a node-mismatched plan")
	}
	// The chunk must remain non-EC (no partial state committed).
	got, err := store.GetChunk(context.Background(), cid)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if got.ECStripeID != "" {
		t.Fatalf("chunk ECStripeID set after rejected record: %q", got.ECStripeID)
	}
}

// TestECRecordDirect_RejectsNonECChunk verifies a chunk not allocated for EC
// (no ECGroup) cannot be directly recorded.
func TestECRecordDirect_RejectsNonECChunk(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)

	const cid = ChunkID(9003)
	plan := []ECShard{{Index: 0, NodeID: 1, DiskID: 1000}}
	if err := store.putMsgpack(chunkMetadataKey(cid), &ChunkMeta{ID: cid, Size: 8, State: ChunkCreated}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, _, err := ec.RecordDirect(context.Background(), cid, plan, 1); err == nil {
		t.Fatal("RecordDirect accepted a non-EC chunk")
	}
}

// TestECRecordDirect_IdempotentComplete verifies re-recording the same chunk
// with the same plan is a clean no-op (same Complete stripe), while a partial /
// in-flight stripe (a raced conversion) is refused.
func TestECRecordDirect_IdempotentComplete(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)

	const cid = ChunkID(9004)
	buildDirectChunk(t, store, cid, []NodeID{1, 2, 3, 1, 2, 3, 1, 2, 3})
	plan := make([]ECShard, 0, 9)
	for i := 0; i < 9; i++ {
		plan = append(plan, ECShard{Index: i, NodeID: uint64([]int{1, 2, 3}[i%3]), DiskID: uint64([]int{1, 2, 3}[i%3])*1000 + uint64(i/3)})
	}
	if _, _, err := ec.RecordDirect(context.Background(), cid, plan, 7); err != nil {
		t.Fatalf("first record: %v", err)
	}
	// Idempotent re-record is a no-op error.
	if _, _, err := ec.RecordDirect(context.Background(), cid, plan, 7); err != nil {
		t.Fatalf("re-record should be idempotent: %v", err)
	}
	// A non-Complete stripe (e.g. a conversion began but never finished) raced:
	// RecordDirect must refuse rather than overwrite it.
	buildDirectChunk(t, store, ChunkID(9005), []NodeID{1, 2, 3, 1, 2, 3, 1, 2, 3})
	if _, err := ec.BeginConversion("ec-9005", 9005, 1, 0); err != nil {
		t.Fatalf("begin raced conversion: %v", err)
	}
	if _, _, err := ec.RecordDirect(context.Background(), ChunkID(9005), plan, 9); err == nil {
		t.Fatal("RecordDirect overwrote a raced in-flight stripe")
	}
}
