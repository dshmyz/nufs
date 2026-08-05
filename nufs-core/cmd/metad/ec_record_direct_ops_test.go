package main

import (
	"context"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

func ecGroupID(cid metadata.ChunkID) string { return "ec-" + strconv.FormatUint(uint64(cid), 10) }

// seedDirectECChunk registers 3 V2.1 nodes (each with shard disks) and
// allocates a 6+3 ECConfig chunk — replicas end up as 9 shard placements each
// carrying the owning node's live Addr + ShardIndex, exactly as the production
// gateway allocation produces for an ECConfig bucket.
func seedDirectECChunk(t *testing.T, store *metadata.PebbleStore, cid *metadata.ChunkID) {
	t.Helper()
	ctx := context.Background()
	// 6 online V2.1 nodes each with 3 shard disks. (PlacementEngine's
	// multi-shard pass puts at most ~2 shards/node, so 6+3 across 6 nodes
	// allocates cleanly; the owning-node set is a subset of these.)
	for id := metadata.NodeID(1); id <= 6; id++ {
		if err := store.RegisterNode(ctx, &metadata.NodeInfo{
			ID: id, Addr: "node-" + strconv.FormatUint(uint64(id), 10) + ":9100",
			CapacityGB: 10, Tier: metadata.TierHot, ShardDiskCount: 3,
		}); err != nil {
			t.Fatalf("RegisterNode(%d): %v", id, err)
		}
	}
	if err := store.CreateBucket(ctx, "direct-ec-bucket", metadata.PlacementPolicy{
		ReplicationFactor: 1,
		ECConfig:          &metadata.ECConfig{DataShards: 6, ParityShards: 3},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "direct-ec-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := store.CreateFile(ctx, bucket.RootInode, "f", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	alloc, err := store.AllocateChunk(ctx, inode.ID, 0, metadata.PlacementPolicy{
		ReplicationFactor: 1,
		ECConfig:          &metadata.ECConfig{DataShards: 6, ParityShards: 3},
	})
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	if alloc.ECGroup == nil || alloc.ECGroup.DataShards != 6 || alloc.ECGroup.ParityShards != 3 {
		t.Fatalf("allocated chunk lacks 6+3 ECGroup: %+v", alloc.ECGroup)
	}
	*cid = alloc.ID
	if *cid == 0 || len(alloc.Replicas) != 9 {
		t.Fatalf("allocated chunk id=%d replicas=%d, want 9", *cid, len(alloc.Replicas))
	}
}

// TestECPlanWrite_DiverseOwners verifies the plan-write RPC (first half of
// write-path direct EC, §14): given a 6+3 ECConfig chunk whose 9 replicas each
// carry an owning node, the authority returns a 9-shard §14-diverse plan where
// each shard lands on its own owning node with a distinct node-local disk.
func TestECPlanWrite_DiverseOwners(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	srv := httptest.NewServer(buildOpsTestMux(t, store, bundle))
	defer srv.Close()

	var cid metadata.ChunkID
	seedDirectECChunk(t, store, &cid)

	auth := metadata.NewHTTPClient(srv.URL, 10*time.Second)
	plan, err := auth.PlanECWrite(context.Background(), cid, 6, 3)
	if err != nil {
		t.Fatalf("PlanECWrite: %v", err)
	}
	if len(plan) != 9 {
		t.Fatalf("plan len = %d, want 9", len(plan))
	}

	// Each planned shard must land on the same owning node the chunk allocation
	// assigned (chunk.Replicas[i].NodeID) — so Replicas[i].Addr receives shard i.
	chunk, err := store.GetChunk(context.Background(), cid)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	var nodes uint64
	seenNodes := map[uint64]bool{}
	for i, sh := range plan {
		if sh.Index != i {
			t.Fatalf("plan[%d].Index = %d, want %d", i, sh.Index, i)
		}
		if sh.NodeID != uint64(chunk.Replicas[i].NodeID) {
			t.Fatalf("plan[%d].NodeID = %d, want owning %d", i, sh.NodeID, chunk.Replicas[i].NodeID)
		}
		if sh.DiskID/1000 != sh.NodeID {
			t.Fatalf("plan[%d] disk %d not on node %d (DiskID = node*1000+d)", i, sh.DiskID, sh.NodeID)
		}
		seenNodes[sh.NodeID] = true
		if sh.NodeID > nodes {
			nodes = sh.NodeID
		}
	}
	// §14: >= 3 distinct owning nodes (ECMinMachines).
	if len(seenNodes) < metadata.ECMinMachines {
		t.Fatalf("plan spans only %d distinct nodes, want >= 3", len(seenNodes))
	}
}

// TestECPlanWrite_V1ClusterFails verifies a V1-only cluster (nodes registered
// with no shard disks → ShardDiskCount==0) yields no §14 candidates and the
// plan-write RPC fails cleanly, so the gateway falls back to V1 replication.
func TestECPlanWrite_V1ClusterFails(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	srv := httptest.NewServer(buildOpsTestMux(t, store, bundle))
	defer srv.Close()

	ctx := context.Background()
	// V1 nodes: no shard disks reported (ShardDiskCount==0).
	for id := metadata.NodeID(1); id <= 6; id++ {
		if err := store.RegisterNode(ctx, &metadata.NodeInfo{
			ID: id, Addr: "node-" + strconv.FormatUint(uint64(id), 10) + ":9100",
			CapacityGB: 10, Tier: metadata.TierHot,
		}); err != nil {
			t.Fatalf("RegisterNode: %v", err)
		}
	}
	if err := store.CreateBucket(ctx, "v1-bucket", metadata.PlacementPolicy{
		ReplicationFactor: 1,
		ECConfig:          &metadata.ECConfig{DataShards: 6, ParityShards: 3},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, _ := store.GetBucket(ctx, "v1-bucket")
	inode, _ := store.CreateFile(ctx, bucket.RootInode, "f", 0644)
	alloc, err := store.AllocateChunk(ctx, inode.ID, 0, metadata.PlacementPolicy{
		ReplicationFactor: 1,
		ECConfig:          &metadata.ECConfig{DataShards: 6, ParityShards: 3},
	})
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}

	auth := metadata.NewHTTPClient(srv.URL, 10*time.Second)
	if plan, err := auth.PlanECWrite(ctx, alloc.ID, 6, 3); err == nil {
		t.Fatalf("PlanECWrite succeeded on a V1-only cluster; plan=%+v", plan)
	}
}

// TestECRecordDirectHTTP verifies the record-direct RPC (second half of
// write-path direct EC): reporting a landed 9-shard plan durably records a
// Complete stripe and lifts the chunk into EC (ECStripeID set) while preserving
// the allocated Replicas the gateway read path dials.
func TestECRecordDirectHTTP(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	srv := httptest.NewServer(buildOpsTestMux(t, store, bundle))
	defer srv.Close()

	var cid metadata.ChunkID
	seedDirectECChunk(t, store, &cid)

	auth := metadata.NewHTTPClient(srv.URL, 10*time.Second)
	ctx := context.Background()

	chunk, err := store.GetChunk(ctx, cid)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if chunk.ECStripeID != "" {
		t.Fatalf("chunk already EC before record-direct")
	}

	// Build the plan aligned to the allocation via the authority.
	plan, err := auth.PlanECWrite(ctx, cid, 6, 3)
	if err != nil {
		t.Fatalf("PlanECWrite: %v", err)
	}
	if err := auth.RecordDirectEC(ctx, cid, 6, 3, plan, 0xDEADBEEF); err != nil {
		t.Fatalf("RecordDirectEC: %v", err)
	}

	got, err := store.GetChunk(ctx, cid)
	if err != nil {
		t.Fatalf("GetChunk(post): %v", err)
	}
	if got.ECStripeID != ecGroupID(cid) {
		t.Fatalf("chunk ECStripeID = %q, want %q", got.ECStripeID, ecGroupID(cid))
	}
	if got.State != metadata.ChunkReady {
		t.Fatalf("chunk state = %v, want ready", got.State)
	}
	if got.Checksum != 0xDEADBEEF {
		t.Fatalf("chunk checksum = %#x, want deadbeef", got.Checksum)
	}
	// Replicas preserved with live Addr for the gateway read path.
	for i, r := range got.Replicas {
		if r.Addr == "" || uint64(r.NodeID) != plan[i].NodeID || r.ShardIndex != i {
			t.Fatalf("replica %d not preserved: %+v", i, r)
		}
	}
	// Durable stripe is Complete with the same landing (authoritative, on-disk).
	st, err := metadata.NewECStore(store).GetStripe(ecGroupID(cid))
	if err != nil {
		t.Fatalf("GetStripe: %v", err)
	}
	if st == nil || st.State != metadata.ECConversionComplete || st.OriginalChecksum != 0xDEADBEEF {
		t.Fatalf("durable stripe = %+v, want complete deadbeef", st)
	}
	if len(st.Shards) != 9 {
		t.Fatalf("durable stripe shards = %d, want 9", len(st.Shards))
	}
}
