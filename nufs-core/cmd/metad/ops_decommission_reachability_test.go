package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// TestDecommissionDoesNotHurtReachability is the real acceptance for "decommission
// must not affect data reachability" (the second of the four directions the user
// picked). It drives the genuine cross-module pipeline:
//
//	metadata authority (PebbleStore) + placement engine + the live HTTP
//	/decommission and /restore routes + the real chunk allocation path.
//
// On a 2-node cluster with RF=2, a chunk is replicated across both nodes. After
// node 1 is decommissioned (draining, excluded by placement), the property under
// test is that data stays reachable:
//
//  1. New writes (AllocateChunk) land only on the surviving node 2 — node 1 is
//     removed from placement (the bucket's RF shrinks to match the smaller
//     healthy pool), so nothing new is lost by its departure.
//  2. Existing reads stay servable — the pre-existing replicated chunk still
//     resolves a replica on the surviving, placement-viable node 2, so the data
//     that already exists remains reachable.
//  3. Recoverability — restoring node 1 re-admits it to placement, so the
//     cluster returns to full 2-replica placement.
//
// This locks the "下线不影响数据可达性" property into CI at the metadata +
// placement + control-plane level, complementing the single-node browser
// decommission/restore loop (task #194).
func TestDecommissionDoesNotHurtReachability(t *testing.T) {
	ctx := context.Background()

	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: t.TempDir(), UseInMemory: true, UseBucketStats: false,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer store.Close()

	bundle, err := metadata.NewPebbleServiceBundle(
		store,
		metadata.WithLeaseTTL(0),
		metadata.WithGCInterval(0),
		metadata.WithScrubInterval(0),
	)
	if err != nil {
		t.Fatalf("NewPebbleServiceBundle: %v", err)
	}
	defer bundle.Close()

	// The real HTTP control plane: /decommission and /restore routes, exactly as
	// the running metad serves them.
	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, store, bundle, "")
	server := httptest.NewServer(mux)
	defer server.Close()
	client := metadata.NewHTTPClient(server.URL, 0)

	// 2-node cluster: both online, both placement-eligible. Node 1 is the
	// load/free-capacity favorite (placements and jitter-free), so a correctly
	// filtered placement MUST prefer it while it is online — and MUST drop to
	// node 2 the moment it is decommissioned. This makes the assertions below
	// sensitive to the state filter rather than to score ties or ID jitter.
	const n1 = metadata.NodeID(1)
	const n2 = metadata.NodeID(2)
	if err := store.RegisterNode(ctx, &metadata.NodeInfo{ID: n1, Addr: "127.0.0.1:19101", CapacityGB: 100, UsedGB: 0, Rack: "rack-1", Zone: "zone-1"}); err != nil {
		t.Fatalf("RegisterNode %d: %v", n1, err)
	}
	if err := store.RegisterNode(ctx, &metadata.NodeInfo{ID: n2, Addr: "127.0.0.1:19102", CapacityGB: 100, UsedGB: 90, Rack: "rack-2", Zone: "zone-2"}); err != nil {
		t.Fatalf("RegisterNode %d: %v", n2, err)
	}

	policy := metadata.PlacementPolicy{ReplicationFactor: 2, StorageTier: metadata.TierHot}
	if err := store.CreateBucket(ctx, "reach", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "reach")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := store.CreateFile(ctx, bucket.RootInode, "a.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	// Before decommission: with both nodes online and RF=2, a chunk is
	// replicated across BOTH nodes — this is the data whose reachability we
	// will later defend.
	first, err := store.AllocateChunk(ctx, inode.ID, 0, policy)
	if err != nil {
		t.Fatalf("AllocateChunk before decommission: %v", err)
	}
	assertReplicaSet(t, "pre-decommission allocation", first, []metadata.NodeID{n1, n2})

	// ---- Decommission node 1 via the real control-plane route. ----
	if err := client.DecommissionNode(ctx, n1); err != nil {
		t.Fatalf("DecommissionNode via route: %v", err)
	}
	one, _ := store.GetNode(ctx, n1)
	if one.State != metadata.NodeDraining {
		t.Fatalf("after decommission state=%v, want NodeDraining", one.State)
	}

	// ---- (1) Write side: new writes must land only on the surviving node 2. ----
	// A decommissioned node is removed from placement, so the healthy-node pool
	// for the bucket shrinks 2→1: the operator (or autobalance) lowers the RF to
	// match the remaining capacity. New chunks then place only on node 2 — node
	// 1, now draining, must never be selected, so nothing new is written to a
	// node being taken out of service.
	shrunk := metadata.PlacementPolicy{ReplicationFactor: 1, StorageTier: metadata.TierHot}
	second, err := store.AllocateChunk(ctx, inode.ID, 1024, shrunk)
	if err != nil {
		t.Fatalf("AllocateChunk after decommission: %v", err)
	}
	assertReplicaSet(t, "post-decommission allocation", second, []metadata.NodeID{n2})

	// ---- (2) Read side: the pre-existing replicated chunk still resolves a
	// replica on a placement-viable (online, non-decommissioned) node, so its
	// data remains reachable from the surviving node's replica. ----
	got, err := store.GetChunk(ctx, first.ID)
	if err != nil {
		t.Fatalf("GetChunk(first): %v", err)
	}
	if !reachesHealthyReplica(t, store, got) {
		t.Fatalf("existing chunk %d has no replica on any online node after decommission — data became unreachable", first.ID)
	}

	// ---- (3) Recoverability: restore node 1 → placement re-admits it. ----
	if err := client.RestoreNode(ctx, n1); err != nil {
		t.Fatalf("RestoreNode via route: %v", err)
	}
	one, _ = store.GetNode(ctx, n1)
	if one.State != metadata.NodeOnline {
		t.Fatalf("after restore state=%v, want NodeOnline", one.State)
	}
	third, err := store.AllocateChunk(ctx, inode.ID, 2048, policy)
	if err != nil {
		t.Fatalf("AllocateChunk after restore: %v", err)
	}
	assertReplicaSet(t, "post-restore allocation", third, []metadata.NodeID{n1, n2})
}

// assertReplicaSet asserts the allocated chunk's replica node set equals want
// (order-insensitive).
func assertReplicaSet(t *testing.T, label string, c *metadata.ChunkMeta, want []metadata.NodeID) {
	t.Helper()
	if c == nil {
		t.Fatalf("%s: nil chunk", label)
	}
	got := map[metadata.NodeID]bool{}
	for _, r := range c.Replicas {
		got[r.NodeID] = true
	}
	if len(got) != len(want) {
		t.Fatalf("%s: replica nodes=%v, want %v", label, replicaList(got), want)
	}
	for _, w := range want {
		if !got[w] {
			t.Fatalf("%s: replica nodes=%v, missing node %d (want %v)", label, replicaList(got), w, want)
		}
	}
}

// reachesHealthyReplica reports whether the chunk has at least one replica on a
// node the authority currently considers online — i.e. the data is reachable
// from a placement-viable replica set.
func reachesHealthyReplica(t *testing.T, store *metadata.PebbleStore, c *metadata.ChunkMeta) bool {
	t.Helper()
	for _, r := range c.Replicas {
		n, err := store.GetNode(context.Background(), r.NodeID)
		if err != nil {
			continue
		}
		if n.State == metadata.NodeOnline {
			return true
		}
	}
	return false
}

func replicaList(m map[metadata.NodeID]bool) []metadata.NodeID {
	var out []metadata.NodeID
	for id := range m {
		out = append(out, id)
	}
	return out
}
