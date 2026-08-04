package datanode

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

// This file is Task #77 / S3: the cross-node EC conversion, where a single
// datanode acts as the coordinator and *pushes* the shards it does not own to
// peer datanodes over the TCP wire (ReqReplicateECShard). Each peer writes only
// the shards assigned to it; the shared metadata authority owns the §14 plan
// and the lifecycle transaction. This closes the "each datanode writes only
// its own shards" gap that single-node S2 deliberately deferred to deployment.

// s3Node bundles one cross-node datanode: the TCP server (the wire surface for
// ReplicateECShard), its V2Store with attached shard stores, and the node ID.
type s3Node struct {
	id  metadata.NodeID
	srv *Server
	v2  *V2Store
}

// buildClusterN starts n datanodes, each with disksPerNode shard stores, over
// real TCP servers, returning the nodes plus the single shared metadata
// authority. Node IDs are 1..n.
func buildClusterN(t *testing.T, n, disksPerNode int) ([]*s3Node, *metadata.PebbleStore) {
	t.Helper()
	nodes := make([]*s3Node, n)
	for i := 0; i < n; i++ {
		v, _ := newTestShardMultiStore(t, disksPerNode)
		cfg := DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.NodeID = metadata.NodeID(i + 1)
		cfg.RequestTimeout = 2 * time.Second
		srv := NewServer(cfg, v)
		if err := srv.Start(); err != nil {
			t.Fatalf("Start node %d: %v", i, err)
		}
		nodes[i] = &s3Node{id: metadata.NodeID(i + 1), srv: srv, v2: v}
		t.Cleanup(func() { srv.Stop() })
	}
	pb, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: t.TempDir(), UseInMemory: true, UseBucketStats: false,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = pb.Close() })
	return nodes, pb
}

// clusterDisks builds the real cross-node candidate topology: n nodes ×
// disksPerNode disks. DiskID = node*1000+disk, so DiskID%1000 recovers the
// node-local shard-store index (the same decode Convention as the synthetic
// topology). The §14 PlanShards then spans the real nodes.
func clusterDisks(n, disksPerNode int) []metadata.ECDisk {
	var disks []metadata.ECDisk
	for node := uint64(1); node <= uint64(n); node++ {
		for d := 0; d < disksPerNode; d++ {
			disks = append(disks, metadata.ECDisk{NodeID: node, DiskID: node*1000 + uint64(d)})
		}
	}
	return disks
}

// TestECCrossNode_CoordinatorPushesShards is the S3 capstone: a coordinator
// datanode converts a replicated chunk to a completed 6+3 stripe by pushing the
// shards assigned to other nodes over the TCP wire, each peer writing only its
// own shards, with the shared metadata authority recording the durable Complete
// stripe. Every shard is verified byte-exact on the node that owns it, and the
// aggregate — and a degraded read after losing a whole node — decode to the
// original payload.
func TestECCrossNode_CoordinatorPushesShards(t *testing.T) {
	const (
		nNodes   = 3
		disksPer = 3 // 3 nodes × 3 disks = 9 candidate shard stores (§14)
	)
	nodes, pb := buildClusterN(t, nNodes, disksPer)
	cid := metadata.ChunkID(97001)

	// A replicated chunk exists on the coordinator (node 1) — the RF layout
	// being replaced. (In production each replica is written by its own node;
	// here the source extent lives on node 1, which is the coordinator.)
	payload := []byte("s3-cross-node-ec-convert-coordinator-push")
	for i := 0; i < 200; i++ {
		payload = append(payload, byte(i*7))
	}
	if err := nodes[0].v2.Write(cid, payload); err != nil {
		t.Fatalf("write replicated chunk: %v", err)
	}

	// The coordinator's ECService is bound to the shared metadata authority and
	// configured in cross-node mode: it pushes shards to peers over TCP.
	auth := metadata.NewECStore(pb)
	coord := NewECService(nodes[0].v2, auth)
	coord.SetCrossNode(uint64(nodes[0].id), func(nodeID uint64) (*Client, bool) {
		for _, nd := range nodes {
			if uint64(nd.id) == nodeID {
				return NewClient(nd.srv.Addr()), true
			}
		}
		return nil, false
	})
	coord.SetCandidateDisks(func() []metadata.ECDisk { return clusterDisks(nNodes, disksPer) })

	st, err := coord.ConvertToEC(context.Background(), cid, 1)
	if err != nil {
		t.Fatalf("ConvertToEC: %v", err)
	}
	if st.State != metadata.ECConversionComplete {
		t.Fatalf("state=%s, want complete", st.State)
	}
	if len(st.Shards) != 9 {
		t.Fatalf("shards=%d, want 9", len(st.Shards))
	}

	// The shared authority durably recorded the completed stripe.
	durable, err := metadata.NewECStore(pb).GetStripe(st.StripeID)
	if err != nil {
		t.Fatalf("GetStripe: %v", err)
	}
	if durable == nil || durable.State != metadata.ECConversionComplete {
		t.Fatalf("durable stripe=%+v, want complete", durable)
	}

	// §14: the 9 planned shards span all 3 distinct nodes, ≤3 per node.
	perNode := map[uint64]int{}
	for _, s := range st.Shards {
		perNode[s.NodeID]++
	}
	if len(perNode) != 3 {
		t.Fatalf("shards span %d nodes, want 3 (all cluster nodes)", len(perNode))
	}
	for n, cnt := range perNode {
		if cnt > 3 {
			t.Fatalf("node %d holds %d shards, want ≤3 §14", n, cnt)
		}
	}

	// Every shard is byte-exact on the node that owns it (the push path for
	// cross-node shards, local write for the coordinator's own).
	expect := mustEncodeEC63(t, payload)
	for _, s := range st.Shards {
		node := nodes[s.NodeID-1]
		got, _, err := node.v2.ReadShard(cid, s.Index)
		if err != nil {
			t.Fatalf("ReadShard(chunk %d, shard %d) on node %d: %v", cid, s.Index, s.NodeID, err)
		}
		if !bytes.Equal(got, expect[s.Index]) {
			t.Fatalf("shard %d on node %d mismatch: got %d bytes, want %d", s.Index, s.NodeID, len(got), len(expect[s.Index]))
		}
	}

	// Aggregate across the cluster decodes to the original payload.
	all := make([][]byte, 9)
	for _, s := range st.Shards {
		got, _, err := nodes[s.NodeID-1].v2.ReadShard(cid, s.Index)
		if err != nil {
			t.Fatalf("reread shard %d node %d: %v", s.Index, s.NodeID, err)
		}
		all[s.Index] = got
	}
	dec, err := decodeEC63(all, len(payload))
	if err != nil {
		t.Fatalf("decode aggregate: %v", err)
	}
	if !bytes.Equal(dec, payload) {
		t.Fatalf("aggregate decode mismatch: %d bytes, want %d", len(dec), len(payload))
	}

	// Losing a whole node (its 3 shards) still reconstructs the original from
	// the six survivors on the other two nodes — degraded read across nodes.
	dead := st.Shards[0].NodeID // node whose 3 shards we drop
	survivors := make([][]byte, 9)
	for _, s := range st.Shards {
		if s.NodeID == dead {
			continue
		}
		got, _, err := nodes[s.NodeID-1].v2.ReadShard(cid, s.Index)
		if err != nil {
			t.Fatalf("read survivor shard %d node %d: %v", s.Index, s.NodeID, err)
		}
		survivors[s.Index] = got
	}
	deg, err := decodeEC63(survivors, len(payload))
	if err != nil {
		t.Fatalf("degraded decode: %v", err)
	}
	if !bytes.Equal(deg, payload) {
		t.Fatalf("degraded decode mismatch after losing node %d", dead)
	}

	// The completed stripe lifts into an atomically-switchable 6+3 layout.
	cm := BuildECGroup(st, int32(len(payload)), metadata.TierCold)
	if cm.ECGroup == nil || cm.ECGroup.DataShards != 6 || cm.ECGroup.ParityShards != 3 {
		t.Fatalf("ECGroup=%+v, want 6+3", cm.ECGroup)
	}
	if len(cm.Replicas) != 9 {
		t.Fatalf("replicas=%d, want 9", len(cm.Replicas))
	}
}

// mustEncodeEC63 encodes payload into 9 shards, failing the test on error.
func mustEncodeEC63(t *testing.T, payload []byte) [][]byte {
	t.Helper()
	all, err := encodeEC63(payload)
	if err != nil {
		t.Fatalf("encodeEC63: %v", err)
	}
	return all
}

// TestECCrossNode_PeerDownRollsBack proves a cross-node conversion rolls back
// cleanly (authority → RolledBack, chunk stays a readable replica) when a peer
// that is assigned shards is unreachable — no partial stripe is committed.
func TestECCrossNode_PeerDownRollsBack(t *testing.T) {
	const (
		nNodes   = 3
		disksPer = 3
	)
	nodes, pb := buildClusterN(t, nNodes, disksPer)
	cid := metadata.ChunkID(97002)
	payload := []byte("s3-peer-down-rollback")
	for i := 0; i < 40; i++ {
		payload = append(payload, 0x33)
	}
	if err := nodes[0].v2.Write(cid, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	auth := metadata.NewECStore(pb)
	coord := NewECService(nodes[0].v2, auth)
	// peerClient only resolves node 1 (ourselves); nodes 2 and 3 are treated as
	// unreachable, so any shard planned onto them fails the push.
	coord.SetCrossNode(uint64(nodes[0].id), func(nodeID uint64) (*Client, bool) {
		return nil, false
	})
	coord.SetCandidateDisks(func() []metadata.ECDisk { return clusterDisks(nNodes, disksPer) })

	if _, err := coord.ConvertToEC(context.Background(), cid, 1); err == nil {
		t.Fatal("ConvertToEC succeeded with unreachable peers, want error")
	}

	// The failed conversion was rolled back, not committed.
	durable, err := metadata.NewECStore(pb).GetStripe("stripe-97002")
	if err != nil {
		t.Fatalf("GetStripe: %v", err)
	}
	if durable == nil || durable.State != metadata.ECConversionRolledBack {
		t.Fatalf("durable stripe=%+v, want rolled_back", durable)
	}

	// The chunk was never switched: it still reads back as the original replica.
	if got, _, err := nodes[0].v2.Read(cid, 0, 0); err != nil || string(got) != string(payload) {
		t.Fatalf("replica unreadable after rollback: data=%q err=%v", got, err)
	}
}
