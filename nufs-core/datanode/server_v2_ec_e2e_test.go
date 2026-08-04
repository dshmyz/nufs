package datanode

import (
	"bytes"
	"testing"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/metadata"
)

// The multi-node EC e2e proves the distributed-service claim the single-node
// E3–E5 primitives cannot: a 6+3 stripe planned by the metadata authority
// (ECStore.PlanShards, §14) is spread across three DISTINCT node V2Stores, each
// holding exactly its own subset of shard extents in its local shard stream —
// the physical analogue of three machines, one node per fault domain — and the
// stripe aggregates, degrades, repairs, and reheats byte-exact across them.
//
// The single-node V2Store's ReadChunkEC/RepairChunkEC/ReheatChunkEC aggregate
// within one V2Store's local shard array; a multi-node stripe lives across
// several V2Stores, so this harness provides the cluster-level orchestration:
// resolve each shard's owning node from the metadata plan, read/build/repair
// across the node V2Stores, and drive the package-local 6+3 coder exactly as
// the serving path would.

// ecNode is one in-process datanode: a V2Store whose shard stores are attached
// (StreamID 2), so it can hold its subset of the stripe's shard extents.
type ecNode struct {
	v2   *V2Store
	node metadata.NodeID
}

// mustNewShardNode builds a single node V2Store with n attached shard stores
// and returns the V2Store (the data stream is unused in the EC e2e — only the
// shard stream holds the stripe's fragments).
func mustNewShardNode(t *testing.T, n int) *V2Store {
	t.Helper()
	v, _ := newTestShardMultiStore(t, n)
	return v
}

// clusterV2Of returns a resolver mapping a node ID to its V2Store for a normal
// N-node cluster (node IDs 1..N map to nodes[NodeID-1]).
func clusterV2Of(nodes []*ecNode) func(uint64) *V2Store {
	return func(id uint64) *V2Store { return nodes[int(id)-1].v2 }
}

// buildECCluster spins up n node V2Stores, each with diskPerNode data + shard
// stores, and returns them along with a candidate ECDisk set mirroring the
// metadata E2 testShardDisks topology (NodeID = node, DiskID = node*100+dev),
// so PlanShards can draft a real §14-diverse placement across the machines.
func buildECCluster(t *testing.T, n, disksPerNode int) ([]*ecNode, []metadata.ECDisk) {
	t.Helper()
	nodes := make([]*ecNode, n)
	var disks []metadata.ECDisk
	for node := 1; node <= n; node++ {
		// Reuse the single-node shard-multistore helper; drop the dir slice (we
		// do not reopen across restart here).
		v, _ := newTestShardMultiStore(t, disksPerNode)
		nodes[node-1] = &ecNode{v2: v, node: metadata.NodeID(node)}
		for dev := 0; dev < disksPerNode; dev++ {
			disks = append(disks, metadata.ECDisk{
				NodeID: uint64(node),
				DiskID: uint64(node)*100 + uint64(dev),
			})
		}
	}
	return nodes, disks
}

// localShardFor maps a planned shard's (NodeID,DiskID) to the owning node index
// and that node's local shard-store index. It mirrors the E4 converter's
// resolveDisk: production routes to the local store owning the (node,disk);
// here the node's local store index is the disk's device unit.
func localShardFor(d metadata.ECDisk) (node int, local int) {
	return int(d.NodeID) - 1, int(d.DiskID - d.NodeID*100)
}

// writePlannedShards encodes payload into 6+3 and writes each shard onto its
// planned owning node's local shard store (the plan was drafted by
// ECStore.PlanShards). Returns the encoded shards so callers can also
// reconstruct the original.
func writePlannedShards(t *testing.T, nodes []*ecNode, plan []metadata.ECShard, cid metadata.ChunkID, payload []byte) [][]byte {
	t.Helper()
	all, err := encodeEC63(payload)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for i, shard := range all {
		node, local := localShardFor(metadata.ECDisk{NodeID: plan[i].NodeID, DiskID: plan[i].DiskID})
		if err := nodes[node].v2.WriteShardAtDisk(cid, i, local, shard); err != nil {
			t.Fatalf("node %d shard %d: %v", nodes[node].node, i, err)
		}
	}
	return all
}

// clusterReadShards reads all nine shards of a stripe from their owning nodes,
// returning each shard's bytes (nil when absent) and the missing indices. It
// defines the cross-node read primitive: shard i is owned by plan[i].NodeID,
// resolved through v2Of (which maps a node's fault-domain slot to its V2Store —
// in normal operation nodes[NodeID-1], and the replacement node when a lost
// machine's slot has been re-provisioned).
func clusterReadShards(plan []metadata.ECShard, cid metadata.ChunkID, v2Of func(nodeID uint64) *V2Store) ([][]byte, []int, error) {
	shards := make([][]byte, ec63Shards)
	var missing []int
	for i, s := range plan {
		data, _, err := v2Of(s.NodeID).ReadShard(cid, i)
		if err != nil {
			if err == storage.ErrExtentNotFound {
				missing = append(missing, i)
				continue
			}
			return nil, nil, err
		}
		shards[i] = data
	}
	return shards, missing, nil
}

// loseNode deletes every shard that a node owns, tombstoning them so the node
// is a lost fault domain (§14: losing one machine loses its ≤3 shards).
func loseNode(t *testing.T, nodes []*ecNode, plan []metadata.ECShard, victim metadata.NodeID, cid metadata.ChunkID) int {
	t.Helper()
	lost := 0
	for i, s := range plan {
		if s.NodeID == uint64(victim) {
			if err := nodes[int(victim)-1].v2.DeleteShard(cid, i); err != nil {
				t.Fatalf("DeleteShard node %d shard %d: %v", victim, i, err)
			}
			lost++
		}
	}
	return lost
}

// TestV2StoreEC_MultiNodeStripe_PlansWritesAggregates is the multi-node §14
// core: the metadata planner drafts a 6+3 placement across three DISTINCT node
// V2Stores, each shard lands byte-exact on its owning node's local shard store,
// and the aggregate decodes to the original payload (checked checksum-exact)
// by reading across all three machines.
func TestV2StoreEC_MultiNodeStripe_PlansWritesAggregates(t *testing.T) {
	nodes, disks := buildECCluster(t, 3, 3)
	ec := newTestECStore(t)

	// Plan across the three machines (each holds 3 shards, none >3, §14).
	st, err := ec.BeginConversion("mn-stripe-1", 20001, 1, 0)
	if err != nil {
		t.Fatalf("BeginConversion: %v", err)
	}
	if err := ec.PlanShards(st, disks); err != nil {
		t.Fatalf("PlanShards: %v", err)
	}
	if len(st.Shards) != ec63Shards {
		t.Fatalf("planned %d shards, want 9", len(st.Shards))
	}
	perNode := map[uint64]int{}
	for _, s := range st.Shards {
		perNode[s.NodeID]++
	}
	if len(perNode) != 3 {
		t.Fatalf("distinct nodes = %d, want 3", len(perNode))
	}
	for nodeID, cnt := range perNode {
		if cnt != 3 {
			t.Fatalf("node %d holds %d shards, want 3", nodeID, cnt)
		}
	}

	cid := metadata.ChunkID(20001)
	payload := bytes.Repeat([]byte("multinode-6+3-"), 500) // 8500 bytes
	writePlannedShards(t, nodes, st.Shards, cid, payload)

	// Shard bytes on each node's local store, byte-exact.
	for i, s := range st.Shards {
		node, _ := localShardFor(metadata.ECDisk{NodeID: s.NodeID, DiskID: s.DiskID})
		shard, _, err := nodes[node].v2.ReadShard(cid, i)
		if err != nil || len(shard) == 0 {
			t.Fatalf("node %d shard %d: len=%d err=%v", nodes[node].node, i, len(shard), err)
		}
	}

	// Aggregate across the three nodes decodes the original byte- and checksum-
	// exact — the same closed loop the metadata degrader relies on.
	shards, missing, err := clusterReadShards(st.Shards, cid, clusterV2Of(nodes))
	if err != nil || len(missing) != 0 {
		t.Fatalf("cluster read: missing=%v err=%v", missing, err)
	}
	got, err := decodeEC63(shards, len(payload))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("aggregate mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	if sum := storage.CRC32C(payload); sum == 0 {
		t.Fatalf("non-empty payload unexpectedly has a zero checksum")
	}
}

// TestV2StoreEC_MultiNode_DegradedReadAcrossNodes kills a whole node (its three
// shards tombstoned) and proves a cluster read still reconstructs the original
// byte-exact from the six surviving shards on the other two machines, reporting
// exactly the three lost indices — the §14 degraded read in a real 3-machine
// topology.
func TestV2StoreEC_MultiNode_DegradedReadAcrossNodes(t *testing.T) {
	nodes, disks := buildECCluster(t, 3, 3)
	ec := newTestECStore(t)

	cid := metadata.ChunkID(20002)
	payload := bytes.Repeat([]byte("mn-degraded-"), 700) // 7700 bytes
	st, _ := ec.BeginConversion("mn-stripe-2", 20002, 1, 0)
	if err := ec.PlanShards(st, disks); err != nil {
		t.Fatalf("PlanShards: %v", err)
	}
	writePlannedShards(t, nodes, st.Shards, cid, payload)

	// Lose machine 2 (its 3 shards): the stripe is degraded but readable.
	lost := loseNode(t, nodes, st.Shards, 2, cid)
	if lost != 3 {
		t.Fatalf("node 2 held %d shards, want 3", lost)
	}

	shards, missing, err := clusterReadShards(st.Shards, cid, clusterV2Of(nodes))
	if err != nil {
		t.Fatalf("cluster read: %v", err)
	}
	if len(missing) != 3 {
		t.Fatalf("missing = %v, want the 3 lost shards", missing)
	}
	got, err := decodeEC63(shards, len(payload))
	if err != nil {
		t.Fatalf("degraded decode: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("degraded mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

// TestV2StoreEC_MultiNode_RepairAcrossNodes rebuilds a lost node's shards from
// the six survivors onto a REPLACEMENT node taking the lost node's fault-domain
// slot, restoring a full byte-exact stripe that still honors §14 (every node at
// ≤3 shards — rebuilding onto an already-populated node would pile ≤6 shards on
// one machine and erode fault tolerance). The cluster aggregate then decodes the
// original byte-exact.
func TestV2StoreEC_MultiNode_RepairAcrossNodes(t *testing.T) {
	nodes, disks := buildECCluster(t, 3, 3)
	ec := newTestECStore(t)

	cid := metadata.ChunkID(20003)
	payload := bytes.Repeat([]byte("mn-repair-"), 800) // 7200 bytes
	st, _ := ec.BeginConversion("mn-stripe-3", 20003, 1, 0)
	if err := ec.PlanShards(st, disks); err != nil {
		t.Fatalf("PlanShards: %v", err)
	}
	writePlannedShards(t, nodes, st.Shards, cid, payload)

	// Lose machine 3 (its 3 shards are tombstoned on it).
	loseNode(t, nodes, st.Shards, 3, cid)

	// Rebuild the missing shards from the six survivors onto a fresh replacement
	// node that takes machine 3's slot in the fault domain, then re-read the full
	// stripe across the cluster and assert byte-exact original.
	shards, missing, err := clusterReadShards(st.Shards, cid, clusterV2Of(nodes))
	if err != nil || len(missing) != 3 {
		t.Fatalf("pre-repair: missing=%v err=%v", missing, err)
	}
	rebuilt, original, err := reconstructEC63(shards, len(payload))
	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if !bytes.Equal(original, payload) {
		t.Fatalf("reconstructed original mismatch")
	}
	replacement := &ecNode{v2: mustNewShardNode(t, 3), node: metadata.NodeID(4)}
	for _, idx := range missing {
		_, local := localShardFor(metadata.ECDisk{NodeID: st.Shards[idx].NodeID, DiskID: st.Shards[idx].DiskID})
		if err := replacement.v2.WriteShardAtDisk(cid, idx, local, rebuilt[idx]); err != nil {
			t.Fatalf("repair shard %d onto replacement: %v", idx, err)
		}
		// The replacement now owns the lost shard (takes machine 3's slot).
		st.Shards[idx].NodeID = 4
	}

	// Rebuild the aggregate over the live nodes (1,2) plus the replacement (4);
	// each of the three now holds exactly 3 shards, §14 intact.
	all := make([][]byte, ec63Shards)
	for idx := 0; idx < ec63Shards; idx++ {
		owner := int(st.Shards[idx].NodeID) - 1
		var ownerV2 *V2Store
		if owner == 3 { // node 4 replaced node 3's slot
			ownerV2 = replacement.v2
		} else {
			ownerV2 = nodes[owner].v2
		}
		b, _, err := ownerV2.ReadShard(cid, idx)
		if err != nil || len(b) == 0 {
			t.Fatalf("post-repair shard %d: len=%d err=%v", idx, len(b), err)
		}
		all[idx] = b
	}
	got, err := decodeEC63(all, len(payload))
	if err != nil {
		t.Fatalf("post-repair decode: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("post-repair mismatch")
	}
	// §14 held: no node hosts more than three shards of the repaired stripe.
	perNode := map[uint64]int{}
	for _, s := range st.Shards {
		perNode[s.NodeID]++
	}
	for nodeID, cnt := range perNode {
		if cnt > 3 {
			t.Fatalf("node %d holds %d shards after repair, max 3 (§14)", nodeID, cnt)
		}
	}
}

// TestV2StoreEC_MultiNode_ReheatFreshNode models whole replacement: a fresh
// node joins the cluster and ReheatChunkEC-equivalent rebuilds the complete
// nine-shard set from the six survivors onto the new machine, leaving a fully
// readable byte-exact stripe served from the replacement node.
func TestV2StoreEC_MultiNode_ReheatFreshNode(t *testing.T) {
	nodes, disks := buildECCluster(t, 3, 3)
	ec := newTestECStore(t)

	cid := metadata.ChunkID(20004)
	payload := bytes.Repeat([]byte("mn-reheat-"), 600) // 6600 bytes
	st, _ := ec.BeginConversion("mn-stripe-4", 20004, 1, 0)
	if err := ec.PlanShards(st, disks); err != nil {
		t.Fatalf("PlanShards: %v", err)
	}
	writePlannedShards(t, nodes, st.Shards, cid, payload)

	// Lose machine 1, then a brand-new machine (node 4) joins.
	loseNode(t, nodes, st.Shards, 1, cid)
	fresh := &ecNode{v2: mustNewShardNode(t, 3), node: metadata.NodeID(4)}

	// Reheat: rebuild the full 9 from the six survivors onto the fresh node.
	shards, missing, err := clusterReadShards(st.Shards, cid, clusterV2Of(nodes))
	if err != nil || len(missing) != 3 {
		t.Fatalf("pre-reheat: missing=%v err=%v", missing, err)
	}
	rebuilt, original, err := reconstructEC63(shards, len(payload))
	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if !bytes.Equal(original, payload) {
		t.Fatalf("reconstructed original mismatch")
	}
	for idx, shard := range rebuilt {
		if err := fresh.v2.WriteShardAtDisk(cid, idx, idx%3, shard); err != nil {
			t.Fatalf("reheat shard %d: %v", idx, err)
		}
	}

	// All nine shards now serve from the fresh node; aggregate decodes the
	// original byte-exact.
	all := make([][]byte, ec63Shards)
	for idx := 0; idx < ec63Shards; idx++ {
		b, _, err := fresh.v2.ReadShard(cid, idx)
		if err != nil || len(b) == 0 {
			t.Fatalf("fresh shard %d: len=%d err=%v", idx, len(b), err)
		}
		all[idx] = b
	}
	got, err := decodeEC63(all, len(payload))
	if err != nil {
		t.Fatalf("post-reheat decode: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("post-reheat mismatch")
	}
}
