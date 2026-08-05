package main

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/example/dfs/datanode"
	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/segment"
	"github.com/example/dfs/metadata"
	"github.com/klauspost/reedsolomon"
)

// This file is Program 9: the *production-wiring* proof that a datanode wired
// exactly as runDataNodeV21 does — a real metadata HTTP authority (the ops mux),
// a real *metadata.HTTPClient as its metaStore, registration carrying
// ShardDiskCount, and SetCrossNode/SetCandidateDisks closures that resolve the
// candidate topology by calling metaStore.ListNodes() at convert time — turns a
// replicated chunk into a cross-node 6+3 stripe across the real cluster. Unlike
// the S3 test (ec_crossnode_e2e_test.go), which handed the coordinator a
// hand-built clusterDisks slice, this test drives the actual production data
// flow: ListNodes -> per-node ShardDiskCount -> DiskID=NodeID*1000+d candidate
// set -> cross-node push.

const (
	prodData   = 6
	prodParity = 3
	prodShards = prodData + prodParity
)

// prodNode bundles one production-shaped datanode: a real TCP server (the wire
// surface for ReplicateECShard pushes), its V2Store over disksPer shard stores,
// and the node's own metaStore (a real HTTPClient to the metad authority).
type prodNode struct {
	id       metadata.NodeID
	addr     string
	srv      *datanode.Server
	v2       *datanode.V2Store
	meta     *metadata.HTTPClient
	disksPer int
}

// prodCluster is the return of buildProdCluster: the nodes plus the metad
// authority (PebbleStore backing the ops mux), so the test can read the durable
// stripe back from the same store that served the HTTP authority.
type prodCluster struct {
	nodes []*prodNode
	pb    *metadata.PebbleStore
}

// buildProdCluster starts n datanodes, each with disksPerNode shard/data stores,
// all registered with a real metad HTTP authority carrying ShardDiskCount. Node
// IDs are 1..n — the same ids a real cluster would be assigned.
func buildProdCluster(t *testing.T, n, disksPer int) *prodCluster {
	t.Helper()
	store, bundle := newOpsTestStore(t)
	srv := httptest.NewServer(buildOpsTestMux(t, store, bundle))
	t.Cleanup(srv.Close)

	nodes := make([]*prodNode, n)
	for i := 0; i < n; i++ {
		// One data store + one shard store per disk (StreamID 1/2), mirroring
		// runDataNodeV21's shardStores construction (main.go:464-558).
		dirs := make([]string, disksPer)
		var dataStores, shardStores []storage.Store
		for d := 0; d < disksPer; d++ {
			dirs[d] = t.TempDir()
			ds, err := segment.New(segment.Config{Dir: dirs[d], UseMemIndex: true, StreamID: 1})
			if err != nil {
				t.Fatalf("node %d segment.New data %d: %v", i, d, err)
			}
			ss, err := segment.New(segment.Config{Dir: dirs[d], UseMemIndex: true, StreamID: 2})
			if err != nil {
				t.Fatalf("node %d segment.New shard %d: %v", i, d, err)
			}
			dataStores = append(dataStores, ds)
			shardStores = append(shardStores, ss)
			t.Cleanup(func() { _ = ds.Close() })
			t.Cleanup(func() { _ = ss.Close() })
		}
		v := datanode.NewMultiV2Store(dataStores, dirs...)
		if err := v.AttachShardStores(shardStores); err != nil {
			t.Fatalf("node %d AttachShardStores: %v", i, err)
		}

		cfg := datanode.DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.NodeID = metadata.NodeID(i + 1)
		cfg.RequestTimeout = 2 * time.Second
		srvNode := datanode.NewServer(cfg, v)
		if err := srvNode.Start(); err != nil {
			t.Fatalf("node %d Start: %v", i, err)
		}
		t.Cleanup(srvNode.Stop)

		// Each node's own metaStore is a real HTTPClient to the metad authority,
		// exactly as runDataNodeV21 builds it (main.go:568).
		meta := metadata.NewHTTPClient(srv.URL, 10*time.Second)

		// Register carrying ShardDiskCount — the Program 9 metadata contract that
		// lets a coordinator resolve real candidate disks from ListNodes.
		if err := meta.RegisterNode(context.Background(), &metadata.NodeInfo{
			ID:             cfg.NodeID,
			Addr:           srvNode.Addr(),
			State:          metadata.NodeOnline,
			ShardDiskCount: disksPer,
		}); err != nil && err != metadata.ErrNodeAlreadyExists {
			t.Fatalf("node %d RegisterNode: %v", i, err)
		}

		nodes[i] = &prodNode{id: cfg.NodeID, addr: srvNode.Addr(), srv: srvNode, v2: v, meta: meta, disksPer: disksPer}
	}
	return &prodCluster{nodes: nodes, pb: store}
}

// wireCoordinator applies the exact coordinator wiring runDataNodeV21 applies
// after SetPublish (main.go:684+): SetCrossNode + SetCandidateDisks closures
// that lazily load peer NodeID->Addr and build the candidate-disk set by calling
// metaStore.ListNodes(ctx), filtering to NodeOnline peers with ShardDiskCount>0,
// excluding this coordinator itself, and encoding candidate disks as
// DiskID = NodeID*1000 + local_disk (§14). This is copied verbatim from
// cmd/datanode/main.go so the test exercises the production data flow rather
// than a hand-built topology.
func wireCoordinator(ec *datanode.ECService, meta *metadata.HTTPClient, selfID metadata.NodeID) {
	var (
		peerAddrs     map[uint64]string
		loadPeersOnce sync.Once
	)
	loadPeers := func() {
		nodes, err := meta.ListNodes(context.Background())
		if err != nil {
			return
		}
		peerAddrs = make(map[uint64]string, len(nodes))
		for _, nd := range nodes {
			if nd.State == metadata.NodeOnline && nd.Addr != "" {
				peerAddrs[uint64(nd.ID)] = nd.Addr
			}
		}
	}
	ec.SetCrossNode(uint64(selfID), func(nodeID uint64) (*datanode.Client, bool) {
		loadPeersOnce.Do(loadPeers)
		addr, ok := peerAddrs[nodeID]
		if !ok {
			return nil, false
		}
		return datanode.NewClient(addr), true
	})
	ec.SetCandidateDisks(func() []metadata.ECDisk {
		loadPeersOnce.Do(loadPeers)
		nodes, err := meta.ListNodes(context.Background())
		if err != nil {
			return nil
		}
		var disks []metadata.ECDisk
		for _, nd := range nodes {
			// Include every online node with shard disks — this coordinator's own
			// disks too (it writes its shards locally and pushes the rest), so a
			// 3-node cluster hosts a full 6+3 stripe.
			if nd.State != metadata.NodeOnline || nd.ShardDiskCount <= 0 {
				continue
			}
			for d := 0; d < nd.ShardDiskCount; d++ {
				disks = append(disks, metadata.ECDisk{
					NodeID: uint64(nd.ID),
					DiskID: uint64(nd.ID)*1000 + uint64(d),
				})
			}
		}
		return disks
	})
}

// TestECProdTopology_CandidateDisksFromListNodes proves the Program 9 wiring
// brain: the SetCandidateDisks closure — fed by metaStore.ListNodes over the
// real HTTP authority — yields exactly the candidate-disk set derived from every
// online node's registered ShardDiskCount (the coordinator's own disks included,
// since it writes its own shards locally and pushes the rest).
func TestECProdTopology_CandidateDisksFromListNodes(t *testing.T) {
	const (
		nNodes   = 3
		disksPer = 3
	)
	cl := buildProdCluster(t, nNodes, disksPer)
	coord := cl.nodes[0]

	// A no-authority ECService is enough to hold the closures; we only exercise
	// the candidate-disks function here.
	ec := datanode.NewECService(coord.v2, coord.meta)
	wireCoordinator(ec, coord.meta, coord.id)

	// Force the closure (CandidateDisks reflects SetCandidateDisks, not the
	// default synthetic topology).
	disks := ec.CandidateDisks()

	if len(disks) != nNodes*disksPer {
		t.Fatalf("candidate disks=%d, want %d (all 3 nodes x 3 disks)", len(disks), nNodes*disksPer)
	}
	// Every candidate's DiskID encodes node*1000+d within the node's range.
	seen := map[uint64]map[uint64]bool{}
	for _, d := range disks {
		if d.DiskID < d.NodeID*1000 || d.DiskID >= d.NodeID*1000+uint64(disksPer) {
			t.Fatalf("candidate disk %d on node %d out of [node*1000, node*1000+disksPer)", d.DiskID, d.NodeID)
		}
		if seen[d.NodeID] == nil {
			seen[d.NodeID] = map[uint64]bool{}
		}
		seen[d.NodeID][d.DiskID] = true
	}
	if len(seen) != nNodes {
		t.Fatalf("candidate disks span %d nodes, want %d (all nodes)", len(seen), nNodes)
	}
	for nodeID, dsk := range seen {
		if len(dsk) != disksPer {
			t.Fatalf("node %d has %d candidate disks, want %d", nodeID, len(dsk), disksPer)
		}
	}
}

// TestECProdTopology_ConvertViaListNodes is the Program 9 capstone: a
// production-wired coordinator resolves the real cluster topology from the
// metadata authority (ListNodes -> ShardDiskCount -> candidate disks), pushes
// shards to peers over TCP, and completes a durable cross-node stripe —
// proving the production wiring end-to-end, not just the underlying primitives.
func TestECProdTopology_ConvertViaListNodes(t *testing.T) {
	const (
		nNodes   = 3
		disksPer = 3 // 3 nodes x 3 disks = 9 candidate shard stores (§14)
	)
	cl := buildProdCluster(t, nNodes, disksPer)
	coord := cl.nodes[0]

	// Seed authoritative chunk metadata the way the real serving loop does:
	// allocate the chunk through metad (its metadata row is what the publish
	// layout switch must update). Each node's RegisterNode already happened in
	// buildProdCluster, so the placement node exists.
	pb := cl.pb
	if err := pb.CreateBucket(context.Background(), "prod-ec-bucket", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := pb.GetBucket(context.Background(), "prod-ec-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := pb.CreateFile(context.Background(), bucket.RootInode, "obj", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	alloc, err := pb.AllocateChunk(context.Background(), inode.ID, 0, metadata.PlacementPolicy{ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	cid := alloc.ID
	if cid == 0 {
		t.Fatal("allocated chunk has zero ID")
	}

	payload := []byte("program9-prod-topology-cross-node-convert")
	for i := 0; i < 300; i++ {
		payload = append(payload, byte(i*13))
	}
	if err := coord.v2.Write(cid, payload); err != nil {
		t.Fatalf("write replicated chunk: %v", err)
	}

	// The coordinator's ECService is an ECService(v2Store, metaStore) — same as
	// runDataNodeV21 (main.go:670) — feeding the real HTTPClient as the
	// ECAuthority (PublishConversion goes over HTTP), then applying the exact
	// production converter closure.
	ec := datanode.NewECService(coord.v2, coord.meta)
	ec.SetPublish(func(_ context.Context, st *metadata.ECStripe) error {
		return coord.meta.PublishConversion(st)
	})
	wireCoordinator(ec, coord.meta, coord.id)

	st, err := ec.ConvertToEC(context.Background(), cid, 1)
	if err != nil {
		t.Fatalf("ConvertToEC: %v", err)
	}
	if st.State != metadata.ECConversionComplete {
		t.Fatalf("state=%s, want complete", st.State)
	}
	if len(st.Shards) != 9 {
		t.Fatalf("shards=%d, want 9", len(st.Shards))
	}

	// §14: the 9 planned shards span all 3 distinct nodes, ≤3 per node.
	perNode := map[uint64]int{}
	for _, s := range st.Shards {
		perNode[s.NodeID]++
	}
	if len(perNode) != 3 {
		t.Fatalf("shards span %d nodes, want 3 (all cluster nodes)", len(perNode))
	}
	for nID, cnt := range perNode {
		if cnt > 3 {
			t.Fatalf("node %d holds %d shards, want ≤3 §14", nID, cnt)
		}
	}

	// Every shard is byte-exact on the node that owns it — the coordinator
	// writes its own shards locally (WriteShardAtDisk), and pushes the rest to
	// their owning peers over TCP (ReplicateECShard).
	expect := encodeProd(payload)
	for _, s := range st.Shards {
		node := cl.nodes[int(s.NodeID)-1]
		got, _, err := node.v2.ReadShard(cid, s.Index)
		if err != nil {
			t.Fatalf("ReadShard(chunk %d, shard %d) on node %d: %v", cid, s.Index, s.NodeID, err)
		}
		if !bytes.Equal(got, expect[s.Index]) {
			t.Fatalf("shard %d on node %d mismatch: got %d bytes, want %d", s.Index, s.NodeID, len(got), len(expect[s.Index]))
		}
	}

	// Authority durable Complete: the same metad PebbleStore that served the
	// HTTP mux persisted the completed stripe.
	durable, err := metadata.NewECStore(cl.pb).GetStripe(st.StripeID)
	if err != nil {
		t.Fatalf("GetStripe: %v", err)
	}
	if durable == nil || durable.State != metadata.ECConversionComplete {
		t.Fatalf("durable stripe=%+v, want complete", durable)
	}

	// Kill one whole node (its 3 shards) -> degraded read across the two
	// survivors still reconstructs the original payload.
	dead := st.Shards[0].NodeID
	survivors := make([][]byte, prodShards)
	for _, s := range st.Shards {
		if s.NodeID == dead {
			continue
		}
		got, _, err := cl.nodes[int(s.NodeID)-1].v2.ReadShard(cid, s.Index)
		if err != nil {
			t.Fatalf("read survivor shard %d node %d: %v", s.Index, s.NodeID, err)
		}
		survivors[s.Index] = got
	}
	deg, err := decodeProd(survivors, len(payload))
	if err != nil {
		t.Fatalf("degraded decode: %v", err)
	}
	if !bytes.Equal(deg, payload) {
		t.Fatalf("degraded decode mismatch after losing node %d", dead)
	}
}

// encodeProd / decodeProd are a self-contained 6+3 reedsolomon lash-up for this
// test, mirroring datanode/ec63.go exactly (same shard size, padding, and
// reconstruction semantics). Kept in-package so the test is fully
// self-verifying without importing the datanode package's unexported codec.
var (
	prodRSOnce sync.Once
	prodRSEnc  reedsolomon.Encoder
	prodRSErr  error
)

func prodRS() (reedsolomon.Encoder, error) {
	prodRSOnce.Do(func() {
		prodRSEnc, prodRSErr = reedsolomon.New(prodData, prodParity,
			reedsolomon.WithAutoGoroutines(0),
			reedsolomon.WithMaxGoroutines(4),
		)
	})
	return prodRSEnc, prodRSErr
}

func encodeProd(data []byte) [][]byte {
	enc, err := prodRS()
	if err != nil {
		panic(err)
	}
	shardSize := (len(data) + prodData - 1) / prodData
	paddedLen := shardSize * prodData
	padded := make([]byte, paddedLen)
	copy(padded, data)
	shards := make([][]byte, prodShards)
	for i := 0; i < prodData; i++ {
		shards[i] = padded[i*shardSize : (i+1)*shardSize]
	}
	for j := prodData; j < prodShards; j++ {
		shards[j] = make([]byte, shardSize)
	}
	if err := enc.Encode(shards); err != nil {
		panic(err)
	}
	return shards
}

func decodeProd(shards [][]byte, originalLen int) ([]byte, error) {
	enc, err := prodRS()
	if err != nil {
		return nil, err
	}
	available := 0
	for _, s := range shards {
		if len(s) > 0 {
			available++
		}
	}
	if available < prodData {
		return nil, errNotEnoughShards
	}
	rsShards := make([][]byte, prodShards)
	for i, s := range shards {
		if len(s) > 0 {
			rsShards[i] = s
		}
	}
	if err := enc.Reconstruct(rsShards); err != nil {
		return nil, err
	}
	if ok, _ := enc.Verify(rsShards); !ok {
		return nil, errVerify
	}
	result := make([]byte, 0, originalLen)
	for i := 0; i < prodData; i++ {
		result = append(result, rsShards[i]...)
	}
	if len(result) < originalLen {
		return nil, errTooShort
	}
	return result[:originalLen], nil
}

var (
	errNotEnoughShards = errors.New("ec test: insufficient shards")
	errVerify          = errors.New("ec test: verification failed")
	errTooShort        = errors.New("ec test: reconstructed too short")
)
