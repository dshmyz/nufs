package chunkstore

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// This file is Program 6 Phase F1: the gateway (DatanodeChunkStore) read path
// served from a real V2.1 6+3 stripe. A V2.1 chunk's shards live as independent
// extents in each datanode's shard stream keyed (chunkID, gen=shardIndex+1),
// readable only via ReadECShard — the legacy V1 readECChunk used
// client.ReadChunk(chunk.ID) which finds nothing there. The ECStripeID
// discriminator branches the gateway read path onto ReadECShard, and the
// completed stripe (shards spanning the cluster) must read back byte-exact
// through the gateway exactly like a plain chunk.

// v21Node bundles one real TCP V2.1 datanode and its store.
type v21Node struct {
	srv *datanode.Server
	v2  *datanode.V2Store
}

// buildV21GatewayCluster starts n real TCP V2.1 datanodes (data store + attached
// shard stores per disk) plus one shared metadata authority. Mirror of the
// datanode buildClusterN helper, replicated here so the gateway (chunkstore)
// test can drive real servers without an import cycle.
func buildV21GatewayCluster(t *testing.T, n, disksPerNode int) ([]*v21Node, *metadata.PebbleStore) {
	t.Helper()
	var dataStoresAll, shardStoresAll []storage.Store
	for i := 0; i < n*disksPerNode; i++ {
		d := t.TempDir()
		ds, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 1})
		if err != nil {
			t.Fatalf("segment.New data %d: %v", i, err)
		}
		ss, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 2})
		if err != nil {
			t.Fatalf("segment.New shard %d: %v", i, err)
		}
		dataStoresAll = append(dataStoresAll, ds)
		shardStoresAll = append(shardStoresAll, ss)
		t.Cleanup(func() { _ = ss.Close() })
		t.Cleanup(func() { _ = ds.Close() })
	}
	nodes := make([]*v21Node, n)
	for i := 0; i < n; i++ {
		lo := i * disksPerNode
		hi := lo + disksPerNode
		v := datanode.NewMultiV2Store(dataStoresAll[lo:hi])
		if err := v.AttachShardStores(shardStoresAll[lo:hi]); err != nil {
			t.Fatalf("AttachShardStores node %d: %v", i, err)
		}
		cfg := datanode.DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.NodeID = metadata.NodeID(i + 1)
		cfg.RequestTimeout = 2 * time.Second
		srv := datanode.NewServer(cfg, v)
		if err := srv.Start(); err != nil {
			t.Fatalf("Start node %d: %v", i, err)
		}
		nodes[i] = &v21Node{srv: srv, v2: v}
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

// v21Topology builds the cluster's real candidate disks: n nodes x disksPerNode,
// DiskID = node*1000+disk (the §14 planner needs >=9 disks / >=3 distinct nodes).
func v21Topology(n, disksPerNode int) []metadata.ECDisk {
	var disks []metadata.ECDisk
	for node := uint64(1); node <= uint64(n); node++ {
		for d := 0; d < disksPerNode; d++ {
			disks = append(disks, metadata.ECDisk{NodeID: node, DiskID: node*1000 + uint64(d)})
		}
	}
	return disks
}

// TestGatewayReadV21ECShards converts a replicated chunk into a completed 6+3
// stripe across a 3-node V2.1 cluster, then serves it back byte-exact through
// the gateway DatanodeChunkStore — the read path that was broken before F1. It
// also covers the range read (ReadChunkRange mid-file) which applies the same
// decode+trim.
func TestGatewayReadV21ECShards(t *testing.T) {
	const (
		nNodes   = 3
		disksPer = 3 // 9 candidate shard stores (§14)
	)
	nodes, pb := buildV21GatewayCluster(t, nNodes, disksPer)
	nodesByID := map[uint64]*v21Node{}
	for i, nd := range nodes {
		nodesByID[uint64(i+1)] = nd
	}

	// Coordinator (node 1) holds the RF source extent being converted.
	coordStore := nodes[0].v2
	cid := metadata.ChunkID(88001)
	payload := []byte("gateway-v2.1-6+3-serving-read")
	for i := 0; i < 300; i++ {
		payload = append(payload, byte(i*13))
	}
	if err := coordStore.Write(cid, payload); err != nil {
		t.Fatalf("write replicated chunk: %v", err)
	}

	// Cross-node conversion driven by the shared metadata authority.
	auth := metadata.NewECStore(pb)
	svc := datanode.NewECService(coordStore, auth)
	svc.SetCrossNode(1, func(nodeID uint64) (*datanode.Client, bool) {
		nd, ok := nodesByID[nodeID]
		if !ok {
			return nil, false
		}
		return datanode.NewClient(nd.srv.Addr()), true
	})
	svc.SetCandidateDisks(func() []metadata.ECDisk { return v21Topology(nNodes, disksPer) })
	st, err := svc.ConvertToEC(context.Background(), cid, 1)
	if err != nil {
		t.Fatalf("ConvertToEC: %v", err)
	}
	if st.State != metadata.ECConversionComplete || len(st.Shards) != 9 {
		t.Fatalf("stripe state=%s shards=%d, want complete 9", st.State, len(st.Shards))
	}

	// Build the V2.1 gateway chunk: ECStripeID set (the discriminator that now
	// selects the ReadECShard branch), 9 replica placements with ShardIndex set,
	// each Addr pointing at the node that owns the shard.
	chunk := &metadata.ChunkMeta{
		ID:         cid,
		Size:       int32(len(payload)),
		State:      metadata.ChunkReady,
		ECStripeID: st.StripeID,
		ECGroup: &metadata.ECGroupInfo{
			GroupID:      st.StripeID,
			ProfileID:    metadata.DefaultECProfileID,
			DataShards:   metadata.ECDataShards,
			ParityShards: metadata.ECParityShards,
		},
	}
	for _, sh := range st.Shards {
		chunk.Replicas = append(chunk.Replicas, metadata.ReplicaInfo{
			NodeID:     metadata.NodeID(sh.NodeID),
			Addr:       nodesByID[sh.NodeID].srv.Addr(),
			State:      metadata.ReplicaReady,
			ShardIndex: sh.Index,
		})
	}

	cs := NewDatanodeChunkStore()
	defer cs.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Full read: byte-exact original via the gateway ReadECShard branch.
	got, err := cs.ReadChunk(ctx, chunk)
	if err != nil {
		t.Fatalf("gateway ReadChunk (V2.1): %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("gateway read mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	// Range read (mid-file window) decodes the same stripe and trims.
	off := int64(50)
	ln := int32(120)
	want := payload[off : off+int64(ln)]
	rng, err := cs.ReadChunkRange(ctx, chunk, off, ln)
	if err != nil {
		t.Fatalf("gateway ReadChunkRange (V2.1): %v", err)
	}
	if !bytes.Equal(rng, want) {
		t.Fatalf("range read mismatch: got %q, want %q", rng, want)
	}
}
