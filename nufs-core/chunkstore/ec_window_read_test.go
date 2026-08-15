package chunkstore

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// countingStore wraps a storage.Store and totals the bytes returned by Read,
// so window reads can assert transport amplification.
type countingStore struct {
	storage.Store
	readBytes atomic.Int64
}

func (c *countingStore) Read(ctx context.Context, req *storage.ReadRequest) (*storage.ReadResult, error) {
	res, err := c.Store.Read(ctx, req)
	if err == nil {
		c.readBytes.Add(int64(len(res.Data)))
	}
	return res, err
}

func totalReadBytes(stores []storage.Store) int64 {
	var total int64
	for _, s := range stores {
		if cs, ok := s.(*countingStore); ok {
			total += cs.readBytes.Load()
		}
	}
	return total
}

// convertToV21 writes payload as a plain chunk and converts it into a
// completed V2.1 6+3 stripe, returning the gateway-side chunk metadata.
func convertToV21(t *testing.T, nodes []*v21Node, pb *metadata.PebbleStore, cid metadata.ChunkID, payload []byte) *metadata.ChunkMeta {
	t.Helper()
	nodesByID := make(map[uint64]*v21Node, len(nodes))
	for i, nd := range nodes {
		nodesByID[uint64(i+1)] = nd
	}
	if err := nodes[0].v2.Write(cid, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	auth := metadata.NewECStore(pb)
	svc := datanode.NewECService(nodes[0].v2, auth)
	svc.SetCrossNode(1, func(nodeID uint64) (*datanode.Client, bool) {
		nd, ok := nodesByID[nodeID]
		if !ok {
			return nil, false
		}
		return datanode.NewClient(nd.srv.Addr()), true
	})
	svc.SetCandidateDisks(func() []metadata.ECDisk { return v21Topology(len(nodes), 3) })
	st, err := svc.ConvertToEC(context.Background(), cid, 1)
	if err != nil {
		t.Fatalf("ConvertToEC: %v", err)
	}
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
	return chunk
}

// v21WindowSetup builds a 3-node V2.1 cluster whose stores count served bytes,
// plus a 4 MiB converted EC chunk. 4 MiB spans several true shards
// (shardSize = ceil(4MiB/6) ≈ 699,051), so single-shard and straddling
// windows are both representable.
func v21WindowSetup(t *testing.T, cid metadata.ChunkID) ([]*v21Node, *metadata.PebbleStore, []storage.Store, *metadata.ChunkMeta, []byte) {
	t.Helper()
	nodes, pb, stores := buildV21GatewayClusterWith(t, 3, 3, func(s storage.Store) storage.Store {
		return &countingStore{Store: s}
	})
	payload := make([]byte, 4<<20)
	for i := range payload {
		payload[i] = byte(i*31 + 7)
	}
	chunk := convertToV21(t, nodes, pb, cid, payload)
	return nodes, pb, stores, chunk, payload
}

// TestECWindowRead_HealthyNoAmplification verifies a range read inside one
// data shard is served by exactly that shard's window bytes: no decode, no
// parity — transport amplification ≈ 1×.
func TestECWindowRead_HealthyNoAmplification(t *testing.T) {
	_, _, stores, chunk, payload := v21WindowSetup(t, metadata.ChunkID(50001))
	cs := NewDatanodeChunkStore()
	defer cs.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	off := int64(1 << 20) // 1 MiB — inside shard 1 (shardSize ≈ 699,051)
	ln := int32(4096)
	for _, s := range stores {
		s.(*countingStore).readBytes.Store(0)
	}
	got, err := cs.ReadChunkRange(ctx, chunk, off, ln)
	if err != nil {
		t.Fatalf("ReadChunkRange: %v", err)
	}
	if want := payload[off : off+int64(ln)]; !bytes.Equal(got, want) {
		t.Fatalf("data mismatch: got %d bytes, want %d", len(got), len(want))
	}
	if total := totalReadBytes(stores); total > int64(ln)*2 {
		t.Fatalf("transport amplification: %d bytes read for a %d-byte window (want ≈ 1×)", total, ln)
	}
}

// TestECWindowRead_StraddlesShardBoundary verifies a window crossing the
// boundary between two data shards is assembled from both shards' windows,
// still with ~1× amplification.
func TestECWindowRead_StraddlesShardBoundary(t *testing.T) {
	_, _, stores, chunk, payload := v21WindowSetup(t, metadata.ChunkID(50002))
	cs := NewDatanodeChunkStore()
	defer cs.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	shardSize := (int64(len(payload)) + int64(metadata.ECDataShards) - 1) / int64(metadata.ECDataShards)
	off := shardSize - 100 // crosses the shard 0/1 boundary
	ln := int32(200)
	for _, s := range stores {
		s.(*countingStore).readBytes.Store(0)
	}
	got, err := cs.ReadChunkRange(ctx, chunk, off, ln)
	if err != nil {
		t.Fatalf("ReadChunkRange: %v", err)
	}
	if want := payload[off : off+int64(ln)]; !bytes.Equal(got, want) {
		t.Fatalf("data mismatch: got %d bytes, want %d", len(got), len(want))
	}
	if total := totalReadBytes(stores); total > int64(ln)*2 {
		t.Fatalf("transport amplification: %d bytes read for a %d-byte window (want ≈ 1×)", total, ln)
	}
}

// TestECWindowRead_DegradedReconstructsFromPeers stops the datanode owning
// the window's shard and verifies the window is reconstructed byte-exact
// from peer shard windows — a window-sized reconstruction (K × window), not
// a shard-sized one.
func TestECWindowRead_DegradedReconstructsFromPeers(t *testing.T) {
	nodes, _, stores, chunk, payload := v21WindowSetup(t, metadata.ChunkID(50003))
	cs := NewDatanodeChunkStore()
	defer cs.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	off := int64(1 << 20) // inside shard 1
	ln := int32(4096)

	var owner metadata.NodeID
	for _, rep := range chunk.Replicas {
		if rep.ShardIndex == 1 {
			owner = rep.NodeID
			break
		}
	}
	nodes[int(owner)-1].srv.Stop()

	for _, s := range stores {
		s.(*countingStore).readBytes.Store(0)
	}
	got, err := cs.ReadChunkRange(ctx, chunk, off, ln)
	if err != nil {
		t.Fatalf("ReadChunkRange degraded: %v", err)
	}
	if want := payload[off : off+int64(ln)]; !bytes.Equal(got, want) {
		t.Fatalf("degraded data mismatch: got %d bytes, want %d", len(got), len(want))
	}
	// 8 peers × window bytes for reconstruction; allow a little slack.
	if total := totalReadBytes(stores); total > int64(ln)*9 {
		t.Fatalf("degraded amplification: %d bytes read for %d-byte window", total, ln)
	}
}

// TestECWindowRead_UnrecoverableError verifies that when fewer than K peer
// windows are available the read fails with an explicit error — no hang, no
// panic, no partial data.  (The window path's error falls back to the full
// read, which reports its own insufficient-shards error.)
func TestECWindowRead_UnrecoverableError(t *testing.T) {
	nodes, _, _, chunk, _ := v21WindowSetup(t, metadata.ChunkID(50004))
	cs := NewDatanodeChunkStore()
	defer cs.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	off := int64(1 << 20) // inside shard 1
	ln := int32(4096)

	var owner metadata.NodeID
	for _, rep := range chunk.Replicas {
		if rep.ShardIndex == 1 {
			owner = rep.NodeID
			break
		}
	}
	// Stop the window's shard owner and one other node: the surviving node
	// holds at most 3 shards (< K=6), so neither the window reconstruction
	// nor the full read can succeed.
	nodes[int(owner)-1].srv.Stop()
	other := owner%3 + 1 // never equals owner for owner ∈ {1,2,3}
	nodes[int(other)-1].srv.Stop()

	_, err := cs.ReadChunkRange(ctx, chunk, off, ln)
	if err == nil {
		t.Fatalf("expected error for unrecoverable window, got nil")
	}
	if !strings.Contains(err.Error(), "insufficient shards") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestECWindowRead_CapSizeFallsBackToFullRead verifies that a directly-written
// EC chunk whose chunk.Size is the MaxChunkSize allocation cap (the S3 ECConfig
// write path never updates it) still reads correctly: the window math would
// point past the true shard extents, fails validation, and the read falls back
// to the full-read path, which derives the padded length from the shards.
func TestECWindowRead_CapSizeFallsBackToFullRead(t *testing.T) {
	_, _, _, chunk, payload := v21WindowSetup(t, metadata.ChunkID(50005))
	chunk.Size = int32(metadata.MaxChunkSize) // direct-EC write leaves the cap
	cs := NewDatanodeChunkStore()
	defer cs.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// A window that the cap-sized math places past the true shard extents.
	off := int64(1 << 20) // 1 MiB — beyond the true shard 0 extent (≈699,051 B)
	ln := int32(4096)
	got, err := cs.ReadChunkRange(ctx, chunk, off, ln)
	if err != nil {
		t.Fatalf("ReadChunkRange: %v", err)
	}
	if want := payload[off : off+int64(ln)]; !bytes.Equal(got, want) {
		t.Fatalf("data mismatch: got %d bytes, want %d", len(got), len(want))
	}
}
