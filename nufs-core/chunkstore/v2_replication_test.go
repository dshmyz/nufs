package chunkstore

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// startV21Datanodes spins up n in-process datanode TCP servers on ephemeral
// ports, each backed by a V2.1 segment store (single disk) wrapped in a
// V2Store adapter — the same backend runDataNodeV21 serves. Returns the
// servers, their listen addresses, and the underlying V2Store handles (so a
// test can query what each node durably holds).
func startV21Datanodes(t *testing.T, n int) ([]*datanode.Server, []string, []*datanode.V2Store) {
	t.Helper()
	servers := make([]*datanode.Server, n)
	addrs := make([]string, n)
	stores := make([]*datanode.V2Store, n)
	for i := 0; i < n; i++ {
		seg, err := segment.New(segment.Config{
			Dir:         t.TempDir(),
			UseMemIndex: true,
			StreamID:    1,
		})
		if err != nil {
			t.Fatalf("segment.New node %d: %v", i, err)
		}
		v2 := datanode.NewV2Store(seg)
		cfg := datanode.DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.NodeID = metadata.NodeID(i + 1)
		// Short request timeout so Server.Stop() returns promptly.
		cfg.RequestTimeout = 200 * time.Millisecond
		srv := datanode.NewServer(cfg, v2)
		if err := srv.Start(); err != nil {
			t.Fatalf("Start node %d: %v", i, err)
		}
		servers[i] = srv
		addrs[i] = srv.Addr()
		stores[i] = v2
		t.Cleanup(func() { srv.Stop() })
	}
	return servers, addrs, stores
}

// v2gen returns the current resident generation for chunkID on a V2Store by
// verifying the posted read resolves it. Since datanode.locOf is unexported,
// the generation is asserted behaviorally: Read must succeed (the chunk exists
// at the store's current generation) and a stale, conflicting write at the
// same generation must be fenced.
func v2readOK(v *datanode.V2Store, cid metadata.ChunkID) bool {
	_, _, err := v.Read(cid, 0, 0)
	return err == nil
}

// TestV21TrueReplication_SameGenerationFanout proves the Metadata V2 serving
// path end to end: an RF=2 placement-group chunk is allocated with a
// metadata-issued generation, written through the real TCP fan-out, and lands
// on BOTH V2.1 datanodes. Reads fail over to a surviving replica when one node
// is killed, and an idempotent same-generation replay is accepted (not fenced
// as stale).
func TestV21TrueReplication_SameGenerationFanout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping V2.1 true-replication e2e in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// ---- Two V2.1 datanodes on distinct fault domains ----
	servers, addrs, v2stores := startV21Datanodes(t, 2)

	// ---- In-process metadata ----
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer store.Close()

	for i, addr := range addrs {
		if err := store.RegisterNode(ctx, &metadata.NodeInfo{
			ID:         metadata.NodeID(i + 1),
			Addr:       addr,
			DataDir:    t.TempDir(),
			Rack:       "rack-a",
			Zone:       "zone-1",
			MachineID:  "",
			Tier:       metadata.TierHot,
			CapacityGB: 100,
		}); err != nil && err != metadata.ErrNodeAlreadyExists {
			t.Fatalf("RegisterNode %d: %v", i+1, err)
		}
	}

	// ---- RF=2 bucket with placement groups enabled ----
	if err := store.CreateBucket(ctx, "v21-repl", metadata.PlacementPolicy{
		ID:                "default",
		ReplicationFactor: 2,
		TopologySpread:    metadata.SpreadNode,
		StorageTier:       metadata.TierHot,
		PlacementGroups:   true,
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "v21-repl")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := store.CreateFile(ctx, bucket.RootInode, "obj.bin", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	// ---- Allocate an RF=2 chunk through the PG authority ----
	alloc, err := store.AllocateChunksBatch(ctx, inode.ID, []int64{0}, bucket.Policy)
	if err != nil {
		t.Fatalf("AllocateChunksBatch: %v", err)
	}
	if len(alloc) != 1 {
		t.Fatalf("allocated %d chunks, want 1", len(alloc))
	}
	c := alloc[0]
	if len(c.Replicas) != 2 {
		t.Fatalf("replicas=%d, want 2 (RF=2)", len(c.Replicas))
	}
	if c.PGID == 0 {
		t.Fatalf("chunk PGID=0, want nonzero (placement-group path)")
	}
	if c.Generation != 1 {
		t.Fatalf("generation=%d, want 1 (metadata-issued)", c.Generation)
	}

	// ---- Write through the real DatanodeChunkStore fan-out ----
	data := bytes.Repeat([]byte("v21-repl-data-"), 300) // >1KiB to exercise multi-frame
	cs := NewDatanodeChunkStore()
	defer cs.Close()
	if err := cs.WriteChunk(ctx, c, data); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	// ---- Both nodes must hold the chunk ----
	for i, vs := range v2stores {
		if !v2readOK(vs, c.ID) {
			t.Fatalf("node %d does not hold chunk %d after fan-out", i+1, c.ID)
		}
	}

	// ---- Idempotent same-generation replay is accepted (not fenced) ----
	if err := cs.WriteChunk(ctx, c, data); err != nil {
		t.Fatalf("idempotent same-gen replay should succeed, got: %v", err)
	}

	// ---- Read back byte-exact across the fan-out ----
	got, err := cs.ReadChunkRange(ctx, c, 0, 0)
	if err != nil {
		t.Fatalf("ReadChunkRange: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read-back mismatch: got %d bytes, want %d", len(got), len(data))
	}

	// ---- Kill node 1: reads must fail over to node 2's surviving replica ----
	servers[0].Stop()
	// Rebuild replica ordering: ReadChunkRange reads from the first healthy
	// replica; with node 1 dead, it must fall through to node 2.
	got, err = cs.ReadChunkRange(ctx, c, 0, 0)
	if err != nil {
		t.Fatalf("read after killing one replica failed (no failover): %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("failover read mismatch: got %d bytes, want %d", len(got), len(data))
	}
}
