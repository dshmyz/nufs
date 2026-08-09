package datanode

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// startV21Nodes spins up n in-process V2.1 datanode TCP servers on ephemeral
// ports, each backed by a single-disk segment store wrapped in a V2Store —
// the same backend runDataNodeV21 serves. Returns the servers, their listen
// addresses, and the underlying V2Store handles.
func startV21Nodes(t *testing.T, n int) ([]*Server, []string, []*V2Store) {
	t.Helper()
	servers := make([]*Server, n)
	addrs := make([]string, n)
	stores := make([]*V2Store, n)
	for i := 0; i < n; i++ {
		seg, err := segment.New(segment.Config{
			Dir:         t.TempDir(),
			UseMemIndex: true,
			StreamID:    1,
		})
		if err != nil {
			t.Fatalf("segment.New node %d: %v", i, err)
		}
		v2 := NewV2Store(seg)
		cfg := DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.NodeID = metadata.NodeID(i + 1)
		cfg.RequestTimeout = 200 * time.Millisecond
		srv := NewServer(cfg, v2)
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

// v2Holds reports whether a V2Store can resolve a read of chunkID at its
// current resident generation (i.e. the datanode durably holds the chunk).
func v2Holds(v *V2Store, cid metadata.ChunkID) bool {
	_, _, err := v.Read(cid, 0, 0)
	return err == nil
}

// TestV21RepairFromSurvivor_RewindsAtMetadataGeneration proves the V2.1
// repair path end to end: when one replica of an RF=2 placement-group chunk
// is lost/failed, the RepairWorker (bound to a surviving node) finds the
// surviving replica over the real TCP wire and rewinds a fresh copy onto a
// healthy target node — landing on the metadata-issued generation, not a
// local bump — then updates metadata so the chunk stays fully replicated.
func TestV21RepairFromSurvivor_RewindsAtMetadataGeneration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping V2.1 repair e2e in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// ---- Three V2.1 datanodes on distinct fault domains ----
	_, addrs, v2stores := startV21Nodes(t, 3)

	// ---- In-process metadata authority ----
	meta, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer meta.Close()

	for i, addr := range addrs {
		if err := meta.RegisterNode(ctx, &metadata.NodeInfo{
			ID:         metadata.NodeID(i + 1),
			Addr:       addr,
			DataDir:    t.TempDir(),
			Rack:       "rack-a",
			Zone:       "zone-1",
			Tier:       metadata.TierHot,
			CapacityGB: 100,
		}); err != nil && err != metadata.ErrNodeAlreadyExists {
			t.Fatalf("RegisterNode %d: %v", i+1, err)
		}
	}

	// ---- RF=2 bucket with placement groups enabled ----
	if err := meta.CreateBucket(ctx, "v21-repair", metadata.PlacementPolicy{
		ID:                "default",
		ReplicationFactor: 2,
		TopologySpread:    metadata.SpreadNode,
		StorageTier:       metadata.TierHot,
		PlacementGroups:   true,
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := meta.GetBucket(ctx, "v21-repair")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := meta.CreateFile(ctx, bucket.RootInode, "obj.bin", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	// ---- Write an RF=2 chunk through the PG authority at generation 1 ----
	alloc, err := meta.AllocateChunksBatch(ctx, inode.ID, []int64{0}, bucket.Policy)
	if err != nil {
		t.Fatalf("AllocateChunksBatch: %v", err)
	}
	if len(alloc) != 1 || len(alloc[0].Replicas) != 2 {
		t.Fatalf("alloc=%d replicas, want 1 chunk with 2 replicas", len(alloc))
	}
	c := alloc[0]
	if c.Generation != 1 {
		t.Fatalf("generation=%d, want 1 (metadata-issued)", c.Generation)
	}
	if c.PGID == 0 {
		t.Fatalf("chunk PGID=0, want nonzero (placement-group path)")
	}

	data := bytes.Repeat([]byte("v21-repair-data-"), 300)
	// Write the chunk to every replica via the raw datanode wire (this is
	// the same protocol the wire-level replicator uses for repair).
	for _, r := range c.Replicas {
		rc := NewClient(r.Addr)
		defer rc.Close()
		if err := rc.Connect(); err != nil {
			t.Fatalf("connect %s: %v", r.Addr, err)
		}
		resp, err := rc.WriteChunkGen(c.ID, c.Generation, data)
		if err != nil {
			t.Fatalf("write to %s: %v", r.Addr, err)
		}
		if resp.Status != StatusOK {
			t.Fatalf("write to %s failed: %s", r.Addr, resp.Error)
		}
	}

	// The placement authority picks two of the three online nodes. Map the
	// replica node IDs to the V2Store handles (index = NodeID-1), and find
	// the third node that is not a replica — it is the repair target.
	survivor := c.Replicas[0].NodeID
	failedNode := c.Replicas[1].NodeID
	targetNode := metadata.NodeID(0)
	for id := metadata.NodeID(1); id <= 3; id++ {
		if id != survivor && id != failedNode {
			targetNode = id
			break
		}
	}
	if targetNode == 0 {
		t.Fatalf("could not find a non-replica node for repair target")
	}

	// Both replica nodes hold the chunk.
	for _, r := range c.Replicas {
		if !v2Holds(v2stores[r.NodeID-1], c.ID) {
			t.Fatalf("node %d does not hold chunk after write", r.NodeID)
		}
	}
	// The repair target does not hold it yet.
	if v2Holds(v2stores[targetNode-1], c.ID) {
		t.Fatalf("node %d unexpectedly holds chunk before repair", targetNode)
	}

	// Newly-allocated replicas are ReplicaSyncing until the owning node
	// reports them durable; mark both replica nodes Ready (this is what the
	// heartbeat / commit path does in production after the fan-out lands).
	if err := meta.ReportChunkState(ctx, survivor, map[metadata.ChunkID]metadata.ReplicaState{
		c.ID: metadata.ReplicaReady,
	}); err != nil {
		t.Fatalf("ReportChunkState survivor: %v", err)
	}
	if err := meta.ReportChunkState(ctx, failedNode, map[metadata.ChunkID]metadata.ReplicaState{
		c.ID: metadata.ReplicaReady,
	}); err != nil {
		t.Fatalf("ReportChunkState failed node: %v", err)
	}

	// ---- Fail the second replica: drop its local copy and mark it Failed ----
	if err := v2stores[failedNode-1].Delete(c.ID); err != nil {
		t.Fatalf("corrupt node %d: %v", failedNode, err)
	}
	if v2Holds(v2stores[failedNode-1], c.ID) {
		t.Fatalf("node %d still holds chunk after corruption", failedNode)
	}
	cur, err := meta.GetChunk(ctx, c.ID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	for i := range cur.Replicas {
		if cur.Replicas[i].NodeID == failedNode {
			cur.Replicas[i].State = metadata.ReplicaFailed
		}
	}
	if err := meta.UpdateChunk(ctx, cur); err != nil {
		t.Fatalf("UpdateChunk: %v", err)
	}
	if err := meta.TriggerRepair(ctx, c.ID); err != nil {
		t.Fatalf("TriggerRepair: %v", err)
	}

	// ---- Drive the repair worker bound to the surviving node ----
	replicator := NewReplicator(addrs[survivor-1], 4)
	replicator.Start()
	defer replicator.Stop()

	rw := NewRepairWorker(RepairConfig{
		Meta:       meta,
		NodeID:     survivor,
		Interval:   time.Hour, // not used; we drive processRepairQueue directly
		Replicator: replicator,
		LocalAddr:  addrs[survivor-1],
	})
	rw.processRepairQueue(ctx)

	// ---- Assert: a fresh replica now holds the byte-exact chunk ----
	modern, err := meta.GetChunk(ctx, c.ID)
	if err != nil {
		t.Fatalf("GetChunk after repair: %v", err)
	}
	if modern.Generation != 1 {
		t.Fatalf("generation after repair=%d, want 1 (metadata authority preserved)", modern.Generation)
	}
	var restored *metadata.ReplicaInfo
	for i := range modern.Replicas {
		if modern.Replicas[i].State == metadata.ReplicaReady && modern.Replicas[i].NodeID == targetNode {
			restored = &modern.Replicas[i]
		}
	}
	if restored == nil {
		t.Fatalf("repair did not move the failed replica onto an online node; replicas=%+v", modern.Replicas)
	}
	// The surviving node's copy is untouched, and the target now holds it.
	if !v2Holds(v2stores[survivor-1], c.ID) {
		t.Fatalf("surviving node %d lost its copy during repair", survivor)
	}
	if !v2Holds(v2stores[targetNode-1], c.ID) {
		t.Fatalf("repair target node %d does not hold the chunk after repair", targetNode)
	}
	got := readV21(v2stores[targetNode-1], c.ID)
	if !bytes.Equal(got, data) {
		t.Fatalf("repaired copy mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func readV21(v *V2Store, cid metadata.ChunkID) []byte {
	b, _, _ := v.Read(cid, 0, 0)
	return b
}
