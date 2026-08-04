package datanode

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/index"
	"github.com/example/dfs/datanode/storage/journal"
	"github.com/example/dfs/datanode/storage/segment"
	"github.com/example/dfs/metadata"
)

// v21Node bundles one in-process V2.1 datanode: the raw segment store (to
// reach its overlay for locating a record to corrupt), the V2Store adapter
// (the serving surface the heartbeat and TCP server bind to), the server,
// and the node's change journal — wired into the segment store exactly as
// runDataNodeV21 does, so a corrupt read emits a real EventCorrupt.
// dir is the segment store's config directory (segment files live under
// {dir}/segments/data/active/ for StreamID=1).
type v21Node struct {
	dir     string
	seg     *segment.Store
	v2      *V2Store
	srv     *Server
	journal *journal.ChangeJournal
}

// startV21NodesWithJournal spins up n V2.1 datanodes, each with its own
// change journal attached to its segment store (the serving-path wiring of
// runDataNodeV21). Returns the nodes.
func startV21NodesWithJournal(t *testing.T, n int) []*v21Node {
	t.Helper()
	nodes := make([]*v21Node, n)
	for i := 0; i < n; i++ {
		dir := t.TempDir()
		j, err := journal.OpenChangeJournal(journal.JournalOptions{Dir: filepath.Join(dir, "change-journal")})
		if err != nil {
			t.Fatalf("OpenChangeJournal node %d: %v", i, err)
		}
		seg, err := segment.New(segment.Config{
			Dir:           dir,
			UseMemIndex:   true,
			StreamID:      1,
			ChangeJournal: j,
		})
		if err != nil {
			t.Fatalf("segment.New node %d: %v", i, err)
		}
		v2 := NewV2Store(seg)
		cfg := DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.NodeID = metadata.NodeID(i + 1)
		cfg.RequestTimeout = 500 * time.Millisecond
		srv := NewServer(cfg, v2)
		if err := srv.Start(); err != nil {
			t.Fatalf("Start node %d: %v", i, err)
		}
		nodes[i] = &v21Node{dir: dir, seg: seg, v2: v2, srv: srv, journal: j}
		t.Cleanup(func() { srv.Stop(); _ = seg.Close(); _ = j.Close() })
	}
	return nodes
}

func (nd *v21Node) addr() string { return nd.srv.Addr() }

// corruptPayload flips a byte inside the record's stored payload region to
// simulate bitrot (§18.3 "bit flip"). The position is the record's last
// stored frame byte: `offset + RecordHeaderSize(55) + FrameIndexEntrySize(13)`
// locates the (single) frame's first byte, and `StoredLen-1` walks to its
// final byte. This is deliberately derived from the record's actual stored
// length rather than a fixed inset, because the frame payload may be
// zstd-compressed (the e2e body is highly repetitive and compresses below the
// 4 KiB no-compression threshold), so a hand-picked offset like 100 could
// land past the compressed frame in the trailer/next record and escape
// validation. Flipping the last stored frame byte is always inside the frame,
// so the frame CRC recomputed on the next read fails, ReadPayloadFrames
// returns ErrChecksumMismatch, and the segment store emits EventCorrupt.
func corruptPayload(t *testing.T, nd *v21Node, cid metadata.ChunkID, gen uint64) {
	t.Helper()
	key := index.Key(storage.ExtentID(cid), storage.Generation(gen))
	v, ok := nd.seg.Overlay().Get(key)
	if !ok {
		t.Fatalf("node %d: no committed location for chunk %d gen %d", nd.srv.cfg.NodeID, cid, gen)
	}
	path := filepath.Join(nd.dir, "segments", "data", "active", fmt.Sprintf("%d.seg", v.SegmentID))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment %s: %v", path, err)
	}
	pos := int(v.Offset) + 55 + 13 + int(v.StoredLen) - 1
	if pos < 0 || pos >= len(data) {
		t.Fatalf("segment too small to corrupt: pos=%d len=%d", pos, len(data))
	}
	data[pos] ^= 0xFF
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write segment: %v", err)
	}
}

// TestChangeJournalE2E_ReconcileRepairsFromSurvivor is the Task #56 capstone:
// it drives the full V2.1 cross-node change-journal loop through the real
// serving path. A replica's payload is bit-flipped on disk; a read detects
// the corruption and emits EventCorrupt to the node's change journal; the
// heartbeat ships it to the metadata authority, which marks that replica
// ReplicaFailed and enqueues a repair; and the RepairWorker (bound to a
// surviving node) heals a fresh byte-exact copy onto a healthy target over
// the datanode TCP wire — landing on the metadata-issued generation.
//
// This is the loop the unit tests cover in isolation (segment emit,
// heartbeat shipping, metadata reconcile, repair-from-survivor) but never
// tie together end to end.
func TestChangeJournalE2E_ReconcileRepairsFromSurvivor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping V2.1 change-journal e2e in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// ---- Three V2.1 datanodes, each with a change journal ----
	nodes := startV21NodesWithJournal(t, 3)

	// ---- In-process metadata authority ----
	meta, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer meta.Close()

	for i, nd := range nodes {
		if err := meta.RegisterNode(ctx, &metadata.NodeInfo{
			ID:         metadata.NodeID(i + 1),
			Addr:       nd.addr(),
			DataDir:    t.TempDir(),
			Rack:       "rack-a",
			Zone:       "zone-1",
			Tier:       metadata.TierHot,
			CapacityGB: 100,
		}); err != nil && err != metadata.ErrNodeAlreadyExists {
			t.Fatalf("RegisterNode %d: %v", i+1, err)
		}
	}

	// ---- RF=2 bucket with placement groups + a file ----
	if err := meta.CreateBucket(ctx, "v21-e2e", metadata.PlacementPolicy{
		ID:                "default",
		ReplicationFactor: 2,
		TopologySpread:    metadata.SpreadNode,
		StorageTier:       metadata.TierHot,
		PlacementGroups:   true,
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := meta.GetBucket(ctx, "v21-e2e")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := meta.CreateFile(ctx, bucket.RootInode, "obj.bin", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	// ---- Allocate a chunk through the PG authority at metadata generation 1 ----
	alloc, err := meta.AllocateChunksBatch(ctx, inode.ID, []int64{0}, bucket.Policy)
	if err != nil {
		t.Fatalf("AllocateChunksBatch: %v", err)
	}
	if len(alloc) != 1 || len(alloc[0].Replicas) != 2 {
		t.Fatalf("alloc=%d replicas, want 1 chunk with 2 replicas", len(alloc))
	}
	c := alloc[0]
	if c.Generation != 1 || c.PGID == 0 {
		t.Fatalf("chunk gen=%d pgid=%d, want metadata-issued gen=1 on PG path", c.Generation, c.PGID)
	}

	data := bytes.Repeat([]byte("v21-change-journal-e2e-"), 300) // 4.8 KiB, single frame
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

	// Map replica node IDs to nodes, and find the non-replica repair target.
	byNode := func(id metadata.NodeID) *v21Node { return nodes[id-1] }
	survivorID, corruptID := c.Replicas[0].NodeID, c.Replicas[1].NodeID
	targetID := metadata.NodeID(0)
	for id := metadata.NodeID(1); id <= 3; id++ {
		if id != survivorID && id != corruptID {
			targetID = id
			break
		}
	}
	if targetID == 0 {
		t.Fatalf("could not find a non-replica repair target")
	}

	// Both replicas hold the chunk; the target does not.
	for _, r := range c.Replicas {
		if !v2Holds(byNode(r.NodeID).v2, c.ID) {
			t.Fatalf("node %d does not hold chunk after write", r.NodeID)
		}
	}
	if v2Holds(byNode(targetID).v2, c.ID) {
		t.Fatalf("target node %d unexpectedly holds chunk before repair", targetID)
	}

	// Newly-allocated replicas are ReplicaSyncing until the owning node
	// reports them durable; mark both Ready (the fan-out/commit path does
	// this in production).
	for _, id := range []metadata.NodeID{survivorID, corruptID} {
		if err := meta.ReportChunkState(ctx, id, map[metadata.ChunkID]metadata.ReplicaState{
			c.ID: metadata.ReplicaReady,
		}); err != nil {
			t.Fatalf("ReportChunkState node %d: %v", id, err)
		}
	}

	// ---- Initial heartbeat (ships the ChunkStates delta; no events yet) ----
	hb := NewHeartbeatReporter(Config{NodeID: corruptID, HeartbeatInterval: time.Second}, meta, byNode(corruptID).v2)
	hb.SetChangeJournal(byNode(corruptID).journal)
	hb.send()

	// ---- Bit-flip the corruptee's on-disk payload, then a read detects it ----
	corruptPayload(t, byNode(corruptID), c.ID, c.Generation)
	if v2Holds(byNode(corruptID).v2, c.ID) {
		t.Fatalf("corrupt chunk still readable before heartbeat — expected checksum failure")
	}
	// That read hit the checksum mismatch and appended EventCorrupt; nothing
	// has acked it yet — the journal still holds one pending event.
	if p, _ := byNode(corruptID).journal.Pending(100, 1<<20); len(p) != 1 {
		t.Fatalf("corruptee journal pending=%d, want 1 EventCorrupt (%+v)", len(p), p)
	}

	// ---- Heartbeat ships the change event; metadata marks the replica
	//      failed and enqueues a repair; the journal is acked ----
	hb.send()

	cur, err := meta.GetChunk(ctx, c.ID)
	if err != nil {
		t.Fatalf("GetChunk after reconcile: %v", err)
	}
	var corruptState metadata.ReplicaState
	for _, r := range cur.Replicas {
		if r.NodeID == corruptID {
			corruptState = r.State
		}
	}
	if corruptState != metadata.ReplicaFailed {
		t.Fatalf("corruptee replica state=%v, want ReplicaFailed (change-journal reconciliation)", corruptState)
	}
	tasks, err := meta.GetRepairQueue(ctx)
	if err != nil {
		t.Fatalf("GetRepairQueue: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ChunkID != c.ID {
		t.Fatalf("repair queue=%+v, want 1 task for chunk %d", tasks, c.ID)
	}
	if p, _ := byNode(corruptID).journal.Pending(100, 1<<20); len(p) != 0 {
		t.Fatalf("journal not acked after metadata reconcile; still pending: %+v", p)
	}

	// ---- Repair heals from the surviving replica onto the target node ----
	replicator := NewReplicator(byNode(survivorID).addr(), 4)
	replicator.Start()
	defer replicator.Stop()

	rw := NewRepairWorker(RepairConfig{
		Meta:       meta,
		NodeID:     survivorID,
		Interval:   time.Hour, // not used; we drive processRepairQueue directly
		Replicator: replicator,
		LocalAddr:  byNode(survivorID).addr(),
	})
	rw.processRepairQueue(ctx)

	// The surviving node keeps its copy; the target now holds a byte-exact
	// replica at the metadata-issued generation; the chunk is fully healed.
	if !v2Holds(byNode(survivorID).v2, c.ID) {
		t.Fatalf("surviving node %d lost its copy during repair", survivorID)
	}
	if !v2Holds(byNode(targetID).v2, c.ID) {
		t.Fatalf("repair target node %d does not hold the chunk after repair", targetID)
	}
	if got, want := readV21(byNode(targetID).v2, c.ID), data; !bytes.Equal(got, want) {
		t.Fatalf("repaired copy mismatch: got %d bytes, want %d", len(got), len(want))
	}
	modern, err := meta.GetChunk(ctx, c.ID)
	if err != nil {
		t.Fatalf("GetChunk after repair: %v", err)
	}
	if modern.Generation != 1 {
		t.Fatalf("generation after repair=%d, want 1 (metadata authority preserved)", modern.Generation)
	}
	ready := 0
	for _, r := range modern.Replicas {
		if r.State == metadata.ReplicaReady {
			ready++
		}
	}
	if ready < 2 {
		t.Fatalf("chunk not healed: %d ready replicas, want >=2 (%+v)", ready, modern.Replicas)
	}
}
