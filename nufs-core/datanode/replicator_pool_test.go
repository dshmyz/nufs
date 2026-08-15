package datanode

import (
	"crypto/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func startTestServer(t *testing.T, nodeID metadata.NodeID) (*Server, *V2Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := segment.New(segment.Config{Dir: dir, UseMemIndex: true, StreamID: 1})
	if err != nil {
		t.Fatalf("segment.New (node %d): %v", nodeID, err)
	}
	v := NewMultiV2Store([]storage.Store{storage.Store(s)}, dir)
	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = nodeID
	srv := NewServer(cfg, v)
	if err := srv.Start(); err != nil {
		t.Fatalf("Server.Start (node %d): %v", nodeID, err)
	}
	t.Cleanup(func() { s.Close() })
	return srv, v, srv.listener.Addr().String()
}

// TestReplicator_ConnectionReuse verifies that multiple replication
// tasks to the same target reuse a single TCP connection rather than
// dialing anew each time. We measure this by checking the dial count
// exposed by the connection pool.
func TestReplicator_ConnectionReuse(t *testing.T) {
	srcSrv, _, srcAddr := startTestServer(t, 1)
	defer srcSrv.Stop()
	tgtSrv, tgtStore, tgtAddr := startTestServer(t, 2)
	defer tgtSrv.Stop()

	// Write 3 distinct chunks to source
	srcClient := NewClient(srcAddr)
	if err := srcClient.Connect(); err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcClient.Close()

	chunks := make([]metadata.ChunkID, 3)
	for i := range chunks {
		chunks[i] = metadata.ChunkID(100 + i)
		data := make([]byte, 4096)
		rand.Read(data)
		if resp, err := srcClient.WriteChunk(chunks[i], data); err != nil || resp.Status != StatusOK {
			t.Fatalf("write chunk %d: err=%v status=%v", chunks[i], err, resp)
		}
	}

	replicator := NewReplicator(srcAddr, 1) // single worker for deterministic dial counting
	replicator.Start()
	defer replicator.Stop()

	// Submit 3 replication tasks to the SAME target
	for _, cid := range chunks {
		task := ReplicationTask{
			ChunkID:    cid,
			SourceAddr: srcAddr,
			TargetAddr: tgtAddr,
			CreatedAt:  time.Now(),
		}
		if err := replicator.Submit(task); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	// Wait for all replications to complete
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		allDone := true
		for _, cid := range chunks {
			if _, ok := tgtStore.Info(cid); !ok {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify all chunks replicated
	for _, cid := range chunks {
		if _, ok := tgtStore.Info(cid); !ok {
			t.Fatalf("chunk %d not replicated to target", cid)
		}
	}

	// The connection pool should have dialed at most twice total:
	// once for source and once for target. All 3 tasks reuse these
	// connections. Any more indicates the pool is broken.
	dialCount := replicator.poolDialCount.Load()
	if dialCount > 2 {
		t.Errorf("expected at most 2 dials (1 source + 1 target), got %d (connection pool not reusing connections)", dialCount)
	}
}

// TestReplicator_PoolClosesOnStop verifies that all pooled
// connections are closed when the replicator stops.
func TestReplicator_PoolClosesOnStop(t *testing.T) {
	srcSrv, _, srcAddr := startTestServer(t, 10)
	defer srcSrv.Stop()
	tgtSrv, _, tgtAddr := startTestServer(t, 11)
	defer tgtSrv.Stop()

	// Write a chunk to source
	srcClient := NewClient(srcAddr)
	if err := srcClient.Connect(); err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcClient.Close()

	chunkID := metadata.ChunkID(200)
	data := make([]byte, 1024)
	rand.Read(data)
	srcClient.WriteChunk(chunkID, data)

	replicator := NewReplicator(srcAddr, 1)
	replicator.Start()

	task := ReplicationTask{
		ChunkID:    chunkID,
		SourceAddr: srcAddr,
		TargetAddr: tgtAddr,
		CreatedAt:  time.Now(),
	}
	replicator.Submit(task)

	// Wait for replication to establish a pooled connection
	time.Sleep(1 * time.Second)

	replicator.Stop()

	// After Stop, the pool should have no open connections
	openConns := replicator.poolOpenConns.Load()
	if openConns != 0 {
		t.Errorf("expected 0 open connections after Stop, got %d", openConns)
	}
}

// TestReplicator_PoolReconnectsAfterFailure verifies that if a pooled
// connection breaks, the next task establishes a new connection.
func TestReplicator_PoolReconnectsAfterFailure(t *testing.T) {
	srcSrv, _, srcAddr := startTestServer(t, 20)
	defer srcSrv.Stop()
	tgtSrv, tgtStore, tgtAddr := startTestServer(t, 21)
	defer tgtSrv.Stop()

	// Write chunk to source
	srcClient := NewClient(srcAddr)
	srcClient.Connect()
	defer srcClient.Close()

	chunkID := metadata.ChunkID(300)
	data := make([]byte, 1024)
	rand.Read(data)
	srcClient.WriteChunk(chunkID, data)

	replicator := NewReplicator(srcAddr, 1)
	replicator.Start()
	defer replicator.Stop()

	// First replication — establishes connection
	task1 := ReplicationTask{
		ChunkID:    chunkID,
		SourceAddr: srcAddr,
		TargetAddr: tgtAddr,
		CreatedAt:  time.Now(),
	}
	replicator.Submit(task1)
	time.Sleep(1 * time.Second)

	if _, ok := tgtStore.Info(chunkID); !ok {
		t.Fatal("first replication failed")
	}

	dialsAfterFirst := replicator.poolDialCount.Load()

	// Simulate connection failure by closing all pooled connections
	replicator.closeAllPooledConns()

	// Write a new chunk and replicate — should reconnect
	chunkID2 := metadata.ChunkID(301)
	data2 := make([]byte, 1024)
	rand.Read(data2)
	srcClient.WriteChunk(chunkID2, data2)

	task2 := ReplicationTask{
		ChunkID:    chunkID2,
		SourceAddr: srcAddr,
		TargetAddr: tgtAddr,
		CreatedAt:  time.Now(),
	}
	replicator.Submit(task2)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := tgtStore.Info(chunkID2); ok {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if _, ok := tgtStore.Info(chunkID2); !ok {
		t.Fatal("second replication after connection failure did not complete")
	}

	dialsAfterSecond := replicator.poolDialCount.Load()
	if dialsAfterSecond <= dialsAfterFirst {
		t.Errorf("expected new dial after connection failure: before=%d after=%d", dialsAfterFirst, dialsAfterSecond)
	}
}

// Ensure atomic is used
var _ atomic.Int64
