package datanode

import (
	"bytes"
	"crypto/rand"
	"hash/crc32"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

// startTestServer creates a test server with a temp directory and returns it along with its address.
func startTestServer(t *testing.T, nodeID metadata.NodeID) (*Server, *ChunkStore, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatalf("NewChunkStore (node %d): %v", nodeID, err)
	}
	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = nodeID
	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatalf("Server.Start (node %d): %v", nodeID, err)
	}
	return srv, store, srv.listener.Addr().String()
}

func TestReplicator_BasicReplication(t *testing.T) {
	// Start source and target servers
	srcSrv, _, srcAddr := startTestServer(t, 1)
	defer srcSrv.Stop()
	tgtSrv, tgtStore, tgtAddr := startTestServer(t, 2)
	defer tgtSrv.Stop()

	// Write chunk to source
	srcClient := NewClient(srcAddr)
	if err := srcClient.Connect(); err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcClient.Close()

	chunkID := metadata.ChunkID(42)
	data := make([]byte, 4096)
	rand.Read(data)

	resp, err := srcClient.WriteChunk(chunkID, data)
	if err != nil {
		t.Fatalf("write to source: %v", err)
	}
	if resp.Status != StatusOK {
		t.Fatalf("write to source status: %v", resp.Status)
	}

	// Create replicator and submit task
	replicator := NewReplicator(srcAddr, 2)
	replicator.Start()
	defer replicator.Stop()

	task := ReplicationTask{
		ChunkID:    chunkID,
		SourceAddr: srcAddr,
		TargetAddr: tgtAddr,
		CreatedAt:  time.Now(),
	}
	if err := replicator.Submit(task); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Wait for replication to complete
	time.Sleep(2 * time.Second)

	// Verify target has the chunk
	info, ok := tgtStore.Info(chunkID)
	if !ok {
		t.Fatal("target should have replicated chunk")
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("replicated size mismatch: got %d, want %d", info.Size, len(data))
	}

	// Read from target and verify data
	tgtClient := NewClient(tgtAddr)
	if err := tgtClient.Connect(); err != nil {
		t.Fatalf("connect target: %v", err)
	}
	defer tgtClient.Close()

	readResp, err := tgtClient.ReadChunk(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("read from target: %v", err)
	}
	if !bytes.Equal(readResp.Data, data) {
		t.Fatal("replicated data mismatch")
	}
	if readResp.Checksum != crc32.ChecksumIEEE(data) {
		t.Fatal("replicated checksum mismatch")
	}
}

func TestReplicator_SubmitReplication(t *testing.T) {
	srcSrv, _, srcAddr := startTestServer(t, 1)
	defer srcSrv.Stop()
	tgt1Srv, _, tgt1Addr := startTestServer(t, 2)
	defer tgt1Srv.Stop()
	tgt2Srv, _, tgt2Addr := startTestServer(t, 3)
	defer tgt2Srv.Stop()

	// Write to source
	srcClient := NewClient(srcAddr)
	srcClient.Connect()
	defer srcClient.Close()

	chunkID := metadata.ChunkID(100)
	data := []byte("multi-target replication test")
	srcClient.WriteChunk(chunkID, data)

	// Submit replication to multiple targets
	replicator := NewReplicator(srcAddr, 4)
	replicator.Start()
	defer replicator.Stop()

	targets := []metadata.ReplicaInfo{
		{NodeID: 1, Addr: srcAddr}, // self, should be skipped
		{NodeID: 2, Addr: tgt1Addr},
		{NodeID: 3, Addr: tgt2Addr},
	}
	replicator.SubmitReplication(chunkID, srcAddr, targets)

	// Wait for replication
	time.Sleep(3 * time.Second)

	// Verify both targets
	for _, addr := range []string{tgt1Addr, tgt2Addr} {
		c := NewClient(addr)
		c.Connect()
		resp, err := c.ReadChunk(chunkID, 0, 0)
		c.Close()
		if err != nil {
			t.Fatalf("read from %s: %v", addr, err)
		}
		if !bytes.Equal(resp.Data, data) {
			t.Fatalf("data mismatch on %s", addr)
		}
	}
}

func TestReplicator_RetryOnFailure(t *testing.T) {
	// Source server
	srcSrv, _, srcAddr := startTestServer(t, 1)
	defer srcSrv.Stop()

	// Write to source
	srcClient := NewClient(srcAddr)
	srcClient.Connect()
	defer srcClient.Close()

	chunkID := metadata.ChunkID(200)
	srcClient.WriteChunk(chunkID, []byte("retry test"))

	// Target on invalid address (will fail)
	badAddr := "127.0.0.1:1" // port 1 should refuse connections

	replicator := NewReplicator(srcAddr, 1)
	replicator.Start()
	defer replicator.Stop()

	task := ReplicationTask{
		ChunkID:    chunkID,
		SourceAddr: srcAddr,
		TargetAddr: badAddr,
		CreatedAt:  time.Now(),
	}
	if err := replicator.Submit(task); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Wait enough time for retries (3 retries with exponential backoff)
	time.Sleep(8 * time.Second)

	// The test verifies no panic/crash occurs during retry attempts.
	// The replication should fail gracefully after max retries.
}

func TestReplicator_Repair(t *testing.T) {
	srcSrv, _, srcAddr := startTestServer(t, 1)
	defer srcSrv.Stop()
	newSrv, newStore, newAddr := startTestServer(t, 4)
	defer newSrv.Stop()

	// Write to source
	srcClient := NewClient(srcAddr)
	srcClient.Connect()
	defer srcClient.Close()

	chunkID := metadata.ChunkID(300)
	data := []byte("repair test data - replacing lost replica")
	srcClient.WriteChunk(chunkID, data)

	// Repair: copy from surviving source to new target
	replicator := NewReplicator(srcAddr, 2)
	replicator.Start()
	defer replicator.Stop()

	err := replicator.Repair(ChunkRepairTask{
		ChunkID:       chunkID,
		SurvivingAddr: srcAddr,
		NewTargetAddr: newAddr,
	})
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}

	// Verify new target
	info, ok := newStore.Info(chunkID)
	if !ok {
		t.Fatal("repair target should have chunk")
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("repair size mismatch: got %d, want %d", info.Size, len(data))
	}
}

func TestReplicator_ShutdownClean(t *testing.T) {
	replicator := NewReplicator("127.0.0.1:0", 2)
	replicator.Start()

	// Submit a task then immediately stop
	_ = replicator.Submit(ReplicationTask{
		ChunkID:    metadata.ChunkID(1),
		SourceAddr: "127.0.0.1:9999",
		TargetAddr: "127.0.0.1:9998",
	})

	// Should not deadlock
	done := make(chan struct{})
	go func() {
		replicator.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Clean shutdown
	case <-time.After(5 * time.Second):
		t.Fatal("replicator shutdown deadlocked")
	}
}

func TestReplicator_QueueFull(t *testing.T) {
	replicator := NewReplicator("127.0.0.1:0", 1)
	// Don't start workers - queue will fill up

	// Fill the queue (capacity 1024)
	for i := 0; i < 1024; i++ {
		err := replicator.Submit(ReplicationTask{
			ChunkID:    metadata.ChunkID(uint64(i)),
			SourceAddr: "127.0.0.1:9999",
			TargetAddr: "127.0.0.1:9998",
		})
		if err != nil {
			t.Fatalf("Submit %d: unexpected error: %v", i, err)
		}
	}

	// Next submit should fail (queue full)
	err := replicator.Submit(ReplicationTask{
		ChunkID:    metadata.ChunkID(9999),
		SourceAddr: "127.0.0.1:9999",
		TargetAddr: "127.0.0.1:9998",
	})
	if err == nil {
		t.Fatal("expected queue full error")
	}

	// Clean shutdown
	replicator.Start()
	replicator.Stop()
}
