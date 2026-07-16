package datanode

import (
	"context"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

// TestE2E_DataNodeMetaDIntegration tests the full data path:
//
//	metad: CreateBucket → AllocateChunk
//	datanode: WriteChunk → ReadChunk
//	metad: CommitChunk → GetChunk (verify)
//
// This test starts a real datanode TCP server and a real metad PebbleStore
// (not a mock), exercising the actual interaction between the two services.
func TestE2E_DataNodeMetaDIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e integration test in short mode")
	}
	if os.Getenv("NUFS_E2E_TEST") != "1" {
		t.Skip("skipping e2e test; set NUFS_E2E_TEST=1 to enable")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ---- Setup metad (PebbleStore) ----
	metaDir := t.TempDir()
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: metaDir})
	if err != nil {
		t.Fatalf("create PebbleStore: %v", err)
	}
	defer store.Close()

	// ---- Setup datanode ----
	dataDir := t.TempDir()
	wal, err := NewWriteAheadLog(filepath.Join(dataDir, "wal"))
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}

	chunkStore, err := NewChunkStore(dataDir, 16, 16, wal)
	if err != nil {
		t.Fatalf("create ChunkStore: %v", err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = 1
	cfg.CapacityGB = 100

	srv := NewServer(cfg, chunkStore)
	if err := srv.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer srv.Stop()

	dnAddr := srv.listener.Addr().String()
	t.Logf("datanode listening on %s", dnAddr)

	// Step 1: Register datanode with metad
	t.Run("RegisterNode", func(t *testing.T) {
		err := store.RegisterNode(ctx, &metadata.NodeInfo{
			ID:         1,
			Addr:       dnAddr,
			DataDir:    dataDir,
			Rack:       "rack-1",
			Zone:       "zone-1",
			Tier:       metadata.TierHot,
			CapacityGB: 100,
		})
		if err != nil && err != metadata.ErrNodeAlreadyExists {
			t.Fatalf("register node: %v", err)
		}
	})

	// Step 2: Create bucket
	t.Run("CreateBucket", func(t *testing.T) {
		err := store.CreateBucket(ctx, "e2e-test-bucket", metadata.PlacementPolicy{
			ID:                "default",
			ReplicationFactor: 1,
		})
		if err != nil {
			t.Fatalf("create bucket: %v", err)
		}
	})

	// Step 3: Allocate chunk via metad
	var chunkID metadata.ChunkID
	t.Run("AllocateChunk", func(t *testing.T) {
		bucket, err := store.GetBucket(ctx, "e2e-test-bucket")
		if err != nil {
			t.Fatalf("get bucket: %v", err)
		}

		// Create a file inode first
		dirInode, err := store.MkDir(ctx, bucket.RootInode, "testdir", 0755)
		if err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		fileInode, err := store.CreateFile(ctx, dirInode.ID, "testfile.dat", 0644)
		if err != nil {
			t.Fatalf("create file: %v", err)
		}

		// Allocate a chunk for the file
		chunkMeta, err := store.AllocateChunk(ctx, fileInode.ID, 0, metadata.PlacementPolicy{
			ID:                "default",
			ReplicationFactor: 1,
		})
		if err != nil {
			t.Fatalf("allocate chunk: %v", err)
		}
		chunkID = chunkMeta.ID
		t.Logf("allocated chunk %d with %d replicas", chunkID, len(chunkMeta.Replicas))
	})

	// Step 4: Write chunk data to datanode
	t.Run("WriteChunk", func(t *testing.T) {
		client := NewClient(dnAddr)
		if err := client.Connect(); err != nil {
			t.Fatalf("connect to datanode: %v", err)
		}
		defer client.Close()

		data := make([]byte, 4096)
		for i := range data {
			data[i] = byte(i % 256)
		}

		resp, err := client.WriteChunk(chunkID, data)
		if err != nil {
			t.Fatalf("write chunk: %v", err)
		}
		if resp.Status != StatusOK {
			t.Fatalf("write status: %v, error: %s", resp.Status, resp.Error)
		}
		t.Logf("wrote %d bytes to chunk %d", resp.Length, chunkID)
	})

	// Step 5: Read chunk data back from datanode
	t.Run("ReadChunk", func(t *testing.T) {
		client := NewClient(dnAddr)
		if err := client.Connect(); err != nil {
			t.Fatalf("connect to datanode: %v", err)
		}
		defer client.Close()

		resp, err := client.ReadChunk(chunkID, 0, 0)
		if err != nil {
			t.Fatalf("read chunk: %v", err)
		}
		if resp.Status != StatusOK {
			t.Fatalf("read status: %v, error: %s", resp.Status, resp.Error)
		}
		if resp.Length != 4096 {
			t.Fatalf("read length: got %d, want 4096", resp.Length)
		}

		// Verify data integrity
		for i := 0; i < 4096; i++ {
			if resp.Data[i] != byte(i%256) {
				t.Fatalf("data mismatch at offset %d: got %d, want %d", i, resp.Data[i], byte(i%256))
			}
		}
		t.Logf("read %d bytes from chunk %d, data verified", resp.Length, chunkID)
	})

	// Step 6: Commit chunk in metad
	t.Run("CommitChunk", func(t *testing.T) {
		checksum := crc32.ChecksumIEEE(make([]byte, 4096))
		err := store.CommitChunk(ctx, chunkID, checksum)
		if err != nil {
			t.Fatalf("commit chunk: %v", err)
		}
		t.Logf("committed chunk %d in metad", chunkID)
	})

	// Step 7: Verify chunk metadata in metad
	t.Run("VerifyChunkMeta", func(t *testing.T) {
		meta, err := store.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("get chunk: %v", err)
		}
		if meta.ID != chunkID {
			t.Fatalf("chunk ID mismatch: got %d, want %d", meta.ID, chunkID)
		}
		t.Logf("chunk meta verified: id=%d, size=%d, state=%d", meta.ID, meta.Size, meta.State)
	})
}

// TestE2E_GracefulShutdown tests that the datanode drains in-flight writes
// before shutting down, and data survives the restart.
func TestE2E_GracefulShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	dataDir := t.TempDir()
	wal, err := NewWriteAheadLog(filepath.Join(dataDir, "wal"))
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}

	chunkStore, err := NewChunkStore(dataDir, 16, 16, wal)
	if err != nil {
		t.Fatalf("create ChunkStore: %v", err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = 1

	srv := NewServer(cfg, chunkStore)
	if err := srv.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}

	addr := srv.listener.Addr().String()

	// Write some data
	client := NewClient(addr)
	if err := client.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}

	data := []byte("graceful shutdown test data")
	resp, err := client.WriteChunk(metadata.ChunkID(100), data)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if resp.Status != StatusOK {
		t.Fatalf("write status: %v", resp.Status)
	}
	client.Close()

	// Stop server gracefully
	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()

	// Server should stop within a reasonable time
	select {
	case <-done:
		t.Log("server stopped gracefully")
	case <-time.After(10 * time.Second):
		t.Fatal("server did not stop within 10 seconds")
	}

	// Verify data persisted — create new store and read
	chunkStore2, err := NewChunkStore(dataDir, 16, 16, nil)
	if err != nil {
		t.Fatalf("reopen chunk store: %v", err)
	}

	readData, _, err := chunkStore2.Read(metadata.ChunkID(100), 0, 0)
	if err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	if string(readData) != string(data) {
		t.Fatalf("data mismatch after restart: got %q, want %q", string(readData), string(data))
	}
	t.Log("data survived graceful shutdown")
}

// TestE2E_MultipleChunkWrites tests writing multiple chunks and verifying
// they are all readable.
func TestE2E_MultipleChunkWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	dataDir := t.TempDir()
	wal, err := NewWriteAheadLog(filepath.Join(dataDir, "wal"))
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}

	chunkStore, err := NewChunkStore(dataDir, 16, 16, wal)
	if err != nil {
		t.Fatalf("create ChunkStore: %v", err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = 1

	srv := NewServer(cfg, chunkStore)
	if err := srv.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()
	client := NewClient(addr)
	if err := client.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	// Write 100 chunks
	const numChunks = 100
	for i := 0; i < numChunks; i++ {
		data := []byte(fmt.Sprintf("chunk-data-%04d", i))
		resp, err := client.WriteChunk(metadata.ChunkID(i), data)
		if err != nil {
			t.Fatalf("write chunk %d: %v", i, err)
		}
		if resp.Status != StatusOK {
			t.Fatalf("write chunk %d status: %v", i, resp.Status)
		}
	}

	// Read all chunks back and verify
	for i := 0; i < numChunks; i++ {
		expected := fmt.Sprintf("chunk-data-%04d", i)
		resp, err := client.ReadChunk(metadata.ChunkID(i), 0, 0)
		if err != nil {
			t.Fatalf("read chunk %d: %v", i, err)
		}
		if resp.Status != StatusOK {
			t.Fatalf("read chunk %d status: %v", i, resp.Status)
		}
		if string(resp.Data) != expected {
			t.Fatalf("chunk %d data mismatch: got %q, want %q", i, string(resp.Data), expected)
		}
	}

	// Verify stats
	totalBytes, chunkCount := chunkStore.Stats()
	if chunkCount != numChunks {
		t.Fatalf("chunk count: got %d, want %d", chunkCount, numChunks)
	}
	t.Logf("wrote and verified %d chunks (%d bytes)", numChunks, totalBytes)
}

// ============================================================
// 3-Node Raft Cluster + Multi-Datanode E2E Integration Test
// ============================================================

// TestE2E_RaftClusterFullPipeline tests the complete production path:
//  1. Start a 3-node Raft cluster (metad)
//  2. Start 3 datanode TCP servers
//  3. Register datanodes with metad
//  4. Create bucket → allocate chunk → write data → read back → commit
//  5. Kill leader → verify failover → verify data still accessible
//  6. Verify data consistency across Raft nodes
func TestE2E_RaftClusterFullPipeline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e Raft cluster test in short mode")
	}
	if os.Getenv("NUFS_E2E_TEST") != "1" {
		t.Skip("skipping e2e Raft cluster test; set NUFS_E2E_TEST=1 to enable")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	// ---- Create 3 Raft metad nodes ----
	type metadNode struct {
		store *metadata.PebbleStore
		raft  *metadata.RaftNode
		dir   string
		addr  string
	}

	const numNodes = 3
	nodes := make([]*metadNode, numNodes)
	raftAddrs := make([]string, numNodes)

	// Allocate ports
	for i := 0; i < numNodes; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("allocate port: %v", err)
		}
		raftAddrs[i] = ln.Addr().String()
		ln.Close()
	}

	// Create stores and Raft nodes
	for i := 0; i < numNodes; i++ {
		nodeDir := filepath.Join(tmpDir, fmt.Sprintf("metad-%d", i))
		raftDir := filepath.Join(nodeDir, "raft")

		store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: nodeDir})
		if err != nil {
			t.Fatalf("create store %d: %v", i, err)
		}

		peers := make([]string, 0, numNodes-1)
		for j := 0; j < numNodes; j++ {
			if j != i {
				peers = append(peers, fmt.Sprintf("node-%d", j+1))
			}
		}

		preVote := true
		rn, err := metadata.NewRaftNode(store, metadata.RaftNodeConfig{
			NodeID:             fmt.Sprintf("node-%d", i+1),
			BindAddr:           raftAddrs[i],
			RaftDir:            raftDir,
			Bootstrap:          i == 0,        // Only first node bootstraps
			Peers:              raftAddrs[1:], // Peer addresses for bootstrap
			HeartbeatTimeout:   2 * time.Second,
			ElectionTimeout:    2 * time.Second,
			LeaderLeaseTimeout: 1 * time.Second,
			SnapshotThreshold:  1024,
			SnapshotInterval:   30 * time.Second,
			TrailingLogs:       2048,
			PreVote:            &preVote,
		})
		if err != nil {
			store.Close()
			t.Fatalf("create raft node %d: %v", i, err)
		}

		nodes[i] = &metadNode{
			store: store,
			raft:  rn,
			dir:   nodeDir,
			addr:  raftAddrs[i],
		}
	}

	// Cleanup
	defer func() {
		for _, n := range nodes {
			if n.raft != nil {
				n.raft.Shutdown()
			}
			if n.store != nil {
				n.store.Close()
			}
		}
	}()

	// Wait for leader election
	t.Log("waiting for Raft leader election...")
	var leaderNode *metadNode
	for attempt := 0; attempt < 60; attempt++ {
		for _, n := range nodes {
			if n.raft.IsLeader() {
				leaderNode = n
				break
			}
		}
		if leaderNode != nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if leaderNode == nil {
		t.Fatal("no leader elected after 30 seconds")
	}
	t.Logf("leader elected: %s at %s", leaderNode.raft.NodeID(), leaderNode.addr)

	// ---- Start 3 datanode TCP servers ----
	type dnNode struct {
		store *ChunkStore
		srv   *Server
		addr  string
		dir   string
	}

	dnNodes := make([]*dnNode, numNodes)
	for i := 0; i < numNodes; i++ {
		dnDir := filepath.Join(tmpDir, fmt.Sprintf("datanode-%d", i))
		wal, err := NewWriteAheadLog(filepath.Join(dnDir, "wal"))
		if err != nil {
			t.Fatalf("create WAL %d: %v", i, err)
		}

		cs, err := NewChunkStore(dnDir, 16, 16, wal)
		if err != nil {
			t.Fatalf("create ChunkStore %d: %v", i, err)
		}

		cfg := DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.NodeID = metadata.NodeID(i + 1)
		cfg.CapacityGB = 100

		srv := NewServer(cfg, cs)
		if err := srv.Start(); err != nil {
			t.Fatalf("start datanode %d: %v", i, err)
		}

		dnNodes[i] = &dnNode{
			store: cs,
			srv:   srv,
			addr:  srv.listener.Addr().String(),
			dir:   dnDir,
		}
	}
	defer func() {
		for _, dn := range dnNodes {
			if dn.srv != nil {
				dn.srv.Stop()
			}
		}
	}()

	t.Logf("started %d datanodes", numNodes)
	for i, dn := range dnNodes {
		t.Logf("  datanode-%d: %s", i+1, dn.addr)
	}

	// Step 1: Register all datanodes with the leader's store
	t.Run("RegisterDatanodes", func(t *testing.T) {
		for i, dn := range dnNodes {
			err := leaderNode.store.RegisterNode(ctx, &metadata.NodeInfo{
				ID:         metadata.NodeID(i + 1),
				Addr:       dn.addr,
				DataDir:    dn.dir,
				Rack:       fmt.Sprintf("rack-%d", i),
				Zone:       "zone-1",
				Tier:       metadata.TierHot,
				CapacityGB: 100,
			})
			if err != nil && err != metadata.ErrNodeAlreadyExists {
				t.Fatalf("register node %d: %v", i+1, err)
			}
		}
	})

	// Step 2: Create bucket with replication factor 3
	t.Run("CreateBucket", func(t *testing.T) {
		err := leaderNode.store.CreateBucket(ctx, "raft-e2e-bucket", metadata.PlacementPolicy{
			ID:                "default",
			ReplicationFactor: 3,
		})
		if err != nil {
			t.Fatalf("create bucket: %v", err)
		}
	})

	// Step 3: Allocate chunk and write data
	var chunkID metadata.ChunkID
	t.Run("WritePath", func(t *testing.T) {
		bucket, err := leaderNode.store.GetBucket(ctx, "raft-e2e-bucket")
		if err != nil {
			t.Fatalf("get bucket: %v", err)
		}

		// Create directory and file
		dirInode, err := leaderNode.store.MkDir(ctx, bucket.RootInode, "data", 0755)
		if err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		fileInode, err := leaderNode.store.CreateFile(ctx, dirInode.ID, "test.bin", 0644)
		if err != nil {
			t.Fatalf("create file: %v", err)
		}

		// Allocate chunk
		chunkMeta, err := leaderNode.store.AllocateChunk(ctx, fileInode.ID, 0, metadata.PlacementPolicy{
			ID:                "default",
			ReplicationFactor: 3,
		})
		if err != nil {
			t.Fatalf("allocate chunk: %v", err)
		}
		chunkID = chunkMeta.ID
		t.Logf("allocated chunk %d with %d replicas", chunkID, len(chunkMeta.Replicas))

		// Write data to all replica datanodes
		testData := make([]byte, 8192)
		for i := range testData {
			testData[i] = byte(i % 256)
		}

		for _, replica := range chunkMeta.Replicas {
			client := NewClient(replica.Addr)
			if err := client.Connect(); err != nil {
				t.Fatalf("connect to datanode %s: %v", replica.Addr, err)
			}

			resp, err := client.WriteChunk(chunkID, testData)
			client.Close()
			if err != nil {
				t.Fatalf("write chunk to %s: %v", replica.Addr, err)
			}
			if resp.Status != StatusOK {
				t.Fatalf("write status from %s: %v", replica.Addr, resp.Status)
			}
			t.Logf("wrote %d bytes to datanode %s", resp.Length, replica.Addr)
		}
	})

	// Step 4: Read data back from each replica
	t.Run("ReadPath", func(t *testing.T) {
		chunkMeta, err := leaderNode.store.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("get chunk: %v", err)
		}

		for _, replica := range chunkMeta.Replicas {
			client := NewClient(replica.Addr)
			if err := client.Connect(); err != nil {
				t.Fatalf("connect to datanode %s: %v", replica.Addr, err)
			}

			resp, err := client.ReadChunk(chunkID, 0, 0)
			client.Close()
			if err != nil {
				t.Fatalf("read chunk from %s: %v", replica.Addr, err)
			}
			if resp.Status != StatusOK {
				t.Fatalf("read status from %s: %v", replica.Addr, resp.Status)
			}
			if resp.Length != 8192 {
				t.Fatalf("read length from %s: got %d, want 8192", replica.Addr, resp.Length)
			}

			// Verify data integrity
			for i := 0; i < 8192; i++ {
				if resp.Data[i] != byte(i%256) {
					t.Fatalf("data mismatch at offset %d from %s", i, replica.Addr)
				}
			}
			t.Logf("verified %d bytes from datanode %s", resp.Length, replica.Addr)
		}
	})

	// Step 5: Commit chunk in metad
	t.Run("CommitChunk", func(t *testing.T) {
		checksum := crc32.ChecksumIEEE(make([]byte, 8192))
		err := leaderNode.store.CommitChunk(ctx, chunkID, checksum)
		if err != nil {
			t.Fatalf("commit chunk: %v", err)
		}
		t.Logf("committed chunk %d", chunkID)
	})

	// Step 6: Leader failover — kill the current leader
	t.Run("LeaderFailover", func(t *testing.T) {
		oldLeaderID := leaderNode.raft.NodeID()
		t.Logf("shutting down leader %s...", oldLeaderID)

		leaderNode.raft.Shutdown()

		// Wait for new leader
		var newLeader *metadNode
		for attempt := 0; attempt < 60; attempt++ {
			for _, n := range nodes {
				if n == leaderNode {
					continue
				}
				if n.raft.IsLeader() {
					newLeader = n
					break
				}
			}
			if newLeader != nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if newLeader == nil {
			t.Fatal("no new leader elected after failover")
		}
		t.Logf("new leader elected: %s at %s", newLeader.raft.NodeID(), newLeader.addr)

		// Verify data is still accessible via new leader
		bucket, err := newLeader.store.GetBucket(ctx, "raft-e2e-bucket")
		if err != nil {
			t.Fatalf("get bucket after failover: %v", err)
		}
		if bucket.Name != "raft-e2e-bucket" {
			t.Fatalf("bucket name mismatch after failover: %s", bucket.Name)
		}

		chunkMeta, err := newLeader.store.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("get chunk after failover: %v", err)
		}
		if chunkMeta.ID != chunkID {
			t.Fatalf("chunk ID mismatch after failover: got %d, want %d", chunkMeta.ID, chunkID)
		}
		t.Logf("data verified after failover: bucket=%s, chunk=%d", bucket.Name, chunkMeta.ID)

		leaderNode = newLeader
	})

	// Step 7: Verify data consistency across surviving Raft nodes
	t.Run("RaftConsistency", func(t *testing.T) {
		for _, n := range nodes {
			if n.raft == nil {
				continue
			}
			if !n.raft.IsLeader() && n.raft.LeaderAddr() == "" {
				continue // Node is shut down
			}

			bucket, err := n.store.GetBucket(ctx, "raft-e2e-bucket")
			if err != nil {
				t.Errorf("node %s: get bucket: %v", n.raft.NodeID(), err)
				continue
			}
			if bucket.Name != "raft-e2e-bucket" {
				t.Errorf("node %s: bucket name mismatch: %s", n.raft.NodeID(), bucket.Name)
			}
		}
	})

	// Step 8: AntiEntropy scan on a datanode
	t.Run("AntiEntropyScan", func(t *testing.T) {
		ae := NewAntiEntropy(dnNodes[0].store, leaderNode.store, 1)
		result, err := ae.Scan(ctx)
		if err != nil {
			t.Fatalf("anti-entropy scan: %v", err)
		}
		t.Logf("anti-entropy scan: scanned=%d, mismatches=%d, repaired=%d",
			result.ChunksScanned, result.Mismatches, result.Repaired)
	})
}

// TestE2E_LifecyclePrefixPruning tests that lifecycle rules with prefix
// matching correctly prune directory subtrees that cannot match.
func TestE2E_LifecyclePrefixPruning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a single metad store
	metaDir := t.TempDir()
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: metaDir})
	if err != nil {
		t.Fatalf("create PebbleStore: %v", err)
	}
	defer store.Close()

	// Create bucket
	err = store.CreateBucket(ctx, "lifecycle-test", metadata.PlacementPolicy{
		ID:                "default",
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	bucket, err := store.GetBucket(ctx, "lifecycle-test")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}

	// Create directory structure:
	//   /logs/2024/app.log
	//   /logs/2025/app.log
	//   /cache/data.bin
	//   /important/doc.txt
	logsDir, err := store.MkDir(ctx, bucket.RootInode, "logs", 0755)
	if err != nil {
		t.Fatalf("mkdir logs: %v", err)
	}
	dir2024, err := store.MkDir(ctx, logsDir.ID, "2024", 0755)
	if err != nil {
		t.Fatalf("mkdir 2024: %v", err)
	}
	dir2025, err := store.MkDir(ctx, logsDir.ID, "2025", 0755)
	if err != nil {
		t.Fatalf("mkdir 2025: %v", err)
	}
	cacheDir, err := store.MkDir(ctx, bucket.RootInode, "cache", 0755)
	if err != nil {
		t.Fatalf("mkdir cache: %v", err)
	}
	importantDir, err := store.MkDir(ctx, bucket.RootInode, "important", 0755)
	if err != nil {
		t.Fatalf("mkdir important: %v", err)
	}

	// Create files
	oldTime := time.Now().Add(-100 * 24 * time.Hour)  // 100 days ago
	recentTime := time.Now().Add(-1 * 24 * time.Hour) // 1 day ago

	createFileWithTime := func(parent metadata.InodeID, name string, ctime time.Time) {
		inode, err := store.CreateFile(ctx, parent, name, 0644)
		if err != nil {
			t.Fatalf("create file %s: %v", name, err)
		}
		inode.CTime = ctime.UnixNano()
		inode.MTime = ctime.UnixNano()
		inode.ATime = ctime.UnixNano()
		if err := store.UpdateInode(ctx, inode); err != nil {
			t.Fatalf("update inode %s: %v", name, err)
		}
	}

	createFileWithTime(dir2024.ID, "app.log", oldTime)         // 100 days old
	createFileWithTime(dir2025.ID, "app.log", oldTime)         // 100 days old
	createFileWithTime(cacheDir.ID, "data.bin", oldTime)       // 100 days old
	createFileWithTime(importantDir.ID, "doc.txt", recentTime) // 1 day old

	// Add lifecycle rule: expire files under "logs/" after 30 days
	engine := metadata.NewLifecycleEngine(store)
	engine.AddRule(metadata.LifecycleRule{
		Bucket: "lifecycle-test",
		Prefix: "logs/",
		Expiration: &metadata.Expiration{
			Days: 30,
		},
	})

	// Execute lifecycle directly; Start waits for the first ticker interval.
	if err := engine.ExecuteOnce(ctx); err != nil {
		t.Fatalf("execute lifecycle: %v", err)
	}

	transitions, deletions := engine.Stats()
	t.Logf("lifecycle: transitions=%d, deletions=%d", transitions, deletions)

	// The old file under logs/2024/ should have been expired
	// The file under cache/ and important/ should NOT be touched
	entries2024, err := store.ReadDir(ctx, dir2024.ID, 0, 0)
	if err != nil {
		t.Fatalf("read dir 2024: %v", err)
	}
	// After expiration, the old file should have NLink=0
	for _, e := range entries2024 {
		inode, err := store.GetInode(ctx, e.InodeID)
		if err != nil {
			t.Fatalf("get inode: %v", err)
		}
		if inode.NLink != 0 {
			t.Errorf("file logs/2024/app.log should have been expired (NLink=0), got NLink=%d", inode.NLink)
		}
	}

	// Files under cache/ should still exist
	cacheEntries, err := store.ReadDir(ctx, cacheDir.ID, 0, 0)
	if err != nil {
		t.Fatalf("read dir cache: %v", err)
	}
	for _, e := range cacheEntries {
		inode, err := store.GetInode(ctx, e.InodeID)
		if err != nil {
			t.Fatalf("get inode: %v", err)
		}
		if inode.NLink == 0 {
			t.Errorf("file cache/data.bin should NOT have been expired by logs/ rule")
		}
	}
}
