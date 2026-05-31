package datanode

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

func TestServerClient_WriteReadCycle(t *testing.T) {
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0" // random port
	cfg.NodeID = 1

	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatalf("Server.Start: %v", err)
	}
	defer srv.Stop()

	// Get actual listen address
	addr := srv.listener.Addr().String()

	// Connect client
	client := NewClient(addr)
	if err := client.Connect(); err != nil {
		t.Fatalf("Client.Connect: %v", err)
	}
	defer client.Close()

	// Write a chunk
	chunkID := metadata.ChunkID(42)
	data := []byte("hello from integration test")

	resp, err := client.WriteChunk(chunkID, data)
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	if resp.Status != StatusOK {
		t.Fatalf("WriteChunk status: %v, error: %s", resp.Status, resp.Error)
	}
	if resp.Length != int32(len(data)) {
		t.Fatalf("WriteChunk length: got %d, want %d", resp.Length, len(data))
	}

	// Read it back
	resp, err = client.ReadChunk(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if resp.Status != StatusOK {
		t.Fatalf("ReadChunk status: %v, error: %s", resp.Status, resp.Error)
	}
	if !bytes.Equal(resp.Data, data) {
		t.Fatalf("ReadChunk data mismatch: got %q, want %q", resp.Data, data)
	}
	if resp.Checksum != crc32.ChecksumIEEE(data) {
		t.Fatalf("ReadChunk checksum mismatch")
	}
}

func TestServerClient_ReadNotFound(t *testing.T) {
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"

	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatalf("Server.Start: %v", err)
	}
	defer srv.Stop()

	client := NewClient(srv.listener.Addr().String())
	if err := client.Connect(); err != nil {
		t.Fatalf("Client.Connect: %v", err)
	}
	defer client.Close()

	resp, err := client.ReadChunk(metadata.ChunkID(99999), 0, 0)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if resp.Status != StatusNotFound {
		t.Fatalf("expected StatusNotFound, got %v", resp.Status)
	}
}

func TestServerClient_HealthCheck(t *testing.T) {
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = 7
	cfg.CapacityGB = 500

	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatalf("Server.Start: %v", err)
	}
	defer srv.Stop()

	client := NewClient(srv.listener.Addr().String())
	if err := client.Connect(); err != nil {
		t.Fatalf("Client.Connect: %v", err)
	}
	defer client.Close()

	// Write a chunk first to have some data
	client.WriteChunk(metadata.ChunkID(1), []byte("test"))

	// Send health check via generic request
	header := &Header{
		Type:      ReqHealth,
		RequestID: 1,
	}
	resp, err := client.sendRequest(header, nil)
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if resp.Status != StatusOK {
		t.Fatalf("Health status: %v, error: %s", resp.Status, resp.Error)
	}
	if len(resp.Data) == 0 {
		t.Fatal("Health returned empty data")
	}
}

func TestServerClient_LargeChunkTransfer(t *testing.T) {
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"

	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatalf("Server.Start: %v", err)
	}
	defer srv.Stop()

	client := NewClient(srv.listener.Addr().String())
	if err := client.Connect(); err != nil {
		t.Fatalf("Client.Connect: %v", err)
	}
	defer client.Close()

	// Write a 2MB chunk
	chunkID := metadata.ChunkID(1000)
	data := make([]byte, 2*1024*1024)
	rand.Read(data)

	resp, err := client.WriteChunk(chunkID, data)
	if err != nil {
		t.Fatalf("WriteChunk large: %v", err)
	}
	if resp.Status != StatusOK {
		t.Fatalf("WriteChunk large status: %v", resp.Status)
	}

	// Read it back
	resp, err = client.ReadChunk(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("ReadChunk large: %v", err)
	}
	if resp.Status != StatusOK {
		t.Fatalf("ReadChunk large status: %v", resp.Status)
	}
	if !bytes.Equal(resp.Data, data) {
		t.Fatal("large chunk data mismatch")
	}
}

func TestServerClient_MultipleChunks(t *testing.T) {
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"

	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatalf("Server.Start: %v", err)
	}
	defer srv.Stop()

	client := NewClient(srv.listener.Addr().String())
	if err := client.Connect(); err != nil {
		t.Fatalf("Client.Connect: %v", err)
	}
	defer client.Close()

	// Write 10 chunks
	const numChunks = 10
	written := make(map[metadata.ChunkID][]byte)
	for i := 0; i < numChunks; i++ {
		chunkID := metadata.ChunkID(uint64(i + 100))
		data := make([]byte, 1024)
		rand.Read(data)
		written[chunkID] = data

		resp, err := client.WriteChunk(chunkID, data)
		if err != nil {
			t.Fatalf("WriteChunk %d: %v", chunkID, err)
		}
		if resp.Status != StatusOK {
			t.Fatalf("WriteChunk %d status: %v", chunkID, resp.Status)
		}
	}

	// Read all back and verify
	for chunkID, expected := range written {
		resp, err := client.ReadChunk(chunkID, 0, 0)
		if err != nil {
			t.Fatalf("ReadChunk %d: %v", chunkID, err)
		}
		if !bytes.Equal(resp.Data, expected) {
			t.Fatalf("chunk %d data mismatch", chunkID)
		}
	}
}

func TestServerClient_ReplicateChunk(t *testing.T) {
	// Set up source and target servers
	srcDir := t.TempDir()
	srcStore, err := NewChunkStore(srcDir, 8, 8, nil)
	if err != nil {
		t.Fatalf("src NewChunkStore: %v", err)
	}
	tgtDir := t.TempDir()
	tgtStore, err := NewChunkStore(tgtDir, 8, 8, nil)
	if err != nil {
		t.Fatalf("tgt NewChunkStore: %v", err)
	}

	srcCfg := DefaultConfig()
	srcCfg.ListenAddr = "127.0.0.1:0"
	srcCfg.NodeID = 1
	tgtCfg := DefaultConfig()
	tgtCfg.ListenAddr = "127.0.0.1:0"
	tgtCfg.NodeID = 2

	srcSrv := NewServer(srcCfg, srcStore)
	tgtSrv := NewServer(tgtCfg, tgtStore)
	if err := srcSrv.Start(); err != nil {
		t.Fatalf("src Start: %v", err)
	}
	defer srcSrv.Stop()
	if err := tgtSrv.Start(); err != nil {
		t.Fatalf("tgt Start: %v", err)
	}
	defer tgtSrv.Stop()

	srcAddr := srcSrv.listener.Addr().String()
	tgtAddr := tgtSrv.listener.Addr().String()

	// Write chunk to source
	srcClient := NewClient(srcAddr)
	if err := srcClient.Connect(); err != nil {
		t.Fatalf("src Connect: %v", err)
	}
	defer srcClient.Close()

	chunkID := metadata.ChunkID(555)
	data := []byte("replicate this data across nodes")
	resp, err := srcClient.WriteChunk(chunkID, data)
	if err != nil {
		t.Fatalf("src WriteChunk: %v", err)
	}
	if resp.Status != StatusOK {
		t.Fatalf("src WriteChunk status: %v", resp.Status)
	}

	// Replicate: read from source, write to target
	tgtClient := NewClient(tgtAddr)
	if err := tgtClient.Connect(); err != nil {
		t.Fatalf("tgt Connect: %v", err)
	}
	defer tgtClient.Close()

	// Read from source
	readResp, err := srcClient.ReadChunk(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("src ReadChunk: %v", err)
	}

	// Write to target via replicate
	replResp, err := tgtClient.ReplicateChunk(chunkID, readResp.Data)
	if err != nil {
		t.Fatalf("tgt ReplicateChunk: %v", err)
	}
	if replResp.Status != StatusOK {
		t.Fatalf("tgt ReplicateChunk status: %v", replResp.Status)
	}

	// Verify target has the chunk
	verifyResp, err := tgtClient.ReadChunk(chunkID, 0, 0)
	if err != nil {
		t.Fatalf("tgt ReadChunk: %v", err)
	}
	if !bytes.Equal(verifyResp.Data, data) {
		t.Fatal("replicated data mismatch")
	}
	if verifyResp.Checksum != crc32.ChecksumIEEE(data) {
		t.Fatal("replicated checksum mismatch")
	}
}

func TestServerClient_ConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 16, 16, nil)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.MaxConcurrentWrites = 16
	cfg.MaxConcurrentReads = 16

	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatalf("Server.Start: %v", err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()

	// Pre-write some chunks
	preClient := NewClient(addr)
	preClient.Connect()
	defer preClient.Close()

	const numChunks = 8
	chunkData := make(map[metadata.ChunkID][]byte)
	for i := 0; i < numChunks; i++ {
		cid := metadata.ChunkID(uint64(i + 200))
		data := make([]byte, 512)
		rand.Read(data)
		chunkData[cid] = data
		preClient.WriteChunk(cid, data)
	}

	// Concurrent reads
	done := make(chan error, numChunks)
	for cid, expected := range chunkData {
		go func(chunkID metadata.ChunkID, exp []byte) {
			c := NewClient(addr)
			if err := c.Connect(); err != nil {
				done <- err
				return
			}
			defer c.Close()

			resp, err := c.ReadChunk(chunkID, 0, 0)
			if err != nil {
				done <- err
				return
			}
			if !bytes.Equal(resp.Data, exp) {
				done <- fmt.Errorf("concurrent read mismatch for chunk %d", chunkID)
				return
			}
			done <- nil
		}(cid, expected)
	}

	for i := 0; i < numChunks; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("concurrent read: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent read timeout")
		}
	}
}
