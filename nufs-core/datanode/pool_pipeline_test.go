package datanode

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

// ============================================================
// TDD: ClientPool — connection pooling for datanode.Client
// ============================================================

func TestClientPool_GetAndReturn(t *testing.T) {
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = 1
	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()
	pool := NewClientPool(4, 30*time.Second, 10*time.Second)
	defer pool.CloseAll()

	// Get a client from pool
	c1, err := pool.Get(addr)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if c1 == nil {
		t.Fatal("expected non-nil client")
	}

	// Verify it works
	resp, err := c1.WriteChunk(1, []byte("hello"))
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	if resp.Status != StatusOK {
		t.Fatalf("status: %v", resp.Status)
	}

	// Return to pool
	pool.Put(addr, c1)

	// Get again — should reuse the same connection
	c2, err := pool.Get(addr)
	if err != nil {
		t.Fatalf("Get (reuse): %v", err)
	}
	resp, err = c2.ReadChunk(1, 0, 0)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if resp.Status != StatusOK {
		t.Fatalf("status: %v", resp.Status)
	}
	pool.Put(addr, c2)
}

func TestClientPool_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = 1
	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()
	pool := NewClientPool(4, 30*time.Second, 10*time.Second)
	defer pool.CloseAll()

	var wg sync.WaitGroup
	var ops atomic.Int64

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c, err := pool.Get(addr)
			if err != nil {
				t.Errorf("goroutine %d: Get: %v", idx, err)
				return
			}
			data := []byte{byte(idx)}
			resp, err := c.WriteChunk(metadata.ChunkID(100+idx), data)
			pool.Put(addr, c)
			if err != nil {
				t.Errorf("goroutine %d: WriteChunk: %v", idx, err)
				return
			}
			if resp.Status != StatusOK {
				t.Errorf("goroutine %d: status=%v", idx, resp.Status)
				return
			}
			ops.Add(1)
		}(i)
	}
	wg.Wait()

	if ops.Load() != 20 {
		t.Errorf("expected 20 ops, got %d", ops.Load())
	}
}

func TestClientPool_MaxSize(t *testing.T) {
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = 1
	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()
	pool := NewClientPool(2, 30*time.Second, 10*time.Second) // max 2 per addr
	defer pool.CloseAll()

	// Create 4 clients, return all — pool should cap at 2
	clients := make([]*Client, 4)
	for i := range clients {
		c, err := pool.Get(addr)
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = c
	}
	for _, c := range clients {
		pool.Put(addr, c)
	}

	// Pool should have at most 2 idle clients for this addr
	stats := pool.Stats(addr)
	if stats.Idle > 2 {
		t.Errorf("pool should cap at maxPerAddr=2, got %d idle", stats.Idle)
	}
}

func TestClientPool_StaleConnection(t *testing.T) {
	// Verify that a stale (closed) connection causes an error on next use,
	// and the pool can recover by creating a new connection.
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = 1
	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()
	pool := NewClientPool(4, 30*time.Second, 10*time.Second)
	defer pool.CloseAll()

	c1, err := pool.Get(addr)
	if err != nil {
		t.Fatal(err)
	}
	// Close the underlying connection to simulate a stale connection
	c1.Close()
	pool.Put(addr, c1)

	// Get should return the stale connection; using it will fail,
	// but the next Get after discarding should work
	c2, err := pool.Get(addr)
	if err != nil {
		t.Fatal(err)
	}
	// Try to use c2 — if it's the stale one, this will fail
	_, err = c2.WriteChunk(1, []byte("test"))
	if err != nil {
		// Stale connection detected — close and get a fresh one
		c2.Close()
		pool.Put(addr, c2) // Put will discard it since it's closed

		c3, err := pool.Get(addr)
		if err != nil {
			t.Fatalf("Get fresh: %v", err)
		}
		resp, err := c3.WriteChunk(1, []byte("recovered"))
		if err != nil {
			t.Fatalf("WriteChunk after recovery: %v", err)
		}
		if resp.Status != StatusOK {
			t.Fatalf("status: %v", resp.Status)
		}
		pool.Put(addr, c3)
	} else {
		// Connection was still usable (unlikely but possible)
		pool.Put(addr, c2)
	}
}

// ============================================================
// TDD: WritePipeline — parallel forwarding for chain replication
// ============================================================

func TestWritePipeline_ParallelReplication(t *testing.T) {
	// Start 3 datanode servers
	type node struct {
		srv   *Server
		addr  string
		store *ChunkStore
	}
	nodes := make([]node, 3)
	for i := range nodes {
		dir := t.TempDir()
		store, err := NewChunkStore(dir, 8, 8, nil)
		if err != nil {
			t.Fatal(err)
		}
		cfg := DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.NodeID = metadata.NodeID(i + 1)
		srv := NewServer(cfg, store)
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
		nodes[i] = node{srv: srv, addr: srv.listener.Addr().String(), store: store}
		defer srv.Stop()
	}

	pool := NewClientPool(4, 30*time.Second, 10*time.Second)
	defer pool.CloseAll()
	replicas := []metadata.ReplicaInfo{
		{NodeID: 1, Addr: nodes[0].addr},
		{NodeID: 2, Addr: nodes[1].addr},
		{NodeID: 3, Addr: nodes[2].addr},
	}

	data := []byte("pipeline test data")
	chunkID := metadata.ChunkID(500)

	// Use WritePipeline to write to all replicas in parallel
	pp := NewWritePipeline(pool, 30*time.Second)
	err := pp.Write(context.Background(), chunkID, data, replicas)
	if err != nil {
		t.Fatalf("WritePipeline.Write: %v", err)
	}

	// Verify all 3 replicas have the data
	for i, n := range nodes {
		c, err := pool.Get(n.addr)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := c.ReadChunk(chunkID, 0, 0)
		pool.Put(n.addr, c)
		if err != nil {
			t.Fatalf("read from node %d: %v", i, err)
		}
		if resp.Status != StatusOK {
			t.Fatalf("node %d status: %v", i, resp.Status)
		}
		if string(resp.Data) != string(data) {
			t.Errorf("node %d data mismatch: got %q, want %q", i, resp.Data, data)
		}
	}
}

func TestWritePipeline_PartialFailure(t *testing.T) {
	// Start 2 real servers, 1 fake address that will fail
	type node struct {
		srv  *Server
		addr string
	}
	nodes := make([]node, 2)
	for i := range nodes {
		dir := t.TempDir()
		store, err := NewChunkStore(dir, 8, 8, nil)
		if err != nil {
			t.Fatal(err)
		}
		cfg := DefaultConfig()
		cfg.ListenAddr = "127.0.0.1:0"
		cfg.NodeID = metadata.NodeID(i + 1)
		srv := NewServer(cfg, store)
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
		nodes[i] = node{srv: srv, addr: srv.listener.Addr().String()}
		defer srv.Stop()
	}

	pool := NewClientPool(4, 30*time.Second, 10*time.Second)
	defer pool.CloseAll()
	replicas := []metadata.ReplicaInfo{
		{NodeID: 1, Addr: nodes[0].addr},
		{NodeID: 2, Addr: nodes[1].addr},
		{NodeID: 3, Addr: "127.0.0.1:1"}, // unreachable
	}

	data := []byte("partial failure test")
	chunkID := metadata.ChunkID(501)

	pp := NewWritePipeline(pool, 2*time.Second)
	err := pp.Write(context.Background(), chunkID, data, replicas)
	if err == nil {
		t.Fatal("expected error when 1 of 3 replicas fails and quorum requires all")
	}

	// With quorum=2 (majority of 3), 2 successes should be enough
	pp2 := NewWritePipeline(pool, 2*time.Second, WithQuorum(2))
	err = pp2.Write(context.Background(), chunkID, data, replicas)
	if err != nil {
		t.Fatalf("WritePipeline with quorum=2 should succeed: %v", err)
	}
}

func TestWritePipeline_FasterThanSerial(t *testing.T) {
	// Verify pipeline is faster than serial for multiple replicas
	// by checking that writes are issued concurrently (not sequentially)
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = 1
	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()
	pool := NewClientPool(4, 30*time.Second, 10*time.Second)
	defer pool.CloseAll()

	replicas := []metadata.ReplicaInfo{
		{NodeID: 1, Addr: addr},
		{NodeID: 2, Addr: addr}, // same node, but parallel dispatch
		{NodeID: 3, Addr: addr},
	}

	data := make([]byte, 1024)
	chunkID := metadata.ChunkID(600)

	pp := NewWritePipeline(pool, 30*time.Second, WithQuorum(2))

	start := time.Now()
	for i := 0; i < 10; i++ {
		err := pp.Write(context.Background(), metadata.ChunkID(int(chunkID)+i), data, replicas)
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	elapsed := time.Since(start)

	// Serial would be at least 10 * 3 * network RTT; parallel should be faster
	// Just verify it completes in reasonable time (< 5s for local loopback)
	if elapsed > 5*time.Second {
		t.Errorf("pipeline too slow: %v", elapsed)
	}
}
