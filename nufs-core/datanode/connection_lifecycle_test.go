package datanode

import (
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

// killServerConns closes every live server-side connection, simulating
// the peer closing them (e.g. a restart or idle reaping) without
// notifying the client.
func killServerConns(t *testing.T, srv *Server) {
	t.Helper()
	srv.connMu.Lock()
	defer srv.connMu.Unlock()
	for c := range srv.conns {
		c.Close()
	}
}

// TestClient_RetriesOnDeadConnection verifies that a pooled client whose
// underlying connection has been closed by the peer transparently
// reconnects and retries the operation, instead of surfacing a failure.
func TestClient_RetriesOnDeadConnection(t *testing.T) {
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WaitForScan(); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = 1
	cfg.RequestTimeout = 200 * time.Millisecond
	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	addr := srv.Addr()

	pool := NewClientPool(4, 30*time.Second, 10*time.Second)
	defer pool.CloseAll()

	// Establish and use a connection, then return it to the pool.
	c, err := pool.Get(addr)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := c.WriteChunk(1, []byte("first")); err != nil {
		t.Fatalf("WriteChunk 1: %v", err)
	}
	pool.Put(addr, c)

	// Kill the connection from the server side. The pooled client still
	// believes it is healthy (IsClosed()==false), but its TCP connection
	// is dead.
	killServerConns(t, srv)

	// The next operation must auto-reconnect and succeed.
	c2, err := pool.Get(addr)
	if err != nil {
		t.Fatalf("Get after kill: %v", err)
	}
	defer pool.Put(addr, c2)
	resp, err := c2.WriteChunk(2, []byte("second"))
	if err != nil {
		t.Fatalf("WriteChunk 2 after conn death: %v", err)
	}
	if resp.Status != StatusOK {
		t.Fatalf("status: %v", resp.Status)
	}

	// Both chunks should be readable via the reconnected client.
	rr, err := c2.ReadChunk(1, 0, 0)
	if err != nil {
		t.Fatalf("ReadChunk 1: %v", err)
	}
	if string(rr.Data) != "first" {
		t.Fatalf("chunk 1 data: %q", rr.Data)
	}
}

// TestServer_StopClosesActiveConnections verifies that Stop() returns
// promptly even when idle connections are still open. Without active
// connection closing, Stop would block for the full RequestTimeout
// (30s by default) waiting for handleConn goroutines to time out.
func TestServer_StopClosesActiveConnections(t *testing.T) {
	dir := t.TempDir()
	store, err := NewChunkStore(dir, 8, 8, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WaitForScan(); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig() // RequestTimeout defaults to 30s
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = 1
	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	addr := srv.Addr()

	// Establish an idle connection on the server.
	pool := NewClientPool(4, 30*time.Second, 10*time.Second)
	c, err := pool.Get(addr)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := c.WriteChunk(metadata.ChunkID(1), []byte("x")); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	pool.Put(addr, c) // connection now idle on both sides

	// Stop must return promptly despite the 30s reqTimeout, because it
	// actively closes the idle connection.
	done := make(chan struct{})
	start := time.Now()
	go func() {
		srv.Stop()
		close(done)
	}()
	select {
	case <-done:
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("Stop took %v, want < 2s (active connections not closed)", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return within 5s - idle connections not being closed")
	}
}
