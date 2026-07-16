package datanode

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/example/dfs/internal/tlsutil"
	"github.com/example/dfs/metadata"
)

// ============================================================
// ClientPool — connection pooling for datanode.Client
// ============================================================
//
// Instead of dialing a new TCP connection for every operation and
// closing it immediately, ClientPool maintains a per-address pool
// of idle connections. Callers Get() a client, use it, then Put()
// it back. Stale (closed) connections are automatically replaced.
//
// Limits:
//   - maxPerAddr: max idle connections per target address
//   - idleTimeout: connections idle longer than this are evicted
//   - dialTimeout: timeout for new connections

// PoolStats returns pool statistics for a given address.
type PoolStats struct {
	Idle    int // connections sitting in the pool
	Active  int // connections currently in use (handed out via Get)
	Created int // total connections created (cumulative)
}

// idleEntry is a pooled client with its return timestamp.
type idleEntry struct {
	client  *Client
	putTime time.Time
}

// ClientPool is a connection pool for datanode.Client instances.
type ClientPool struct {
	mu          sync.Mutex
	maxPerAddr  int
	maxTotal    int           // total connection limit across all addresses
	idleTimeout time.Duration
	dialTimeout time.Duration
	tlsCfg      tlsutil.Config

	// addr -> slice of idle clients (most recently put at end)
	idle map[string][]idleEntry
	// addr -> count of active (in-use) clients
	active map[string]int
	// addr -> total created
	created map[string]int
	// total active connections across all addresses
	totalActive int
}

// NewClientPool creates a connection pool.
// maxPerAddr caps the number of idle connections per address.
// maxTotal caps the total number of active connections (0 = unlimited).
// idleTimeout and dialTimeout are passed to each Client.
func NewClientPool(maxPerAddr int, idleTimeout, dialTimeout time.Duration, opts ...func(*ClientPool)) *ClientPool {
	p := &ClientPool{
		maxPerAddr:  maxPerAddr,
		idleTimeout: idleTimeout,
		dialTimeout: dialTimeout,
		idle:        make(map[string][]idleEntry),
		active:      make(map[string]int),
		created:     make(map[string]int),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// WithMaxTotal sets the total connection limit across all addresses.
func WithMaxTotal(n int) func(*ClientPool) {
	return func(p *ClientPool) { p.maxTotal = n }
}

// SetTLS configures TLS for all future connections.
func (p *ClientPool) SetTLS(cfg tlsutil.Config) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tlsCfg = cfg
}

// Get retrieves or creates a datanode.Client for the given address.
// The caller must call Put(addr, client) when done.
func (p *ClientPool) Get(addr string) (*Client, error) {
	p.mu.Lock()
	// Try to reuse an idle connection (LIFO: most recently put first)
	for len(p.idle[addr]) > 0 {
		entry := p.idle[addr][len(p.idle[addr])-1]
		p.idle[addr] = p.idle[addr][:len(p.idle[addr])-1]

		// Discard connections that have been idle too long or are closed
		if time.Since(entry.putTime) > p.idleTimeout || entry.client.IsClosed() {
			entry.client.Close()
			slog.Debug("pool: discarded stale connection", "addr", addr)
			continue
		}

		p.active[addr]++
		p.totalActive++
		p.mu.Unlock()
		return entry.client, nil
	}
	p.mu.Unlock()

	// Check total connection limit before creating new connection
	p.mu.Lock()
	if p.maxTotal > 0 && p.totalActive >= p.maxTotal {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool: total connection limit reached (%d)", p.maxTotal)
	}
	p.mu.Unlock()

	// No idle connection available, dial a new one
	c, err := p.dial(addr)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.active[addr]++
	p.totalActive++
	p.created[addr]++
	p.mu.Unlock()

	return c, nil
}

// Put returns a client to the pool for reuse.
// If the pool is full for this address, the client is closed.
func (p *ClientPool) Put(addr string, c *Client) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.active[addr]--
	p.totalActive--

	// Discard closed connections
	if c.IsClosed() {
		return
	}

	if len(p.idle[addr]) >= p.maxPerAddr {
		// Pool full, close the connection
		c.Close()
		return
	}

	p.idle[addr] = append(p.idle[addr], idleEntry{client: c, putTime: time.Now()})
}

// Stats returns pool statistics for a given address.
func (p *ClientPool) Stats(addr string) PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PoolStats{
		Idle:    len(p.idle[addr]),
		Active:  p.active[addr],
		Created: p.created[addr],
	}
}

// CloseAll closes all idle connections in the pool.
func (p *ClientPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, entries := range p.idle {
		for _, e := range entries {
			e.client.Close()
		}
		delete(p.idle, addr)
	}
}

// dial creates a new Client and connects it.
func (p *ClientPool) dial(addr string) (*Client, error) {
	p.mu.Lock()
	tlsCfg := p.tlsCfg
	p.mu.Unlock()

	var c *Client
	var err error

	if tlsCfg.Enabled() {
		c, err = NewTLSClient(addr, tlsCfg)
	} else {
		c = NewClient(addr)
	}
	if err != nil {
		return nil, fmt.Errorf("pool: create client for %s: %w", addr, err)
	}

	c.SetTimeout(p.idleTimeout)
	if err := c.Connect(); err != nil {
		return nil, fmt.Errorf("pool: connect to %s: %w", addr, err)
	}

	return c, nil
}

// ============================================================
// WritePipeline — parallel chunk replication
// ============================================================
//
// Instead of the serial chain write (Head → Middle → Tail),
// WritePipeline dispatches writes to all replicas concurrently.
// This reduces latency from O(N * RTT) to O(RTT + max_slow_replica).
//
// Quorum controls durability: the write returns success once the
// quorum number of replicas acknowledge. Remaining replicas are
// best-effort.

// PipelineConfig holds configuration for WritePipeline.
type PipelineConfig struct {
	Quorum int // number of replicas that must succeed (0 = all)
}

// PipelineOption configures a PipelineConfig.
type PipelineOption func(*PipelineConfig)

// WithQuorum sets the minimum number of replicas that must succeed.
func WithQuorum(n int) PipelineOption {
	return func(cfg *PipelineConfig) {
		cfg.Quorum = n
	}
}

// WritePipeline dispatches chunk writes to multiple replicas in parallel.
type WritePipeline struct {
	pool    *ClientPool
	timeout time.Duration
	quorum  int // 0 means all replicas must succeed
}

// NewWritePipeline creates a write pipeline backed by the given connection pool.
func NewWritePipeline(pool *ClientPool, timeout time.Duration, opts ...PipelineOption) *WritePipeline {
	cfg := PipelineConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &WritePipeline{
		pool:    pool,
		timeout: timeout,
		quorum:  cfg.Quorum,
	}
}

// Write sends data to all replicas concurrently and waits for quorum
// acknowledgements. If quorum == 0, all replicas must succeed.
func (pp *WritePipeline) Write(ctx context.Context, chunkID metadata.ChunkID, data []byte, replicas []metadata.ReplicaInfo) error {
	if len(replicas) == 0 {
		return fmt.Errorf("pipeline: no replicas for chunk %d", chunkID)
	}

	required := pp.quorum
	if required <= 0 {
		required = len(replicas)
	}
	if required > len(replicas) {
		required = len(replicas)
	}

	type result struct {
		nodeID metadata.NodeID
		err    error
	}

	results := make(chan result, len(replicas))

	for _, rep := range replicas {
		go func(r metadata.ReplicaInfo) {
			client, err := pp.pool.Get(r.Addr)
			if err != nil {
				results <- result{nodeID: r.NodeID, err: fmt.Errorf("connect %s: %w", r.Addr, err)}
				return
			}
			resp, err := client.ReplicateChunk(chunkID, data)
			pp.pool.Put(r.Addr, client)

			if err != nil {
				results <- result{nodeID: r.NodeID, err: fmt.Errorf("write %s: %w", r.Addr, err)}
				return
			}
			if resp.Status != StatusOK {
				results <- result{nodeID: r.NodeID, err: fmt.Errorf("node %d status=%d: %s", r.NodeID, resp.Status, resp.Error)}
				return
			}
			results <- result{nodeID: r.NodeID}
		}(rep)
	}

	successes := 0
	var lastErr error
	for i := 0; i < len(replicas); i++ {
		select {
		case r := <-results:
			if r.err != nil {
				lastErr = r.err
				slog.Warn("pipeline: replica write failed", "chunkID", chunkID, "nodeID", r.nodeID, "error", r.err)
			} else {
				successes++
			}
		case <-ctx.Done():
			return fmt.Errorf("pipeline: context cancelled: %w", ctx.Err())
		}
	}

	if successes < required {
		if lastErr != nil {
			return fmt.Errorf("pipeline: only %d/%d replicas succeeded for chunk %d: %w",
				successes, required, chunkID, lastErr)
		}
		return fmt.Errorf("pipeline: only %d/%d replicas succeeded for chunk %d",
			successes, required, chunkID)
	}

	return nil
}
