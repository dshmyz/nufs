package datanode

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dshmyz/nufs/nufs-core/internal/tlsutil"
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
	maxTotal    int // total connection limit across all addresses
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
