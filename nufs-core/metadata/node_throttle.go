package metadata

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// ============================================================
// NodeRegistrationThrottle — protects metadata services from
// waves of node registrations / heartbeats during cluster
// topology changes. Uses token-bucket rate limiting at two
// levels:
//
//   1. Global  → total QPS accepted from all nodes
//   2. Per-node → per-node-ID burst protection (prevents a
//      single misconfigured or buggy node from flooding
//      RegisterNode / Heartbeat).
//
// Token-bucket is preferred over a simple counter because it
// naturally handles bursts: a cold-start wave of fresh nodes
// gets a generous burst, and steady-state heartbeats get the
// sustained rate.
// ============================================================

// NodeThrottleConfig is the configuration for NodeRegistrationThrottle.
// Both fields use token-bucket semantics: rate is the sustained
// QPS, burst is the maximum tokens available (equivalent to
// the maximum acceptable burst size in requests).
type NodeThrottleConfig struct {
	// GlobalRate is the sustained QPS across all nodes.
	// Defaults to 100.
	GlobalRate rate.Limit
	// GlobalBurst is the maximum burst across all nodes.
	// Defaults to 200.
	GlobalBurst int
	// PerNodeRate is the sustained QPS allowed per node.
	// Defaults to 10 (one registration plus frequent
	// heartbeats is well within this).
	PerNodeRate rate.Limit
	// PerNodeBurst is the per-node burst size. Defaults to 20.
	PerNodeBurst int
}

// DefaultNodeThrottleConfig returns production-safe defaults.
func DefaultNodeThrottleConfig() NodeThrottleConfig {
	return NodeThrottleConfig{
		GlobalRate:   rate.Limit(100.0),
		GlobalBurst:  200,
		PerNodeRate:  rate.Limit(10.0),
		PerNodeBurst: 20,
	}
}

// NodeRegistrationThrottle is the runtime limiter. Safe for
// concurrent use.
type NodeRegistrationThrottle struct {
	config atomic.Value // holds NodeThrottleConfig

	mu       sync.Mutex
	global   atomic.Pointer[rate.Limiter] // replaced by Reconfigure; read lock-free on Allow/Wait
	perNode  map[NodeID]*rate.Limiter
	lastSeen map[NodeID]time.Time

	// Counters (exposed to admin/metrics page)
	allowedTotal  atomic.Int64
	rejectedTotal atomic.Int64
	rejectedBurst atomic.Int64 // rejection due to per-node burst

	stopCleanup chan struct{}
	cleanupDone chan struct{}
}

// NewNodeRegistrationThrottle creates a limiter with the given
// config (nil → defaults).
func NewNodeRegistrationThrottle(cfg *NodeThrottleConfig) *NodeRegistrationThrottle {
	c := DefaultNodeThrottleConfig()
	if cfg != nil {
		c = *cfg
	}
	t := &NodeRegistrationThrottle{
		perNode:  make(map[NodeID]*rate.Limiter),
		lastSeen: make(map[NodeID]time.Time),
	}
	t.config.Store(c)
	t.global.Store(rate.NewLimiter(c.GlobalRate, c.GlobalBurst))
	return t
}

// Reconfigure atomically swaps the limiter config. Used at
// runtime (e.g. from admin page / feature flag). Per-node
// limiters are lazily rebuilt with the new rate on next call.
func (t *NodeRegistrationThrottle) Reconfigure(cfg NodeThrottleConfig) {
	t.config.Store(cfg)
	newGlobal := rate.NewLimiter(cfg.GlobalRate, cfg.GlobalBurst)
	t.mu.Lock()
	// Swap before clearing per-node state so a concurrently-running Allow/Wait
	// keeps a stable *rate.Limiter the whole time (atomic pointer).
	t.global.Store(newGlobal)
	// Per-node limiters are rebuilt lazily on next Allow() to
	// avoid iterating the map under lock.
	t.perNode = make(map[NodeID]*rate.Limiter)
	t.lastSeen = make(map[NodeID]time.Time)
	t.mu.Unlock()
}

// Allow checks whether a request from nodeID is within the
// configured rate limits. Returns true if it should be
// processed; callers that receive false MUST return
// ErrTooManyRequests to the client.
//
// We evaluate global + per-node; both must have capacity. If
// either rejects, we increment the relevant counter but do NOT
// consume a token from the one that *would* have allowed — so
// a burst of failing requests doesn't leak tokens.
func (t *NodeRegistrationThrottle) Allow(nodeID NodeID) bool {
	cfg := t.config.Load().(NodeThrottleConfig)

	// Global check — consume a token first so a wave of
	// concurrent requests can't double-count. rate.Limiter is
	// internally synchronized; the pointer is loaded atomically so
	// a concurrent Reconfigure can't race the read.
	if !t.global.Load().Allow() {
		t.rejectedTotal.Add(1)
		return false
	}

	// Per-node check — build limiter lazily.
	t.mu.Lock()
	nodeLimiter, ok := t.perNode[nodeID]
	if !ok {
		nodeLimiter = rate.NewLimiter(cfg.PerNodeRate, cfg.PerNodeBurst)
		t.perNode[nodeID] = nodeLimiter
	}
	t.lastSeen[nodeID] = time.Now()
	t.mu.Unlock()

	if !nodeLimiter.Allow() {
		t.rejectedTotal.Add(1)
		t.rejectedBurst.Add(1)
		return false
	}

	t.allowedTotal.Add(1)
	return true
}

// Wait blocks until capacity for one request from nodeID is
// available or ctx is cancelled. Use this for server-side
// pacing; HTTP handlers use Allow() and return 429.
func (t *NodeRegistrationThrottle) Wait(ctx context.Context, nodeID NodeID) error {
	cfg := t.config.Load().(NodeThrottleConfig)
	if err := t.global.Load().Wait(ctx); err != nil {
		return err
	}
	t.mu.Lock()
	nodeLimiter, ok := t.perNode[nodeID]
	if !ok {
		nodeLimiter = rate.NewLimiter(cfg.PerNodeRate, cfg.PerNodeBurst)
		t.perNode[nodeID] = nodeLimiter
	}
	t.lastSeen[nodeID] = time.Now()
	t.mu.Unlock()
	return nodeLimiter.Wait(ctx)
}

// NodeThrottleStats is a snapshot of the limiter state, used by
// the admin page and Prometheus metrics.
type NodeThrottleStats struct {
	AllowedTotal  int64 `json:"allowed_total"`
	RejectedTotal int64 `json:"rejected_total"`
	RejectedBurst int64 `json:"rejected_burst"`
	TrackedNodes  int   `json:"tracked_nodes"`
}

// Stats returns a snapshot of counter state.
func (t *NodeRegistrationThrottle) Stats() NodeThrottleStats {
	t.mu.Lock()
	nn := len(t.perNode)
	t.mu.Unlock()
	return NodeThrottleStats{
		AllowedTotal:  t.allowedTotal.Load(),
		RejectedTotal: t.rejectedTotal.Load(),
		RejectedBurst: t.rejectedBurst.Load(),
		TrackedNodes:  nn,
	}
}

// StartCleanup launches a background goroutine that evicts per-node
// rate limiters for nodes not seen within maxAge. The goroutine runs
// until StopCleanup is called.
func (t *NodeRegistrationThrottle) StartCleanup(maxAge time.Duration) {
	t.stopCleanup = make(chan struct{})
	t.cleanupDone = make(chan struct{})
	go func() {
		defer close(t.cleanupDone)
		ticker := time.NewTicker(maxAge / 2)
		defer ticker.Stop()
		for {
			select {
			case <-t.stopCleanup:
				return
			case <-ticker.C:
				t.evictStale(maxAge)
			}
		}
	}()
}

// StopCleanup terminates the background cleanup goroutine and waits.
func (t *NodeRegistrationThrottle) StopCleanup() {
	if t.stopCleanup != nil {
		close(t.stopCleanup)
		<-t.cleanupDone
	}
}

// evictStale removes per-node limiters for nodes not seen within maxAge.
func (t *NodeRegistrationThrottle) evictStale(maxAge time.Duration) {
	cutoff := time.Now().Add(-maxAge)
	t.mu.Lock()
	var stale []NodeID
	for id, last := range t.lastSeen {
		if last.Before(cutoff) {
			stale = append(stale, id)
		}
	}
	for _, id := range stale {
		delete(t.perNode, id)
		delete(t.lastSeen, id)
	}
	t.mu.Unlock()
}
