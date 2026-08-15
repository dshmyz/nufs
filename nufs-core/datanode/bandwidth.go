package datanode

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Background transfer bandwidth limiter.
//
// Anti-entropy, repair and rebalance all copy chunk data between data nodes and can
// saturate the network if not throttled. We use a shared token bucket at the package
// level so all background data copy competes fairly for a single bandwidth budget —
// separate from foreground client PUT/GET traffic which should be prioritised.
//
// 0 means unlimited (the limiter is disabled).
var (
	backgroundBandwidthMu sync.RWMutex
	backgroundLimiter     *rate.Limiter
)

// SetBackgroundBandwidthMBps sets the global bandwidth limit for all background
// data copy tasks (anti-entropy, repair, rebalance). Pass 0 to disable throttling.
func SetBackgroundBandwidthMBps(mbps int) {
	backgroundBandwidthMu.Lock()
	defer backgroundBandwidthMu.Unlock()
	if mbps <= 0 {
		backgroundLimiter = nil
		return
	}
	bps := mbps * 1024 * 1024
	// Linear token refill at bps/sec, bursting up to ~1/4 of the limit.
	// A burst of ~256 KB / second is enough to not stall individual chunk reads
	// while still enforcing the average rate over time.
	burst := bps / 4
	if burst < 4096 {
		burst = 4096
	}
	backgroundLimiter = rate.NewLimiter(rate.Limit(bps), burst)
}

// WaitBytes blocks until n bytes of background transfer budget are available or
// the context is cancelled. Returns immediately (noop) if no limit is set or n <= 0.
func WaitBytes(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}
	backgroundBandwidthMu.RLock()
	lim := backgroundLimiter
	backgroundBandwidthMu.RUnlock()
	if lim == nil {
		return nil
	}

	// rate.Limiter.WaitN accepts n tokens but if n > burst size, WaitN returns an error immediately —
	// we split the wait into chunks of at most burst so very large chunks still throttle.
	burst := lim.Burst()
	if n <= burst {
		return lim.WaitN(ctx, n)
	}
	remaining := n
	for remaining > 0 {
		tokens := remaining
		if tokens > burst {
			tokens = burst
		}
		if err := lim.WaitN(ctx, tokens); err != nil {
			return err
		}
		remaining -= tokens
	}
	return nil
}

// ThrottleRead wraps a chunk-read path with bandwidth throttling. It should be
// called *before* reading chunk data from a peer in any background copy path.
// Returns immediately (no-op) if the limiter is disabled.
func ThrottleRead(ctx context.Context, dataLen int) error {
	return WaitBytes(ctx, dataLen)
}

// ThrottleDuration estimates the wait time for a given number of bytes (used by
// tests and metrics — not the actual wait).
func ThrottleDuration(n int) time.Duration {
	backgroundBandwidthMu.RLock()
	lim := backgroundLimiter
	backgroundBandwidthMu.RUnlock()
	if lim == nil || n <= 0 {
		return 0
	}
	return time.Duration(float64(n)/float64(lim.Limit())) * time.Second
}
