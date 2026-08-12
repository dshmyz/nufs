package metadata

import (
	"context"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// TestNodeRegistrationThrottle_Allow confirms steady-state requests pass.
func TestNodeRegistrationThrottle_Allow(t *testing.T) {
	cfg := DefaultNodeThrottleConfig()
	cfg.GlobalBurst = 5
	cfg.GlobalRate = 1000 // effectively unlimited for this test
	th := NewNodeRegistrationThrottle(&cfg)
	for i := 0; i < 5; i++ {
		if !th.Allow(NodeID(1)) {
			t.Fatalf("expected allow on request %d", i)
		}
	}
	// Burst of 5 consumed; next should be rejected until bucket refills.
	if th.Allow(NodeID(1)) {
		t.Fatalf("expected reject after burst")
	}
}

// TestNodeRegistrationThrottle_PerNodeLimit ensures a single noisy node
// can't starve other nodes even if the global budget is available.
func TestNodeRegistrationThrottle_PerNodeLimit(t *testing.T) {
	cfg := DefaultNodeThrottleConfig()
	cfg.GlobalRate = 1000
	cfg.GlobalBurst = 1000
	cfg.PerNodeRate = 1000
	cfg.PerNodeBurst = 5
	th := NewNodeRegistrationThrottle(&cfg)

	for i := 0; i < 5; i++ {
		if !th.Allow(NodeID(99)) {
			t.Fatalf("expected allow on request %d", i)
		}
	}
	if th.Allow(NodeID(99)) {
		t.Fatal("expected per-node burst to be exhausted")
	}
	// Another node is unaffected.
	if !th.Allow(NodeID(100)) {
		t.Fatal("expected other node to still be allowed")
	}
}

// TestNodeRegistrationThrottle_Concurrent confirms Allow works under
// concurrent access without data races (caught by -race).
func TestNodeRegistrationThrottle_Concurrent(t *testing.T) {
	th := NewNodeRegistrationThrottle(nil)
	var wg sync.WaitGroup
	for n := 0; n < 16; n++ {
		wg.Add(1)
		go func(id NodeID) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				th.Allow(id)
			}
		}(NodeID(n))
	}
	wg.Wait()
}

// TestNodeRegistrationThrottle_Wait blocks until capacity is available.
func TestNodeRegistrationThrottle_Wait(t *testing.T) {
	cfg := DefaultNodeThrottleConfig()
	cfg.GlobalBurst = 2
	cfg.GlobalRate = rate.Limit(100) // refill 100/sec
	th := NewNodeRegistrationThrottle(&cfg)

	// Consume burst.
	for i := 0; i < 2; i++ {
		if !th.Allow(NodeID(1)) {
			t.Fatalf("expected initial allow")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := th.Wait(ctx, NodeID(1)); err != nil {
		t.Fatalf("Wait should succeed once token refills: %v", err)
	}
}

// TestNodeRegistrationThrottle_EvictStale verifies that nodes not seen
// within maxAge are evicted from the per-node map.
func TestNodeRegistrationThrottle_EvictStale(t *testing.T) {
	th := NewNodeRegistrationThrottle(nil)

	// Touch several nodes so they appear in perNode.
	th.Allow(NodeID(1))
	th.Allow(NodeID(2))
	th.Allow(NodeID(3))

	th.mu.Lock()
	if len(th.perNode) != 3 {
		t.Fatalf("expected 3 tracked nodes, got %d", len(th.perNode))
	}
	th.mu.Unlock()

	// Manually backdate lastSeen for nodes 1 and 2.
	th.mu.Lock()
	th.lastSeen[NodeID(1)] = time.Now().Add(-10 * time.Minute)
	th.lastSeen[NodeID(2)] = time.Now().Add(-10 * time.Minute)
	th.mu.Unlock()

	// Evict entries older than 5 minutes.
	th.evictStale(5 * time.Minute)

	th.mu.Lock()
	defer th.mu.Unlock()
	if len(th.perNode) != 1 {
		t.Fatalf("expected 1 tracked node after eviction, got %d", len(th.perNode))
	}
	if _, ok := th.perNode[NodeID(3)]; !ok {
		t.Fatal("expected node 3 to survive eviction")
	}
}

// TestNodeRegistrationThrottle_StartStopCleanup confirms the cleanup
// goroutine starts and stops without races.
func TestNodeRegistrationThrottle_StartStopCleanup(t *testing.T) {
	th := NewNodeRegistrationThrottle(nil)
	th.Allow(NodeID(1))
	th.StartCleanup(100 * time.Millisecond)
	// Let at least one cleanup tick fire.
	time.Sleep(250 * time.Millisecond)
	th.StopCleanup()
}
