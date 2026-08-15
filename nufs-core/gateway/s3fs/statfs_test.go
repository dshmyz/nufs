package s3fs

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestStatfsUsage_TTLAndNegativeCache verifies the caching contract:
//  1. First get fetches and caches.
//  2. Within TTL: no fetch, stale data served.
//  3. After TTL, failed fetch: stale data served, failure noted (negative cache).
//  4. Within cooldown after failure: stale served, no retry.
//  5. After cooldown: retry succeeds, new data cached.
func TestStatfsUsage_TTLAndNegativeCache(t *testing.T) {
	var calls atomic.Int32
	fetch := func(_ context.Context, _ uint64) (uint64, uint64, bool) {
		n := int(calls.Add(1))
		switch n {
		case 1:
			return 1000, 500, true
		case 2:
			return 0, 0, false // transient failure
		default:
			return 7777, 8888, true
		}
	}

	s := &statfsUsage{
		ttl:      10 * time.Minute, // long TTL so time.Since won't expire naturally
		cooldown: 20 * time.Millisecond,
		fetch:    fetch,
	}

	// (1) First get fetches and caches.
	u, q := s.get(context.Background())
	if u != 1000 || q != 500 {
		t.Fatalf("first get: got usage=%d quota=%d, want 1000/500", u, q)
	}

	// (2) Within TTL: no fetch, stale data served.
	u, q = s.get(context.Background())
	if u != 1000 || q != 500 {
		t.Fatalf("ttl hit: got %d/%d", u, q)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("ttl hit should not trigger fetch, got %d calls", n)
	}

	// (3) Expire the success timestamp; next fetch fails -> stale served.
	s.mu.Lock()
	s.at = time.Now().Add(-20 * time.Minute)
	s.mu.Unlock()

	u, q = s.get(context.Background())
	if u != 1000 || q != 500 {
		t.Fatalf("after ttl fail: got %d/%d", u, q)
	}

	// (4) Within cooldown: stale served, no retry.
	u, _ = s.get(context.Background())
	if u != 1000 {
		t.Fatalf("cooldown stale: got %d", u)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("within cooldown should not retry, got %d calls", n)
	}

	// (5) After cooldown: retry succeeds and caches new data.
	s.mu.Lock()
	s.failAt = time.Now().Add(-20 * time.Millisecond)
	s.mu.Unlock()

	u, q = s.get(context.Background())
	if u != 7777 || q != 8888 {
		t.Fatalf("after cooldown: got %d/%d", u, q)
	}
	if n := calls.Load(); n != 3 {
		t.Fatalf("expected retry after cooldown, got %d calls", n)
	}
}

// TestStatfsUsage_AdminQuotaKeptOnQueryFailure verifies that when the
// DataUsageInfo succeeds but the quota query fails (non-cancel, e.g.
// network), the last known quota is kept rather than overwritten with 0.
func TestStatfsUsage_AdminQuotaKeptOnQueryFailure(t *testing.T) {
	s := &statfsUsage{
		ttl:      time.Minute,
		cooldown: 20 * time.Millisecond,
		fetch: func(_ context.Context, prevQuota uint64) (uint64, uint64, bool) {
			// Simulate: usage correct, quota query fails -> keep prev.
			return 999, prevQuota, true
		},
	}

	u, q := s.get(context.Background())
	if u != 999 || q != 0 {
		t.Fatalf("initial: got %d/%d", u, q)
	}

	// Second fetch: usage updated, quota still 0 (prev was 0).
	s.mu.Lock()
	s.at = time.Now().Add(-time.Hour)
	s.mu.Unlock()
	u, q = s.get(context.Background())
	if u != 999 || q != 0 {
		t.Fatalf("second: got %d/%d", u, q)
	}

	// Third fetch: now quota=1000 (simulating admin setting quota)
	s.fetch = func(_ context.Context, prevQuota uint64) (uint64, uint64, bool) {
		return 888, 1000, true
	}
	s.mu.Lock()
	s.at = time.Now().Add(-time.Hour)
	s.mu.Unlock()
	u, q = s.get(context.Background())
	if u != 888 || q != 1000 {
		t.Fatalf("third: got %d/%d", u, q)
	}

	// Fourth fetch: quota query fails again -> keeps 1000, not 0.
	s.fetch = func(_ context.Context, prevQuota uint64) (uint64, uint64, bool) {
		return 777, prevQuota, true
	}
	s.mu.Lock()
	s.at = time.Now().Add(-time.Hour)
	s.mu.Unlock()
	u, q = s.get(context.Background())
	if u != 777 || q != 1000 {
		t.Fatalf("fourth: got %d/%d", u, q)
	}
}
