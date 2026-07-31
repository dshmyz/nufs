package metadata

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================
// Graceful Shutdown — drain in-flight operations before exit
// ============================================================

// ShutdownDrain tracks in-flight operations and provides a way to
// wait for them all to complete during graceful shutdown.
type ShutdownDrain struct {
	mu       sync.Mutex
	wg       sync.WaitGroup
	counter  atomic.Int64 // tracks in-flight count for diagnostics
	shutdown atomic.Bool
	timeout  time.Duration
}

// NewShutdownDrain creates a new drain with the given timeout.
func NewShutdownDrain(timeout time.Duration) *ShutdownDrain {
	return &ShutdownDrain{timeout: timeout}
}

// Begin marks an operation as in-flight. Returns false if shutdown
// has already been initiated (caller should reject the request).
func (d *ShutdownDrain) Begin() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.shutdown.Load() {
		return false
	}
	d.wg.Add(1)
	d.counter.Add(1)
	return true
}

// End marks an in-flight operation as complete.
func (d *ShutdownDrain) End() {
	d.counter.Add(-1)
	d.wg.Done()
}

// Shutdown initiates graceful drain. It first marks the store as
// shutting down (rejecting new requests), then waits for in-flight
// operations to complete up to the configured timeout.
// If operations remain after timeout, it logs a warning and returns
// an error with the count of remaining operations.
func (d *ShutdownDrain) Shutdown() error {
	d.mu.Lock()
	d.shutdown.Store(true)
	d.mu.Unlock()

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(d.timeout):
		// Count remaining in-flight operations
		remaining := int(d.counter.Load())
		if remaining > 0 {
			slog.Warn("shutdown: timed out waiting for in-flight operations",
				"remaining", remaining, "timeout", d.timeout)
		}
		return fmt.Errorf("shutdown: timed out after %s with %d in-flight operations remaining", d.timeout, remaining)
	}
}

// IsShuttingDown returns true if shutdown has been initiated.
func (d *ShutdownDrain) IsShuttingDown() bool {
	return d.shutdown.Load()
}

// Middleware wraps an HTTP handler so the drain can reject new requests and
// wait for accepted ones to finish during shutdown. Paths in publicPaths bypass
// the drain and remain available for liveness/metrics probes.
func (d *ShutdownDrain) Middleware(publicPaths map[string]struct{}, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := publicPaths[r.URL.Path]; ok {
			next.ServeHTTP(w, r)
			return
		}
		if !d.Begin() {
			http.Error(w, "service shutting down", http.StatusServiceUnavailable)
			return
		}
		defer d.End()
		next.ServeHTTP(w, r)
	})
}

// RemoteStorage is the interface for uploading backup artifacts to a
// remote object store (S3, OSS, GCS, etc.). Implementations handle
// authentication, retry, and multipart upload internally.
type RemoteStorage interface {
	// Upload copies the local backup directory to the remote store.
	// The key parameter identifies the backup (e.g., "backup-20260608-120000").
	Upload(ctx context.Context, key string, localDir string) error
}

// ============================================================
// RateLimiter — Token bucket per-bucket/per-client rate limiting
// ============================================================

// RateLimiter provides per-key token bucket rate limiting.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	rate    float64 // tokens per second
	burst   int     // max tokens
}

type tokenBucket struct {
	tokens   float64
	lastTime time.Time
}

// NewRateLimiter creates a rate limiter with the given rate and burst.
func NewRateLimiter(rate float64, burst int) *RateLimiter {
	return &RateLimiter{
		buckets: make(map[string]*tokenBucket),
		rate:    rate,
		burst:   burst,
	}
}

// Allow checks if a request for the given key should be allowed.
func (rl *RateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[key]
	if !ok {
		b = &tokenBucket{tokens: float64(rl.burst), lastTime: time.Now()}
		rl.buckets[key] = b
	}

	now := time.Now()
	elapsed := now.Sub(b.lastTime).Seconds()
	b.tokens += elapsed * rl.rate
	if b.tokens > float64(rl.burst) {
		b.tokens = float64(rl.burst)
	}
	b.lastTime = now

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true
	}
	return false
}

// Remove removes the rate limit bucket for the given key.
func (rl *RateLimiter) Remove(key string) {
	rl.mu.Lock()
	delete(rl.buckets, key)
	rl.mu.Unlock()
}

// Burst returns the maximum burst size.
func (rl *RateLimiter) Burst() int {
	return rl.burst
}

// Available returns the number of tokens available for the given key.
func (rl *RateLimiter) Available(key string) int {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[key]
	if !ok {
		return rl.burst
	}

	// Calculate tokens without modifying state
	now := time.Now()
	elapsed := now.Sub(b.lastTime).Seconds()
	tokens := b.tokens + elapsed*rl.rate
	if tokens > float64(rl.burst) {
		tokens = float64(rl.burst)
	}
	return int(tokens)
}

// WaitTime returns how long until the next token is available for the key.
func (rl *RateLimiter) WaitTime(key string) time.Duration {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[key]
	if !ok {
		return 0
	}

	if b.tokens >= 1.0 {
		return 0
	}

	// Time to accumulate 1 token
	tokensNeeded := 1.0 - b.tokens
	waitSeconds := tokensNeeded / rl.rate
	return time.Duration(waitSeconds * float64(time.Second))
}

// StartCleanup starts a background goroutine that periodically removes
// stale rate limit buckets to prevent unbounded memory growth.
// Call stop() to terminate the goroutine.
func (rl *RateLimiter) StartCleanup(interval time.Duration) (stop func()) {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				rl.mu.Lock()
				now := time.Now()
				for key, b := range rl.buckets {
					if now.Sub(b.lastTime) > 5*interval {
						delete(rl.buckets, key)
					}
				}
				rl.mu.Unlock()
			}
		}
	}()
	return func() { close(done) }
}

// ============================================================
// BucketQuota — Per-bucket resource quotas
// ============================================================

// BucketQuota defines resource limits for a bucket.
type BucketQuota struct {
	MaxSizeBytes  int64 `json:"max_bytes"`   // Maximum total size in bytes (0 = unlimited)
	MaxObjects    int64 `json:"max_objects"` // Maximum number of objects (0 = unlimited)
	MaxChunkCount int64 `json:"-"`           // Maximum number of chunks (0 = unlimited)
}

// Validate verifies that quota limits are non-negative.
func (q *BucketQuota) Validate() error {
	if q == nil {
		return fmt.Errorf("quota: quota is required")
	}
	if q.MaxSizeBytes < 0 || q.MaxObjects < 0 || q.MaxChunkCount < 0 {
		return fmt.Errorf("quota: negative limits are invalid")
	}
	return nil
}

// QuotaManager tracks per-bucket resource usage against quotas.
// When a QuotaStore is configured, quota changes are persisted so they
// survive restarts.
type QuotaManager struct {
	mu     sync.RWMutex
	quotas map[string]*BucketQuota
	usage  map[string]*BucketUsage
	store  QuotaStore // optional persistence backend
}

// QuotaStore is the interface for persisting quota data.
type QuotaStore interface {
	SaveQuota(bucket string, quota *BucketQuota) error
	SaveUsage(bucket string, usage *BucketUsage) error
}

// NewQuotaManager creates a new quota manager.
func NewQuotaManager() *QuotaManager {
	return &QuotaManager{
		quotas: make(map[string]*BucketQuota),
		usage:  make(map[string]*BucketUsage),
	}
}

// SetStore configures the persistence backend for quota data.
func (qm *QuotaManager) SetStore(store QuotaStore) {
	qm.mu.Lock()
	qm.store = store
	qm.mu.Unlock()
}

// SetQuota sets the quota for a bucket and persists it if a store is configured.
func (qm *QuotaManager) SetQuota(bucket string, quota *BucketQuota) error {
	qm.mu.RLock()
	store := qm.store
	qm.mu.RUnlock()
	if store != nil {
		if err := store.SaveQuota(bucket, quota); err != nil {
			return fmt.Errorf("quota: persist quota for %s: %w", bucket, err)
		}
	}
	qm.mu.Lock()
	qm.quotas[bucket] = quota
	qm.mu.Unlock()
	return nil
}

// GetQuota returns the quota for a bucket, or nil if none is set.
func (qm *QuotaManager) GetQuota(bucket string) *BucketQuota {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	return qm.quotas[bucket]
}

// DeleteQuota removes a bucket quota and its persisted representation.
func (qm *QuotaManager) DeleteQuota(bucket string) error {
	qm.mu.RLock()
	store := qm.store
	qm.mu.RUnlock()
	if store != nil {
		if deleter, ok := store.(interface{ DeleteQuota(bucket string) error }); ok {
			if err := deleter.DeleteQuota(bucket); err != nil {
				return err
			}
		}
	}
	qm.mu.Lock()
	delete(qm.quotas, bucket)
	qm.mu.Unlock()
	return nil
}

// CheckWrite checks if a write of the given size would exceed the quota.
func (qm *QuotaManager) CheckWrite(bucket string, sizeBytes int64) error {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	q, ok := qm.quotas[bucket]
	if !ok || q == nil {
		return nil
	}
	u := qm.usage[bucket]

	if q.MaxSizeBytes > 0 && u != nil && u.UsedBytes+sizeBytes > q.MaxSizeBytes {
		return fmt.Errorf("%w: bucket %s would exceed size limit (%d + %d > %d)",
			ErrQuotaExceeded, bucket, u.UsedBytes, sizeBytes, q.MaxSizeBytes)
	}
	if q.MaxObjects > 0 && u != nil && int64(u.Objects)+1 > q.MaxObjects {
		return fmt.Errorf("%w: bucket %s would exceed object limit (%d + 1 > %d)",
			ErrQuotaExceeded, bucket, u.Objects, q.MaxObjects)
	}
	return nil
}

// CheckWriteDelta checks whether applying actual byte and object deltas would
// exceed a bucket quota.
func (qm *QuotaManager) CheckWriteDelta(bucket string, additionalBytes int64, additionalObjects int64) error {
	qm.mu.RLock()
	defer qm.mu.RUnlock()

	q := qm.quotas[bucket]
	if q == nil {
		return nil
	}
	u := qm.usage[bucket]
	var usedBytes, objects int64
	if u != nil {
		usedBytes = u.UsedBytes
		objects = int64(u.Objects)
	}
	if q.MaxSizeBytes > 0 && additionalBytes > 0 && usedBytes > q.MaxSizeBytes-additionalBytes {
		return fmt.Errorf("%w: bucket %s would exceed size limit (%d + %d > %d)", ErrQuotaExceeded, bucket, usedBytes, additionalBytes, q.MaxSizeBytes)
	}
	if q.MaxObjects > 0 && additionalObjects > 0 && objects > q.MaxObjects-additionalObjects {
		return fmt.Errorf("%w: bucket %s would exceed object limit (%d + %d > %d)", ErrQuotaExceeded, bucket, objects, additionalObjects, q.MaxObjects)
	}
	return nil
}

// UpdateUsage updates the tracked usage for a bucket and persists it.
func (qm *QuotaManager) UpdateUsage(bucket string, usage *BucketUsage) error {
	qm.mu.RLock()
	store := qm.store
	qm.mu.RUnlock()
	if store != nil {
		if err := store.SaveUsage(bucket, usage); err != nil {
			return fmt.Errorf("quota: persist usage for %s: %w", bucket, err)
		}
	}
	qm.mu.Lock()
	qm.usage[bucket] = usage
	qm.mu.Unlock()
	return nil
}

// LoadQuota loads a quota entry from persistence (called during startup).
func (qm *QuotaManager) LoadQuota(bucket string, quota *BucketQuota) {
	qm.mu.Lock()
	if quota == nil {
		delete(qm.quotas, bucket)
	} else {
		qm.quotas[bucket] = quota
	}
	qm.mu.Unlock()
}

// LoadUsage loads a usage entry from persistence (called during startup).
func (qm *QuotaManager) LoadUsage(bucket string, usage *BucketUsage) {
	qm.mu.Lock()
	if usage == nil {
		delete(qm.usage, bucket)
	} else {
		qm.usage[bucket] = usage
	}
	qm.mu.Unlock()
}
