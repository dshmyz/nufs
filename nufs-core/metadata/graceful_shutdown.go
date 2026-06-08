package metadata

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
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
	if d.shutdown.Load() {
		return false
	}
	d.wg.Add(1)
	// Double-check after Add to avoid race with Shutdown
	if d.shutdown.Load() {
		d.wg.Done()
		return false
	}
	return true
}

// End marks an in-flight operation as complete.
func (d *ShutdownDrain) End() {
	d.wg.Done()
}

// Shutdown initiates graceful drain. It first marks the store as
// shutting down (rejecting new requests), then waits for in-flight
// operations to complete up to the configured timeout.
func (d *ShutdownDrain) Shutdown() error {
	d.shutdown.Store(true)

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(d.timeout):
		return fmt.Errorf("shutdown: timed out after %s waiting for in-flight operations", d.timeout)
	}
}

// IsShuttingDown returns true if shutdown has been initiated.
func (d *ShutdownDrain) IsShuttingDown() bool {
	return d.shutdown.Load()
}

// ============================================================
// BackupManager — Scheduled Pebble database backups
// ============================================================

// BackupConfig configures the backup manager.
type BackupConfig struct {
	// Dir is the directory to write backup files.
	Dir string

	// Interval is how often to create a backup.
	Interval time.Duration

	// MaxBackups is the maximum number of backup files to keep.
	// Older backups are deleted. 0 = unlimited.
	MaxBackups int

	// DryRun if true, logs what would happen but doesn't write.
	DryRun bool
}

// BackupManager creates periodic Pebble checkpoint backups.
type BackupManager struct {
	store  *PebbleStore
	cfg    BackupConfig
	stopCh chan struct{}
	done   chan struct{}
}

// NewBackupManager creates a new backup manager.
func NewBackupManager(store *PebbleStore, cfg BackupConfig) *BackupManager {
	return &BackupManager{
		store:  store,
		cfg:    cfg,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// Start begins the backup loop. Returns immediately.
func (bm *BackupManager) Start() {
	go bm.loop()
}

// Stop stops the backup loop and waits for it to finish.
func (bm *BackupManager) Stop() {
	close(bm.stopCh)
	<-bm.done
}

func (bm *BackupManager) loop() {
	defer close(bm.done)

	ticker := time.NewTicker(bm.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := bm.doBackup(); err != nil {
				slog.Error("backup failed", "error", err)
			}
		case <-bm.stopCh:
			return
		}
	}
}

// doBackup creates a single checkpoint backup.
func (bm *BackupManager) doBackup() error {
	if bm.cfg.DryRun {
		slog.Info("backup dry-run: would create backup", "dir", bm.cfg.Dir)
		return nil
	}

	start := time.Now()
	backupPath := fmt.Sprintf("%s/backup-%s", bm.cfg.Dir, time.Now().Format("20060102-150405"))

	// Use Pebble's checkpoint API for a consistent snapshot
	if err := bm.store.db.Checkpoint(bm.cfg.Dir); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}

	slog.Info("backup completed", "duration", time.Since(start).Round(time.Millisecond), "path", backupPath)

	// Prune old backups
	if bm.cfg.MaxBackups > 0 {
		bm.prune()
	}
	return nil
}

// prune removes old backup directories beyond MaxBackups.
func (bm *BackupManager) prune() {
	entries, err := readBackupDirs(bm.cfg.Dir)
	if err != nil {
		slog.Error("backup prune failed", "error", err)
		return
	}
	for len(entries) > bm.cfg.MaxBackups {
		oldest := entries[0]
		entries = entries[1:]
		if err := removeBackupDir(bm.cfg.Dir + "/" + oldest); err != nil {
			slog.Error("backup prune remove failed", "dir", oldest, "error", err)
		} else {
			slog.Info("pruned old backup", "dir", oldest)
		}
	}
}

// readBackupDirs returns sorted list of backup subdirectories.
func readBackupDirs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "backup-") {
			dirs = append(dirs, e.Name())
		}
	}
	sort.Strings(dirs)
	return dirs, nil
}

// removeBackupDir recursively removes a backup directory.
func removeBackupDir(dir string) error {
	return os.RemoveAll(dir)
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

// ============================================================
// BucketQuota — Per-bucket resource quotas
// ============================================================

// BucketQuota defines resource limits for a bucket.
type BucketQuota struct {
	MaxSizeBytes  int64 // Maximum total size in bytes (0 = unlimited)
	MaxObjects    int64 // Maximum number of objects (0 = unlimited)
	MaxChunkCount int64 // Maximum number of chunks (0 = unlimited)
}

// QuotaManager tracks per-bucket resource usage against quotas.
type QuotaManager struct {
	mu     sync.RWMutex
	quotas map[string]*BucketQuota
	usage  map[string]*BucketUsage
}

// NewQuotaManager creates a new quota manager.
func NewQuotaManager() *QuotaManager {
	return &QuotaManager{
		quotas: make(map[string]*BucketQuota),
		usage:  make(map[string]*BucketUsage),
	}
}

// SetQuota sets the quota for a bucket.
func (qm *QuotaManager) SetQuota(bucket string, quota *BucketQuota) {
	qm.mu.Lock()
	qm.quotas[bucket] = quota
	qm.mu.Unlock()
}

// GetQuota returns the quota for a bucket, or nil if none is set.
func (qm *QuotaManager) GetQuota(bucket string) *BucketQuota {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	return qm.quotas[bucket]
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
		return fmt.Errorf("quota: bucket %s would exceed size limit (%d + %d > %d)",
			bucket, u.UsedBytes, sizeBytes, q.MaxSizeBytes)
	}
	if q.MaxObjects > 0 && u != nil && int64(u.Objects)+1 > q.MaxObjects {
		return fmt.Errorf("quota: bucket %s would exceed object limit (%d + 1 > %d)",
			bucket, u.Objects, q.MaxObjects)
	}
	return nil
}

// UpdateUsage updates the tracked usage for a bucket.
func (qm *QuotaManager) UpdateUsage(bucket string, usage *BucketUsage) {
	qm.mu.Lock()
	qm.usage[bucket] = usage
	qm.mu.Unlock()
}
