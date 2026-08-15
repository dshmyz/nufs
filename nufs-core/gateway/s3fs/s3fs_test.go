package s3fs

import (
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ─── Config ──────────────────────────────────────────────────────────────────

func TestParseTarget(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		wantHost   string
		wantBucket string
		wantBase   string
		wantErr    bool
	}{
		{
			name:       "simple bucket",
			target:     "https://s3.amazonaws.com/my-bucket",
			wantHost:   "s3.amazonaws.com",
			wantBucket: "my-bucket",
			wantBase:   "",
		},
		{
			name:       "bucket with prefix",
			target:     "https://s3.amazonaws.com/my-bucket/prefix",
			wantHost:   "s3.amazonaws.com",
			wantBucket: "my-bucket",
			wantBase:   "prefix",
		},
		{
			name:       "deep prefix",
			target:     "https://s3.amazonaws.com/bucket/a/b/c",
			wantHost:   "s3.amazonaws.com",
			wantBucket: "bucket",
			wantBase:   "a/b/c",
		},
		{
			name:       "http scheme",
			target:     "http://localhost:9000/mybucket",
			wantHost:   "localhost:9000",
			wantBucket: "mybucket",
			wantBase:   "",
		},
		{
			name:    "no host",
			target:  "invalid",
			wantErr: true,
		},
		{
			name:    "root path only",
			target:  "https://s3.amazonaws.com/",
			wantErr: true,
		},
		{
			name:    "no path",
			target:  "https://s3.amazonaws.com",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, bucket, base, err := ParseTarget(tt.target)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if u.Host != tt.wantHost {
				t.Errorf("host = %q, want %q", u.Host, tt.wantHost)
			}
			if bucket != tt.wantBucket {
				t.Errorf("bucket = %q, want %q", bucket, tt.wantBucket)
			}
			if base != tt.wantBase {
				t.Errorf("basePath = %q, want %q", base, tt.wantBase)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	u, _ := url.Parse("https://s3.amazonaws.com/bucket")
	cfg := &Config{
		Bucket:   "test",
		Target:   u,
		CacheDir: "/tmp/cache",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ScanTTL != 60*time.Second {
		t.Errorf("ScanTTL = %v, want 60s", cfg.ScanTTL)
	}
	if cfg.MetricsAddr != ":9900" {
		t.Errorf("MetricsAddr = %q, want :9900", cfg.MetricsAddr)
	}
	if cfg.Mode != 0660 {
		t.Errorf("Mode = %v, want 0660", cfg.Mode)
	}

	// Missing bucket.
	cfg2 := &Config{Target: u, CacheDir: "/tmp/cache"}
	if err := cfg2.Validate(); err == nil {
		t.Fatal("expected error for missing bucket")
	}

	// Missing target.
	cfg3 := &Config{Bucket: "test", CacheDir: "/tmp/cache"}
	if err := cfg3.Validate(); err == nil {
		t.Fatal("expected error for missing target")
	}

	// Missing cache dir.
	cfg4 := &Config{Bucket: "test", Target: u}
	if err := cfg4.Validate(); err == nil {
		t.Fatal("expected error for missing cache dir")
	}
}

func TestRemotePath(t *testing.T) {
	u, _ := url.Parse("https://s3.amazonaws.com/bucket")
	cfg := &Config{Bucket: "bucket", Target: u, BasePath: "prefix"}
	if got := cfg.RemotePath("file.txt"); got != "prefix/file.txt" {
		t.Errorf("RemotePath = %q, want %q", got, "prefix/file.txt")
	}
	if got := cfg.RemotePath(""); got != "prefix/" {
		t.Errorf("RemotePath empty = %q, want %q", got, "prefix/")
	}
}

func TestConfigOptions(t *testing.T) {
	cfg := &Config{}
	WithBucket("b")(cfg)
	WithBasePath("p")(cfg)
	WithCacheDir("d")(cfg)
	WithScanTTL(10 * time.Second)(cfg)
	WithMetricsAddr(":9999")(cfg)
	WithReadOnly()(cfg)
	WithCacheQuota(123)(cfg)
	WithUID(1000)(cfg)
	WithGID(1000)(cfg)
	WithInsecure()(cfg)
	WithDebug()(cfg)

	if cfg.Bucket != "b" || cfg.BasePath != "p" || cfg.CacheDir != "d" {
		t.Error("options not applied")
	}
	if cfg.ScanTTL != 10*time.Second {
		t.Error("ScanTTL option failed")
	}
	if cfg.MetricsAddr != ":9999" {
		t.Error("MetricsAddr option failed")
	}
	if !cfg.ReadOnly || cfg.CacheQuota != 123 || cfg.UID != 1000 || cfg.GID != 1000 {
		t.Error("options not applied")
	}
	if !cfg.Insecure || !cfg.Debug {
		t.Error("Insecure/Debug options failed")
	}
}

func TestCredentialsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ac := &AccessConfig{
		Version:   "1",
		AccessKey: "AKID",
		SecretKey: "SECRET",
	}
	if err := SaveCredentials(dir, ac); err != nil {
		t.Fatalf("SaveCredentials: %v", err)
	}
	loaded := LoadCredentials(dir)
	if loaded.AccessKey != "AKID" || loaded.SecretKey != "SECRET" {
		t.Errorf("got %+v, want AKID/SECRET", loaded)
	}
}

func TestCredentialsEnvOverride(t *testing.T) {
	dir := t.TempDir()
	ac := &AccessConfig{Version: "1", AccessKey: "from_file", SecretKey: "from_file"}
	SaveCredentials(dir, ac)

	t.Setenv("S3FS_ACCESS_KEY", "from_env")
	loaded := LoadCredentials(dir)
	if loaded.AccessKey != "from_env" {
		t.Errorf("want from_env, got %s", loaded.AccessKey)
	}
	if loaded.SecretKey != "from_file" {
		t.Errorf("want from_file (not overridden), got %s", loaded.SecretKey)
	}
}

// ─── Breaker ─────────────────────────────────────────────────────────────────

func TestCircuitBreakerClosed(t *testing.T) {
	cb := newCircuitBreaker(3, time.Second)
	if cb.State() != "closed" {
		t.Fatalf("initial state = %s, want closed", cb.State())
	}

	count := 0
	err := cb.Execute(func() error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("executed %d times, want 1", count)
	}
	if cb.State() != "closed" {
		t.Errorf("state = %s, want closed", cb.State())
	}
}

func TestCircuitBreakerOpensOnFailures(t *testing.T) {
	cb := newCircuitBreaker(3, time.Minute)

	for i := 0; i < 3; i++ {
		err := cb.Execute(func() error {
			return errCircuitOpen
		})
		if err == nil {
			t.Fatalf("attempt %d: expected error", i)
		}
	}
	if cb.State() != "open" {
		t.Errorf("state = %s, want open", cb.State())
	}

	err := cb.Execute(func() error {
		return nil
	})
	if err == nil {
		t.Fatal("expected circuitOpen error in open state")
	}
}

func TestCircuitBreakerHalfOpen(t *testing.T) {
	cb := newCircuitBreaker(1, 50*time.Millisecond)

	cb.Execute(func() error { return errCircuitOpen })
	if cb.State() != "open" {
		t.Fatalf("state = %s, want open", cb.State())
	}

	time.Sleep(60 * time.Millisecond)

	count := 0
	err := cb.Execute(func() error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("executed %d times, want 1", count)
	}
	if cb.State() != "closed" {
		t.Errorf("state = %s, want closed", cb.State())
	}
}

func TestCircuitBreakerHalfOpenFailsAgain(t *testing.T) {
	cb := newCircuitBreaker(1, 50*time.Millisecond)

	cb.Execute(func() error { return errCircuitOpen })
	time.Sleep(60 * time.Millisecond)

	err := cb.Execute(func() error { return errCircuitOpen })
	if err == nil {
		t.Fatal("expected error")
	}
	if cb.State() != "open" {
		t.Errorf("state = %s, want open", cb.State())
	}
}

// ─── Retry ───────────────────────────────────────────────────────────────────

func TestRetrySuccess(t *testing.T) {
	count := 0
	err := retryWithBackoff(func() error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("called %d times, want 1", count)
	}
}

func TestRetryEventuallySucceeds(t *testing.T) {
	count := 0
	err := retryWithBackoff(func() error {
		count++
		if count < 3 {
			return errCircuitOpen
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Fatalf("called %d times, want 3", count)
	}
}

func TestRetryExhausted(t *testing.T) {
	count := 0
	err := retryWithBackoff(func() error {
		count++
		return errCircuitOpen
	})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if count != maxRetries+1 {
		t.Fatalf("called %d times, want %d", count, maxRetries+1)
	}
}

// ─── Lock ────────────────────────────────────────────────────────────────────

func TestLockUnlock(t *testing.T) {
	var fs S3FileSystem
	fs.locks = newLockMap()

	if err := fs.Lock("test"); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if !fs.IsLocked("test") {
		t.Error("expected locked")
	}
	fs.Unlock("test")
	if fs.IsLocked("test") {
		t.Error("expected unlocked")
	}
}

func TestLockBlocks(t *testing.T) {
	fs := &S3FileSystem{locks: newLockMap()}
	fs.Lock("test")

	done := make(chan bool)
	go func() {
		fs.Lock("test")
		done <- true
	}()

	select {
	case <-done:
		t.Fatal("Lock should have blocked")
	case <-time.After(50 * time.Millisecond):
	}

	fs.Unlock("test")

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Lock should have been acquired after unlock")
	}
}

func TestWaitTimeout(t *testing.T) {
	fs := &S3FileSystem{locks: newLockMap(), lockWait: 100 * time.Millisecond}
	fs.Lock("test")

	start := time.Now()
	err := fs.Wait("test")
	elapsed := time.Since(start)
	if err != errTimeout {
		t.Fatalf("expected errTimeout, got %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("Wait took %v, should have timed out quickly", elapsed)
	}
}

func TestWaitUnlocked(t *testing.T) {
	fs := &S3FileSystem{locks: newLockMap()}
	if err := fs.Wait("not-locked"); err != nil {
		t.Fatalf("Wait should succeed on unlocked path: %v", err)
	}
}

func TestLockFIFO(t *testing.T) {
	// With the direct-transfer FIFO implementation the lock is served
	// in queue order, not spawn order. Since goroutine scheduling is
	// non-deterministic this test simply verifies that all waiters
	// eventually acquire the lock.
	fs := &S3FileSystem{locks: newLockMap()}
	fs.Lock("test")

	done := make(chan struct{}, 3)
	for i := 0; i < 3; i++ {
		go func() {
			fs.Lock("test")
			fs.Unlock("test")
			done <- struct{}{}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	fs.Unlock("test")

	timeout := time.After(2 * time.Second)
	for i := 0; i < 3; i++ {
		select {
		case <-done:
		case <-timeout:
			t.Fatalf("timeout waiting for waiter %d", i)
		}
	}
}

func TestIsLockedNotExist(t *testing.T) {
	fs := &S3FileSystem{locks: newLockMap()}
	if fs.IsLocked("nonexistent") {
		t.Error("expected false for non-existent path")
	}
}

// ─── Metrics ─────────────────────────────────────────────────────────────────

func TestHistogram(t *testing.T) {
	h := newHistogram()
	h.observe(0.001)
	h.observe(0.05)
	h.observe(0.5)
	h.observe(5.0)

	snap := h.snapshot()
	if snap.count != 4 {
		t.Errorf("count = %d, want 4", snap.count)
	}
	if snap.sum <= 0 {
		t.Error("sum should be positive")
	}
}

func TestMetricsSnapshot(t *testing.T) {
	// Reset the counters this test asserts on so it is idempotent: Go runs the
	// same test N times in one process under -count=N, so without a reset the
	// second iteration sees the first one's increments (open=2, want 1).
	atomic.StoreUint64(&globalMetrics.OpsOpen, 0)
	atomic.StoreUint64(&globalMetrics.OpsRead, 0)
	atomic.StoreUint64(&globalMetrics.S3Get, 0)
	atomic.StoreUint64(&globalMetrics.ActiveHandles, 0)

	metricsIncOpen()
	metricsIncRead()
	metricsIncS3Get()
	metricsIncActiveHandles()
	defer metricsDecActiveHandles()

	snap := globalMetrics.Snapshot()
	if snap["active_handles"].(uint64) != 1 {
		t.Errorf("active_handles = %d, want 1", snap["active_handles"])
	}
	ops := snap["ops"].(map[string]uint64)
	if ops["open"] != 1 {
		t.Errorf("ops.open = %d, want 1", ops["open"])
	}
	s3ops := snap["s3"].(map[string]uint64)
	if s3ops["get"] != 1 {
		t.Errorf("s3.get = %d, want 1", s3ops["get"])
	}
}

func TestPrometheusOutput(t *testing.T) {
	handler := prometheusHandler()
	req, _ := http.NewRequest("GET", "/metrics", nil)
	w := &testResponseWriter{header: make(http.Header)}
	handler(w, req)

	body := w.buf.String()
	if !strings.Contains(body, "s3fs_uptime_seconds") {
		t.Error("prometheus output missing uptime")
	}
	if !strings.Contains(body, "s3fs_ops_total{op=\"open\"}") {
		t.Error("prometheus output missing ops")
	}
	if !strings.Contains(body, "s3fs_s3_get_duration_seconds") {
		t.Error("prometheus output missing s3 histogram")
	}
}

func TestMetricsJSONHandler(t *testing.T) {
	handler := metricsJSONHandler()
	req, _ := http.NewRequest("GET", "/metrics/json", nil)
	w := &testResponseWriter{header: make(http.Header)}
	handler(w, req)

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(w.buf.String()), &result); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if _, ok := result["uptime_seconds"]; !ok {
		t.Error("json missing uptime_seconds")
	}
}

func TestHealthHandler(t *testing.T) {
	handler := healthHandler()
	req, _ := http.NewRequest("GET", "/healthz", nil)
	w := &testResponseWriter{header: make(http.Header)}
	handler(w, req)

	if w.code != 200 {
		t.Errorf("status = %d, want 200", w.code)
	}
	if strings.TrimSpace(w.buf.String()) != "ok" {
		t.Errorf("body = %q, want ok", w.buf.String())
	}
}

// ─── Ops ─────────────────────────────────────────────────────────────────────

func TestNewMoveOp(t *testing.T) {
	op := newMoveOp("src", "dst")
	if op.Source != "src" || op.Target != "dst" {
		t.Error("MoveOp fields not set")
	}
	if cap(op.Error) != 1 {
		t.Error("Error channel should be buffered with cap 1")
	}
}

func TestNewCopyOp(t *testing.T) {
	op := newCopyOp("src", "dst")
	if op.Source != "src" || op.Target != "dst" {
		t.Error("CopyOp fields not set")
	}
}

func TestNewPutOp(t *testing.T) {
	op := newPutOp("src", "dst", 100)
	if op.Source != "src" || op.Target != "dst" || op.Length != 100 {
		t.Error("PutOp fields not set")
	}
}

// ─── Cache ───────────────────────────────────────────────────────────────────

func TestPebbleCacheInodeRoundTrip(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCache(path.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("OpenCache: %v", err)
	}
	defer c.Close()

	in := &CacheInode{
		ID:   42,
		Name: "test.txt",
		Size: 100,
		Mode: 0644,
		UID:  1000,
		GID:  1000,
	}
	if err := c.PutInode(in); err != nil {
		t.Fatalf("PutInode: %v", err)
	}

	got, err := c.GetInode(42)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if got.Name != "test.txt" || got.Size != 100 {
		t.Errorf("got %+v, want Name=test.txt Size=100", got)
	}
}

func TestPebbleCacheInodeNotFound(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCache(path.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("OpenCache: %v", err)
	}
	defer c.Close()

	_, err = c.GetInode(999)
	if err != errCacheNotFound {
		t.Fatalf("expected errCacheNotFound, got %v", err)
	}
}

func TestPebbleCacheDeleteInode(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCache(path.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("OpenCache: %v", err)
	}
	defer c.Close()

	c.PutInode(&CacheInode{ID: 1, Name: "del"})
	if err := c.DeleteInode(1); err != nil {
		t.Fatalf("DeleteInode: %v", err)
	}
	_, err = c.GetInode(1)
	if err != errCacheNotFound {
		t.Fatal("expected not found after delete")
	}
}

func TestPebbleCacheDirEntries(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCache(path.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("OpenCache: %v", err)
	}
	defer c.Close()

	if err := c.PutDirEntry(1, "file1.txt", 100); err != nil {
		t.Fatalf("PutDirEntry: %v", err)
	}
	if err := c.PutDirEntry(1, "file2.txt", 200); err != nil {
		t.Fatalf("PutDirEntry: %v", err)
	}

	id, err := c.GetDirEntry(1, "file1.txt")
	if err != nil {
		t.Fatalf("GetDirEntry: %v", err)
	}
	if id != 100 {
		t.Errorf("id = %d, want 100", id)
	}

	_, err = c.GetDirEntry(1, "nonexistent")
	if err != errCacheNotFound {
		t.Fatal("expected not found for nonexistent")
	}

	entries, err := c.ListDirEntries(1)
	if err != nil {
		t.Fatalf("ListDirEntries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	if entries["file1.txt"] != 100 || entries["file2.txt"] != 200 {
		t.Errorf("entries = %v", entries)
	}
}

func TestPebbleCacheDeleteDirEntry(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCache(path.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("OpenCache: %v", err)
	}
	defer c.Close()

	c.PutDirEntry(1, "f1", 10)
	c.DeleteDirEntry(1, "f1")
	_, err = c.GetDirEntry(1, "f1")
	if err != errCacheNotFound {
		t.Fatal("expected not found after delete")
	}
}

func TestPebbleCacheNextID(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCache(path.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("OpenCache: %v", err)
	}
	defer c.Close()

	id1 := c.NextID()
	id2 := c.NextID()
	if id2 != id1+1 {
		t.Errorf("ids not monotonic: %d then %d", id1, id2)
	}
}

func TestPebbleCacheNextIDResumesFromMax(t *testing.T) {
	dir := t.TempDir()
	dbPath := path.Join(dir, "cache.db")

	c1, err := OpenCache(dbPath)
	if err != nil {
		t.Fatalf("OpenCache 1: %v", err)
	}
	c1.PutInode(&CacheInode{ID: 100, Name: "high"})
	c1.Close()

	c2, err := OpenCache(dbPath)
	if err != nil {
		t.Fatalf("OpenCache 2: %v", err)
	}
	defer c2.Close()

	id := c2.NextID()
	if id <= 100 {
		t.Errorf("NextID = %d, want > 100", id)
	}
}

func TestPebbleCachePendingUploads(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCache(path.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("OpenCache: %v", err)
	}
	defer c.Close()

	pu := &PendingUpload{CachePath: "/tmp/cache/file", RemotePath: "remote/file", Size: 42}
	if err := c.RecordPending(pu); err != nil {
		t.Fatalf("RecordPending: %v", err)
	}

	list, err := c.ListPending()
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("got %d pending, want 1", len(list))
	}
	if list[0].CachePath != "/tmp/cache/file" || list[0].RemotePath != "remote/file" || list[0].Size != 42 {
		t.Errorf("got %+v", list[0])
	}

	c.ClearPending("/tmp/cache/file")
	list, _ = c.ListPending()
	if len(list) != 0 {
		t.Fatalf("got %d pending after clear, want 0", len(list))
	}
}

func TestPebbleCacheLastScan(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCache(path.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("OpenCache: %v", err)
	}
	defer c.Close()

	id := c.NextID()
	c.PutInode(&CacheInode{ID: id})

	zero := c.GetLastScan(id)
	if !zero.IsZero() {
		t.Error("expected zero time for not-scanned dir")
	}

	now := time.Now()
	if err := c.SetLastScan(id, now); err != nil {
		t.Fatalf("SetLastScan: %v", err)
	}

	got := c.GetLastScan(id)
	if got.Unix() != now.Unix() {
		t.Errorf("got %v, want %v", got, now)
	}
}

func TestPebbleCacheConcurrent(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenCache(path.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("OpenCache: %v", err)
	}
	defer c.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := c.NextID()
			c.PutInode(&CacheInode{ID: id, Name: "f"})
			got, err := c.GetInode(id)
			if err != nil || got.Name != "f" {
				t.Errorf("concurrent get failed: id=%d err=%v", id, err)
			}
		}()
	}
	wg.Wait()
}

// ─── New filesystem validation ───────────────────────────────────────────────

func TestNewConfigValidation(t *testing.T) {
	// Missing bucket should fail.
	u, _ := url.Parse("https://s3.amazonaws.com/bucket")
	cfg := &Config{
		Target:   u,
		CacheDir: t.TempDir(),
	}
	_, err := New(cfg)
	if err == nil {
		t.Fatal("expected error for missing bucket")
	}
}

func TestNewInvalidTarget(t *testing.T) {
	cfg := &Config{
		Bucket:   "test",
		CacheDir: t.TempDir(),
	}
	_, err := New(cfg)
	if err == nil {
		t.Fatal("expected error for nil target")
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

type testResponseWriter struct {
	header http.Header
	buf    strings.Builder
	code   int
}

func (w *testResponseWriter) Header() http.Header { return w.header }

func (w *testResponseWriter) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	return w.buf.Write(b)
}

func (w *testResponseWriter) WriteHeader(code int) { w.code = code }
