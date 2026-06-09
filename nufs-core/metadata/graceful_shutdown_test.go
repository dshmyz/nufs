package metadata

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================
// ShutdownDrain Tests
// ============================================================

func TestShutdownDrain_Begin(t *testing.T) {
	d := NewShutdownDrain(5 * time.Second)
	if !d.Begin() {
		t.Fatal("Begin should succeed before shutdown")
	}
	d.End()
}

func TestShutdownDrain_BeginRejectsAfterShutdown(t *testing.T) {
	d := NewShutdownDrain(1 * time.Second)
	d.Shutdown()
	if d.Begin() {
		t.Fatal("Begin should fail after shutdown")
	}
}

func TestShutdownDrain_WaitsForInflight(t *testing.T) {
	d := NewShutdownDrain(5 * time.Second)

	var done atomic.Bool
	go func() {
		if !d.Begin() {
			return
		}
		time.Sleep(50 * time.Millisecond)
		done.Store(true)
		d.End()
	}()

	time.Sleep(10 * time.Millisecond)
	err := d.Shutdown()
	if err != nil {
		t.Fatalf("Shutdown should succeed: %v", err)
	}
	if !done.Load() {
		t.Fatal("Shutdown should have waited for in-flight operation")
	}
}

func TestShutdownDrain_Timeout(t *testing.T) {
	d := NewShutdownDrain(50 * time.Millisecond)

	d.Begin() // Never call End

	err := d.Shutdown()
	if err == nil {
		t.Fatal("Shutdown should timeout")
	}
}

func TestShutdownDrain_ConcurrentBegin(t *testing.T) {
	d := NewShutdownDrain(5 * time.Second)

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if d.Begin() {
				mu.Lock()
				successCount++
				mu.Unlock()
				time.Sleep(1 * time.Millisecond)
				d.End()
			}
		}()
	}

	wg.Wait()
	if successCount == 0 {
		t.Fatal("at least some Begins should succeed")
	}
}

func TestShutdownDrain_MiddlewareRejectsAfterShutdown(t *testing.T) {
	d := NewShutdownDrain(50 * time.Millisecond)
	h := d.Middleware(nil, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	if err := d.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after shutdown, got %d", rr.Code)
	}
}

func TestShutdownDrain_MiddlewarePublicPathBypassesShutdown(t *testing.T) {
	d := NewShutdownDrain(50 * time.Millisecond)
	h := d.Middleware(map[string]struct{}{"/healthz": {}}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	if err := d.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected public path through, got %d", rr.Code)
	}
}

// ============================================================
// ============================================================

func TestRateLimiter_BasicAllow(t *testing.T) {
	rl := NewRateLimiter(10, 10) // 10/sec, burst 10
	if !rl.Allow("key1") {
		t.Fatal("first request should be allowed")
	}
}

func TestRateLimiter_BurstExhaustion(t *testing.T) {
	rl := NewRateLimiter(1, 5) // 1/sec, burst 5

	for i := 0; i < 5; i++ {
		if !rl.Allow("key1") {
			t.Fatalf("request %d should be allowed", i)
		}
	}

	// 6th request should be rejected (burst exhausted)
	if rl.Allow("key1") {
		t.Fatal("request 6 should be rejected")
	}
}

func TestRateLimiter_Refill(t *testing.T) {
	rl := NewRateLimiter(100, 1) // 100/sec, burst 1

	if !rl.Allow("key1") {
		t.Fatal("first request should be allowed")
	}

	// Wait for refill
	time.Sleep(20 * time.Millisecond)

	if !rl.Allow("key1") {
		t.Fatal("request after refill should be allowed")
	}
}

func TestRateLimiter_IndependentKeys(t *testing.T) {
	rl := NewRateLimiter(1, 1) // 1/sec, burst 1

	if !rl.Allow("key1") {
		t.Fatal("key1 first request should be allowed")
	}
	if !rl.Allow("key2") {
		t.Fatal("key2 first request should be allowed (independent)")
	}
}

func TestRateLimiter_Remove(t *testing.T) {
	rl := NewRateLimiter(1, 1)

	rl.Allow("key1") // exhaust
	rl.Remove("key1")

	if !rl.Allow("key1") {
		t.Fatal("after Remove, key should be allowed again")
	}
}

// ============================================================
// QuotaManager Tests
// ============================================================

func TestQuotaManager_NoQuota(t *testing.T) {
	qm := NewQuotaManager()
	if err := qm.CheckWrite("bucket1", 1024); err != nil {
		t.Fatalf("no quota should allow any write: %v", err)
	}
}

func TestQuotaManager_SizeLimit(t *testing.T) {
	qm := NewQuotaManager()
	if err := qm.SetQuota("bucket1", &BucketQuota{MaxSizeBytes: 1000}); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}

	if err := qm.UpdateUsage("bucket1", &BucketUsage{UsedBytes: 500, Objects: 1}); err != nil {
		t.Fatalf("UpdateUsage: %v", err)
	}

	if err := qm.CheckWrite("bucket1", 400); err != nil {
		t.Fatalf("write within quota should succeed: %v", err)
	}

	if err := qm.CheckWrite("bucket1", 600); err == nil {
		t.Fatal("write exceeding quota should fail")
	}
}

func TestQuotaManager_ObjectLimit(t *testing.T) {
	qm := NewQuotaManager()
	if err := qm.SetQuota("bucket1", &BucketQuota{MaxObjects: 2}); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}

	if err := qm.UpdateUsage("bucket1", &BucketUsage{Objects: 2}); err != nil {
		t.Fatalf("UpdateUsage: %v", err)
	}

	if err := qm.CheckWrite("bucket1", 100); err == nil {
		t.Fatal("write exceeding object limit should fail")
	}
}

func TestQuotaManager_SetGetQuota(t *testing.T) {
	qm := NewQuotaManager()

	if qm.GetQuota("bucket1") != nil {
		t.Fatal("GetQuota should return nil for unset bucket")
	}

	if err := qm.SetQuota("bucket1", &BucketQuota{MaxSizeBytes: 1000}); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	q := qm.GetQuota("bucket1")
	if q == nil || q.MaxSizeBytes != 1000 {
		t.Fatal("GetQuota should return the set quota")
	}
}

// ============================================================
// BackupManager Tests (unit-level, no actual Pebble)
// ============================================================

func TestBackupConfig_Fields(t *testing.T) {
	cfg := BackupConfig{
		Dir:        "/tmp/backup",
		Interval:   1 * time.Hour,
		MaxBackups: 7,
		DryRun:     true,
	}
	if cfg.Dir != "/tmp/backup" {
		t.Fatal("Dir mismatch")
	}
	if cfg.Interval != 1*time.Hour {
		t.Fatal("Interval mismatch")
	}
	if cfg.MaxBackups != 7 {
		t.Fatal("MaxBackups mismatch")
	}
	if !cfg.DryRun {
		t.Fatal("DryRun should be true")
	}
}
