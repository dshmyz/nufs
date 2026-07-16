package fuse

import (
	"sync/atomic"
	"testing"
)

// ========== MetricsRecorder 接口契约测试 ==========

// TestNoopRecorder_AllNoOp 验证 noopMetricsRecorder 的所有方法都是空操作，
// 不 panic、不 panic、不影响任何状态。
func TestNoopRecorder_AllNoOp(t *testing.T) {
	var r MetricsRecorder = noopMetricsRecorder{}
	r.IncOp("open")
	r.IncOpError("read")
	r.IncCacheHit()
	r.IncCacheMiss()
	r.IncCacheEvict()
	r.IncBreakerOpen("meta")
	r.IncRetry("write")
	// 无状态可断言，只要不 panic 即通过
}

// TestFUSEMetrics_IncOp 验证 IncOp 正确递增对应的 ops 计数器。
func TestFUSEMetrics_IncOp(t *testing.T) {
	m := &FUSEMetrics{}

	m.IncOp("open")
	m.IncOp("open")
	m.IncOp("read")
	m.IncOp("write")
	m.IncOp("flush")
	m.IncOp("release")
	m.IncOp("lookup")
	m.IncOp("readdir")
	m.IncOp("create")
	m.IncOp("mkdir")
	m.IncOp("rmdir")  // 复用 OpsRemove
	m.IncOp("unlink") // 复用 OpsRemove
	m.IncOp("rename")
	m.IncOp("symlink") // 走 OpsOther
	m.IncOp("link")    // 走 OpsOther
	m.IncOp("statfs")  // 走 OpsOther

	cases := []struct {
		name string
		got  uint64
		want uint64
	}{
		{"OpsOpen", atomic.LoadUint64(&m.OpsOpen), 2},
		{"OpsRead", atomic.LoadUint64(&m.OpsRead), 1},
		{"OpsWrite", atomic.LoadUint64(&m.OpsWrite), 1},
		{"OpsFlush", atomic.LoadUint64(&m.OpsFlush), 1},
		{"OpsRelease", atomic.LoadUint64(&m.OpsRelease), 1},
		{"OpsLookup", atomic.LoadUint64(&m.OpsLookup), 1},
		{"OpsReadDir", atomic.LoadUint64(&m.OpsReadDir), 1},
		{"OpsCreate", atomic.LoadUint64(&m.OpsCreate), 1},
		{"OpsMkdir", atomic.LoadUint64(&m.OpsMkdir), 1},
		{"OpsRemove", atomic.LoadUint64(&m.OpsRemove), 2}, // rmdir + unlink
		{"OpsRename", atomic.LoadUint64(&m.OpsRename), 1},
		{"OpsOther", atomic.LoadUint64(&m.OpsOther), 3}, // symlink + link + statfs
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", c.name, c.got, c.want)
		}
	}
}

// TestFUSEMetrics_IncOp_UnknownOpNoPanic 验证未知 op 名不 panic 也不计数。
func TestFUSEMetrics_IncOp_UnknownOpNoPanic(t *testing.T) {
	m := &FUSEMetrics{}
	m.IncOp("unknown-op")
	if atomic.LoadUint64(&m.OpsOther) != 0 {
		t.Errorf("OpsOther = %d, want 0 (unknown op should not be counted)", m.OpsOther)
	}
}

// TestFUSEMetrics_IncOpError 验证错误计数递增。
func TestFUSEMetrics_IncOpError(t *testing.T) {
	m := &FUSEMetrics{}
	m.IncOpError("read")
	m.IncOpError("read")
	m.IncOpError("write")
	if got := atomic.LoadUint64(&m.OpsErrors); got != 3 {
		t.Errorf("OpsErrors = %d, want 3", got)
	}
}

// TestFUSEMetrics_IncCacheHitMiss 验证缓存命中/未命中计数。
func TestFUSEMetrics_IncCacheHitMiss(t *testing.T) {
	m := &FUSEMetrics{}
	m.IncCacheHit()
	m.IncCacheHit()
	m.IncCacheMiss()
	if got := atomic.LoadUint64(&m.CacheHits); got != 2 {
		t.Errorf("CacheHits = %d, want 2", got)
	}
	if got := atomic.LoadUint64(&m.CacheMisses); got != 1 {
		t.Errorf("CacheMisses = %d, want 1", got)
	}
}

// TestFUSEMetrics_IncCacheEvict 验证缓存淘汰计数。
func TestFUSEMetrics_IncCacheEvict(t *testing.T) {
	m := &FUSEMetrics{}
	m.IncCacheEvict()
	m.IncCacheEvict()
	m.IncCacheEvict()
	if got := atomic.LoadUint64(&m.CacheEvicts); got != 3 {
		t.Errorf("CacheEvicts = %d, want 3", got)
	}
}

// TestFUSEMetrics_IncBreakerOpen 验证熔断器开路计数。
func TestFUSEMetrics_IncBreakerOpen(t *testing.T) {
	m := &FUSEMetrics{}
	m.IncBreakerOpen("meta")
	m.IncBreakerOpen("chunkStore")
	m.IncBreakerOpen("meta")
	if got := atomic.LoadUint64(&m.BreakerOpens); got != 3 {
		t.Errorf("BreakerOpens = %d, want 3", got)
	}
}

// TestFUSEMetrics_IncRetry 验证重试计数递增。
func TestFUSEMetrics_IncRetry(t *testing.T) {
	m := &FUSEMetrics{}
	m.IncRetry("read")
	m.IncRetry("write")
	m.IncRetry("read")
	if got := atomic.LoadUint64(&m.OpsRetries); got != 3 {
		t.Errorf("OpsRetries = %d, want 3", got)
	}
}

// TestFUSEMetrics_SnapshotContainsAllOps 验证 Snapshot 包含全部 op 计数。
func TestFUSEMetrics_SnapshotContainsAllOps(t *testing.T) {
	m := &FUSEMetrics{}
	// 触发每种 op
	for _, op := range []string{"open", "read", "write", "flush", "release", "lookup", "readdir", "create", "mkdir", "rmdir", "unlink", "rename", "symlink", "link", "statfs"} {
		m.IncOp(op)
	}
	m.IncOpError("read")
	m.IncCacheHit()
	m.IncCacheMiss()
	m.IncCacheEvict()
	m.IncBreakerOpen("meta")
	m.IncRetry("read")

	snap := m.Snapshot()

	ops := snap["ops"].(map[string]uint64)
	expectedOps := map[string]uint64{
		"open": 1, "read": 1, "write": 1, "flush": 1, "release": 1,
		"lookup": 1, "readdir": 1, "create": 1, "mkdir": 1,
		"remove": 2, "rename": 1, "other": 3,
	}
	for op, want := range expectedOps {
		if got := ops[op]; got != want {
			t.Errorf("Snapshot ops[%q] = %d, want %d", op, got, want)
		}
	}

	cache := snap["cache"].(map[string]uint64)
	if cache["hits"] != 1 || cache["misses"] != 1 || cache["evicts"] != 1 {
		t.Errorf("Snapshot cache = %+v, want hits=1 misses=1 evicts=1", cache)
	}

	if snap["errors"].(uint64) != 1 {
		t.Errorf("Snapshot errors = %d, want 1", snap["errors"])
	}
	if snap["retries"].(uint64) != 1 {
		t.Errorf("Snapshot retries = %d, want 1", snap["retries"])
	}
	if snap["breaker"].(uint64) != 1 {
		t.Errorf("Snapshot breaker = %d, want 1", snap["breaker"])
	}
}

// TestFUSEMetrics_ConcurrentSafe 验证并发调用不触发 race detector。
// 运行：go test -race -run TestFUSEMetrics_ConcurrentSafe
func TestFUSEMetrics_ConcurrentSafe(t *testing.T) {
	m := &FUSEMetrics{}
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				m.IncOp("read")
				m.IncOpError("read")
				m.IncCacheHit()
				m.IncCacheMiss()
			}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	if got := atomic.LoadUint64(&m.OpsRead); got != 1000 {
		t.Errorf("OpsRead = %d, want 1000", got)
	}
}
