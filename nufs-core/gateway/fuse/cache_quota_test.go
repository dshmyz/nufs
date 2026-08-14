package fuse

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// ========== 缓存字节配额淘汰测试（TDD 红→绿） ==========

// TestChunkCache_ByteQuotaEvictsOldest 验证 maxBytes 配额触发 LRU 淘汰最旧条目。
// 场景：maxBytes=1MB，Add 5 个 256KB chunk → 总 1.25MB > 1MB
// 期望：最旧的 chunk 被淘汰，curBytes ≤ 1MB
func TestChunkCache_ByteQuotaEvictsOldest(t *testing.T) {
	cache, err := NewChunkCacheWithQuota("", 100, 1024*1024, nil) // 1MB quota
	if err != nil {
		t.Fatalf("NewChunkCacheWithQuota: %v", err)
	}

	chunkSize := 256 * 1024 // 256KB
	for i := 1; i <= 5; i++ {
		data := make([]byte, chunkSize)
		data[0] = byte(i)
		cache.Add(uint64(i), 0, data)
	}

	// chunk 1（最旧）应该被淘汰
	if _, ok := cache.Get(1, 0); ok {
		t.Errorf("chunk 1 should have been evicted (oldest), but got cache hit")
	}
	// chunk 5（最新）应该还在
	if _, ok := cache.Get(5, 0); !ok {
		t.Errorf("chunk 5 should still be cached (newest)")
	}
	// curBytes 不应超过 maxBytes
	if got := cache.Bytes(); got > 1024*1024 {
		t.Errorf("curBytes = %d, want <= %d (quota)", got, 1024*1024)
	}
}

// TestChunkCache_ByteQuotaDeletesDiskFile 验证淘汰时同时删除磁盘文件。
func TestChunkCache_ByteQuotaDeletesDiskFile(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewChunkCacheWithQuota(dir, 100, 1024*1024, nil)
	if err != nil {
		t.Fatalf("NewChunkCacheWithQuota: %v", err)
	}

	chunkSize := 256 * 1024
	for i := 1; i <= 5; i++ {
		data := make([]byte, chunkSize)
		cache.Add(uint64(i), 0, data)
	}

	// chunk 1 的磁盘文件应该被删除
	diskPath := filepath.Join(dir, "0000000000000001")
	if _, err := os.Stat(diskPath); !os.IsNotExist(err) {
		t.Errorf("disk file for chunk 1 should be deleted, got err=%v", err)
	}
}

// TestChunkCache_QuotaZeroMeansUnlimited 验证 maxBytes=0 时不淘汰。
func TestChunkCache_QuotaZeroMeansUnlimited(t *testing.T) {
	cache, err := NewChunkCacheWithQuota("", 100, 0, nil)
	if err != nil {
		t.Fatalf("NewChunkCacheWithQuota: %v", err)
	}

	// Add 远超 100 条目容量的数据（但 LRU 条数会淘汰，这里测字节配额不触发）
	for i := 0; i < 50; i++ {
		data := make([]byte, 1024) // 1KB each
		cache.Add(uint64(i+1), 0, data)
	}

	// 所有 chunk 应该可读到（条数 50 < 100，字节 50KB，无配额限制）
	for i := 1; i <= 50; i++ {
		if _, ok := cache.Get(uint64(i), 0); !ok {
			t.Errorf("chunk %d should be cached (unlimited quota)", i)
		}
	}
}

// TestChunkCache_LargeChunkExceedsQuota 验证单个 chunk 超过 maxBytes 仍能写入（避免饿死）。
func TestChunkCache_LargeChunkExceedsQuota(t *testing.T) {
	cache, err := NewChunkCacheWithQuota("", 100, 1024, nil) // 1KB quota
	if err != nil {
		t.Fatalf("NewChunkCacheWithQuota: %v", err)
	}

	// 单个 chunk 2KB > 1KB quota
	bigData := make([]byte, 2048)
	cache.Add(1, 0, bigData)

	// 应该能读回
	if data, ok := cache.Get(1, 0); !ok || len(data) != 2048 {
		t.Errorf("large chunk should be cached despite exceeding quota, got ok=%v len=%d", ok, len(data))
	}
}

// TestChunkCache_EvictionIncrementsCounter 验证淘汰时通过 recorder 递增淘汰计数。
func TestChunkCache_EvictionIncrementsCounter(t *testing.T) {
	rec := &FUSEMetrics{}
	cache, err := NewChunkCacheWithQuota("", 100, 1024*1024, rec)
	if err != nil {
		t.Fatalf("NewChunkCacheWithQuota: %v", err)
	}

	chunkSize := 256 * 1024
	for i := 1; i <= 5; i++ {
		data := make([]byte, chunkSize)
		cache.Add(uint64(i), 0, data)
	}

	// 至少淘汰了 1 个（1.25MB → 1MB，淘汰 1 个 256KB）
	if got := atomic.LoadUint64(&rec.CacheEvicts); got < 1 {
		t.Errorf("CacheEvicts = %d, want >= 1", got)
	}
}

// TestChunkCache_Bytes_Accurate 验证 Bytes() 准确反映当前缓存字节数。
func TestChunkCache_Bytes_Accurate(t *testing.T) {
	cache, err := NewChunkCacheWithQuota("", 100, 0, nil) // unlimited
	if err != nil {
		t.Fatalf("NewChunkCacheWithQuota: %v", err)
	}

	if got := cache.Bytes(); got != 0 {
		t.Errorf("initial Bytes() = %d, want 0", got)
	}

	cache.Add(1, 0, make([]byte, 1024))
	cache.Add(2, 0, make([]byte, 2048))
	if got := cache.Bytes(); got != 3072 {
		t.Errorf("after 2 adds: Bytes() = %d, want 3072", got)
	}

	cache.Remove(1, 0)
	if got := cache.Bytes(); got != 2048 {
		t.Errorf("after remove chunk 1: Bytes() = %d, want 2048", got)
	}
}

// TestChunkCache_EvictCallback_NotPanics 验证 LRU 淘汰回调不 panic。
func TestChunkCache_EvictCallback_NotPanics(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewChunkCacheWithQuota(dir, 5, 0, nil) // 5 entries, no byte quota
	if err != nil {
		t.Fatalf("NewChunkCacheWithQuota: %v", err)
	}

	// Add 10 chunks to trigger LRU eviction (capacity 5)
	for i := 1; i <= 10; i++ {
		cache.Add(uint64(i), 0, make([]byte, 100))
	}

	// 只要没 panic 即通过
	if cache.Len() > 5 {
		t.Errorf("Len = %d, want <= 5 (LRU capacity)", cache.Len())
	}
}
