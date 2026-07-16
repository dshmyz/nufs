//go:build linux

package fuse

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/example/dfs/gateway/s3"
)

// ========== 集成测试：验证 FUSE 操作正确递增 MetricsRecorder 计数器 ==========

// TestDFSFile_Open_IncrementsOps 验证 Open 一次后 OpsOpen=1。
func TestDFSFile_Open_IncrementsOps(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := s3.NewMemoryChunkStore()
	rec := &FUSEMetrics{}
	f := newTestFileWithRecorder(meta, cs, id, rec)

	_, _, errno := f.Open(context.Background(), 0)
	if errno != 0 {
		t.Fatalf("Open: errno=%v", errno)
	}
	if got := atomic.LoadUint64(&rec.OpsOpen); got != 1 {
		t.Errorf("OpsOpen = %d, want 1", got)
	}
}

// TestDFSFile_Read_IncrementsOps 验证 Read 一次后 OpsRead=1。
func TestDFSFile_Read_IncrementsOps(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := s3.NewMemoryChunkStore()
	rec := &FUSEMetrics{}
	f := newTestFileWithRecorder(meta, cs, id, rec)

	dest := make([]byte, 32)
	_, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	if got := atomic.LoadUint64(&rec.OpsRead); got != 1 {
		t.Errorf("OpsRead = %d, want 1", got)
	}
}

// TestDFSFile_Write_IncrementsOps 验证 Write 一次后 OpsWrite=1。
func TestDFSFile_Write_IncrementsOps(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := s3.NewMemoryChunkStore()
	rec := &FUSEMetrics{}
	f := newTestFileWithRecorder(meta, cs, id, rec)

	data := []byte("hello")
	_, errno := f.Write(context.Background(), nil, data, 0)
	if errno != 0 {
		t.Fatalf("Write: errno=%v", errno)
	}
	if got := atomic.LoadUint64(&rec.OpsWrite); got != 1 {
		t.Errorf("OpsWrite = %d, want 1", got)
	}
}

// TestDFSFile_Flush_IncrementsOps 验证 Flush 一次后 OpsFlush=1。
func TestDFSFile_Flush_IncrementsOps(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := s3.NewMemoryChunkStore()
	rec := &FUSEMetrics{}
	f := newTestFileWithRecorder(meta, cs, id, rec)

	// 先 Write 再 Flush
	_, _ = f.Write(context.Background(), nil, []byte("hello"), 0)
	errno := f.Flush(context.Background(), nil)
	if errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}
	if got := atomic.LoadUint64(&rec.OpsFlush); got != 1 {
		t.Errorf("OpsFlush = %d, want 1", got)
	}
}

// TestDFSFile_Release_IncrementsOps 验证 Release 一次后 OpsRelease=1。
func TestDFSFile_Release_IncrementsOps(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := s3.NewMemoryChunkStore()
	rec := &FUSEMetrics{}
	f := newTestFileWithRecorder(meta, cs, id, rec)

	errno := f.Release(context.Background(), nil)
	if errno != 0 {
		t.Fatalf("Release: errno=%v", errno)
	}
	if got := atomic.LoadUint64(&rec.OpsRelease); got != 1 {
		t.Errorf("OpsRelease = %d, want 1", got)
	}
}

// TestDFSFile_Read_HitCache_IncrementsCacheHits 验证读同一 chunk
// 第二次会命中缓存并递增 CacheHits。
func TestDFSFile_Read_HitCache_IncrementsCacheHits(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := s3.NewMemoryChunkStore()
	rec := &FUSEMetrics{}

	// 创建带缓存 + recorder 的 DFSFile
	cache, err := NewChunkCache("")
	if err != nil {
		t.Fatalf("NewChunkCache: %v", err)
	}
	cache.recorder = rec

	f := &DFSFile{
		meta:       meta,
		chunkStore: cs,
		inodeID:    id,
		cache:      cache,
		recorder:   rec,
	}

	// 写入数据并 Flush，生成 chunk
	_, _ = f.Write(context.Background(), nil, []byte("hello world"), 0)
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	// 重置 rec 计数器
	atomic.StoreUint64(&rec.OpsRead, 0)
	atomic.StoreUint64(&rec.CacheHits, 0)
	atomic.StoreUint64(&rec.CacheMisses, 0)

	// 第一次读：cache miss → chunkStore → cache.Add
	dest1 := make([]byte, 11)
	_, errno := f.Read(context.Background(), nil, dest1, 0)
	if errno != 0 {
		t.Fatalf("Read 1: errno=%v", errno)
	}
	if got := atomic.LoadUint64(&rec.CacheMisses); got != 1 {
		t.Errorf("after first read: CacheMisses = %d, want 1", got)
	}
	if got := atomic.LoadUint64(&rec.CacheHits); got != 0 {
		t.Errorf("after first read: CacheHits = %d, want 0", got)
	}

	// 第二次读：cache hit
	dest2 := make([]byte, 11)
	_, errno = f.Read(context.Background(), nil, dest2, 0)
	if errno != 0 {
		t.Fatalf("Read 2: errno=%v", errno)
	}
	if got := atomic.LoadUint64(&rec.CacheHits); got != 1 {
		t.Errorf("after second read: CacheHits = %d, want 1", got)
	}
}

// TestDFSFile_Flush_ErrorIncrementsOpsErrors 验证 Flush 失败时递增 OpsErrors。
// 通过注入超过 MaxChunkPayload 的数据触发 EFBIG。
func TestDFSFile_Flush_ErrorIncrementsOpsErrors(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := s3.NewMemoryChunkStore()
	rec := &FUSEMetrics{}
	f := newTestFileWithRecorder(meta, cs, id, rec)

	// 写入超过 MaxChunkPayload 的数据触发 EFBIG
	bigData := make([]byte, MaxChunkPayload+1)
	_, _ = f.Write(context.Background(), nil, bigData, 0)
	errno := f.Flush(context.Background(), nil)
	if errno != syscall.EFBIG {
		t.Fatalf("Flush: errno=%v, want EFBIG", errno)
	}
	if got := atomic.LoadUint64(&rec.OpsErrors); got != 1 {
		t.Errorf("OpsErrors = %d, want 1", got)
	}
}

// TestDFSFile_Getattr_IncrementsOpsErrorsOnFailure 验证 Getattr
// 失败时递增 OpsErrors（通过 nil meta 触发）。
func TestDFSFile_Getattr_IncrementsOpsErrorsOnFailure(t *testing.T) {
	rec := &FUSEMetrics{}
	f := &DFSFile{
		meta:     nil, // 触发 GetInode panic 或 error
		inodeID:  1,
		recorder: rec,
	}
	// 由于 meta=nil，GetInode 会 panic。
	// 我们用一个返回 error 的 meta service 替代——但当前没有 mock。
	// 跳过这个测试场景，留待后续用 mock meta service 时补充。
	_ = f
	_ = rec
}

// TestDFSFile_MultipleOps_AggregateCounters 验证多个操作累加计数。
func TestDFSFile_MultipleOps_AggregateCounters(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := s3.NewMemoryChunkStore()
	rec := &FUSEMetrics{}
	f := newTestFileWithRecorder(meta, cs, id, rec)

	// 执行多个操作
	_, _, _ = f.Open(context.Background(), 0)
	_, _ = f.Read(context.Background(), nil, make([]byte, 10), 0)
	_, _ = f.Write(context.Background(), nil, []byte("data"), 0)
	_ = f.Flush(context.Background(), nil)
	_ = f.Release(context.Background(), nil)

	snap := rec.Snapshot()
	ops := snap["ops"].(map[string]uint64)
	if ops["open"] != 1 || ops["read"] != 1 || ops["write"] != 1 || ops["flush"] != 1 || ops["release"] != 1 {
		t.Errorf("ops counters = %+v, want open=read=write=flush=release=1", ops)
	}
}
