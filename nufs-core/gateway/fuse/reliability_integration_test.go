//go:build linux

package fuse

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/dfs/gateway"
	"github.com/example/dfs/gateway/s3"
	"github.com/example/dfs/metadata"
)

// ========== 集成测试：验证 ReliabilityWrapper 正确接入 Read/Flush 路径 ==========

// newTestFileWithReliability 创建带 ReliabilityWrapper 的 DFSFile。
func newTestFileWithReliability(meta metadata.MetadataService, cs gateway.ChunkStore, id metadata.InodeID, rec MetricsRecorder, rel *ReliabilityWrapper) *DFSFile {
	return &DFSFile{
		meta:        meta,
		chunkStore:  cs,
		inodeID:     id,
		recorder:    rec,
		reliability: rel,
	}
}

// TestDFSFile_Read_WithReliability_Works 验证注入 ReliabilityWrapper 后
// Read 路径仍然正确返回数据。如果闭包变量捕获或 DoMeta/DoChunk 接线
// 有误，数据将不匹配。
func TestDFSFile_Read_WithReliability_Works(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := s3.NewMemoryChunkStore()
	rec := &FUSEMetrics{}
	rel := NewReliabilityWrapper(rec, fastRetryCfg(), fastBreakerCfg(nil))

	f := newTestFileWithReliability(meta, cs, id, rec, rel)

	data := []byte("hello reliability read")
	_, _ = f.Write(context.Background(), nil, data, 0)
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	dest := make([]byte, len(data))
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if string(got) != string(data) {
		t.Errorf("Read data = %q, want %q", got, data)
	}
}

// TestDFSFile_Flush_WithReliability_Works 验证注入 ReliabilityWrapper 后
// Flush 路径仍然正确持久化数据，且路径锁不阻塞单实例 Flush。
func TestDFSFile_Flush_WithReliability_Works(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := s3.NewMemoryChunkStore()
	rec := &FUSEMetrics{}
	rel := NewReliabilityWrapper(rec, fastRetryCfg(), fastBreakerCfg(nil))

	f := newTestFileWithReliability(meta, cs, id, rec, rel)

	data := []byte("flush with reliability")
	_, _ = f.Write(context.Background(), nil, data, 0)
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	// 读回验证
	dest := make([]byte, len(data))
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if string(got) != string(data) {
		t.Errorf("Read after flush = %q, want %q", got, data)
	}
}

// concurrencyTrackingChunkStore 包装 ChunkStore，跟踪 WriteChunk 的并发度。
// 用于验证 Flush 路径锁是否正确串行化。
type concurrencyTrackingChunkStore struct {
	inner      gateway.ChunkStore
	concurrent *int32
	maxConc    *int32
}

func (c *concurrencyTrackingChunkStore) WriteChunk(ctx context.Context, chunk *metadata.ChunkMeta, data []byte) error {
	cur := atomic.AddInt32(c.concurrent, 1)
	for {
		old := atomic.LoadInt32(c.maxConc)
		if cur <= old || atomic.CompareAndSwapInt32(c.maxConc, old, cur) {
			break
		}
	}
	defer atomic.AddInt32(c.concurrent, -1)
	time.Sleep(20 * time.Millisecond) // 拉长 WriteChunk 窗口，增加重叠概率
	return c.inner.WriteChunk(ctx, chunk, data)
}

func (c *concurrencyTrackingChunkStore) ReadChunk(ctx context.Context, chunk *metadata.ChunkMeta) ([]byte, error) {
	return c.inner.ReadChunk(ctx, chunk)
}

func (c *concurrencyTrackingChunkStore) ReadChunkRange(ctx context.Context, chunk *metadata.ChunkMeta, off int64, length int32) ([]byte, error) {
	return c.inner.ReadChunkRange(ctx, chunk, off, length)
}

// TestDFSFile_Flush_SerializesConcurrent 验证两个 DFSFile 实例共享同一
// ReliabilityWrapper 时，对同一 inode 的并发 Flush 被路径锁串行化。
// 如果 LockInode 未接入 Flush，WriteChunk 将出现并发 >1。
func TestDFSFile_Flush_SerializesConcurrent(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := s3.NewMemoryChunkStore()
	rec := &FUSEMetrics{}
	rel := NewReliabilityWrapper(rec, fastRetryCfg(), fastBreakerCfg(nil))

	var concurrent, maxConc int32
	trackingCS := &concurrencyTrackingChunkStore{
		inner:      cs,
		concurrent: &concurrent,
		maxConc:    &maxConc,
	}

	f1 := newTestFileWithReliability(meta, trackingCS, id, rec, rel)
	f2 := newTestFileWithReliability(meta, trackingCS, id, rec, rel)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = f1.Write(context.Background(), nil, []byte("from-f1"), 0)
		_ = f1.Flush(context.Background(), nil)
	}()
	go func() {
		defer wg.Done()
		_, _ = f2.Write(context.Background(), nil, []byte("from-f2"), 0)
		_ = f2.Flush(context.Background(), nil)
	}()

	wg.Wait()

	if got := atomic.LoadInt32(&maxConc); got > 1 {
		t.Errorf("max concurrent WriteChunk = %d, want <= 1 (Flush should be serialized by path lock)", got)
	}
}

// TestDFSFile_Flush_NilReliability_Passthrough 验证 reliability=nil 时
// Flush 仍然正常工作（passthrough 模式），不 panic。
func TestDFSFile_Flush_NilReliability_Passthrough(t *testing.T) {
	meta, id := newTestMetaStore(t)
	cs := s3.NewMemoryChunkStore()
	rec := &FUSEMetrics{}

	// reliability=nil → passthrough
	f := &DFSFile{
		meta:       meta,
		chunkStore: cs,
		inodeID:    id,
		recorder:   rec,
	}

	data := []byte("nil reliability passthrough")
	_, _ = f.Write(context.Background(), nil, data, 0)
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush: errno=%v", errno)
	}

	dest := make([]byte, len(data))
	rr, errno := f.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read: errno=%v", errno)
	}
	got, _ := rr.Bytes(dest)
	if string(got) != string(data) {
		t.Errorf("Read data = %q, want %q", got, data)
	}
}
