package fuse

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru"
)

const defaultCacheEntries = 10_000

type cacheStats struct {
	hit  uint64
	miss uint64
}

// chunkSliceKey is the cache key for a single slice of a chunk. The offset
// dimension lets the cache hold hot windows of a chunk independently: a read
// that fetches one window caches under (chunkID, offset), so a later read of
// the same window hits without re-fetching, while different windows of the
// same chunk do not evict one another (and are never stored as a misleading
// "whole chunk").
type chunkSliceKey struct {
	id  uint64
	off int64
}

// ChunkCache 缓存 chunk 数据以减少 datanode 往返。
// 支持两级缓存：内存 LRU + 磁盘文件；支持字节配额淘汰。
type ChunkCache struct {
	memory   *lru.Cache
	diskDir  string
	stats    cacheStats
	recorder MetricsRecorder // 方案1.4 起统一走 recorder；stats 保留兼容旧 HitRate()

	// 字节配额淘汰（方案2.2）
	// maxBytes=0 表示不限字节配额，仅按条目数淘汰。
	maxBytes int64
	curBytes int64 // 原子操作，当前缓存总字节数
}

// NewChunkCache creates a chunk cache with optional disk directory.
// If size <= 0, uses defaultCacheEntries (10,000).
// 字节配额不限（maxBytes=0）。
func NewChunkCache(diskDir string, size ...int) (*ChunkCache, error) {
	n := defaultCacheEntries
	if len(size) > 0 && size[0] > 0 {
		n = size[0]
	}
	return NewChunkCacheWithQuota(diskDir, n, 0, nil)
}

// NewChunkCacheWithQuota 创建支持字节配额淘汰的 chunk 缓存。
//
//   - maxEntries: LRU 条目数上限（<=0 时用 defaultCacheEntries）
//   - maxBytes:   字节配额上限（0 表示不限字节，仅按条目数淘汰）
//   - rec:        指标记录器，nil 时使用 noopMetricsRecorder
//
// 淘汰策略：当新增 entry 会导致 curBytes+newSize > maxBytes 时，
// 主动淘汰最旧条目直到有足够空间。单个 entry 超过 maxBytes 时不淘汰
// （避免饿死），允许写入。
func NewChunkCacheWithQuota(diskDir string, maxEntries int, maxBytes int64, rec MetricsRecorder) (*ChunkCache, error) {
	if maxEntries <= 0 {
		maxEntries = defaultCacheEntries
	}

	c := &ChunkCache{
		diskDir:  diskDir,
		maxBytes: maxBytes,
		recorder: rec,
	}

	memory, err := lru.NewWithEvict(maxEntries, c.onEvict)
	if err != nil {
		return nil, fmt.Errorf("new chunk cache: %w", err)
	}
	c.memory = memory

	if diskDir != "" {
		if err := os.MkdirAll(diskDir, 0700); err != nil {
			return nil, fmt.Errorf("create cache dir: %w", err)
		}
	}
	return c, nil
}

// onEvict 是 LRU 淘汰回调，在以下场景被调用：
//  1. 条目数超限，LRU 自动淘汰最旧条目（capacity eviction）
//  2. 字节配额超限，addInternal 主动调用 RemoveOldest 淘汰
//  3. 用户显式调用 Remove
//
// 本回调负责：扣减 curBytes + 删除磁盘文件（所有移除路径的公共清理）。
// 不负责：递增淘汰计数器 —— 仅场景 1/2 算淘汰，由调用方递增。
func (c *ChunkCache) onEvict(key, value interface{}) {
	if data, ok := value.([]byte); ok {
		atomic.AddInt64(&c.curBytes, -int64(len(data)))
	}
	if c.diskDir != "" {
		if k, ok := key.(chunkSliceKey); ok {
			_ = os.Remove(c.diskPath(k))
		}
	}
}

func (c *ChunkCache) Get(chunkID uint64, off int64) ([]byte, bool) {
	k := chunkSliceKey{id: chunkID, off: off}
	rec := recorderFor(c.recorder)
	if v, ok := c.memory.Get(k); ok {
		atomic.AddUint64(&c.stats.hit, 1)
		rec.IncCacheHit()
		return v.([]byte), true
	}
	if c.diskDir != "" {
		data, err := os.ReadFile(c.diskPath(k))
		if err == nil {
			buf := make([]byte, len(data))
			copy(buf, data)
			c.addInternal(k, buf)
			atomic.AddUint64(&c.stats.hit, 1)
			rec.IncCacheHit()
			return data, true
		}
	}
	atomic.AddUint64(&c.stats.miss, 1)
	rec.IncCacheMiss()
	return nil, false
}

func (c *ChunkCache) Add(chunkID uint64, off int64, data []byte) {
	k := chunkSliceKey{id: chunkID, off: off}
	buf := make([]byte, len(data))
	copy(buf, data)

	// 如果 key 已存在，LRU.Add 会更新 value 但不调用 onEvict。
	// 需要先手动扣减旧条目的字节数，避免 curBytes 重复累加。
	if old, exists := c.memory.Peek(k); exists {
		if oldData, ok := old.([]byte); ok {
			atomic.AddInt64(&c.curBytes, -int64(len(oldData)))
		}
	}

	c.addInternal(k, buf)

	if c.diskDir != "" {
		_ = os.WriteFile(c.diskPath(k), buf, 0600)
	}
}

// addInternal 将 buf 加入内存 LRU，处理字节配额淘汰和计数。
// 调用前需确保：若 key 已存在，旧 value 的字节数已从 curBytes 扣减。
func (c *ChunkCache) addInternal(k chunkSliceKey, buf []byte) {
	rec := recorderFor(c.recorder)
	newSize := int64(len(buf))

	// 字节配额淘汰：主动淘汰最旧条目直到有足够空间。
	// 单个 entry 超过 maxBytes 时不淘汰（避免饿死），允许写入。
	if c.maxBytes > 0 && newSize <= c.maxBytes {
		for atomic.LoadInt64(&c.curBytes)+newSize > c.maxBytes {
			if c.memory.Len() == 0 {
				break // LRU 已空
			}
			c.memory.RemoveOldest()
			rec.IncCacheEvict()
		}
	}

	evicted := c.memory.Add(k, buf)
	if evicted {
		// 条目数超限导致的 LRU 自动淘汰
		rec.IncCacheEvict()
	}
	atomic.AddInt64(&c.curBytes, newSize)
}

func (c *ChunkCache) Remove(chunkID uint64, off int64) {
	k := chunkSliceKey{id: chunkID, off: off}
	// c.memory.Remove 触发 onEvict（扣减 curBytes + 删除磁盘文件）。
	c.memory.Remove(k)
	// 兜底：entry 不在内存 LRU 但磁盘文件仍存在的情况。
	if c.diskDir != "" {
		_ = os.Remove(c.diskPath(k))
	}
}

// RemoveChunk evicts every cached slice of the given chunk. Used for
// metadata change invalidation, which marks the whole chunk stale without
// knowing which windows were cached.
func (c *ChunkCache) RemoveChunk(chunkID uint64) {
	for _, k := range c.memory.Keys() {
		if sk, ok := k.(chunkSliceKey); ok && sk.id == chunkID {
			c.memory.Remove(sk)
		}
	}
	if c.diskDir != "" {
		matches, _ := filepath.Glob(c.diskPathPrefix(chunkID))
		for _, m := range matches {
			_ = os.Remove(m)
		}
	}
}

func (c *ChunkCache) diskPathPrefix(chunkID uint64) string {
	return path.Join(c.diskDir, fmt.Sprintf("%016x_*", chunkID))
}

func (c *ChunkCache) HitRate() float64 {
	h := atomic.LoadUint64(&c.stats.hit)
	m := atomic.LoadUint64(&c.stats.miss)
	total := h + m
	if total == 0 {
		return 0
	}
	return float64(h) / float64(total)
}

func (c *ChunkCache) Len() int { return c.memory.Len() }

// Bytes 返回当前缓存的总字节数（原子读）。
func (c *ChunkCache) Bytes() int64 {
	return atomic.LoadInt64(&c.curBytes)
}

func (c *ChunkCache) diskPath(k chunkSliceKey) string {
	return path.Join(c.diskDir, fmt.Sprintf("%016x_%016x", k.id, uint64(k.off)))
}
