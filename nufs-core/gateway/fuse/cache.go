package fuse

import (
	"fmt"
	"os"
	"path"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru"
)

const defaultCacheEntries = 10_000

type cacheStats struct {
	hit  uint64
	miss uint64
}

type ChunkCache struct {
	memory   *lru.Cache
	diskDir  string
	stats    cacheStats
	recorder MetricsRecorder // 1.4 起统一走 recorder；stats 保留兼容旧 HitRate()
}

// NewChunkCache creates a chunk cache with optional disk directory.
// If size <= 0, uses defaultCacheEntries (10,000).
func NewChunkCache(diskDir string, size ...int) (*ChunkCache, error) {
	n := defaultCacheEntries
	if len(size) > 0 && size[0] > 0 {
		n = size[0]
	}
	memory, err := lru.New(n)
	if err != nil {
		return nil, fmt.Errorf("new chunk cache: %w", err)
	}
	if diskDir != "" {
		if err := os.MkdirAll(diskDir, 0700); err != nil {
			return nil, fmt.Errorf("create cache dir: %w", err)
		}
	}
	return &ChunkCache{
		memory:  memory,
		diskDir: diskDir,
	}, nil
}

func (c *ChunkCache) Get(chunkID uint64) ([]byte, bool) {
	rec := recorderFor(c.recorder)
	if v, ok := c.memory.Get(chunkID); ok {
		atomic.AddUint64(&c.stats.hit, 1)
		rec.IncCacheHit()
		return v.([]byte), true
	}
	if c.diskDir != "" {
		data, err := os.ReadFile(c.diskPath(chunkID))
		if err == nil {
			c.memory.Add(chunkID, data)
			atomic.AddUint64(&c.stats.hit, 1)
			rec.IncCacheHit()
			return data, true
		}
	}
	atomic.AddUint64(&c.stats.miss, 1)
	rec.IncCacheMiss()
	return nil, false
}

func (c *ChunkCache) Add(chunkID uint64, data []byte) {
	buf := make([]byte, len(data))
	copy(buf, data)
	c.memory.Add(chunkID, buf)
	if c.diskDir != "" {
		os.WriteFile(c.diskPath(chunkID), buf, 0600)
	}
}

func (c *ChunkCache) Remove(chunkID uint64) {
	c.memory.Remove(chunkID)
	if c.diskDir != "" {
		os.Remove(c.diskPath(chunkID))
	}
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

func (c *ChunkCache) diskPath(chunkID uint64) string {
	return path.Join(c.diskDir, fmt.Sprintf("%016x", chunkID))
}
