package gateway

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"sync"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ============================================================
// Client Cache — Local Cache for FUSE/S3 Gateway
// ============================================================

// CacheConfig configures the client cache.
type CacheConfig struct {
	// MaxSize is the maximum cache size in bytes (default 1GB)
	MaxSize int64
	// AttrTTL is the time-to-live for attribute cache (default 5s)
	AttrTTL time.Duration
	// WriteBufferSize is the buffer size for write-back (default 500MB)
	WriteBufferSize int64
	// FlushInterval is the interval for flushing dirty data (default 10s)
	FlushInterval time.Duration
}

// ClientCache provides local caching for metadata and file data.
type ClientCache struct {
	cfg CacheConfig

	// Metadata cache (inode attributes)
	attrMu   sync.RWMutex
	attr     map[metadata.InodeID]*AttrEntry
	attrLRU  []metadata.InodeID // eviction order
	attrSize int64

	// Data cache (file content)
	dataMu   sync.RWMutex
	data     map[uint64]*DataEntry
	dataLRU  []uint64
	dataSize int64

	// Write buffer (async write-back)
	writeMu   sync.RWMutex
	writeBuf  map[metadata.ChunkID][]byte // dirty chunks
	writeSize int64

	// Flush callback: sends dirty chunks to datanode
	flushFn func(metadata.ChunkID, []byte) error

	// Background flusher
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// AttrEntry caches inode attributes.
type AttrEntry struct {
	Meta      *metadata.InodeMeta
	ExpiresAt time.Time
}

// DataEntry caches file data blocks.
type DataEntry struct {
	Data      []byte
	ChunkID   metadata.ChunkID
	ExpiresAt time.Time
}

// NewClientCache creates a new client cache.
func NewClientCache(cfg CacheConfig) *ClientCache {
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = 1 << 30 // 1GB
	}
	if cfg.AttrTTL <= 0 {
		cfg.AttrTTL = 5 * time.Second
	}
	if cfg.WriteBufferSize <= 0 {
		cfg.WriteBufferSize = 500 << 20 // 500MB
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 10 * time.Second
	}

	cc := &ClientCache{
		cfg:      cfg,
		attr:     make(map[metadata.InodeID]*AttrEntry),
		data:     make(map[uint64]*DataEntry),
		writeBuf: make(map[metadata.ChunkID][]byte),
		stopCh:   make(chan struct{}),
	}
	cc.wg.Add(1)
	go cc.flushLoop()
	return cc
}

// SetFlushFunc sets the callback used to flush dirty chunks to backend.
func (cc *ClientCache) SetFlushFunc(fn func(metadata.ChunkID, []byte) error) {
	cc.flushFn = fn
}

// GetAttr returns cached inode attributes.
func (cc *ClientCache) GetAttr(id metadata.InodeID) (*metadata.InodeMeta, bool) {
	cc.attrMu.RLock()
	defer cc.attrMu.RUnlock()

	entry, ok := cc.attr[id]
	if !ok || time.Now().After(entry.ExpiresAt) {
		return nil, false
	}
	return entry.Meta, true
}

// SetAttr caches inode attributes.
func (cc *ClientCache) SetAttr(meta *metadata.InodeMeta) {
	cc.attrMu.Lock()
	defer cc.attrMu.Unlock()

	// Evict if needed
	for cc.attrSize >= cc.cfg.MaxSize/10 {
		cc.evictAttr()
	}

	cc.attr[meta.ID] = &AttrEntry{
		Meta:      meta,
		ExpiresAt: time.Now().Add(cc.cfg.AttrTTL),
	}
	cc.attrLRU = append(cc.attrLRU, meta.ID)
	cc.attrSize += 200 // Approximate size
}

// InvalidateAttr removes cached attributes.
func (cc *ClientCache) InvalidateAttr(id metadata.InodeID) {
	cc.attrMu.Lock()
	defer cc.attrMu.Unlock()
	delete(cc.attr, id)
}

// GetChunk returns cached chunk data.
func (cc *ClientCache) GetChunk(chunkID metadata.ChunkID, offset int64, length int32) ([]byte, bool) {
	cc.dataMu.RLock()
	defer cc.dataMu.RUnlock()

	key := chunkKey(chunkID, offset, length)
	entry, ok := cc.data[key]
	if !ok || time.Now().After(entry.ExpiresAt) {
		return nil, false
	}
	return entry.Data, true
}

// SetChunk caches chunk data.
func (cc *ClientCache) SetChunk(chunkID metadata.ChunkID, offset int64, length int32, data []byte) {
	cc.dataMu.Lock()
	defer cc.dataMu.Unlock()

	key := chunkKey(chunkID, offset, length)

	// Evict if needed
	for cc.dataSize >= cc.cfg.MaxSize/2 {
		cc.evictData()
	}

	cc.data[key] = &DataEntry{
		Data:      data,
		ChunkID:   chunkID,
		ExpiresAt: time.Now().Add(cc.cfg.AttrTTL * 10), // Longer TTL for data
	}
	cc.dataLRU = append(cc.dataLRU, key)
	cc.dataSize += int64(len(data))
}

// BufferWrite adds data to write buffer.
func (cc *ClientCache) BufferWrite(chunkID metadata.ChunkID, data []byte) {
	cc.writeMu.Lock()
	defer cc.writeMu.Unlock()

	cc.writeBuf[chunkID] = data
	cc.writeSize += int64(len(data))

	// Trigger flush if buffer too large
	if cc.writeSize >= cc.cfg.WriteBufferSize {
		cc.flushLocked()
	}
}

// GetBufferedWrite returns buffered data if any.
func (cc *ClientCache) GetBufferedWrite(chunkID metadata.ChunkID) ([]byte, bool) {
	cc.writeMu.RLock()
	defer cc.writeMu.RUnlock()
	data, ok := cc.writeBuf[chunkID]
	return data, ok
}

// Flush sends buffered writes to backend.
func (cc *ClientCache) Flush(ctx context.Context, flush func(metadata.ChunkID, []byte) error) error {
	cc.writeMu.Lock()
	defer cc.writeMu.Unlock()
	return cc.flushLockedWithFlusher(ctx, flush)
}

// Close stops background flusher and flushes remaining dirty data.
func (cc *ClientCache) Close() {
	close(cc.stopCh)
	cc.wg.Wait()

	// Final flush of any remaining dirty data
	if cc.flushFn != nil {
		cc.writeMu.Lock()
		for chunkID, data := range cc.writeBuf {
			if err := cc.flushFn(chunkID, data); err != nil {
				// Last resort: log and discard
				_ = err
			}
		}
		cc.writeBuf = make(map[metadata.ChunkID][]byte)
		cc.writeSize = 0
		cc.writeMu.Unlock()
	}
}

// --- Internal methods ---

func (cc *ClientCache) flushLoop() {
	defer cc.wg.Done()
	ticker := time.NewTicker(cc.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if cc.flushFn != nil {
				cc.writeMu.Lock()
				if len(cc.writeBuf) == 0 {
					cc.writeMu.Unlock()
					continue
				}
				// Snapshot and clear dirty buffer
				dirty := make(map[metadata.ChunkID][]byte, len(cc.writeBuf))
				for k, v := range cc.writeBuf {
					dirty[k] = v
				}
				cc.writeBuf = make(map[metadata.ChunkID][]byte)
				cc.writeSize = 0
				cc.writeMu.Unlock()

				// Flush outside lock
				for chunkID, data := range dirty {
					if err := cc.flushFn(chunkID, data); err != nil {
						// Re-buffer on failure
						cc.BufferWrite(chunkID, data)
					}
				}
			}
		case <-cc.stopCh:
			return
		}
	}
}

func (cc *ClientCache) evictAttr() {
	if len(cc.attrLRU) == 0 {
		return
	}
	oldest := cc.attrLRU[0]
	cc.attrLRU = cc.attrLRU[1:]
	delete(cc.attr, oldest)
	cc.attrSize -= 200
}

func (cc *ClientCache) evictData() {
	if len(cc.dataLRU) == 0 {
		return
	}
	oldest := cc.dataLRU[0]
	cc.dataLRU = cc.dataLRU[1:]
	if entry, ok := cc.data[oldest]; ok {
		cc.dataSize -= int64(len(entry.Data))
		delete(cc.data, oldest)
	}
}

func (cc *ClientCache) flushLocked() error {
	if cc.flushFn == nil {
		// No flush callback: discard buffered data
		cc.writeBuf = make(map[metadata.ChunkID][]byte)
		cc.writeSize = 0
		return nil
	}

	var firstErr error
	for chunkID, data := range cc.writeBuf {
		if err := cc.flushFn(chunkID, data); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	cc.writeBuf = make(map[metadata.ChunkID][]byte)
	cc.writeSize = 0
	return firstErr
}

func (cc *ClientCache) flushLockedWithFlusher(ctx context.Context, flush func(metadata.ChunkID, []byte) error) error {
	var errs []error
	for chunkID, data := range cc.writeBuf {
		if err := flush(chunkID, data); err != nil {
			errs = append(errs, err)
		}
	}
	cc.writeBuf = make(map[metadata.ChunkID][]byte)
	cc.writeSize = 0
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func chunkKey(chunkID metadata.ChunkID, offset int64, length int32) uint64 {
	h := fnv.New64a()
	h.Write([]byte{byte(chunkID), byte(chunkID >> 8)})
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(offset)))
	h.Write(binary.BigEndian.AppendUint32(nil, uint32(length)))
	return h.Sum64()
}
