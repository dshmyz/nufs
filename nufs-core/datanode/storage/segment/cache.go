package segment

import (
	"container/list"
	"sync"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage/encryption"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/index"
)

// SegmentDescriptorCache caches open segment Readers per segment ID
// (§8: per-disk segment descriptor LRU with at most 4096 descriptors;
// active descriptors remain pinned). Each entry holds an open *Reader
// so reads avoid os.Open on every call.
type SegmentDescriptorCache struct {
	mu       sync.Mutex
	maxSize  int
	entries  map[string]*list.Element // segPath → lru entry
	lru      *list.List               // *cacheEntry, front=most recent
}

// cacheEntry is one entry in the descriptor LRU.
type cacheEntry struct {
	path  string
	rd    *Reader
	pinned bool
}

// NewSegmentDescriptorCache creates a descriptor cache.
func NewSegmentDescriptorCache(maxSize int) *SegmentDescriptorCache {
	if maxSize <= 0 {
		maxSize = 4096 // §16 default
	}
	return &SegmentDescriptorCache{
		maxSize: maxSize,
		entries: make(map[string]*list.Element),
		lru:     list.New(),
	}
}

// Get returns a cached reader for the segment path, or opens a new one
// and inserts it into the LRU. The caller must not close the returned
// reader — the cache owns its lifecycle.
func (c *SegmentDescriptorCache) Get(path string, enc *encryption.KeyRegistry) (*Reader, error) {
	c.mu.Lock()
	if el, ok := c.entries[path]; ok {
		c.lru.MoveToFront(el)
		entry := el.Value.(*cacheEntry)
		c.mu.Unlock()
		return entry.rd, nil
	}
	c.mu.Unlock()
	// Miss: open a new reader.
	rd, err := OpenReaderWithEnc(path, enc)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	// Check again in case another goroutine inserted it.
	if el, ok := c.entries[path]; ok {
		c.lru.MoveToFront(el)
		c.mu.Unlock()
		rd.Close()
		return el.Value.(*cacheEntry).rd, nil
	}
	// Evict if over capacity.
	for c.lru.Len() >= c.maxSize {
		back := c.lru.Back()
		if back == nil {
			break
		}
		entry := back.Value.(*cacheEntry)
		if entry.pinned {
			// Move pinned entries to front so they survive eviction.
			c.lru.MoveToFront(back)
			break
		}
		entry.rd.Close()
		c.lru.Remove(back)
		delete(c.entries, entry.path)
	}
	el := c.lru.PushFront(&cacheEntry{path: path, rd: rd})
	c.entries[path] = el
	c.mu.Unlock()
	return rd, nil
}

// Pin marks a segment path as pinned (never evicted, for active segments).
func (c *SegmentDescriptorCache) Pin(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[path]; ok {
		el.Value.(*cacheEntry).pinned = true
	}
}

// Close closes all cached readers and clears the cache.
func (c *SegmentDescriptorCache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, el := range c.entries {
		entry := el.Value.(*cacheEntry)
		entry.rd.Close()
	}
	c.entries = make(map[string]*list.Element)
	c.lru.Init()
}

// LocationCache caches (extent_id, generation) → *index.Value lookups
// (§8: TinyLFU/LRU location cache, default 1M entries). When the
// location is in the location cache, Read skips the Pebble lookup.
type LocationCache struct {
	mu      sync.Mutex
	maxSize int
	entries map[string]*list.Element
	lru     *list.List
}

type locCacheEntry struct {
	key   string
	value *index.Value
}

// NewLocationCache creates a location cache.
func NewLocationCache(maxSize int) *LocationCache {
	if maxSize <= 0 {
		maxSize = 1000000 // §16 default
	}
	return &LocationCache{
		maxSize: maxSize,
		entries: make(map[string]*list.Element),
		lru:     list.New(),
	}
}

// Get returns a cached location, or nil.
func (c *LocationCache) Get(key []byte) (*index.Value, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[string(key)]; ok {
		c.lru.MoveToFront(el)
		v := el.Value.(*locCacheEntry).value
		return v, true
	}
	return nil, false
}

// Put inserts a location into the cache, evicting LRU if needed.
func (c *LocationCache) Put(key []byte, v *index.Value) {
	sk := string(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[sk]; ok {
		el.Value.(*locCacheEntry).value = v
		c.lru.MoveToFront(el)
		return
	}
	for c.lru.Len() >= c.maxSize {
		back := c.lru.Back()
		if back == nil {
			break
		}
		entry := back.Value.(*locCacheEntry)
		c.lru.Remove(back)
		delete(c.entries, entry.key)
	}
	c.lru.PushFront(&locCacheEntry{key: sk, value: v})
	c.entries[sk] = c.lru.Front()
}

// Delete removes an entry from the cache.
func (c *LocationCache) Delete(key []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[string(key)]; ok {
		c.lru.Remove(el)
		delete(c.entries, string(key))
	}
}

// Clear empties the cache.
func (c *LocationCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*list.Element)
	c.lru.Init()
}

var _ = index.KeyLen