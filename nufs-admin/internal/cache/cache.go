// Package cache provides TTL-based response caching.
package cache

import (
	"sync"
	"time"
)

// entry represents a cached value with expiration time.
type entry struct {
	data      []byte
	expiresAt time.Time
}

// Cache is a simple TTL cache using sync.Map.
type Cache struct {
	items sync.Map
	ttl   time.Duration
	stop  chan struct{}
}

// New creates a cache with specified TTL and starts background cleanup.
func New(ttl time.Duration) *Cache {
	c := &Cache{
		ttl:  ttl,
		stop: make(chan struct{}),
	}

	// Start background cleanup goroutine
	go c.cleanup()

	return c
}

// Get returns cached data if not expired.
func (c *Cache) Get(key string) ([]byte, bool) {
	val, ok := c.items.Load(key)
	if !ok {
		return nil, false
	}

	e := val.(*entry)
	if time.Now().After(e.expiresAt) {
		c.items.Delete(key)
		return nil, false
	}

	return e.data, true
}

// Set stores data with TTL-based expiration.
func (c *Cache) Set(key string, data []byte) {
	e := &entry{
		data:      data,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.items.Store(key, e)
}

// Delete removes a cached entry.
func (c *Cache) Delete(key string) {
	c.items.Delete(key)
}

// cleanup removes expired entries every 30 seconds.
func (c *Cache) cleanup() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.items.Range(func(key, val interface{}) bool {
				e := val.(*entry)
				if time.Now().After(e.expiresAt) {
					c.items.Delete(key)
				}
				return true
			})
		}
	}
}

// Close stops the cleanup goroutine.
func (c *Cache) Close() {
	close(c.stop)
}