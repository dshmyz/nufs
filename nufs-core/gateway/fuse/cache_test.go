//go:build linux

package fuse

import (
	"os"
	"path"
	"testing"

	lru "github.com/hashicorp/golang-lru"
)

func TestChunkCacheMemoryOnly(t *testing.T) {
	c, err := NewChunkCache("")
	if err != nil {
		t.Fatalf("NewChunkCache: %v", err)
	}

	if _, ok := c.Get(1); ok {
		t.Fatal("expected miss for empty cache")
	}

	c.Add(1, []byte("hello"))
	got, ok := c.Get(1)
	if !ok {
		t.Fatal("expected hit after add")
	}
	if string(got) != "hello" {
		t.Errorf("got %q, want %q", string(got), "hello")
	}
}

func TestChunkCacheDisk(t *testing.T) {
	dir := t.TempDir()
	c, err := NewChunkCache(path.Join(dir, "cache"))
	if err != nil {
		t.Fatalf("NewChunkCache: %v", err)
	}

	c.Add(1, []byte("disk-data"))
	got, ok := c.Get(1)
	if !ok {
		t.Fatal("expected hit")
	}
	if string(got) != "disk-data" {
		t.Errorf("got %q, want %q", string(got), "disk-data")
	}

	// Verify file exists on disk
	entries, err := os.ReadDir(path.Join(dir, "cache"))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 cache file, got %d", len(entries))
	}
}

func TestChunkCacheDiskPersistence(t *testing.T) {
	dir := t.TempDir()
	cacheDir := path.Join(dir, "cache")

	c1, err := NewChunkCache(cacheDir)
	if err != nil {
		t.Fatalf("NewChunkCache 1: %v", err)
	}
	c1.Add(42, []byte("stays"))
	c1.Add(99, []byte("goes"))
	c1.Remove(99)

	c2, err := NewChunkCache(cacheDir)
	if err != nil {
		t.Fatalf("NewChunkCache 2: %v", err)
	}

	// 42 should still be on disk
	got, ok := c2.Get(42)
	if !ok {
		t.Fatal("expected hit for chunk 42 from disk")
	}
	if string(got) != "stays" {
		t.Errorf("got %q, want %q", string(got), "stays")
	}

	// 99 should be gone (removed)
	if _, ok := c2.Get(99); ok {
		t.Fatal("expected miss for removed chunk 99")
	}
}

func TestChunkCacheRemove(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewChunkCache(path.Join(dir, "cache"))

	c.Add(1, []byte("data"))
	c.Remove(1)

	if _, ok := c.Get(1); ok {
		t.Fatal("expected miss after remove")
	}

	// Verify disk file is removed
	entries, _ := os.ReadDir(path.Join(dir, "cache"))
	if len(entries) != 0 {
		t.Fatalf("expected 0 cache files after remove, got %d", len(entries))
	}
}

func TestChunkCacheHitRate(t *testing.T) {
	c, _ := NewChunkCache("")

	// No accesses yet
	if r := c.HitRate(); r != 0 {
		t.Fatalf("expected 0 hit rate, got %f", r)
	}

	c.Get(1) // miss
	c.Get(2) // miss
	c.Add(1, []byte("x"))
	c.Get(1) // hit

	rate := c.HitRate()
	if rate != 0.25 {
		t.Errorf("expected 0.25 hit rate, got %f", rate)
	}
}

func TestChunkCacheEviction(t *testing.T) {
	// LRU with small capacity — force eviction.
	memory, err := NewChunkCache("")
	if err != nil {
		t.Fatalf("NewChunkCache: %v", err)
	}
	// Overwrite the default 1000-entry LRU with a 2-entry one manually.
	memory.memory, _ = lru.New(2)

	memory.Add(1, []byte("a"))
	memory.Add(2, []byte("b"))
	memory.Add(3, []byte("c")) // evicts 1

	if _, ok := memory.Get(1); ok {
		t.Fatal("expected chunk 1 to be evicted")
	}
	if _, ok := memory.Get(2); !ok {
		t.Fatal("expected chunk 2 to still be present")
	}
	if _, ok := memory.Get(3); !ok {
		t.Fatal("expected chunk 3 to be present")
	}
}

func TestChunkCacheDiskDataIntegrity(t *testing.T) {
	dir := t.TempDir()
	c, _ := NewChunkCache(path.Join(dir, "cache"))

	data := make([]byte, 1024*64)
	for i := range data {
		data[i] = byte(i % 256)
	}
	c.Add(7, data)

	got, ok := c.Get(7)
	if !ok {
		t.Fatal("expected hit")
	}
	if len(got) != len(data) {
		t.Fatalf("len = %d, want %d", len(got), len(data))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("byte %d: got %d, want %d", i, got[i], data[i])
		}
	}
}

func TestChunkCacheConcurrent(t *testing.T) {
	c, _ := NewChunkCache("")
	done := make(chan struct{})

	go func() {
		for i := 0; i < 100; i++ {
			c.Add(uint64(i), []byte("data"))
		}
		done <- struct{}{}
	}()
	go func() {
		for i := 0; i < 100; i++ {
			c.Get(uint64(i))
		}
		done <- struct{}{}
	}()

	<-done
	<-done
}
