package datanode

import (
	"bytes"
	"testing"

	"github.com/example/dfs/metadata"
)

func BenchmarkWrite_1KB(b *testing.B) {
	dir := b.TempDir()
	store, _ := NewChunkStore(dir, 64, 256, nil)
	store.WaitForScan()
	defer store.Close()
	data := bytes.Repeat([]byte("x"), 1024)
	b.ResetTimer()
	b.SetBytes(1024)
	for i := 0; i < b.N; i++ {
		store.Write(metadata.ChunkID(i+1), data)
	}
}

func BenchmarkWrite_64KB(b *testing.B) {
	dir := b.TempDir()
	store, _ := NewChunkStore(dir, 64, 256, nil)
	store.WaitForScan()
	defer store.Close()
	data := bytes.Repeat([]byte("x"), 64*1024)
	b.ResetTimer()
	b.SetBytes(64 * 1024)
	for i := 0; i < b.N; i++ {
		store.Write(metadata.ChunkID(i+1), data)
	}
}

func BenchmarkRead_1KB(b *testing.B) {
	dir := b.TempDir()
	store, _ := NewChunkStore(dir, 64, 256, nil)
	store.WaitForScan()
	defer store.Close()
	data := bytes.Repeat([]byte("x"), 1024)
	for i := 0; i < 100; i++ {
		store.Write(metadata.ChunkID(i+1), data)
	}
	b.ResetTimer()
	b.SetBytes(1024)
	for i := 0; i < b.N; i++ {
		store.Read(metadata.ChunkID((i%100)+1), 0, 0)
	}
}

func BenchmarkRead_64KB(b *testing.B) {
	dir := b.TempDir()
	store, _ := NewChunkStore(dir, 64, 256, nil)
	store.WaitForScan()
	defer store.Close()
	data := bytes.Repeat([]byte("x"), 64*1024)
	for i := 0; i < 100; i++ {
		store.Write(metadata.ChunkID(i+1), data)
	}
	b.ResetTimer()
	b.SetBytes(64 * 1024)
	for i := 0; i < b.N; i++ {
		store.Read(metadata.ChunkID((i%100)+1), 0, 0)
	}
}
