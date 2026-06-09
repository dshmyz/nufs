package metadata

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkPebbleStoreCreateBucket benchmarks bucket creation.
func BenchmarkPebbleStoreCreateBucket(b *testing.B) {
	dir := b.TempDir()
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	policy := PlacementPolicy{ID: "default", ReplicationFactor: 1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("bench-bucket-%d", i)
		if err := store.CreateBucket(ctx, name, policy); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPebbleStoreGetBucket benchmarks bucket lookups.
func BenchmarkPebbleStoreGetBucket(b *testing.B) {
	dir := b.TempDir()
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	policy := PlacementPolicy{ID: "default", ReplicationFactor: 1}
	store.CreateBucket(ctx, "bench-bucket", policy)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.GetBucket(ctx, "bench-bucket"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPebbleStoreMkDir benchmarks directory creation.
func BenchmarkPebbleStoreMkDir(b *testing.B) {
	dir := b.TempDir()
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	policy := PlacementPolicy{ID: "default", ReplicationFactor: 1}
	store.CreateBucket(ctx, "bench-bucket", policy)
	bucket, _ := store.GetBucket(ctx, "bench-bucket")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("dir-%d", i)
		if _, err := store.MkDir(ctx, bucket.RootInode, name, 0755); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPebbleStoreCreateFile benchmarks file creation.
func BenchmarkPebbleStoreCreateFile(b *testing.B) {
	dir := b.TempDir()
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	policy := PlacementPolicy{ID: "default", ReplicationFactor: 1}
	store.CreateBucket(ctx, "bench-bucket", policy)
	bucket, _ := store.GetBucket(ctx, "bench-bucket")
	dirInode, _ := store.MkDir(ctx, bucket.RootInode, "files", 0755)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("file-%d", i)
		if _, err := store.CreateFile(ctx, dirInode.ID, name, 0644); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPebbleStoreAllocateChunk benchmarks chunk allocation.
func BenchmarkPebbleStoreAllocateChunk(b *testing.B) {
	dir := b.TempDir()
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	policy := PlacementPolicy{ID: "default", ReplicationFactor: 1}
	store.CreateBucket(ctx, "bench-bucket", policy)
	bucket, _ := store.GetBucket(ctx, "bench-bucket")
	dirInode, _ := store.MkDir(ctx, bucket.RootInode, "chunks", 0755)
	fileInode, _ := store.CreateFile(ctx, dirInode.ID, "bigfile.dat", 0644)

	// Register a node so placement can allocate
	store.RegisterNode(ctx, &NodeInfo{
		ID: 1, Addr: "127.0.0.1:9100", Rack: "rack-1", Zone: "zone-1",
		Tier: TierHot, CapacityGB: 1000,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i) * MaxChunkSize
		if _, err := store.AllocateChunk(ctx, fileInode.ID, offset, policy); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPebbleStoreLookup benchmarks file lookup in a directory.
func BenchmarkPebbleStoreLookup(b *testing.B) {
	dir := b.TempDir()
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	policy := PlacementPolicy{ID: "default", ReplicationFactor: 1}
	store.CreateBucket(ctx, "bench-bucket", policy)
	bucket, _ := store.GetBucket(ctx, "bench-bucket")
	dirInode, _ := store.MkDir(ctx, bucket.RootInode, "lookup", 0755)

	// Pre-create files
	const numFiles = 1000
	for i := 0; i < numFiles; i++ {
		store.CreateFile(ctx, dirInode.ID, fmt.Sprintf("file-%04d", i), 0644)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("file-%04d", i%numFiles)
		if _, err := store.Lookup(ctx, dirInode.ID, name); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPebbleStoreListBuckets benchmarks listing all buckets.
func BenchmarkPebbleStoreListBuckets(b *testing.B) {
	dir := b.TempDir()
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	policy := PlacementPolicy{ID: "default", ReplicationFactor: 1}

	// Pre-create buckets
	for i := 0; i < 100; i++ {
		store.CreateBucket(ctx, fmt.Sprintf("bucket-%03d", i), policy)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.ListBuckets(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
