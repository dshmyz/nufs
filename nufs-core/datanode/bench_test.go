package datanode

import (
	"fmt"
	"testing"

	"github.com/example/dfs/metadata"
)

// BenchmarkChunkStoreWrite benchmarks sequential chunk writes.
func BenchmarkChunkStoreWrite(b *testing.B) {
	dir := b.TempDir()
	store, err := NewChunkStore(dir, 64, 64, nil)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 64*1024) // 64KB chunks
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		chunkID := metadata.ChunkID(i + 1)
		if err := store.Write(chunkID, data); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkChunkStoreRead benchmarks sequential chunk reads.
func BenchmarkChunkStoreRead(b *testing.B) {
	dir := b.TempDir()
	store, err := NewChunkStore(dir, 64, 64, nil)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 64*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Pre-write chunks
	const numChunks = 1000
	for i := 0; i < numChunks; i++ {
		store.Write(metadata.ChunkID(i+1), data)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		chunkID := metadata.ChunkID(i%numChunks + 1)
		if _, _, err := store.Read(chunkID, 0, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkChunkStoreWriteParallel benchmarks concurrent chunk writes.
func BenchmarkChunkStoreWriteParallel(b *testing.B) {
	dir := b.TempDir()
	store, err := NewChunkStore(dir, 64, 64, nil)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 4*1024) // 4KB chunks
	for i := range data {
		data[i] = byte(i % 256)
	}

	var seq int64
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			chunkID := metadata.ChunkID(seq + 1)
			seq++
			if err := store.Write(chunkID, data); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkChunkStoreReadParallel benchmarks concurrent chunk reads.
func BenchmarkChunkStoreReadParallel(b *testing.B) {
	dir := b.TempDir()
	store, err := NewChunkStore(dir, 64, 64, nil)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 4*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	const numChunks = 500
	for i := 0; i < numChunks; i++ {
		store.Write(metadata.ChunkID(i+1), data)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			chunkID := metadata.ChunkID(i%numChunks + 1)
			i++
			if _, _, err := store.Read(chunkID, 0, 0); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkChunkStoreMixed benchmarks a mixed read/write workload (80% read, 20% write).
func BenchmarkChunkStoreMixed(b *testing.B) {
	dir := b.TempDir()
	store, err := NewChunkStore(dir, 64, 64, nil)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 8*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	const numPreloaded = 200
	for i := 0; i < numPreloaded; i++ {
		store.Write(metadata.ChunkID(i+1), data)
	}

	var writeSeq int64 = numPreloaded
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			// 20% writes
			chunkID := metadata.ChunkID(writeSeq + 1)
			writeSeq++
			if err := store.Write(chunkID, data); err != nil {
				b.Fatal(err)
			}
		} else {
			// 80% reads
			chunkID := metadata.ChunkID(i%numPreloaded + 1)
			if _, _, err := store.Read(chunkID, 0, 0); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkServerClientRoundTrip benchmarks a full write-then-read round trip
// through the TCP server.
func BenchmarkServerClientRoundTrip(b *testing.B) {
	dir := b.TempDir()
	store, err := NewChunkStore(dir, 64, 64, nil)
	if err != nil {
		b.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = 1

	srv := NewServer(cfg, store)
	if err := srv.Start(); err != nil {
		b.Fatal(err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()
	client := NewClient(addr)
	if err := client.Connect(); err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	data := make([]byte, 4*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data)) * 2) // write + read
	for i := 0; i < b.N; i++ {
		chunkID := metadata.ChunkID(i + 1)
		resp, err := client.WriteChunk(chunkID, data)
		if err != nil {
			b.Fatal(err)
		}
		if resp.Status != StatusOK {
			b.Fatalf("write status: %v", resp.Status)
		}

		readResp, err := client.ReadChunk(chunkID, 0, 0)
		if err != nil {
			b.Fatal(err)
		}
		if readResp.Status != StatusOK {
			b.Fatalf("read status: %v", readResp.Status)
		}
	}
}

// BenchmarkWALWrite benchmarks WAL log entry writes.
func BenchmarkWALWrite(b *testing.B) {
	dir := b.TempDir()
	wal, err := NewWriteAheadLog(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunkID := metadata.ChunkID(i + 1)
		if err := wal.LogWrite(chunkID, 4096); err != nil {
			b.Fatal(err)
		}
		if err := wal.LogCommit(chunkID); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkChunkStoreRangeReadAmplification benchmarks small range reads from large chunks.
func BenchmarkChunkStoreRangeReadAmplification(b *testing.B) {
	dir := b.TempDir()
	store, err := NewChunkStore(dir, 64, 64, nil)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 4*1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	chunkID := metadata.ChunkID(1)
	if err := store.Write(chunkID, data); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.SetBytes(4 * 1024)
	for i := 0; i < b.N; i++ {
		off := int64((i * 4096) % (len(data) - 4096))
		if _, _, err := store.Read(chunkID, off, 4*1024); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkChunkStoreWriteAt benchmarks partial writes that update CRC state.
func BenchmarkChunkStoreWriteAt(b *testing.B) {
	dir := b.TempDir()
	store, err := NewChunkStore(dir, 64, 64, nil)
	if err != nil {
		b.Fatal(err)
	}

	base := make([]byte, 1024*1024)
	if err := store.Write(metadata.ChunkID(1), base); err != nil {
		b.Fatal(err)
	}
	patch := make([]byte, 4*1024)
	for i := range patch {
		patch[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(patch)))
	for i := 0; i < b.N; i++ {
		off := int64((i * len(patch)) % (len(base) - len(patch)))
		if err := store.WriteAt(metadata.ChunkID(1), off, patch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkChunkStoreRangeReadUnsealed benchmarks small range reads on unsealed
// chunks where the optimized SectionReader path applies (no CRC, no full read).
func BenchmarkChunkStoreRangeReadUnsealed(b *testing.B) {
	dir := b.TempDir()
	store, err := NewChunkStore(dir, 64, 64, nil)
	if err != nil {
		b.Fatal(err)
	}

	base := make([]byte, 4*1024*1024)
	for i := range base {
		base[i] = byte(i % 256)
	}
	chunkID := metadata.ChunkID(1)
	if err := store.Write(chunkID, base); err != nil {
		b.Fatal(err)
	}
	// Partial write marks chunk as unsealed (CRC=0)
	if err := store.WriteAt(chunkID, 0, make([]byte, 4096)); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.SetBytes(4 * 1024)
	for i := 0; i < b.N; i++ {
		off := int64((i * 4096) % (len(base) - 4096))
		if _, _, err := store.Read(chunkID, off, 4096); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkChunkStoreListChunks benchmarks full in-memory chunk list snapshots.
func BenchmarkChunkStoreListChunks(b *testing.B) {
	dir := b.TempDir()
	store, err := NewChunkStore(dir, 64, 64, nil)
	if err != nil {
		b.Fatal(err)
	}

	data := []byte("x")
	for i := 0; i < 10000; i++ {
		if err := store.Write(metadata.ChunkID(i+1), data); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunks := store.ListChunks()
		if len(chunks) != 10000 {
			b.Fatalf("expected 10000 chunks, got %d", len(chunks))
		}
	}
}

// BenchmarkChunkStoreVariousSizes benchmarks writes at different chunk sizes.
func BenchmarkChunkStoreVariousSizes(b *testing.B) {
	sizes := []int{1024, 4 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Write_%dKB", size/1024), func(b *testing.B) {
			dir := b.TempDir()
			store, err := NewChunkStore(dir, 64, 64, nil)
			if err != nil {
				b.Fatal(err)
			}

			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				if err := store.Write(metadata.ChunkID(i+1), data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
