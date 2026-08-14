//go:build linux

package fuse

import (
	"context"
	"sync"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// BenchmarkDFSFileWriteHotPath measures the per-write cost of a small
// sequential write to a fresh file handle. This is the hot path that
// previously issued a GetInode metadata RPC per write (#38); post-fix it
// is pure in-memory (buffer copy + high-water update) except when a new
// chunk base is first touched.
func BenchmarkDFSFileWriteHotPath(b *testing.B) {
	meta, id := newTestMetaStore(b)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i)
	}
	var off int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, errno := f.Write(context.Background(), nil, payload, off); errno != 0 {
			b.Fatalf("Write: errno=%v", errno)
		}
		off += int64(len(payload))
		// Every 8 MiB roll over to a fresh region so successive writes
		// keep re-touching new chunk bases (as a long file append would),
		// exercising the lazy-hydration cost per base rather than only the
		// in-buffer copy.
		if off >= 8<<20 {
			off = 0
		}
	}
	b.SetBytes(int64(len(payload)))
}

// BenchmarkDFSFileWriteInBuffer is the control for the write benchmark: the
// same 4 KiB write repeated over a single already-hydrated chunk base, i.e.
// the pure buffer-copy + high-water cost with no per-call metadata or base
// hydration. Comparing against WriteHotPath isolates what the removed
// per-write RPC was costing per hot write.
func BenchmarkDFSFileWriteInBuffer(b *testing.B) {
	meta, id := newTestMetaStore(b)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	// Touch base 0 once so the buffer exists and no base hydration
	// happens inside the timed loop.
	priming := make([]byte, 1)
	if _, errno := f.Write(context.Background(), nil, priming, 0); errno != 0 {
		b.Fatalf("Write (prime): errno=%v", errno)
	}

	payload := make([]byte, 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, errno := f.Write(context.Background(), nil, payload, 0); errno != 0 {
			b.Fatalf("Write: errno=%v", errno)
		}
	}
	b.SetBytes(int64(len(payload)))
}

// BenchmarkDFSFileConcurrentReadParallel has 8 goroutines read disjoint
// regions of the same committed file in parallel. It demonstrates the #37
// fix: Read runs under RLock, so parallel readers proceed concurrently
// instead of serializing behind one exclusive lock.
func BenchmarkDFSFileConcurrentReadParallel(b *testing.B) {
	meta, id := newTestMetaStore(b)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	seed := make([]byte, 4<<20)
	for i := range seed {
		seed[i] = byte(i)
	}
	if _, errno := f.Write(context.Background(), nil, seed, 0); errno != 0 {
		b.Fatalf("Write seed: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		b.Fatalf("Flush seed: errno=%v", errno)
	}

	const workers = 8
	perWorker := int64(len(seed) / workers)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		dest := make([]byte, 4096)
		for pb.Next() {
			for off := int64(0); off < perWorker; off += int64(len(dest)) {
				rr, errno := f.Read(context.Background(), nil, dest, off)
				if errno != 0 {
					b.Fatalf("Read: errno=%v", errno)
				}
				if _, st := rr.Bytes(dest); st != 0 {
					b.Fatalf("rr.Bytes: %v", st)
				}
			}
		}
	})
	b.SetBytes(int64(len(seed)))
}

// BenchmarkDFSFileConcurrentReadSerial is the control for the read
// benchmark: the same total read work done in one goroutine. Comparing
// parallel vs serial isolates the RLock scaling (parallel should approach
// workers× throughput if readers no longer serialize).
func BenchmarkDFSFileConcurrentReadSerial(b *testing.B) {
	meta, id := newTestMetaStore(b)
	cs := chunkstore.NewMemoryChunkStore()
	f := newTestFile(meta, cs, id)

	seed := make([]byte, 4<<20)
	for i := range seed {
		seed[i] = byte(i)
	}
	if _, errno := f.Write(context.Background(), nil, seed, 0); errno != 0 {
		b.Fatalf("Write seed: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		b.Fatalf("Flush seed: errno=%v", errno)
	}

	perWorker := int64(len(seed)) / 8
	b.SetBytes(int64(len(seed)))
	b.ResetTimer()
	dest := make([]byte, 4096)
	for i := 0; i < b.N; i++ {
		for w := 0; w < 8; w++ {
			for off := int64(0); off < perWorker; off += int64(len(dest)) {
				rr, errno := f.Read(context.Background(), nil, dest, off)
				if errno != 0 {
					b.Fatalf("Read: errno=%v", errno)
				}
				if _, st := rr.Bytes(dest); st != 0 {
					b.Fatalf("rr.Bytes: %v", st)
				}
			}
		}
	}
}

// byteCountingStore wraps a ChunkStore and tallies how many raw chunk bytes
// each ReadChunkRange actually transfers, so benchmarks can quantify read
// amplification (a whole 64 MiB chunk vs the requested window).
type byteCountingStore struct {
	chunkstore.ChunkStore
	mu        sync.Mutex
	readCalls int64
	readBytes int64
}

func (b *byteCountingStore) ReadChunkRange(ctx context.Context, chunk *metadata.ChunkMeta, offset int64, length int32) ([]byte, error) {
	data, err := b.ChunkStore.ReadChunkRange(ctx, chunk, offset, length)
	b.mu.Lock()
	b.readCalls++
	b.readBytes += int64(len(data))
	b.mu.Unlock()
	return data, err
}

// BenchmarkDFSFileReadAmplification quantifies FUSE read amplification: how
// many raw chunk bytes a single small read pulls from the chunkstore. Before
// the range-read fix this fetches the whole 64 MiB chunk regardless of the
// requested window; after the fix it fetches only the window. The
// bytes-fetched/iter metric is the measured fetch size per read.
func BenchmarkDFSFileReadAmplification(b *testing.B) {
	meta, id := newTestMetaStore(b)
	underlying := chunkstore.NewMemoryChunkStore()
	counting := &byteCountingStore{ChunkStore: underlying}
	f := newTestFile(meta, counting, id)

	// 8 MiB committed chunk; read a single 4 KiB window near the middle so the
	// fetch must span most of the chunk's extent if amplification exists.
	const chunkSize = 8 << 20
	seed := make([]byte, chunkSize)
	for i := range seed {
		seed[i] = byte(i)
	}
	if _, errno := f.Write(context.Background(), nil, seed, 0); errno != 0 {
		b.Fatalf("Write seed: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		b.Fatalf("Flush seed: errno=%v", errno)
	}

	dest := make([]byte, 4096)
	off := int64(chunkSize / 2)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr, errno := f.Read(context.Background(), nil, dest, off)
		if errno != 0 {
			b.Fatalf("Read: errno=%v", errno)
		}
		if _, st := rr.Bytes(dest); st != 0 {
			b.Fatalf("rr.Bytes: %v", st)
		}
	}
	counting.mu.Lock()
	bytesPer := counting.readBytes
	callsPer := counting.readCalls
	counting.mu.Unlock()
	b.ReportMetric(float64(bytesPer), "bytes-fetched/iter")
	b.ReportMetric(float64(callsPer), "range-calls/iter")
}

// BenchmarkDFSFileReadSequentialWindow reads small windows sequentially across
// a multi-chunk file and reports the total bytes fetched. It verifies the
// fetched bytes scale linearly with the read volume rather than with chunk
// size: with amplification, bytes-fetched ≈ total remote-file bytes read from
// every touched chunk (each up to 64 MiB), independent of the tiny windows.
func BenchmarkDFSFileReadSequentialWindow(b *testing.B) {
	meta, id := newTestMetaStore(b)
	underlying := chunkstore.NewMemoryChunkStore()
	counting := &byteCountingStore{ChunkStore: underlying}
	f := newTestFile(meta, counting, id)

	// ~3× the single 64 MiB chunk so reads cross chunk boundaries.
	const total = 3 * 64 << 20
	seed := make([]byte, total)
	for i := range seed {
		seed[i] = byte(i)
	}
	if _, errno := f.Write(context.Background(), nil, seed, 0); errno != 0 {
		b.Fatalf("Write seed: errno=%v", errno)
	}
	if errno := f.Flush(context.Background(), nil); errno != 0 {
		b.Fatalf("Flush seed: errno=%v", errno)
	}

	window := 4096
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for off := int64(0); off < total; off += int64(window) {
			dest := make([]byte, window)
			rr, errno := f.Read(context.Background(), nil, dest, off)
			if errno != 0 {
				b.Fatalf("Read: errno=%v", errno)
			}
			if _, st := rr.Bytes(dest); st != 0 {
				b.Fatalf("rr.Bytes: %v", st)
			}
		}
	}
	counting.mu.Lock()
	bytesPer := counting.readBytes
	callsPer := counting.readCalls
	counting.mu.Unlock()
	b.ReportMetric(float64(bytesPer)/float64(4096), "fetch-bytes/read-window")
	b.ReportMetric(float64(callsPer), "range-calls/iter")
}
