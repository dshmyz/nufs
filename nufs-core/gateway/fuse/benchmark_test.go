package fuse

import (
	"context"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
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
