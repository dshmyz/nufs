package segment

import (
	"context"
	"math/rand"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
)

// benchmarkStore builds a Store with a large segment (so the write stays in
// the active segment) and no at-rest encryption registry (s.enc == nil), so
// reads exercise the zero-copy fast path in ReadRangeFrames: frames pread
// straight into the destination buffer with no per-frame temp allocation and
// no trailing append.
func benchmarkStore(b *testing.B) *Store {
	b.Helper()
	s, err := New(Config{
		Dir:         b.TempDir(),
		SegmentSize: 1 << 30, // 1 GiB: keep the write in the active segment
		UseMemIndex: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

// BenchmarkReadRangeFrames_Plain measures the zero-copy read path on a record
// stored verbatim (unencrypted, uncompressed). A range read over the body pays
// only the disk pread + CRC per frame — no per-frame temp allocation, no copy.
// Contrast is the buffered path (compression or encryption enabled) which also
// allocates and copies per frame. The payload is seeded with incompressible
// high-entropy bytes so the store's compression sampler does NOT select zstd,
// guaranteeing the record lands on disk uncompressed.
func BenchmarkReadRangeFrames_Plain(b *testing.B) {
	s := benchmarkStore(b)
	ctx := context.Background()

	// ~48 KiB logical record (below the 64 KiB extent auto-compress threshold,
	// in the 4 KiB..64 KiB sampling range) across multiple frames (64 KiB
	// default frame size means this is 1 frame, but spans the range-read frame
	// partition math).
	data := make([]byte, 3<<14)
	rng := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = byte(rng.Intn(256))
	}
	rec, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: data})
	if err != nil {
		b.Fatalf("Write: %v", err)
	}

	// Read the whole-body range so every frame uses the zero-copy path.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := s.Read(ctx, &storage.ReadRequest{
			ExtentID:      rec.ExtentID,
			Generation:    rec.Generation,
			LogicalOffset: 0,
			Length:        int32(len(data)),
		})
		if err != nil {
			b.Fatalf("Read: %v", err)
		}
		if len(got.Data) != len(data) {
			b.Fatalf("short read: got %d want %d", len(got.Data), len(data))
		}
	}
}
