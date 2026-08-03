package segment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/example/dfs/datanode/storage"
)

// countingReaderAt wraps a segment file and records how many bytes were
// pulled off disk, so a range read's IO amplification can be measured
// rather than assumed (§19).
type countingReaderAt struct {
	f    *os.File
	read int64
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := c.f.ReadAt(p, off)
	c.read += int64(n)
	return n, err
}

func (c *countingReaderAt) Close() error { return c.f.Close() }

// newRangeTestStore opens a store with segments large enough to hold the
// multi-megabyte extents these tests need.
func newRangeTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(Config{
		Dir:         t.TempDir(),
		SegmentSize: 256 << 20,
		UseMemIndex: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// openCountingReader reopens the segment holding extentID and swaps in a
// counting readerAt. It returns the reader, the counter, and the index
// value locating the record.
func openCountingReader(t *testing.T, s *Store, extentID storage.ExtentID) (*Reader, *countingReaderAt, *storage.StatResult) {
	t.Helper()
	st, err := s.Stat(context.Background(), &storage.StatRequest{ExtentID: extentID, Generation: 1})
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	path := filepath.Join(s.segDir, streamClassDir(s.streamID), "active", fmt.Sprintf("%d.seg", st.SegmentID))
	rd, err := OpenReader(path)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	t.Cleanup(func() { rd.Close() })

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open counting file: %v", err)
	}
	counter := &countingReaderAt{f: f}
	// Swap in the counter only now, so the counted bytes are exactly the
	// ones the range read itself performs.
	rd.f.Close()
	rd.f = counter
	return rd, counter, st
}

// TestRangeRead_ReadsOnlyIntersectingFrames is the §19 amplification
// gate: a small range read of a large extent must touch only the frames
// it overlaps. Before the frame-index split, ReadRangeFrames called
// ReadPayloadFrames and pulled the entire extent off disk — a 4 KiB read
// of a 16 MiB extent cost 16 MiB of IO.
func TestRangeRead_ReadsOnlyIntersectingFrames(t *testing.T) {
	s := newRangeTestStore(t)
	ctx := context.Background()

	// 16 MiB of incompressible data: compression would shrink the record
	// and weaken the amplification claim.
	const size = 16 << 20
	data := make([]byte, size)
	rng := rand.New(rand.NewSource(1))
	rng.Read(data)
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: data}); err != nil {
		t.Fatalf("write: %v", err)
	}

	rd, counter, st := openCountingReader(t, s, 1)

	// Read 4 KiB from the middle of the extent.
	const (
		reqOff = 8 << 20
		reqLen = 4 << 10
	)
	got, err := rd.ReadRangeFrames(st.Offset, st.StoredLen, st.LogicalLen, reqOff, reqLen)
	if err != nil {
		t.Fatalf("range read: %v", err)
	}
	if !bytes.Equal(got, data[reqOff:reqOff+reqLen]) {
		t.Fatal("range read returned wrong bytes")
	}

	// §19: requested + at most 2 frames. The frame index for a 16 MiB
	// extent is itself ~3 KiB, so allow the layout reads on top.
	const maxPayload = reqLen + 2*DefaultFrameSize
	layoutOverhead := int64(RecordHeaderSize + RecordTrailerSize + (size/DefaultFrameSize+1)*FrameIndexEntrySize)
	limit := int64(maxPayload) + layoutOverhead
	if counter.read > limit {
		t.Fatalf("range read pulled %d bytes off disk, want <= %d (%.1fx amplification)",
			counter.read, limit, float64(counter.read)/float64(reqLen))
	}
	// Guard the guard: a passing test must not be passing because the
	// read did nothing.
	if counter.read < reqLen {
		t.Fatalf("range read only touched %d bytes, less than the %d requested", counter.read, reqLen)
	}
	t.Logf("4 KiB range read of a 16 MiB extent touched %d bytes (limit %d)", counter.read, limit)
}

// TestRangeRead_BoundedForEveryOffset sweeps offsets across frame
// boundaries: every range read must stay within the bound and return
// exactly the requested bytes, whether it lands inside one frame or
// straddles two.
func TestRangeRead_BoundedForEveryOffset(t *testing.T) {
	s := newRangeTestStore(t)
	ctx := context.Background()

	const size = 1 << 20 // 16 frames at 64 KiB
	data := make([]byte, size)
	rand.New(rand.NewSource(2)).Read(data)
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 2, Generation: 1, Data: data}); err != nil {
		t.Fatalf("write: %v", err)
	}
	rd, counter, st := openCountingReader(t, s, 2)

	cases := []struct{ off, length int64 }{
		{0, 1},                                      // first byte
		{0, DefaultFrameSize},                       // exactly one frame
		{DefaultFrameSize - 1, 2},                   // straddles a boundary
		{DefaultFrameSize, DefaultFrameSize},        // exactly the second frame
		{3*DefaultFrameSize + 17, 4096},             // inside one frame
		{size - 1, 1},                               // last byte
		{size - DefaultFrameSize, DefaultFrameSize}, // final frame
	}
	for _, tc := range cases {
		before := counter.read
		got, err := rd.ReadRangeFrames(st.Offset, st.StoredLen, st.LogicalLen, tc.off, int32(tc.length))
		if err != nil {
			t.Fatalf("range read off=%d len=%d: %v", tc.off, tc.length, err)
		}
		if !bytes.Equal(got, data[tc.off:tc.off+tc.length]) {
			t.Fatalf("range read off=%d len=%d returned wrong bytes", tc.off, tc.length)
		}
		// Payload cost must be bounded by the frames actually spanned,
		// never by the extent size.
		spent := counter.read - before
		limit := tc.length + 2*DefaultFrameSize + int64(RecordHeaderSize+RecordTrailerSize+(size/DefaultFrameSize+1)*FrameIndexEntrySize)
		if spent > limit {
			t.Fatalf("range read off=%d len=%d touched %d bytes, want <= %d", tc.off, tc.length, spent, limit)
		}
	}
}

// TestRangeRead_RejectsOutOfRange proves an out-of-range request fails
// instead of silently returning the whole extent, which was both a
// correctness bug (unrequested bytes) and an amplification bug.
func TestRangeRead_RejectsOutOfRange(t *testing.T) {
	s := newRangeTestStore(t)
	ctx := context.Background()
	data := bytes.Repeat([]byte("payload!"), 1024) // 8 KiB
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: 3, Generation: 1, Data: data}); err != nil {
		t.Fatalf("write: %v", err)
	}
	rd, _, st := openCountingReader(t, s, 3)

	bad := []struct {
		name        string
		off, length int64
	}{
		{"offset past end", int64(len(data)), 16},
		{"offset far past end", int64(len(data)) * 4, 16},
		{"negative offset", -1, 16},
		{"zero length", 0, 0},
		{"negative length", 0, -5},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rd.ReadRangeFrames(st.Offset, st.StoredLen, st.LogicalLen, tc.off, int32(tc.length))
			if !errors.Is(err, storage.ErrInvalidRange) {
				t.Fatalf("got (%d bytes, %v), want ErrInvalidRange", len(got), err)
			}
		})
	}

	// A tail read overshooting the end is legitimate and short-reads.
	got, err := rd.ReadRangeFrames(st.Offset, st.StoredLen, st.LogicalLen, int64(len(data)-10), 100)
	if err != nil {
		t.Fatalf("tail read: %v", err)
	}
	if !bytes.Equal(got, data[len(data)-10:]) {
		t.Fatalf("tail read returned %d bytes, want the final 10", len(got))
	}
}
