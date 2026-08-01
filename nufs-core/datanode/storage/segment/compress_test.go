package segment

import (
	"bytes"
	"context"
	"testing"

	"github.com/example/dfs/datanode/storage"
)

// TestCompressionRoundtrip_Compressible verifies a highly-compressible
// payload round-trips through the frame-level zstd path (both in memory
// and after reopen).
func TestCompressionRoundtrip_Compressible(t *testing.T) {
	dir := t.TempDir()
	s, err := New(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), 4096)
	if _, err := s.Write(context.Background(), &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: data}); err != nil {
		t.Fatal(err)
	}
	// In-memory read.
	got, err := s.Read(context.Background(), &storage.ReadRequest{ExtentID: 1, Generation: 1})
	if err != nil {
		t.Fatalf("in-memory read: %v", err)
	}
	if !bytes.Equal(got.Data, data) {
		t.Fatal("in-memory compressed roundtrip mismatch")
	}
	s.Close()

	// Reopen and read (recovery replay path).
	s2, err := New(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got2, err := s2.Read(context.Background(), &storage.ReadRequest{ExtentID: 1, Generation: 1})
	if err != nil {
		t.Fatalf("reopen read: %v", err)
	}
	if !bytes.Equal(got2.Data, data) {
		t.Fatal("reopen compressed roundtrip mismatch")
	}
}

// TestCompressionRoundtrip_Incompressible verifies that a payload that
// does not compress is stored uncompressed per-frame and still
// round-trips.
func TestCompressionRoundtrip_Incompressible(t *testing.T) {
	dir := t.TempDir()
	s, err := New(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	// Pseudo-random incompressible bytes.
	data := make([]byte, 128<<10)
	for i := range data {
		data[i] = byte((i * 31) ^ 0x5A)
	}
	if _, err := s.Write(context.Background(), &storage.WriteRequest{ExtentID: 2, Generation: 1, Data: data}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Read(context.Background(), &storage.ReadRequest{ExtentID: 2, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Data, data) {
		t.Fatal("incompressible roundtrip mismatch")
	}
	s.Close()
}

// TestCompressionSamplingRule verifies the §9 sampling: tiny payloads
// are never compressed; the compressibility decision is size-driven.
func TestCompressionSamplingRule(t *testing.T) {
	if ShouldCompress(100, make([]byte, 100)) {
		t.Fatal("sub-4KiB payload should never be compressed")
	}
	if ShouldCompress(60000, bytes.Repeat([]byte("a"), 60000)) != true {
		t.Fatal("highly compressible 60KiB payload should be compressed")
	}
	if ShouldCompress(60000, func() []byte {
		// XOR-shift pseudo-random: genuinely incompressible.
		b := make([]byte, 60000)
		x := uint32(0x12345678)
		for i := range b {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			b[i] = byte(x)
		}
		return b
	}()) {
		t.Fatal("incompressible 60KiB payload should not be compressed")
	}
}
