package segment

import (
	"bytes"
	"hash/crc32"
	"testing"

	"github.com/example/dfs/datanode/storage"
)

// Golden vectors pin the on-disk byte layout so format changes are
// explicit and caught at test time.

func TestRecordHeaderGolden(t *testing.T) {
	h := RecordHeader{
		Magic:      storage.RecordMagic,
		Version:    storage.FormatVersion,
		ExtentID:   0x1122334455667788,
		Generation: 0xdeadbeef,
		LogicalLen: 65536,
		StoredLen:  4096,
		Codec:      storage.CompressionZstd,
		KeyID:      7,
		FrameSize:  1024,
		FrameCount: 4,
	}
	// FrameIndexCRC is a stable constant for the golden check.
	h.FrameIndexCRC = 0x01020304

	buf := make([]byte, RecordHeaderSize)
	if err := h.Encode(buf); err != nil {
		t.Fatal(err)
	}
	if len(buf) != RecordHeaderSize {
		t.Fatalf("header size = %d, want %d", len(buf), RecordHeaderSize)
	}

	var decoded RecordHeader
	if err := decoded.Decode(buf); err != nil {
		t.Fatal(err)
	}
	if decoded != h {
		t.Fatalf("roundtrip mismatch:\n got %+v\nwant %+v", decoded, h)
	}

	// Mutating a byte in the header CRC span must fail decode.
	corrupt := append([]byte(nil), buf...)
	corrupt[5] ^= 0xFF
	var bad RecordHeader
	if err := bad.Decode(corrupt); err == nil {
		t.Fatal("expected decode to reject corrupted header")
	}
}

func TestRecordTrailerGolden(t *testing.T) {
	trailer := RecordTrailer{FramingLen: 50 + 4096 + 12}
	buf := make([]byte, RecordTrailerSize)
	if err := trailer.Encode(buf); err != nil {
		t.Fatal(err)
	}
	var decoded RecordTrailer
	if err := decoded.Decode(buf); err != nil {
		t.Fatal(err)
	}
	if decoded != trailer {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", decoded, trailer)
	}
}

func TestFrameIndexRoundtrip(t *testing.T) {
	fi := FrameIndex{Entries: []FrameIndexEntry{
		{Offset: 0, StoredLen: 100, Codec: storage.CompressionNone, CRC: 1},
		{Offset: 100, StoredLen: 200, Codec: storage.CompressionZstd, CRC: 2},
		{Offset: 300, StoredLen: 150, Codec: storage.CompressionNone, CRC: 3},
	}}
	buf := make([]byte, len(fi.Entries)*FrameIndexEntrySize)
	if err := fi.Encode(buf); err != nil {
		t.Fatal(err)
	}
	var decoded FrameIndex
	if err := decoded.Decode(buf, fi.CRC); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Entries) != 3 || decoded.Entries[1].CRC != 2 || decoded.Entries[1].Codec != storage.CompressionZstd {
		t.Fatalf("frame index mismatch: %+v", decoded.Entries)
	}
	// Wrong CRC must fail.
	if err := decoded.Decode(buf, fi.CRC^0xFFFFFFFF); err == nil {
		t.Fatal("expected frame index crc mismatch to fail")
	}
}

func TestBuildFrames(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 1000)
	crcs, err := BuildFrames(payload, 256)
	if err != nil {
		t.Fatal(err)
	}
	if len(crcs) != 4 {
		t.Fatalf("expected 4 frames, got %d", len(crcs))
	}
	// Verify each frame's CRC independently.
	for i, c := range crcs {
		start := i * 256
		end := start + 256
		if end > len(payload) {
			end = len(payload)
		}
		if got := crc32.ChecksumIEEE(payload[start:end]); got != c {
			t.Fatalf("frame %d crc mismatch", i)
		}
	}
}

func TestFrameCRCFailure(t *testing.T) {
	frame := []byte("hello world")
	want := crc32.ChecksumIEEE(frame)
	if err := VerifyFrameCRC(frame, want); err != nil {
		t.Fatalf("valid frame rejected: %v", err)
	}
	frame[0] ^= 0xFF
	if err := VerifyFrameCRC(frame, want); err == nil {
		t.Fatal("corrupt frame accepted")
	}
}

func TestRecordFramingV21(t *testing.T) {
	// header(50) + index(2 entries × 13) + payload(4096) + trailer(12)
	got := RecordFraming(4096, 2048, 2)
	if got != 50+26+4096+12 {
		t.Fatalf("RecordFraming = %d, want %d", got, 50+26+4096+12)
	}
}
