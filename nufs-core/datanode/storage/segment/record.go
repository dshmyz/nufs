package segment

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/example/dfs/datanode/storage"
)

// Record layout in a segment (V2.1 §5.3):
//
//	RecordHeader  (fixed size)
//	FrameIndex    (fixed size × frame_count; checksummed)
//	Frame 0       (frame_size bytes, per-frame CRC32C + AEAD)
//	Frame 1       ...
//	RecordTrailer (fixed size)
//
// Data records are divided into independently readable frames (default
// 64 KiB). Range reads fetch and authenticate only intersecting frames.
// Uncompressed frame offsets are computed directly; compressed records
// use the checksummed frame index. Each frame carries its own CRC32C
// (and AEAD tag when encryption is enabled, handled at a higher layer).

// DefaultFrameSize is the per-frame payload size (§16).
const DefaultFrameSize = 64 << 10 // 64 KiB

// RecordHeaderSize is the fixed on-disk size of RecordHeader (V2.1).
const RecordHeaderSize = 4 + 1 + 8 + 8 + 4 + 4 + 1 + 8 + 2 + 2 + 4 + 4 + 4 // 54

// RecordTrailerSize is the fixed on-disk size of RecordTrailer.
const RecordTrailerSize = 8 + 4 // 12

// FrameIndexEntrySize is the fixed on-disk size of one frame-index entry.
const FrameIndexEntrySize = 4 + 4 + 1 + 4 // offset + stored_len + codec + crc

// RecordHeader is the fixed-size header of one record (V2.1 §5.3).
type RecordHeader struct {
	Magic      uint32                   // RecordMagic
	Version    uint8                    // FormatVersion
	ExtentID   storage.ExtentID         // 8 bytes
	Generation storage.Generation       // 8 bytes
	LogicalLen uint32                   // logical payload length
	StoredLen  uint32                   // stored (possibly compressed/encrypted) length
	Codec      storage.CompressionCodec // 1 byte
	KeyID      uint64                   // encryption key ID (0 = plaintext)
	FrameSize  uint16                   // bytes per frame payload (0 = default 64KiB)
	FrameCount uint16                   // number of frames in this record
	// PayloadChecksum is the checksum of the logical payload. It lets
	// recovery bind a BatchDescriptor without reading payload frames.
	PayloadChecksum uint32
	HeaderCRC       uint32 // CRC32C of the preceding fields
	FrameIndexCRC   uint32 // CRC32C of the frame-index bytes
}

// RecordTrailer is appended after the payload. It carries the framing
// length so a reader can detect truncation and skip records.
type RecordTrailer struct {
	FramingLen uint32 // total record length incl. header + index + frames + trailer
	TrailerCRC uint32 // CRC32C of FramingLen
}

// Encode writes the header as fixed-size big-endian bytes. HeaderCRC is
// computed over all bytes before the checksum fields themselves.
func (h *RecordHeader) Encode(dst []byte) error {
	if len(dst) < RecordHeaderSize {
		return fmt.Errorf("storage: record header buffer too small: %d < %d", len(dst), RecordHeaderSize)
	}
	binary.BigEndian.PutUint32(dst[0:4], h.Magic)
	dst[4] = h.Version
	binary.BigEndian.PutUint64(dst[5:13], uint64(h.ExtentID))
	binary.BigEndian.PutUint64(dst[13:21], uint64(h.Generation))
	binary.BigEndian.PutUint32(dst[21:25], h.LogicalLen)
	binary.BigEndian.PutUint32(dst[25:29], h.StoredLen)
	dst[29] = byte(h.Codec)
	binary.BigEndian.PutUint64(dst[30:38], h.KeyID)
	binary.BigEndian.PutUint16(dst[38:40], h.FrameSize)
	binary.BigEndian.PutUint16(dst[40:42], h.FrameCount)
	// HeaderCRC covers bytes [0, 46); then write both checksums.
	binary.BigEndian.PutUint32(dst[42:46], h.PayloadChecksum)
	hdrCRC := crc32.ChecksumIEEE(dst[0:46])
	binary.BigEndian.PutUint32(dst[46:50], hdrCRC)
	binary.BigEndian.PutUint32(dst[50:54], h.FrameIndexCRC)
	return nil
}

// Decode parses a header from fixed-size bytes and verifies Magic,
// Version, and HeaderCRC.
func (h *RecordHeader) Decode(src []byte) error {
	if len(src) < RecordHeaderSize {
		return fmt.Errorf("storage: record header too short: %d < %d", len(src), RecordHeaderSize)
	}
	h.Magic = binary.BigEndian.Uint32(src[0:4])
	h.Version = src[4]
	h.ExtentID = storage.ExtentID(binary.BigEndian.Uint64(src[5:13]))
	h.Generation = storage.Generation(binary.BigEndian.Uint64(src[13:21]))
	h.LogicalLen = binary.BigEndian.Uint32(src[21:25])
	h.StoredLen = binary.BigEndian.Uint32(src[25:29])
	h.Codec = storage.CompressionCodec(src[29])
	h.KeyID = binary.BigEndian.Uint64(src[30:38])
	h.FrameSize = binary.BigEndian.Uint16(src[38:40])
	h.FrameCount = binary.BigEndian.Uint16(src[40:42])
	h.PayloadChecksum = binary.BigEndian.Uint32(src[42:46])
	wantCRC := binary.BigEndian.Uint32(src[46:50])
	h.FrameIndexCRC = binary.BigEndian.Uint32(src[50:54])

	if h.Magic != storage.RecordMagic {
		return fmt.Errorf("storage: bad record magic 0x%x", h.Magic)
	}
	if h.Version != storage.FormatVersion {
		return fmt.Errorf("storage: unsupported record version %d", h.Version)
	}
	gotCRC := crc32.ChecksumIEEE(src[0:46])
	if gotCRC != wantCRC {
		return fmt.Errorf("storage: record header crc mismatch: got %d want %d", gotCRC, wantCRC)
	}
	return nil
}

// EffectiveFrameSize returns the frame payload size for this record.
func (h *RecordHeader) EffectiveFrameSize() int {
	if h.FrameSize == 0 {
		return DefaultFrameSize
	}
	return int(h.FrameSize)
}

// FrameIndexEntry locates one frame within a record. Offsets are
// relative to the first frame byte (after the frame index). For
// compressed records the stored length is needed to delimit frames;
// for uncompressed records it equals the frame size.
type FrameIndexEntry struct {
	// Offset is the byte offset of the frame's payload start relative
	// to the record's first frame byte.
	Offset uint32
	// StoredLen is the stored length of this frame.
	StoredLen uint32
	// Codec is the compression codec applied to THIS frame (independent
	// per-frame compression, §5.3).
	Codec storage.CompressionCodec
	// CRC is the frame payload CRC32C.
	CRC uint32
}

// FrameIndex is the checksummed list of frame offsets/CRCs.
type FrameIndex struct {
	Entries []FrameIndexEntry
	// CRC covers the serialized entries (for compressed records where
	// offsets are not directly derivable; uncompressed records still
	// carry it for uniform validation).
	CRC uint32
}

// Encode serializes the frame index entries and computes their CRC.
func (fi *FrameIndex) Encode(dst []byte) error {
	need := len(fi.Entries) * FrameIndexEntrySize
	if len(dst) < need {
		return fmt.Errorf("storage: frame index buffer too small")
	}
	for i, e := range fi.Entries {
		base := i * FrameIndexEntrySize
		binary.BigEndian.PutUint32(dst[base:base+4], e.Offset)
		binary.BigEndian.PutUint32(dst[base+4:base+8], e.StoredLen)
		dst[base+8] = byte(e.Codec)
		binary.BigEndian.PutUint32(dst[base+9:base+13], e.CRC)
	}
	fi.CRC = crc32.ChecksumIEEE(dst[0:need])
	return nil
}

// Decode parses the frame index entries and verifies their CRC.
func (fi *FrameIndex) Decode(src []byte, expectedCRC uint32) error {
	if len(src)%FrameIndexEntrySize != 0 {
		return fmt.Errorf("storage: frame index length not multiple of entry size")
	}
	fi.Entries = fi.Entries[:0]
	for i := 0; i < len(src); i += FrameIndexEntrySize {
		fi.Entries = append(fi.Entries, FrameIndexEntry{
			Offset:    binary.BigEndian.Uint32(src[i : i+4]),
			StoredLen: binary.BigEndian.Uint32(src[i+4 : i+8]),
			Codec:     storage.CompressionCodec(src[i+8]),
			CRC:       binary.BigEndian.Uint32(src[i+9 : i+13]),
		})
	}
	got := crc32.ChecksumIEEE(src)
	if got != expectedCRC {
		return fmt.Errorf("storage: frame index crc mismatch: got %d want %d", got, expectedCRC)
	}
	return nil
}

// BuildFrames splits a payload into frames and returns each frame's
// CRC32C. The caller writes frames sequentially; offsets for
// uncompressed records are derivable, but the CRCs are always stored.
func BuildFrames(payload []byte, frameSize int) ([]uint32, error) {
	if frameSize <= 0 {
		frameSize = DefaultFrameSize
	}
	n := (len(payload) + frameSize - 1) / frameSize
	crCS := make([]uint32, n)
	for i := 0; i < n; i++ {
		start := i * frameSize
		end := start + frameSize
		if end > len(payload) {
			end = len(payload)
		}
		crCS[i] = crc32.ChecksumIEEE(payload[start:end])
	}
	return crCS, nil
}

// Encode writes the trailer bytes. TrailerCRC covers FramingLen.
func (t *RecordTrailer) Encode(dst []byte) error {
	if len(dst) < RecordTrailerSize {
		return fmt.Errorf("storage: record trailer buffer too small")
	}
	binary.BigEndian.PutUint32(dst[0:4], t.FramingLen)
	binary.BigEndian.PutUint32(dst[4:8], crc32.ChecksumIEEE(dst[0:4]))
	return nil
}

// Decode parses a trailer and verifies TrailerCRC.
func (t *RecordTrailer) Decode(src []byte) error {
	if len(src) < RecordTrailerSize {
		return fmt.Errorf("storage: record trailer too short")
	}
	t.FramingLen = binary.BigEndian.Uint32(src[0:4])
	wantCRC := binary.BigEndian.Uint32(src[4:8])
	gotCRC := crc32.ChecksumIEEE(src[0:4])
	if gotCRC != wantCRC {
		return fmt.Errorf("storage: record trailer crc mismatch")
	}
	return nil
}

// Framing returns the full on-disk record size for a payload split
// into frames with the given frame size. Offsets are sequential:
// header + index + sum(frame_payload).
func RecordFraming(storedLen uint32, frameSize int, frameCount int) uint32 {
	indexBytes := frameCount * FrameIndexEntrySize
	return RecordHeaderSize + uint32(indexBytes) + storedLen + RecordTrailerSize
}

// VerifyFrameCRC checks one frame payload against its CRC.
func VerifyFrameCRC(payload []byte, want uint32) error {
	got := crc32.ChecksumIEEE(payload)
	if got != want {
		return fmt.Errorf("%w: stored=%d computed=%d", storage.ErrChecksumMismatch, want, got)
	}
	return nil
}
