package segment

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/example/dfs/datanode/storage"
)

// Segment file layout (§5.3):
//
//	SegmentHeader  (fixed size, written at creation)
//	RecordHeader + Payload + RecordTrailer  ...  (sealed writes)
//	SegmentFooter  (fixed size, written at seal)
//
// The header is immutable after creation. The footer is written once,
// at seal time, and its presence distinguishes a sealed segment from an
// active one.

// SegmentHeaderSize is the fixed on-disk size of SegmentHeader.
const SegmentHeaderSize = storage.SegmentHeaderSize

// SegmentFooterSize is the fixed on-disk size of SegmentFooter.
const SegmentFooterSize = storage.SegmentFooterSize

// SegmentHeader is the immutable header of a segment file.
type SegmentHeader struct {
	Magic   uint32 // SegmentMagic
	Version uint8  // FormatVersion
	ID      storage.SegmentID
	SegmentClass storage.SegmentClass // small vs data
	Reserved    uint32
	HeaderCRC   uint32 // CRC32C of the preceding fields
}

// SegmentFooter is written once when a segment seals (V2.1 §5.3/§7.2).
// LastCommittedSeq is the last committed stream sequence so recovery
// knows exactly which records in this segment are durable.
type SegmentFooter struct {
	Magic            uint32 // SegmentMagic
	Version          uint8  // FormatVersion
	RecordCount      uint64
	TotalPayload     uint64
	MinExtentID      storage.ExtentID
	MaxExtentID      storage.ExtentID
	LastCommittedSeq uint64 // last committed BatchCommit seq in this segment
	CreatedAtUnix    int64
	SealedAtUnix     int64
	SegmentCRC       uint32 // CRC32C of the sealed segment bytes (header..footer excl. this field)
}

// Encode writes the header bytes.
func (h *SegmentHeader) Encode(dst []byte) error {
	if len(dst) < SegmentHeaderSize {
		return fmt.Errorf("storage: segment header buffer too small")
	}
	binary.BigEndian.PutUint32(dst[0:4], h.Magic)
	dst[4] = h.Version
	binary.BigEndian.PutUint64(dst[5:13], uint64(h.ID))
	dst[13] = byte(h.SegmentClass)
	binary.BigEndian.PutUint32(dst[14:18], h.Reserved)
	crc := crc32.ChecksumIEEE(dst[0:18])
	binary.BigEndian.PutUint32(dst[18:22], crc)
	return nil
}

// Decode parses and validates a segment header.
func (h *SegmentHeader) Decode(src []byte) error {
	if len(src) < SegmentHeaderSize {
		return fmt.Errorf("storage: segment header too short")
	}
	h.Magic = binary.BigEndian.Uint32(src[0:4])
	h.Version = src[4]
	h.ID = storage.SegmentID(binary.BigEndian.Uint64(src[5:13]))
	h.SegmentClass = storage.SegmentClass(src[13])
	h.Reserved = binary.BigEndian.Uint32(src[14:18])
	wantCRC := binary.BigEndian.Uint32(src[18:22])
	if h.Magic != storage.SegmentMagic {
		return fmt.Errorf("storage: bad segment magic 0x%x", h.Magic)
	}
	if h.Version != storage.FormatVersion {
		return fmt.Errorf("storage: unsupported segment version %d", h.Version)
	}
	gotCRC := crc32.ChecksumIEEE(src[0:18])
	if gotCRC != wantCRC {
		return fmt.Errorf("storage: segment header crc mismatch")
	}
	return nil
}

// Encode writes the footer bytes.
func (f *SegmentFooter) Encode(dst []byte) error {
	if len(dst) < SegmentFooterSize {
		return fmt.Errorf("storage: segment footer buffer too small")
	}
	binary.BigEndian.PutUint32(dst[0:4], f.Magic)
	dst[4] = f.Version
	binary.BigEndian.PutUint64(dst[5:13], f.RecordCount)
	binary.BigEndian.PutUint64(dst[13:21], f.TotalPayload)
	binary.BigEndian.PutUint64(dst[21:29], uint64(f.MinExtentID))
	binary.BigEndian.PutUint64(dst[29:37], uint64(f.MaxExtentID))
	binary.BigEndian.PutUint64(dst[37:45], f.LastCommittedSeq)
	binary.BigEndian.PutUint64(dst[45:53], uint64(f.CreatedAtUnix))
	binary.BigEndian.PutUint64(dst[53:61], uint64(f.SealedAtUnix))
	binary.BigEndian.PutUint32(dst[61:65], f.SegmentCRC)
	return nil
}

// Decode parses and validates a segment footer.
func (f *SegmentFooter) Decode(src []byte) error {
	if len(src) < SegmentFooterSize {
		return fmt.Errorf("storage: segment footer too short")
	}
	f.Magic = binary.BigEndian.Uint32(src[0:4])
	f.Version = src[4]
	f.RecordCount = binary.BigEndian.Uint64(src[5:13])
	f.TotalPayload = binary.BigEndian.Uint64(src[13:21])
	f.MinExtentID = storage.ExtentID(binary.BigEndian.Uint64(src[21:29]))
	f.MaxExtentID = storage.ExtentID(binary.BigEndian.Uint64(src[29:37]))
	f.LastCommittedSeq = binary.BigEndian.Uint64(src[37:45])
	f.CreatedAtUnix = int64(binary.BigEndian.Uint64(src[45:53]))
	f.SealedAtUnix = int64(binary.BigEndian.Uint64(src[53:61]))
	f.SegmentCRC = binary.BigEndian.Uint32(src[61:65])
	if f.Magic != storage.SegmentMagic {
		return fmt.Errorf("storage: bad segment footer magic 0x%x", f.Magic)
	}
	if f.Version != storage.FormatVersion {
		return fmt.Errorf("storage: unsupported segment footer version %d", f.Version)
	}
	return nil
}
