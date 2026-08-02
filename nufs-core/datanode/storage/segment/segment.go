package segment

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

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

const (
	// segmentFooterCRCOffset is the first byte excluded from SegmentCRC.
	// SegmentCRC covers byte range [0, footerOffset+segmentFooterCRCOffset).
	segmentFooterCRCOffset = SegmentFooterSize - 4
	segmentCRCBufferSize   = 64 << 10
)

// SegmentHeader is the immutable header of a segment file.
type SegmentHeader struct {
	Magic        uint32 // SegmentMagic
	Version      uint8  // FormatVersion
	ID           storage.SegmentID
	SegmentClass storage.SegmentClass // small vs data
	Reserved     uint32
	HeaderCRC    uint32 // CRC32C of the preceding fields
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
	crc := storage.CRC32C(dst[0:18])
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
	gotCRC := storage.CRC32C(src[0:18])
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

// Decode parses the fixed footer fields only. A decoded footer does not prove
// that the containing sealed segment is intact; callers that trust a sealed
// file must use ValidateSealedSegment.
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

// ValidateSealedSegment validates the header, footer, and SegmentCRC of a
// sealed segment. SegmentCRC is CRC32C over the complete file from byte zero
// through the footer byte immediately before the SegmentCRC field.
func ValidateSealedSegment(f *os.File) (*SegmentFooter, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < int64(SegmentHeaderSize+SegmentFooterSize) {
		return nil, fmt.Errorf("storage: sealed segment shorter than header and footer")
	}
	footerOffset := info.Size() - int64(SegmentFooterSize)
	var headerBuf [SegmentHeaderSize]byte
	if err := readFullAt(f, headerBuf[:], 0); err != nil {
		return nil, err
	}
	var header SegmentHeader
	if err := header.Decode(headerBuf[:]); err != nil {
		return nil, err
	}
	var footerBuf [SegmentFooterSize]byte
	if err := readFullAt(f, footerBuf[:], footerOffset); err != nil {
		return nil, err
	}
	var footer SegmentFooter
	if err := footer.Decode(footerBuf[:]); err != nil {
		return nil, err
	}
	if footer.SegmentCRC == 0 {
		return nil, fmt.Errorf("%w: sealed segment crc is unset", storage.ErrChecksumMismatch)
	}
	got, err := sealedSegmentCRC(f, footerOffset, footerBuf[:segmentFooterCRCOffset])
	if err != nil {
		return nil, err
	}
	if got != footer.SegmentCRC {
		return nil, fmt.Errorf("%w: sealed segment crc got %08x want %08x", storage.ErrChecksumMismatch, got, footer.SegmentCRC)
	}
	return &footer, nil
}

// hasEncodedSegmentFooter recognizes a decodable footer at EOF without
// treating decoding as integrity validation. Readers use it only to decide
// whether they must invoke ValidateSealedSegment before serving the file.
func hasEncodedSegmentFooter(f *os.File, size int64) (bool, error) {
	if size < int64(SegmentHeaderSize+SegmentFooterSize) {
		return false, nil
	}
	var prefix [5]byte
	if err := readFullAt(f, prefix[:], size-int64(SegmentFooterSize)); err != nil {
		return false, err
	}
	return beUint32(prefix[:4]) == storage.SegmentMagic && prefix[4] == storage.FormatVersion, nil
}

// sealedSegmentCRC is shared by the sealer and validator so the excluded
// SegmentCRC field is defined in exactly one place. It streams fixed-size
// chunks and therefore never allocates in proportion to segment size.
func sealedSegmentCRC(r io.ReaderAt, footerOffset int64, footerPrefix []byte) (uint32, error) {
	if footerOffset < int64(SegmentHeaderSize) || len(footerPrefix) != segmentFooterCRCOffset {
		return 0, fmt.Errorf("storage: invalid sealed segment crc range")
	}
	h := storage.NewCRC32C()
	var buf [segmentCRCBufferSize]byte
	for off := int64(0); off < footerOffset; {
		n := int64(len(buf))
		if remaining := footerOffset - off; remaining < n {
			n = remaining
		}
		read, err := r.ReadAt(buf[:n], off)
		if read != int(n) {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return 0, err
		}
		if err != nil && err != io.EOF {
			return 0, err
		}
		if _, err := h.Write(buf[:n]); err != nil {
			return 0, err
		}
		off += n
	}
	if _, err := h.Write(footerPrefix); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}
