package journal

import (
	"encoding/binary"
	"fmt"

	"github.com/example/dfs/datanode/storage"
)

// BatchCommit is the foreground durability point (V2.1 §5.3/§6.4).
// Every acknowledged local batch is represented by one durable
// BatchCommit record appended to the active segment, synced with the
// payloads in a single fdatasync. Records after the last valid
// BatchCommit are uncommitted tail data and are discarded during
// recovery.
//
// A disk has two commit streams (small + data); a batch never spans
// two segment files. Each stream's sequence is stream-local and
// monotonically increasing.
type BatchCommit struct {
	// Magic/Version identify the record.
	Magic   uint32 // BatchCommitMagic
	Version uint8  // FormatVersion
	// StreamID distinguishes small (0) from data (1) commit streams.
	StreamID uint8
	// Seq is the stream-local commit sequence.
	Seq uint64
	// RecordCount is the number of logical records committed.
	RecordCount uint32
	// FirstOffset/LastOffset bound the committed record offsets within
	// the segment (for tail validation).
	FirstOffset int64
	LastOffset  int64
	// DescriptorsCRC checksums the batch descriptors (one per record).
	DescriptorsCRC uint32
	// CRC covers this BatchCommit body.
	CRC uint32
}

// BatchCommitMagic distinguishes BatchCommit records in a segment.
const BatchCommitMagic = 0x42434F4D // "BCOM"

// BatchCommitSize is the fixed on-disk size of a BatchCommit body.
const BatchCommitSize = 4 + 1 + 1 + 8 + 4 + 8 + 8 + 4 + 4 // 42

// BatchDescriptor describes one committed record within a batch.
type BatchDescriptor struct {
	ExtentID   storage.ExtentID
	Generation storage.Generation
	SegmentID  storage.SegmentID
	Offset     int64
	StoredLen  uint32
	LogicalLen uint32
	Checksum   uint32
	// Op binds the durable mutation type to the BatchCommit checksum.
	Op uint8
}

// BatchDescriptorSize is the fixed on-disk size of one descriptor.
const BatchDescriptorSize = 8 + 8 + 8 + 8 + 4 + 4 + 4 + 1 // 45

// Encode writes the BatchCommit body.
func (b *BatchCommit) Encode(dst []byte) error {
	if len(dst) < BatchCommitSize {
		return fmt.Errorf("storage: batchcommit buffer too small")
	}
	binary.BigEndian.PutUint32(dst[0:4], b.Magic)
	dst[4] = b.Version
	dst[5] = b.StreamID
	binary.BigEndian.PutUint64(dst[6:14], b.Seq)
	binary.BigEndian.PutUint32(dst[14:18], b.RecordCount)
	binary.BigEndian.PutUint64(dst[18:26], uint64(b.FirstOffset))
	binary.BigEndian.PutUint64(dst[26:34], uint64(b.LastOffset))
	binary.BigEndian.PutUint32(dst[34:38], b.DescriptorsCRC)
	// CRC covers [0,38).
	crc := storage.CRC32C(dst[0:38])
	binary.BigEndian.PutUint32(dst[38:42], crc)
	return nil
}

// Decode parses and validates a BatchCommit body.
func (b *BatchCommit) Decode(src []byte) error {
	if len(src) < BatchCommitSize {
		return fmt.Errorf("storage: batchcommit too short")
	}
	b.Magic = binary.BigEndian.Uint32(src[0:4])
	b.Version = src[4]
	b.StreamID = src[5]
	b.Seq = binary.BigEndian.Uint64(src[6:14])
	b.RecordCount = binary.BigEndian.Uint32(src[14:18])
	b.FirstOffset = int64(binary.BigEndian.Uint64(src[18:26]))
	b.LastOffset = int64(binary.BigEndian.Uint64(src[26:34]))
	b.DescriptorsCRC = binary.BigEndian.Uint32(src[34:38])
	want := binary.BigEndian.Uint32(src[38:42])
	got := storage.CRC32C(src[0:38])
	if b.Magic != BatchCommitMagic {
		return fmt.Errorf("storage: bad batchcommit magic 0x%x", b.Magic)
	}
	if got != want {
		return fmt.Errorf("storage: batchcommit crc mismatch")
	}
	return nil
}

// EncodeDescriptors serializes batch descriptors and returns their CRC.
// The descriptors themselves live in the committed-delta overlay / are
// re-derived from the segment; the CRC binds the batch to its records.
func EncodeDescriptors(dst []byte, descs []BatchDescriptor) (uint32, error) {
	for i, d := range descs {
		base := i * BatchDescriptorSize
		if base+BatchDescriptorSize > len(dst) {
			return 0, fmt.Errorf("storage: descriptor buffer too small")
		}
		binary.BigEndian.PutUint64(dst[base:base+8], uint64(d.ExtentID))
		binary.BigEndian.PutUint64(dst[base+8:base+16], uint64(d.Generation))
		binary.BigEndian.PutUint64(dst[base+16:base+24], uint64(d.SegmentID))
		binary.BigEndian.PutUint64(dst[base+24:base+32], uint64(d.Offset))
		binary.BigEndian.PutUint32(dst[base+32:base+36], d.StoredLen)
		binary.BigEndian.PutUint32(dst[base+36:base+40], d.LogicalLen)
		binary.BigEndian.PutUint32(dst[base+40:base+44], d.Checksum)
		dst[base+44] = d.Op
	}
	used := len(descs) * BatchDescriptorSize
	return storage.CRC32C(dst[0:used]), nil
}

// DecodeDescriptors parses batch descriptors from a buffer.
func DecodeDescriptors(src []byte) ([]BatchDescriptor, error) {
	if len(src)%BatchDescriptorSize != 0 {
		return nil, fmt.Errorf("storage: descriptor buffer length not a multiple of entry size")
	}
	out := make([]BatchDescriptor, len(src)/BatchDescriptorSize)
	for i := range out {
		base := i * BatchDescriptorSize
		out[i] = BatchDescriptor{
			ExtentID:   storage.ExtentID(binary.BigEndian.Uint64(src[base : base+8])),
			Generation: storage.Generation(binary.BigEndian.Uint64(src[base+8 : base+16])),
			SegmentID:  storage.SegmentID(binary.BigEndian.Uint64(src[base+16 : base+24])),
			Offset:     int64(binary.BigEndian.Uint64(src[base+24 : base+32])),
			StoredLen:  binary.BigEndian.Uint32(src[base+32 : base+36]),
			LogicalLen: binary.BigEndian.Uint32(src[base+36 : base+40]),
			Checksum:   binary.BigEndian.Uint32(src[base+40 : base+44]),
			Op:         src[base+44],
		}
	}
	return out, nil
}
