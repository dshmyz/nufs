package journal

import (
	"encoding/binary"
	"fmt"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
)

// CommitLogOp is the operation encoded in a commit-log record (V2.1
// §7.1). These records are length-delimited, checksummed entries in
// active segments alongside data records.
type CommitLogOp uint8

const (
	OpPut CommitLogOp = iota
	OpTombstone
	OpRelocate
	OpSealSegment
	OpIndexSafe
)

func (o CommitLogOp) String() string {
	switch o {
	case OpPut:
		return "put"
	case OpTombstone:
		return "tombstone"
	case OpRelocate:
		return "relocate"
	case OpSealSegment:
		return "seal_segment"
	case OpIndexSafe:
		return "index_safe"
	default:
		return "unknown"
	}
}

// CommitRecord is one entry in the commit log. It carries the extent's
// logical location after the operation applies, plus the batch-commit
// sequence that made it durable (V2.1 §7.1).
type CommitRecord struct {
	// Seq is the stream-local batch-commit sequence covering this op.
	Seq        uint64
	Op         CommitLogOp
	ExtentID   storage.ExtentID
	Generation storage.Generation
	SegmentID  storage.SegmentID
	Offset     int64
	StoredLen  uint32
	LogicalLen uint32
	Checksum   uint32
	// SourceSegment/SourceOffset are set for OpRelocate: the old
	// location the index must still match for the relocation to apply
	// (§10.3 step 6).
	SourceSegment storage.SegmentID
	SourceOffset  int64
	// CRC covers the body.
	CRC uint32
}

// CommitRecordSize is the fixed on-disk size of a CommitRecord body.
const CommitRecordSize = 8 + 1 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 8 + 8 + 4 // 73

// Encode writes the fixed-size body bytes.
func (r *CommitRecord) Encode(dst []byte) error {
	if len(dst) < CommitRecordSize {
		return fmt.Errorf("storage: commit record body too small")
	}
	binary.BigEndian.PutUint64(dst[0:8], r.Seq)
	dst[8] = byte(r.Op)
	binary.BigEndian.PutUint64(dst[9:17], uint64(r.ExtentID))
	binary.BigEndian.PutUint64(dst[17:25], uint64(r.Generation))
	binary.BigEndian.PutUint64(dst[25:33], uint64(r.SegmentID))
	binary.BigEndian.PutUint64(dst[33:41], uint64(r.Offset))
	binary.BigEndian.PutUint32(dst[41:45], r.StoredLen)
	binary.BigEndian.PutUint32(dst[45:49], r.LogicalLen)
	binary.BigEndian.PutUint32(dst[49:53], r.Checksum)
	binary.BigEndian.PutUint64(dst[53:61], uint64(r.SourceSegment))
	binary.BigEndian.PutUint64(dst[61:69], uint64(r.SourceOffset))
	// CRC over [0,69).
	r.CRC = storage.CRC32C(dst[0:69])
	binary.BigEndian.PutUint32(dst[69:73], r.CRC)
	return nil
}

// Decode parses a commit-record body and verifies its CRC.
func (r *CommitRecord) Decode(src []byte) error {
	if len(src) < CommitRecordSize {
		return fmt.Errorf("storage: commit record body too short")
	}
	r.Seq = binary.BigEndian.Uint64(src[0:8])
	r.Op = CommitLogOp(src[8])
	r.ExtentID = storage.ExtentID(binary.BigEndian.Uint64(src[9:17]))
	r.Generation = storage.Generation(binary.BigEndian.Uint64(src[17:25]))
	r.SegmentID = storage.SegmentID(binary.BigEndian.Uint64(src[25:33]))
	r.Offset = int64(binary.BigEndian.Uint64(src[33:41]))
	r.StoredLen = binary.BigEndian.Uint32(src[41:45])
	r.LogicalLen = binary.BigEndian.Uint32(src[45:49])
	r.Checksum = binary.BigEndian.Uint32(src[49:53])
	r.SourceSegment = storage.SegmentID(binary.BigEndian.Uint64(src[53:61]))
	r.SourceOffset = int64(binary.BigEndian.Uint64(src[61:69]))
	want := binary.BigEndian.Uint32(src[69:73])
	got := storage.CRC32C(src[0:69])
	if got != want {
		return fmt.Errorf("storage: commit record crc mismatch")
	}
	return nil
}
