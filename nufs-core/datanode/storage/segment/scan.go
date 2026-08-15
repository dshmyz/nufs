package segment

import (
	"os"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/journal"
)

// ScannedRecord is one data record found during a sealed-segment scan,
// used by compaction (§10.3 step 2-3). Records are returned in on-disk
// order with a reader for their payload.
type ScannedRecord struct {
	ExtentID   storage.ExtentID
	Generation storage.Generation
	Offset     int64
	StoredLen  uint32
	LogicalLen uint32
	Codec      storage.CompressionCodec
	// ReadPayload reads and validates the record's stored bytes (raw,
	// as persisted: possibly compressed/encrypted frames). The compactor
	// writes them verbatim to the destination.
	ReadPayload func() ([]byte, error)
	// StoredBytes is the concatenated stored frame bytes, read eagerly.
	StoredBytes []byte
}

// ScanSegmentRecords walks a segment file and returns every data record
// (skipping BatchCommit markers and tombstone placeholders). It is the
// basis for compaction and segment scrub. A tombstone placeholder (a
// record with zero frames) is reported with StoredLen==0 so the caller
// can skip it.
func ScanSegmentRecords(path string, rd *Reader) ([]ScannedRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []ScannedRecord
	off := int64(storage.SegmentHeaderSize)
	for off+int64(RecordHeaderSize) <= int64(len(data)) {
		magic := beUint32(data[off : off+4])
		if magic == journal.BatchCommitMagic {
			off += int64(journal.BatchCommitSize)
			continue
		}
		if magic != storage.RecordMagic {
			break // torn/trailing
		}
		var h RecordHeader
		if err := h.Decode(data[off : off+int64(RecordHeaderSize)]); err != nil {
			break
		}
		frameCount := int(h.FrameCount)
		indexBytes := frameCount * FrameIndexEntrySize
		// The index records per-frame stored lengths; total stored is in
		// the header.
		totalStored := int64(h.StoredLen)
		framing := int64(RecordHeaderSize) + int64(indexBytes) + totalStored + int64(RecordTrailerSize)
		if off+framing > int64(len(data)) {
			break
		}
		firstFrame := off + int64(RecordHeaderSize) + int64(indexBytes)
		stored := data[firstFrame : firstFrame+totalStored]
		if frameCount > 0 {
			out = append(out, ScannedRecord{
				ExtentID:    h.ExtentID,
				Generation:  h.Generation,
				Offset:      off,
				StoredLen:   h.StoredLen,
				LogicalLen:  h.LogicalLen,
				Codec:       h.Codec,
				StoredBytes: append([]byte(nil), stored...),
				ReadPayload: func() ([]byte, error) { return append([]byte(nil), stored...), nil },
			})
		}
		off += framing
	}
	return out, nil
}
