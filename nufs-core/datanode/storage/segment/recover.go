package segment

import (
	"fmt"
	"os"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/journal"
)

// RecoverFromSegmentLog rebuilds the committed-delta overlay from the
// active segment's committed BatchCommit records (V2.1 §7.5 step 4-6).
// Records after the last valid BatchCommit are uncommitted tail data
// and are discarded; the segment file is truncated to the last
// committed offset.
//
// This is the durability authority on recovery: a committed record
// absent from Pebble is replayed here, and a Pebble entry beyond the
// last committed sequence is invalid and removed (the overlay takes
// precedence because reads consult it first).
func RecoverFromSegmentLog(path string, streamID uint8, overlay *Overlay, apply func(CommitDescriptor) error) (*RecoverResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var res RecoverResult
	// Walk the segment: records (header+index+frames+trailer) interleaved
	// with BatchCommit markers.
	off := int64(storage.SegmentHeaderSize)
	lastCommitted := int64(storage.SegmentHeaderSize)
	var pending []CommitDescriptor
	lastSeq := uint64(0)

	for off+int64(RecordHeaderSize) <= int64(len(data)) {
		// Peek the magic to decide record vs BatchCommit.
		magic := uint32(0)
		if off+4 <= int64(len(data)) {
			magic = beUint32(data[off : off+4])
		}
		if magic == journal.BatchCommitMagic {
			// Parse the BatchCommit.
			if off+int64(journal.BatchCommitSize) > int64(len(data)) {
				// Torn BatchCommit: discard from here on.
				break
			}
			var bc journal.BatchCommit
			if err := bc.Decode(data[off : off+int64(journal.BatchCommitSize)]); err != nil {
				break // invalid commit: stop (records after are uncommitted)
			}
			// The commit covers pending descriptors with seq <= bc.Seq.
			for _, d := range pending {
				if err := apply(d); err != nil {
					return nil, err
				}
				res.Applied++
			}
			pending = pending[:0]
			lastSeq = bc.Seq
			lastCommitted = off + int64(journal.BatchCommitSize)
			off += int64(journal.BatchCommitSize)
			res.Commits++
			continue
		}

		// An INDEX_SAFE record is a CommitRecord with Op==OpIndexSafe
		// carrying the safe sequence (§7.1). Detect it before treating
		// the bytes as a data record: decode the CommitRecord and check
		// its op. The op byte is at offset 8 of the CommitRecord body.
		if off+int64(journal.CommitRecordSize) <= int64(len(data)) {
			var cr journal.CommitRecord
			if err := cr.Decode(data[off : off+int64(journal.CommitRecordSize)]); err == nil &&
				cr.Op == journal.OpIndexSafe {
				// Safe-sequence marker: record it and continue past it.
				res.SafeSeq = cr.Seq
				lastCommitted = off + int64(journal.CommitRecordSize)
				off += int64(journal.CommitRecordSize)
				continue
			}
		}

		// Otherwise a record: read its header, compute framing, advance.
		var h RecordHeader
		if err := h.Decode(data[off : off+int64(RecordHeaderSize)]); err != nil {
			break // torn record: discard tail
		}
		frameSize := h.EffectiveFrameSize()
		framing := int64(RecordFraming(h.StoredLen, frameSize, int(h.FrameCount)))
		if off+framing > int64(len(data)) {
			break // torn record: discard tail
		}
		if h.FrameCount == 0 {
			// Tombstone marker (no payload frames): consume the marker.
			pending = append(pending, CommitDescriptor{
				ExtentID:   h.ExtentID,
				Generation: h.Generation,
				SegmentID:  segIDFromPath(path),
				Offset:     off,
				StoredLen:  h.StoredLen,
				LogicalLen: h.LogicalLen,
			})
		} else {
			pending = append(pending, CommitDescriptor{
				ExtentID:   h.ExtentID,
				Generation: h.Generation,
				SegmentID:  segIDFromPath(path),
				Offset:     off,
				StoredLen:  h.StoredLen,
				LogicalLen: h.LogicalLen,
			})
		}
		off += framing
	}

	res.LastCommittedOffset = lastCommitted
	res.LastSeq = lastSeq
	res.TrailingBytes = int64(len(data)) - lastCommitted
	return &res, nil
}

// RecoverResult reports what segment-log recovery found.
type RecoverResult struct {
	Commits            int
	Applied            int
	LastSeq            uint64
	LastCommittedOffset int64
	TrailingBytes      int64
	// SafeSeq is the last INDEX_SAFE sequence found in the log (§7.4).
	SafeSeq uint64
}

// CommitDescriptor is a committed extent location discovered during
// segment-log replay.
type CommitDescriptor struct {
	ExtentID   storage.ExtentID
	Generation storage.Generation
	SegmentID  storage.SegmentID
	Offset     int64
	StoredLen  uint32
	LogicalLen uint32
}

// segIDFromPath extracts the segment ID from a `{id}.seg` filename.
func segIDFromPath(path string) storage.SegmentID {
	var id uint64
	base := path
	for i := len(base) - 1; i >= 0; i-- {
		if base[i] == '/' {
			base = base[i+1:]
			break
		}
	}
	fmt.Sscanf(base, "%d.seg", &id)
	return storage.SegmentID(id)
}

func beUint32(b []byte) uint32 {
	if len(b) < 4 {
		return 0
	}
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}
