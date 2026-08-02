package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/journal"
)

// ErrRecoveryBudgetExceeded reports recovery work beyond its caller-supplied
// bound. The file is left unchanged so an operator can choose a safe policy.
var ErrRecoveryBudgetExceeded = errors.New("storage: recovery budget exceeded")

var errUnsupportedRecoveryFormat = errors.New("storage: unsupported recovery format")

// RecoverOptions bounds and identifies active-segment recovery.
type RecoverOptions struct {
	StreamID         uint8
	SafeOffset       int64
	SafeSeq          uint64
	MaxRecords       uint64
	MaxReplayBytes   int64
	MaxTrailingBytes int64
}

// RecoverFromSegmentLog streams an active segment with positional reads.
// It validates each record and BatchCommit before replaying descriptors and
// removes only the invalid or uncommitted tail after the last valid boundary.
func RecoverFromSegmentLog(path string, opts RecoverOptions, apply func(CommitDescriptor) error) (*RecoverResult, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size < int64(storage.SegmentHeaderSize) {
		return nil, fmt.Errorf("storage: segment shorter than header")
	}

	var headerBuf [storage.SegmentHeaderSize]byte
	if err := readFullAt(f, headerBuf[:], 0); err != nil {
		return nil, err
	}
	var segmentHeader SegmentHeader
	if err := segmentHeader.Decode(headerBuf[:]); err != nil {
		return nil, err
	}

	if opts.SafeOffset < 0 {
		return nil, fmt.Errorf("storage: safe offset %d outside segment", opts.SafeOffset)
	}
	start := opts.SafeOffset
	if start < int64(storage.SegmentHeaderSize) {
		start = int64(storage.SegmentHeaderSize)
	}
	if start > size {
		return nil, fmt.Errorf("storage: safe offset %d outside segment size %d", opts.SafeOffset, size)
	}

	state := recoveryState{
		path: path, opts: opts, size: size, lastValid: int64(storage.SegmentHeaderSize),
		lastSeq: 0, safeSeq: opts.SafeSeq,
	}
	off := int64(storage.SegmentHeaderSize)
	for off < size {
		entryApply := apply
		if off < start {
			entryApply = nil
		}
		next, commit, indexSafe, err := state.parseEntry(f, off, entryApply)
		if err != nil {
			if errors.Is(err, ErrRecoveryBudgetExceeded) {
				return nil, err
			}
			if errors.Is(err, errUnsupportedRecoveryFormat) {
				return nil, err
			}
			if off < start {
				return nil, fmt.Errorf("storage: safe offset %d is not an encoded boundary: %w", opts.SafeOffset, err)
			}
			break
		}
		if next > start && off < start {
			return nil, fmt.Errorf("storage: safe offset %d is inside encoded entry [%d,%d)", opts.SafeOffset, off, next)
		}
		if off == start && off > int64(storage.SegmentHeaderSize) {
			state.lastValid = start
		}
		off = next
		if commit || indexSafe {
			state.lastValid = off
		}
	}
	if off != start && start != int64(storage.SegmentHeaderSize) && off < start {
		return nil, fmt.Errorf("storage: safe offset %d is not an encoded boundary", opts.SafeOffset)
	}

	trailing := size - state.lastValid
	if opts.MaxTrailingBytes > 0 && trailing > opts.MaxTrailingBytes {
		return nil, ErrRecoveryBudgetExceeded
	}
	if trailing > 0 {
		if state.lastValid < start {
			return nil, fmt.Errorf("storage: refusing to truncate below safe offset %d", opts.SafeOffset)
		}
		if err := f.Truncate(state.lastValid); err != nil {
			return nil, err
		}
		if err := f.Sync(); err != nil {
			return nil, err
		}
	}
	return &RecoverResult{
		Commits: state.commits, Applied: state.applied, LastSeq: state.lastSeq,
		SafeSeq: state.safeSeq, LastCommittedOffset: state.lastValid, TrailingBytes: trailing,
	}, nil
}

type recoveryState struct {
	path        string
	opts        RecoverOptions
	size        int64
	pending     []CommitDescriptor
	lastValid   int64
	lastSeq     uint64
	safeSeq     uint64
	commits     int
	applied     int
	records     uint64
	replayBytes int64
}

// parseEntry validates one record, commit, or INDEX_SAFE marker. It never
// reads payload frames; only fixed-size metadata and the current frame index.
func (s *recoveryState) parseEntry(f *os.File, off int64, apply func(CommitDescriptor) error) (int64, bool, bool, error) {
	var magicBuf [4]byte
	if err := readFullAt(f, magicBuf[:], off); err != nil {
		return 0, false, false, err
	}
	magic := beUint32(magicBuf[:])
	if magic == journal.BatchCommitMagic {
		return s.parseCommit(f, off, apply)
	}

	// INDEX_SAFE does not have a magic prefix. Decode it only after excluding
	// a record header, whose magic makes it unambiguous in normal operation.
	if magic != storage.RecordMagic && off+int64(journal.CommitRecordSize) <= s.size {
		var buf [journal.CommitRecordSize]byte
		if err := readFullAt(f, buf[:], off); err == nil {
			var record journal.CommitRecord
			if err := record.Decode(buf[:]); err == nil && record.Op == journal.OpIndexSafe {
				if len(s.pending) != 0 {
					return 0, false, false, fmt.Errorf("storage: index safe follows uncommitted records")
				}
				if record.Seq > s.safeSeq {
					s.safeSeq = record.Seq
				}
				return off + int64(journal.CommitRecordSize), false, true, nil
			}
		}
	}
	return s.parseRecord(f, off)
}

func (s *recoveryState) parseRecord(f *os.File, off int64) (int64, bool, bool, error) {
	var headerBuf [RecordHeaderSize]byte
	if err := readFullAt(f, headerBuf[:], off); err != nil {
		return 0, false, false, err
	}
	var header RecordHeader
	if headerBuf[4] == 2 {
		return 0, false, false, fmt.Errorf("%w: record version %d", errUnsupportedRecoveryFormat, headerBuf[4])
	}
	if err := header.Decode(headerBuf[:]); err != nil {
		return 0, false, false, err
	}
	indexBytes := int64(header.FrameCount) * FrameIndexEntrySize
	const maxFrameIndexBytes = 1 << 20
	if indexBytes > maxFrameIndexBytes || indexBytes < 0 {
		return 0, false, false, fmt.Errorf("storage: frame index too large")
	}
	if header.FrameCount == 0 && header.StoredLen != 0 {
		return 0, false, false, fmt.Errorf("storage: zero-frame record has stored bytes")
	}
	framing := int64(RecordHeaderSize) + indexBytes + int64(header.StoredLen) + int64(RecordTrailerSize)
	if framing < int64(RecordHeaderSize) || off > s.size-framing {
		return 0, false, false, io.ErrUnexpectedEOF
	}
	index := make([]byte, indexBytes)
	if err := readFullAt(f, index, off+int64(RecordHeaderSize)); err != nil {
		return 0, false, false, err
	}
	if crc32.ChecksumIEEE(index) != header.FrameIndexCRC {
		return 0, false, false, fmt.Errorf("storage: frame index crc mismatch")
	}
	var stored uint64
	var expectedOffset uint64
	for i := 0; i < int(header.FrameCount); i++ {
		base := i * FrameIndexEntrySize
		entryOffset := uint64(beUint32(index[base : base+4]))
		entryLen := uint64(beUint32(index[base+4 : base+8]))
		if entryOffset != expectedOffset || entryLen > uint64(header.StoredLen)-stored {
			return 0, false, false, fmt.Errorf("storage: invalid frame index bounds")
		}
		stored += entryLen
		expectedOffset += entryLen
	}
	if stored != uint64(header.StoredLen) {
		return 0, false, false, fmt.Errorf("storage: frame index stored length mismatch")
	}
	trailerOff := off + int64(RecordHeaderSize) + indexBytes + int64(header.StoredLen)
	var trailerBuf [RecordTrailerSize]byte
	if err := readFullAt(f, trailerBuf[:], trailerOff); err != nil {
		return 0, false, false, err
	}
	var trailer RecordTrailer
	if err := trailer.Decode(trailerBuf[:]); err != nil || trailer.FramingLen != uint32(framing) {
		if err != nil {
			return 0, false, false, err
		}
		return 0, false, false, fmt.Errorf("storage: record trailer framing mismatch")
	}
	if s.opts.MaxRecords > 0 && s.records == s.opts.MaxRecords {
		return 0, false, false, ErrRecoveryBudgetExceeded
	}
	s.records++
	s.pending = append(s.pending, CommitDescriptor{
		ExtentID: header.ExtentID, Generation: header.Generation, SegmentID: segIDFromPath(s.path),
		Offset: off, StoredLen: header.StoredLen, LogicalLen: header.LogicalLen,
		Checksum: header.PayloadChecksum,
	})
	return off + framing, false, false, nil
}

func (s *recoveryState) parseCommit(f *os.File, off int64, apply func(CommitDescriptor) error) (int64, bool, bool, error) {
	var buf [journal.BatchCommitSize]byte
	if err := readFullAt(f, buf[:], off); err != nil {
		return 0, false, false, err
	}
	var commit journal.BatchCommit
	if err := commit.Decode(buf[:]); err != nil {
		return 0, false, false, err
	}
	if commit.Version == 2 {
		return 0, false, false, fmt.Errorf("%w: batchcommit version %d", errUnsupportedRecoveryFormat, commit.Version)
	}
	if commit.Version != storage.FormatVersion {
		return 0, false, false, fmt.Errorf("storage: unsupported batchcommit version %d", commit.Version)
	}
	if commit.StreamID != s.opts.StreamID || commit.Seq <= s.lastSeq || commit.RecordCount != uint32(len(s.pending)) {
		return 0, false, false, fmt.Errorf("storage: invalid batchcommit fields")
	}
	if len(s.pending) == 0 || commit.FirstOffset != s.pending[0].Offset || commit.LastOffset != off {
		return 0, false, false, fmt.Errorf("storage: invalid batchcommit offsets")
	}
	var batchBytes int64
	for _, d := range s.pending {
		batchBytes += int64(d.StoredLen)
	}
	if commit.DescriptorsCRC != pendingDescriptorsCRC(s.pending) {
		return 0, false, false, fmt.Errorf("storage: batch descriptor crc mismatch")
	}
	if commit.Seq > s.opts.SafeSeq {
		if s.opts.MaxReplayBytes > 0 && batchBytes > s.opts.MaxReplayBytes-s.replayBytes {
			return 0, false, false, ErrRecoveryBudgetExceeded
		}
		if apply != nil {
			for _, d := range s.pending {
				if err := apply(d); err != nil {
					return 0, false, false, err
				}
				s.applied++
			}
		}
		s.replayBytes += batchBytes
	}
	s.pending = s.pending[:0]
	s.lastSeq = commit.Seq
	s.commits++
	return off + int64(journal.BatchCommitSize), true, false, nil
}

func pendingDescriptorsCRC(pending []CommitDescriptor) uint32 {
	h := crc32.NewIEEE()
	var buf [journal.BatchDescriptorSize]byte
	for _, d := range pending {
		binary.BigEndian.PutUint64(buf[0:8], uint64(d.ExtentID))
		binary.BigEndian.PutUint64(buf[8:16], uint64(d.Generation))
		binary.BigEndian.PutUint64(buf[16:24], uint64(d.SegmentID))
		binary.BigEndian.PutUint64(buf[24:32], uint64(d.Offset))
		binary.BigEndian.PutUint32(buf[32:36], d.StoredLen)
		binary.BigEndian.PutUint32(buf[36:40], d.LogicalLen)
		binary.BigEndian.PutUint32(buf[40:44], d.Checksum)
		_, _ = h.Write(buf[:])
	}
	return h.Sum32()
}

func descriptorCRC(descs []journal.BatchDescriptor) uint32 {
	buf := make([]byte, len(descs)*journal.BatchDescriptorSize)
	crc, err := journal.EncodeDescriptors(buf, descs)
	if err != nil {
		panic(err)
	}
	return crc
}

func readFullAt(f *os.File, buf []byte, off int64) error {
	_, err := f.ReadAt(buf, off)
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// RecoverResult reports what segment-log recovery found.
type RecoverResult struct {
	Commits             int
	Applied             int
	LastSeq             uint64
	LastCommittedOffset int64
	TrailingBytes       int64
	SafeSeq             uint64
}

// CommitDescriptor is a committed extent location discovered during replay.
type CommitDescriptor struct {
	ExtentID   storage.ExtentID
	Generation storage.Generation
	SegmentID  storage.SegmentID
	Offset     int64
	StoredLen  uint32
	LogicalLen uint32
	Checksum   uint32
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
