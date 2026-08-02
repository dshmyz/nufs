package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/journal"
)

// ErrRecoveryBudgetExceeded is retained as a package alias for Task 2A
// callers. The canonical sentinel lives in storage so recovery orchestration
// and segment parsing agree without an import cycle.
var ErrRecoveryBudgetExceeded = storage.ErrRecoveryBudgetExceeded

var errUnsupportedRecoveryFormat = errors.New("storage: unsupported recovery format")

// recoveryMaxRecords bounds both parser work and pending descriptor memory
// even when callers leave RecoverOptions.MaxRecords unset.
const recoveryMaxRecords = storage.MaxRecoveryRecords

// RecoverOptions bounds and identifies active-segment recovery.
type RecoverOptions struct {
	StreamID   uint8
	SafeOffset int64
	SafeSeq    uint64
	// RequireSafeMarker requires SafeOffset to be immediately after the
	// INDEX_SAFE marker matching SafeSeq. Store.New enables it only for the
	// index-owned persisted checkpoint; ordinary parser callers may resume
	// after any validated BatchCommit boundary.
	RequireSafeMarker bool
	MaxRecords        uint64
	MaxReplayBytes    int64
	MaxTrailingBytes  int64
	// Clock and Deadline form a deterministic elapsed-time seam. A nil Clock
	// uses wall time; zero Deadline defaults to storage.RecoveryBudget.
	Clock     func() time.Time
	Deadline  time.Time
	StartedAt time.Time
}

// RecoverFromSegmentLog streams an active segment with positional reads.
// It validates each record and BatchCommit before replaying descriptors and
// removes only the invalid or uncommitted tail after the last valid boundary.
func RecoverFromSegmentLog(path string, opts RecoverOptions, apply func(CommitDescriptor) error) (*RecoverResult, error) {
	startedAt := opts.StartedAt
	if startedAt.IsZero() {
		startedAt = recoveryNow(opts)
	}
	if opts.Deadline.IsZero() {
		opts.Deadline = startedAt.Add(storage.RecoveryBudget)
	}
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

	var checkpointSeq uint64
	if start > int64(storage.SegmentHeaderSize) {
		var isIndexSafe bool
		var err error
		checkpointSeq, isIndexSafe, err = validateSafeRecoveryStart(f, start, opts.StreamID)
		if err != nil {
			return nil, fmt.Errorf("storage: safe offset %d is not an encoded boundary: %w", opts.SafeOffset, err)
		}
		if opts.RequireSafeMarker && (!isIndexSafe || checkpointSeq != opts.SafeSeq) {
			return nil, fmt.Errorf("storage: safe offset %d does not match index-safe checkpoint", opts.SafeOffset)
		}
	}
	state := recoveryState{
		path: path, opts: opts, size: size, lastValid: start,
		lastCommittedSeq: checkpointSeq, safeSeq: opts.SafeSeq, recordLimit: recoveryRecordLimit(opts),
		safeOffset: start, startedAt: startedAt,
	}
	off := start
	for off < size {
		if state.deadlineExceeded() {
			return state.result(false), ErrRecoveryBudgetExceeded
		}
		next, commit, indexSafe, err := state.parseEntry(f, off, apply)
		if err != nil {
			if errors.Is(err, ErrRecoveryBudgetExceeded) {
				return state.result(false), err
			}
			if errors.Is(err, errUnsupportedRecoveryFormat) {
				return nil, err
			}
			break
		}
		off = next
		if commit || indexSafe {
			state.lastValid = off
		}
	}
	trailing := size - state.lastValid
	if opts.MaxTrailingBytes > 0 && trailing > opts.MaxTrailingBytes {
		return state.result(false), ErrRecoveryBudgetExceeded
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
	if state.deadlineExceeded() {
		return state.result(false), ErrRecoveryBudgetExceeded
	}
	return state.result(true), nil
}

type recoveryState struct {
	path         string
	opts         RecoverOptions
	size         int64
	pending      []CommitDescriptor
	pendingBytes int64
	lastValid    int64
	safeOffset   int64
	// lastCommittedSeq is structural validation state. It advances for every
	// valid BatchCommit, including commits suppressed by SafeSeq. It is seeded
	// only from a proven SafeOffset checkpoint, never from SafeSeq itself.
	lastCommittedSeq uint64
	safeSeq          uint64
	commits          int
	applied          int
	records          uint64
	replayBytes      int64
	recordLimit      uint64
	startedAt        time.Time
}

func recoveryRecordLimit(opts RecoverOptions) uint64 {
	if opts.MaxRecords == 0 || opts.MaxRecords > recoveryMaxRecords {
		return recoveryMaxRecords
	}
	return opts.MaxRecords
}

func recoveryNow(opts RecoverOptions) time.Time {
	if opts.Clock != nil {
		return opts.Clock()
	}
	return time.Now()
}

func (s *recoveryState) deadlineExceeded() bool {
	return !s.opts.Deadline.IsZero() && recoveryNow(s.opts).After(s.opts.Deadline)
}

func (s *recoveryState) result(dataReady bool) *RecoverResult {
	return &RecoverResult{
		Commits:             s.commits,
		Applied:             s.applied,
		LastSeq:             s.lastCommittedSeq,
		SafeOffset:          s.safeOffset,
		SafeSeq:             s.safeSeq,
		ReplayBytes:         s.replayBytes,
		LastCommittedOffset: s.lastValid,
		TrailingBytes:       s.size - s.lastValid,
		Duration:            recoveryNow(s.opts).Sub(s.startedAt),
		DataReady:           dataReady,
	}
}

// validateSafeRecoveryStart proves a caller-supplied checkpoint by decoding
// the fixed-size commit structure immediately before it. SafeOffset is a
// checkpoint published by our format only after a BatchCommit or INDEX_SAFE;
// the prefix before it is trusted and deliberately not replayed. In
// particular, a plausible RecordHeader at SafeOffset is not proof: identical
// bytes can occur in a payload. This reads only a candidate fixed metadata
// structure, never scans or loads payload frames.
func validateSafeRecoveryStart(f *os.File, off int64, streamID uint8) (uint64, bool, error) {
	commitOff := off - int64(journal.BatchCommitSize)
	if commitOff >= int64(storage.SegmentHeaderSize) {
		var buf [journal.BatchCommitSize]byte
		if err := readFullAt(f, buf[:], commitOff); err == nil {
			var commit journal.BatchCommit
			if err := commit.Decode(buf[:]); err == nil {
				if commit.Version != storage.FormatVersion {
					return 0, false, fmt.Errorf("storage: unsupported checkpoint batchcommit version %d", commit.Version)
				}
				if commit.StreamID != streamID || commit.Seq == 0 || commit.RecordCount == 0 ||
					commit.FirstOffset < int64(storage.SegmentHeaderSize) ||
					commit.FirstOffset >= commitOff || commit.LastOffset != commitOff {
					return 0, false, fmt.Errorf("storage: invalid checkpoint batchcommit")
				}
				return commit.Seq, false, nil
			}
		}
	}

	markerOff := off - int64(journal.CommitRecordSize)
	if markerOff >= int64(storage.SegmentHeaderSize) {
		var buf [journal.CommitRecordSize]byte
		if err := readFullAt(f, buf[:], markerOff); err == nil {
			var marker journal.CommitRecord
			if err := marker.Decode(buf[:]); err == nil && marker.Op == journal.OpIndexSafe && marker.Seq != 0 {
				return marker.Seq, true, nil
			}
		}
	}
	return 0, false, fmt.Errorf("storage: safe offset is not after a committed boundary")
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
				if err := s.addReplayBytes(int64(journal.CommitRecordSize)); err != nil {
					return 0, false, false, err
				}
				return off + int64(journal.CommitRecordSize), false, true, nil
			}
		}
	}
	return s.parseRecord(f, off)
}

func (s *recoveryState) parseRecord(f *os.File, off int64) (int64, bool, bool, error) {
	var prefix [5]byte
	if err := readFullAt(f, prefix[:], off); err != nil {
		return 0, false, false, err
	}
	if beUint32(prefix[:4]) != storage.RecordMagic {
		return 0, false, false, fmt.Errorf("storage: bad record magic 0x%x", beUint32(prefix[:4]))
	}
	if prefix[4] == 2 {
		if err := validateV2RecordHeader(f, off); err != nil {
			return 0, false, false, err
		}
		return 0, false, false, fmt.Errorf("%w: record version 2", errUnsupportedRecoveryFormat)
	}
	var headerBuf [RecordHeaderSize]byte
	if err := readFullAt(f, headerBuf[:], off); err != nil {
		return 0, false, false, err
	}
	var header RecordHeader
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
	if header.Op == RecordDelete && (header.StoredLen != 0 || header.LogicalLen != 0 || header.FrameCount != 0) {
		return 0, false, false, fmt.Errorf("storage: delete record carries payload")
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
	if s.records == s.recordLimit {
		return 0, false, false, ErrRecoveryBudgetExceeded
	}
	s.records++
	s.pendingBytes += framing
	s.pending = append(s.pending, CommitDescriptor{
		ExtentID: header.ExtentID, Generation: header.Generation, SegmentID: segIDFromPath(s.path),
		Offset: off, StoredLen: header.StoredLen, LogicalLen: header.LogicalLen,
		Checksum: header.PayloadChecksum, Op: header.Op,
	})
	return off + framing, false, false, nil
}

// validateV2RecordHeader recognizes only a genuine legacy V2 header. A
// corrupt tail whose fifth byte happens to be 2 is therefore truncated like
// any other torn record rather than being misreported as an unsupported V2
// segment.
func validateV2RecordHeader(f *os.File, off int64) error {
	const v2RecordHeaderSize = 50
	var buf [v2RecordHeaderSize]byte
	if err := readFullAt(f, buf[:], off); err != nil {
		return err
	}
	if beUint32(buf[0:4]) != storage.RecordMagic || buf[4] != 2 {
		return fmt.Errorf("storage: invalid V2 record header")
	}
	wantCRC := binary.BigEndian.Uint32(buf[42:46])
	if gotCRC := crc32.ChecksumIEEE(buf[0:42]); gotCRC != wantCRC {
		return fmt.Errorf("storage: invalid V2 record header crc")
	}
	return nil
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
	if commit.StreamID != s.opts.StreamID || commit.Seq <= s.lastCommittedSeq || commit.RecordCount != uint32(len(s.pending)) {
		return 0, false, false, fmt.Errorf("storage: invalid batchcommit fields")
	}
	if len(s.pending) == 0 || commit.FirstOffset != s.pending[0].Offset || commit.LastOffset != off {
		return 0, false, false, fmt.Errorf("storage: invalid batchcommit offsets")
	}
	for range s.pending {
		if s.deadlineExceeded() {
			return 0, false, false, ErrRecoveryBudgetExceeded
		}
	}
	batchBytes := s.pendingBytes + int64(journal.BatchCommitSize)
	if commit.DescriptorsCRC != pendingDescriptorsCRC(s.pending) {
		return 0, false, false, fmt.Errorf("storage: batch descriptor crc mismatch")
	}
	if commit.Seq > s.opts.SafeSeq {
		if err := s.addReplayBytes(batchBytes); err != nil {
			return 0, false, false, ErrRecoveryBudgetExceeded
		}
		if apply != nil {
			for _, d := range s.pending {
				if s.deadlineExceeded() {
					return 0, false, false, ErrRecoveryBudgetExceeded
				}
				if err := apply(d); err != nil {
					return 0, false, false, err
				}
				s.applied++
			}
		}
	}
	s.pending = s.pending[:0]
	s.pendingBytes = 0
	s.lastCommittedSeq = commit.Seq
	s.commits++
	return off + int64(journal.BatchCommitSize), true, false, nil
}

// addReplayBytes is the single accounting rule for unindexed committed
// on-disk bytes: each committed record's complete framing, its BatchCommit,
// and any committed INDEX_SAFE metadata after the persisted checkpoint.
func (s *recoveryState) addReplayBytes(n int64) error {
	if n < 0 || (s.opts.MaxReplayBytes > 0 && n > s.opts.MaxReplayBytes-s.replayBytes) {
		return ErrRecoveryBudgetExceeded
	}
	s.replayBytes += n
	return nil
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
		buf[44] = byte(d.Op)
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
	SafeOffset          int64
	LastCommittedOffset int64
	TrailingBytes       int64
	SafeSeq             uint64
	ReplayBytes         int64
	Duration            time.Duration
	DataReady           bool
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
	Op         RecordOp
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
