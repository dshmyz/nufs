package segment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/encryption"
	"github.com/example/dfs/datanode/storage/index"
	"github.com/example/dfs/datanode/storage/journal"
)

// Store implements storage.Store for a single commit stream on one disk
// (V2.1 §6.4). It owns one active segment and its single-fsync group
// commit: append records + BatchCommit, one fdatasync, then update the
// committed-delta overlay and acknowledge. Pebble is a derived index
// applied asynchronously after the durability point.
//
// V2.1 durability model (§6.1):
//
//  1. reserve offset in active segment
//  2. append record header + frame index + frames + trailer
//  3. append BatchCommit for the group
//  4. one fdatasync on the segment
//  5. apply committed locations to the bounded in-memory delta overlay
//  6. return DurableReceipt for every request in the commit
//  7. apply mutations asynchronously to Pebble
//
// BatchCommit is the foreground durability point. Pebble may lag the
// committed sequence but never lead it; recovery replays committed
// segment records, and a Pebble entry beyond the last committed
// sequence is invalid and removed.
type Store struct {
	// mu serializes active-segment appends with the entire checkpoint
	// transaction. While mu is held by flush, no batch may append a durable
	// record or publish its overlay mutation. Consequently a batch is either
	// included in the drained overlay and INDEX_SAFE boundary, or it appends
	// after that marker and is replayed from the sidecar offset on restart.
	//
	// Lock order when both locks are needed is mu -> publicationMu. The
	// checkpoint path takes only mu; publication never takes mu while holding
	// publicationMu. Overlay has its own internal lock, acquired only after
	// mu on serving paths.
	mu         sync.Mutex
	alloc      *Allocator
	writer     *Writer
	writerPath string

	// index is the derived location index (Pebble). The committed-delta
	// overlay is the read authority until async apply catches up.
	index   *index.Index
	overlay *Overlay

	// streamID distinguishes small (0) from data (1) commit streams.
	streamID uint8
	// streamSeq is the stream-local commit sequence (monotonic).
	streamSeq uint64
	dataReady atomic.Bool
	// recoveryResult is immutable after New succeeds and records actual
	// parser replay/truncation work rather than checkpoint validation alone.
	recoveryResult     RecoverResult
	recoveryClock      func() time.Time
	recoveryStartedAt  time.Time
	recoveryDeadline   time.Time
	recoveryPolicy     recoveryStartupPolicy
	recoveryCheckpoint *index.RecoveryCheckpoint

	nextSeg uint64
	segDir  string
	// segmentSize is retained across seals so a fresh segment restores the
	// configured capacity rather than inheriting the old segment's tail.
	segmentSize int64

	// Async Pebble apply queue.
	applyCh chan []index.Mutation
	stopCh  chan struct{}
	applyWG sync.WaitGroup
	// disableAsyncApply is a package-private crash-test seam.
	disableAsyncApply bool

	// group commits writes to the active segment (§6.4): the coordinator
	// collects requests, appends records + one BatchCommit, syncs once,
	// then completes every request.
	group *groupCommitCoordinator

	// syncCalls counts writer.Sync() invocations (observability + tests
	// proving group commit shares one barrier per batch).
	syncCalls atomic.Int64

	// Safe-sequence tracking for INDEX_SAFE flush (§7.4).
	// flushSeq is the highest committed stream sequence (mutation
	// counter for the flush trigger). flushInterval bounds how long the
	// index can lag the committed log.
	flushSeq       atomic.Uint64
	flushMutations atomic.Int64
	// publicationMu orders completed overlay publications. A batch may wake
	// several writers concurrently; SafeSeq may advance only once every
	// mutation in the prior contiguous BatchCommit reached the overlay.
	publicationMu sync.Mutex
	publishedSeq  uint64
	readyBatches  map[uint64]*recoveryPublishBatch
	flushInterval time.Duration
	// beforeCommitLock, flushCheckpointHook, and flushApply are package-private
	// test seams for proving the checkpoint exclusion and injecting a failed
	// index apply. Production leaves them nil.
	beforeCommitLock    func()
	flushCheckpointHook func()
	flushApply          func([]index.Mutation) error
	// safeSeq is the last sequence covered by an INDEX_SAFE record.
	safeSeq atomic.Uint64
	// flushLoopDone signals the background flush loop.
	flushDone chan struct{}
	flushWG   sync.WaitGroup

	// enc is the record encryption registry (nil = plaintext).
	enc *encryption.KeyRegistry

	// Caches (§8).
	locCache *LocationCache          // extent → index.Value
	segCache *SegmentDescriptorCache // segment path → *Reader

	// changeJournal is the async change journal (§12). nil when not
	// configured (production deployments should always provide one).
	changeJournal *journal.ChangeJournal

	// closeOnce runs the shutdown body exactly once. closeDone is closed
	// when that body returns, so concurrent callers wait for the real
	// shutdown instead of racing past it, and closeErr is the single
	// terminal error every caller observes.
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error

	// closing is set before shutdown tears anything down, so new requests
	// are rejected with ErrStoreClosed instead of reaching a closed index.
	// gate is held for read by every request that touches the index or the
	// writer; shutdown takes it for write, which waits for in-flight
	// requests to drain. Without it a request that passed the closing check
	// could still reach Pebble after index.Close() and panic the process.
	closing atomic.Bool
	gate    sync.RWMutex

	faults storage.FaultHook
}

// Config configures a Store.
type Config struct {
	Dir          string // disk root
	SegmentSize  int64  // 0 = DefaultDataSegmentSize
	IndexMemSize uint64
	UseMemIndex  bool // in-memory index (tests)
	Faults       storage.FaultHook
	// StreamID is the commit-stream ID (0=small, 1=data).
	StreamID uint8
	// Enc is the record encryption registry (nil = plaintext).
	Enc *encryption.KeyRegistry
	// FlushInterval bounds how long the index may lag the committed
	// log before an INDEX_SAFE flush (§7.4 flush_max_interval: 2s).
	FlushInterval time.Duration
	// FlushMaxMutations is the committed-mutation flush trigger
	// (§7.4 flush_max_committed_records: 100000).
	FlushMaxMutations int64
	// LocationCacheSize sets the location cache entry count (0 = default 1M).
	LocationCacheSize int
	// SegCacheSize sets the segment descriptor cache size (0 = default 4096).
	SegCacheSize int
	// ChangeJournal is the async change journal for out-of-band events
	// (§12). Deployments that do not wire a journal suppress event
	// emission but still function correctly.
	ChangeJournal *journal.ChangeJournal
	// recoveryClock supplies startup time for deterministic deadline tests.
	// Production callers cannot override the wall clock or 30-second budget.
	recoveryClock func() time.Time
	// recoveryPolicy is a package-private test seam. Production always uses
	// the exact storage recovery limits and 30-second startup budget.
	recoveryPolicy recoveryStartupPolicy
	// disableAsyncApply is a package-private crash-test seam. Production
	// always applies committed overlay mutations asynchronously.
	disableAsyncApply bool
	// flushCheckpointHook pauses a flush while its checkpoint transaction
	// holds the segment lock. It is only for deterministic race tests.
	flushCheckpointHook func()
	// flushApply replaces the synchronous checkpoint index apply in tests.
	// Production always uses applyNow.
	flushApply func([]index.Mutation) error
}

type recoveryStartupPolicy struct {
	maxRecords       uint64
	maxReplayBytes   int64
	maxTrailingBytes int64
	budget           time.Duration
}

func (p recoveryStartupPolicy) withProductionDefaults() recoveryStartupPolicy {
	if p.maxRecords == 0 {
		p.maxRecords = storage.MaxRecoveryRecords
	}
	if p.maxReplayBytes == 0 {
		p.maxReplayBytes = storage.MaxRecoveryReplayBytes
	}
	if p.maxTrailingBytes == 0 {
		p.maxTrailingBytes = storage.MaxRecoveryTrailingBytes
	}
	if p.budget == 0 {
		p.budget = storage.RecoveryBudget
	}
	return p
}

// New opens (creating if needed) a Store for one commit stream.
//
// On restart it recovers: Pebble reopens (persisted), and committed
// segment records after the safe sequence are replayed into the overlay
// (recovery module). Segment IDs are seeded past surviving files.
func New(cfg Config) (*Store, error) {
	policy := cfg.recoveryPolicy.withProductionDefaults()
	recoveryClock := cfg.recoveryClock
	if recoveryClock == nil {
		recoveryClock = time.Now
	}
	recoveryStartedAt := recoveryClock()
	segDir := filepath.Join(cfg.Dir, "segments", streamClassDir(cfg.StreamID), "active")
	if err := os.MkdirAll(segDir, 0755); err != nil {
		return nil, err
	}
	ix, err := index.Open(index.Options{
		Dir:          filepath.Join(cfg.Dir, "index"),
		MemTableSize: cfg.IndexMemSize,
		UseInMemory:  cfg.UseMemIndex,
	})
	if err != nil {
		return nil, err
	}
	checkpoint, err := index.LoadRecoveryCheckpoint(ix, cfg.StreamID)
	if err != nil {
		_ = ix.Close()
		return nil, err
	}
	s := &Store{
		index:               ix,
		overlay:             NewOverlay(),
		segDir:              filepath.Join(cfg.Dir, "segments"),
		streamID:            cfg.StreamID,
		faults:              cfg.Faults,
		enc:                 cfg.Enc,
		applyCh:             make(chan []index.Mutation, 256),
		stopCh:              make(chan struct{}),
		flushDone:           make(chan struct{}),
		closeDone:           make(chan struct{}),
		readyBatches:        make(map[uint64]*recoveryPublishBatch),
		flushInterval:       cfg.FlushInterval,
		disableAsyncApply:   cfg.disableAsyncApply,
		flushCheckpointHook: cfg.flushCheckpointHook,
		flushApply:          cfg.flushApply,
		recoveryClock:       recoveryClock,
		recoveryStartedAt:   recoveryStartedAt,
		recoveryDeadline:    recoveryStartedAt.Add(policy.budget),
		recoveryPolicy:      policy,
		recoveryCheckpoint:  checkpoint,
	}
	if s.flushInterval <= 0 {
		s.flushInterval = 2 * time.Second
	}
	s.group = newGroupCommitCoordinator(defaultGroupCommitConfig())
	s.locCache = NewLocationCache(cfg.LocationCacheSize)
	s.segCache = NewSegmentDescriptorCache(cfg.SegCacheSize)
	s.changeJournal = cfg.ChangeJournal
	segSize := cfg.SegmentSize
	if segSize <= 0 {
		segSize = storage.DefaultDataSegmentSize
	}
	s.segmentSize = segSize
	activePath := filepath.Join(segDir, fmt.Sprintf("%d.seg", maxSegmentID(segDir)))
	// V2.1 recovery: replay committed segment-log records from the
	// active segment into the overlay, and truncate uncommitted tail
	// data (§7.5 step 4-6). A committed record absent from Pebble is
	// replayed here; a Pebble entry beyond the last committed sequence
	// is invalid and reads consult the overlay first, so it is shadowed.
	if err := s.recoverActiveSegment(activePath); err != nil {
		s.group.close()
		s.segCache.Close()
		ix.Close()
		return nil, err
	}
	// A recovered suffix exists only in memory until it is synchronously
	// indexed, flushed, marked INDEX_SAFE, and checkpointed. That durable
	// handoff must finish while the recovered segment is still the highest
	// active segment; otherwise a second crash can select the new empty
	// segment and lose acknowledged data.
	if err := s.persistRecoveredOverlay(activePath, classForStream(cfg.StreamID), segSize); err != nil {
		s.group.close()
		s.segCache.Close()
		ix.Close()
		return nil, err
	}
	s.nextSeg = maxSegmentID(segDir)
	if err := s.newActiveSegment(classForStream(cfg.StreamID), segSize); err != nil {
		s.group.close()
		s.segCache.Close()
		ix.Close()
		return nil, err
	}
	if err := s.finishRecoveryStartup(); err != nil {
		_ = s.writer.Close()
		s.group.close()
		s.segCache.Close()
		ix.Close()
		return nil, err
	}
	// A Store is not returned until recovery replay/truncation and opening the
	// new active writer both succeeded. This is the serving-state transition.
	s.applyWG.Add(1)
	go s.applyLoop()
	s.flushWG.Add(1)
	go s.flushLoop()
	return s, nil
}

// recoverActiveSegment replays committed records from an active segment
// into the overlay. If the segment does not exist (fresh disk), it is a
// no-op.
//
// Records with sequence ≤ SafeSeq are already in Pebble (a prior
// INDEX_SAFE flush); only records after it are replayed into the
// overlay (§7.4: "A committed record absent from Pebble is replayed
// before the node serves that extent").
func (s *Store) recoverActiveSegment(path string) error {
	startedAt := s.recoveryStartedAt
	if path == "" {
		s.recoveryResult = RecoverResult{Duration: s.recoveryClock().Sub(startedAt)}
		if s.recoveryClock().After(s.recoveryDeadline) {
			return storage.ErrRecoveryBudgetExceeded
		}
		return nil
	}
	if _, err := recoveryStat(path); err != nil {
		if os.IsNotExist(err) {
			s.recoveryResult = RecoverResult{Duration: s.recoveryClock().Sub(startedAt)}
			if s.recoveryClock().After(s.recoveryDeadline) {
				return storage.ErrRecoveryBudgetExceeded
			}
			return nil
		}
		return err
	}
	safeOffset := int64(storage.SegmentHeaderSize)
	safeSeq := uint64(0)
	if checkpoint := s.recoveryCheckpoint; checkpoint != nil && checkpoint.SegmentID == segIDFromPath(path) {
		safeOffset = checkpoint.SafeOffset
		safeSeq = checkpoint.SafeSeq
	}
	res, err := RecoverFromSegmentLog(path, RecoverOptions{
		StreamID:          s.streamID,
		SafeOffset:        safeOffset,
		SafeSeq:           safeSeq,
		RequireSafeMarker: safeOffset > int64(storage.SegmentHeaderSize),
		MaxRecords:        s.recoveryPolicy.maxRecords,
		MaxReplayBytes:    s.recoveryPolicy.maxReplayBytes,
		MaxTrailingBytes:  s.recoveryPolicy.maxTrailingBytes,
		clock:             s.recoveryClock,
		startedAt:         startedAt,
		deadline:          s.recoveryDeadline,
	}, func(d CommitDescriptor) error {
		// Replay into the overlay (read authority). Committed-but-
		// unflushed records are served from the overlay until the async
		// apply loop or a later flush persists them to Pebble.
		state := storage.ExtentDurable
		skip := false
		switch d.Op {
		case RecordPut:
		case RecordDelete:
			state = storage.ExtentTombstoned
		case RecordRelocate:
			// no-op: a durable RELOCATE record cannot encode a target offset
			// (see Relocate), and every relocation ships a fresh PUT at the
			// target first, so the prior binding already points at the target.
			// Skipping avoids clobbering that correct binding with the empty
			// RELOCATE record's own location. The record is still validated by
			// descriptor CRC, so a torn/corrupt RELOCATE still fails closed.
			skip = true
		default:
			return fmt.Errorf("%w: %d", storage.ErrUnsupportedRecordOperation, d.Op)
		}
		if skip {
			return nil
		}
		s.overlay.Put(index.Key(d.ExtentID, d.Generation), index.Value{
			SegmentID:  d.SegmentID,
			Offset:     d.Offset,
			StoredLen:  d.StoredLen,
			LogicalLen: d.LogicalLen,
			State:      state,
			Checksum:   d.Checksum,
		})
		return nil
	})
	if res != nil {
		s.recoveryResult = *res
	}
	if err != nil {
		return err
	}
	// Record the recovered safe sequence so the async apply loop knows
	// what is already persisted in Pebble.
	if res.SafeSeq > 0 {
		s.safeSeq.Store(res.SafeSeq)
	}
	// Commit sequences are stream-local, including across segment rotation.
	// The next fresh active segment must continue after recovered commits.
	if res.LastSeq > s.streamSeq {
		s.streamSeq = res.LastSeq
	}
	if res.LastSeq > 0 {
		s.publicationMu.Lock()
		s.publishedSeq = res.LastSeq
		s.publicationMu.Unlock()
		s.flushSeq.Store(res.LastSeq)
	}
	return nil
}

// persistRecoveredOverlay establishes the same durable index/INDEX_SAFE/
// sidecar boundary as a normal flush before startup creates a higher-numbered
// active segment. It intentionally reuses flush so there is only one
// checkpoint publication protocol.
func (s *Store) persistRecoveredOverlay(path string, class storage.SegmentClass, segSize int64) (err error) {
	if s.overlay.Len() == 0 {
		return s.checkRecoveryDeadline()
	}
	if err := s.checkRecoveryDeadline(); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() < int64(SegmentHeaderSize) {
		return fmt.Errorf("storage: recovered segment shorter than header")
	}
	w, err := OpenWriter(path)
	if err != nil {
		return err
	}
	s.writer = w
	s.writerPath = path
	s.alloc = NewAllocator(storage.SegmentID(segIDFromPath(path)), class, segSize, nowUnixNano())
	s.alloc.Consume(info.Size() - int64(SegmentHeaderSize))
	s.alloc.RecordCommit(s.streamSeq)
	defer func() {
		closeErr := w.Close()
		s.writer = nil
		s.writerPath = ""
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	if err := s.flush(); err != nil {
		return err
	}
	return s.checkRecoveryDeadline()
}

func (s *Store) checkRecoveryDeadline() error {
	now := s.recoveryClock()
	s.recoveryResult.Duration = now.Sub(s.recoveryStartedAt)
	if now.After(s.recoveryDeadline) {
		s.recoveryResult.DataReady = false
		return storage.ErrRecoveryBudgetExceeded
	}
	return nil
}

// finishRecoveryStartup records the elapsed interval through active-writer
// setup and performs the final inclusive deadline check immediately before
// publishing DataReady.
func (s *Store) finishRecoveryStartup() error {
	if err := s.checkRecoveryDeadline(); err != nil {
		return err
	}
	s.recoveryResult.DataReady = true
	s.dataReady.Store(true)
	return nil
}

// DataReady reports whether this Store completed startup recovery and may
// serve requests. New returns a Store only after this becomes true.
func (s *Store) DataReady() bool { return s.dataReady.Load() }

// RecoveryResult returns the actual active-segment replay/truncation result
// captured during startup.
func (s *Store) RecoveryResult() RecoverResult { return s.recoveryResult }

var recoveryStat = os.Stat

func streamClassDir(streamID uint8) string {
	if streamID == 0 {
		return "small"
	}
	return "data"
}

func classForStream(streamID uint8) storage.SegmentClass {
	if streamID == 0 {
		return storage.SegmentSmall
	}
	return storage.SegmentData
}

// newActiveSegment creates a fresh active segment and opens its writer.
func (s *Store) newActiveSegment(class storage.SegmentClass, segSize int64) error {
	s.nextSeg++
	segID := storage.SegmentID(s.nextSeg)
	dir := filepath.Join(s.segDir, streamClassDir(s.streamID), "active")
	path := filepath.Join(dir, fmt.Sprintf("%d.seg", segID))
	w, err := OpenWriter(path)
	if err != nil {
		return err
	}
	var hdr SegmentHeader
	hdr.Magic = storage.SegmentMagic
	hdr.Version = storage.FormatVersion
	hdr.ID = segID
	hdr.SegmentClass = class
	hb := make([]byte, SegmentHeaderSize)
	if err := hdr.Encode(hb); err != nil {
		w.Close()
		return err
	}
	if _, err := w.f.WriteAt(hb, 0); err != nil {
		w.Close()
		return err
	}
	s.alloc = NewAllocator(segID, class, segSize, nowUnixNano())
	s.writer = w
	s.writerPath = path
	if s.segCache != nil {
		s.segCache.Pin(path)
	}
	return nil
}

// maxSegmentID returns the largest segment file ID in a directory.
func maxSegmentID(dir string) uint64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var max uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".seg") {
			continue
		}
		idStr := strings.TrimSuffix(name, ".seg")
		var id uint64
		if _, err := fmt.Sscanf(idStr, "%d", &id); err == nil && id > max {
			max = id
		}
	}
	return max
}

// Write implements storage.Store.Write (V2.1 §6.1 single barrier).
func (s *Store) Write(_ context.Context, req *storage.WriteRequest) (*storage.DurableReceipt, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()

	// Phase 0: idempotency + generation fencing against the overlay
	// (read authority) and the derived index.
	if v, err := s.lookup(req.ExtentID, req.Generation); err == nil {
		if v.Checksum == checksumOf(req.Data) {
			return s.receiptFor(req, v), nil
		}
		return nil, storage.ErrStaleGeneration
	} else if err != storage.ErrExtentNotFound {
		return nil, err
	}

	if err := s.faultStage(storage.CrashBeforeBatchAppend); err != nil {
		return nil, err
	}

	// Phase 1: build the record material (offsets reserved by the group
	// commit leader when the batch commits).
	payloadCRC := checksumOf(req.Data)
	frameSize := DefaultFrameSize
	// Decide compression from the sampling rule (§9).
	compressed := ShouldCompress(len(req.Data), SampledBytes(req.Data, 4096))
	// Resolve the active encryption key (nil when encryption is off).
	var encKey []byte
	keyID := uint64(0)
	if s.enc != nil && s.enc.Enabled() {
		kid, key, err := s.enc.ActiveKey()
		if err != nil {
			return nil, err
		}
		encKey = key
		keyID = kid
	}
	storedBytes, fi, codec, _, err := BuildFramedRecord(req.Data, frameSize, compressed, encKey)
	if err != nil {
		return nil, err
	}
	storedLen := uint32(len(storedBytes))
	frameCount := len(fi.Entries)
	// Serialize the frame index.
	idxBuf := make([]byte, frameCount*FrameIndexEntrySize)
	if err := fi.Encode(idxBuf); err != nil {
		return nil, err
	}
	if err := s.faultStage(storage.CrashAfterFrameIndex); err != nil {
		return nil, err
	}
	header := &RecordHeader{
		Magic:           storage.RecordMagic,
		Version:         storage.FormatVersion,
		Op:              RecordPut,
		ExtentID:        req.ExtentID,
		Generation:      req.Generation,
		LogicalLen:      uint32(len(req.Data)),
		StoredLen:       storedLen,
		Codec:           codec,
		KeyID:           keyID,
		FrameSize:       uint16(frameSize),
		FrameCount:      uint16(frameCount),
		FrameIndexCRC:   fi.CRC,
		PayloadChecksum: payloadCRC,
	}
	pw := &pendingWrite{
		extentID:   req.ExtentID,
		generation: req.Generation,
		header:     header,
		idxBuf:     idxBuf,
		stored:     storedBytes,
		frameSize:  frameSize,
		storedLen:  storedLen,
		logicalLen: uint32(len(req.Data)),
		payloadCRC: payloadCRC,
	}

	// Phases 2-4: submit to the group-commit coordinator. The batch
	// leader appends every record + one BatchCommit and syncs once; each
	// request is acknowledged only after that single barrier (§6.4).
	if err := s.group.Submit(pw, s.commitBatch); err != nil {
		return nil, err
	}

	// The batch callback published the committed location to the overlay while
	// it still held the segment/checkpoint lock. Schedule only the derived
	// Pebble apply here, after the durable publication point.
	s.enqueueApply([]index.Mutation{{
		ExtentID:   req.ExtentID,
		Generation: req.Generation,
		Value: index.Value{
			SegmentID:  pw.segID,
			Offset:     pw.offset,
			StoredLen:  storedLen,
			LogicalLen: uint32(len(req.Data)),
			State:      storage.ExtentDurable,
			Checksum:   payloadCRC,
		},
	}})

	return &storage.DurableReceipt{
		ExtentID:   req.ExtentID,
		Generation: req.Generation,
		SegmentID:  pw.segID,
		Offset:     pw.offset,
		StoredLen:  storedLen,
		LogicalLen: uint32(len(req.Data)),
		Seq:        pw.streamSeq,
	}, nil
}

// commitBatch is the group-commit callback: it reserves offsets for the
// whole batch, appends every record + one BatchCommit, and syncs ONCE
// (§6.4). It runs under the store lock so offset reservation and segment
// sealing are atomic with respect to the batch.
//
// Correctness:
//   - receipts are produced only after this returns (the caller closes
//     done channels after the sync);
//   - if a record would overflow the active segment, the batch is sealed
//     first and continues on a fresh segment — a batch never spans two
//     segment files, but the commit stays a single barrier on the new
//     file;
//   - any error (append or sync) is returned to every writer in the
//     batch.
func (s *Store) commitBatch(batch []*pendingWrite) error {
	if s.beforeCommitLock != nil {
		s.beforeCommitLock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	required := int64(journal.BatchCommitSize)
	for _, pw := range batch {
		required += int64(RecordFraming(pw.storedLen, pw.frameSize, int(pw.header.FrameCount)))
	}
	if !s.alloc.CanReserveBatch(required) || !s.alloc.CanReserveRecords(len(batch)) {
		if err := s.sealActiveLocked(); err != nil {
			return err
		}
		if !s.alloc.CanReserveBatch(required) || !s.alloc.CanReserveRecords(len(batch)) {
			return storage.ErrSegmentFull
		}
	}

	// Reserve every record consecutively after the whole-batch preflight.
	segID := s.alloc.State().SegmentID
	for _, pw := range batch {
		framing := RecordFraming(pw.storedLen, pw.frameSize, int(pw.header.FrameCount))
		off, err := s.alloc.Reserve(framing, pw.extentID, pw.storedLen)
		if err != nil {
			return err
		}
		pw.segID = segID
		pw.offset = off
		pw.streamSeq = s.streamSeq
		s.streamSeq++
	}
	commitOffset, err := s.alloc.ReserveCommit(uint32(journal.BatchCommitSize))
	if err != nil {
		return err
	}

	// Second pass: append all records.
	for _, pw := range batch {
		if _, err := s.writer.WriteRecordFramed(pw.offset, pw.header, pw.idxBuf, pw.stored, pw.frameSize); err != nil {
			return err
		}
	}
	if err := s.faultStage(storage.CrashAfterRecordAppend); err != nil {
		return err
	}

	// One BatchCommit covering the whole batch at the end.
	first := batch[0]
	bc := &journal.BatchCommit{
		Magic:          journal.BatchCommitMagic,
		Version:        storage.FormatVersion,
		StreamID:       s.streamID,
		Seq:            s.streamSeq,
		RecordCount:    uint32(len(batch)),
		FirstOffset:    first.offset,
		LastOffset:     commitOffset,
		DescriptorsCRC: batchDescriptorsCRC(batch),
	}
	if err := s.writer.WriteBatchCommit(commitOffset, bc); err != nil {
		return err
	}
	if err := s.faultStage(storage.CrashAfterBatchCommitWrite); err != nil {
		return err
	}

	// ONE fdatasync covers payloads + BatchCommit (§6.1 step 5).
	if err := s.writer.Sync(); err != nil {
		return err
	}
	s.syncCalls.Add(1)
	if err := s.faultStage(storage.CrashAfterBatchSync); err != nil {
		return err
	}
	s.alloc.RecordCommit(s.streamSeq)
	publication := &recoveryPublishBatch{
		previousSeq: bc.Seq - uint64(len(batch)),
		seq:         bc.Seq,
		remaining:   len(batch),
		mutations:   len(batch),
	}
	for _, pw := range batch {
		pw.recoveryBatch = publication
	}
	if err := s.faultStage(storage.CrashBeforeOverlayApply); err != nil {
		return err
	}
	for _, pw := range batch {
		value := index.Value{
			SegmentID:  pw.segID,
			Offset:     pw.offset,
			StoredLen:  pw.storedLen,
			LogicalLen: pw.logicalLen,
			State:      storage.ExtentDurable,
			Checksum:   pw.payloadCRC,
		}
		if pw.publishedValue != nil {
			value = *pw.publishedValue
		}
		s.overlay.Put(index.Key(pw.extentID, pw.generation), value)
	}
	if err := s.faultStage(storage.CrashAfterOverlayApply); err != nil {
		return err
	}
	for range batch {
		s.publishRecoveryBatch(publication)
	}
	return nil
}

// recoveryPublishBatch represents one durable BatchCommit awaiting overlay
// publication. Its sequence is the cumulative stream sequence recorded by
// that BatchCommit, while previousSeq is the preceding cumulative sequence.
type recoveryPublishBatch struct {
	previousSeq uint64
	seq         uint64
	remaining   int
	mutations   int
}

// publishRecoveryBatch advances the flush watermark only through contiguous
// batches whose every mutation has been published to the overlay. Serving
// callers hold s.mu before this acquires publicationMu (the documented lock
// order); startup recovery has no concurrent commit or checkpoint. This is
// the index-durability precondition for an INDEX_SAFE sidecar checkpoint.
func (s *Store) publishRecoveryBatch(batch *recoveryPublishBatch) {
	if batch == nil {
		return
	}
	s.publicationMu.Lock()
	defer s.publicationMu.Unlock()
	batch.remaining--
	if batch.remaining != 0 {
		return
	}
	s.readyBatches[batch.previousSeq] = batch
	for {
		next := s.readyBatches[s.publishedSeq]
		if next == nil {
			return
		}
		delete(s.readyBatches, s.publishedSeq)
		s.publishedSeq = next.seq
		s.flushSeq.Store(next.seq)
		s.flushMutations.Add(int64(next.mutations))
	}
}

// batchDescriptorsCRC binds every location and checksum committed by a batch.
func batchDescriptorsCRC(batch []*pendingWrite) uint32 {
	descs := make([]journal.BatchDescriptor, len(batch))
	for i, pw := range batch {
		descs[i] = journal.BatchDescriptor{
			ExtentID: pw.extentID, Generation: pw.generation, SegmentID: pw.segID,
			Offset: pw.offset, StoredLen: pw.storedLen, LogicalLen: pw.logicalLen,
			Checksum: pw.payloadCRC, Op: uint8(pw.header.Op),
		}
	}
	buf := make([]byte, len(descs)*journal.BatchDescriptorSize)
	crc, err := journal.EncodeDescriptors(buf, descs)
	if err != nil {
		panic(err)
	}
	return crc
}

// AppendRecord writes a payload into the active segment and commits it,
// returning the new durable location. Used by the compactor to move live
// records (§10.3 step 3). The codec is passed through so already-
// compressed records are not recompressed; encryption uses the store's
// active key.
func (s *Store) AppendRecord(extentID storage.ExtentID, gen storage.Generation, data []byte, codec storage.CompressionCodec) (*storage.Reloc, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()

	// Resolve the active encryption key (nil when encryption is off).
	var encKey []byte
	keyID := uint64(0)
	if s.enc != nil && s.enc.Enabled() {
		kid, key, err := s.enc.ActiveKey()
		if err != nil {
			return nil, err
		}
		encKey = key
		keyID = kid
	}
	// Compaction preserves the original stored bytes: when the codec is
	// already zstd, the data is compressed per-frame and must not be
	// re-compressed. BuildFramedRecord(compressed=false) stores frames
	// verbatim with the given codec on the record header.
	frameSize := DefaultFrameSize
	storedBytes, fi, _, _, err := BuildFramedRecord(data, frameSize, codec == storage.CompressionZstd, encKey)
	if err != nil {
		return nil, err
	}
	storedLen := uint32(len(storedBytes))
	frameCount := len(fi.Entries)
	framing := uint32(RecordHeaderSize) + uint32(frameCount*FrameIndexEntrySize) + storedLen + uint32(RecordTrailerSize)

	s.mu.Lock()
	defer s.mu.Unlock()
	off, err := s.alloc.Reserve(framing, extentID, storedLen)
	if err != nil {
		if err == storage.ErrSegmentFull {
			if serr := s.sealActiveLocked(); serr != nil {
				return nil, serr
			}
			off, err = s.alloc.Reserve(framing, extentID, storedLen)
		}
		if err != nil {
			return nil, err
		}
	}
	if _, err := s.alloc.ReserveCommit(uint32(journal.BatchCommitSize)); err != nil {
		return nil, err
	}
	segID := s.alloc.State().SegmentID
	streamSeq := s.streamSeq
	s.streamSeq++
	header := &RecordHeader{
		Magic:           storage.RecordMagic,
		Version:         storage.FormatVersion,
		Op:              RecordPut,
		ExtentID:        extentID,
		Generation:      gen,
		LogicalLen:      uint32(len(data)),
		StoredLen:       storedLen,
		Codec:           codec,
		KeyID:           keyID,
		FrameSize:       uint16(frameSize),
		FrameCount:      uint16(frameCount),
		PayloadChecksum: checksumOf(data),
	}
	idxBuf := make([]byte, frameCount*FrameIndexEntrySize)
	if err := fi.Encode(idxBuf); err != nil {
		return nil, err
	}
	header.FrameIndexCRC = fi.CRC
	if _, err := s.writer.WriteRecordFramed(off, header, idxBuf, storedBytes, frameSize); err != nil {
		return nil, err
	}
	commitOffset := off + int64(framing)
	bc := &journal.BatchCommit{
		Magic:       journal.BatchCommitMagic,
		Version:     storage.FormatVersion,
		StreamID:    s.streamID,
		Seq:         streamSeq + 1,
		RecordCount: 1,
		FirstOffset: off,
		LastOffset:  commitOffset,
		DescriptorsCRC: descriptorCRC([]journal.BatchDescriptor{{
			ExtentID: extentID, Generation: gen, SegmentID: segID, Offset: off,
			StoredLen: storedLen, LogicalLen: uint32(len(data)), Checksum: checksumOf(data), Op: uint8(RecordPut),
		}}),
	}
	if err := s.writer.WriteBatchCommit(commitOffset, bc); err != nil {
		return nil, err
	}
	if err := s.writer.Sync(); err != nil {
		return nil, err
	}

	// Update the overlay so immediate reads observe the relocation.
	s.overlay.Put(index.Key(extentID, gen), index.Value{
		SegmentID:  segID,
		Offset:     off,
		StoredLen:  storedLen,
		LogicalLen: uint32(len(data)),
		State:      storage.ExtentDurable,
		Checksum:   checksumOf(data),
	})
	s.publishRecoveryBatch(&recoveryPublishBatch{previousSeq: streamSeq, seq: streamSeq + 1, remaining: 1, mutations: 1})
	s.enqueueApply([]index.Mutation{{
		ExtentID:   extentID,
		Generation: gen,
		Value: index.Value{
			SegmentID:  segID,
			Offset:     off,
			StoredLen:  storedLen,
			LogicalLen: uint32(len(data)),
			State:      storage.ExtentDurable,
			Checksum:   checksumOf(data),
		},
	}})
	s.alloc.RecordCommit(streamSeq + 1)

	return &storage.Reloc{ExtentID: extentID, Generation: gen, SegmentID: segID, Offset: off, StoredLen: storedLen, LogicalLen: uint32(len(data)), Checksum: checksumOf(data)}, nil
}

// Relocate repoints a record to the location already made durable by
// AppendRecord (§10.3 step 6). The compactor writes a fresh PUT record at
// the new location before calling Relocate, so the relocation is ALREADY
// durable: on crash + segment-log replay that PUT binds (extent,gen) to the
// target location and checksum. This step therefore only updates the live
// read authorities (overlay, location cache) and the change journal.
//
// We deliberately do NOT append a standalone durable RELOCATE record: the
// on-disk record format cannot encode a destination offset (recovery
// derives Offset from the record's physical position and SegmentID from the
// current segment path), so a standalone RELOCATE record would replay to its
// own empty location and clobber the correct PUT-at-target binding. The
// durable relocation is the AppendRecord PUT itself.
//
// It honors the documented "apply only if the old location still matches"
// precondition as a tombstone-preservation guard: if a concurrent delete
// tombstones (extent,gen) between the source-scan live check and this call,
// the tombstone is preserved and the relocation is skipped rather than
// resurrecting a deleted extent.
func (s *Store) Relocate(relocs []storage.Reloc) error {
	if err := s.enter(); err != nil {
		return err
	}
	defer s.leave()

	for _, r := range relocs {
		key := index.Key(r.ExtentID, r.Generation)
		if v, ok := s.overlay.Get(key); ok && v.State == storage.ExtentTombstoned {
			// A delete committed after this record was scanned live. Keep
			// the tombstone: compacting over it would resurrect a deleted
			// extent. The dead bytes are reclaimed by a later pass.
			continue
		}
		s.overlay.Put(key, index.Value{
			SegmentID:  r.SegmentID,
			Offset:     r.Offset,
			StoredLen:  r.StoredLen,
			LogicalLen: r.LogicalLen,
			State:      storage.ExtentDurable,
			// Preserve the extent's real logical checksum (the old code
			// shadowed it to 0, silently disabling read/repair integrity
			// validation after compaction).
			Checksum: r.Checksum,
		})
		// A prior read may have cached the old location; invalidate so the
		// next read re-resolves through the overlay/index.
		s.locCache.Delete(key)
		s.emitChangeEvent(journal.EventRelocated, r.ExtentID, r.Generation, r.SegmentID, "relocated")
	}
	return nil
}

// lookup checks the overlay (read authority) first, then the derived
// index, matching the V2.1 read path (§6.4).
func (s *Store) lookup(extentID storage.ExtentID, generation storage.Generation) (*index.Value, error) {
	if v, ok := s.overlay.Get(index.Key(extentID, generation)); ok {
		return &v, nil
	}
	return s.index.Get(extentID, generation)
}

// Read implements storage.Store.Read. Range reads fetch and
// authenticate only intersecting frames (§8). Uses the location cache
// to avoid Pebble lookups and the segment descriptor cache to avoid
// os.Open on every read.
func (s *Store) Read(_ context.Context, req *storage.ReadRequest) (*storage.ReadResult, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()

	v, err := s.cachedLookup(req.ExtentID, req.Generation)
	if err != nil {
		return nil, err
	}
	if v.State == storage.ExtentTombstoned {
		return nil, storage.ErrExtentNotFound
	}
	if v.State == storage.ExtentCorrupt {
		return nil, storage.ErrQuarantined
	}
	path := filepath.Join(s.segDir, streamClassDir(s.streamID), "active", fmt.Sprintf("%d.seg", v.SegmentID))
	rd, err := s.segCache.Get(path, s.enc)
	if err != nil {
		return nil, storage.ErrSegmentUnavailable
	}
	var payload []byte
	if req.Length > 0 {
		payload, err = rd.ReadRangeFrames(v.Offset, v.StoredLen, v.LogicalLen, req.LogicalOffset, req.Length)
	} else {
		payload, err = rd.ReadPayloadFrames(v.Offset, v.StoredLen, v.LogicalLen)
	}
	if err != nil {
		if s.changeJournal != nil && (errors.Is(err, storage.ErrChecksumMismatch) || errors.Is(err, storage.ErrDecryptFailed)) {
			s.emitChangeEvent(journal.EventCorrupt, req.ExtentID, req.Generation, v.SegmentID, err.Error())
		}
		return nil, err
	}
	return &storage.ReadResult{Data: payload, Checksum: v.Checksum}, nil
}

// emitChangeEvent appends an async event to the change journal (§12).
// The normal write path does NOT emit EXTENT_DURABLE here — those are
// already durable receipts to metadata. This is only for out-of-band
// events: corruption, disk/segment loss, relocation, etc.
func (s *Store) emitChangeEvent(kind journal.ChangeEventKind, extentID storage.ExtentID, gen storage.Generation, segID storage.SegmentID, reason string) {
	if s.changeJournal == nil {
		return
	}
	if _, err := s.changeJournal.Append(kind, extentID, gen, segID, reason); err != nil {
		slog.Error("storage: change journal append", "error", err)
	}
}

// cachedLookup checks the location cache first, then the overlay, then
// the derived index, backfilling the location cache on miss.
func (s *Store) cachedLookup(extentID storage.ExtentID, generation storage.Generation) (*index.Value, error) {
	key := index.Key(extentID, generation)
	if v, ok := s.locCache.Get(key); ok {
		return v, nil
	}
	v, err := s.lookup(extentID, generation)
	if err != nil {
		return nil, err
	}
	s.locCache.Put(key, v)
	return v, nil
}

// Delete implements storage.Store.Delete (generation-fenced). A
// tombstone is appended and committed in the stream, synced once,
// before acknowledging (§10.1).
func (s *Store) Delete(_ context.Context, req *storage.DeleteRequest) error {
	if err := s.enter(); err != nil {
		return err
	}
	defer s.leave()

	v, err := s.lookup(req.ExtentID, req.Generation)
	if err != nil {
		if err == storage.ErrExtentNotFound {
			return nil // already gone
		}
		return err
	}
	// Submit a zero-frame delete record through the normal coordinator so it
	// remains contiguous with writes and shares the exact one-sync batch
	// durability rule.
	tombstone := *v
	tombstone.State = storage.ExtentTombstoned
	pw := &pendingWrite{
		extentID:       req.ExtentID,
		generation:     req.Generation,
		publishedValue: &tombstone,
		header: &RecordHeader{
			Magic: storage.RecordMagic, Version: storage.FormatVersion,
			Op: RecordDelete, ExtentID: req.ExtentID, Generation: req.Generation,
			FrameIndexCRC: storage.CRC32C(nil),
		},
		frameSize: DefaultFrameSize,
	}
	if err := s.group.Submit(pw, s.commitBatch); err != nil {
		return err
	}
	key := index.Key(req.ExtentID, req.Generation)
	// cachedLookup checks locCache before the overlay. Publish the tombstone
	// into that cache as part of the durable-delete visibility update so a
	// prior live cache entry cannot outlive an acknowledged delete.
	s.locCache.Put(key, &tombstone)
	return s.enqueueApply([]index.Mutation{{ExtentID: req.ExtentID, Generation: req.Generation, Value: tombstone}})
}

// Stat implements storage.Store.Stat.
func (s *Store) Stat(_ context.Context, req *storage.StatRequest) (*storage.StatResult, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()

	v, err := s.lookup(req.ExtentID, req.Generation)
	if err != nil {
		return nil, err
	}
	return &storage.StatResult{
		SegmentID:  v.SegmentID,
		Offset:     v.Offset,
		StoredLen:  v.StoredLen,
		LogicalLen: v.LogicalLen,
		State:      v.State,
		Checksum:   v.Checksum,
	}, nil
}

// Index exposes the derived index.
func (s *Store) Index() *index.Index { return s.index }

// Overlay exposes the committed-delta overlay.
func (s *Store) Overlay() *Overlay { return s.overlay }

// LiveExtent is one enumerated extent in the read-authority view,
// carrying its latest generation so callers can route generation-fenced
// operations.
type LiveExtent struct {
	ExtentID   storage.ExtentID
	Generation storage.Generation
	Value      index.Value
}

// ListExtents enumerates the extents of this store in the
// read-authority view: the committed-delta overlay takes priority over
// the flushed Pebble index (matching lookup). For each extent it yields
// the single live generation with the highest generation number — the
// same coalescing rule as Index.Iterate and the read path — so a
// tombstone at any generation hides every live generation of that extent
// (an acknowledged delete is immediately and durably invisible), and an
// overwrite resolves to its newest payload. Tombstoned extents are
// excluded; corrupt ones are kept so callers can surface them as
// failed/repairable rather than silently dropping them. It takes the
// store read gate so shutdown cannot race the enumeration against a
// closing index.
func (s *Store) ListExtents() ([]LiveExtent, error) {
	if err := s.enter(); err != nil {
		return nil, err
	}
	defer s.leave()

	// Coalesce both sources (overlay + flushed Pebble) per extent to the
	// highest generation. The overlay may hold several generations of one
	// extent at once (only the newest key is overwritten by an overwrite or
	// delete), so map-iteration order must not decide visibility — the max
	// generation does.
	merged := make(map[storage.ExtentID]LiveExtent)
	update := func(e LiveExtent) {
		cur, ok := merged[e.ExtentID]
		if !ok || e.Generation > cur.Generation {
			merged[e.ExtentID] = e
		}
	}
	for k, v := range s.overlay.Snapshot() {
		if len(k) < index.KeyLen {
			continue
		}
		kb := []byte(k)
		update(LiveExtent{
			ExtentID:   index.ExtentFromKey(kb),
			Generation: index.GenerationFromKey(kb),
			Value:      v,
		})
	}
	if err := s.index.Iterate(func(id storage.ExtentID, gen storage.Generation, v index.Value) error {
		update(LiveExtent{ExtentID: id, Generation: gen, Value: v})
		return nil
	}); err != nil {
		return nil, err
	}

	out := make([]LiveExtent, 0, len(merged))
	for _, e := range merged {
		if e.Value.State == storage.ExtentTombstoned {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

// Close flushes pending index applies and closes the store. It is a
// durability barrier: any committed overlay entries not yet applied to
// Pebble are flushed synchronously before the DB closes, so no
// acknowledged write is lost across a clean shutdown.
//
// Close is idempotent and safe to call concurrently. The shutdown body
// runs exactly once; every other caller blocks until it finishes and
// then observes the same terminal error.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		defer close(s.closeDone)
		s.closeErr = s.closeInternal()
	})
	<-s.closeDone
	return s.closeErr
}

// enter admits a request into the store for the duration of one
// operation. It fails with ErrStoreClosed once shutdown has begun, and
// otherwise holds the gate so shutdown cannot close the index underneath
// the caller. Every caller that succeeds must call leave.
func (s *Store) enter() error {
	if s.closing.Load() {
		return storage.ErrStoreClosed
	}
	s.gate.RLock()
	// Re-check under the gate: shutdown may have set closing and drained
	// between the check above and acquiring the lock.
	if s.closing.Load() {
		s.gate.RUnlock()
		return storage.ErrStoreClosed
	}
	return nil
}

func (s *Store) leave() { s.gate.RUnlock() }

// closeInternal performs the actual shutdown. It runs under closeOnce,
// so it never executes concurrently with itself.
func (s *Store) closeInternal() error {
	// Refuse new requests, then take the gate for write to wait until
	// every in-flight request has left. Only after that is it safe to
	// close the index: Pebble panics rather than erroring on use-after-close.
	s.closing.Store(true)
	s.gate.Lock()
	s.gate.Unlock()

	// Close the group-commit coordinator first: it wakes any queued
	// followers so they do not block forever on a closed store.
	if s.group != nil {
		s.group.close()
	}
	// Stop the flush loop (it performs a final flush on stop) and the
	// async apply loop.
	close(s.stopCh)
	s.flushWG.Wait()
	s.applyWG.Wait()
	// Flush the committed-delta overlay into Pebble synchronously. After
	// flushWG.Wait the flush loop is dead, so no concurrent flushes exist,
	// but Drain stages entries rather than discarding them. Since Close is
	// shutting down, discard immediately after the apply succeeds to avoid
	// leaking the draining set.
	muts := s.overlay.Drain()
	if len(muts) > 0 {
		if err := s.applyNow(muts); err != nil {
			return err
		}
		s.overlay.DiscardDrained()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writer != nil {
		s.writer.Close()
	}
	if s.segCache != nil {
		s.segCache.Close()
	}
	return s.index.Close()
}

// sealActiveLocked seals the current active segment (V2.1 §7.2):
// drains the batch, appends footer + SEAL_SEGMENT in one committed
// batch, syncs once, then opens a fresh active segment.
func (s *Store) sealActiveLocked() error {
	st := s.alloc.State()
	footer := &SegmentFooter{
		Magic:            storage.SegmentMagic,
		Version:          storage.FormatVersion,
		RecordCount:      st.RecordCount,
		TotalPayload:     uint64(st.PayloadBytes),
		MinExtentID:      st.MinExtent,
		MaxExtentID:      st.MaxExtent,
		LastCommittedSeq: st.LastCommitSeq,
		CreatedAtUnix:    st.CreatedAt,
		SealedAtUnix:     nowUnixNano(),
	}
	if err := s.writer.WriteFooter(st.NextOffset, footer); err != nil {
		return err
	}
	if err := s.writer.Sync(); err != nil {
		return err
	}
	return s.newActiveSegment(st.Class, s.segmentSize)
}

// enqueueApply schedules an async Pebble mutation. A crash here (after
// ack, before Pebble apply) loses only the derived index, which
// recovery rebuilds from the committed segment log. Close() drains the
// overlay into Pebble, so an unqueued mutation here is still durable
// via the segment log + overlay.
func (s *Store) enqueueApply(muts []index.Mutation) error {
	if s.disableAsyncApply {
		return nil
	}
	if err := s.faultStage(storage.CrashAfterAck); err != nil {
		return err
	}
	select {
	case s.applyCh <- muts:
		return nil
	default:
		// Channel full: apply synchronously to avoid unbounded backlog.
		return s.applyNow(muts)
	}
}

// applyNow applies mutations to Pebble.
func (s *Store) applyNow(muts []index.Mutation) error {
	batch := s.index.NewBatch()
	defer batch.Close()
	for _, m := range muts {
		if err := s.index.PutBatch(batch, m.ExtentID, m.Generation, &m.Value); err != nil {
			return err
		}
	}
	return s.index.ApplyBatch(batch)
}

// applyLoop drains the async apply queue.
func (s *Store) applyLoop() {
	defer s.applyWG.Done()
	for {
		select {
		case muts := <-s.applyCh:
			_ = s.applyNow(muts)
		case <-s.stopCh:
			// Drain any remaining queued mutations before exiting.
			for {
				select {
				case muts := <-s.applyCh:
					_ = s.applyNow(muts)
				default:
					return
				}
			}
		}
	}
}

// faultStage consults the injected fault hook, if any.
func (s *Store) faultStage(point storage.CrashPoint) error {
	if s.faults != nil {
		return s.faults.OnStage(point)
	}
	return nil
}

// receiptFor constructs a receipt from an existing index entry.
func (s *Store) receiptFor(req *storage.WriteRequest, v *index.Value) *storage.DurableReceipt {
	return &storage.DurableReceipt{
		ExtentID:   req.ExtentID,
		Generation: req.Generation,
		SegmentID:  v.SegmentID,
		Offset:     v.Offset,
		StoredLen:  v.StoredLen,
		LogicalLen: v.LogicalLen,
		Seq:        s.streamSeq,
	}
}

func checksumOf(data []byte) uint32 {
	return storage.CRC32C(data)
}
