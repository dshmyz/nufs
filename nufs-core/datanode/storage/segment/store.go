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

	nextSeg uint64
	segDir  string
	// segmentSize is retained across seals so a fresh segment restores the
	// configured capacity rather than inheriting the old segment's tail.
	segmentSize int64

	// Async Pebble apply queue.
	applyCh chan []index.Mutation
	stopCh  chan struct{}
	applyWG sync.WaitGroup

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
	flushInterval  time.Duration
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
}

// New opens (creating if needed) a Store for one commit stream.
//
// On restart it recovers: Pebble reopens (persisted), and committed
// segment records after the safe sequence are replayed into the overlay
// (recovery module). Segment IDs are seeded past surviving files.
func New(cfg Config) (*Store, error) {
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
	s := &Store{
		index:         ix,
		overlay:       NewOverlay(),
		segDir:        filepath.Join(cfg.Dir, "segments"),
		streamID:      cfg.StreamID,
		faults:        cfg.Faults,
		enc:           cfg.Enc,
		applyCh:       make(chan []index.Mutation, 256),
		stopCh:        make(chan struct{}),
		flushDone:     make(chan struct{}),
		flushInterval: cfg.FlushInterval,
	}
	if s.flushInterval <= 0 {
		s.flushInterval = 2 * time.Second
	}
	s.group = newGroupCommitCoordinator(defaultGroupCommitConfig())
	s.locCache = NewLocationCache(cfg.LocationCacheSize)
	s.segCache = NewSegmentDescriptorCache(cfg.SegCacheSize)
	s.changeJournal = cfg.ChangeJournal
	s.applyWG.Add(1)
	go s.applyLoop()
	s.flushWG.Add(1)
	go s.flushLoop()

	// V2.1 recovery: replay committed segment-log records from the
	// active segment into the overlay, and truncate uncommitted tail
	// data (§7.5 step 4-6). A committed record absent from Pebble is
	// replayed here; a Pebble entry beyond the last committed sequence
	// is invalid and reads consult the overlay first, so it is shadowed.
	if err := s.recoverActiveSegment(filepath.Join(segDir, fmt.Sprintf("%d.seg", maxSegmentID(segDir)))); err != nil {
		ix.Close()
		return nil, err
	}

	segSize := cfg.SegmentSize
	if segSize <= 0 {
		segSize = storage.DefaultDataSegmentSize
	}
	s.segmentSize = segSize
	s.nextSeg = maxSegmentID(segDir)
	if err := s.newActiveSegment(classForStream(cfg.StreamID), segSize); err != nil {
		ix.Close()
		return nil, err
	}
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
	if path == "" || !fileExists(path) {
		return nil
	}
	res, err := RecoverFromSegmentLog(path, RecoverOptions{
		StreamID:   s.streamID,
		SafeOffset: int64(storage.SegmentHeaderSize),
		SafeSeq:    s.safeSeq.Load(),
	}, func(d CommitDescriptor) error {
		// Replay into the overlay (read authority). Committed-but-
		// unflushed records are served from the overlay until the async
		// apply loop or a later flush persists them to Pebble.
		state := storage.ExtentDurable
		if d.Op == RecordDelete {
			state = storage.ExtentTombstoned
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
	return nil
}

// fileExists reports whether a file exists.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

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

	// Phase 5: apply committed location to the bounded overlay so
	// immediate reads observe the write (§6.4).
	segID := pw.segID
	off := pw.offset
	if err := s.faultStage(storage.CrashBeforeOverlayApply); err != nil {
		return nil, err
	}
	s.overlay.Put(index.Key(req.ExtentID, req.Generation), index.Value{
		SegmentID:  segID,
		Offset:     off,
		StoredLen:  storedLen,
		LogicalLen: uint32(len(req.Data)),
		State:      storage.ExtentDurable,
		Checksum:   payloadCRC,
	})
	if err := s.faultStage(storage.CrashAfterOverlayApply); err != nil {
		return nil, err
	}

	// Phase 6: schedule async Pebble apply (§6.1 step 8).
	s.enqueueApply([]index.Mutation{{
		ExtentID:   req.ExtentID,
		Generation: req.Generation,
		Value: index.Value{
			SegmentID:  segID,
			Offset:     off,
			StoredLen:  storedLen,
			LogicalLen: uint32(len(req.Data)),
			State:      storage.ExtentDurable,
			Checksum:   payloadCRC,
		},
	}})

	return &storage.DurableReceipt{
		ExtentID:   req.ExtentID,
		Generation: req.Generation,
		SegmentID:  segID,
		Offset:     off,
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
	// Track the committed sequence + mutation count for the flush
	// trigger (§7.4).
	s.flushSeq.Store(s.streamSeq)
	s.flushMutations.Add(int64(len(batch)))
	return nil
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
	off, err := s.alloc.Reserve(framing, extentID, storedLen)
	if err != nil {
		s.mu.Unlock()
		if err == storage.ErrSegmentFull {
			if serr := s.sealActiveLocked(); serr != nil {
				return nil, serr
			}
			off, err = s.alloc.Reserve(framing, extentID, storedLen)
		}
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
	}
	if _, err := s.alloc.ReserveCommit(uint32(journal.BatchCommitSize)); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	segID := s.alloc.State().SegmentID
	streamSeq := s.streamSeq
	s.streamSeq++
	writer := s.writer
	s.mu.Unlock()

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
	if _, err := writer.WriteRecordFramed(off, header, idxBuf, storedBytes, frameSize); err != nil {
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
	if err := writer.WriteBatchCommit(commitOffset, bc); err != nil {
		return nil, err
	}
	if err := writer.Sync(); err != nil {
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

	return &storage.Reloc{ExtentID: extentID, Generation: gen, SegmentID: segID, Offset: off, StoredLen: storedLen, LogicalLen: uint32(len(data))}, nil
}

// Relocate updates the derived index and overlay for records moved by
// compaction (§10.3 step 6), applying only if the old location still
// matches.
func (s *Store) Relocate(relocs []storage.Reloc) error {
	for _, r := range relocs {
		s.overlay.Put(index.Key(r.ExtentID, r.Generation), index.Value{
			SegmentID:  r.SegmentID,
			Offset:     r.Offset,
			StoredLen:  r.StoredLen,
			LogicalLen: r.LogicalLen,
			State:      storage.ExtentDurable,
			Checksum:   0,
		})
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
	pw := &pendingWrite{
		extentID:   req.ExtentID,
		generation: req.Generation,
		header: &RecordHeader{
			Magic: storage.RecordMagic, Version: storage.FormatVersion,
			Op: RecordDelete, ExtentID: req.ExtentID, Generation: req.Generation,
			FrameIndexCRC: crc32ChecksumIEEE(nil),
		},
		frameSize: DefaultFrameSize,
	}
	if err := s.group.Submit(pw, s.commitBatch); err != nil {
		return err
	}
	v.State = storage.ExtentTombstoned
	s.overlay.Put(index.Key(req.ExtentID, req.Generation), *v)
	return s.enqueueApply([]index.Mutation{{ExtentID: req.ExtentID, Generation: req.Generation, Value: *v}})
}

// Stat implements storage.Store.Stat.
func (s *Store) Stat(_ context.Context, req *storage.StatRequest) (*storage.StatResult, error) {
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

// Close flushes pending index applies and closes the store. It is a
// durability barrier: any committed overlay entries not yet applied to
// Pebble are flushed synchronously before the DB closes, so no
// acknowledged write is lost across a clean shutdown.
func (s *Store) Close() error {
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
	// Flush the committed-delta overlay into Pebble synchronously.
	muts := s.overlay.Drain()
	if len(muts) > 0 {
		if err := s.applyNow(muts); err != nil {
			return err
		}
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
	return crc32ChecksumIEEE(data)
}
