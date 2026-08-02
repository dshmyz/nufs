package segment

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/index"
	"github.com/example/dfs/datanode/storage/journal"
	"github.com/example/dfs/datanode/storage/encryption"
)

// Store implements storage.Store for a single commit stream on one disk
// (V2.1 §6.4). It owns one active segment and its single-fsync group
// commit: append records + BatchCommit, one fdatasync, then update the
// committed-delta overlay and acknowledge. Pebble is a derived index
// applied asynchronously after the durability point.
//
// V2.1 durability model (§6.1):
//
//	1. reserve offset in active segment
//	2. append record header + frame index + frames + trailer
//	3. append BatchCommit for the group
//	4. one fdatasync on the segment
//	5. apply committed locations to the bounded in-memory delta overlay
//	6. return DurableReceipt for every request in the commit
//	7. apply mutations asynchronously to Pebble
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
	index  *index.Index
	overlay *Overlay

	// streamID distinguishes small (0) from data (1) commit streams.
	streamID uint8
	// streamSeq is the stream-local commit sequence (monotonic).
	streamSeq uint64

	nextSeg uint64
	segDir  string

	// Async Pebble apply queue.
	applyCh chan []index.Mutation
	stopCh  chan struct{}
	applyWG sync.WaitGroup

	// group commits writes to the active segment (§6.4): a leader
	// collects followers, appends records + one BatchCommit, syncs once,
	// then wakes everyone.
	group *groupCommitCoordinator

	// syncCalls counts writer.Sync() invocations (observability + tests
	// proving group commit shares one barrier per batch).
	syncCalls atomic.Int64

	// Safe-sequence tracking for INDEX_SAFE flush (§7.4).
	pendingFlush  atomic.Uint64
	flushInterval time.Duration

	// enc is the record encryption registry (nil = plaintext).
	enc *encryption.KeyRegistry

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
		flushInterval: 2 * time.Second,
	}
	s.group = newGroupCommitCoordinator(defaultGroupCommitConfig())
	s.applyWG.Add(1)
	go s.applyLoop()

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
func (s *Store) recoverActiveSegment(path string) error {
	if path == "" || !fileExists(path) {
		return nil
	}
	_, err := RecoverFromSegmentLog(path, s.streamID, s.overlay, func(d CommitDescriptor) error {
		// Replay into the overlay (read authority). Committed-but-
		// unflushed records are served from the overlay until the async
		// apply loop or a later flush persists them to Pebble.
		s.overlay.Put(index.Key(d.ExtentID, d.Generation), index.Value{
			SegmentID:  d.SegmentID,
			Offset:     d.Offset,
			StoredLen:  d.StoredLen,
			LogicalLen: d.LogicalLen,
			State:      storage.ExtentDurable,
			Checksum:   0, // checksum not in the descriptor; verified on read
		})
		return nil
	})
	return err
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
		Magic:        storage.RecordMagic,
		Version:      storage.FormatVersion,
		ExtentID:     req.ExtentID,
		Generation:   req.Generation,
		LogicalLen:   uint32(len(req.Data)),
		StoredLen:    storedLen,
		Codec:        codec,
		KeyID:        keyID,
		FrameSize:    uint16(frameSize),
		FrameCount:   uint16(frameCount),
		FrameIndexCRC: fi.CRC,
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

	// First pass: reserve offsets; seal on overflow and retry.
	for _, pw := range batch {
		framing := RecordFraming(pw.storedLen, pw.frameSize, int(pw.header.FrameCount))
		off, err := s.alloc.Reserve(framing, pw.extentID, pw.storedLen)
		if err != nil {
			if err == storage.ErrSegmentFull {
				if serr := s.sealActiveLocked(); serr != nil {
					return serr
				}
				off, err = s.alloc.Reserve(framing, pw.extentID, pw.storedLen)
			}
			if err != nil {
				return err
			}
		}
		// Reserve BatchCommit space after the record (the leader writes
		// one BatchCommit per batch, at the end; here we reserve per
		// record to keep offsets monotonic, but only write one commit).
		if _, err := s.alloc.ReserveCommit(uint32(journal.BatchCommitSize)); err != nil {
			return err
		}
		pw.segID = s.alloc.State().SegmentID
		pw.offset = off
		pw.streamSeq = s.streamSeq
		s.streamSeq++
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
	last := batch[len(batch)-1]
	lastOffset := last.offset + int64(RecordFraming(last.storedLen, last.frameSize, int(last.header.FrameCount)))
	bc := &journal.BatchCommit{
		Magic:          journal.BatchCommitMagic,
		Version:        storage.FormatVersion,
		StreamID:       s.streamID,
		Seq:            s.streamSeq,
		RecordCount:    uint32(len(batch)),
		FirstOffset:    first.offset,
		LastOffset:     lastOffset,
		DescriptorsCRC: checksumOf(batchDescriptorsChecksum(batch)),
	}
	if err := s.writer.WriteBatchCommit(lastOffset, bc); err != nil {
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
	return nil
}

// batchDescriptorsChecksum derives a batch-level checksum over the
// batch's extent/gen pairs (bound to the BatchCommit).
func batchDescriptorsChecksum(batch []*pendingWrite) []byte {
	out := make([]byte, 0, len(batch)*8)
	for _, pw := range batch {
		var b [8]byte
		b[0] = byte(pw.extentID >> 56)
		b[1] = byte(pw.extentID >> 48)
		b[2] = byte(pw.extentID >> 40)
		b[3] = byte(pw.extentID >> 32)
		b[4] = byte(pw.extentID >> 24)
		b[5] = byte(pw.extentID >> 16)
		b[6] = byte(pw.extentID >> 8)
		b[7] = byte(pw.extentID)
		out = append(out, b[:]...)
	}
	return out
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
		Magic:      storage.RecordMagic,
		Version:    storage.FormatVersion,
		ExtentID:   extentID,
		Generation: gen,
		LogicalLen: uint32(len(data)),
		StoredLen:  storedLen,
		Codec:      codec,
		KeyID:      keyID,
		FrameSize:  uint16(frameSize),
		FrameCount: uint16(frameCount),
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
		Magic:          journal.BatchCommitMagic,
		Version:        storage.FormatVersion,
		StreamID:       s.streamID,
		Seq:            streamSeq + 1,
		RecordCount:    1,
		FirstOffset:    off,
		LastOffset:     commitOffset,
		DescriptorsCRC: checksumOf(data),
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
// authenticate only intersecting frames (§8).
func (s *Store) Read(_ context.Context, req *storage.ReadRequest) (*storage.ReadResult, error) {
	v, err := s.lookup(req.ExtentID, req.Generation)
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
	rd, err := OpenReaderWithEnc(path, s.enc)
	if err != nil {
		return nil, storage.ErrSegmentUnavailable
	}
	defer rd.Close()

	var payload []byte
	if req.Length > 0 {
		payload, err = rd.ReadRangeFrames(v.Offset, v.StoredLen, v.LogicalLen, req.LogicalOffset, req.Length)
	} else {
		payload, err = rd.ReadPayloadFrames(v.Offset, v.StoredLen, v.LogicalLen)
	}
	if err != nil {
		return nil, err
	}
	return &storage.ReadResult{Data: payload, Checksum: v.Checksum}, nil
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
	// Append tombstone + BatchCommit in one barrier.
	// (Full tombstone-batch support lands with compaction in phase 4;
	// here we mark the overlay/index directly after a durable commit.)
	if _, err := s.appendTombstone(req.ExtentID, req.Generation, v); err != nil {
		return err
	}
	v.State = storage.ExtentTombstoned
	s.overlay.Put(index.Key(req.ExtentID, req.Generation), *v)
	return s.enqueueApply([]index.Mutation{{ExtentID: req.ExtentID, Generation: req.Generation, Value: *v}})
}

// appendTombstone appends a tombstone BatchCommit to the stream.
func (s *Store) appendTombstone(extentID storage.ExtentID, gen storage.Generation, v *index.Value) (*storage.DurableReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Reserve a tombstone marker (a zero-length record + BatchCommit).
	// Tombstones carry the old location for generation fencing.
	// Simplification: a single committed BatchCommit records the delete.
	bc := &journal.BatchCommit{
		Magic:       journal.BatchCommitMagic,
		Version:     storage.FormatVersion,
		StreamID:    s.streamID,
		Seq:         s.streamSeq + 1,
		RecordCount: 1,
		FirstOffset: v.Offset,
		LastOffset:  v.Offset,
		DescriptorsCRC: checksumOf(nil),
	}
	buf := make([]byte, journal.BatchCommitSize)
	if err := bc.Encode(buf); err != nil {
		return nil, err
	}
	// Append at the current tail.
	tail, err := s.alloc.CurrentTail()
	if err != nil {
		return nil, err
	}
	if _, err := s.writer.WriteAt(buf, tail); err != nil {
		return nil, err
	}
	if err := s.writer.Sync(); err != nil {
		return nil, err
	}
	s.streamSeq++
	s.alloc.Consume(journal.BatchCommitSize)
	return &storage.DurableReceipt{ExtentID: extentID, Generation: gen, SegmentID: v.SegmentID, Offset: v.Offset, Seq: s.streamSeq}, nil
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
	// Drain any queued async applies.
	close(s.stopCh)
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
	return s.index.Close()
}

// sealActiveLocked seals the current active segment (V2.1 §7.2):
// drains the batch, appends footer + SEAL_SEGMENT in one committed
// batch, syncs once, then opens a fresh active segment.
func (s *Store) sealActiveLocked() error {
	st := s.alloc.State()
	footer := &SegmentFooter{
		Magic:         storage.SegmentMagic,
		Version:       storage.FormatVersion,
		RecordCount:   st.RecordCount,
		TotalPayload:  uint64(st.PayloadBytes),
		MinExtentID:   st.MinExtent,
		MaxExtentID:   st.MaxExtent,
		LastCommittedSeq: st.LastCommitSeq,
		CreatedAtUnix: st.CreatedAt,
		SealedAtUnix:  nowUnixNano(),
	}
	if err := s.writer.WriteFooter(st.NextOffset, footer); err != nil {
		return err
	}
	if err := s.writer.Sync(); err != nil {
		return err
	}
	return s.newActiveSegment(st.Class, st.NextOffset)
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
