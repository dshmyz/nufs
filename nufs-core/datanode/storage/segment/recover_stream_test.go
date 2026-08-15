package segment

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/journal"
)

func TestRecoverStreaming_ReplaysCommittedBatchAndTruncatesTail(t *testing.T) {
	path, desc, committedEnd := writeRecoveryFixture(t, 0, 1, nil)
	tail := []byte("uncommitted-tail")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(tail); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	var applied []CommitDescriptor
	res, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0}, func(got CommitDescriptor) error {
		applied = append(applied, got)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 1 || applied[0] != desc {
		t.Fatalf("applied = %+v, want [%+v]", applied, desc)
	}
	if res.TrailingBytes != int64(len(tail)) {
		t.Fatalf("trailing bytes = %d, want %d", res.TrailingBytes, len(tail))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != committedEnd {
		t.Fatalf("file size = %d, want truncated %d", info.Size(), committedEnd)
	}
}

// TestStoreRecovery_RelocateNoopDoesNotClobber verifies recovery treats a
// RELOCATE record as a no-op: it must not fail closed (a supported record
// op) and must not bind the extent to the RELOCATE record's own empty
// location. A RELOCATE record cannot encode a target offset (see
// Store.Relocate), so its durable role is validation-only; the relocation's
// real durability comes from the fresh PUT at the target that precedes it.
// When the fixture's only record is a RELOCATE (no prior PUT), the extent
// must therefore resolve as NOT FOUND rather than being bound to the empty
// RELOCATE location.
func TestStoreRecovery_RelocateNoopDoesNotClobber(t *testing.T) {
	path, desc, committedEnd := writeRecoveryFixture(t, 0, 1, nil)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	headerBytes := make([]byte, RecordHeaderSize)
	if _, err := f.ReadAt(headerBytes, int64(SegmentHeaderSize)); err != nil {
		t.Fatal(err)
	}
	headerBytes[5] = byte(RecordRelocate)
	binary.BigEndian.PutUint32(headerBytes[47:51], storage.CRC32C(headerBytes[0:47]))
	if _, err := f.WriteAt(headerBytes, int64(SegmentHeaderSize)); err != nil {
		t.Fatal(err)
	}

	commitOffset := committedEnd - int64(journal.BatchCommitSize)
	commitBytes := make([]byte, journal.BatchCommitSize)
	if _, err := f.ReadAt(commitBytes, commitOffset); err != nil {
		t.Fatal(err)
	}
	var commit journal.BatchCommit
	if err := commit.Decode(commitBytes); err != nil {
		t.Fatal(err)
	}
	commit.DescriptorsCRC = descriptorCRC([]journal.BatchDescriptor{{
		ExtentID: desc.ExtentID, Generation: desc.Generation, SegmentID: desc.SegmentID,
		Offset: desc.Offset, StoredLen: desc.StoredLen, LogicalLen: desc.LogicalLen, Checksum: desc.Checksum, Op: uint8(RecordRelocate),
	}})
	if err := commit.Encode(commitBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(commitBytes, commitOffset); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	active := filepath.Join(dir, "segments", "small", "active")
	if err := os.MkdirAll(active, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, filepath.Join(active, "7.seg")); err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Dir: dir, UseMemIndex: true})
	if err != nil {
		t.Fatalf("recovery rejected a supported relocate record: %v", err)
	}
	defer s.Close()

	// The RELOCATE record's own (empty) location must NOT be applied: the
	// extent has no prior PUT in this fixture, so it resolves as not found
	// rather than being bound to 0-byte payload at the relocate record.
	if _, err := s.Stat(context.Background(), &storage.StatRequest{ExtentID: desc.ExtentID, Generation: desc.Generation}); !errors.Is(err, storage.ErrExtentNotFound) {
		t.Fatalf("recovery applied the empty relocate location instead of skipping it: Stat = %v, want ErrExtentNotFound", err)
	}
}

func TestRecoverStreaming_ApplyErrorPreservesCommittedBatch(t *testing.T) {
	path, _, committedEnd := writeRecoveryFixture(t, 0, 1, nil)
	applyErr := errors.New("apply committed batch")

	res, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0}, func(CommitDescriptor) error {
		return applyErr
	})
	if !errors.Is(err, applyErr) {
		t.Fatalf("recovery error = %v, want apply error", err)
	}
	if res == nil || res.DataReady {
		t.Fatalf("recovery result = %+v, want DataReady false", res)
	}
	if res.LastCommittedOffset != committedEnd || res.TrailingBytes != 0 {
		t.Fatalf("recovery result = %+v, want committed boundary %d without trailing bytes", res, committedEnd)
	}
	assertRecoverySize(t, path, committedEnd)
}

func TestRecoverStreaming_TruncatesTornRecordAndCommitTails(t *testing.T) {
	for name, tail := range map[string][]byte{
		"header":  tornRecordBytes(t, 0),
		"payload": tornRecordBytes(t, RecordHeaderSize),
		"trailer": tornRecordBytes(t, RecordHeaderSize+FrameIndexEntrySize+1),
		"commit":  {'B', 'C', 'O', 'M'},
	} {
		t.Run(name, func(t *testing.T) {
			path, _, committedEnd := writeRecoveryFixture(t, 0, 1, nil)
			appendRecoveryBytes(t, path, tail)
			res, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0}, func(CommitDescriptor) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			if res.LastCommittedOffset != committedEnd || res.TrailingBytes != int64(len(tail)) {
				t.Fatalf("result = %+v, want boundary %d and tail %d", res, committedEnd, len(tail))
			}
			assertRecoverySize(t, path, committedEnd)
		})
	}
}

func TestRecoverStreaming_RejectsInvalidBatchCommit(t *testing.T) {
	cases := map[string]func(*journal.BatchCommit){
		"stream":         func(b *journal.BatchCommit) { b.StreamID = 1 },
		"record count":   func(b *journal.BatchCommit) { b.RecordCount = 2 },
		"first offset":   func(b *journal.BatchCommit) { b.FirstOffset++ },
		"last offset":    func(b *journal.BatchCommit) { b.LastOffset++ },
		"descriptor crc": func(b *journal.BatchCommit) { b.DescriptorsCRC++ },
		"zero sequence":  func(b *journal.BatchCommit) { b.Seq = 0 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			path, _, _ := writeRecoveryFixture(t, 0, 1, mutate)
			applied := 0
			res, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0}, func(CommitDescriptor) error {
				applied++
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if applied != 0 || res.Commits != 0 || res.LastCommittedOffset != int64(SegmentHeaderSize) {
				t.Fatalf("invalid commit applied: applied=%d result=%+v", applied, res)
			}
			assertRecoverySize(t, path, int64(SegmentHeaderSize))
		})
	}
}

func TestRecoverStreaming_SafeSeqValidatesWithoutReplay(t *testing.T) {
	path, _, committedEnd := writeRecoveryFixture(t, 0, 1, nil)
	applied := 0
	res, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0, SafeSeq: 1}, func(CommitDescriptor) error {
		applied++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if applied != 0 || res.Applied != 0 || res.Commits != 1 || res.LastSeq != 1 || res.LastCommittedOffset != committedEnd {
		t.Fatalf("safe replay result = %+v, applied=%d", res, applied)
	}
	assertRecoverySize(t, path, committedEnd)
}

func TestRecoverStreaming_SparseTailUsesBoundedMemoryAndCanonicalBudget(t *testing.T) {
	path, _, committedEnd := writeRecoveryFixture(t, 0, 1, nil)
	const sparseSize = int64(4 << 30)
	if err := os.Truncate(path, sparseSize); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	res, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0}, func(CommitDescriptor) error { return nil })
	if !errors.Is(err, ErrRecoveryBudgetExceeded) {
		t.Fatalf("error = %v, want recovery budget exceeded", err)
	}
	runtime.ReadMemStats(&after)
	if after.TotalAlloc-before.TotalAlloc > 8<<20 {
		t.Fatalf("recovery allocated %d bytes for sparse tail", after.TotalAlloc-before.TotalAlloc)
	}
	if res.TrailingBytes != sparseSize-committedEnd {
		t.Fatalf("trailing bytes = %d, want %d", res.TrailingBytes, sparseSize-committedEnd)
	}
	assertRecoverySize(t, path, sparseSize)
}

func TestRecoverStreaming_EnforcesTrailingBudget(t *testing.T) {
	path, _, _ := writeRecoveryFixture(t, 0, 1, nil)
	appendRecoveryBytes(t, path, []byte("tail"))
	_, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0, MaxTrailingBytes: 3}, func(CommitDescriptor) error { return nil })
	if !errors.Is(err, ErrRecoveryBudgetExceeded) {
		t.Fatalf("error = %v, want recovery budget exceeded", err)
	}
}

func TestRecoverStreaming_EnforcesRecordAndReplayBudgets(t *testing.T) {
	t.Run("replay bytes", func(t *testing.T) {
		path, desc, _ := writeRecoveryFixture(t, 0, 1, nil)
		_, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0, MaxReplayBytes: int64(desc.StoredLen) - 1}, nil)
		if !errors.Is(err, ErrRecoveryBudgetExceeded) {
			t.Fatalf("error = %v, want recovery budget exceeded", err)
		}
	})
	t.Run("records", func(t *testing.T) {
		path, _, end := writeRecoveryFixture(t, 0, 1, nil)
		appendRecoveryRecord(t, path, end, 2)
		_, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0, MaxRecords: 1}, nil)
		if !errors.Is(err, ErrRecoveryBudgetExceeded) {
			t.Fatalf("error = %v, want recovery budget exceeded", err)
		}
	})
}

func TestRecoverStreaming_DefaultRecordLimitIsBounded(t *testing.T) {
	if got := recoveryRecordLimit(RecoverOptions{}); got == 0 || got != recoveryMaxRecords {
		t.Fatalf("zero-option record limit = %d, want hard limit %d", got, recoveryMaxRecords)
	}
	if got := recoveryRecordLimit(RecoverOptions{MaxRecords: 7}); got != 7 {
		t.Fatalf("explicit tighter record limit = %d, want 7", got)
	}
	if got := recoveryRecordLimit(RecoverOptions{MaxRecords: recoveryMaxRecords + 1}); got != recoveryMaxRecords {
		t.Fatalf("oversized record limit = %d, want hard limit %d", got, recoveryMaxRecords)
	}
}

func TestRecoverStreaming_RejectsFormatVersion2(t *testing.T) {
	t.Run("segment", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "7.seg")
		header := SegmentHeader{Magic: storage.SegmentMagic, Version: 2, ID: 7, SegmentClass: storage.SegmentSmall}
		buf := make([]byte, SegmentHeaderSize)
		if err := header.Encode(buf); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, buf, 0644); err != nil {
			t.Fatal(err)
		}
		_, err := RecoverFromSegmentLog(path, RecoverOptions{}, nil)
		if err == nil || !strings.Contains(err.Error(), "unsupported segment version 2") {
			t.Fatalf("error = %v, want explicit V2 rejection", err)
		}
	})
	t.Run("batch commit", func(t *testing.T) {
		path, _, _ := writeRecoveryFixture(t, 0, 1, func(b *journal.BatchCommit) { b.Version = 2 })
		_, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0}, nil)
		if !errors.Is(err, errUnsupportedRecoveryFormat) {
			t.Fatalf("error = %v, want explicit V2 rejection", err)
		}
	})
	t.Run("record", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "7.seg")
		segmentHeader := SegmentHeader{Magic: storage.SegmentMagic, Version: storage.FormatVersion, ID: 7, SegmentClass: storage.SegmentSmall}
		segmentBuf := make([]byte, SegmentHeaderSize)
		if err := segmentHeader.Encode(segmentBuf); err != nil {
			t.Fatal(err)
		}
		recordBuf := validV2RecordHeaderBytes()
		if err := os.WriteFile(path, append(segmentBuf, recordBuf...), 0644); err != nil {
			t.Fatal(err)
		}
		_, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0}, nil)
		if !errors.Is(err, errUnsupportedRecoveryFormat) {
			t.Fatalf("error = %v, want explicit V2 rejection", err)
		}
	})
}

func TestBatchDescriptorCRCIncludesOperation(t *testing.T) {
	put := journal.BatchDescriptor{ExtentID: 1, Generation: 2, SegmentID: 3, Offset: 4, StoredLen: 5, LogicalLen: 6, Checksum: 7, Op: uint8(RecordPut)}
	delete := put
	delete.Op = uint8(RecordDelete)
	putCRC := descriptorCRC([]journal.BatchDescriptor{put})
	if deleteCRC := descriptorCRC([]journal.BatchDescriptor{delete}); deleteCRC == putCRC {
		t.Fatal("descriptor checksum does not bind operation")
	}
}

func TestRecoverStreaming_RejectsUnsafeSafeOffset(t *testing.T) {
	path, _, committedEnd := writeRecoveryFixture(t, 0, 1, nil)
	for _, safeOffset := range []int64{int64(SegmentHeaderSize) + 1, committedEnd + 1} {
		_, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0, SafeOffset: safeOffset}, nil)
		if err == nil {
			t.Fatalf("safe offset %d was accepted", safeOffset)
		}
	}
}

func TestRecoverStreaming_RejectsSafeOffsetInsidePayloadWithCraftedHeader(t *testing.T) {
	payload := bytes.Repeat([]byte{'p'}, RecordHeaderSize+32)
	path, _, _ := writeRecoveryFixtureWithPayload(t, 0, 1, payload, nil)
	// Place a complete, CRC-valid V3 header inside the payload. A boundary
	// check that only decodes at SafeOffset would accept this forged start.
	safeOffset := int64(SegmentHeaderSize + RecordHeaderSize + FrameIndexEntrySize + 7)
	crafted := RecordHeader{Magic: storage.RecordMagic, Version: storage.FormatVersion, Op: RecordPut}
	craftedBytes := make([]byte, RecordHeaderSize)
	if err := crafted.Encode(craftedBytes); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(craftedBytes, safeOffset); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	_, err = RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0, SafeOffset: safeOffset}, nil)
	if err == nil {
		t.Fatal("crafted record header inside payload was accepted as SafeOffset")
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("invalid SafeOffset truncated segment: got %d want %d", after.Size(), before.Size())
	}
}

func TestRecoverStreaming_SafeOffsetSkipsCommittedPrefix(t *testing.T) {
	path, _, safeOffset := writeRecoveryFixture(t, 0, 1, nil)
	appendRecoveryRecord(t, path, safeOffset, 2)

	var applied []CommitDescriptor
	res, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0, SafeOffset: safeOffset}, func(d CommitDescriptor) error {
		applied = append(applied, d)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 1 || applied[0].ExtentID != 12 {
		t.Fatalf("applied = %+v, want only the suffix descriptor", applied)
	}
	if res.Commits != 1 || res.LastSeq != 2 {
		t.Fatalf("result counted the safe prefix: %+v", res)
	}
}

func TestRecoverStreaming_CorruptTailWithVersion2ByteIsTruncated(t *testing.T) {
	path, _, committedEnd := writeRecoveryFixture(t, 0, 1, nil)
	tail := make([]byte, RecordHeaderSize)
	binary.BigEndian.PutUint32(tail[0:4], storage.RecordMagic)
	tail[4] = 2
	appendRecoveryBytes(t, path, tail)

	res, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0}, nil)
	if err != nil {
		t.Fatalf("corrupt tail returned unsupported format: %v", err)
	}
	if res.TrailingBytes != int64(len(tail)) {
		t.Fatalf("trailing bytes = %d, want %d", res.TrailingBytes, len(tail))
	}
	assertRecoverySize(t, path, committedEnd)
}

func TestRecoverStreaming_IndexSafeAdvancesValidBoundary(t *testing.T) {
	path, _, committedEnd := writeRecoveryFixture(t, 0, 1, nil)
	marker := journal.CommitRecord{Seq: 1, Op: journal.OpIndexSafe}
	buf := make([]byte, journal.CommitRecordSize)
	if err := marker.Encode(buf); err != nil {
		t.Fatal(err)
	}
	appendRecoveryBytes(t, path, buf)
	res, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0}, func(CommitDescriptor) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if got, want := res.LastCommittedOffset, committedEnd+int64(journal.CommitRecordSize); got != want {
		t.Fatalf("last valid offset = %d, want %d", got, want)
	}
	if res.SafeSeq != 1 || res.TrailingBytes != 0 {
		t.Fatalf("index-safe result = %+v", res)
	}
}

func writeRecoveryFixture(t *testing.T, streamID uint8, seq uint64, mutate func(*journal.BatchCommit)) (string, CommitDescriptor, int64) {
	return writeRecoveryFixtureWithPayload(t, streamID, seq, []byte("fixture payload"), mutate)
}

func writeRecoveryFixtureWithPayload(t *testing.T, streamID uint8, seq uint64, payload []byte, mutate func(*journal.BatchCommit)) (string, CommitDescriptor, int64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "7.seg")
	header := SegmentHeader{Magic: storage.SegmentMagic, Version: storage.FormatVersion, ID: 7, SegmentClass: storage.SegmentSmall}
	headerBuf := make([]byte, SegmentHeaderSize)
	if err := header.Encode(headerBuf); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, headerBuf, 0644); err != nil {
		t.Fatal(err)
	}

	w, err := OpenWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	frameCRCs, err := BuildFrames(payload, DefaultFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	idx := FrameIndex{Entries: []FrameIndexEntry{{StoredLen: uint32(len(payload)), CRC: frameCRCs[0]}}}
	idxBuf := make([]byte, FrameIndexEntrySize)
	if err := idx.Encode(idxBuf); err != nil {
		t.Fatal(err)
	}
	payloadChecksum := storage.CRC32C(payload)
	record := &RecordHeader{
		Magic: storage.RecordMagic, Version: storage.FormatVersion,
		Op:       RecordPut,
		ExtentID: 11, Generation: 3, LogicalLen: uint32(len(payload)), StoredLen: uint32(len(payload)),
		FrameCount: 1, FrameIndexCRC: idx.CRC, PayloadChecksum: payloadChecksum,
	}
	off := int64(SegmentHeaderSize)
	if _, err := w.WriteRecordFramed(off, record, idxBuf, payload, DefaultFrameSize); err != nil {
		t.Fatal(err)
	}
	desc := CommitDescriptor{ExtentID: 11, Generation: 3, SegmentID: 7, Offset: off, StoredLen: uint32(len(payload)), LogicalLen: uint32(len(payload)), Checksum: payloadChecksum, Op: RecordPut}
	commitOff := off + int64(RecordFraming(record.StoredLen, DefaultFrameSize, 1))
	batch := &journal.BatchCommit{Magic: journal.BatchCommitMagic, Version: storage.FormatVersion, StreamID: streamID, Seq: seq, RecordCount: 1, FirstOffset: off, LastOffset: commitOff}
	batch.DescriptorsCRC = descriptorCRC([]journal.BatchDescriptor{{
		ExtentID: desc.ExtentID, Generation: desc.Generation, SegmentID: desc.SegmentID,
		Offset: desc.Offset, StoredLen: desc.StoredLen, LogicalLen: desc.LogicalLen, Checksum: desc.Checksum, Op: uint8(RecordPut),
	}})
	if mutate != nil {
		mutate(batch)
	}
	if err := w.WriteBatchCommit(commitOff, batch); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	return path, desc, commitOff + int64(journal.BatchCommitSize)
}

func tornRecordBytes(t *testing.T, n int) []byte {
	t.Helper()
	payload := []byte{'x'}
	frameCRC := storage.CRC32C(payload)
	idx := FrameIndex{Entries: []FrameIndexEntry{{StoredLen: 1, CRC: frameCRC}}}
	idxBuf := make([]byte, FrameIndexEntrySize)
	if err := idx.Encode(idxBuf); err != nil {
		t.Fatal(err)
	}
	h := RecordHeader{Magic: storage.RecordMagic, Version: storage.FormatVersion, Op: RecordPut, StoredLen: 1, LogicalLen: 1, FrameCount: 1, FrameIndexCRC: idx.CRC, PayloadChecksum: frameCRC}
	hdr := make([]byte, RecordHeaderSize)
	if err := h.Encode(hdr); err != nil {
		t.Fatal(err)
	}
	full := append(hdr, idxBuf...)
	full = append(full, payload...)
	if n > len(full) {
		n = len(full)
	}
	return append([]byte(nil), full[:n]...)
}

func appendRecoveryBytes(t *testing.T, path string, data []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func appendRecoveryRecord(t *testing.T, path string, off int64, seq uint64) {
	t.Helper()
	w, err := OpenWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	payload := []byte("second fixture payload")
	checksum := storage.CRC32C(payload)
	idx := FrameIndex{Entries: []FrameIndexEntry{{StoredLen: uint32(len(payload)), CRC: checksum}}}
	idxBuf := make([]byte, FrameIndexEntrySize)
	if err := idx.Encode(idxBuf); err != nil {
		t.Fatal(err)
	}
	header := &RecordHeader{Magic: storage.RecordMagic, Version: storage.FormatVersion, Op: RecordPut, ExtentID: storage.ExtentID(10 + seq), Generation: 3, LogicalLen: uint32(len(payload)), StoredLen: uint32(len(payload)), FrameCount: 1, FrameIndexCRC: idx.CRC, PayloadChecksum: checksum}
	if _, err := w.WriteRecordFramed(off, header, idxBuf, payload, DefaultFrameSize); err != nil {
		t.Fatal(err)
	}
	commitOff := off + int64(RecordFraming(header.StoredLen, DefaultFrameSize, 1))
	batch := &journal.BatchCommit{Magic: journal.BatchCommitMagic, Version: storage.FormatVersion, Seq: seq, RecordCount: 1, FirstOffset: off, LastOffset: commitOff}
	batch.DescriptorsCRC = descriptorCRC([]journal.BatchDescriptor{{ExtentID: header.ExtentID, Generation: header.Generation, SegmentID: 7, Offset: off, StoredLen: header.StoredLen, LogicalLen: header.LogicalLen, Checksum: header.PayloadChecksum, Op: uint8(RecordPut)}})
	if err := w.WriteBatchCommit(commitOff, batch); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
}

func assertRecoverySize(t *testing.T, path string, want int64) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != want {
		t.Fatalf("file size = %d, want %d", info.Size(), want)
	}
}

func validV2RecordHeaderBytes() []byte {
	const v2RecordHeaderSize = 50
	buf := make([]byte, v2RecordHeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], storage.RecordMagic)
	buf[4] = 2
	binary.BigEndian.PutUint32(buf[42:46], crc32.ChecksumIEEE(buf[0:42]))
	return buf
}
