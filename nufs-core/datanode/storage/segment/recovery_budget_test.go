package segment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/journal"
)

func TestRecoverBudget_CanonicalBoundaries(t *testing.T) {
	t.Run("records", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			records int
			wantErr bool
		}{
			{name: "exact", records: int(storage.MaxRecoveryRecords)},
			{name: "plus one", records: int(storage.MaxRecoveryRecords) + 1, wantErr: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				path, end := writeRecoveryDeleteBatch(t, tc.records)
				res, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0, MaxRecords: storage.MaxRecoveryRecords}, func(CommitDescriptor) error { return nil })
				if tc.wantErr {
					if !errors.Is(err, storage.ErrRecoveryBudgetExceeded) {
						t.Fatalf("error = %v, want ErrRecoveryBudgetExceeded", err)
					}
					if res == nil || res.DataReady {
						t.Fatalf("failed recovery result = %+v, want DataReady=false", res)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if !res.DataReady || res.Applied != tc.records || res.LastCommittedOffset != end {
					t.Fatalf("result = %+v, want ready %d applied records through %d", res, tc.records, end)
				}
			})
		}
	})

	t.Run("replay bytes", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			bytes   uint32
			wantErr bool
		}{
			{name: "exact", bytes: uint32(storage.MaxRecoveryReplayBytes)},
			{name: "plus one", bytes: uint32(storage.MaxRecoveryReplayBytes + 1), wantErr: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				path, end := writeSparseRecoveryPutBatch(t, tc.bytes)
				res, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0, MaxReplayBytes: storage.MaxRecoveryReplayBytes}, func(CommitDescriptor) error { return nil })
				if tc.wantErr {
					if !errors.Is(err, storage.ErrRecoveryBudgetExceeded) || res == nil || res.DataReady {
						t.Fatalf("result=%+v error=%v, want non-ready budget failure", res, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if !res.DataReady || res.Applied != 1 || res.LastCommittedOffset != end {
					t.Fatalf("result = %+v, want actual successful replay", res)
				}
			})
		}
	})

	t.Run("trailing bytes", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			tail    int64
			wantErr bool
		}{
			{name: "exact", tail: storage.MaxRecoveryTrailingBytes},
			{name: "plus one", tail: storage.MaxRecoveryTrailingBytes + 1, wantErr: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				path, _, end := writeRecoveryFixture(t, 0, 1, nil)
				if err := os.Truncate(path, end+tc.tail); err != nil {
					t.Fatal(err)
				}
				res, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0, MaxTrailingBytes: storage.MaxRecoveryTrailingBytes}, func(CommitDescriptor) error { return nil })
				if tc.wantErr {
					if !errors.Is(err, storage.ErrRecoveryBudgetExceeded) || res == nil || res.DataReady {
						t.Fatalf("result=%+v error=%v, want non-ready budget failure", res, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if !res.DataReady || res.TrailingBytes != tc.tail || res.LastCommittedOffset != end {
					t.Fatalf("result = %+v, want successful truncation", res)
				}
				assertRecoverySize(t, path, end)
			})
		}
	})
}

func TestRecoverBudget_DeadlineLeavesDataNotReady(t *testing.T) {
	path, _, _ := writeRecoveryFixture(t, 0, 1, nil)
	start := time.Unix(100, 0)
	calls := 0
	now := func() time.Time {
		calls++
		if calls == 1 {
			return start
		}
		return start.Add(storage.RecoveryBudget + time.Nanosecond)
	}
	res, err := RecoverFromSegmentLog(path, RecoverOptions{
		StreamID: 0,
		Clock:    now,
		Deadline: start.Add(storage.RecoveryBudget),
	}, func(CommitDescriptor) error { return nil })
	if !errors.Is(err, storage.ErrRecoveryBudgetExceeded) {
		t.Fatalf("error = %v, want ErrRecoveryBudgetExceeded", err)
	}
	if res == nil || res.DataReady {
		t.Fatalf("failed recovery result = %+v, want DataReady=false", res)
	}
}

func TestStoreRecovery_ResultBecomesDataReadyOnlyAfterSuccessfulReplay(t *testing.T) {
	dir := t.TempDir()
	active := filepath.Join(dir, "segments", "small", "active")
	if err := os.MkdirAll(active, 0755); err != nil {
		t.Fatal(err)
	}
	path, _, end := writeRecoveryFixture(t, 0, 1, nil)
	marker := journal.CommitRecord{Seq: 1, Op: journal.OpIndexSafe}
	markerBuf := make([]byte, journal.CommitRecordSize)
	if err := marker.Encode(markerBuf); err != nil {
		t.Fatal(err)
	}
	appendRecoveryBytes(t, path, markerBuf)
	end += journal.CommitRecordSize
	tail := []byte("uncommitted-tail")
	appendRecoveryBytes(t, path, tail)
	if err := os.Rename(path, filepath.Join(active, "7.seg")); err != nil {
		t.Fatal(err)
	}
	start := time.Unix(200, 0)
	calls := 0
	s, err := New(Config{
		Dir:         dir,
		UseMemIndex: true,
		RecoveryClock: func() time.Time {
			calls++
			if calls == 1 {
				return start
			}
			return start.Add(2 * time.Second)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !s.DataReady() {
		t.Fatal("store served before successful recovery marked DataReady")
	}
	res := s.RecoveryResult()
	if !res.DataReady || res.Applied != 1 || res.LastSeq != 1 || res.SafeSeq != 1 || res.LastCommittedOffset != end || res.TrailingBytes != int64(len(tail)) || res.Duration != 2*time.Second {
		t.Fatalf("result = %+v, want actual replay/truncation result", res)
	}
}

func TestStoreRecovery_DeadlineDoesNotReturnUsableStore(t *testing.T) {
	dir := t.TempDir()
	active := filepath.Join(dir, "segments", "small", "active")
	if err := os.MkdirAll(active, 0755); err != nil {
		t.Fatal(err)
	}
	path, _, _ := writeRecoveryFixture(t, 0, 1, nil)
	if err := os.Rename(path, filepath.Join(active, "7.seg")); err != nil {
		t.Fatal(err)
	}
	start := time.Unix(300, 0)
	calls := 0
	s, err := New(Config{
		Dir:         dir,
		UseMemIndex: true,
		RecoveryClock: func() time.Time {
			calls++
			if calls == 1 {
				return start
			}
			return start.Add(storage.RecoveryBudget + time.Nanosecond)
		},
	})
	if !errors.Is(err, storage.ErrRecoveryBudgetExceeded) {
		t.Fatalf("error = %v, want ErrRecoveryBudgetExceeded", err)
	}
	if s != nil {
		t.Fatal("deadline-exceeded recovery returned a usable store")
	}
}

func writeRecoveryDeleteBatch(t *testing.T, count int) (string, int64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "7.seg")
	writeRecoverySegmentHeader(t, path)
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	off := int64(SegmentHeaderSize)
	descs := make([]journal.BatchDescriptor, 0, count)
	for i := 0; i < count; i++ {
		header := &RecordHeader{Magic: storage.RecordMagic, Version: storage.FormatVersion, Op: RecordDelete, ExtentID: storage.ExtentID(i + 1), Generation: 1}
		buf := make([]byte, RecordHeaderSize)
		if err := header.Encode(buf); err != nil {
			t.Fatal(err)
		}
		trailer := &RecordTrailer{FramingLen: RecordHeaderSize + RecordTrailerSize}
		trailerBuf := make([]byte, RecordTrailerSize)
		if err := trailer.Encode(trailerBuf); err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(buf, off); err != nil {
			f.Close()
			t.Fatal(err)
		}
		if _, err := f.WriteAt(trailerBuf, off+RecordHeaderSize); err != nil {
			f.Close()
			t.Fatal(err)
		}
		descs = append(descs, journal.BatchDescriptor{ExtentID: storage.ExtentID(i + 1), Generation: 1, SegmentID: 7, Offset: off, Op: uint8(RecordDelete)})
		off += RecordHeaderSize + RecordTrailerSize
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path, appendRecoveryBatchCommit(t, path, descs, off)
}

func writeSparseRecoveryPutBatch(t *testing.T, storedLen uint32) (string, int64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "7.seg")
	writeRecoverySegmentHeader(t, path)
	off := int64(SegmentHeaderSize)
	idx := FrameIndex{Entries: []FrameIndexEntry{{StoredLen: storedLen}}}
	idxBuf := make([]byte, FrameIndexEntrySize)
	if err := idx.Encode(idxBuf); err != nil {
		t.Fatal(err)
	}
	header := &RecordHeader{Magic: storage.RecordMagic, Version: storage.FormatVersion, Op: RecordPut, ExtentID: 1, Generation: 1, StoredLen: storedLen, LogicalLen: storedLen, FrameCount: 1, FrameIndexCRC: idx.CRC}
	headerBuf := make([]byte, RecordHeaderSize)
	if err := header.Encode(headerBuf); err != nil {
		t.Fatal(err)
	}
	trailerOff := off + RecordHeaderSize + FrameIndexEntrySize + int64(storedLen)
	if err := os.Truncate(path, trailerOff+RecordTrailerSize); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(headerBuf, off); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if _, err := f.WriteAt(idxBuf, off+RecordHeaderSize); err != nil {
		f.Close()
		t.Fatal(err)
	}
	trailer := &RecordTrailer{FramingLen: uint32(RecordHeaderSize + FrameIndexEntrySize + int(storedLen) + RecordTrailerSize)}
	trailerBuf := make([]byte, RecordTrailerSize)
	if err := trailer.Encode(trailerBuf); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if _, err := f.WriteAt(trailerBuf, trailerOff); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	desc := journal.BatchDescriptor{ExtentID: 1, Generation: 1, SegmentID: 7, Offset: off, StoredLen: storedLen, LogicalLen: storedLen, Op: uint8(RecordPut)}
	return path, appendRecoveryBatchCommit(t, path, []journal.BatchDescriptor{desc}, trailerOff+RecordTrailerSize)
}

func writeRecoverySegmentHeader(t *testing.T, path string) {
	t.Helper()
	header := SegmentHeader{Magic: storage.SegmentMagic, Version: storage.FormatVersion, ID: 7, SegmentClass: storage.SegmentSmall}
	buf := make([]byte, SegmentHeaderSize)
	if err := header.Encode(buf); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf, 0644); err != nil {
		t.Fatal(err)
	}
}

func appendRecoveryBatchCommit(t *testing.T, path string, descs []journal.BatchDescriptor, off int64) int64 {
	t.Helper()
	commit := &journal.BatchCommit{Magic: journal.BatchCommitMagic, Version: storage.FormatVersion, StreamID: 0, Seq: 1, RecordCount: uint32(len(descs)), FirstOffset: descs[0].Offset, LastOffset: off, DescriptorsCRC: descriptorCRC(descs)}
	buf := make([]byte, journal.BatchCommitSize)
	if err := commit.Encode(buf); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(buf, off); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return off + journal.BatchCommitSize
}
