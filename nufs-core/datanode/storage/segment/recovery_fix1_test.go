package segment

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/index"
	"github.com/example/dfs/datanode/storage/journal"
)

func TestStoreRecovery_CheckpointSkipsLargeIndexedPrefix(t *testing.T) {
	const (
		indexedRecords = int(storage.MaxRecoveryRecords)
		suffixRecords  = 1
	)
	dir := t.TempDir()
	active := filepath.Join(dir, "segments", "small", "active")
	if err := os.MkdirAll(active, 0755); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(active, "7.seg")
	w := newProductionRecoveryLog(t, path)
	w.appendDeleteRecords(indexedRecords, 256)
	safeSeq := w.seq
	safeOffset := w.appendIndexSafe(safeSeq)
	w.appendDeleteRecords(suffixRecords, 1)
	w.close()

	ix, err := index.Open(index.Options{Dir: filepath.Join(dir, "index")})
	if err != nil {
		t.Fatal(err)
	}
	err = index.StoreRecoveryCheckpoint(ix, index.RecoveryCheckpoint{
		StreamID:   0,
		SegmentID:  7,
		SafeOffset: safeOffset,
		SafeSeq:    safeSeq,
	})
	if closeErr := ix.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}

	s, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("recovery replayed indexed prefix instead of suffix: %v", err)
	}
	defer s.Close()
	if !s.DataReady() {
		t.Fatal("store did not become DataReady")
	}
	res := s.RecoveryResult()
	wantReplayBytes := int64(RecordFraming(0, DefaultFrameSize, 0) + journal.BatchCommitSize)
	if res.SafeOffset != safeOffset || res.SafeSeq != safeSeq || res.Applied != suffixRecords || res.Commits != 1 || res.LastSeq != safeSeq+1 || res.ReplayBytes != wantReplayBytes {
		t.Fatalf("recovery result = %+v, want checkpoint offset=%d seq=%d and one suffix record (%d bytes)", res, safeOffset, safeSeq, wantReplayBytes)
	}
}

func TestFlush_IndexSafePublishesDurableRecoveryCheckpoint(t *testing.T) {
	dir := t.TempDir()
	s, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Write(context.Background(), &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: []byte("checkpoint")}); err != nil {
		t.Fatal(err)
	}
	if err := s.flush(); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := index.LoadRecoveryCheckpoint(s.index, s.streamID)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint == nil || checkpoint.FormatVersion == 0 || checkpoint.StreamID != s.streamID || checkpoint.SegmentID != s.alloc.State().SegmentID || checkpoint.SafeSeq != s.SafeSeq() {
		t.Fatalf("checkpoint = %+v, want durable active-stream INDEX_SAFE boundary", checkpoint)
	}
	info, err := os.Stat(s.writerPath)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.SafeOffset != info.Size() {
		t.Fatalf("checkpoint offset = %d, want synced INDEX_SAFE end %d", checkpoint.SafeOffset, info.Size())
	}
}

func TestStoreRecovery_MarkerWithoutSidecarReplaysConservatively(t *testing.T) {
	dir := t.TempDir()
	active := filepath.Join(dir, "segments", "small", "active")
	if err := os.MkdirAll(active, 0755); err != nil {
		t.Fatal(err)
	}
	w := newProductionRecoveryLog(t, filepath.Join(active, "7.seg"))
	w.appendDeleteRecords(1, 1)
	w.appendIndexSafe(w.seq) // durable marker, deliberately no sidecar
	w.appendDeleteRecords(1, 1)
	w.close()

	s, err := New(Config{Dir: dir, UseMemIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	res := s.RecoveryResult()
	if !res.DataReady || res.SafeOffset != SegmentHeaderSize || res.Applied != 2 {
		t.Fatalf("result = %+v, want conservative header replay of both committed batches", res)
	}
}

func TestStoreRecovery_CorruptCheckpointFailsClosed(t *testing.T) {
	dir := t.TempDir()
	active := filepath.Join(dir, "segments", "small", "active")
	if err := os.MkdirAll(active, 0755); err != nil {
		t.Fatal(err)
	}
	w := newProductionRecoveryLog(t, filepath.Join(active, "7.seg"))
	w.appendDeleteRecords(1, 1)
	safeOffset := w.appendIndexSafe(w.seq)
	w.close()

	ix, err := index.Open(index.Options{Dir: filepath.Join(dir, "index")})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.StoreRecoveryCheckpoint(ix, index.RecoveryCheckpoint{StreamID: 0, SegmentID: 7, SafeOffset: safeOffset, SafeSeq: w.seq}); err != nil {
		_ = ix.Close()
		t.Fatal(err)
	}
	if err := ix.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index", "recovery-checkpoint-0"), []byte("corrupt"), 0644); err != nil {
		t.Fatal(err)
	}

	s, err := New(Config{Dir: dir})
	if err == nil || s != nil {
		t.Fatalf("corrupt recovery checkpoint returned store=%v error=%v, want fail closed", s, err)
	}
}

func TestStoreRecovery_CheckpointForOtherSegmentFallsBack(t *testing.T) {
	dir := t.TempDir()
	active := filepath.Join(dir, "segments", "small", "active")
	if err := os.MkdirAll(active, 0755); err != nil {
		t.Fatal(err)
	}
	w := newProductionRecoveryLog(t, filepath.Join(active, "7.seg"))
	w.appendDeleteRecords(1, 1)
	safeOffset := w.appendIndexSafe(w.seq)
	w.appendDeleteRecords(1, 1)
	w.close()

	ix, err := index.Open(index.Options{Dir: filepath.Join(dir, "index")})
	if err != nil {
		t.Fatal(err)
	}
	err = index.StoreRecoveryCheckpoint(ix, index.RecoveryCheckpoint{StreamID: 0, SegmentID: 6, SafeOffset: safeOffset, SafeSeq: w.seq})
	if closeErr := ix.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}

	s, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if res := s.RecoveryResult(); !res.DataReady || res.SafeOffset != SegmentHeaderSize || res.Applied != 2 {
		t.Fatalf("result = %+v, want header fallback replay", res)
	}
}

func TestStoreRecovery_ReplayBytesUseFullCommittedFramingBoundary(t *testing.T) {
	payload := bytes.Repeat([]byte("p"), 1024)
	wantBytes := int64(RecordFraming(uint32(len(payload)), DefaultFrameSize, 1) + journal.BatchCommitSize)
	for _, tc := range []struct {
		name    string
		limit   int64
		wantErr bool
	}{
		{name: "exact", limit: wantBytes},
		{name: "plus one", limit: wantBytes - 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			active := filepath.Join(dir, "segments", "small", "active")
			if err := os.MkdirAll(active, 0755); err != nil {
				t.Fatal(err)
			}
			path, _, _ := writeRecoveryFixtureWithPayload(t, 0, 1, payload, nil)
			if err := os.Rename(path, filepath.Join(active, "7.seg")); err != nil {
				t.Fatal(err)
			}

			s, err := New(Config{Dir: dir, UseMemIndex: true, recoveryPolicy: recoveryStartupPolicy{maxReplayBytes: tc.limit}})
			if tc.wantErr {
				if !errors.Is(err, storage.ErrRecoveryBudgetExceeded) {
					t.Fatalf("error = %v, want ErrRecoveryBudgetExceeded", err)
				}
				if s != nil {
					t.Fatal("budget failure returned a usable Store")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if res := s.RecoveryResult(); !res.DataReady || res.ReplayBytes != wantBytes {
				t.Fatalf("result = %+v, want DataReady with %d replay bytes", res, wantBytes)
			}
		})
	}
}

func TestStoreRecovery_TrailingBytesBoundaryUsesStoreStartup(t *testing.T) {
	for _, tc := range []struct {
		name    string
		tail    int
		wantErr bool
	}{
		{name: "exact", tail: 4},
		{name: "plus one", tail: 5, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			active := filepath.Join(dir, "segments", "small", "active")
			if err := os.MkdirAll(active, 0755); err != nil {
				t.Fatal(err)
			}
			path, _, end := writeRecoveryFixture(t, 0, 1, nil)
			appendRecoveryBytes(t, path, bytes.Repeat([]byte("x"), tc.tail))
			if err := os.Rename(path, filepath.Join(active, "7.seg")); err != nil {
				t.Fatal(err)
			}

			s, err := New(Config{Dir: dir, UseMemIndex: true, recoveryPolicy: recoveryStartupPolicy{maxTrailingBytes: int64(4)}})
			if tc.wantErr {
				if !errors.Is(err, storage.ErrRecoveryBudgetExceeded) || s != nil {
					t.Fatalf("result store=%v error=%v, want unavailable Store and canonical budget error", s, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if res := s.RecoveryResult(); !res.DataReady || res.TrailingBytes != int64(tc.tail) {
				t.Fatalf("result = %+v, want successful %d-byte tail truncation", res, tc.tail)
			}
			assertRecoverySize(t, filepath.Join(active, "7.seg"), end)
		})
	}
}

func TestStoreRecovery_DeadlineBoundaryIncludesWriterSetup(t *testing.T) {
	for _, tc := range []struct {
		name    string
		now     time.Duration
		wantErr bool
	}{
		{name: "exact", now: storage.RecoveryBudget},
		{name: "plus epsilon", now: storage.RecoveryBudget + time.Nanosecond, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			active := filepath.Join(dir, "segments", "small", "active")
			if err := os.MkdirAll(active, 0755); err != nil {
				t.Fatal(err)
			}
			path, _, _ := writeRecoveryFixture(t, 0, 1, nil)
			if err := os.Rename(path, filepath.Join(active, "7.seg")); err != nil {
				t.Fatal(err)
			}
			start := time.Unix(1_000, 0)
			calls := 0
			s, err := New(Config{
				Dir:         dir,
				UseMemIndex: true,
				RecoveryClock: func() time.Time {
					calls++
					if calls < 5 {
						return start
					}
					return start.Add(tc.now)
				},
			})
			if tc.wantErr {
				if !errors.Is(err, storage.ErrRecoveryBudgetExceeded) || s != nil {
					t.Fatalf("result store=%v error=%v, want unavailable Store and canonical budget error", s, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if res := s.RecoveryResult(); !res.DataReady || res.Duration != storage.RecoveryBudget {
				t.Fatalf("result = %+v, want DataReady and full %s startup duration", res, storage.RecoveryBudget)
			}
		})
	}
}

func TestRecoverBudget_DeadlineBoundaryReportsFailurePoint(t *testing.T) {
	path, _, _ := writeRecoveryFixture(t, 0, 1, nil)
	start := time.Unix(2_000, 0)
	for _, tc := range []struct {
		name    string
		now     time.Duration
		wantErr bool
	}{
		{name: "exact", now: storage.RecoveryBudget},
		{name: "plus epsilon", now: storage.RecoveryBudget + time.Nanosecond, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := RecoverFromSegmentLog(path, RecoverOptions{
				StreamID:  0,
				StartedAt: start,
				Deadline:  start.Add(storage.RecoveryBudget),
				Clock: func() time.Time {
					return start.Add(tc.now)
				},
			}, nil)
			if tc.wantErr {
				if !errors.Is(err, storage.ErrRecoveryBudgetExceeded) || res == nil || res.DataReady || res.Duration != tc.now {
					t.Fatalf("result=%+v error=%v, want non-ready duration %s", res, err, tc.now)
				}
				return
			}
			if err != nil || res == nil || !res.DataReady || res.Duration != tc.now {
				t.Fatalf("result=%+v error=%v, want ready duration %s", res, err, tc.now)
			}
		})
	}
}

type productionRecoveryLog struct {
	t      *testing.T
	path   string
	f      *os.File
	off    int64
	seq    uint64
	nextID storage.ExtentID
}

func newProductionRecoveryLog(t *testing.T, path string) *productionRecoveryLog {
	t.Helper()
	writeRecoverySegmentHeader(t, path)
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	return &productionRecoveryLog{t: t, path: path, f: f, off: SegmentHeaderSize, nextID: 1}
}

func (w *productionRecoveryLog) appendDeleteRecords(count, batchSize int) {
	w.t.Helper()
	if batchSize <= 0 || batchSize > defaultGroupCommitConfig().MaxBatch {
		w.t.Fatalf("invalid production batch size %d", batchSize)
	}
	for count > 0 {
		n := count
		if n > batchSize {
			n = batchSize
		}
		descs := make([]journal.BatchDescriptor, 0, n)
		first := w.off
		for i := 0; i < n; i++ {
			header := RecordHeader{Magic: storage.RecordMagic, Version: storage.FormatVersion, Op: RecordDelete, ExtentID: w.nextID, Generation: 1}
			headerBuf := make([]byte, RecordHeaderSize)
			if err := header.Encode(headerBuf); err != nil {
				w.t.Fatal(err)
			}
			trailer := RecordTrailer{FramingLen: RecordHeaderSize + RecordTrailerSize}
			trailerBuf := make([]byte, RecordTrailerSize)
			if err := trailer.Encode(trailerBuf); err != nil {
				w.t.Fatal(err)
			}
			if _, err := w.f.WriteAt(headerBuf, w.off); err != nil {
				w.t.Fatal(err)
			}
			if _, err := w.f.WriteAt(trailerBuf, w.off+RecordHeaderSize); err != nil {
				w.t.Fatal(err)
			}
			descs = append(descs, journal.BatchDescriptor{ExtentID: w.nextID, Generation: 1, SegmentID: 7, Offset: w.off, Op: uint8(RecordDelete)})
			w.off += RecordHeaderSize + RecordTrailerSize
			w.nextID++
		}
		w.seq++
		commit := journal.BatchCommit{Magic: journal.BatchCommitMagic, Version: storage.FormatVersion, StreamID: 0, Seq: w.seq, RecordCount: uint32(n), FirstOffset: first, LastOffset: w.off, DescriptorsCRC: descriptorCRC(descs)}
		commitBuf := make([]byte, journal.BatchCommitSize)
		if err := commit.Encode(commitBuf); err != nil {
			w.t.Fatal(err)
		}
		if _, err := w.f.WriteAt(commitBuf, w.off); err != nil {
			w.t.Fatal(err)
		}
		w.off += journal.BatchCommitSize
		count -= n
	}
}

func (w *productionRecoveryLog) appendIndexSafe(seq uint64) int64 {
	w.t.Helper()
	marker := journal.CommitRecord{Seq: seq, Op: journal.OpIndexSafe}
	buf := make([]byte, journal.CommitRecordSize)
	if err := marker.Encode(buf); err != nil {
		w.t.Fatal(err)
	}
	if _, err := w.f.WriteAt(buf, w.off); err != nil {
		w.t.Fatal(err)
	}
	w.off += journal.CommitRecordSize
	return w.off
}

func (w *productionRecoveryLog) close() {
	w.t.Helper()
	if err := w.f.Sync(); err != nil {
		w.t.Fatal(err)
	}
	if err := w.f.Close(); err != nil {
		w.t.Fatal(err)
	}
}
