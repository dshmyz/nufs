package segment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/journal"
)

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
		clock:    now,
		deadline: start.Add(storage.RecoveryBudget),
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
		recoveryClock: func() time.Time {
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
		recoveryClock: func() time.Time {
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

func TestRecoverBudget_ByteLimitsClampToCanonicalCaps(t *testing.T) {
	tests := []struct {
		name          string
		replay, trail int64
		wantReplay    int64
		wantTrailing  int64
	}{
		{name: "zero", wantReplay: storage.MaxRecoveryReplayBytes, wantTrailing: storage.MaxRecoveryTrailingBytes},
		{name: "negative", replay: -1, trail: -1, wantReplay: storage.MaxRecoveryReplayBytes, wantTrailing: storage.MaxRecoveryTrailingBytes},
		{name: "exact cap", replay: storage.MaxRecoveryReplayBytes, trail: storage.MaxRecoveryTrailingBytes, wantReplay: storage.MaxRecoveryReplayBytes, wantTrailing: storage.MaxRecoveryTrailingBytes},
		{name: "below cap", replay: storage.MaxRecoveryReplayBytes - 1, trail: storage.MaxRecoveryTrailingBytes - 1, wantReplay: storage.MaxRecoveryReplayBytes - 1, wantTrailing: storage.MaxRecoveryTrailingBytes - 1},
		{name: "above cap", replay: storage.MaxRecoveryReplayBytes + 1, trail: storage.MaxRecoveryTrailingBytes + 1, wantReplay: storage.MaxRecoveryReplayBytes, wantTrailing: storage.MaxRecoveryTrailingBytes},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotReplay, gotTrailing := recoveryByteLimits(RecoverOptions{MaxReplayBytes: tc.replay, MaxTrailingBytes: tc.trail})
			if gotReplay != tc.wantReplay || gotTrailing != tc.wantTrailing {
				t.Fatalf("limits = (%d, %d), want (%d, %d)", gotReplay, gotTrailing, tc.wantReplay, tc.wantTrailing)
			}
		})
	}
}

func TestRecoverBudget_NegativeTrailingLimitCannotDisableCanonicalBudget(t *testing.T) {
	path, _, committedEnd := writeRecoveryFixture(t, 0, 1, nil)
	size := committedEnd + storage.MaxRecoveryTrailingBytes + 1
	if err := os.Truncate(path, size); err != nil {
		t.Fatal(err)
	}
	res, err := RecoverFromSegmentLog(path, RecoverOptions{StreamID: 0, MaxTrailingBytes: -1}, nil)
	if !errors.Is(err, ErrRecoveryBudgetExceeded) {
		t.Fatalf("error = %v, want recovery budget exceeded", err)
	}
	if res == nil || res.DataReady || res.TrailingBytes != size-committedEnd {
		t.Fatalf("result = %+v, want unsuccessful recovery with %d-byte tail", res, size-committedEnd)
	}
	assertRecoverySize(t, path, size)
}

func TestStoreRecovery_StatFailureDoesNotReturnDataReadyStore(t *testing.T) {
	dir := t.TempDir()
	active := filepath.Join(dir, "segments", "small", "active")
	if err := os.MkdirAll(active, 0755); err != nil {
		t.Fatal(err)
	}
	path, _, _ := writeRecoveryFixture(t, 0, 1, nil)
	if err := os.Rename(path, filepath.Join(active, "7.seg")); err != nil {
		t.Fatal(err)
	}

	stat := recoveryStat
	recoveryStat = func(path string) (os.FileInfo, error) {
		if filepath.Base(path) == "7.seg" {
			return nil, os.ErrPermission
		}
		return stat(path)
	}
	t.Cleanup(func() { recoveryStat = stat })

	s, err := New(Config{Dir: dir, UseMemIndex: true})
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("error = %v, want permission failure", err)
	}
	if s != nil {
		t.Fatal("stat failure returned a DataReady store")
	}
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
