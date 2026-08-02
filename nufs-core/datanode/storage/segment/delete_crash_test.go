package segment

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/index"
	"github.com/example/dfs/datanode/storage/journal"
	"github.com/example/dfs/datanode/storage/testutil"
)

// TestDelete_AcknowledgedBeforeIndexApplySurvivesRecovery restores an index
// directory copied before delete acknowledgement. The durable segment log is
// therefore the only source that can prove the delete during restart.
func TestDelete_AcknowledgedBeforeIndexApplySurvivesRecovery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	const (
		extentID   = storage.ExtentID(307)
		generation = storage.Generation(11)
	)

	baseline, err := New(Config{Dir: dir, UseMemIndex: false, FlushInterval: time.Hour, disableAsyncApply: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := baseline.Write(ctx, &storage.WriteRequest{ExtentID: extentID, Generation: generation, Data: []byte("pre-delete")}); err != nil {
		baseline.Close()
		t.Fatal(err)
	}
	if err := baseline.Close(); err != nil {
		t.Fatal(err)
	}

	preDeleteIndex := filepath.Join(t.TempDir(), "index")
	copyTestTree(t, filepath.Join(dir, "index"), preDeleteIndex)

	deleting, err := New(Config{Dir: dir, UseMemIndex: false, FlushInterval: time.Hour, disableAsyncApply: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := deleting.Delete(ctx, &storage.DeleteRequest{ExtentID: extentID, Generation: generation}); err != nil {
		crashStoreForTest(t, deleting)
		t.Fatal(err)
	}
	crashStoreForTest(t, deleting)

	indexDir := filepath.Join(dir, "index")
	if err := os.RemoveAll(indexDir); err != nil {
		t.Fatal(err)
	}
	copyTestTree(t, preDeleteIndex, indexDir)

	reopened, err := New(Config{Dir: dir, UseMemIndex: false, FlushInterval: time.Hour, disableAsyncApply: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Read(ctx, &storage.ReadRequest{ExtentID: extentID, Generation: generation}); !errors.Is(err, storage.ErrExtentNotFound) {
		t.Fatalf("read after recovered delete = %v, want ErrExtentNotFound", err)
	}
}

func TestDelete_GenerationFencing(t *testing.T) {
	s := newTestStore(t, nil)
	ctx := context.Background()
	const extentID = storage.ExtentID(308)

	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: extentID, Generation: 1, Data: []byte("generation one")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, &storage.DeleteRequest{ExtentID: extentID, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(ctx, &storage.ReadRequest{ExtentID: extentID, Generation: 1}); !errors.Is(err, storage.ErrExtentNotFound) {
		t.Fatalf("put-delete read = %v, want ErrExtentNotFound", err)
	}

	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: extentID, Generation: 2, Data: []byte("generation two")}); err != nil {
		t.Fatal(err)
	}
	// A repeat delete is idempotent and a stale-generation delete must not
	// touch the exact higher-generation key.
	if err := s.Delete(ctx, &storage.DeleteRequest{ExtentID: extentID, Generation: 1}); err != nil {
		t.Fatalf("duplicate/stale delete: %v", err)
	}
	got, err := s.Read(ctx, &storage.ReadRequest{ExtentID: extentID, Generation: 2})
	if err != nil {
		t.Fatalf("put-delete-new-generation read: %v", err)
	}
	if string(got.Data) != "generation two" {
		t.Fatalf("generation two data = %q", got.Data)
	}
}

func TestDelete_CrashBeforeSyncHasNoAcknowledgementOrVisibility(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	const (
		extentID   = storage.ExtentID(309)
		generation = storage.Generation(1)
	)
	s, err := New(Config{Dir: dir, UseMemIndex: false, FlushInterval: time.Hour, disableAsyncApply: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: extentID, Generation: generation, Data: []byte("live")}); err != nil {
		crashStoreForTest(t, s)
		t.Fatal(err)
	}
	durableInfo, err := os.Stat(s.writerPath)
	if err != nil {
		crashStoreForTest(t, s)
		t.Fatal(err)
	}
	durablePath := s.writerPath
	s.faults = testutil.NewScriptedFaults([]testutil.Step{{
		Point: storage.CrashAfterBatchCommitWrite,
		Err:   testutil.ErrSimulatedCrash,
	}})

	if err := s.Delete(ctx, &storage.DeleteRequest{ExtentID: extentID, Generation: generation}); !errors.Is(err, testutil.ErrSimulatedCrash) {
		crashStoreForTest(t, s)
		t.Fatalf("delete error = %v, want simulated crash", err)
	}
	// This is an abrupt process-death simulation, deliberately not Store.Close:
	// close descriptors/index only, then drop the bytes written after the last
	// known sync to model an unsynced delete tail being lost by the OS.
	crashStoreForTest(t, s)
	if err := os.Truncate(durablePath, durableInfo.Size()); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(Config{Dir: dir, UseMemIndex: false, FlushInterval: time.Hour, disableAsyncApply: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Read(ctx, &storage.ReadRequest{ExtentID: extentID, Generation: generation})
	if err != nil {
		t.Fatalf("old value absent after unacknowledged delete restart: %v", err)
	}
	if string(got.Data) != "live" {
		t.Fatalf("recovered data = %q, want live", got.Data)
	}
}

func TestDelete_AfterSyncSurvivesWithoutPebbleTombstone(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	const (
		extentID   = storage.ExtentID(310)
		generation = storage.Generation(1)
	)
	s, err := New(Config{Dir: dir, UseMemIndex: false, FlushInterval: time.Hour, disableAsyncApply: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: extentID, Generation: generation, Data: []byte("live")}); err != nil {
		crashStoreForTest(t, s)
		t.Fatal(err)
	}
	live, err := s.lookup(extentID, generation)
	if err != nil {
		crashStoreForTest(t, s)
		t.Fatal(err)
	}
	if err := s.applyNow([]index.Mutation{{ExtentID: extentID, Generation: generation, Value: *live}}); err != nil {
		crashStoreForTest(t, s)
		t.Fatal(err)
	}
	if err := s.Delete(ctx, &storage.DeleteRequest{ExtentID: extentID, Generation: generation}); err != nil {
		crashStoreForTest(t, s)
		t.Fatal(err)
	}
	pebbleValue, err := s.index.Get(extentID, generation)
	if err != nil {
		crashStoreForTest(t, s)
		t.Fatal(err)
	}
	if pebbleValue.State != storage.ExtentDurable {
		crashStoreForTest(t, s)
		t.Fatalf("Pebble state = %v, want pre-delete durable value", pebbleValue.State)
	}
	crashStoreForTest(t, s)

	reopened, err := New(Config{Dir: dir, UseMemIndex: false, FlushInterval: time.Hour, disableAsyncApply: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Read(ctx, &storage.ReadRequest{ExtentID: extentID, Generation: generation}); !errors.Is(err, storage.ErrExtentNotFound) {
		t.Fatalf("read after crash recovery without Pebble tombstone = %v, want ErrExtentNotFound", err)
	}
}

func TestDelete_UsesFramedRecordDescriptorBatchCommitAndOneSync(t *testing.T) {
	s := newTestStore(t, nil)
	ctx := context.Background()
	const (
		extentID   = storage.ExtentID(311)
		generation = storage.Generation(1)
	)
	receipt, err := s.Write(ctx, &storage.WriteRequest{ExtentID: extentID, Generation: generation, Data: []byte("live")})
	if err != nil {
		t.Fatal(err)
	}
	beforeSync := s.syncCalls.Load()
	if err := s.Delete(ctx, &storage.DeleteRequest{ExtentID: extentID, Generation: generation}); err != nil {
		t.Fatal(err)
	}
	if got := s.syncCalls.Load(); got != beforeSync+1 {
		t.Fatalf("delete sync calls = %d, want %d", got, beforeSync+1)
	}

	tombstone, err := s.lookup(extentID, generation)
	if err != nil {
		t.Fatal(err)
	}
	if tombstone.State != storage.ExtentTombstoned {
		t.Fatalf("overlay state = %v, want tombstone", tombstone.State)
	}
	segmentBytes, err := os.ReadFile(s.writerPath)
	if err != nil {
		t.Fatal(err)
	}
	deleteOffset := receipt.Offset + int64(RecordFraming(receipt.StoredLen, DefaultFrameSize, 1)) + int64(journal.BatchCommitSize)
	deleteHeaderBytes := segmentBytes[deleteOffset : deleteOffset+RecordHeaderSize]
	var deleteHeader RecordHeader
	if err := deleteHeader.Decode(deleteHeaderBytes); err != nil {
		t.Fatal(err)
	}
	if deleteHeader.Op != RecordDelete || deleteHeader.ExtentID != extentID || deleteHeader.Generation != generation || deleteHeader.FrameCount != 0 {
		t.Fatalf("delete header = %+v", deleteHeader)
	}
	commitOffset := deleteOffset + int64(RecordFraming(0, DefaultFrameSize, 0))
	var commit journal.BatchCommit
	if err := commit.Decode(segmentBytes[commitOffset : commitOffset+journal.BatchCommitSize]); err != nil {
		t.Fatal(err)
	}
	if commit.RecordCount != 1 || commit.FirstOffset != deleteOffset || commit.LastOffset != commitOffset {
		t.Fatalf("delete batch commit = %+v", commit)
	}
	wantCRC := descriptorCRC([]journal.BatchDescriptor{{
		ExtentID: extentID, Generation: generation, SegmentID: tombstone.SegmentID,
		Offset: deleteOffset, StoredLen: 0, LogicalLen: 0, Checksum: 0, Op: uint8(RecordDelete),
	}})
	if commit.DescriptorsCRC != wantCRC {
		t.Fatalf("delete descriptor CRC = %x, want %x", commit.DescriptorsCRC, wantCRC)
	}
}

func copyTestTree(t *testing.T, source, destination string) {
	t.Helper()
	if err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0644)
	}); err != nil {
		t.Fatal(err)
	}
}
