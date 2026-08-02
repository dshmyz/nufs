package segment

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/journal"
)

func TestCommitLayout_ConcurrentBatchIsContiguous(t *testing.T) {
	dir := t.TempDir()
	s, err := New(Config{Dir: dir, UseMemIndex: true, SegmentSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.group.close()
	beforeWait := make(chan struct{})
	releaseWait := make(chan struct{})
	s.group = newGroupCommitCoordinator(groupCommitConfig{
		MaxBatch: 2,
		MaxWait:  time.Second,
		beforeWait: func() {
			close(beforeWait)
			<-releaseWait
		},
	})

	type result struct {
		receipt *storage.DurableReceipt
		err     error
	}
	results := make(chan result, 2)
	write := func(id storage.ExtentID, data string) {
		receipt, err := s.Write(context.Background(), &storage.WriteRequest{
			ExtentID:   id,
			Generation: 1,
			Data:       []byte(data),
		})
		results <- result{receipt: receipt, err: err}
	}

	go write(1, "first-record")
	select {
	case <-beforeWait:
	case <-time.After(time.Second):
		close(releaseWait)
		t.Fatal("coordinator did not reach batch wait")
	}
	go write(2, "second-record")
	close(releaseWait)

	receipts := make([]*storage.DurableReceipt, 0, 2)
	for range 2 {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatal(got.err)
			}
			receipts = append(receipts, got.receipt)
		case <-time.After(time.Second):
			t.Fatal("concurrent batch did not complete")
		}
	}
	if got := s.syncCalls.Load(); got != 1 {
		t.Fatalf("sync calls = %d, want one concurrent batch", got)
	}
	sort.Slice(receipts, func(i, j int) bool { return receipts[i].Offset < receipts[j].Offset })

	path := filepath.Join(dir, "segments", "small", "active", fmt.Sprintf("%d.seg", receipts[0].SegmentID))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	framingAt := func(offset int64) int64 {
		t.Helper()
		if offset < SegmentHeaderSize || offset+RecordHeaderSize > int64(len(raw)) {
			t.Fatalf("record offset %d outside segment length %d", offset, len(raw))
		}
		var header RecordHeader
		if err := header.Decode(raw[offset : offset+RecordHeaderSize]); err != nil {
			t.Fatalf("decode record at %d: %v", offset, err)
		}
		return int64(RecordFraming(header.StoredLen, header.EffectiveFrameSize(), int(header.FrameCount)))
	}

	firstOffset := receipts[0].Offset
	firstFraming := framingAt(firstOffset)
	secondOffset := receipts[1].Offset
	if got, want := secondOffset, firstOffset+firstFraming; got != want {
		t.Fatalf("record gap: second offset=%d want=%d", got, want)
	}
	lastOffset := secondOffset
	lastFraming := framingAt(lastOffset)
	commitOffset := lastOffset + lastFraming
	if commitOffset+int64(journal.BatchCommitSize) > int64(len(raw)) {
		t.Fatalf("commit offset %d outside segment length %d", commitOffset, len(raw))
	}
	var commit journal.BatchCommit
	if err := commit.Decode(raw[commitOffset : commitOffset+int64(journal.BatchCommitSize)]); err != nil {
		t.Fatalf("commit offset=%d want contiguous offset=%d: %v", commitOffset, lastOffset+lastFraming, err)
	}
	if commit.RecordCount != 2 {
		t.Fatalf("commit record count = %d, want 2", commit.RecordCount)
	}
	if got, want := commit.FirstOffset, firstOffset; got != want {
		t.Fatalf("commit first offset=%d want=%d", got, want)
	}
	if got, want := commit.LastOffset, commitOffset; got != want {
		t.Fatalf("commit last offset=%d want contiguous offset=%d", got, want)
	}
	firstHeader := RecordHeader{}
	if err := firstHeader.Decode(raw[firstOffset : firstOffset+RecordHeaderSize]); err != nil {
		t.Fatal(err)
	}
	secondHeader := RecordHeader{}
	if err := secondHeader.Decode(raw[secondOffset : secondOffset+RecordHeaderSize]); err != nil {
		t.Fatal(err)
	}
	descs := []journal.BatchDescriptor{
		{ExtentID: firstHeader.ExtentID, Generation: firstHeader.Generation, SegmentID: receipts[0].SegmentID, Offset: firstOffset, StoredLen: firstHeader.StoredLen, LogicalLen: firstHeader.LogicalLen, Checksum: firstHeader.PayloadChecksum, Op: uint8(firstHeader.Op)},
		{ExtentID: secondHeader.ExtentID, Generation: secondHeader.Generation, SegmentID: receipts[1].SegmentID, Offset: secondOffset, StoredLen: secondHeader.StoredLen, LogicalLen: secondHeader.LogicalLen, Checksum: secondHeader.PayloadChecksum, Op: uint8(secondHeader.Op)},
	}
	descBuf := make([]byte, len(descs)*journal.BatchDescriptorSize)
	wantCRC, err := journal.EncodeDescriptors(descBuf, descs)
	if err != nil {
		t.Fatal(err)
	}
	if got := commit.DescriptorsCRC; got != wantCRC {
		t.Fatalf("commit descriptor crc=%d want full descriptor crc=%d", got, wantCRC)
	}
}

func TestCommitLayout_OverflowBatchStartsOnFreshSegment(t *testing.T) {
	dir := t.TempDir()
	s, err := New(Config{Dir: dir, UseMemIndex: true, SegmentSize: 300})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	prime, err := s.Write(context.Background(), &storage.WriteRequest{
		ExtentID:   1,
		Generation: 1,
		Data:       []byte("prime-record"),
	})
	if err != nil {
		t.Fatal(err)
	}
	oldAllocator := s.alloc
	oldState := oldAllocator.State()

	s.group.close()
	beforeWait := make(chan struct{})
	releaseWait := make(chan struct{})
	s.group = newGroupCommitCoordinator(groupCommitConfig{
		MaxBatch: 2,
		MaxWait:  time.Second,
		beforeWait: func() {
			close(beforeWait)
			<-releaseWait
		},
	})

	type result struct {
		receipt *storage.DurableReceipt
		err     error
	}
	results := make(chan result, 2)
	write := func(id storage.ExtentID, data string) {
		receipt, err := s.Write(context.Background(), &storage.WriteRequest{
			ExtentID:   id,
			Generation: 1,
			Data:       []byte(data),
		})
		results <- result{receipt: receipt, err: err}
	}

	go write(2, "first-record")
	select {
	case <-beforeWait:
	case <-time.After(time.Second):
		close(releaseWait)
		t.Fatal("coordinator did not reach batch wait")
	}
	go write(3, "second-record")
	close(releaseWait)

	receipts := make([]*storage.DurableReceipt, 0, 2)
	for range 2 {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatal(got.err)
			}
			receipts = append(receipts, got.receipt)
		case <-time.After(2 * time.Second):
			t.Fatal("overflow batch did not complete")
		}
	}
	sort.Slice(receipts, func(i, j int) bool { return receipts[i].Offset < receipts[j].Offset })
	if got := receipts[0].SegmentID; got == prime.SegmentID {
		t.Fatalf("overflow batch started on old segment %d", got)
	}
	if got, want := receipts[1].SegmentID, receipts[0].SegmentID; got != want {
		t.Fatalf("batch spans segments: second=%d first=%d", got, want)
	}
	if got, want := receipts[0].Offset, int64(SegmentHeaderSize); got != want {
		t.Fatalf("first offset on fresh segment=%d want=%d", got, want)
	}
	firstFraming := int64(RecordFraming(receipts[0].StoredLen, DefaultFrameSize, 1))
	if got, want := receipts[1].Offset, receipts[0].Offset+firstFraming; got != want {
		t.Fatalf("record gap after seal: second offset=%d want=%d", got, want)
	}
	secondFraming := int64(RecordFraming(receipts[1].StoredLen, DefaultFrameSize, 1))
	if got, want := s.alloc.State().NextOffset, receipts[1].Offset+secondFraming+int64(journal.BatchCommitSize); got != want {
		t.Fatalf("fresh segment tail=%d want one contiguous commit at %d", got, want)
	}
	if s.alloc == oldAllocator {
		t.Fatal("overflow batch retained the old allocator")
	}
	gotOldState := oldAllocator.State()
	if got, want := gotOldState.RecordCount, oldState.RecordCount; got != want {
		t.Fatalf("old segment record count = %d, want pre-existing %d", got, want)
	}
	if got, want := gotOldState.NextOffset, oldState.NextOffset; got != want {
		t.Fatalf("old segment tail = %d, want pre-existing %d", got, want)
	}
	if got, want := gotOldState.LastCommitSeq, oldState.LastCommitSeq; got != want {
		t.Fatalf("old segment last commit = %d, want pre-existing %d", got, want)
	}
}
