package segment

import (
	"bytes"
	"context"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
)

func TestSmallStore_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSmallStore(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("small-file"), 1000) // ~10 KiB, in the sampled range
	rec, err := s.WriteSmallFile(&storage.WriteRequest{ExtentID: 1, Generation: 1, Data: data})
	if err != nil {
		t.Fatalf("WriteSmallFile: %v", err)
	}
	if rec.SegmentID == 0 {
		t.Fatalf("bad receipt: %+v", rec)
	}
	got, err := s.Read(context.Background(), &storage.ReadRequest{ExtentID: 1, Generation: 1})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got.Data, data) {
		t.Fatal("small-file roundtrip mismatch")
	}
	s.Close()

	// Reopen through the small stream and re-read.
	s2, err := NewSmallStore(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got2, err := s2.Read(context.Background(), &storage.ReadRequest{ExtentID: 1, Generation: 1})
	if err != nil {
		t.Fatalf("reopen read: %v", err)
	}
	if !bytes.Equal(got2.Data, data) {
		t.Fatal("small-file reopen mismatch")
	}
}

func TestSmallStore_RejectsOversized(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSmallStore(Config{Dir: dir, UseMemIndex: false})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// >64 KiB must be rejected by the small stream (§5.1: files above
	// the threshold use extent records, not the small segment).
	big := make([]byte, storage.SmallFileThreshold+1)
	if _, err := s.WriteSmallFile(&storage.WriteRequest{ExtentID: 9, Generation: 1, Data: big}); err != storage.ErrCapacity {
		t.Fatalf("expected ErrCapacity for oversized small file, got %v", err)
	}
}
