package segment

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/journal"
)

// TestRelocate_PreservesChecksumAndData is the P0 regression for the
// relocation checksum-supersession bug: Relocate used to shadow the
// relocated extent's checksum with 0, so after a compaction move the
// location carried Checksum==0 and read/repair integrity validation
// silently vanished. After the fix, relocating the very same extent to a
// new offset must preserve its real logical checksum and byte-exact data,
// both in the live store and across a crash + reopen (the durable RELOCATE
// record replay).
func TestRelocate_PreservesChecksumAndData(t *testing.T) {
	dir := t.TempDir()
	cj, err := journal.OpenChangeJournal(journal.JournalOptions{Dir: dir + "/cj"})
	if err != nil {
		t.Fatal(err)
	}
	defer cj.Close()
	ctx := context.Background()

	s, err := New(Config{Dir: dir, UseMemIndex: true, ChangeJournal: cj})
	if err != nil {
		t.Fatal(err)
	}

	extentID := storage.ExtentID(100)
	data := []byte("the-original-logical-payload-that-must-not-lose-its-checksum")
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: extentID, Generation: 1, Data: data}); err != nil {
		t.Fatal(err)
	}
	before, err := s.Stat(ctx, &storage.StatRequest{ExtentID: extentID, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	wantCRC := storage.CRC32C(data)

	// Move the extent to a new location via the compactor sink seam.
	loc, err := s.AppendRecord(extentID, 1, data, storage.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if loc.Checksum != wantCRC {
		t.Fatalf("AppendRecord reloc checksum = %d, want %d", loc.Checksum, wantCRC)
	}
	if err := s.Relocate([]storage.Reloc{*loc}); err != nil {
		t.Fatal(err)
	}

	after, err := s.Stat(ctx, &storage.StatRequest{ExtentID: extentID, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if after.Offset == before.Offset {
		t.Fatalf("expected relocation to a new offset, same offset %d", after.Offset)
	}
	if after.Checksum != wantCRC {
		t.Fatalf("relocated checksum = %d, want %d (was shadowed to 0 before the fix)", after.Checksum, wantCRC)
	}

	got, err := s.Read(ctx, &storage.ReadRequest{ExtentID: extentID, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Data, data) {
		t.Fatalf("relocated data mismatch: got %q", got.Data)
	}
	if got.Checksum != wantCRC {
		t.Fatalf("relocated Read checksum = %d, want %d", got.Checksum, wantCRC)
	}

	// The relocation must be durable: reopen replays it and the read still
	// lands on the new location with the real checksum.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(Config{Dir: dir, UseMemIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	got2, err := restarted.Read(ctx, &storage.ReadRequest{ExtentID: extentID, Generation: 1})
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if !bytes.Equal(got2.Data, data) || got2.Checksum != wantCRC {
		t.Fatalf("post-reopen relocated read = data %q checksum %d, want data %q checksum %d",
			got2.Data, got2.Checksum, data, wantCRC)
	}

	// The relocation must have emitted an async EventRelocated.
	pending, _ := cj.Pending(100, 1<<20)
	found := false
	for _, ev := range pending {
		if ev.ExtentID == extentID && ev.Kind == journal.EventRelocated {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no EventRelocated in change journal after relocate; pending: %+v", pending)
	}
}

// TestRelocate_PreservesTombstone is the P0 regression for the
// tombstone-clobber race: if an extent is deleted after it was scanned as
// live but before Relocate runs, Relocate must not resurrect it back to
// ExtentDurable.
func TestRelocate_PreservesTombstone(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := New(Config{Dir: dir, UseMemIndex: true})
	if err != nil {
		t.Fatal(err)
	}

	extentID := storage.ExtentID(200)
	data := []byte("delete-me-before-relocate")
	if _, err := s.Write(ctx, &storage.WriteRequest{ExtentID: extentID, Generation: 1, Data: data}); err != nil {
		t.Fatal(err)
	}
	// The compactor first appends the live record to a new location...
	loc, err := s.AppendRecord(extentID, 1, data, storage.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	// ...then a delete lands after the source scan's isLive check but before
	// Relocate runs — the race the conditional apply must guard.
	if err := s.Delete(ctx, &storage.DeleteRequest{ExtentID: extentID, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	// Attempt to relocate the (now deleted) extent to its fresh location.
	if err := s.Relocate([]storage.Reloc{*loc}); err != nil {
		t.Fatal(err)
	}
	st, err := s.Stat(ctx, &storage.StatRequest{ExtentID: extentID, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if st.State != storage.ExtentTombstoned {
		t.Fatalf("relocate resurrected a deleted extent: state = %v, want ExtentTombstoned", st.State)
	}
	if _, err := s.Read(ctx, &storage.ReadRequest{ExtentID: extentID, Generation: 1}); !errors.Is(err, storage.ErrExtentNotFound) {
		t.Fatalf("read of tombstoned extent = %v, want ErrExtentNotFound", err)
	}

	// The tombstone must also survive crash + recovery: both the AppendRecord
	// PUT and the Delete are durable, and Relocate (a no-op on recovery) must
	// not resurrect the extent.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(Config{Dir: dir, UseMemIndex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if st, err := reopened.Stat(ctx, &storage.StatRequest{ExtentID: extentID, Generation: 1}); err != nil {
		t.Fatalf("Stat after reopen: %v", err)
	} else if st.State != storage.ExtentTombstoned {
		t.Fatalf("relocate resurrected a deleted extent after reopen: state = %v, want ExtentTombstoned", st.State)
	}
	if _, err := reopened.Read(ctx, &storage.ReadRequest{ExtentID: extentID, Generation: 1}); !errors.Is(err, storage.ErrExtentNotFound) {
		t.Fatalf("read of tombstoned extent after reopen = %v, want ErrExtentNotFound", err)
	}
}
