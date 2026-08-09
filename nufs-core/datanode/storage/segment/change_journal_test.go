package segment

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/journal"
)

func TestChangeJournal_EmittedOnCorruptRead(t *testing.T) {
	dir := t.TempDir()
	j, err := journal.OpenChangeJournal(journal.JournalOptions{Dir: dir + "/cj"})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Dir: dir, UseMemIndex: false, ChangeJournal: j})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("change-journal-test"), 100)
	if _, err := s.Write(context.Background(), &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: data}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Corrupt the segment file's payload.
	raw, _ := os.ReadFile(dir + "/segments/small/active/1.seg")
	raw[len(raw)/2] ^= 0xFF
	os.WriteFile(dir+"/segments/small/active/1.seg", raw, 0644)

	s2, err := New(Config{Dir: dir, UseMemIndex: false, ChangeJournal: j})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	// Read must fail (corrupt data never returned as successful reads).
	if _, err := s2.Read(context.Background(), &storage.ReadRequest{ExtentID: 1, Generation: 1}); err == nil {
		t.Fatal("corrupt read must fail")
	}
	// The change journal must contain an EventCorrupt event.
	events, _ := j.Pending(100, 1<<20)
	found := false
	for _, e := range events {
		if e.Kind == journal.EventCorrupt && e.ExtentID == 1 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("change journal should contain EventCorrupt after corrupt read")
	}
}
