package metadata

import (
	"testing"

	"github.com/cockroachdb/pebble"
)

// Regression: nufs-cli kv scan (local mode) passed []byte("") for "no cursor",
// and scanPrefixPaged treated any non-nil cursor as an explicit LowerBound —
// the empty bound replaced the prefix bound and the scan started at the first
// key of the entire keyspace, leaking keys outside the requested prefix.
func TestKVScanEmptyCursorStaysInPrefix(t *testing.T) {
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for _, k := range []string{"/bucket/b1", "/chunk/7", "/inode/42"} {
		if err := store.db.Set([]byte(k), []byte("v"), pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}

	for name, cursor := range map[string][]byte{
		"nil cursor":   nil,
		"empty cursor": []byte(""),
	} {
		page, err := store.KVScan("/chunk/", cursor, 100)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(page.Keys) != 1 || string(page.Keys[0]) != "/chunk/7" {
			t.Fatalf("%s: got keys %q, want exactly [/chunk/7]", name, page.Keys)
		}
		if page.HasMore {
			t.Fatalf("%s: HasMore should be false for a single key", name)
		}
	}
}

// Paging with a real cursor still works: cursor is exclusive-start.
func TestKVScanCursorPaging(t *testing.T) {
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for _, k := range []string{"/inode/1", "/inode/2", "/inode/3"} {
		if err := store.db.Set([]byte(k), []byte("v"), pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}

	page1, err := store.KVScan("/inode/", nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Keys) != 2 || !page1.HasMore {
		t.Fatalf("page1: got %d keys hasMore=%v, want 2 keys hasMore=true", len(page1.Keys), page1.HasMore)
	}
	page2, err := store.KVScan("/inode/", page1.NextKey, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Keys) != 1 || string(page2.Keys[0]) != "/inode/3" || page2.HasMore {
		t.Fatalf("page2: got %q hasMore=%v, want [/inode/3] hasMore=false", page2.Keys, page2.HasMore)
	}
}
