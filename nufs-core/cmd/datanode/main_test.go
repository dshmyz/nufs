package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSupervisorLoadOrAllocateNodeID(t *testing.T) {
	tmpDir := t.TempDir()

	// First call: allocate new
	id1 := loadOrAllocateNodeID(tmpDir, 42)
	if id1 != 42 {
		t.Fatalf("expected 42, got %d", id1)
	}

	// Second call: load existing
	id2 := loadOrAllocateNodeID(tmpDir, 99)
	if id2 != 42 {
		t.Fatalf("expected 42 (persisted), got %d", id2)
	}

	// Verify file exists
	b, err := os.ReadFile(filepath.Join(tmpDir, nodeIDFile))
	if err != nil {
		t.Fatalf("read node id file: %v", err)
	}
	if string(b) != "42\n" {
		t.Fatalf("expected '42\\n', got %q", string(b))
	}
}

func TestResolveNodeID(t *testing.T) {
	tmpDir := t.TempDir()

	id, err := resolveNodeID("123", tmpDir, "machine-a")
	if err != nil {
		t.Fatalf("resolve numeric node id: %v", err)
	}
	if id != 123 {
		t.Fatalf("expected 123, got %d", id)
	}

	if _, err := resolveNodeID("0", tmpDir, "machine-a"); err == nil {
		t.Fatal("expected error for zero node id")
	}
	if _, err := resolveNodeID("bogus", tmpDir, "machine-a"); err == nil {
		t.Fatal("expected error for malformed node id")
	}
}

func TestResolveAutoNodeIDPersists(t *testing.T) {
	tmpDir := t.TempDir()
	id1, err := resolveNodeID("auto", tmpDir, "machine-a")
	if err != nil {
		t.Fatalf("resolve auto node id: %v", err)
	}
	id2, err := resolveNodeID("auto", tmpDir, "machine-b")
	if err != nil {
		t.Fatalf("resolve auto node id second time: %v", err)
	}
	if id1 == 0 {
		t.Fatal("expected non-zero auto node id")
	}
	if id2 != id1 {
		t.Fatalf("expected persisted node id %d, got %d", id1, id2)
	}
}

func TestSplitAndClean(t *testing.T) {
	cases := []struct {
		input string
		want  int
	}{
		{"/a,/b,/c", 3},
		{"/a, /b, /c ", 3},
		{"", 0},
		{"/single", 1},
	}
	for _, c := range cases {
		got := splitAndClean(c.input)
		if len(got) != c.want {
			t.Fatalf("splitAndClean(%q): want %d, got %d (%v)", c.input, c.want, len(got), got)
		}
	}
}
