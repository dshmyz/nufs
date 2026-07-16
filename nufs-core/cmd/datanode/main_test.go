package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func startTestSocket(t *testing.T) (*supervisor, string, func()) {
	sockPath := filepath.Join(os.TempDir(), fmt.Sprintf("dfs-test-%d.sock", time.Now().UnixNano()))
	sv := &supervisor{
		children: make(map[string]*childInfo),
		stopCh:   make(chan struct{}),
	}

	sv.startSocketListener(sockPath)

	for i := 0; i < 100; i++ {
		conn, err := net.Dial("unix", sockPath)
		if err == nil {
			conn.Close()
			break
		}
		if i == 99 {
			t.Fatalf("socket never became ready: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cleanup := func() {
		close(sv.stopCh)
		sv.wg.Wait()
		os.Remove(sockPath)
	}
	return sv, sockPath, cleanup
}

func TestSupervisorSocketStatus(t *testing.T) {
	sv, sockPath, cleanup := startTestSocket(t)
	defer cleanup()

	sv.mu.Lock()
	sv.children["/test-dir"] = &childInfo{
		Dir: "/test-dir", Port: 9100, NodeID: 1, Pid: 12345,
		State: childRunning, Started: time.Now(),
	}
	sv.mu.Unlock()

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial socket: %v", err)
	}
	defer conn.Close()

	json.NewEncoder(conn).Encode(sockMsg{Cmd: "status"})
	var resp sockResp
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("expected ok, got %s: %s", resp.Status, resp.Error)
	}

	statuses, ok := resp.Data.([]interface{})
	if !ok {
		t.Fatalf("expected array, got %T", resp.Data)
	}
	if len(statuses) != 1 {
		t.Fatalf("expected 1 child, got %d", len(statuses))
	}
}

func TestSupervisorSocketAdoptRetire(t *testing.T) {
	sv, sockPath, cleanup := startTestSocket(t)
	defer cleanup()

	sv.mu.Lock()
	sv.children["/test-dfs-dir"] = &childInfo{
		Dir: "/test-dfs-dir", Port: 9100, NodeID: 1, Pid: 99999,
		State: childRunning, Started: time.Now(),
	}
	sv.mu.Unlock()

	connect := func() net.Conn {
		conn, err := net.Dial("unix", sockPath)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		return conn
	}
	doCmd := func(cmd, path string) sockResp {
		conn := connect()
		defer conn.Close()
		json.NewEncoder(conn).Encode(sockMsg{Cmd: cmd, Path: path})
		var resp sockResp
		json.NewDecoder(conn).Decode(&resp)
		return resp
	}

	// Retire non-existent dir
	resp := doCmd("retire", "/nonexistent")
	if resp.Status != "error" {
		t.Fatalf("expected error for nonexistent dir, got %s", resp.Status)
	}

	// Adopt without path
	resp = doCmd("adopt", "")
	if resp.Status != "error" {
		t.Fatalf("expected error for empty path, got %s", resp.Status)
	}

	// Unknown command
	resp = doCmd("bogus", "")
	if resp.Status != "error" {
		t.Fatalf("expected error for unknown cmd, got %s", resp.Status)
	}

	// Adopt dir that's already managed
	resp = doCmd("adopt", "/test-dfs-dir")
	if resp.Status != "error" {
		t.Fatalf("expected error for existing dir adopt, got %s", resp.Status)
	}

	// Retire the existing child
	resp = doCmd("retire", "/test-dfs-dir")
	if resp.Status != "ok" {
		t.Fatalf("expected ok for retire, got %s: %s", resp.Status, resp.Error)
	}
}

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
