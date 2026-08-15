package main

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// newV2StoreForTest builds a single-disk V2Store over a real segment store,
// the V2.1 engine, returning it plus a short socket dir (the management unix
// socket path is length-limited, so it cannot live under the long t.TempDir()).
func newV2StoreForTest(t *testing.T) (*datanode.V2Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := segment.New(segment.Config{Dir: dir, UseMemIndex: true, StreamID: 1})
	if err != nil {
		t.Fatalf("segment.New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	v := datanode.NewV2Store(s, dir)
	if err := v.Write(metadata.ChunkID(1), []byte("manage-me")); err != nil {
		t.Fatalf("write: %v", err)
	}
	sockDir, err := os.MkdirTemp("", "mgmt")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	return v, sockDir
}

// TestManagementServer_V2StoreStatus proves the unix-socket management
// channel serves V2.1 state (status reports the disk/bytes) the same way it
// does for V1.
func TestManagementServer_V2StoreStatus(t *testing.T) {
	v, dir := newV2StoreForTest(t)
	stop, err := startManagementServer(v, []string{dir})
	if err != nil {
		t.Fatalf("startManagementServer: %v", err)
	}
	defer stop()

	sockPath := filepath.Join(dir, ".datanode.sock")
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial socket: %v", err)
	}
	defer conn.Close()

	if err := json.NewEncoder(conn).Encode(sockMsg{Cmd: "status"}); err != nil {
		t.Fatalf("encode: %v", err)
	}
	var resp sockResp
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("status=%q error=%q, want ok", resp.Status, resp.Error)
	}
	data, ok := resp.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("status data type %T, want map", resp.Data)
	}
	if data["total_chunks"].(float64) != 1 {
		t.Fatalf("total_chunks=%v, want 1", data["total_chunks"])
	}
	if int64(data["total_bytes"].(float64)) != int64(len("manage-me")) {
		t.Fatalf("total_bytes=%v, want %d", data["total_bytes"], len("manage-me"))
	}
}

// TestManagementServer_V2StoreLifecycleUnsupported proves disk-lifecycle
// socket commands degrade cleanly for a factory-less V2Store (the operational
// degrade path when the datanode main has NOT injected a disk factory via
// SetDiskFactory). V2Store now satisfies DiskLifecycleOps and DrainOps, so:
//   - adopt degrades to "add disk: not configured (no disk factory)…" because
//     the engine cannot construct a segment store for an arbitrary new dir;
//   - retire/decommission/migrate route through DiskInfos() and answer
//     "dir not found" for an unknown path.
//
// (drain is NOT in this table: V2Store exposes DrainOps, so /drain succeeds —
// it is exercised positively in TestManagementServer_V2StoreDrain.)
//
// None of these panic on the missing capability; production main always wires
// the factory, which is exercised end-to-end in datanode/ops_v2store_test.go.
func TestManagementServer_V2StoreLifecycleUnsupported(t *testing.T) {
	v, dir := newV2StoreForTest(t)
	stop, err := startManagementServer(v, []string{dir})
	if err != nil {
		t.Fatalf("startManagementServer: %v", err)
	}
	defer stop()

	sockPath := filepath.Join(dir, ".datanode.sock")
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial socket: %v", err)
	}
	defer conn.Close()

	for _, tc := range []struct{ cmd, wantErr string }{
		{cmd: "adopt", wantErr: "add disk: not configured (no disk factory); disk lifecycle unsupported by this engine"},
		{cmd: "retire", wantErr: "dir not found"},
		{cmd: "decommission", wantErr: "dir not found"},
		{cmd: "migrate", wantErr: "dir not found"},
	} {
		c, err := net.Dial("unix", sockPath)
		if err != nil {
			t.Fatalf("dial socket (%s): %v", tc.cmd, err)
		}
		msg := sockMsg{Cmd: tc.cmd, Path: "/tmp/nonexistent"}
		if err := json.NewEncoder(c).Encode(msg); err != nil {
			c.Close()
			t.Fatalf("encode %s: %v", tc.cmd, err)
		}
		var resp sockResp
		if err := json.NewDecoder(c).Decode(&resp); err != nil {
			c.Close()
			t.Fatalf("decode %s: %v", tc.cmd, err)
		}
		c.Close()
		if resp.Status != "error" || resp.Error != tc.wantErr {
			t.Fatalf("%s status=%q error=%q, want error %q", tc.cmd, resp.Status, resp.Error, tc.wantErr)
		}
	}
}

// TestManagementServer_V2StoreDrain proves the management unix-socket /drain
// channel serves V2.1 drain (DrainOps parity): it acquires the QuiesceWrites
// barrier and reports "drained" instead of the previous "drain unsupported by
// this engine".
func TestManagementServer_V2StoreDrain(t *testing.T) {
	v, dir := newV2StoreForTest(t)
	stop, err := startManagementServer(v, []string{dir})
	if err != nil {
		t.Fatalf("startManagementServer: %v", err)
	}
	defer stop()

	sockPath := filepath.Join(dir, ".datanode.sock")
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial socket: %v", err)
	}
	defer conn.Close()

	msg := sockMsg{Cmd: "drain"}
	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		t.Fatalf("encode: %v", err)
	}
	var resp sockResp
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("drain status=%q error=%q, want ok", resp.Status, resp.Error)
	}
	data, ok := resp.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("drain data type %T, want map", resp.Data)
	}
	if data["status"] != "drained" {
		t.Fatalf("drain status field=%v, want 'drained'", data["status"])
	}
}
