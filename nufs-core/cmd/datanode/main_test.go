package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode"
	"github.com/dshmyz/nufs/nufs-core/internal/tlsutil"
)

// productionGateError must not accept an unauthenticated datanode in
// production: an open ops API is a full control-plane take-over. In-cluster
// transport is plaintext (trusted network, TLS at the edge), so a TLS-less
// node with auth is allowed.
func TestProductionGateRejectsOpenControlPlane(t *testing.T) {
	cases := []struct {
		name          string
		cfg           datanode.Config
		allowInsecure bool
		wantErr       bool
	}{
		{
			name:          "dev opt-out always allowed",
			cfg:           datanode.Config{},
			allowInsecure: true,
			wantErr:       false,
		},
		{
			name:    "production requires ops auth",
			cfg:     datanode.Config{},
			wantErr: true,
		},
		{
			name: "production auth-only is allowed (in-cluster plaintext)",
			cfg: datanode.Config{
				OpsAuthToken: "s3cr3t",
			},
			wantErr: false,
		},
		{
			name: "production TLS + auth is allowed",
			cfg: datanode.Config{
				TLS:          tlsutil.Config{CertFile: "/certs/server.crt", KeyFile: "/certs/server.key"},
				OpsAuthToken: "s3cr3t",
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			if tc.allowInsecure {
				cfg.AllowInsecureDev = true
			}
			err := productionGateError(cfg)
			if tc.wantErr && err == nil {
				t.Fatalf("expected production gate to reject, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected production gate to allow, got: %v", err)
			}
		})
	}
}

func TestLoadOrAllocateNodeID(t *testing.T) {
	tmpDir := t.TempDir()
	idPath := filepath.Join(tmpDir, nodeIDFile)

	// First call: allocate new
	id1 := loadOrAllocateNodeID(idPath, 42)
	if id1 != 42 {
		t.Fatalf("expected 42, got %d", id1)
	}

	// Second call: load existing
	id2 := loadOrAllocateNodeID(idPath, 99)
	if id2 != 42 {
		t.Fatalf("expected 42 (persisted), got %d", id2)
	}

	// Verify file exists
	b, err := os.ReadFile(idPath)
	if err != nil {
		t.Fatalf("read node id file: %v", err)
	}
	if string(b) != "42\n" {
		t.Fatalf("expected '42\\n', got %q", string(b))
	}
}

func TestResolveNodeID(t *testing.T) {
	tmpDir := t.TempDir()
	idPath := filepath.Join(tmpDir, nodeIDFile)

	id, err := resolveNodeID("123", idPath, "machine-a")
	if err != nil {
		t.Fatalf("resolve numeric node id: %v", err)
	}
	if id != 123 {
		t.Fatalf("expected 123, got %d", id)
	}

	if _, err := resolveNodeID("0", idPath, "machine-a"); err == nil {
		t.Fatal("expected error for zero node id")
	}
	if _, err := resolveNodeID("bogus", idPath, "machine-a"); err == nil {
		t.Fatal("expected error for malformed node id")
	}
}

func TestResolveAutoNodeIDPersists(t *testing.T) {
	tmpDir := t.TempDir()
	idPath := filepath.Join(tmpDir, nodeIDFile)
	id1, err := resolveNodeID("auto", idPath, "machine-a")
	if err != nil {
		t.Fatalf("resolve auto node id: %v", err)
	}
	id2, err := resolveNodeID("auto", idPath, "machine-b")
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

func TestAutoNodeIDStableAcrossDirChange(t *testing.T) {
	// auto node ID should be stable based on machineID, not data dir.
	// Changing data dirs should NOT change the auto-assigned node ID.
	id1 := stableAutoNodeID("machine-1")
	id2 := stableAutoNodeID("machine-1")
	if id1 != id2 {
		t.Fatalf("auto node ID should be stable for same machine: %d vs %d", id1, id2)
	}
	id3 := stableAutoNodeID("machine-2")
	if id1 == id3 {
		t.Fatalf("auto node ID should differ for different machines: %d vs %d", id1, id3)
	}
}

func TestResolveNodeIDPath(t *testing.T) {
	// Env var takes priority
	t.Setenv(nodeIDEnv, "/custom/path/node_id")
	if p := resolveNodeIDPath("/data/dir"); p != "/custom/path/node_id" {
		t.Fatalf("env var path: got %s", p)
	}

	// Fall back to data dir
	os.Unsetenv(nodeIDEnv)
	if p := resolveNodeIDPath("/data/dir"); p != "/data/dir/node_id" {
		t.Fatalf("data dir path: got %s", p)
	}
}
