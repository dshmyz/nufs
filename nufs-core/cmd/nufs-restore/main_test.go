package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func TestRestoreCommandInspectJSON(t *testing.T) {
	root := createCLIRestoreRepository(t)
	configPath := writeRestoreRepositoryConfig(t, root)

	var stdout, stderr bytes.Buffer
	code := runRestoreCommand(context.Background(), []string{"--json", "inspect", "cli-restore", "--repository-config", configPath}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d stderr=%s", code, stderr.String())
	}
	var out map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if out["backup_id"] != "cli-restore" {
		t.Fatalf("inspect output = %+v", out)
	}
}

func TestRestoreCommandRestoreRejectsUnsafeTarget(t *testing.T) {
	root := createCLIRestoreRepository(t)
	configPath := writeRestoreRepositoryConfig(t, root)
	target := filepath.Join(t.TempDir(), "metadata")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "existing"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := runRestoreCommand(context.Background(), []string{
		"restore", "cli-restore",
		"--repository-config", configPath,
		"--target-dir", target,
		"--new-cluster-id", "restored-cluster",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("restore into non-empty target exited 0: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func TestRestoreCommandRestoreJSON(t *testing.T) {
	root := createCLIRestoreRepository(t)
	configPath := writeRestoreRepositoryConfig(t, root)
	target := filepath.Join(t.TempDir(), "metadata")

	var stdout, stderr bytes.Buffer
	code := runRestoreCommand(context.Background(), []string{
		"--json", "restore", "cli-restore",
		"--repository-config", configPath,
		"--target-dir", target,
		"--new-cluster-id", "restored-cluster",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	var out map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if out["new_cluster_id"] != "restored-cluster" {
		t.Fatalf("restore output = %+v", out)
	}
	if _, err := os.Stat(target + ".restore-report.json"); err != nil {
		t.Fatalf("restore report missing: %v", err)
	}
}

func TestRestoreCommandRestoreRejectsNUFSVersionFlag(t *testing.T) {
	root := createCLIRestoreRepository(t)
	configPath := writeRestoreRepositoryConfig(t, root)
	target := filepath.Join(t.TempDir(), "metadata")

	var stdout, stderr bytes.Buffer
	code := runRestoreCommand(context.Background(), []string{
		"restore", "cli-restore",
		"--repository-config", configPath,
		"--target-dir", target,
		"--new-cluster-id", "restored-cluster",
		"--nufs-version", "test",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("restore accepted --nufs-version: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func createCLIRestoreRepository(t *testing.T) string {
	t.Helper()
	checkpointDir := t.TempDir()
	db, err := pebble.Open(checkpointDir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	writeCLIRestoreValue(t, db, "/inode/1", &metadata.InodeMeta{ID: metadata.RootInodeID, Type: metadata.FileDirectory})
	writeCLIRestoreValue(t, db, "system/cluster-id", "source-cluster")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, err := metadata.BuildBackupManifest(context.Background(), checkpointDir, metadata.BackupSnapshotMetadata{
		BackupID:        "cli-restore",
		SourceClusterID: "source-cluster",
		CreatedAt:       time.Unix(30, 0).UTC(),
		AppliedIndex:    7,
	})
	if err != nil {
		t.Fatal(err)
	}
	repoRoot := t.TempDir()
	repo, err := metadata.NewFilesystemBackupRepository(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	return repoRoot
}

func writeRestoreRepositoryConfig(t *testing.T, root string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "repository.json")
	data, err := json.Marshal(map[string]string{"type": "filesystem", "root": root})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeCLIRestoreValue(t *testing.T, db *pebble.DB, key string, value interface{}) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte(key), data, pebble.Sync); err != nil {
		t.Fatal(err)
	}
}
