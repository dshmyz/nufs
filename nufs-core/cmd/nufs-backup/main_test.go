package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/example/dfs/metadata"
)

func TestBackupCommandListJSON(t *testing.T) {
	root := createCLIBackupRepository(t)
	configPath := writeRepositoryConfig(t, root)

	var stdout, stderr bytes.Buffer
	code := runBackupCommand(context.Background(), []string{"--json", "list", "--repository-config", configPath}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d stderr=%s", code, stderr.String())
	}
	var rows []map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &rows); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if len(rows) != 1 || rows[0]["id"] != "cli-backup" {
		t.Fatalf("list output = %+v", rows)
	}
}

func TestBackupCommandVerifyReturnsNonZeroForCorruptBackup(t *testing.T) {
	root := createCLIBackupRepository(t)
	if err := os.WriteFile(filepath.Join(root, "backups", "cli-backup", "files", "CURRENT"), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := writeRepositoryConfig(t, root)

	var stdout, stderr bytes.Buffer
	code := runBackupCommand(context.Background(), []string{"--json", "verify", "cli-backup", "--repository-config", configPath}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("verify exit = 0 stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String()+stdout.String(), "mismatch") {
		t.Fatalf("verify output did not mention corruption: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func TestBackupCommandCreatePostsToOpsEndpoint(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/backups" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"id":"created"}`))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := runBackupCommand(context.Background(), []string{"--json", "create", "--ops-url", server.URL, "--auth-token", "secret"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if gotAuth != "Bearer secret" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
}

func TestBackupCommandPruneRequiresDryRun(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runBackupCommand(context.Background(), []string{"prune", "--ops-url", "http://127.0.0.1:1"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("prune without --dry-run exited 0: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func TestBackupCommandPruneDryRunPostsRegisteredOpsEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/backups/prune" || r.URL.Query().Get("dry_run") != "true" {
			t.Fatalf("request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"dry_run":true,"deletion_candidates":[]}`))
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	code := runBackupCommand(context.Background(), []string{"--json", "prune", "--ops-url", server.URL, "--dry-run"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), `"dry_run":true`) {
		t.Fatalf("stdout = %s, want dry-run response", stdout.String())
	}
}

func TestBackupCommandPruneRejectsAuthTokenFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runBackupCommand(context.Background(), []string{"prune", "--ops-url", "http://127.0.0.1:1", "--dry-run", "--auth-token", "secret"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("prune accepted --auth-token: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func createCLIBackupRepository(t *testing.T) string {
	t.Helper()
	checkpointDir := t.TempDir()
	db, err := pebble.Open(checkpointDir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	root, err := metadataTestValue(&metadata.InodeMeta{ID: metadata.RootInodeID, Type: metadata.FileDirectory})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("/inode/1"), root, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, err := metadata.BuildBackupManifest(context.Background(), checkpointDir, metadata.BackupSnapshotMetadata{
		BackupID:        "cli-backup",
		SourceClusterID: "source-cluster",
		CreatedAt:       time.Unix(20, 0).UTC(),
		AppliedIndex:    5,
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

func writeRepositoryConfig(t *testing.T, root string) string {
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

func metadataTestValue(v interface{}) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return data, nil
}
