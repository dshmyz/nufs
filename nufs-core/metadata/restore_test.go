package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestRestoreRejectsNonEmptyTarget(t *testing.T) {
	repo, manifest := createRestoreRepository(t, "source-cluster")
	target := filepath.Join(t.TempDir(), "metadata")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "existing"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := RestoreBackupToNewCluster(context.Background(), repo, RestoreOptions{
		BackupID:     manifest.BackupID,
		TargetDir:    target,
		NewClusterID: "restored-cluster",
	})
	if err == nil {
		t.Fatal("RestoreBackupToNewCluster accepted a non-empty target")
	}
	if got, err := os.ReadFile(filepath.Join(target, "existing")); err != nil || string(got) != "keep" {
		t.Fatalf("existing target content changed: got %q err %v", got, err)
	}
}

func TestRestoreRejectsCorruptArtifactWithoutPublishingTarget(t *testing.T) {
	repo, manifest := createRestoreRepository(t, "source-cluster")
	corruptCommittedFile(t, repo, manifest)
	target := filepath.Join(t.TempDir(), "metadata")

	_, err := RestoreBackupToNewCluster(context.Background(), repo, RestoreOptions{
		BackupID:     manifest.BackupID,
		TargetDir:    target,
		NewClusterID: "restored-cluster",
	})
	if err == nil {
		t.Fatal("RestoreBackupToNewCluster accepted a corrupt artifact")
	}
	if _, statErr := os.Stat(target); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("target stat after corrupt restore = %v, want not exist", statErr)
	}
	if _, statErr := os.Stat(target + ".restore-report.json"); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("restore report stat after failed restore = %v, want not exist", statErr)
	}
}

func TestRestoreRewritesClusterIdentity(t *testing.T) {
	repo, manifest := createRestoreRepository(t, "source-cluster")
	target := filepath.Join(t.TempDir(), "metadata")

	report, err := RestoreBackupToNewCluster(context.Background(), repo, RestoreOptions{
		BackupID:     manifest.BackupID,
		TargetDir:    target,
		NewClusterID: "restored-cluster",
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.SourceClusterID != "source-cluster" || report.NewClusterID != "restored-cluster" {
		t.Fatalf("restore report identities = %+v", report)
	}

	store, err := NewPebbleStore(PebbleStoreConfig{Dir: target})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var clusterID string
	if found, err := store.getValue(keyClusterID, &clusterID); err != nil || !found {
		t.Fatalf("read restored cluster ID found=%v err=%v", found, err)
	}
	if clusterID != "restored-cluster" {
		t.Fatalf("cluster ID = %q, want rewritten", clusterID)
	}
	assertRawMissing(t, store, prefixBackupTask+"runtime-local")
	marker, err := store.GetRestorePendingMarker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if marker == nil || marker.BackupID != manifest.BackupID || marker.SourceClusterID != "source-cluster" {
		t.Fatalf("restore pending marker = %+v", marker)
	}
	if _, err := os.Stat(filepath.Join(target, "raft", "peers.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old raft peer state stat = %v, want not copied", err)
	}
}

func TestRestorePublishesTargetAtomically(t *testing.T) {
	repo, manifest := createRestoreRepository(t, "source-cluster")
	target := filepath.Join(t.TempDir(), "metadata")
	observedBeforePublish := false
	repo.beforeOpenTarget = func(string) error {
		observedBeforePublish = true
		if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("final target was visible before publish: %v", err)
		}
		return nil
	}

	if _, err := RestoreBackupToNewCluster(context.Background(), repo, RestoreOptions{
		BackupID:     manifest.BackupID,
		TargetDir:    target,
		NewClusterID: "restored-cluster",
	}); err != nil {
		t.Fatal(err)
	}
	if !observedBeforePublish {
		t.Fatal("repository fetch hook was not observed")
	}
	if info, err := os.Stat(target); err != nil || !info.IsDir() {
		t.Fatalf("target was not published as a directory: info=%v err=%v", info, err)
	}
	if _, err := os.Stat(target + ".restore-report.json"); err != nil {
		t.Fatalf("restore report missing after publish: %v", err)
	}
}

func TestRestoreCleansInterruptedTemporaryDirectory(t *testing.T) {
	repo, manifest := createRestoreRepository(t, "source-cluster")
	target := filepath.Join(t.TempDir(), "metadata")
	interrupted := target + ".restore-" + strings.Repeat("a", 32)
	manual := target + ".restore-manual-copy"
	if err := os.MkdirAll(interrupted, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeRestoreTempMarker(interrupted); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(manual, 0o700); err != nil {
		t.Fatal(err)
	}
	repo.beforeOpenTarget = func(string) error { return errors.New("injected fetch interruption") }

	_, err := RestoreBackupToNewCluster(context.Background(), repo, RestoreOptions{
		BackupID:     manifest.BackupID,
		TargetDir:    target,
		NewClusterID: "restored-cluster",
	})
	if err == nil {
		t.Fatal("RestoreBackupToNewCluster returned nil after interrupted fetch")
	}
	if _, statErr := os.Stat(interrupted); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("legal interrupted temp stat = %v, want removed", statErr)
	}
	if _, statErr := os.Stat(manual); statErr != nil {
		t.Fatalf("manual sibling was removed or changed: %v", statErr)
	}
	if _, statErr := os.Stat(target); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("target stat after interrupted restore = %v, want not exist", statErr)
	}
}

func TestRestoreRejectsSameSourceAndNewClusterID(t *testing.T) {
	repo, manifest := createRestoreRepository(t, "source-cluster")
	target := filepath.Join(t.TempDir(), "metadata")

	if _, err := RestoreBackupToNewCluster(context.Background(), repo, RestoreOptions{
		BackupID:     manifest.BackupID,
		TargetDir:    target,
		NewClusterID: "source-cluster",
	}); err == nil {
		t.Fatal("RestoreBackupToNewCluster accepted the source cluster ID")
	}
	if _, statErr := os.Stat(target); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("target stat after rejected identity = %v, want not exist", statErr)
	}
}

func createRestoreRepository(t *testing.T, sourceClusterID string) (*FilesystemBackupRepository, *BackupManifest) {
	t.Helper()
	checkpointDir := t.TempDir()
	db, err := pebble.Open(checkpointDir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	writeRestoreFixtureValue(t, db, prefixInode+"1", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
	writeRestoreFixtureValue(t, db, keyClusterID, sourceClusterID)
	writeRestoreFixtureValue(t, db, prefixBackupTask+"runtime-local", &BackupTask{
		ID:              "runtime-local",
		SourceClusterID: sourceClusterID,
		OwnerNodeID:     "old-node",
		LeadershipTerm:  1,
		AppliedIndex:    42,
		State:           BackupTaskCreating,
		StartedAt:       time.Unix(1, 0).UTC(),
		UpdatedAt:       time.Unix(1, 0).UTC(),
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(checkpointDir, "raft"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkpointDir, "raft", "peers.json"), []byte("old peers"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := BuildBackupManifest(context.Background(), checkpointDir, BackupSnapshotMetadata{
		BackupID:           "backup-restore",
		SourceClusterID:    sourceClusterID,
		CreatedAt:          time.Unix(10, 0).UTC(),
		RaftTerm:           3,
		AppliedIndex:       42,
		MinimumNUFSVersion: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	repo, err := NewFilesystemBackupRepository(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	return repo, manifest
}

func writeRestoreFixtureValue(t *testing.T, db *pebble.DB, key string, value interface{}) {
	t.Helper()
	data, err := marshalValue(value, codecMsgpack)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte(key), data, pebble.Sync); err != nil {
		t.Fatal(err)
	}
}

func corruptCommittedFile(t *testing.T, repo *FilesystemBackupRepository, manifest *BackupManifest) {
	t.Helper()
	for _, file := range manifest.Files {
		if filepath.Base(file.Path) == "CURRENT" {
			path := filepath.Join(repo.root, backupCommittedDir, manifest.BackupID, backupFilesDir, filepath.FromSlash(file.Path))
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, bytes.Replace(data, []byte("MANIFEST"), []byte("CORRUPT!"), 1), 0o600); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatal("CURRENT file not found in manifest")
}

func readRestoreReportFile(t *testing.T, target string) RestoreReport {
	t.Helper()
	data, err := os.ReadFile(target + ".restore-report.json")
	if err != nil {
		t.Fatal(err)
	}
	var report RestoreReport
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	return report
}
