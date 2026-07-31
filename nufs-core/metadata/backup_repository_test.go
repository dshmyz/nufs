package metadata

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRepositoryDoesNotListInterruptedPublish(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	repo.beforeCommit = func(string) error { return errors.New("injected interruption") }

	if err := repo.Publish(context.Background(), checkpointDir, manifest); err == nil {
		t.Fatal("Publish returned nil")
	}
	got, err := repo.ListCommitted(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("ListCommitted returned %v after interrupted publish", got)
	}
	if _, err := os.Stat(filepath.Join(root, "backups", manifest.BackupID, "COMMITTED")); !os.IsNotExist(err) {
		t.Fatalf("COMMITTED stat error = %v, want not exist", err)
	}
}

func TestRepositoryWritesCommittedMarkerLast(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	observed := false
	repo.beforeCommit = func(finalDir string) error {
		observed = true
		if _, err := os.Stat(filepath.Join(finalDir, "COMMITTED")); !os.IsNotExist(err) {
			t.Fatalf("COMMITTED exists before commit hook: %v", err)
		}
		if _, err := os.Stat(filepath.Join(finalDir, "manifest.json")); err != nil {
			t.Fatalf("manifest missing before commit hook: %v", err)
		}
		for _, file := range manifest.Files {
			if _, err := os.Stat(filepath.Join(finalDir, "files", filepath.FromSlash(file.Path))); err != nil {
				t.Fatalf("declared file %q missing before commit hook: %v", file.Path, err)
			}
		}
		return nil
	}

	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	if !observed {
		t.Fatal("beforeCommit hook was not called")
	}
	if _, err := os.Stat(filepath.Join(root, "backups", manifest.BackupID, "COMMITTED")); err != nil {
		t.Fatalf("COMMITTED missing after Publish: %v", err)
	}
}

func TestRepositoryFetchRejectsMissingCommittedMarker(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	repo.beforeCommit = func(string) error { return errors.New("stop before marker") }
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err == nil {
		t.Fatal("Publish returned nil")
	}

	target := filepath.Join(realTempDir(t), "restore")
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, target); err == nil {
		t.Fatal("Fetch returned nil without COMMITTED")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target stat error = %v, want not exist", err)
	}
}

func TestRepositoryDeleteIsIdempotent(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(context.Background(), manifest.BackupID); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(context.Background(), manifest.BackupID); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
}

func TestRepositoryListSortsNewestFirst(t *testing.T) {
	root := t.TempDir()
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	for _, item := range []struct {
		id      string
		created time.Time
		index   uint64
	}{
		{id: "older", created: base.Add(-time.Hour), index: 1},
		{id: "same-b", created: base, index: 2},
		{id: "same-a", created: base, index: 3},
	} {
		checkpointDir, manifest := createManifestFixture(t)
		manifest.BackupID = item.id
		manifest.CreatedAt = item.created
		manifest.AppliedIndex = item.index
		if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
			t.Fatal(err)
		}
	}
	got, err := repo.ListCommitted(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"same-a", "same-b", "older"}
	if len(got) != len(want) {
		t.Fatalf("ListCommitted length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i] {
			t.Fatalf("ListCommitted[%d].ID = %q, want %q", i, got[i].ID, want[i])
		}
	}
}

func TestRepositoryRejectsMalformedBackupIDs(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", ".", "..", "a/b", `a\b`, "C:escape", "a//b"} {
		t.Run(id, func(t *testing.T) {
			copyManifest := *manifest
			copyManifest.BackupID = id
			if err := repo.Publish(context.Background(), checkpointDir, &copyManifest); err == nil {
				t.Fatalf("Publish accepted backup ID %q", id)
			}
			if err := repo.Delete(context.Background(), id); err == nil {
				t.Fatalf("Delete accepted backup ID %q", id)
			}
			if _, err := repo.Fetch(context.Background(), id, filepath.Join(realTempDir(t), "target")); err == nil {
				t.Fatalf("Fetch accepted backup ID %q", id)
			}
		})
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid manifests touched repository state: %v", entries)
	}
}

func TestRepositoryValidatesManifestBeforeWrites(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}

	manifest.Files[0].Path = "../escape"
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err == nil {
		t.Fatal("Publish accepted unsafe manifest path")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid manifest touched repository state: %v", entries)
	}
}

func TestRepositoryFetchUsesExclusiveFilesAndCleansPartialOutput(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}

	target := realTempDir(t)
	existingPath := filepath.Join(target, filepath.FromSlash(manifest.Files[0].Path))
	if err := os.MkdirAll(filepath.Dir(existingPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(existingPath, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, target); err == nil {
		t.Fatal("Fetch overwrote an existing target file")
	}
	got, err := os.ReadFile(existingPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "keep" {
		t.Fatalf("existing file changed to %q", got)
	}
}

func TestRepositoryFetchRejectsSymlinkTraversal(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}

	outside := t.TempDir()
	target := filepath.Join(realTempDir(t), "restore-link")
	if err := os.Symlink(outside, target); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, target); err == nil {
		t.Fatal("Fetch traversed target symlink")
	}
}

func TestRepositoryFetchCleansCorruptedArtifact(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	corruptPath := filepath.Join(root, "backups", manifest.BackupID, "files", filepath.FromSlash(manifest.Files[0].Path))
	if err := os.WriteFile(corruptPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(realTempDir(t), "restore")
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, target); err == nil {
		t.Fatal("Fetch returned nil for corrupted artifact")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target stat error = %v, want cleaned", err)
	}
}

func TestRepositoryHonorsCanceledContext(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := repo.Publish(ctx, checkpointDir, manifest); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish error = %v, want context canceled", err)
	}
	if _, err := os.Stat(filepath.Join(root, "staging")); !os.IsNotExist(err) {
		t.Fatalf("canceled Publish touched repository: %v", err)
	}
}

func TestRepositoryDeleteStagingOlderThan(t *testing.T) {
	root := t.TempDir()
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	staging := filepath.Join(root, backupStagingDir)
	oldID := strings.Repeat("a", 32)
	newID := strings.Repeat("b", 32)
	oldDir := filepath.Join(staging, oldID)
	newDir := filepath.Join(staging, newID)
	for _, attempt := range []filesystemBackupAttempt{
		{BackupID: "old", AttemptID: oldID, CreatedAt: time.Now().UTC().Add(-2 * time.Hour)},
		{BackupID: "new", AttemptID: newID, CreatedAt: time.Now().UTC()},
	} {
		dir := filepath.Join(staging, attempt.AttemptID)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(attempt)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, backupAttemptFile), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cutoff := time.Now().UTC().Add(-time.Hour)
	oldTime := cutoff.Add(-time.Hour)
	if err := os.Chtimes(oldDir, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	if err := repo.DeleteStagingOlderThan(context.Background(), cutoff); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldDir); !os.IsNotExist(err) {
		t.Fatalf("old staging stat = %v, want deleted", err)
	}
	if _, err := os.Stat(newDir); err != nil {
		t.Fatalf("new staging was deleted: %v", err)
	}
}

func TestRepositoryListSkipsMalformedCommittedEntries(t *testing.T) {
	root := t.TempDir()
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(root, backupCommittedDir, "bad")
	if err := os.MkdirAll(bad, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bad, backupCommitMarker), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bad, backupManifestFile), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := repo.ListCommitted(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("ListCommitted returned malformed backup: %v", got)
	}
}

func TestRepositoryDoesNotOverwriteDifferentCommittedBackup(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatalf("idempotent Publish: %v", err)
	}
	different := *manifest
	different.AppliedIndex++
	if err := repo.Publish(context.Background(), checkpointDir, &different); err == nil {
		t.Fatal("Publish overwrote a committed backup with different contents")
	}
}

func TestRepositoryTwoInstancesSerializeDifferentPublishers(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifestA := createManifestFixture(t)
	manifestB := *manifestA
	manifestB.AppliedIndex = manifestA.AppliedIndex + 1
	repoA, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	repoB, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	promoted := make(chan struct{})
	releaseA := make(chan struct{})
	repoA.beforeCommit = func(string) error {
		close(promoted)
		<-releaseA
		return nil
	}
	errA := make(chan error, 1)
	go func() { errA <- repoA.Publish(context.Background(), checkpointDir, manifestA) }()
	<-promoted

	errB := make(chan error, 1)
	go func() { errB <- repoB.Publish(context.Background(), checkpointDir, &manifestB) }()
	select {
	case err := <-errB:
		t.Fatalf("second publisher did not wait for cross-process lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseA)
	if err := <-errA; err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	if err := <-errB; err == nil {
		t.Fatal("different second Publish succeeded")
	}

	target := filepath.Join(realTempDir(t), "restore")
	fetched, err := repoB.Fetch(context.Background(), manifestA.BackupID, target)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.AppliedIndex != manifestA.AppliedIndex {
		t.Fatalf("fetched AppliedIndex = %d, want first publisher %d", fetched.AppliedIndex, manifestA.AppliedIndex)
	}
}

func TestRepositoryIDLockWaitHonorsContextAndDoesNotPoisonLock(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repoA, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	repoB, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	locked := make(chan struct{})
	release := make(chan struct{})
	repoA.beforeCommit = func(string) error {
		close(locked)
		<-release
		return nil
	}
	publishA := make(chan error, 1)
	go func() { publishA <- repoA.Publish(context.Background(), checkpointDir, manifest) }()
	<-locked

	assertCanceledPromptly := func(name string, operation func(context.Context) error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := operation(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s error = %v, want deadline exceeded", name, err)
		}
		if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
			t.Fatalf("%s cancellation took %v", name, elapsed)
		}
	}
	assertCanceledPromptly("Delete", func(ctx context.Context) error {
		return repoB.Delete(ctx, manifest.BackupID)
	})
	assertCanceledPromptly("Publish", func(ctx context.Context) error {
		return repoB.Publish(ctx, checkpointDir, manifest)
	})

	close(release)
	if err := <-publishA; err != nil {
		t.Fatal(err)
	}
	if err := repoB.Delete(context.Background(), manifest.BackupID); err != nil {
		t.Fatalf("Delete after lock release: %v", err)
	}
	if err := repoB.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatalf("Publish after canceled wait and release: %v", err)
	}
}

func TestRepositoryIDLockSerializesAcrossSubprocess(t *testing.T) {
	const (
		helperEnv = "NUFS_FLOCK_HELPER"
		rootEnv   = "NUFS_FLOCK_ROOT"
		backupID  = "subprocess-lock"
	)
	if os.Getenv(helperEnv) == "1" {
		repo, err := NewFilesystemBackupRepository(os.Getenv(rootEnv))
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.ensureRepositoryRoots(); err != nil {
			t.Fatal(err)
		}
		lock, err := repo.acquireIDLock(context.Background(), backupID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stdout.WriteString("locked\n"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond)
		if err := lock.release(); err != nil {
			t.Fatal(err)
		}
		return
	}

	root := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestRepositoryIDLockSerializesAcrossSubprocess$")
	command.Env = append(os.Environ(), helperEnv+"=1", rootEnv+"="+root)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "locked" {
		_ = command.Process.Kill()
		t.Fatalf("subprocess lock signal = %q, %v", scanner.Text(), scanner.Err())
	}

	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.ensureRepositoryRoots(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	if lock, err := repo.acquireIDLock(ctx, backupID); !errors.Is(err, context.DeadlineExceeded) {
		if lock != nil {
			_ = lock.release()
		}
		t.Fatalf("parent lock while subprocess owns it = %v, want deadline exceeded", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	lock, err := repo.acquireIDLock(context.Background(), backupID)
	if err != nil {
		t.Fatalf("parent lock after subprocess exit: %v", err)
	}
	if err := lock.release(); err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryUsesUniqueStagingAttemptMetadata(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	repo.afterStage = func(stageDir string) error {
		if filepath.Base(stageDir) == manifest.BackupID {
			t.Fatalf("staging directory reused backup ID: %q", stageDir)
		}
		data, err := os.ReadFile(filepath.Join(stageDir, backupAttemptFile))
		if err != nil {
			t.Fatalf("read attempt metadata: %v", err)
		}
		attempt, err := decodeFilesystemAttempt(data)
		if err != nil {
			t.Fatalf("decode attempt metadata: %v", err)
		}
		if attempt.BackupID != manifest.BackupID || attempt.AttemptID != filepath.Base(stageDir) {
			t.Fatalf("attempt metadata = %+v, stage = %q", attempt, stageDir)
		}
		return nil
	}
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryDeleteWaitsForPublisherAndHidesAtomically(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	publisher, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	deleter, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	promoted := make(chan struct{})
	release := make(chan struct{})
	publisher.beforeCommit = func(string) error {
		close(promoted)
		<-release
		return nil
	}
	publishErr := make(chan error, 1)
	go func() { publishErr <- publisher.Publish(context.Background(), checkpointDir, manifest) }()
	<-promoted
	deleteErr := make(chan error, 1)
	go func() { deleteErr <- deleter.Delete(context.Background(), manifest.BackupID) }()
	select {
	case err := <-deleteErr:
		t.Fatalf("Delete did not wait for publisher: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-publishErr; err != nil {
		t.Fatal(err)
	}
	if err := <-deleteErr; err != nil {
		t.Fatal(err)
	}
	if _, err := deleter.Fetch(context.Background(), manifest.BackupID, filepath.Join(realTempDir(t), "restore")); err == nil {
		t.Fatal("new Fetch saw deleted backup")
	}
}

func TestRepositoryFetchAndDeleteEitherFinishOrFailCleanly(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	fetcher, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	deleter, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := fetcher.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	opened := make(chan struct{})
	release := make(chan struct{})
	fetcher.afterOpenFetch = func() {
		close(opened)
		<-release
	}
	target := filepath.Join(realTempDir(t), "restore")
	fetchErr := make(chan error, 1)
	go func() {
		_, err := fetcher.Fetch(context.Background(), manifest.BackupID, target)
		fetchErr <- err
	}()
	<-opened
	if err := deleter.Delete(context.Background(), manifest.BackupID); err != nil {
		t.Fatal(err)
	}
	close(release)
	err = <-fetchErr
	if err != nil {
		if _, statErr := os.Stat(target); !os.IsNotExist(statErr) {
			t.Fatalf("failed Fetch left partial target: %v", statErr)
		}
	}
	if _, err := fetcher.Fetch(context.Background(), manifest.BackupID, filepath.Join(realTempDir(t), "new")); err == nil {
		t.Fatal("Fetch started after Delete saw backup")
	}
}

func TestRepositoryRejectsAncestorSymlinkAndReplacement(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}

	base := realTempDir(t)
	outside := realTempDir(t)
	link := filepath.Join(base, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	existingOutside := filepath.Join(outside, "existing")
	if err := os.Mkdir(existingOutside, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, filepath.Join(link, "existing", "restore")); err == nil {
		t.Fatal("Fetch followed ancestor symlink")
	}
	if _, err := os.Stat(filepath.Join(existingOutside, "restore")); !os.IsNotExist(err) {
		t.Fatalf("ancestor symlink target was modified: %v", err)
	}

	target := filepath.Join(base, "replacement")
	repo.beforeOpenTarget = func(path string) error {
		if err := os.Remove(path); err != nil {
			return err
		}
		return os.Symlink(outside, path)
	}
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, target); err == nil {
		t.Fatal("Fetch accepted target replaced by symlink")
	}
	entries, err := os.ReadDir(existingOutside)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("replacement symlink target was modified: %v", entries)
	}

	ordinaryTarget := filepath.Join(base, "ordinary-replacement")
	sentinel := filepath.Join(ordinaryTarget, "caller-owned.txt")
	repo.beforeOpenTarget = func(path string) error {
		if err := os.Remove(path); err != nil {
			return err
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			return err
		}
		return os.WriteFile(sentinel, []byte("keep"), 0o600)
	}
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, ordinaryTarget); err == nil {
		t.Fatal("Fetch accepted target replaced by ordinary directory")
	}
	data, err := os.ReadFile(sentinel)
	if err != nil || string(data) != "keep" {
		t.Fatalf("caller-owned replacement content = %q, %v", data, err)
	}
}

func TestRepositoryAcceptsRawPlatformTemporaryDirectory(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, target); err != nil {
		t.Fatalf("Fetch raw temporary target %q: %v", target, err)
	}
	tmpAliasBase, err := os.MkdirTemp("/tmp", "nufs-restore-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpAliasBase)
	tmpAliasTarget := filepath.Join(tmpAliasBase, "restore")
	if _, err := repo.Fetch(context.Background(), manifest.BackupID, tmpAliasTarget); err != nil {
		t.Fatalf("Fetch /tmp alias target %q: %v", tmpAliasTarget, err)
	}
}

func realTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	real, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return real
}

func TestRepositoryPostMarkerSyncFailureIsNotListable(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	repo, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	realSync := repo.syncDirectory
	finalDir := filepath.Join(root, backupCommittedDir, manifest.BackupID)
	repo.syncDirectory = func(path string) error {
		if path == finalDir {
			return errors.New("injected final directory sync failure")
		}
		return realSync(path)
	}
	if err := repo.Publish(context.Background(), checkpointDir, manifest); err == nil {
		t.Fatal("Publish returned nil")
	}
	got, err := repo.ListCommitted(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("sync-failed backup is visible: %v", got)
	}
}

func TestRepositoryJanitorWaitsForActiveAttempt(t *testing.T) {
	root := t.TempDir()
	checkpointDir, manifest := createManifestFixture(t)
	publisher, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	janitor, err := NewFilesystemBackupRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	staged := make(chan struct{})
	release := make(chan struct{})
	cutoff := time.Now().UTC().Add(-time.Hour)
	publisher.afterStage = func(stageDir string) error {
		old := cutoff.Add(-time.Hour)
		if err := os.Chtimes(stageDir, old, old); err != nil {
			return err
		}
		close(staged)
		<-release
		return nil
	}
	publishErr := make(chan error, 1)
	go func() { publishErr <- publisher.Publish(context.Background(), checkpointDir, manifest) }()
	<-staged
	janitorErr := make(chan error, 1)
	go func() { janitorErr <- janitor.DeleteStagingOlderThan(context.Background(), cutoff) }()
	select {
	case err := <-janitorErr:
		t.Fatalf("janitor did not wait for active ID lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-publishErr; err != nil {
		t.Fatal(err)
	}
	if err := <-janitorErr; err != nil {
		t.Fatal(err)
	}
	got, err := janitor.ListCommitted(context.Background())
	if err != nil || len(got) != 1 {
		t.Fatalf("committed backup after janitor = %v, %v", got, err)
	}
}
