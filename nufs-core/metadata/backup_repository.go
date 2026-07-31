package metadata

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	backupStagingDir   = "staging"
	backupCommittedDir = "backups"
	backupLocksDir     = ".locks"
	backupTrashDir     = ".trash"
	backupFilesDir     = "files"
	backupManifestFile = "manifest.json"
	backupCommitMarker = "COMMITTED"
	backupAttemptFile  = "attempt.json"
)

type BackupDescriptor struct {
	ID           string
	CreatedAt    time.Time
	AppliedIndex uint64
	TotalBytes   int64
}

type BackupRepository interface {
	Publish(ctx context.Context, checkpointDir string, manifest *BackupManifest) error
	ListCommitted(ctx context.Context) ([]BackupDescriptor, error)
	Fetch(ctx context.Context, backupID, targetDir string) (*BackupManifest, error)
	Delete(ctx context.Context, backupID string) error
	DeleteStagingOlderThan(ctx context.Context, cutoff time.Time) error
}

type FilesystemBackupRepository struct {
	root             string
	beforeCommit     func(finalDir string) error
	afterStage       func(stageDir string) error
	afterOpenFetch   func()
	beforeOpenTarget func(targetDir string) error
	syncDirectory    func(string) error
	lockRetry        time.Duration
}

type filesystemBackupAttempt struct {
	BackupID  string    `json:"backup_id"`
	AttemptID string    `json:"attempt_id"`
	CreatedAt time.Time `json:"created_at"`
}

func NewFilesystemBackupRepository(root string) (*FilesystemBackupRepository, error) {
	if root == "" {
		return nil, fmt.Errorf("backup repository: root is required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("backup repository: resolve root: %w", err)
	}
	return &FilesystemBackupRepository{
		root:          filepath.Clean(absolute),
		syncDirectory: syncDirectory,
		lockRetry:     10 * time.Millisecond,
	}, nil
}

func (r *FilesystemBackupRepository) Publish(ctx context.Context, checkpointDir string, manifest *BackupManifest) (retErr error) {
	if err := validateRepositoryManifest(manifest); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := VerifyBackupArtifact(ctx, checkpointDir, manifest); err != nil {
		return fmt.Errorf("backup repository: verify source artifact: %w", err)
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("backup repository: encode manifest: %w", err)
	}
	if err := r.ensureRepositoryRoots(); err != nil {
		return err
	}
	idLock, err := r.acquireIDLock(ctx, manifest.BackupID)
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, idLock.release())
	}()
	same, err := r.matchesCommitted(manifest.BackupID, manifestData)
	if err != nil {
		return err
	}
	if same {
		return nil
	}
	stagingParent := filepath.Join(r.root, backupStagingDir)
	finalParent := filepath.Join(r.root, backupCommittedDir)
	attemptID, err := newBackupAttemptID()
	if err != nil {
		return err
	}
	attempt := filesystemBackupAttempt{
		BackupID:  manifest.BackupID,
		AttemptID: attemptID,
		CreatedAt: time.Now().UTC(),
	}
	attemptData, err := json.Marshal(attempt)
	if err != nil {
		return fmt.Errorf("backup repository: encode attempt metadata: %w", err)
	}
	stageDir := filepath.Join(stagingParent, attemptID)
	if err := os.Mkdir(stageDir, 0o700); err != nil {
		return fmt.Errorf("backup repository: create staging directory: %w", err)
	}
	stageOwned := true
	defer func() {
		if stageOwned {
			_ = os.RemoveAll(stageDir)
		}
	}()

	stageRoot, err := os.OpenRoot(stageDir)
	if err != nil {
		return fmt.Errorf("backup repository: open staging directory: %w", err)
	}
	if err := stageRoot.Mkdir(backupFilesDir, 0o700); err != nil {
		stageRoot.Close()
		return fmt.Errorf("backup repository: create staging files directory: %w", err)
	}
	if err := writeRootFileExclusive(stageRoot, backupAttemptFile, attemptData, 0o600); err != nil {
		stageRoot.Close()
		return fmt.Errorf("backup repository: write attempt metadata: %w", err)
	}
	if err := copyDeclaredFiles(ctx, checkpointDir, stageRoot, manifest.Files); err != nil {
		stageRoot.Close()
		return err
	}
	if err := writeRootFileExclusive(stageRoot, backupManifestFile, manifestData, 0o600); err != nil {
		stageRoot.Close()
		return fmt.Errorf("backup repository: write staging manifest: %w", err)
	}
	if err := stageRoot.Close(); err != nil {
		return fmt.Errorf("backup repository: close staging directory: %w", err)
	}
	if err := syncDirectoryTree(stageDir, r.syncDirectory); err != nil {
		return fmt.Errorf("backup repository: sync staging tree: %w", err)
	}
	if r.afterStage != nil {
		if err := r.afterStage(stageDir); err != nil {
			return fmt.Errorf("backup repository: after stage: %w", err)
		}
	}

	finalDir := filepath.Join(finalParent, manifest.BackupID)
	if err := r.discardStaleIncompleteFinalLocked(manifest.BackupID); err != nil {
		return err
	}
	if err := os.Rename(stageDir, finalDir); err != nil {
		return fmt.Errorf("backup repository: promote staging directory: %w", err)
	}
	stageOwned = false
	finalOwned := true
	committed := false
	defer func() {
		if retErr == nil || !finalOwned || committed {
			return
		}
		retErr = errors.Join(retErr, r.discardOwnedFinalLocked(manifest.BackupID, attemptID))
	}()
	if err := r.syncDirectory(stagingParent); err != nil {
		return fmt.Errorf("backup repository: sync staging root: %w", err)
	}
	if err := r.syncDirectory(finalParent); err != nil {
		return fmt.Errorf("backup repository: sync backup root: %w", err)
	}

	if r.beforeCommit != nil {
		if err := r.beforeCommit(finalDir); err != nil {
			return fmt.Errorf("backup repository: before commit: %w", err)
		}
	}
	if err := createFilesystemCommitMarker(finalDir, r.syncDirectory); err != nil {
		return err
	}
	committed = true
	return nil
}

func (r *FilesystemBackupRepository) ListCommitted(ctx context.Context) ([]BackupDescriptor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	parent := filepath.Join(r.root, backupCommittedDir)
	entries, err := os.ReadDir(parent)
	if errors.Is(err, os.ErrNotExist) {
		return []BackupDescriptor{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("backup repository: list committed backups: %w", err)
	}
	descriptors := make([]BackupDescriptor, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() || validateBackupID(entry.Name()) != nil {
			continue
		}
		dir := filepath.Join(parent, entry.Name())
		if !isRegularFile(filepath.Join(dir, backupCommitMarker)) {
			continue
		}
		manifest, err := readFilesystemManifest(dir)
		if err != nil || manifest.BackupID != entry.Name() {
			continue
		}
		descriptors = append(descriptors, descriptorFromManifest(manifest))
	}
	sortBackupDescriptors(descriptors)
	return descriptors, nil
}

func (r *FilesystemBackupRepository) Fetch(ctx context.Context, backupID, targetDir string) (_ *BackupManifest, retErr error) {
	if err := validateBackupID(backupID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	finalDir := filepath.Join(r.root, backupCommittedDir, backupID)
	if !isRegularFile(filepath.Join(finalDir, backupCommitMarker)) {
		return nil, fmt.Errorf("backup repository: backup %q is not committed", backupID)
	}
	manifest, err := readFilesystemManifest(finalDir)
	if err != nil {
		return nil, err
	}
	if manifest.BackupID != backupID {
		return nil, fmt.Errorf("backup repository: manifest ID %q does not match backup %q", manifest.BackupID, backupID)
	}

	target, err := prepareRestoreTarget(targetDir, r.beforeOpenTarget)
	if err != nil {
		return nil, err
	}
	var createdFiles, createdDirectories []restorePathIdentity
	defer func() {
		removeTarget := retErr != nil && target.created
		if retErr != nil {
			retErr = errors.Join(
				retErr,
				cleanupRootEntries(target.root, createdFiles, createdDirectories),
			)
		}
		retErr = errors.Join(retErr, target.finish(removeTarget))
	}()
	repositoryRoot, err := os.OpenRoot(r.root)
	if err != nil {
		return nil, fmt.Errorf("backup repository: open repository root: %w", err)
	}
	defer repositoryRoot.Close()
	sourceRoot, err := repositoryRoot.OpenRoot(filepath.ToSlash(filepath.Join(backupCommittedDir, backupID, backupFilesDir)))
	if err != nil {
		return nil, fmt.Errorf("backup repository: open backup files: %w", err)
	}
	defer sourceRoot.Close()
	if r.afterOpenFetch != nil {
		r.afterOpenFetch()
	}

	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := copyRootFileExclusive(
			sourceRoot,
			file.Path,
			target.root,
			file.Path,
			&createdFiles,
			&createdDirectories,
		); err != nil {
			return nil, fmt.Errorf("backup repository: fetch %q: %w", file.Path, err)
		}
	}
	if _, err := VerifyBackupArtifact(ctx, target.absolute, manifest); err != nil {
		return nil, fmt.Errorf("backup repository: verify fetched artifact: %w", err)
	}
	return manifest, nil
}

func (r *FilesystemBackupRepository) Delete(ctx context.Context, backupID string) (retErr error) {
	if err := validateBackupID(backupID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.ensureRepositoryRoots(); err != nil {
		return err
	}
	idLock, err := r.acquireIDLock(ctx, backupID)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, idLock.release()) }()
	finalDir := filepath.Join(r.root, backupCommittedDir, backupID)
	if _, err := os.Lstat(finalDir); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("backup repository: inspect %q before delete: %w", backupID, err)
	}
	return r.moveFinalToTrashLocked(backupID)
}

func (r *FilesystemBackupRepository) DeleteStagingOlderThan(ctx context.Context, cutoff time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.ensureRepositoryRoots(); err != nil {
		return err
	}
	parent := filepath.Join(r.root, backupStagingDir)
	entries, err := os.ReadDir(parent)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("backup repository: list staging backups: %w", err)
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() || validateAttemptID(entry.Name()) != nil {
			continue
		}
		stageDir := filepath.Join(parent, entry.Name())
		attempt, err := readFilesystemAttempt(stageDir)
		if err != nil {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.ModTime().Before(cutoff) {
			continue
		}
		idLock, err := r.acquireIDLock(ctx, attempt.BackupID)
		if err != nil {
			return err
		}
		currentAttempt, readErr := readFilesystemAttempt(stageDir)
		currentInfo, statErr := os.Lstat(stageDir)
		if readErr == nil && statErr == nil && currentAttempt == attempt && currentInfo.ModTime().Before(cutoff) {
			if err := os.RemoveAll(stageDir); err != nil {
				_ = idLock.release()
				return fmt.Errorf("backup repository: delete staging %q: %w", entry.Name(), err)
			}
			if err := r.syncDirectory(parent); err != nil {
				_ = idLock.release()
				return fmt.Errorf("backup repository: sync staging root: %w", err)
			}
		}
		if err := idLock.release(); err != nil {
			return err
		}
	}
	return nil
}

func validateRepositoryManifest(manifest *BackupManifest) error {
	if err := manifest.Validate(); err != nil {
		return err
	}
	if err := validateBackupID(manifest.BackupID); err != nil {
		return err
	}
	return nil
}

func validateBackupID(id string) error {
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, `/\`) ||
		strings.ContainsRune(id, 0) || isWindowsVolumePath(id) ||
		filepath.VolumeName(id) != "" || filepath.Clean(id) != id {
		return fmt.Errorf("backup repository: invalid backup ID %q", id)
	}
	return nil
}

func descriptorFromManifest(manifest *BackupManifest) BackupDescriptor {
	return BackupDescriptor{
		ID:           manifest.BackupID,
		CreatedAt:    manifest.CreatedAt,
		AppliedIndex: manifest.AppliedIndex,
		TotalBytes:   manifest.TotalBytes,
	}
}

func sortBackupDescriptors(descriptors []BackupDescriptor) {
	sort.Slice(descriptors, func(i, j int) bool {
		if descriptors[i].CreatedAt.Equal(descriptors[j].CreatedAt) {
			return descriptors[i].ID < descriptors[j].ID
		}
		return descriptors[i].CreatedAt.After(descriptors[j].CreatedAt)
	})
}

func decodeBackupManifest(data []byte) (*BackupManifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var manifest BackupManifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("backup repository: decode manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("additional JSON value")
		}
		return nil, fmt.Errorf("backup repository: decode manifest: %w", err)
	}
	if err := validateRepositoryManifest(&manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func (r *FilesystemBackupRepository) matchesCommitted(backupID string, manifestData []byte) (bool, error) {
	finalDir := filepath.Join(r.root, backupCommittedDir, backupID)
	if !isRegularFile(filepath.Join(finalDir, backupCommitMarker)) {
		return false, nil
	}
	existing, err := readFilesystemManifest(finalDir)
	if err != nil {
		return false, err
	}
	if existing.BackupID != backupID {
		return false, fmt.Errorf("backup repository: committed manifest ID %q does not match backup %q", existing.BackupID, backupID)
	}
	existingData, err := json.Marshal(existing)
	if err != nil {
		return false, fmt.Errorf("backup repository: encode existing manifest: %w", err)
	}
	if !bytes.Equal(existingData, manifestData) {
		return false, fmt.Errorf("backup repository: committed backup %q already exists with different contents", backupID)
	}
	return true, nil
}

func readFilesystemManifest(dir string) (*BackupManifest, error) {
	path := filepath.Join(dir, backupManifestFile)
	if !isRegularFile(path) {
		return nil, fmt.Errorf("backup repository: manifest is missing or not a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("backup repository: read manifest: %w", err)
	}
	return decodeBackupManifest(data)
}

func isRegularFile(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode().IsRegular()
}

func createFilesystemCommitMarker(finalDir string, syncDir func(string) error) error {
	root, err := os.OpenRoot(finalDir)
	if err != nil {
		return fmt.Errorf("backup repository: open final directory: %w", err)
	}
	defer root.Close()
	tempName := backupCommitMarker + ".tmp"
	file, err := root.OpenFile(tempName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("backup repository: create temporary commit marker: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("backup repository: sync temporary commit marker: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("backup repository: close temporary commit marker: %w", err)
	}
	if err := root.Rename(tempName, backupCommitMarker); err != nil {
		_ = root.Remove(tempName)
		return fmt.Errorf("backup repository: install commit marker: %w", err)
	}
	if err := syncDir(finalDir); err != nil {
		removeErr := root.Remove(backupCommitMarker)
		cleanupSyncErr := syncDir(finalDir)
		return errors.Join(
			fmt.Errorf("backup repository: sync committed directory: %w", err),
			wrapCleanupError("remove uncertain commit marker", removeErr),
			wrapCleanupError("sync fail-closed marker removal", cleanupSyncErr),
		)
	}
	return nil
}

func wrapCleanupError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("backup repository: cleanup %s: %w", operation, err)
}

func syncDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func syncDirectoryTree(root string, syncDir func(string) error) error {
	var dirs []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("backup repository: staging directory contains symlink %q", path)
		}
		if entry.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(dirs, func(i, j int) bool {
		return strings.Count(filepath.Clean(dirs[i]), string(os.PathSeparator)) >
			strings.Count(filepath.Clean(dirs[j]), string(os.PathSeparator))
	})
	for _, dir := range dirs {
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	return nil
}

type filesystemIDLock struct {
	file *os.File
}

func (r *FilesystemBackupRepository) ensureRepositoryRoots() error {
	if err := ensureDurableDirectory(r.root, r.syncDirectory); err != nil {
		return fmt.Errorf("backup repository: create repository root: %w", err)
	}
	for _, name := range []string{backupLocksDir, backupStagingDir, backupCommittedDir, backupTrashDir} {
		dir := filepath.Join(r.root, name)
		info, err := os.Lstat(dir)
		created := false
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
				return fmt.Errorf("backup repository: create %s root: %w", name, err)
			}
			info, err = os.Lstat(dir)
			created = true
		}
		if err != nil {
			return fmt.Errorf("backup repository: inspect %s root: %w", name, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("backup repository: %s root is not a directory", name)
		}
		if created {
			if err := r.syncDirectory(dir); err != nil {
				return fmt.Errorf("backup repository: sync %s root: %w", name, err)
			}
			if err := r.syncDirectory(r.root); err != nil {
				return fmt.Errorf("backup repository: sync repository root: %w", err)
			}
		}
	}
	return nil
}

func ensureDurableDirectory(dir string, syncDir func(string) error) error {
	info, err := os.Lstat(dir)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("%q is not a directory", dir)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var missing []string
	current := dir
	for {
		if _, err := os.Lstat(current); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := syncDir(missing[i]); err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		return syncDir(filepath.Dir(missing[len(missing)-1]))
	}
	return nil
}

func (r *FilesystemBackupRepository) acquireIDLock(ctx context.Context, backupID string) (*filesystemIDLock, error) {
	if err := validateBackupID(backupID); err != nil {
		return nil, err
	}
	repositoryRoot, err := os.OpenRoot(r.root)
	if err != nil {
		return nil, fmt.Errorf("backup repository: anchor ID lock root: %w", err)
	}
	defer repositoryRoot.Close()
	lockName := filepath.Join(backupLocksDir, backupID+".lock")
	existed := true
	if info, err := repositoryRoot.Lstat(lockName); errors.Is(err, os.ErrNotExist) {
		existed = false
	} else if err != nil {
		return nil, fmt.Errorf("backup repository: inspect ID lock: %w", err)
	} else if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("backup repository: ID lock is not a regular file")
	}
	file, err := repositoryRoot.OpenFile(lockName, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("backup repository: open ID lock: %w", err)
	}
	if !existed {
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, fmt.Errorf("backup repository: sync ID lock: %w", err)
		}
		if err := r.syncDirectory(filepath.Join(r.root, backupLocksDir)); err != nil {
			file.Close()
			return nil, fmt.Errorf("backup repository: sync lock root: %w", err)
		}
	}
	retry := r.lockRetry
	if retry <= 0 {
		retry = 10 * time.Millisecond
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			file.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return &filesystemIDLock{file: file}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			file.Close()
			return nil, fmt.Errorf("backup repository: lock backup ID: %w", err)
		}
		timer.Reset(retry)
	}
}

func (l *filesystemIDLock) release() error {
	if l == nil || l.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	if unlockErr != nil {
		return fmt.Errorf("backup repository: unlock backup ID: %w", unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("backup repository: close backup ID lock: %w", closeErr)
	}
	return nil
}

func newBackupAttemptID() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", fmt.Errorf("backup repository: generate attempt ID: %w", err)
	}
	return hex.EncodeToString(data[:]), nil
}

func validateAttemptID(id string) error {
	if len(id) != 32 {
		return fmt.Errorf("backup repository: invalid attempt ID %q", id)
	}
	decoded, err := hex.DecodeString(id)
	if err != nil || hex.EncodeToString(decoded) != id {
		return fmt.Errorf("backup repository: invalid attempt ID %q", id)
	}
	return nil
}

func decodeFilesystemAttempt(data []byte) (filesystemBackupAttempt, error) {
	var attempt filesystemBackupAttempt
	if err := json.Unmarshal(data, &attempt); err != nil {
		return attempt, fmt.Errorf("backup repository: decode attempt metadata: %w", err)
	}
	if err := validateBackupID(attempt.BackupID); err != nil {
		return attempt, err
	}
	if err := validateAttemptID(attempt.AttemptID); err != nil {
		return attempt, err
	}
	if attempt.CreatedAt.IsZero() {
		return attempt, fmt.Errorf("backup repository: attempt creation time is required")
	}
	return attempt, nil
}

func readFilesystemAttempt(dir string) (filesystemBackupAttempt, error) {
	data, err := os.ReadFile(filepath.Join(dir, backupAttemptFile))
	if err != nil {
		return filesystemBackupAttempt{}, fmt.Errorf("backup repository: read attempt metadata: %w", err)
	}
	attempt, err := decodeFilesystemAttempt(data)
	if err != nil {
		return filesystemBackupAttempt{}, err
	}
	if attempt.AttemptID != filepath.Base(dir) && filepath.Base(filepath.Dir(dir)) == backupStagingDir {
		return filesystemBackupAttempt{}, fmt.Errorf("backup repository: attempt directory does not match metadata")
	}
	return attempt, nil
}

func (r *FilesystemBackupRepository) discardStaleIncompleteFinalLocked(backupID string) error {
	finalDir := filepath.Join(r.root, backupCommittedDir, backupID)
	info, err := os.Lstat(finalDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("backup repository: inspect incomplete final: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("backup repository: final path is not a directory")
	}
	if isRegularFile(filepath.Join(finalDir, backupCommitMarker)) {
		return fmt.Errorf("backup repository: committed backup already exists")
	}
	attempt, err := readFilesystemAttempt(finalDir)
	if err != nil {
		return fmt.Errorf("backup repository: incomplete final ownership is unknown: %w", err)
	}
	if attempt.BackupID != backupID {
		return fmt.Errorf(
			"backup repository: incomplete final ownership is unknown: attempt belongs to %q",
			attempt.BackupID,
		)
	}
	return r.moveFinalToTrashLocked(backupID)
}

func (r *FilesystemBackupRepository) discardOwnedFinalLocked(backupID, attemptID string) error {
	finalDir := filepath.Join(r.root, backupCommittedDir, backupID)
	if _, err := os.Lstat(finalDir); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	attempt, err := readFilesystemAttempt(finalDir)
	if err != nil {
		return err
	}
	if attempt.BackupID != backupID || attempt.AttemptID != attemptID {
		return fmt.Errorf("backup repository: refusing to clean final owned by another attempt")
	}
	return r.moveFinalToTrashLocked(backupID)
}

func (r *FilesystemBackupRepository) moveFinalToTrashLocked(backupID string) error {
	trashID, err := newBackupAttemptID()
	if err != nil {
		return err
	}
	finalDir := filepath.Join(r.root, backupCommittedDir, backupID)
	trashDir := filepath.Join(r.root, backupTrashDir, backupID+"--"+trashID)
	if err := os.Rename(finalDir, trashDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("backup repository: hide backup %q: %w", backupID, err)
	}
	if err := r.syncDirectory(filepath.Join(r.root, backupCommittedDir)); err != nil {
		return fmt.Errorf("backup repository: sync hidden backup namespace: %w", err)
	}
	if err := os.RemoveAll(trashDir); err != nil {
		return fmt.Errorf("backup repository: remove trash %q: %w", trashDir, err)
	}
	if err := r.syncDirectory(filepath.Join(r.root, backupTrashDir)); err != nil {
		return fmt.Errorf("backup repository: sync trash root: %w", err)
	}
	return nil
}

func copyDeclaredFiles(ctx context.Context, checkpointDir string, targetRoot *os.Root, files []BackupFile) error {
	info, err := os.Lstat(checkpointDir)
	if err != nil {
		return fmt.Errorf("backup repository: inspect checkpoint: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("backup repository: checkpoint is not a directory")
	}
	sourceRoot, err := os.OpenRoot(checkpointDir)
	if err != nil {
		return fmt.Errorf("backup repository: open checkpoint: %w", err)
	}
	defer sourceRoot.Close()
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		targetName := filepath.ToSlash(filepath.Join(backupFilesDir, file.Path))
		if err := copyRootFileExclusive(sourceRoot, file.Path, targetRoot, targetName, nil, nil); err != nil {
			return fmt.Errorf("backup repository: stage %q: %w", file.Path, err)
		}
	}
	return nil
}

func copyRootFileExclusive(
	sourceRoot *os.Root,
	sourceName string,
	targetRoot *os.Root,
	targetName string,
	createdFiles *[]restorePathIdentity,
	createdDirectories *[]restorePathIdentity,
) error {
	if err := rejectRootSymlinks(sourceRoot, sourceName, false); err != nil {
		return err
	}
	source, err := sourceRoot.Open(sourceName)
	if err != nil {
		return err
	}
	sourceInfo, err := source.Stat()
	if err != nil {
		source.Close()
		return err
	}
	if !sourceInfo.Mode().IsRegular() {
		source.Close()
		return fmt.Errorf("source is not a regular file")
	}
	if err := ensureRootParents(targetRoot, targetName, createdDirectories); err != nil {
		source.Close()
		return err
	}
	target, err := targetRoot.OpenFile(targetName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		source.Close()
		return err
	}
	_, copyErr := io.Copy(target, source)
	sourceCloseErr := source.Close()
	syncErr := target.Sync()
	targetInfo, statErr := target.Stat()
	targetCloseErr := target.Close()
	if copyErr != nil || sourceCloseErr != nil || syncErr != nil || statErr != nil || targetCloseErr != nil {
		_ = targetRoot.Remove(targetName)
	}
	if copyErr != nil {
		return copyErr
	}
	if sourceCloseErr != nil {
		return sourceCloseErr
	}
	if syncErr != nil {
		return syncErr
	}
	if statErr != nil {
		return statErr
	}
	if targetCloseErr != nil {
		return targetCloseErr
	}
	if createdFiles != nil {
		*createdFiles = append(*createdFiles, restorePathIdentity{name: targetName, info: targetInfo})
	}
	return nil
}

func writeRootFileExclusive(root *os.Root, name string, data []byte, perm os.FileMode) error {
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func ensureRootParents(root *os.Root, name string, createdDirectories *[]restorePathIdentity) error {
	parent := filepath.ToSlash(filepath.Dir(filepath.FromSlash(name)))
	if parent == "." {
		return nil
	}
	current := ""
	for _, part := range strings.Split(parent, "/") {
		if current == "" {
			current = part
		} else {
			current += "/" + part
		}
		info, err := root.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := root.Mkdir(current, 0o700); err != nil {
				return err
			}
			info, err = root.Lstat(current)
			if err == nil && createdDirectories != nil {
				*createdDirectories = append(
					*createdDirectories,
					restorePathIdentity{name: current, info: info},
				)
			}
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("target parent %q is not a directory", current)
		}
	}
	return nil
}

func rejectRootSymlinks(root *os.Root, name string, allowMissingFinal bool) error {
	current := ""
	parts := strings.Split(filepath.ToSlash(name), "/")
	for i, part := range parts {
		if current == "" {
			current = part
		} else {
			current += "/" + part
		}
		info, err := root.Lstat(current)
		if allowMissingFinal && i == len(parts)-1 && errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("path component %q is a symbolic link", current)
		}
		if i < len(parts)-1 && !info.IsDir() {
			return fmt.Errorf("path component %q is not a directory", current)
		}
	}
	return nil
}

type restorePathIdentity struct {
	name string
	info os.FileInfo
}

type preparedRestoreTarget struct {
	root               *os.Root
	cleanupAnchor      *os.Root
	createdDirectories []restorePathIdentity
	absolute           string
	created            bool
}

func (t *preparedRestoreTarget) finish(remove bool) error {
	if t == nil {
		return nil
	}
	var errs []error
	if t.root != nil {
		errs = append(errs, t.root.Close())
		t.root = nil
	}
	if t.cleanupAnchor != nil {
		if remove {
			errs = append(
				errs,
				removeRootEntriesIdentityChecked(
					t.cleanupAnchor,
					reverseRestoreEntries(t.createdDirectories),
				),
			)
		}
		errs = append(errs, t.cleanupAnchor.Close())
		t.cleanupAnchor = nil
	}
	return errors.Join(errs...)
}

func prepareRestoreTarget(targetDir string, beforeOpen func(string) error) (*preparedRestoreTarget, error) {
	absolute, err := filepath.Abs(targetDir)
	if err != nil {
		return nil, fmt.Errorf("backup repository: resolve target directory: %w", err)
	}
	absolute = canonicalizeTrustedRootAlias(filepath.Clean(absolute))
	rootPath, components, err := splitAbsolutePath(absolute)
	if err != nil {
		return nil, err
	}
	current := rootPath
	firstMissing := -1
	for index, component := range components {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err == nil {
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return nil, fmt.Errorf("backup repository: target component %q is not a directory", current)
			}
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("backup repository: inspect target component %q: %w", current, err)
		}
		firstMissing = index
		break
	}

	created := firstMissing >= 0
	anchorPath := filepath.Dir(absolute)
	relativeComponents := []string{filepath.Base(absolute)}
	if created {
		anchorPath = rootPath
		if firstMissing > 0 {
			anchorPath = filepath.Join(append([]string{rootPath}, components[:firstMissing]...)...)
		}
		relativeComponents = components[firstMissing:]
	}
	anchorInfo, err := os.Lstat(anchorPath)
	if err != nil || anchorInfo.Mode()&os.ModeSymlink != 0 || !anchorInfo.IsDir() {
		return nil, fmt.Errorf("backup repository: unsafe target anchor %q", anchorPath)
	}
	anchor, err := os.OpenRoot(anchorPath)
	if err != nil {
		return nil, fmt.Errorf("backup repository: anchor target ancestor: %w", err)
	}
	var createdDirectories []restorePathIdentity
	cleanup := func() error {
		cleanupErr := removeRootEntriesIdentityChecked(
			anchor,
			reverseRestoreEntries(createdDirectories),
		)
		closeErr := anchor.Close()
		return errors.Join(cleanupErr, closeErr)
	}
	anchoredInfo, statErr := anchor.Stat(".")
	currentAnchorInfo, lstatErr := os.Lstat(anchorPath)
	if statErr != nil || lstatErr != nil || currentAnchorInfo.Mode()&os.ModeSymlink != 0 ||
		!os.SameFile(anchorInfo, currentAnchorInfo) || !os.SameFile(currentAnchorInfo, anchoredInfo) {
		return nil, errors.Join(
			fmt.Errorf("backup repository: target anchor changed while opening"),
			cleanup(),
		)
	}

	relative := ""
	var targetInfo os.FileInfo
	for _, component := range relativeComponents {
		relative = filepath.ToSlash(filepath.Join(relative, component))
		info, err := anchor.Lstat(relative)
		if errors.Is(err, os.ErrNotExist) {
			if err := anchor.Mkdir(relative, 0o700); err != nil {
				return nil, errors.Join(
					fmt.Errorf("backup repository: create target component %q: %w", relative, err),
					cleanup(),
				)
			}
			info, err = anchor.Lstat(relative)
			if err == nil {
				createdDirectories = append(
					createdDirectories,
					restorePathIdentity{name: relative, info: info},
				)
			}
		}
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return nil, errors.Join(
				fmt.Errorf("backup repository: unsafe target component %q", relative),
				cleanup(),
			)
		}
		targetInfo = info
	}
	if beforeOpen != nil {
		if err := beforeOpen(absolute); err != nil {
			return nil, errors.Join(
				fmt.Errorf("backup repository: before opening target: %w", err),
				cleanup(),
			)
		}
	}

	current = ""
	for _, component := range relativeComponents {
		current = filepath.ToSlash(filepath.Join(current, component))
		info, err := anchor.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return nil, errors.Join(
				fmt.Errorf("backup repository: target changed before open"),
				cleanup(),
			)
		}
		if current == relative && !os.SameFile(targetInfo, info) {
			return nil, errors.Join(
				fmt.Errorf("backup repository: target identity changed before open"),
				cleanup(),
			)
		}
	}
	root, err := anchor.OpenRoot(relative)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("backup repository: open target directory: %w", err),
			cleanup(),
		)
	}
	openedInfo, err := root.Stat(".")
	if err != nil || !os.SameFile(targetInfo, openedInfo) {
		root.Close()
		return nil, errors.Join(
			fmt.Errorf("backup repository: target changed while opening"),
			cleanup(),
		)
	}
	prepared := &preparedRestoreTarget{
		root:     root,
		absolute: absolute,
		created:  created,
	}
	if created {
		prepared.cleanupAnchor = anchor
		prepared.createdDirectories = createdDirectories
	} else if err := anchor.Close(); err != nil {
		root.Close()
		return nil, fmt.Errorf("backup repository: close target anchor: %w", err)
	}
	return prepared, nil
}

func splitAbsolutePath(absolute string) (string, []string, error) {
	volume := filepath.VolumeName(absolute)
	root := volume + string(os.PathSeparator)
	remainder := strings.TrimPrefix(absolute, root)
	if remainder == "" {
		return "", nil, fmt.Errorf("backup repository: filesystem root cannot be a restore target")
	}
	components := strings.FieldsFunc(remainder, func(r rune) bool {
		return r == os.PathSeparator
	})
	if len(components) == 0 {
		return "", nil, fmt.Errorf("backup repository: restore target has no path components")
	}
	return root, components, nil
}

func canonicalizeTrustedRootAlias(absolute string) string {
	if runtime.GOOS != "darwin" {
		return absolute
	}
	aliases := map[string]string{
		"/tmp": "/private/tmp",
		"/var": "/private/var",
	}
	for alias, canonical := range aliases {
		if absolute != alias && !strings.HasPrefix(absolute, alias+"/") {
			continue
		}
		linkInfo, linkErr := os.Lstat(alias)
		parentInfo, parentErr := os.Lstat("/")
		canonicalInfo, canonicalErr := os.Lstat(canonical)
		resolved, resolveErr := filepath.EvalSymlinks(alias)
		if linkErr != nil || parentErr != nil || canonicalErr != nil || resolveErr != nil ||
			linkInfo.Mode()&os.ModeSymlink == 0 || resolved != canonical ||
			!ownedByRoot(linkInfo) || !ownedByRoot(parentInfo) || !ownedByRoot(canonicalInfo) ||
			parentInfo.Mode().Perm()&0o022 != 0 || !canonicalInfo.IsDir() {
			return absolute
		}
		return canonical + strings.TrimPrefix(absolute, alias)
	}
	return absolute
}

func ownedByRoot(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0
}

func cleanupRootEntries(
	root *os.Root,
	files []restorePathIdentity,
	directories []restorePathIdentity,
) error {
	return errors.Join(
		removeRootEntriesIdentityChecked(root, reverseRestoreEntries(files)),
		removeRootEntriesIdentityChecked(root, reverseRestoreEntries(directories)),
	)
}

func reverseRestoreEntries(entries []restorePathIdentity) []restorePathIdentity {
	reversed := make([]restorePathIdentity, len(entries))
	for index := range entries {
		reversed[len(entries)-1-index] = entries[index]
	}
	return reversed
}

func removeRootEntriesIdentityChecked(root *os.Root, entries []restorePathIdentity) error {
	var errs []error
	for _, entry := range entries {
		current, err := root.Lstat(entry.name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("backup repository: cleanup inspect %q: %w", entry.name, err))
			continue
		}
		if !os.SameFile(entry.info, current) {
			errs = append(
				errs,
				fmt.Errorf("backup repository: cleanup uncertainty: %q identity changed", entry.name),
			)
			continue
		}
		if err := root.Remove(entry.name); err != nil {
			errs = append(errs, fmt.Errorf("backup repository: cleanup remove %q: %w", entry.name, err))
		}
	}
	return errors.Join(errs...)
}
