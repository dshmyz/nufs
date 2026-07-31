package metadata

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

type RestoreOptions struct {
	BackupID     string
	TargetDir    string
	NewClusterID string
	NUFSVersion  string
}

type RestoreReport struct {
	BackupID        string                   `json:"backup_id"`
	SourceClusterID string                   `json:"source_cluster_id"`
	NewClusterID    string                   `json:"new_cluster_id"`
	StartedAt       time.Time                `json:"started_at"`
	CompletedAt     time.Time                `json:"completed_at"`
	AppliedIndex    uint64                   `json:"applied_index"`
	Verification    BackupVerificationReport `json:"verification"`
}

const restoreTempMarker = ".nufs-restore-temp"

func RestoreBackupToNewCluster(ctx context.Context, repository BackupRepository, opts RestoreOptions) (*RestoreReport, error) {
	if repository == nil {
		return nil, fmt.Errorf("restore: repository is required")
	}
	if err := validateBackupID(opts.BackupID); err != nil {
		return nil, err
	}
	if opts.TargetDir == "" {
		return nil, fmt.Errorf("restore: target dir is required")
	}
	if strings.TrimSpace(opts.NewClusterID) == "" {
		return nil, fmt.Errorf("restore: new cluster ID is required")
	}
	target, err := filepath.Abs(opts.TargetDir)
	if err != nil {
		return nil, fmt.Errorf("restore: resolve target dir: %w", err)
	}
	target = filepath.Clean(target)
	if err := ensureTargetAbsentOrEmpty(target); err != nil {
		return nil, err
	}
	if err := cleanupInterruptedRestoreDirs(target); err != nil {
		return nil, err
	}
	suffix, err := restoreRandomSuffix()
	if err != nil {
		return nil, err
	}
	tempDir := target + ".restore-" + suffix
	if err := os.Mkdir(tempDir, 0o700); err != nil {
		return nil, fmt.Errorf("restore: create temporary directory: %w", err)
	}
	if err := writeRestoreTempMarker(tempDir); err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, err
	}
	tempOwned := true
	defer func() {
		if tempOwned {
			_ = os.RemoveAll(tempDir)
			_ = os.Remove(restoreTempMarkerPath(tempDir))
		}
	}()

	started := time.Now().UTC()
	manifest, err := repository.Fetch(ctx, opts.BackupID, tempDir)
	if err != nil {
		return nil, fmt.Errorf("restore: fetch backup: %w", err)
	}
	if manifest.SourceClusterID == "" {
		return nil, fmt.Errorf("restore: source cluster ID is missing")
	}
	if manifest.SourceClusterID == opts.NewClusterID {
		return nil, fmt.Errorf("restore: new cluster ID must differ from source cluster ID")
	}
	if err := checkRestoreNUFSVersion(manifest, opts.NUFSVersion); err != nil {
		return nil, err
	}
	verification, err := VerifyBackupArtifact(ctx, tempDir, manifest)
	if err != nil {
		return nil, fmt.Errorf("restore: verify fetched artifact: %w", err)
	}
	if err := removeOldRaftState(tempDir); err != nil {
		return nil, err
	}
	if err := rewriteRestoredPebble(ctx, tempDir, manifest, opts.NewClusterID, started); err != nil {
		return nil, err
	}
	if err := syncDirectoryTree(tempDir, syncDirectory); err != nil {
		return nil, fmt.Errorf("restore: sync temporary tree: %w", err)
	}
	if err := ensureTargetAbsentOrEmpty(target); err != nil {
		return nil, err
	}
	if err := removeEmptyTargetIfPresent(target); err != nil {
		return nil, err
	}
	if err := os.Rename(tempDir, target); err != nil {
		return nil, fmt.Errorf("restore: publish target: %w", err)
	}
	tempOwned = false
	if err := os.Remove(restoreTempMarkerPath(tempDir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("restore: remove temp ownership marker: %w", err)
	}
	if err := syncDirectory(filepath.Dir(target)); err != nil {
		return nil, fmt.Errorf("restore: sync target parent: %w", err)
	}
	report := &RestoreReport{
		BackupID:        manifest.BackupID,
		SourceClusterID: manifest.SourceClusterID,
		NewClusterID:    opts.NewClusterID,
		StartedAt:       started,
		CompletedAt:     time.Now().UTC(),
		AppliedIndex:    manifest.AppliedIndex,
		Verification:    *verification,
	}
	if err := writeRestoreReport(target, report); err != nil {
		return report, err
	}
	return report, nil
}

func ensureTargetAbsentOrEmpty(target string) error {
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("restore: inspect target: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("restore: target must be an absent or empty directory")
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		return fmt.Errorf("restore: read target: %w", err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("restore: target directory is not empty")
	}
	return nil
}

func cleanupInterruptedRestoreDirs(target string) error {
	entries, err := os.ReadDir(filepath.Dir(target))
	if err != nil {
		return fmt.Errorf("restore: list target parent: %w", err)
	}
	prefix := filepath.Base(target) + ".restore-"
	for _, entry := range entries {
		tempPath := filepath.Join(filepath.Dir(target), entry.Name())
		if !isOwnedRestoreTempEntry(entry, prefix, tempPath) {
			continue
		}
		if err := os.RemoveAll(tempPath); err != nil {
			return fmt.Errorf("restore: clean interrupted temp %q: %w", entry.Name(), err)
		}
		if err := os.Remove(restoreTempMarkerPath(tempPath)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("restore: clean interrupted temp marker %q: %w", entry.Name(), err)
		}
	}
	return nil
}

func isOwnedRestoreTempEntry(entry os.DirEntry, prefix, path string) bool {
	if !strings.HasPrefix(entry.Name(), prefix) {
		return false
	}
	suffix := strings.TrimPrefix(entry.Name(), prefix)
	if !isRestoreTempSuffix(suffix) || entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() {
		return false
	}
	return isRegularFile(restoreTempMarkerPath(path))
}

func isRestoreTempSuffix(suffix string) bool {
	if len(suffix) != 32 {
		return false
	}
	for _, r := range suffix {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func writeRestoreTempMarker(tempDir string) error {
	if err := os.WriteFile(restoreTempMarkerPath(tempDir), []byte("nufs restore temporary directory\n"), 0o600); err != nil {
		return fmt.Errorf("restore: write temp ownership marker: %w", err)
	}
	return nil
}

func restoreTempMarkerPath(tempDir string) string {
	return tempDir + "." + restoreTempMarker
}

func restoreRandomSuffix() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", fmt.Errorf("restore: generate temp suffix: %w", err)
	}
	return hex.EncodeToString(data[:]), nil
}

func checkRestoreNUFSVersion(manifest *BackupManifest, current string) error {
	if current == "" || manifest.MinimumNUFSVersion == "" {
		return nil
	}
	if current < manifest.MinimumNUFSVersion {
		return fmt.Errorf("restore: backup requires NUFS version %s or newer", manifest.MinimumNUFSVersion)
	}
	return nil
}

func removeOldRaftState(tempDir string) error {
	raftDir := filepath.Join(tempDir, "raft")
	info, err := os.Lstat(raftDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("restore: inspect old raft state: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("restore: old raft state path is a symlink")
	}
	if err := os.RemoveAll(raftDir); err != nil {
		return fmt.Errorf("restore: remove old raft state: %w", err)
	}
	return nil
}

func rewriteRestoredPebble(ctx context.Context, dir string, manifest *BackupManifest, newClusterID string, restoredAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return fmt.Errorf("restore: open fetched pebble: %w", err)
	}
	defer db.Close()
	batch := db.NewBatch()
	defer batch.Close()
	clusterID, err := marshalValue(newClusterID, codecMsgpack)
	if err != nil {
		return fmt.Errorf("restore: encode cluster ID: %w", err)
	}
	if err := batch.Set([]byte(keyClusterID), clusterID, nil); err != nil {
		return fmt.Errorf("restore: set cluster ID: %w", err)
	}
	if err := deletePebblePrefix(batch, db, []byte(prefixBackupTask)); err != nil {
		return err
	}
	marker := RestorePendingMarker{
		BackupID:        manifest.BackupID,
		SourceClusterID: manifest.SourceClusterID,
		AppliedIndex:    manifest.AppliedIndex,
		RestoredAt:      restoredAt,
	}
	normalizedMarker, err := normalizeRestorePendingMarker(&marker)
	if err != nil {
		return fmt.Errorf("restore: normalize pending marker: %w", err)
	}
	markerData, err := marshalValue(&normalizedMarker, codecMsgpack)
	if err != nil {
		return fmt.Errorf("restore: encode pending marker: %w", err)
	}
	if err := batch.Set([]byte(keyRestorePending), markerData, nil); err != nil {
		return fmt.Errorf("restore: set pending marker: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("restore: commit restored metadata rewrites: %w", err)
	}
	if err := db.Flush(); err != nil {
		return fmt.Errorf("restore: flush restored metadata: %w", err)
	}
	return nil
}

func deletePebblePrefix(batch *pebble.Batch, db *pebble.DB, prefix []byte) error {
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(string(prefix))})
	if err != nil {
		return fmt.Errorf("restore: scan prefix %q: %w", prefix, err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		key := append([]byte(nil), iter.Key()...)
		if err := batch.Delete(key, nil); err != nil {
			return fmt.Errorf("restore: delete runtime-local key %q: %w", key, err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("restore: iterate prefix %q: %w", prefix, err)
	}
	return nil
}

func removeEmptyTargetIfPresent(target string) error {
	if _, err := os.Lstat(target); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("restore: inspect empty target before publish: %w", err)
	}
	if err := os.Remove(target); err != nil {
		return fmt.Errorf("restore: remove empty target before publish: %w", err)
	}
	return nil
}

func writeRestoreReport(target string, report *RestoreReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("restore: encode report: %w", err)
	}
	data = append(data, '\n')
	reportPath := target + ".restore-report.json"
	tmpPath := reportPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return fmt.Errorf("restore: write report: %w", err)
	}
	if err := os.Rename(tmpPath, reportPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("restore: publish report: %w", err)
	}
	if err := syncDirectory(filepath.Dir(reportPath)); err != nil {
		return fmt.Errorf("restore: sync report parent: %w", err)
	}
	return nil
}
