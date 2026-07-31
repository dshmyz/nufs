package metadata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const BackupFormatVersion = 1

type BackupFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type BackupSnapshotMetadata struct {
	BackupID           string
	SourceClusterID    string
	CreatedAt          time.Time
	RaftTerm           uint64
	AppliedIndex       uint64
	MinimumNUFSVersion string
}

// BackupRecordCounts records every durable metadata keyspace included in a backup.
type BackupRecordCounts struct {
	Buckets             int64 `json:"buckets"`
	BucketByRoot        int64 `json:"bucket_by_root"`
	BucketStats         int64 `json:"bucket_stats"`
	DirectoryEntries    int64 `json:"directory_entries"`
	Inodes              int64 `json:"inodes"`
	Chunks              int64 `json:"chunks"`
	Nodes               int64 `json:"nodes"`
	Policies            int64 `json:"policies"`
	RepairTasks         int64 `json:"repair_tasks"`
	AuditRecords        int64 `json:"audit_records"`
	BucketPolicies      int64 `json:"bucket_policies"`
	Quotas              int64 `json:"quotas"`
	QuotaUsage          int64 `json:"quota_usage"`
	FreeInodes          int64 `json:"free_inodes"`
	WriteAttempts       int64 `json:"write_attempts"`
	WriteAttemptStates  int64 `json:"write_attempt_states"`
	BackgroundTasks     int64 `json:"background_tasks"`
	BackgroundTaskQueue int64 `json:"background_task_queue"`
	RaftNodeOps         int64 `json:"raft_node_ops"`
}

type BackupManifest struct {
	FormatVersion      int                `json:"format_version"`
	BackupID           string             `json:"backup_id"`
	SourceClusterID    string             `json:"source_cluster_id"`
	CreatedAt          time.Time          `json:"created_at"`
	RaftTerm           uint64             `json:"raft_term"`
	AppliedIndex       uint64             `json:"applied_index"`
	CheckpointFormat   string             `json:"checkpoint_format"`
	MinimumNUFSVersion string             `json:"minimum_nufs_version"`
	Files              []BackupFile       `json:"files"`
	RecordCounts       BackupRecordCounts `json:"record_counts"`
	TotalBytes         int64              `json:"total_bytes"`
	DurationMillis     int64              `json:"duration_ms"`
}

// Validate verifies that a manifest can be used to safely address an artifact directory.
func (m *BackupManifest) Validate() error {
	if m == nil {
		return fmt.Errorf("backup manifest: nil")
	}
	if m.FormatVersion != BackupFormatVersion {
		return fmt.Errorf("backup manifest: unsupported format version %d", m.FormatVersion)
	}
	var total int64
	paths := make(map[string]struct{}, len(m.Files))
	for _, file := range m.Files {
		if err := validateBackupPath(file.Path); err != nil {
			return err
		}
		if _, ok := paths[file.Path]; ok {
			return fmt.Errorf("backup manifest: duplicate file path %q", file.Path)
		}
		paths[file.Path] = struct{}{}
		if file.Size < 0 {
			return fmt.Errorf("backup manifest: negative size for %q", file.Path)
		}
		if len(file.SHA256) != sha256.Size*2 {
			return fmt.Errorf("backup manifest: invalid sha256 length for %q", file.Path)
		}
		if _, err := hex.DecodeString(file.SHA256); err != nil || file.SHA256 != strings.ToLower(file.SHA256) {
			return fmt.Errorf("backup manifest: invalid sha256 for %q", file.Path)
		}
		if total > int64(^uint64(0)>>1)-file.Size {
			return fmt.Errorf("backup manifest: total bytes overflow")
		}
		total += file.Size
	}
	if total != m.TotalBytes {
		return fmt.Errorf("backup manifest: total bytes %d does not match files %d", m.TotalBytes, total)
	}
	return nil
}

func validateBackupPath(filePath string) error {
	if filePath == "" || strings.Contains(filePath, "\\") || isWindowsVolumePath(filePath) || path.IsAbs(filePath) || filepath.IsAbs(filePath) {
		return fmt.Errorf("backup manifest: unsafe file path %q", filePath)
	}
	if path.Clean(filePath) != filePath || filePath == "." || strings.HasPrefix(filePath, "../") {
		return fmt.Errorf("backup manifest: non-normalized file path %q", filePath)
	}
	for _, part := range strings.Split(filePath, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("backup manifest: unsafe file path %q", filePath)
		}
	}
	return nil
}

func isWindowsVolumePath(filePath string) bool {
	return len(filePath) >= 2 && filePath[1] == ':' &&
		((filePath[0] >= 'A' && filePath[0] <= 'Z') || (filePath[0] >= 'a' && filePath[0] <= 'z'))
}

func BuildBackupManifest(ctx context.Context, checkpointDir string, meta BackupSnapshotMetadata) (*BackupManifest, error) {
	files, totalBytes, err := collectBackupFiles(ctx, checkpointDir)
	if err != nil {
		return nil, err
	}
	counts, err := inspectBackupCheckpoint(ctx, checkpointDir)
	if err != nil {
		return nil, err
	}
	manifest := &BackupManifest{
		FormatVersion:      BackupFormatVersion,
		BackupID:           meta.BackupID,
		SourceClusterID:    meta.SourceClusterID,
		CreatedAt:          meta.CreatedAt,
		RaftTerm:           meta.RaftTerm,
		AppliedIndex:       meta.AppliedIndex,
		CheckpointFormat:   "pebble",
		MinimumNUFSVersion: meta.MinimumNUFSVersion,
		Files:              files,
		RecordCounts:       counts,
		TotalBytes:         totalBytes,
		DurationMillis:     0,
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	return manifest, nil
}

func collectBackupFiles(ctx context.Context, checkpointDir string) ([]BackupFile, int64, error) {
	info, err := os.Stat(checkpointDir)
	if err != nil {
		return nil, 0, fmt.Errorf("backup artifact: stat checkpoint: %w", err)
	}
	if !info.IsDir() {
		return nil, 0, fmt.Errorf("backup artifact: checkpoint is not a directory")
	}
	var files []BackupFile
	var total int64
	err = filepath.WalkDir(checkpointDir, func(filePath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if filePath == checkpointDir {
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return fmt.Errorf("backup artifact: unsupported entry %q", filePath)
		}
		rel, err := filepath.Rel(checkpointDir, filePath)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if err := validateBackupPath(rel); err != nil {
			return err
		}
		file, err := os.Open(filePath)
		if err != nil {
			return err
		}
		digest := sha256.New()
		_, copyErr := io.Copy(digest, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		fileInfo, err := entry.Info()
		if err != nil {
			return err
		}
		if total > int64(^uint64(0)>>1)-fileInfo.Size() {
			return fmt.Errorf("backup artifact: total bytes overflow")
		}
		total += fileInfo.Size()
		files = append(files, BackupFile{Path: rel, Size: fileInfo.Size(), SHA256: hex.EncodeToString(digest.Sum(nil))})
		return nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("backup artifact: list files: %w", err)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, total, nil
}
