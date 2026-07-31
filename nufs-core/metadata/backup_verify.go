package metadata

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble"
)

type BackupVerificationCheck struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
}

type BackupVerificationReport struct {
	ManifestValid bool                      `json:"manifest_valid"`
	FilesVerified int                       `json:"files_verified"`
	BytesVerified int64                     `json:"bytes_verified"`
	RecordCounts  BackupRecordCounts        `json:"record_counts"`
	Checks        []BackupVerificationCheck `json:"checks"`
}

func VerifyBackupArtifact(ctx context.Context, checkpointDir string, manifest *BackupManifest) (*BackupVerificationReport, error) {
	report := newBackupVerificationReport()
	if err := manifest.Validate(); err != nil {
		return report, err
	}
	report.ManifestValid = true
	report.setCheck("manifest", true)
	files, totalBytes, err := collectBackupFiles(ctx, checkpointDir)
	if err != nil {
		return report, err
	}
	if err := verifyBackupFiles(manifest.Files, files, manifest.TotalBytes, totalBytes); err != nil {
		return report, err
	}
	report.FilesVerified = len(files)
	report.BytesVerified = totalBytes
	report.setCheck("file_set", true)
	report.setCheck("checksums", true)

	counts, err := inspectBackupCheckpoint(ctx, checkpointDir)
	if err != nil {
		return report, err
	}
	report.RecordCounts = counts
	report.setCheck("pebble_read_only", true)
	report.setCheck("durable_values", true)
	report.setCheck("root_inode", true)
	report.setCheck("bucket_roots", true)
	report.setCheck("directory_entries", true)
	report.setCheck("inode_chunk_references", true)
	if err := compareBackupRecordCounts(manifest.RecordCounts, counts); err != nil {
		return report, err
	}
	report.setCheck("record_counts", true)
	return report, nil
}

func newBackupVerificationReport() *BackupVerificationReport {
	names := []string{"manifest", "file_set", "checksums", "pebble_read_only", "durable_values", "root_inode", "bucket_roots", "directory_entries", "inode_chunk_references", "record_counts"}
	report := &BackupVerificationReport{Checks: make([]BackupVerificationCheck, len(names))}
	for i, name := range names {
		report.Checks[i].Name = name
	}
	return report
}

func (r *BackupVerificationReport) setCheck(name string, passed bool) {
	for i := range r.Checks {
		if r.Checks[i].Name == name {
			r.Checks[i].Passed = passed
			return
		}
	}
}

func verifyBackupFiles(expected, actual []BackupFile, expectedTotal, actualTotal int64) error {
	if expectedTotal != actualTotal {
		return fmt.Errorf("backup artifact: total bytes mismatch: manifest=%d actual=%d", expectedTotal, actualTotal)
	}
	actualByPath := make(map[string]BackupFile, len(actual))
	for _, file := range actual {
		actualByPath[file.Path] = file
	}
	if len(expected) != len(actualByPath) {
		return fmt.Errorf("backup artifact: file set mismatch: manifest=%d actual=%d", len(expected), len(actualByPath))
	}
	for _, expectedFile := range expected {
		actualFile, ok := actualByPath[expectedFile.Path]
		if !ok {
			return fmt.Errorf("backup artifact: declared file %q missing", expectedFile.Path)
		}
		if actualFile.Size != expectedFile.Size || actualFile.SHA256 != expectedFile.SHA256 {
			return fmt.Errorf("backup artifact: checksum mismatch for %q", expectedFile.Path)
		}
	}
	return nil
}

func inspectBackupCheckpoint(ctx context.Context, checkpointDir string) (BackupRecordCounts, error) {
	db, err := pebble.Open(checkpointDir, &pebble.Options{ReadOnly: true})
	if err != nil {
		return BackupRecordCounts{}, fmt.Errorf("backup artifact: open read-only pebble checkpoint: %w", err)
	}
	defer db.Close()

	state := backupCheckpointState{inodes: make(map[InodeID]struct{}), chunks: make(map[ChunkID]struct{})}
	for _, spec := range backupPrefixSpecs(&state) {
		if err := scanBackupPrefix(ctx, db, spec.prefix, spec.decode); err != nil {
			return BackupRecordCounts{}, err
		}
	}
	if _, ok := state.inodes[RootInodeID]; !ok {
		return BackupRecordCounts{}, fmt.Errorf("backup artifact: root inode %d missing", RootInodeID)
	}
	for _, root := range state.bucketRoots {
		if _, ok := state.inodes[root]; !ok {
			return BackupRecordCounts{}, fmt.Errorf("backup artifact: bucket root inode %d missing", root)
		}
	}
	for _, entry := range state.directoryEntries {
		if _, ok := state.inodes[entry.InodeID]; !ok {
			return BackupRecordCounts{}, fmt.Errorf("backup artifact: directory entry references missing inode %d", entry.InodeID)
		}
	}
	for _, parent := range state.namespaceParents {
		if _, ok := state.inodes[parent]; !ok {
			return BackupRecordCounts{}, fmt.Errorf("backup artifact: namespace parent inode %d missing", parent)
		}
	}
	for _, ref := range state.chunkRefs {
		if _, ok := state.chunks[ref.ID]; !ok {
			return BackupRecordCounts{}, fmt.Errorf("backup artifact: inode chunk reference lacks chunk metadata %d", ref.ID)
		}
	}
	return state.counts, nil
}

type backupCheckpointState struct {
	counts           BackupRecordCounts
	inodes           map[InodeID]struct{}
	chunks           map[ChunkID]struct{}
	bucketRoots      []InodeID
	directoryEntries []DirEntry
	namespaceParents []InodeID
	chunkRefs        []ChunkRef
}

type backupPrefixSpec struct {
	prefix string
	decode func(key, value []byte) error
}

func backupPrefixSpecs(state *backupCheckpointState) []backupPrefixSpec {
	decode := func(value []byte, into any, count *int64) error {
		if err := unmarshalValue(value, into); err != nil {
			return err
		}
		*count = *count + 1
		return nil
	}
	return []backupPrefixSpec{
		{prefixBucket, func(_ []byte, value []byte) error {
			var v BucketInfo
			if err := decode(value, &v, &state.counts.Buckets); err != nil {
				return err
			}
			state.bucketRoots = append(state.bucketRoots, v.RootInode)
			return nil
		}},
		{prefixBucketByRoot, func(key, value []byte) error {
			var v string
			if err := decode(value, &v, &state.counts.BucketByRoot); err != nil {
				return err
			}
			id, err := keyID(key, prefixBucketByRoot)
			if err != nil {
				return fmt.Errorf("invalid bucket-by-root key: %w", err)
			}
			state.bucketRoots = append(state.bucketRoots, InodeID(id))
			return nil
		}},
		{prefixBucketStats, func(_ []byte, value []byte) error {
			var v BucketUsage
			return decode(value, &v, &state.counts.BucketStats)
		}},
		{prefixNS, func(key []byte, value []byte) error {
			parent, err := namespaceParentID(key)
			if err != nil {
				return err
			}
			var v DirEntry
			if err := decode(value, &v, &state.counts.DirectoryEntries); err != nil {
				return err
			}
			state.directoryEntries = append(state.directoryEntries, v)
			state.namespaceParents = append(state.namespaceParents, parent)
			return nil
		}},
		{prefixInode, func(key []byte, value []byte) error {
			id, err := keyID(key, prefixInode)
			if err != nil {
				return err
			}
			var v InodeMeta
			if err := decode(value, &v, &state.counts.Inodes); err != nil {
				return err
			}
			if v.ID != InodeID(id) {
				return fmt.Errorf("inode key ID %d does not match value ID %d", id, v.ID)
			}
			state.inodes[InodeID(id)] = struct{}{}
			state.chunkRefs = append(state.chunkRefs, v.ChunkMap...)
			return nil
		}},
		{prefixChunk, func(key []byte, value []byte) error {
			id, err := keyID(key, prefixChunk)
			if err != nil {
				return err
			}
			var v ChunkMeta
			if err := decode(value, &v, &state.counts.Chunks); err != nil {
				return err
			}
			if v.ID != ChunkID(id) {
				return fmt.Errorf("chunk key ID %d does not match value ID %d", id, v.ID)
			}
			state.chunks[ChunkID(id)] = struct{}{}
			return nil
		}},
		{prefixNode, func(_ []byte, value []byte) error { var v NodeInfo; return decode(value, &v, &state.counts.Nodes) }},
		{prefixPolicy, func(_ []byte, value []byte) error {
			var v PlacementPolicy
			return decode(value, &v, &state.counts.Policies)
		}},
		{prefixRepair, func(_ []byte, value []byte) error {
			var v RepairTask
			return decode(value, &v, &state.counts.RepairTasks)
		}},
		{prefixAudit, func(_ []byte, value []byte) error {
			var v AuditRecord
			return decode(value, &v, &state.counts.AuditRecords)
		}},
		{prefixACL, func(_ []byte, value []byte) error {
			var v BucketPolicy
			return decode(value, &v, &state.counts.BucketPolicies)
		}},
		{prefixQuota, func(_ []byte, value []byte) error { var v BucketQuota; return decode(value, &v, &state.counts.Quotas) }},
		{prefixQuotaUsage, func(_ []byte, value []byte) error {
			var v BucketUsage
			return decode(value, &v, &state.counts.QuotaUsage)
		}},
		{prefixFreeList, func(_ []byte, value []byte) error { var v InodeID; return decode(value, &v, &state.counts.FreeInodes) }},
		{prefixWriteAttempt, func(_ []byte, value []byte) error {
			var v ObjectWriteAttempt
			return decode(value, &v, &state.counts.WriteAttempts)
		}},
		{prefixWriteAttemptState, func(_ []byte, value []byte) error {
			var v string
			return decode(value, &v, &state.counts.WriteAttemptStates)
		}},
		{prefixBackgroundTask, func(_ []byte, value []byte) error {
			var v BackgroundTask
			return decode(value, &v, &state.counts.BackgroundTasks)
		}},
		{prefixBackgroundTaskQ, func(_ []byte, value []byte) error {
			var v string
			return decode(value, &v, &state.counts.BackgroundTaskQueue)
		}},
		{metaNodeOpsPrefix, func(_ []byte, value []byte) error {
			if len(value) == 0 {
				return fmt.Errorf("empty raft node ops URL")
			}
			state.counts.RaftNodeOps++
			return nil
		}},
	}
}

func keyID(key []byte, prefix string) (uint64, error) {
	segment := string(key[len(prefix):])
	id, err := strconv.ParseUint(segment, 10, 64)
	if err != nil {
		return 0, err
	}
	if segment != strconv.FormatUint(id, 10) {
		return 0, fmt.Errorf("non-canonical numeric key %q", key)
	}
	return id, nil
}

func namespaceParentID(key []byte) (InodeID, error) {
	parts := strings.SplitN(string(key[len(prefixNS):]), "/", 2)
	if len(parts) != 2 || parts[1] == "" {
		return 0, fmt.Errorf("invalid namespace key %q", key)
	}
	id, err := keyID([]byte(prefixNS+parts[0]), prefixNS)
	return InodeID(id), err
}

func scanBackupPrefix(ctx context.Context, db *pebble.DB, prefix string, decode func(key, value []byte) error) error {
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte(prefix), UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return fmt.Errorf("backup artifact: iterate %q: %w", prefix, err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := decode(iter.Key(), iter.Value()); err != nil {
			return fmt.Errorf("backup artifact: decode %q key %q: %w", prefix, iter.Key(), err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("backup artifact: iterate %q: %w", prefix, err)
	}
	return nil
}

func compareBackupRecordCounts(expected, actual BackupRecordCounts) error {
	if expected != actual {
		return fmt.Errorf("backup artifact: record counts mismatch: manifest=%+v actual=%+v", expected, actual)
	}
	return nil
}
