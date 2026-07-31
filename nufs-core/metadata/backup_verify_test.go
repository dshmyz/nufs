package metadata

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func createManifestFixture(t *testing.T) (string, *BackupManifest) {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	root, err := marshalValue(&InodeMeta{ID: RootInodeID, Type: FileDirectory}, codecMsgpack)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte(prefixInode+"1"), root, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, err := BuildBackupManifest(context.Background(), dir, BackupSnapshotMetadata{
		BackupID:  "fixture",
		CreatedAt: time.Unix(0, 0).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return dir, manifest
}

func TestVerifyBackupArtifactRejectsCorruptionAndUndeclaredFiles(t *testing.T) {
	dir, manifest := createManifestFixture(t)
	if err := os.WriteFile(filepath.Join(dir, manifest.Files[0].Path), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyBackupArtifact(context.Background(), dir, manifest); err == nil {
		t.Fatal("corrupt artifact verified")
	}
}

func TestVerifyBackupArtifactRejectsUndeclaredFiles(t *testing.T) {
	dir, manifest := createManifestFixture(t)
	if err := os.WriteFile(filepath.Join(dir, "undeclared"), []byte("unexpected"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyBackupArtifact(context.Background(), dir, manifest); err == nil {
		t.Fatal("artifact with undeclared file verified")
	}
}

func TestVerifyBackupArtifactReportsVerifiedCheckpoint(t *testing.T) {
	dir, manifest := createManifestFixture(t)
	report, err := VerifyBackupArtifact(context.Background(), dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !report.ManifestValid || report.FilesVerified != len(manifest.Files) || report.RecordCounts.Inodes != 1 {
		t.Fatalf("unexpected verification report: %+v", report)
	}
	if len(report.Checks) != 10 {
		t.Fatalf("checks = %d, want 10", len(report.Checks))
	}
}

func TestVerifyBackupArtifactAcceptsRawRaftNodeOpsURL(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	writeFixtureValue(t, db, prefixInode+"1", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
	if err := db.Set([]byte(metaNodeOpsKey("meta-1")), []byte("https://meta-1.example"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, err := BuildBackupManifest(context.Background(), dir, BackupSnapshotMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	report, err := VerifyBackupArtifact(context.Background(), dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if report.RecordCounts.RaftNodeOps != 1 {
		t.Fatalf("RaftNodeOps = %d, want 1", report.RecordCounts.RaftNodeOps)
	}
}

func TestVerifyBackupArtifactRejectsStructuralCorruption(t *testing.T) {
	tests := []struct {
		name  string
		write func(t *testing.T, db *pebble.DB)
	}{
		{"missing root inode", func(t *testing.T, _ *pebble.DB) {}},
		{"missing bucket root", func(t *testing.T, db *pebble.DB) {
			writeFixtureValue(t, db, prefixInode+"1", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
			writeFixtureValue(t, db, prefixBucket+"bucket", &BucketInfo{Name: "bucket", RootInode: 2})
		}},
		{"missing directory inode", func(t *testing.T, db *pebble.DB) {
			writeFixtureValue(t, db, prefixInode+"1", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
			writeFixtureValue(t, db, prefixNS+"1/file", &DirEntry{InodeID: 2, Name: "file"})
		}},
		{"missing namespace parent", func(t *testing.T, db *pebble.DB) {
			writeFixtureValue(t, db, prefixInode+"1", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
			writeFixtureValue(t, db, prefixNS+"2/file", &DirEntry{InodeID: 1, Name: "file"})
		}},
		{"inode key value mismatch", func(t *testing.T, db *pebble.DB) {
			writeFixtureValue(t, db, prefixInode+"1", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
			writeFixtureValue(t, db, prefixInode+"2", &InodeMeta{ID: 3})
		}},
		{"non canonical inode key", func(t *testing.T, db *pebble.DB) {
			writeFixtureValue(t, db, prefixInode+"01", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
		}},
		{"chunk key value mismatch", func(t *testing.T, db *pebble.DB) {
			writeFixtureValue(t, db, prefixInode+"1", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
			writeFixtureValue(t, db, prefixChunk+"2", &ChunkMeta{ID: 3})
		}},
		{"non canonical chunk key", func(t *testing.T, db *pebble.DB) {
			writeFixtureValue(t, db, prefixInode+"1", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
			writeFixtureValue(t, db, prefixChunk+"03", &ChunkMeta{ID: 3})
		}},
		{"non canonical namespace parent", func(t *testing.T, db *pebble.DB) {
			writeFixtureValue(t, db, prefixInode+"1", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
			writeFixtureValue(t, db, prefixNS+"01/file", &DirEntry{InodeID: 1, Name: "file"})
		}},
		{"missing chunk metadata", func(t *testing.T, db *pebble.DB) {
			writeFixtureValue(t, db, prefixInode+"1", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
			writeFixtureValue(t, db, prefixInode+"2", &InodeMeta{ID: 2, ChunkMap: []ChunkRef{{ID: 3}}})
		}},
		{"undecodable value", func(t *testing.T, db *pebble.DB) {
			writeFixtureValue(t, db, prefixInode+"1", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
			if err := db.Set([]byte(prefixNode+"1"), []byte("corrupt"), pebble.Sync); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := pebble.Open(dir, &pebble.Options{})
			if err != nil {
				t.Fatal(err)
			}
			test.write(t, db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			manifest := manifestForCheckpoint(t, dir)
			_, verifyErr := VerifyBackupArtifact(context.Background(), dir, manifest)
			if verifyErr == nil {
				t.Fatal("structurally corrupt artifact verified")
			}
			if strings.Contains(test.name, "non canonical") && !strings.Contains(verifyErr.Error(), "non-canonical") {
				t.Fatalf("VerifyBackupArtifact error = %v, want non-canonical key error", verifyErr)
			}
		})
	}
}

func TestVerifyBackupArtifactReportsFailureStages(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(t *testing.T) (string, *BackupManifest)
		failed string
	}{
		{"manifest", func(t *testing.T) (string, *BackupManifest) { return t.TempDir(), nil }, "manifest"},
		{"file", func(t *testing.T) (string, *BackupManifest) {
			dir, m := createManifestFixture(t)
			_ = os.WriteFile(filepath.Join(dir, m.Files[0].Path), []byte("bad"), 0600)
			return dir, m
		}, "file_set"},
		{"structure", func(t *testing.T) (string, *BackupManifest) {
			dir := t.TempDir()
			db, _ := pebble.Open(dir, &pebble.Options{})
			_ = db.Close()
			return dir, manifestForCheckpoint(t, dir)
		}, "root_inode"},
		{"count", func(t *testing.T) (string, *BackupManifest) {
			dir, m := createManifestFixture(t)
			m.RecordCounts.Inodes++
			return dir, m
		}, "record_counts"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir, manifest := test.setup(t)
			report, err := VerifyBackupArtifact(context.Background(), dir, manifest)
			if err == nil || len(report.Checks) != 10 || checkPassed(report, test.failed) {
				t.Fatalf("report=%+v err=%v", report, err)
			}
		})
	}
}

func TestVerifyBackupArtifactSupportsLegacyJSONAndCountsEveryPrefix(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	writeFixtureJSON(t, db, prefixInode+"1", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
	writeFixtureValue(t, db, prefixBucket+"b", &BucketInfo{RootInode: 1})
	writeFixtureValue(t, db, prefixBucketByRoot+"1", "b")
	writeFixtureValue(t, db, prefixBucketStats+"1", &BucketUsage{})
	writeFixtureValue(t, db, prefixNS+"1/a", &DirEntry{InodeID: 1})
	writeFixtureValue(t, db, prefixChunk+"1", &ChunkMeta{ID: 1})
	writeFixtureValue(t, db, prefixNode+"1", &NodeInfo{})
	writeFixtureValue(t, db, prefixPolicy+"b", &PlacementPolicy{})
	writeFixtureValue(t, db, prefixRepair+"1", &RepairTask{})
	writeFixtureValue(t, db, prefixAudit+"1", &AuditRecord{})
	writeFixtureValue(t, db, prefixACL+"b", &BucketPolicy{})
	writeFixtureValue(t, db, prefixQuota+"b", &BucketQuota{})
	writeFixtureValue(t, db, prefixQuotaUsage+"b", &BucketUsage{})
	writeFixtureValue(t, db, prefixFreeList+"2", InodeID(2))
	writeFixtureValue(t, db, prefixWriteAttempt+"a", &ObjectWriteAttempt{})
	writeFixtureValue(t, db, prefixWriteAttemptState+"pending/00000000000000000000/a", "a")
	writeFixtureValue(t, db, prefixBackgroundTask+"a", &BackgroundTask{})
	writeFixtureValue(t, db, prefixBackgroundTaskQ+"gc/queued/00000000000000000000/a", "a")
	if err := db.Set([]byte(metaNodeOpsKey("a")), []byte("https://a"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, err := BuildBackupManifest(context.Background(), dir, BackupSnapshotMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	report, err := VerifyBackupArtifact(context.Background(), dir, manifest)
	if err != nil {
		t.Fatal(err)
	}
	for name, count := range map[string]int64{"Buckets": report.RecordCounts.Buckets, "BucketByRoot": report.RecordCounts.BucketByRoot, "BucketStats": report.RecordCounts.BucketStats, "DirectoryEntries": report.RecordCounts.DirectoryEntries, "Inodes": report.RecordCounts.Inodes, "Chunks": report.RecordCounts.Chunks, "Nodes": report.RecordCounts.Nodes, "Policies": report.RecordCounts.Policies, "RepairTasks": report.RecordCounts.RepairTasks, "AuditRecords": report.RecordCounts.AuditRecords, "BucketPolicies": report.RecordCounts.BucketPolicies, "Quotas": report.RecordCounts.Quotas, "QuotaUsage": report.RecordCounts.QuotaUsage, "FreeInodes": report.RecordCounts.FreeInodes, "WriteAttempts": report.RecordCounts.WriteAttempts, "WriteAttemptStates": report.RecordCounts.WriteAttemptStates, "BackgroundTasks": report.RecordCounts.BackgroundTasks, "BackgroundTaskQueue": report.RecordCounts.BackgroundTaskQueue, "RaftNodeOps": report.RecordCounts.RaftNodeOps} {
		if count != 1 {
			t.Fatalf("%s = %d, want 1", name, count)
		}
	}
}

func checkPassed(report *BackupVerificationReport, name string) bool {
	for _, check := range report.Checks {
		if check.Name == name {
			return check.Passed
		}
	}
	return true
}

func manifestForCheckpoint(t *testing.T, dir string) *BackupManifest {
	t.Helper()
	files, total, err := collectBackupFiles(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	return &BackupManifest{FormatVersion: BackupFormatVersion, Files: files, TotalBytes: total}
}

func writeFixtureValue(t *testing.T, db *pebble.DB, key string, value interface{}) {
	t.Helper()
	data, err := marshalValue(value, codecMsgpack)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte(key), data, pebble.Sync); err != nil {
		t.Fatal(err)
	}
}

func writeFixtureJSON(t *testing.T, db *pebble.DB, key string, value interface{}) {
	t.Helper()
	data, err := marshalValue(value, codecJSON)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte(key), data, pebble.Sync); err != nil {
		t.Fatal(err)
	}
}
