# NUFS Metadata Disaster Recovery v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce hourly, verifiable metadata backups in S3-compatible storage and restore a selected backup into an empty, newly bootstrapped metad cluster.

**Architecture:** A Leader-only `BackupCoordinator` obtains an immutable Pebble checkpoint at a known Raft applied index, verifies it, and publishes it through a versioned `BackupRepository` protocol whose commit marker is written last. A separate offline restore engine downloads and verifies committed artifacts before atomically publishing an empty target directory, while durable chunk tombstones prevent retained backups from referencing payloads already removed by datanode GC.

**Tech Stack:** Go 1.25; Pebble; Hashicorp Raft; AWS SDK v2 S3 client; existing metad Ops API, Prometheus text exporter, scrubber, and disaster drill framework.

## Global Constraints

- RPO is at most one hour; stale-backup alert threshold is 75 minutes.
- RTO is at most 30 minutes on the production-size recovery fixture.
- Retain the latest 24 successful backups.
- S3-compatible remote storage is authoritative; local checkpoints are temporary.
- Restore is allowed only into an absent or empty target directory.
- Restore creates a new cluster identity and never reuses old Raft peer state.
- Backup artifacts contain metadata only, not chunk payloads.
- Physical chunk deletion waits at least 25 hours and must also pass the retained-backup catalog guard.
- Repository/catalog uncertainty fails closed for pruning and chunk purge.
- No online or in-place restore endpoint is added.
- No Admin UI or gateway feature work is included.
- Do not commit during implementation unless explicitly requested by the user.

---

## File Structure

- Create `nufs-core/metadata/backup_manifest.go`: manifest types, validation, hashing, and artifact reports.
- Create `nufs-core/metadata/backup_manifest_test.go`: malformed path, checksum, extra-file, and compatibility tests.
- Create `nufs-core/metadata/backup_verify.go`: read-only Pebble structural and reference verifier.
- Create `nufs-core/metadata/backup_verify_test.go`: root, inode, chunk, quota, and count validation tests.
- Modify `nufs-core/metadata/pebble_raft.go`: add the FSM checkpoint gate and exact-index portable checkpoint source.
- Modify `nufs-core/metadata/snapshot_checkpoint.go`: persist immutable checkpoint directories rather than a live store pointer.
- Modify `nufs-core/metadata/pebble_store_test.go`: lock down snapshot point-in-time behavior.
- Create `nufs-core/metadata/backup_repository.go`: repository interfaces, descriptors, and filesystem test implementation.
- Modify `nufs-core/metadata/backup_s3.go`: publish, fetch, list, delete, and staging cleanup.
- Create `nufs-core/metadata/backup_repository_test.go`: atomic publication and interrupted-upload tests.
- Create `nufs-core/metadata/backup_catalog.go`: durable backup tasks, committed catalog, cluster identity, and reconciliation state.
- Create `nufs-core/metadata/backup_catalog_test.go`: persistence and state-transition tests.
- Modify `nufs-core/metadata/keys.go`: backup, cluster identity, and chunk tombstone prefixes.
- Create `nufs-core/metadata/backup_coordinator.go`: Leader scheduling, single-flight orchestration, verification, retention, and cancellation.
- Create `nufs-core/metadata/backup_coordinator_test.go`: success, failure, leadership loss, and retention tests.
- Modify `nufs-core/metadata/graceful_shutdown.go`: remove the legacy scheduled `BackupManager` after coordinator wiring is complete.
- Create `nufs-core/metadata/chunk_tombstone.go`: durable tombstones and backup-aware purge guard.
- Create `nufs-core/metadata/chunk_tombstone_test.go`: quarantine and fail-closed tests.
- Modify `nufs-core/metadata/production.go`: make `ChunkGC` create/process tombstones.
- Modify `nufs-core/metadata/production_test.go`: verify delayed and catalog-gated deletion.
- Create `nufs-core/metadata/restore.go`: download, verify, rewrite cluster identity, and atomic target publication.
- Create `nufs-core/metadata/restore_test.go`: empty-target, corruption, rollback, and identity tests.
- Create `nufs-core/metadata/restore_readiness.go`: persistent restore marker and read-only replica verification.
- Create `nufs-core/metadata/restore_readiness_test.go`: restored-cluster readiness gate tests.
- Create `nufs-core/cmd/nufs-backup/main.go`: manual create/list/verify/prune CLI.
- Create `nufs-core/cmd/nufs-restore/main.go`: inspect/restore CLI.
- Create `nufs-core/cmd/metad/ops_backup.go`: backup status and trigger handlers.
- Create `nufs-core/cmd/metad/ops_backup_test.go`: method, redirect, and response tests.
- Modify `nufs-core/cmd/metad/ops_handlers.go`: register backup routes and hold a coordinator dependency.
- Modify `nufs-core/cmd/metad/ops_prometheus.go`: export backup and tombstone metrics.
- Modify `nufs-core/cmd/metad/main.go`: configure repository/coordinator and replace the legacy manager.
- Modify `nufs-core/metadata/disaster_drill.go`: add daily restore verification scenario.
- Create `nufs-core/metadata/disaster_drill_backup_test.go`: restore drill report tests.
- Modify `nufs-core/metadata/service.go`: keep restored clusters not ready until replica verification passes.
- Create `nufs-core/cmd/metad/restore_readiness.go`: datanode replica probe used during restored startup.
- Create `nufs-core/cmd/metad/restore_readiness_test.go`: reachable and missing-replica startup tests.
- Modify `nufs-core/deploy/monitoring/alerting-rules.yaml`: add backup and tombstone alerts.
- Create `nufs-core/docs/runbooks/metadata-disaster-recovery.md`: backup and restore operator procedure.
- Create `nufs-core/tests/metadata_dr/restore_cluster_test.go`: end-to-end backup and new-cluster restore test.

## Task 1: Versioned Backup Artifact and Verifier

**Files:**
- Create: `nufs-core/metadata/backup_manifest.go`
- Create: `nufs-core/metadata/backup_manifest_test.go`
- Create: `nufs-core/metadata/backup_verify.go`
- Create: `nufs-core/metadata/backup_verify_test.go`

**Interfaces:**
- Produces:
  - `const BackupFormatVersion = 1`
  - `type BackupManifest struct`
  - `type BackupFile struct`
  - `type BackupRecordCounts struct`
  - `type BackupVerificationReport struct`
  - `func BuildBackupManifest(ctx context.Context, checkpointDir string, meta BackupSnapshotMetadata) (*BackupManifest, error)`
  - `func VerifyBackupArtifact(ctx context.Context, checkpointDir string, manifest *BackupManifest) (*BackupVerificationReport, error)`

- [ ] **Step 1: Write failing manifest validation tests**

Add table-driven tests with these exact cases:

```go
func TestBackupManifestRejectsUnsafePaths(t *testing.T) {
	for _, path := range []string{"", "/etc/passwd", "../escape", "a/../../escape", "a\\..\\escape"} {
		t.Run(path, func(t *testing.T) {
			m := validBackupManifest()
			m.Files[0].Path = path
			if err := m.Validate(); err == nil {
				t.Fatalf("Validate(%q) returned nil", path)
			}
		})
	}
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
```

- [ ] **Step 2: Run the tests and confirm RED**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./metadata -run 'TestBackupManifest|TestVerifyBackupArtifact' -count=1 -v
```

Expected: compile failure because the manifest and verifier types do not exist.

- [ ] **Step 3: Implement the public artifact contract**

Use these wire types:

```go
const BackupFormatVersion = 1

type BackupFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type BackupSnapshotMetadata struct {
	BackupID          string
	SourceClusterID   string
	CreatedAt         time.Time
	RaftTerm          uint64
	AppliedIndex      uint64
	MinimumNUFSVersion string
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
```

`Validate` must reject unsupported versions, duplicate/non-normalized paths,
negative sizes, non-64-character lowercase hexadecimal checksums, and totals
that do not equal the file list. `BuildBackupManifest` must sort file paths so
the same checkpoint produces deterministic JSON.

- [ ] **Step 4: Implement structural Pebble verification**

Open the checkpoint with:

```go
db, err := pebble.Open(checkpointDir, &pebble.Options{ReadOnly: true})
```

Scan all known key prefixes. Return an error for undecodable durable values,
missing root inode, missing bucket root, directory entries that reference
missing inodes, or inode chunk references that lack chunk metadata. Compare
actual counts with the manifest and include every check in
`BackupVerificationReport`.

- [ ] **Step 5: Run focused and package tests**

```bash
go test ./metadata -run 'TestBackupManifest|TestVerifyBackupArtifact' -count=1 -v
go test ./metadata -count=1
```

Expected: PASS.

- [ ] **Step 6: Review checkpoint**

Review only the four Task 1 files. Do not stage or commit under the current
branch policy.

## Task 2: Exact-Index Immutable Checkpoints

**Files:**
- Modify: `nufs-core/metadata/pebble_raft.go`
- Modify: `nufs-core/metadata/snapshot_checkpoint.go`
- Modify: `nufs-core/metadata/pebble_store_test.go`
- Create: `nufs-core/metadata/backup_checkpoint_test.go`

**Interfaces:**
- Produces:
  - `type PortableCheckpoint struct { Dir string; Term uint64; AppliedIndex uint64 }`
  - `func (c *PortableCheckpoint) Release() error`
  - `func (n *RaftNode) CreateBackupCheckpoint(ctx context.Context, parentDir string) (*PortableCheckpoint, error)`
  - `func (s *PebbleStore) CreateStandaloneCheckpoint(ctx context.Context, parentDir string) (*PortableCheckpoint, error)`

- [ ] **Step 1: Add a point-in-time regression test**

The test must block `PebbleSnapshot.Persist`, mutate the live store after
`Snapshot`, then restore and prove the later key is absent:

```go
func TestPebbleSnapshotDoesNotIncludeWritesAfterSnapshot(t *testing.T) {
	store := newCheckpointStore(t)
	putTestKey(t, store, "before", "one")
	fsm := NewPebbleFSM(store)
	snapshot, err := fsm.Snapshot()
	if err != nil { t.Fatal(err) }
	putTestKey(t, store, "after", "two")
	data := persistSnapshotToBytes(t, snapshot)
	restored := restoreSnapshotBytes(t, data)
	assertTestKey(t, restored, "before", "one")
	assertTestKeyMissing(t, restored, "after")
}
```

- [ ] **Step 2: Run the regression test and confirm RED**

```bash
go test ./metadata -run TestPebbleSnapshotDoesNotIncludeWritesAfterSnapshot -count=1 -v
```

Expected: FAIL because `PebbleSnapshot` currently checkpoints the live DB
during `Persist`.

- [ ] **Step 3: Move checkpoint creation into the FSM snapshot boundary**

Add `snapshotMu sync.RWMutex` to `PebbleFSM`. `Apply` takes `RLock`; `Snapshot`
takes `Lock`, flushes Pebble, creates the checkpoint directory, and returns:

```go
type PebbleSnapshot struct {
	checkpointDir string
	releaseOnce   sync.Once
}
```

`Persist` calls `checkpointWriteDir(s.checkpointDir, sink)`.
`Release` removes `checkpointDir` exactly once. Never retain a live
`*PebbleStore` inside `PebbleSnapshot`.

- [ ] **Step 4: Add portable checkpoint creation**

`CreateBackupCheckpoint` must:

1. reject non-Leaders;
2. execute `raft.Barrier` with the caller deadline;
3. take the FSM checkpoint write lock;
4. read `raft.AppliedIndex()` and the numeric term;
5. flush and checkpoint into `parentDir`;
6. release the lock before returning.

The standalone method uses the same checkpoint helper with term/index zero.

- [ ] **Step 5: Verify exact-index and existing snapshot behavior**

```bash
go test ./metadata -run 'TestPebbleSnapshot|TestRaft.*Snapshot|TestCreateBackupCheckpoint' -count=1 -v
go test -race ./metadata -run 'TestPebbleSnapshot|TestCreateBackupCheckpoint' -count=1
```

Expected: PASS with no race reports.

- [ ] **Step 6: Review checkpoint**

Review Task 2 for lock ordering. The FSM gate must never be held during hashing
or upload.

## Task 3: Atomic Backup Repository

**Files:**
- Create: `nufs-core/metadata/backup_repository.go`
- Create: `nufs-core/metadata/backup_repository_test.go`
- Modify: `nufs-core/metadata/backup_s3.go`
- Create: `nufs-core/metadata/backup_s3_test.go`

**Interfaces:**
- Consumes: `BackupManifest`.
- Produces:

```go
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
```

- [ ] **Step 1: Write repository contract tests**

Cover:

```go
func TestRepositoryDoesNotListInterruptedPublish(t *testing.T)
func TestRepositoryWritesCommittedMarkerLast(t *testing.T)
func TestRepositoryFetchRejectsMissingCommittedMarker(t *testing.T)
func TestRepositoryDeleteIsIdempotent(t *testing.T)
func TestRepositoryListSortsNewestFirst(t *testing.T)
```

Use a filesystem repository rooted at `t.TempDir()` as the deterministic
contract implementation.

- [ ] **Step 2: Run tests and confirm RED**

```bash
go test ./metadata -run 'TestRepository' -count=1 -v
```

Expected: compile failure because `BackupRepository` is undefined.

- [ ] **Step 3: Implement filesystem repository and S3 protocol**

Publication order is exact:

```text
staging/<id>/files/*
staging/<id>/manifest.json
backups/<id>/files/*
backups/<id>/manifest.json
backups/<id>/COMMITTED
```

The S3 implementation uses `PutObject` for staging, `CopyObject` for
promotion, and a zero-byte `COMMITTED` object last. `Fetch` downloads only
manifest-declared files, uses `O_CREATE|O_EXCL`, and reuses
`VerifyBackupArtifact` before returning.

- [ ] **Step 4: Add an injectable S3 API**

Define the narrow internal interface used by tests:

```go
type backupS3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}
```

Test that a failure before marker creation is never listed as committed.

- [ ] **Step 5: Run verification**

```bash
go test ./metadata -run 'TestRepository|TestS3BackupRepository' -count=1 -v
go test ./metadata -count=1
```

Expected: PASS.

- [ ] **Step 6: Review checkpoint**

Confirm S3 object keys cannot escape the configured prefix and no file
descriptor is deferred inside an unbounded walk loop.

## Task 4: Durable Backup Catalog and Cluster Identity

**Files:**
- Modify: `nufs-core/metadata/keys.go`
- Create: `nufs-core/metadata/backup_catalog.go`
- Create: `nufs-core/metadata/backup_catalog_test.go`
- Modify: `nufs-core/metadata/service.go`

**Interfaces:**
- Produces:
  - `type BackupTaskState string`
  - `type BackupTask struct`
  - `type CommittedBackup struct`
  - `type BackupCatalogState struct`
  - `func (s *PebbleStore) PutBackupTask(ctx context.Context, task *BackupTask) error`
  - `func (s *PebbleStore) ListBackupTasks(ctx context.Context, limit int) ([]BackupTask, error)`
  - `func (s *PebbleStore) ReplaceCommittedBackupCatalog(ctx context.Context, backups []CommittedBackup, reconciledAt time.Time) error`
  - `func (s *PebbleStore) GetBackupCatalogState(ctx context.Context) (*BackupCatalogState, error)`
  - `func (s *PebbleStore) EnsureClusterID(ctx context.Context, requested string) (string, error)`
  - `func (s *PebbleStore) PutRestorePendingMarker(ctx context.Context, marker *RestorePendingMarker) error`
  - `func (s *PebbleStore) GetRestorePendingMarker(ctx context.Context) (*RestorePendingMarker, error)`
  - `func (s *PebbleStore) ClearRestorePendingMarker(ctx context.Context) error`

- [ ] **Step 1: Write persistence and transition tests**

Tests must prove:

- valid transition order is `creating -> uploading -> verifying -> committed`;
- any active state may transition to `failed`;
- `committed` and `failed` are terminal;
- catalog replacement is one Raft/Pebble batch;
- catalog survives close/reopen;
- an existing non-empty cluster ID cannot be silently replaced.

- [ ] **Step 2: Run tests and confirm RED**

```bash
go test ./metadata -run 'TestBackupCatalog|TestBackupTask|TestClusterID' -count=1 -v
```

Expected: compile failure for the new types and methods.

- [ ] **Step 3: Add durable keys and records**

Use these prefixes:

```go
prefixBackupTask    = "backup/task/"
prefixBackupCatalog = "backup/catalog/"
keyBackupCatalog    = "backup/catalog-state"
keyClusterID        = "system/cluster-id"
keyRestorePending   = "system/restore-pending"
```

Persist task state transitions and catalog replacement through the existing
Raft-aware batch path. Sort committed catalog entries newest first and reject
duplicates. Define `RestorePendingMarker` in this file and persist/clear it
through the same Raft-aware path:

```go
type RestorePendingMarker struct {
	BackupID        string
	SourceClusterID string
	AppliedIndex    uint64
	RestoredAt      time.Time
}
```

- [ ] **Step 4: Add the service boundary**

Add:

```go
type BackupMetadataService interface {
	PutBackupTask(context.Context, *BackupTask) error
	ListBackupTasks(context.Context, int) ([]BackupTask, error)
	ReplaceCommittedBackupCatalog(context.Context, []CommittedBackup, time.Time) error
	GetBackupCatalogState(context.Context) (*BackupCatalogState, error)
	PutRestorePendingMarker(context.Context, *RestorePendingMarker) error
	GetRestorePendingMarker(context.Context) (*RestorePendingMarker, error)
	ClearRestorePendingMarker(context.Context) error
}
```

Keep it separate from `MetadataService`; only metad backup and tombstone
workers require it.

- [ ] **Step 5: Run package and reopen tests**

```bash
go test ./metadata -run 'TestBackupCatalog|TestBackupTask|TestClusterID' -count=1 -v
go test ./metadata -count=1
```

Expected: PASS.

- [ ] **Step 6: Review checkpoint**

Confirm a repository operation never occurs while a Pebble or Raft mutation
lock is held.

## Task 5: Leader-Only Backup Coordinator

**Files:**
- Create: `nufs-core/metadata/backup_coordinator.go`
- Create: `nufs-core/metadata/backup_coordinator_test.go`
- Modify: `nufs-core/metadata/graceful_shutdown.go`
- Modify: `nufs-core/cmd/metad/main.go`

**Interfaces:**
- Consumes: portable checkpoint source, verifier, repository, and durable catalog.
- Produces:

```go
type BackupCoordinatorConfig struct {
	ClusterID       string
	Interval        time.Duration
	Retention       int
	LocalTempDir    string
	StagingMaxAge   time.Duration
	UploadTimeout   time.Duration
}

type BackupRunResult struct {
	Task     BackupTask
	Manifest *BackupManifest
}

func NewBackupCoordinator(cfg BackupCoordinatorConfig, store *PebbleStore, repository BackupRepository) *BackupCoordinator
func (c *BackupCoordinator) Start()
func (c *BackupCoordinator) Stop()
func (c *BackupCoordinator) Trigger(ctx context.Context) (*BackupRunResult, error)
func (c *BackupCoordinator) Status(ctx context.Context) BackupCoordinatorStatus
```

- [ ] **Step 1: Write coordinator behavior tests**

Use fake clock, checkpoint source, repository, and leadership function. Cover:

```go
func TestBackupCoordinatorPublishesVerifiedBackup(t *testing.T)
func TestBackupCoordinatorFollowerDoesNotRun(t *testing.T)
func TestBackupCoordinatorCoalescesConcurrentTriggers(t *testing.T)
func TestBackupCoordinatorCancelsOnLeadershipLoss(t *testing.T)
func TestBackupCoordinatorDoesNotPruneAfterFailure(t *testing.T)
func TestBackupCoordinatorKeepsLatest24CommittedBackups(t *testing.T)
```

- [ ] **Step 2: Run tests and confirm RED**

```bash
go test ./metadata -run TestBackupCoordinator -count=1 -v
```

Expected: compile failure because `BackupCoordinator` is undefined.

- [ ] **Step 3: Implement the run pipeline**

The implementation order is:

```go
checkpoint, err := source.CreateBackupCheckpoint(ctx, cfg.LocalTempDir)
manifest, err := BuildBackupManifest(ctx, checkpoint.Dir, snapshotMeta)
report, err := VerifyBackupArtifact(ctx, checkpoint.Dir, manifest)
err = repository.Publish(ctx, checkpoint.Dir, manifest)
remoteManifest, err := repository.Fetch(ctx, manifest.BackupID, verifyDir)
_, err = VerifyBackupArtifact(ctx, verifyDir, remoteManifest)
err = reconcileCatalogAndPrune(ctx)
```

Persist every state transition. Defer checkpoint/temp cleanup. Poll leadership
through a cancellation watcher while upload or verification is active.

- [ ] **Step 4: Wire metad configuration**

Replace the legacy `--backup-dir` manager with:

```text
--backup-enabled
--backup-local-dir
--backup-interval=1h
--backup-retention=24
--backup-s3-bucket
--backup-s3-prefix
--backup-s3-region
--backup-s3-endpoint
--backup-upload-timeout=10m
--cluster-id
```

Production mode must reject backup-enabled configuration without cluster ID,
remote bucket, or a writable local temp directory.

- [ ] **Step 5: Remove the legacy scheduling path**

Delete `BackupManager` and its obsolete tests only after coordinator tests and
metad startup tests pass. Retain shared S3 construction helpers.

- [ ] **Step 6: Run verification**

```bash
go test ./metadata ./cmd/metad -run 'TestBackupCoordinator|Test.*BackupConfig' -count=1 -v
go test -race ./metadata -run TestBackupCoordinator -count=1
```

Expected: PASS.

- [ ] **Step 7: Review checkpoint**

Confirm `Stop` is idempotent, waits for the active run, and cannot race
`Trigger`.

## Task 6: Backup-Aware Chunk Tombstones

**Files:**
- Modify: `nufs-core/metadata/keys.go`
- Create: `nufs-core/metadata/chunk_tombstone.go`
- Create: `nufs-core/metadata/chunk_tombstone_test.go`
- Modify: `nufs-core/metadata/pebble_store.go`
- Modify: `nufs-core/metadata/production.go`
- Modify: `nufs-core/metadata/production_test.go`

**Interfaces:**
- Produces:

```go
type ChunkTombstone struct {
	ChunkID      ChunkID
	Replicas     []ReplicaInfo
	Size         int64
	Reason       string
	DeletedAt    time.Time
	DeleteAfter  time.Time
}

func (s *PebbleStore) TombstoneChunk(ctx context.Context, chunkID ChunkID, reason string) error
func (s *PebbleStore) ListChunkTombstones(ctx context.Context, limit int) ([]ChunkTombstone, error)
func (s *PebbleStore) PurgeChunk(ctx context.Context, chunkID ChunkID) error
func (s *PebbleStore) CanPurgeChunk(ctx context.Context, tombstone ChunkTombstone, now time.Time) (bool, error)
```

- [ ] **Step 1: Write fail-closed purge tests**

Cover exact cases:

- 24h59m59s is not eligible;
- 25h with a retained backup older than deletion is not eligible;
- 25h with all retained backups newer than deletion is eligible;
- empty, stale, or unreconciled catalog returns an error and is not eligible;
- repeated tombstone and purge calls are idempotent.

- [ ] **Step 2: Run tests and confirm RED**

```bash
go test ./metadata -run 'TestChunkTombstone|TestChunkGC.*Tombstone' -count=1 -v
```

Expected: compile failure for tombstone APIs.

- [ ] **Step 3: Implement tombstone persistence**

Use `prefixChunkTombstone = "chunk-tombstone/"`. `TombstoneChunk` reads the
current `ChunkMeta` and atomically creates the tombstone without deleting
`prefixChunk`. `PurgeChunk` atomically deletes both records.

Change public `DeleteChunk` to tombstone the chunk. This centralizes protection
for every current caller without gateway-specific edits.

- [ ] **Step 4: Change ChunkGC into two phases**

Phase A scans live inode references and creates tombstones for newly orphaned
chunks. Phase B scans tombstones and calls `CanPurgeChunk`; only eligible
tombstones call `PurgeChunk`. Deleting chunk metadata then allows existing
datanode orphan GC to remove physical payloads.

Extend `GCScanResult` with:

```go
TombstonesCreated int
TombstonesRetained int
ChunksPurged       int
RetainedBytes      int64
```

- [ ] **Step 5: Run metadata and datanode GC tests**

```bash
go test ./metadata -run 'TestChunkTombstone|TestChunkGC' -count=1 -v
go test ./datanode -run 'Test.*GC|Test.*Delete' -count=1 -v
go test -race ./metadata -run 'TestChunkTombstone|TestChunkGC' -count=1
```

Expected: PASS.

- [ ] **Step 6: Review checkpoint**

Audit every `DeleteChunk` caller. Tests that require immediate failed-write
cleanup must assert logical removal from the active namespace, not premature
physical payload deletion.

## Task 7: Offline Restore Engine and CLIs

**Files:**
- Create: `nufs-core/metadata/restore.go`
- Create: `nufs-core/metadata/restore_test.go`
- Create: `nufs-core/metadata/restore_readiness.go`
- Create: `nufs-core/metadata/restore_readiness_test.go`
- Create: `nufs-core/cmd/nufs-backup/main.go`
- Create: `nufs-core/cmd/nufs-backup/main_test.go`
- Create: `nufs-core/cmd/nufs-restore/main.go`
- Create: `nufs-core/cmd/nufs-restore/main_test.go`

**Interfaces:**
- Consumes: `BackupRepository`, verifier, and cluster identity records.
- Produces:

```go
type RestoreOptions struct {
	BackupID    string
	TargetDir   string
	NewClusterID string
	NUFSVersion string
}

type RestoreReport struct {
	BackupID       string
	SourceClusterID string
	NewClusterID   string
	StartedAt      time.Time
	CompletedAt    time.Time
	AppliedIndex   uint64
	Verification   BackupVerificationReport
}

func RestoreBackupToNewCluster(ctx context.Context, repository BackupRepository, opts RestoreOptions) (*RestoreReport, error)

type RestoreReplicaProbe interface {
	ReachableReplicas(context.Context, *ChunkMeta) (int, error)
}

func VerifyRestoredChunkAvailability(ctx context.Context, store *PebbleStore, probe RestoreReplicaProbe, minimumReplicas int) (*RestoreReadinessReport, error)
```

- [ ] **Step 1: Write restore safety tests**

Cover:

```go
func TestRestoreRejectsNonEmptyTarget(t *testing.T)
func TestRestoreRejectsCorruptArtifactWithoutPublishingTarget(t *testing.T)
func TestRestoreRewritesClusterIdentity(t *testing.T)
func TestRestorePublishesTargetAtomically(t *testing.T)
func TestRestoreCleansInterruptedTemporaryDirectory(t *testing.T)
func TestRestoreRejectsSameSourceAndNewClusterID(t *testing.T)
```

- [ ] **Step 2: Run tests and confirm RED**

```bash
go test ./metadata -run TestRestore -count=1 -v
```

Expected: compile failure because the restore engine does not exist.

- [ ] **Step 3: Implement offline restore**

Restore into `TargetDir + ".restore-" + randomSuffix`. After artifact
verification, open Pebble, replace `keyClusterID`, remove runtime-local backup
task state, and write a `RestorePendingMarker`. Close Pebble, fsync the
directory and parent, then rename to `TargetDir`. Write
`<TargetDir>.restore-report.json` only after rename.

Never copy or restore the old Raft directory. The operator bootstraps a new
Raft cluster with existing metad flags.

- [ ] **Step 4: Implement CLI parsing**

Commands and required flags:

```text
nufs-backup create --ops-url --auth-token
nufs-backup list --repository-config
nufs-backup verify <backup-id> --repository-config
nufs-backup prune --ops-url --dry-run
nufs-restore inspect <backup-id> --repository-config
nufs-restore restore <backup-id> --repository-config --target-dir --new-cluster-id
```

Both commands support `--json`. Exit code is nonzero for verification failure,
unsafe target, missing committed marker, or incompatible format.

- [ ] **Step 5: Run CLI and restore tests**

```bash
go test ./metadata ./cmd/nufs-backup ./cmd/nufs-restore -run 'TestRestore|TestBackupCommand|TestRestoreCommand' -count=1 -v
go build -o /tmp/nufs-backup-check ./cmd/nufs-backup
go build -o /tmp/nufs-restore-check ./cmd/nufs-restore
```

Expected: PASS and both binaries build.

- [ ] **Step 6: Review checkpoint**

Confirm all created files use restrictive permissions and archive paths cannot
escape the temporary restore directory.

## Task 8: Ops API, Metrics, Alerts, and Runbook

**Files:**
- Create: `nufs-core/cmd/metad/ops_backup.go`
- Create: `nufs-core/cmd/metad/ops_backup_test.go`
- Modify: `nufs-core/cmd/metad/ops_handlers.go`
- Modify: `nufs-core/cmd/metad/ops_prometheus.go`
- Modify: `nufs-core/cmd/metad/main.go`
- Modify: `nufs-core/deploy/monitoring/alerting-rules.yaml`
- Create: `nufs-core/docs/runbooks/metadata-disaster-recovery.md`

**Interfaces:**
- Produces:
  - `GET /api/v1/backups`
  - `GET /api/v1/backups/status`
  - `POST /api/v1/backups`
  - `POST /api/v1/backups/{id}/verify`

- [ ] **Step 1: Write failing handler and metric tests**

Tests must assert:

- GET works on a follower;
- POST receives the existing 307 Leader redirect;
- concurrent POST returns the active task rather than starting another;
- verification errors are structured JSON;
- Prometheus output contains backup/tombstone metrics and no backup-ID label.

- [ ] **Step 2: Run tests and confirm RED**

```bash
go test ./cmd/metad -run 'TestOpsBackup|TestPrometheus.*Backup' -count=1 -v
```

Expected: route not found or compile failure.

- [ ] **Step 3: Add handlers and dependency wiring**

Extend `opsHandlers` with:

```go
backups *metadata.BackupCoordinator
```

Register read-only routes directly and wrap trigger/verify routes with
`requireLeader`. Return `503 backup_disabled` when no coordinator is
configured, `409 backup_in_progress` for incompatible concurrent actions, and
`500 backup_failed` with a stable code for backend failures.

- [ ] **Step 4: Add metrics and alerts**

Export the exact metrics from the design. Add:

```yaml
- alert: NUFSBackupStale
  expr: time() - nufs_backup_last_success_timestamp_seconds > 4500
  for: 5m
- alert: NUFSBackupVerificationFailed
  expr: increase(nufs_backup_verification_failures_total[15m]) > 0
  for: 1m
- alert: NUFSChunkTombstoneBacklog
  expr: nufs_chunk_tombstone_oldest_age_seconds > 93600
  for: 30m
```

- [ ] **Step 5: Write the operator runbook**

Document:

- repository configuration and credentials;
- manual create/list/verify commands;
- new-cluster restore sequence;
- readiness and scrub checks;
- alert diagnosis;
- how to disable pruning and physical purge;
- rollback and failed-restore cleanup.

- [ ] **Step 6: Run verification**

```bash
go test ./cmd/metad -run 'TestOpsBackup|TestPrometheus.*Backup' -count=1 -v
ruby -e 'require "yaml"; YAML.load_file(ARGV[0]); puts "alerts ok"' deploy/monitoring/alerting-rules.yaml
if command -v promtool >/dev/null 2>&1; then promtool check rules deploy/monitoring/alerting-rules.yaml; fi
```

Expected: tests pass and YAML parses; report explicitly when `promtool` is not
installed.

- [ ] **Step 7: Review checkpoint**

Confirm no restore endpoint exists and backup IDs never become Prometheus
labels.

## Task 9: Automated Restore Drill and End-to-End Recovery

**Files:**
- Modify: `nufs-core/metadata/disaster_drill.go`
- Create: `nufs-core/metadata/disaster_drill_backup_test.go`
- Modify: `nufs-core/metadata/service.go`
- Create: `nufs-core/cmd/metad/restore_readiness.go`
- Create: `nufs-core/cmd/metad/restore_readiness_test.go`
- Create: `nufs-core/tests/metadata_dr/restore_cluster_test.go`
- Modify: `nufs-core/cmd/metad/main.go`

**Interfaces:**
- Produces:
  - `DrillBackupRestore DrillScenario = "backup_restore"`
  - daily scheduling configuration for read-only restore verification;
  - machine-readable observed RPO and RTO in `DrillReport.Checks`.

- [ ] **Step 1: Write failing drill tests**

Tests must prove the drill:

- selects only the latest committed backup;
- runs artifact and metadata verification;
- opens the restored store without joining production Raft;
- runs read-only metadata-to-replica checks;
- deletes the temporary environment;
- records RPO and restore duration;
- reports failure without modifying production metadata.

- [ ] **Step 2: Run tests and confirm RED**

```bash
go test ./metadata -run TestDisasterDrillBackupRestore -count=1 -v
```

Expected: compile failure because `DrillBackupRestore` is undefined.

- [ ] **Step 3: Implement the drill scenario**

Inject repository, restore engine, temporary directory root, clock, and
read-only replica probe into the drill runner. Set a 30-minute scenario
deadline. Never call `RegisterNode`, repair, or any mutating metadata API from
this scenario.

- [ ] **Step 4: Gate restored-cluster readiness**

On metad startup, detect `RestorePendingMarker`. Start the Ops server so health
and metrics remain observable, but make `ServiceBundle.IsReady` return false.
Run `VerifyRestoredChunkAvailability` with a metad-side datanode probe that
checks each referenced chunk replica without modifying metadata.

Add `--restore-minimum-readable-replicas=1`; reject values below one.

When every referenced chunk has at least the configured minimum reachable
replicas, remove the marker through Raft and open the readiness gate. If any
chunk has zero reachable replicas, keep the marker, remain not ready, and
publish the failed check. Degraded-but-readable chunks may trigger repair only
after the readiness verification itself has completed.

Tests:

```go
func TestRestoredClusterStaysNotReadyUntilReplicaVerificationPasses(t *testing.T)
func TestRestoredClusterRemainsNotReadyWhenAllReplicasAreMissing(t *testing.T)
func TestNormalClusterDoesNotWaitForRestoreVerification(t *testing.T)
```

- [ ] **Step 5: Add end-to-end recovery test**

The test fixture must:

1. start a three-node source metad cluster;
2. create buckets, namespace entries, chunks, quotas, write attempts, and
   background tasks;
3. create and publish a backup;
4. restore it into a new directory with a new cluster ID;
5. start a new three-node Raft cluster from the restored data;
6. compare durable records;
7. assert `/ready` is 503 before replica verification;
8. verify referenced test chunk replicas are reachable;
9. assert `/ready` becomes 200 after the restore marker is cleared;
10. assert observed recovery time is below 30 minutes.

- [ ] **Step 6: Run complete verification**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./metadata ./cmd/metad ./cmd/nufs-backup ./cmd/nufs-restore ./tests/metadata_dr -count=1
go test ./... -count=1
go test -race ./metadata ./cmd/metad -count=1
go vet ./...
go build -o /tmp/nufs-metad-check ./cmd/metad
go build -o /tmp/nufs-backup-check ./cmd/nufs-backup
go build -o /tmp/nufs-restore-check ./cmd/nufs-restore
cd /Users/gracegaoya/work/project/nufs
git diff --check
```

Expected: every command exits zero with no race or vet findings.

- [ ] **Step 7: Perform final spec review**

Check the implementation against every acceptance criterion in
`docs/superpowers/specs/2026-07-28-nufs-metadata-disaster-recovery-v1-design.md`.
Document any environment-only test that could not run, including the exact
missing dependency.

- [ ] **Step 8: Final review checkpoint**

Review the complete working-tree diff for data-loss risks, lock ordering,
path traversal, partial publication, stale Leader behavior, and fail-open GC.
Do not stage or commit under the current branch policy.
