# NUFS Metadata Disaster Recovery v1 Design

Date: 2026-07-28

## Summary

NUFS Metadata Disaster Recovery v1 provides hourly, verifiable metadata
backups to an S3-compatible remote repository and restores a selected backup
into an empty, newly bootstrapped metad cluster.

The design builds on the existing Pebble checkpoint, Raft snapshot, and S3
upload code. It adds a durable backup artifact protocol, exact consistency
boundaries, independent verification, a restore CLI, automated recovery
drills, and delayed physical chunk deletion.

The v1 recovery objectives are:

- Recovery point objective (RPO): at most one hour.
- Recovery time objective (RTO): at most 30 minutes.
- Retention: the latest 24 successful hourly backups.
- Authoritative backup location: S3-compatible remote storage.
- Restore target: an empty, newly bootstrapped cluster only.

## Scope

### In Scope

- Leader-coordinated metadata checkpoints.
- Portable, versioned backup artifacts.
- S3-compatible upload, download, listing, and deletion.
- File-level SHA-256 integrity verification.
- Structural and referential validation of restored metadata.
- Restore into an empty data directory.
- New Raft cluster identity after restore.
- A 25-hour physical chunk deletion quarantine.
- Backup, verification, restore, and tombstone metrics and alerts.
- Automated daily restore drills.

Metadata includes:

- buckets and placement policies;
- namespace entries and inodes;
- chunk metadata and inode-to-chunk mappings;
- node metadata;
- bucket quotas and usage counters;
- advisory-independent durable write attempts;
- background task leases and durable task records;
- other durable Pebble records required to restart metad.

### Out of Scope

- Copying chunk payloads into the backup repository.
- Online restore into a running production cluster.
- In-place replacement of an existing metad cluster.
- Point-in-time recovery between hourly checkpoints.
- Cross-version schema migration beyond explicitly supported manifest and
  checkpoint versions.
- Gateway-specific backup or recovery behavior.
- Admin UI workflows.

## Existing Capabilities and Gaps

NUFS already has:

- Pebble checkpoint creation;
- Raft PBL3 snapshot persistence and restore;
- a scheduled `BackupManager`;
- an `S3RemoteStorage` uploader;
- local `CreateBackup` and `RestoreBackup` methods;
- metadata disaster drill infrastructure.

These capabilities are not yet a production recovery system:

- backup artifacts have no manifest or integrity contract;
- remote upload has no atomic publication marker;
- there is no remote download and restore workflow;
- checkpoints are not exposed with an exact, auditable Raft applied index;
- backup execution is not explicitly Leader-owned;
- restore does not enforce an empty target or new Raft identity;
- backup verification is not independent of the live store;
- old metadata may reference chunk payloads deleted after the checkpoint;
- no automated restore drill proves that a backup remains usable.

## Architecture

### BackupCoordinator

`BackupCoordinator` owns scheduling and orchestration.

Responsibilities:

- run only while the local metad node is the Raft Leader;
- trigger one backup per hour;
- serialize backup runs so at most one is active per cluster;
- obtain an immutable checkpoint at an exact Raft applied index;
- build and verify the local artifact;
- publish the artifact through `BackupRepository`;
- retain the latest 24 committed backups;
- persist the latest task state and expose operational metrics.

A leadership change cancels the active upload. An incomplete upload remains
uncommitted and cannot be selected for restore.

### BackupRepository

`BackupRepository` separates artifact semantics from S3 implementation:

```go
type BackupRepository interface {
    Begin(ctx context.Context, backupID string) (BackupUpload, error)
    ListCommitted(ctx context.Context) ([]BackupDescriptor, error)
    Open(ctx context.Context, backupID string) (BackupDownload, error)
    Delete(ctx context.Context, backupID string) error
}
```

The concrete v1 implementation supports AWS S3, MinIO, and compatible object
stores through the AWS SDK.

Artifacts are uploaded under:

```text
staging/<backup-id>/
backups/<backup-id>/
```

Files are uploaded to staging first. Publication copies or promotes the
verified files to the committed prefix, writes `manifest.json`, and writes
`COMMITTED` last. A backup is recoverable only when:

- the committed marker exists;
- the manifest is valid;
- all declared files exist;
- all file sizes and checksums match.

Repository pruning considers only committed artifacts. Stale staging prefixes
are garbage collected separately after 24 hours.

### BackupVerifier

`BackupVerifier` performs two validation levels:

1. Artifact validation:
   - manifest schema and format compatibility;
   - declared file presence;
   - exact file sizes;
   - SHA-256 checksums;
   - no undeclared checkpoint files.
2. Metadata validation:
   - open the checkpoint as a read-only Pebble database;
   - verify the root inode;
   - enumerate and decode all durable key families;
   - verify directory entries reference existing inodes;
   - verify inode chunk references resolve to chunk metadata;
   - validate bucket roots, quota records, and task records;
   - compare actual record counts with manifest statistics.

Verification produces a machine-readable report. A failed report prevents
publication or restore.

### Restore CLI

The `nufs-restore` command is an offline tool:

```text
nufs-restore inspect <backup-id>
nufs-restore restore <backup-id> --target-dir <empty-dir>
```

It:

- lists or opens only committed backups;
- downloads into a temporary sibling directory;
- verifies the artifact and metadata;
- confirms the target directory is absent or empty;
- strips old Raft peer membership and runtime-local state;
- generates bootstrap metadata for a new cluster identity;
- atomically renames the verified directory into place;
- writes a restore report next to the restored directory.

Restore never mutates a running store and never overwrites a non-empty target.

## Consistency Model

### Exact Checkpoint Boundary

The checkpoint must correspond to a known Raft applied index.

The Leader requests an FSM snapshot through Raft after a barrier. The snapshot
callback executes at the serialized FSM boundary. It flushes Pebble and creates
the immutable checkpoint before later apply operations can modify the snapshot
view.

The snapshot handle owns the checkpoint directory. Compression, hashing, and
remote upload happen after the FSM resumes. This limits metadata write
interruption to Pebble flush and checkpoint creation rather than the full
upload duration.

The manifest records the Raft term and exact applied index associated with the
checkpoint. These values are audit metadata; a restored cluster does not
resume the old Raft log.

### Backup Publication

Publication follows this state machine:

```text
creating -> uploading -> verifying -> committed
    |           |            |
    +-----------+------------+-> failed
```

Only `committed` artifacts count toward the RPO and retention policy.

Backup task records include:

- backup ID;
- source cluster ID;
- owner node and leadership term;
- applied index;
- state;
- start and completion times;
- bytes and files uploaded;
- last error.

### Chunk Payload Availability

Metadata backup does not copy chunk payloads. To prevent a retained checkpoint
from referencing already deleted data, physical chunk deletion is delayed.

When a chunk becomes unreferenced:

- metadata creates a durable tombstone;
- the tombstone records chunk ID, replicas, size, deletion reason, and
  `delete_after`;
- `delete_after` is at least 25 hours after logical deletion;
- the live namespace no longer references the chunk;
- datanode payloads remain readable during quarantine.

The tombstone GC deletes physical replicas only when both conditions hold:

- `delete_after` has passed;
- every retained committed backup was created after the chunk's logical
  deletion.

Successful backup and prune operations maintain a durable committed-backup
catalog in Raft metadata. A catalog entry is removed only after the remote
artifact is deleted successfully. If the catalog is unavailable, empty, stale,
or inconsistent with the repository, tombstone GC fails closed and retains the
payload. This matters when backup failures cause the latest 24 successful
backups to span more than 25 hours.

The restored checkpoint predates its corresponding tombstones, so it contains
the original chunk metadata and references. Post-restore scrub validates that
the retained replicas remain available.

## Backup Manifest

`manifest.json` contains:

```json
{
  "format_version": 1,
  "backup_id": "20260728T120000Z-000000012345",
  "source_cluster_id": "cluster-a",
  "created_at": "2026-07-28T12:00:00Z",
  "raft_term": 42,
  "applied_index": 12345,
  "checkpoint_format": "pebble-checkpoint-v1",
  "minimum_nufs_version": "0.1.0",
  "files": [
    {
      "path": "000123.sst",
      "size": 1048576,
      "sha256": "hex-encoded-sha256"
    }
  ],
  "record_counts": {
    "buckets": 10,
    "inodes": 1000,
    "chunks": 3000,
    "quotas": 8,
    "write_attempts": 2,
    "background_tasks": 4
  },
  "total_bytes": 1048576,
  "duration_ms": 850
}
```

Paths must be normalized relative paths. Absolute paths, empty paths, path
traversal, duplicate paths, and undeclared files are rejected.

Unknown manifest fields are tolerated within the same format version.
Unsupported higher format versions fail closed.

## Operations API

The metad operations API exposes:

- `GET /api/v1/backups`
- `GET /api/v1/backups/status`
- `POST /api/v1/backups`
- `POST /api/v1/backups/{id}/verify`

Mutating requests require the Leader. Followers preserve the existing
temporary redirect behavior.

There is deliberately no restore endpoint. Restore requires offline access to
an empty target directory.

## Command-Line Interface

The operational commands are:

```text
nufs-backup create
nufs-backup list
nufs-backup verify <backup-id>
nufs-backup prune
nufs-restore inspect <backup-id>
nufs-restore restore <backup-id> --target-dir <path>
```

Commands support human-readable output and `--json`. Destructive pruning
supports `--dry-run`. Restore requires explicit repository and target
configuration and refuses an ambiguous default target.

## Startup and Readiness

A newly restored cluster starts with a new cluster ID and bootstrap peer set.
Old peer IDs, network addresses, leadership term, and local advisory locks are
not reused.

The restored cluster remains not ready until:

- Pebble opens successfully;
- manifest verification has already passed;
- durable services initialize;
- metadata-to-datanode scrub completes;
- every referenced chunk has enough reachable replicas for the configured
  minimum read policy.

Missing replicas may trigger existing repair machinery. Missing all replicas
for a referenced chunk fails readiness and the restore drill.

## Retention and Cleanup

The default schedule is hourly.

Retention rules:

- retain the latest 24 committed backups;
- never prune the newest committed backup;
- do not prune after a failed backup run;
- remove committed backup 25 only after backup 24 is committed and verified;
- remove stale staging uploads after 24 hours;
- apply repository deletion idempotently.

The durable committed-backup catalog is reconciled with the remote repository
before pruning or physical chunk deletion. Repository or reconciliation
failure disables both destructive operations for that run.

Local checkpoint directories are temporary caches. They may be deleted after
the corresponding remote artifact is committed and verified.

## Metrics and Alerts

Metrics:

- `nufs_backup_last_success_timestamp_seconds`
- `nufs_backup_last_success_applied_index`
- `nufs_backup_duration_seconds`
- `nufs_backup_artifact_bytes`
- `nufs_backup_upload_failures_total`
- `nufs_backup_verification_failures_total`
- `nufs_backup_staging_artifacts`
- `nufs_restore_verification_duration_seconds`
- `nufs_restore_verification_failures_total`
- `nufs_chunk_tombstones`
- `nufs_chunk_tombstone_bytes`
- `nufs_chunk_tombstone_oldest_age_seconds`

Alerts:

- `NUFSBackupStale`: no committed backup for 75 minutes.
- `NUFSBackupVerificationFailed`: a new artifact fails verification.
- `NUFSChunkTombstoneBacklog`: eligible tombstones remain undeleted beyond the
  configured GC grace period.

Metrics must not include backup IDs as labels because they are unbounded.

## Failure Handling

- Non-Leader scheduled runs exit without creating an artifact.
- Concurrent manual and scheduled runs return the active task.
- Leadership loss cancels the current run.
- Upload failure leaves staging data only.
- Checksum mismatch retries the individual transfer and then fails the run.
- A failed backup does not advance the latest-success metric.
- A failed restore leaves the target unchanged.
- An interrupted restore leaves only a temporary directory, which a later run
  may safely remove.
- Repository listing failure prevents pruning.
- Tombstone deletion is idempotent across partial replica deletion.
- Unknown manifest versions, malformed paths, or incompatible NUFS versions
  fail closed.

## Automated Disaster Drill

Once per day, a drill runner:

1. selects the latest committed backup;
2. downloads it into a temporary directory;
3. runs artifact and metadata verification;
4. opens the restored store without joining production Raft;
5. runs metadata-to-datanode scrub in read-only mode;
6. records observed RPO and restore duration;
7. deletes the temporary environment.

Reports are persisted as JSON and exposed through existing disaster drill
reporting. The drill never injects writes into the restored store.

## Testing Strategy

### Unit Tests

- manifest encoding, decoding, validation, and version handling;
- normalized path and traversal rejection;
- checksum and size mismatch detection;
- upload publication order;
- committed marker requirements;
- retention ordering and failed-run behavior;
- 25-hour tombstone eligibility boundary;
- Leader-only scheduling and single-flight behavior.

### Integration Tests

- create a checkpoint containing buckets, inodes, chunks, quotas, write
  attempts, and background tasks;
- upload, download, verify, and restore it into an empty directory;
- compare restored durable records with the source snapshot;
- reject truncated, corrupted, incomplete, or undeclared artifact files;
- cancel an upload on leadership loss;
- retry safely after a partial remote upload;
- verify old backups remain usable while chunks are quarantined.

### Cluster Tests

- initialize a new three-node metad cluster from a restored checkpoint;
- verify the new cluster identity differs from the source;
- verify namespace and quota behavior;
- run scrub against retained datanode replicas;
- verify the cluster remains not ready when all replicas for a referenced chunk
  are missing;
- demonstrate recovery within 30 minutes on the production-size test fixture.

### Race and Fault Tests

- concurrent scheduled and manual backup triggers;
- leadership changes during checkpoint creation and upload;
- process termination during upload, verification, and restore;
- repository timeout and partial object visibility;
- tombstone GC retry after partial datanode failure.

## Acceptance Criteria

- An hourly Leader-owned backup produces a committed, independently verifiable
  artifact in S3-compatible storage.
- The latest 24 successful backups are retained.
- The latest committed backup is no more than one hour old under normal
  operation.
- Any retained valid backup restores into an empty new cluster within 30
  minutes on the production-size test fixture.
- Namespace, quota, chunk mappings, write attempts, and background task records
  match the checkpoint.
- Any corrupted or incomplete artifact is rejected before target publication.
- Tombstone GC deletes a chunk only after its time delay and after every
  retained committed backup is newer than the chunk's logical deletion.
- Daily restore drills publish machine-readable RPO and RTO results.
- Unit, integration, cluster, race, and fault-injection tests pass.

## Rollout

1. Introduce the manifest, repository abstraction, and verifier behind disabled
   scheduling.
2. Produce and verify manual backups without pruning.
3. Enable hourly backups and stale-backup alerting.
4. Enable daily restore drills.
5. Enable tombstone-based physical deletion and monitor retained bytes.
6. Enable automatic retention pruning after at least 24 consecutive verified
   hourly backups.

Rollback disables scheduling, pruning, and tombstone physical deletion.
Already committed artifacts remain readable by the restore CLI.
