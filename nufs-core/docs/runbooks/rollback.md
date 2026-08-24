# Rollback Runbook

Rollback is the action taken when a release is bad. NUFS's rollback story splits
cleanly in two — and only **one** of them is currently supported. Read
`compatibility-matrix.md` for why before acting.

## Which rollback applies?

| Condition | Rollback path |
|---|---|
| Schema unchanged, backup format compatible, Raft entries compatible, new release is only a code bug | **Not supported today** (binary rollback) |
| Irreversible schema change happened, or new version wrote data an old version cannot parse, or Raft entries are incompatible | **Restore to a new cluster** (supported) |

The first row is listed for completeness and to make the boundary explicit: NUFS
is currently on the direct-format-replacement design, so a *binary* rollback
(reverting the image and reopening the same data) is **not available**. An old
binary cannot open a newer on-disk format or Raft log.

## Supported rollback: restore to a new cluster

This is the same path as whole-cluster loss recovery and is already gated by
`scripts/soak/run-v21-metadata-restore.sh` (Layer 2). Operate it as defined in
`metadata-disaster-recovery.md` and `metadata-backup-restore-drill.md`:

1. **Stop writes.** Quiesce the broken cluster and isolate it so it cannot
   serve or be written to.
2. **Select a verified backup.** Choose the newest **non-stale, verified**
   committed backup (`/api/v1/backups/$id/verify`).
3. **Restore to a new cluster ID.** Provision fresh metad data + empty Raft
   directories; run `RestoreBackupToNewCluster` into a brand-new `cluster-id`.
   Never replay old Raft dirs.
4. **Start the new cluster.** It stays `ServiceUnavailable` until the replica
   probe passes, so it cannot serve stale reads prematurely.
5. **Verify metadata / object / chunk.** Confirm the restore is complete and the
   readiness gate has released.
6. **Cut traffic.** Point the gateway / clients at the restored cluster.

## Why binary rollback is not offered

- The V2.1 design (see `docs/superpowers/specs/2026-08-02-…-v2-design.md`)
  replaces the legacy layout directly with **no dual-format reads and no
  rollback to the old on-disk format**.
- `RaftLogOp` entries carry no entry-level version, so an old binary cannot
  safely re-apply a log written by a newer binary.

Until versioning is added (compatibility-matrix §6), **rollback == restore to a
new cluster**. Never attempt "swap images back" on the same data.
