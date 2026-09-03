# Snapshot & Bucket Stats — Design Spec

**Date:** 2026-06-04
**Status:** Draft

## Overview

Two independent optimizations for 100B-key scale:

1. **Checkpoint-based snapshot** — replaces KV-stream Persist/Restore with
   `db.Checkpoint()` → tar → zstd. Avoids scanning all keys.
2. **Per-bucket usage counters** — real-time counters updated atomically
   with mutations, eliminating the O(all objects) scan on admin requests.

---

# Part 1: Checkpoint-Based Snapshot

## Current State

### Persist (`PebbleSnapshot.Persist`)

Iterates every KV pair via `NewIter(nil)`, serializes with length-prefixed
encoding, zstd-compresses, and writes to Raft SnapshotSink. For 100B keys
this takes many hours and consumes significant CPU.

### Restore (`PebbleFSM.Restore`)

Reads zstd stream, decompresses, iterates every KV pair, writes via Pebble
batch (50K per batch). Same O(all data) cost.

## Problem

| Dataset | Current Persist | Current Restore | Impact |
|---------|----------------|-----------------|--------|
| 100M keys | ~min | ~sec | Fine |
| 10B keys | ~hours | ~min | Painful |
| 100B keys | ~day+ | ~hours | Unacceptable |

The bottleneck is **iterating all KV pairs**, not I/O or compression.
Even with parallel scan, 100B keys must be read from LSM — Pebble's merge
iterator overhead makes this slow at scale.

## Design: Checkpoint-Based Snapshot

Use Pebble's `db.Checkpoint()` to create a filesystem-level snapshot via hard
links (milliseconds, no data copy), then archive the checkpoint directory.

### New format

```
[magic:4 "PBL3"]
[file_count:4]
[for each file:]
  [path_len:2][relative_path]
  [data_len:8][file_data (zstd compressed)]
```

Archive is a flat list of files from the checkpoint directory, each compressed
individually with zstd. No tar dependency — custom format with 80 lines of
serialization code.

### Persist flow

```
s.store.db.Checkpoint(tempDir)    # ms, hard links to SSTs
  ↓
Walk tempDir (SSTs + MANIFEST + OPTIONS)
  ↓
For each file:
  → read, zstd compress, write [path_len][path][data_len][data] to sink
  ↓
Remove tempDir
  ↓
sink.Close()
```

Key properties:
- No key iteration. Checkpoint is a metadata operation (~50ms).
- File read + zstd is streaming — ~50MB/s per file, bound by disk read + CPU.
- Files can be parallelized if desired (future optimization).
- Multiple WAL files are included automatically (flushed before checkpoint).

### Restore flow

```
Read magic "PBL3"
  ↓
For each file entry:
  → decompress zstd → write to tempDir/relative_path
  ↓
Close current DB instance
  ↓
os.Rename(tempDir → dataDir)      # atomic on same filesystem
  ↓
Open new Pebble DB on relocated dataDir
  ↓
Assign new *pebble.DB to store.db
  ↓
Close old DB instance
```

### Expected improvement

| Phase | KV-stream (current) | Checkpoint (PBL3) |
|-------|--------------------|--------------------|
| Persist I/O | Full scan (hours) | File read ~ 1 copy (min) |
| Persist zstd | Per KV (high overhead) | Per SST (bulk efficient) |
| **Total Persist** | **~day** | **~minutes** |
| **Total Restore** | **~hours** | **~minutes** (file rename + reopen) |

### Obstacles & Mitigations

#### 1. Backward compatibility

`Restore` checks magic:
- `"PBL1"` → legacy single-stream path (current + our earlier optimization)
- `"PBL3"` → new checkpoint-based path

Migration: deploy code, then manually trigger a snapshot. Old snapshots
(PBL1) remain restorable indefinitely.

#### 2. DB instance swap safety

Restore must close the old Pebble DB, replace its directory, and open a new
one. Raft guarantees that `FSM.Restore()` is called when no log commits are
in flight — no concurrent access to `db` during the swap.

Implementation is straightforward:

```go
func (f *PebbleFSM) Restore(rc io.ReadCloser) error {
    // 1. Extract archive to tempDir
    // 2. Close current DB
    f.store.db.Close()
    // 3. Replace DB directory atomically
    os.Rename(tempDir, f.store.cfg.Dir)
    // 4. Open new DB on relocated directory
    newDB, _ := pebble.Open(f.store.cfg.Dir, ...)
    f.store.db = newDB
    return nil
}
```

No mutex needed — Raft serializes access during restore.
`PebbleStore.Close()` blocks until in-flight operations complete.

#### 3. Checkpoint directory cleanup

Checkpoints are created in a temp subdirectory under `RaftDir` (e.g.,
`/var/lib/nufs/raft/checkpoint/`). Deleted after Persist completes.
Stale checkpoints from crashed processes are cleaned on startup.

#### 4. Snapshot size

Checkpoint includes all SST files. Size on disk = actual Pebble data size
(not reduced by zstd dictionary). A `CompactAll()` before snapshot can
reduce size by ~30%.

zstd compression of each SST file achieves comparable ratio to the KV-stream
format (~3-5x on disk) because SST files are already Snappy-compressed by
Pebble. Total snapshot artifact size remains dominated by the raw LSM data.

### Key changes required

**New:**
- `dbCheckpoint()` method on `PebbleStore` that returns a `CheckpointDir`
- Write checkpoint to Raft sink (`archiveCheckpoint`)
- Read checkpoint from Raft sink and restore (`extractCheckpoint`)
- `replaceDB(dir)` to atomically swap the Pebble instance
- File format: `snapshot_format.go` with read/write helpers

**Modified:**
- `PebbleSnapshot.Persist` — switch to checkpoint flow
- `PebbleFSM.Restore` — switch to checkpoint flow, handle both PBL1 and PBL3
- `PebbleStore` — add `dbMu RWMutex`, protect `db` field on read paths

**Removed (superseded):**
- `writeBytesStream` / `readBytesStream` — no longer needed for snapshot
  (still used by Raft log entry encoding, keep those)

---

# Part 2: Per-Bucket Usage Counters

## Problem

`ComputeAllBucketUsage()` scans the entire namespace (`/ns/`) and inode (`/inode/`)
prefixes to compute per-bucket usage. For 100B+ objects this takes minutes/hours
and is too expensive for an admin endpoint.

## Solution

Maintain real-time per-bucket counters (`/bucket-stats/{name}` → `{UsedBytes, Objects}`)
that are updated atomically with each filesystem mutation, via the same Raft batch.
The admin endpoint becomes a single point read per bucket.

## Storage Schema

### Key prefix

`/bucket-stats/` + bucket name → JSON `BucketUsage`

```go
type BucketUsage struct {
    Name      string `json:"name"`
    UsedBytes int64  `json:"used_bytes"`
    Objects   int64  `json:"objects"`
}
```

### InodeMeta extension

```go
type InodeMeta struct {
    // ... existing fields ...
    BucketRoot InodeID `json:"bucket_root,omitempty"`
}
```

`BucketRoot` is populated at creation time by inheriting from the parent inode.
Root inodes (created by `CreateBucket`) set `BucketRoot = self.ID`.

## Mutation → Counter Mapping

All counter updates happen inside `applyBatchJSON`, in the same Raft batch as
the primary mutation. No extra Raft round-trips.

| Operation | Counter Change | Details |
|-----------|---------------|---------|
| CreateBucket | Initialize {0, 0} | Add `OpSet` to batch |
| DeleteBucket | Delete key | Add `OpDelete` to batch |
| CreateFile | Objects++ | Read parent inode to get `BucketRoot` |
| MkDir | (none) | Directories don't count as objects |
| Symlink | Objects++ | Symlink = 1 object, size = len(target) |
| Unlink (NLink→0) | Objects--, UsedBytes -= size | Last hard link removed |
| Unlink (NLink>0) | (none) | Hard link still exists |
| Rename | (none) | Cross-bucket is prohibited (EXDEV) |
| UpdateInode (Size changed) | UsedBytes += delta | Read old inode → compute diff |
| Link (NLink++) | (none) | Hard link ≠ new object |

### Size delta in UpdateInode

Change `UpdateInode` from blind `putJSON` to:

1. Read current inode from Pebble
2. Compute `delta = new.Size - old.Size`
3. If delta != 0, add counter update to the Raft batch
4. Write new inode

## Rename Cross-Bucket Protection

The `Rename` implementation compares parent inodes' `BucketRoot`.

- If different → return `EXDEV` (POSIX cross-device link error).
- S3 gateway and FUSE both handle `EXDEV` by falling back to copy + delete.

## Admin Endpoint Changes

### ComputeAllBucketUsage (server-side)

Replace namespace prefix scan with:

1. `ListBuckets()` → get all bucket names
2. For each name, point read `/bucket-stats/{name}`
3. Return results

### S3 gateway

No change needed — already calls `gw.meta.ComputeAllBucketUsage()`.

## Migration

After deployment, existing installations have no counter keys.

1. Add feature flag `UseBucketStats` (default false) in `PebbleStoreConfig`.
2. Deploy code — new mutations start populating `BucketRoot` and counters.
3. Backfill: admin endpoint triggers a one-time `ComputeAllBucketUsage` scan
   and writes counter keys. Run once on the leader.
4. Flip flag to `true` — admin reads switch from scan to counter lookup.

Remove the flag as a follow-up after all deployments are migrated.

---

# Implementation Order

1. **Per-bucket counters** (easier, immediate benefit for admin API)
   - Add `BucketRoot` to `InodeMeta`
   - Add `/bucket-stats/` prefix + `BucketUsage` type
   - Wire counter updates into `CreateFile`, `Unlink`, `UpdateInode`, etc.
   - Migrate `ComputeAllBucketUsage` to counter reads
   - Migration + feature flag

2. **Checkpoint snapshot** (bigger impact, more complex)
   - Add `dbMu` to `PebbleStore` for safe DB instance swap
   - Implement checkpoint archive format (PBL3)
   - Rewrite `PebbleSnapshot.Persist` with checkpoint flow
   - Rewrite `PebbleFSM.Restore` with extract + rename + reopen flow
   - Backward compatibility for PBL1

---

# Testing

## Snapshot

- Unit: Persist + Restore roundtrip, verify all keys match
- Unit: PBL1 backward compat — restore old-format snapshot
- Unit: Restore with corrupted archive → graceful error
- Integration: trigger Raft snapshot → verify node restarts correctly
- Integration: three-node cluster, trigger snapshot on leader, follower joins
  via snapshot transfer

## Bucket Stats

- Create bucket → verify counter exists with {0, 0}
- Create file → verify Objects = 1
- Create file + Unlink → verify Objects = 0
- Create file + UpdateInode (change Size) → verify UsedBytes = newSize
- Hard link: create file, Link, Unlink twice → objects go to 0 only on last unlink
- Rename across bucket → verify EXDEV
- Backfill → verify counters match scan results
- S3 PutObject → GET /admin/buckets → verify correct count and bytes
