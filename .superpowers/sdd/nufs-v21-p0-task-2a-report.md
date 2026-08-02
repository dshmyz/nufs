# Task 2A Report — Streaming Segment Parser and Safe Tail Truncation

## Status

Implemented Task 2A only. Task 2B budgets/DataReady integration was not
implemented; recovery limits are enforced solely by `RecoverOptions` inside
the parser.

## Approved format decision

The project owner authorized an incompatible on-disk correction because NUFS
is not in production and V2.1 excludes legacy compatibility.

- `storage.FormatVersion` is now **3**.
- V2 segment headers are rejected by the header decoder; V2 record and
  BatchCommit entries are rejected explicitly by recovery.
- Record headers now carry `PayloadChecksum`, included in the header CRC.
- New commits checksum full serialized `journal.BatchDescriptor` values:
  extent ID, generation, segment ID, offset, stored length, logical length,
  and payload checksum.

Consequence: active V2 segment data is intentionally unsupported and must
not be opened by the V3 recovery path.

## RED / GREEN evidence

1. RED: `TestRecoverStreaming_ReplaysCommittedBatchAndTruncatesTail` failed
   before implementation because recovery left the 16-byte uncommitted tail:
   `file size = 170, want truncated 154`.
2. GREEN: the same test passed after the positional parser and truncate/sync
   implementation.
3. RED: `TestRecoverStreaming_EnforcesRecordAndReplayBudgets/replay_bytes`
   failed because a nil apply callback bypassed `MaxReplayBytes` enforcement.
4. GREEN: after moving budget validation outside the callback guard, the
   focused recovery, format, and group-commit tests passed.

## Validation

- `go test ./datanode/storage/segment -run '^(TestRecoverStreaming_|TestRecord|TestCommitLayout_)' -count=1 -timeout 60s`
  passed after the final budget correction.
- `go test -race ./datanode/storage/segment -run 'Recover|Crash|CommitLayout' -count=10 -timeout 120s`
  passed.
- `go test -race ./datanode/storage/segment -count=1 -timeout 180s` passed.

## I/O and memory proof

- `RecoverFromSegmentLog` opens the active file read-write and uses only
  positional `ReadAt` calls plus `Truncate`/`Sync`; it no longer uses
  `os.ReadFile`.
- It reads fixed-size segment/record/commit/trailer buffers and uses a
  bounded current frame-index buffer plus the bounded pending descriptor list
  needed to validate a commit. Descriptor CRC is recomputed with a fixed
  44-byte scratch buffer; no payload frame is read or allocated.
- `TestRecoverStreaming_SparseTailUsesBoundedMemory` creates a sparse 4 GiB
  segment with a small committed prefix, asserts under 8 MiB total allocation
  during recovery, and verifies truncation back to the prefix.

## Files

- `nufs-core/datanode/storage/types.go`
- `nufs-core/datanode/storage/segment/record.go`
- `nufs-core/datanode/storage/segment/recover.go`
- `nufs-core/datanode/storage/segment/store.go`
- `nufs-core/datanode/storage/segment/record_test.go`
- `nufs-core/datanode/storage/segment/commit_layout_test.go`
- `nufs-core/datanode/storage/segment/recover_stream_test.go`
- `.superpowers/sdd/nufs-v21-p0-task-2a-report.md`

## Concerns

- V3 has no V2 fallback by design; opening legacy data returns an explicit
  unsupported-format error rather than attempting recovery or truncation.
- Tombstone commits now write a zero-frame record so their declared one-record
  descriptor can pass the same strict BatchCommit validation as data records.
