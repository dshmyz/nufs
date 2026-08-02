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

## Fix 1 — Independent Review Findings

### Scope

This follow-up fixes all evidence-backed Task 2A review findings without
adding Task 2B DataReady or production recovery-budget plumbing. The V3
format remains version 3: its still-unreleased record and descriptor layouts
now include a durable operation field instead of introducing another format
bump. V2 remains explicitly unsupported with no fallback.

### RED / GREEN evidence

RED command:

```bash
go test ./datanode/storage/segment -run 'TestRecoverStreaming_(SafeOffsetSkipsCommittedPrefix|CorruptTailWithVersion2ByteIsTruncated)|TestRecordTrailerGolden|TestDelete_RecoveredTombstoneRemainsDeleted|TestStoreRecoveryRestoresStreamSequence' -count=1 -timeout 60s
```

Before the fix it failed for all five review regressions:

- `TestRecordTrailerGolden`: corruption in bytes 8..11 was accepted.
- `TestRecoverStreaming_SafeOffsetSkipsCommittedPrefix`: recovery reported two
  commits, proving it parsed and counted the safe prefix.
- `TestRecoverStreaming_CorruptTailWithVersion2ByteIsTruncated`: an invalid
  tail was returned as unsupported V2 solely because byte 4 was 2.
- `TestDelete_CrashReopenTombstoneRemainsDeleted`: recovered delete returned
  an empty live extent rather than `ErrExtentNotFound`.
- `TestStoreRecoveryRestoresStreamSequence`: recovered stream sequence was 0
  after a prior committed sequence of 1.

GREEN evidence:

- The same focused test set passed after the fix.
- `go test ./datanode/storage/segment -count=1 -timeout 180s` passed after
  formatting and updating every V3 layout consumer.
- `go test -race ./datanode/storage/segment -run 'Recover|Crash|CommitLayout|Tombstone|Delete' -count=10 -timeout 180s` passed.
- `go test -race ./datanode/storage/segment -count=1 -timeout 240s` passed.

### Fixes and regressions

- Added `RecordOp` (`RecordPut`, `RecordDelete`) to the V3 record header and
  to `journal.BatchDescriptor`; both header CRC and descriptor CRC cover it.
  Delete now uses the normal group-commit coordinator with a zero-frame
  `RecordDelete`, and recovery maps it to `ExtentTombstoned`.
- `RecoverFromSegmentLog` now begins at `max(SegmentHeaderSize, SafeOffset)`,
  validates a non-header checkpoint start, initializes its valid boundary at
  that offset, and never parses/counts/replays the trusted prefix or truncates
  below it.
- Parser record accounting now has a hard 100,000-record bound even when
  `MaxRecords` is zero; explicit smaller limits remain effective.
- V2 record classification now requires a valid legacy magic and header CRC.
  Invalid/torn tails with a version-like fifth byte are truncated and synced;
  genuine V2 remains an explicit unsupported-format error.
- Active recovery restores `streamSeq` from `RecoverResult.LastSeq`, so the
  next append continues the stream-local commit sequence.
- The 12-byte trailer writes deterministic zero reserved bytes and rejects
  nonzero bytes 8..11.

Regression coverage was added or extended in:

- `nufs-core/datanode/storage/segment/recover_stream_test.go`
- `nufs-core/datanode/storage/segment/store_test.go`
- `nufs-core/datanode/storage/segment/record_test.go`
- `nufs-core/datanode/storage/segment/commit_layout_test.go`

### Files

- `nufs-core/datanode/storage/journal/commit.go`
- `nufs-core/datanode/storage/segment/record.go`
- `nufs-core/datanode/storage/segment/recover.go`
- `nufs-core/datanode/storage/segment/store.go`
- the regression test files listed above

### Concerns

- This deliberately changes the V3 record and descriptor layout while V3 is
  still the approved incompatible V2.1 correction. Existing V3 test data is
  not compatible, but no additional format-version bump was introduced.
- The parser bound is local to `RecoverFromSegmentLog`; Task 2B remains
  responsible for production integration budgets and DataReady state.

## Fix Round 2 — Recovery Semantics, Checkpoint Boundaries, and Delete Cache

### Scope

This focused follow-up addresses the second independent review only. It keeps
the V3-only format, positional streaming parser, bounded memory, strict
validate-before-apply ordering, and Task 2B/DataReady exclusion intact.

### Fixes

- Recovery now keeps `lastCommittedSeq` as structural monotonic-validation
  state. It advances for every valid `BatchCommit`, including a commit at or
  below `SafeSeq`; `SafeSeq` now controls replay suppression only. A covered
  committed batch remains validated, counted, preserved, and untruncated.
- A non-header `SafeOffset` is now accepted only when the fixed-size structure
  immediately before it is a CRC-valid, coherent V3 `BatchCommit` for the
  stream (ending exactly at the offset), or a valid `INDEX_SAFE` marker.
  Recovery begins its actual parse/count/replay at that offset and seeds
  structural sequence state from the proven boundary.
- The checkpoint trust invariant is explicit: producers may publish
  `SafeOffset` only after a durable `BatchCommit` or `INDEX_SAFE`. Recovery
  does not parse the trusted prefix and does not treat bytes that merely look
  like a record header as a boundary. It reads only the one fixed-size
  candidate commit/marker before the checkpoint, never scans or loads payload
  frames. Consequently the trusted prefix's descriptors are not revalidated
  at checkpoint time; their earlier durable commit is the format authority.
- A durable delete now publishes its tombstone into `locCache` alongside the
  overlay update. Since `cachedLookup` checks this cache first, a previous
  live cache entry cannot remain visible after delete acknowledgement.
- The V3 record golden test now checks all 55 encoded bytes, `RecordFraming`
  expects the 55-byte header, and recovery fixtures include `RecordPut` in
  their expected descriptor.

### RED / GREEN evidence

Before the production changes, the focused RED command failed for the exact
review regressions: SafeSeq treated a valid commit as non-monotonic and
truncated it, a crafted valid header inside payload was accepted as a safe
checkpoint, and a read-populated location cache returned live data after an
acknowledged delete.

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core && go test ./datanode/storage/segment -run 'TestRecoverStreaming_(SafeSeqValidatesWithoutReplay|RejectsSafeOffsetInsidePayloadWithCraftedHeader)|TestRecord(HeaderGolden|FramingV3)|TestDelete_EvictsLiveLocationCacheAndSurvivesReopen' -count=1 -timeout 60s
# FAIL: SafeSeq, crafted-SafeOffset, and cached-delete regressions
```

Reviewer-reproduced focused GREEN command (including recovery fixture and
record-layout coverage):

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core && go test ./datanode/storage/segment -run 'TestRecoverStreaming_(ReplaysCommittedBatchAndTruncatesTail|SafeSeqValidatesWithoutReplay|RejectsSafeOffsetInsidePayloadWithCraftedHeader|SafeOffsetSkipsCommittedPrefix)|TestRecord(HeaderGolden|TrailerGolden|FramingV3)|TestDelete_(EvictsLiveLocationCacheAndSurvivesReopen|CrashReopenTombstoneRemainsDeleted)' -count=1 -timeout 60s
# ok github.com/example/dfs/datanode/storage/segment 1.658s
```

Required race verification:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core && go test -race ./datanode/storage/segment -run 'Recover|Crash|CommitLayout|Tombstone|Delete|SafeSeq|SafeOffset' -count=10 -timeout 180s
# PASS (exit 0)

cd /Users/gracegaoya/work/project/nufs/nufs-core && go test -race ./datanode/storage/segment -count=1 -timeout 240s
# PASS (exit 0)
```

### Files

- `nufs-core/datanode/storage/segment/recover.go`
- `nufs-core/datanode/storage/segment/recover_stream_test.go`
- `nufs-core/datanode/storage/segment/store.go`
- `nufs-core/datanode/storage/segment/store_test.go`
- `nufs-core/datanode/storage/segment/record_test.go`
- `.superpowers/sdd/nufs-v21-p0-task-2a-report.md`
- `.superpowers/sdd/nufs-v21-p0-task-2a-fix2-report.md`

### Concerns

- A checkpoint is necessarily a trust boundary: validating its preceding
  commit's CRC and coherence proves its encoded boundary without rescanning
  the trusted prefix, but cannot recompute descriptor CRCs for that prefix.
- Task 2B recovery-budget integration and DataReady state remain out of
  scope. V2 remains unsupported, and no payload frames are read by recovery.
