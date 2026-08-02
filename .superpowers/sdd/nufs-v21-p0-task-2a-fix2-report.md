# NUFS V2.1 Task 2A — Fix Round 2 Report

## Scope

Fixed the second independent re-review findings in Task 2A only. The change
does not add Task 2B DataReady integration or production recovery-budget
plumbing; V2 remains unsupported.

## Fixes

- `SafeSeq` no longer initializes recovery's sequence-monotonicity state.
  `lastCommittedSeq` is structural validation state, so every valid committed
  batch advances it and contributes to `Commits`, `LastSeq`, and the valid
  truncation boundary. Batches at or below `SafeSeq` are validated but not
  applied.
- `SafeOffset` no longer trusts a record-looking byte sequence at the
  checkpoint. Other than the segment-header start, it must immediately follow
  a CRC-valid/coherent V3 `BatchCommit` for the requested stream or a valid
  `INDEX_SAFE` marker. The parser then starts actual parse/count/replay at the
  checkpoint and seeds sequence validation from that boundary.
- This checkpoint rule is a format trust invariant: a producer publishes
  `SafeOffset` only after durable commit metadata. Validation reads one
  fixed-size candidate commit/marker before the offset and never scans or
  loads payload frames; it deliberately does not rescan the trusted prefix to
  recompute its descriptor CRC.
- Acknowledged deletes now replace a live `locCache` entry with the tombstone
  published to the overlay, so `cachedLookup` cannot return stale data after
  acknowledgement.
- V3 fixture/golden coverage now includes `RecordPut`, exact 55-byte record
  header bytes, and the correct 55-byte framing expectation.

## RED / GREEN evidence

RED command run before the production changes:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core && go test ./datanode/storage/segment -run 'TestRecoverStreaming_(SafeSeqValidatesWithoutReplay|RejectsSafeOffsetInsidePayloadWithCraftedHeader)|TestRecord(HeaderGolden|FramingV3)|TestDelete_EvictsLiveLocationCacheAndSurvivesReopen' -count=1 -timeout 60s
# FAIL: SafeSeq, crafted-SafeOffset, and cached-delete regressions
```

Reviewer-focused GREEN command:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core && go test ./datanode/storage/segment -run 'TestRecoverStreaming_(ReplaysCommittedBatchAndTruncatesTail|SafeSeqValidatesWithoutReplay|RejectsSafeOffsetInsidePayloadWithCraftedHeader|SafeOffsetSkipsCommittedPrefix)|TestRecord(HeaderGolden|TrailerGolden|FramingV3)|TestDelete_(EvictsLiveLocationCacheAndSurvivesReopen|CrashReopenTombstoneRemainsDeleted)' -count=1 -timeout 60s
# ok github.com/example/dfs/datanode/storage/segment 1.658s
```

Required race runs:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core && go test -race ./datanode/storage/segment -run 'Recover|Crash|CommitLayout|Tombstone|Delete|SafeSeq|SafeOffset' -count=10 -timeout 180s
# PASS (exit 0)

cd /Users/gracegaoya/work/project/nufs/nufs-core && go test -race ./datanode/storage/segment -count=1 -timeout 240s
# PASS (exit 0)
```

## Files

- `nufs-core/datanode/storage/segment/recover.go`
- `nufs-core/datanode/storage/segment/recover_stream_test.go`
- `nufs-core/datanode/storage/segment/store.go`
- `nufs-core/datanode/storage/segment/store_test.go`
- `nufs-core/datanode/storage/segment/record_test.go`
- `.superpowers/sdd/nufs-v21-p0-task-2a-report.md`
- `.superpowers/sdd/nufs-v21-p0-task-2a-fix2-report.md`

## Concerns

The checkpoint proof validates durable boundary metadata but, by design,
does not revalidate the trusted prefix. This is required to keep recovery
streaming/bounded and avoid payload reads. Task 2B remains out of scope.
