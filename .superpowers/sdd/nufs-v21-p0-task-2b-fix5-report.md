# NUFS V2.1 P0 Task 2B Fix Round 5 Report

## Scope

Critical flush/checkpoint exclusion hardening from clean HEAD `3c0da8e` on
`codex/v21-p0-hardening`.

## RED

The deterministic regression was written before its package-private seams and
the synchronization implementation:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./datanode/storage/segment -run 'TestFlush_(CheckpointExcludesCommitPublication|ErrorReleasesCheckpointTransaction)' -count=1 -timeout 60s
```

Exact result: exit 1, build failed because `Config.flushCheckpointHook`,
`Config.flushApply`, and `Store.flushApply` were undefined. The refined
barrier then also failed as intended with `Store.beforeCommitLock undefined`.
These failures established the test seams before the production
synchronization was added.

## GREEN

After the implementation, the focused regression passed:

```bash
go test ./datanode/storage/segment -run 'TestFlush_CheckpointExcludesCommitPublication' -count=1 -timeout 60s
# ok github.com/example/dfs/datanode/storage/segment (exit 0)

go test ./datanode/storage/segment -run 'TestFlush_(CheckpointExcludesCommitPublication|ErrorReleasesCheckpointTransaction)' -count=1 -timeout 60s
# ok github.com/example/dfs/datanode/storage/segment (exit 0)
```

## Implementation

- `s.mu` now covers the complete checkpoint transaction: overlay drain and
  snapshot, Pebble apply/flush, `INDEX_SAFE` append+sync, atomic recovery
  sidecar publication, and pending-counter reconciliation.
- `commitBatch` publishes every normal write and delete mutation to the overlay
  and advances the contiguous flush watermark while it still holds `s.mu`.
  `AppendRecord` now observes the same append-through-publication exclusion.
- The only nested order is `s.mu -> publicationMu`; checkpoint code does not
  take `publicationMu`. This prevents lock inversion while preserving Task 1
  group batching and one-sync batch semantics outside a periodic flush.
- Successful checkpoints subtract their snapshotted pending count, never reset
  it. Every failure after drain restores the drained overlay, retains the
  pending count, and returns without publishing a newer sidecar.
- `TestFlush_CheckpointExcludesCommitPublication` pauses the checkpoint at the
  former unsafe boundary, releases a concurrent commit to contend explicitly
  at its pre-lock barrier, proves it remains unacknowledged, completes the
  checkpoint, verifies the post-checkpoint counter, then crashes and reopens
  to validate one-record suffix replay and both acknowledged values.
  `TestFlush_ErrorReleasesCheckpointTransaction` injects a flush apply error
  and proves no lock leak/deadlock or overlay loss.

## Final verification

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test -race ./datanode/storage/segment -run 'Flush|Checkpoint|Concurrent|Recovery|RepeatedCrash' -count=20 -timeout 180s
# exit 0

go test -race ./datanode/storage/segment ./datanode/storage/recovery ./datanode/storage/index -count=1 -timeout 240s
# exit 0
```

`git diff --check` was also run after final report edits and returned exit 0.

## Concerns

The checkpoint lock deliberately serializes group commits only for the duration
of a periodic flush, including Pebble and sidecar durability work. This is the
required correctness boundary; normal group collection, batching, and its
single-fsync behavior are otherwise unchanged.
