# NUFS V2.1 P0 Task 2B Fix Round 2 Report

## Scope

Focused follow-up to `8cfbb84` on `codex/v21-p0-hardening`. This round fixes
the repeated-crash window where startup replayed an acknowledged suffix only
to memory, then created a higher empty active segment before that suffix was
made durable in the derived index.

## RED

Added these tests before the implementation:

- `TestStoreRecovery_RepeatedCrashPersistsRecoveredSuffixBeforeNewSegment`
- `TestStoreRecovery_RecordLimitBoundaryUsesStoreStartup`
- `TestStoreRecoveryCheckpoint_CleansTemporaryFileOnFailure`

The focused RED command failed to build as expected because the required test
seams did not exist:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./datanode/storage/segment ./datanode/storage/index -run 'RepeatedCrash|RecordLimitBoundary|CleansTemporary' -count=1
# FAIL: Config.disableAsyncApply and index.recoveryCheckpointFileOps undefined
```

## GREEN

`Store.New` now calls `persistRecoveredOverlay` after parser replay and before
`newActiveSegment`. When replay populated the overlay, it opens the recovered
segment at its recovered tail and reuses `flush`/`writeIndexSafe` plus
`StoreRecoveryCheckpoint` to durably publish the same ordered boundary used
by normal flushing. The recovery clock is checked before and after this work;
failure returns no Store before `DataReady`.

The sidecar writer has package-level file-operation seams for deterministic
write/sync/close/rename failures. Its deferred cleanup closes any remaining
temporary descriptor, joins a relevant close error with the original error,
and best-effort removes the temporary sidecar unless rename completed.

The exact/+1 Store.New record limit test uses the injected small policy
(`maxRecords: 2`); exact is DataReady and +1 returns
`storage.ErrRecoveryBudgetExceeded` with a nil Store. Production policy still
defaults to 100,000 records.

The drill no longer accepts arbitrary `Store.New` errors: it corrupts a
known payload byte in deterministic incompressible data, so a startup error is
reported as an unrelated regression.

## Verification

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./datanode/storage/segment ./datanode/storage/index -run 'RepeatedCrash|RecordLimitBoundary|CleansTemporary|Drill_CorruptRead' -count=1
# PASS: segment 3.450s; index 2.015s

go test ./datanode/storage/segment ./datanode/storage/recovery ./datanode/storage/index -count=1 -timeout 180s
# PASS (exit 0)

go test -race ./datanode/storage/segment ./datanode/storage/recovery -run 'Recover|Budget|DataReady|Checkpoint|RepeatedCrash|Drill' -count=10 -timeout 180s
# PASS (exit 0)

go test -race ./datanode/storage/segment ./datanode/storage/recovery -count=1 -timeout 240s
# PASS (exit 0)

go test -race ./datanode/storage/index -run 'RecoveryCheckpoint' -count=10 -timeout 120s
# PASS (exit 0)

cd /Users/gracegaoya/work/project/nufs
git diff --check
# PASS
```

## Files

- `nufs-core/datanode/storage/segment/store.go`
- `nufs-core/datanode/storage/segment/recovery_fix1_test.go`
- `nufs-core/datanode/storage/segment/drill_test.go`
- `nufs-core/datanode/storage/index/recovery_checkpoint.go`
- `nufs-core/datanode/storage/index/recovery_checkpoint_test.go`
- `.superpowers/sdd/nufs-v21-p0-task-2b-report.md`
- `.superpowers/sdd/nufs-v21-p0-task-2b-fix2-report.md`

## Concerns

The deterministic crash seam deliberately leaves background goroutines as a
model of process termination; its configurations disable asynchronous apply
and delay periodic flushing so they cannot access the closed resources during
the test. It is package-private and unavailable to production callers.
