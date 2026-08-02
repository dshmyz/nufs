# NUFS V2.1 P0 Task 2B Fix Round 3 Report

## Scope

Focused cleanup from clean HEAD `e077309` on `codex/v21-p0-hardening`.
No Task 3 or architectural work was included.

## RED evidence

The new tests were added before production changes. The segment test command
failed to build because the requested private seams and default-limit helper
did not exist:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./datanode/storage/segment -run 'ZeroByteLimits|StatFailure|DeadlineLeaves|ResultBecomes' -count=1
# FAIL: Config.recoveryClock, recoveryByteLimits, and recoveryStat undefined
```

The checkpoint short-write test then failed against the old implementation:

```bash
go test ./datanode/storage/index -run 'ShortWrite' -count=1
# FAIL: error = <nil>, want io.ErrShortWrite
```

## Implementation

- Removed exported recovery clock/deadline injection from public configuration
  and parser options; same-package tests use lowercase fields.
- Defaulted zero replay/trailing parser limits to
  `storage.MaxRecoveryReplayBytes` (256 MiB) and
  `storage.MaxRecoveryTrailingBytes` (128 MiB).
- Required full checkpoint writes before sync/rename, returning
  `io.ErrShortWrite` and retaining deferred temporary-file cleanup on short
  writes.
- Replaced `fileExists` with a package-private stat seam that treats only
  `os.IsNotExist` as absence and propagates all other errors through
  `Store.New`.
- Made the deterministic corruption drill fail on any successful read after
  its known payload-byte corruption.

Focused green evidence:

```bash
go test ./datanode/storage/segment ./datanode/storage/index -run 'ZeroByteLimits|StatFailure|ShortWrite|DeadlineLeaves|ResultBecomes|CorruptRead' -count=1
# PASS: segment 3.748s; index 1.665s
```

## Final verification

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test -race ./datanode/storage/segment ./datanode/storage/recovery -run 'Recover|Budget|DataReady|Checkpoint|RepeatedCrash|Drill|ShortWrite|Stat' -count=10 -timeout 180s
# PASS: segment 102.262s; recovery 4.053s

go test -race ./datanode/storage/segment ./datanode/storage/recovery ./datanode/storage/index -count=1 -timeout 240s
# PASS: segment 138.532s; recovery 2.454s; index 6.170s

cd /Users/gracegaoya/work/project/nufs
git diff --check
# PASS
```

## Files

- `nufs-core/datanode/storage/segment/recover.go`
- `nufs-core/datanode/storage/segment/store.go`
- `nufs-core/datanode/storage/segment/recovery_budget_test.go`
- `nufs-core/datanode/storage/segment/recovery_fix1_test.go`
- `nufs-core/datanode/storage/segment/recover_stream_test.go`
- `nufs-core/datanode/storage/segment/drill_test.go`
- `nufs-core/datanode/storage/index/recovery_checkpoint.go`
- `nufs-core/datanode/storage/index/recovery_checkpoint_test.go`
- `.superpowers/sdd/nufs-v21-p0-task-2b-report.md`
- `.superpowers/sdd/nufs-v21-p0-task-2b-fix3-report.md`

## Concerns

The package-private timing and stat seams are mutable globals/fields intended
only for same-package tests. The tests that replace them do not run in
parallel and restore the original values with cleanup. No production caller
can set these seams.
