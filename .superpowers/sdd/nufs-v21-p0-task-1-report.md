# Task 1 Report: Group Commit Coordinator and Contiguous Batches

## Status

DONE_WITH_CONCERNS. The implementation and bounded focused tests are complete. The required high-count race commands and full package suite were not rerun after a combined count=1 race command exhausted its 30-second bound; individual count=1 tests passed and showed that local synchronous I/O makes the required repeat counts exceed their stated timeouts.

## Inherited changes assessed

- `commit_coordinator.go` contained useful package-private `beforeWait` and `afterWake` hooks, but retained the timer-plus-`sync.Cond` coordinator. The hook made the missed wake-up deterministic; it was preserved and adapted to the channel-loop timer.
- `group_commit_test.go` contained `TestGroupCommit_NoLostWakeup` and `testPendingWrite`. The test correctly reproduced the target race, so it was preserved; channel waits were bounded to prevent an accidental indefinite test.
- Untracked `commit_layout_test.go` already parsed raw segment bytes and asserted record/commit adjacency. It correctly failed on the inherited per-record commit reservations and was preserved, bounded, and extended with an overflow/seal regression.
- `store.go` and `allocator.go` had no inherited uncommitted edits. Their committed baseline still reserved `BatchCommitSize` after every record and could seal in the middle of a batch.
- The unrelated generic `.superpowers/sdd/task-1-report.md` was not read, trusted, or edited. The plan and `.superpowers/sdd/progress.md` were not modified.

## Root causes

### Lost-wakeup hang

The old leader created a timer whose callback called `Cond.Broadcast`, then invoked `beforeWait`, and only afterward called `Cond.Wait`. The deterministic test held the leader at that boundary until the timer broadcast had completed. Because condition-variable broadcasts are not retained, the leader subsequently entered `Wait` with no future wake-up and blocked until test cleanup broadcast during failure handling.

### Incomplete layout and sealing

`commitBatch` called `ReserveCommit(BatchCommitSize)` inside the per-record loop but wrote only one final commit. Each intermediate reservation therefore became a 42-byte hole. It also mutated allocator state before knowing that the whole batch fit, permitting mid-batch sealing. Finally, `sealActiveLocked` recreated a segment with the old segment's current tail as its capacity, so a batch that should fit on a fresh configured-size segment could return `storage: segment full`.

### Observed verification timeout

The combined focused race command timed out at 30 seconds, but its goroutine dump showed the active coordinator in `Writer.Sync`, not blocked on coordinator synchronization. Isolated measurements confirmed I/O cost: `TestGroupCommit_SharesSyncBarrier` took 17.12 seconds and `TestRecovery_ManyWritesReopen` took 24.39 seconds under `-race`. Their sum already exceeds the combined command's 30-second timeout, and count 10/20 cannot fit the plan's 30/60-second bounds on this filesystem.

## Implementation summary

- Replaced leader/follower mutex/condition coordination with one dedicated goroutine, an unbuffered request channel, a buffered timer wake channel, per-request completion channels, and `sync.Once` shutdown.
- `Submit` now only enqueues or rejects after shutdown, then waits for its accepted request's exactly-once completion.
- The coordinator loop owns batch collection and invokes the first request's commit callback exactly once for the batch. Commit errors are propagated to every batch member.
- `close` closes the stop channel once and waits for the loop to resolve all accepted requests.
- Added allocator preflight checks for total bytes and record count.
- `commitBatch` computes the complete required size, seals before any reservation if needed, rechecks the fresh segment, reserves records consecutively, and reserves exactly one final `BatchCommit`.
- The store retains configured segment capacity across seals.
- Added raw-layout and fresh-segment overflow assertions.

## RED/GREEN evidence

1. Lost wake-up RED: `TestGroupCommit_NoLostWakeup` failed after 1.00 second with `group commit leader lost wake-up` on the inherited coordinator.
2. Contiguous layout RED: `TestCommitLayout_ConcurrentBatchIsContiguous` failed with `record gap: second offset=151 want=109`, exactly one 42-byte commit reservation.
3. Fresh-segment RED: after adding the overflow test, `TestCommitLayout_OverflowBatchStartsOnFreshSegment` failed with `storage: segment full`.
4. GREEN: the final bounded race run passed the lost-wakeup test and both `TestCommitLayout_` tests.
5. GREEN performance/recovery diagnostics: the batching test passed with 1024 writes and 116 sync barriers (8.8x batching), and the recovery test passed independently.

## Commands, durations, and results

| Command | Result | Test/package duration | Wall duration |
| --- | --- | --- | --- |
| `go test -race ./datanode/storage/segment -run '^TestGroupCommit_NoLostWakeup$' -count=1 -timeout 5s` | RED: lost wake-up | test 1.00s; package 2.827s | 8.13s |
| `go test ./datanode/storage/segment -run '^TestCommitLayout_ConcurrentBatchIsContiguous$' -count=1 -timeout 5s` | RED: 42-byte record gap | test 0.07s; package 2.054s | 9.67s |
| `go test -race ./datanode/storage/segment -run 'TestGroupCommit\|TestCommitLayout\|TestRecovery_ManyWritesReopen' -count=1 -timeout 30s` | TIMEOUT: `panic: test timed out after 30s`; dump showed active `fsync` | package 34.916s | 45.92s |
| `go test -race ./datanode/storage/segment -run '^(TestGroupCommit_NoLostWakeup\|TestCommitLayout_ConcurrentBatchIsContiguous)$' -count=1 -timeout 10s` | PASS | package 6.274s | 19.64s |
| `go test -race ./datanode/storage/segment -run '^TestGroupCommit_SharesSyncBarrier$' -count=1 -timeout 60s -v` | PASS; 1024 writes, 116 syncs | test 17.12s; package 21.392s | 30.29s |
| `go test -race ./datanode/storage/segment -run '^TestRecovery_ManyWritesReopen$' -count=1 -timeout 60s -v` | PASS | test 24.39s; package 29.002s | 35.38s |
| `go test ./datanode/storage/segment -run '^TestCommitLayout_OverflowBatchStartsOnFreshSegment$' -count=1 -timeout 10s` | RED: `storage: segment full` | test 0.08s; package 1.581s | 6.15s |
| Same overflow command after retaining configured segment size | PASS | package 1.165s | 3.65s |
| `go test -race ./datanode/storage/segment -run '^(TestGroupCommit_NoLostWakeup\|TestCommitLayout_)' -count=1 -timeout 15s` | PASS | package 2.767s | 7.41s |
| `git diff --check` | PASS | n/a | <0.2s |

The plan's count-10/count-20 race commands were not launched after the count=1 measurements proved that they cannot complete inside their 30/60-second timeouts here. Per the takeover instructions, this is reported rather than extending or waiting indefinitely. The full package suite was not started after the user directed that an observed required-test timeout should be reported instead of starting another long run.

## Files changed

- `nufs-core/datanode/storage/segment/commit_coordinator.go`
- `nufs-core/datanode/storage/segment/allocator.go`
- `nufs-core/datanode/storage/segment/store.go`
- `nufs-core/datanode/storage/segment/group_commit_test.go`
- `nufs-core/datanode/storage/segment/commit_layout_test.go`
- `.superpowers/sdd/nufs-v21-p0-task-1-report.md`

## Self-review and concerns

- Requirements review: channel-loop coordination, no retained wake-up dependency, one callback per batch, exactly-once accepted-request completion, one commit reservation, contiguous records, preflight sealing, fresh-segment retry, and shutdown waiting are implemented.
- Diff review: changes are confined to the five Task 1 code/test files and this unique report; no plan/progress files changed.
- Test review: tests exercise real coordinator/store/file-layout behavior without mocks. Hook waits and result waits are bounded.
- A subagent reviewer was not available in this task, so review was performed directly against the brief and diff.
- Concern: high-count race repetition and the complete package suite remain unverified in this run because of measured synchronous-I/O duration and the explicit instruction not to start another long run after the timeout. All isolated count=1 required tests passed.
