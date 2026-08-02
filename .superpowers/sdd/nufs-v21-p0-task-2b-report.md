# NUFS V2.1 P0 Task 2B Report — Recovery Budgets and DataReady

## Status

Implemented Task 2B only on `codex/v21-p0-hardening`, based on
`4095ce5`. The Task 2A streaming parser remains the only segment-log parser;
this task wires it into `segment.Store.New` with production recovery limits,
an elapsed deadline, and actual startup recovery results.

## RED evidence

The focused Task 2B tests were added before production changes and run with:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./datanode/storage/segment -run 'TestRecoverBudget|TestStoreRecovery_ResultBecomesDataReadyOnlyAfterSuccessfulReplay' -count=1 -timeout 120s
```

They failed to build as intended because the requested behavior did not yet
exist: `storage.MaxRecovery*`, `storage.ErrRecoveryBudgetExceeded`, recovery
`DataReady`/`Duration`, `RecoverOptions.Clock`/`Deadline`, and Store
`DataReady`/`RecoveryResult` were undefined. Existing `Store.New` also passed
no replay/trailing limits or deadline to the parser and did not retain its
actual result.

## GREEN evidence

After the implementation, the focused suite passed:

```bash
go test ./datanode/storage/segment -run 'TestRecoverBudget|TestStoreRecovery_(ResultBecomesDataReadyOnlyAfterSuccessfulReplay|DeadlineDoesNotReturnUsableStore)' -count=1 -timeout 120s
# ok github.com/example/dfs/datanode/storage/segment
```

It covers exact-boundary success and boundary+1 failure for:

- 100,000 committed records;
- 256 MiB sparse replay payload bytes;
- 128 MiB sparse trailing bytes.

It also covers an injected no-sleep elapsed deadline breach, failed recovery
returning `storage.ErrRecoveryBudgetExceeded` with `DataReady=false`, and
`Store.New` returning no usable Store on that failure. Successful startup
checks the parser's actual `Applied`, `LastSeq`, `SafeSeq`,
`LastCommittedOffset`, `TrailingBytes`, `Duration`, and `DataReady` after an
`INDEX_SAFE` marker plus tail truncation.

Required final verification:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test -race ./datanode/storage/segment ./datanode/storage/recovery -run 'Recover|Budget|DataReady' -count=10 -timeout 120s
# PASS (exit 0)

go test -race ./datanode/storage/segment ./datanode/storage/recovery -count=1 -timeout 180s
# PASS (exit 0)
```

An additional complete non-race package run passed before the final race
verification:

```bash
go test ./datanode/storage/segment ./datanode/storage/recovery -count=1 -timeout 180s
# PASS (exit 0)
```

## Implementation

- Canonical recovery policy now lives in `storage`: 100,000 records, 256 MiB
  replay, 128 MiB trailing tail, 30 seconds, and the canonical
  `storage.ErrRecoveryBudgetExceeded` sentinel. The recovery package retains
  aliases for its published constants; the segment package retains its
  Task 2A sentinel alias. This avoids a segment/recovery import cycle.
- `Store.New` supplies all four limits to the existing
  `RecoverFromSegmentLog` call. It starts worker loops only after recovery and
  active-writer setup succeed, returns `nil, err` for a failed recovery, and
  sets `DataReady` only immediately before returning the usable Store.
- `RecoverOptions` now carries a clock/deadline seam. `Store.New` starts the
  30-second timer before opening the index, and the parser checks that shared
  deadline while streaming entries, while assessing/applying a batch, and
  after truncation/sync. Deadline and budget failures return a non-nil actual
  `RecoverResult` with `DataReady=false` and the canonical sentinel.
- `RecoverResult` now reports `Duration` and `DataReady`; `Store.DataReady()`
  and `Store.RecoveryResult()` expose the successful startup state and actual
  segment replay/truncation measurements.

## Files

- `nufs-core/datanode/storage/types.go`
- `nufs-core/datanode/storage/recovery/recovery.go`
- `nufs-core/datanode/storage/segment/recover.go`
- `nufs-core/datanode/storage/segment/store.go`
- `nufs-core/datanode/storage/segment/recovery_budget_test.go`
- `.superpowers/sdd/nufs-v21-p0-task-2b-report.md`

## Concerns

- `recovery.Recover` remains the pre-store superblock/manifest/checkpoint
  validation phase documented by Task 2A. The serving-path result is exposed
  by `segment.Store.RecoveryResult()` because only `Store.New` owns the
  parser callback and replay overlay; no second parser was introduced.
- A deadline can occur after a valid tail has been truncated and synced. In
  that case recovery still returns the budget error and `DataReady=false`, so
  the node never serves after missing the elapsed startup budget.

## Fix Round 1

The interrupted follow-up added the index-owned per-stream recovery
checkpoint sidecar and repaired the Task 2B verification path. The sidecar
contains version, stream ID, segment ID, safe offset, safe sequence, and CRC;
it is atomically published after the synced `INDEX_SAFE` marker with a synced
temporary file, rename, and parent-directory sync. `Store.New` trusts it only
when it names the active segment, otherwise replays from the header; malformed
or CRC-invalid sidecars fail closed.

Recovery now accounts for complete committed on-disk bytes (record framing,
`BatchCommit`, and replayed `INDEX_SAFE` metadata), exposes `SafeOffset` and
`SafeSeq`, and measures duration through active-writer setup and the final
`DataReady` transition. The package-private startup-policy seam keeps
production limits unchanged while allowing small deterministic Store.New
boundary tests. The production-log helper keeps every generated batch at or
below `MaxBatch` (256); its delete records have zero payload and therefore
remain within the 16 MiB extent limit.

The first required focused race command initially timed out because
`TestSustained_Recovery500Writes` accidentally matched `Recover` despite its
comment saying it should not. It was renamed to
`TestSustained_Reopen500Writes`; the test remains in the full suite. A full
race run then exposed `TestDrill_CorruptReadNeverSucceeds`: its bit flip can
damage the `INDEX_SAFE` marker certified by the sidecar. The drill now treats
fail-closed startup as a successful corrupt-data rejection.

Final verification:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test -race ./datanode/storage/segment ./datanode/storage/recovery -run 'Recover|Budget|DataReady|Checkpoint' -count=10 -timeout 180s
# PASS: segment 83.270s; recovery 2.864s

go test -race ./datanode/storage/segment ./datanode/storage/recovery -count=1 -timeout 240s
# PASS: segment 145.172s; recovery 3.881s

cd /Users/gracegaoya/work/project/nufs
git diff --check
# PASS
```

## Fix Round 2

Fixed the repeated-crash data-loss window in startup ordering. If recovery
replays acknowledged records into the overlay, `Store.New` now reopens that
recovered segment long enough to run the existing flush protocol
synchronously: apply overlay mutations to the derived index, flush Pebble,
append and sync `INDEX_SAFE`, and atomically publish the recovery sidecar.
Only after this durable handoff succeeds does startup create a higher-numbered
active segment or transition to `DataReady`. The handoff checks the shared
startup deadline before and after the durable work and returns the canonical
budget error with no usable Store when it is exceeded.

The new deterministic repeated-crash test disables only asynchronous index
apply and uses a one-hour periodic interval; it does not sleep. It verifies
the recovered sidecar certifies segment 1 before active segment 2, crashes
immediately, then proves a second reopen on active segment 3 can still read
the acknowledged suffix and is `DataReady`.

Also added Store.New record-limit exact/+1 coverage through the injectable
startup policy with production-valid delete batches (two records, below the
256-record batch maximum and zero-byte extents). Production defaults remain
100,000 records. The corruption drill now deterministically flips a known
uncompressed payload byte and fails on any unrelated startup error. Sidecar
publication now cleans the temporary file after write, sync, close, and rename
failures, and preserves close errors; injected tests cover each path.

Final verification:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
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

## Fix Round 3

Narrow final cleanup on `codex/v21-p0-hardening` from clean HEAD `e077309`.
Production `segment.Config` no longer exposes a recovery clock; the parser's
clock, deadline, and start-time injection fields are package-private too, so
external callers always use wall time and the fixed 30-second storage budget.

Zero `RecoverOptions.MaxReplayBytes` and `MaxTrailingBytes` now resolve to
the canonical 256 MiB and 128 MiB limits. The 4 GiB sparse bounded-I/O test
passes its exceptional trailing allowance explicitly. New direct limit tests
verify the zero-option values without allocating a boundary-sized fixture.

Recovery checkpoint publication now returns `io.ErrShortWrite` if a writer
reports `n != len(data)` with no error; the temporary file is removed and no
rename is attempted. Active-segment probing now fails closed: only
`os.IsNotExist` is a fresh-disk condition, while stat permission/I/O errors
make `Store.New` return a nil Store. The corruption drill now fails if a
corrupted read succeeds even with unchanged data.

The existing trailing-boundary assertion now accounts for the already-required
durable recovered-overlay handoff, which appends a synced `INDEX_SAFE` marker
after truncation.

Final verification:

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

## Fix Round 4

Hardened the exported `segment.RecoverOptions` byte-budget inputs. Effective
replay and trailing limits now clamp every nonpositive or over-cap value to
the canonical storage limits: 256 MiB replay and 128 MiB trailing. Positive
values within those caps remain available for tighter caller policy and
tests, but no exported caller can bypass either production cap.

Focused table coverage exercises zero, negative, exact-cap, below-cap, and
above-cap values for both byte limits. A recovery-level sparse-tail test
proves a negative trailing limit cannot disable the canonical budget. The
existing sparse 4 GiB test now confirms bounded allocation while returning
the expected canonical budget error and leaving the oversized tail intact.

Final verification:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test -race ./datanode/storage/segment ./datanode/storage/recovery -run 'Recover|Budget|DataReady|Checkpoint' -count=10 -timeout 180s
# PASS (exit 0)

go test -race ./datanode/storage/segment ./datanode/storage/recovery ./datanode/storage/index -count=1 -timeout 240s
# PASS (exit 0)

cd /Users/gracegaoya/work/project/nufs
git diff --check
# PASS
```

## Fix Round 5

Fixed the critical flush/checkpoint race in the active segment. `s.mu` is now
the checkpoint transaction lock: it excludes a batch's append, sync, overlay
publication, and flush-watermark publication from overlay drain through Pebble
durability, `INDEX_SAFE` append+sync, recovery-sidecar publication, and pending
counter reconciliation. The documented nested lock order is `mu` then
`publicationMu`; checkpoints take only `mu`.

A batch is therefore wholly before the checkpoint (its overlay mutation is in
the drain and its sequence is certified), or wholly after the synced marker
and sidecar (its record is physically in the replay suffix). Successful flush
now subtracts the snapshotted pending count rather than resetting it, so
post-checkpoint publications cannot be lost. Every error after an overlay drain
re-inserts that drain; no sidecar is published before the index and marker, and
the next flush retries conservatively.

The package-private checkpoint hook deterministically pauses the transaction
at the former drain/marker race boundary. The regression proves a concurrent
write cannot be acknowledged there, verifies the post-checkpoint pending count,
simulates a crash, and verifies one-record suffix replay plus byte-exact reads
after reopen. A separate injected apply failure proves the transaction lock is
released and the drained overlay is retained for retry.
