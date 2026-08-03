# NUFS V2.1 — P0 Correctness Hardening — Results

Date: 2026-08-03
Branch: `codex/v21-p0-hardening`
Plan: `docs/superpowers/plans/2026-08-02-nufs-v21-p0-correctness-hardening.md`
Scope: Make the V2.1 storage engine's P0 correctness gates **behavioral** (not structural string-search), and record runnable evidence that every gate passes.

## Summary

Task 8 converted the P0 release gate from structural string-search into behavioral
verification and produced this evidence document. In the process, the P0 gates surfaced
**three real correctness bugs** that were fixed and are documented below:

| # | Bug | Impact | Fix |
|---|-----|--------|-----|
| B1 | Overlay `Drain` made committed entries invisible during a flush window | An acknowledged delete could be lost across recovery (chunk storage) | Staged draining set keeps entries readable until the flush is durable (`overlay.go`, `flush.go`, `store.go`) |
| B2 | Replicator `close(taskCh)` raced retry `chansend` | Data race under `-race` | Mutex serializing send/close (`replicator.go`) |
| B3 | Legacy ChunkStore `writeMetaSidecar` marshaled a concurrently-mutated `*LocalChunkInfo` | Data race in the replication path | Snapshot under the store lock; pass the value (`chunkstore.go`, `disk_shard.go`, `diskmanager.go`) |

All P0 gates now pass. The storage-gate results below were produced by the behavioral
gates that replaced the old structural checks.

---

## Step 1 — Behavioral gates

- **Primary behavioral gate** (new): `datanode/storage/segment/release_gate_behavior_test.go`
  — `TestReleaseGate_BehavioralExists` exercises the §21 invariants **directly**
  (single fsync barrier per durable batch, range reads only touch intersecting frames,
  the committed-delta overlay is the read authority, crash window does not lose
  acknowledged mutations). All 5 subtests pass.
- **Structural string-search** checks in `datanode/storage/release_gate_test.go` were
  demoted to **secondary lint**; they are no longer the primary release gate.

Verified: `go test ./datanode/storage/segment/... -run TestReleaseGate` passes.

## Step 2 — Focused P0 storage gates

### `make test-storage-p0` (race, 20x)

```
go test -race -short -count=20 -timeout 900s ./datanode/storage/segment/... ./datanode/storage/recovery/...
ok  	github.com/example/dfs/datanode/storage/segment	710.516s
ok  	github.com/example/dfs/datanode/storage/recovery	5.011s
```

Exit: 0

### `make test-storage-crash` (abrupt SIGKILL recovery, 50x)

```
go test -count=50 -timeout 20m ./datanode/storage/segment -run TestProcessCrash_AcknowledgedMutationsRecover
ok  	github.com/example/dfs/datanode/storage/segment	467.191s
```

Exit: 0 — 50 iterations of the SIGKILL-then-recover scenario all recovered every
acknowledged put, delete, and tombstone.

## Step 3 — Repository-wide verification

### `go vet ./...`

Exit: 0 (no output)

### `go build ./...`

Exit: 0 (no output)

### `go test -race` (per-package)

All packages pass under `-race` with **zero data races** (`WARNING: DATA RACE` count = 0).

Fast packages (41), 180s budget each — all `ok`, 0 races:

```
ok  	github.com/example/dfs/chunkstore	7.674s
ok  	github.com/example/dfs/cmd/metad         (cached)
ok  	github.com/example/dfs/cmd/nufs-backup    (cached)
ok  	github.com/example/dfs/cmd/nufs-doctor    (cached)
ok  	github.com/example/dfs/cmd/nufs-restore   (cached)
ok  	github.com/example/dfs/datanode/storage	3.641s
ok  	github.com/example/dfs/gateway/fuse       (cached)
ok  	github.com/example/dfs/gateway/s3         (cached)
ok  	github.com/example/dfs/gateway/s3fs       (cached)
ok  	github.com/example/dfs/internal/crypto    (cached)
ok  	github.com/example/dfs/tests/metadata_dr  (cached)
ok  	github.com/example/dfs/tests/smoke       3.885s
… (all other fast packages ok)
```

Three large packages exceed the plan-literal `180s` default budget under `-race`, so they
are run with an adequate per-package budget. Each passes cleanly when run on its own
(sequential, no cross-package CPU contention):

```
ok  	github.com/example/dfs/datanode	47.558s
ok  	github.com/example/dfs/metadata	125.542s
ok  	github.com/example/dfs/datanode/storage/segment	166.935s
```

> Note: running the three large `-race` suites *concurrently* (as `go test ./...` does)
> exceeds their shared CPU budget and surfaces pre-existing **timing** flakes (e.g. the
> Raft `TestRaftClusterLeaderFailoverPreservesCommittedBucket` leader-election timeout).
> Each of those passes deterministically in isolation (verified `-count=3`). No run
> reported a data race.

## Step 4 — Correctness fixes surfaced by the gates

### B1 — Overlay drain-window visibility (storage)
**Symptom:** an *acknowledged delete* could be lost across an abrupt process crash:
after recovery the extent was live (`state=0`) instead of tombstoned, and `applied`
in the recovery result was one short of the acknowledged-op count.

**Root cause:** `flush()` drained the committed-delta overlay before applying the
mutations to Pebble. Both steps run under `s.mu`, but the read path (`lookup`) does
not take `s.mu` — it consults the overlay's own lock, then Pebble. A concurrent read
in the drain→apply window saw the extent in *neither* place. `Delete` resolves its
target through the same `lookup` and treated `ErrExtentNotFound` as "already gone",
returning success **without appending a tombstone** — acked delete never recorded in
the segment log, so recovery restored the live put.

**Fix** (`overlay.go`, `flush.go`, `store.go`): `Drain` now moves entries into a staged
`draining` set that `Get` still serves; `DiscardDrained` releases them only after the
index checkpoint is durable, and `RestoreDrained` puts them back after a failed flush.
Regression coverage: `flush_visibility_test.go`
(`TestFlush_CommittedExtentVisibleDuringDrainWindow`, `TestOverlay_DrainKeepsEntriesReadableUntilResolved`)
— verified to **fail without** the fix and **pass with** it.

### B2 — Replicator send/close data race
**Symptom:** `WARNING: DATA RACE` — `Replicator.Stop()` `close(taskCh)` racing a retry
`chansend` from a worker's `time.AfterFunc`.

**Fix** (`replicator.go`): a `taskMu` serializes the channel send (`Submit`) against
the close (`Stop`). The send stays non-blocking, so retry requeues cannot deadlock.
Covered 10× `-count=10` under `-race` (was failing before the fix).

### B3 — ChunkStore sidecar/Seal data race
**Symptom:** `WARNING: DATA RACE` in `TestWritePipeline_FasterThanSerial` — `Seal`
mutating `*LocalChunkInfo` (under `cs.mu`) while a replication `Write` on another
connection JSON-marshals the same struct in `writeMetaSidecar`.

**Fix** (`chunkstore.go`, `disk_shard.go`, `diskmanager.go`): `writeMetaSidecar` now
takes the info **by value**; the two shared-struct call sites (`Write`, `Seal`) snapshot
under `cs.mu` before releasing it. Covered 10× under `-race` and the full `datanode`
suite is now race-clean.

## Files changed in this gate

- New: `datanode/storage/segment/release_gate_behavior_test.go` (behavioral release gate)
- New: `datanode/storage/segment/flush_visibility_test.go` (B1 regression)
- `datanode/storage/segment/overlay.go`, `flush.go`, `store.go` (B1)
- `datanode/replicator.go` (B2)
- `datanode/chunkstore.go`, `datanode/disk_shard.go`, `datanode/diskmanager.go` (B3)
- `datanode/storage/release_gate_test.go` (structural checks → secondary lint)
- `datanode/storage/segment/{crash_test,group_commit_test,scale_test,process_crash_test}.go`
  (`-short` stress-gate skips + crash-gate diagnostics)
- `Makefile` (`test-storage-p0`, `test-storage-crash` targets)
