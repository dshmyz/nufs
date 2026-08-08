# NUFS V2.1 P0 Correctness Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the correctness, crash-recovery, range-read, relocation, and shutdown defects that currently prevent the V2.1 local storage engine from being a trustworthy production foundation.

**Architecture:** Keep the segment commit log as the local durability authority and Pebble as a derived index. Make each batch a contiguous sequence of records followed by exactly one `BatchCommit` and one sync; make recovery stream and validate only the bounded active tail. Deletes and relocations become first-class committed operations, while range reads operate directly on intersecting frames.

**Tech Stack:** Go 1.25, Pebble v1.1.5, segment files with `ReadAt`/`WriteAt`, CRC32C, optional per-frame AEAD, Go race detector.

## Global Constraints

- Acknowledged data must survive process crash, kill -9, and immediate restart without relying on a clean `Store.Close()`.
- Pebble remains a derived index and must not be required for reconstructing acknowledged mutations.
- One foreground batch uses exactly one segment-file sync after all records and its `BatchCommit` are written.
- Startup work is bounded by at most 100,000 committed mutations, 256 MiB of unindexed committed bytes, and 128 MiB of uncommitted tail per disk.
- Process-crash `DataReady` remains at most 30 seconds on the reference hardware defined by the V2.1 design.
- Range reads read, authenticate, decrypt, and decompress only intersecting 64 KiB frames.
- Do not modify the current unrelated worktree changes in `nufs-core/deploy/docker-compose.yml` or `nufs-core/scripts/run-v21-integration.sh`.
- Do not begin multi-disk production wiring, V2 metadata cutover, EC completion, or billion-scale qualification until this plan's exit gate passes.

---

## 1. Scope and sequencing

This is the first of four production-readiness plans:

1. **This plan:** local durability and crash correctness.
2. Production data-path wiring: native V2 protocol, multi-disk `StoreGroup`, small/data streams, placement tokens, quorum receipts.
3. Metadata and operations: `InodeMetaV2`, extent pages, PG epochs, inventory, maintenance workers, operator tools.
4. Scale qualification: 30-million and 100-million extent nodes, 100-million and 1-billion file clusters, failure drills, and 72-hour soak.

The tasks in this document are ordered. Task 1 establishes the commit layout used by Tasks 2 and 3. Task 2 establishes the recovery parser used by durable delete. Tasks 4-6 can proceed after Task 2, but the final gate runs only after all tasks complete.

## 2. File responsibility map

- `nufs-core/datanode/storage/segment/commit_coordinator.go`: owns batching, leader election, wake-up, cancellation, and completion; it never writes disk data.
- `nufs-core/datanode/storage/segment/store.go`: owns offset reservation, committed operation construction, overlay/index publication, relocation CAS, and idempotent shutdown.
- `nufs-core/datanode/storage/segment/writer.go`: owns positional writes and the single durability barrier.
- `nufs-core/datanode/storage/segment/recover.go`: owns streaming segment-tail parsing, batch validation, replay, and truncation decisions.
- `nufs-core/datanode/storage/journal/commit.go`: owns encoded batch metadata and descriptor checksums.
- `nufs-core/datanode/storage/segment/reader.go`: owns header/index reads and selective frame IO.
- `nufs-core/datanode/storage/index/index.go`: owns conditional index mutation helpers used by relocation.
- `nufs-core/datanode/storage/segment/*_test.go`: owns deterministic unit, race, and crash recovery coverage.
- `nufs-core/tests/storage-crash-helper/`: owns the subprocess used to simulate abrupt process termination without calling `Store.Close()`.

---

### Task 1: Replace the group-commit coordinator and make batches contiguous

**Files:**

- Modify: `nufs-core/datanode/storage/segment/commit_coordinator.go`
- Modify: `nufs-core/datanode/storage/segment/store.go`
- Modify: `nufs-core/datanode/storage/segment/allocator.go`
- Modify: `nufs-core/datanode/storage/segment/group_commit_test.go`
- Create: `nufs-core/datanode/storage/segment/commit_layout_test.go`

**Interfaces:**

- Consumes: `pendingWrite` values prepared by `Store.Write`.
- Produces: `Submit(*pendingWrite, func([]*pendingWrite) error) error` with no lost wake-up and exactly-once completion.
- Guarantees: `record[0] ... record[n-1] BatchCommit` are contiguous; the allocator reserves `BatchCommitSize` once per batch.

- [ ] **Step 1: Add a deterministic lost-wakeup regression test**

Add a package-private test hook to the coordinator so the test can pause immediately before the leader waits. Write a test that starts one leader, releases the timer/wakeup at that boundary, and requires completion within one second:

```go
func TestGroupCommit_NoLostWakeup(t *testing.T) {
	c := newGroupCommitCoordinator(groupCommitConfig{MaxBatch: 8, MaxWait: time.Millisecond})
	done := make(chan error, 1)
	go func() {
		done <- c.Submit(testPendingWrite(1), func(batch []*pendingWrite) error {
			if len(batch) != 1 {
				t.Fatalf("batch size = %d, want 1", len(batch))
			}
			return nil
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("group commit leader lost wake-up")
	}
}
```

- [ ] **Step 2: Run the race regression and confirm failure**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test -race ./datanode/storage/segment -run 'TestGroupCommit_NoLostWakeup|TestRecovery_ManyWritesReopen' -count=10 -timeout 30s
```

Expected: FAIL or timeout in `groupCommitCoordinator.Submit` before the implementation change.

- [ ] **Step 3: Replace timer-plus-Cond waiting with one coordinator loop**

Use a dedicated coordinator goroutine and request channel. `Submit` only enqueues and waits on its own result channel; the coordinator owns the batch timer, so wake-ups cannot be lost:

```go
type commitRequest struct {
	write  *pendingWrite
	commit func([]*pendingWrite) error
}

type groupCommitCoordinator struct {
	cfg    groupCommitConfig
	reqs   chan commitRequest
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
}

func (c *groupCommitCoordinator) Submit(pw *pendingWrite, commit func([]*pendingWrite) error) error {
	pw.done = make(chan struct{})
	select {
	case c.reqs <- commitRequest{write: pw, commit: commit}:
	case <-c.stop:
		return storage.ErrCapacity
	}
	<-pw.done
	return pw.err
}
```

The loop starts a timer after receiving the first request, drains up to `MaxBatch`, invokes the first request's callback once, and calls `finish` once for every member. `close()` closes `stop` through `sync.Once` and waits for `done`.

- [ ] **Step 4: Add the contiguous-layout test**

After a concurrent batch completes, parse raw bytes from `SegmentHeaderSize`. Require every next record offset to equal the previous record end and require one `BatchCommit` immediately after the last record:

```go
if got, want := secondOffset, firstOffset+int64(firstFraming); got != want {
	t.Fatalf("record gap: second offset=%d want=%d", got, want)
}
if got, want := commitOffset, lastOffset+int64(lastFraming); got != want {
	t.Fatalf("commit offset=%d want contiguous offset=%d", got, want)
}
```

- [ ] **Step 5: Reserve commit space once per batch**

In `commitBatch`, reserve all record framings consecutively, then reserve one `journal.BatchCommitSize`. If the complete batch cannot fit, seal before reserving any member and retry the whole batch on a fresh segment. Do not seal in the middle of a batch.

```go
required := int64(journal.BatchCommitSize)
for _, pw := range batch {
	required += int64(RecordFraming(pw.storedLen, pw.frameSize, int(pw.header.FrameCount)))
}
if !s.alloc.CanReserveBatch(required) {
	if err := s.sealActiveLocked(); err != nil {
		return err
	}
}
```

- [ ] **Step 6: Verify batching and layout repeatedly**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test -race ./datanode/storage/segment -run 'TestGroupCommit|TestCommitLayout|TestRecovery_ManyWritesReopen' -count=20 -timeout 60s
```

Expected: PASS with no timeout and fewer syncs than writes in the concurrent case.

- [ ] **Step 7: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/datanode/storage/segment/commit_coordinator.go nufs-core/datanode/storage/segment/store.go nufs-core/datanode/storage/segment/allocator.go nufs-core/datanode/storage/segment/group_commit_test.go nufs-core/datanode/storage/segment/commit_layout_test.go
git commit -m "fix(storage): make group commits contiguous and deadlock-free"
```

---

### Task 2: Stream, validate, and truncate segment recovery

**Files:**

- Modify: `nufs-core/datanode/storage/segment/recover.go`
- Modify: `nufs-core/datanode/storage/segment/store.go`
- Modify: `nufs-core/datanode/storage/recovery/recovery.go`
- Create: `nufs-core/datanode/storage/segment/recover_stream_test.go`
- Create: `nufs-core/datanode/storage/segment/recover_bounds_test.go`

**Interfaces:**

- Consumes: active segment path, stream ID, checkpoint safe offset/sequence, replay limits.
- Produces: `RecoverFromSegmentLog(path string, opts RecoverOptions, apply func(CommitDescriptor) error) (*RecoverResult, error)`.
- Guarantees: bounded memory, descriptor/offset/count validation, valid-tail truncation, and explicit `DataReady` result.

- [ ] **Step 1: Define recovery options and limit errors**

```go
type RecoverOptions struct {
	StreamID         uint8
	SafeOffset       int64
	SafeSeq          uint64
	MaxRecords       uint64
	MaxReplayBytes   int64
	MaxTrailingBytes int64
}

var ErrRecoveryBudgetExceeded = errors.New("storage: recovery budget exceeded")
```

Defaults must use `recovery.MaxReplayRecords`, `MaxUnindexedBytes`, and `MaxUncommittedTail`.

- [ ] **Step 2: Write failing bounded-memory and truncation tests**

Use a sparse file with a valid header, one valid committed batch, and an invalid tail. Assert that recovery applies the batch and truncates exactly to `LastCommittedOffset`. Wrap reads with a counting `ReaderAt` or expose a package-private reader function; assert no read allocation is proportional to the full segment size.

```go
st, err := os.Stat(path)
if err != nil {
	t.Fatal(err)
}
if st.Size() != res.LastCommittedOffset {
	t.Fatalf("size=%d want=%d", st.Size(), res.LastCommittedOffset)
}
```

- [ ] **Step 3: Confirm the old implementation fails**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./datanode/storage/segment -run 'TestRecover_Streamed|TestRecover_TruncatesTail|TestRecover_RejectsBudget' -count=1
```

Expected: FAIL because recovery currently uses `os.ReadFile`, does not enforce limits, and does not truncate.

- [ ] **Step 4: Implement streaming parsing**

Open the file read-write, seek to `max(SegmentHeaderSize, SafeOffset)`, and use fixed-size header/commit buffers plus frame-index-sized allocations. Do not read payload frames during replay; compute record framing from the validated header and frame index.

For each `BatchCommit`, require:

```go
if bc.StreamID != opts.StreamID ||
	bc.RecordCount != uint32(len(pending)) ||
	bc.FirstOffset != pending[0].Offset ||
	bc.LastOffset != currentOffset ||
	bc.DescriptorsCRC != descriptorsCRC(pending) {
	break
}
```

Apply only after all checks pass. Increment replay records/bytes and return `ErrRecoveryBudgetExceeded` before exceeding configured bounds.

- [ ] **Step 5: Truncate only invalid uncommitted tail**

After parsing, call `f.Truncate(lastCommittedOffset)` when trailing bytes exist and do one `f.Sync()` before returning. Never truncate a sealed segment or bytes below the checkpoint safe offset.

- [ ] **Step 6: Close the recovery state loop**

Set `Result.Applied`, `Result.DataReady`, duration, safe sequence, and trailing bytes from actual segment replay. `DataReady` is true only after replay and truncation succeed within `RecoveryBudget`.

- [ ] **Step 7: Verify recovery behavior**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test -race ./datanode/storage/segment ./datanode/storage/recovery -run 'Recover|Crash' -count=10 -timeout 60s
```

Expected: PASS; no test reads a full 4 GiB sparse segment into memory.

- [ ] **Step 8: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/datanode/storage/segment/recover.go nufs-core/datanode/storage/segment/store.go nufs-core/datanode/storage/recovery/recovery.go nufs-core/datanode/storage/segment/recover_stream_test.go nufs-core/datanode/storage/segment/recover_bounds_test.go
git commit -m "fix(storage): bound and validate segment recovery"
```

---

### Task 3: Persist deletes as committed tombstone records

**Files:**

- Modify: `nufs-core/datanode/storage/segment/record.go`
- Modify: `nufs-core/datanode/storage/segment/store.go`
- Modify: `nufs-core/datanode/storage/segment/recover.go`
- Create: `nufs-core/datanode/storage/segment/delete_crash_test.go`

**Interfaces:**

- Consumes: exact `(ExtentID, Generation)` and its current location.
- Produces: a framed `OpDelete` record followed by one `BatchCommit` and one sync.
- Guarantees: an acknowledged delete remains deleted after abrupt restart even if Pebble never observes the tombstone.

- [ ] **Step 1: Encode operation type in the record header**

```go
type RecordOp uint8

const (
	RecordPut RecordOp = iota + 1
	RecordDelete
	RecordRelocate
)
```

Add `Op RecordOp` to the encoded `RecordHeader`, update the fixed header size, CRC coverage, encode/decode tests, and format-version compatibility test.

- [ ] **Step 2: Add a failing delete replay test**

Write an extent, persist its index, delete it, then reopen using a copied pre-delete index directory so only the segment log can prove the delete. Require `ErrExtentNotFound`.

```go
_, err := reopened.Read(ctx, &storage.ReadRequest{ExtentID: 7, Generation: 1})
if !errors.Is(err, storage.ErrExtentNotFound) {
	t.Fatalf("read after recovered delete = %v, want ErrExtentNotFound", err)
}
```

- [ ] **Step 3: Confirm the test fails**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./datanode/storage/segment -run TestDelete_AcknowledgedBeforeIndexApplySurvivesRecovery -count=1
```

Expected: FAIL because the current delete writes only a `BatchCommit` with no replayable descriptor.

- [ ] **Step 4: Route delete through the normal commit coordinator**

Build a zero-payload framed record with `Op=RecordDelete`, the exact extent/generation, and a descriptor checksum. Submit it to the same batch coordinator as writes. Publish `ExtentTombstoned` to overlay/index only after the batch sync succeeds.

- [ ] **Step 5: Replay tombstones**

Extend `CommitDescriptor` with `Op`. During replay, publish `ExtentTombstoned` for delete records; never synthesize a delete from an empty pending batch.

- [ ] **Step 6: Verify write/delete interleavings**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test -race ./datanode/storage/segment -run 'Delete|Generation|Crash' -count=20 -timeout 60s
```

Expected: PASS for put-delete, put-delete-new-generation, duplicate delete, and crash-before/after batch sync.

- [ ] **Step 7: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/datanode/storage/segment/record.go nufs-core/datanode/storage/segment/store.go nufs-core/datanode/storage/segment/recover.go nufs-core/datanode/storage/segment/delete_crash_test.go
git commit -m "fix(storage): persist generation-fenced tombstones"
```

---

### Task 4: Read only intersecting frames for range requests

**Files:**

- Modify: `nufs-core/datanode/storage/segment/reader.go`
- Modify: `nufs-core/datanode/storage/segment/store_test.go`
- Create: `nufs-core/datanode/storage/segment/range_io_test.go`

**Interfaces:**

- Consumes: record offset, stored/logical lengths, logical offset, and requested length.
- Produces: exact requested bytes with IO bounded to header, frame index, trailer, and intersecting frame bytes.

- [ ] **Step 1: Add an IO-counting range-read test**

Create a 16 MiB extent with 64 KiB frames. Request 4 KiB from the middle and assert returned bytes match while payload bytes read stay below 128 KiB. Introduce a package-private `readerAt` interface so the test can count `ReadAt` calls and bytes.

```go
if got := counting.BytesRead(); got > 128<<10 {
	t.Fatalf("range read consumed %d bytes, want <= 128 KiB", got)
}
```

- [ ] **Step 2: Confirm the current full-read path fails**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./datanode/storage/segment -run TestRangeRead_ReadsOnlyIntersectingFrames -count=1
```

Expected: FAIL because `ReadRangeFrames` calls `ReadPayloadFrames`.

- [ ] **Step 3: Split metadata parsing from frame loading**

Add:

```go
type recordLayout struct {
	header        RecordHeader
	frameIndex    FrameIndex
	firstFrameOff int64
	trailerOff    int64
}

func (r *Reader) readRecordLayout(offset int64, storedLen, logicalLen uint32) (*recordLayout, error)
func (r *Reader) readLogicalFrame(layout *recordLayout, frameNo int) ([]byte, error)
```

`readRecordLayout` validates header, frame-index CRC, stored-length sum, and trailer without reading payload frames.

- [ ] **Step 4: Implement frame selection and slicing**

Calculate:

```go
first := int(logicalOffset / int64(layout.header.EffectiveFrameSize()))
last := int((logicalOffset + int64(length) - 1) / int64(layout.header.EffectiveFrameSize()))
```

Read only frames `first..last`, verify CRC before decrypting, decrypt before decompressing, concatenate at most the selected frames, then slice to the exact requested logical range. Invalid ranges return a defined error rather than silently returning the full extent.

- [ ] **Step 5: Verify encrypted, compressed, and boundary cases**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test -race ./datanode/storage/segment -run 'RangeRead|Compression|Encrypt|Checksum' -count=20 -timeout 60s
```

Expected: PASS for one-frame, cross-frame, final-frame, compressed, encrypted, and corrupt selected-frame cases.

- [ ] **Step 6: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/datanode/storage/segment/reader.go nufs-core/datanode/storage/segment/store_test.go nufs-core/datanode/storage/segment/range_io_test.go
git commit -m "perf(storage): bound range reads to intersecting frames"
```

---

### Task 5: Make compaction relocation conditional and durable

**Files:**

- Modify: `nufs-core/datanode/storage/types.go`
- Modify: `nufs-core/datanode/storage/index/index.go`
- Modify: `nufs-core/datanode/storage/segment/store.go`
- Modify: `nufs-core/datanode/storage/segment/recover.go`
- Modify: `nufs-core/datanode/storage/maintenance/compactor.go`
- Create: `nufs-core/datanode/storage/segment/relocation_test.go`

**Interfaces:**

- Consumes: exact old and new location for `(ExtentID, Generation)`.
- Produces: `Relocate(ctx context.Context, relocs []Reloc) error` with compare-and-swap semantics and replayable `RecordRelocate` operations.

- [ ] **Step 1: Extend relocation with the expected old location**

```go
type Reloc struct {
	ExtentID   ExtentID
	Generation Generation
	OldSegmentID SegmentID
	OldOffset  int64
	SegmentID  SegmentID
	Offset     int64
	StoredLen  uint32
	LogicalLen uint32
}
```

- [ ] **Step 2: Add failing stale-relocation tests**

Cover overwrite during compaction and delete during compaction. In both cases, applying a relocation whose old location no longer matches must leave the newer state untouched.

- [ ] **Step 3: Add a single-key conditional index helper**

```go
func (ix *Index) CompareLocationAndPut(
	extentID storage.ExtentID,
	generation storage.Generation,
	oldSegment storage.SegmentID,
	oldOffset int64,
	next *Value,
) (bool, error)
```

Serialize compare-and-put with the store mutation lock. Return `false, nil` on mismatch; do not overwrite.

- [ ] **Step 4: Commit relocation records before publication**

Write a `RecordRelocate` operation containing old and new locations, append its `BatchCommit`, sync once, then conditionally publish it. Recovery applies the same old-location comparison, making replay idempotent.

- [ ] **Step 5: Verify relocation races and replay**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test -race ./datanode/storage/segment ./datanode/storage/maintenance -run 'Relocat|Compact' -count=20 -timeout 60s
```

Expected: PASS; stale compaction can neither resurrect a delete nor replace a newer generation/location.

- [ ] **Step 6: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/datanode/storage/types.go nufs-core/datanode/storage/index/index.go nufs-core/datanode/storage/segment/store.go nufs-core/datanode/storage/segment/recover.go nufs-core/datanode/storage/maintenance/compactor.go nufs-core/datanode/storage/segment/relocation_test.go
git commit -m "fix(storage): make relocation conditional and replayable"
```

---

### Task 6: Make shutdown idempotent and leak-free

**Files:**

- Modify: `nufs-core/datanode/storage/segment/store.go`
- Modify: `nufs-core/cmd/datanode/main.go`
- Create: `nufs-core/datanode/storage/segment/close_test.go`

**Interfaces:**

- Consumes: zero or more concurrent `Close()` calls.
- Produces: one shutdown execution and the same terminal error for every caller.

- [ ] **Step 1: Add repeated and concurrent close tests**

```go
func TestStoreClose_IsIdempotent(t *testing.T) {
	s := openTestStore(t)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Close()
		}()
	}
	wg.Wait()
}
```

Run under the race detector and require no panic, deadlock, or send-on-closed-channel.

- [ ] **Step 2: Implement one-shot shutdown**

Add `closeOnce sync.Once`, `closeDone chan struct{}`, and `closeErr error`. Move the existing close body into `closeInternal`, run it once, and make all callers wait for `closeDone`.

- [ ] **Step 3: Remove duplicate ownership in the DataNode entry point**

Keep one close owner in `runDataNodeV21`: either defer cleanup for initialization failure or explicit shutdown cleanup, but not both. Stop heartbeat before closing stores.

- [ ] **Step 4: Verify lifecycle tests**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test -race ./datanode/storage/segment ./cmd/datanode -run 'Close|Shutdown|V21' -count=20 -timeout 60s
```

Expected: PASS without panic or goroutine leak.

- [ ] **Step 5: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/datanode/storage/segment/store.go nufs-core/cmd/datanode/main.go nufs-core/datanode/storage/segment/close_test.go
git commit -m "fix(storage): make V2 store shutdown idempotent"
```

---

### Task 7: Add an abrupt-process crash qualification harness

**Files:**

- Create: `nufs-core/tests/storage-crash-helper/main.go`
- Create: `nufs-core/datanode/storage/segment/process_crash_test.go`
- Modify: `nufs-core/Makefile`
- Modify: `nufs-core/datanode/storage/RUNBOOK.md`

**Interfaces:**

- Consumes: a temporary store directory, operation script, and crash point supplied through arguments.
- Produces: a subprocess that reports acknowledged operation IDs through stdout and exits via `SIGKILL` without calling `Store.Close()`.

- [ ] **Step 1: Implement the helper protocol**

The helper accepts:

```text
storage-crash-helper --dir PATH --writers 16 --writes 4096 --delete-every 17
```

After each successful receipt it prints one JSON line and flushes stdout:

```json
{"op":"put","extent_id":42,"generation":1,"checksum":1234,"ack":true}
```

It then blocks. The parent test sends `SIGKILL`; the helper must not install a signal handler and must not defer `Store.Close()`.

- [ ] **Step 2: Write the parent crash-recovery test**

The parent starts the helper, captures acknowledged operations, waits until at least one multi-record batch is observed, kills it, reopens the store, and verifies every acknowledged put/delete exactly. Before reopening, it may remove or roll back the derived index to the pre-write snapshot to force segment-log replay.

- [ ] **Step 3: Run the qualification test repeatedly**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./datanode/storage/segment -run TestProcessCrash_AcknowledgedMutationsRecover -count=50 -timeout 10m
```

Expected: PASS with zero acknowledged mutation loss and zero corrupt successful read.

- [ ] **Step 4: Add Makefile gates**

```make
test-storage-p0:
	go test -race -count=20 -timeout 120s ./datanode/storage/segment/... ./datanode/storage/recovery/...

test-storage-crash:
	go test -count=50 -timeout 10m ./datanode/storage/segment -run TestProcessCrash_AcknowledgedMutationsRecover
```

- [ ] **Step 5: Document drill interpretation**

Update the runbook with the exact command, successful output requirements, retained artifact paths, and the rule that a clean `Close()` test does not count as process-crash evidence.

- [ ] **Step 6: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/tests/storage-crash-helper/main.go nufs-core/datanode/storage/segment/process_crash_test.go nufs-core/Makefile nufs-core/datanode/storage/RUNBOOK.md
git commit -m "test(storage): qualify abrupt process crash recovery"
```

---

### Task 8: Run the P0 release gate and publish evidence

**Files:**

- Create: `docs/superpowers/verification/2026-08-02-nufs-v21-p0-correctness-results.md`
- Modify: `nufs-core/datanode/storage/release_gate_test.go`

**Interfaces:**

- Consumes: Tasks 1-7 and their deterministic tests.
- Produces: a reproducible verification report containing command, commit, hardware, duration, pass/fail, and artifact paths.

- [ ] **Step 1: Replace structural string-search gates with behavioral gates**

Retain source-level checks only as secondary lint. The release gate must directly execute contiguous commit parsing, selective range IO, bounded recovery, durable delete, relocation CAS, and concurrent close behavior.

- [ ] **Step 2: Run focused P0 verification**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
make test-storage-p0
make test-storage-crash
```

Expected: PASS.

- [ ] **Step 3: Run repository-wide verification**

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go vet ./...
go build ./...
go test -race -timeout 180s ./...
```

Expected: all commands exit 0. Package timeout, goroutine leak, skipped required crash test, or race report is a release-gate failure.

- [ ] **Step 4: Record evidence**

Collect immutable environment values first:

```bash
cd /Users/gracegaoya/work/project/nufs
git rev-parse HEAD
uname -a
sysctl -n machdep.cpu.brand_string 2>/dev/null || true
sysctl -n hw.memsize 2>/dev/null || true
diskutil info / 2>/dev/null | awk '/Device \/ Media Name|Solid State|Protocol/ {print}'
```

Create the results document with the exact stdout from those commands and the measured duration printed by each test command. Use this fixed schema:

```markdown
# NUFS V2.1 P0 Correctness Verification Results

- Git commit: value from `git rev-parse HEAD`
- OS/kernel: complete output from `uname -a`
- CPU/RAM/storage: complete non-empty outputs from the hardware commands
- `make test-storage-p0`: PASS and its wall-clock duration
- `make test-storage-crash`: PASS and its wall-clock duration
- `go test -race -timeout 180s ./...`: PASS and its wall-clock duration
- Acknowledged mutations tested: sum reported by the 50 crash iterations
- Lost acknowledged mutations: `0`
- Corrupt successful reads: `0`
- Maximum observed recovery duration: maximum reported by the crash iterations
```

Do not commit the results document if any source command is missing, empty, skipped, timed out, or failed.

- [ ] **Step 5: Commit the gate**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/datanode/storage/release_gate_test.go docs/superpowers/verification/2026-08-02-nufs-v21-p0-correctness-results.md
git commit -m "test(storage): pass V2.1 P0 correctness gate"
```

---

## 3. Exit criteria

This plan is complete only when all statements below are true:

- `go test -race -timeout 180s ./...` passes without package timeout.
- Fifty abrupt-process crash iterations lose zero acknowledged puts, deletes, or relocations.
- A batch of 2-256 records has no on-disk gaps and ends in exactly one valid `BatchCommit`.
- Recovery validates stream, sequence, count, offsets, descriptor CRC, and operation type before applying a batch.
- Recovery does not allocate or read proportional to a 4 GiB active segment and truncates invalid tail safely.
- A 4 KiB read from a 16 MiB extent reads at most two 64 KiB payload frames plus metadata.
- Stale relocation cannot overwrite a newer location or resurrect a tombstoned extent.
- Concurrent and repeated `Close()` calls are race-free and idempotent.
- The verification result document contains measured evidence tied to an exact commit.

Failure of any item blocks production-path wiring and scale qualification.

## 4. Deferred follow-on plans

After this gate passes, create separate implementation plans in this order:

1. `nufs-v21-production-data-path`: native request/receipt protocol, placement token validation, exact generation, multi-disk `StoreGroup`, small/data streams, and two-of-three durable quorum.
2. `nufs-v21-metadata-and-operations`: production `InodeMetaV2`, atomic extent-page root switching, PG epoch migration, inventory/change journal, maintenance startup, and operator CLI.
3. `nufs-v21-scale-qualification`: 50K CI, 1M nightly, 10M/30M/100M node tests, 100M/1B cluster profiles, restart RTO, failure drills, and 72-hour soak.

Do not combine those plans: each has a different correctness boundary, deployment risk, and acceptance environment.
