# NUFS V2.1 P0 Task 3 Report — Durable Delete Audit

## Scope

Audited Task 3 on `codex/v21-p0-hardening` from base `5b18ac2` against the
Task 2A/2B reports and current segment implementation. Task 2A had already
introduced V3 `RecordOp`, descriptor `Op` CRC coverage, zero-frame deletes,
and recovery tombstones; Task 2B had already serialized checkpoint publication
with commit publication. No format or version change was made.

The only uncovered requirement was the exact pre-delete-index recovery
regression requested for this task. This change adds focused regression tests
only; no production code changed.

## Audit mapping

| Task 3 guarantee | Evidence |
| --- | --- |
| Delete is a framed `RecordDelete` with descriptor and one `BatchCommit` | `Store.Delete` builds a zero-frame `RecordDelete` and submits it through `commitBatch`; the new raw-layout test decodes the delete header and commit, verifies `RecordCount == 1`, descriptor CRC including `OpDelete`, and one additional sync. |
| Acknowledgement follows one group-batch sync | `commitBatch` writes records then one commit, performs `Writer.Sync` once, and only then publishes overlay state; the new layout test checks the delete sync delta is exactly one. |
| Exact `(ExtentID, Generation)` fencing | The new table-style flow covers put-delete, put-delete-new-generation, duplicate delete, and stale-generation delete; the higher generation remains readable. |
| Pre-sync delete carries no acknowledgement or visibility claim | The new deterministic `CrashAfterBatchCommitWrite` test requires delete to return an error and the prior live value to remain visible. |
| Post-sync acknowledged delete survives without a Pebble tombstone | The new test persists a live Pebble value, disables async apply, acknowledges delete, verifies Pebble still has the live value, abruptly closes descriptors, and requires restart to return `ErrExtentNotFound`. |
| Recovery overrides a copied pre-delete index | `TestDelete_AcknowledgedBeforeIndexApplySurvivesRecovery` copies the established pre-delete index, durably deletes with async apply disabled, simulates abrupt termination, restores the copied index, and requires `ErrExtentNotFound`. The segment log is the only evidence of delete. |
| Tombstone/cache/checkpoint ordering | `commitBatch` publishes the tombstone to overlay after sync while holding the Task 2B publication exclusion; `Delete` then overwrites the location-cache entry before derived-index enqueue. Recovery maps `RecordDelete` to `ExtentTombstoned`, so stale Pebble state is hidden by overlay. |

## RED / GREEN

- RED (test-design correction): the initial raw layout assertion used the
  tombstone's retained prior payload offset and failed. The failure proved the
  assertion was inspecting the put rather than the delete record; no production
  defect was indicated.
- GREEN: the corrected assertion derives the next contiguous record offset
  from the prior committed put. It passed with:

  ```text
  go test ./datanode/storage/segment -run TestDelete_UsesFramedRecordDescriptorBatchCommitAndOneSync -count=1 -timeout 60s
  # PASS (1.560s)
  ```

- The named pre-delete-index regression was new coverage over already-present
  behavior and passed on first execution; therefore no production red phase
  was applicable.

## Validation

Executed from `nufs-core`:

```text
go test ./datanode/storage/segment -run TestDelete_AcknowledgedBeforeIndexApplySurvivesRecovery -count=1 -timeout 60s
# PASS (3.096s)

go test -race ./datanode/storage/segment -run 'Delete|Generation|Crash' -count=20 -timeout 60s
# Timed out (exit 1, 61.285s) while repeatedly running the existing crash matrix;
# no assertion or race failure after the test-design correction.

go test -race ./datanode/storage/segment -run 'Delete|Generation|Crash' -count=20 -timeout 180s
# PASS (143.193s)

go test -race ./datanode/storage/segment -count=1 -timeout 240s
# PASS (146.948s)
```

The 180-second rerun is the minimum justified larger cap: the required
20-iteration selector completed only about eight iterations before the
60-second timeout, so 120 seconds could not cover the observed rate.

## Files

- `nufs-core/datanode/storage/segment/delete_crash_test.go`
- `.superpowers/sdd/nufs-v21-p0-task-3-report.md`

## Concerns

- The required 60-second repeated race selector is too short on this host;
  the documented 180-second rerun is clean.
- Task 3 itself is complete within scope. No Task 4 range-read work, S3 work,
  format changes, or version bump was introduced.
