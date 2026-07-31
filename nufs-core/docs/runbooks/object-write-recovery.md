# Object Write Recovery Runbook

This runbook covers NUFS object writes that are stuck, failed, or waiting for
the S3 gateway recovery and GC workers.

## Alerts

These alerts use metrics from `metad` `/metrics`.

| Metric | Use |
| --- | --- |
| `nufs_object_write_attempts{state="..."}` | Counts write attempts by state. |
| `nufs_object_write_background_task_state{task="recovery|gc",state="..."}` | Shows the current recovery and GC worker task state. |
| `nufs_object_write_background_task_attempts{task="recovery|gc"}` | Shows task retry count. |
| `nufs_object_write_background_task_last_updated_seconds{task="recovery|gc"}` | Shows when the periodic task last changed. |

| Alert | Severity | Meaning |
| --- | --- | --- |
| `NUFSObjectWriteFailuresPersisting` | critical | One or more object write attempts are in `failed` for at least 5 minutes. |
| `NUFSObjectWriteWorkerDeadLetter` | critical | The recovery or GC background task exhausted retries and entered `dead_letter`. |
| `NUFSObjectWriteRecoveryBacklog` | warning | Attempts in `recovery_needed` are not draining. |
| `NUFSObjectWriteHalfWritesPersisting` | warning | Attempts are stuck before durable commit in `pending` or `chunks_allocated`. |
| `NUFSObjectWriteWorkerStale` | warning | Recovery or GC task has not updated for more than 15 minutes. |

## Quick Triage

Set these variables before running commands:

```bash
export METAD_OPS_URL=http://127.0.0.1:8091
export LIMIT=20
```

Check write operation status:

```bash
curl -sS "$METAD_OPS_URL/api/v1/write-ops/status"
```

Check Prometheus series directly:

```bash
curl -sS "$METAD_OPS_URL/metrics" | grep 'nufs_object_write'
```

Read the S3 gateway logs for worker activity:

```bash
grep -E 'object write recovery|object write gc|write attempt|commit' /var/log/nufs/s3gw.log
```

If the metad endpoint is unavailable, handle metad availability or Raft leader
issues first. Do not run cleanup actions while the metadata quorum is unstable.

## State Meaning

| State | Visibility | Operator meaning |
| --- | --- | --- |
| `pending` | Not visible | Upload started, but chunk allocation or writes did not finish. |
| `chunks_allocated` | Not visible | Chunks exist or were planned, but durability is not confirmed. |
| `chunks_durable` | Not visible until inode update succeeds | Enough chunk replicas are durable; recovery can safely commit the object pointer. |
| `recovery_needed` | Not visible until recovery succeeds | Commit path failed after data durability; recovery should replay metadata commit. |
| `committed` | Visible | Object pointer is committed. |
| `failed` | Not visible unless an older object version already existed | Recovery or write commit failed and needs diagnosis before GC removes leftovers. |

## Recovery Backlog

Use this when `NUFSObjectWriteRecoveryBacklog` fires.

1. List recoverable attempts:

```bash
curl -sS "$METAD_OPS_URL/api/v1/write-attempts?state=recovery_needed&limit=$LIMIT"
curl -sS "$METAD_OPS_URL/api/v1/write-attempts?state=chunks_durable&limit=$LIMIT"
```

2. Check the recovery task:

```bash
curl -sS "$METAD_OPS_URL/api/v1/background-tasks/object-write-recovery-periodic"
```

3. Confirm at least one S3 gateway is running with object write workers enabled.

The gateway must be started with `-write-workers=true`. The worker creates and
leases `object-write-recovery-periodic`; a healthy task usually cycles through
`queued`, `leased`, and `succeeded`.

4. If no gateway worker is active, restart or scale an S3 gateway with:

```bash
nufs-s3 -write-workers=true -write-worker-interval=30s -write-recovery-limit=100
```

5. Re-check the backlog. The count should fall:

```bash
watch -n 10 'curl -sS "$METAD_OPS_URL/api/v1/write-ops/status"'
```

Escalate if attempts remain in `recovery_needed` and `last_error` mentions
missing chunks, missing inode, or repeated metadata update failures.

## Failed Attempts

Use this when `NUFSObjectWriteFailuresPersisting` fires.

1. List failed attempts:

```bash
curl -sS "$METAD_OPS_URL/api/v1/write-attempts?state=failed&limit=$LIMIT"
```

2. For each failed attempt, inspect `last_error`, `bucket`, `key`, `inode_id`,
and `chunks`.

3. Classify the failure:

| Error pattern | Action |
| --- | --- |
| Chunk not durable or missing | Check datanode health, chunk replicas, and repair queue before retrying. |
| Inode missing | Confirm whether the object was deleted or bucket lifecycle removed it. Do not force commit. |
| Metadata update failure | Check Raft leader, quorum, disk, and metad logs. Recovery should succeed once metad is healthy. |
| Permission or invalid argument | Treat as a write-path bug or bad metadata record. Preserve the attempt for investigation. |

4. Do not delete the write attempt until you confirm the chunks are not the only
copy of a user-visible object. GC skips chunks referenced by the attempt inode,
but manual deletion bypasses your investigation trail.

## Half Writes

Use this when `NUFSObjectWriteHalfWritesPersisting` fires.

1. List stuck attempts:

```bash
curl -sS "$METAD_OPS_URL/api/v1/write-attempts?state=pending&limit=$LIMIT"
curl -sS "$METAD_OPS_URL/api/v1/write-attempts?state=chunks_allocated&limit=$LIMIT"
```

2. Compare `updated_at` with the gateway timeout and `-write-gc-abandon-age`.

3. If the attempts are newer than the abandon age, wait one more worker interval
and check whether writers are still active.

4. If they are older than the abandon age, verify the GC worker is active:

```bash
curl -sS "$METAD_OPS_URL/api/v1/background-tasks/object-write-gc-periodic"
```

The GC worker deletes chunks for abandoned `pending`, `chunks_allocated`, and
`failed` attempts only when those chunks are not referenced by the attempt inode.

## Worker Dead Letter

Use this when `NUFSObjectWriteWorkerDeadLetter` fires.

1. Inspect both tasks:

```bash
curl -sS "$METAD_OPS_URL/api/v1/background-tasks/object-write-recovery-periodic"
curl -sS "$METAD_OPS_URL/api/v1/background-tasks/object-write-gc-periodic"
```

2. Fix the cause from `last_error`.

3. Requeue the affected periodic task after the cause is fixed:

```bash
now_ns=$(date +%s000000000)
curl -sS -X PUT "$METAD_OPS_URL/api/v1/background-tasks/object-write-recovery-periodic" \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"object-write-recovery-periodic\",\"type\":\"write_recovery\",\"state\":\"queued\",\"target\":\"object-write-recovery\",\"next_run_at\":$now_ns}"

curl -sS -X PUT "$METAD_OPS_URL/api/v1/background-tasks/object-write-gc-periodic" \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"object-write-gc-periodic\",\"type\":\"write_gc\",\"state\":\"queued\",\"target\":\"object-write-gc\",\"next_run_at\":$now_ns}"
```

Only requeue the task that is actually in `dead_letter`.

## Worker Stale

Use this when `NUFSObjectWriteWorkerStale` fires.

1. Confirm at least one S3 gateway process is alive.
2. Confirm the gateway was started with `-write-workers=true`.
3. Check whether the worker lease is stuck on a dead owner.
4. Restart one gateway instance if the owner process is gone.
5. If the task remains stale after restart, requeue it using the dead-letter
procedure above.

## Safety Rules

- Do not mark `failed` attempts as `committed` manually.
- Do not delete chunks manually unless you have confirmed they are not referenced
  by the current inode chunk map.
- Do not run GC while metad quorum or the leader is unstable.
- Do not lower `-write-gc-abandon-age` during an incident unless active writers
  are already stopped.
- Keep failed attempts until customer impact and data safety are understood.

## Resolution Criteria

The incident is resolved when all of these are true for at least two alert
evaluation windows:

```bash
curl -sS "$METAD_OPS_URL/api/v1/write-ops/status"
curl -sS "$METAD_OPS_URL/metrics" | grep 'nufs_object_write'
```

- `failed == 0`
- `recovery_needed == 0`
- `pending + chunks_allocated == 0`, or remaining attempts are younger than the
  configured abandon age
- recovery and GC tasks are not in `dead_letter`
- `nufs_object_write_background_task_last_updated_seconds` is recent for active workers
