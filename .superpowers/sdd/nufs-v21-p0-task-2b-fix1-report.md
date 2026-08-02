# NUFS V2.1 P0 Task 2B Fix Round 1 Report

## Outcome

Completed the interrupted Task 2B follow-up on `codex/v21-p0-hardening` from
base `0a5a878`. The change preserves the prior uncommitted recovery work and
adds the authoritative index-owned recovery checkpoint sidecar.

## Delivered behavior

- A durable `{version, streamID, segmentID, safeOffset, safeSeq, CRC}`
  sidecar is written only after Pebble flush and synced `INDEX_SAFE`, using
  temp-file sync, rename, and parent-directory sync.
- `Store.New` loads a valid matching checkpoint and replays/budgets only its
  suffix. Missing or other-segment checkpoints fall back to header replay;
  malformed or CRC-invalid sidecars fail closed.
- The sole segment parser validates checkpoint boundaries, counts full
  committed on-disk bytes, returns `SafeOffset`/`SafeSeq`, and reports startup
  duration through writer setup and final `DataReady`.
- Deterministic exact-30-second success and +1ns failure tests are covered.
- Crash-order coverage includes marker-only conservative replay, valid
  suffix-only replay, corrupt-sidecar rejection, and segment mismatch
  fallback.

## Verification evidence

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

## Repair during verification

The required focused selector previously included the 500-write endurance
test because its name contained `Recovery`; the test was renamed to preserve
the intended selector and still runs in the full suite. The full suite then
proved that a corruption drill can damage the sidecar-certified marker; the
drill now recognizes fail-closed `Store.New` as the required safe outcome.

## Concerns

None known after the final race verification.
