# Rolling Upgrade Runbook

> **Current ability (2026-08-24): NUFS does not support in-place rolling
> upgrades.** There is no entry-level Raft-log version and no mixed-version
> (N-1 + N) operation. The V2.1 storage engine replaces the on-disk format
> directly and cannot be downgraded. The only supported way to move a cluster
> from one release to the next is the **non-serviceable mixed window** below:
> back up, stop the whole fleet, move the whole fleet to the new binary, verify,
> reopen. If you need true per-node rolling later, see the compatibility-matrix
> §6 judgment for what versioning work that requires first.

This runbook defines how a production release is applied when in-place rolling
is not available. It is a publish / release runbook, not a per-node upgrade
procedure.

## Why there is no in-place rolling today

- `RaftLogOp` (first byte of a log entry) is the top-level switch; entries carry
  no entry-level version. A node on the new binary cannot safely coexist in the
  same Raft log with a node on the old binary.
- The on-disk Pebble layout and the backup artifact are replaced wholesale by the
  V2.1 design; an old binary cannot open the new format.
- `backup → whole-fleet move → restore` is therefore the only safe path, and it
  is the same machinery as disaster recovery.

## Release procedure (non-serviceable mixed window)

1. **Back up + verify.** Confirm a committed backup exists and is **non-stale**
   — within `backup-interval` + margin. See `scripts/check-backup-freshness.sh`.
   Never proceed with a stale or unverified backup.
2. **Health pre-checks.** Run the release gate:
   `VERIFY_COUNT=1 bash scripts/verify.sh -l full`. Confirm Raft quorum (3/3 or
   2/3 metad), no outstanding repair backlog, no leader-transition
   out-of-window errors.
3. **Open the maintenance window.** Stop writers at the gateway (or accept a
   brief outage — this is by design the non-serviceable window).
4. **Move the whole fleet.** Deploy the new binary to **all** metad, then all
   datanode, then the gateway. Do **not** leave any node on the old binary
   serving.
5. **Verify.** Run S3 PUT/GET smoke and a metadata mutation, confirm a fresh
   backup is created, confirm Raft re-formed quorum, confirm repair converges.
6. **Close the window.** Resume traffic.

## In the window, these are prohibited

- Leaving any node on the old binary while others serve (mixed version).
- Upgrading in the middle of a large repair backlog or with a stale backup.
- Proceeding while any node is not ready.
- Attempting a binary downgrade ("swap images back") — not supported; see
  `rollback.md`.

## When this becomes a true rolling upgrade

Turning this into per-node rolling requires the compatibility foundation the
matrix §6 describes: an entry-level Raft version, cluster-minimum-version
enforcement that refuses to start on an incompatible log, and a leader-transfer
sequence (upgrade followers → wait ready → transfer leadership → upgrade former
leader). None of that exists yet; until it does, treat this non-serviceable
window as the release path.
