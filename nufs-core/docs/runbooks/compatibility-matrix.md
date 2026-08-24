# Compatibility Matrix

This runbook records what NUFS **actually supports today** across upgrade,
downgrade, and rollback — what is safe, what is not, and the rule that decides
when a new field or format change is allowed. It is a source of truth for
release engineering and must be updated before any change to on-disk or Raft
encoding.

> Status note: NUFS has **not entered production**. The V2.1 storage-engine
> design (`docs/superpowers/specs/2026-08-02-nufs-billion-scale-storage-engine-v2-design.md`)
> deliberately replaces the legacy one-chunk-per-file layout directly, with **no
> dual-format reads, no online legacy migration, and no rollback to the old
> on-disk format**. Everything below starts from that constraint.

## 1. What the wire / disk formats look like today

| Artifact | Encoding | Self-describing? | Compat-extension hooks present today |
|---|---|---|---|
| Pebble row value | msgpack (`codecMsgpack`), written via `marshalValue` | **Yes** — msgpack carries its own type tags | `getValue` keeps a JSON read-sniff (first byte `0x7B` `{`) → old JSON reads still work; `CodecJSON` sniff retained |
| Raft log entry | first byte is `RaftLogOp` (0x01 Set, 0x02 Delete, 0x03 Batch, 0x05 ConditionalBatch), followed by op payload | Partial — op byte is the top-level switch | `ConditionalBatch.Version uint8` — an existing per-payload version field |
| Backup artifact | S3 repository via `CreateBackupCheckpoint` (PBL3) → `RestoreBackupToNewCluster` into a fresh cluster ID | Marker carries `format-version` | Backup is an opaque whole-cluster snapshot, not a per-row format |

**Key consequence:** msgpack being self-describing means *adding* an optional
field to a row/record is safe for old readers (they ignore unknown fields). That
is the only compatibility property NUFS has by construction today. Raft entries
have no entry-level version; compatibility across mixed metad versions is
**not** provided.

## 2. Upgrade / downgrade / rollback matrix

"Supported" below means the operations claim is true of the code and the
migration path exists *and has been exercised by an automated gate*. Anything
not listed is **not supported**.

| Operation | Supported? | How / why (or why not) |
|---|---|---|
| Read old-format rows written by N-1 | **Yes (JSON rows only)** | `getValue` JSON read-sniff remains. msgpack rows written by N on a format where N-1 also wrote msgpack are fine only if N added fields, not changed them. |
| Read on-disk format written by N with N-1 binary | **No** | Direct format replacement; N-1 cannot open N's layout. Binary downgrade is not a supported path. |
| Mixed-version running cluster (N-1 metad + N metad) | **No** | No entry-level Raft version; old nodes cannot parse new entries. Two-voter rolling upgrade is prohibited (see §4). |
| Rolling upgrade N-1 → N in place | **No today** | Requires the Raft-entry version + compatibility layer first (see §6 judgment). |
| Binary rollback (swap image back) | **No** | The direct-format-replacement design makes N-1 unable to read N's state. Do not attempt it. |
| Data / schema rollback via restore | **Yes — the *only* safe rollback** | Whole-cluster `RestoreBackupToNewCluster` into a fresh cluster ID. See §3. |
| Backup N → restore N-1 | **No** | Restore targets a new cluster ID with the backup's own format; cross-version restore of a committed backup is not provided. Restore always reconstructs from the backup format itself. |
| Fresh-checkpoint / LOCK etc. re-read | Yes | Restore fetches `manifest.Files` and rebuilds; collect excludes `LOCK`. |

## 3. The one rollback path: restore to a new cluster

Because there is no binary downgrade, **rollback == disaster recovery**: "undo"
an upgrade by restoring a committed, verified backup into a brand-new cluster
and cutting traffic over. This is the same path as whole-cluster loss recovery
and is already gated:

- Harness / gate: `scripts/soak/run-v21-metadata-restore.sh` (Layer 2), wired into
  `scripts/verify.sh -l drill` and `-l full` as the `drill: metadata-restore` step.
- Design: `docs/runbooks/metadata-disaster-recovery.md`.
- Procedure runbook: `docs/runbooks/metadata-backup-restore-drill.md`.
- Release / rollback decisions: `docs/runbooks/rolling-upgrade.md` and
  `docs/runbooks/rollback.md`.
- Pre-release backup-freshness check: `scripts/check-backup-freshness.sh`.

Operating rules for rollback-via-restore:

1. **Backup must be non-stale.** Before any release, the newest committed backup
   must be within the configured `--backup-interval` + margin, else rollback
   loses whatever was written since the last backup. There is currently **no
   automated guard** for this in `verify.sh` — treat it as an operator check
   (tracked as a known gap, see §5).
2. **Never restore old Raft dirs.** Restore provisions a *fresh* metad data +
   empty Raft directory; you re-bootstrap the Raft cluster, you do not replay old
   WALs.
3. **Readiness-gate before serving.** Restore stays `ServiceUnavailable` until
   the replica probe passes, so a partial/corrupt restore cannot serve stale
   reads.
4. **Verify before you trust.** Run the backup `verify` step
   (`/api/v1/backups/$id/verify`) on the selected backup before restore.

## 4. Schema / wire format evolution rules (do / don't)

For as long as NUFS ships the direct-format-replacement design, these are the
only changes that are safe:

**DO**
- **Add** an optional msgpack field to a row/record (old readers ignore it).
- **Add** a new `RaftLogOp` value **without** renumbering existing values
  (op byte is the switch; appending is safe, reordering is not).
- Add a new `Version` discriminator inside an existing payload, mirroring
  `ConditionalBatch.Version`.
- Keep `getValue`'s JSON read-sniff alive while any JSON rows may remain.

**DON'T (these are breaking and require the versioning foundation first)**
- Renumber or reuse an existing `RaftLogOp`.
- Change the semantic meaning of an existing field (same bytes, new meaning).
- Remove a field or a versioned payload branch.
- Start mixed-version operation (upgrade leader before followers, two-voter
  rolling, etc.) without an entry-level version and an explicit
  cluster-minimum-version gate.

If breaking a change is unavoidable, it is **not** an in-place upgrade — it is a
backup → restore migration to a new cluster ID with the whole fleet on the new
binary.

## 5. Known gaps (facts, not assumptions)

- **No backup-freshness gate** in `verify.sh` / release automation: nothing
  fails a release if the newest backup is older than `--backup-interval`.
- **No entry-level Raft version** and no cluster-minimum-version enforcement:
  mixed-version rolling is impossible to do safely until these exist.
- **Real-environment drills not yet run**: per
  `p0-production-gates-results.md`, real three-node Kubernetes failover, real
  S3 whole-cluster backup→restore, and real network-fault injection have not
  been executed (configs exist; evidence does not).

## 6. Judgment: when must we add versioning?

Add Raft-entry/on-disk versioning **only when** you need **N-1/N mixed-version
rolling upgrades on a live cluster** (nodes of two binaries coexisting and both
reading/writing the same Raft log / data). Until that point, adding a full
version layer on metadata serialization is pure cost with no live legacy data to
protect, and it touches the highest-risk code (metadata encoding) — which the
repo treats specially (see the auth-gate / signature-stability conventions).

If you do reach that point, the three concrete hooks already present are:

1. `RaftLogOp` first byte → extend to carry an entry-format version.
2. `ConditionalBatch.Version` → model for per-payload version tags.
3. `getValue` JSON sniff + msgpack self-description → old readers keep working
   across additive changes.

Until then: **backup → restart whole fleet on new binary → restore is the only
safe upgrade path. Never attempt a binary downgrade.**
