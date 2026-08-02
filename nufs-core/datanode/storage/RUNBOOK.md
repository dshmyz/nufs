# NUFS Storage Engine V2.1 — Operator Runbook & Failure Drills

Phase 8 deliverable (design §17.1/§18.3/§22). This runbook covers the
V2.1 `datanode/storage` engine. The legacy chunkstore has its own
runbook until V2.1 reaches parity.

## 1. Serving states (§17)

A DataNode moves through:

```
Starting → Recovering → DataReady → InventoryReady → FullyVerified
                              ↘ Degraded / ReadOnly / Draining / Failed
```

- **DataReady**: local segment-log replay done; reads/writes served.
- **InventoryReady**: metadata/inventory sync resumed.
- **FullyVerified**: asynchronous sealed-segment verification finished.

Each transition records cause and time.

## 2. Operator tools (§17.1)

Supported tools are part of the release:

| Tool | Purpose |
|---|---|
| `inspect-superblock` | dump disk/cluster identity, format version, checksum |
| `inspect-manifest` | dump sealed-segment manifest (CURRENT + generation) |
| `inspect-segment` | walk a segment file: records, BatchCommits, framing |
| `lookup-extent` | resolve an extent → segment/offset via the local index |
| `verify-record` | read + verify a record's frames/checksums |
| `force-seal` | seal an active segment (drain batch, footer, manifest) |
| `force-checkpoint` | flush Pebble + publish INDEX_SAFE checkpoint |
| `show-recovery-budget` | report measured replay throughput vs 30s budget |
| `show-space-debt` | report time-to-full, time-to-reclaim, debt bytes |
| `pause/resume-compaction` | control background compaction |
| `pause/resume-repair` | control background repair |
| `quarantine-segment` | mark a segment for evacuation (corruption) |
| `rebuild-index --offline` | offline index rebuild from segments (hours-scale) |
| `reconcile-partition` | force inventory reconciliation for one partition |
| `explain-placement` | show PG epoch → replica node resolution |
| `drain-disk` / `drain-node` | retire a disk/node with migration |

Mutating commands require: an operation ID, dry-run support, audit
record, conflict detection, explicit target scope, cancellation or
resumable progress, and a machine-readable result (§17.1).

## 3. Failure drills (§18.3)

Each drill must verify the §18.2 invariants: no acknowledged write is
lost, no corrupt data is returned, no index points beyond the last
committed sequence, and process-crash DataReady ≤ 30s.

### 3.1 Process crash (kill -9)

```bash
# Preconditions: write load running; note last DurableReceipt.
kill -9 <datanode-pid>
# Restart; verify:
#   1. DataReady within 30s (journal shows duration).
#   2. All acknowledged extents read back byte-exact.
#   3. No unacknowledged write surfaces as "successful".
```

Automated equivalent: `storage/segment/crash_test.go` (crash matrix at
every §18.2 point) + `TestRecovery_ManyWritesReopen`.

### 3.2 Power loss

```bash
# Same as kill -9 but also drops the page cache (cold recovery):
sync; echo 3 > /proc/sys/vm/drop_caches
# Restart and verify recovery duration + data integrity.
# Recovery qualification uses a cold page cache (§18.4).
```

### 3.3 NVMe removal / disk loss

```bash
# A disk is failed/removed. Expected:
#   1. Change journal records EventDiskLost (§12).
#   2. Repair batch created for affected PGs (§13.1, one batch per PG).
#   3. Inventory reconciliation proves target epoch before source
#      replicas are removed (§11.3).
# No local rebuild promise; repair from replicas (§7.5).
```

### 3.4 Network partition

```bash
# Partition a node from metadata. Expected:
#   1. Heartbeat loss → lease expiry → replica marked lost ≤30s (§22).
#   2. High-risk repair starts ≤60s.
#   3. On reconnect, change-journal gaps retransmit by sequence.
```

### 3.5 Rapid disk exhaustion (ENOSPC)

```bash
# Fill a disk. Expected (§10.4):
#   1. 15% free: prioritize reclaim (compaction).
#   2. 10% free: reject ordinary writes, allow reads/deletes/repair-out.
#   3. 5% free: force protective read-only.
#   4. Admission control rejects before the watermark when
#      time_to_full < time_to_reclaim.
```

## 4. Operational SLOs (§22)

| SLO | Target |
|---|---|
| Replica loss detection | ≤ 30s |
| High-risk repair start | ≤ 60s |
| Normal journal backlog drain | ≤ 5 min |
| Global inventory digest comparison | every 6h |
| Tier 1 inventory convergence | ≤ 24h |
| Tier 2 inventory convergence | ≤ 72h |
| Expired GC task P99 delay | ≤ 1h |
| High-risk repair backlog | < 15 min arrivals |
| Normal repair backlog | < 24h arrivals |

## 5. Release gates (§21)

Before release, all of the following must hold (verified by
`datanode/storage/release_gate_test.go` + the crash matrix):

- startup does not scan all segments;
- no unbounded in-memory map of all local extents;
- inventory/repair/GC are paginated;
- ordinary writes are not redundantly uploaded through heartbeat journal;
- node loss does not create one Raft repair task per extent;
- a durable acknowledgement cannot be followed by data loss after
  one-node failure;
- a local durable batch requires exactly one foreground fsync barrier;
- stale compaction/delete cannot replace a higher generation;
- corrupt or unverifiable data is never returned successfully;
- a range read does not read/authenticate an entire large extent;
- process-crash DataReady ≤ 30s at target scale;
- normal writes stop below the hard free-space threshold;
- metadata ownership does not change implicitly with a hash ring;
- a PG epoch cannot be removed before inventory proves migration complete;
- small logical files do not create individual filesystem files;
- the crash matrix, 72h soak, real power-loss test, and scale
  qualification have all passed.
