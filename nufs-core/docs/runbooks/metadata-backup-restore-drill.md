# Metadata Backup / Restore Drill Runbook

This runbook covers the recurring **metadata data-loss drill**: it machine-checks
the two independent guarantee layers behind "元数据集群怎么保证数据不丢失", and
archives auditable evidence for each.

- Layer 1 — **raft majority**: a real 3-metad raft cluster withstands the worst
  single-failure case (a whole metad process **and its disk** simultaneously
  destroyed) without losing a single committed object.
- Layer 2 — **backup / restore**: a full-cluster loss is rescued through the
  production backup → restore chain (source → backup → destroy → restore to a
  new cluster ID → readiness gate → RTO).

- Harness: `scripts/soak/run-v21-metadata-restore.sh`
- Gate: `scripts/soak/run-v21-metadata-restore.sh` PASS/FAIL
- Complementary coverage: `leader-failover-drill.md` (the RTO/graceful-degradation
  drill); `metadata-disaster-recovery.md` (the design).

## How each layer prevents data loss

| Fail-safe layer | Guards against | Mechanism | Survivable loss |
|-----------------|----------------|-----------|-----------------|
| 1. raft majority | single node crash / disk failure / network partition | committed metadata is replicated to ≥ `(N+1)/2` nodes before ack; durability comes from the raft WAL (bbolt fsync), *not* the FSM | up to `(N-1)/2` nodes of a 3-node cluster |
| 2. backup / restore | ≥ half the nodes dying at once, or the whole cluster | `CreateBackupCheckpoint` (PBL3) → S3 repository → `RestoreBackupToNewCluster` into a brand-new cluster ID → readiness gated until replica probe passes | entire cluster, restored to a fresh home |

Neither layer alone covers the other's failure: majority replication cannot help
when ≥ half the nodes vanish at once; a backup is useless against a single-node
bad disk if the cluster cannot continue serving. The two layers are complementary
— which is exactly what the drill proves end-to-end.

See `metadata-disaster-recovery.md` for the full RPO/RTO/risk table and the
per-topology "guaranteed or not" matrix.

## Running the drill

Run from `nufs-core/` (Go build rules apply). No Docker, no Minio required —
the only prerequisites are a Go toolchain and spare host ports.

```sh
./scripts/soak/run-v21-metadata-restore.sh --results /var/log/nufs-tests
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--metad N` | 3 | raft nodes (odd, ≥ 3) |
| `--nodes N` | 1 | datanodes |
| `--results DIR` | `/tmp/nufs-restore/results` | where `REPORT.txt` + evidence land |
| `--no-cleanup` | off | leave processes running on exit |
| `--keep-alive` | off | leave the cluster up for interactive inspection |

### Layer 1 — single-node bad disk (real processes)

Three real `metad-raft` + one `datanode` + the `s3` gateway are launched as
separate processes; the gate writes an object (its metadata coordinates
committed to the raft majority). Then **one whole metad — process and data+raft
directories — is destroyed** (SIGKILL + `rm -rf`):

- the surviving 2/3 nodes still form a majority,
- a new leader is elected,
- a *new* write commits — proving the already-committed metadata survived
  byte-for-byte and the cluster kept serving.

### Layer 2 — backup / restore (automated gates)

Real-process whole-cluster loss + S3 restore requires a Minio, which this host
lacks; the drill therefore drives the identical restore semantics through the
existing automated DR/restore/snapshot gates (restore is store-level identical
whether the staging root is a filesystem or S3):

- `tests/metadata_dr` — end-to-end DR: source store → `Publish` backup → destroy
  source → `RestoreBackupToNewCluster` (new cluster ID, non-empty-target /
  corrupt-artifact / atomicity safety) → chunk records intact → **ServiceUnavailable
  until the replica-probe readiness gate passes** → RTO gate.
- `metadata` `TestRestore*` — restore target / corrupt-artifact / atomicity safety.
- `metadata` snapshot + checkpoint — PBL1/PBL3, immutability, non-leader rejection.

The **full production restore runbook** (with a real S3/Minio repository and the
`nufs-backup` CLI) is below.

## Full production restore runbook (backup to S3)

When the restore objective must be exercised against a real S3 repository (the
production hard requirement for backup), follow this path. It needs a Minio (or
any S3 endpoint) reachable from the metad host.

1. **Provision backup runtime on metad.** Backup is leader-only and
   `validateBackupRuntimeConfig` hard-requires an S3 bucket:

   ```sh
   ./bin/metad --backup-enabled \
     --backup-s3-endpoint http://127.0.0.1:9000 \
     --backup-s3-bucket nufs-meta-backups \
     --backup-s3-access-key "$AK" --backup-s3-secret-key "$SK"
   ```

   (unauthenticated staging root may instead use `--backup-local-dir`; it is
   staging-only and cannot satisfy the production requirement.)

2. **Create a backup checkpoint** — leader-only `POST /api/v1/backups/create` or
   `./bin/nufs-backup create` (backup-coordinator `repository.Publish`, which
   captures a backup-checkpoint marker establishing the exact FSM position).

3. **List / verify / prune** — `./bin/nufs-backup list|verify|prune`; backup
   catalog rows + PBL3 checkpoints are durable in the source cluster.

4. **On whole-cluster loss** — `nufs-backup` has **no restore subcommand today**;
   restore goes through the store-level API (`RestoreBackupToNewCluster`) on a
   brand-new metad: it refuses a non-empty target, rejects corrupt artifacts
   without publishing, refuses to serve until the restored replica passes the
   availability probe, and assigns a **new cluster id** so the restored cluster
   can never be mistaken for the destroyed original.

5. **RTO envelope** — restore-readiness stays `ServiceUnavailable` until the
   replica probe passes, so partial/incorrect restores cannot serve stale reads;
   the automated DR gate asserts the whole path completes within the RTO budget.

## Interpreting the evidence

Evidence is archived per run under `<results>/restore-<ts>/`:

- `REPORT.txt` — verdict, `layer1_baddisk`, `layer2_restore`, metad/datanode
  counts, log root.
- `log/metad*.log`, `log/datanode*.log`, `log/s3.log` — per-process logs; the
  wiped metad's log ends at the SIGKILL boundary, the new leader's log shows the
  election + the post-kill committed write.
- `log/layer2-*.log` — the DR/restore/snapshot gate test output.

A **PASS** line prints only when Layer 1 elected a new leader and committed a new
write after the bad-disk, **and** all Layer 2 restore gates are green.

## Cleaning up after the drill

The drill tears down its own processes on exit (unless `--no-cleanup` /
`--keep-alive`). If an interrupted run left stray processes, they are killed on
the next `start_cluster` via the built-in `kill_stale` — a clean port slate is
what lets the 3-node simultaneous-start raft form its majority and elect a leader.
