# NUFS P0 Production Gates Design

## Goal

Close the four production blockers identified in the current readiness review: stabilize datanode connection reuse, establish CI gates, validate a real three-node Raft deployment through Helm, and validate backup/restore plus multi-node fault recovery.

## Scope and sequencing

The work is split into four independently verifiable sub-projects:

1. Fix and stabilize `TestReplicator_ConnectionReuse`.
2. Add CI for core/admin tests, frontend build, vet, and the fast production verification gate.
3. Add a repeatable three-node Raft + Helm deployment verification.
4. Add repeatable backup/restore and multi-node fault-injection verification.

The sequence is intentional: the unit-test failure is fixed first; CI then prevents regressions; deployment verification runs only after the local gates are green; backup and fault-injection checks build on the deployment harness.

## Architecture

The production path remains unchanged. The connection-reuse fix must deepen the existing replicator connection-pool seam rather than introduce a second networking implementation. The CI workflow calls existing Make targets and scripts where they already provide the canonical behavior. Deployment and disaster-recovery checks are implemented as idempotent verification scripts around the existing Helm chart and backup tooling, with diagnostics preserved on failure.

The verification layers are:

```text
unit regression -> package tests/vet -> CI fast gate -> real 3-node Helm smoke
                                      -> backup/restore + network fault drill
```

## Connection reuse behavior

The pool must reuse one healthy connection per stable endpoint/pool key, avoid duplicate concurrent dials for the same key, and remove/replace broken connections after a failed operation. The regression test must assert both the observable dial bound and successful replication, and must be repeatable without relying on timing sleeps beyond bounded synchronization.

## CI behavior

CI must run on pull requests and the default branch. It must use reproducible Go and Node setup, cache dependencies, run the core and admin tests, run `go vet`, build the admin frontend, and execute `nufs-core/scripts/verify.sh --level fast` (or the existing Make wrapper). Each job must fail on non-zero exit and upload relevant logs/artifacts for integration failures.

## Real deployment verification

The Helm verification must deploy three metadata voters, enough datanodes for the configured replication policy, and the S3 gateway into an ephemeral Kubernetes namespace. It must verify bucket/object write-read behavior, identify and terminate the current metadata leader, verify re-election and post-failover reads/writes, and collect pod descriptions/logs on failure. It must not treat a two-node or `--allow-insecure-dev` compose stack as production evidence.

## Backup, restore, and fault injection

The backup/restore check must use a real S3-compatible repository such as MinIO, create committed metadata and object data, run the supported backup path, restore into a clean target, and compare metadata and object bytes. The fault drill must exercise leader termination, inter-node delay/loss, and datanode unavailability, then verify convergence, readiness, and repair outcomes. Every drill must emit machine-readable PASS/FAIL status, timing, and artifact locations.

## Acceptance criteria

- `TestReplicator_ConnectionReuse` passes repeatedly (minimum 20 consecutive runs) with no connection-count flake.
- Core/admin tests, frontend build, `go vet`, and the fast production verification gate pass in CI.
- A real three-node Helm deployment completes write, leader failure, re-election, read/write, and readiness checks.
- A real backup/restore run proves metadata and object-content equivalence.
- Multi-node fault injection completes with no data corruption and a ready, converged cluster.
- Existing unrelated working-tree changes are preserved.

## Non-goals

- No broad storage-engine rewrite.
- No change to the public S3/FUSE API.
- No replacement of Helm, backup, or fault-injection tooling with a parallel deployment system.
- No claim of production readiness based only on local unit tests.
