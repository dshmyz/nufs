# NUFS Production Architecture v1 Design

## Purpose

NUFS v1 should be a production-grade distributed object/file storage system with one deeply verified path before more advanced features expand. The v1 architecture freezes the golden path around S3 Put/Get, strongly consistent metadata, replicated chunk storage, deterministic repair, and operator visibility.

The goal is not to remove future capabilities such as erasure coding, cross-zone replication, lifecycle, tiering, or FUSE. The goal is to keep them outside the production-critical path until the smallest reliable storage loop is provably correct.

## Production v1 Scope

In scope:

- S3 `PutObject`, `GetObject`, `HeadObject`, bucket create/list/delete.
- Metadata service backed by Pebble and a real three-node Raft cluster.
- Datanode chunk storage with replicated writes.
- Repair, GC, and scrub as managed background tasks.
- Admin UI/API for health, topology, repair queue, storage usage, and safe cluster changes.
- Docker-based deployable admin server and local multi-node test harness.

Out of scope for v1 production readiness:

- Erasure coding as a default write path.
- Cross-zone active-active replication.
- Automatic tier migration.
- Complex POSIX/FUSE semantics beyond compatibility testing.
- Multi-tenant billing or policy engines beyond basic auth/RBAC.

## Architectural Principle

The production system has four deep modules:

1. **MetadataLog**: owns namespace, inode, chunk metadata, bucket metadata, node state, and task state. Its interface is Raft-backed and strongly consistent for all mutations.
2. **ChunkIO**: owns durable chunk writes and reads across datanodes. Its interface hides replica fanout, quorum, retries, and checksum validation.
3. **ObjectCommitter**: owns S3 object write state. Its interface turns a client body into either a committed object or a recoverable failed attempt.
4. **BackgroundTaskRunner**: owns repair, scrub, GC, rebalance, and future maintenance jobs through one task state model.

Each module should expose a small interface and hide the complicated implementation behind it. Tests should cross those same interfaces, not reach through them.

## Golden Write Path

The v1 production write path is:

```text
S3 PutObject
  -> validate bucket and object key
  -> create ObjectWriteAttempt
  -> allocate chunk metadata in Pending state
  -> write chunk bytes to datanode replicas
  -> verify quorum durability and checksum
  -> commit chunk metadata
  -> update inode/object pointer atomically
  -> mark ObjectWriteAttempt committed
  -> return ETag
```

The important rule: an object is not visible as committed until the inode/object pointer update succeeds after enough chunk replicas are durable.

## Write State Model

Object writes should use explicit states:

- `AttemptPending`: request accepted internally, no visible object mutation yet.
- `ChunksAllocated`: chunk IDs exist, but data may not be durable.
- `ChunksDurable`: enough replicas acknowledged each chunk.
- `ObjectCommitted`: inode/object pointer references the new chunk set.
- `AttemptFailed`: write failed before visibility.
- `RecoveryNeeded`: data and metadata diverged and a background task must reconcile.

Failure handling:

- Failure before `ChunksDurable`: return 5xx/503 and leave no visible object update.
- Failure after `ChunksDurable` but before `ObjectCommitted`: return 5xx and enqueue GC/recovery metadata.
- Failure after `ObjectCommitted`: return success unless response write itself fails.
- Existing object overwrite must preserve the old object until the new object commits.

## Metadata Service

Metadata is the source of truth. For production v1, all mutating metadata calls must go through Raft:

- bucket create/delete
- inode create/update/delete
- chunk allocate/commit/delete
- node register/heartbeat/state change
- repair/GC/scrub task state transitions
- admin cluster state mutations

Required guarantees:

- A committed metadata write survives leader failover.
- Snapshot restore preserves namespace and chunk metadata.
- Followers either serve verified read-index reads or return a clear redirect/not-leader error.
- Rolling restart of one node at a time does not block writes if quorum remains.

Required tests:

- three-node leader failover after committed bucket/object write
- follower read after leader commit
- snapshot restore with bucket, inode, chunk, node, and task records
- minority partition cannot elect a writer
- restart of follower catches up without manual repair

## Datanode Storage

Datanodes own bytes, checksums, and local disk health. They do not decide object visibility.

Chunk write requirements:

- Write intent is durable before chunk file mutation.
- Chunk file contains versioned header, chunk ID, length, checksum, and payload.
- Replica write returns success only after data is durable enough for the configured policy.
- Corrupt reads fail closed and report replica state to metadata.
- Local disk failure marks affected replicas failed and enqueues repair.

Replica policy for v1:

- Default replication factor is 3.
- Write quorum for production is all selected replicas unless explicitly configured lower for test/local mode.
- Reads may try replicas in order, but must verify checksum before returning data.

## ObjectCommitter Module

`ObjectCommitter` should become the module that hides the multipart state, chunk allocation, chunk writes, metadata commit, and cleanup rules from S3 handlers.

Proposed interface:

```go
type ObjectCommitter interface {
    Put(ctx context.Context, req PutObjectRequest) (PutObjectResult, error)
    Get(ctx context.Context, req GetObjectRequest) (ObjectReader, error)
}
```

The S3 handler should translate HTTP/S3 concerns into this interface. It should not directly know the full write state machine.

## Background Task System

Repair, GC, scrub, and rebalance should share one task model:

- `Queued`
- `Leased`
- `Running`
- `Succeeded`
- `Retrying`
- `DeadLetter`
- `Canceled`

Each task has:

- task ID
- type
- target resource
- idempotency key
- lease owner
- attempt count
- next run time
- last error
- created/updated timestamps

This gives operators one mental model and lets tests validate all background work through one interface.

Repair rules:

- Never create duplicate replicas for one node.
- Never remove a repair task until metadata confirms the new replica state.
- Prefer same-zone repair for v1 unless policy explicitly requires topology spread.
- Repair must be idempotent under repeated worker crashes.

GC rules:

- Only delete chunks not referenced by any committed object or active write attempt.
- Recovery-needed attempts must be resolved before deleting durable chunk bytes.
- Dry-run mode must be available and visible in admin.

Scrub rules:

- Scrub reads data, verifies checksum, marks corrupt replicas, and enqueues repair.
- Scrub must be rate-limited below foreground traffic.

## Admin And Operations

The admin surface must be a production control plane, not just a dashboard.

Required v1 views:

- cluster overview: health, capacity, write/read error rates
- Raft status: leader, peers, applied index, snapshot age
- datanodes: state, disk usage, last heartbeat, failed disks
- repair queue: backlog, retry count, dead-letter tasks
- GC/scrub: last run, orphan count, corrupt count
- audit log: user, action, target, result

Required v1 actions:

- add/remove dynamic cluster
- enter/exit node maintenance
- trigger repair for chunk/node/bucket
- trigger GC dry run
- pause/resume background task type

Safety:

- All mutating admin actions require authenticated user identity.
- All mutating admin actions write audit records.
- Default/dev secrets must fail production startup.

## Security

Minimum production security:

- TLS for all admin and data-plane external endpoints.
- mTLS or signed internal requests between metadata, datanodes, and gateways.
- JWT secret must come from environment/secret manager in production.
- S3 credentials must be loaded from secret-managed config, not repository files.
- Admin token should not rely on long-lived localStorage for production deployment.
- Audit logs must include user, action, target, result, and request ID.

## Observability

Production readiness requires SLO-oriented metrics:

- S3 request rate, latency, and error rate by operation.
- PutObject phase latency: allocate, replica write, commit chunk, update inode.
- Raft apply latency, leader changes, snapshot duration, follower lag.
- Datanode write/read latency, checksum failures, disk failures.
- Repair backlog, retry rate, success/failure count.
- GC orphan count and bytes freed.
- Scrub corrupt replica count.
- Capacity used/free by node, rack, zone, and bucket.

Alerts:

- no Raft leader
- Raft follower lag above threshold
- PutObject 5xx rate above threshold
- repair backlog age above threshold
- corrupt replicas found
- any node disk failed
- capacity above high-water mark

## Deployment Model

v1 deployment has these runnable roles:

- `metad`: metadata + Raft + ops API
- `datanode`: chunk storage + ops API
- `s3gw`: S3-compatible gateway
- `admin-server`: admin API + built web SPA

Local development must support a full multi-node stack. Production deployment must avoid dev defaults.

Startup validation:

- production mode refuses default JWT secrets
- production mode refuses empty S3 credential source
- production mode refuses single-node Raft unless explicitly configured as dev
- production mode warns or fails if TLS is disabled

## Verification Gates

A change cannot be considered production-ready unless these pass:

```bash
cd nufs-core
go test ./...
```

```bash
cd nufs-admin
go test ./...
```

```bash
cd nufs-admin/web
npm run build
```

Additional production gates:

- real three-node Raft integration test
- write failure injection test
- repair idempotency test
- snapshot restore test
- admin Docker image build
- local multi-node smoke test: create bucket, put object, kill metadata leader, get object

## Migration From Current Architecture

Phase 1: stabilize current P0 fixes.

- Keep admin web and backend buildable.
- Keep real Raft failover test in the suite.
- Keep S3 write failure and repair idempotency regressions.

Phase 2: introduce `ObjectCommitter`.

- Move write state transitions out of S3 handler.
- Add explicit write attempt metadata.
- Add recovery task for durable chunks without committed object pointer.

Phase 3: unify background tasks.

- Move repair queue, GC, scrub, and rebalance into one task model.
- Add task leases and dead-letter state.
- Add admin controls and metrics.

Phase 4: harden deployment and security.

- Add production mode config validation.
- Add secret source requirements.
- Add TLS/mTLS defaults.
- Add Docker and local cluster smoke tests.

## Self-Review

- Placeholder scan: no placeholder requirements remain.
- Consistency check: the document consistently treats metadata as the source of truth and datanodes as byte durability modules.
- Scope check: v1 is intentionally limited to replicated S3 storage and operational recovery. Advanced features are deferred.
- Ambiguity check: visibility and failure rules for object writes are explicit.
