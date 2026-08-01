# NUFS Billion-Scale Storage Engine V2 Design

## 1. Purpose

This document defines the bottom-layer storage architecture required for NUFS to support two capacity tiers without relying on gateway-specific behavior:

- **Tier 1:** 100 million logical files per cluster.
- **Tier 2:** 1 billion logical files per cluster, including a stress target of 100 million extents on one DataNode.

The workload assumption is mixed:

- 70% of files are at most 64 KiB.
- 25% are between 64 KiB and 64 MiB.
- 5% are larger than 64 MiB.

Hot data uses three replicas. A write succeeds after any two replicas durably persist both data and the local index WAL. Immutable cold data may later transition to Reed-Solomon EC 6+3. A DataNode must become available within 30 seconds after restart without scanning all stored data.

NUFS has not entered production, so this design replaces the legacy one-chunk-per-file layout directly. It does not include dual-format reads, online legacy migration, or rollback to the old on-disk format.

## 2. Scope

In scope:

- Segment/container-based DataNode storage.
- Persistent local extent index, WAL, manifest, checkpoint, and bounded recovery.
- Small-file packing without one filesystem inode per logical file.
- Generation-fenced overwrite and deletion.
- Compaction, scrub, inventory, repair, and GC.
- Metadata inode/extent separation and stable shard ownership.
- Placement groups and metadata directory partitioning.
- Hot three-replica to cold EC 6+3 lifecycle.
- Capacity, durability, scale, failure, and performance acceptance criteria.

Out of scope:

- Gateway protocol semantics.
- Compatibility with existing on-disk chunk files.
- Cross-region active-active metadata.
- Arbitrary distributed transactions; only namespace rename receives a cross-shard transaction.
- Inline payload storage in the metadata database.

## 3. Architectural Principles

The design has five deep modules:

1. **LocalExtentStore** owns durable local extent reads, writes, and deletes. Its interface hides segment layout, index, WAL, checkpoint, compression, and encryption.
2. **MetadataStoreV2** owns namespace, inode, extent layout, placement, generations, and lifecycle state through Raft-backed shards.
3. **PlacementCatalog** owns stable metadata shard assignments and placement groups. It is not on the per-file data path.
4. **InventoryReconciler** owns incremental node state, partition digests, Merkle comparison, and repair task creation.
5. **MaintenanceScheduler** owns compaction, scrub, repair, GC, rebalance, and EC conversion with shared resource budgets.

The system enforces these invariants:

- An index entry never points to data that was not made durable first.
- A confirmed write survives loss or immediate restart of any one replica node.
- A stale operation cannot overwrite or delete a higher generation.
- Startup work is bounded by changes since the last checkpoint, not total stored extents.
- Online maintenance is incremental, partitioned, resumable, and budgeted.
- No process builds an unbounded in-memory map of all cluster or node extents.
- Corrupt bytes are never returned as successful reads.

## 4. Capacity Profiles

| Metric | 100-million tier | 1-billion tier |
|---|---:|---:|
| Logical files per cluster | 100 million | 1 billion |
| Files per metadata shard | at most 20 million | at most 30 million |
| Normal active extent index per DataNode | at most 20 million | at most 30 million |
| Single-node stress target | 30 million extents | 100 million extents |
| DataNode time to Ready | at most 30 seconds | at most 30 seconds |
| Full background inventory convergence | at most 24 hours | at most 72 hours |
| Replica loss detection | at most 30 seconds | at most 30 seconds |

Logical file count and stored byte capacity are planned independently. Operators use both file-count and byte-capacity models when sizing a cluster.

## 5. Extent and Segment Layout

### 5.1 File-size classes

- Files at most 64 KiB are one record in a small-record segment.
- Files from 64 KiB through 16 MiB are one extent record.
- Larger files are split into fixed 16 MiB extents, with a variable-length final extent.
- The default small segment size is 1 GiB.
- The default data segment size is 4 GiB.
- A segment seals at its size limit, one million records, ten minutes of age, normal shutdown, or disk transition to maintenance/read-only.

### 5.2 Per-disk layout

```text
{disk}/
  superblock
  manifests/
    CURRENT
    MANIFEST-{generation}
  wal/
    index-{sequence}.wal
  index/
    Pebble files
  segments/
    small/active/
    small/sealed/
    data/active/
    data/sealed/
    compacting/
    trash/
  checkpoints/
    checkpoint-{sequence}/
```

The superblock contains the disk identity, cluster identity, format version, and checksum. A disk with a mismatched cluster identity or unsupported format version is rejected.

### 5.3 Segment format

```text
SegmentHeader
RecordHeader + Payload + RecordTrailer
RecordHeader + Payload + RecordTrailer
...
SegmentFooter
```

Each record stores:

- record magic and format version;
- extent ID and generation;
- logical and stored lengths;
- compression codec;
- encryption key ID;
- header CRC32C;
- payload CRC32C;
- record framing length and trailer checksum.

The sealed footer stores record count, total payload bytes, extent ID bounds, segment checksum, last WAL sequence, creation time, and seal time.

### 5.4 Local persistent index

The local Pebble index maps:

```text
extent_id ->
  segment_id
  offset
  stored_length
  logical_length
  generation
  state
  checksum
```

The index is the local location authority. Segment scans are verification and repair tools, not a normal startup mechanism.

## 6. Local Write Transaction

### 6.1 Single-replica sequence

1. Validate extent ID, generation, length, checksum, and idempotency key.
2. Reserve an offset in the active segment.
3. Append record header, payload, and trailer.
4. Execute `fdatasync` on the segment.
5. Append `PutExtent` to the local index WAL.
6. Execute `fsync` on the index WAL.
7. Apply the index mutation to Pebble/memtable.
8. Return `DurableReceipt`.

Data durability precedes index visibility. A record durable without a durable index entry is an orphan and is safe to reclaim. A durable index entry pointing to absent or invalid data is corruption and places the affected segment into quarantine.

### 6.2 Idempotency and generations

- Repeating the same extent ID, generation, checksum, and idempotency key succeeds without duplicating the logical write.
- Reusing the extent ID and generation with a different checksum returns a conflict.
- A higher generation may supersede a lower generation.
- Reads return only the requested/current generation.
- Delete operations include an operation ID and exact generation.

### 6.3 Cluster acknowledgement

The caller writes to three replicas in parallel. Metadata marks the extent `Ready` after any two DataNodes return durable receipts and the metadata Raft mutation commits. A missing third replica produces a `ReadyDegraded` state and an immediate high-priority repair task. One durable receipt is never sufficient for success.

### 6.4 Group commit

Each disk owns one append writer. It groups at most 256 requests or waits at most 2 ms. One data `fdatasync` covers the payload batch, followed by one WAL `fsync` covering its index mutations. Every request waits for both barriers. Writes larger than one extent may form their own batch so they do not cause small-request head-of-line blocking.

## 7. WAL, Manifest, Checkpoint, and Recovery

### 7.1 WAL records

WAL records are length-delimited, checksummed binary values containing:

- sequence;
- operation (`PUT`, `TOMBSTONE`, `RELOCATE`, or `SEAL_SEGMENT`);
- extent ID and generation;
- segment ID and offset;
- stored and logical lengths;
- checksum;
- record CRC.

WAL files rotate at 256 MiB or ten minutes. An old WAL can be removed only after its sequence is in Pebble, a published checkpoint includes it, the checkpoint manifest is durable, and no active compaction depends on it.

### 7.2 Segment sealing

Sealing stops allocation, drains the active batch, writes and syncs the footer, records and syncs `SEAL_SEGMENT`, publishes a new manifest, atomically updates `CURRENT`, and moves the file to the sealed directory.

### 7.3 Checkpoints

A checkpoint is generated every five minutes or after one million WAL records or 2 GiB of WAL, whichever comes first. It contains:

- a Pebble checkpoint;
- immutable manifest;
- active-segment safe offsets;
- last applied WAL sequence;
- checksums for all checkpoint metadata.

The checkpoint is built and synced in a temporary directory, then atomically published. The latest three valid checkpoints are retained.

### 7.4 Recovery

Recovery performs only bounded work:

1. Validate the superblock.
2. Read `CURRENT` and validate its manifest.
3. Open the newest valid index checkpoint.
4. Replay WAL after the checkpoint sequence.
5. Scan only active-segment tails after checkpoint safe offsets.
6. Truncate incomplete tail records.
7. Initialize bounded caches.
8. transition to `Ready`.
9. Start asynchronous sealed-segment verification.

WAL replay is bounded to one million records or 2 GiB. Each disk has at most two active segments, and tail scanning is bounded to 256 MiB per active segment. Exceeding these limits forces an earlier checkpoint during normal operation.

## 8. Read Path and Caching

Reads query the local index, acquire a cached segment descriptor, use `pread` for the requested range, validate record identity and generation, decrypt/decompress as required, verify checksum, and stream the result.

The bounded cache layers are:

- Pebble block and bloom-filter cache, defaulting to 20% of the DataNode memory limit.
- A sharded TinyLFU/LRU location cache with a default capacity of one million entries.
- A per-disk segment descriptor LRU with at most 4096 descriptors; active descriptors remain pinned.
- The operating-system page cache for payload data; payloads are not duplicated in an unbounded Go heap cache.

Errors are typed as not found, stale generation, checksum mismatch, segment unavailable, index corrupt, or decrypt failure. Checksum and decrypt failures never fall back to unverified bytes.

## 9. Small Files

Small files are independent logical records in 1 GiB small segments. There is no separate filesystem file and no mutable 1 MiB block containing an embedded list of 256 names.

- Files smaller than 4 KiB are not compressed by default.
- Files from 4 KiB through 64 KiB use sampled Zstd only when estimated savings are at least 10%.
- Encryption and checksum are per record, preserving random reads and record-level repair.
- DataNodes know only extent IDs and generations, never namespace names.
- Overwrite writes a new record and generation, then metadata atomically switches the file layout.

This keeps lifecycle isolation while reducing filesystem inode count by orders of magnitude.

## 10. Delete and Compaction

### 10.1 Logical deletion

Metadata commits deletion or replacement, creates an idempotent delete task, and sends generation-fenced tombstones to DataNodes. A DataNode appends and syncs the tombstone in its index WAL before acknowledging. Physical bytes remain until compaction.

### 10.2 Compaction candidates

A sealed segment is eligible when any condition holds:

- dead bytes are at least 30%;
- dead records are at least 40%;
- local corruption requires evacuation;
- a small segment has less than 70% live data;
- the disk is under space pressure;
- data changes tier or encryption key.

### 10.3 Compaction transaction

1. Fence the source segment at a compaction generation.
2. Select records whose index still points to the source.
3. Copy and validate live records into a new segment.
4. Sync the new segment.
5. Append and sync conditional `RELOCATE` records.
6. Apply index changes only if old locations still match.
7. Publish a new manifest.
8. Move the old segment to trash.
9. Delete it after a one-hour safety window.

### 10.4 Capacity protection

- Reserve 10% of disk for emergency compaction.
- Reserve 5% for WAL, checkpoints, manifests, and index growth.
- Below 15% free space, prioritize reclaim work.
- Below 10%, reject new ordinary writes while allowing reads, deletes, and repair migration out.
- Below 5%, force protective read-only mode.

## 11. Metadata V2

### 11.1 Inodes and extent pages

An inode contains fixed attributes, size, generation, layout type, optional single inline extent reference, extent-page count, and extent-root version. It does not contain an unbounded chunk map.

Layouts are:

- `Empty`;
- `InlineExtent` for one-extent files;
- `ExtentPages` for multi-extent files.

Extent pages are stored under:

```text
/extent-page/{inode_id}/{extent_root}/{page_no}
```

Each page holds at most 256 extent references and therefore describes up to 4 GiB at a 16 MiB extent size. Updates use copy-on-write pages followed by one atomic Raft mutation switching `inode.extent_root`. Old roots enter delayed GC.

### 11.2 Extent metadata

Each extent stores generation, logical length, checksum, placement group, lifecycle state, storage class, optional EC stripe ID, and creation time. It does not repeat complete DataNode addresses.

### 11.3 Placement groups

- The 100-million tier uses 4096 placement groups.
- The 1-billion tier uses 16384 to 65536 placement groups after capacity modeling.
- Each hot-data PG maps to three replica nodes across fault domains.
- PG configuration has an epoch.
- Rebalance changes PG assignments instead of rewriting every extent metadata record.
- Segment IDs and offsets remain DataNode-local details.

### 11.4 Stable shard ownership

Inode and extent IDs encode a 16-bit owner-shard ID. Creation chooses an owner once; later operations route by the encoded owner. Hash-ring changes do not silently change ownership. Explicit migration publishes a forwarding record until all references and routing metadata converge.

### 11.5 Directory partitioning

Normal directory entries remain colocated with their directory inode. A directory becomes range-partitioned when it exceeds one million entries, 256 MiB of namespace values, sustained 5000 writes/s, or 70% shard CPU/write utilization.

A versioned directory map assigns ordered name ranges to metadata shards. Lookup targets one range. Enumeration walks ranges in lexical order. Stale directory-map versions cause a routing refresh rather than speculative writes.

### 11.6 Cross-shard rename

Same-shard rename uses one Raft batch. Cross-shard namespace rename uses a dedicated coordinator record, source and target prepare records, one durable commit decision, idempotent application, and a recovery worker. Prepare lease expiry never independently decides rollback; the durable coordinator decision is authoritative.

### 11.7 Raft shard limits

- Three replicas per metadata shard.
- At most 30 million files per shard.
- Target sustained 10000 mutations/s per group.
- Batch at most 512 logical changes or 4 MiB.
- Group-commit wait at most 2 ms.
- Catalog uses a separate small Raft group.
- Snapshot, backup, and compaction are staggered between groups.

### 11.8 Metadata capacity

The logical budget per file is 400 to 1000 bytes across namespace, inode, extent state, extent-page amortization, and lifecycle records. Including Pebble index, bloom filters, WAL, compaction, and snapshots, provision 1 to 2 KiB per file per metadata replica.

| File count | One metadata replica | Three replicas |
|---:|---:|---:|
| 100 million | 100-200 GiB | 300-600 GiB |
| 1 billion | 1-2 TiB | 3-6 TiB |

An additional 40% free space is reserved for compaction, snapshots, and recovery. Metadata Raft/Pebble storage uses local enterprise NVMe.

## 12. Incremental Heartbeat and Inventory

Every local state change appends a persistent, monotonically sequenced change-journal event in the same transaction as its index mutation. Events include durable, deleted, corrupt, relocated, segment sealed/lost, and disk failed/recovered.

Every five seconds, heartbeat reports aggregate capacity/health and at most 10000 events or 4 MiB. Metadata acknowledges a sequence. DataNodes retain unacknowledged events, retransmit gaps, and retain at least 24 hours and ten million recent events. Process restart changes `boot_id` but not the durable sequence.

Each node maintains 65536 inventory partitions selected by extent-ID hash. Each partition tracks count, live bytes, maximum generation, and Merkle root. Reconciliation compares global roots, then partition and subtree roots, and exchanges only differing pages of at most 4096 entries. No interface returns a full-node inventory in one response.

## 13. Repair, Scrub, and GC

### 13.1 Repair

Repair tasks are Raft-persisted state machines with extent ID, generation, source, target, reason, priority, lease, attempts, and copied bytes. The workflow is queued, leased, copying, verifying, and committed, with retryable and permanent-failure states.

Priority order is:

1. only one healthy replica remains;
2. two replicas remain in an unsafe fault-domain combination;
3. metadata expects a replica that is locally absent;
4. checksum mismatch;
5. node drain;
6. PG rebalance;
7. tier migration.

Repair uses at most 20% of disk bandwidth by default. Compaction uses at most 10%. Foreground work retains at least 70%, and background concurrency decreases when foreground latency crosses its threshold.

### 13.2 Scrub

- Every online read verifies record framing and checksum.
- Background segment scrub sequentially validates sealed segments.
- Inventory scrub reconciles metadata and DataNode identity/generation sets.
- Hot segments complete a cycle within 30 days.
- Cold EC segments complete a cycle within 90 days.
- Newly sealed segments receive one scrub within 24 hours.
- Scrub cursors are persistent and resumable.

### 13.3 GC

Old extents enter a time-ordered queue:

```text
/gc-queue/{delete_after}/{extent_id}/{generation}
```

Default retention is 24 hours for overwrite/delete, one hour for abandoned writes, six hours for failed-repair temporary copies, and one hour for compacted segments. Active snapshots and transactions pin generations. GC verifies references, marks `Deleting`, sends generation-fenced tombstones, waits for quorum acknowledgement, marks `Deleted`, and later removes the extent metadata. GC cost is proportional to expired queue entries, not total inodes.

## 14. Cold EC 6+3

All new data begins as three replicas. EC conversion requires an immutable file version, 30 days without modification by default, no active transaction/snapshot conflict, healthy replicas, a completed scrub, and sufficient fault-domain diversity.

The conversion transaction builds six data and three parity shards across nine fault domains, syncs and validates all shards, atomically switches metadata to EC, then schedules delayed deletion of the replicated form. Failure leaves metadata pointing to the original three replicas and treats partial EC shards as reclaimable orphans.

Any six shards reconstruct data. A degraded read verifies the original extent checksum and immediately schedules repair. Reheated data may asynchronously return to three replicas.

## 15. Interfaces and Code Organization

The foreground local interface contains only `Write`, `Read`, `Delete`, and `Stat`, all carrying exact generations. Maintenance uses a separate interface for checkpoint, compact, scrub, inventory page, and health.

Proposed DataNode packages:

```text
nufs-core/datanode/storage/
  interface.go
  errors.go
  types.go
  segment/{store,writer,reader,record,segment,footer,allocator}.go
  index/{index,keys,codec,checkpoint}.go
  journal/{wal,record,replay,change_journal}.go
  manifest/{manifest,current,superblock}.go
  recovery/{recovery,active_tail,validation}.go
  maintenance/{compactor,scrubber,inventory,merkle,scheduler,bandwidth}.go
  encryption/{record_crypto,key_registry}.go
  testutil/{crash_store,fault_disk,fixtures}.go
```

Proposed metadata modules:

```text
nufs-core/metadata/
  inode_store.go
  extent_store.go
  extent_page.go
  placement_group.go
  directory_partition.go
  shard_catalog.go
  cross_shard_txn.go
  gc_queue.go
  inventory_reconcile.go
  repair_scheduler.go
```

The legacy production chunk store is removed after V2 reaches feature parity; no compatibility adapter remains.

## 16. Configuration Defaults

```yaml
storage:
  format_version: 2
  extent_size: 16MiB
  small_file_threshold: 64KiB
  small_segment_size: 1GiB
  data_segment_size: 4GiB
  max_records_per_segment: 1000000
  group_commit: {max_requests: 256, max_wait: 2ms}
  index:
    block_cache: 4GiB
    location_cache_entries: 1000000
    checkpoint_interval: 5m
    checkpoint_max_wal_records: 1000000
    checkpoint_max_wal_bytes: 2GiB
    retain_checkpoints: 3
  wal: {segment_size: 256MiB, retain_min_duration: 24h}
  compaction:
    dead_bytes_ratio: 0.30
    dead_records_ratio: 0.40
    max_disk_bandwidth_percent: 10
    trash_retention: 1h
  repair:
    max_disk_bandwidth_percent: 20
    max_network_bandwidth: 500MiB/s
    high_priority_workers: 8
    normal_workers: 4
  inventory:
    partitions: 65536
    page_size: 4096
    heartbeat_interval: 5s
    max_events_per_heartbeat: 10000
    max_heartbeat_bytes: 4MiB
  capacity:
    compaction_reserve_percent: 10
    metadata_reserve_percent: 5
    reject_writes_free_percent: 10
    force_read_only_free_percent: 5
```

Startup rejects unsafe combinations, including segment sizes below extent size, insufficient reserve, and WAL/checkpoint limits that violate the 30-second recovery bound.

## 17. Runtime Protection and Observability

DataNode states are `Starting`, `Recovering`, `Ready`, `Degraded`, `ReadOnly`, `Draining`, and `Failed`. Each transition records cause and time. Backpressure bounds queued batches, unsynced bytes, WAL backlog, active segments, disk space, index compaction debt, and concurrent bodies.

Required metrics cover extent read/write/fsync latency, group batch size, recovery duration, WAL replay count, live/dead segment bytes, compaction amplification/backlog, change-journal backlog, repair backlog, oldest scrub age, disk state, and index cache hit ratio.

Metric labels are limited to node, disk, operation, result, storage class, and priority. Extent IDs, segment IDs, names, and error text are forbidden as labels.

## 18. Verification Strategy

### 18.1 Correctness and model tests

Unit tests cover formats, checksums, generation fencing, idempotency, replay, manifest publication, tail truncation, checkpoint fallback, extent-page copy-on-write, directory routing, Merkle updates, repair leases, GC retention, and EC recovery.

A deterministic reference-model test executes at least one million random writes, overwrites, reads, range reads, deletes, checkpoints, seals, compactions, crashes, recoveries, corruption events, and segment losses. Seeds are persisted for reproduction.

### 18.2 Crash matrix

Crashes are injected after record header, partial payload, trailer, data sync, WAL append/sync, Pebble apply, footer, manifest write, `CURRENT` rename, checkpoint stages, relocation stages, and trash transition. Every case verifies no acknowledged write is lost, no corrupt data is returned, no index points to a truncated record, and recovery finishes within 30 seconds.

### 18.3 Fault and concurrency tests

Fault injection includes short write, delayed write, fsync error, latent corruption, bit flip, ENOSPC, EIO, read-only remount, WAL/manifest/checkpoint loss, disk loss, network partition, node restart, Raft leader change, and competing workers. Real staging tests include kill -9, power loss, NVMe removal, network port loss, and rapid disk exhaustion.

Concurrency covers generation races, overwrite/read, delete/repair, compaction/overwrite, compaction/delete, checkpoint under load, PG migration/repair, EC conversion/delete, and shard split/rename. Race-enabled tests and a 72-hour mixed-load soak are mandatory.

### 18.4 Scale tests

- Tier 1 creates 100 million logical metadata records, 20 million files per metadata shard, and 20 million extents per DataNode.
- Tier 2 creates one billion logical metadata records, normally 30 million extents per DataNode, and a separate 100-million-extent single-node stress case.
- Every inventory, repair, and GC operation remains paginated.
- Synthetic valid segment/index/checkpoint fixtures may accelerate setup, but one long-running cluster test must use the real write path.

## 19. Performance Acceptance

Reference DataNode hardware is 32 CPU cores, 128 GiB memory, four enterprise NVMe disks each sustaining at least 1.5 GiB/s, XFS, and 25 GbE. Reference metadata hardware is 16-32 cores, 64-128 GiB memory, local enterprise NVMe, and 25 GbE.

DataNode targets:

- at least 20000 64-KiB mixed small writes/s;
- at least 1 GiB/s sequential 16-MiB extent writes;
- at least 50000 cached and 20000 NVMe random small reads/s;
- group-commit P99 at most 10 ms;
- local extent-write P99 at most 30 ms;
- range-read P99 at most 20 ms;
- Ready within 30 seconds;
- compaction or repair increases foreground P99 by at most 20%.

Metadata shard targets:

- at least 10000 sustained mutations/s;
- inode and extent-page point-read P99 at most 10 ms;
- Raft leader transition at most 10 seconds;
- snapshot P99 increase at most 30%;
- restart of a 20-million-file shard at most 60 seconds.

Results with durability, checksum, encryption, or generation fencing disabled do not count.

## 20. Delivery Phases

1. **Format and harness:** freeze record/WAL/manifest/checkpoint formats; implement model, crash, fault, benchmark, and capacity tools.
2. **SegmentStore:** implement append, read, seal, local index, range read, idempotency, and generation-fenced deletion.
3. **Durability and recovery:** implement WAL, group commit, manifest, checkpoint, bounded recovery, and crash matrix.
4. **Small-file and reclaim:** implement segment classes, compression, record encryption, tombstones, compaction, trash, and capacity protection.
5. **Metadata V2:** implement fixed inode, inline extent, extent pages, placement groups, stable ownership, directory partitioning, and cross-shard rename.
6. **Incremental control plane:** implement change journal, heartbeat, inventory/Merkle, repair, GC queue, and scrub.
7. **EC 6+3:** implement stripe conversion, atomic metadata switch, degraded reads, repair, and reheating.
8. **Scale qualification:** run 100-million and 1-billion profiles, 100-million-extent node stress, 72-hour soak, failure drills, and operating runbooks.

Each phase must produce independently testable software and meet its phase-specific correctness gate before the next phase becomes part of the production path.

## 21. Release Gates

NUFS Storage Engine V2 is not releasable if any condition is true:

- startup scans all segments;
- all local extents are loaded into an unbounded in-memory map;
- inventory, repair, or GC exposes an unpaginated full listing;
- a durable acknowledgement can be followed by data loss after one-node failure;
- stale compaction/delete can replace a higher generation;
- corrupt or unverifiable data can be returned successfully;
- DataNode recovery exceeds 30 seconds at target scale;
- normal writes continue below the hard free-space threshold;
- metadata ownership changes implicitly with a hash ring;
- small logical files create individual filesystem files;
- the crash matrix, 72-hour soak, real power-loss test, or scale qualification has not passed.

## 22. Operational SLOs

| SLO | Target |
|---|---:|
| Replica loss detection | at most 30 seconds |
| High-risk repair start | at most 60 seconds |
| Normal journal backlog drain | at most 5 minutes |
| Global inventory digest comparison | every 6 hours |
| Tier 1 inventory convergence | at most 24 hours |
| Tier 2 inventory convergence | at most 72 hours |
| Expired GC task P99 delay | at most 1 hour |
| High-risk repair backlog | less than 15 minutes of arrivals |
| Normal repair backlog | less than 24 hours of arrivals |

These SLOs are continuously measured in scale and failure tests and become alert thresholds in production.
