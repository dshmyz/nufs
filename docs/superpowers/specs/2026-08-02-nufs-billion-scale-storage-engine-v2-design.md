# NUFS Billion-Scale Storage Engine V2.1 Design

## 1. Purpose

This document defines the bottom-layer storage architecture required for NUFS to support two capacity tiers without relying on gateway-specific behavior:

- **Tier 1:** 100 million logical files per cluster.
- **Tier 2:** 1 billion logical files per cluster, including a stress target of 100 million extents on one DataNode.

The workload assumption is mixed:

- 70% of files are at most 64 KiB.
- 25% are between 64 KiB and 64 MiB.
- 5% are larger than 64 MiB.

Hot data uses three replicas. A write succeeds after any two replicas durably persist a segment `BatchCommit`. Immutable cold data may later transition to Reed-Solomon EC 6+3. A DataNode must become available within 30 seconds after an ordinary restart or process crash without scanning all stored data.

NUFS has not entered production, so this design replaces the legacy one-chunk-per-file layout directly. It does not include dual-format reads, online legacy migration, or rollback to the old on-disk format.

V2.1 uses the segment commit log as the local durability authority and treats Pebble as a derived location index. This removes the independent synchronous index WAL from the foreground path, reduces each local group commit to one durability barrier, and makes recovery depend on a bounded committed segment-log delta.

## 2. Scope

In scope:

- Segment/container-based DataNode storage.
- Persistent local extent index, segment commit log, incremental manifest, checkpoint, and bounded recovery.
- Small-file packing without one filesystem inode per logical file.
- Generation-fenced overwrite and deletion.
- Compaction, scrub, inventory, repair, and GC.
- Bounded inode/extent-page layouts, sparse extent exceptions, and stable shard ownership.
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

1. **LocalExtentStore** owns durable local extent reads, writes, and deletes. Its interface hides segment layout, commit log, derived index, checkpoint, compression, and encryption.
2. **MetadataStoreV2** owns namespace, bounded inode/extent-page layouts, sparse extent exceptions, placement, generations, and lifecycle state through Raft-backed shards.
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
- Every acknowledged local batch is represented by one durable `BatchCommit` record.
- Pebble index loss can make a node unavailable but cannot change which payload bytes were acknowledged.
- Range reads authenticate and checksum only the frames they return, without reading an entire large extent.
- Node failure, mass deletion, and rebalance create bounded batch tasks rather than one Raft task per extent.

## 4. Capacity Profiles

| Metric | 100-million tier | 1-billion tier |
|---|---:|---:|
| Logical files per cluster | 100 million | 1 billion |
| Files per metadata shard | at most 20 million | at most 30 million |
| Normal active extent index per DataNode | at most 20 million | at most 30 million |
| Single-node stress target | 30 million extents | 100 million extents |
| DataNode time to `DataReady` after process crash | at most 30 seconds | at most 30 seconds |
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
  index/
    Pebble files
  journal/
    change-{sequence}.log
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

### 5.3 Segment and record format

```text
SegmentHeader
RecordHeader + FrameIndex + Frames + RecordTrailer
RecordHeader + FrameIndex + Frames + RecordTrailer
...
BatchCommit
SegmentFooter
```

Each record stores:

- record magic and format version;
- extent ID and generation;
- logical and stored lengths;
- compression codec;
- encryption key ID;
- frame size and frame count;
- header and frame-index CRC32C;
- whole-payload checksum for end-to-end replica verification;
- record framing length and trailer checksum.

Data records are divided into independently readable frames:

- The default frame size is 64 KiB.
- Each frame has its own CRC32C and AEAD tag.
- Compression is applied independently per frame.
- Uncompressed frame offsets are computed directly; compressed records use the checksummed frame index.
- A range read fetches and authenticates only intersecting frames.
- Small-file records normally contain one frame.

`BatchCommit` contains the logical stream ID, stream-local sequence, committed record count, record-offset bounds, and a checksum over the batch descriptors. Records after the last valid `BatchCommit` are uncommitted tail data and are discarded during recovery.

The sealed footer stores record count, total payload bytes, extent ID bounds, segment checksum, last committed stream sequence, creation time, and seal time.

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
  commit_stream
  commit_sequence
```

The index is the local location authority for serving reads, while committed segment-log records are the durability authority. Segment scans are verification and disaster-rebuild tools, not a normal startup mechanism.

## 6. Local Write Transaction

### 6.1 Single-replica sequence

1. Validate extent ID, generation, length, checksum, and idempotency key.
2. Reserve an offset in the active segment.
3. Append record header, frame index, frames, and trailer.
4. Append `BatchCommit` for the group-commit batch.
5. Execute one `fdatasync` on the active segment.
6. Apply committed locations to the bounded in-memory delta overlay.
7. Return `DurableReceipt` for every request covered by the commit.
8. Apply the committed mutations asynchronously to Pebble.

`BatchCommit` is the foreground durability point. Pebble may lag the committed sequence but cannot lead it. A crash can lose unsynced Pebble updates because recovery replays committed segment records. A Pebble entry pointing beyond the last committed segment sequence is invalid and is removed during recovery. A committed record absent from Pebble is replayed before the node serves that extent.

### 6.2 Idempotency and generations

- Repeating the same extent ID, generation, checksum, and idempotency key succeeds without duplicating the logical write.
- Reusing the extent ID and generation with a different checksum returns a conflict.
- A higher generation may supersede a lower generation.
- Reads return only the requested/current generation.
- Delete operations include an operation ID and exact generation.

### 6.3 Cluster acknowledgement

Metadata issues an epoch-fenced placement token containing extent ID, generation, PG ID, placement epoch, allowed replica set, metadata term, and expiry. A DataNode rejects a write whose token is expired, targets another node, or is older than the node's durable PG fence. This prevents a stale coordinator from writing into a source epoch after cutover.

The caller writes to three authorized replicas in parallel. Metadata marks the extent `Ready` after any two DataNodes return durable receipts and the metadata Raft mutation commits. A missing third replica produces a `ReadyDegraded` state and an immediate high-priority repair task. One durable receipt is never sufficient for success.

### 6.4 Group commit

Each disk owns separate small-record and data-record commit streams. A batch never spans two segment files. Each stream groups at most 256 requests or waits at most 2 ms, appends its own monotonically sequenced `BatchCommit`, and executes one `fdatasync`; every request waits for that one barrier. After sync, the stream updates a bounded in-memory committed-delta index before acknowledging, so immediate reads observe the write. Reads consult this overlay before Pebble. Writes larger than one extent may form their own batch so they do not cause small-request head-of-line blocking.

## 7. Commit Log, Manifest, Index Flush, and Recovery

### 7.1 Commit-log records

Data, tombstone, relocation, and seal records are length-delimited, checksummed entries in active segments. Logical records contain:

- stream ID and stream-local sequence;
- operation (`PUT`, `TOMBSTONE`, `RELOCATE`, `SEAL_SEGMENT`, or `INDEX_SAFE`);
- extent ID and generation;
- segment ID and offset;
- stored and logical lengths;
- checksum;
- record CRC and batch-commit sequence.

There is no independent synchronous index WAL in the foreground path. Pebble may keep its own internal WAL with synchronous durability disabled because segment commit streams are authoritative. The committed-delta overlay is bounded by the flush-recovery budget. A committed log range can be retired only after each stream's safe sequence is represented in stable Pebble SSTs, an `INDEX_SAFE` safe-sequence vector and manifest delta are durable, and no active compaction or recovery checkpoint depends on it.

### 7.2 Segment sealing

Sealing stops allocation, drains the active batch, appends the footer and `SEAL_SEGMENT` in one committed batch, and syncs once. It then appends and syncs a manifest delta before the path transition becomes authoritative. Directory entries involved in a rename are fsynced. Recovery trusts the manifest, not directory placement alone.

### 7.3 Incremental manifest

The manifest uses a bounded snapshot-plus-delta layout:

```text
MANIFEST-SNAPSHOT-{sequence}
MANIFEST-LOG-{sequence}
CURRENT
```

Seal, trash, compaction, and disk-state changes append compact deltas. A new snapshot is generated after 10000 deltas or 64 MiB. Recovery reads one snapshot and one bounded log; publishing a segment never rewrites a list proportional to total segment count.

### 7.4 Index flush and checkpoints

Pebble is flushed when the measured recovery-cost estimate reaches ten seconds, with initial guardrails of 100000 committed mutations or five seconds of foreground time, whichever comes first. After flush and SST sync, an `INDEX_SAFE` record and manifest delta publish the safe-sequence vector for all commit streams.

A full checkpoint is generated every 30 minutes, before controlled shutdown or upgrade, and on operator request. It contains:

- a Pebble checkpoint;
- immutable manifest;
- active-segment safe offsets;
- last applied safe-sequence vector;
- checksums for all checkpoint metadata.

The checkpoint is built and synced in a temporary directory, then atomically published. The latest three valid checkpoints are retained.

### 7.5 Recovery

Recovery performs only bounded work:

1. Validate the superblock.
2. Read `CURRENT` and validate its manifest.
3. Open the current Pebble index and read its safe-sequence vector; if it is invalid, restore the newest valid checkpoint and report degraded recovery.
4. Replay committed segment-log batches after each stream's safe sequence into Pebble and the committed-delta overlay.
5. Validate only active-segment tails after recorded safe offsets.
6. Discard records not covered by a valid `BatchCommit`.
7. Initialize bounded caches and transition to `DataReady`.
8. Resume metadata/inventory synchronization and transition to `InventoryReady`.
9. Start asynchronous sealed-segment verification and eventually report `FullyVerified`.

All disks recover in parallel. The online flush trigger is derived from measured replay throughput and must leave at least 20 seconds of the 30-second process-crash budget for Pebble open, manifest validation, cache setup, and variance. Initial hard caps are 100000 committed mutations and 256 MiB of unindexed committed records per disk. Each disk has at most two active segments and at most 128 MiB of uncommitted tail.

Recovery objectives are failure-class specific:

| Failure class | Objective |
|---|---:|
| clean restart or process crash | `DataReady` within 30 seconds |
| newest checkpoint or manifest delta corrupt | fallback within 60 seconds using an already published checkpoint generation, without copying the full index |
| complete local index corruption with data intact | leave placement within 30 seconds; offline/isolated rebuild is hours-scale |
| physical disk loss | no local rebuild promise; repair from replicas |
| full segment inventory rebuild | disaster-recovery workflow, outside the 30-second SLO |

## 8. Read Path and Caching

Reads query the local index, acquire a cached segment descriptor, use `pread` for the record header and intersecting frame-index entries, read only required frames, validate identity and generation, authenticate/decrypt/decompress each frame, verify frame checksums, and stream the result. Full reads additionally verify the whole-payload checksum.

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
- Encryption, compression, and checksum are per frame; the record retains an end-to-end checksum for full validation.
- DataNodes know only extent IDs and generations, never namespace names.
- Overwrite writes a new record and generation, then metadata atomically switches the file layout.

This keeps lifecycle isolation while reducing filesystem inode count by orders of magnitude.

## 10. Delete and Compaction

### 10.1 Logical deletion

Metadata commits deletion or replacement, creates an idempotent delete batch, and sends generation-fenced tombstones to DataNodes. A DataNode appends the tombstones and a `BatchCommit` to the appropriate commit stream, syncs once, updates its committed-delta overlay, and acknowledges. Physical bytes remain until compaction.

### 10.2 Compaction candidates and scoring

A sealed segment is eligible when any condition holds:

- dead bytes are at least 30%;
- dead records are at least 40%;
- local corruption requires evacuation;
- a small segment has less than 70% live data;
- the disk is under space pressure;
- data changes tier or encryption key.

Eligibility does not imply immediate compaction. The scheduler ranks candidates using reclaim benefit and operational cost:

```text
score =
  reclaimable_bytes / expected_read_bytes
  * age_factor
  * space_pressure
  * media_health_factor
  / foreground_latency_penalty
```

The scheduler enforces a steady-state total data write-amplification budget of 2.5 and reduces concurrency when foreground latency, disk queue depth, or write amplification exceeds its budget.

### 10.3 Compaction transaction

1. Fence the source segment at a compaction generation.
2. Select records whose index still points to the source.
3. Copy and validate live records into a new segment.
4. Sync the new segment.
5. Append and sync conditional `RELOCATE` records.
6. Apply index changes only if old locations still match.
7. Publish a new manifest.
8. Move the old segment to trash and attach a reader epoch.
9. Delete it after the manifest is durable, no reader can reference the old epoch, and the configured minimum safety window has passed.

### 10.4 Capacity protection

- Reserve at least 10% of disk for emergency compaction, and never less than twice the live bytes of the largest concurrent compaction set.
- Reserve 5% for commit-log headroom, checkpoints, manifests, and index growth.
- Below 15% free space, prioritize reclaim work.
- Below 10%, reject new ordinary writes while allowing reads, deletes, and repair migration out.
- Below 5%, force protective read-only mode.

Admission control also compares `time_to_full` with `time_to_reclaim`. It rejects or throttles writes before the fixed watermarks when projected reclaim cannot keep pace with allocation. Under emergency pressure, reader-epoch completion permits trash deletion earlier than the normal retention window.

## 11. Metadata V2

### 11.1 Inodes and extent pages

An inode contains fixed attributes, size, generation, layout type, optional single inline extent descriptor, extent-page count, and extent-root version. It does not contain an unbounded chunk map.

Layouts are:

- `Empty`;
- `InlineExtent` for one-extent files, containing the complete compact descriptor;
- `ExtentPages` for multi-extent files.

Extent pages are stored under:

```text
/extent-page/{inode_id}/{extent_root}/{page_no}
```

Each page holds at most 256 compact extent descriptors and therefore describes up to 4 GiB at a 16 MiB extent size. A descriptor contains extent ID, file offset, length, generation, checksum, PG ID, placement epoch, and storage class. Updates use copy-on-write pages followed by one atomic Raft mutation switching `inode.extent_root`. Old roots enter delayed GC.

### 11.2 Structured extent identity and sparse exceptions

Extent IDs are 128-bit structured identifiers containing logical partition, inode-local identity, extent ordinal, and an allocation nonce. Generation remains an explicit fenced field. Given an extent ID, metadata can route to the owning logical partition and locate the inode or extent page without a one-record-per-extent reverse index.

The normal healthy state lives only in the inode's inline descriptor or an extent page. There is no mandatory `/extent/{id}` KV for every extent. Sparse records are created only for non-normal state:

```text
/extent-exception/{extent_id}/{generation}
  ReadyDegraded
  Corrupt
  Repairing
  Migrating
  Deleting
  ECConversion
```

Repair completion or successful reconciliation removes the exception. This model avoids roughly one billion extra metadata KVs when one billion files each contain one ordinary extent, while retaining explicit state for the small fraction requiring intervention.

### 11.3 Placement groups and epochs

- The 100-million tier uses 4096 placement groups.
- The 1-billion tier uses 16384 to 65536 placement groups after capacity modeling.
- Each hot-data PG maps to three replica nodes across fault domains.
- Every extent stores both PG ID and placement epoch.
- Rebalance changes PG assignments without rewriting every extent metadata record, but old epochs remain resolvable until migration completes.
- Segment IDs and offsets remain DataNode-local details.

A PG migration record contains:

```text
pg_id
source_epoch
target_epoch
cutover_sequence
source_replicas
target_replicas
inventory_partition
migration_cursor
state
```

Writes after the cutover sequence use the target epoch. Reads resolve the extent's stored epoch and may consult both source and target while its inventory partition is migrating. Source replicas are removed only after every partition cursor completes and inventory reconciliation proves the target epoch complete.

### 11.4 Stable shard ownership

Inode and extent IDs encode a 16-bit stable logical-partition ID, not a physical Raft-group identity. The catalog maps logical partitions to physical Raft groups. Moving a logical partition updates the catalog through an explicit epoch-fenced handoff; IDs and callers do not change, and permanent per-record forwarding is unnecessary.

### 11.5 Directory partitioning

Normal directory entries remain colocated with their directory inode. A directory begins proactive partitioning at 500000 entries, 128 MiB of namespace values, sustained 2500 writes/s, or 60% shard CPU/write utilization, and must complete before one million entries, 256 MiB, 5000 writes/s, or 70% utilization.

A versioned directory map assigns sampled ordered name ranges to metadata shards. Lookup targets one range. Enumeration walks ranges in lexical order. Stale versions cause a routing refresh rather than speculative writes. Monotonic-name workloads use adaptive split points derived from recent samples instead of fixed hexadecimal ranges.

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

The logical budget per file is 300 to 800 bytes across namespace, inode/inline descriptor, extent-page amortization, sparse exceptions, and lifecycle records. Including Pebble index, bloom filters, Raft/Pebble internal logs, compaction, and snapshots, provision 0.75 to 1.5 KiB per file per metadata replica until scale measurements refine the bound.

| File count | One metadata replica | Three replicas |
|---:|---:|---:|
| 100 million | 75-150 GiB | 225-450 GiB |
| 1 billion | 0.75-1.5 TiB | 2.25-4.5 TiB |

An additional 40% free space is reserved for compaction, snapshots, and recovery. Metadata Raft/Pebble storage uses local enterprise NVMe.

## 12. Incremental Heartbeat and Inventory

The normal write path already submits durable receipts to metadata and therefore does not upload a second `EXTENT_DURABLE` event. The persistent change journal contains asynchronous or out-of-band changes: corruption, disk/segment loss, relocation, asynchronous third-replica completion, repair-created replicas, scrub findings, and completed deletions.

Every five seconds, heartbeat reports aggregate capacity/health and at most 10000 events or 4 MiB. Metadata acknowledges a sequence. DataNodes retain unacknowledged events, retransmit gaps, and retain at least 24 hours. Journal retention is additionally bounded by configurable bytes; reaching the hard bound stops destructive asynchronous actions and forces inventory reconciliation rather than allowing unbounded disk growth. Process restart changes `boot_id` but not the durable sequence.

Each node maintains 65536 inventory partitions selected by extent-ID hash. The foreground path updates only fixed-size commutative summaries: count, live bytes, XOR hash, sum hash, and maximum generation. Reconciliation first compares these summaries. For a mismatching partition it builds a temporary Merkle subtree from a stable index snapshot, narrows the differing ranges, and exchanges pages of at most 4096 entries. Foreground writes never update a full Merkle path, and no interface returns a full-node inventory in one response.

## 13. Repair, Scrub, and GC

### 13.1 Repair

Large-scale repair is represented by a Raft-persisted batch state machine keyed by PG, source epoch, target epoch, and resumable inventory cursor. Each lease processes 512 to 4096 extents and persists aggregate progress. A node failure creates at most one initial batch task per affected PG; workers lazily advance through inventory partitions instead of pre-creating partition tasks. Individual extent tasks are reserved for sparse checksum corruption and exceptions.

The workflow is queued, leased, copying, verifying, and committed, with retryable and permanent-failure states. Every copied extent remains generation-fenced and idempotent.

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

Old extents enter time-bucketed batch queues:

```text
/gc-bucket/{hour}/{logical_shard}/{batch_id}
```

Each batch contains a compressed, checksummed list of extent ID/generation pairs and a cursor. Workers issue and acknowledge tombstones in pages of 512 to 4096 entries. Default retention is 24 hours for overwrite/delete, one hour for abandoned writes, six hours for failed-repair temporary copies, and the reader-epoch safety rule for compacted segments. Active snapshots and transactions pin generations. GC verifies references, marks a batch deleting, sends generation-fenced tombstones, waits for quorum acknowledgement, and then removes queue entries and sparse exceptions in batches; normal descriptors disappear with their inode or old extent root. GC cost is proportional to expired batches, not total inodes or individual Raft task count.

## 14. Cold EC 6+3

All new data begins as three replicas. EC conversion requires an immutable file version, 30 days without modification by default, no active transaction/snapshot conflict, healthy replicas, a completed scrub, and sufficient fault-domain diversity.

The conversion transaction builds six data and three parity shards on nine distinct physical disks across at least three machines. No machine stores more than three shards, so loss of one machine loses at most the three shards tolerated by 6+3. Rack diversity is required when the deployment provides at least three racks. The transaction syncs and validates all shards, atomically switches metadata to EC, then schedules delayed deletion of the replicated form. Failure leaves metadata pointing to the original three replicas and treats partial EC shards as reclaimable orphans.

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
  journal/{commit,record,replay,change_journal}.go
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
  extent_layout.go
  extent_page.go
  extent_exception.go
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
  large_sequential_extent_size: 64MiB
  frame_size: 64KiB
  small_file_threshold: 64KiB
  small_segment_size: 1GiB
  data_segment_size: 4GiB
  max_records_per_segment: 1000000
  group_commit: {max_requests: 256, max_wait: 2ms}
  index:
    block_cache: 4GiB
    location_cache_entries: 1000000
    flush_recovery_budget: 10s
    flush_max_committed_records: 100000
    flush_max_interval: 5s
    checkpoint_interval: 30m
    retain_checkpoints: 3
  manifest: {snapshot_max_deltas: 10000, snapshot_max_log_bytes: 64MiB}
  compaction:
    dead_bytes_ratio: 0.30
    dead_records_ratio: 0.40
    max_disk_bandwidth_percent: 10
    min_trash_retention: 10m
    max_write_amplification: 2.5
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
    journal_retention: 24h
    journal_max_bytes: 8GiB
  capacity:
    compaction_reserve_percent: 10
    metadata_reserve_percent: 5
    reject_writes_free_percent: 10
    force_read_only_free_percent: 5
```

Startup rejects unsafe combinations, including segment sizes below extent size, frame sizes that do not divide configured extents, insufficient reserve, and index-flush/recovery limits that violate the measured 30-second recovery bound. Extent size is frozen per file generation: normal files default to 16 MiB, while explicitly classified large sequential files may use 64 MiB extents aligned with EC data units.

## 17. Runtime Protection and Observability

DataNode serving states are `Starting`, `Recovering`, `DataReady`, `InventoryReady`, `FullyVerified`, `Degraded`, `ReadOnly`, `Draining`, and `Failed`. `DataReady` permits existing reads and idempotent writes carrying a valid placement token, but the node is not selected for new placements until `InventoryReady`. Each transition records cause and time. Backpressure bounds queued batches, unsynced bytes, commit-log/index lag, active segments, disk space, index compaction debt, and concurrent bodies.

Required metrics cover extent read/write/fsync latency, frame read amplification, group batch size, recovery duration, commit-log replay count, index safe-sequence lag, live/dead segment bytes, time-to-full, time-to-reclaim, compaction amplification/backlog, change-journal backlog, repair/GC debt bytes, oldest scrub age, disk state, metadata-shard skew, PG migration progress, and index cache hit ratio.

Metric labels are limited to node, disk, operation, result, storage class, and priority. Extent IDs, segment IDs, names, and error text are forbidden as labels.

### 17.1 Operator interface

The supported operator tools are part of the release, not ad-hoc debugging scripts:

```text
inspect-superblock        inspect-manifest
inspect-segment           lookup-extent
verify-record             force-seal
force-checkpoint          show-recovery-budget
show-space-debt           pause/resume-compaction
pause/resume-repair       quarantine-segment
rebuild-index --offline   reconcile-partition
explain-placement         drain-disk
drain-node
```

Mutating commands require an operation ID, dry-run support, audit record, conflict detection, explicit target scope, cancellation or resumable progress where applicable, and a machine-readable result.

### 17.2 Format evolution

The superblock and manifest publish minimum readable and writable format versions plus enabled feature bits. Rolling upgrade preflight checks the cluster capability matrix. Writers do not emit a new record feature until every required reader can consume it. Downgrade is rejected after an irreversible feature bit is committed. Unknown mandatory features force read-only or failed state rather than automatic conversion.

### 17.3 Maintenance capacity admission

Cluster capacity planning includes background-work throughput, not only free bytes:

```text
required_scrub_Bps = protected_bytes / scrub_cycle_seconds
required_repair_Bps = largest_failure_domain_bytes / repair_window_seconds
required_compaction_Bps = measured_ingest_Bps * (target_total_write_amplification - 1)
```

Placement admission verifies that healthy disks and network links can satisfy foreground demand plus these maintenance minima after losing the largest configured fault domain. A cluster that lacks this headroom enters capacity-warning state before raw space is exhausted. Operator status reports current sustainable throughput, required throughput, debt bytes, and estimated completion time for scrub, repair, GC, compaction, and PG migration.

### 17.4 Recovery and disaster operations

Metadata Raft groups, catalog state, manifests, and format capabilities are included in coordinated metadata checkpoints and restore drills. Data payloads are protected by replicas or EC rather than copied into metadata backups. Local derived indexes are checkpointed on their own disks but are not treated as the only copy of data ownership.

If an index is lost while segments remain, the node is fenced out of placement and `rebuild-index --offline` creates a new index generation without modifying source segments. If a segment is partially readable, `inspect-segment` and a read-only salvage workflow emit verified records into a new quarantined generation; operators never edit index or manifest files manually. Restore, index rebuild, node replacement, PG recovery, and catalog loss each have a regularly exercised runbook with measured RTO and explicit abort conditions.

## 18. Verification Strategy

### 18.1 Correctness and model tests

Unit tests cover formats, frame-index and frame AEAD/checksums, `BatchCommit`, generation fencing, idempotency, replay, manifest snapshot/delta publication, tail truncation, checkpoint fallback, extent-page copy-on-write, directory routing, PG epoch migration, partition summaries, repair-batch leases, GC-batch retention, and EC recovery.

A deterministic reference-model test executes at least one million random writes, overwrites, reads, range reads, deletes, checkpoints, seals, compactions, crashes, recoveries, corruption events, and segment losses. Seeds are persisted for reproduction.

### 18.2 Crash matrix

Crashes are injected after record header, partial frame, frame index, trailer, `BatchCommit`, segment sync, asynchronous Pebble apply/flush, `INDEX_SAFE`, footer, manifest delta/snapshot, `CURRENT` rename, checkpoint stages, relocation stages, and trash transition. Every case verifies no acknowledged write is lost, no corrupt data is returned, no index points beyond the last committed sequence, and process-crash recovery finishes within 30 seconds.

### 18.3 Fault and concurrency tests

Fault injection includes short write, delayed write, fsync error, latent corruption, bit flip, ENOSPC, EIO, read-only remount, commit-log/manifest/checkpoint/index loss, disk loss, network partition, node restart, Raft leader change, and competing workers. Real staging tests include kill -9, power loss, NVMe removal, network port loss, and rapid disk exhaustion.

Concurrency covers generation races, overwrite/read, delete/repair, compaction/overwrite, compaction/delete, checkpoint under load, PG migration/repair, EC conversion/delete, and shard split/rename. Race-enabled tests and a 72-hour mixed-load soak are mandatory.

### 18.4 Scale tests

- Tier 1 creates 100 million logical metadata records, 20 million files per metadata shard, and 20 million extents per DataNode.
- Tier 2 creates one billion logical metadata records, normally 30 million extents per DataNode, and a separate 100-million-extent single-node stress case.
- Every inventory, repair, and GC operation remains paginated.
- Synthetic valid segment/index/checkpoint fixtures may accelerate setup, but one long-running cluster test must use the real write path.
- Recovery qualification uses a cold page cache, maximum permitted commit-log lag, maximum manifest delta, and 100-million-extent local index.
- Steady-state performance is measured after overwrite/delete/compaction reaches equilibrium, not only on an empty disk.

## 19. Performance Acceptance

Reference DataNode hardware is 32 CPU cores, 128 GiB memory, four enterprise NVMe disks each sustaining at least 1.5 GiB/s, XFS, and 25 GbE. Reference metadata hardware is 16-32 cores, 64-128 GiB memory, local enterprise NVMe, and 25 GbE.

Load tests use open-loop arrival control and report offered load, admitted load, queueing, P50, P99, and P99.9 without coordinated-omission correction errors. Throughput targets must hold at or below 70% sustained CPU/disk saturation with checksums, frame AEAD, adaptive compression, replication, and background maintenance enabled. Recovery tests use cold caches and the maximum allowed safe-sequence lag.

DataNode targets:

- at least 20000 64-KiB mixed small writes/s;
- at least 1 GiB/s sequential 16-MiB extent writes;
- at least 50000 cached and 20000 NVMe random small reads/s;
- group-commit P99 at most 10 ms;
- local extent-write P99 at most 30 ms;
- range-read P99 at most 20 ms;
- range-read physical amplification at most requested bytes plus two frames and metadata;
- `DataReady` within 30 seconds after clean restart or process crash;
- compaction or repair increases foreground P99 by at most 20%;
- steady-state total data write amplification at most 2.5;
- Go heap at most 16 GiB at the 100-million-extent stress target;
- open file descriptors at most 10000 per DataNode.

Metadata shard targets:

- at least 10000 sustained mutations/s;
- inode and extent-page point-read P99 at most 10 ms;
- Raft leader transition at most 10 seconds;
- snapshot P99 increase at most 30%;
- restart of a 20-million-file shard at most 60 seconds.

Results with durability, checksum, encryption, or generation fencing disabled do not count.

## 20. Delivery Phases

1. **Format and harness:** freeze framed record, commit-log, manifest, and checkpoint formats; implement model, crash, fault, benchmark, replay-budget, and capacity tools.
2. **SegmentStore:** implement append, frame-level range read, `BatchCommit`, seal, derived local index, idempotency, and generation-fenced deletion.
3. **Durability and recovery:** implement single-fsync group commit, asynchronous index flush, incremental manifest, checkpoint, bounded recovery, and crash matrix.
4. **Small-file and reclaim:** implement segment classes, compression, record encryption, tombstones, compaction, trash, and capacity protection.
5. **Metadata V2:** implement fixed inode, inline extent, extent pages, logical partitions, PG epochs/migration, proactive directory partitioning, and cross-shard rename.
6. **Incremental control plane:** implement asynchronous change journal, heartbeat, partition summaries/on-demand Merkle, repair batches, GC batches, and scrub.
7. **EC 6+3:** implement stripe conversion, atomic metadata switch, degraded reads, repair, and reheating.
8. **Scale qualification:** run 100-million and 1-billion profiles, 100-million-extent node stress, 72-hour soak, failure drills, and operating runbooks.

Each phase must produce independently testable software and meet its phase-specific correctness gate before the next phase becomes part of the production path.

## 21. Release Gates

NUFS Storage Engine V2.1 is not releasable if any condition is true:

- startup scans all segments;
- all local extents are loaded into an unbounded in-memory map;
- inventory, repair, or GC exposes an unpaginated full listing;
- ordinary successful writes are redundantly uploaded through heartbeat journal;
- node loss creates one Raft repair task per extent;
- a durable acknowledgement can be followed by data loss after one-node failure;
- a local durable batch requires more than one foreground fsync barrier;
- stale compaction/delete can replace a higher generation;
- corrupt or unverifiable data can be returned successfully;
- a range read must read/authenticate an entire large extent;
- process-crash `DataReady` exceeds 30 seconds at target scale;
- normal writes continue below the hard free-space threshold;
- metadata ownership changes implicitly with a hash ring;
- a PG epoch can be removed before inventory proves migration complete;
- small logical files create individual filesystem files;
- the crash matrix, 72-hour soak, real power-loss test, or scale qualification has not passed.

## 22. Operational SLOs

| SLO | Target |
|---|---:|
| Replica loss detection | at most 30 seconds |
| High-risk repair start | at most 60 seconds |
| `InventoryReady` after process restart with intact journal | at most 60 seconds |
| Normal journal backlog drain | at most 5 minutes |
| Global inventory digest comparison | every 6 hours |
| Tier 1 inventory convergence | at most 24 hours |
| Tier 2 inventory convergence | at most 72 hours |
| Expired GC task P99 delay | at most 1 hour |
| High-risk repair backlog | less than 15 minutes of arrivals |
| Normal repair backlog | less than 24 hours of arrivals |

These SLOs are continuously measured in scale and failure tests and become alert thresholds in production.
