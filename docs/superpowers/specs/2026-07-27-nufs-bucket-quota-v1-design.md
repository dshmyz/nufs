# NUFS Bucket Quota v1 Design

## Goal

Add first-class bucket capacity quotas so operators can cap how much data and
how many objects a bucket may accept. The feature must work through the S3
gateway, metad ops API, Admin UI, Prometheus metrics, and alerting rules.

This is a production resource-governance feature, not a billing system. v1
focuses on preventing obvious single-bucket capacity abuse while keeping the
write path simple and recoverable.

## Scope

In scope:

- Per-bucket `max_bytes` quota.
- Per-bucket `max_objects` quota.
- Ops APIs to read, set, and clear quota.
- S3 `PutObject` admission checks.
- Admin bucket-list display and quota editing.
- Prometheus quota ratio metrics and warning/critical alerts.
- Tests for metadata persistence, ops API behavior, S3 rejection, and Admin API
  proxying.

Out of scope for v1:

- Per-tenant or per-user quota.
- Write rate limiting.
- Strict distributed reservation across multiple S3 gateways.
- Billing, chargeback, or quota history.
- Multipart-specific reservation semantics beyond the existing object commit
  flow.

## Existing Context

The codebase already has useful quota primitives:

- `metadata.BucketQuota` with `MaxSizeBytes`, `MaxObjects`, and
  `MaxChunkCount`.
- `metadata.QuotaManager` with in-memory checks and optional persistence.
- Pebble keys `prefixQuota` and `prefixQuotaUsage`.
- `PebbleStore.SetQuotaManager`, `SaveQuota`, `SaveUsage`, and `loadQuotas`.
- `ComputeAllBucketUsage`, including a fast path using bucket stats.

The missing pieces are the public service interface, ops API, correct S3
admission semantics, Admin UI integration, and Prometheus/alerting coverage.

## Data Model

Use the existing `metadata.BucketQuota` as the internal model:

```go
type BucketQuota struct {
    MaxSizeBytes int64
    MaxObjects   int64
    MaxChunkCount int64
}
```

v1 exposes only:

- `max_bytes`
- `max_objects`

`MaxChunkCount` stays internal and unused by public APIs. It can support future
chunk-count governance if needed.

Unset or zero values mean unlimited. Negative values are invalid.

Quota data is persisted under the existing quota prefix. Usage remains derived
from `ComputeAllBucketUsage`; stored quota usage should not become the only
source of truth.

## Metadata Interface

Add a narrow quota service interface:

```go
type BucketQuotaService interface {
    GetBucketQuota(ctx context.Context, bucket string) (*BucketQuota, error)
    SetBucketQuota(ctx context.Context, bucket string, quota *BucketQuota) error
    DeleteBucketQuota(ctx context.Context, bucket string) error
    CheckBucketQuota(ctx context.Context, bucket string, additionalBytes int64, additionalObjects int64) error
}
```

`MetadataService` includes `BucketQuotaService`.

Rules:

- `GetBucketQuota` returns `ErrBucketNotFound` if the bucket does not exist.
- `GetBucketQuota` returns `nil, nil` if the bucket exists but has no quota.
- `SetBucketQuota` validates the bucket exists and rejects negative limits.
- `DeleteBucketQuota` is idempotent for an existing bucket.
- `CheckBucketQuota` compares current usage plus requested delta against quota.

The check should use current bucket usage from `ComputeAllBucketUsage` or an
equivalent single-bucket helper. It must not rely on `MaxChunkSize` estimates for
known object sizes.

## S3 Gateway Behavior

`PutObject` performs quota admission in the object committer.

Flow:

1. Read bucket metadata.
2. If `ContentLength >= 0`, check quota with:
   - `additionalBytes = ContentLength`
   - `additionalObjects = 1` only when this key does not already exist
3. Write chunks using the existing object write attempt flow.
4. Before updating the inode pointer, check quota again using the actual
   `totalSize`.
5. If quota is exceeded, mark the write attempt `failed`, return an S3 XML error,
   and let object write GC remove uncommitted chunks when safe.

Overwrite semantics:

- If the key already exists, bytes delta is `newSize - oldSize`.
- If the key already exists, object delta is `0`.
- Negative byte delta is allowed.

Unknown content length:

- Skip the early byte check.
- Always perform the final check using actual `totalSize`.

S3 error mapping:

- Error code: `QuotaExceeded`
- HTTP status: `403 Forbidden`
- Message: concise bucket quota exceeded reason.

## Ops API

Add metad endpoints:

- `GET /api/v1/buckets/{bucket}/quota`
- `PUT /api/v1/buckets/{bucket}/quota`
- `DELETE /api/v1/buckets/{bucket}/quota`

`GET` response:

```json
{
  "bucket": "photos",
  "quota": {
    "max_bytes": 10737418240,
    "max_objects": 100000
  },
  "usage": {
    "used_bytes": 5368709120,
    "objects": 42000
  },
  "ratios": {
    "bytes": 0.5,
    "objects": 0.42
  }
}
```

If no quota exists, `quota` is `null` and ratios are `0`.

`PUT` request:

```json
{
  "max_bytes": 10737418240,
  "max_objects": 100000
}
```

`DELETE` clears the quota.

Admin server proxies the same paths under:

- `GET /api/v1/clusters/{cluster}/buckets/{bucket}/quota`
- `PUT /api/v1/clusters/{cluster}/buckets/{bucket}/quota`
- `DELETE /api/v1/clusters/{cluster}/buckets/{bucket}/quota`

## Admin UI

Update the bucket page to show quota and usage in the bucket table:

- Used bytes.
- Object count.
- Max bytes.
- Max objects.
- Used ratio badge.
- Edit quota action.
- Clear quota action.

The UI should stay operational and compact. Use the existing table layout and a
small modal or inline panel for editing quota.

Editing rules:

- Empty input means unlimited.
- Values are stored as raw bytes and object counts.
- Display can format bytes for readability.
- Failed quota API calls should show inline error text and leave current values
  unchanged.

## Prometheus And Alerts

Extend metad `/metrics` with:

```text
nufs_bucket_quota_used_ratio{bucket="photos",resource="bytes"} 0.5
nufs_bucket_quota_used_ratio{bucket="photos",resource="objects"} 0.42
nufs_bucket_quota_limit{bucket="photos",resource="bytes"} 10737418240
nufs_bucket_quota_limit{bucket="photos",resource="objects"} 100000
nufs_bucket_quota_usage{bucket="photos",resource="bytes"} 5368709120
nufs_bucket_quota_usage{bucket="photos",resource="objects"} 42000
```

Only buckets with configured quota need ratio/limit metrics. Usage metrics may
be emitted for all buckets if inexpensive.

Add alerts:

- `NUFSBucketQuotaBytesHigh`: bytes ratio > 0.80 for 15m.
- `NUFSBucketQuotaBytesCritical`: bytes ratio > 0.95 for 5m.
- `NUFSBucketQuotaObjectsHigh`: object ratio > 0.80 for 15m.
- `NUFSBucketQuotaObjectsCritical`: object ratio > 0.95 for 5m.

## Consistency Model

v1 is admission-control with eventual usage precision. It is not a distributed
reservation system.

Implication:

- Concurrent writes from multiple S3 gateways may briefly overshoot a quota.
- A final pre-commit check narrows the window for single write paths.
- Strict quota reservation can be added in v2 if required.

This trade-off is acceptable for v1 because it delivers operational guardrails
without adding a new cross-gateway consensus path to every object write.

## Testing

Metadata tests:

- Set/get/delete quota persists through Pebble.
- Negative quota values are rejected.
- `CheckBucketQuota` rejects byte and object overages.
- Overwrite checks use deltas, not full object count increments.

Ops API tests:

- `PUT` then `GET` quota returns quota, usage, and ratios.
- `DELETE` clears quota.
- Missing bucket returns `404`.
- Negative values return `400`.

S3 tests:

- PutObject below quota succeeds.
- PutObject exceeding byte quota returns `QuotaExceeded`.
- PutObject exceeding object quota returns `QuotaExceeded`.
- Overwriting an existing object with a smaller object is allowed.
- Unknown content length is checked before final inode update.

Admin tests:

- Admin backend proxies quota paths to metad.
- Frontend build passes with quota types and editing UI.

Metrics tests:

- `/metrics` includes quota limit, usage, and ratio metrics.
- Alerting YAML includes four quota alerts and parses successfully.

## Rollout

1. Ship metadata quota API with no configured quotas.
2. Expose Admin read-only usage/quota display.
3. Enable editing quotas for test buckets.
4. Add alerts in warning-only mode.
5. Apply production quotas after observing usage accuracy.

## Self-Review

- Placeholder scan: no placeholder requirements remain.
- Consistency check: quota is modeled as metadata-owned policy, while S3 gateway
  performs admission using metadata interfaces.
- Scope check: v1 excludes rate limiting and strict reservation to keep this
  implementable in one focused plan.
- Ambiguity check: overwrite, unknown content length, and unset quota semantics
  are explicit.
