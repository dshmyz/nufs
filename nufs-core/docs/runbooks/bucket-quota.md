# Bucket Quota Runbook

Bucket quota alerts indicate that a configured byte or object limit is close to
exhaustion. Act before the critical threshold so clients do not receive
`QuotaExceeded` responses.

## Alerts

| Alert | Resource | Threshold | Duration | Severity |
| --- | --- | --- | --- | --- |
| `NUFSBucketQuotaBytesHigh` | bytes | greater than 80% | 15m | warning |
| `NUFSBucketQuotaBytesCritical` | bytes | greater than 95% | 5m | critical |
| `NUFSBucketQuotaObjectsHigh` | objects | greater than 80% | 15m | warning |
| `NUFSBucketQuotaObjectsCritical` | objects | greater than 95% | 5m | critical |

The alert's `bucket` label identifies the affected bucket. The `resource` label
is either `bytes` or `objects`.

## Quick Triage

Set the metad operations endpoint and affected bucket:

```bash
export METAD_OPS_URL=http://127.0.0.1:8091
export BUCKET='<bucket>'
export BUCKET_PATH="$(python3 -c 'import os, urllib.parse; print(urllib.parse.quote(os.environ["BUCKET"], safe=""))')"

curl -fsS "$METAD_OPS_URL/metrics" \
  | grep 'nufs_bucket_quota'
curl -fsS "$METAD_OPS_URL/api/v1/buckets"
curl -fsS "$METAD_OPS_URL/api/v1/buckets/$BUCKET_PATH/quota"
```

Record the reported limit, usage, ratio, alert start time, and recent change
rate. Prometheus label values escape quotes, backslashes, and newlines, so use
the bucket list and encoded quota endpoint to disambiguate unusual names.
Check physical cluster capacity and datanode health before changing a byte
limit.

## Quota Semantics

- `max_bytes` limits the aggregate logical object bytes in the bucket.
- `max_objects` limits the number of objects in the bucket.
- A limit of `0` means unlimited. Its resource does not produce quota metric
  series or alerts.
- Bucket Quota v1 performs admission checks without a strict distributed
  reservation across gateways. Concurrent writes can briefly overshoot a
  limit. Treat a small transient overshoot as a known v1 consistency boundary,
  not automatically as counter corruption.
- Removing or setting a quota to zero removes admission protection; it does
  not create storage capacity.

## Determine Growth Or A Statistics Problem

1. Compare the quota endpoint with all three Prometheus series for the same
   bucket and resource. The ratio should equal usage divided by limit.
2. Review the series over time. A steady increase aligned with client writes
   indicates real growth. A discontinuous jump, negative usage, or a ratio
   inconsistent with usage and limit suggests a statistics issue.
3. Confirm object and byte growth from the owning workload, recent deployments,
   bulk imports, retention changes, and delete failures.
4. Check object-write recovery and GC metrics. A recovery backlog or failed
   cleanup can delay usage convergence.
5. Re-query metad after several scrape intervals. If usage remains inconsistent,
   preserve logs and metric samples, stop automated quota changes, and escalate
   to the storage team for counter reconciliation.

## Mitigation

Choose one of these actions with the bucket owner:

### Add Physical Capacity

1. Confirm the alert is for bytes and that physical capacity, not only the
   logical quota, is constrained.
2. Add or expand datanode capacity using the normal capacity procedure.
3. Wait for datanodes to report healthy capacity and for any required rebalance
   to settle.
4. Raise the bucket quota only after the new capacity is usable.

### Delete Data

1. Identify expired, duplicated, or otherwise removable objects with the owner.
2. Apply the normal retention or deletion workflow. Do not delete arbitrary
   objects solely to silence the alert.
3. Monitor object-write recovery and GC until cleanup has converged.
4. Re-query quota usage and verify the ratio falls below the threshold.

### Adjust The Limit

Use the bucket quota API with the approved replacement values:

```bash
curl -fsS -X PUT \
  -H 'Content-Type: application/json' \
  --data '{"max_bytes":<bytes>,"max_objects":<objects>}' \
  "$METAD_OPS_URL/api/v1/buckets/$BUCKET/quota"
curl -fsS "$METAD_OPS_URL/api/v1/buckets/$BUCKET/quota"
```

Keep the unaffected resource's current limit in the request. Confirm the new
limit fits physical capacity, expected growth, replication overhead, and the
owner's retention plan.

Never clear a quota or set a limit to `0` during an incident until available
capacity and workload growth have been confirmed. An unlimited bucket can
consume capacity needed by other tenants.

## Recovery Verification

1. Confirm the quota endpoint returns the intended limits.
2. Confirm `nufs_bucket_quota_limit`, `nufs_bucket_quota_usage`, and
   `nufs_bucket_quota_used_ratio` agree for the affected resource.
3. Verify the ratio remains below 80% for at least 15 minutes, or document why a
   higher approved operating point is acceptable.
4. Verify expected writes succeed and no new `QuotaExceeded` responses occur.
5. Confirm datanode capacity, object-write recovery, and GC are healthy.
6. Record the action, owner approval, old and new limits, and follow-up capacity
   or retention work in the incident.
