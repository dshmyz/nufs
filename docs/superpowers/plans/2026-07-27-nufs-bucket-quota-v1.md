# NUFS Bucket Quota v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bucket-level `max_bytes` and `max_objects` quotas across metadata, S3 write admission, metad Ops API, Admin UI, Prometheus metrics, and alerts.

**Architecture:** Reuse the existing `metadata.BucketQuota`, `QuotaManager`, and Pebble quota prefixes, but expose them through a formal `BucketQuotaService`. S3 gateway performs admission before commit using current bucket usage and final object size; v1 intentionally avoids distributed quota reservation across gateways.

**Tech Stack:** Go 1.25 for `nufs-core`; Go 1.22 for `nufs-admin`; React + TypeScript + Vite for `nufs-admin/web`; Pebble metadata storage; existing Prometheus text exporter.

## Global Constraints

- v1 exposes only `max_bytes` and `max_objects`; `MaxChunkCount` remains internal.
- Zero values mean unlimited; negative values are invalid.
- v1 does not implement write rate limiting.
- v1 does not implement strict distributed reservation across multiple S3 gateways.
- S3 quota rejection uses XML error code `QuotaExceeded`.
- Do not commit during implementation unless explicitly requested by the user.

---

## File Structure

- Modify `nufs-core/metadata/graceful_shutdown.go`: keep `BucketQuota`/`QuotaManager`, add validation and delta-aware check helpers if needed.
- Modify `nufs-core/metadata/service.go`: add `BucketQuotaService` and include it in `MetadataService`.
- Modify `nufs-core/metadata/pebble_store.go`: add public quota methods, delete quota persistence, and single-bucket usage helper.
- Modify `nufs-core/metadata/shard.go`: forward quota methods across sharded stores.
- Modify `nufs-core/metadata/client.go`: add HTTP client quota methods.
- Create `nufs-core/cmd/metad/ops_bucket_quota.go`: metad quota handlers and response DTOs.
- Modify `nufs-core/cmd/metad/ops_handlers.go`: dispatch `/api/v1/buckets/{bucket}/quota`.
- Modify `nufs-core/cmd/metad/ops_buckets.go`: optionally include quota/usage in bucket list only after quota API is stable.
- Modify `nufs-core/gateway/s3/committer.go`: add `ErrObjectQuotaExceeded`.
- Modify `nufs-core/gateway/s3/committer_put.go`: check bucket quota before inode update.
- Modify `nufs-core/gateway/s3/object.go`: map quota error to S3 XML response.
- Modify `nufs-core/cmd/metad/ops_prometheus.go`: emit bucket quota metrics.
- Modify `nufs-core/deploy/monitoring/alerting-rules.yaml`: add bucket quota alerts.
- Create `nufs-core/docs/runbooks/bucket-quota.md`: alert/operator runbook.
- Modify `nufs-admin/internal/api/handler_buckets.go`: proxy quota subpaths.
- Modify `nufs-admin/web/src/api/client.ts`: add quota DTOs and client methods.
- Modify `nufs-admin/web/src/pages/buckets/Buckets.tsx`: display and edit quotas.

## Task 1: Metadata Quota Service

**Files:**
- Modify: `nufs-core/metadata/service.go`
- Modify: `nufs-core/metadata/graceful_shutdown.go`
- Modify: `nufs-core/metadata/pebble_store.go`
- Modify: `nufs-core/metadata/shard.go`
- Test: `nufs-core/metadata/bucket_quota_test.go`

**Interfaces:**
- Produces:
  - `type BucketQuotaService interface`
  - `func (s *PebbleStore) GetBucketQuota(ctx context.Context, bucket string) (*BucketQuota, error)`
  - `func (s *PebbleStore) SetBucketQuota(ctx context.Context, bucket string, quota *BucketQuota) error`
  - `func (s *PebbleStore) DeleteBucketQuota(ctx context.Context, bucket string) error`
  - `func (s *PebbleStore) CheckBucketQuota(ctx context.Context, bucket string, additionalBytes int64, additionalObjects int64) error`
  - `func (s *PebbleStore) GetBucketUsage(ctx context.Context, bucket string) (*BucketUsage, error)`

- [ ] **Step 1: Write failing metadata tests**

Create `nufs-core/metadata/bucket_quota_test.go`:

```go
package metadata

import (
	"context"
	"strings"
	"testing"
)

func TestPebbleStoreBucketQuotaRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newQuotaTestStore(t)
	if err := store.CreateBucket(ctx, "photos", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", &BucketQuota{MaxSizeBytes: 1024, MaxObjects: 2}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	got, err := store.GetBucketQuota(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketQuota: %v", err)
	}
	if got == nil || got.MaxSizeBytes != 1024 || got.MaxObjects != 2 {
		t.Fatalf("quota = %+v", got)
	}
	if err := store.DeleteBucketQuota(ctx, "photos"); err != nil {
		t.Fatalf("DeleteBucketQuota: %v", err)
	}
	got, err = store.GetBucketQuota(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketQuota after delete: %v", err)
	}
	if got != nil {
		t.Fatalf("quota after delete = %+v, want nil", got)
	}
}

func TestPebbleStoreBucketQuotaRejectsInvalidAndMissingBucket(t *testing.T) {
	ctx := context.Background()
	store := newQuotaTestStore(t)
	if err := store.SetBucketQuota(ctx, "missing", &BucketQuota{MaxSizeBytes: 1}); err != ErrBucketNotFound {
		t.Fatalf("SetBucketQuota missing = %v, want %v", err, ErrBucketNotFound)
	}
	if err := store.CreateBucket(ctx, "photos", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", &BucketQuota{MaxSizeBytes: -1}); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("SetBucketQuota negative = %v, want negative validation error", err)
	}
}

func TestPebbleStoreCheckBucketQuotaUsesDeltas(t *testing.T) {
	ctx := context.Background()
	store := newQuotaTestStore(t)
	if err := store.CreateBucket(ctx, "photos", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", &BucketQuota{MaxSizeBytes: 100, MaxObjects: 1}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	if err := store.quota.UpdateUsage("photos", &BucketUsage{Name: "photos", UsedBytes: 80, Objects: 1}); err != nil {
		t.Fatalf("UpdateUsage: %v", err)
	}
	if err := store.CheckBucketQuota(ctx, "photos", 10, 0); err != nil {
		t.Fatalf("CheckBucketQuota overwrite shrink/small delta: %v", err)
	}
	if err := store.CheckBucketQuota(ctx, "photos", 25, 0); err == nil || !strings.Contains(err.Error(), "size") {
		t.Fatalf("CheckBucketQuota bytes = %v, want size quota error", err)
	}
	if err := store.CheckBucketQuota(ctx, "photos", 0, 1); err == nil || !strings.Contains(err.Error(), "object") {
		t.Fatalf("CheckBucketQuota objects = %v, want object quota error", err)
	}
}

func newQuotaTestStore(t *testing.T) *PebbleStore {
	t.Helper()
	store, err := NewPebbleStore(PebbleStoreConfig{
		Dir:            t.TempDir(),
		UseInMemory:    true,
		NodeID:         1,
		UseBucketStats: true,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	store.SetQuotaManager(NewQuotaManager())
	t.Cleanup(func() { _ = store.Close() })
	return store
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./metadata -run 'TestPebbleStoreBucketQuota' -count=1 -v
```

Expected: FAIL because `GetBucketQuota`, `SetBucketQuota`, `DeleteBucketQuota`, `CheckBucketQuota`, or `GetBucketUsage` are not implemented on `PebbleStore`.

- [ ] **Step 3: Implement metadata quota methods**

Add `BucketQuotaService` to `nufs-core/metadata/service.go`:

```go
type BucketQuotaService interface {
	GetBucketQuota(ctx context.Context, bucket string) (*BucketQuota, error)
	SetBucketQuota(ctx context.Context, bucket string, quota *BucketQuota) error
	DeleteBucketQuota(ctx context.Context, bucket string) error
	CheckBucketQuota(ctx context.Context, bucket string, additionalBytes int64, additionalObjects int64) error
	GetBucketUsage(ctx context.Context, bucket string) (*BucketUsage, error)
}
```

Include `BucketQuotaService` in `MetadataService`.

In `nufs-core/metadata/graceful_shutdown.go`, add validation and delta support:

```go
func (q *BucketQuota) Validate() error {
	if q == nil {
		return nil
	}
	if q.MaxSizeBytes < 0 || q.MaxObjects < 0 || q.MaxChunkCount < 0 {
		return fmt.Errorf("quota: negative limits are invalid")
	}
	return nil
}

func (qm *QuotaManager) DeleteQuota(bucket string) error {
	qm.mu.Lock()
	delete(qm.quotas, bucket)
	store := qm.store
	qm.mu.Unlock()
	if store != nil {
		if deleter, ok := store.(interface{ DeleteQuota(bucket string) error }); ok {
			return deleter.DeleteQuota(bucket)
		}
	}
	return nil
}

func (qm *QuotaManager) CheckWriteDelta(bucket string, additionalBytes int64, additionalObjects int64) error {
	qm.mu.RLock()
	defer qm.mu.RUnlock()
	q := qm.quotas[bucket]
	if q == nil {
		return nil
	}
	u := qm.usage[bucket]
	var usedBytes int64
	var objects int64
	if u != nil {
		usedBytes = u.UsedBytes
		objects = int64(u.Objects)
	}
	if q.MaxSizeBytes > 0 && usedBytes+additionalBytes > q.MaxSizeBytes {
		return fmt.Errorf("quota: bucket %s would exceed size limit (%d + %d > %d)", bucket, usedBytes, additionalBytes, q.MaxSizeBytes)
	}
	if q.MaxObjects > 0 && objects+additionalObjects > q.MaxObjects {
		return fmt.Errorf("quota: bucket %s would exceed object limit (%d + %d > %d)", bucket, objects, additionalObjects, q.MaxObjects)
	}
	return nil
}
```

In `nufs-core/metadata/pebble_store.go`, add methods near the existing `QuotaStore` section:

```go
func (s *PebbleStore) GetBucketQuota(ctx context.Context, bucket string) (*BucketQuota, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	var quota BucketQuota
	exists, err := s.getJSON(prefixQuota+bucket, &quota)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return &quota, nil
}

func (s *PebbleStore) SetBucketQuota(ctx context.Context, bucket string, quota *BucketQuota) error {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	}
	if err := quota.Validate(); err != nil {
		return err
	}
	if s.quota == nil {
		s.SetQuotaManager(NewQuotaManager())
	}
	return s.quota.SetQuota(bucket, quota)
}

func (s *PebbleStore) DeleteBucketQuota(ctx context.Context, bucket string) error {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	}
	if s.quota == nil {
		return s.applyBatchMsgpack(nil, []string{prefixQuota + bucket})
	}
	return s.quota.DeleteQuota(bucket)
}

func (s *PebbleStore) CheckBucketQuota(ctx context.Context, bucket string, additionalBytes int64, additionalObjects int64) error {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	}
	if s.quota == nil {
		return nil
	}
	usage, err := s.GetBucketUsage(ctx, bucket)
	if err != nil {
		return err
	}
	if err := s.quota.UpdateUsage(bucket, usage); err != nil {
		return err
	}
	return s.quota.CheckWriteDelta(bucket, additionalBytes, additionalObjects)
}

func (s *PebbleStore) GetBucketUsage(ctx context.Context, bucket string) (*BucketUsage, error) {
	b, err := s.GetBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if s.cfg.UseBucketStats {
		stats := s.readBucketStats(b.RootInode)
		stats.Name = b.Name
		return &stats, nil
	}
	all, err := s.ComputeAllBucketUsage(ctx)
	if err != nil {
		return nil, err
	}
	for i := range all {
		if all[i].Name == bucket {
			return &all[i], nil
		}
	}
	return &BucketUsage{Name: bucket}, nil
}

func (s *PebbleStore) DeleteQuota(bucket string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.applyBatchMsgpack(nil, []string{prefixQuota + bucket})
}
```

In `nufs-core/metadata/shard.go`, forward quota mutations to every shard and read from the first available shard, matching the existing bucket broadcast model:

```go
func (ss *ShardedStore) GetBucketQuota(ctx context.Context, bucket string) (*BucketQuota, error) {
	ss.mu.RLock()
	for _, store := range ss.shards {
		ss.mu.RUnlock()
		return store.GetBucketQuota(ctx, bucket)
	}
	ss.mu.RUnlock()
	return nil, fmt.Errorf("sharded store: no shards available")
}

func (ss *ShardedStore) SetBucketQuota(ctx context.Context, bucket string, quota *BucketQuota) error {
	return ss.forEachShard(func(s *PebbleStore) error {
		return s.SetBucketQuota(ctx, bucket, quota)
	})
}

func (ss *ShardedStore) DeleteBucketQuota(ctx context.Context, bucket string) error {
	return ss.forEachShard(func(s *PebbleStore) error {
		return s.DeleteBucketQuota(ctx, bucket)
	})
}

func (ss *ShardedStore) CheckBucketQuota(ctx context.Context, bucket string, additionalBytes int64, additionalObjects int64) error {
	ss.mu.RLock()
	for _, store := range ss.shards {
		ss.mu.RUnlock()
		return store.CheckBucketQuota(ctx, bucket, additionalBytes, additionalObjects)
	}
	ss.mu.RUnlock()
	return fmt.Errorf("sharded store: no shards available")
}

func (ss *ShardedStore) GetBucketUsage(ctx context.Context, bucket string) (*BucketUsage, error) {
	ss.mu.RLock()
	for _, store := range ss.shards {
		ss.mu.RUnlock()
		return store.GetBucketUsage(ctx, bucket)
	}
	ss.mu.RUnlock()
	return nil, fmt.Errorf("sharded store: no shards available")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
gofmt -w metadata/service.go metadata/graceful_shutdown.go metadata/pebble_store.go metadata/shard.go metadata/bucket_quota_test.go
go test ./metadata -run 'TestPebbleStoreBucketQuota' -count=1 -v
```

Expected: PASS.

## Task 2: metad Quota Ops API

**Files:**
- Create: `nufs-core/cmd/metad/ops_bucket_quota.go`
- Modify: `nufs-core/cmd/metad/ops_handlers.go`
- Modify: `nufs-core/metadata/client.go`
- Test: `nufs-core/cmd/metad/ops_bucket_quota_test.go`

**Interfaces:**
- Consumes Task 1 `BucketQuotaService`.
- Produces:
  - `GET /api/v1/buckets/{bucket}/quota`
  - `PUT /api/v1/buckets/{bucket}/quota`
  - `DELETE /api/v1/buckets/{bucket}/quota`
  - `func (c *HTTPClient) GetBucketQuota(ctx context.Context, bucket string) (*BucketQuotaStatus, error)`
  - `func (c *HTTPClient) SetBucketQuota(ctx context.Context, bucket string, quota *BucketQuota) error`
  - `func (c *HTTPClient) DeleteBucketQuota(ctx context.Context, bucket string) error`

- [ ] **Step 1: Write failing ops API test**

Create `nufs-core/cmd/metad/ops_bucket_quota_test.go`:

```go
package main

import (
	"context"
	"testing"

	"github.com/example/dfs/metadata"
)

func TestOpsHandlersBucketQuotaRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, bundle := newOpsTestStore(t)
	store.SetQuotaManager(metadata.NewQuotaManager())
	if err := store.CreateBucket(ctx, "photos", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	server := newOpsTestHTTPServer(t, store, bundle)
	client := metadata.NewHTTPClient(server.URL, 0)

	if err := client.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxSizeBytes: 1000, MaxObjects: 10}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	got, err := client.GetBucketQuota(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketQuota: %v", err)
	}
	if got.Bucket != "photos" || got.Quota == nil || got.Quota.MaxSizeBytes != 1000 || got.Quota.MaxObjects != 10 {
		t.Fatalf("quota status = %+v", got)
	}
	if got.Usage.Name != "photos" {
		t.Fatalf("usage = %+v", got.Usage)
	}
	if err := client.DeleteBucketQuota(ctx, "photos"); err != nil {
		t.Fatalf("DeleteBucketQuota: %v", err)
	}
	got, err = client.GetBucketQuota(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketQuota after delete: %v", err)
	}
	if got.Quota != nil {
		t.Fatalf("quota after delete = %+v, want nil", got.Quota)
	}
}
```

Add helper if not already available in this package:

```go
func newOpsTestHTTPServer(t *testing.T, store *metadata.PebbleStore, bundle *metadata.ServiceBundle) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, bundle, "")
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./cmd/metad -run TestOpsHandlersBucketQuotaRoundTrip -count=1 -v
```

Expected: FAIL because client methods and handlers do not exist.

- [ ] **Step 3: Implement ops API and client**

In `nufs-core/cmd/metad/ops_handlers.go`, dispatch quota before generic bucket GET/DELETE:

```go
mux.HandleFunc("/api/v1/buckets/", mut(s.handleBucketByID))
```

Inside `handleBucketByID`, if the path suffix is `/quota`, call `h.handleBucketQuota`.

Create `nufs-core/cmd/metad/ops_bucket_quota.go`:

```go
package main

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/example/dfs/metadata"
)

type bucketQuotaStatus struct {
	Bucket string                `json:"bucket"`
	Quota  *metadata.BucketQuota `json:"quota"`
	Usage  metadata.BucketUsage  `json:"usage"`
	Ratios bucketQuotaRatios     `json:"ratios"`
}

type bucketQuotaRatios struct {
	Bytes   float64 `json:"bytes"`
	Objects float64 `json:"objects"`
}

func (h *opsHandlers) handleBucketQuota(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodGet:
		h.getBucketQuota(w, r, bucket)
	case http.MethodPut:
		h.putBucketQuota(w, r, bucket)
	case http.MethodDelete:
		if err := h.store.DeleteBucketQuota(r.Context(), bucket); err != nil {
			writeBucketQuotaError(w, err)
			return
		}
		writeJSON(w, map[string]string{"status": "deleted"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *opsHandlers) getBucketQuota(w http.ResponseWriter, r *http.Request, bucket string) {
	quota, err := h.store.GetBucketQuota(r.Context(), bucket)
	if err != nil {
		writeBucketQuotaError(w, err)
		return
	}
	usage, err := h.store.GetBucketUsage(r.Context(), bucket)
	if err != nil {
		writeBucketQuotaError(w, err)
		return
	}
	writeJSON(w, newBucketQuotaStatus(bucket, quota, usage))
}

func (h *opsHandlers) putBucketQuota(w http.ResponseWriter, r *http.Request, bucket string) {
	var quota metadata.BucketQuota
	if err := json.NewDecoder(r.Body).Decode(&quota); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.SetBucketQuota(r.Context(), bucket, &quota); err != nil {
		writeBucketQuotaError(w, err)
		return
	}
	usage, err := h.store.GetBucketUsage(r.Context(), bucket)
	if err != nil {
		writeBucketQuotaError(w, err)
		return
	}
	writeJSON(w, newBucketQuotaStatus(bucket, &quota, usage))
}

func newBucketQuotaStatus(bucket string, quota *metadata.BucketQuota, usage *metadata.BucketUsage) bucketQuotaStatus {
	status := bucketQuotaStatus{Bucket: bucket, Quota: quota}
	if usage != nil {
		status.Usage = *usage
	}
	if quota != nil {
		if quota.MaxSizeBytes > 0 {
			status.Ratios.Bytes = float64(status.Usage.UsedBytes) / float64(quota.MaxSizeBytes)
		}
		if quota.MaxObjects > 0 {
			status.Ratios.Objects = float64(status.Usage.Objects) / float64(quota.MaxObjects)
		}
	}
	return status
}

func bucketNameAndQuotaPath(path string) (string, bool) {
	trimmed := strings.TrimPrefix(path, "/api/v1/buckets/")
	if !strings.HasSuffix(trimmed, "/quota") {
		return "", false
	}
	return strings.TrimSuffix(trimmed, "/quota"), true
}

func writeBucketQuotaError(w http.ResponseWriter, err error) {
	if err == metadata.ErrBucketNotFound {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSONError(w, http.StatusBadRequest, err.Error())
}
```

In `nufs-core/metadata/client.go`, add exported status type and methods:

```go
type BucketQuotaStatus struct {
	Bucket string       `json:"bucket"`
	Quota  *BucketQuota `json:"quota"`
	Usage  BucketUsage  `json:"usage"`
	Ratios struct {
		Bytes   float64 `json:"bytes"`
		Objects float64 `json:"objects"`
	} `json:"ratios"`
}

func (c *HTTPClient) GetBucketQuota(ctx context.Context, bucket string) (*BucketQuotaStatus, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, "/api/v1/buckets/"+url.PathEscape(bucket)+"/quota", nil)
	if err != nil {
		return nil, err
	}
	var status BucketQuotaStatus
	if err := c.readResponse(resp, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

func (c *HTTPClient) SetBucketQuota(ctx context.Context, bucket string, quota *BucketQuota) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPut, "/api/v1/buckets/"+url.PathEscape(bucket)+"/quota", quota)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) DeleteBucketQuota(ctx context.Context, bucket string) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodDelete, "/api/v1/buckets/"+url.PathEscape(bucket)+"/quota", nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
gofmt -w cmd/metad/ops_handlers.go cmd/metad/ops_bucket_quota.go cmd/metad/ops_bucket_quota_test.go metadata/client.go
go test ./cmd/metad -run TestOpsHandlersBucketQuotaRoundTrip -count=1 -v
```

Expected: PASS.

## Task 3: S3 PutObject Quota Admission

**Files:**
- Modify: `nufs-core/gateway/s3/committer.go`
- Modify: `nufs-core/gateway/s3/committer_put.go`
- Modify: `nufs-core/gateway/s3/object.go`
- Test: `nufs-core/gateway/s3/quota_test.go`

**Interfaces:**
- Consumes Task 1 `CheckBucketQuota`.
- Produces `ErrObjectQuotaExceeded`.

- [ ] **Step 1: Write failing S3 tests**

Create `nufs-core/gateway/s3/quota_test.go`:

```go
package s3

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/example/dfs/metadata"
)

func TestPutObjectRejectsByteQuotaExceeded(t *testing.T) {
	ctx := context.Background()
	meta := newTestMetadataWithQuota(t)
	if err := meta.CreateBucket(ctx, "photos", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := meta.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxSizeBytes: 3}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	gw := NewGateway(GatewayConfig{MetaService: meta, ChunkStore: NewMemoryChunkStore()})
	req := httptest.NewRequest(http.MethodPut, "/photos/a.txt", strings.NewReader("1234"))
	req.ContentLength = 4
	rr := httptest.NewRecorder()
	gw.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "QuotaExceeded") {
		t.Fatalf("body missing QuotaExceeded: %s", rr.Body.String())
	}
}

func TestObjectCommitterAllowsOverwriteWithinObjectQuota(t *testing.T) {
	ctx := context.Background()
	meta := newTestMetadataWithQuota(t)
	if err := meta.CreateBucket(ctx, "photos", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	committer := newMetadataObjectCommitter(meta, NewMemoryChunkStore(), false)
	if _, err := committer.Put(ctx, PutObjectRequest{Bucket: "photos", Key: "a.txt", Body: bytes.NewReader([]byte("old")), ContentLength: 3}); err != nil {
		t.Fatalf("initial Put: %v", err)
	}
	if err := meta.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxObjects: 1, MaxSizeBytes: 10}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	if _, err := committer.Put(ctx, PutObjectRequest{Bucket: "photos", Key: "a.txt", Body: bytes.NewReader([]byte("new")), ContentLength: 3}); err != nil {
		t.Fatalf("overwrite Put: %v", err)
	}
	if _, err := committer.Put(ctx, PutObjectRequest{Bucket: "photos", Key: "b.txt", Body: bytes.NewReader([]byte("x")), ContentLength: 1}); !errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("new object Put err = %v, want %v", err, ErrObjectQuotaExceeded)
	}
}

func newTestMetadataWithQuota(t *testing.T) *metadata.PebbleStore {
	t.Helper()
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:            t.TempDir(),
		UseInMemory:    true,
		NodeID:         1,
		UseBucketStats: true,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	store.SetQuotaManager(metadata.NewQuotaManager())
	t.Cleanup(func() { _ = store.Close() })
	return store
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./gateway/s3 -run 'TestPutObjectRejectsByteQuotaExceeded|TestObjectCommitterAllowsOverwriteWithinObjectQuota' -count=1 -v
```

Expected: FAIL because quota error mapping and committer quota checks do not exist.

- [ ] **Step 3: Implement quota admission**

In `nufs-core/gateway/s3/committer.go`, add:

```go
ErrObjectQuotaExceeded = errors.New("object quota exceeded")
```

In `nufs-core/gateway/s3/committer_put.go`, add a local interface:

```go
type bucketQuotaChecker interface {
	CheckBucketQuota(ctx context.Context, bucket string, additionalBytes int64, additionalObjects int64) error
}
```

After resolving existing inode/old chunks but before chunk allocation, compute early deltas:

```go
objectDelta := int64(1)
oldSize := int64(0)
if inode != nil && len(oldChunks) > 0 {
	objectDelta = 0
	oldSize = inode.Size
}
if checker, ok := c.meta.(bucketQuotaChecker); ok && req.ContentLength >= 0 {
	if err := checker.CheckBucketQuota(ctx, req.Bucket, req.ContentLength-oldSize, objectDelta); err != nil {
		c.recordAttempt(ctx, &metadata.ObjectWriteAttempt{
			ID:        attemptID,
			Bucket:    req.Bucket,
			Key:       req.Key,
			InodeID:   inode.ID,
			State:     metadata.WriteAttemptFailed,
			LastError: err.Error(),
		})
		return PutObjectResult{}, fmt.Errorf("%w: %v", ErrObjectQuotaExceeded, err)
	}
}
```

Before `UpdateInode`, do the final check:

```go
if checker, ok := c.meta.(bucketQuotaChecker); ok {
	if err := checker.CheckBucketQuota(ctx, req.Bucket, totalSize-oldSize, objectDelta); err != nil {
		c.recordAttemptFailure(ctx, attemptID, req, inode.ID, newChunkRefs, metadata.WriteAttemptFailed, err)
		return PutObjectResult{}, fmt.Errorf("%w: %v", ErrObjectQuotaExceeded, err)
	}
}
```

In `nufs-core/gateway/s3/object.go`, map the error:

```go
case errors.Is(err, ErrObjectQuotaExceeded):
	WriteXMLError(w, http.StatusForbidden, "QuotaExceeded", err.Error(), "/"+bucket+"/"+key, requestID)
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
gofmt -w gateway/s3/committer.go gateway/s3/committer_put.go gateway/s3/object.go gateway/s3/quota_test.go
go test ./gateway/s3 -run 'TestPutObjectRejectsByteQuotaExceeded|TestObjectCommitterAllowsOverwriteWithinObjectQuota' -count=1 -v
```

Expected: PASS.

## Task 4: Prometheus Metrics, Alerts, And Runbook

**Files:**
- Modify: `nufs-core/cmd/metad/ops_prometheus.go`
- Modify: `nufs-core/deploy/monitoring/alerting-rules.yaml`
- Create: `nufs-core/docs/runbooks/bucket-quota.md`
- Test: `nufs-core/cmd/metad/ops_bucket_quota_metrics_test.go`

**Interfaces:**
- Consumes Task 2 quota status methods.
- Produces Prometheus metrics:
  - `nufs_bucket_quota_used_ratio`
  - `nufs_bucket_quota_limit`
  - `nufs_bucket_quota_usage`

- [ ] **Step 1: Write failing metrics test**

Create `nufs-core/cmd/metad/ops_bucket_quota_metrics_test.go`:

```go
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/example/dfs/metadata"
)

func TestPrometheusMetricsIncludesBucketQuota(t *testing.T) {
	ctx := context.Background()
	store, bundle := newOpsTestStore(t)
	store.SetQuotaManager(metadata.NewQuotaManager())
	if err := store.CreateBucket(ctx, "photos", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxSizeBytes: 100, MaxObjects: 10}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	if err := store.CheckBucketQuota(ctx, "photos", 0, 0); err != nil {
		t.Fatalf("CheckBucketQuota refresh usage: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	prometheusMetricsHandler(store, bundle.Metrics).ServeHTTP(rr, req)
	body := rr.Body.String()
	for _, want := range []string{
		`nufs_bucket_quota_limit{bucket="photos",resource="bytes"} 100`,
		`nufs_bucket_quota_limit{bucket="photos",resource="objects"} 10`,
		`nufs_bucket_quota_used_ratio{bucket="photos",resource="bytes"} 0`,
		`nufs_bucket_quota_used_ratio{bucket="photos",resource="objects"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./cmd/metad -run TestPrometheusMetricsIncludesBucketQuota -count=1 -v
```

Expected: FAIL because quota metrics are not emitted.

- [ ] **Step 3: Implement metrics and alerts**

In `nufs-core/cmd/metad/ops_prometheus.go`, add `writePrometheusBucketQuota` and call it after object write metrics:

```go
func writePrometheusBucketQuota(ctx context.Context, w io.Writer, store *metadata.PebbleStore) {
	buckets, err := store.ListBuckets(ctx)
	if err != nil {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "# HELP nufs_bucket_quota_limit Bucket quota limit")
	fmt.Fprintln(w, "# TYPE nufs_bucket_quota_limit gauge")
	fmt.Fprintln(w, "# HELP nufs_bucket_quota_usage Bucket quota usage")
	fmt.Fprintln(w, "# TYPE nufs_bucket_quota_usage gauge")
	fmt.Fprintln(w, "# HELP nufs_bucket_quota_used_ratio Bucket quota used ratio")
	fmt.Fprintln(w, "# TYPE nufs_bucket_quota_used_ratio gauge")
	for _, bucket := range buckets {
		quota, err := store.GetBucketQuota(ctx, bucket.Name)
		if err != nil || quota == nil {
			continue
		}
		usage, err := store.GetBucketUsage(ctx, bucket.Name)
		if err != nil {
			continue
		}
		if quota.MaxSizeBytes > 0 {
			ratio := float64(usage.UsedBytes) / float64(quota.MaxSizeBytes)
			fmt.Fprintf(w, "nufs_bucket_quota_limit{bucket=\"%s\",resource=\"bytes\"} %d\n", prometheusLabelValue(bucket.Name), quota.MaxSizeBytes)
			fmt.Fprintf(w, "nufs_bucket_quota_usage{bucket=\"%s\",resource=\"bytes\"} %d\n", prometheusLabelValue(bucket.Name), usage.UsedBytes)
			fmt.Fprintf(w, "nufs_bucket_quota_used_ratio{bucket=\"%s\",resource=\"bytes\"} %.6g\n", prometheusLabelValue(bucket.Name), ratio)
		}
		if quota.MaxObjects > 0 {
			ratio := float64(usage.Objects) / float64(quota.MaxObjects)
			fmt.Fprintf(w, "nufs_bucket_quota_limit{bucket=\"%s\",resource=\"objects\"} %d\n", prometheusLabelValue(bucket.Name), quota.MaxObjects)
			fmt.Fprintf(w, "nufs_bucket_quota_usage{bucket=\"%s\",resource=\"objects\"} %d\n", prometheusLabelValue(bucket.Name), usage.Objects)
			fmt.Fprintf(w, "nufs_bucket_quota_used_ratio{bucket=\"%s\",resource=\"objects\"} %.6g\n", prometheusLabelValue(bucket.Name), ratio)
		}
	}
}
```

Add four alerts to `nufs-core/deploy/monitoring/alerting-rules.yaml` with runbook `docs/runbooks/bucket-quota.md`.

Create `nufs-core/docs/runbooks/bucket-quota.md` with:

````md
# Bucket Quota Runbook

## Alerts

- `NUFSBucketQuotaBytesHigh`
- `NUFSBucketQuotaBytesCritical`
- `NUFSBucketQuotaObjectsHigh`
- `NUFSBucketQuotaObjectsCritical`

## Triage

```bash
export METAD_OPS_URL=http://127.0.0.1:8091
curl -sS "$METAD_OPS_URL/metrics" | grep 'nufs_bucket_quota'
curl -sS "$METAD_OPS_URL/api/v1/buckets"
curl -sS "$METAD_OPS_URL/api/v1/buckets/<bucket>/quota"
```

If quota is expected, ask the owning team to delete data or raise quota.
If usage is unexpected, inspect bucket object growth and recent writers.
Do not clear quota during an incident unless storage capacity is confirmed.
````

- [ ] **Step 4: Run test and YAML validation**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
gofmt -w cmd/metad/ops_prometheus.go cmd/metad/ops_bucket_quota_metrics_test.go
go test ./cmd/metad -run TestPrometheusMetricsIncludesBucketQuota -count=1 -v
cd /Users/gracegaoya/work/project/nufs
python3 - <<'PY'
from pathlib import Path
import yaml
rules = yaml.safe_load(Path('nufs-core/deploy/monitoring/alerting-rules.yaml').read_text())
alerts = [r['alert'] for g in rules['groups'] for r in g.get('rules', [])]
for name in ['NUFSBucketQuotaBytesHigh','NUFSBucketQuotaBytesCritical','NUFSBucketQuotaObjectsHigh','NUFSBucketQuotaObjectsCritical']:
    assert name in alerts, name
print('bucket quota alerts ok')
PY
```

Expected: PASS and `bucket quota alerts ok`.

## Task 5: Admin Backend Quota Proxy

**Files:**
- Modify: `nufs-admin/internal/api/handler_buckets.go`
- Modify: `nufs-admin/internal/proxy/proxy.go`
- Modify: `nufs-admin/internal/cluster/client.go`
- Test: `nufs-admin/internal/api/handler_bucket_quota_test.go`

**Interfaces:**
- Consumes Task 2 metad endpoints.
- Produces Admin backend quota proxy under `/api/v1/clusters/{cluster}/buckets/{bucket}/quota`.
- Produces:
  - `func (c *cluster.Client) Put(ctx context.Context, path string, body io.Reader, result interface{}) error`
  - `func (p *proxy.Proxy) Put(ctx context.Context, clusterName, path string, body io.Reader, result interface{}) error`

- [ ] **Step 1: Write failing Admin API test**

Create `nufs-admin/internal/api/handler_bucket_quota_test.go`:

```go
package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/your-org/nufs-admin/internal/auth"
	"github.com/your-org/nufs-admin/internal/cache"
	"github.com/your-org/nufs-admin/internal/cluster"
	"github.com/your-org/nufs-admin/internal/config"
	"github.com/your-org/nufs-admin/internal/proxy"
)

func TestHandleBucketQuotaProxiesToMetad(t *testing.T) {
	metad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/buckets/photos/quota" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"bucket":"photos","quota":{"max_bytes":1000,"max_objects":10}}`))
	}))
	defer metad.Close()

	router := newBucketQuotaProxyTestRouter(t, metad.URL)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/clusters/prod/buckets/photos/quota", strings.NewReader(`{"max_bytes":1000,"max_objects":10}`))
	rr := httptest.NewRecorder()
	router.handleClusterRoutes(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"bucket":"photos"`) {
		t.Fatalf("body = %s", rr.Body.String())
	}
}

func newBucketQuotaProxyTestRouter(t *testing.T, metadURL string) *Router {
	t.Helper()
	cfgPath := writeAdminTestConfig(t, metadURL)
	cfgMgr, err := config.NewManager(cfgPath, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	registry, err := cluster.NewRegistry(cfgMgr, nil)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	t.Cleanup(func() { registry.Close() })
	c := cache.New(time.Second)
	t.Cleanup(c.Close)
	pr := proxy.NewProxy(registry, c)
	return NewRouter(pr, proxy.NewAggregator(pr), auth.NewJWTManager("secret"), &auth.UserStore{}, registry)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin
go test ./internal/api -run TestHandleBucketQuotaProxiesToMetad -count=1 -v
```

Expected: FAIL with method/path not handled.

- [ ] **Step 3: Implement proxying**

In `nufs-admin/internal/api/handler_buckets.go`, add a branch before `len(subpath) == 1` delete handling:

In `nufs-admin/internal/cluster/client.go`, add:

```go
func (c *Client) Put(ctx context.Context, path string, body io.Reader, result interface{}) error {
	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("cluster %s unreachable: %w", c.name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cluster %s returned %d: %s", c.name, resp.StatusCode, respBody)
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("cluster %s decode error: %w", c.name, err)
		}
	}
	return nil
}
```

In `nufs-admin/internal/proxy/proxy.go`, add:

```go
func (p *Proxy) Put(ctx context.Context, clusterName, path string, body io.Reader, result interface{}) error {
	client, ok := p.Registry.GetClient(clusterName)
	if !ok {
		return fmt.Errorf("cluster %s not found", clusterName)
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	return client.Put(ctx, path, body, result)
}
```

```go
case len(subpath) == 2 && subpath[1] == "quota":
	bucketName := subpath[0]
	path := "/api/v1/buckets/" + bucketName + "/quota"
	switch req.Method {
	case http.MethodGet:
		var status map[string]interface{}
		if err := r.proxy.Get(req.Context(), clusterID, path, &status); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	case http.MethodPut:
		var status map[string]interface{}
		if err := r.proxy.Put(req.Context(), clusterID, path, req.Body, &status); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	case http.MethodDelete:
		if err := r.proxy.Delete(req.Context(), clusterID, path); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
```

If `proxy.Proxy` lacks `Put`, add it following the existing `Post`/`Delete` style.

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin
gofmt -w internal/api/handler_buckets.go internal/api/handler_bucket_quota_test.go
go test ./internal/api -run TestHandleBucketQuotaProxiesToMetad -count=1 -v
```

Expected: PASS.

## Task 6: Admin Frontend Quota UI

**Files:**
- Modify: `nufs-admin/web/src/api/client.ts`
- Modify: `nufs-admin/web/src/pages/buckets/Buckets.tsx`

**Interfaces:**
- Consumes Task 5 Admin backend paths.
- Produces `getBucketQuota`, `setBucketQuota`, `deleteBucketQuota` frontend client helpers.

- [ ] **Step 1: Add frontend API types and methods**

In `nufs-admin/web/src/api/client.ts`, add:

```ts
export async function getBucketQuota(clusterId: string, bucket: string): Promise<BucketQuotaStatus> {
  const resp = await api.get(`/clusters/${clusterId}/buckets/${encodeURIComponent(bucket)}/quota`)
  return resp.data
}

export async function setBucketQuota(clusterId: string, bucket: string, quota: BucketQuota): Promise<BucketQuotaStatus> {
  const resp = await api.put(`/clusters/${clusterId}/buckets/${encodeURIComponent(bucket)}/quota`, quota)
  return resp.data
}

export async function deleteBucketQuota(clusterId: string, bucket: string): Promise<void> {
  await api.delete(`/clusters/${clusterId}/buckets/${encodeURIComponent(bucket)}/quota`)
}

export interface BucketQuota {
  max_bytes: number
  max_objects: number
}

export interface BucketQuotaStatus {
  bucket: string
  quota: BucketQuota | null
  usage: {
    name: string
    used_bytes: number
    objects: number
  }
  ratios: {
    bytes: number
    objects: number
  }
}
```

- [ ] **Step 2: Modify Bucket page**

Update `nufs-admin/web/src/pages/buckets/Buckets.tsx` to:

- load quota statuses with `Promise.all(buckets.map(...getBucketQuota...))`
- store `quotaByBucket: Record<string, BucketQuotaStatus | { error: string }>`
- add columns `容量配额`, `对象配额`, `使用率`
- add buttons `编辑配额` and `清除`
- render a compact edit panel with raw byte and object inputs

Use these helpers in the file:

```tsx
function formatBytes(value: number) {
  if (!value) return '-'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
  let n = value
  let i = 0
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024
    i++
  }
  return `${n.toFixed(i === 0 ? 0 : 1)} ${units[i]}`
}

function quotaTone(ratio: number) {
  if (ratio >= 0.95) return { background: '#fee2e2', color: '#b91c1c' }
  if (ratio >= 0.85) return { background: '#fef3c7', color: '#92400e' }
  return { background: '#d1fae5', color: '#047857' }
}
```

- [ ] **Step 3: Run frontend build**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin/web
npm run build
```

Expected: PASS.

## Task 7: Final Verification

**Files:**
- Verify all files changed in Tasks 1-6.

- [ ] **Step 1: Core targeted tests**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./metadata ./cmd/metad ./gateway/s3 -count=1
```

Expected: PASS.

- [ ] **Step 2: Core full tests**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./...
```

Expected: PASS.

- [ ] **Step 3: Admin tests and build**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin
go test ./...
go build -o /tmp/nufs-admin-server-check ./cmd/admin-server
cd /Users/gracegaoya/work/project/nufs/nufs-admin/web
npm run build
```

Expected: all commands exit 0.

- [ ] **Step 4: Alert YAML validation**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs
python3 - <<'PY'
from pathlib import Path
import yaml
rules = yaml.safe_load(Path('nufs-core/deploy/monitoring/alerting-rules.yaml').read_text())
alerts = [r['alert'] for g in rules['groups'] for r in g.get('rules', [])]
for name in ['NUFSBucketQuotaBytesHigh','NUFSBucketQuotaBytesCritical','NUFSBucketQuotaObjectsHigh','NUFSBucketQuotaObjectsCritical']:
    assert name in alerts, name
print('bucket quota alerts ok')
PY
if command -v promtool >/dev/null 2>&1; then
  promtool check rules nufs-core/deploy/monitoring/alerting-rules.yaml
else
  echo 'promtool not installed'
fi
```

Expected: YAML check prints `bucket quota alerts ok`. `promtool` may be unavailable on local machines; report that explicitly.

## Self-Review

- Spec coverage: metadata quota API, S3 admission, Ops API, Admin proxy, Admin UI, metrics, alerts, runbook, and tests are covered.
- Unresolved-marker scan: no unfinished markers or unspecified "add tests" steps remain.
- Type consistency: public quota fields use Go `MaxSizeBytes`/`MaxObjects` internally and JSON `max_bytes`/`max_objects` externally.
- Scope check: write rate limiting and distributed quota reservation are excluded from this plan as specified.
