# NUFS Production Architecture v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the first production-architecture v1 convergence pass: ObjectCommitter, explicit write-attempt state, unified background task state, production config validation, and a local smoke-test harness.

**Architecture:** Keep the S3 handler shallow by moving object write/read orchestration behind an `ObjectCommitter` module. Store write-attempt and background-task state in metadata so recovery, GC, repair, and admin views share one source of truth. Add production startup validation and smoke tests as gates, without enabling advanced features such as erasure coding, cross-zone active-active replication, or automatic tier migration.

**Tech Stack:** Go 1.25 for `nufs-core`; Go 1.22 for `nufs-admin`; React + TypeScript + Vite for `nufs-admin/web`; Pebble + hashicorp/raft for metadata; local TCP datanode protocol for chunk IO.

## Global Constraints

- Keep v1 scope limited to replicated S3 storage and operational recovery.
- Do not make erasure coding the default write path.
- Do not add cross-zone active-active replication to the production path.
- Do not continue expanding FUSE semantics in this plan.
- Metadata remains the source of truth for object visibility, write attempts, and background task state.
- Datanodes own bytes and checksums, but never decide object visibility.
- Existing object overwrite must preserve the old object until the new object commits.
- Production mode must reject default/dev secrets.
- Every task ends with focused verification and a commit.

---

## File Structure

- `nufs-core/gateway/s3/committer.go`: new `ObjectCommitter` interface and request/result types.
- `nufs-core/gateway/s3/committer_put.go`: production `metadataObjectCommitter.Put` implementation.
- `nufs-core/gateway/s3/committer_get.go`: production `metadataObjectCommitter.Get` implementation.
- `nufs-core/gateway/s3/committer_test.go`: unit tests for write state and overwrite visibility.
- `nufs-core/gateway/s3/object.go`: thin HTTP translation into `ObjectCommitter`.
- `nufs-core/gateway/s3/handler.go`: wires default `ObjectCommitter`.
- `nufs-core/metadata/write_attempt.go`: write-attempt types and metadata store methods.
- `nufs-core/metadata/write_attempt_test.go`: tests for write-attempt persistence and recovery queries.
- `nufs-core/metadata/background_task.go`: unified task model and store methods.
- `nufs-core/metadata/background_task_test.go`: tests for leases, retries, and dead-letter transitions.
- `nufs-core/datanode/repair.go`: migrates repair queue usage toward unified task state.
- `nufs-core/metadata/production_config.go`: production startup validation helpers.
- `nufs-core/metadata/production_config_test.go`: tests for rejecting dev/default settings.
- `nufs-core/tests/smoke/production_smoke_test.go`: local multi-node smoke test.

---

### Task 1: Define ObjectCommitter Interface

**Files:**
- Create: `nufs-core/gateway/s3/committer.go`
- Create: `nufs-core/gateway/s3/committer_test.go`
- Modify: `nufs-core/gateway/s3/handler.go`

**Interfaces:**
- Produces:

```go
type ObjectCommitter interface {
	Put(ctx context.Context, req PutObjectRequest) (PutObjectResult, error)
	Get(ctx context.Context, req GetObjectRequest) (ObjectReader, error)
}

type PutObjectRequest struct {
	Bucket        string
	Key           string
	Body          io.Reader
	ContentLength int64
	MaxObjectSize int64
	RequestID     string
}

type PutObjectResult struct {
	ETag string
	Size int64
}

type GetObjectRequest struct {
	Bucket string
	Key    string
	Range  *ObjectRange
}

type ObjectRange struct {
	Start int64
	End   int64
}

type ObjectReader interface {
	io.ReadCloser
	Size() int64
	ETag() string
}
```

- Consumes: existing `metadata.MetadataService` and `ChunkStore`.

- [ ] **Step 1: Write the failing interface construction test**

Create `nufs-core/gateway/s3/committer_test.go`:

```go
package s3

import "testing"

func TestNewGatewayInstallsDefaultObjectCommitter(t *testing.T) {
	meta := newMockMetaService()
	gw := NewGateway(GatewayConfig{
		MetaService: meta,
		ChunkStore:  NewMemoryChunkStore(),
	})

	if gw.committer == nil {
		t.Fatal("expected gateway to install an ObjectCommitter")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./gateway/s3 -run TestNewGatewayInstallsDefaultObjectCommitter -count=1
```

Expected: FAIL with `gw.committer undefined`.

- [ ] **Step 3: Add interface and gateway field**

Create `nufs-core/gateway/s3/committer.go`:

```go
package s3

import (
	"context"
	"io"
)

type ObjectCommitter interface {
	Put(ctx context.Context, req PutObjectRequest) (PutObjectResult, error)
	Get(ctx context.Context, req GetObjectRequest) (ObjectReader, error)
}

type PutObjectRequest struct {
	Bucket        string
	Key           string
	Body          io.Reader
	ContentLength int64
	MaxObjectSize int64
	RequestID     string
}

type PutObjectResult struct {
	ETag string
	Size int64
}

type GetObjectRequest struct {
	Bucket string
	Key    string
	Range  *ObjectRange
}

type ObjectRange struct {
	Start int64
	End   int64
}

type ObjectReader interface {
	io.ReadCloser
	Size() int64
	ETag() string
}
```

Modify `nufs-core/gateway/s3/handler.go`:

```go
type Gateway struct {
	meta       metadata.MetadataService
	creds      *CredentialStore
	chunkStore ChunkStore
	committer  ObjectCommitter
	...
}
```

In `NewGateway`, after `gw.chunkStore` is set:

```go
gw.committer = newMetadataObjectCommitter(gw.meta, gw.chunkStore)
```

- [ ] **Step 4: Add minimal constructor**

Create `nufs-core/gateway/s3/committer_put.go`:

```go
package s3

import "github.com/dshmyz/nufs/nufs-core/metadata"

type metadataObjectCommitter struct {
	meta       metadata.MetadataService
	chunkStore ChunkStore
}

func newMetadataObjectCommitter(meta metadata.MetadataService, chunkStore ChunkStore) *metadataObjectCommitter {
	return &metadataObjectCommitter{meta: meta, chunkStore: chunkStore}
}
```

- [ ] **Step 5: Run test to verify it passes**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./gateway/s3 -run TestNewGatewayInstallsDefaultObjectCommitter -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/gateway/s3/committer.go nufs-core/gateway/s3/committer_put.go nufs-core/gateway/s3/committer_test.go nufs-core/gateway/s3/handler.go
git commit -m "feat(s3): introduce object committer interface"
```

---

### Task 2: Move PutObject Into ObjectCommitter

**Files:**
- Modify: `nufs-core/gateway/s3/committer_put.go`
- Modify: `nufs-core/gateway/s3/object.go`
- Modify: `nufs-core/gateway/s3/committer_test.go`
- Test: `nufs-core/gateway/s3/write_commit_test.go`

**Interfaces:**
- Consumes: `ObjectCommitter.Put(ctx, PutObjectRequest)`.
- Produces: S3 handler delegates write orchestration to `ObjectCommitter`.

- [ ] **Step 1: Write failing delegation test**

Add to `nufs-core/gateway/s3/committer_test.go`:

```go
type recordingCommitter struct {
	putCalled bool
}

func (r *recordingCommitter) Put(ctx context.Context, req PutObjectRequest) (PutObjectResult, error) {
	r.putCalled = true
	if req.Bucket != "bucket" || req.Key != "object.txt" {
		return PutObjectResult{}, errors.New("unexpected request")
	}
	return PutObjectResult{ETag: "\"etag\"", Size: 7}, nil
}

func (r *recordingCommitter) Get(ctx context.Context, req GetObjectRequest) (ObjectReader, error) {
	return nil, errors.New("not used")
}

func TestPutObjectDelegatesToObjectCommitter(t *testing.T) {
	meta := newMockMetaService()
	if err := meta.CreateBucket(context.Background(), "bucket", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	rec := &recordingCommitter{}
	gw := NewGateway(GatewayConfig{
		MetaService: meta,
		ChunkStore:  NewMemoryChunkStore(),
	})
	gw.committer = rec
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/bucket/object.txt", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if !rec.putCalled {
		t.Fatal("expected ObjectCommitter.Put to be called")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./gateway/s3 -run TestPutObjectDelegatesToObjectCommitter -count=1
```

Expected: FAIL because `handlePutObject` still performs orchestration directly.

- [ ] **Step 3: Move current PutObject body into committer**

In `nufs-core/gateway/s3/committer_put.go`, implement:

```go
func (c *metadataObjectCommitter) Put(ctx context.Context, req PutObjectRequest) (PutObjectResult, error) {
	// Move the existing body/chunk orchestration from handlePutObject here.
	// Return typed package errors declared in committer.go:
	// ErrObjectBucketNotFound, ErrObjectLocked, ErrObjectNoReplicas,
	// ErrObjectWriteFailed, ErrObjectCommitFailed.
}
```

Declare errors in `committer.go`:

```go
var (
	ErrObjectBucketNotFound = errors.New("object bucket not found")
	ErrObjectLocked         = errors.New("object locked")
	ErrObjectNoReplicas     = errors.New("object no replicas")
	ErrObjectWriteFailed    = errors.New("object write failed")
	ErrObjectCommitFailed   = errors.New("object commit failed")
)
```

- [ ] **Step 4: Make handler translate committer errors to S3 responses**

Replace most of `handlePutObject` with:

```go
result, err := gw.committer.Put(ctx, PutObjectRequest{
	Bucket:        bucket,
	Key:           key,
	Body:          r.Body,
	ContentLength: r.ContentLength,
	MaxObjectSize: gw.maxObjectSize,
	RequestID:     requestID,
})
if err != nil {
	writePutObjectCommitterError(w, err, bucket, key, requestID)
	return
}
w.Header().Set("ETag", result.ETag)
w.WriteHeader(http.StatusOK)
```

Add `writePutObjectCommitterError` in `object.go` mapping typed errors to current status codes.

- [ ] **Step 5: Run focused S3 tests**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./gateway/s3 -run 'TestPutObjectDelegatesToObjectCommitter|TestPutObjectDoesNotExposeCommittedChunksWhenReplicaWriteFails|TestPutAndHeadObject' -count=1
```

Expected: PASS.

- [ ] **Step 6: Run all S3 tests**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./gateway/s3 -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/gateway/s3/committer.go nufs-core/gateway/s3/committer_put.go nufs-core/gateway/s3/object.go nufs-core/gateway/s3/committer_test.go
git commit -m "refactor(s3): move put object orchestration into committer"
```

---

### Task 3: Persist Object Write Attempts

**Files:**
- Create: `nufs-core/metadata/write_attempt.go`
- Create: `nufs-core/metadata/write_attempt_test.go`
- Modify: `nufs-core/metadata/pebble_store.go`
- Modify: `nufs-core/gateway/s3/committer_put.go`

**Interfaces:**
- Produces:

```go
type WriteAttemptState string

const (
	WriteAttemptPending       WriteAttemptState = "pending"
	WriteAttemptChunksAllocated WriteAttemptState = "chunks_allocated"
	WriteAttemptChunksDurable WriteAttemptState = "chunks_durable"
	WriteAttemptCommitted     WriteAttemptState = "committed"
	WriteAttemptFailed        WriteAttemptState = "failed"
	WriteAttemptRecoveryNeeded WriteAttemptState = "recovery_needed"
)

type ObjectWriteAttempt struct {
	ID        string
	Bucket    string
	Key       string
	InodeID   InodeID
	Chunks    []ChunkRef
	State     WriteAttemptState
	LastError string
	CreatedAt int64
	UpdatedAt int64
}
```

- Consumes: `PebbleStore.applyBatchMsgpack`.

- [ ] **Step 1: Write failing metadata tests**

Create `nufs-core/metadata/write_attempt_test.go`:

```go
package metadata

import (
	"context"
	"testing"
)

func TestPebbleStoreWriteAttemptLifecycle(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	attempt := &ObjectWriteAttempt{
		ID:     "attempt-1",
		Bucket: "bucket",
		Key:    "object.txt",
		State:  WriteAttemptPending,
	}
	if err := store.PutWriteAttempt(ctx, attempt); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	attempt.State = WriteAttemptChunksDurable
	attempt.Chunks = []ChunkRef{{ID: 10, Offset: 0, Length: 7, Version: 1}}
	if err := store.PutWriteAttempt(ctx, attempt); err != nil {
		t.Fatalf("update attempt: %v", err)
	}

	got, err := store.GetWriteAttempt(ctx, "attempt-1")
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if got.State != WriteAttemptChunksDurable || len(got.Chunks) != 1 {
		t.Fatalf("unexpected attempt: %+v", got)
	}
}

func TestPebbleStoreListRecoverableWriteAttempts(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	_ = store.PutWriteAttempt(ctx, &ObjectWriteAttempt{ID: "recover", State: WriteAttemptRecoveryNeeded})
	_ = store.PutWriteAttempt(ctx, &ObjectWriteAttempt{ID: "committed", State: WriteAttemptCommitted})

	attempts, err := store.ListWriteAttemptsByState(ctx, WriteAttemptRecoveryNeeded, 100)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].ID != "recover" {
		t.Fatalf("attempts = %+v", attempts)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./metadata -run 'TestPebbleStoreWriteAttemptLifecycle|TestPebbleStoreListRecoverableWriteAttempts' -count=1
```

Expected: FAIL because types and store methods do not exist.

- [ ] **Step 3: Implement write-attempt types and store methods**

Create `nufs-core/metadata/write_attempt.go` with the types above and methods:

```go
func writeAttemptKey(id string) string {
	return prefixWriteAttempt + id
}

func writeAttemptStateKey(state WriteAttemptState, id string) string {
	return fmt.Sprintf("%s%s/%s", prefixWriteAttemptState, state, id)
}
```

Add prefixes in `pebble_store.go`:

```go
const (
	prefixWriteAttempt      = "write_attempt:"
	prefixWriteAttemptState = "write_attempt_state:"
)
```

Implement methods on `PebbleStore`:

```go
func (s *PebbleStore) PutWriteAttempt(ctx context.Context, attempt *ObjectWriteAttempt) error
func (s *PebbleStore) GetWriteAttempt(ctx context.Context, id string) (*ObjectWriteAttempt, error)
func (s *PebbleStore) ListWriteAttemptsByState(ctx context.Context, state WriteAttemptState, limit int) ([]ObjectWriteAttempt, error)
func (s *PebbleStore) DeleteWriteAttempt(ctx context.Context, id string) error
```

- [ ] **Step 4: Wire ObjectCommitter write state**

In `committer_put.go`, generate an attempt ID:

```go
attemptID := fmt.Sprintf("%s/%s/%d", req.Bucket, req.Key, time.Now().UnixNano())
```

Call metadata methods if `c.meta` supports:

```go
type writeAttemptStore interface {
	PutWriteAttempt(context.Context, *metadata.ObjectWriteAttempt) error
}
```

State updates:

- before allocation: `WriteAttemptPending`
- after chunk refs prepared: `WriteAttemptChunksAllocated`
- after chunk writes and commits: `WriteAttemptChunksDurable`
- after inode update: `WriteAttemptCommitted`
- on durable chunks but inode update failure: `WriteAttemptRecoveryNeeded`
- on earlier failure: `WriteAttemptFailed`

- [ ] **Step 5: Run metadata and S3 tests**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./metadata ./gateway/s3 -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/metadata/write_attempt.go nufs-core/metadata/write_attempt_test.go nufs-core/metadata/pebble_store.go nufs-core/gateway/s3/committer_put.go
git commit -m "feat(metadata): persist object write attempts"
```

---

### Task 4: Add Unified Background Task State

**Files:**
- Create: `nufs-core/metadata/background_task.go`
- Create: `nufs-core/metadata/background_task_test.go`
- Modify: `nufs-core/metadata/pebble_store.go`

**Interfaces:**
- Produces:

```go
type BackgroundTaskType string
type BackgroundTaskState string

const (
	TaskRepair    BackgroundTaskType = "repair"
	TaskGC        BackgroundTaskType = "gc"
	TaskScrub     BackgroundTaskType = "scrub"
	TaskRebalance BackgroundTaskType = "rebalance"

	TaskQueued     BackgroundTaskState = "queued"
	TaskLeased     BackgroundTaskState = "leased"
	TaskRunning    BackgroundTaskState = "running"
	TaskSucceeded  BackgroundTaskState = "succeeded"
	TaskRetrying   BackgroundTaskState = "retrying"
	TaskDeadLetter BackgroundTaskState = "dead_letter"
	TaskCanceled   BackgroundTaskState = "canceled"
)

type BackgroundTask struct {
	ID             string
	Type           BackgroundTaskType
	State          BackgroundTaskState
	Target         string
	IdempotencyKey string
	LeaseOwner     string
	AttemptCount   int
	NextRunAt       int64
	LastError       string
	CreatedAt       int64
	UpdatedAt       int64
}
```

- [ ] **Step 1: Write failing task lifecycle tests**

Create `nufs-core/metadata/background_task_test.go`:

```go
package metadata

import (
	"context"
	"testing"
	"time"
)

func TestBackgroundTaskLeaseLifecycle(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	task := &BackgroundTask{
		ID:             "task-1",
		Type:           TaskRepair,
		State:          TaskQueued,
		Target:         "chunk:10",
		IdempotencyKey: "repair/chunk/10",
	}
	if err := store.PutBackgroundTask(ctx, task); err != nil {
		t.Fatalf("put task: %v", err)
	}

	leased, err := store.LeaseBackgroundTask(ctx, TaskRepair, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("lease task: %v", err)
	}
	if leased.ID != "task-1" || leased.State != TaskLeased || leased.LeaseOwner != "worker-1" {
		t.Fatalf("leased = %+v", leased)
	}

	if err := store.CompleteBackgroundTask(ctx, "task-1"); err != nil {
		t.Fatalf("complete task: %v", err)
	}

	got, err := store.GetBackgroundTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.State != TaskSucceeded {
		t.Fatalf("state = %s, want %s", got.State, TaskSucceeded)
	}
}

func TestBackgroundTaskFailureEventuallyDeadLetters(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	task := &BackgroundTask{ID: "task-2", Type: TaskRepair, State: TaskQueued, Target: "chunk:20"}
	_ = store.PutBackgroundTask(ctx, task)

	for i := 0; i < 4; i++ {
		if err := store.FailBackgroundTask(ctx, "task-2", "boom", 3); err != nil {
			t.Fatalf("fail task %d: %v", i, err)
		}
	}

	got, err := store.GetBackgroundTask(ctx, "task-2")
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.State != TaskDeadLetter {
		t.Fatalf("state = %s, want %s", got.State, TaskDeadLetter)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./metadata -run 'TestBackgroundTaskLeaseLifecycle|TestBackgroundTaskFailureEventuallyDeadLetters' -count=1
```

Expected: FAIL because types and methods do not exist.

- [ ] **Step 3: Implement task storage methods**

Create `nufs-core/metadata/background_task.go` with types and helpers:

```go
func backgroundTaskKey(id string) string
func backgroundTaskQueueKey(taskType BackgroundTaskType, state BackgroundTaskState, nextRunAt int64, id string) string
```

Implement on `PebbleStore`:

```go
func (s *PebbleStore) PutBackgroundTask(ctx context.Context, task *BackgroundTask) error
func (s *PebbleStore) GetBackgroundTask(ctx context.Context, id string) (*BackgroundTask, error)
func (s *PebbleStore) LeaseBackgroundTask(ctx context.Context, taskType BackgroundTaskType, owner string, lease time.Duration) (*BackgroundTask, error)
func (s *PebbleStore) CompleteBackgroundTask(ctx context.Context, id string) error
func (s *PebbleStore) FailBackgroundTask(ctx context.Context, id string, lastErr string, maxAttempts int) error
```

- [ ] **Step 4: Run lifecycle tests**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./metadata -run 'TestBackgroundTaskLeaseLifecycle|TestBackgroundTaskFailureEventuallyDeadLetters' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/metadata/background_task.go nufs-core/metadata/background_task_test.go nufs-core/metadata/pebble_store.go
git commit -m "feat(metadata): add unified background task state"
```

---

### Task 5: Route Repair Queue Through Background Tasks

**Files:**
- Modify: `nufs-core/datanode/repair.go`
- Modify: `nufs-core/datanode/repair_test.go`
- Modify: `nufs-core/metadata/pebble_store.go`

**Interfaces:**
- Consumes: `LeaseBackgroundTask`, `CompleteBackgroundTask`, `FailBackgroundTask`.
- Produces: repair worker can consume `TaskRepair` tasks while preserving existing `GetRepairQueue` compatibility.

- [ ] **Step 1: Write failing repair task adapter test**

Add to `nufs-core/datanode/repair_test.go`:

```go
func TestRepairWorker_ProcessesUnifiedRepairTask(t *testing.T) {
	meta := newMockMetadataService()
	meta.backgroundTasks = []metadata.BackgroundTask{
		{ID: "repair-100", Type: metadata.TaskRepair, State: metadata.TaskQueued, Target: "chunk:100"},
	}
	meta.chunks[100] = &metadata.ChunkMeta{
		ID: 100,
		Replicas: []metadata.ReplicaInfo{
			{NodeID: 1, State: metadata.ReplicaReady},
			{NodeID: 2, State: metadata.ReplicaReady},
		},
	}

	rw := &RepairWorker{meta: meta, nodeID: 1}
	rw.processRepairQueue(context.Background())

	if !meta.backgroundCompleted["repair-100"] {
		t.Fatal("expected unified repair task to be completed")
	}
}
```

Extend the test fake with `backgroundTasks` and `backgroundCompleted`.

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./datanode -run TestRepairWorker_ProcessesUnifiedRepairTask -count=1
```

Expected: FAIL because repair worker does not consume unified tasks.

- [ ] **Step 3: Add adapter interface in repair worker**

In `repair.go`, add:

```go
type unifiedRepairTaskMeta interface {
	LeaseBackgroundTask(context.Context, metadata.BackgroundTaskType, string, time.Duration) (*metadata.BackgroundTask, error)
	CompleteBackgroundTask(context.Context, string) error
	FailBackgroundTask(context.Context, string, string, int) error
}
```

Update `processRepairQueue`:

```go
if unified, ok := rw.meta.(unifiedRepairTaskMeta); ok {
	if rw.processUnifiedRepairTask(ctx, unified) {
		return
	}
}
```

Implement `processUnifiedRepairTask` to parse targets of form `chunk:<id>`, call `repairChunk`, then complete/fail the task.

- [ ] **Step 4: Run datanode tests**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./datanode -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/datanode/repair.go nufs-core/datanode/repair_test.go nufs-core/metadata/pebble_store.go
git commit -m "feat(repair): consume unified background repair tasks"
```

---

### Task 6: Add Production Startup Validation

**Files:**
- Create: `nufs-core/metadata/production_config.go`
- Create: `nufs-core/metadata/production_config_test.go`
- Modify: `nufs-admin/internal/config/config.go`
- Modify: `nufs-admin/internal/config/config_test.go`

**Interfaces:**
- Produces:

```go
type RuntimeMode string

const (
	RuntimeDev        RuntimeMode = "dev"
	RuntimeProduction RuntimeMode = "production"
)

type ProductionValidationConfig struct {
	Mode              RuntimeMode
	JWTSecret         string
	S3CredentialPath  string
	RaftNodeCount     int
	TLSEnabled        bool
	AllowInsecureDev  bool
}

func ValidateProductionConfig(cfg ProductionValidationConfig) error
```

- [ ] **Step 1: Write failing production validation tests**

Create `nufs-core/metadata/production_config_test.go`:

```go
package metadata

import "testing"

func TestValidateProductionConfigRejectsDevSecret(t *testing.T) {
	err := ValidateProductionConfig(ProductionValidationConfig{
		Mode:             RuntimeProduction,
		JWTSecret:        "dev-secret-change-in-production",
		S3CredentialPath: "/etc/nufs/s3.yaml",
		RaftNodeCount:    3,
		TLSEnabled:       true,
	})
	if err == nil {
		t.Fatal("expected production config with dev secret to fail")
	}
}

func TestValidateProductionConfigRejectsSingleNodeRaft(t *testing.T) {
	err := ValidateProductionConfig(ProductionValidationConfig{
		Mode:             RuntimeProduction,
		JWTSecret:        "a-long-production-secret-value",
		S3CredentialPath: "/etc/nufs/s3.yaml",
		RaftNodeCount:    1,
		TLSEnabled:       true,
	})
	if err == nil {
		t.Fatal("expected single-node production raft to fail")
	}
}

func TestValidateProductionConfigAllowsExplicitDevMode(t *testing.T) {
	err := ValidateProductionConfig(ProductionValidationConfig{
		Mode:             RuntimeDev,
		JWTSecret:        "dev-secret-change-in-production",
		RaftNodeCount:    1,
		TLSEnabled:       false,
		AllowInsecureDev: true,
	})
	if err != nil {
		t.Fatalf("dev config should pass: %v", err)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./metadata -run TestValidateProductionConfig -count=1
```

Expected: FAIL because validation does not exist.

- [ ] **Step 3: Implement validation**

Create `nufs-core/metadata/production_config.go`:

```go
package metadata

import (
	"errors"
	"fmt"
	"strings"
)

type RuntimeMode string

const (
	RuntimeDev        RuntimeMode = "dev"
	RuntimeProduction RuntimeMode = "production"
)

type ProductionValidationConfig struct {
	Mode             RuntimeMode
	JWTSecret        string
	S3CredentialPath string
	RaftNodeCount    int
	TLSEnabled       bool
	AllowInsecureDev bool
}

func ValidateProductionConfig(cfg ProductionValidationConfig) error {
	if cfg.Mode != RuntimeProduction {
		if cfg.AllowInsecureDev {
			return nil
		}
		return errors.New("non-production mode requires AllowInsecureDev")
	}
	var errs []string
	if cfg.JWTSecret == "" || strings.Contains(cfg.JWTSecret, "dev-secret") || strings.Contains(cfg.JWTSecret, "change-in-production") {
		errs = append(errs, "production JWT secret is empty or uses a dev default")
	}
	if cfg.S3CredentialPath == "" {
		errs = append(errs, "production S3 credential source is required")
	}
	if cfg.RaftNodeCount < 3 {
		errs = append(errs, "production Raft requires at least 3 nodes")
	}
	if !cfg.TLSEnabled {
		errs = append(errs, "production TLS must be enabled")
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid production config: %s", strings.Join(errs, "; "))
	}
	return nil
}
```

- [ ] **Step 4: Run validation tests**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./metadata -run TestValidateProductionConfig -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/metadata/production_config.go nufs-core/metadata/production_config_test.go
git commit -m "feat(config): validate production startup safety"
```

---

### Task 7: Add Local Production Smoke Test

**Files:**
- Create: `nufs-core/tests/smoke/production_smoke_test.go`
- Modify only if needed: `nufs-core/metadata/raft_integration_test.go`

**Interfaces:**
- Consumes: existing local test harness patterns for Raft, datanode, and S3 gateway.
- Produces: one smoke test for create bucket, put object, leader failover, get object.

- [ ] **Step 1: Write skipped smoke test scaffold**

Create `nufs-core/tests/smoke/production_smoke_test.go`:

```go
package smoke

import (
	"os"
	"testing"
)

func TestProductionSmokePutFailoverGet(t *testing.T) {
	if os.Getenv("NUFS_RUN_SMOKE") != "1" {
		t.Skip("set NUFS_RUN_SMOKE=1 to run local multi-node smoke test")
	}
	t.Fatal("smoke harness not wired")
}
```

- [ ] **Step 2: Run test to verify default skip**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./tests/smoke -count=1
```

Expected: PASS with skip.

- [ ] **Step 3: Run test to verify smoke harness is missing**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
NUFS_RUN_SMOKE=1 go test ./tests/smoke -run TestProductionSmokePutFailoverGet -count=1
```

Expected: FAIL with `smoke harness not wired`.

- [ ] **Step 4: Implement smoke harness**

Implement in `production_smoke_test.go`:

- start three metadata Raft nodes
- start one S3 gateway backed by in-memory chunk store for the first smoke iteration
- create bucket through HTTP
- put object through HTTP
- stop current metadata leader
- get object through HTTP and verify bytes

Use the metadata package exported constructors only. If the existing real-Raft harness is unexported, duplicate the small test-only helper inside this package instead of exporting production internals.

- [ ] **Step 5: Run smoke test**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
NUFS_RUN_SMOKE=1 go test ./tests/smoke -run TestProductionSmokePutFailoverGet -count=1 -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/tests/smoke/production_smoke_test.go
git commit -m "test(smoke): add local production failover smoke test"
```

---

## Final Verification

- [ ] Run core tests:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./...
```

- [ ] Run admin tests:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin
go test ./...
```

- [ ] Run admin web build:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin/web
npm run build
```

- [ ] Run smoke test:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
NUFS_RUN_SMOKE=1 go test ./tests/smoke -run TestProductionSmokePutFailoverGet -count=1 -v
```

- [ ] Run Docker build when Docker is available:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin
docker build -t nufs-admin:local .
```

Expected final result: S3 writes flow through `ObjectCommitter`, write attempts are explicit and recoverable, repair can consume unified task state, production config rejects unsafe defaults, and the local smoke test validates put/failover/get.

## Self-Review

- Spec coverage: covers ObjectCommitter, write-attempt state, background task state, production config validation, and local smoke test. It intentionally defers EC, cross-zone replication, tiering, and deeper FUSE work.
- Placeholder scan: no `TBD` or vague implementation-only placeholders remain. The smoke test starts as an explicit failing scaffold and is wired in the same task.
- Type consistency: `ObjectCommitter`, `ObjectWriteAttempt`, `BackgroundTask`, and `ValidateProductionConfig` signatures are defined before downstream tasks consume them.
