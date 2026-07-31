# Production Storage P0 Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the P0 production-readiness gaps that currently prevent NUFS from being built, deployed, and trusted under real storage failure modes.

**Architecture:** Treat admin UI, admin backend static serving, metadata Raft validation, and data write/repair correctness as separate modules with explicit interfaces and independent verification. Each task starts with a failing test or build command, implements the smallest fix, then runs a package-specific command plus the relevant full-suite command.

**Tech Stack:** Go 1.25 for `nufs-core`; Go 1.22 for `nufs-admin`; React + TypeScript + Vite for `nufs-admin/web`; Pebble + hashicorp/raft for metadata; local TCP datanode protocol for chunk replication.

## Global Constraints

- Do not modify unrelated user changes; current git status already contains unrelated untracked content under `nufs-core/docs/superpowers/`.
- Keep admin API JSON field names aligned with Go struct tags.
- Keep production hardening changes behind existing module seams; avoid broad rewrites.
- Every task must end with concrete verification output.
- No default production secrets may be introduced.

---

## File Structure

- `nufs-admin/web/src/api/client.ts`: Owns frontend API client types and request helpers.
- `nufs-admin/web/src/App.tsx`: Owns top-level routing and auth state.
- `nufs-admin/web/src/pages/clusters/Clusters.tsx`: Owns the cluster-management page.
- `nufs-admin/internal/server/server.go`: Owns HTTP lifecycle and SPA static file fallback.
- `nufs-admin/internal/server/server_test.go`: New tests for API path pass-through and SPA fallback.
- `nufs-admin/cmd/admin-server/main.go`: Wires optional embedded/static frontend assets into the server.
- `nufs-core/metadata/raft_integration_test.go`: New real multi-node Raft integration coverage.
- `nufs-core/gateway/s3/write_commit_test.go`: New tests for data-write versus metadata-commit failure ordering.
- `nufs-core/datanode/repair_idempotency_test.go`: New tests for repair task idempotency and metadata state transitions.

---

### Task 1: Restore Admin Web Contract And Build

**Files:**
- Modify: `nufs-admin/web/src/api/client.ts`
- Modify: `nufs-admin/web/src/App.tsx`
- Modify: `nufs-admin/web/src/pages/clusters/Clusters.tsx`

**Interfaces:**
- Consumes: backend JSON from `cluster.ClusterInfo` with `name`, `region`, `description`, `health`, `lastCheck`, `source`.
- Consumes: backend JSON from `store.AuditLogEntry` with `id`, `cluster_id`, `action`, `operator`, `detail`, `created_at`.
- Produces: exported TypeScript interfaces `ClusterInfo` and `ClusterAuditLog`.

- [ ] **Step 1: Reproduce the current failing build**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin/web
npm run build
```

Expected: FAIL with TypeScript errors for `ClusterAuditLog`, unused `api`, and `ClusterInfo.source`.

- [ ] **Step 2: Add the missing frontend API types**

Edit `nufs-admin/web/src/api/client.ts` so `ClusterInfo` matches the backend contract and add `ClusterAuditLog`:

```ts
export interface ClusterInfo {
  name: string
  region: string
  description: string
  health: 'healthy' | 'unhealthy' | 'unknown'
  lastCheck: string
  source: 'static' | 'dynamic'
}

export interface ClusterAuditLog {
  id: number
  cluster_id: string
  action: 'add' | 'remove' | 'update'
  operator: string
  detail: string
  created_at: string
}
```

- [ ] **Step 3: Remove the unused top-level API import**

Edit `nufs-admin/web/src/App.tsx` and remove:

```ts
import { api } from './api/client'
```

- [ ] **Step 4: Fix the Ops URL table column**

Edit `nufs-admin/web/src/api/client.ts` to include the backend field once the backend exposes it, or change the table heading in `nufs-admin/web/src/pages/clusters/Clusters.tsx` from `Ops URL` to `描述` if the current backend contract remains unchanged.

Preferred production fix: add `metad_ops_url` to the backend list response and expose it in `ClusterInfo`:

```ts
metad_ops_url?: string
```

Then render:

```tsx
{cluster.metad_ops_url || '-'}
```

- [ ] **Step 5: Verify frontend build passes**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin/web
npm run build
```

Expected: PASS with `tsc && vite build` completing successfully.

- [ ] **Step 6: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-admin/web/src/api/client.ts nufs-admin/web/src/App.tsx nufs-admin/web/src/pages/clusters/Clusters.tsx
git commit -m "fix(admin-web): restore cluster contract build"
```

---

### Task 2: Serve Admin SPA From The Go Backend

**Files:**
- Modify: `nufs-admin/internal/server/server.go`
- Create: `nufs-admin/internal/server/server_test.go`
- Modify: `nufs-admin/cmd/admin-server/main.go`

**Interfaces:**
- Consumes: `http.FileSystem` or `fs.FS` containing Vite output.
- Produces: `server.New(addr string, router *api.Router, staticFS fs.FS) *Server` or an equivalent small interface.
- Guarantees: `/api/` paths remain API-only; non-API paths serve `index.html`; static assets serve with normal file responses.

- [ ] **Step 1: Write a failing SPA fallback test**

Create `nufs-admin/internal/server/server_test.go`:

```go
package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"
)

func TestSPAFallbackServesIndexForNonAPIPath(t *testing.T) {
	static := fstest.MapFS{
		"index.html": {Data: []byte("<html>nufs admin</html>")},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := withSPAFallback(mux, static)
	req := httptest.NewRequest(http.MethodGet, "/clusters/manage", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if rec.Body.String() != "<html>nufs admin</html>" {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

func TestSPAFallbackDoesNotHandleAPIPath(t *testing.T) {
	static := fstest.MapFS{
		"index.html": {Data: []byte("<html>nufs admin</html>")},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := withSPAFallback(mux, static)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}
```

- [ ] **Step 2: Run the failing package test**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin
go test ./internal/server
```

Expected: FAIL because `withSPAFallback` does not exist.

- [ ] **Step 3: Implement the SPA fallback helper**

Add to `nufs-admin/internal/server/server.go`:

```go
func withSPAFallback(apiHandler http.Handler, static fs.FS) http.Handler {
	fileServer := http.FileServer(http.FS(static))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			apiHandler.ServeHTTP(w, r)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/")
		if path != "" {
			if f, err := static.Open(path); err == nil {
				_ = f.Close()
				fileServer.ServeHTTP(w, r)
				return
			}
		}

		index, err := static.Open("index.html")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer index.Close()

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		http.ServeContent(w, r, "index.html", time.Time{}, readSeekCloser(index))
	})
}
```

Use a small helper or simpler `io.ReadAll(index)` implementation if the opened file is not seekable.

- [ ] **Step 4: Wire static assets into server construction**

Modify `server.New` so it wraps the mux when a static filesystem is supplied. Modify `cmd/admin-server/main.go` to pass `os.DirFS("web/dist")` for the Docker layout.

- [ ] **Step 5: Verify admin backend and image build path**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin
go test ./...
docker build -t nufs-admin:local .
```

Expected: `go test ./...` PASS and Docker build reaches the final image stage.

- [ ] **Step 6: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-admin/internal/server/server.go nufs-admin/internal/server/server_test.go nufs-admin/cmd/admin-server/main.go
git commit -m "feat(admin): serve built spa from backend"
```

---

### Task 3: Add Real Metadata Raft Integration Coverage

**Files:**
- Create: `nufs-core/metadata/raft_integration_test.go`
- Modify only if required: `nufs-core/metadata/pebble_raft.go`
- Modify only if required: `nufs-core/metadata/pebble_store.go`

**Interfaces:**
- Consumes: existing `RaftNode`, `PebbleStore`, and Raft configuration types.
- Produces: an integration test that starts three actual Raft nodes with real transports.
- Guarantees: write on leader is visible after leader restart/failover; follower writes redirect or fail predictably.

- [ ] **Step 1: Write a failing real-cluster test skeleton**

Create `nufs-core/metadata/raft_integration_test.go`:

```go
package metadata

import (
	"context"
	"testing"
	"time"
)

func TestRaftClusterLeaderFailoverPreservesCommittedBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("real raft integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cluster := startRealRaftTestCluster(t, 3)
	defer cluster.Stop()

	leader := cluster.WaitForLeader(t, ctx)
	if err := leader.Store.CreateBucket(ctx, "prod-check", BucketPolicy{ReplicationFactor: 2}); err != nil {
		t.Fatalf("create bucket on leader: %v", err)
	}

	cluster.StopNode(t, leader.ID)

	newLeader := cluster.WaitForLeader(t, ctx)
	bucket, err := newLeader.Store.GetBucket(ctx, "prod-check")
	if err != nil {
		t.Fatalf("get bucket after failover: %v", err)
	}
	if bucket.Name != "prod-check" {
		t.Fatalf("bucket name = %q", bucket.Name)
	}
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./metadata -run TestRaftClusterLeaderFailoverPreservesCommittedBucket -count=1
```

Expected: FAIL because `startRealRaftTestCluster` does not exist or current Raft wiring cannot support the test.

- [ ] **Step 3: Implement the test harness**

In `raft_integration_test.go`, add helpers that allocate temp dirs and localhost ports, start three `PebbleStore` instances with Raft enabled, join peers through the existing Raft bootstrap/join path, and expose:

```go
type realRaftTestCluster struct {
	Nodes []*realRaftTestNode
}

type realRaftTestNode struct {
	ID    NodeID
	Store *PebbleStore
}

func startRealRaftTestCluster(t *testing.T, n int) *realRaftTestCluster
func (c *realRaftTestCluster) Stop()
func (c *realRaftTestCluster) StopNode(t *testing.T, id NodeID)
func (c *realRaftTestCluster) WaitForLeader(t *testing.T, ctx context.Context) *realRaftTestNode
```

- [ ] **Step 4: Verify failover behavior**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./metadata -run TestRaftClusterLeaderFailoverPreservesCommittedBucket -count=1
```

Expected: PASS.

- [ ] **Step 5: Run metadata package tests**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./metadata -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/metadata/raft_integration_test.go nufs-core/metadata/pebble_raft.go nufs-core/metadata/pebble_store.go
git commit -m "test(metadata): cover real raft leader failover"
```

---

### Task 4: Lock Down Write Commit And Repair Idempotency

**Files:**
- Create: `nufs-core/gateway/s3/write_commit_test.go`
- Create: `nufs-core/datanode/repair_idempotency_test.go`
- Modify only if tests expose bugs: `nufs-core/gateway/s3/object.go`
- Modify only if tests expose bugs: `nufs-core/datanode/repair.go`
- Modify only if tests expose bugs: `nufs-core/metadata/pebble_store.go`

**Interfaces:**
- Consumes: S3 gateway `PutObject` path and datanode repair worker interfaces.
- Produces: regression coverage for partial write failure, metadata commit failure, repeated repair tasks, and repair metadata state transitions.
- Guarantees: no object is reported committed unless enough data replicas are durable; repeated repair attempts do not duplicate ready replicas.

- [ ] **Step 1: Add S3 write commit failure tests**

Create `nufs-core/gateway/s3/write_commit_test.go` with table cases:

```go
package s3

import "testing"

func TestPutObjectDoesNotCommitMetadataWhenReplicaQuorumFails(t *testing.T) {
	t.Skip("enable after injecting a chunk writer failure seam into Gateway")
}

func TestPutObjectLeavesRecoverablePendingStateWhenMetadataCommitFailsAfterReplicaWrite(t *testing.T) {
	t.Skip("enable after exposing commit failure injection on mock metadata")
}
```

The first implementation step should remove `t.Skip` after the existing test mocks can inject the failures.

- [ ] **Step 2: Add repair idempotency tests**

Create `nufs-core/datanode/repair_idempotency_test.go`:

```go
package datanode

import "testing"

func TestRepairByAddingReplicaDoesNotDuplicateExistingTarget(t *testing.T) {
	t.Skip("enable after extracting recordReplacementReplica behavior into a directly testable helper")
}

func TestRepairCompletedTaskIsRemovedOnlyAfterReadyReportSucceeds(t *testing.T) {
	t.Skip("enable after fake RepairMeta can force ReportChunkState failure")
}
```

The first implementation step should remove each skip as the required seam is introduced.

- [ ] **Step 3: Introduce narrow test seams**

If current mocks cannot force the required failures, introduce the smallest package-private interfaces:

```go
type chunkWriter interface {
	WriteChunk(ctx context.Context, chunk *metadata.ChunkMeta, data []byte) error
}
```

and package-private fake metadata implementations inside test files only.

- [ ] **Step 4: Implement minimal fixes for exposed bugs**

Expected production behavior:

- Replica quorum failure returns a 5xx response and does not make `Lookup(bucket, key)` return a committed inode.
- Metadata commit failure after replica write leaves enough metadata for GC or recovery to find orphan chunks.
- Repair replacement does not append duplicate replicas for the same target node.
- Repair task remains queued when `ReportChunkState` fails.

- [ ] **Step 5: Verify focused packages**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./gateway/s3 ./datanode -count=1
```

Expected: PASS.

- [ ] **Step 6: Verify full core suite**

Run:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/gracegaoya/work/project/nufs
git add nufs-core/gateway/s3/write_commit_test.go nufs-core/datanode/repair_idempotency_test.go nufs-core/gateway/s3/object.go nufs-core/datanode/repair.go nufs-core/metadata/pebble_store.go
git commit -m "test(core): lock down write commit and repair idempotency"
```

---

## Final Verification

- [ ] Run admin frontend build:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin/web
npm run build
```

- [ ] Run admin backend tests:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin
go test ./...
```

- [ ] Run core tests:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-core
go test ./...
```

- [ ] Build admin Docker image:

```bash
cd /Users/gracegaoya/work/project/nufs/nufs-admin
docker build -t nufs-admin:local .
```

Expected final result: all commands pass, admin UI is included in the backend image, real Raft failover has test coverage, and write/repair correctness has regression coverage.

## Self-Review

- Spec coverage: P0 admin build, admin deploy, real Raft validation, write commit correctness, and repair idempotency are covered.
- Placeholder scan: The plan contains no `TBD`. Skipped tests are intentional red tests that must be unskipped in the same task after the seam exists.
- Type consistency: Frontend `ClusterInfo.source` and `ClusterAuditLog` match the Go JSON names used by `cluster.ClusterInfo` and `store.AuditLogEntry`.
