# NUFS P0 Production Gates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the four P0 blockers identified by the production-readiness review and produce reproducible evidence for local, CI, Helm, recovery, and multi-node fault gates.

**Architecture:** Preserve the existing replicator pool, verification scripts, backup APIs, and Helm chart. Fix the connection-pool seam in place, make CI call the canonical repository gates, and add only the missing orchestration/diagnostic wrappers for real Kubernetes and MinIO-backed verification. Each task has its own executable validation and does not depend on an unverified later task.

**Tech Stack:** Go 1.25, Go test/vet, Bash, GitHub Actions, Docker/MinIO, Helm, Kubernetes, existing NUFS `scripts/verify.sh` and soak drills.

## Global Constraints

- Preserve all existing user modifications in the working tree.
- Do not change the public S3/FUSE interfaces.
- Do not replace the existing connection pool, Helm chart, backup API, or fault-injection mechanism with a parallel implementation.
- CI must fail on non-zero test, vet, build, or production-verification exit codes.
- Production evidence must use three metadata voters; two-node or `--allow-insecure-dev` compose runs are not acceptance evidence.
- Every claim of passing behavior must be backed by a fresh command result.

## File map

- Modify `nufs-core/datanode/replicator.go`: fix connection-pool reuse, single-flight dialing, and broken-connection eviction only where required by the failing regression.
- Modify `nufs-core/datanode/replicator_pool_test.go`: make reuse and reconnect assertions deterministic and add the repeated-run regression coverage.
- Create `.github/workflows/ci.yml`: reproducible core/admin/frontend/verify jobs with caches and artifacts.
- Create `nufs-core/scripts/soak/run-v21-helm-smoke.sh`: idempotent Helm deployment, leader-failover smoke, cleanup, and failure diagnostics.
- Create `nufs-core/scripts/soak/run-v21-production-drill.sh`: MinIO-backed backup/restore plus network fault orchestration, or extend the existing `run-v21-network-faults.sh` if its interface can support the required stages without duplication.
- Modify `nufs-core/scripts/verify.sh`: expose the new real-environment gates only when explicitly requested and fail closed when required tools/configuration are absent.
- Modify `nufs-core/docs/runbooks/metadata-backup-restore-drill.md`: document the real MinIO/Helm command, artifacts, and interpretation of results.
- Create `nufs-core/docs/runbooks/helm-production-smoke.md`: operator prerequisites, namespace lifecycle, expected checks, and cleanup.
- Modify `nufs-core/README.md` only if the new canonical commands need to be discoverable from the existing verification section.

---

### Task 1: Stabilize replicator connection reuse

**Files:**
- Modify: `nufs-core/datanode/replicator.go:40-370`
- Modify: `nufs-core/datanode/replicator_pool_test.go:30-235`

**Interfaces:**
- Consumes: existing `newConnPool`, `ClientPool`-style endpoint keying, `ReplicationTask`, and `Replicator.Submit`/`Stop`.
- Produces: the existing pool behavior with one healthy connection per stable endpoint under the single-worker regression, bounded duplicate dials under concurrent access, and reconnect after eviction.

- [ ] **Step 1: Reproduce the failure in isolation.**

  Run:

  ```bash
  cd nufs-core
  go test ./datanode -run '^TestReplicator_ConnectionReuse$' -count=1 -v
  ```

  Expected: the current baseline fails with a dial count greater than 2, or records the exact timing/eviction behavior in the verbose log.

- [ ] **Step 2: Inspect the pool lifecycle before editing.**

  Trace `newConnPool`, `get`, `put`, `close`, and the error paths around `replicator.go:280-355`. Record whether the connection is returned to the pool after every successful source read/target write and whether `poolDialCount` increments only at actual network dials.

- [ ] **Step 3: Add a deterministic synchronization assertion.**

  Replace polling-only completion in `TestReplicator_ConnectionReuse` with a bounded completion signal based on target-store visibility, while retaining the 10-second timeout. Add a subtest that submits the same endpoint workload repeatedly without changing the production API.

- [ ] **Step 4: Run the test to capture the red result.**

  Run:

  ```bash
  go test ./datanode -run '^TestReplicator_ConnectionReuse$' -count=5 -v
  ```

  Expected: failure remains attributable to the pool implementation, not an unbounded test race.

- [ ] **Step 5: Implement the smallest pool fix.**

  Ensure the pool uses a stable normalized address key, returns healthy clients after each task, serializes first dial for the same key or uses an equivalent single-flight guard, and removes a client exactly once on I/O failure. Do not relax the test threshold or hide dials from the counter.

- [ ] **Step 6: Verify focused, race, and repeated behavior.**

  Run:

  ```bash
  go test ./datanode -run '^TestReplicator_(ConnectionReuse|PoolClosesOnStop|PoolReconnectsAfterFailure)$' -count=20 -v
  go test -race ./datanode -run '^TestReplicator_(ConnectionReuse|PoolClosesOnStop|PoolReconnectsAfterFailure)$' -count=5
  ```

  Expected: zero failures and no race reports.

- [ ] **Step 7: Commit the isolated fix.**

  ```bash
  git add nufs-core/datanode/replicator.go nufs-core/datanode/replicator_pool_test.go
  git commit -m "fix(datanode): stabilize replicator connection reuse"
  ```

---

### Task 2: Add CI production gates

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `nufs-core/Makefile:1-90` only if a CI-friendly target is needed
- Modify: `nufs-core/README.md` verification section if command documentation changes

**Interfaces:**
- Consumes: `go test`, `go vet`, `npm ci`, `npm run build`, and `nufs-core/scripts/verify.sh --level fast`.
- Produces: pull-request and default-branch jobs with reproducible toolchains, dependency caches, and failure artifacts.

- [ ] **Step 1: Define the workflow jobs without changing application code.**

  Create jobs for core tests/vet, admin tests, frontend build, and the fast production verification gate. Pin the Go toolchain to the repository-required version, use `actions/setup-node` with npm cache, and set explicit timeouts.

- [ ] **Step 2: Add failure artifact collection.**

  Upload `/tmp/nufs-verify-testlogs`, any `nufs-core/.drill-results`, and relevant test output with `if: failure()`. Do not upload credential files or generated secrets.

- [ ] **Step 3: Validate workflow syntax and local command parity.**

  Run:

  ```bash
  ruby -e 'require "yaml"; YAML.load_file(".github/workflows/ci.yml"); puts "workflow yaml ok"'
  cd nufs-core && go test ./... && go vet ./...
  cd ../nufs-admin && go test ./...
  cd web && npm ci && npm run build
  cd ../../nufs-core && bash scripts/verify.sh --level fast
  ```

  If a local YAML parser rejects GitHub expression syntax, validate with the repository's available actionlint tool and document that fallback in the workflow comments.

- [ ] **Step 4: Commit CI separately.**

  ```bash
  git add .github/workflows/ci.yml nufs-core/Makefile nufs-core/README.md
  git commit -m "ci: enforce production readiness gates"
  ```

---

### Task 3: Add real three-node Helm verification

**Files:**
- Create: `nufs-core/scripts/soak/run-v21-helm-smoke.sh`
- Create: `nufs-core/docs/runbooks/helm-production-smoke.md`
- Modify: `nufs-core/deploy/helm/nufs/values.yaml` only for a demonstrable readiness/replica configuration gap
- Modify: `nufs-core/scripts/verify.sh` only to add an explicit `helm` level or opt-in stage

**Interfaces:**
- Consumes: existing Helm chart at `nufs-core/deploy/helm/nufs`, `kubectl`, `helm`, a container image reference, and an S3-compatible endpoint/client.
- Produces: a repeatable script with `--namespace`, `--release`, `--image`, `--kube-context`, `--results`, and `--keep` options; exit 0 only after leader failover and read/write checks pass.

- [ ] **Step 1: Inspect chart values and render the production topology.**

  Run:

  ```bash
  cd nufs-core
  helm lint deploy/helm/nufs
  helm template nufs-p0 deploy/helm/nufs --set metad.replicas=3 --set global.image.tag="$IMAGE_TAG" > /tmp/nufs-p0-rendered.yaml
  ```

  Confirm the rendered manifests contain three metadata voters, persistent volumes, readiness probes, and no `allow-insecure-dev` setting.

- [ ] **Step 2: Implement namespace-scoped lifecycle and diagnostics.**

  The script must create a unique namespace, install/upgrade the chart, wait for rollout/readiness, and register an `EXIT` trap that collects `kubectl get all`, events, descriptions, and logs before cleanup. Cleanup must delete only the generated namespace unless `--keep` is supplied.

- [ ] **Step 3: Implement functional checks.**

  Use the existing CLI/S3 client path to create a bucket, write a payload, read it back and hash it. Query cluster status to find the current leader, delete only that metad pod, wait for a different leader and ready state, then repeat write/read and verify the original object hash.

- [ ] **Step 4: Verify the script in dry-run/render mode.**

  Run:

  ```bash
  bash nufs-core/scripts/soak/run-v21-helm-smoke.sh --help
  helm lint nufs-core/deploy/helm/nufs
  ```

  Expected: help is deterministic, chart lint passes, and missing cluster/image prerequisites fail with an actionable error before mutating resources.

- [ ] **Step 5: Run the real three-node verification.**

  Run with a real Kubernetes context and built image:

  ```bash
  nufs-core/scripts/soak/run-v21-helm-smoke.sh \
    --image "$NUFS_IMAGE" --results "$NUFS_HELM_RESULTS"
  ```

  Expected: exit 0, report contains `metad_replicas=3`, leader change, post-failover read/write, and ready state. On failure, retain logs and events.

- [ ] **Step 6: Commit Helm verification and runbook.**

  ```bash
  git add nufs-core/scripts/soak/run-v21-helm-smoke.sh nufs-core/docs/runbooks/helm-production-smoke.md nufs-core/deploy/helm/nufs/values.yaml nufs-core/scripts/verify.sh
  git commit -m "test: add three-node Helm production smoke"
  ```

---

### Task 4: Complete real backup/restore and multi-node fault injection

**Files:**
- Create or modify: `nufs-core/scripts/soak/run-v21-production-drill.sh`
- Modify: `scripts/soak/run-v21-network-faults.sh` only if it lacks parameterized results/cleanup required by the drill
- Modify: `nufs-core/scripts/soak/run-v21-metadata-restore.sh` only to add the real MinIO path while preserving the existing local path
- Modify: `nufs-core/docs/runbooks/metadata-backup-restore-drill.md`

**Interfaces:**
- Consumes: `metad`, `datanode`, `nufs-s3`, `nufs-backup`, `nufs-restore`/`RestoreBackupToNewCluster`, Docker/MinIO, and the existing network fault helper.
- Produces: one command that emits a machine-readable report with backup ID, restore target, object hashes, leader-failover timing, fault stages, and artifact paths.

- [ ] **Step 1: Add explicit prerequisite and secret checks.**

  Require `docker`, `curl`, `jq`, built NUFS binaries, and explicit MinIO credentials/endpoint for the real S3 path. Reject empty/default credentials and never print secret values.

- [ ] **Step 2: Provision an isolated MinIO repository.**

  Start a uniquely named MinIO container/network or use an explicitly supplied endpoint, create the backup bucket, record endpoint metadata, and register cleanup that removes only resources created by the run.

- [ ] **Step 3: Exercise backup and restore.**

  Start a three-node metadata cluster and datanode set, create deterministic bucket/object fixtures, create and verify a committed backup in MinIO, destroy the source metadata directories/processes, restore to a new cluster ID and clean target, wait for readiness, and compare metadata/object hashes.

- [ ] **Step 4: Exercise multi-node faults.**

  Run the existing network-fault stages for leader kill, inter-node delay/loss, and datanode unavailability. After each stage, verify the expected availability behavior, recovery, repair queue convergence, and final object hash. Record RTO and error counts.

- [ ] **Step 5: Add report and artifact checks.**

  Write `REPORT.txt` plus JSON containing `status`, `backup_id`, `restore_status`, `leader_failover_seconds`, `fault_stages`, `object_hash_before`, `object_hash_after`, and `artifact_dir`. Exit non-zero on any mismatch or missing evidence.

- [ ] **Step 6: Run the real drill and update the runbook.**

  Run:

  ```bash
  cd nufs-core
  bash scripts/soak/run-v21-production-drill.sh --results "$NUFS_DRILL_RESULTS"
  ```

  Expected: backup/restore and all fault stages are PASS with retained logs. Update the runbook with the exact command and prerequisites.

- [ ] **Step 7: Commit the recovery drill.**

  ```bash
  git add nufs-core/scripts/soak/run-v21-production-drill.sh scripts/soak/run-v21-network-faults.sh nufs-core/scripts/soak/run-v21-metadata-restore.sh nufs-core/docs/runbooks/metadata-backup-restore-drill.md
  git commit -m "test: add real backup and fault recovery drill"
  ```

---

### Task 5: Final P0 gate and evidence package

**Files:**
- Modify: `nufs-core/docs/runbooks/production-readiness-checklist.md`
- Modify: `nufs-core/README.md` if command links or gate status need updating
- Create: `docs/superpowers/verification/2026-08-24-nufs-p0-production-gates-results.md`

**Interfaces:**
- Consumes: all prior task commands and their retained artifacts.
- Produces: a dated evidence report that distinguishes local, CI, real Helm, backup/restore, and fault-injection results.

- [ ] **Step 1: Run the focused regression gate.**

  ```bash
  cd nufs-core
  go test ./datanode -run '^TestReplicator_(ConnectionReuse|PoolClosesOnStop|PoolReconnectsAfterFailure)$' -count=20
  ```

- [ ] **Step 2: Run repository gates.**

  ```bash
  cd nufs-core && go test ./... && go vet ./... && bash scripts/verify.sh --level fast
  cd ../nufs-admin && go test ./...
  cd web && npm ci && npm run build
  ```

- [ ] **Step 3: Run real Helm and recovery gates.**

  Execute Tasks 3 and 4 with explicit results directories and retain the generated reports/logs.

- [ ] **Step 4: Write the evidence report.**

  Include command, timestamp, commit, exit code, summary, artifact path, and unresolved limitations for every gate. Do not mark a gate PASS from a unit test when the acceptance criterion requires real multi-node infrastructure.

- [ ] **Step 5: Re-read the acceptance criteria and update the checklist.**

  Mark only evidence-backed gates as complete. If infrastructure is unavailable, leave the gate blocked and report the exact prerequisite rather than weakening the criterion.

- [ ] **Step 6: Run final diff and secret checks.**

  ```bash
  git diff --check HEAD~1..HEAD
  rg -n "dev-ops-token|change-in-production|secret123|password=|AWS_SECRET_ACCESS_KEY=" .github nufs-core/scripts docs/superpowers 2>/dev/null || true
  git status --short
  ```

  Review any match manually before claiming completion.
