# NUFS P0 Production Gates — Evidence Report

Date: 2026-08-25 (plan created 2026-08-24)
Commit: helm-smoke PASS on `nufs/runtime:smoke5` (offline-address fix)

## Summary

All four P0 blockers from the production-readiness review are now backed by
executable gates. The decisive E1 gate (real Helm deployment, production mode)
was closed on 2026-08-25: the helm-smoke passed on a live Kubernetes cluster
(docker-desktop, single node) with auth enabled and no `--allow-insecure-dev`.

## Gate-by-gate

### Task 1 — Replicator connection reuse (`5b41b50`)
- `go test ./datanode -run '^TestReplicator_(ConnectionReuse|PoolClosesOnStop|PoolReconnectsAfterFailure)$' -count=20` — PASS.
- `go test -race ./datanode -run '^TestReplicator_(ConnectionReuse|PoolClosesOnStop|PoolReconnectsAfterFailure)$' -count=5` — PASS.

### Task 2 — CI production gates
- `.github/workflows/ci.yml` committed and hardened (`446eeac`, `cecb9c0`, `164cc82`): core tests/vet, admin tests, frontend build, fast production gate, failure-artifact collection.

### Task 3 — Real three-node Helm verification (E1) — CLOSED 2026-08-25
- `bash nufs-core/scripts/soak/run-v21-helm-smoke.sh --image nufs/runtime:smoke5 ...` — **PASS**.
- Evidence: `nufs-core/.drill-results/helm-smoke-nufs-smoke-20260825173206/`
  - `report.env`: `result=PASS`, `stage=complete`, `metad_voters=3`, `datanodes_required_for_smoke_rf=3`
  - `leader_before=nufs-smoke-metad-0`, `leader_after=nufs-smoke-metad-2`
  - `object-sha256.txt`: before- and after-failover objects byte-identical (`7e2fedf1...`)
- Production mode proved live: shared Secret wired auth-token/token-signing-key/credential-secret-key into all three components; S3 credential seeded into the metad registry; SigV4-signed S3 create/write/read through the gateway.

### Task 4 — Backup/restore + fault injection (`82dcb1f`)
- `run-v21-production-drill.sh` and `run-v21-network-faults.sh` exist; harness-level leader-failover drill PASS (RTO 7.92s, out_of_window_errors=0), metadata-restore drill PASS, chaos-soak 6n PASS.
- Real S3 backup→restore and multi-host network faults still require external infra (see limitations).

### Task 5 — Evidence package
- This report + updated `nufs-core/docs/runbooks/production-readiness-checklist.md` (E1 → ✅).

## New defects found & fixed by the helm-smoke (2026-08-25)

The first-ever production-mode helm smoke exposed bugs that dev-mode drills
never reached:

1. **`checkExpiredNodes` wiped the node address on offline-marking** (`metadata/production.go`).
   The lease manager wrote a bare `NodeInfo{ID, State: NodeOffline, LastSeen}`
   (Addr=""), erasing the persisted address/rack/zone/capacity. After a leader
   failover dropped heartbeats past the lease TTL, datanodes were marked
   offline and their addresses were permanently blanked (raft-replicated to the
   new leader); `HeartbeatLiveness` promotes offline→online from the stored
   record but carries no address, so every subsequent write failed
   `dial tcp: missing address` / `only 0/3 replicas` → 503. **Fixed**: the
   offline snapshot now preserves the current record. Regression test added
   (`TestLeaseManager_ExpiresOfflineNodes`).
2. Smoke-script bugs (all fixed in `run-v21-helm-smoke.sh`):
   - leader detection parsed a pod-name pattern that `/api/v1/cluster/status`
     never returns (`leader_uri` is an IP) → now maps IP→pod.
   - `wait_for_leader` passed the auth header as an unquoted string → word-split
     at the space, every poll rejected "missing bearer" → now uses an array.
   - `kubectl port-forward svc/` pins to one pod; deleting the pinned leader pod
     killed the metad port-forward → restart forwards after leader-kill recovery.
   - after-failover S3 check recreated an existing bucket → metad 500 → now
     reuses the bucket (create only once).

## Limitations / not yet proven

- **B3** real deployed backup→restore: `metad.backup.enabled` default off; needs a live cluster with a MinIO/S3 repository.
- **E2** multi-host network partition/delay/loss: single-node helm-smoke covers leader-kill only.
- **A6** capacity/load RTO: no target-load benchmark defined yet.
- **F2** security review: not performed.
- Suite note: `TestRaftClusterInodeIDNoReuseAcrossFailover` flaked once under
  concurrent CPU load (passed in isolation); known raft election sensitivity to
  CPU congestion, not a regression.
