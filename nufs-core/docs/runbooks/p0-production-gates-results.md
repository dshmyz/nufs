# P0 production-gate result (2026-08-24, updated 2026-08-25)

## Green evidence

- `go test ./datanode -run '^TestReplicator_(ConnectionReuse|PoolClosesOnStop|PoolReconnectsAfterFailure)$' -count=20` — PASS.
- `go vet ./...` — PASS.
- `go test ./...` in `nufs-admin` — PASS.
- `npm ci && npm run build` in `nufs-admin/web` — PASS.
- `go test ./metadata -run 'TestLeaseManager_(ExpiresOfflineNodes|PreservesOperatorStates|DrainingTerminalState)'` — PASS.
- Helm smoke syntax/help/lint/render-only checks — PASS.

## Real-cluster Helm gate (2026-08-25) — PASS

`scripts/soak/run-v21-helm-smoke.sh` first ran to completion in production mode
on a live Kubernetes cluster (docker-desktop, single node): 3 metad voters +
3 datanodes + S3 gateway, auth enabled via shared Secret, no
`--allow-insecure-dev`. S3 SigV4 create/write/read passed before and after a
leader kill; leader changed `metad-0 → metad-2`; object hashes byte-identical.
Evidence: `nufs-core/.drill-results/helm-smoke-nufs-smoke-20260825173206/`
(`report.env result=PASS`).

The run exposed and fixed one production defect and four smoke-script bugs;
see `docs/superpowers/verification/2026-08-24-nufs-p0-production-gates-results.md`.

## Still not green / not yet proven

- Real S3 backup → whole-cluster loss → new-cluster restore: no live cluster
  with a MinIO/S3 repository and `metad.backup.enabled` (B3).
- Multi-host network partition/delay/loss fault injection (E2).
- Capacity/load RTO benchmark (A6).

## Verdict

The repository now has executable gates, CI wiring, and **real Helm
production-mode evidence**. Release approval still requires the B3 backup/restore
and E2 multi-host fault runs against a production-shaped cluster.
