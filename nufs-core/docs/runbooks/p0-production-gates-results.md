# P0 production-gate result (2026-08-24)

## Green evidence

- `go test ./datanode -run '^TestReplicator_(ConnectionReuse|PoolClosesOnStop|PoolReconnectsAfterFailure)$' -count=20` — PASS.
- `go vet ./...` — PASS.
- `go test ./...` in `nufs-admin` — PASS.
- `npm ci && npm run build` in `nufs-admin/web` — PASS. Local npm audit reported 3 moderate and 3 high vulnerabilities; dependency remediation remains separate work.
- `go test ./metadata -run 'TestRestore|TestBackup.*S3|TestCreateBackupCheckpoint' -count=1` — PASS.
- Helm smoke syntax/help/lint/render-only checks — PASS.

## Not green / not yet proven

- The original `scripts/verify.sh --level fast` count=2 gate exposed ephemeral-port exhaustion when the large `cmd/metad` process-level package was repeated. The EC path now reuses and closes peer clients per conversion, and the reliable fast gate defaults to count=1 with targeted high-count stress for Replicator. `VERIFY_COUNT=1 bash scripts/verify.sh --level fast` — PASS.
- Real three-node Kubernetes deployment/failover was not run: the configured Docker Desktop Kubernetes API refused the connection.
- Real S3 backup → whole-cluster loss → new-cluster restore was not run: no production S3 repository and committed backup ID were supplied.
- Real Docker network fault injection was not run: no prepared multi-node image/stack was available.

## Verdict

The repository now has executable gates and CI wiring, but it does **not** yet have production-release evidence. Release approval remains blocked on a green full fast gate plus successful real Helm, S3 restore, and fault-injection runs.
