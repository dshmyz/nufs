# P0 production-gate result (2026-08-24)

## Green evidence

- `go test ./datanode -run '^TestReplicator_(ConnectionReuse|PoolClosesOnStop|PoolReconnectsAfterFailure)$' -count=20` — PASS.
- `go vet ./...` — PASS.
- `go test ./...` in `nufs-admin` — PASS.
- `npm ci && npm run build` in `nufs-admin/web` — PASS. Local npm audit reported 3 moderate and 3 high vulnerabilities; dependency remediation remains separate work.
- `go test ./metadata -run 'TestRestore|TestBackup.*S3|TestCreateBackupCheckpoint' -count=1` — PASS.
- Helm smoke syntax/help/lint/render-only checks — PASS.

## Not green / not yet proven

- `scripts/verify.sh --level fast` did not complete green in the local full serial run. EC topology tests failed with ephemeral-port/resource errors; the two named EC tests and the metadata restore test passed when rerun individually. This remains a test-harness stability issue until reproduced and fixed under the same gate.
- Real three-node Kubernetes deployment/failover was not run: the configured Docker Desktop Kubernetes API refused the connection.
- Real S3 backup → whole-cluster loss → new-cluster restore was not run: no production S3 repository and committed backup ID were supplied.
- Real Docker network fault injection was not run: no prepared multi-node image/stack was available.

## Verdict

The repository now has executable gates and CI wiring, but it does **not** yet have production-release evidence. Release approval remains blocked on a green full fast gate plus successful real Helm, S3 restore, and fault-injection runs.
