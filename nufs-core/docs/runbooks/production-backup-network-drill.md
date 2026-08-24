# Production backup/restore and fault-injection gate

Use `scripts/soak/run-v21-production-drill.sh` only against a real S3-compatible
backup repository and the real multi-node Docker compose fault environment. The
gate rejects filesystem repositories, requires an existing committed backup,
restores it into an empty directory under a new cluster ID, and then runs the
network scenarios with strict exit-status checking.

```sh
./scripts/soak/run-v21-production-drill.sh \
  --repository-config /secure/nufs-backup-s3.json \
  --backup-id 2026-08-24T120000Z-abcdef \
  --restore-target /var/lib/nufs/restore-drill/new-cluster \
  --new-cluster-id restore-drill-20260824 \
  --results /var/log/nufs/production-drill
```

The repository JSON must contain `type: s3`, `bucket`, and `endpoint`; keep
credentials in the runtime environment/credential chain rather than in this
file. The backup must be created and verified by the production metad backup
API before this gate starts. `nufs-restore` proves the artifact can be fetched,
inspected, and restored to a fresh target; it intentionally does not overwrite
an existing target.

The network phase requires Docker, the `deploy/docker-compose.e2e.yml` stack,
the `nufs-netem` image, and a reachable gateway at `http://localhost:8180` (or
the endpoint configured by `--network-endpoint`). Each scenario must preserve
baseline objects after partition or loss/latency injection. Any failed read,
unavailable gateway, or failed cleanup returns non-zero and writes evidence
under the results directory.

This is a real-environment gate. The existing filesystem/Go DR tests remain
useful unit/integration coverage, but they are not accepted as evidence for the
production S3 backup/restore requirement.
