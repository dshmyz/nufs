# Metadata Disaster Recovery Runbook

This runbook covers metadata backup operations and offline new-cluster restore.
The ops API intentionally does not expose an online restore endpoint.

## Repository Configuration

Configure every metad node with the same backup repository and cluster identity:

```sh
--backup-enabled=true
--cluster-id=<stable-cluster-id>
--backup-s3-bucket=<bucket>
--backup-s3-prefix=<environment>/metadata
--backup-s3-region=<region>
--backup-interval=1h
--backup-retention=24
```

Provide S3 credentials through the runtime environment or the platform secret
mechanism used by the deployment. Credentials must allow listing, reading,
writing, and deleting objects below the configured prefix. Use a dedicated
prefix per NUFS cluster.

### Enable via Helm

The `nufs` chart wires the same flags through `metad.backup` (off by default).
Set the object-store endpoint, bucket, prefix, region, a stable cluster ID, and
the IAM/secret, then upgrade:

```sh
helm upgrade nufs deploy/helm/nufs \
  --set metad.backup.enabled=true \
  --set metad.backup.clusterId=<stable-cluster-id> \
  --set metad.backup.s3.bucket=<bucket> \
  --set metad.backup.s3.prefix=<env>/metadata \
  --set metad.backup.s3.region=<region> \
  --set metad.backup.s3.endpoint=<s3-compatible-endpoint> \
  --set metad.backup.credentialsSecret=<k8s-secret>  # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
```

For AWS IRSA / workload identity, annotate the metad ServiceAccount instead of
providing `credentialsSecret`. The local temp dir (`metad.backup.localTmpDir`)
is scratch only — the S3 bucket is the durable, cross-fault-domain target. The
backup coordinator runs on the raft leader only.

## Manual Operations

List recent backup tasks:

```sh
curl -fsS "$METAD_OPS/api/v1/backups?limit=20"
```

Check coordinator status:

```sh
curl -fsS "$METAD_OPS/api/v1/backups/status"
```

Create a backup. This must be sent to the leader; followers return a 307
redirect to the leader ops URL:

```sh
curl -fsS -X POST -L "$METAD_OPS/api/v1/backups"
```

Verify a committed backup artifact:

```sh
curl -fsS -X POST "$METAD_OPS/api/v1/backups/$BACKUP_ID/verify"
```

Expected structured errors:

```json
{"code":"backup_disabled","error":"backup coordinator is not configured"}
{"code":"backup_in_progress","error":"backup is currently in progress"}
{"code":"backup_failed","error":"..."}
```

## New-Cluster Restore

1. Stop writers and record the last healthy backup ID from
   `/api/v1/backups` or the repository catalog.
2. Provision a fresh metad data directory and an empty Raft directory for the
   replacement cluster.
3. Fetch and verify the selected backup with the offline restore tooling.
4. Restore metadata into the fresh data directory. Do not restore old Raft
   peer state into the new cluster.
5. Start metad with a new Raft bootstrap configuration and the same
   `--cluster-id`.
6. Keep gateway and datanode write traffic disabled until readiness checks and
   scrub pass.
7. Re-enable traffic only after the restore pending marker is cleared by the
   documented readiness workflow.

## Readiness And Scrub Checks

Run:

```sh
curl -fsS "$METAD_OPS/ready"
curl -fsS "$METAD_OPS/api/v1/cluster/status"
curl -fsS "$METAD_OPS/api/v1/scrub"
```

Confirm the restored cluster is leader-elected, ready, and free of metadata
scrub errors before allowing writes.

## Alert Diagnosis

`NUFSBackupStale` means no recent committed backup was observed. Check
`nufs_backup_active`, `/api/v1/backups/status`, repository credentials, and
metad logs for `backup_failed` errors.

`NUFSBackupVerificationFailed` means an operator-triggered verify failed.
Repeat verify for the same ID, inspect the structured error, and check the
repository object set for missing or corrupted files.

`NUFSChunkTombstoneBacklog` means retained chunk tombstones are older than the
expected purge window. Check that backup catalog reconciliation is fresh, at
least one committed backup is retained, and physical purge is enabled.

## Disable Pruning And Physical Purge

To preserve evidence during an incident, disable backup pruning by increasing
`--backup-retention` above the current repository count before restarting the
leader. To stop physical chunk purge, run metad with GC dry-run enabled:

```sh
--gc-dry-run=true
```

Leave backups enabled while purge is disabled so the retained-backup catalog
continues to advance.

## Rollback And Failed-Restore Cleanup

If validation fails before traffic is enabled, stop the restored metad nodes,
discard the new data and Raft directories, and restart from the selected backup
or a newer verified backup. Do not reuse a partially restored data directory.

If traffic was already enabled, stop writers first, preserve the failed
restore's data directory for investigation, and perform another new-cluster
restore from the last known-good backup. Keep old and new cluster endpoints
separated until operators explicitly cut traffic back over.

---

## Related

- **Leader failover drill** (`leader-failover-drill.md`) — recurring raft
  leader-kill drill asserting recovery-time objective (RTO), graceful
  degradation, and byte-exact durability. Backups cover *recovery from data
  loss*; the drill covers *availability during leadership change*; run both for
  the 5-9 tier.
