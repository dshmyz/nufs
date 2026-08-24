# Task 4 report: real backup/restore and fault injection gate

Implemented:

- `nufs-core/scripts/soak/run-v21-production-drill.sh`: strict entry point
  requiring an S3 repository config, committed backup ID, empty restore target,
  new cluster ID, and the Docker network-fault environment.
- `scripts/soak/run-v21-network-faults.sh`: scenario failures now return
  non-zero; unavailable S3 and failed baseline readback are no longer warning-only.
- `nufs-core/docs/runbooks/production-backup-network-drill.md`: operator gate.

Local validation:

- `bash -n` passed for both scripts.
- `--help` passed for the production entry point.
- Missing real S3 config fails before any mutation.
- Focused restore/backup metadata tests passed:
  `go test ./metadata -run 'TestRestore|TestBackup.*S3|TestCreateBackupCheckpoint' -count=1`.

Real environment status:

- Not run here: no production S3 repository config/backup ID was supplied.
- Not run here: Docker is installed, but no image/cluster/S3 gateway was
  provisioned for this task. The gate therefore remains unpassed until the
  operator supplies the real environment and archives its report.
