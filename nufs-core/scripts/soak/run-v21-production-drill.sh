#!/usr/bin/env bash
# Strict production drill entry point.
#
# This command intentionally requires a real S3 repository for restore and a
# real Docker multi-node fault-injection environment. It never falls back to
# filesystem restore fixtures or reports a warning-only network run as PASS.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
CORE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
REPO_ROOT="$(cd "$CORE_DIR/.." && pwd -P)"
RESULTS_DIR="${NUFS_DRILL_RESULTS:-$CORE_DIR/.drill-results/production-$(date +%Y%m%dT%H%M%S)}"
REPOSITORY_CONFIG="${NUFS_BACKUP_REPOSITORY_CONFIG:-}"
BACKUP_ID="${NUFS_BACKUP_ID:-}"
RESTORE_TARGET="${NUFS_RESTORE_TARGET:-}"
NEW_CLUSTER_ID="${NUFS_RESTORE_CLUSTER_ID:-}"
NETWORK_RESULTS="${NUFS_NETWORK_RESULTS:-$RESULTS_DIR/network}"

usage() {
  cat <<'EOF'
Usage: run-v21-production-drill.sh [options]

Run the real backup/restore and network-fault production gates.

Options:
  --repository-config FILE  JSON repository config; must have type=s3.
  --backup-id ID            committed backup to inspect and restore.
  --restore-target DIR      empty target directory for the new cluster.
  --new-cluster-id ID       cluster ID different from the source cluster.
  --results DIR             evidence directory.
  --network-endpoint URL    S3 endpoint used by the network drill.
  --help                    show this help.

The same values may be supplied with NUFS_BACKUP_REPOSITORY_CONFIG,
NUFS_BACKUP_ID, NUFS_RESTORE_TARGET, NUFS_RESTORE_CLUSTER_ID, and
NUFS_NETWORK_RESULTS. This gate fails when Docker, the real S3 repository, or
the multi-node compose environment is unavailable.
EOF
}

die() { printf '[production-drill] ERROR: %s\n' "$*" >&2; exit 1; }
require_command() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }

while [ "$#" -gt 0 ]; do
  case "$1" in
    --repository-config) [ "$#" -ge 2 ] || die "--repository-config requires a file"; REPOSITORY_CONFIG="$2"; shift 2 ;;
    --backup-id) [ "$#" -ge 2 ] || die "--backup-id requires an ID"; BACKUP_ID="$2"; shift 2 ;;
    --restore-target) [ "$#" -ge 2 ] || die "--restore-target requires a directory"; RESTORE_TARGET="$2"; shift 2 ;;
    --new-cluster-id) [ "$#" -ge 2 ] || die "--new-cluster-id requires an ID"; NEW_CLUSTER_ID="$2"; shift 2 ;;
    --results) [ "$#" -ge 2 ] || die "--results requires a directory"; RESULTS_DIR="$2"; NETWORK_RESULTS="$RESULTS_DIR/network"; shift 2 ;;
    --network-endpoint) [ "$#" -ge 2 ] || die "--network-endpoint requires a URL"; NETWORK_ENDPOINT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

NETWORK_ENDPOINT="${NETWORK_ENDPOINT:-http://localhost:8180}"
mkdir -p "$RESULTS_DIR" "$NETWORK_RESULTS"
require_command go
require_command curl
require_command docker
require_command python3
[ -x "$REPO_ROOT/scripts/soak/run-v21-network-faults.sh" ] || die "network drill is not executable"
[ -f "$REPOSITORY_CONFIG" ] || die "real S3 repository config is required"
[ -n "$BACKUP_ID" ] || die "--backup-id is required"
[ -n "$RESTORE_TARGET" ] || die "--restore-target is required"
[ -n "$NEW_CLUSTER_ID" ] || die "--new-cluster-id is required"
[ -d "$RESTORE_TARGET" ] && [ -n "$(find "$RESTORE_TARGET" -mindepth 1 -maxdepth 1 -print -quit)" ] && \
  die "restore target must be empty: $RESTORE_TARGET"

python3 - "$REPOSITORY_CONFIG" <<'PY'
import json, sys
path = sys.argv[1]
with open(path, encoding="utf-8") as fh:
    cfg = json.load(fh)
if cfg.get("type") != "s3":
    raise SystemExit("repository config type must be s3; filesystem restore is not production evidence")
for key in ("bucket", "endpoint"):
    if not cfg.get(key):
        raise SystemExit(f"repository config missing {key}")
PY

if [ ! -x "$CORE_DIR/bin/nufs-restore" ]; then
  mkdir -p "$CORE_DIR/bin"
  (cd "$CORE_DIR" && go build -o bin/nufs-restore ./cmd/nufs-restore)
fi

printf 'stage=inspect\n' > "$RESULTS_DIR/REPORT.txt"
"$CORE_DIR/bin/nufs-restore" --json inspect "$BACKUP_ID" \
  --repository-config "$REPOSITORY_CONFIG" > "$RESULTS_DIR/backup-inspect.json"
python3 - "$RESULTS_DIR/backup-inspect.json" "$NEW_CLUSTER_ID" <<'PY'
import json, sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
if not data.get("source_cluster_id"):
    raise SystemExit("backup inspect did not return source_cluster_id")
if data["source_cluster_id"] == sys.argv[2]:
    raise SystemExit("new cluster ID must differ from source cluster ID")
PY

mkdir -p "$RESTORE_TARGET"
"$CORE_DIR/bin/nufs-restore" --json restore "$BACKUP_ID" \
  --repository-config "$REPOSITORY_CONFIG" --target-dir "$RESTORE_TARGET" \
  --new-cluster-id "$NEW_CLUSTER_ID" > "$RESULTS_DIR/restore.json"
[ -n "$(find "$RESTORE_TARGET" -mindepth 1 -maxdepth 1 -print -quit)" ] || die "restore produced an empty target"

NUFS_NETWORK_RESULTS="$NETWORK_RESULTS" S3_ENDPOINT="$NETWORK_ENDPOINT" \
  "$REPO_ROOT/scripts/soak/run-v21-network-faults.sh" > "$RESULTS_DIR/network.log" 2>&1

printf 'result=PASS\nbackup_id=%s\nrestore_target=%s\nnetwork_results=%s\n' \
  "$BACKUP_ID" "$RESTORE_TARGET" "$NETWORK_RESULTS" | tee "$RESULTS_DIR/REPORT.txt"
