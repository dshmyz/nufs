#!/usr/bin/env bash
# Empirical test: does a multi-process baremetal 3-node metad raft cluster converge?
# PRE-FIX baseline (expected to FAIL for non-bootstrap nodes before the join driver).
set -eu
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BASE_DIR="${BASE_DIR:-/tmp/nufs-metad-cluster}"
rm -rf "$BASE_DIR"
mkdir -p "$BASE_DIR"
export BASE_DIR
source "$ROOT/scripts/dev/metad-cluster-helpers.sh"

# meta-1 is the ONLY bootstrap node; it carries the full peer list.
# meta-2 / meta-3 run as singletons (no bootstrap, no peers) — the authoritative pattern.
BOOTSTRAP_PEERS_FOR="meta-1=127.0.0.1:$((BASE_RAFT_PORT+1)),meta-2=127.0.0.1:$((BASE_RAFT_PORT+2)),meta-3=127.0.0.1:$((BASE_RAFT_PORT+3))"
launch_meta 1 --raft-bootstrap=true
# singletons: no bootstrap, no bootstrap peers
export BOOTSTRAP_PEERS_FOR=""
launch_meta 2
launch_meta 3

sleep 6

echo ""
echo "==== raft stats ===="
for n in 1 2 3; do
  ops=$((BASE_OPS_PORT+n))
  echo "--- meta-$n ($ops) ---"
  curl -s "http://127.0.0.1:$ops/api/v1/cluster/status" | head -c 600 || echo "curl failed"
  echo ""
done

echo ""
echo "==== who is leader / members ===="
grep -h "leader\|state=\|added\|membership\|bootstrap\|part of the cluster" "$BASE_DIR"/m*/out.log 2>/dev/null | sort -u | head -30
