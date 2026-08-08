#!/usr/bin/env bash
# Shared helpers for launching a multi-process baremetal metad raft cluster.
# Source this from a test script; do not run directly.
set -u

METAD_BIN="${METAD_BIN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../bin" && pwd)/metad}"
N=${N:-3}
BASE_DIR="${BASE_DIR:-/tmp/nufs-metad-cluster}"
BASE_RAFT_PORT=${BASE_RAFT_PORT:-7010}
BASE_OPS_PORT=${BASE_OPS_PORT:-8130}

# meta_n flags -> global array METAD_FLAGS[n]
metad_common=(
  --allow-insecure-dev
  --raft=true
)

launch_meta() {
  local n="$1"; shift
  local raft_port=$((BASE_RAFT_PORT + n))
  local ops_port=$((BASE_OPS_PORT + n))
  local dir="$BASE_DIR/m$n"
  local peer_spec=""
  local i
  if test -n "${BOOTSTRAP_PEERS_FOR:-}"; then
    peer_spec="$BOOTSTRAP_PEERS_FOR"
  fi
  mkdir -p "$dir"
  "$METAD_BIN" \
    ${metad_common[@]+"${metad_common[@]}"} \
    --node-id="$n" \
    --raft-addr="127.0.0.1:$raft_port" \
    --raft-advertise-addr="127.0.0.1:$raft_port" \
    --raft-dir="$dir/raft" \
    --data-dir="$dir/pebble" \
    --ops-addr="127.0.0.1:$ops_port" \
    --advertise-ops-addr="http://127.0.0.1:$ops_port" \
    --raft-peer-ops="meta-1=http://127.0.0.1:$((BASE_OPS_PORT+1)),meta-2=http://127.0.0.1:$((BASE_OPS_PORT+2)),meta-3=http://127.0.0.1:$((BASE_OPS_PORT+3))" \
    --raft-bootstrap-peers="$peer_spec" \
    "$@" \
    > "$BASE_DIR/m$n/out.log" 2>&1 &
  echo "launched meta-$n pid=$! ops=$ops_port raft=$raft_port dir=$dir peer_spec=[$peer_spec]"
}
