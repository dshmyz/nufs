#!/bin/bash
# 最小复现:datanode 注册 metad 时的 503 具体来源
# 起 3 个真实 metad-raft,等 leader,然后对 leader POST /api/v1/nodes 抓完整 503 body。
set -euo pipefail
cd "$(dirname "$0")/../.."

BIN=bin/metad
ROOT="${REPRO_ROOT:-/tmp/nufs-regrepro}"
rm -rf "$ROOT"; mkdir -p "$ROOT"/{log,metad1,metad2,metad3,raft1,raft2,raft3}
OPS=(18100 18101 18102); RAFT=(18200 18201 18202)
PID=""
cleanup() { [ -n "$PID" ] && kill $PID 2>/dev/null || true; pkill -9 -f "$BIN --node-id" 2>/dev/null || true; }
trap cleanup EXIT

for i in 1 2 3; do
  n=$((i-1))
  "$BIN" --node-id=$i --data-dir="$ROOT/metad$i" --ops-addr=127.0.0.1:${OPS[$n]} \
    --raft=true --raft-bootstrap=true --raft-bootstrap-owner=meta-1 \
    --raft-addr=127.0.0.1:${RAFT[$n]} --raft-advertise-addr=127.0.0.1:${RAFT[$n]} \
    --raft-dir="$ROOT/raft$i" \
    --raft-bootstrap-peers="meta-1=127.0.0.1:18200,meta-2=127.0.0.1:18201,meta-3=127.0.0.1:18202" \
    --raft-peer-ops="meta-1=http://127.0.0.1:18100,meta-2=http://127.0.0.1:18101,meta-3=http://127.0.0.1:18102" \
    --advertise-ops-addr="http://127.0.0.1:${OPS[$n]}" --allow-insecure-dev --log-level=info \
    > "$ROOT/log/metad$i.log" 2>&1 &
  PID="$PID $!"
done

echo "waiting for raft leader..."
LEADER=""
for t in $(seq 1 60); do
  for i in 1 2 3; do
    n=$((i-1))
    body="$(curl -sf --max-time 2 "http://127.0.0.1:${OPS[$n]}/api/v1/cluster/status" 2>/dev/null)" || continue
    if printf '%s' "$body" | python3 -c "import sys,json;print(json.load(sys.stdin).get('is_leader',False))" 2>/dev/null | grep -q True; then
      LEADER=$i; break 2
    fi
  done
  sleep 1
done
[ -n "$LEADER" ] || { echo "NO LEADER ELECTED"; exit 1; }
n=$((LEADER-1))
echo "leader = meta-$LEADER (ops 127.0.0.1:${OPS[$n]})"

# 立即(选主后立刻,尽量贴近 datanode 冷启动时机)对 leader POST 注册一个最小 NodeInfo
echo "=== POST /api/v1/nodes to leader, immediate ==="
curl -si -X POST "http://127.0.0.1:${OPS[$n]}/api/v1/nodes" \
  -H 'Content-Type: application/json' \
  --data '{"id":1,"addr":"127.0.0.1:18300","data_dir":"/tmp/x","rack":"rack1","zone":"zone1","machine_id":"m1","tier":0,"capacity_gb":0,"used_gb":0,"chunk_count":0,"state":"online","last_seen":0,"shard_disk_count":0}' \
  2>&1 | grep -E "HTTP/|error|message|code|\{|msg" | head -20

# 连续 15 秒每 500ms 打一次注册,看 503 是瞬态 catch-up 还是长期冻结
echo "=== continuous register sampling for 15s (500ms interval) ==="
for k in $(seq 1 30); do
  code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:${OPS[$n]}/api/v1/nodes" \
    -H 'Content-Type: application/json' \
    --data '{"id":1,"addr":"127.0.0.1:18300","data_dir":"/tmp/x","rack":"rack1","zone":"zone1","machine_id":"m1","tier":0,"capacity_gb":0,"used_gb":0,"chunk_count":0,"state":"online","last_seen":0,"shard_disk_count":0}' 2>/dev/null)"
  printf 't=%02ds http=%s\n' $((k/2)) "$code"
  sleep 0.5
done
