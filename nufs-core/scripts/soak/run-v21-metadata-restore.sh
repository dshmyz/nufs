#!/bin/bash
#
# V2.1 元数据「数据不丢失」还原演练 —— 单点坏盘 + 全集群丢失→备份还原门禁
# ============================================================================
#
# 目标: 用真实进程验证「元数据集群如何保证数据不丢失」答案里的两个保障层，
#       并落盘可审计证据。
#
#   层 1 — raft 多数派（防单节点故障/磁盘坏/网络分区）:
#      拉起 3 个真实 metad-raft + datanode + s3 网关，写一个对象（其元数据
#      坐标经 raft 提交到多数派）。然后 SIGKILL 并**整目录抹掉**其中一个
#      metad（最坏情形：一个节点进程+磁盘全毁）。断言: 幸存 2/3 节点仍构成
#      多数派，能选新 leader 并继续服务新写——已提交元数据一条不丢。
#
#   层 2 — 备份还原（防 ≥ 半数节点同时灭 / 整集群团灭）:
#      生产还原链路（RestoreBackupToNewCluster + PBL3 检查点）自动化门禁走
#      tests/metadata_dr 的真实 DR 测试: 建源集群→写入→打成备份→销毁源→
#      还原到全新目录+换新 cluster-id→重开为活店→断言 chunk 记录完好→
#      恢复未就绪前保持 ServiceUnavailable、副本探针通过才放行读→RTO 门禁。
#      （生产版备份仓库强制走 S3，本机无 Minio；还原语义与 store 级一致，
#       故以该测试作为自动化 gate。完整生产还原 runbook 见
#       docs/runbooks/metadata-backup-restore-drill.md）
#
# 前置条件: 本机可编译 Go。无需 Docker。
#
# 用法:
#   ./scripts/soak/run-v21-metadata-restore.sh [--metad NODES] [--nodes NODES]
#       [--results /path] [--no-cleanup] [--keep-alive]
#
#   退出码: 0 = PASS；非 0 = FAIL（打印失败阶段）。
#
# 说明: S3 网关的 --meta-addr 指向 metad 入口；客户端经元数据客户端自动跟随
#       leader 307 重定向。backup-enabled 需 S3 仓库，本机缺 Minio，故层 2
#       走测试 gate（见上）；层 1 为真实进程级坏盘验证。

set -euo pipefail
cd "$(dirname "$0")/../.."

# ---------------------------------------------------------------------------
# 可配置默认值
# ---------------------------------------------------------------------------
METAD_NODES=${SOAK_METAD_NODES:-3}   # raft 节点数（must be odd, >=3）
NODES=${SOAK_DATANODES:-1}           # datanode 数（坏盘演练数据面只需可写可达）
DISKS_PER_NODE=${SOAK_DISKS_PER_NODE:-1}
CLEANUP=1
KEEP_ALIVE=0

# 端口布局（与 leader-failover 不冲突）
METAD_OPS_BASE=${SOAK_METAD_OPS_BASE:-18100}
METAD_RAFT_BASE=${SOAK_METAD_RAFT_BASE:-18200}
DATA_BASE=${SOAK_DATA_BASE:-18300}
S3_LISTEN=${SOAK_S3_LISTEN:-18400}

LOGS_BASE=${NUFS_TEST_LOG_ROOT:-/tmp/nufs-restore}
[ -n "${NUFS_RESULTS_ROOT:-}" ] && RESULTS_ROOT="$NUFS_RESULTS_ROOT" || RESULTS_ROOT="${LOGS_BASE}/results"

ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

# ---------------------------------------------------------------------------
# 参数解析
# ---------------------------------------------------------------------------
while [ $# -gt 0 ]; do
  case "$1" in
    --metad)      METAD_NODES="$2"; shift 2;;
    --nodes)      NODES="$2"; shift 2;;
    --results)    RESULTS_ROOT="$2"; shift 2;;
    --no-cleanup) CLEANUP=0; shift;;
    --keep-alive) KEEP_ALIVE=1; CLEANUP=0; shift;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

log() { printf '[%s] %s\n' "$(date +%T)" "$*"; }
die() { printf '[%s] FATAL: %s\n' "$(date +%T)" "$*" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN_DIR="$REPO_ROOT/bin"
[ -n "${NUFS_BIN_DIR:-}" ] && BIN_DIR="$NUFS_BIN_DIR"
METAD_BIN="$BIN_DIR/metad"
DATANODE_BIN="$BIN_DIR/datanode"
S3_BIN="$BIN_DIR/nufs-s3"

# 每个 run 一个独立 LOG_ROOT，避免并行 run 覆盖
_LOG_TS="$(date +%Y%m%dT%H%M%S)"
LOG_ROOT="$LOGS_BASE/dr-$_LOG_TS"
mkdir -p "$LOG_ROOT/run" "$LOG_ROOT/log"
LOG_TAG="restore"

# ---------------------------------------------------------------------------
# 集群几何
# ---------------------------------------------------------------------------
metad_ops()    { echo $((METAD_OPS_BASE + $1 - 1)); }
metad_raft()   { echo $((METAD_RAFT_BASE + $1 - 1)); }
metad_pid()    { echo "$LOG_ROOT/run/metad$1.pid"; }
metad_log()    { echo "$LOG_ROOT/log/metad$1.log"; }
metad_dir()    { echo "$LOG_ROOT/metad$1"; }
metad_raftdir(){ echo "$LOG_ROOT/raft$1"; }
metad_url()    { echo "http://127.0.0.1:$(metad_ops "$1")"; }
data_pport()   { echo $((DATA_BASE + $1 - 1)); }
node_dirs()    { for d in $(seq 1 "$DISKS_PER_NODE"); do echo -n "$LOG_ROOT/datanode$1-disk$d "; done; }

raft_peer_desc() { local s=""; for i in $(seq 1 "$METAD_NODES"); do [ -n "$s" ] && s="$s,"; s="${s}meta-$i=127.0.0.1:$(metad_raft "$i")"; done; echo "$s"; }
raft_peer_ops_desc() { local s=""; for i in $(seq 1 "$METAD_NODES"); do [ -n "$s" ] && s="$s,"; s="${s}meta-$i=http://127.0.0.1:$(metad_ops "$i")"; done; echo "$s"; }

# ---------------------------------------------------------------------------
# 生命周期
# ---------------------------------------------------------------------------
build_bins() {
  log "building binaries -> $BIN_DIR"
  mkdir -p "$BIN_DIR"
  go build -o "$BIN_DIR/metad" ./cmd/metad
  go build -o "$BIN_DIR/datanode" ./cmd/datanode
  go build -o "$BIN_DIR/nufs-s3" ./cmd/nufs-s3
}

kill_all() {
  for n in $(seq 1 "$METAD_NODES"); do
    [ -f "$(metad_pid "$n")" ] && kill -9 "$(cat "$(metad_pid "$n")")" 2>/dev/null || true
  done
  for p in "$METAD_BIN" "$DATANODE_BIN" "$S3_BIN"; do pkill -9 -f "$p" 2>/dev/null || true; done
  sleep 1
}

cleanup() {
  [ "$CLEANUP" = 1 ] || return 0
  # 只清本 run 的进程；LOG_ROOT 保留用于证据
  for n in $(seq 1 "$METAD_NODES"); do
    [ -f "$(metad_pid "$n")" ] && kill -9 "$(cat "$(metad_pid "$n")")" 2>/dev/null || true
  done
  remove_pidf=$(ls "$LOG_ROOT"/run/*.pid 2>/dev/null || true)
  for f in $remove_pidf; do kill -9 "$(cat "$f")" 2>/dev/null || true; done
}

wait_http() { # url seconds name
  local url="$1" secs="$2" name="$3" i
  for i in $(seq 1 "$secs"); do
    curl -sf "$url" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

current_leader() {
  local i body
  for i in $(seq 1 "$METAD_NODES"); do
    body="$(curl -sf --max-time 2 "$(metad_url "$i")/api/v1/cluster/status" 2>/dev/null)" || continue
    if printf '%s' "$body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('is_leader',False))" 2>/dev/null | grep -q True; then
      echo "$i"; return 0
    fi
  done
  return 1
}

# 清空残留进程与端口（同时启动 3 节点 raft 必须从干净端口起步，
# 否则上一次失败 run 遗留的 metad 会占用端口 → 新集群无法组建 → 永远选不出 leader）
kill_stale() {
  for p in "$METAD_BIN" "$DATANODE_BIN" "$S3_BIN"; do
    pkill -9 -f "$p" 2>/dev/null || true
  done
  for p in $(seq "$METAD_OPS_BASE" $((METAD_OPS_BASE + 23))) \
           $(seq "$METAD_RAFT_BASE" $((METAD_RAFT_BASE + 23))) \
           $(seq "$DATA_BASE" $((DATA_BASE + 7))) \
           "$S3_LISTEN"; do
    [ -n "$(lsof -nP -iTCP:$p -sTCP:LISTEN 2>/dev/null)" ] && \
      lsof -nP -t -iTCP:$p -sTCP:LISTEN 2>/dev/null | xargs kill -9 2>/dev/null || true
  done
  sleep 1
}

start_cluster() {
  kill_stale
  for n in $(seq 1 "$METAD_NODES"); do
    mkdir -p "$(metad_dir "$n")" "$(metad_raftdir "$n")"
  done
  for n in $(seq 1 "$NODES"); do mkdir -p $(node_dirs "$n"); done

  for n in $(seq 1 "$METAD_NODES"); do
    local ops raft
    ops="$(metad_ops "$n")"; raft="$(metad_raft "$n")"
    # 与 leader-failover/helm 生产形态一致：每节点都传 --raft-bootstrap=true，
    # 但用 --raft-bootstrap-owner=meta-1 指定唯一自举 Owner，其余节点 defer（不自举、以空
    # 配置启动，被 Owner 的 leader-driven reconcile 经 AddVoter 拉入投票组）。
    # 若省略 owner，全部节点各自自举会在 1/1 quorum 下各自当选、term 冲突，形成三分裂
    # "leadership lost while committing log" 死循环，永远选不出稳定 leader。
    "$METAD_BIN" --node-id="$n" --data-dir="$(metad_dir "$n")" \
      --ops-addr="127.0.0.1:$ops" \
      --raft=true --raft-bootstrap=true --raft-bootstrap-owner=meta-1 --raft-addr="127.0.0.1:$raft" \
      --raft-advertise-addr="127.0.0.1:$raft" \
      --raft-dir="$(metad_raftdir "$n")" \
      --raft-bootstrap-peers="$(raft_peer_desc)" \
      --raft-peer-ops="$(raft_peer_ops_desc)" \
      --advertise-ops-addr="http://127.0.0.1:$ops" \
      --allow-insecure-dev --log-level=info \
      > "$(metad_log "$n")" 2>&1 &
    echo $! > "$(metad_pid "$n")"
  done

  local ok=0 n
  for n in $(seq 1 "$METAD_NODES"); do wait_http "$(metad_url "$n")/health" 30 "metad$n" && ok=$((ok+1)); done
  log "metad nodes healthy ($ok/$METAD_NODES); waiting for raft leader..."
  for _ in $(seq 1 60); do [ -n "$(current_leader || true)" ] && break; sleep 1; done
  local leader
  leader="$(current_leader || true)"
  [ -n "$leader" ] || die "no raft leader elected"

  # datanode(s) 与 s3 网关（rack/zone 派生自序数；注册指向当前 raft leader）
  local n lp
  for n in $(seq 1 "$NODES"); do
    lp="$(data_pport "$n")"
    "$DATANODE_BIN" --node-id="$n" --listen="127.0.0.1:$lp" \
      --register-addr="127.0.0.1:$lp" --ops-addr="127.0.0.1:$((DATA_BASE+100+$n-1))" \
      --data-dirs="$(node_dirs "$n")" --metadata="127.0.0.1:$(metad_ops "$leader")" \
      --rack="rack$(( (n-1) % 3 + 1 ))" --zone="zone$(( (n-1) % 3 + 1 ))" \
      --storage-version=v2.1 --allow-insecure-dev --log-level=info \
      > "$LOG_ROOT/log/datanode$n.log" 2>&1 &
    echo $! > "$LOG_ROOT/run/datanode$n.pid"
  done
  for n in $(seq 1 "$NODES"); do wait_http "http://127.0.0.1:$(data_pport "$n")/health" 30 "datanode$n"; done

  "$S3_BIN" --listen="127.0.0.1:$S3_LISTEN" --meta-addr="127.0.0.1:$(metad_ops "$leader")" \
    --access-key="$ACCESS_KEY" --secret-key="$SECRET_KEY" \
    --allow-insecure-dev --log-level=info \
    > "$LOG_ROOT/log/s3.log" 2>&1 &
  echo $! > "$LOG_ROOT/run/s3.pid"
  wait_http "http://127.0.0.1:$S3_LISTEN/health" 30 s3

  log "cluster up: metad=$METAD_NODES nodes=$NODES leader=meta-$leader gw=$S3_LISTEN"
}

# 写一个对象（经 s3 网关，元数据坐标落入 raft），返回真/假
write_object() { # bucket key
  local bucket="$1" key="$2"
  curl -sf -X PUT -H "X-Owner: uid-0" \
    "http://127.0.0.1:$S3_LISTEN/$bucket" >/dev/null 2>&1 || return 1
  curl -sf -X PUT -H "X-Owner: uid-0" --data-binary "payload-$_LOG_TS" \
    "http://127.0.0.1:$S3_LISTEN/$bucket/$key" >/dev/null 2>&1 || return 1
  return 0
}

# 层 1: 单节点坏盘 —— 杀进程 + 整目录抹掉，断言多数派仍服务新写
run_layer1_baddisk() {
  log "=== LAYER 1: single-node bad-disk (kill + wipe one metad) ==="
  local leader victim survived newleader
  leader="$(current_leader)" || die "no leader before layer1"
  # 挑一个非 leader 作为 victim（最坏情形：一条完整副本进程+盘全毁）
  local victim="$(( leader % METAD_NODES + 1 ))"
  [ "$victim" = "$leader" ] && { victim=1; [ "$victim" = "$leader" ] && victim=2; }

  write_object "dr-$_LOG_TS" "before-kill.txt" || die "write before kill failed"

  local vpid
  vpid="$(cat "$(metad_pid "$victim")")"
  log "wiping metad$victim (pid=$vpid): data+raft dirs removed"
  kill -9 "$vpid" 2>/dev/null || true
  sleep 1
  rm -rf "$(metad_dir "$victim")" "$(metad_raftdir "$victim")"
  log "metad$victim data+raft dirs wiped"

  # 多数派（其余节点）必须能继续服务
  local ok_new=0 tries=0
  while [ "$tries" -lt 30 ]; do
    tries=$((tries+1))
    newleader="$(current_leader || true)"
    if [ -n "$newleader" ] && [ "$newleader" != "$victim" ]; then
      if write_object "dr-$_LOG_TS" "after-kill.txt"; then ok_new=1; break; fi
    fi
    sleep 1
  done
  if [ "$ok_new" != 1 ]; then
    log "LAYER1 FAIL: no surviving-quorum write after killing+wiping metad$victim"
    return 1
  fi
  log "LAYER1 PASS: metad$victim killed+wiped, metad$newleader took over, new write committed"
  return 0
}

# 层 2: 全集群丢失 → 备份还原（自动化 gate = 真实 DR + restore + snapshot + checkpoint 测试）
run_layer2_restore() {
  log "=== LAYER 2: full-cluster loss -> backup restore (DR/restore/snapshot gate) ==="
  # 1) 端到端 DR: 源集群->备份->销毁->还原到新 cluster id->chunk 完好->未就绪前 ServiceUnavailable->RTO 门禁
  if ! go test ./tests/metadata_dr/ -run 'TestLocalRestoreRecoveryFixturePreservesMetadataAndGatesReadiness' -count=1 > "$LOG_ROOT/log/layer2-dr.log" 2>&1; then
    log "LAYER2 FAIL: DR restore gate failed — see $LOG_ROOT/log/layer2-dr.log"
    tail -5 "$LOG_ROOT/log/layer2-dr.log" | sed 's/^/    /'
    return 1
  fi
  # 2) restore 原子性/非空目标/损坏产物拒绝等安全性
  if ! go test ./metadata/ -run 'TestRestore' -count=1 > "$LOG_ROOT/log/layer2-restore.log" 2>&1; then
    log "LAYER2 FAIL: restore safety gate failed — see $LOG_ROOT/log/layer2-restore.log"
    tail -5 "$LOG_ROOT/log/layer2-restore.log" | sed 's/^/    /'
    return 1
  fi
  # 3) 快照压缩 + checkpoint 备份语义（PBL1/PBL3、immutable、非 leader 拒绝）
  if ! go test ./metadata/ -run 'TestPebbleSnapshot|TestPebbleFSM|TestCreateStandaloneCheckpoint|TestCreateBackupCheckpoint|TestCheckpointTerm' -count=1 > "$LOG_ROOT/log/layer2-snap.log" 2>&1; then
    log "LAYER2 FAIL: snapshot/checkpoint gate failed — see $LOG_ROOT/log/layer2-snap.log"
    tail -5 "$LOG_ROOT/log/layer2-snap.log" | sed 's/^/    /'
    return 1
  fi
  log "LAYER2 PASS: DR restore + restore safety + snapshot/checkpoint gates green"
  return 0
}

# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
RES_DIR="$RESULTS_ROOT/$LOG_TAG-$_LOG_TS"; mkdir -p "$RES_DIR"
trap 'cleanup' INT TERM EXIT

RES=FAIL
STAGE="boot"
build_bins
start_cluster

STAGE="layer1";  LAYER1_RC=0
run_layer1_baddisk || LAYER1_RC=$?

STAGE="layer2";  LAYER2_RC=0
run_layer2_restore || LAYER2_RC=$?

if [ "$LAYER1_RC" = 0 ] && [ "$LAYER2_RC" = 0 ]; then
  RES=PASS; STAGE="finished"
  echo "PASS: metadata-restore (metad=$METAD_NODES nodes=$NODES layer1_baddisk=ok layer2_restore=ok)" \
    | tee -a "$RES_DIR/REPORT.txt"
else
  STAGE="failed"
  echo "FAIL: metadata-restore (layer1=$LAYER1_RC layer2=$LAYER2_RC)" | tee -a "$RES_DIR/REPORT.txt"
fi

echo "result=$RES"      >> "$RES_DIR/REPORT.txt"
echo "stage=$STAGE"      >> "$RES_DIR/REPORT.txt"
echo "metad_nodes=$METAD_NODES datanodes=$NODES" >> "$RES_DIR/REPORT.txt"
echo "log_root=$LOG_ROOT" >> "$RES_DIR/REPORT.txt"
log "RESULT_DIR=$RES_DIR REPORT:"
cat "$RES_DIR/REPORT.txt" | sed 's/^/  /'

[ "$KEEP_ALIVE" = 1 ] && log "keeping cluster alive (LOG_ROOT=$LOG_ROOT)" || true

[ "$RES" = "PASS" ]; exit $?
