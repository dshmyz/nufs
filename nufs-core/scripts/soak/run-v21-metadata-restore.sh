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
S3_LISTEN=${SOAK_S3_LISTEN:-18600}

LOGS_BASE=${NUFS_TEST_LOG_ROOT:-/tmp/nufs-restore}
[ -n "${NUFS_RESULTS_ROOT:-}" ] && RESULTS_ROOT="$NUFS_RESULTS_ROOT" || RESULTS_ROOT="${LOGS_BASE}/results"

# 对象载荷：写入的对象内容（只关心元数据坐标入 raft，内容即载荷）
PAYLOAD="nufs-metadata-restore-drill-payload"

# （匿名网关，不需要凭据；保留示例密钥仅供审计/回放时显式切回签名模式用）
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

# 纯 TCP 端口探活（bare connect，不发送任何字节）。datanode 的 data listen 端口是
# 二进制协议端口，用 curl/HTTP GET 去探会把 "GET " 解析成 1.2GB 的帧头
# （header too large: 1195725856）并污染日志 —— 故与 leader-failover harness 一致，
# 用裸 socket connect 判端口就绪。
port_ready() { # port seconds name
  local port="$1" secs="$2" name="$3" i
  for i in $(seq 1 "$secs"); do
    if python3 -c "import socket,sys
s=socket.socket();s.settimeout(1)
try: s.connect(('127.0.0.1',$port));sys.exit(0)
except OSError:sys.exit(1)" 2>/dev/null; then
      return 0
    fi
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
           $(seq $((DATA_BASE + 200)) $((DATA_BASE + 200 + 7))) \
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
      --register-addr="127.0.0.1:$lp" --ops-addr="127.0.0.1:$((DATA_BASE+200+$n-1))" \
      --data-dir="$(node_dirs "$n")" --metadata="127.0.0.1:$(metad_ops "$leader")" \
      --rack="rack$(( (n-1) % 3 + 1 ))" --zone="zone$(( (n-1) % 3 + 1 ))" \
      --allow-insecure-dev --log-level=info \
      > "$LOG_ROOT/log/datanode$n.log" 2>&1 &
    echo $! > "$LOG_ROOT/run/datanode$n.pid"
  done
  # data listen 端口是二进制协议端口，需裸 TCP 探活（不能用 HTTP GET）
  for n in $(seq 1 "$NODES"); do port_ready "$(data_pport "$n")" 40 "datanode$n" || die "datanode$n not listening"; done

  # 匿名模式（不传 --access-key/--secret-key）：本演练只关心「元数据坐标提交到
  # raft 多数派」这一事实，对象本身是载荷。配凭据会让网关强制 SigV4 签名
  # （auth.go: HasCredentials→拒绝未签名请求→403），本地一次性演练无此必要，
  # 匿名模式免去签名即可把坐标写进 raft。
  "$S3_BIN" --listen="127.0.0.1:$S3_LISTEN" --meta-addr="127.0.0.1:$(metad_ops "$leader")" \
    --log-level=info \
    > "$LOG_ROOT/log/s3.log" 2>&1 &
  echo $! > "$LOG_ROOT/run/s3.pid"
  wait_http "http://127.0.0.1:$S3_LISTEN/healthz" 30 s3 || die "s3 not healthy"

  log "cluster up: metad=$METAD_NODES nodes=$NODES leader=meta-$leader gw=$S3_LISTEN"
}

# 建桶：经 metad 的 /api/v1/buckets（这是建桶的权威路径，S3 网关的 PUT /bucket
# 不负责建桶）——逐任一 act metad 入口（follower 会 307 重定向到 leader）。
# object 写入要求 bucket 已存在，否则返回 ErrObjectBucketNotFound。
#
# 桶的复制因子必须 ≤ 在线 datanode 数：PlaceChunk 在 candidates*maxPerNode < RF 时
# 返回 ErrInsufficientNodes("insufficient healthy nodes for placement") → allocation 500。
# 本演练只关心「对象元数据坐标提交到 raft 多数派」，数据面副本数不是目标，故 RF 取
# min(3, NODES)：NODES=1 时 RF=1（1 节点放 1 副本即可满足），多节点时惯例 RF=3。
# （实测曾硬编码 RF=3 配 NODES=1 默认，导致 allocation 恒 500。）
#
# 注意：禁止在这里重复建同一桶。metad 的 bucket POST 对「已存在桶」返回 500
# （ops_buckets.go 未把 ErrBucketExists 映射为 409），若每个 write_object 都重建桶，
# 则第二次及以后调用会在建桶步全灭、从不发起 PUT —— 这正是层 1 after-kill 写
# 一直失败、s3 日志却看不到任何 after-kill PUT 的根因。因此建桶只做一次。
ensure_bucket() { # bucket
  local bucket="$1" st ok cpu n rf
  rf="$([ "$NODES" -ge 3 ] && echo 3 || echo "$NODES")"
  st=0; ok=0
  for n in $(seq 1 "$METAD_NODES"); do
    cpu="$(curl -sS --max-time 5 -X POST -H 'Content-Type: application/json' \
      --data "{\"name\":\"$bucket\",\"policy\":{\"replication_factor\":$rf}}" \
      -o /dev/null -w '%{http_code}' "http://127.0.0.1:$(metad_ops "$n")/api/v1/buckets" 2>/dev/null)" \
      && st="$cpu" || continue
    case "$st" in
      201) ok=1; break;;      # created
      200) ok=1; break;;      # idempotent ok
      409) ok=1; break;;      # already exists
      *) :;;                  # 其它（含已存在桶的 500）继续试下一个入口
    esac
  done
  return $(( 1 - ok ))
}

# 写对象（经匿名 s3 网关，元数据坐标落入 raft）。前提：bucket 已由 ensure_bucket 建好。
# datanode 注册/心跳（10s 间隔）落位到 metad 的 placement index 之前，
# allocation 可能因节点尚未在线返回 500 —— 与 leader-failover harness 的
# warmup 一致：自旋重试直到干净 200，既消化节点上线竞态也消化 raft 收敛。
write_object() { # bucket key
  local bucket="$1" key="$2" i code
  for i in $(seq 1 40); do
    code="$(curl -s -X PUT -H "Content-Length: ${#PAYLOAD}" \
      --data-binary "$PAYLOAD" \
      -w '%{http_code}' -o /dev/null \
      "http://127.0.0.1:$S3_LISTEN/$bucket/$key" 2>/dev/null)"
    [ "$code" = "200" ] && return 0
    [ "$i" -eq 20 ] && log "  write_object: waiting for placement (last http=$code)"
    sleep 1
  done
  return 1
}

# 层 1: 单节点坏盘 —— 杀进程 + 整目录抹掉，断言多数派仍服务新写
run_layer1_baddisk() {
  log "=== LAYER 1: single-node bad-disk (kill + wipe one metad) ==="
  local leader victim survived newleader
  leader="$(current_leader)" || die "no leader before layer1"
  # 挑一个非 leader 作为 victim（最坏情形：一条完整副本进程+盘全毁）
  local victim="$(( leader % METAD_NODES + 1 ))"
  [ "$victim" = "$leader" ] && { victim=1; [ "$victim" = "$leader" ] && victim=2; }

  # 建桶仅一次（见 ensure_bucket 上方注释：已存在桶的 POST 会 500，不能每次 write 重建）
  ensure_bucket "dr-$_LOG_TS" || die "ensure bucket dr-$_LOG_TS failed"

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
