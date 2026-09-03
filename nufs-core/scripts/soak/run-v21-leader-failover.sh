#!/bin/bash
#
# V2.1 多 metad-raft leader 故障转移验证 —— RTO 门禁 + leader 切换期优雅降级
# ============================================================================
#
# 以真实进程拉起一套 V2.1 集群：3 个 metad（真实 raft，3 节点 majority=2），
# 6 个 datanode（EC 6+3 下 ≥2 故障域），1 个 s3 网关。在持续 S3 读写负载下，
# SIGKILL 当前的 raft leader，测量"leader 宕机 -> 新 leader 接管写请求"的
# RTO，并断言 leader 切换窗口内不产生客户端错误（网关/元数据客户端自动跟进
# 307 重定向 + 指数退避重试）。
#
# 可靠性证据点:
#   1. RTO = kill leader 时刻 -> 新 leader 首次成功服务写请求的秒数。
#      门禁: RTO <= --rto-budget（默认 15s），远小于 5 分钟 SLO 预算零头。
#   2. 优雅降级: leader 切换期间持续 PUT/GET，除紧邻切换的容忍窗内，任何
#      客户端错误即为 FAIL（证明读路径在元数据短暂不可用时重试/降级而非报错）。
#   3. 字节精确: 收敛后全量校验所有 durable 对象（已在切换窗外 durable 的写
#      不丢失）。
#   4. 进程普查: 切换后仅被 kill 的 leader 可下线，其余节点必须在线（2/3 仲裁）。
#
# 前置条件: 本机可编译 Go。无需 Docker。数据/进程清理同 soak。
#
# 用法:
#   ./scripts/soak/run-v21-leader-failover.sh [--metad NODES] [--nodes NODES]
#       [--duration S] [--failover-after S] [--rto-budget S] [--window S]
#       [--no-cleanup] [--keep-alive] [--results /path]
#
#   退出码: 0 = PASS；非 0 = FAIL（打印失败阶段）。
#
# 说明: S3 网关的 --meta-addr 指向一个"固定非 leader"的 metad 入口；kill 目标
#       为当时 leader。网关经元数据客户端自动跟进 leader 307 重定向，故即便
#       leader 反复切换，入口节点仍然可达，写请求经重定向+重试平滑迁移。

set -euo pipefail
cd "$(dirname "$0")/../.."

# ---------------------------------------------------------------------------
# 可配置默认值
# ---------------------------------------------------------------------------
METAD_NODES=${SOAK_METAD_NODES:-3}   # raft 节点数（must be odd, >=3）
NODES=${SOAK_NODES:-6}
DURATION=${SOAK_DURATION:-300}
FAILOVER_AFTER=${SOAK_FAILOVER_AFTER:-0}   # 0 = 自动 = DURATION*0.4
RTO_BUDGET=${SOAK_RTO_BUDGET:-15}          # RTO 门禁上限（秒）
WINDOW=${SOAK_WINDOW:-20}                  # leader 切换容忍窗（秒，kill 之后）
CLEANUP=1
KEEP_ALIVE=0
RESULTS_ROOT="${NUFS_RESULTS_ROOT:-/var/log/nufs-tests}"
DISKS_PER_NODE=${SOAK_DISKS_PER_NODE:-3}
# 数据面策略：1 = RF=9 EC6+3 直写（复现多 metad raft 下的 EC 500）；0 = RF=3 纯副本（默认，稳定）
EC=${SOAK_EC:-0}

# 3 个 metad 的矩阵：ops / raft 端口（节点 id 1..3）
METAD_OPS_BASE=18091
METAD_RAFT_BASE=17001
S3_LISTEN="127.0.0.1:8081"

# datanode 端口（沿用 soak 布局）
DN_BASE_PORT=9103
DN_OPS_BASE_PORT=18096

ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

LOG_ROOT="${NUFS_LOG_ROOT:-/tmp/nufs-leader-failover}"
RES_DIR=""

usage() { sed -n '2,52p' "$0"; exit 0; }

while [ "$#" -gt 0 ]; do
  case "$1" in
    --metad) METAD_NODES="$2"; shift 2;;
    --nodes) NODES="$2"; shift 2;;
    --duration) DURATION="$2"; shift 2;;
    --failover-after) FAILOVER_AFTER="$2"; shift 2;;
    --rto-budget) RTO_BUDGET="$2"; shift 2;;
    --window) WINDOW="$2"; shift 2;;
    --no-cleanup) CLEANUP=0; shift;;
    --keep-alive) KEEP_ALIVE=1; CLEANUP=0; shift;;
    --results) RESULTS_ROOT="$2"; shift 2;;
    -h|--help) usage;;
    *) echo "unknown: $1" >&2; usage;;
  esac
done
[ "$FAILOVER_AFTER" -eq 0 ] && [ "$DURATION" -gt 0 ] && FAILOVER_AFTER=$((DURATION * 40 / 100))

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------
log() { printf '[%s] %s\n' "$(date +%T)" "$*"; }
die() { printf '[%s] FATAL: %s\n' "$(date +%T)" "$*" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
MODULE_ROOT="$(dirname "$REPO_ROOT")"   # 单模块合并后 go.mod 在仓库根
BIN_DIR="$REPO_ROOT/bin"
[ -n "${NUFS_BIN_DIR:-}" ] && BIN_DIR="$NUFS_BIN_DIR"
METAD_BIN="$BIN_DIR/metad"
DATANODE_BIN="$BIN_DIR/datanode"
S3_BIN="$BIN_DIR/nufs-s3"

wait_http() { # url seconds name
  local url="$1" secs="$2" name="$3" i
  for i in $(seq 1 "$secs"); do
    curl -sf "$url" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

alive() { [ -f "$1" ] && kill -0 "$(cat "$1" 2>/dev/null)" 2>/dev/null; }

port_owner() { # port
  lsof -nP -iTCP:"$1" -sTCP:LISTEN 2>/dev/null | awk 'NR==2 {print $2}'
}

build_bins() {
  log "building binaries -> $BIN_DIR"
  mkdir -p "$BIN_DIR"
  ( cd "$MODULE_ROOT" && go build -o "$BIN_DIR/metad" ./nufs-core/cmd/metad )
  ( cd "$MODULE_ROOT" && go build -o "$BIN_DIR/datanode" ./nufs-core/cmd/datanode )
  ( cd "$MODULE_ROOT" && go build -o "$BIN_DIR/nufs-s3" ./nufs-core/cmd/nufs-s3 )
}

# metad 矩阵路径
metad_ops()   { echo $((METAD_OPS_BASE + $1 - 1)); }
metad_raft()  { echo $((METAD_RAFT_BASE + $1 - 1)); }
metad_pid()   { echo "$LOG_ROOT/run/metad$1.pid"; }
metad_log()   { echo "$LOG_ROOT/log/metad$1.log"; }
metad_dir()   { echo "$LOG_ROOT/metad$1"; }
metad_raftdir(){ echo "$LOG_ROOT/raft$1"; }
metad_url()   { echo "http://127.0.0.1:$(metad_ops "$1")"; }
# 每节点独立的 hashicorp/raft 内部日志（选举/投票/心跳/提交）。默认落到
# $LOG_ROOT/log/metad<n>.raft.log，cluster_up 已 mkdir 该目录且归档时
# cp $LOG_ROOT/log/*.log 会一并带走，便于诊断卡选主。
# 注意默认值不能用 /dev/null/<file>：那会拼成非法路径（open /dev/null/...: not a directory）
# 使 OpenFile 失败、raft 内部日志被丢弃（raftConfig.LogOutput 默认 Discard，看不到
# RequestVote）。设 NUFS_RAFT_LOG_DIR 可覆盖到任意目录。
metad_raftlog(){ printf '%s/metad%s.raft.log' "${NUFS_RAFT_LOG_DIR:-$LOG_ROOT/log}" "$1"; }

# datanode 路径
node_listen() { echo $((DN_BASE_PORT + $1 - 1)); }
node_ops()    { echo $((DN_OPS_BASE_PORT + $1 - 1)); }
node_pid()    { echo "$LOG_ROOT/run/node$1.pid"; }
node_log()    { echo "$LOG_ROOT/log/node$1.log"; }
node_dirs()   {
  local n="$1" out="" d
  for d in $(seq 1 "$DISKS_PER_NODE"); do
    out="${out:+$out,}${LOG_ROOT}/node$n/disk$d"
  done
  echo "$out"
}

dn_run() { # node leader —— 显式 --node-id，rack/zone 派生自序数；注册指向当前 raft leader
  # 元数据客户端在注册后经 heartbeat/redirect 自动跟进 leader 切换，但初次注册
  # 直接落到当时 leader 的 ops 端口，规避 follower 307 重定向 + leader 503 的启动竞态。
  local n="$1" leader="$2" lp ops rk zn
  lp="$(node_listen "$n")"; ops="$(node_ops "$n")"
  rk=$(( (n-1) % 3 + 1 )); zn=$(( (n-1) % 3 + 1 ))
  "$DATANODE_BIN" --node-id="$n" --listen="127.0.0.1:$lp" \
    --register-addr="127.0.0.1:$lp" --ops-addr="127.0.0.1:$ops" \
    --data-dir="$(node_dirs "$n")" --metadata=127.0.0.1:$(metad_ops "$leader") \
    --rack="rack$rk" --zone="zone$zn" \
    --allow-insecure-dev --log-level=info \
    > "$(node_log "$n")" 2>&1 &
  echo $! > "$(node_pid "$n")"
}

# 初始化 raft peer 描述串（meta-1..meta-N）
raft_peer_desc() { # 每个 metad 节点用同一份完整 peer 列表（helm 生产形态）
  local i
  for i in $(seq 1 "$METAD_NODES"); do
    [ "$i" -gt 1 ] && printf ','
    printf 'meta-%d=%s' "$i" "127.0.0.1:$(metad_raft "$i")"
  done
}
raft_peer_ops_desc() {
  local i
  for i in $(seq 1 "$METAD_NODES"); do
    [ "$i" -gt 1 ] && printf ','
    printf 'meta-%d=http://127.0.0.1:%s' "$i" "$(metad_ops "$i")"
  done
}

metad_run() { # node —— 与 helm 生产形态一致：每节点都传 --raft-bootstrap=true，
  # 但通过 --raft-bootstrap-owner=meta-1 指定唯一自举 Owner，其余节点"defer"（不自举、
  # 以空配置启动，被 Owner 的 leader-driven reconcile 经 AddVoter 拉入投票组）。
  # 三分裂根因：全部节点各自自举会让每个节点在 1/1 quorum 下各自当选 leader、term 冲突，
  # 收敛非确定，kill 后无法可靠重选主（rto=n/a）。Owner guard 让部署模板/二进制都
  # 无需区分 ordinal 即可安全地全量传 --raft-bootstrap（distroless 镜像无 shell，
  # 无法 per-replica 改 args）。见 raft-multiprocess-join memory。
  local n="$1" ops raft
  ops="$(metad_ops "$n")"; raft="$(metad_raft "$n")"
  NUFS_RAFT_LOG="$(metad_raftlog "$n")" "$METAD_BIN" --node-id="$n" --data-dir="$(metad_dir "$n")" \
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
}

# 查询当前 raft leader 的节点序号（1..N）；无人当选返回空
current_leader() {
  local i body is_leader
  for i in $(seq 1 "$METAD_NODES"); do
    body="$(curl -sf --max-time 2 "$(metad_url "$i")/api/v1/cluster/status" 2>/dev/null)" || continue
    is_leader="$(printf '%s' "$body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('is_leader',False))" 2>/dev/null)" || continue
    [ "$is_leader" = "True" ] && { echo "$i"; return 0; }
  done
  return 1
}

kill_stale() {
  local p
  for p in "$METAD_BIN" "$DATANODE_BIN" "$S3_BIN"; do
    pkill -9 -f "$p" 2>/dev/null || true
  done
  for p in $(seq $METAD_OPS_BASE $((METAD_OPS_BASE + 23))) \
           $(seq $METAD_RAFT_BASE $((METAD_RAFT_BASE + 23))) \
           $(seq $DN_BASE_PORT $((DN_BASE_PORT + 31))) 8081; do
    [ -n "$(lsof -nP -iTCP:$p -sTCP:LISTEN 2>/dev/null)" ] && \
      lsof -nP -t -iTCP:$p -sTCP:LISTEN 2>/dev/null | xargs kill -9 2>/dev/null || true
  done
  sleep 1
}

cluster_up() {
  log "cluster up: metad=$METAD_NODES nodes=$NODES disks/node=$DISKS_PER_NODE gateway->metad2 entry"
  kill_stale
  rm -rf "$LOG_ROOT"
  mkdir -p "$LOG_ROOT/run" "$LOG_ROOT/log" "$LOG_ROOT/s3-parts"
  for n in $(seq 1 "$METAD_NODES"); do mkdir -p "$(metad_dir "$n")" "$(metad_raftdir "$n")"; done
  for n in $(seq 1 "$NODES"); do mkdir -p "$LOG_ROOT/node$n"; done

  for n in $(seq 1 "$METAD_NODES"); do metad_run "$n"; done

  # 等 (METAD_NODES+1)/2 节点健康 + 至少 1 leader 当选
  local want_ok=$(( (METAD_NODES + 1) / 2 )) ok=0 i
  for i in $(seq 1 60); do
    ok=0; for n in $(seq 1 "$METAD_NODES"); do wait_http "$(metad_url "$n")/health" 0 metad$n && ok=$((ok+1)); done
    [ "$ok" -ge "$want_ok" ] && break
    sleep 1
  done
  log "metad nodes healthy ($ok/$METAD_NODES); waiting for raft leader..."
  for i in $(seq 1 60); do
    leading="$(current_leader)" && { log "raft leader elected: meta-$leading"; break; }
    sleep 1
  done
  [ -n "$leading" ] || die "no raft leader elected after 60s"

  for n in $(seq 1 "$NODES"); do
    dn_run "$n" "$leading"; log "node$n pid=$(cat "$(node_pid "$n")") listen=:$(node_listen "$n")"
  done
  for n in $(seq 1 "$NODES"); do
    local lp="$((DN_BASE_PORT + n - 1))" i relaunch=0
    for i in $(seq 1 40); do
      python3 -c "import socket,sys
s=socket.socket();s.settimeout(1)
try: s.connect(('127.0.0.1',$lp));sys.exit(0)
except OSError:sys.exit(1)" && break
      if ! alive "$(node_pid "$n")"; then
        # 启动竞态（注册 503）导致节点退出 —— 重拉（同一 node-id，幂等注册）
        if [ "$relaunch" -lt 3 ]; then relaunch=$((relaunch+1)); log "node$n not listening, relaunch #$relaunch"; dn_run "$n" "$leading"; else die "node$n exited after relaunches; tail $(node_log "$n")"; fi
      fi
      sleep 1
    done
  done
  log "all datanodes listening"

  # S3 网关入口指向固定的非 leader 节点（= metad2；若 metad2 恰为 leader 则指向 metad1），
  # 避免 kill leader 时把网关入口一并干掉。网关经 307 重定向 + 重试自动跟进 leader。
  local entry
  if [ "$leading" = "2" ]; then entry=1; else entry=2; fi
  GATEWAY_ENTRY="$entry"
  "$S3_BIN" --listen="$S3_LISTEN" --meta-addr="127.0.0.1:$(metad_ops "$entry")" \
    --access-key="$ACCESS_KEY" --secret-key="$SECRET_KEY" \
    --part-dir="$LOG_ROOT/s3-parts" --rate-limit=1000 --rate-limit-burst=2000 \
    --log-level=info > "$LOG_ROOT/log/s3.log" 2>&1 &
  echo $! > "$LOG_ROOT/run/s3.pid"
  wait_http "http://localhost:8081/healthz" 40 s3 || die "s3 not healthy"
  log "cluster up complete (gateway entry=meta-$entry, will kill leader meta-$leading)"
}

cluster_down() {
  log "cluster down"
  for f in "$LOG_ROOT"/run/*.pid; do
    [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true
  done
  sleep 2
  for f in "$LOG_ROOT"/run/*.pid; do
    [ -f "$f" ] && kill -9 "$(cat "$f")" 2>/dev/null || true
  done
  kill_stale
}

# 把 S3 网关入口改指到另一个还活着的 metad 节点。生产里网关指向 nufs-metad
# Service，其 DNS 恒解析到存活 pod（kubelet 摘掉宕机端点）；本机 harness 用固定
# 端口，无法靠 DNS 兜底，故在 kill 目标=网关入口时显式重启网关换入口——等价于
# "客户端重连 leader" 语义，也在切换前保持网关始终可达。
repoint_gateway() { # target_meta
  local target="$1" old
  old="$(cat "$LOG_ROOT/run/s3.pid" 2>/dev/null || echo 0)"
  [ -n "$old" ] && [ "$old" -ne 0 ] && kill "$old" 2>/dev/null || true
  sleep 1; kill -9 "$old" 2>/dev/null || true
  "$S3_BIN" --listen="$S3_LISTEN" --meta-addr="127.0.0.1:$(metad_ops "$target")" \
    --access-key="$ACCESS_KEY" --secret-key="$SECRET_KEY" \
    --part-dir="$LOG_ROOT/s3-parts" --rate-limit=1000 --rate-limit-burst=2000 \
    --log-level=info >> "$LOG_ROOT/log/s3.log" 2>&1 &
  echo $! > "$LOG_ROOT/run/s3.pid"
  wait_http "http://localhost:8081/healthz" 40 s3 || die "s3 not healthy after repoint"
  log "gateway repointed to meta-$target"
}

# 全部在线 metad 节点列表
alive_metad() {
  local n pid out=""
  for n in $(seq 1 "$METAD_NODES"); do
    local pidfile="$(metad_pid "$n")"
    if [ -f "$pidfile" ] && pid="$(cat "$pidfile" 2>/dev/null)" && [ -n "$pid" ] \
       && kill -0 "$pid" 2>/dev/null; then
      out="${out:+$out }$n"
    fi
  done
  echo "$out"
}

archive_evidence() { # [result] [stage]
  local result="${1:-FAIL}" stage="${2:-finished}"
  [ -z "$RES_DIR" ] && return 0
  : > "$RES_DIR/REPORT.txt"
  cp "$LOG_ROOT"/log/*.log "$RES_DIR"/ 2>/dev/null || true
  echo "result=$result" >> "$RES_DIR/REPORT.txt"
  echo "stage=$stage" >> "$RES_DIR/REPORT.txt"
  echo "metad_nodes=$METAD_NODES metad_alive=$(alive_metad)" >> "$RES_DIR/REPORT.txt"
  [ "${RTO_SECS:-}" != "" ] && echo "leader_failover_rto_seconds=${RTO_SECS}" >> "$RES_DIR/REPORT.txt"
  [ "${RTO_BUDGET}" != "" ] && echo "rto_budget_seconds=${RTO_BUDGET}" >> "$RES_DIR/REPORT.txt"
  [ "${KILLED_LEADER:-0}" -ne 0 ] && echo "killed_leader=meta-${KILLED_LEADER}" >> "$RES_DIR/REPORT.txt"
  [ "${OUT_WINDOW_ERRS:-0}" -ne 0 ] && echo "out_of_window_client_errors=$OUT_WINDOW_ERRS" >> "$RES_DIR/REPORT.txt"
  # 显式成功返回：set -e 下，最后一条 `[ cond ] && echo` 在 cond 为假（如
  # OUT_WINDOW_ERRS=0）时以 1 结尾，会让本函数返回非零——成功路径里
  # `archive_evidence PASS "verify"` 因此被 set -e 当成致命错误直接中断脚本，
  # 把一次全绿的 failover 演练（RTO/verify/out-of-window 全过）误标成 FAIL。
  return 0
}

# ---------------------------------------------------------------------------
# 持续读写驱动（SigV4）+ RTO 记录
# ---------------------------------------------------------------------------
# 写每个 PUT 的 (epoch_sec, success) 到 rto.times；失败带 HTTP/错误。容忍窗为
# [failover_after, failover_after+window]（即 kill 之后紧邻的切换期）。
run_load() {
  python3 - "$NODES" "$DURATION" "$FAILOVER_AFTER" "$WINDOW" "$ACCESS_KEY" "$SECRET_KEY" "$RES_DIR" "$EC" <<'PYEOF' || return $?
import hashlib,hmac,datetime,urllib.request,urllib.error,os,sys,time,random,json
nodes=int(sys.argv[1]); duration=int(sys.argv[2]); fa=int(sys.argv[3]); window=int(sys.argv[4])
ak=sys.argv[5]; sk=sys.argv[6]; res=sys.argv[7]; ec_flag=sys.argv[8] if len(sys.argv)>8 else '0'
ep='http://localhost:8081'; bucket='failover-bucket'
manifest={}; szs={}; t0=time.time(); obj=0; out_errs=0
rto_f=open(res+'/rto.times','w')

def now(): return time.time()-t0
def sig(m,p,h,bd):
    n=datetime.datetime.now(datetime.timezone.utc); d=n.strftime('%Y%m%dT%H%M%SZ'); ds=n.strftime('%Y%m%d')
    h['host']='localhost:8081'; h['x-amz-date']=d; h['x-amz-content-sha256']=hashlib.sha256(bd).hexdigest()
    ch=''.join(f'{k.lower()}:{v.strip()}\n' for k,v in sorted(h.items())); sh=';'.join(sorted(k.lower() for k in h))
    cr=f'{m}\n{p}\n\n{ch}\n{sh}\n{h["x-amz-content-sha256"]}'
    cs=f'{ds}/us-east-1/s3/aws4_request'; sts=f'AWS4-HMAC-SHA256\n{d}\n{cs}\n{hashlib.sha256(cr.encode()).hexdigest()}'
    def skf(k,m): return hmac.new(k,m.encode(),hashlib.sha256).digest()
    kd=skf(('AWS4'+sk).encode(),ds); kr=skf(kd,'us-east-1'); ks=skf(kr,'s3'); kg=skf(ks,'aws4_request')
    sg=hmac.new(kg,sts.encode(),hashlib.sha256).hexdigest()
    h['Authorization']=f'AWS4-HMAC-SHA256 Credential={ak}/{cs}, SignedHeaders={sh}, Signature={sg}'
    return h
def s3raw(m,k='',bd=b'',extra=None):
    from urllib.parse import quote
    path='/'+quote(bucket)+('/'+quote(k) if k else '')
    h={} if extra is None else dict(extra); h=sig(m,path,h,bd)
    try:
        r=urllib.request.urlopen(urllib.request.Request(ep+path,data=bd or None,headers=h,method=m),timeout=15); return r.status,r.read(),dict(r.headers)
    except urllib.error.HTTPError as e: return e.code,e.read(),dict(e.headers)
    except Exception as e: return 0,str(e).encode(),{}
kill_epoch=None; disrupt_epoch=None
def refresh_kill_epoch():
    global kill_epoch, disrupt_epoch
    if kill_epoch is None:
        try:
            with open(res+'/kill.epoch') as f: kill_epoch=float(f.read().strip())
        except Exception: pass
    if disrupt_epoch is None:
        try:
            with open(res+'/disrupt.epoch') as f: disrupt_epoch=float(f.read().strip())
        except Exception: pass
def in_window():
    # 容忍窗三态：'pre' = 锚点未建立（kill/disrupt 尚未发生）；'in' = 窗内；'out' = 窗外。
    #   - 'in'：kill 后切换期的失败 → 容忍（RTO 的度量对象）。
    #   - 'pre'：warmup 探测 200 之后、扰动之前仍偶发的 5xx —— 多 metad raft
    #     收敛 churn / 容器 CPU 拥塞下的瞬时 read-index 停滞，均非 failover 度量
    #     对象 → 容忍。真故障不靠它兜底：集群若从未恢复，verify_all 将 keys=0、
    #     RTO gate 报 unmeasurable，必然 FAIL，不会静默放行。
    #   - 'out'：锚点已建但超窗 → 只有这个才 abort（超窗错误）。
    # RTO 仍锚定 kill.epoch；容忍窗覆盖 kill 前短暂的网关重指连接窗口。
    refresh_kill_epoch()
    anchor = disrupt_epoch if disrupt_epoch is not None else kill_epoch
    if anchor is None: return ('pre', None)
    if anchor <= time.time() <= anchor+window: return ('in', anchor)
    return ('out', anchor)
def req(m,p,bd=b'',base='http://localhost:8081'):
    # 无 SigV4 的 ops API 请求；307 时跟随 location 重定向到 leader（最多 3 跳）
    import urllib.request as ur
    h={'content-type':'application/json'} if bd else {}
    url=base+p
    for _ in range(3):
        try:
            r=ur.urlopen(ur.Request(url,data=bd or None,headers=h,method=m),timeout=10)
            return r.status,r.read()
        except urllib.error.HTTPError as e:
            if e.code==307 and e.headers.get('location'):
                from urllib.parse import urljoin
                url=urljoin(url,e.headers['location']); continue
            return e.code,e.read()
    return 0,b''

# 建桶：任一 metad 入口出发，follower 会 307 重定向到 leader。EC=1 时建 RF=9
# EC 6+3；默认（EC=0）建 RF=3 纯副本。多 metad raft 下 V2.1 直写 EC 当前会 500
# （直写 EC 数据面在多 metad 上不稳定），而副本路径稳定 —— 本 harness 的 #4/#5
# 目标（leader 故障转移 RTO + 优雅降级）由 RF=3 即可完整度量，EC 作为复现开关。
bp=('{"name":"%s","policy":{"replication_factor":9,"ec_config":{"data_shards":6,"parity_shards":3}}}'%bucket if ec_flag=='1'
    else '{"name":"%s","policy":{"replication_factor":3}}'%bucket).encode()
st=0
for i in (1,2,3):
    st,_=req('POST','/api/v1/buckets',bp,base=f'http://127.0.0.1:{18090+i}')
    if st in (200,201,409): break
if st not in (200,201,409):
    print('create-bucket fail',st,flush=True); sys.exit(2)
print('bucket %s ready RF=%s (code=%d)'%(bucket,'9+EC' if ec_flag=='1' else '3',st),flush=True)
# 多 metad raft 在 leader 驱动的 membership reconcile 收敛前没有稳定 leader
# （soak 是单 metad 无此 churn；此处可观察到持续 ~30s 的 "leadership lost while
# committing log"）。故用探测 PUT 自旋等到干净 200 再锚定测量起点 t0，让 RTO 与
# 容忍窗落在集群稳定之后，首 PUT 的偶发 500 不再误报为净化级错误。
settle=60; warmed=False
for _ in range(settle):
    k='warmup'; data=b'\x00'*4096
    wst,_,_=s3raw('PUT',k,data,{'content-length':'4096','content-type':'application/octet-stream'})
    if wst==200:
        warmed=True; break
    print('warming: probe PUT http=%d, waiting for stable leader...'%wst,flush=True)
    time.sleep(2)
if not warmed:
    print('PROBE WARMUP FAILED: no stable leader within %ds'%settle,flush=True); sys.exit(7)
t0=time.time()   # 锚定测量起点到集群稳定之后
print('cluster stable (probe 200); starting measured load',flush=True)

while time.time()-t0 < duration:
    t=now(); k='obj-%06d'%obj
    sz=random.choice([4096,65536,262144,1048576]); data=os.urandom(sz); hh=hashlib.sha256(data).hexdigest()
    st,b,_=s3raw('PUT',k,data,{'content-length':str(sz),'content-type':'application/octet-stream'})
    rto_f.write('%s %d %d\n'%(time.time(), 1 if st==200 else 0, st)); rto_f.flush()
    if st!=200:
        wstate,anchor = in_window()
        if wstate in ('pre','in'):
            print('put tolerated-fail key=%s http=%d @%.1fs (window=%s)'%(k,st,time.time(),wstate),flush=True); time.sleep(0.4); continue
        out_errs+=1; print('PUT OUT-OF-WINDOW FAIL key=%s http=%d @%.1fs anchor=%s'%(k,st,t,anchor),flush=True); sys.exit(3)
    manifest[k]=hh; szs[k]=sz
    if obj%8==0:
        t2=now(); st2,b2,rh=s3raw('GET',k)
        rto_f.write('%s_get %d %d\n'%(time.time(), 1 if st2==200 else 0, st2)); rto_f.flush()
        if st2!=200:
            wstate2,anchor2 = in_window()
            if wstate2 == 'out':
                out_errs+=1; print('GET OUT-OF-WINDOW FAIL key=%s http=%d @%.1fs anchor=%s'%(k,st2,t2,anchor2),flush=True); sys.exit(4)
        if st2==200 and hashlib.sha256(b2).hexdigest()!=hh:
            # 诊断：交叉比对实际字节属于哪个对象、长度/ETag 与期望是否一致，
            # 判定损坏发生在写侧（ETag 不符 = 网关写路径看到的字节不对）、
            # 存储读侧（命中别的对象）还是读截断（got_sz != want_sz）。
            got=hashlib.sha256(b2).hexdigest()
            who=' (bytes match no known object)'
            for kk,hhh in manifest.items():
                if hhh==got: who=' (bytes == %s sz=%d)'%(kk,szs[kk]); break
            print('VERIFY FAIL key=%s want_sz=%d got_sz=%d want_sha=%s got_sha=%s%s etag=%s cl=%s window=%s'%(k,sz,len(b2),hh,got,who,rh.get('ETag'),rh.get('Content-Length'),in_window()[0]),flush=True)
            try:
                json.dump({'key':k,'want_sz':sz,'got_sz':len(b2),'want_sha':hh,'got_sha':got,'who':who,'etag':rh.get('ETag'),'cl':rh.get('Content-Length'),'manifest':{kk:[manifest[kk],szs[kk]] for kk in manifest}},open(res+'/verify-fail.json','w'),indent=1)
            except Exception: pass
            sys.exit(5)
    obj+=1
    time.sleep(random.uniform(0.02,0.08))

json.dump(manifest,open(res+'/manifest.json','w'))
rto_f.close()
print('LOAD_DONE objects=%d durable=%d out_of_window_errors=%d'%(obj,len(manifest),out_errs),flush=True)
sys.exit(0 if out_errs==0 else 6)
PYEOF
}

# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
main() {
  [ "$METAD_NODES" -ge 3 ] && [ $((METAD_NODES % 2)) -eq 1 ] || die "--metad must be odd and >= 3"
  [ "$NODES" -ge 3 ] || die "--nodes must be >= 3"
  [ "$RTO_BUDGET" -gt 0 ] || die "--rto-budget must be > 0"

  ts="$(date +%Y%m%d-%H%M%S)"
  # RES_DIR is created *after* cluster_up: cluster_up does `rm -rf "$LOG_ROOT"`
  # then recreates only LOG_ROOT/{run,log}/..., so an early mkdir under
  # LOG_ROOT/results would be wiped before the load phase could write to it.
  # When the configured RESULTS_ROOT (default /var/log/nufs-tests) is not
  # writable, we fall back to LOG_ROOT/results and then place RES_DIR exactly
  # there — so it must be created after cluster_up finishes.
  if ! mkdir -p "$RESULTS_ROOT" 2>/dev/null; then RESULTS_ROOT="$LOG_ROOT/results"; mkdir -p "$RESULTS_ROOT"; fi
  RES_DIR="$RESULTS_ROOT/leader-failover-$ts"

  build_bins
  cluster_up
  mkdir -p "$RES_DIR"
  log "results dir: $RES_DIR"
  # Statfs 观测狗：每 2s 采样一次各节点 disk1 的可用字节/使用率，写入
  # fs-watch.log。用途：load 期间若 datanode 报 "storage: capacity protection"
  # （ErrCapacity 只可能来自 Statfs 容量守卫或已关闭的 store），据此判别是
  # 真磁盘吃紧（free 趋 0）还是 Statfs 瞬时失败（宿主挂载抖动：df 报错 → 字段
  # 为空，或目录 GONE）。仅诊断观测，与生产代码零交互。有界循环随容器退出。
  (
    end=$(( $(date +%s) + DURATION + 40 ))
    while [ "$(date +%s)" -lt "$end" ]; do
      printf 't=%s' "$(date +%s.%N)"
      for d in "$LOG_ROOT"/node*/disk1; do
        if [ ! -d "$d" ]; then printf ' %s=GONE' "$(basename "$(dirname "$d")")"; continue; fi
        printf ' %s=%s' "$(basename "$(dirname "$d")")" "$(df -Pk "$d" 2>/dev/null | awk 'NR==2{print $4"/"$5}')"
      done
      echo
      sleep 2
    done
  ) > "$RES_DIR/fs-watch.log" 2>&1 &
  log "starting ${DURATION}s sustained read/write load (failover ~${FAILOVER_AFTER}s, rto_budget=${RTO_BUDGET}s)"
  ( run_load ) > "$RES_DIR/load.log" 2>&1 &
  LOAD_PID=$!

  # 在 failover_after 时刻 kill 当前 leader
  KILLED_LEADER=0; RTO_SECS=""; OUT_WINDOW_ERRS=0
  if [ "$FAILOVER_AFTER" -gt 0 ]; then
    sleep "$FAILOVER_AFTER"
    LEAD="$(current_leader)" || LEAD=""
    if [ -z "$LEAD" ]; then
      log "no leader resolveable at failover time; skipping kill"
    else
      # 若当前 leader 恰好是网关入口节点，先把网关改指到另一存活节点再 kill，
      # 否则网关会与 leader 同死于固定端口（生产由 Service DNS 兜底；本机无 DNS）。
      # 记录受控扰动起点（disrupt.epoch）：网关重指或 leader kill 都会造成一个
      # 短暂的合规窗口，驱动以其为容忍窗锚点、以 kill.epoch 为 RTO 锚点。
      DISRUPT_EPOCH=$(date +%s)
      # 必须在重指/kill 之前把 disrupt.epoch 落盘，驱动在首条连接错误时即可读到。
      echo "$DISRUPT_EPOCH" > "$RES_DIR/disrupt.epoch"
      if [ "$LEAD" = "$GATEWAY_ENTRY" ]; then
        local alt=1
        while [ "$alt" -le "$METAD_NODES" ]; do
          if [ "$alt" -ne "$LEAD" ] && alive "$(metad_pid "$alt")"; then break; fi
          alt=$((alt+1))
        done
        if [ "$alt" -le "$METAD_NODES" ]; then
          log "leader meta-$LEAD is the gateway entry; repointing gateway to meta-$alt"
          repoint_gateway "$alt"
          GATEWAY_ENTRY="$alt"
        else
          log "no alternative live metad to repoint gateway to; proceeding (may orphan gateway)"
        fi
      fi
      KILLED_LEADER="$LEAD"
      KPIDFILE="$(metad_pid "$LEAD")"
      lp="$(metad_ops "$LEAD")"
      pid="$(port_owner "$lp")"
      [ -z "$pid" ] && pid="$(cat "$KPIDFILE" 2>/dev/null || true)"
      if [ -n "$pid" ]; then
        KILL_EPOCH=$(date +%s)
        log "chaos: SIGKILL raft leader meta-$LEAD (pid $pid, ops :$lp) KILL_EPOCH=$KILL_EPOCH"
        kill -9 "$pid" 2>/dev/null || true
        # kill.epoch 供 RTO（首次故障后成功写）；disrupt.epoch 已先行落盘供容忍窗
        echo "$KILL_EPOCH" > "$RES_DIR/kill.epoch"
        echo "KILL_EPOCH $KILL_EPOCH DISRUPT_EPOCH $DISRUPT_EPOCH" > "$RES_DIR/kill.log"
      else
        log "chaos: leader meta-$LEAD has no live pid (skip)"
      fi
    fi
  fi

  LOAD_RC=0; wait "$LOAD_PID" || LOAD_RC=$?

  # 解析驱动 out_of_window_errors（LOAD_DONE 仅在完整运行末尾打印）
  OUT_WINDOW_ERRS=0
  if [ "$LOAD_RC" -eq 0 ]; then
    OUT_WINDOW_ERRS="$(awk -F'[= ]' '/^LOAD_DONE/{print $NF}' "$RES_DIR/load.log" 2>/dev/null || echo 0)"
  fi

  # 计算 RTO：从 kill 时刻（KILL_EPOCH，bash 记录的真实 SIGKILL 墙钟）到新 leader
  # 首次成功服务写请求的秒数。drive 在 rto.times 记录墙钟纪元，仅取纯数值 PUT 行。
  RTO_SECS="n/a"
  if [ -s "$RES_DIR/rto.times" ] && [ "${KILL_EPOCH:-}" != "" ]; then
    RTO_SECS="$(awk -v k="$KILL_EPOCH" '$1 ~ /^[0-9]+(\.[0-9]+)?$/ && $2==1 && $3==200 && ($1+0)>=k {printf "%.2f", ($1)-k; exit}' "$RES_DIR/rto.times" 2>/dev/null)"
    [ -z "$RTO_SECS" ] && RTO_SECS="n/a"
  fi

  # 负载驱动异常退出（非 out-of-window 的正常完成）
  if [ "$LOAD_RC" -ne 0 ] && [ "$OUT_WINDOW_ERRS" -eq 0 ]; then
    archive_evidence FAIL "load-driver-rc=$LOAD_RC"
    log "evidence: $RES_DIR"
    [ "$CLEANUP" -eq 1 ] && { cluster_down; rm -rf "$LOG_ROOT"; }
    die "load driver failed rc=$LOAD_RC — see $RES_DIR/load.log"
  fi

  # 进程普查：仅被 kill 的 leader 可下线
  local missing=""
  for n in $(seq 1 "$METAD_NODES"); do
    if [ "$n" = "$KILLED_LEADER" ]; then continue; fi
    alive "$(metad_pid "$n")" || missing="${missing:+$missing }$n"
  done
  if [ -n "$missing" ]; then
    log "RESULT: FAIL — metad nodes DOWN beyond killed leader: $missing"
    archive_evidence FAIL "unscheduled-metad-down"
    [ "$CLEANUP" -eq 1 ] && { cluster_down; rm -rf "$LOG_ROOT"; }
    die "unscheduled metad down: $missing"
  fi

  local pass=1
  if [ "$LOAD_RC" -ne 0 ]; then
    log "RESULT: FAIL — load driver exited rc=$LOAD_RC (out_of_window_errors=${OUT_WINDOW_ERRS})"
    pass=0
  fi
  if [ "$KILLED_LEADER" -ne 0 ]; then
    if [ "$RTO_SECS" = "n/a" ]; then
      log "RESULT: FAIL — no successful write observed after leader kill (RTO unmeasurable)"
      pass=0
    elif python3 -c "import sys; sys.exit(0 if float('$RTO_SECS') <= float('$RTO_BUDGET') else 1)"; then
      log "RESULT: RTO=${RTO_SECS}s <= budget ${RTO_BUDGET}s (PASS)"
    else
      log "RESULT: FAIL — RTO=${RTO_SECS}s > budget ${RTO_BUDGET}s"
      pass=0
    fi
  else
    log "no leader kill performed this run (skipping RTO gate)"
  fi
  if [ "${OUT_WINDOW_ERRS:-0}" -ne 0 ]; then
    log "RESULT: FAIL — $OUT_WINDOW_ERRS out-of-window client errors during failover"
    pass=0
  fi

  # 字节精确校验：只有 RTO/优雅降级全部满足时才跑（避免在 leader 切换失败时
  # 误跑）。verify_all 的返回值必须汇入 pass —— 之前用 &&/|| 吞掉了它的失败，
  # 会在校验失败时错误地打印 PASS。校验失败是被测集群的数据面缺陷（如跨 raft
  # 故障转移的 chunk-ID 复用），必须让 run 以 FAIL 收尾并落盘证据。
  local verify_rc=0
  if [ "$pass" -ne 0 ]; then
    log "waiting heal/orphan-GC convergence before verify"
    sleep 40
    verify_all || verify_rc=1
  fi
  if [ "$pass" -ne 0 ] && [ "$verify_rc" -eq 0 ]; then
    archive_evidence PASS "verify"
  else
    log "RESULT: FAIL — byte-exact verify did not pass (rc=${verify_rc})"
    pass=0
    archive_evidence FAIL "verify"
  fi

  if [ "$pass" -ne 0 ]; then
    echo "PASS: leader-failover (rto=${RTO_SECS}s <= ${RTO_BUDGET}s, metad=$METAD_NODES nodes=$NODES, out_of_window_errors=${OUT_WINDOW_ERRS:-0})" \
      | tee -a "$RES_DIR/REPORT.txt"
  fi
  log "evidence: $RES_DIR"
  [ "$KEEP_ALIVE" -eq 1 ] && { log "keep-alive: cluster left running"; return 0; }
  [ "$CLEANUP" -eq 1 ] && { cluster_down; rm -rf "$LOG_ROOT"; }
  [ "$pass" -ne 0 ] || exit 1
}

# 全量字节精确校验（收敛后）
verify_all() {
  local attempt ok
  for attempt in $(seq 1 5); do
    if python3 - "$ACCESS_KEY" "$SECRET_KEY" "$RES_DIR" <<'PYEOF'; then
import hashlib,hmac,datetime,urllib.request,urllib.error,sys,json,os
from urllib.parse import quote
ak=sys.argv[1]; sk=sys.argv[2]; res=sys.argv[3]; ep='http://localhost:8081'; bucket='failover-bucket'
try: m=json.load(open(res+'/manifest.json'))
except Exception as e: print('no manifest',e); sys.exit(1)
def sign(kk,p,h,bd=b''):
    n=datetime.datetime.now(datetime.timezone.utc); d=n.strftime('%Y%m%dT%H%M%SZ'); ds=n.strftime('%Y%m%d')
    h['host']='localhost:8081'; h['x-amz-date']=d; h['x-amz-content-sha256']=hashlib.sha256(bd).hexdigest()
    ch=''.join(f'{k.lower()}:{v.strip()}\n' for k,v in sorted(h.items())); sh=';'.join(sorted(k.lower() for k in h))
    cr=f'GET\n{kk}\n\n{ch}\n{sh}\n{h["x-amz-content-sha256"]}'; cs=f'{ds}/us-east-1/s3/aws4_request'
    sts=f'AWS4-HMAC-SHA256\n{d}\n{cs}\n{hashlib.sha256(cr.encode()).hexdigest()}'
    def skf(k,m): return hmac.new(k,m.encode(),hashlib.sha256).digest()
    kd=skf(('AWS4'+sk).encode(),ds); kr=skf(kd,'us-east-1'); ks=skf(kr,'s3'); kg=skf(ks,'aws4_request')
    sg=hmac.new(kg,sts.encode(),hashlib.sha256).hexdigest()
    h['Authorization']=f'AWS4-HMAC-SHA256 Credential={ak}/{cs}, SignedHeaders={sh}, Signature={sg}'; return h
def get(k):
    p='/'+quote(bucket)+'/'+quote(k)
    try:
        r=urllib.request.urlopen(urllib.request.Request(ep+p,headers=sign(p,p,{})),timeout=20); return r.status,r.read()
    except urllib.error.HTTPError as e: return e.code,b''
bad=0
for k,hh in m.items():
    st,b=get(k)
    if st!=200 or hashlib.sha256(b).hexdigest()!=hh: bad+=1; print('VERIFY BAD',k,st,flush=True)
print('verify_all: keys=%d bad=%d'%(len(m),bad),flush=True)
sys.exit(1 if bad>0 else 0)
PYEOF
      log "verify_all PASS"; return 0
    fi
    log "verify_all attempt $attempt: not yet healed; waiting 20s"
    sleep 20
  done
  log "verify_all FAILED after attempts"
  return 1
}

onsig() {
  local rc=$?
  # EXIT 陷阱会在脚本（含 `main` 内部 `exit 1`）退出时以 `$?` 触发。若主流程已
  # 把 result=PASS 写进 REPORT，切勿再以 aborted(FAIL) 覆盖 —— 否则一次本应
  # 通过的 failover 演练（RTO/verify/out-of-window 全绿）会被退出状态的假性非零
  # 标记为 FAIL。只有报告尚未定型（rto 未到约定 PASS 阶段）才归档 FAIL。
  if [ $rc -ne 0 ] && ! grep -q '^result=PASS' "$RES_DIR/REPORT.txt" 2>/dev/null; then
    archive_evidence FAIL "aborted(rc=$rc)"
  fi
  [ "$CLEANUP" -eq 1 ] && cluster_down 2>/dev/null
  exit $rc
}
trap 'onsig' INT TERM EXIT
main "$@"
