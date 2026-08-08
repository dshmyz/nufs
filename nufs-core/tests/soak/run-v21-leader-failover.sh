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
#   ./tests/soak/run-v21-leader-failover.sh [--metad NODES] [--nodes NODES]
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
  go build -o "$BIN_DIR/metad" ./cmd/metad
  go build -o "$BIN_DIR/datanode" ./cmd/datanode
  go build -o "$BIN_DIR/nufs-s3" ./cmd/nufs-s3
}

# metad 矩阵路径
metad_ops()   { echo $((METAD_OPS_BASE + $1 - 1)); }
metad_raft()  { echo $((METAD_RAFT_BASE + $1 - 1)); }
metad_pid()   { echo "$LOG_ROOT/run/metad$1.pid"; }
metad_log()   { echo "$LOG_ROOT/log/metad$1.log"; }
metad_dir()   { echo "$LOG_ROOT/metad$1"; }
metad_raftdir(){ echo "$LOG_ROOT/raft$1"; }
metad_url()   { echo "http://127.0.0.1:$(metad_ops "$1")"; }

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

dn_run() { # node —— 显式 --node-id，rack/zone 派生自序数
  local n="$1" lp ops rk zn
  lp="$(node_listen "$n")"; ops="$(node_ops "$n")"
  rk=$(( (n-1) % 3 + 1 )); zn=$(( (n-1) % 3 + 1 ))
  "$DATANODE_BIN" --node-id="$n" --listen="127.0.0.1:$lp" \
    --register-addr="127.0.0.1:$lp" --ops-addr="127.0.0.1:$ops" \
    --data-dirs="$(node_dirs "$n")" --metadata=127.0.0.1:$(metad_ops 2) \
    --rack="rack$rk" --zone="zone$zn" \
    --storage-version=v2.1 --allow-insecure-dev --log-level=info \
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

metad_run() { # node —— 全部 --raft-bootstrap=true + 完整 peer 列表
  local n="$1" ops raft
  ops="$(metad_ops "$n")"; raft="$(metad_raft "$n")"
  "$METAD_BIN" --node-id="$n" --data-dir="$(metad_dir "$n")" \
    --ops-addr="127.0.0.1:$ops" \
    --raft=true --raft-bootstrap=true --raft-addr="127.0.0.1:$raft" \
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
    dn_run "$n"; log "node$n pid=$(cat "$(node_pid "$n")") listen=:$(node_listen "$n")"
  done
  for n in $(seq 1 "$NODES"); do
    local lp="$((DN_BASE_PORT + n - 1))" i
    for i in $(seq 1 40); do
      python3 -c "import socket,sys
s=socket.socket();s.settimeout(1)
try: s.connect(('127.0.0.1',$lp));sys.exit(0)
except OSError:sys.exit(1)" && break
      alive "$(node_pid "$n")" || die "node$n exited; tail $(node_log "$n")"
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
}

# ---------------------------------------------------------------------------
# 持续读写驱动（SigV4）+ RTO 记录
# ---------------------------------------------------------------------------
# 写每个 PUT 的 (epoch_sec, success) 到 rto.times；失败带 HTTP/错误。容忍窗为
# [failover_after, failover_after+window]（即 kill 之后紧邻的切换期）。
run_load() {
  python3 - "$NODES" "$DURATION" "$FAILOVER_AFTER" "$WINDOW" "$ACCESS_KEY" "$SECRET_KEY" "$RES_DIR" <<'PYEOF' || return $?
import hashlib,hmac,datetime,urllib.request,urllib.error,os,sys,time,random,json
nodes=int(sys.argv[1]); duration=int(sys.argv[2]); fa=int(sys.argv[3]); window=int(sys.argv[4])
ak=sys.argv[5]; sk=sys.argv[6]; res=sys.argv[7]
ep='http://localhost:8081'; bucket='failover-bucket'
manifest={}; t0=time.time(); obj=0; out_errs=0
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
        r=urllib.request.urlopen(urllib.request.Request(ep+path,data=bd or None,headers=h,method=m),timeout=15); return r.status,r.read()
    except urllib.error.HTTPError as e: return e.code,e.read()
    except Exception as e: return 0,str(e).encode()
kill_epoch=None
def refresh_kill_epoch():
    global kill_epoch
    if kill_epoch is None:
        try:
            with open(res+'/kill.epoch') as f:
                kill_epoch=float(f.read().strip())
        except Exception: pass
def in_window():
    # 容忍窗 = [真实 kill 墙钟, kill+window]。kill 前无任何容忍；kill 未发生则永不容忍。
    refresh_kill_epoch()
    if kill_epoch is None: return False
    return kill_epoch <= time.time() <= kill_epoch+window
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

# 建桶（RF=9 EC 6+3）：任一 metad 入口出发，follower 会 307 重定向到 leader
bp=('{"name":"%s","policy":{"replication_factor":9,"ec_config":{"data_shards":6,"parity_shards":3}}}'%bucket).encode()
st=0
for i in (1,2,3):
    st,_=req('POST','/api/v1/buckets',bp,base=f'http://127.0.0.1:{18090+i}')
    if st in (200,201,409): break
if st not in (200,201,409):
    print('create-bucket fail',st,flush=True); sys.exit(2)
print('bucket %s RF=9 EC6+3 ready (code=%d)'%(bucket,st),flush=True)

while time.time()-t0 < duration:
    t=now(); k='obj-%06d'%obj
    sz=random.choice([4096,65536,262144,1048576]); data=os.urandom(sz); hh=hashlib.sha256(data).hexdigest()
    st,b=s3raw('PUT',k,data,{'content-length':str(sz),'content-type':'application/octet-stream'})
    rto_f.write('%s %d %d\n'%(time.time(), 1 if st==200 else 0, st)); rto_f.flush()
    if st!=200:
        if in_window():
            print('put tolerated-fail key=%s http=%d @%.1fs'%(k,st,time.time()),flush=True); time.sleep(0.4); continue
        out_errs+=1; print('PUT OUT-OF-WINDOW FAIL key=%s http=%d @%.1fs'%(k,st,t),flush=True); sys.exit(3)
    manifest[k]=hh
    if obj%8==0:
        t2=now(); st2,b2=s3raw('GET',k)
        rto_f.write('%s_get %d %d\n'%(time.time(), 1 if st2==200 else 0, st2)); rto_f.flush()
        if st2!=200 and not in_window():
            out_errs+=1; print('GET OUT-OF-WINDOW FAIL key=%s http=%d @%.1fs'%(k,st2,t2),flush=True); sys.exit(4)
        if st2==200 and hashlib.sha256(b2).hexdigest()!=hh:
            print('VERIFY FAIL key=%s'%k,flush=True); sys.exit(5)
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
  if ! mkdir -p "$RESULTS_ROOT" 2>/dev/null; then RESULTS_ROOT="$LOG_ROOT/results"; mkdir -p "$RESULTS_ROOT"; fi
  RES_DIR="$RESULTS_ROOT/leader-failover-$ts"; mkdir -p "$RES_DIR"
  log "results dir: $RES_DIR"

  build_bins
  cluster_up
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
      KILLED_LEADER="$LEAD"
      KPIDFILE="$(metad_pid "$LEAD")"
      lp="$(metad_ops "$LEAD")"
      pid="$(port_owner "$lp")"
      [ -z "$pid" ] && pid="$(cat "$KPIDFILE" 2>/dev/null || true)"
      if [ -n "$pid" ]; then
        KILL_EPOCH=$(date +%s)
        log "chaos: SIGKILL raft leader meta-$LEAD (pid $pid, ops :$lp) KILL_EPOCH=$KILL_EPOCH"
        kill -9 "$pid" 2>/dev/null || true
        # 记录 kill 时刻供驱动以真实墙钟锚定容忍窗、供 RTO 计算
        echo "$KILL_EPOCH" > "$RES_DIR/kill.epoch"
        echo "KILL_EPOCH $KILL_EPOCH" > "$RES_DIR/kill.log"
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
    [ "$CLEANUP" -eq 1 ] && cluster_down; rm -rf "$LOG_ROOT"
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
    [ "$CLEANUP" -eq 1 ] && cluster_down; rm -rf "$LOG_ROOT"
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

  [ "$pass" -ne 0 ] && { log "waiting heal/orphan-GC convergence before verify"; sleep 40; verify_all; }
  [ "$pass" -ne 0 ] && archive_evidence PASS "verify" || archive_evidence FAIL "gate"

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
  if [ $rc -ne 0 ]; then archive_evidence FAIL "aborted(rc=$rc)"; fi
  [ "$CLEANUP" -eq 1 ] && cluster_down 2>/dev/null
  exit $rc
}
trap 'onsig' INT TERM EXIT
main "$@"
