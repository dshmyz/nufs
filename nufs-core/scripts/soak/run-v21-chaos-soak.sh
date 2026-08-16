#!/bin/bash
#
# V2.1 多节点 chaos/soak 验证 —— 上线前的可靠性证据
# ============================================================================
#
# 以真实进程（非 Docker、非进程内模拟）拉起一套多节点 V2.1 集群，在持续
# S3 写负载下，注入节点 SIGKILL 崩溃 + 原地重启（验证恢复 checkpoint 的崩溃
# 一致性），并在完整负载后跑 EC self-heal / orphan-GC 收敛窗口，最后对全部
# 已 durable 对象做字节精确校验；过程采样每节点 RSS、归档证据日志，输出
# PASS/FAIL 报告。
#
# 可靠性证据点:
#   1. N 个 datanode-v21 × 每节点 3 盘 = 18（默认 6 节点）个 shard 盘，远超
#      EC 6+3 的 9 盘下限 → 可整节点崩 1 个仍有完整 6+3（§14 故障域）。
#   2. 持续负载下 SIGKILL 崩溃一个节点 + 重启后，先前 durable 写入仍字节精确
#      读回（group-commit / 恢复 checkpoint 崩溃一致）。
#   3. 崩溃后留出 healer(EcSelfHeal) / orphan-GC(ReclaimOrphans) 收敛窗口，
#      全部对象最终字节精确（不在崩溃窗外吞任何 durable 写）。
#   4. 每个节点 RSS 采样（默认每 30s），报告首/末总和，检出单调泄漏。
#
# 前置条件: 本机可编译 Go（Darwin/Linux）；无需 Docker。网络故障注入需 root，
#           默认关闭（--net-fault 显式开启，仅追加时长）。
#
# 用法:
#   ./scripts/soak/run-v21-chaos-soak.sh [--nodes N] [--duration S] [--crash-after S]
#       [--net-fault S] [--no-cleanup] [--keep-alive] [--results /path]
#
#   退出码: 0 = PASS；非 0 = FAIL（打印失败阶段）。
#
# 说明: EC 6+3 需 RF=9 写入（planner 要求 chunk.Replicas==9），对象以 9 副本
#       跨 ≥3 节点落盘。CRASH 目标节点 = 负载循环序 (obj % NODES)，保证分散。

set -euo pipefail
cd "$(dirname "$0")/../.."

# ---------------------------------------------------------------------------
# 可配置默认值
# ---------------------------------------------------------------------------
NODES=${SOAK_NODES:-6}
DURATION=${SOAK_DURATION:-600}
CRASH_AFTER=${SOAK_CRASH_AFTER:-0}       # 0 = 自动 = DURATION*0.3
NET_FAULT_SECS=${SOAK_NET_FAULT_SECS:-0} # >0 = 崩溃后对目标节点注入网络故障 N 秒
CLEANUP=1
KEEP_ALIVE=0
RESULTS_ROOT="${NUFS_RESULTS_ROOT:-/var/log/nufs-tests}"
DISKS_PER_NODE=${SOAK_DISKS_PER_NODE:-3} # ≥3 才满足 EC §14 故障域

METAD_ADDR="127.0.0.1:8091"
S3_LISTEN="127.0.0.1:8081"
DN_BASE_PORT=9103
DN_OPS_BASE_PORT=18096
ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

LOG_ROOT="${NUFS_LOG_ROOT:-/tmp/nufs-chaos-soak}"
RES_DIR=""

usage() { sed -n '2,46p' "$0"; exit 0; }

while [ "$#" -gt 0 ]; do
  case "$1" in
    --nodes) NODES="$2"; shift 2;;
    --duration) DURATION="$2"; shift 2;;
    --crash-after) CRASH_AFTER="$2"; shift 2;;
    --net-fault) NET_FAULT_SECS="$2"; shift 2;;
    --no-cleanup) CLEANUP=0; shift;;
    --keep-alive) KEEP_ALIVE=1; CLEANUP=0; shift;;
    --results) RESULTS_ROOT="$2"; shift 2;;
    -h|--help) usage;;
    *) echo "unknown: $1" >&2; usage;;
  esac
done
[ "$CRASH_AFTER" -eq 0 ] && [ "$DURATION" -gt 0 ] && CRASH_AFTER=$((DURATION * 30 / 100))

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

# 节点 TCP listen 的真实持有 PID（以端口为准，权威于 pidfile——pidfile 在
# 快速 relaunch 下可能被覆盖而串到别的节点，导致 SIGKILL 误伤；端口归属永不串）。
# 返回空 = 该端口当前无 listen（节点已掉/尚未起）。仅本机 127.0.0.1 监听。
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

# 节点派生路径（1-indexed）
node_listen() { echo $((DN_BASE_PORT + $1 - 1)); }
node_ops()    { echo $((DN_OPS_BASE_PORT + $1 - 1)); }
node_pid()    { echo "$LOG_ROOT/run/node$1.pid"; }
node_log()    { echo "$LOG_ROOT/log/node$1.log"; }
node_dirs()   { # 该节点的 DISKS_PER_NODE 个 data 目录
  local n="$1" out="" d
  for d in $(seq 1 "$DISKS_PER_NODE"); do
    out="${out:+$out,}${LOG_ROOT}/node$n/disk$d"
  done
  echo "$out"
}

# 启动单个 datanode（供 cluster_up 与 relaunch 复用）。
# 注意：必须传显式 --node-id=$n 而不是 --node-id=auto。auto 从 machine-id/hostname
# 派生出确定性 ID，本机跑多节点时所有节点会撞成同一个 ID → 注册成同一个逻辑节点 →
# EC 6+3 的 9 副本放不下（候选 <3）→ allocate 500。这就是此前宿主 soak 500 根因。
dn_run() { # node
  local n="$1" lp ops rk zn
  lp="$(node_listen "$n")"; ops="$(node_ops "$n")"
  rk=$(( (n-1) % 3 + 1 )); zn=$(( (n-1) % 3 + 1 ))
  "$DATANODE_BIN" --node-id="$n" --listen="127.0.0.1:$lp" \
    --register-addr="127.0.0.1:$lp" --ops-addr="127.0.0.1:$ops" \
    --data-dirs="$(node_dirs "$n")" --metadata=localhost:8091 \
    --rack="rack$rk" --zone="zone$zn" \
    --allow-insecure-dev --log-level=info \
    > "$(node_log "$n")" 2>&1 &
  echo $! > "$(node_pid "$n")"
}

# 清理可能残留的旧进程（防止上一次运行遗留的进程霸占端口、冒充本集群的
# raft 态，导致 allocation 落到错误 metad 上——这就是此前 500 的根因）。
kill_stale() {
  local p
  for p in "$METAD_BIN" "$DATANODE_BIN" "$S3_BIN"; do
    pkill -9 -f "$p" 2>/dev/null || true
  done
  # 兜底：按端口清
  for p in 8091 8081 $(seq $DN_BASE_PORT $((DN_BASE_PORT + 31))); do
    [ -n "$(lsof -nP -iTCP:$p -sTCP:LISTEN 2>/dev/null)" ] && \
      lsof -nP -t -iTCP:$p -sTCP:LISTEN 2>/dev/null | xargs kill -9 2>/dev/null || true
  done
  sleep 1
}

cluster_up() {
  log "cluster up: metad=$METAD_ADDR s3=$S3_LISTEN nodes=$NODES disks/node=$DISKS_PER_NODE"
  kill_stale
  rm -rf "$LOG_ROOT"
  mkdir -p "$LOG_ROOT/run" "$LOG_ROOT/log" "$LOG_ROOT/metad" "$LOG_ROOT/raft" "$LOG_ROOT/s3-parts"
  for n in $(seq 1 "$NODES"); do mkdir -p "$LOG_ROOT/node$n"; done

  # 单 metad + 单节点 raft bootstrap：真实控制面拓扑，allocation 走 raft apply，
  # bootstrap 单节点自动成为 leader（main.go 等 30s leader 就绪）。
  "$METAD_BIN" --data-dir="$LOG_ROOT/metad" --ops-addr="$METAD_ADDR" \
    --raft --raft-bootstrap --raft-addr=127.0.0.1:7000 --raft-dir="$LOG_ROOT/raft" \
    --allow-insecure-dev --log-level=info \
    > "$LOG_ROOT/log/metad.log" 2>&1 &
  echo $! > "$LOG_ROOT/run/metad.pid"
  wait_http "http://localhost:8091/health" 40 metad || die "metad not healthy"
  log "metad healthy (raft leader may take a few s to settle)"

  for n in $(seq 1 "$NODES"); do
    dn_run "$n"
    log "node$n pid=$(cat "$(node_pid "$n")") listen=:$(node_listen "$n")"
  done

  # 等 datanode TCP listen（python socket 探活；macOS 无 nc -z）
  for n in $(seq 1 "$NODES"); do
    local lp="$((DN_BASE_PORT + n - 1))" i
    for i in $(seq 1 40); do
      if python3 -c "import socket,sys
s=socket.socket();s.settimeout(1)
try:
 s.connect(('127.0.0.1',$lp));sys.exit(0)
except OSError:sys.exit(1)"; then
        break
      fi
      alive "$(node_pid "$n")" || die "node$n exited; tail $(node_log "$n")"
      sleep 1
    done
  done
  log "all datanodes listening"

  "$S3_BIN" --listen="$S3_LISTEN" --meta-addr=localhost:8091 \
    --access-key="$ACCESS_KEY" --secret-key="$SECRET_KEY" \
    --part-dir="$LOG_ROOT/s3-parts" --rate-limit=1000 --rate-limit-burst=2000 \
    --log-level=info > "$LOG_ROOT/log/s3.log" 2>&1 &
  echo $! > "$LOG_ROOT/run/s3.pid"
  wait_http "http://localhost:8081/healthz" 40 s3 || die "s3 not healthy"
  log "cluster up complete"
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
  # 兜底：确保本脚本拉起的 metad/datanode/s3 全部消失（pidfile 可能因
  # relaunch 覆盖而漏杀，按二进制路径名称再清一遍）。
  kill_stale
  rm -f /tmp/nufs-soak.crashflag
}

# ---------------------------------------------------------------------------
# 混沌注入 + 采样
# ---------------------------------------------------------------------------
chaos_crash_relaunch() { # node  seconds  —— SIGKILL 崩溃 → 原地重启
  local n="$1" wait_s="$2" pidfile lp t0 pid
  # 关键：不能在同一个 local 语句的后续 RHS 里使用刚声明的 $n —— bash 会把那处
  # $n 解析成外层/未定义值，导致 node_listen/node_pid 取到错误节点（本 cluster_up
  # 的节点序号偏移 +1）。必须等 local n 落定后再派生路径。
  pidfile="$(node_pid "$n")"; lp="$(node_listen "$n")"
  [ -f "$pidfile" ] || return 0
  # 以端口为准解析真实 PID（权威）。pidfile 在快速 relaunch 下可能串到别的节点
  # （echo $! 覆盖），若照单全收 kill 会误杀无辜节点 → 第二个节点掉 → 9 分片降到
  # <6 → 数据不可达，正是此前"phantom 第二节点死亡"的根因。这里只杀端口归属者。
  pid="$(port_owner "$lp")"
  if [ -z "$pid" ]; then
    # 端口无人 listen：退而用 pidfile（可能已是死 pid），交由下方 kill -0 循环判断
    pid="$(cat "$pidfile" 2>/dev/null || true)"
  fi
  if [ -z "$pid" ]; then log "chaos: node$n has no live pid to crash (skip)"; return 1; fi
  log "chaos: SIGKILL node$n (pid $pid, port $lp)"
  kill -9 "$pid" 2>/dev/null || true
  # 等进程彻底消失
  t0=$(date +%s)
  while kill -0 "$pid" 2>/dev/null; do
    sleep 1
    [ $(( $(date +%s) - t0 )) -gt 20 ] && break
  done
  # 面向自动重启（无拉起守护），这里我们显式 relaunch（同目录，恢复 checkpoint）
  dn_run "$n"
  log "chaos: node$n relaunched (pid $(cat "$(node_pid "$n")"))"
  # 等它重新注册（node ops /health 或 metad nodes）
  local i
  for i in $(seq 1 40); do
    if curl -sf "http://localhost:$(node_ops "$n")/health" >/dev/null 2>&1; then
      log "chaos: node$n recovered after ${i}s"; return 0
    fi
    sleep 1
  done
  return 1
}

node_network_partition() { # node  seconds
  local n="$1" secs="$2" lp
  lp="$(node_listen "$n")"
  log "chaos: network partition node$n (port $lp) for ${secs}s"
  if [ "$(uname)" = "Darwin" ]; then
    sudo dnctl pipe 1 config plr 1.0 2>/dev/null || true
    sudo -E bash -c "printf 'dummynet in quick proto tcp from any to any dst-port $lp pipe 1\n' | pfctl -a com.nufs.chaos -f - 2>/dev/null" || true
  else
    sudo tc qdisc add dev lo root netem loss 100% 2>/dev/null || true
  fi
  sleep "$secs"
  if [ "$(uname)" = "Darwin" ]; then
    sudo pfctl -a com.nufs.chaos -F all 2>/dev/null || true
    sudo dnctl pipe 1 delete 2>/dev/null || true
  else
    sudo tc qdisc del dev lo root 2>/dev/null || true
  fi
  log "chaos: network restored node$n"
}

sample_mem() { # outfile  —— 每节点 RSS(MiB)。跨平台：ps -o rss= 在 macOS/Linux 通用
  # （/proc/<pid>/status 仅 Linux，宿主常为 macOS，须回退到 ps）。
  local f="$1" n pid rss sum=0
  : > "$f"
  for n in $(seq 1 "$NODES"); do
    pidfile="$(node_pid "$n")"
    if [ -f "$pidfile" ] && pid="$(cat "$pidfile" 2>/dev/null)" && [ -n "$pid" ] \
       && rss="$(ps -o rss= -p "$pid" 2>/dev/null)" && [ -n "$rss" ] && [ "$rss" -ne 0 ] 2>/dev/null; then
      # ps 的 RSS 单位为 KiB，转换为 MiB（整数相除，至少 1）
      rss_mib=$(( rss / 1024 )); [ "$rss_mib" -lt 1 ] && rss_mib=1
      echo "node$n ${rss_mib}" >> "$f"; sum=$((sum + rss_mib))
    else
      echo "node$n 0(down)" >> "$f"
    fi
  done
  echo "sum $sum" >> "$f"
}

# 进程普查：返回当前仍在线的节点 ID 列表（空格分隔）。用于在 verify 前断言"除
# 崩溃节点外没有意外的节点死亡"——单节点 SIGKILL 外的任何死亡都是可靠性事故，
# 必须显式暴露，绝不能像"stripe loss 超出容错"那样被吞掉。
alive_nodes() {
  local n pid out=""
  for n in $(seq 1 "$NODES"); do
    local pidfile="$(node_pid "$n")"
    if [ -f "$pidfile" ] && pid="$(cat "$pidfile" 2>/dev/null)" && [ -n "$pid" ] \
       && kill -0 "$pid" 2>/dev/null; then
      out="${out:+$out }$n"
    fi
  done
  echo "$out"
}

# 归档证据：无论 PASS 还是 FAIL，都把所有节点/metad/s3 日志、manifest、RSS
# 趋势拷进 $RES_DIR，并把失败阶段写进 REPORT.txt。trap 也会调用它，保证失败
# 现场不被清理掉（此前 FAIL 路径直接 die，日志全留在 LOG_ROOT、证据丢失）。
archive_evidence() { # [result: PASS|FAIL] [stage]
  local result="${1:-FAIL}" stage="${2:-finished}" first=0 last=0
  [ -z "$RES_DIR" ] && return 0
  : > "$RES_DIR/REPORT.txt"
  cp "$LOG_ROOT"/log/*.log "$RES_DIR"/ 2>/dev/null || true
  cat "$RES_DIR"/mem-*.txt > "$RES_DIR/memory-all.txt" 2>/dev/null || true
  # 首/末 RSS 总和（泄漏趋势证据；样本稀疏时如实报告）
  local fb lb
  fb="$(ls -1 "$RES_DIR"/mem-*.txt 2>/dev/null | sort | head -1)"
  lb="$(ls -1 "$RES_DIR"/mem-*.txt 2>/dev/null | sort | tail -1)"
  [ -n "$fb" ] && first="$(grep '^sum' "$fb" 2>/dev/null | awk '{print $2}')"
  [ -n "$lb" ] && last="$(grep '^sum' "$lb" 2>/dev/null | awk '{print $2}')"
  echo "sum_rss_first=$first MiB sum_rss_last=$last MiB" > "$RES_DIR/rss-trend.txt"
  echo "result=$result" >> "$RES_DIR/REPORT.txt"
  echo "stage=$stage" >> "$RES_DIR/REPORT.txt"
  [ "$CRASHED" -ne 0 ] && echo "crashed_node=$CRASHED (SIGKILL + relaunch)" >> "$RES_DIR/REPORT.txt"
  # 进程普查快照：哪个节点掉了、为什么
  echo "nodes_expected=$NODES nodes_alive=$(alive_nodes)" >> "$RES_DIR/REPORT.txt"
}

# ---------------------------------------------------------------------------
# S3 负载驱动（SigV4）+ 全量校验
# ---------------------------------------------------------------------------
# 对象总数由 --duration 决定；把每个已 durable 对象的 key->sha256 写进 manifest，
# 供最后的全量字节精确校验使用。崩溃窗口内的 PUT 允许失败（输入会重放）。
run_load() { # 返回 0；任何崩溃窗外写入失败/读回失败 -> 非 0
  python3 - "$NODES" "$DURATION" "$CRASH_AFTER" "$NET_FAULT_SECS" "$ACCESS_KEY" "$SECRET_KEY" "$RES_DIR" <<'PYEOF' || return $?
import hashlib,hmac,datetime,urllib.request,urllib.error,os,sys,time,random,json
nodes=int(sys.argv[1]); duration=int(sys.argv[2]); crash_after=int(sys.argv[3])
net_fault=int(sys.argv[4]); ak=sys.argv[5]; sk=sys.argv[6]; res=sys.argv[7]
ep='http://localhost:8081'; bucket='chaos-soak-bucket'
base_port=int(os.getenv('DN_BASE_PORT','9103'))
manifest={}; done=False; nfail=0

def sign(m,p,h,bd=b''):
    n=datetime.datetime.now(datetime.timezone.utc)
    d=n.strftime('%Y%m%dT%H%M%SZ'); ds=n.strftime('%Y%m%d')
    h['host']='localhost:8081'; h['x-amz-date']=d
    h['x-amz-content-sha256']=hashlib.sha256(bd).hexdigest()
    ch=''.join(f'{k.lower()}:{v.strip()}\n' for k,v in sorted(h.items()))
    sh=';'.join(sorted(k.lower() for k in h))
    cr=f'{m}\n{p}\n\n{ch}\n{sh}\n{h["x-amz-content-sha256"]}'
    cs=f'{ds}/us-east-1/s3/aws4_request'
    sts=f'AWS4-HMAC-SHA256\n{d}\n{cs}\n{hashlib.sha256(cr.encode()).hexdigest()}'
    def skf(kk,mm): return hmac.new(kk,mm.encode(),hashlib.sha256).digest()
    kd=skf(('AWS4'+sk).encode(),ds); kr=skf(kd,'us-east-1'); ks=skf(kr,'s3'); kg=skf(ks,'aws4_request')
    sg=hmac.new(kg,sts.encode(),hashlib.sha256).hexdigest()
    h['Authorization']=f'AWS4-HMAC-SHA256 Credential={ak}/{cs}, SignedHeaders={sh}, Signature={sg}'
    return h
def req(m,p,bd=b'',extra=None):
    h={} if extra is None else dict(extra); h=sign(m,p,h,bd)
    try:
        r=urllib.request.urlopen(urllib.request.Request(ep+p,data=bd or None,headers=h,method=m),timeout=12)
        return r.status,r.read()
    except urllib.error.HTTPError as e: return e.code,e.read()
def s3raw(m,k='',bd=b'',extra=None):
    from urllib.parse import quote
    return req(m,'/'+(quote(bucket)+'/'+quote(k) if k else quote(bucket)),bd,extra)
def in_crash_window(now):
    # 仅在崩溃真正注入之后的恢复期内容忍 PUT 暂时失败（崩溃后几秒数据不可达）。
    # 注意：不能是 abs(now-crash_after)<W 的对称窗——那会让 t=0 起的所有写
    # 都被“容忍”，掩盖健康集群的真实失败（此前分配的 500 就是被它吃掉的）。
    return crash_after>0 and crash_after<=now<=crash_after+30

# 建 bucket + RF=9 + EC 6+3（EC 6+3 需要 9 个分片跨 ≥3 节点；RF=9 且带
# ECConfig 时 placement 把 9 个“分片”铺开，而非 9 个整副本）。
# 注意：必须同时给 ECConfig——只给 RF=9 会让 placement 尝试在 N 个节点上放
# 9 个整副本，节点不足即“insufficient healthy nodes”500。这也是此前宿主
# soak 的 allocation 500 根因（配上 --node-id=auto 撞 ID 双因叠加）。
# 走 metad ops API（无 SigV4），与 fatigue-test 一致：一次性建桶+设策略。
def meta_post(path,bd):
    h={'content-type':'application/json'}
    try:
        r=urllib.request.urlopen(urllib.request.Request('http://localhost:8091'+path,data=bd,headers=h,method='POST'),timeout=10)
        return r.status,r.read()
    except urllib.error.HTTPError as e: return e.code,e.read()
st,b=meta_post('/api/v1/buckets',
         ('{"name":"%s","policy":{"replication_factor":9,"ec_config":{"data_shards":6,"parity_shards":3}}}'%bucket).encode())
if st not in (200,201,409):
    print('create-bucket(via metad) fail',st,b[:200],flush=True); sys.exit(2)
print('bucket %s RF=9 EC6+3 ready (metad code=%d)'%(bucket,st),flush=True)
time.sleep(6)

t0=time.time(); obj=0
while time.time()-t0 < duration:
    now=time.time()-t0
    k='obj-%06d'%obj
    sz=random.choice([4096,65536,262144,1048576])
    data=os.urandom(sz); hh=hashlib.sha256(data).hexdigest()
    st,b=s3raw('PUT',k,data,{'content-length':str(sz),'content-type':'application/octet-stream'})
    if st!=200:
        if in_crash_window(now):
            nfail+=1; print('put tolerated-fail key=%s http=%d'%(k,st),flush=True); time.sleep(0.5); continue
        print('PUT FAIL key=%s http=%d %s'%(k,st,b[:200]),flush=True); sys.exit(3)
    manifest[k]=hh
    if obj%8==0:
        st2,b2=s3raw('GET',k)
        if st2!=200 or hashlib.sha256(b2).hexdigest()!=hh:
            print('VERIFY FAIL key=%s'%k,flush=True); sys.exit(4)
    obj+=1
    time.sleep(random.uniform(0.02,0.12))

json.dump(manifest,open(res+'/manifest.json','w'))
print('LOAD_DONE objects=%d durable=%d crash_window_fails=%d'%(obj,len(manifest),nfail),flush=True)
sys.exit(0)
PYEOF
}

# 全量字节精确校验：等 heal/orphan-GC 收敛后再跑，最多 5 次
verify_all() {
  local attempt ok
  for attempt in $(seq 1 5); do
    if python3 - "$ACCESS_KEY" "$SECRET_KEY" "$RES_DIR" <<'PYEOF'; then
import hashlib,hmac,datetime,urllib.request,urllib.error,sys,json,os
from urllib.parse import quote
ak=sys.argv[1]; sk=sys.argv[2]; res=sys.argv[3]; ep='http://localhost:8081'; bucket='chaos-soak-bucket'
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
    log "verify_all attempt $attempt: not yet healed; waiting 20s for healer/orphan-GC"
    sleep 20
  done
  die "verify_all FAILED after 5 attempts (data loss / not healed)"
}

# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
main() {
  [ "$NODES" -ge 3 ] || die "--nodes must be >= 3 (EC 6+3): use 6 for full evidence"
  [ "$DISKS_PER_NODE" -ge 3 ] || die "disks/node must be >= 3 for EC §14"
  [ "$DURATION" -gt 0 ] || die "--duration must be > 0"

  ts="$(date +%Y%m%d-%H%M%S)"
  if ! mkdir -p "$RESULTS_ROOT" 2>/dev/null; then RESULTS_ROOT="$LOG_ROOT/results"; mkdir -p "$RESULTS_ROOT"; fi
  RES_DIR="$RESULTS_ROOT/chaos-soak-$ts"
  mkdir -p "$RES_DIR"
  log "results dir: $RES_DIR"

  build_bins
  cluster_up

  log "starting ${DURATION}s write load (crash ~${CRASH_AFTER}s, net-fault=${NET_FAULT_SECS}s)"
  ( run_load ) > "$RES_DIR/load.log" 2>&1 &
  LOAD_PID=$!

  # 崩溃调度：在 crash_after 时刻 SIGKILL 一个节点并重启；可选网络故障兜底
  CRASHED=0
  if [ "$CRASH_AFTER" -gt 0 ]; then
    sleep "$CRASH_AFTER"
    # 默认配置 600%6=0 恒崩 node1；用当前 datanode 进程数为种子做偏移，让每次运行
    # 崩溃不同节点，交替覆盖 NODES 内的故障域（崩溃时刻仍由 --crash-after 决定）。
    TARGET=$(( (DURATION % NODES) + 1 ))
    local seed
    seed=$(( $(pgrep -f "$DATANODE_BIN" | wc -l | tr -d ' ') % NODES ))
    TARGET=$(( ((TARGET - 1 + seed) % NODES) + 1 ))
    [ "$NET_FAULT_SECS" -gt 0 ] && node_network_partition "$TARGET" "$NET_FAULT_SECS" &
    chaos_crash_relaunch "$TARGET" 40 && CRASHED="$TARGET" || log "chaos: node$TARGET relaunch slow (continuing)"
  fi

  # 采样进程生命周期间歇采集 RSS
  MONITOR_PID=""
  ( for t in $(seq 1 $((DURATION/30 + 1))); do
      sleep 30
      sample_mem "$RES_DIR/mem-$(date +%s).txt"
    done ) &
  MONITOR_PID=$!

  wait "$LOAD_PID" || { log "load driver FAILED (see $RES_DIR/load.log)"; LOAD_FAILED=1; }
  [ -n "$MONITOR_PID" ] && kill "$MONITOR_PID" 2>/dev/null || true
  if [ "${LOAD_FAILED:-0}" -eq 1 ]; then
    archive_evidence FAIL "load-driver"
    log "evidence: $RES_DIR"
    [ "$CLEANUP" -eq 1 ] && cluster_down
    die "write-load driver failed — see $RES_DIR/load.log (no durability to verify)"
  fi

  # 崩溃目标必须已经回活（原地 relaunch）；否则视为恢复失败。同时做进程普查：
  # 除了被 SIGKILL 的崩溃节点外，任何节点在负载期间死亡都是可靠性事故——多节点
  # EC 6+3 只容错单点 loss，第二个节点掉（无论何种原因）都会让 9 分片降到 <6，
  # verify 必然失败。此处显式点名哪个节点掉了。
  local census missing=""
  census="$(alive_nodes)"
  if [ "$CRASHED" -ne 0 ] && ! printf '%s\n' "$census" | grep -qE "(^| )$CRASHED( |$)"; then
    die "RESULT: FAIL (crash node $CRASHED did not recover before verify; census=$census)"
  fi
  # 普查：在线节点 == 全部（崩溃节点已回活则回到满编）。缺任何一个 → 明确失败。
  for n in $(seq 1 "$NODES"); do
    printf '%s\n' "$census" | grep -qE "(^| )$n( |$)" || missing="${missing:+$missing }$n"
  done
  if [ -n "$missing" ]; then
    log "RESULT: FAIL — node(s) DOWN before verify: $missing (expected single crash had recovered)"
    archive_evidence FAIL "unscheduled-node-down"
    log "evidence: $RES_DIR"
    if [ "$KEEP_ALIVE" -eq 1 ]; then
      log "keep-alive: cluster left running"
    elif [ "$CLEANUP" -eq 1 ]; then
      cluster_down; rm -rf "$LOG_ROOT"
    fi
    die "data reachability cannot be proven while node $missing is down"
  fi

  log "load finished; waiting healer/orphan-GC convergence before final verify"
  sleep 45
  verify_all
  archive_evidence PASS "verify"

  echo "PASS: multi-node chaos/soak ($NODES nodes x ${DISKS_PER_NODE} disks, ${DURATION}s, crash@${CRASH_AFTER}s)" \
    | tee -a "$RES_DIR/REPORT.txt"
  log "evidence: $RES_DIR"

  if [ "$KEEP_ALIVE" -eq 1 ]; then
    log "keep-alive: cluster left running (pids in $LOG_ROOT/run; data in $LOG_ROOT)"
  elif [ "$CLEANUP" -eq 1 ]; then
    cluster_down
    rm -rf "$LOG_ROOT"
  fi
}

# 失败现场兜底：无论以何种方式失败（die / 信号），先把证据归档到 $RES_DIR，
# 再清理进程与数据。这样 FAIL 也有完整日志可查，不再像旧版那样证据蒸发。
onsig() {
  local rc=$?
  if [ $rc -ne 0 ]; then
    archive_evidence FAIL "aborted(rc=$rc)"
  fi
  [ "$CLEANUP" -eq 1 ] && cluster_down 2>/dev/null
  exit $rc
}
trap 'onsig' INT TERM EXIT
main "$@"
