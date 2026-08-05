#!/bin/bash
#
# NUFS 裸机（无 Docker）集群 + FUSE 挂载共享工具库（source 用，不直接执行）。
#
# 与 deploy/mount-helpers.sh 同构，但完全不依赖 Docker：metad / datanode /
# nufs-s3 / nufs-fuse 全部作为宿主机独立进程启动，用 pidfile + 日志管理，
# 崩溃注入用 kill -SIGKILL，内存采样读 /proc/<pid>/status。
#
# 拓扑（单机）：所有服务都在本机，register-addr 用 127.0.0.1，因此
#   * 无需 compose override（无 9103 端口发布问题）
#   * 无需改 /etc/hosts（Replicas[i].Addr = 127.0.0.1:9103，本机可直达）
#
# 端口默认（与 deploy/docker-compose.yml 对齐，避免混用冲突）：
#   metad :8091 ; s3 :8081 ; datanode(v2.1 JBOD) :9103 ; fuse 挂载 /mnt/nufs-fuse
#
# 用法：
#   source deploy/host/mount-helpers.sh
#   host_cluster_up && host_mount /mnt/nufs-fuse
#   host_unmount && host_cluster_down

set -euo pipefail

# ---------------------------------------------------------------------------
# 可覆盖配置（环境变量）
# ---------------------------------------------------------------------------
_hh_src="${BASH_SOURCE[0]:-$0}"
HH_DIR="$(cd "$(dirname "$_hh_src")/../.." && pwd)"
BIN_DIR="${NUFS_BIN_DIR:-$HH_DIR/bin}"

# 各服务二进制
METAD_BIN="${NUFS_METAD_BIN:-$BIN_DIR/metad}"
DATANODE_BIN="${NUFS_DATANODE_BIN:-$BIN_DIR/datanode}"
S3_BIN="${NUFS_S3_BIN:-$BIN_DIR/nufs-s3}"
FUSE_BIN="${NUFS_FUSE_BIN:-$BIN_DIR/nufs-fuse}"

# 数据目录（默认放在 /tmp 下，裸机测试不污染系统盘；可用 NUFS_DATA_ROOT 覆盖）
DATA_ROOT="${NUFS_DATA_ROOT:-/tmp/nufs-host}"
METAD_DIR="${NUFS_METAD_DIR:-$DATA_ROOT/metad}"
DN_D0="${NUFS_DN_D0:-$DATA_ROOT/d0}"
DN_D1="${NUFS_DN_D1:-$DATA_ROOT/d1}"
S3_PARTS="${NUFS_S3_PARTS:-$DATA_ROOT/parts}"

# 端口
METAD_OPS_ADDR="${NUFS_METAD_OPS_ADDR:-127.0.0.1:8091}"
DN_LISTEN="${NUFS_DN_LISTEN:-127.0.0.1:9103}"
DN_REGISTER="${NUFS_DN_REGISTER:-127.0.0.1:9103}"
DN_OPS_ADDR="${NUFS_DN_OPS_ADDR:-127.0.0.1:8092}"   # datanode ops 默认 8091 与 metad 冲突，移开
S3_LISTEN="${NUFS_S3_LISTEN:-127.0.0.1:8081}"

# 健康端点
METAD_HEALTH="${NUFS_METAD_HEALTH:-http://localhost:8091/health}"
S3_HEALTH="${NUFS_S3_HEALTH:-http://localhost:8081/healthz}"

# FUSE
MOUNTPOINT="${NUFS_MOUNTPOINT:-/mnt/nufs-fuse}"

# 日志目录
LOG_DIR="${NUFS_LOG_DIR:-$DATA_ROOT/log}"
METAD_LOG="$LOG_DIR/metad.log"
DATANODE_LOG="$LOG_DIR/datanode.log"
S3_LOG="$LOG_DIR/nufs-s3.log"
FUSE_LOG="$LOG_DIR/nufs-fuse.log"

# credentials（裸机 nufs-s3 用 CLI 传，无需 credentials 文件）
ACCESS_KEY="${NUFS_ACCESS_KEY:-AKIAIOSFODNN7EXAMPLE}"
SECRET_KEY="${NUFS_SECRET_KEY:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}"

# pidfile 目录
PID_DIR="${NUFS_PID_DIR:-$DATA_ROOT/run}"
METAD_PID="$PID_DIR/metad.pid"
DATANODE_PID="$PID_DIR/datanode.pid"
S3_PID="$PID_DIR/nufs-s3.pid"
FUSE_PID="$PID_DIR/nufs-fuse.pid"

# ---------------------------------------------------------------------------
# 通用
# ---------------------------------------------------------------------------
log() { printf '[%s] %s\n' "$(date +%T)" "$*"; }
die() { log "FATAL: $*" >&2; exit 1; }

ready_dir() { mkdir -p "$BIN_DIR" "$DATA_ROOT" "$LOG_DIR" "$PID_DIR" "$METAD_DIR" "$DN_D0" "$DN_D1" "$S3_PARTS"; }

# 等一个 HTTP 就绪端点（最多 ~N 秒）
wait_http() { # url, seconds, name
  local url="$1" n="$2" name="$3" i
  for i in $(seq 1 "$n"); do
    curl -sf "$url" >/dev/null 2>&1 && { log "$name ready after ${i}s"; return 0; }
    [ "$i" -eq "$n" ] && die "$name not ready after ${n}s"
    sleep 1
  done
}

# 一个进程活着吗（按 pidfile）
alive() { # pidfile
  [ -f "$1" ] && kill -0 "$(cat "$1" 2>/dev/null)" 2>/dev/null
}

# ---------------------------------------------------------------------------
# 编译（裸机需要所有 4 个二进制）
# ---------------------------------------------------------------------------
host_build() {
  ready_dir
  for b in metad datanode nufs-s3; do
    log "build bin/$b ..."
    ( cd "$HH_DIR" && go build -trimpath -o "bin/$b" "./cmd/$b" ) || die "build $b failed"
  done
  # nufs-fuse 是 linux-only（`//go:build linux`）；在部署机（Linux）上能编，
  # 在非 Linux 上跳过并告警，避免 cluster-only 用法被误伤。
  if [ "$(uname -s)" = "Linux" ]; then
    log "build bin/nufs-fuse ..."
    ( cd "$HH_DIR" && go build -trimpath -o "bin/nufs-fuse" "./cmd/nufs-fuse" ) || die "build nufs-fuse failed"
  else
    log "WARN: $(uname -s) 不支持 nufs-fuse（linux-only），跳过构建（集群可用，挂载不可用）"
  fi
  log "binaries ready: $BIN_DIR"
}

# ---------------------------------------------------------------------------
# 单点启动 / 停止（各自独立，便于崩溃注入后单独拉起 datanode）
# ---------------------------------------------------------------------------
host_start_metad() {
  alive "$METAD_PID" && { log "metad already running (pid $(cat "$METAD_PID"))"; return 0; }
  ready_dir
  "$METAD_BIN" --data-dir="$METAD_DIR" --ops-addr="$METAD_OPS_ADDR" \
    --raft=false --allow-insecure-dev --log-level=info \
    > "$METAD_LOG" 2>&1 &
  echo $! > "$METAD_PID"
  log "metad pid=$(cat "$METAD_PID") (log: $METAD_LOG)"
  wait_http "$METAD_HEALTH" 40 metad
}

host_start_datanode() {
  alive "$DATANODE_PID" && { log "datanode already running (pid $(cat "$DATANODE_PID"))"; return 0; }
  ready_dir
  "$DATANODE_BIN" --node-id=auto --listen="$DN_LISTEN" --register-addr="$DN_REGISTER" \
    --ops-addr="$DN_OPS_ADDR" \
    --data-dirs="$DN_D0,$DN_D1" --metadata=localhost:8091 \
    --rack=rack1 --zone=zone1 --storage-version=v2.1 \
    --log-level=info \
    > "$DATANODE_LOG" 2>&1 &
  echo $! > "$DATANODE_PID"
  log "datanode pid=$(cat "$DATANODE_PID") (log: $DATANODE_LOG)"
  # 等 datanode 注册（TCP 监听已开）。用 python socket 探活（macOS 无 nc -z / timeout）。
  local i
  for i in $(seq 1 40); do
    if python3 -c "
import socket,sys
s=socket.socket(); s.settimeout(1)
try:
    s.connect(('127.0.0.1', ${DN_LISTEN##*:})); sys.exit(0)
except OSError: sys.exit(1)
" 2>/dev/null; then
      log "datanode listening after ${i}s"; return 0
    fi
    alive "$DATANODE_PID" || die "datanode exited; tail $DATANODE_LOG"
    sleep 1
  done
  die "datanode listen not ready after 40s"
}

host_start_s3() {
  alive "$S3_PID" && { log "s3 already running (pid $(cat "$S3_PID"))"; return 0; }
  ready_dir
  "$S3_BIN" --listen="$S3_LISTEN" --meta-addr=localhost:8091 \
    --access-key="$ACCESS_KEY" --secret-key="$SECRET_KEY" \
    --part-dir="$S3_PARTS" --rate-limit=1000 --rate-limit-burst=2000 \
    --log-level=info \
    > "$S3_LOG" 2>&1 &
  echo $! > "$S3_PID"
  log "nufs-s3 pid=$(cat "$S3_PID") (log: $S3_LOG)"
  wait_http "$S3_HEALTH" 40 s3
}

# 一次拉起完整集群
host_cluster_up() {
  host_start_metad
  host_start_datanode
  host_start_s3
  log "cluster up: metad :8091 | datanode :${DN_LISTEN##*:} | s3 :${S3_LISTEN##*:}"
}

# 单独停某个（按名），不关闭其他
_pidfile_for() { # metad|datanode|s3|fuse -> 打印对应 pidfile 路径
  case "$1" in
    metad)   echo "$METAD_PID" ;;
    datanode) echo "$DATANODE_PID" ;;
    s3)      echo "$S3_PID" ;;
    fuse)    echo "$FUSE_PID" ;;
  esac
}

host_stop_one() { # metad|datanode|s3|fuse
  local name="$1"
  local pf
  pf="$(_pidfile_for "$name")"
  if [ -n "$pf" ] && [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null; then
    kill "$(cat "$pf")" 2>/dev/null || true
    wait "$(cat "$pf")" 2>/dev/null || true
    rm -f "$pf"
    log "$name stopped"
  fi
}

host_cluster_down() {
  host_stop_one s3
  host_stop_one datanode
  host_stop_one metad
  log "cluster down"
}

host_status() {
  local s pf
  for s in metad datanode s3 fuse; do
    pf="$(_pidfile_for "$s")"
    if [ -n "$pf" ] && [ -f "$pf" ] && kill -0 "$(cat "$pf")" 2>/dev/null; then
      echo "$s: RUNNING pid=$(cat "$pf")"
    else
      echo "$s: stopped"
    fi
  done
  mountpoint -q "$MOUNTPOINT" 2>/dev/null && echo "fuse-mounted: yes at $MOUNTPOINT" || echo "fuse-mounted: no"
}

host_logs() { # service
  local s="$1"
  case "$s" in
    metad)   tail -n 50 -f "$METAD_LOG" ;;
    datanode) tail -n 50 -f "$DATANODE_LOG" ;;
    s3)      tail -n 50 -f "$S3_LOG" ;;
    fuse)    tail -n 50 -f "$FUSE_LOG" ;;
    *)       log "unknown: $s (metad|datanode|s3|fuse)" ;;
  esac
}

# ---------------------------------------------------------------------------
# 崩溃注入（SIGKILL）——测试用，裸机版 docker kill
# ---------------------------------------------------------------------------
host_crash_datanode() { # [wait_seconds] 崩溃后等待自动观察时间（由调用方管理重启）
  local pid
  [ -f "$DATANODE_PID" ] || die "no datanode pid"
  pid="$(cat "$DATANODE_PID")"
  log ">>> SIGKILL datanode pid=$pid"
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$DATANODE_PID"
}

# 崩溃后重启 datanode（幂等：若已在跑则跳过）
host_relaunch_datanode() {
  host_start_datanode
  log "datanode relaunched (pid $(cat "$DATANODE_PID"))"
}

# ---------------------------------------------------------------------------
# 内存采样（裸机版 docker stats）——输出单位 MiB
# ---------------------------------------------------------------------------
host_sample_mem_mib() { # pidfile -> MiB（int），失败打印 -1
  local pidfile="$1"
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    awk '/VmRSS:/{print $2}' "/proc/$(cat "$pidfile")/status" 2>/dev/null \
      | awk '{printf "%d\n", $1/1024}'
  else
    echo -1
  fi
}

# ---------------------------------------------------------------------------
# FUSE 挂载 / 卸载
# ---------------------------------------------------------------------------
host_mount() { # [mountpoint]
  local mp="${1:-$MOUNTPOINT}"
  command -v "$FUSE_BIN" >/dev/null 2>&1 || [ -x "$FUSE_BIN" ] || die "nufs-fuse not found: $FUSE_BIN"
  mkdir -p "$mp"
  fusermount -u "$mp" 2>/dev/null || true
  : > "$FUSE_LOG"
  "$FUSE_BIN" --backend=nufs --meta-addr=localhost:8091 \
    --dfs-metrics-addr=:9901 --log-level=info "$mp" > "$FUSE_LOG" 2>&1 &
  echo $! > "$FUSE_PID"
  log "nufs-fuse pid=$(cat "$FUSE_PID") -> $mp (log: $FUSE_LOG)"
  local i ok=0
  for i in $(seq 1 30); do
    if mountpoint -q "$mp" 2>/dev/null; then ok=1; break; fi
    if ! alive "$FUSE_PID"; then
      log "FAIL: nufs-fuse exited early"; tail -40 "$FUSE_LOG" >&2; return 1
    fi
    sleep 1
  done
  [ "$ok" -eq 1 ] || { log "FAIL: mountpoint not ready after 30s"; tail -40 "$FUSE_LOG" >&2; return 1; }
  log "mounted at $mp"
}

host_unmount() { # [mountpoint]
  local mp="${1:-$MOUNTPOINT}"
  fusermount -u "$mp" 2>/dev/null || {
    log "fusermount fail; killing fuse pid"
    host_stop_one fuse
    sleep 2
    fusermount -u "$mp" 2>/dev/null || true
  }
  host_stop_one fuse
  log "unmounted $mp"
}
