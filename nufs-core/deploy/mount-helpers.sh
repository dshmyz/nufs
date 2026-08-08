#!/bin/bash
#
# NUFS 集群 + FUSE 挂载共享工具库（source 用，不直接执行）。
#
# 提供一套"开箱即用"的命令原语，把"编译 → 启集群 → 打通网络 → 挂载 FUSE →
# 卸载 → 清集群"的零散命令收敛成可复用的函数，供 dfs-cluster.sh / mount.sh /
# 以及 scripts/fatigue-fuse.sh、scripts/smallfile-fuse.sh 引用。
#
# 全部路径/主机名默认值与 deploy/docker-compose.yml 对齐：
#   metad  :8091 ; s3 :8081 ; datanode-v21-multi :9103（JBOD /d0,/d1, multidisk profile）
#   nufs-fuse 挂载点默认 /mnt/nufs-fuse
#
# 关键网络事实（必须知晓）：
#   datanode 以 --register-addr=datanode-v21-multi:9103 上报（容器内主机名），
#   chunk.Replicas[i].Addr 存的就是它。宿主机上的 nufs-fuse 要能拨通 datanode，
#   需要 (a) compose override 把 9103 发布到宿主、(b) /etc/hosts 把该主机名映射到
#   容器 bridge IP。本库的 patch_hosts / restore_hosts 处理后者，ensure_port_map
#   生成前者。
#
# 用法：
#   source deploy/mount-helpers.sh
#   dfs_up && mount_fuse /mnt/nufs-fuse   # 一条龙
#   unmount_fuse /mnt/nufs-fuse && dfs_down

set -euo pipefail

# ---------------------------------------------------------------------------
# 可覆盖配置（环境变量）
# ---------------------------------------------------------------------------
DFC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${NUFS_COMPOSE:-$DFC_DIR/deploy/docker-compose.yml}"
FUSE_OVERRIDE="${NUFS_OVERRIDE:-$DFC_DIR/deploy/compose.fuse-override.yml}"
MULTI_CONTAINER="${NUFS_MULTI_CONTAINER:-deploy-datanode-v21-multi-1}"
DATANODE_HOST="${NUFS_DATANODE_HOST:-datanode-v21-multi}"
DATANODE_PORT="${NUFS_DATANODE_PORT:-9103}"
METAD_HEALTH="${NUFS_METAD_HEALTH:-http://localhost:8091/health}"
S3_HEALTH="${NUFS_S3_HEALTH:-http://localhost:8081/healthz}"
FUSE_BIN="${NUFS_FUSE_BIN:-$DFC_DIR/bin/nufs-fuse}"
MOUNTPOINT="${NUFS_MOUNTPOINT:-/mnt/nufs-fuse}"
HOSTS_BACKUP="${NUFS_HOSTS_BACKUP:-/tmp/nufs-hosts.bak}"
FUSE_PIDFILE="${NUFS_FUSE_PIDFILE:-/tmp/nufs-fuse.pid}"
FUSE_LOG="${NUFS_FUSE_LOG:-/tmp/nufs-fuse-mount.log}"

# ---------------------------------------------------------------------------
# 通用
# ---------------------------------------------------------------------------
log()  { printf '[%s] %s\n' "$(date +%T)" "$*"; }
die()  { log "FATAL: $*" >&2; exit 1; }

# 等一个 HTTP 就绪端点（最多 ~N 秒）
wait_http() { # url, seconds, name
  local url="$1" n="$2" name="$3" i
  for i in $(seq 1 "$n"); do
    curl -sf "$url" >/dev/null 2>&1 && { log "$name ready after ${i}s"; return 0; }
    [ "$i" -eq "$n" ] && die "$name not ready after ${n}s"
    sleep 1
  done
}

# ---------------------------------------------------------------------------
# 编译
# ---------------------------------------------------------------------------
build_native() { # [bins...] 默认 nufs-fuse nufs-cli（宿主机要跑的服务）
  local bins=("$@"); [ ${#bins[@]} -eq 0 ] && bins=(nufs-fuse nufs-cli)
  mkdir -p "$DFC_DIR/bin"
  for b in "${bins[@]}"; do
    log "build bin/$b ..."
    ( cd "$DFC_DIR" && go build -trimpath -o "bin/$b" "./cmd/$b" ) || die "build $b failed"
  done
  log "native binaries: ${bins[*]} ($DFC_DIR/bin)"
}

build_all_docker() {
  log "build docker images (metad/datanode/nufs-s3) ..."
  docker compose -f "$COMPOSE_FILE" build 2>&1 | tail -2
}

# ---------------------------------------------------------------------------
# 集群启停（V2.1 multidisk + metad + s3，供 FUSE 使用的最小拓扑）
# ---------------------------------------------------------------------------
dfs_up() {
  log "start cluster: metad + datanode-v21-multi + s3"
  docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
  docker compose -f "$COMPOSE_FILE" --profile multidisk up -d metad datanode-v21-multi s3 2>&1
  wait_http "$METAD_HEALTH" 40 metad
  local i
  for i in $(seq 1 40); do
    docker exec "$MULTI_CONTAINER" pgrep -f datanode >/dev/null 2>&1 && { log "datanode up after ${i}s"; break; }
    [ "$i" -eq 40 ] && die "datanode not up"
    sleep 1
  done
  wait_http "$S3_HEALTH" 40 s3
  log "cluster up"
}

dfs_down() {
  log "teardown cluster"
  docker compose -f "$COMPOSE_FILE" --profile multidisk down -v 2>/dev/null || true
  log "cluster torn down"
}

dfs_status() {
  docker compose -f "$COMPOSE_FILE" --profile multidisk ps 2>/dev/null || true
}

dfs_logs() { # [service...]
  docker compose -f "$COMPOSE_FILE" --profile multidisk logs -f --tail=50 "$@"
}

# ---------------------------------------------------------------------------
# 网络打通（让宿主机 nufs-fuse 摸到容器内 datanode）
# ---------------------------------------------------------------------------
# 生成一个临时 override：把 datanode TCP 端口发布到宿主。幂等。
ensure_port_map() {
  cat > "$FUSE_OVERRIDE" <<YAML
services:
  $DATANODE_HOST:
    ports:
      - "$DATANODE_PORT:$DATANODE_PORT"
YAML
  log "override written: $FUSE_OVERRIDE (publishes $DATANODE_HOST:$DATANODE_PORT)"
}

# 取容器 bridge IP 写 /etc/hosts；先备份原文件。幂等（重复调用不会重复追加）。
patch_hosts() {
  local ip
  ip="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$MULTI_CONTAINER" 2>/dev/null | awk '{print $1}')"
  [ -z "$ip" ] && die "cannot resolve bridge IP for $MULTI_CONTAINER"
  [ -f "$HOSTS_BACKUP" ] || cp /etc/hosts "$HOSTS_BACKUP"
  if grep -q "^[^#]*$DATANODE_HOST" /etc/hosts; then
    sed -i "/$DATANODE_HOST/d" /etc/hosts
  fi
  printf '%-20s %s\n' "$ip" "$DATANODE_HOST" >> /etc/hosts
  log "/etc/hosts: $DATANODE_HOST -> $ip"
}

# 还原 /etc/hosts（若本库改过）。幂等。
restore_hosts() {
  if [ -f "$HOSTS_BACKUP" ]; then
    cp "$HOSTS_BACKUP" /etc/hosts
    rm -f "$HOSTS_BACKUP"
    log "/etc/hosts restored"
  fi
}

# ---------------------------------------------------------------------------
# FUSE 挂载 / 卸载
# ---------------------------------------------------------------------------
mount_fuse() { # [mountpoint]
  local mp="${1:-$MOUNTPOINT}"
  command -v "$FUSE_BIN" >/dev/null 2>&1 || [ -x "$FUSE_BIN" ] || die "nufs-fuse not found: $FUSE_BIN (build first?)"
  mkdir -p "$mp"
  fusermount -u "$mp" 2>/dev/null || true
  : > "$FUSE_LOG"
  "$FUSE_BIN" --backend=nufs --meta-addr=localhost:8091 \
    --dfs-metrics-addr=:9901 --log-level=info "$mp" > "$FUSE_LOG" 2>&1 &
  echo $! > "$FUSE_PIDFILE"
  log "nufs-fuse pid=$(cat "$FUSE_PIDFILE") -> $mp (log: $FUSE_LOG)"
  local i ok=0
  for i in $(seq 1 30); do
    if mountpoint -q "$mp" 2>/dev/null; then ok=1; break; fi
    if ! kill -0 "$(cat "$FUSE_PIDFILE")" 2>/dev/null; then
      log "FAIL: nufs-fuse exited early"; tail -40 "$FUSE_LOG" >&2; return 1
    fi
    sleep 1
  done
  [ "$ok" -eq 1 ] || { log "FAIL: mountpoint not ready after 30s"; tail -40 "$FUSE_LOG" >&2; return 1; }
  log "mounted at $mp"
}

unmount_fuse() { # [mountpoint]
  local mp="${1:-$MOUNTPOINT}"
  fusermount -u "$mp" 2>/dev/null || {
    log "fusermount fail; killing fuse pid"
    [ -f "$FUSE_PIDFILE" ] && kill "$(cat "$FUSE_PIDFILE")" 2>/dev/null || true
    sleep 2
    fusermount -u "$mp" 2>/dev/null || true
  }
  [ -f "$FUSE_PIDFILE" ] && wait "$(cat "$FUSE_PIDFILE")" 2>/dev/null || true
  rm -f "$FUSE_PIDFILE"
  log "unmounted $mp"
}
