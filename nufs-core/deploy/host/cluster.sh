#!/bin/bash
#
# NUFS 裸机（无 Docker）集群管理命令。
#
# 直接以宿主机进程启动 metad + datanode(v2.1 JBOD) + nufs-s3 + nufs-fuse，
# 不依赖 docker/docker-compose/compose override//etc/hosts 打通（单机 localhost 即可）。
#
# 用法:
#   ./deploy/host/cluster.sh up          # 编译 4 个二进制 + 起全部服务
#   ./deploy/host/cluster.sh mount       # 一条龙：起集群 + 挂载 FUSE
#   ./deploy/host/cluster.sh unmount     # 卸载 FUSE
#   ./deploy/host/cluster.sh down        # 卸载 + 停全部服务
#   ./deploy/host/cluster.sh status      # 各进程 + 挂载点状态
#   ./deploy/host/cluster.sh logs [svc]  # 尾随日志 (metad|datanode|s3|fuse)
#   ./deploy/host/cluster.sh build       # 只编译
#   ./deploy/host/cluster.sh crash       # 对 datanode 注入 SIGKILL（测试用）
#   ./deploy/host/cluster.sh relaunch    # 崩溃后重启 datanode
#   ./deploy/host/cluster.sh help
#
# 环境变量可覆盖默认值（见 deploy/host/mount-helpers.sh）：NUFS_*。

set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/host/mount-helpers.sh
source "$DIR/mount-helpers.sh"

usage() {
  sed -n '2,16p' "$0"
  exit 0
}

cmd_up() {
  host_build
  host_cluster_up
  log "=== cluster ready ==="
}

cmd_mount() {
  host_build
  if mountpoint -q "$MOUNTPOINT" 2>/dev/null; then host_unmount; fi
  host_cluster_up
  host_mount
  log "=== mounted at $MOUNTPOINT (pid $(cat "$FUSE_PID")) ==="
}

cmd_unmount() {
  mountpoint -q "$MOUNTPOINT" 2>/dev/null && host_unmount || log "not mounted at $MOUNTPOINT"
  log "=== unmounted ==="
}

cmd_down() {
  cmd_unmount
  host_cluster_down
  log "=== cluster down ==="
}

case "${1:-help}" in
  up)       cmd_up ;;
  mount)    cmd_mount ;;
  unmount)  cmd_unmount ;;
  down)     cmd_down ;;
  status)   host_status ;;
  logs)     shift; host_logs "${1:-fuse}" ;;
  build)    host_build; log "build done" ;;
  crash)    host_crash_datanode; log "datanode SIGKILLed (relaunch to resume)" ;;
  relaunch) host_relaunch_datanode; log "datanode relaunched" ;;
  *)        usage ;;
esac
