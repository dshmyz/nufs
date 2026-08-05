#!/bin/bash
#
# NUFS 集群管理命令。把"编译 → 启停集群 → 网络打通 → 挂载/卸载 → 状态/日志"
# 收敛成一条命令。
#
# 用法:
#   ./deploy/dfs-cluster.sh up          # 编译 nufs-fuse + 建镜像 + 起 V2.1 集群
#   ./deploy/dfs-cluster.sh mount       # 起集群(如需) + 打通网络 + 挂载 FUSE
#   ./deploy/dfs-cluster.sh unmount     # 卸载 FUSE + 还原 /etc/hosts
#   ./deploy/dfs-cluster.sh down        # 卸载 + 清集群 + 还原 hosts
#   ./deploy/dfs-cluster.sh status      # 容器状态
#   ./deploy/dfs-cluster.sh logs [svc]  # 尾随集群日志
#   ./deploy/dfs-cluster.sh build       # 只建本机 nufs-fuse 二进制 + docker 镜像
#   ./deploy/dfs-cluster.sh help
#
# 环境变量可覆盖默认值（见 mount-helpers.sh）：NUFS_*。

set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/mount-helpers.sh
source "$DIR/mount-helpers.sh"

usage() {
  sed -n '2,14p' "$0"
  exit 0
}

cmd_up() {
  build_native
  build_all_docker
  dfs_up
  log "=== cluster ready: metad :8091 | s3 :8081 | datanode-$DATANODE_PORT ==="
}

cmd_mount() {
  build_native
  # 若 FUSE 已在挂载则先卸载，避免重挂冲突
  if mountpoint -q "$MOUNTPOINT" 2>/dev/null; then unmount_fuse; fi
  ensure_port_map
  dfs_up
  patch_hosts
  mount_fuse
  log "=== mounted at $MOUNTPOINT (nufs-fuse pid $(cat "$FUSE_PIDFILE")) ==="
}

cmd_unmount() {
  mountpoint -q "$MOUNTPOINT" 2>/dev/null && unmount_fuse || log "not mounted at $MOUNTPOINT"
  restore_hosts
  log "=== unmounted + hosts restored ==="
}

cmd_down() {
  cmd_unmount
  dfs_down
  log "=== cluster down ==="
}

case "${1:-help}" in
  up)      cmd_up ;;
  mount)   cmd_mount ;;
  unmount) cmd_unmount ;;
  down)    cmd_down ;;
  status)  dfs_status ;;
  logs)    shift; dfs_logs "$@" ;;
  build)   build_native; build_all_docker; log "build done" ;;
  *)       usage ;;
esac
