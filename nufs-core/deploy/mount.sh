#!/bin/bash
#
# NUFS FUSE 挂载快捷命令（面向"只挂载/卸载"的轻量入口）。
# 完整管理入口见 ./dfs-cluster.sh；本脚本复用同一套 mount-helpers.sh。
#
# 用法:
#   ./deploy/mount.sh build      # 编译本机 nufs-fuse 二进制
#   ./deploy/mount.sh mount      # 打通网络(/etc/hosts) + 挂载 FUSE
#   ./deploy/mount.sh unmount    # 卸载 FUSE + 还原 /etc/hosts
#   ./deploy/mount.sh status     # 显示挂载点/端口/二进制/pid
#
# 默认挂载点 /mnt/nufs-fuse，用 NUFS_MOUNTPOINT 覆盖。

set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/mount-helpers.sh
source "$DIR/mount-helpers.sh"

usage() {
  sed -n '2,10p' "$0"
  exit 0
}

cmd_status() {
  echo "fuse-binary : $FUSE_BIN"
  echo "mountpoint  : $MOUNTPOINT"
  echo "datanode    : $DATANODE_HOST:$DATANODE_PORT"
  if mountpoint -q "$MOUNTPOINT" 2>/dev/null; then
    echo "mounted     : yes (pid $(cat "$FUSE_PIDFILE" 2>/dev/null || echo unknown))"
  else
    echo "mounted     : no"
  fi
}

case "${1:-help}" in
  build)   build_native; log "bin/nufs-fuse ready" ;;
  mount)   patch_hosts; mount_fuse; log "=== mounted at $MOUNTPOINT ===" ;;
  unmount) mountpoint -q "$MOUNTPOINT" 2>/dev/null && unmount_fuse; restore_hosts; log "=== unmounted ===" ;;
  status)  cmd_status ;;
  *)       usage ;;
esac
