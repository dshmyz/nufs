#!/usr/bin/env bash
#
# NUFS 一键发布脚本（面向 no-Docker 裸机部署 / 可靠性测试机）。
#
# 统一发布链路的最后一步，把原本分的三件事收敛成一条命令：
#   1) 用 Makefile 的 build 目标编译全部 5 个二进制（metad/datanode/nufs-s3/
#      nufs-fuse/nufs-cli）→ 落到 nufs-core/bin/
#   2) install 到 /usr/local/bin（5 个二进制全装，方便 PATH 直达 + 与
#      /sbin/mount.nufs 默认找 /usr/local/bin/nufs-fuse 对齐）
#   3) install 挂载 helper 到 /sbin/mount.nufs → 开启标准 `mount -t nufs`
#
# 用法（需 root，因为要写 /usr/local/bin 与 /sbin）:
#   sudo ./deploy/install.sh            # 全量：build + install 二进制 + helper
#   sudo ./deploy/install.sh --no-build # 跳过编译，只装（假设 bin/ 已有产物）
#   sudo ./deploy/install.sh --bin   /usr/local/bin   # 覆盖二进制安装目录
#   sudo ./deploy/install.sh --sbin  /sbin            # 覆盖 helper 安装目录
#   ./deploy/install.sh --dry-run      # 只打印将要执行的动作，不实际安装
#
# 等价地，也可以直接用 Makefile：
#   make install                 # build + install 5 二进制到 /usr/local/bin
#   sudo make install-mount-helper # install mount.nufs 到 /sbin
# 本脚本就是把这两步（+ 可选跳过编译）封成一条，并打印对齐后的用法。
#
# 安全约束：本脚本不触碰任何 gateway/s3 源码，只做构建 + 文件安装。

set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$DIR/.." && pwd)"

BIN_DEST="${NUFS_INSTALL_BIN_DIR:-/usr/local/bin}"
SBIN_DEST="${NUFS_INSTALL_SBIN_DIR:-/sbin}"
MOUNT_HELPER_SRC="${DIR}/mount.nufs"

DO_BUILD=1
DRY_RUN=0

usage() {
  sed -n '2,24p' "$0"
  exit 0
}

say()  { printf '\033[1m[install]\033[0m %s\n' "$*"; }
die()  { echo "FATAL: $*" >&2; exit 1; }

# 解析参数
while [ $# -gt 0 ]; do
  case "$1" in
    --no-build)  DO_BUILD=0 ;;
    --dry-run)   DRY_RUN=1 ;;
    --bin)       shift; [ $# -ge 1 ] || die "--bin 需要参数"; BIN_DEST="$1" ;;
    --sbin)      shift; [ $# -ge 1 ] || die "--sbin 需要参数"; SBIN_DEST="$1" ;;
    -h|--help)   usage ;;
    *) die "未知参数: $1（--bin/--sbin/--no-build/--dry-run）" ;;
  esac
  shift
done

# 二进制清单（与 Makefile BINS 对齐）
BINS=(metad datanode nufs-s3 nufs-fuse nufs-cli)

run() {
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '  [dry-run] %s\n' "$*"
  else
    "$@"
  fi
}

if [ "$DO_BUILD" -eq 1 ]; then
  say "编译 5 个二进制（make build）..."
  run make -C "$ROOT" build
else
  say "跳过编译（--no-build），使用 ${ROOT}/bin 既有产物"
fi

# 校验产物存在（dry-run 无法实际编译 → 跳过校验，仅展示将执行的动作）
if [ "$DRY_RUN" -eq 0 ]; then
  for b in "${BINS[@]}"; do
    [ -x "$ROOT/bin/$b" ] || die "缺少产物: $ROOT/bin/$b（先跑 make build 或去掉 --no-build）"
  done
fi

say "安装二进制到 $BIN_DEST ..."
run install -d "$BIN_DEST"
for b in "${BINS[@]}"; do
  run install -m 0755 "$ROOT/bin/$b" "$BIN_DEST/$b"
done

say "安装挂载 helper 到 $SBIN_DEST/mount.nufs ..."
[ -f "$MOUNT_HELPER_SRC" ] || die "缺少 $MOUNT_HELPER_SRC"
run install -m 0755 "$MOUNT_HELPER_SRC" "$SBIN_DEST/mount.nufs"

if [ "$DRY_RUN" -eq 1 ]; then
  say "（dry-run 结束，未实际安装）"
  exit 0
fi

say "发布完成。"
cat <<EOF

已安装:
  $BIN_DEST/{metad,datanode,nufs-s3,nufs-fuse,nufs-cli}
  $SBIN_DEST/mount.nufs

挂载（需内核 FUSE + /dev/fuse，普通用户即可）:
  mkdir -p /mnt/nufs-fuse
  mount -t nufs none /mnt/nufs-fuse [-o meta=host:port,log=level]

卸载:
  umount /mnt/nufs-fuse

裸机一键集群（测试用）:
  ./deploy/host/cluster.sh mount     # build + 启集群 + 挂载

注: mount.nufs 默认找 FUSE_BIN=$BIN_DEST/nufs-fuse；
    用 NUFS_FUSE_BIN=/path/nufs-fuse 或 -o 覆盖。
EOF
