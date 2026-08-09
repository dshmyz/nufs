#!/bin/bash
#
# NUFS 回归门禁的 Docker 版入口（make verify-docker 的后端）
#
# 为什么需要 Docker：`make verify [-l fast|drill|full]` 在 macOS 宿主上跑 -count=2
# 的分包串行单测时，cmd/metad · datanode · metadata 这些端口吃紧的包会撞上 macOS 的
# 临时端口上限（net.inet.ip.portrange = 49152..65535 仅 16384 个，且 tcp_fin_timeout=120s），
# 报 `can't assign requested address`（EADDRNOTAVAIL）假失败——与产品正确性无关。
# 在 Linux 容器里临时端口池大一倍多（32768..60999 = 28232 个）且 fin_timeout=60s、又无
# 桌面应用抢端口，同一个 -count=2 全量门禁实测全部 PASS（见 memory「suite-count2-stability」的
# 端口维度：那里的"分包 -count=2 -p 1"可靠门禁，在这台 macOS 上被 EC-topology 测试的
# 数据面扇出顶穿端口预算；Docker 把它恢复为干净基线）。
#
# 本脚本只做一件事：把宿主的 nufs-core 源码 + Go 模块/构建缓存只读挂进 golang 容器，
# 在容器内重新调用 scripts/verify.sh（保持单一事实来源），并把 drill 的二进制输出目录
# 重定向到容器本地（NUFS_BIN_DIR），避免在宿主 bin/ 里留下 Linux 二进制。
# go 走默认 -mod=readonly（不写 go.mod/go.sum，保持宿主树干净）。
#
# 用法: ./scripts/verify-docker.sh [verify.sh 的全部参数]   # e.g. -l fast / -l drill / -l full
# 环境: 需要本机已安装并运行 Docker（Linux 容器，临时端口池为 Linux 默认）。
#       模块下载用只读的宿主 GOMODCACHE，无需网络（GOPROXY=off）。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CORE_DIR="$(dirname "$SCRIPT_DIR")"

# 复用宿主的 Go 缓存，让容器内 build/test 免重新下载依赖、免重编依赖
GOMODCACHE_HOST="$(go env GOMODCACHE)"
GOCACHE_HOST="$(go env GOCACHE)"
[ -d "$GOMODCACHE_HOST" ] || { echo "GOMODCACHE 不存在: $GOMODCACHE_HOST" >&2; exit 2; }
[ -d "$GOCACHE_HOST" ]    || mkdir -p "$GOCACHE_HOST"

# 容器内固定路径
SRC_DIR=/src
MOD=/gomod
CACHE=/gocache
BIN_DIR=/nufs-bin   # drill 进程的 Linux 二进制落这里，不污染宿主 bin/

# 本地已缓存、与 go.mod(1.25) 兼容的 golang 镜像（无新拉取）
GO_IMAGE="${NUFS_VERIFY_GO_IMAGE:-golang:1.26}"

echo "== NUFS verify (Docker) image=$GO_IMAGE src=$CORE_DIR bin->container-local =="
echo "   mounts: GOMODCACHE=$GOMODCACHE_HOST (ro), GOCACHE=$GOCACHE_HOST (ro)"

exec docker run --rm \
  -v "$CORE_DIR":"$SRC_DIR" \
  -v "$GOMODCACHE_HOST":"$MOD":ro \
  -v "$GOCACHE_HOST":"$CACHE":ro \
  -e GOMODCACHE="$MOD" \
  -e GOCACHE="$CACHE" \
  -e GOPROXY=off \
  -e NUFS_BIN_DIR="$BIN_DIR" \
  -e VERIFY_LEVEL="${VERIFY_LEVEL:-fast}" \
  -w "$SRC_DIR" \
  "$GO_IMAGE" \
  bash "$SRC_DIR/scripts/verify.sh" "$@"
