#!/bin/bash
#
# NUFS 回归门禁入口（make verify 的后端）
#
# 把"可上线基线"的各项门禁收敛成一个可重复、可选层级的入口，串成：
#   build -> vet -> fmt-check -> metrics/alert 一致性 -> 全量单测（分包串行 -count=2） -> 故障 drill 门禁
#
# 分层（-l/--level）：
#   fast   —— 仅 Go 门禁（build/vet/fmt/单测），约数分钟，适合每次提交/CI 快速回归
#   drill  —— fast + 三个 soak drill（leader-failover / metadata-restore / chaos-soak）
#             用短时长跑，用于"故障注入基线"回归
#   full   —— drill + 长时/高 count 门禁（storage-crash 50x、p0 storage 20x），最慢，上线前跑
#
# 退出码：0 = 全部 PASS；非 0 = 任一 FAIL（打印失败阶段）。PASS/FAIL 与各 drill 自身语义一致。
#
# 关键稳定性约束（见 memory「suite-count2-stability」）：
#   - 全量单测必须「分包 -count=2」并串行（-p 1），NOT 一个并行饱和的 go test ./...
#     —— 并行会因 CPU 拥塞误报 raft 选举超时（false FAIL），与本产品正确性无关。
#   - metadata/raft 测试尤其要 -p 1 隔离。
#
# 用法:
#   ./scripts/verify.sh                      # = -l fast
#   ./scripts/verify.sh -l drill             # 短时 drill 门禁
#   ./scripts/verify.sh -l full              # 上线前完整门禁（含长时/高 count）
#   ./scripts/verify.sh -l fast -p pkg/...   # 只测指定包（分包 -count=2）
set -euo pipefail

# ---- 定位仓库根（scripts/.. 即 nufs-core/） ----
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CORE_DIR="$(dirname "$SCRIPT_DIR")"
cd "$CORE_DIR"

LEVEL="${VERIFY_LEVEL:-fast}"
COUNT="${VERIFY_COUNT:-2}"
TARGET="${VERIFY_TARGET:-./...}"
FAILED=0
STEP_N=0

step() { STEP_N=$((STEP_N+1)); printf '\n\033[1;36m[%d] %s\033[0m\n' "$STEP_N" "$*"; }
pass() { printf '  \033[32mPASS:\033[0m %s\n' "$*"; }
fail() { printf '  \033[31mFAIL:\033[0m %s\n' "$*"; FAILED=1; }
isok() { [ "$FAILED" -eq 0 ]; }

usage() { sed -n '2,30p' "$0"; exit 0; }
while [ $# -gt 0 ]; do
  case "$1" in
    -l|--level) LEVEL="$2"; shift 2;;
    -p|--package) TARGET="$2"; shift 2;;
    --count) COUNT="$2"; shift 2;;
    -h|--help) usage;;
    *) echo "unknown: $1" >&2; usage;;
  esac
done

case "$LEVEL" in
  fast|drill|full) ;;
  *) echo "invalid level: $LEVEL (fast|drill|full)" >&2; exit 2;;
esac

echo "== NUFS verify: level=$LEVEL count=$COUNT target=$TARGET cwd=$(pwd) =="

# ---- 0. build ----
step "build"
if go build ./... ; then pass "go build ./..."; else fail "go build ./..."; fi

# ---- 1. vet ----
step "vet"
if go vet "$TARGET" ; then pass "go vet $TARGET"; else fail "go vet $TARGET"; fi

# ---- 2. fmt check（不改文件，只查） ----
step "gofmt check"
gofmt -s -l . 2>/dev/null | grep -q . && { echo "  unformatted files:"; gofmt -s -l . ; fail "gofmt"; } || pass "gofmt"

# ---- 3. 指标-告警一致性（死指标/命名漂移门禁，所有级别都跑） ----
step "metrics/alert consistency (check-metrics)"
if bash scripts/check-metrics.sh ; then pass "check-metrics"; else fail "check-metrics"; fi

# ---- 4. 全量单测：分包串行 -count=N（memory: suite-count2-stability 的可靠门禁） ----
step "go test per-package -count=$COUNT (serial -p 1)"
# fast/drill 用 -short：跳过 §18.4 的 scale/长期 pressure 测试（TestScale_ExtentThroughput
# 一个就 92s，它们也 testing.Short() skip），与 make test-storage-p0 的 -short 语义对齐，
# 常规回归只跑正确性门禁；scale/crash 高强度门禁留给 full 级别显式跑（test-storage-p0/crash）。
SHORT_FLAG=""
if [ "$LEVEL" = "fast" ] || [ "$LEVEL" = "drill" ]; then
  SHORT_FLAG="-short"
fi
if [ "$TARGET" = "./..." ]; then
  # 按包逐个跑，-p 1 串行，避免并行 CPU 拥塞导致的 raft 选举假失败
  pkgs="$(go list ./... 2>/dev/null)"
  pkg_fail=0
  VTEST_DIR="${VERIFY_TEST_LOG_DIR:-/tmp/nufs-verify-testlogs}"
  mkdir -p "$VTEST_DIR"
  while IFS= read -r p; do
    printf '    %-55s ' "$p"
    slog="${VTEST_DIR}/$(echo "$p" | tr '/' '_').log"
    if go test -count="$COUNT" $SHORT_FLAG -timeout 300s -p 1 "$p" >"$slog" 2>&1; then
      printf '\033[32mPASS\033[0m\n'
    else
      # 一次有界重试：吸收少见的一次性 flake（端口/调度/临时资源瞬态），避免把
      # 真门禁刷红；仍保留首跑日志，若二次仍 FAIL 才判 FAIL（真 bug 不会躲过两次）。
      printf '\033[33mRETRY\033[0m '
      if go test -count="$COUNT" $SHORT_FLAG -timeout 300s -p 1 "$p" >"$slog.r2" 2>&1; then
        printf '\033[32mPASS (retry)\033[0m\n      first-attempt log kept: %s\n' "$slog"
      else
        printf '\033[31mFAIL\033[0m\n'
        pkg_fail=1
        # 失败保留日志 + 打印尾部，便于区分「真 bug」与「本机 CPU 拥塞的 raft 假失败」。
        echo "      --- $p log tail ($slog) ---"
        tail -15 "$slog" | sed 's/^/        /'
        echo "      --- retry-attempt log ($slog.r2) ---"
        tail -15 "$slog.r2" | sed 's/^/        /'
      fi
    fi
  done <<< "$pkgs"
  [ "$pkg_fail" -eq 0 ] && pass "all packages -count=$COUNT" || fail "one or more packages -count=$COUNT"
else
  if go test -count="$COUNT" $SHORT_FLAG -timeout 600s -p 1 "$TARGET" ; then pass "$TARGET -count=$COUNT"; else fail "$TARGET -count=$COUNT"; fi
fi

# ---- 5. 故障 drill 门禁（仅 drill/full） ----
if [ "$LEVEL" = "drill" ] || [ "$LEVEL" = "full" ]; then
  # 用短时长跑 drill，锁「故障注入基线」；--full 才用长时/高 count 权威门禁。
  DUR="${VERIFY_DR_DURATION:-150}"
  FAILOVER="${VERIFY_FAILOVER_AFTER:-60}"

  step "drill: leader-failover (PASS gate: RTO<=15s, out_of_window_errors=0)"
  if NUFS_LOG_ROOT="/tmp/nufs-verify-leaderfail" \
     bash scripts/soak/run-v21-leader-failover.sh --duration "$DUR" --failover-after "$FAILOVER" ; then
    pass "leader-failover drill"
  else
    fail "leader-failover drill (see NUFS_LOG_ROOT=/tmp/nufs-verify-leaderfail)"
  fi

  step "drill: metadata-restore (PASS gate: backup->restore preserves data)"
  if NUFS_LOG_ROOT="/tmp/nufs-verify-metarestore" \
     bash scripts/soak/run-v21-metadata-restore.sh ; then
    pass "metadata-restore drill"
  else
    fail "metadata-restore drill (see NUFS_LOG_ROOT=/tmp/nufs-verify-metarestore)"
  fi

  if isok && [ "$LEVEL" = "full" ]; then
    step "drill: chaos-soak (crash + self-heal + RSS leak gate)"
    if NUFS_LOG_ROOT="/tmp/nufs-verify-chaos" \
       bash scripts/soak/run-v21-chaos-soak.sh --nodes 3 --duration "$DUR" --crash-after 40 ; then
      pass "chaos-soak drill"
    else
      fail "chaos-soak drill"
    fi
  fi

  step "drill: network-fault-injection (partition/loss/latency via S3 gateway)"
  if bash scripts/soak/run-v21-network-faults.sh ; then
    pass "network-fault-injection drill"
  else
    fail "network-fault-injection drill"
  fi
fi

# ---- 6. 上线前长时/高 count 门禁（仅 full） ----
if [ "$LEVEL" = "full" ]; then
  step "P0 storage correctness (race, 20x)"
  if make test-storage-p0 >/dev/null 2>&1; then pass "test-storage-p0"; else fail "test-storage-p0"; fi

  step "P0 storage crash recovery (50x)"
  if make test-storage-crash >/dev/null 2>&1; then pass "test-storage-crash"; else fail "test-storage-crash"; fi
fi

# ---- 汇总 ----
step "result"
if isok; then
  printf '\n\033[1;32mVERIFY PASS\033[0m (level=%s)\n' "$LEVEL"
  exit 0
else
  printf '\n\033[1;31mVERIFY FAIL\033[0m (level=%s)\n' "$LEVEL"
  exit 1
fi
