#!/bin/bash
#
# NUFS 监控指标 ↔ 告警/SLO 配置一致性校验（make verify 门禁之一）
#
# 目的：生产就绪的监控必须是"可查询"的——每个被 告警规则 或 SLO/SLI 引用的
# Prometheus 指标（nufs_*）都必须真的被某个 exporter 在 /metrics 上发出。
# 否则就会出现「规则写了但永远没数据」的死链告警（checklist E3 的 ⚠️ 项）。
#
# 做法：
#   1. 从 deploy/monitoring/alerting-rules.yaml  提取被引用的指标名（告警）
#   2. 从 internal/slo/slo.go                    提取被引用的指标名（SLO/SLI/AlertRules）
#   3. 从全部 Go 源码（排除 slo.go 自身的引用、测试文件、nufs-fuse 无 build tag）
#      提取 exporter 实际发出的指标名（nufs_*）
#   4. 断言：yaml 引用 ⊆ 实际发出，且 slo 引用 ⊆ 实际发出（集合差必须为空）
#   5. 若本机有 promtool，再对 yaml 做规则语法检查
#
# 注意：
#   - 指标名以 `nufs_` 为前缀（metad/datanode exporter 统一前缀）。
#     `metad_leader_failover_rto_seconds` 是 failover drill 产出的测量值、非运行期
#     Prometheus 指标，故此脚本不要求它存在（独立于本检查）。
#   - 前缀白名单（如 kv 命令的允许前缀）不在此校验范围。
#
# 用法: ./scripts/check-metrics.sh
# 退出码: 0 = 全部一致/PASS；非 0 = 存在死指标或语法错误（打印清单）。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CORE_DIR="$(dirname "$SCRIPT_DIR")"
cd "$CORE_DIR"

YAML="deploy/monitoring/alerting-rules.yaml"
SLO="internal/slo/slo.go"
RC=0

# ---- 提取在 Go exporter（非 slo、非测试、非 fuse）源码中实际出现的 nufs_* 指标 ----
emitted() {
  find . -name '*.go' \
    -not -path './cmd/nufs-fuse/*' \
    -not -name '*_test.go' \
    -not -path './internal/slo/*' \
    -print0 | xargs -0 grep -ohE 'nufs_[a-zA-Z0-9_]+' 2>/dev/null \
    | sort -u
}

metric_name() { grep -oE 'nufs_[a-zA-Z0-9_]+' "$1" | sort -u; }

GO_FILE="$(mktemp)"; YAML_FILE="$(mktemp)"; SLO_FILE="$(mktemp)"; trap 'rm -f "$GO_FILE" "$YAML_FILE" "$SLO_FILE"' EXIT
emitted > "$GO_FILE"
metric_name "$YAML" > "$YAML_FILE"
metric_name "$SLO"  > "$SLO_FILE"

report_delta() {
  local label="$1" ref="$2" base="$3"
  # 集合差：在被引用但不在 exporter 发出集合中的指标
  local missing
  missing="$(comm -23 "$ref" "$base")"
  if [ -n "$missing" ]; then
    echo "  FAIL: $label 引用了以下 exporter 未发出的指标（死链）："
    echo "$missing" | sed 's/^/    - /'
    RC=1
  else
    echo "  PASS: $label 引用的所有指标均有 exporter 发出"
  fi
}

echo "== NUFS 监控指标一致性校验 =="
echo "  告警规则: $YAML"
echo "  SLO/规则: $SLO"
echo "  exporter 发出指标数: $(wc -l < "$GO_FILE")"

report_delta "alerting-rules.yaml" "$YAML_FILE" "$GO_FILE"
report_delta "slo.go" "$SLO_FILE" "$GO_FILE"

# ---- promtool 规则语法检查（存在才跑，优雅降级） ----
# 优先用宿主 promtool；没有就跳过。语法检查是锦上添花，核心的"死指标"检查
# 不依赖它、始终运行。promtool 未安装时吞掉语法层，但死指标失败仍会 fail。
if command -v promtool >/dev/null 2>&1; then
  if promtool check rules "$YAML" >/dev/null 2>&1; then
    echo "  PASS: promtool check rules (alerting-rules.yaml)"
  else
    echo "  FAIL: promtool check rules (alerting-rules.yaml)"
    promtool check rules "$YAML" || true
    RC=1
  fi
else
  echo "  SKIP: promtool 未安装，跳过规则语法检查"
fi

if [ "$RC" -eq 0 ]; then
  printf '\033[1;32mMETRICS CHECK PASS\033[0m\n'
else
  printf '\033[1;31mMETRICS CHECK FAIL\033[0m\n'
fi
exit "$RC"
