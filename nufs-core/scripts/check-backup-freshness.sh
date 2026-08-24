#!/bin/bash
#
# 发布前「备份非 stale」门禁
# ============================================================================
#
# 为什么需要: NUFS 的回滚路径是「restore 到新 cluster-id」(见
# docs/runbooks/rollback.md)。如果发布的时刻最近一次 committed backup 已经
# 超过 backup-interval 太久,那么一旦要回滚,备份点之后写入的数据就会丢失。
# 因此任何发布在动手前都必须确认「存在一个非 stale 的 committed backup」。
#
# 读什么: GET /api/v1/backups 返回 backupListResponse,其中
#   catalog.backups[].created_at   —— 每个 committed backup 的创建时间
# 门禁语义: 找到最新一个 committed backup 的 created_at;若
#   now - latest_created_at <= max_age  则 PASS,否则 FAIL。
#
# 注意: 本脚本只检查 metad ops 端点能看到的 committed backup 时间戳。真正的
# 「这份 backup 在 S3 仓库里可 verify」由 /api/v1/backups/$id/verify 单独把关
# (见 metadata-backup-restore-drill.md)。两者都是发布前必查。
#
# 用法:
#   ./scripts/check-backup-freshness.sh --endpoint http://<metad>:<ops> \
#       [--max-age 2h] [--backup-interval 1h] [--margin 1h] [--auth-token TOKEN]
#
# 退出码: 0 = 存在非 stale 的 committed backup;非 0 = 过旧 / 不可读 / 无 backup。
#
# 依赖: curl, jq, date。endpoint 指向 metad 的 ops 端口(通常是 8099)。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

ENDPOINT=""
MAX_AGE=""
BACKUP_INTERVAL=""
MARGIN=""
AUTH=""
ALLOW_NO_SUCH_LATEST=0   # 调试用;机构发布必须缺省关闭

usage() { sed -n '2,30p' "$0"; exit 0; }
while [ $# -gt 0 ]; do
  case "$1" in
    --endpoint)       ENDPOINT="$2"; shift 2;;
    --max-age)        MAX_AGE="$2"; shift 2;;
    --backup-interval) BACKUP_INTERVAL="$2"; shift 2;;
    --margin)         MARGIN="$2"; shift 2;;
    --auth-token)     AUTH="$2"; shift 2;;
    -h|--help)        usage;;
    *) echo "unknown arg: $1" >&2; usage;;
  esac
done

[ -n "$ENDPOINT" ] || { echo "error: --endpoint is required" >&2; usage; }
for t in curl jq date; do
  command -v "$t" >/dev/null 2>&1 || { echo "error: '$t' not found" >&2; exit 2; }
done

# 从 backup-interval + margin 推导 max-age,除非显式给了 --max-age
if [ -z "$MAX_AGE" ]; then
  [ -n "$BACKUP_INTERVAL" ] || { echo "error: need --max-age or --backup-interval" >&2; exit 2; }
  MARGIN="${MARGIN:-1h}"
  MAX_AGE="$BACKUP_INTERVAL + $MARGIN"
  echo "== computed max-age = ${MAX_AGE} (interval ${BACKUP_INTERVAL} + margin ${MARGIN}) =="
fi

# 便携的 Unix 时间:BSD date(macOS) 与 GNU date 用法不同,统一走 -j。
# 失败(无 date -j 支持的宿主)时打回退到词法比较,但必须显式报警,不许静默 PASS。
now_epoch="$(date -u +%s)"
parse_ts_epoch() { # $1 = ISO8601 timestamp -> stdout epoch(失败非零)
  local ts="$1"
  date -u -j -f "%Y-%m-%dT%H:%M:%SZ" "$ts" +%s 2>/dev/null
}
# 把 "24 hours"/"30 minutes"/"2h"/"90m" 这类合法化间隔解析成秒。
parse_dur_sec() { # $1 = duration string -> stdout seconds(失败非零)
  local d="$1" num unit
  d="$(echo "$d" | sed -E 's/[[:space:]]+/ /g;s/^ +| +$//g')"
  case "$d" in
    *" + "*)
      # "interval + margin" 两个可解析项相加
      local a="${d%% + *}" b="${d##* + }" sa sb
      sa="$(parse_dur_sec "$a")" || return 1
      sb="$(parse_dur_sec "$b")" || return 1
      echo $(( sa + sb )); return 0
      ;;
  esac
  num="$(echo "$d" | sed -E 's/[^0-9].*$//')"
  unit="$(echo "$d" | sed -E 's/^[0-9]+ *//')"
  [ -n "$num" ] || return 1
  case "$unit" in
    s|sec|second|seconds)     echo $(( num * 1 )) ;;
    m|min|minute|minutes)     echo $(( num * 60 )) ;;
    h|hour|hours)             echo $(( num * 3600 )) ;;
    d|day|days)               echo $(( num * 86400 )) ;;
    *) return 1 ;;
  esac
}

echo "== backup-freshness: endpoint=$ENDPOINT now=$(date -u +%Y-%m-%dT%H:%M:%SZ) =="

# 拉备份列表。backup 未启用时 ops 返回 503 + backup_disabled —— 那是发布前的硬失败。
CURL_ARGS=(-fsS)
[ -n "$AUTH" ] && CURL_ARGS+=(-H "Authorization: Bearer $AUTH")
resp="$(curl "${CURL_ARGS[@]}" "$ENDPOINT/api/v1/backups")" \
  || { echo "FAIL: cannot read $ENDPOINT/api/v1/backups (curl rc=$?)" >&2; exit 1; }

if ! echo "$resp" | jq -e '.catalog.backups' >/dev/null 2>&1; then
  if echo "$resp" | jq -e '.error' >/dev/null 2>&1; then
    echo "FAIL: ops returned: $(echo "$resp" | jq -r '.error')" >&2
  else
    echo "FAIL: response has no catalog.backups; raw: $(echo "$resp" | head -c 300)" >&2
  fi
  exit 1
fi

# 找最新 committed backup 的 created_at(GET /backups 返回的 catalog 已按时间排序)。
latest="$(echo "$resp" | jq -r '[.catalog.backups[] | select(.created_at != null and .created_at != "")] | sort_by(.created_at) | last | .created_at // empty')"
if [ -z "$latest" ]; then
  echo "FAIL: no committed backup found in catalog — nothing safe to roll back to" >&2
  exit 1
fi
echo "== latest committed backup created_at = $latest =="

# 计算 now - latest,与 max-age 比较。先用 -j 解析 created_at;解析失败的宿主
# 必须显式 FAIL(不许静默 PASS),让 operator 知道门禁没真正验到时间。
latest_epoch="$(parse_ts_epoch "$latest")" || latest_epoch=""
max_sec="$(parse_dur_sec "$MAX_AGE")" || max_sec=""
if [ -z "$latest_epoch" ] || [ -z "$max_sec" ]; then
  echo "FAIL: cannot compute age on this host (created_at=$latest max_age=$MAX_AGE) — do a real freshness check before release" >&2
  exit 2
fi
age_sec=$(( now_epoch - latest_epoch ))

echo "== backup age = ${age_sec}s, max-age = ${max_sec}s =="
if [ "$age_sec" -le "$max_sec" ]; then
  echo "PASS: newest backup is non-stale"
  exit 0
else
  echo "FAIL: newest backup is ${age_sec}s old, exceeds max-age ${max_sec}s — do NOT release without a fresh, verified backup" >&2
  exit 1
fi
