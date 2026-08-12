#!/bin/bash
#
# RBAC 挂载认证冒烟测试（针对 deploy/docker-compose.rbac.yml 部署栈）。
#
# 验证 Phase 1 安全链路在真实 compose 环境里的行为：
#   A. 坏 secret 换 token → 拒绝（401）
#   B. 好 secret 换 token → 签发，principal 绑定为凭证 principal
#   C. 无 policy 的桶 → 挂载后写操作 EACCES（默认 deny，关闭旧 open 洞）
#   D. 设 owner policy 后 → 写操作放行（owner 恒有权）
#
# 依赖：本脚本在部署机（Linux，有 docker + /dev/fuse）上运行，compose 栈
#       的镜像（nufs-rbac:latest 或 $RBAC_IMAGE）已构建。
# 路径：在仓库根目录执行   bash deploy/host/rbac-smoke.sh
#
# 可覆盖环境变量：
#   COMPOSE_FILE   compose 文件（默认 deploy/docker-compose.rbac.yml）
#   RBAC_SECRET_KEY   seed 凭证的 secret（默认 secret123，须与 compose 一致）
#   ACCESS_KEY        挂载凭证 access key（默认 app-server-1）
#   BUCKET            mount 桶（默认 secdata）
#   METAD_PORT        metad ops 发布端口（默认 8590）

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$REPO_ROOT/deploy/docker-compose.rbac.yml}"
ACCESS_KEY="${ACCESS_KEY:-app-server-1}"
BUCKET="${BUCKET:-secdata}"
SECRET="${RBAC_SECRET_KEY:-secret123}"
METAD_PORT="${METAD_PORT:-8590}"
FUSE_CT="nufs-rbac-fuse"

TOTAL=0; FAILED=0
pass() { printf '  \033[32mPASS\033[0m %s\n' "$*"; TOTAL=$((TOTAL+1)); }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAILED=$((FAILED+1)); TOTAL=$((TOTAL+1)); }
die()  { printf '\033[31mFATAL\033[0m %s\n' "$*" >&2; exit 2; }

wait_healthy() {
  for _ in $(seq 1 60); do
    if docker inspect --format '{{.State.Health.Status}}' "nufs-rbac-metad" 2>/dev/null | grep -q healthy; then
      return 0
    fi
    sleep 1
  done
  return 1
}

cli() { # nufs-cli remote helper against metad on the compose network
  docker run --rm --network "nufs-rbac-net" --entrypoint nufs-cli \
    "$IMAGE" --mode=remote --meta-addr=nufs-rbac-metad:8091 \
    --auth-token "$OPS_TOKEN" "$@"
}

echo "== RBAC mount-auth smoke: stack=$COMPOSE_FILE =="
IMAGE="${RBAC_IMAGE:-nufs-rbac:latest}"
OPS_TOKEN="${OPS_TOKEN:-rbac-ops-secret-change-in-production}"

# --- bring up metad + datanode (not fuse yet: bucket must exist first) ---
echo "== starting metad + datanode =="
docker compose -f "$COMPOSE_FILE" up -d metad datanode
wait_healthy || die "metad did not become healthy"
echo "--- metad healthy ---"

# --- create bucket (idempotent) ---
echo "== ensure bucket '$BUCKET' =="
cli bucket create "$BUCKET" >/dev/null 2>&1 || true
cli bucket info "$BUCKET" >/dev/null || die "bucket '$BUCKET' not visible via nufs-cli"

# --- verification A/B: token exchange ---
echo "== token exchange =="
META_URL="http://localhost:${METAD_PORT}/api/v1/auth/token"

bad_status=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$META_URL" \
  -H 'Content-Type: application/json' \
  -d "{\"access_key\":\"$ACCESS_KEY\",\"secret_key\":\"wrong-secret\",\"bucket\":\"$BUCKET\"}" || true)
if [ "$bad_status" = "401" ]; then
  pass "A: wrong secret rejected (401)"
else
  fail "A: wrong secret returned $bad_status, want 401"
fi

resp=$(curl -s -X POST "$META_URL" -H 'Content-Type: application/json' \
  -d "{\"access_key\":\"$ACCESS_KEY\",\"secret_key\":\"$SECRET\",\"bucket\":\"$BUCKET\"}" || die "token curl failed")
principal=$(printf '%s' "$resp" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("principal",""))' 2>/dev/null || true)
if [ "$principal" = "$ACCESS_KEY" ]; then
  pass "B: good secret issued token bound to principal '$ACCESS_KEY'"
else
  fail "B: token principal='$principal', want '$ACCESS_KEY'"
fi

# --- bring up fuse (bucket now exists) ---
echo "== starting fuse (no policy yet → expect default deny) =="
docker compose -f "$COMPOSE_FILE" up -d fuse
for _ in $(seq 1 30); do
  if docker exec "$FUSE_CT" sh -c 'mountpoint -q /mnt/rbac' 2>/dev/null; then break; fi
  sleep 1
done
docker exec "$FUSE_CT" sh -c 'mountpoint -q /mnt/rbac' || die "fuse did not mount /mnt/rbac"

# --- verification C: default deny with no policy ---
echo "== default-deny assertion (no policy) =="
if docker exec "$FUSE_CT" touch "/mnt/rbac/.probe-$$" 2>/dev/null; then
  fail "C: write succeeded with no bucket policy (open-mode regression!)"
else
  pass "C: write denied with no policy (EACCES, default deny)"
fi

# --- set owner policy, then verification D ---
echo "== set owner policy → expect allow =="
cli acl set "$BUCKET" --default deny --owner "$ACCESS_KEY" >/dev/null || die "acl set failed"
sleep 1
if docker exec "$FUSE_CT" sh -c "touch '/mnt/rbac/.probe-$$' && rm -f '/mnt/rbac/.probe-$$'"; then
  pass "D: write allowed after owner policy set"
else
  fail "D: write still denied after owner policy set"
fi

# --- verify credential management via nufs-cli auth ---
echo "== credential registry =="
if cli auth list >/dev/null 2>&1; then
  pass "credential registry listable via nufs-cli auth list"
else
  fail "credential registry list via nufs-cli"
fi

echo
echo "== RBAC smoke ${TOTAL} checks: $((TOTAL-FAILED)) passed, $FAILED failed =="
[ "$FAILED" = "0" ] || exit 1
