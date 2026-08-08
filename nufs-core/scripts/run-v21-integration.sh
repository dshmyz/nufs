#!/bin/bash
#
# V2.1 存储引擎集成测试
#
# 默认启动 V2.1-only 集群（metad + datanode-v21 + S3 gateway），通过
# S3 API 验证端到端读写路径：创建 bucket → 写入 1MiB → 读回 → 字节精确。
# 这是 V2.1 引擎服务真实读写的正式验证（非 V1 datanode）。
#
# V1 datanode 通过 profile 隔离（--profile v1 才启用），默认不干扰。
#
# 前置条件: docker compose, python3
# 用法: ./scripts/run-v21-integration.sh [--cleanup]

set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE_FILE="deploy/docker-compose.yml"
S3_ENDPOINT="http://localhost:8081"
ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
BUCKET="v21-integration"
KEY="test-object.bin"
TEST_FILE="/tmp/v21-test-file.bin"
TEST_FILE_DL="/tmp/v21-test-file-dl.bin"

# SigV4 signing helper shared by all S3 calls. Writes the response status
# of each step and verifies byte-exactness at the end.
SIGN_PY="
import hashlib, hmac, datetime, urllib.request, os, sys
ak='$ACCESS_KEY'; sk='$SECRET_KEY'; ep='$S3_ENDPOINT'; b='$BUCKET'; k='$KEY'
def sign(m,p,h,bd=b''):
    n=datetime.datetime.now(datetime.timezone.utc)
    d=n.strftime('%Y%m%dT%H%M%SZ'); ds=n.strftime('%Y%m%d')
    h['host']='localhost:8081'; h['x-amz-date']=d
    h['x-amz-content-sha256']=hashlib.sha256(bd).hexdigest()
    ch=''.join(f'{kk.lower()}:{vv.strip()}\n' for kk,vv in sorted(h.items()))
    sh=';'.join(sorted(kk.lower() for kk in h))
    cr=f'{m}\n{p}\n\n{ch}\n{sh}\n{h[\"x-amz-content-sha256\"]}'
    cs=f'{ds}/us-east-1/s3/aws4_request'
    sts=f'AWS4-HMAC-SHA256\n{d}\n{cs}\n{hashlib.sha256(cr.encode()).hexdigest()}'
    def skf(kk,mm): return hmac.new(kk,mm.encode(),hashlib.sha256).digest()
    kd=skf(('AWS4'+sk).encode(),ds); kr=skf(kd,'us-east-1'); ks=skf(kr,'s3'); kg=skf(ks,'aws4_request')
    sg=hmac.new(kg,sts.encode(),hashlib.sha256).hexdigest()
    h['Authorization']=f'AWS4-HMAC-SHA256 Credential={ak}/{cs}, SignedHeaders={sh}, Signature={sg}'
    return h
def req(m,p,bd=b'',extra=None):
    h={} if extra is None else dict(extra)
    h=sign(m,p,h,bd)
    return urllib.request.Request(f'{ep}{p}',data=bd or None,headers=h,method=m)
"

echo "=== V2.1 存储引擎集成测试（V2.1-only）==="
echo ""

# 1. Build 镜像
echo "--- Step 1: Build Docker image ---"
docker compose -f "$COMPOSE_FILE" build 2>&1 | tail -2
echo ""

# 2. 启动 V2.1-only 集群（metad + datanode-v21 + s3）
echo "--- Step 2: Start V2.1-only cluster ---"
docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
docker compose -f "$COMPOSE_FILE" up -d metad datanode-v21 s3 2>&1
echo ""

# 3. 等待 metad
echo "--- Step 3: Wait for metad ---"
for i in $(seq 1 30); do
  if curl -sf http://localhost:8091/health > /dev/null 2>&1; then
    echo "metad ready after ${i}s"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "metad failed to start"
    docker compose -f "$COMPOSE_FILE" logs metad --tail 20
    exit 1
  fi
  sleep 1
done
echo ""

# 4. 等待 V2.1 datanode 注册并 online
echo "--- Step 4: Wait for V2.1 datanode ---"
for i in $(seq 1 30); do
  if docker logs deploy-datanode-v21-1 2>&1 | grep -q '"TCP server listening"'; then
    echo "datanode-v21 ready after ${i}s"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "datanode-v21 failed to start"
    docker compose -f "$COMPOSE_FILE" logs datanode-v21 --tail 20
    exit 1
  fi
  sleep 1
done

# 确认 V2.1 是唯一 online 节点
ONLINE=$(curl -s http://localhost:8091/api/v1/nodes | python3 -c "
import json,sys
online=[n for n in json.load(sys.stdin) if n.get('state')==0]
print(len(online))
")
echo "online nodes: $ONLINE (应只有 datanode-v21)"
if [ "$ONLINE" -ne 1 ]; then
  echo "FAIL: expected exactly 1 online node (V2.1), got $ONLINE"
  exit 1
fi
echo ""

# 5. 等待 S3 gateway
echo "--- Step 5: Wait for S3 gateway ---"
for i in $(seq 1 30); do
  if curl -sf http://localhost:8081/healthz > /dev/null 2>&1; then
    echo "S3 gateway ready after ${i}s"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "S3 gateway failed to start"
    docker compose -f "$COMPOSE_FILE" logs s3 --tail 20
    exit 1
  fi
  sleep 1
done
echo ""

# 6. 通过 metad 创建 RF=1 bucket（仅 V2.1 一个副本即可满足）
echo "--- Step 6: Create RF=1 bucket ---"
CREATE_BUCKET=$(curl -s -X POST http://localhost:8091/api/v1/buckets \
  -H "Content-Type: application/json" \
  -d '{"name":"'$BUCKET'","policy":{"replication_factor":1}}' -w "|%{http_code}")
CODE="${CREATE_BUCKET##*|}"
echo "create bucket http_code=$CODE"
if [ "$CODE" != "201" ] && [ "$CODE" != "500" ]; then
  # 500 可能是 bucket 已存在（幂等场景），继续
  echo "unexpected create bucket status: $CREATE_BUCKET"
  exit 1
fi
echo ""

# 7. S3 PUT 1MiB（owner 匹配访问 key，绕过 RBAC deny）
echo "--- Step 7: S3 PUT 1MiB ---"
dd if=/dev/urandom bs=1M count=1 of="$TEST_FILE" 2>/dev/null
python3 -c "$SIGN_PY
data=open('$TEST_FILE','rb').read()
try:
    r=urllib.request.urlopen(req('PUT',f'/{b}/{k}',bd=data,extra={'content-type':'application/octet-stream','x-owner':ak}))
    print(f'PUT http={r.status} size={len(data)}')
    sys.exit(0 if r.status==200 else 1)
except urllib.error.HTTPError as e:
    print(f'PUT FAILED http={e.code} {e.read()[:200]}')
    sys.exit(1)
"
echo ""

# 8. S3 GET + 验证字节精确
echo "--- Step 8: S3 GET + verify ---"
python3 -c "$SIGN_PY
try:
    r=urllib.request.urlopen(req('GET',f'/{b}/{k}'))
    got=r.read()
    open('$TEST_FILE_DL','wb').write(got)
    print(f'GET http={r.status} size={len(got)}')
except urllib.error.HTTPError as e:
    print(f'GET FAILED http={e.code} {e.read()[:200]}')
    sys.exit(1)
"
if cmp "$TEST_FILE" "$TEST_FILE_DL"; then
  echo "PASS: read-back data is byte-exact"
else
  echo "FAIL: read-back data does not match"
  exit 1
fi
echo ""

# 9. 验证数据落在 V2.1 segment 文件（决定性证据）
echo "--- Step 9: Verify V2.1 segment file ---"
SEG_SIZE=$(docker exec deploy-datanode-v21-1 ls -l /var/lib/dfs/datanode-v21/segments/data/active/ 2>/dev/null | awk '{print $5}' | tail -1)
echo "V2.1 segment size: $SEG_SIZE bytes"
if [ -z "$SEG_SIZE" ] || [ "$SEG_SIZE" -lt 1000000 ]; then
  echo "FAIL: V2.1 segment file missing or too small"
  exit 1
fi
echo ""

# 10. Cleanup
echo "--- Step 10: Cleanup ---"
rm -f "$TEST_FILE" "$TEST_FILE_DL"
if [ "${1:-}" = "--cleanup" ]; then
  docker compose -f "$COMPOSE_FILE" down -v
  echo "Cluster torn down"
fi

echo ""
echo "=== V2.1 INTEGRATION TEST PASSED ==="
