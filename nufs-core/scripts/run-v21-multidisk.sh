#!/bin/bash
#
# V2.1 多盘（JBOD）引擎集成测试
#
# 启动 metad + datanode-v21-multi（--data-dir=/d0,/d1，multi-disk profile）+
# S3 gateway，通过 S3 API 写入多个对象，验证 multi-disk parity 门禁：
#   1. V2.1 多盘适配器真正聚合所有磁盘（least-used 放置），数据落在 /d0 与 /d1 两盘；
#   2. 端到端读写字节精确。
#
# 前置条件: docker compose, python3
# 用法: ./scripts/run-v21-multidisk.sh [--cleanup]

set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE_FILE="deploy/docker-compose.yml"
S3_ENDPOINT="http://localhost:8081"
ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
BUCKET="v21-multidisk"
MULTI_CONTAINER="deploy-datanode-v21-multi-1"
TEST_FILE="/tmp/v21md-test-object.bin"
TEST_FILE_DL="/tmp/v21md-test-object-dl.bin"

# SigV4 signing helper shared by all S3 calls.
SIGN_PY="
import hashlib, hmac, datetime, urllib.request, os, sys
ak='$ACCESS_KEY'; sk='$SECRET_KEY'; ep='$S3_ENDPOINT'; b='$BUCKET'
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

echo "=== V2.1 多盘（JBOD）引擎集成测试 ==="
echo ""

# 1. Build 镜像
echo "--- Step 1: Build Docker image ---"
docker compose -f "$COMPOSE_FILE" build 2>&1 | tail -2
echo ""

# 2. 启动 metad + 多盘 V2.1 datanode + s3
echo "--- Step 2: Start metad + datanode-v21-multi + s3 ---"
docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
docker compose -f "$COMPOSE_FILE" --profile multidisk up -d metad datanode-v21-multi s3 2>&1
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

# 4. 等待多盘 V2.1 datanode 注册并 online
echo "--- Step 4: Wait for datanode-v21-multi ---"
for i in $(seq 1 30); do
  if docker logs $MULTI_CONTAINER 2>&1 | grep -q '"TCP server listening"'; then
    echo "datanode-v21-multi ready after ${i}s"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "datanode-v21-multi failed to start"
    docker compose -f "$COMPOSE_FILE" logs datanode-v21-multi --tail 30
    exit 1
  fi
  sleep 1
done

# 确认 V2.1 多盘节点初始化并聚合了所有磁盘，且它是唯一 online 节点
# （s3 已不再 depends_on 单盘 datanode-v21，因此本次只启动多盘节点，
#  放置确定性落在它身上）。
echo "--- Confirm multi-disk initialization ---"
if ! docker logs $MULTI_CONTAINER 2>&1 | grep -q '"V2.1 storage engine ready".*"disks":2'; then
  echo "WARN: did not see disks=2 init log (non-fatal, checking segment presence later)"
fi
ONLINE=0
for i in $(seq 1 15); do
  ONLINE=$(curl -s http://localhost:8091/api/v1/nodes | python3 -c "
import json,sys
online=[n for n in json.load(sys.stdin) if n.get('state')==0]
print(len(online))
")
  if [ "$ONLINE" -eq 1 ]; then
    break
  fi
  sleep 1
done
echo "online nodes: $ONLINE (预期只有 datanode-v21-multi)"
if [ "$ONLINE" -ne 1 ]; then
  echo "FAIL: expected only the multi-disk node online for deterministic placement"
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

# 6. 创建 RF=1 bucket（单个 V2.1 多盘节点即可满足）
echo "--- Step 6: Create RF=1 bucket ---"
CREATE_BUCKET=$(curl -s -X POST http://localhost:8091/api/v1/buckets \
  -H "Content-Type: application/json" \
  -d '{"name":"'$BUCKET'","policy":{"replication_factor":1}}' -w "|%{http_code}")
CODE="${CREATE_BUCKET##*|}"
echo "create bucket http_code=$CODE"
if [ "$CODE" != "201" ] && [ "$CODE" != "500" ]; then
  echo "unexpected create bucket status: $CREATE_BUCKET"
  exit 1
fi
echo ""

# 7. 写入多个对象（least-used 放置会逐步填满两盘）
echo "--- Step 7: S3 PUT 4 x 1MiB ---"
dd if=/dev/urandom bs=1M count=1 of="$TEST_FILE" 2>/dev/null
for i in 1 2 3 4; do
  OBJ="obj-$i.bin"
  python3 -c "$SIGN_PY
import sys
k='$OBJ'
data=open('$TEST_FILE','rb').read()
try:
    r=urllib.request.urlopen(req('PUT',f'/{b}/{k}',bd=data,extra={'content-type':'application/octet-stream','x-owner':ak}))
    print(f'PUT $OBJ http={r.status} size={len(data)}')
    sys.exit(0 if r.status==200 else 1)
except urllib.error.HTTPError as e:
    print(f'PUT $OBJ FAILED http={e.code} {e.read()[:200]}')
    sys.exit(1)
" || { echo "FAIL: PUT $OBJ"; exit 1; }
done
echo ""

# 8. 逐对象 S3 GET + 验证字节精确
echo "--- Step 8: S3 GET + verify ---"
for i in 1 2 3 4; do
  OBJ="obj-$i.bin"
  python3 -c "$SIGN_PY
import sys
k='$OBJ'
try:
    r=urllib.request.urlopen(req('GET',f'/{b}/{k}'))
    got=r.read()
    open('$TEST_FILE_DL','wb').write(got)
except urllib.error.HTTPError as e:
    print(f'GET $OBJ FAILED http={e.code} {e.read()[:200]}')
    sys.exit(1)
" || { echo "FAIL: GET $OBJ"; exit 1; }
  if cmp "$TEST_FILE" "$TEST_FILE_DL"; then
    echo "$OBJ read-back byte-exact"
  else
    echo "FAIL: $OBJ read-back does not match"
    exit 1
  fi
done
echo ""

# 9. 验证数据落在两盘子目录（multi-disk parity 决定性证据）
echo "--- Step 9: Verify data landed on BOTH disks ---"
for d in d0 d1; do
  SEG=$(docker exec $MULTI_CONTAINER find /var/lib/dfs/$d/segments/data/active/ -name '*.seg' 2>/dev/null | wc -l | tr -d ' ')
  echo "disk $d active segments: $SEG"
  if [ "$SEG" -lt 1 ]; then
    echo "FAIL: no segments on $d — data did not spread across disks"
    exit 1
  fi
done
echo ""

# 10. Cleanup
echo "--- Step 10: Cleanup ---"
rm -f "$TEST_FILE" "$TEST_FILE_DL"
if [ "${1:-}" = "--cleanup" ]; then
  docker compose -f "$COMPOSE_FILE" --profile multidisk down -v
  echo "Cluster torn down"
fi

echo ""
echo "=== V2.1 MULTI-DISK INTEGRATION TEST PASSED ==="
