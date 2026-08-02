#!/bin/bash
#
# V2.1 存储引擎集成测试
#
# 启动 Docker Compose 集群（含 V2.1 数据节点），通过 S3 API 验证
# 端到端读写路径：写入一个文件 → 读回 → 验证字节精确
#
# 前置条件: docker compose, curl, awscli (或 curl + 手动签名)
# 用法: ./tests/run-v21-integration.sh

set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE_FILE="deploy/docker-compose.yml"
S3_ENDPOINT="http://localhost:8081"
ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
BUCKET="v21-integration-test"
TEST_FILE="/tmp/v21-test-file.bin"
TEST_FILE_DL="/tmp/v21-test-file-dl.bin"

echo "=== V2.1 存储引擎集成测试 ==="
echo ""

# 1. Build 镜像
echo "--- Step 1: Build Docker image ---"
docker compose -f "$COMPOSE_FILE" build 2>&1 | tail -3
echo ""

# 2. 启动集群
echo "--- Step 2: Start cluster ---"
docker compose -f "$COMPOSE_FILE" up -d metad datanode-v21 2>&1
echo ""

# 3. 等待 metad healthy
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

# 4. 等待 V2.1 datanode 注册
echo "--- Step 4: Wait for V2.1 datanode to register ---"
for i in $(seq 1 30); do
  if docker logs deploy-datanode-v21-1 2>&1 | grep -q '"TCP server listening"'; then
    echo "datanode-v21 ready after ${i}s"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "datanode-v21 failed to register"
    docker compose -f "$COMPOSE_FILE" logs datanode-v21 --tail 30
    exit 1
  fi
  sleep 1
done
echo ""

# 5. Start S3 gateway + V1 datanodes
echo "--- Step 5: Start S3 gateway ---"
docker compose -f "$COMPOSE_FILE" up -d datanode-1 datanode-2 s3 2>&1
echo ""

# 6. Wait for S3 gateway
echo "--- Step 6: Wait for S3 gateway ---"
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

# 7. Create bucket
echo "--- Step 7: Create bucket ---"
python3 -c "
import hashlib, hmac, datetime, urllib.request
ak = '$ACCESS_KEY'; sk = '$SECRET_KEY'; ep = '$S3_ENDPOINT'; b = '$BUCKET'
def sign(m, p, h, bd=b''):
    n = datetime.datetime.now(datetime.timezone.utc)
    d = n.strftime('%Y%m%dT%H%M%SZ'); ds = n.strftime('%Y%m%d')
    h['host'] = 'localhost:8081'; h['x-amz-date'] = d
    h['x-amz-content-sha256'] = hashlib.sha256(bd).hexdigest()
    ch = ''.join(f'{k.lower()}:{v.strip()}\n' for k,v in sorted(h.items()))
    sh = ';'.join(sorted(k.lower() for k in h))
    cr = f'{m}\n{p}\n\n{ch}\n{sh}\n{h[\"x-amz-content-sha256\"]}'
    cs = f'{ds}/us-east-1/s3/aws4_request'
    sts = f'AWS4-HMAC-SHA256\n{d}\n{cs}\n{hashlib.sha256(cr.encode()).hexdigest()}'
    def skf(k, m): return hmac.new(k, m.encode(), hashlib.sha256).digest()
    kd = skf(('AWS4'+sk).encode(), ds); kr = skf(kd, 'us-east-1')
    ks = skf(kr, 's3'); kg = skf(ks, 'aws4_request')
    sg = hmac.new(kg, sts.encode(), hashlib.sha256).hexdigest()
    h['Authorization'] = f'AWS4-HMAC-SHA256 Credential={ak}/{cs}, SignedHeaders={sh}, Signature={sg}'
    return h
try:
    r = urllib.request.urlopen(urllib.request.Request(f'{ep}/{b}', headers=sign('PUT', f'/{b}', {}), method='PUT'))
    print(f'Create bucket: {r.status}')
except Exception as e: print(f'Create bucket: {e}')
" 2>&1

# 8. Write and read back a test file
echo "--- Step 8: Write + read back test file ---"
dd if=/dev/urandom bs=1M count=1 of="$TEST_FILE" 2>/dev/null
echo "Generated 1MiB test file"

python3 -c "
import hashlib, hmac, datetime, urllib.request, os
ak = '$ACCESS_KEY'; sk = '$SECRET_KEY'; ep = '$S3_ENDPOINT'; b = '$BUCKET'
k = 'test-object'; tf = '$TEST_FILE'; tdl = '$TEST_FILE_DL'
def sign(m, p, h, bd=b''):
    n = datetime.datetime.now(datetime.timezone.utc)
    d = n.strftime('%Y%m%dT%H%M%SZ'); ds = n.strftime('%Y%m%d')
    h['host'] = 'localhost:8081'; h['x-amz-date'] = d
    h['x-amz-content-sha256'] = hashlib.sha256(bd).hexdigest()
    ch = ''.join(f'{k.lower()}:{v.strip()}\n' for k,v in sorted(h.items()))
    sh = ';'.join(sorted(k.lower() for k in h))
    cr = f'{m}\n{p}\n\n{ch}\n{sh}\n{h[\"x-amz-content-sha256\"]}'
    cs = f'{ds}/us-east-1/s3/aws4_request'
    sts = f'AWS4-HMAC-SHA256\n{d}\n{cs}\n{hashlib.sha256(cr.encode()).hexdigest()}'
    def skf(k, m): return hmac.new(k, m.encode(), hashlib.sha256).digest()
    kd = skf(('AWS4'+sk).encode(), ds); kr = skf(kd, 'us-east-1')
    ks = skf(kr, 's3'); kg = skf(ks, 'aws4_request')
    sg = hmac.new(kg, sts.encode(), hashlib.sha256).hexdigest()
    h['Authorization'] = f'AWS4-HMAC-SHA256 Credential={ak}/{cs}, SignedHeaders={sh}, Signature={sg}'
    return h
data = open(tf, 'rb').read()
# PUT with content-type
hs = sign('PUT', f'/{b}/{k}', {'content-type': 'application/octet-stream'}, data)
try:
    r = urllib.request.urlopen(urllib.request.Request(f'{ep}/{b}/{k}', data=data, headers=hs, method='PUT'))
    print(f'PUT object: {r.status}')
except Exception as e: print(f'PUT object: {e}')
# GET
hs = sign('GET', f'/{b}/{k}', {})
try:
    r = urllib.request.urlopen(urllib.request.Request(f'{ep}/{b}/{k}', headers=hs, method='GET'))
    open(tdl, 'wb').write(r.read())
    print(f'GET object: {r.status}, size={os.path.getsize(tdl)}')
except Exception as e: print(f'GET object: {e}')
" 2>&1

# 9. Verify byte-exact
echo "--- Step 9: Verify byte-exact ---"
if cmp "$TEST_FILE" "$TEST_FILE_DL"; then
  echo "PASS: read-back data is byte-exact"
else
  echo "FAIL: read-back data does not match"
  docker compose -f "$COMPOSE_FILE" logs datanode-v21 --tail 50
  exit 1
fi
echo ""

# 10. Cleanup
echo "--- Step 10: Cleanup ---"
rm -f "$TEST_FILE" "$TEST_FILE_DL"
# Don't tear down — let the user inspect. Use --cleanup to tear down.
if [ "${1:-}" = "--cleanup" ]; then
  docker compose -f "$COMPOSE_FILE" down -v
  echo "Cluster torn down"
fi

echo ""
echo "=== V2.1 INTEGRATION TEST PASSED ==="