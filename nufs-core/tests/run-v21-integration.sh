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
S3_ENDPOINT="http://localhost:8080"
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
  NODES=$(curl -sf http://localhost:8091/api/v1/nodes 2>/dev/null || echo "[]")
  if echo "$NODES" | grep -q "online"; then
    echo "datanode-v21 registered after ${i}s"
    echo "$NODES" | head -5
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
  if curl -sf http://localhost:8080/healthz > /dev/null 2>&1; then
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
# Use AWS CLI if available, otherwise curl with presigned
if command -v aws &>/dev/null; then
  aws --endpoint-url "$S3_ENDPOINT" s3api create-bucket --bucket "$BUCKET" 2>&1 || \
  aws --endpoint-url "$S3_ENDPOINT" s3 mb "s3://$BUCKET" 2>&1
else
  # Fallback: use curl with S3 presigned URL via the gateway
  curl -X PUT "$S3_ENDPOINT/$BUCKET" -H "x-amz-acl: private" -w "%{http_code}" -o /dev/null
fi
echo ""

# 8. Write and read back a test file
echo "--- Step 8: Write + read back test file ---"
dd if=/dev/urandom bs=1M count=1 of="$TEST_FILE" 2>/dev/null
echo "Generated 1MiB test file"

if command -v aws &>/dev/null; then
  aws --endpoint-url "$S3_ENDPOINT" s3 cp "$TEST_FILE" "s3://$BUCKET/test-object" 2>&1
  aws --endpoint-url "$S3_ENDPOINT" s3 cp "s3://$BUCKET/test-object" "$TEST_FILE_DL" 2>&1
else
  curl -X PUT -T "$TEST_FILE" "$S3_ENDPOINT/$BUCKET/test-object" -w "%{http_code}" -o /dev/null
  curl -o "$TEST_FILE_DL" "$S3_ENDPOINT/$BUCKET/test-object" -w "%{http_code}"
fi
echo ""

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