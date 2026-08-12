# NUFS 测试环境执行指南

## 前置条件

- Linux 机器（或 Docker）
- Go 1.25+ 已安装
- Docker（用于网络故障注入测试）
- 至少一个 datanode + 一个 metad 实例（本地开发集群）

---

## 一、快速启动本地开发集群

### 1. 启动 metad

```bash
cd nufs-core
go build -o /tmp/metad ./cmd/metad/
/tmp/metad --data-dir=/tmp/nufs-metad --listen=:19090 --raft=:19091
```

### 2. 启动 datanode

```bash
go build -o /tmp/datanode-server ./cmd/datanode-server/
/tmp/datanode-server --listen=:19092 --data-dir=/tmp/nufs-data --metad=localhost:19090 --node-id=1
```

### 3. 创建 bucket（通过 metad API）

```bash
curl -X POST http://localhost:19090/api/v1/buckets \
  -H 'Content-Type: application/json' \
  -d '{"name":"test-bucket","policy":{"replication_factor":1}}'
```

### 4. 挂载 FUSE

```bash
go build -o /tmp/nufs-fuse ./cmd/nufs-fuse/
mkdir -p /mnt/nufs
/tmp/nufs-fuse --bucket=test-bucket --metad=localhost:19090 --mountpoint=/mnt/nufs
```

---

## 二、运行测试

### POSIX 合规测试

```bash
# 在挂载完成后运行
./scripts/soak/fuse-posix-test.sh /mnt/nufs/test-bucket
```

预期输出：
```
=== File operations ===
create and read file                              PASS
overwrite file                                    PASS
...
=== Permissions ===
chmod 0644                                        PASS
...
================================
Results: 42 passed, 0 failed (42 total)
================================
All POSIX compliance tests passed.
```

### Go 单元测试（不需要挂载）

```bash
cd nufs-core

# 元数据层测试（crash+replay、GC、tombstone）
go test ./metadata/ -run 'TestCrashReplay|TestChunkGC' -v -count=1

# FUSE 层测试（需要 Linux，用 Docker）
docker run --rm -v $(pwd):/src -w /src \
  -v $(go env GOMODCACHE):/go/pkg/mod:ro \
  -e GOPROXY=off \
  golang:1.25 \
  go test ./gateway/fuse/... -count=1

# S3 网关测试
go test ./gateway/s3/... -count=1
```

### 网络故障注入测试（需要 Docker）

```bash
# 启动本地 dev cluster 后运行
./scripts/soak/run-v21-network-faults.sh

# 测试内容：
# - Scenario 1: 网络延迟注入（200ms delay，60秒）
# - Scenario 2: 网络丢包注入（30% loss，60秒）
# - Scenario 3: 网络延迟+丢包组合（20% loss + 50ms delay，60秒）
#
# 每个 scenario 都会：
# 1. 启动 netem sidecar（带 CAP_NET_ADMIN）
# 2. 注入故障
# 3. 运行读写负载
# 4. 清理 netem（显式 reset + docker stop）
# 5. 验证数据完整性
```

---

## 三、测试项说明

### POSIX 合规测试（fuse-posix-test.sh）

| 类别 | 测试项 | 关注点 |
|------|--------|--------|
| 文件操作 | create/read/overwrite/append/delete | 基础 CRUD |
| 截断 | truncate down/up/zero/fallocate | 文件大小变更 |
| 部分覆写 | 中间写入保留尾部 | **截断 bug 回归** |
| 目录 | mkdir/rmdir/readdir | 目录操作 |
| 权限 | chmod/chown/sticky/setgid | POSIX 权限模型 |
| Symlink | create/readlink/stat follows | 符号链接语义 |
| Hard link | unlink→link survives/shared data/nlink | 硬链接引用计数 |
| 元数据 | mtime/file size | 属性更新 |
| 并发 | 5 进程并行 append | 并发安全 |
| 大文件 | 10 MiB 顺序写/1 MiB 随机写 | I/O 正确性 |

### Crash+Replay 测试（Go 单元测试）

验证崩溃恢复场景：
- 分配 chunk 但不更新 inode → GC 清理孤儿
- WriteAttempt 台账记录状态转换
- 不需要实际杀进程，模拟元数据层的崩溃状态

---

## 四、完整端到端演练

以下是一个完整的端到端测试流程，覆盖正常写入 → 网络故障 → 恢复：

```bash
#!/bin/bash
# e2e-drill.sh — 完整端到端演练
set -e

# 1. 启动集群
echo ">>> 启动 metad + datanode"
/tmp/metad --data-dir=/tmp/nufs-metad --listen=:19090 &
METAD_PID=$!
sleep 1

/tmp/datanode-server --listen=:19092 --data-dir=/tmp/nufs-data \
  --metad=localhost:19090 --node-id=1 &
DATANODE_PID=$!
sleep 1

# 2. 创建 bucket + 挂载
curl -s -X POST http://localhost:19090/api/v1/buckets \
  -H 'Content-Type: application/json' \
  -d '{"name":"drill-bucket","policy":{"replication_factor":1}}' > /dev/null

/tmp/nufs-fuse --bucket=drill-bucket --metad=localhost:19090 \
  --mountpoint=/mnt/nufs &
FUSE_PID=$!
sleep 2

# 3. 写入测试数据
echo ">>> 写入测试数据"
dd if=/dev/urandom of=/mnt/nufs/drill-bucket/data.bin bs=1M count=10 2>/dev/null
MD5=$(md5sum /mnt/nufs/drill-bucket/data.bin | awk '{print $1}')
echo "原始 MD5: $MD5"

# 4. 运行 POSIX 合规测试
echo ">>> 运行 POSIX 测试"
./scripts/soak/fuse-posix-test.sh /mnt/nufs/drill-bucket

# 5. 注入网络故障
echo ">>> 注入网络故障（5秒）"
docker run --rm --cap-add=NET_ADMIN --net container:<datanode-container> \
  nufs-netem:latest losslat 30 100ms 5 &
NETEM_PID=$!
sleep 6
kill $NETEM_PID 2>/dev/null || true

# 6. 故障后验证数据完整性
echo ">>> 验证数据完整性"
POST_MD5=$(md5sum /mnt/nufs/drill-bucket/data.bin | awk '{print $1}')
if [ "$MD5" = "$POST_MD5" ]; then
  echo "数据完整性验证通过"
else
  echo "数据完整性验证失败！原始=$MD5 故障后=$POST_MD5"
  exit 1
fi

# 7. 清理
echo ">>> 清理"
kill $FUSE_PID $DATANODE_PID $METAD_PID 2>/dev/null || true
umount /mnt/nufs 2>/dev/null || true
echo "演练完成"
```

---

## 五、常见问题

### Q: 挂载时报 `fusermount: mount failed: Operation not permitted`
A: 需要 root 权限或 `/dev/fuse` 权限。检查 `ls -la /dev/fuse`。

### Q: POSIX 测试中 `chmod` 测试失败
A: FUSE 网关需要实现 `Setattr` 的权限位持久化。检查 `s3fs/file.go` 的 `Setattr` 实现。

### Q: 网络故障注入后数据校验失败
A: 检查 `ReliabilityWrapper` 的重试配置。默认重试 3 次，间隔 100ms。网络故障持续时间不应超过重试预算。

### Q: Go 测试在 macOS 上运行失败
A: FUSE 测试需要 Linux（`//go:build linux`）。用 Docker：
```bash
docker run --rm -v $(pwd):/src -w /src golang:1.25 \
  go test ./gateway/fuse/... -count=1
```
