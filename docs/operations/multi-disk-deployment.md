# datanode 多盘部署指南

本文档描述如何使用 `--data-dirs` 在 JBOD（Just a Bunch of Disks）模式下部署 datanode，
以及如何通过管理接口进行在线运维操作。

## 部署架构

```
物理机
├── /data1/          ← datanode 磁盘 1
│   ├── chunks/      ← chunk 数据文件
│   └── wal/         ← 预写日志（崩溃恢复）
├── /data2/          ← datanode 磁盘 2
│   ├── chunks/
│   └── wal/
└── /data3/          ← datanode 磁盘 3
    ├── chunks/
    └── wal/
```

单个 datanode 进程管理所有磁盘，写入均匀分布、单盘故障隔离。

## systemd 部署

### 配置 /etc/default/nufs-datanode

```bash
DATA_DIRS=/data1,/data2,/data3
CAPACITY_GB=1000
RACK=rack1
ZONE=zone1
```

### 服务单元 nufs-datanode@.service

```ini
[Unit]
Description=NUFS Data Node (JBOD multi-disk)
After=network.target nufs-metad.service

[Service]
Type=simple
User=nufs
Group=nufs
EnvironmentFile=-/etc/default/nufs-datanode
ExecStart=/usr/local/bin/datanode \
    --node-id=auto \
    --listen=0.0.0.0:9100 \
    --data-dirs=${DATA_DIRS} \
    --metadata=localhost:8091 \
    --rack=${RACK:-rack1} \
    --zone=${ZONE:-zone1} \
    --capacity=${CAPACITY_GB:-500} \
    --log-level=${LOG_LEVEL:-info} \
    --log-format=${LOG_FORMAT:-	ext}

Restart=always
RestartSec=5
LimitNOFILE=131072
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

### 启动

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now nufs-datanode@9100.service
```

单进程管理所有磁盘，占用 1 个端口、1 份配置、1 个 metadata 心跳。

## 磁盘初始化

每块盘在首次使用前需要创建目录：

```bash
for disk in /data1 /data2 /data3; do
    sudo mkdir -p "$disk/chunks" "$disk/wal"
    sudo chown -R nufs:nufs "$disk"
done
```

datanode 首次启动时会自动在每个 `{disk}/chunks/` 下创建 256 个分片目录（`00`~`ff`）。
无需手动创建分片目录。

## 管理接口

datanode 启动后在 `{dataDir}/.datanode.sock` 监听 unix socket，支持三个运维命令：

### 查看状态

```bash
datanode status --data-dir=/data1
```

输出示例：

```json
{
  "disks": [
    {"index": 0, "dir": "/data1", "failed": false, "chunks": 15234, "bytes": 1073741824},
    {"index": 1, "dir": "/data2", "failed": false, "chunks": 14987, "bytes": 1048576000},
    {"index": 2, "dir": "/data3", "failed": false, "chunks": 15102, "bytes": 1061109760}
  ],
  "total_chunks": 45323,
  "total_bytes": 3183431680
}
```

### 热加盘

```bash
datanode adopt /new-disk --data-dir=/data1
```

将一块新盘（或从其他节点迁移来的盘）在线加入运行中的 datanode：
- 扫描新盘上的已有 chunk 文件，合并进全局索引
- 后续写入自动分布到新盘
- 无需重启 datanode 进程

### 热摘盘

```bash
datanode retire /old-disk --data-dir=/data1
```

将一块盘标记为下线：
- 该盘不再接收新写入
- 该盘上的 chunk 可通过 EC/复制从其他节点读取
- 可在摘盘前调用 `MigrateDisk` 迁移数据到其他盘

## 数据迁移

当需要更换磁盘或均衡负载时，可在线迁移数据：

```
# 通过管理接口触发（CLI 或直接调用 ChunkStore.MigrateDisk）
datanode migrate-disk --src-index=2 --data-dir=/data1
```

迁移过程：
1. 遍历源盘上所有 chunk
2. 逐个读取 → 写入最空闲的目标盘 → 更新索引 → 删除源文件
3. 迁移期间 chunk 仍可读（迁移完成前旧文件保留）
4. 迁移完成后源盘清空，可物理移除

## 故障排查

### 单盘故障

**现象**：`datanode status` 显示某盘 `failed: true`

**原因**：连续写入错误达到阈值（默认 5 次），自动标记失败

**处理**：
1. `dmesg | grep error` 检查磁盘硬件错误
2. `smartctl -a /dev/sdX` 检查 SMART 状态
3. 如确认盘故障，执行 `datanode retire /bad-disk --data-dir=/data1`
4. 物理更换磁盘，格式化后 `datanode adopt /new-disk --data-dir=/data1`

### 写入慢

**现象**：PUT 延迟升高，`datanode status` 某盘 bytes 持续增长

**原因**：
- 磁盘接近容量上限（>90%），触发拒绝写入
- 磁盘 I/O 达到硬件极限

**处理**：
1. 检查 `datanode status` 确认各盘使用率
2. 如单盘接近满，执行 `datanode migrate-disk` 将数据迁移到空闲盘
3. 如整体容量不足，`datanode adopt /extra-disk` 增加容量

### 写入 503

**现象**：S3 PUT 返回 503 Service Unavailable

**原因**：
- 所有盘都满了（CapacityGB 限制）
- 所有盘都标记为失败
- metadata 服务不可达

**排查**：
```bash
# 检查 datanode 健康
curl http://localhost:9100/healthz

# 检查 metadata 连接
curl http://localhost:8091/healthz

# 查看 datanode 日志
journalctl -u nufs-datanode@9100 -f
```

### 进程崩溃恢复

datanode 进程崩溃后 systemd 自动重启。恢复流程：
1. `scanExisting` 扫描每个磁盘的 `chunks/` 目录，重建内存索引
2. WAL 恢复：清理未提交的写入产生的孤儿文件
3. 向 metadata 重新注册，恢复心跳
4. 恢复对客户端的服务

无需人工干预。

## 监控

关键指标（Prometheus）：

| 指标 | 说明 |
|------|------|
| `datanode_chunk_count` | 本地 chunk 总数 |
| `datanode_used_bytes` | 本地已用空间 |
| `datanode_write_error_rate` | 写入错误率（0.0-1.0） |
| `datanode_disk_state` | 磁盘状态（online/degraded/failed） |
| `datanode_fd_cache_hits` | fd 缓存命中次数 |
| `datanode_fsync_ns` | fsync 耗时 |

Grafana dashboard 已配置在 `deploy/monitoring/grafana-dashboard.json`。
告警规则在 `deploy/monitoring/alerting-rules.yaml`。
