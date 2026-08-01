# NUFS 生产运维 Runbook

> 最后更新：2026-07-31
> 指标前缀：`nufs_`（对外） / `metad_`（内部）
> 告警规则：`deploy/monitoring/alerting-rules.yaml`
> Grafana 面板：`deploy/monitoring/grafana-dashboard.json` (uid: `nufs-production`)

---

## 一、集群就绪状态（ClusterReadiness）

### 状态定义

| 状态 | 含义 | 可写？ |
|------|------|--------|
| `ready` (2) | 所有检查通过 | ✅ |
| `degraded` (1) | 部分检查异常，服务可用但性能下降 | ✅（可能降级） |
| `not_ready` (0) | 关键检查失败，不建议写入 | ❌ |

### 检查项

| 检查项 | 键名 | 正常值 | 降级条件 |
|--------|------|--------|----------|
| Quorum | `quorum` | `ok` | `在线节点 < (总节点/2)+1` |
| Leader | `leader` | `stable` / `standalone` | 无 leader 地址 |
| 降级状态 | `degradation` | `normal` | PebbleStore 进入 ReadOnly/Unavailable |
| 副本健康 | `replication` | `ok` | 存在副本不足的 chunk |
| 修复队列 | `repair_queue` | `ok` | 队列深度 > `ReadinessRepairQueueThreshold`（默认 1000） |

### API

```bash
# 直接查询 metad
curl http://<metad>:9100/api/v1/cluster/readiness | jq .

# 通过 admin proxy（自动跟随 leader）
curl http://<admin>:8080/api/v1/clusters/<id>/readiness | jq .
```

### 典型场景处理

**场景：quorum 报 "at risk"**
1. 检查节点心跳：`GET /api/v1/nodes`，确认哪些节点 `state != online`
2. 检查节点进程是否存活、网络是否可达
3. 如节点不可恢复，使用 `POST /api/v1/nodes/{id}/decommission` 标记下线
4. Quorum 恢复后会自动变为 `ok`

**场景：leader 报 "no leader"**
1. 检查 Raft 状态：`metad_raft_info` 指标，确认各节点 term/log_index
2. 如集群只剩 1 节点（单节点模式），`standalone` 是正常值
3. 多节点集群无 leader：检查网络分区、Raft 日志是否卡住

**场景：replication 报副本不足**
1. 查看 `nufs_cluster_chunks_under_replicated` 数值
2. 等待自动修复（`auto_repair_enabled=true` 时自动触发）
3. 修复队列积压：检查 datanode 是否在线、网络是否通畅

---

## 二、DynamicConfig 运行时调参

所有字段可通过 `SetDynamicConfig` API 运行时热更新，无需重启。

### 调用方式

```bash
curl -X PUT http://<metad>:9100/api/v1/config \
  -H 'Content-Type: application/json' \
  -d '{"placement_error_rate_filter": 0.5}'
```

### 可调字段速查

| JSON 字段 | 默认值 | 说明 | 调优建议 |
|-----------|--------|------|----------|
| **放置策略** | | | |
| `placement_error_rate_filter` | `0.8` | 写错误率高于此值的节点被排除出放置 | 故障排查时可临时降至 0.5；恢复后回调 |
| `placement_weight_capacity` | `0.4` | 容量权重 | 容量不均时提高至 0.5+ |
| `placement_weight_load` | `0.25` | 负载权重 | 负载热点明显时提高 |
| `placement_weight_tier` | `0.2` | 存储层匹配权重 | 多层存储时可调高 |
| `placement_weight_health` | `0.15` | 健康度权重（含错误率） | 节点质量参差时调高 |
| **就绪阈值** | | | |
| `readiness_repair_queue_threshold` | `1000` | 修复队列超过此值 → degraded | 业务低峰可调低，高峰可调高 |
| **GC** | | | |
| `gc_enabled` | `true` | GC 总开关 | 排查问题时临时关闭 |
| `gc_interval` | `15m` | GC 扫描间隔 | 存储压力大时调短 |
| `gc_chunk_batch_size` | `1000` | 每批扫描 chunk 数 | |
| `gc_dry_run` | `false` | 只扫描不删除 | 上线前先开 dry_run 验证 |
| **心跳** | | | |
| `heartbeat_ttl_seconds` | `30` | 节点多久未心跳标记离线 | 网络抖动多时可调高 |
| `auto_repair_enabled` | `true` | 自动触发 chunk 修复 | |
| **缓存** | | | |
| `cache_enabled` | `true` | 元数据缓存 | |
| `cache_max_size` | `65536` | 最大缓存条目数 | 内存充足时可调高 |
| **写批处理** | | | |
| `write_batching_enabled` | `true` | 写批处理 | |
| `write_batch_max_size` | `256` | 每批最大条目 | |
| `write_batch_max_wait` | `50ms` | 批处理最大等待 | |

---

## 三、告警规则速查

### Critical 告警（立即处理）

| 告警名 | 触发条件 | 持续 | 处理 |
|--------|----------|------|------|
| `NUFSClusterQuorumAtRisk` | online/total < 0.66 | 2m | 检查节点进程、网络；必要时 decommission 故障节点 |
| `NUFSDiskUsageCritical` | 磁盘使用 > 95% | 5m | 扩容或迁移数据；检查 GC 是否正常运行 |
| `NUFSDiskFailed` | 磁盘标记 failed | 1m | 更换磁盘；节点自动进入降级模式 |
| `NUFSNodeWriteErrorRateCritical` | 节点写错误率 > 50% | 3m | 检查 datanode 日志；磁盘/网络问题；节点会被放置引擎自动排除 |
| `NUFSClusterReadinessDegraded` | readiness < 2 | 2m | 见"集群就绪状态"场景处理 |
| `NUFSClusterLeaderUnstable` | leader_stable == 0 | 2m | 检查 Raft 节点状态；可能网络分区 |
| `NUFSObjectWriteFailuresPersisting` | 失败写入 > 0 | 5m | 检查 S3 gateway → metad 链路 |
| `NUFSObjectWriteWorkerDeadLetter` | dead_letter > 0 | 1m | 检查后台任务日志 |
| `NUFSBucketQuotaBytesCritical` | 字节配额 > 95% | 5m | 清理数据或扩大配额 |
| `NUFSBucketQuotaObjectsCritical` | 对象配额 > 95% | 5m | 清理对象或扩大配额 |

### Warning 告警（30 分钟内处理）

| 告警名 | 触发条件 | 持续 | 处理 |
|--------|----------|------|------|
| `NUFSDiskUsageHigh` | 磁盘使用 > 85% | 30m | 规划扩容 |
| `NUFSNodeOffline` | 离线节点 > 20% | 5m | 检查节点状态 |
| `NUFSNodeWriteErrorRateHigh` | 写错误率 > 20% | 5m | 关注，可能恶化为 critical |
| `NUFSClusterUnderReplicated` | 副本不足 chunk > 0 | 15m | 等待自动修复或手动触发 |
| `NUFSErrorRateHigh` | 错误率 > 0 | 10m | 查日志定位根因 |
| `NUFSMetadReadLatencyHigh` | 读 P99 > 10ms | 10m | 检查缓存命中率、磁盘 I/O |
| `NUFSMetadWriteLatencyHigh` | 写 P99 > 50ms | 10m | 检查 Raft 提交延迟、WAL fsync |
| `NUFSBackupStale` | 备份 > 75min 未成功 | 5m | 检查备份任务日志 |
| `NUFSBackupVerificationFailed` | 备份校验失败 | 1m | 检查备份完整性 |
| `NUFSChunkTombstoneBacklog` | 最老 tombstone > 26h | 30m | GC 可能卡住 |
| `NUFSBucketQuotaBytesHigh` | 字节配额 > 80% | 15m | 预警，规划扩容 |
| `NUFSBucketQuotaObjectsHigh` | 对象配额 > 80% | 15m | 预警 |

---

## 四、Admin Proxy 故障排查

### 307 Redirect 跟随

Admin proxy 自动跟随 leader 307 重定向（最多 5 跳）。遇到写请求被拒绝时：

1. 检查 admin proxy 日志中的 `redirect` 记录
2. 确认 metad 集群有稳定 leader
3. 如果重定向循环，检查 `advertise_ops_addr` 配置是否正确

### Retry 机制

- 5xx / 429 → 自动重试 3 次，指数退避（base 200ms）
- 4xx → 不重试，直接返回错误
- 可通过 `WithMaxRetries(n)` / `WithRetryBaseDelay(d)` 调整

### CheckHealth 307

`CheckHealth` 将 307 视为健康（follower 知道 leader 地址）。只有连接失败或 5xx 才标记不健康。

---

## 五、Prometheus 指标速查

### 集群级（无 label）

| 指标 | 类型 | 说明 |
|------|------|------|
| `nufs_cluster_readiness` | gauge | 0=not_ready, 1=degraded, 2=ready |
| `nufs_cluster_can_write_rf` | gauge | 可支撑的最大 RF |
| `nufs_cluster_chunks_under_replicated` | gauge | 副本不足的 chunk 数 |
| `nufs_cluster_leader_stable` | gauge | 0/1 |
| `metad_nodes_online` / `metad_nodes_total` | gauge | 节点计数 |
| `metad_repair_tasks_queued` | gauge | 修复队列深度 |

### 节点级（label: `node_id`）

| 指标 | 说明 |
|------|------|
| `nufs_node_write_error_rate` | 滚动写错误率 0.0–1.0 |
| `nufs_node_disk_io` | 磁盘 I/O 利用率 0.0–1.0 |
| `nufs_node_capacity_bytes` | 总容量（字节） |
| `nufs_node_used_bytes` | 已用（字节） |

### Bucket 配额（label: `bucket`, `resource`）

| 指标 | 说明 |
|------|------|
| `nufs_bucket_quota_limit` | 配额上限 |
| `nufs_bucket_quota_usage` | 当前用量 |
| `nufs_bucket_quota_used_ratio` | 使用率 0.0–1.0 |

### 性能（label: `quantile`）

| 指标 | 说明 |
|------|------|
| `metad_read_latency_us{quantile="0.99"}` | 读 P99 延迟（微秒） |
| `metad_write_latency_us{quantile="0.99"}` | 写 P99 延迟（微秒） |
| `rate(metad_errors_total[5m])` | 全局错误率 |

---

## 六、常见操作手册

### 手动触发修复

```bash
curl -X POST http://<metad>:9100/api/v1/repair/trigger
```

### 手动触发重平衡

```bash
curl -X POST http://<metad>:9100/api/v1/rebalance/trigger
```

### 下线节点

```bash
curl -X POST http://<metad>:9100/api/v1/nodes/{node_id}/decommission
```

### 查看 Bucket 配额

```bash
curl http://<metad>:9100/api/v1/buckets/{name} | jq '.quota'
```

### 更新 DynamicConfig（示例：降低错误率过滤阈值）

```bash
curl -X PUT http://<metad>:9100/api/v1/config \
  -H 'Content-Type: application/json' \
  -d '{"placement_error_rate_filter": 0.5}'
```

### Grafana 面板导入

1. 打开 Grafana → Dashboards → Import
2. 上传 `deploy/monitoring/grafana-dashboard.json`
3. 选择 Prometheus 数据源
4. 面板 uid: `nufs-production`

---

## 七、JBOD 多盘运维

> datanode 支持 `--data-dirs=/d1,/d2,/d3` 多盘 JBOD 模式，单进程管理多块磁盘。

### 部署要点

```bash
# /etc/default/nufs-datanode
DATA_DIRS=/data1,/data2,/data3
CAPACITY_GB=1000
RACK=rack1
ZONE=zone1
```

每个磁盘独立存储 WAL + chunks，写入均匀分布（least-used），单盘故障不影响其他盘。

### 管理命令

```bash
# 查看每盘状态
datanode status --data-dir=/data1

# 热加盘（新盘或从其他节点迁移来的盘）
datanode adopt /new-disk --data-dir=/data1

# 热摘盘（标记 failed，停止写入）
datanode retire /old-disk --data-dir=/data1

# 手动迁移（摘盘前将数据搬到其他盘）
# 通过 ChunkStore.MigrateDisk(srcIdx) 或管理接口触发
```

### 常见故障场景

#### 场景 1：单盘故障

**告警**：`datanode_disk_state = failed`

**排查**：
```bash
# 1. 确认哪块盘故障
datanode status --data-dir=/data1
# 查看 failed: true 的磁盘

# 2. 检查磁盘硬件
dmesg | grep -i error | tail -20
smartctl -a /dev/sdX

# 3. 如果磁盘物理故障
datanode retire /failed-disk --data-dir=/data1
# 更换磁盘后
datanode adopt /new-disk --data-dir=/data1
```

**影响**：写入不落故障盘，读取通过 EC/复制从其他盘恢复。无数据丢失。

#### 场景 2：磁盘空间不足

**告警**：`datanode_disk_state = degraded` 或写入 503

**排查**：
```bash
datanode status --data-dir=/data1
# 查看各盘 bytes / capacity 比值

# 如果单盘快满，迁移到空闲盘
# ChunkStore.MigrateDisk(srcIdx)
```

#### 场景 3：磁盘 I/O 慢导致写入延迟升高

**现象**：PUT 延迟升高，但未触发失败阈值

**排查**：
```bash
# 检查 I/O 指标
iostat -x 1

# 检查 datanode 指标
curl http://localhost:8091/api/v1/perf
```

**处理**：如持续慢，手动 `retire` 该盘 + `adopt` 备用盘，物理检查慢盘。

#### 场景 4：热加盘后分布不均

**原因**：新盘 empty 时所有写入都涌向新盘

**处理**：
- 正常行为：least-used 策略会在新盘追上后自动均衡
- 如需立即均衡：对新盘上的部分 chunk 手动 `MigrateChunk` 到老盘

### 指标（JBOD 特有）

| 指标 | 说明 |
|------|------|
| `datanode_disk_state` | 按盘显示 online/degraded/failed |
| `datanode_disk_chunks` | 按盘的 chunk 数量（heartbeat 上报） |
| `datanode_disk_bytes` | 按盘的使用字节数（heartbeat 上报） |
| `datanode_write_error_rate` | 全局写入错误率 |

per-disk 指标通过 heartbeat 的 `DiskStats` 字段上报到 metadata。

### 从旧版 supervisor 模式迁移

如果之前使用 `--data-dirs` 的 supervisor 模式（每盘一个子进程）：

1. 停止 supervisor 服务
2. 对每个磁盘目录，确认数据目录结构一致（`{dir}/chunks/` + `{dir}/wal/`）
3. 以 JBOD 模式启动：`--data-dirs=/d1,/d2,/d3`
4. 新进程会自动扫描所有盘并重建内存索引
5. 向 metadata 重新注册（旧的 N 个 NodeID 变为 1 个）

**注意**：迁移期间旧的子进程 NodeID 会过期被摘除，新的 JBOD NodeID 注册后接管。

### 生产安全校验

metad 启动时自动校验：

```bash
# 无 token / 无 TLS / 单节点 → 启动失败
metad --data-dir=/var/lib/dfs/metadata
# Error: production config validation failed:
#   - production JWT secret is empty or uses a dev default
#   - production Raft requires at least 3 nodes
#   - production TLS is required

# 开发环境跳过校验
metad --data-dir=/tmp/test --allow-insecure-dev --raft=false
```

必须满足：JWT token 非空 + TLS 启用 + Raft ≥ 3 节点。`--allow-insecure-dev` 可跳过。

### nufs-cli 远程管理（不需要 SSH）

```bash
# 集群状态
nufs-cli nodes
nufs-cli balance

# 磁盘管理（--node 指定目标 datanode 地址）
nufs-cli disk status --node=10.0.0.1:8091
nufs-cli disk adopt /new-disk --node=10.0.0.1:8091
nufs-cli disk retire /bad-disk --node=10.0.0.1:8091
nufs-cli disk decommission /old-disk --node=10.0.0.1:8091
nufs-cli disk drain --node=10.0.0.1:8091
nufs-cli disk verify /disk1 --node=10.0.0.1:8091

# Raft 领导权切换（维护前切换 leader）
nufs-cli leader              # 查看当前 leader
nufs-cli leader meta-2       # 切换到 meta-2（~1-2s，期间写请求重试）
```

### 滚动重启流程

```bash
# 1. 切换 Raft leader 到非目标节点
nufs-cli leader meta-2

# 2. drain 目标节点（停写，等进行中写完成）
nufs-cli disk drain --node=10.0.0.1:8091

# 3. 安全重启
ssh target-node
systemctl restart nufs-datanode

# 4. 验证恢复
nufs-cli disk status --node=10.0.0.1:8091
# 确认所有盘 online + chunks 数稳定
```
