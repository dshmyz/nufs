# NUFS 监控与告警手册（Monitoring & Alerting）

本文档说明 NUFS 的**可观测性**如何配置与使用：Prometheus 抓什么、指标分哪些命名空间、
告警规则覆盖哪些故障、如何手工查询、以及「指标-告警」一致性门禁如何在 `make verify`
里保证规则不是死链。配套「上线验收清单 E3」。

> 快速入口：想用 `nufs-cli` 查文件元数据 / KV / 运维状态，看 [ops-cli.md](ops-cli.md)。
> 本文只讲监控指标与告警本身的语义和运维。

---

## 1. 抓取拓扑

NUFS 三个组件都暴露 Prometheus **文本格式**的 `/metrics` 端点（非 client_golang，是
手写文本 exposition，见 `metadata/prometheus.go` / `cmd/metad/ops_prometheus.go` /
`datanode/ops.go`）。

| 组件 | `/metrics` 端点 | 认证 | 说明 |
|------|-----------------|------|------|
| `metad` | ops HTTP 地址 `:8091/metrics` | **public（免鉴权）** | 元数据面：命名空间/raft/GC/备份/配额/cluster |
| `datanode` | ops HTTP 地址 `:8091/metrics`（注意：不是 `--listen` 的 chunk TCP） | **public（免鉴权）** | 数据面：disk/读写/复制/修复/anto-entropy |
| `nufs-s3` | 见其 HTTP `/metrics` | public | 网关层（如启用） |

> 为什么 `/metrics` 放在 public 白名单：Prometheus 抓取无需 token，与
> `--auth-token` 保护的运维 API（如 `/api/v1/kv`）区分开。若 cluster 网络不可信，
> 请用 `--metrics` 单独监听内网口，或在前置反向代理加抓取鉴权。

两套部署方式的抓取配置：

- **Docker / compose**：`deploy/monitoring/prometheus.yml`（`metad:8091`、`datanode-1:9100`、
  `datanode-2:9101`，实为 compose 暴露的 ops/listen 端口映射）。
- **Kubernetes / helm**：`deploy/helm/nufs/templates/servicemonitor.yaml` 生成两个
  `ServiceMonitor`（metad 走 `http` 端口、datanode 走 `ops` 端口，path `/metrics`），
  配 `values.yaml` 的 `observability.prometheus.serviceMonitor` 开关。

一套抓取配置的骨架（`prometheus.yml`）：

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
rule_files:
  - "alerting-rules.yaml"
scrape_configs:
  - job_name: "metad"
    static_configs: [ { targets: ["metad:8091"] } ]
    metrics_path: /metrics
  - job_name: "datanode-1"
    static_configs: [ { targets: ["datanode-1:9100"] } ]
    metrics_path: /metrics
```

---

## 2. 指标命名空间

所有指标以 `nufs_`（集群统一）或 `metad_`/`datanode_`（少数 legacy）为前缀。按主题分类：

### 2.1 集群 / 健康（metad `ops_prometheus.go`）
| 指标 | 含义 |
|------|------|
| `nufs_cluster_readiness` | 集群就绪度（0/1/2，见告警）；`< 2` 告警 |
| `nufs_cluster_leader_stable` | 1=leader 稳定，0=选举中/翻转 |
| `nufs_nodes_total` / `nufs_nodes_online` | 节点总数 / 在线数 |
| `nufs_cluster_chunks_under_replicated` | 副本不足的 chunk 数 |
| `nufs_cluster_can_write_rf` | 当前能否按 RF 写入 |
| `nufs_raft_state` / `nufs_raft_term` / `nufs_raft_log_index` | Raft 状态 / 任期 / 日志 index |
| `nufs_leader_stable` 相关 | leader 稳定性衡量 |

### 2.2 元数据面（metad 核心 `metadata/prometheus.go`）
`nufs_read_ops_total` / `nufs_write_ops_total` / `nufs_errors_total`、
`nufs_read_latency_us{quantile}` / `nufs_write_latency_us{quantile}`（P50/P90/P99）、
`nufs_keys_total` / `nufs_buckets_total` / `nufs_chunks_total`、
`nufs_gc_{scanned_chunks,deleted_chunks,orphan_chunks}`、`nufs_wal_*`。

> **结构说明**：`metad_`（legacy，如 `metad_read_latency_us`、`metad_ops_total`）与
> `nufs_` 系并存；新告警/SLO 统一引用 `nufs_` 系，避免命名漂移。

### 2.3 数据面（datanode `ops.go`）
| 指标 | 含义 |
|------|------|
| `nufs_disk_capacity/used/available_bytes` | 磁盘容量（**gauge**，供使用率告警） |
| `nufs_disk_state{state="degraded|failed"}` | 磁盘健康状态计数 |
| `nufs_node_used_bytes` / `nufs_node_write_error_rate` | 节点用量 / 写错误率 |
| `nufs_datanode_read_*_bytes_total` / `write_*_bytes_total` | 读写字节（read amplification 可算） |
| `nufs_datanode_replication_writes_total` / `errors_total` | 复制 / 修复 |
| `nufs_datanode_antientropy_mismatches/repaired/scanned_total` | 反熵扫描（V1） |
| `nufs_datanode_fd_cache_*` / `fsync_total` / `semaphore_wait_seconds_total` | 缓存 / fsync / 信号量 |

> 这些是**纯计数/gauge**（无 `_bucket`/`_sum` 直方图）。需要 P99 时要么用
> **max_over_time**，要么接受「计数+速率」语义；不要对它们用 `histogram_quantile()`。

### 2.4 业务生命周期（备份 / 配额 / 对象写 / 事件）
| 命名空间 | 代表指标 |
|----------|----------|
| `nufs_backup_*` | `last_success_timestamp_seconds`、`runs_total`、`verification_failures_total`、`artifact_bytes`、`active` |
| `nufs_bucket_quota_*` | `limit`、`usage`、`used_ratio{resource="bytes"|"objects"}` |
| `nufs_object_write_attempts{state=...}` | 写恢复：pending/committed/failed/recovery_needed |
| `nufs_object_write_background_task_*` | 后台写 GC/恢复 worker 状态、`dead_letter`、stale |
| `nufs_events_published/dropped_total`、`watcher_count` | EventBus 丢事件可观测 |
| `nufs_chunk_tombstone_*` | 墓碑 backlog / 最老年龄 |
| `nufs_repair_tasks_queued`、`nufs_repair_oldest_timestamp` | **修复队列深度 / 最老任务**（运维关键，`ops_prometheus.go` 挂出） |
| `nufs_restore_verification_*` | 备份还原校验 |

---

## 3. 告警规则（`deploy/monitoring/alerting-rules.yaml`）

29 条规则分三组，严重度递减（当前计数：11 / 17 / 1）：

| 组 | 规则数 | 举例 |
|----|-------|------|
| `nufs.critical` | 11 | `NUFSClusterQuorumAtRisk`、`NUFSDiskUsageCritical`、`NUFSDiskFailed`、`NUFSClusterReadinessDegraded`、`NUFSClusterLeaderUnstable`、`NUFSClusterUnderReplicated`、`NUFSObjectWriteFailuresPersisting`、`NUFSBucketQuotaBytesCritical`、`NUFSBackupStale`、`NufsBackupVerificationFailed` |
| `nufs.warning` | 17 | `NUFSDiskUsageHigh`、`NUFSNodeOffline`、`NUFSMetadReadLatencyHigh`、`NUFSMetadWriteLatencyHigh`、`NUFSDatanodeReplicationErrors`、`NUFSNodeWriteErrorRateHigh`、`NUFSObjectWriteRecoveryBacklog`、`NUFSBucketQuotaBytesHigh`、`NufsEventsDropped`、`NUFSDiskDegraded` |
| `nufs.info` | 1 | `NUFSDiskDegraded`（磁盘降级提示，等待恢复） |

> 完整清单见 yaml 本身；规则语义与 SLO 对齐的另一副本在 `internal/slo/slo.go`（描述性
> registry，声明 SLI 表达式 + AlertRules 供审计）。

---

## 4. Grafana

`deploy/monitoring/` 提供开箱即用的 Grafana 侧：

- `grafana-datasource.yaml` — Prometheus 数据源
- `grafana-dashboard-provider.yaml` — 面板提供者（自动加载面板目录）
- `grafana-dashboard.json` — 集群监控面板（节点/磁盘/读写/复制/备份/配额等）

把这三件放入 Grafana provisioning 目录即可。看板参照 §2 的指标命名空间编排视图。

---

## 5. 手工查询 / 排障

不依赖 Grafana，直接 curl 单个实例，或对 Prometheus 发 PromQL：

```bash
# 直接看某 metad 的原始指标（text exposition）
curl -s http://metad:8091/metrics | grep -E '^nufs_cluster|^nufs_nodes|^nufs_raft'

# 直接看某 datanode 的磁盘用量
curl -s http://datanode:9100/metrics | grep -E 'nufs_disk_(capacity|used|available|state)'

# 对 Prometheus：在线节点比例（> 目标才有合格 quorum）
sum(nufs_nodes_online) / sum(nufs_nodes_total)

# 磁盘使用率最高的节点 Top
topk(5, nufs_disk_used_bytes / nufs_disk_capacity_bytes)

# 最近 10 分钟的错误速率
sum(rate(nufs_errors_total[10m]))

# 备份是否太久没成功（NUFSBackupStale 的表达式本质）
time() - nufs_backup_last_success_timestamp_seconds
```

---

## 6. 上线前告警链路复验（E3 升级为 ✅）

`make verify` 内置的 `scripts/check-metrics.sh` 只做**静态**门禁（规则引用的每个指标都
有 exporter 发出 + promtool 语法），不能证明「真实触发」。上线后务必补一次**动态复验**：

1. 部署 Prometheus + Grafana（或已有监控栈）抓 NUFS 三个 job。
2. 造一次可观察事件，确认告警真的触发并送达（建议按严重度各测一条）：
   - **critical**：停一个 datanode / 让 leader 翻转（`nufs-cli leader` 或 kill metad），
     确认 `NUFSClusterReadinessDegraded` / `NUFSClusterLeaderUnstable` firing。
   - **warning**：写一个副本不足的 chunk 或抬高 `nufs_node_write_error_rate`。
3. 在 checklist 的 **E3** 行把「⏳ 上线动作」从「真实集群验证告警链路」勾掉，状态定格 ✅。

---

## 7. 指标-告警一致性门禁（防死链）

`scripts/check-metrics.sh`（已纳入 `make verify`，fast/drill/full 每一级都跑）：

- 解析 `alerting-rules.yaml` 与 `internal/slo/slo.go` 引用的每个 `nufs_*` 指标；
- 与全部 Go exporter 源码实际发出的指标做**集合差**（`comm -23`）；
- 若某指标「写了规则但没人发」→ 判为死链，`FAIL` 并列出；
- 若本机有 `promtool`，再对 yaml 做规则语法检查（无则优雅 `SKIP`）。

这保证「规则可查询」这一半的 E3。跑法：

```bash
./scripts/check-metrics.sh          # 独立跑
make verify LEVEL=fast              # 作为门禁步骤之一跑
```
