# DFS 待办任务清单

## ✅ 已完成 (P0-P1)

### 核心存储
- [x] Pebble KV 存储引擎（替代 etcd）
- [x] hashicorp/raft 共识层
- [x] 日志二进制编码 + 快照持久化
- [x] 元数据分片路由器（一致性哈希）

### 数据节点
- [x] Chunk 存储（256 分片目录）
- [x] 异步复制器（Replicator）
- [x] TCP 服务器（读写/复制协议）
- [x] 磁盘管理器（容量监控 + WAL + 存储分层）
- [x] 高可用模块（链复制 + 反熵修复 + 就近读）

### 小文件优化
- [x] SmallFileBlock 合并存储（64KB 阈值）
- [x] Block 索引（256 文件/Block）
- [x] 元数据优化（80% 缩减）

### 客户端优化
- [x] 本地 LRU 缓存（1GB）
- [x] 异步写回缓冲（500MB）
- [x] 属性缓存（5s TTL）

### 一致性保证
- [x] Raft 链复制（多数派确认）
- [x] Lease 租约管理（30s TTL + 心跳）
- [x] MVCC 乐观锁（CAS 更新）

### 运维平台
- [x] HTTP 管理 API（7 个端点）
- [x] CLI 工具（dfsctl）
- [x] Prometheus 指标收集
- [x] Health Check 端点

### 生命周期管理
- [x] 规则引擎（Transition + Expiration）
- [x] 自动分层迁移
- [x] 过期自动删除

### 部署
- [x] Docker Compose（3 数据节点 + S3 网关）
- [x] 多阶段 Dockerfile
- [x] 嵌入式 etcd 模式

### 测试
- [x] 106 个测试全部通过
  - datanode: 27 测试
  - metadata: 59 测试
  - gateway/s3: 20 测试

### V2.1 加固（`codex/v21-p0-hardening` 分支，66 commit）

> 详细逐项清单（含 commit、capstone 测试、设计决策）见
> [`docs/v21-hardening-delivery.md`](docs/v21-hardening-delivery.md)。

- [x] **V2.1 跨节点 EC 生产拓扑（Program 9）**：`NodeInfo.ShardDiskCount` 上报 + `runDataNodeV21` 接 `SetCrossNode`/`SetCandidateDisks`，convert 经 `ListNodes` 解析在线 peer 候选盘，非本节点分片 TCP push
- [x] **V2.1 写路径直写 EC（Program 10）**：网关直写分片，`ECWriteAuthority`（PlanECWrite/RecordDirectEC）经元数据 HTTP RPC，单节点/多节点均支持
- [x] **V2.1 EC 6+3 服务路径收官（Program 6/7）**：gateway serving 读、自愈、权威落盘（`ResolveStripeLanding`）、孤儿分片 GC，全部经元数据 HTTP RPC 点亮
- [x] **V2.1 EC 服务路径接线（Program 2/3）**：E1–E5 分片存储直写/转换/降级读，S1 serving、S2 元数据 RPC 权威、S3 跨节点 push、publish 原子布局切换，Program 5 共享 ECProfile
- [x] **V2.1 磁盘健康（Program 4/8）**：3-tier 状态机 + 主动 I/O health monitor
- [x] **V2.1 V1 子系统 parity**：SIGHUP 热重载、`DrainWrites` 写屏障、management/ops 通道泛化 OpsStore
- [x] **V2.1 元数据权威 + 真复制（Task #56）**：PG/epoch 放置权威、generation fencing、跨节点复制、change-journal 对账 + repair
- [x] **Task #49 relocation 加固 + 跨盘 rebalance**：P1 safe conditional relocation + 真实 checksum，P2 MultiV2Store 跨盘 rebalance
- [x] **FUSE 补完 + 多 chunk Flush（Program 11）**：解除 64MiB EFBIG、整文件 ChunkMap 重建、读写状态一致性（同 class bug）修复
- [x] **命名空间一致性加固**：create/delete 双路径 `ConditionalBatch` CAS，消除并发同名 RMW 竞态
- [x] **V2.1 存储引擎可靠性（P0 gate）**：group commit 无死锁、恢复 checkpoint 硬化、崩溃验收等

**门禁**：全包 `-race -count=1` 绿；`tests/run-v21-multidisk.sh` e2e PASSED；
`git diff HEAD -- gateway/s3/auth.go` 恒为 0。

---

## 📋 待实现 (P2-P3)

### P2: 跨机房复制 (预计 2 周)
- [ ] **异步多活架构**
  - [ ] Append-Only Log 推送协议
  - [ ] Chunk 异步复制（仅 Seal 后的数据）
  - [ ] CRDT Last-Write-Wins 冲突解决
  - [ ] 断网重试 + 72h 日志保留

- [ ] **故障切换**
  - [ ] DNS 自动切换（1min）
  - [ ] 备节点提升为主
  - [ ] 原主恢复后增量同步

- [ ] **测试**
  - [ ] 同城复制延迟 < 5s
  - [ ] 跨城复制延迟 < 30s
  - [ ] 故障切换自动化测试

### P2: Kubernetes 部署 (预计 1 周)
- [ ] **Helm Chart**
  - [ ] StatefulSet 配置（metad/datanode/s3gw）
  - [ ] PVC 存储卷模板
  - [ ] Service + Ingress 暴露
  - [ ] ConfigMap 配置管理

- [ ] **滚动升级**
  - [ ] 零停机升级策略
  - [ ] 数据迁移验证
  - [ ] 回滚机制

- [ ] **测试**
  - [ ] kubectl rollout restart
  - [ ] 节点故障自动恢复
  - [ ] 存储卷挂载测试

### P3: 平台化 Dashboard (预计 2 周)
- [ ] **Web UI**
  - [ ] 集群概览（节点/容量/QPS）
  - [ ] Bucket 管理界面
  - [ ] 文件浏览器
  - [ ] 实时监控图表

- [ ] **告警系统**
  - [ ] Prometheus AlertManager 集成
  - [ ] 磁盘使用 > 90% 告警
  - [ ] 节点宕机 > 5min 告警
  - [ ] 复制延迟 > 30s 告警
  - [ ] 孤儿 chunk > 1000 告警

- [ ] **审计日志**
  - [ ] 操作日志记录
  - [ ] 生命周期执行日志
  - [ ] 故障切换事件日志

### P3: 性能优化 (预计 2 周)
- [ ] **元数据分片扩展**
  - [ ] 128 分片支持
  - [ ] 分片间迁移
  - [ ] 负载均衡算法

- [ ] **数据路径优化**
  - [ ] 零拷贝网络（sendfile）
  - [ ] 批量操作（multi-get/multi-put）
  - [ ] 压缩传输（zstd）

- [ ] **基准测试**
  - [ ] 小文件场景：100K files/s
  - [ ] 大文件场景：10GB/s 吞吐
  - [ ] 混合负载测试

### P3: 安全加固 (预计 1 周)
- [ ] **认证授权**
  - [ ] AWS Sig V4 完善
  - [ ] RBAC 权限控制
  - [ ] 租户隔离

- [ ] **数据安全**
  - [ ] 传输加密（TLS）
  - [ ] 静态加密（AES-256）
  - [ ] 密钥管理（KMS 集成）

- [ ] **审计合规**
  - [ ] S3 访问日志
  - [ ] 操作审计追踪
  - [ ] 数据保留策略

### P3: 高级特性 (持续)
- [ ] **纠删码完善**
  - [ ] GF(256) 性能优化（SIMD）
  - [ ] 动态重建策略
  - [ ] 冷热数据不同 EC 策略

- [ ] **再平衡优化**
  - [ ] 后台限速再平衡
  - [ ] 增量再平衡
  - [ ] 拓扑感知迁移

- [ ] **多协议支持**
  - [ ] NFS Gateway
  - [ ] HDFS Gateway
  - [ ] FTP Gateway

---

## 🐛 已知问题

- [ ] 客户端缓存未与 FUSE/S3 Gateway 集成
- [ ] 生命周期 prefix 匹配需要遍历 DirEntry（当前跳过）
- [ ] AntiEntropy repair 未写入元数据层副本状态更新（只修复本地数据）

---

## 📊 里程碑

| 阶段 | 目标 | 预计完成 | 状态 |
|------|------|---------|------|
| P0 | 核心存储 + 数据节点 | 已完成 | ✅ |
| P1 | 小文件优化 + 运维平台 | 已完成 | ✅ |
| P2 | 跨机房复制 + K8s | 3 周 | 📋 |
| P3 | Dashboard + 安全 + 性能 | 5 周 | 📋 |
| GA | 生产就绪 | 8 周 | 📋 |

---

## 🎯 核心指标目标

| 指标 | 当前值 | 目标值 |
|------|--------|--------|
| 元数据写入 | 210K ops/s | 500K ops/s |
| 读延迟（缓存命中） | < 100μs | < 50μs |
| 读延迟（网络） | 2-5ms | < 1ms |
| 小文件元数据 | 500B/文件 | 100B/文件 |
| 跨城复制延迟 | 未实现 | < 30s |
| 可用性 | 未测量 | 99.99% |
| 数据持久性 | 未测量 | 99.999999999% (11个9) |
