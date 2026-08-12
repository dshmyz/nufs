# NUFS 脚本清单

所有脚本按用途分类，包含位置、用途、前置条件和用法。

---

## 快速参考

| 脚本 | 用途 | 前置条件 |
|------|------|----------|
| `scripts/verify.sh` | 全量 Go 回归门禁 | Go |
| `scripts/verify-docker.sh` | Docker 版回归门禁 | Docker |
| `scripts/check-metrics.sh` | Prometheus 指标一致性检查 | Go 源码 |
| `scripts/run.sh` | 一键启动测试场景 | Docker + Go |

---

## 回归/门禁

### `scripts/verify.sh`
全量回归门禁（`make verify` 的后端），三层级别可选：

```bash
./scripts/verify.sh                 # -l fast：build/vet/fmt/单测，几分钟
./scripts/verify.sh -l drill        # fast + 三个 soak drill（短时长）
./scripts/verify.sh -l full         # drill + 长时/高 count 门禁
./scripts/verify.sh -l fast -p pkg/ # 只测指定包（分包 -count=2）
```

**关键约束**：全量单测必须**分包 -count=2 串行**（`-p 1`），不能并行饱和 —— 并行会因 CPU 拥塞误报 raft 选举超时。

### `scripts/verify-docker.sh`
Docker 版回归门禁。解决 macOS 临时端口耗尽（`EADDRNOTAVAIL`）问题，在 Linux 容器内跑干净基线：

```bash
./scripts/verify-docker.sh                  # 等价于 verify.sh -l fast
./scripts/verify-docker.sh -l drill         # Docker 内 drill 门禁
```

### `scripts/check-metrics.sh`
Prometheus 指标 ↔ 告警/SLO 配置一致性校验。确保每个被告警规则引用的指标都有 exporter 发出：

```bash
./scripts/check-metrics.sh
# 退出码 0 = 全部一致；非 0 = 存在死指标
```

---

## 集成测试

### `scripts/run-v21-integration.sh`
V2.1 存储引擎全链路集成测试（metad + datanode-v21 + S3 gateway），通过 S3 API 写入/读取/校验字节精确性：

```bash
./scripts/run-v21-integration.sh           # 运行测试
./scripts/run-v21-integration.sh --cleanup # 运行后清理
```

### `scripts/run-v21-multidisk.sh`
V2.1 多盘（JBOD）集成测试。验证 multi-disk 适配器聚合所有磁盘（least-used 放置），数据落在多盘，端到端读写字节精确：

```bash
./scripts/run-v21-multidisk.sh           # 运行测试
./scripts/run-v21-multidisk.sh --cleanup # 运行后清理
```

---

## 疲劳/压力测试

### `scripts/fatigue-test.sh`（S3 API 路径）
持续 S3 读写负载 + 崩溃恢复 + 数据完整性巡检。部署完整 V2.1 集群（Docker），负载中途 SIGKILL datanode + 自动恢复，最后字节精确校验：

```bash
./scripts/fatigue-test.sh --duration 600           # 10 分钟负载
./scripts/fatigue-test.sh --duration 600 --crash-after 120  # 2 分钟后崩溃
./scripts/fatigue-test.sh --no-cleanup             # 保留环境供调试
```

### `scripts/fatigue-fuse.sh`（FUSE 挂载路径，Docker 版）
与 `fatigue-test.sh` 互补 —— 走**真实 FUSE 挂载**（内核 VFS → go-fuse → DFSFile → metad + datanode），验证 FUSE 层（含多 chunk Flush、truncate、O_APPEND）在持续负载 + 崩溃恢复下的写读一致性：

```bash
./scripts/fatigue-fuse.sh --duration 600 --crash-after 120
```

**前置条件**：Linux（真实 `/dev/fuse`）、Docker、python3、可执行 nufs-fuse。

### `scripts/fatigue-fuse-host.sh`（FUSE 挂载路径，裸机版）
与 `fatigue-fuse.sh` 等价，但完全在宿主机运行（无 Docker），用 `deploy/host/mount-helpers.sh` 裸机启动 metad + datanode：

```bash
./scripts/fatigue-fuse-host.sh --duration 600 --crash-after 120
```

**前置条件**：Linux + `/dev/fuse`、可执行 nufs-fuse、python3。

### `scripts/smallfile-fuse.sh`（海量小文件，Docker 版）
百万级小文件 FUSE 挂载测试：创建海量小文件 → 分层校验 → 崩溃恢复 → rm -rf 批量删除。暴露小文件路径的元数据/内存压力：

```bash
./scripts/smallfile-fuse.sh --scale 1000000 --shards 8 --filesize 2048
./scripts/smallfile-fuse.sh --crash-after 120 --full-verify
```

### `scripts/smallfile-fuse-host.sh`（海量小文件，裸机版）
与 `smallfile-fuse.sh` 等价，裸机运行：

```bash
./scripts/smallfile-fuse-host.sh --scale 1000000 --shards 8
```

---

## Soak/稳定性演练

### `scripts/soak/run-v21-leader-fault-injection.sh`
Leader 故障转移验证：3 个 metad-raft 节点 + 6 个 datanode，在持续 S3 负载下 SIGKILL leader，测量 RTO + 验证切换期无客户端错误 + 字节精确校验：

```bash
./scripts/soak/run-v21-leader-fault-injection.sh \
  --duration 300 --failover-after 120 --rto-budget 15
```

### `scripts/soak/run-v21-metadata-restore.sh`
元数据还原演练：raft 多数派防单节点故障 + 备份还原防全集群丢失。验证还原后 chunk 记录完好、未就绪前保持 ServiceUnavailable：

```bash
./scripts/soak/run-v21-metadata-restore.sh
```

### `scripts/soak/run-v21-chaos-soak.sh`
多节点混沌/soak 验证：真实进程拉起集群，持续 S3 负载下 SIGKILL 节点 + 原地重启，验证恢复 checkpoint 崩溃一致性 + EC self-heal + orphan-GC 收敛 + RSS 泄漏检测：

```bash
./scripts/soak/run-v21-chaos-soak.sh --nodes 6 --duration 600 --crash-after 120
./scripts/soak/run-v21-chaos-soak.sh --net-fault 30  # 追加网络故障注入
```

### `scripts/soak/run-v21-network-faults.sh`
网络故障注入：在 Docker 容器内注入 netem 故障（delay/loss/partition），验证集群在恶劣网络下的表现：

```bash
./scripts/soak/run-v21-network-faults.sh
# Scenario 1: 200ms delay，60 秒
# Scenario 2: 30% loss，60 秒
# Scenario 3: 20% loss + 50ms delay，60 秒
```

---

## 开发辅助

### `scripts/run.sh`
一键启动测试场景的统一入口：

```bash
./scripts/run.sh smoke        # 快速冒烟（S3 PUT/GET + kill datanode，~30s）
./scripts/run.sh regression   # 全量回归（datanode + chunkstore + gateway，~90s）
./scripts/run.sh load         # 负载测试（可配并发/时长）
./scripts/run.sh benchmark    # 科学基准（90% 读 10% 写，P50-P99.9）
./scripts/run.sh chaos        # 混沌（随机 kill datanode）
./scripts/run.sh ops-flow     # 运维流程（adopt → migrate → decommission）
./scripts/run.sh ec           # 纠删码集成
./scripts/run.sh multidisk    # 多盘测试
./scripts/run.sh full         # 全部测试（~180s）
```

### `scripts/dev/metad-cluster-helpers.sh`
metad 多节点集群辅助函数（`launch_meta`、端口/目录管理），被 `raft-join-test.sh` 和其他 soak 脚本引用。

### `scripts/dev/raft-join-test.sh`
Raft 多节点加入测试：启动 3 个 metad，验证集群选举和 quorum。

---

## 一键执行流程

**测试环境快速验证**（推荐顺序）：

```bash
# 1. 基础门禁（几分钟）
./scripts/verify.sh -l fast

# 2. POSIX 合规（需要挂载）
./scripts/soak/fuse-posix-test.sh /mnt/nufs/test-bucket

# 3. 集成测试（需要 Docker）
./scripts/run-v21-integration.sh

# 4. Leader 故障转移（需要 Go 编译）
./scripts/soak/run-v21-leader-fault-injection.sh --duration 120

# 5. 网络故障注入（需要 Docker + netem 镜像）
./scripts/soak/run-v21-network-faults.sh
```
