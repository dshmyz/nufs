# V2.1 加固交付清单（`codex/v21-p0-hardening`）

> 状态：完成 ✅ | 分支：`codex/v21-p0-hardening`（相对 `main` 66 个 commit）| 工作树干净
> 这是本分支交付的**权威清单**。各 Program 的逐项设计/取舍见 plan 文件
> `~/.claude/plans/virtual-gathering-crab.md` 的对应 Program 节；V1↔V2.1 功能对齐补完
> 见仓库根 `MULTI_DISK_GAP.md`。
>
> 门禁：全包 `-race -count=1` 绿（metadata / gateway/s3 / chunkstore / gateway/fuse /
> datanode + datanode/storage/* 子包 / cmd/metad）；`tests/run-v21-multidisk.sh`
> Docker-compose 全栈 e2e PASSED；`go build ./... && go vet ./...` 干净；
> `git diff HEAD -- gateway/s3/auth.go` 恒为 0（从未触碰，standing 安全约束）。

---

## 1. 命名空间一致性加固（最近，P0 同 class bug 收官）

| Commit | 内容 |
|--------|------|
| `792459c` | **命名空间 create 原子化**：`MkDir`/`CreateFile`/`Symlink`/`Link`/`CreateObjectWithChunks` 并发同名创建改用 `ConditionalBatch` CAS —— 消除孤儿 inode + 父目录 NLink 漂移竞态。 |
| `ea87f55` | **命名空间 delete 原子化**：`RmDir`/`Unlink` 并发同名删除改用对 `nsKey` 原始字节的 CAS（`getRawBytes` 作 `ExpectedValue`）—— 消除双重 `NLink--`/下溢 + 双 `releaseInodeID`；`releaseInodeID`/`inCache.del`/`publishEvent` 仅在胜出提交后执行。**同时修掉** create 修复（`792459c`）引入的潜回归：非 raft 命名空间写不再受 `ctx.Err()` 门控（对齐 `applyBatchViaRaft`，S3 写尝试恢复 worker 依赖"ctx 取消也照常提交"）。 |

**验证**：`TestPebbleStore_ConcurrentSameNameMkDir/Unlink/RmDir`、`TestRaftClusterConcurrentSameNameMkDir/Unlink`（3 节点 raft 集群 CAS 唯一胜出）全绿 `-race`；`gateway/s3` 的 `TestObjectWriteRecoveryReleasesLockAfterTaskContextCanceled` 复绿（该测试暴露了 ctx 门控回归）。

## 2. FUSE 补完 + 多 chunk Flush（Program 11）

| Commit | 内容 |
|--------|------|
| `15b2510` | **多 chunk Flush**：`DFSFile.Flush` 重写为多 chunk + 整文件 `ChunkMap` 重建（对齐 S3 overwrite），**解除 64MiB EFBIG 单 chunk 限制**；跨 flush 复用旧 chunk（新分配 ID 永不相交 → 安全 delete 旧）。 |
| `120f0a6` / `4a58b45` / `5695194` / `3da2f6c` / `d4a7184` / `09d8516` | **FUSE 读写状态一致性（同 class）**：truncate-extend 空洞读零、O_APPEND 追加 + 非零 offset 已提交缓冲水合、Read/Getattr 反映未 flush 缓冲写、gap-aware 已提交 chunk 读 + 尊重 cref.Length、**任一未 flush 写即水合**（数据丢失修复）、buffer-image 不变量运行时 tripwire。 |
| `08de69d` | 修 FUSE linux-gated 套件潜在缺陷。 |

**验证**：`gateway/fuse -race` 绿；capstone `TestDFSFile_Flush_MultiChunk_ReadBack` / `_CrossFlushReuse`。

## 3. V2.1 跨节点 EC 生产拓扑（Program 9）

| Commit | 内容 |
|--------|------|
| `a608740` | `runDataNodeV21` 接通 `SetCrossNode` + `SetCandidateDisks`：`NodeInfo` 新增 `ShardDiskCount`（注册上报 + `ListNodes` 返回 + 重注册刷新），coordinator convert 时经 `ListNodes` 实时解析在线 peer 候选盘（`DiskID=NodeID*1000+d`），非本节点分片经 TCP `ReplicateECShard` push 给所属 peer —— 真正 ≥3 节点故障域。 |

**验证**：`TestECProdTopology_CandidateDisksFromListNodes` / `_ConvertViaListNodes`、
`TestECCrossNode_CoordinatorPushesShards` / `_PeerDownRollsBack`、`TestPebbleStore_ShardDiskCountRoundTrip` 全绿 `-race`。

## 4. V2.1 写路径直写 EC（网关直写分片，Program 10，方案 A）

| Commit | 内容 |
|--------|------|
| `f528f22` | V2.1 **写路径直写 EC**（对齐 V1 直写语义）：`chunkstore` 编码 6+3 分片后经 `ReplicateECShard` 直推各节点 shard store，不写中间复制副本；新增窄接口 `ECWriteAuthority`（`PlanECWrite`/`RecordDirectEC`，背靠 `gw.meta` 经 `metadata.HTTPClient` 结构型满足）+ `cmd/metad` plan-write / record-direct RPC（服务器权威做 §14 规划 + 落 Complete 态 durable stripe + 设 `ECStripeID`）。单节点亦支持（9 分片全落本节点 shard store）。读路径判别器 `ECStripeID!=""` → `ReadECShard`，自愈/孤儿 GC 全生效。 |

**验证**：`TestGatewayDirectECWrite_ReadBack` / `_SingleNode` / `_DegradedRead` / `_V1Fallback`、`TestECPlanWrite_DiverseOwners` / `_V1ClusterFails`、`TestECRecordDirectHTTP` 全绿 `-race`（V1 节点/未设 seam → 回退 `writeECChunk` 回归）。

## 5. V2.1 磁盘健康（Program 4 V1-c + Program 8）

| Commit | 内容 |
|--------|------|
| `b1a98fc` | **V2Store 磁盘健康状态机**：3-tier（online/degraded/failed）`failCount` 阈值驱动；placement（`nextLoc` 跳过坏盘）+ 写准入（Failed 拒写）+ 经 `DiskInfos`/`DiskStats` 暴露。 |
| `391203d` | **主动 I/O 健康 monitor**：周期 `Stat` probe 把 idle/读-wedged 盘推上 degraded/failed；恢复严格写路径成功清 streak（probe 不解封 Failed）。 |

## 6. V2.1 V1 子系统 parity（Program 4 V1-a/V1-b + 泛化）

| Commit | 内容 |
|--------|------|
| `d9127de` | **SIGHUP 配置热重载** parity。 |
| `4207956` | **V2Store.DrainWrites**（DrainOps 写屏障，HTTP `/drain` 从 501 → 200）。 |
| `51fe133` | **management/ops 通道泛化为 OpsStore**：`managementServer`（unix socket）+ `OpsServer`（HTTP）持有 `OpsStore` 接口，V1 `ChunkStore` 与 V2.1 `V2Store` 都实现；`runDataNodeV21` 现已启动这两条通道。 |
| `e5c3eb4` / `d396d00` / `38260e2` | V2Store.WriteErrorRate 滚动窗口对齐、live V2 DiskIO、V2.1 serving+心跳 parity。 |

## 7. V2.1 元数据权威 + 真复制（Task #56/#65/#66/#67）

| Commit | 内容 |
|--------|------|
| `8c141b4` | **PG/epoch 作为放置权威**（V2.1 地基）。 |
| `967d375` | 元数据权威 **generation fencing** 端到端（A2）。 |
| `e09e84d` | V2.1 真多节点复制（同 gen fan-out）e2e。 |
| `d428d57` | repair-from-survivor @ metadata generation。 |
| `0b64e54` / `d961df7` | change-journal 对账消费者 + 跨节点对账+修复 e2e capstone。 |

## 8. V2.1 EC 6+3 服务路径（Program 2/3，Task #74/#75/#76/#77/#78）

| Commit | 内容 |
|--------|------|
| `f5415d8` | **E1**：EC 分片 = 独立 extent，落专用 shard stream（StreamID 2，`ecshard` class dir），与数据流物理命名空间隔离。 |
| `f1acaff` | **E2**：真实 6+3 分片规划器（§14，≥3 node、≤3/机）+ 休眠 EC 字段接线。 |
| `eca27fa` | **E3**：默认 6+3 直写分片聚合（`WriteChunkEC`/`ReadChunkEC`，本地 reedsolomon）。 |
| `38f47ac` | **E4**：RF→EC 6+3 转换服务路径（真实分片写 + 原子切换 + 回滚）。 |
| `618823e` | **E5**：6+3 降级读 / repair / reheat。 |
| `74530c4` | 多节点 EC 6+3 stripe capstone（Program B）。 |
| `b706bf3` | **S1**：EC 转换驱动 serving 接线（shard store 附件 + `ECService` + ops 触发 + 降级读服务；`ECAuthority` 窄接口 authority seam）。 |
| `c262a8b` | **S2**：EC 生命周期权威经元数据 HTTP RPC（5 分段转换端点 + `HTTPClient` 结构型满足接口）。 |
| `0c3230d` | **S3**：跨节点 EC 转换（coordinator push 分片，`ReqReplicateECShard`/`ReqReadECShard`）。 |
| `271734c` / `069875b` | **publish hook**：原子 §14 chunk 布局切换（`SwitchChunkToEC`）+ 修 EC shard store index-dir 锁冲突（prod lock collision）。 |
| `8bc476e` | **Program 5**：共享 `ECProfile` + durable `ECStripeID` 指针 + `ResolveStripeLanding`（保留 `Replicas` 物化，V1 兼容）。 |

## 9. V2.1 EC 服务路径收官（Program 6，serving 读 + 自愈 + 权威落盘 + 孤儿 GC）

| Commit | 内容 |
|--------|------|
| `4994f9a` | **F1**：gateway 读路径接 V2.1 分片（`ECStripeID!=""` → `ReadECShard`，**serving 读打通**）。 |
| `db9c6b5` | **F2**：EC 自愈扫描 → `RepairChunkEC`。 |
| `d1af6dc` | **F3**：`ResolveStripeLanding` 接入 repair/verify（权威落盘）。 |
| `cd1c394` | **F4**：stripe 孤儿分片 GC（RolledBack 残留回收）。 |
| `101ed2b` / `7e1bc2d` | **Program 7**：EC resolver seam（landing + orphan）提为元数据 HTTP RPC，`runDataNodeV21` 接上，孤儿 GC 生产路径点亮。 |

## 10. Task #49 relocation 加固 + 跨盘 rebalance

| Commit | 内容 |
|--------|------|
| `5e8ed34` | **P1**：safe conditional relocation + 真实 checksum 保留（修复 `Checksum:0` 遮蔽 + tombstone-clobber 竞态 + location-cache 陈旧；采用 PUT-at-target 持久化方案，用户拍板；recovery 对 RELOCATE 按已校验 no-op）。 |
| `bdddba0` | **P2**：MultiV2Store 跨盘 rebalance（`RebalanceOne`/`RebalanceBalanced`，受锁 CAS 重指向 + 双盘账目一致）。 |

## 11. V2.1 存储引擎可靠性（P0 正确性 gate，Program 早期）

从 `7474509` 起的 P0 加固提交群：group commit 连续无死锁、恢复 checkpoint 硬化、tombstone 在恢复中保留、恢复字节预算、stream V3 segment 恢复、fail-closed on recovery apply error、V2 store 幂等关闭、范围读只读相交帧、进程崩溃（SIGKILL）验收、durable delete recovery、V3 CRC 硬化、V2.1 P0 正确性 gate。

---

## 任务清单映射

任务清单（`TaskList`）107 项全部 completed —— #45–#107 对应以上各 Program/Task；
唯一 pending 的 **#49（relocation 条件化 + 可重放）** 核心已被 `5e8ed34`（P1 加固）覆盖，且当时用户拍板采用 PUT-at-target 方案（on-disk 格式无法编码 RELOCATE 目标 offset，recovery 对 RELOCATE 按已校验 no-op 处理）——视为已收敛，无需再做。

## 关键设计决策（避免重读 plan 的速览）

- **EC 读判别器**：`ChunkMeta.ECStripeID != ""` ⇔ V2.1 已转换 6+3 布局 → `ReadECShard`；`""` ⇔ V1 语义 → 原 `ReadChunk`。V1 一字不动。
- **分片落点**：每分片 = 独立 extent，`extentID = chunkID`、`generation = shardIndex+1`，落专用 shard stream（StreamID 2），与数据流物理命名空间隔离（disjointness 来自 namespace 而非 chunk-ID 位布局）。
- **EC 落盘形态**：V2.1 用 `ReplicateECShard`（分片进 shard store），**不**用 V1 `WriteChunk`（会把分片当复制副本写进 data store）。
- **直写 vs 转换**：默认写路径 = 直写（Program 10）；转换路径保留为手动/备用（ops `POST /api/v1/ec/convert`）。
- **安全约束**：全程 `git diff HEAD -- gateway/s3/auth.go` = 0。

## 后续（未做 / 明示不做）

- #49 的"可重放 RELOCATE 记录"（用户拍板不落地，改 PUT-at-target）。
- metadata `ChunkMeta.Replicas` O(N×9) 物化 → PG/EC-profile 级收拢（Program 5 标注的过渡形态）。
- 自动 EC 转换调度（当前手动 ops 触发）。
- 跨节点多节点"只写自己分片"生产部署的进一步验证（Program 9 已真实走该路径）。
