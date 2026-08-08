# V1 ↔ V2.1 功能对齐 gap 清单

> 状态：2026-08-03 服务+心跳→盘对齐完成后盘点。范围边界沿用用户既定决策：
> 目标 = **服务路径 + 心跳到盘**；磁盘生命周期运维命令**未包含在 V1→V2.1 对齐目标内**（用户原先明确排除）。

## 1. 已对齐（TCP 服务路径 + 心跳）✅

`LocalChunkStore`（`datanode/server.go:25`）是 TCP 服务器对存储后端的全部依赖，
`V2Store` 全实现且等价：

| Server op | V2Store 方法 | 说明 |
|---|---|---|
| `Write` | `Write` | 多盘 least-used 放置 + 覆盖写 gen+1 分代栅栏 |
| `Read` | `Read` | 分帧读取，只校验相交帧，返回真实 checksum |
| `Delete` | `Delete` | 分代 fenced tombstone，幂等 |
| `Replicate`(Write+Seal) | `Write`+`Seal` | Seal 为 no-op 但完整性在每次读校验（加强） |
| `ChunkInfo` | `Info` | 真实 size/checksum/state/disk |
| `ListChunks` | `ListChunks` | 枚举 live extent |
| `Health`(Stats) | `Stats` | 真实聚合 |

心跳 `HeartbeatStore`（`HeartbeatStore` 接口 + `diskIOProvider`）：
`Stats`/`ChunkStateSnapshot`/`StateVersion`/`DiskStats`/`WriteErrorRate` 全部真实；
`DiskIO` 由 `ReadWriteBytes()` 产出实时值（**V1 恒为 0**，V2.1 超出，用户批准的差异）。

**验证**：`scripts/run-v21-multidisk.sh`（多盘放置 + 字节精确读写 e2e 门禁）通过。

## 2. 行为差异（非缺口，引擎机制不同）⚠️

| 项目 | V1 | V2.1 | 影响 |
|---|---|---|---|
| `Seal` 返回值 | 返回真实 CRC 并持久化到文件头 | `(0,nil)` no-op；完整性靠每次读时逐帧 CRC/AEAD 校验 | 服务等价，完整性**加强**。Server 忽略返回值 |
| `WriteErrorRate` | 每次调用 `Swap(0)` 重置（滚动窗口速率） | 累计比率（自启动至今，从不重置） | 都能报 0-1，但 V2.1 是累计不是窗口。若要对齐：心跳周期内 `Swap(0)` |
| `Read` 的 `LastAccess`/`AccessCount` | 每次读更新（优化路径不校验 CRC） | `Info` 返回 `time.Now()`/0 | 管理性字段，非服务语义 |
| 加密接线 | `ChunkStore.SetEncryptor` | `segment.Config.Enc`（重启需 KMS，LocalKMS 仅 dev） | V2.1 在 runDataNodeV21 已接线 |

## 3. 未对齐：V2.1 daemon 缺失的整块子系统 ❌

> 更新（2026-08-04）：**运维通道已对齐**（任务 #55）。`managementServer`（unix
> socket）与 `OpsServer`（HTTP）已泛化为持有 `OpsStore` 接口（`datanode/store_ops.go`），
> V1 `ChunkStore` 与 V2.1 `V2Store` 都实现它，`runDataNodeV21` 现已启动这两条通道
> （读写/状态/verify/metrics 子集）。磁盘生命周期命令（adopt/retire/decommission/
> migrate/drain）用能力接口点菜——V2.1 无则返回 "unsupported by this engine"（HTTP
> 501 / unix error）。V1-only 子系统（disk manager/replicator/anti-entropy/repair）
> 全部 nil 守卫。下表其余子系统仍缺失。

`runDataNode`（V1）启动的组件，在 `runDataNodeV21` 中仍然**没有**（除上注已对齐项）：

| V1 子系统 | 类型 | 说明 |
|---|---|---|
| **磁盘健康状态机** `DiskManager`/`NewMultiDiskManager` | 磁盘健康 | V1 标记 bad disk、写拒绝；V2Store.`DiskManager()` 返回 nil，`diskFailed` 是兜底 |
| **副本复制** `Replicator`/`ParallelReplicator` | 复制 | 副本写入管线。V2.1 无 |
| **后台修复** `RepairWorker` | 修复 | 损坏 chunk 后台重修复。V2.1 无 |
| **anti-entropy** `AntiEntropy` | 对账 | 周期 chunk 元数据对账。V2.1 无 |
| **写入排空** `DrainWrites` | 优雅停机 | runDataNode 停机前排空；V2.1 靠 `runDataNodeV21` 停机顺序（`DrainOps` 未实现，命令报 unsupported） |
| **SIGHUP reload** | 配置重载 | V1 有，V2.1 无 |

**架构根因**：上表 2-7 项都位于 `*datanode.ChunkStore` 之上（ops.go/ha.go 直接读
`chunks`/`totalBytes`/`chunkCount`），而这些状态在 V2.1 由 `V2Store`/`segment.Store`
持有。把它们喂给 V2Store 需要把这些子系统泛化（改为对 `LocalChunkStore`+`HeartbeatStore`
接口编程），或按 Metadata V2 的放置/EC/change-journal 服务路径重做——后者是
V2.1 文档中已注明的"到完整生产"的最后缺口（见 `datanode/storage/RUNBOOK.md`：
"legacy chunkstore has its own runbook until V2.1 reaches parity"）。

**注意**：复制/anti-entropy/repair 对 V2.1 而言语义上与 Metadata V2 的
placement-group/epoch/change-journal EC 服务强相关，不是单纯"把 V1 的 ha.go/ops.go
泛化"就能对齐——这是 V1 架构（每 chunk 一文件 + 全局 map 索引）与 V2.1 架构
（segment + 异步位置索引 + PG epoch）的根本差异。

## 4. 结论与建议

- **服务路径 + 心跳→盘：已对齐，无功能缺口。** 这是用户当前对齐目标，已完成。
- **运维通道（任务 #55，2026-08-04）：已对齐。** `OpsStore`/`DiskLifecycleOps`/
  `DrainOps` 接口抽取，`managementServer` + `OpsServer` 都持有 `OpsStore`，V2.1
  `runDataNodeV21` 启动 unix-socket 管理 + HTTP ops 通道，服务读写/状态/verify/
  metrics 子集。磁盘生命周期命令 V2.1 报 "unsupported"。验证：
  - `TestOpsServer_V2Store*`（health/disks/metrics/verify/501-unsupported）
  - `TestManagementServer_V2Store*`（unix status + unsupported）
  - `scripts/run-v21-multidisk.sh` e2e 门禁绿（重建镜像后重启 datanode-v21-multi，
    字节精确读写 + 双盘落地）
- **磁盘健康状态机 `DiskManager`**：V2.1 用 `diskFailed`（连续写失败）兜底，无
  完整 bad-disk 状态机。要补需泛化 `DiskManager` 对 `*ChunkStore` 的依赖。
- **`WriteErrorRate` 窗口语义**：已对齐（2026-08-03，任务 #54，心跳周期内 `Swap(0)`）。
- **复制/anti-entropy/repair + Metadata V2 服务路径（任务 #56）**：本身在用户三项
  清单内（第 3 项），是上表其余子系统的地基，工程量最大，下一项做。
- **Task #49（relocation 条件化 + 可重放）**：理论性风险（tombstone + generation
  fencing 双重兜底），已延后。
