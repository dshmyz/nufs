# V1 退役路线图

> 目标：项目未上线、无存量数据，把 V1（legacy）三层整体退役，V2.1 成为唯一模型。
> 前提认知：**V1 不是一块，是三层**；其中"ChunkMap 元数据模型"目前仍是网关的
> 唯一 serving 路径，必须先完成 V2.1 inode 接线才能删（见阶段 1）。
>
> **硬门禁：任何一层 V1 被删除之前，该层的全部功能必须有 V2.1 等价实现
> （阶段 0 parity audit 通过）。不允许迁移后丢功能/丢优化。**

## 决策记录（已定，不再摇摆）

- **方向：切到 V2.1 元数据，且在上线前完成**。
- 依据：已声明的目标"**目标是元数据量级**"。按此标准，上线前切是最优时点——
  纯代码成本（6 个 ❌ 项重建 + inode 接线），零数据迁移、零兼容负担、零迁移工具。
  上线后切 = 同样的代码成本 + 数据迁移 + 双格式共存 + 线上风险，纯亏。
- 推论：**上线本身以阶段 0 + 阶段 1 完成为门禁**；阶段 2/3/4 为收尾，可并行。
- 护栏：V1 存储引擎层 ① 已完全删除（`--storage-version` flag、`chunkstore.go`、WAL、DiskManager），
  `pool.go`/`pipeline.go` 保留（客户端网络传输，`chunkstore/` client 库仍依赖）。
  新 inode/ref/存储功能只进 V2.1。

## V1 的三层定义

| 层 | 代码位置 | 现状 |
|---|---|---|
| ① V1 存储引擎 | ~~`cmd/datanode/main.go:319`（V1 路径，`--storage-version` 默认 `v1`）~~、~~`datanode/chunkstore.go` + WAL~~ | **✅ 已删除**（2026-08-15）。V2.1 是唯一引擎，`--storage-version` flag 已移除 |
| ② V1 EC | `chunkstore/ec.go:59-61`（`writeECShardDirect` 的 fallback）、`ec.go:134 writeECChunk`（整片文件）、`readECChunk` 的 `ECStripeID==""` 分支 | 直写 EC 的静默降级路径；V1 EC chunk 只能 V1 分支读 |
| ③ V1 元数据 inode 模型 | `gateway/s3`、`gateway/fuse` 的 ChunkMap + `ChunkMeta.Replicas` | **网关唯一 serving 模型**；`InodeStoreV2`/extent 模型（`metadata/inode_store.go`、`extent_page.go`）已建成未接线 |

---

## 阶段 0：功能对标（parity audit）——硬门禁

**删除任一 V1 层之前，逐项确认等价实现存在并接线。** 已完成的审计：

### ③ ChunkMap 元数据模型 vs extent 模型

| V1 功能 | V1 载体 | extent 模型等价物 | 状态 |
|---|---|---|---|
| 稀疏文件/hole | `ChunkRef.Offset` | `ExtentRef.LogicalOffset`（`types.go:178`） | ✅ 可表示，未接线 |
| MVCC read-your-writes | `ChunkRef.Version` | COW root + `ExtentRootVersion` | ✅ 更强，未接线 |
| 放置策略 | `ChunkMeta` placement | PGID + PlacementEpoch（§11.3） | ✅ 已实现 |
| 存储 tier | `ChunkMeta.Tier` | `ExtentMetaV2.StorageClass`（`types.go:126`） | ⚠️ 字段有，机制未接 |
| 心跳降级检测 | Chunk 状态机 + `batchUpdateChunkStatesCtx` | `ExtentMetaV2.Lifecycle`（`types.go:124`） | ⚠️ 字段有，心跳路径无 |
| **Scrubber**（校验/降级计数） | `ScanAllChunks`/ChunkMeta | **无 extent 版 scrubber** | ❌ 缺 |
| **修复**（TriggerRepair/队列） | ChunkMeta + repair queue | **无 extent 版** | ❌ 缺 |
| **bucket 配额** | `ChunkMeta.Size` 聚合 | **无 extent 版** | ❌ 缺 |
| **备份/恢复** | ChunkMeta 清单 | **无 extent 版** | ❌ 缺 |
| **S3 multipart** | 分片 → chunk 合并 | **无 extent 版** | ❌ 缺 |
| **孤儿 GC** | 元数据查 ChunkMeta | **无 extent 版**（现有 GC 是数据面） | ❌ 缺 |

> 结论：❌ 项全是 chunk 级机制，没有任何代码触碰 `ExtentMetaV2/ExtentRef`（仅
> inode_store/extent_page/ec_lifecycle/types 四处是 extent 家族自身）。**删 ③
> 前，上述 ❌ 项需在 extent 层实现或移植；⚠️ 项需把字段接成机制。**

### ① V1 存储引擎 vs V2.1 segment 引擎 ✅ 完成

> V1 存储引擎（`chunkstore.go`、WAL、DiskManager、AntiEntropy、ParallelReplicator）
> 已于 2026-08-15 删除。V2.1 segment 引擎是唯一存储路径。

| V1 功能 | V2.1 等价 | 状态 |
|---|---|---|
| WAL 崩溃恢复 | journal + recovery 包（`datanode/storage/journal|recovery`） | ✅ |
| 磁盘健康监控 | Program 4 V1-c + Program 8（delivery doc §5） | ✅ |
| 容量水位（15%/10%/5%） | `data-organization.md` §3.5 已定义 | ✅ |
| 压缩/加密/change journal | `a033d2f`（存储引擎阶段4） | ✅ |
| 客户端连接池 + 并行复制 | `datanode/pool.go` + `datanode/pipeline.go`（保留，`chunkstore/` client 库仍在用） | ✅ |

### ② V1 EC vs V2.1 EC

| V1 功能 | V2.1 等价 | 状态 |
|---|---|---|
| 编解码（klauspost RS） | 同一 codec | ✅ 共享 |
| 自愈/孤儿 GC/修复 | `ec_reaper`/ops GC/repair | ✅ 共享 |
| 整片文件读 | 分片 extent + **窗口读（1×）** | ✅ 更强 |

---

## 阶段 1：V2.1 inode 接线（删 ③ 的前提，最大改动）

> 来源：小文件优化清单。分两条线，按"写前是否可知大小"分流。

### 1.1 选型（网关无关——路由挂"写入"，不挂"网关"）
- **同一 bucket 同时支持 S3 API 与 FUSE 挂载**（共享 metad 命名空间与 datanode），
  布局/路由决策必须让两条路径交叉可读。
- **物理路由**：datanode 侧按**写入大小**统一决策（≤64KiB → small stream），
  S3 与 FUSE 写入自动一致，双向可读。不做"仅 S3 路由"。
- **元数据布局是文件属性**：inline/extent-pages/chunk 由读路径统一解析。
  S3（写前知大小）可提前落 inline；FUSE（关文件才知道）先按块写、flush 时
  单小块再转 inline——殊途同归到同一布局。
- **交叉验证是硬要求**：S3 写 → FUSE 读；FUSE 写 → S3 读；同文件双路径并发访问。

### 1.2 数据面（small segment）
- [ ] `MultiV2Store`/`v2_store.go` 挂载 `SmallStore`（`datanode/storage/segment/small_store.go:35`，现 `NewSmallStore` 零调用者）
- [ ] `datanode/server_v2.go` V2Store.Write ≤64KiB 路由到 small stream
- [ ] 读路径按 small 标志分流（small stream 独立 index 已有）
- [ ] 复用 `compress.go` 的 4KiB–64KiB 采样压缩
- [ ] 确认 small stream 纳入 seal/compact/磁盘水位维护

### 1.3 元数据面（inline extent）
- [ ] `metadata/inode_store.go:65 SetInlineExtent` 接入 FUSE Flush（≤16MiB 单块转 inline）
- [ ] 超限走 `PromoteToPages`（`inode_store.go:84`，已实现）
- [ ] 网关读路径兼容 ChunkMap + ExtentPages 双模型（`ResolveExtents` `inode_store.go:163` 已支持两态）
- [ ] EC 交互重测（`ec_lifecycle.go:528` 已假设 inline 布局）

### 1.4 extent 级机制补齐（阶段 0 的 ❌/⚠️ 项，可与接线并行）
- [ ] Scrubber extent 版（按 ExtentMetaV2 校验 + Lifecycle 计数）
- [ ] 修复机制 extent 版（TriggerRepair 的 extent 语义 + 队列）
- [ ] 心跳降级检测 extent 版（把 `ExtentLifecycle` 接进上报/降级路径）
- [ ] bucket 配额按 extent 聚合
- [ ] 备份/恢复按 extent 清单
- [ ] S3 multipart 合并落 extent
- [ ] 孤儿 GC 认 extent 布局

### 1.5 退出条件
- 网关写入路径不再产生新 ChunkMap；ChunkMap 只读存量（阶段 4 删）。
- 阶段 0 的 ❌/⚠️ 项全部闭环。
- 小文件读改写、promote 后读写、稀疏文件、EC 转换、配额、备份全绿。

---

## 阶段 2：V2.1 存储引擎默认化 + v1 引擎退役（删 ①） ✅ 完成

> 2026-08-15 完成。V1 存储引擎（chunkstore.go、WAL、DiskManager、AntiEntropy、
> ParallelReplicator、failover.go）已全部删除。`--storage-version` flag 已移除。
> V2.1 segment 引擎是唯一存储路径。客户端传输层（pool.go、pipeline.go）保留。

### 退出条件 ✅
- ✅ 无 `--storage-version` flag；无 legacy ChunkStore 代码；V2.1 引擎全协议面测试覆盖等价。

---

## 阶段 3：V1 EC 退役（删 ②）

- [ ] `chunkstore/ec.go:59-61` fallback 改为**硬错误**（ECConfig 桶在 authority 不可用时直接写失败，行为显式化）
- [ ] `writeECChunk`（`ec.go:134`）与 `readECChunk` 的 `ECStripeID==""` 分支删除（V1 整片 EC）
- [ ] 确认无路径产生 `ECStripeID==""` 的 EC chunk（转换路径 `ConvertToEC` 不产生）

### 决策点
- fallback 删除 = 代价从"降级写成功"变"写失败"。对 ECConfig 桶是正确行为；需在发布说明写明。

---

## 阶段 4：格式与测试清理

- [ ] JSON/msgpack 双格式共存（`docs/architecture/data-organization.md:60`）→ 只留 msgpack
- [ ] V1 引擎/EC 的 legacy 测试迁移或删除（`datanode/chunkstore_test.go` 等）
- [ ] 全仓 grep 清理 `storage-version`、`ECStripeID==""`、`ChunkMap` 写路径引用

### 退出条件
- 全仓无 V1 引用；`-race` 全量绿。

---

## 依赖与风险

- **阶段 0 是硬门禁**：❌ 项（scrubber/修复/配额/备份/multipart/孤儿 GC 的 extent 版）未闭环前，③ 不可删。
- **上线门禁**：阶段 0 + 阶段 1 完成 = 上线前置条件；在此之前 V1 三层全部保留
  （测试矩阵都基于 ChunkMap，删 ③ 会无路径可跑）。
- 阶段 2 的协议面等价抽查是硬门禁（V2.1 声称 parity ≠ 已验证）。
- 阶段 3 的 fallback 硬错误是行为变更，需显式决策。
- 顺序：**0 + 1（上线门禁）→ 2/3（可并行收尾）→ 4**。
