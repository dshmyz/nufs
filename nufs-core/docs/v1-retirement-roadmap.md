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

### 1.2 数据面（small segment） ✅ 完成

> 2026-08-16 完成。small-stream 数据面接线随 V2.1 迁移落地（提交 `5123ce4`）：
> `cmd/datanode/main.go` 每盘创建 `NewSmallStore`（StreamID 0，独立 `index-small`）
> 并经 `AttachSmallStores` 挂载；`datanode/server_v2.go` 的 `Write`/`WriteGen`
> ≤ `SmallFileThreshold`(64KiB) 路由到 small stream，超限 `migrateSmallToData`；
> 读路径按 `loc.small` 经 `backendAt` 分流；采样压缩走共享 `Store.Write`；
> compaction worker 经 `DataStores()`（含 small）驱动。`AddDisk` 运行期热加盘
> 同样建/挂 small stream（`server_v2.go` AddDisk，factory 三件套化，本次补上）。
> 测试覆盖在 `datanode/v2_small_store_test.go`。
>
> 注：roadmap 原文 `v2_store.go` 引用陈旧——接线在 `datanode/server_v2.go`。

- [x] `MultiV2Store`/`server_v2.go` 挂载 `SmallStore`（`datanode/storage/segment/small_store.go`，每盘一个）
- [x] `server_v2.go` V2Store.Write ≤64KiB 路由到 small stream
- [x] 读路径按 small 标志分流（small stream 独立 index 已有）
- [x] 复用 `compress.go` 的 4KiB–64KiB 采样压缩
- [x] 确认 small stream 纳入 seal/compact/磁盘水位维护

### 1.3 元数据面（inline extent）
> 2026-08-16 完成（1.3a `c6d455f` + 1.3b `ef467eb` + 1.3c `写侧双模型`）。
> 写侧双模型：FUSE Flush 与 S3 PUT 共用 `metadata.CommitChunkRefsModelAware`
> （`inode_v2_serving.go`）——≤16MiB 单 chunk → `SetInlineExtent`，否则整集
> `ReplaceExtents`（`inode_store.go` 新增，旧 extent 不保留，COW pages）。
> 超限走的是 `ReplaceExtents` 而非 roadmap 原文的 `PromoteToPages`：后者把旧
> inline extent 保成 page 0，对覆写语义是悬垂引用。`ReplaceExtents` 同样由
> serving 面（PebbleStore/ShardedStore/metad HTTP `PUT /inodes/{id}/replace-extents`）
> 与 `HTTPClient` 贯通，V2 写经 `putWithBucketStats` 累计 bucket 用量。
> 交叉验证（§1.1 硬要求）在 `gateway/s3/dual_model_cross_test.go`：
> S3 写 → FUSE ReadView 读回、FUSE flush → S3 GET 读回，inline/pages 双布局。
> 1.3d（本刀）：写侧 commit 经 `ecClassForRef` 逐 ref 解析 chunk 的 EC 真值并标
> extent——ECConfig 桶落 inline/pages 的 `ExtentMetaV2` 得 `StorageClass=ColdEC` +
> `ECStripeID=ec-<chunkID>`（chunk 行在 `writeECShardDirect` 的 `RecordDirectEC`
> 抬升后必带两者；stripe 回退 `ECGroup.GroupID` 兜住 RecordDirect 前后窗口）。
> 单元 `metadata/ec_extent_marking_test.go`（EC 桶 inline/pages → ColdEC、热桶回归、
> 缺 chunk 优雅降级为 HotReplica）+ smoke `TestRepair_EndToEnd_EC` 端到端断言。
> **Deferred**：① conversion 路径（`publishECConvert`→`SwitchChunkToEC`）不标 extent——
> publish 端点无 inode 上下文，标 extent 需 datanode 侧传 inode，归 §1.4 转换重做；
> ② `MarkExtentColdEC` 保持仅 inline（pages 拒 `ErrExtentNotInline`），既有测试覆盖其假设。

- [x] `metadata` 的 `SetInlineExtent` 接入 FUSE Flush（≤16MiB 单块转 inline）
- [x] 超限整集走 `ReplaceExtents`（`inode_store.go`，整体写 COW pages；roadmap 原文 `PromoteToPages` 语义不匹配覆写，见上注）
- [x] 网关读路径兼容 ChunkMap + ExtentPages 双模型（`ResolveExtents`，1.3b 已铺读侧，本刀写侧落地后交叉读回验证）
- [x] EC 交互重测（`ec_lifecycle.go` 的 inline 假设已测；标注为 `:528` 过期，实际假设
  在 `:618` `MarkExtentColdEC`，1.3d 完成见上注）

### 1.4 extent 级机制补齐（阶段 0 的 ❌/⚠️ 项，可与接线并行）
- [ ] Scrubber extent 版（按 ExtentMetaV2 校验 + Lifecycle 计数）
- [ ] 修复机制 extent 版（TriggerRepair 的 extent 语义 + 队列）
- [ ] 心跳降级检测 extent 版（把 `ExtentLifecycle` 接进上报/降级路径）
- [ ] bucket 配额按 extent 聚合
- [ ] 备份/恢复按 extent 清单
- [x] S3 multipart 合并落 extent（gateway/s3/multipart.go——见下注）
- [ ] 孤儿 GC 认 extent 布局

> 2026-08-16 完成（本刀）。`handleCompleteMultipartUpload` 的 501 stub 改为真合并：按
> complete 请求顺序用 `io.MultiReader` 流式拼装 staged parts（磁盘 part `os.Open`，
> 内存 part `bytes.NewReader`），整个 merge+commit 持 `upload.mu`，交 `gw.committer.Put`
> ——与单发 PUT 完全同一编排（quota/AdvisoryLock/超写 supersede+tombstone/
> ECConfig ColdEC 标注全继承），经 `CommitChunkRefsModelAware` 落 V2 inline/pages
> extent。守卫：parts 非空→400、part 号严格递增唯一→400、逐 part ETag 匹配→400、
> 任一 part `Size==0`→400、bucket/key 与 upload 一致→404；`result.Size != total` 视作
> 失败并保留暂存（可重试/abort）。**complete 是终态**：新增 `multipartUpload.finished`
> 旗标 + `writePart` 锁内守卫 + `errUploadFinished`——成功即置位并 `cleanupUpload`
> + `activeUploads.remove`，晚到 UploadPart（已持指针）→404 且丢弃、double-complete→404；
> 锁序新增 `upload.mu→tracker.mu`（remove）嵌套，无反向持有，无环。测试：改写
> `TestMultipartUpload`（协议面 200→二次 complete 404→abort 404）+ 新增
> `TestMultipartUploadRejectedCompleteKeepsParts`（坏 ETag→400 且 upload 保留）+ 新增
> `multipart_dual_model_test.go`（真 PebbleStore fixture：inline/pages 布局 + GET 读回 +
> 超写旧 chunk tombstone 经 `ListChunkTombstones` 断言）。
> **行为注记**：complete 总大小受 `gw.maxObjectSize`（默认 5GiB，与单发 PUT 同 cap，
> 已逐 part 受同 cap）——"multipart 超单发 PUT 上限"不可达，413 EntityTooLarge 接受。
> **Deferred**：写尝试恢复语义沿用 `Put` 自带 attempt+补偿（recover 幂等，补偿用新分配
> chunk ID 无碰撞）；part 暂存为 in-process 语义不变（gateway 重启即丢）。

### 1.5 退出条件
- 网关写入路径不再产生新 ChunkMap；ChunkMap 只读存量（阶段 4 删）。
  > 1.3c/§1.4 后三条写路径——单发 PUT 与 FUSE flush（`CommitChunkRefsModelAware`）
  > 与 S3 multipart complete（同一编排，见 §1.4）——全部落 V2 布局，网关写路径不再
  > 产生新 ChunkMap。
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

## 阶段 3：V1 EC 退役（删 ②） ✅ 完成

> 2026-08-15 完成。`writeECShardDirect` 的 fallback（authority 未接 / `PlanECWrite`
> 失败）改为**硬错误**（`chunkstore.ErrECUnavailable`，S3 映射 503）；`writeECChunk`
>（V1 整片 EC）与 `readECChunk` 的 `ECStripeID==""` 整片读分支已删除。同时
> `*metadata.PebbleStore` 新增 `PlanECWrite`/`RecordDirectEC`，结构性满足
> `chunkstore.ECWriteAuthority`（与 `*HTTPClient` 并列），metad `/api/v1/ec/plan-write`
> handler 委托之 —— 本地/PebbleStore 模式与 smoke EC 测试因此走**真实 direct-EC 路径**
>（encode K+M → `ReplicateECShard` → 持久 Complete stripe），不再依赖 V1 降级。

### 退出条件 ✅
- ✅ `chunkstore/ec.go` fallback 改为硬错误（`ErrECUnavailable`；authority 中途被清也硬失败）
- ✅ `writeECChunk` 已删除；`readECChunk` 无 `ECStripeID==""` 分支（全走 `ReadECShard`）
- ✅ 无路径产生 `ECStripeID==""` 的 EC chunk：三处写入点
  （`SwitchChunkToEC` 转换、`RecordDirect` 直写、datanode `ec_converter.go`）均设
  `ECStripeID`；仅未写入的 EC chunk 无 stripe，读写失败行为与退役前一致

### 行为变更
- fallback 删除 = 代价从"降级写成功"变"写失败"。对 ECConfig 桶是正确行为；需在发布说明写明。
- smoke `TestRepair_EndToEnd_EC`（4+2、6 节点）改走真实 direct-EC：注册节点报
  `ShardDiskCount=1`，kill 1 节点 → 5≥K=4 重建通过。

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
