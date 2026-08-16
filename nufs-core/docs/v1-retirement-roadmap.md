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
| 存储 tier / EC 存储类 | `ChunkMeta.Tier` + `ChunkMeta.ECGroup`/`ECStripeID` | 数据面 `ChunkMeta.ECGroup`/`ECStripeID`（权威源）+ 管理面 `ExtentMetaV2.Lifecycle`；`ExtentMetaV2.StorageClass` 为**写-闭环派生镜像** | ✅ 已接线（§1.5 判据重定义：StorageClass 读面不消费系有意避免双源漂移） |
| 心跳降级检测 | Chunk 状态机 + `batchUpdateChunkStatesCtx` | `ExtentMetaV2.Lifecycle`（`types.go:124`） | ✅（§1.4 第三刀：chunk 降级镜像 extent，`MarkExtentDegraded`） |
| **Scrubber**（校验/降级计数） | `ScanAllChunks`/ChunkMeta | extent 版 `ExtentScrubber`（`extent_scrub.go`） | ✅（§1.4 第四刀：校验 + Lifecycle 计数 + ReadyDegraded→Ready 恢复） |
| **修复**（TriggerRepair/队列） | ChunkMeta + repair queue | `TriggerExtentRepair` + `/repair/{id}` 队列（extent ID==chunk ID 不变式） | ✅（§1.4 第五刀：心跳盲区由 scrubber 兜底入队） |
| **bucket 配额** | `ChunkMeta.Size` 聚合 | 慢路径 `extentBytesForInode` 按 extent 聚合 + 快路径计数器 | ✅（§1.4 第六刀） |
| **备份/恢复** | ChunkMeta 清单 | 整 checkpoint 归档 + verify 层模型感知（extent 交叉校验，format 1→2） | ✅（§1.4 第七刀） |
| **S3 multipart** | 分片 → chunk 合并 | complete 流式合并落 V2 extent（复用单发 PUT 同一编排） | ✅（§1.4 首刀） |
| **孤儿 GC** | 元数据查 ChunkMeta | `stableInodeReferenceSnapshot` 模型感知引用集（per-inode resolve） | ✅（§1.4 第二刀，修 V2 extent-backed chunk 误删） |

> 结论（§1.5 收官）：阶段 0 的 ❌/⚠️ 项已全部在 §1.4 七刀闭环并附回归钩验证，本表按
> 代码事实更新。「存储 tier」行依 §1.5 判据重定义——机制真实承载 = `ChunkMeta.ECGroup`/
> `ECStripeID`（数据面路由）+ `ExtentMetaV2.Lifecycle`（管理面），`StorageClass` 是从
> 前者派生的镜像标注，读面零消费系有意避免双源漂移；物理 `StorageTier`（hot/warm/cold/
> archive）是另一套活跃机制（ChunkMeta.Tier 全链路真消费，非本 ⚠️ 指涉）。**删 ③ 前**
> 剩余仅"可表示未接线"类（稀疏/MVCC，见上两行）与 阶段 4 清理。

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
- [x] Scrubber extent 版（按 ExtentMetaV2 校验 + Lifecycle 计数）
- [x] 修复机制 extent 版（TriggerRepair 的 extent 语义 + 队列）
- [x] 心跳降级检测 extent 版（把 `ExtentLifecycle` 接进上报/降级路径）
- [x] bucket 配额按 extent 聚合（重建路径按 extent 聚合 + Unlink/Link 模型感知——见下注）
- [x] 备份/恢复按 extent 清单（`backup_verify.go` 模型感知——见下注）
- [x] S3 multipart 合并落 extent（gateway/s3/multipart.go——见下注）
- [x] 孤儿 GC 认 extent 布局

> 2026-08-16 完成（本刀，§1.4 收官——全部 ❌ 闭环）。备份本体=整 Pebble checkpoint
> 归档（V2 行随原始字节天然入账），真正 deferred 的是**校验层**：`inspectBackupCheckpoint`
> 只 decode V1 `InodeMeta`，V2 布局行（inline/pages）得出空 ChunkMap → `inode_chunk_references`
> 交叉校验空转，V2 backing chunk 从归档丢失 verify 照样 PASS。本刀改 V2-first 模型感知
> decode：inline/pages 收集被引用 extent，新增 `extent_references` 交叉校验——被 inode 引用的
> extent 必须有 `/extent-meta/` + `/chunk/`（数据在 `ID==ExtentID` 的 chunk 行）双行；
> `/extent-meta/`、`/extent-page/` 两个 keyspace 首次入账 `RecordCounts`（`ExtentMeta`/
> `ExtentPages`，manifest 升 `BackupFormatVersion` 1→2，旧 v1 清单与新 verify 严格 record-count
> 相等不再成立，须 re-manifest）。**顺带修复**生产 latent bug：`collectBackupFiles` 排除
> `LOCK`——fresh checkpoint（`CreateStandaloneCheckpoint`/raft `CreateBackupCheckpoint`）
> 出生时无 LOCK，而 `BuildBackupManifest` 内含 read-only `pebble.Open` 会补写 LOCK，导致同一
> 份 artifact build 时 N 文件、verify 时 N+1 文件 → `file set mismatch`（coordinator
> `verifyArtifact` 与 S3 `Publish` 的 `VerifyBackupArtifact` 都撞此坑）。marker.format-version
> 不可排除——它是 checkpoint 出生即有的组成部分，pebble 靠它识别目录为数据库，去掉后 restore
> fetch 的副本 `pebble.Open` 报 "database does not exist"。测试：`backup_extent_test.go` 真库
> fixture（V1 ChunkMap + V2 inline + V2 pages 三型文件）——PASS 记数断言 + 删 backing chunk/删
> extent-meta 行→build 拒绝 + golden round-trip（Publish→Restore→重开 `ResolveExtents` 回原
> extent ID 集）；回归钩双验（V1-only decode → 两拒绝测试红；names 去掉 extent_references →
> 11-check 计数断言红）已实际 revert 验证。

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

> 2026-08-16 完成（第二刀，修数据丢失 bug）。**修复对象**：`ChunkGC` 的引用集
> `stableInodeReferenceSnapshot`（`metadata/chunk_tombstone.go`）此前只 decode `InodeMeta`
> 收 `ChunkMap`，V2 布局 inode（InlineExtent/ExtentPages）decode 出空 ChunkMap → 其
> extent-backed chunk（数据在 `ID==ExtentID` 的 chunk 行）被 GC 判孤儿 → tombstone + 隔离后
> purge **物理删数据**——而 GC 生产默认 10min 周期（`cmd/metad/main.go` `gc-interval`），
> V2 写自 §1.3c 起是默认路径，属活跃数据丢失。**修复**：同一 `/inode/` 扫描改成
> 模型感知——每行先按 `InodeMetaV2` decode，`Layout != LayoutEmpty`（V1 行 decode 出
> Layout==0，`ResolveExtents` 探测判别器）即按 V2 收引用：inline → `InlineExtent.ID`；
> pages → 复用 `ExtentPageStore.ResolveExtents` per-inode（COW root 回走），收集每个
> `ExtentRef.ExtentID`；其余仍走 V1 `decodeReferencedInode` 收 ChunkMap。**选 per-inode
> resolve 而非直接扫 `/extent-page/` prefix**：扫 prefix 会把已删 inode 遗留的旧 COW page
> 计入引用（V2 孤儿永不回收，存储泄漏）；per-inode 只数 live V2 inode 真实引用的 extent。
> 测试：`TestChunkGC_KeepsV2ExtentBackedChunks`（inline/pages 双 layout + 真孤儿，断言
> `TombstonesCreated==1`；无修复时引用集为空 → `OrphanChunks==2` 直接打死）。
> **Deferred**：epoch 栅栏语义未动（epoch 本就不被 bump，per-inode CAS 是守门机制，V1/V2
> 行为一致）；备份 verify 的 extent 交叉校验仍在待办。

> 2026-08-16 完成（第三刀）。**心跳降级检测 extent 版**：激活 `ExtentLifecycle`——此前
> `LifecycleReadyDegraded` 零写者零读者（纯装饰字段），本刀把它接进降级路径。生产改动两处：
> `metadata/inode_v2_serving.go` 新增 `MarkExtentDegraded`（读 /extent-meta 行，`Lifecycle==Ready`
> 才翻 `ReadyDegraded`，幂等 + 单调守卫：ECConverting 不动、V1 chunk 无行不造）；
> `metadata/pebble_store.go` 的 `batchUpdateChunkStatesCtx` 在 chunk 降级成功后镜像 extent。
> **条件用 `chunk.State==ChunkDegraded` 而非 `changed`**：心跳是 `lastKnownState` 差分，mark
> 失败整批返错 → lastKnownState 不前进 → datanode 下轮重发同差分 → 镜像重试自愈（fail-closed，
> 与引用集收集同风格）。**路由关键**：chunk 行与 /extent-meta 行都按 `inode:{id}` 路由共置 →
> 降级镜像是一次同 store 写，无需跨 shard 扫描、无需改 ShardedStore/metad handler
> （`ShardedStore.Heartbeat` + `ChangeCorrupt`/`markNodeReplicasFailedAndRepair` 全汇入同一
> `batchUpdateChunkStatesCtx`，单钩子全覆盖）。测试 `metadata/extent_lifecycle_test.go`：
> inline/pages 双布局断言 chunk Degraded + extent ReadyDegraded、重复 ReplicaFailed 幂等、
> V1 降级不造 extent 行（GetExtentMeta→ErrExtentNotFound）、单调守卫。回归钩已验证：
> stash 掉钩子后 extent Lifecycle 停留 0 直接红。
> **行为注记**：升级回 Ready 不做（与 V1 chunk 单向降级一致，`:2781` 注释明言升级留
> scrubber/anti-entropy；extent 恢复方向归 Scrubber extent 刀）；`MarkExtentColdEC` 写
> inode 行 `InlineExtent.Lifecycle` 不改 /extent-meta 行——两者可发散（既有行为非本刀引入），
> 本刀以 /extent-meta 行为 Lifecycle 权威。
> **发现未修（独立刀候选）**：`--shards N>1`（默认 1，`--raft=false` 专属）下
> `AllocateChunk`（inode 路由）与 `GetChunk`/`ReportChunkState`/`CommitChunk`（chunk-key
> 路由）约半数分叉 → 分叉 chunk 对读/心跳不可见，心跳降级链本就失效。既有潜在问题，
> 测试只覆盖单 store 路径；建议独立排查 `AllocateChunk` 与 chunk-key 路由对齐。

> 2026-08-16 完成（第四刀）。**Scrubber extent 版**：激活的 `ExtentLifecycle` 从"只读
> 装饰"闭环成真机制——镜像 V1 `Scrubber`（production.go，chunk 级校验）在 extent 层做
> **校验 + Lifecycle 计数 + 恢复**。生产改动四处：`pebble_store.go` 新增 `ScanExtents`
>（镜像 `ScanAllChunks`，扫 `/extent-meta/`）与 `ScrubExtents`（count-only，供 ops 端点）；
> 新建 `extent_scrub.go` 的 `ExtentScrubber`（`Scan` 逐 extent：Lifecycle 分布计数 →
> `GetChunk(ext.ID)` 校验 backing chunk：`ErrChunkNotFound`→`Dangling`（孤儿行，良性非
> 数据丢失，chunk 行才是权威）、`ECGroup` 非 nil→跳过副本判断（EC 分片健康归 EC healer）、
> 无健康副本→`Unhealthy`；**恢复**：`LifecycleReadyDegraded` + 全副本 `ReplicaReady` →
> `chunk.State=ChunkReady`（`UpdateChunk`）+ `ext.Lifecycle=LifecycleReady`
>（`putExtentMeta`），一起恢复防两模型发散）；`service.go` 的 ServiceBundle 加
> `ExtentScrub` 字段，与 V1 `Scrub` 同 `ScrubInterval` 门控同启同停；`cmd/metad/ops_scrub.go`
> 的 `handleScrub` 响应加 `extents_scanned/ready/degraded/dangling/unhealthy`。
> **恢复动机**：`batchUpdateChunkStatesCtx`（第三刀注释 `:2781`）只降级从不升级，真实
> 修复路径（`repairByAddingReplica`→`ReportChunkState(target, ReplicaReady)`）也不翻
> chunk.State → 已修复 chunk 全副本 Ready 但 State 仍 Degraded、extent 仍 ReadyDegraded，
> 正是本刀恢复键（**副本健康**，非 chunk.State）。测试 `metadata/extent_scrub_test.go`：
> 复用第三刀 `newExtentDegradeFixture` + 模拟修复完成（全副本置 Ready、State 保持
> Degraded）→ Scan 断言 Recovered==1 + chunk Ready + extent Ready + 重扫幂等；计数 /
> 副本仍 Failed 不恢复 / EC 跳过恢复 / Dangling+Unhealthy 标记 / StartStop 周期真跑
> Scan（slog recovered=1→0）; bundle 门控测试（10ms→running，0→nil）。回归钩已验证：
> 临时禁掉恢复块 → `recovered = 0, want 1` 直接红。cmd/metad `ops_scrub_test.go` 断言
> 端点新字段。**Deferred**：① 修复触发不做（Unhealthy 只计数，`/repair/{id}` 触发归
> "修复机制 extent 版"刀）；② 孤儿 /extent-meta 行 = Dangling 良性（`Unlink` 无 extent-meta
> 清理路径，删文件后行成孤儿是常态），真清理归未来 extent-GC 刀；③ 恢复写经
> `applyViaRaft` 自动转发 leader，follower 安全（与 GC 同款）；④ V1 chunk（无 /extent-meta
> 行）不碰，两 scrubber 并存互补。

> 2026-08-16 完成（第五刀）。**修复机制 extent 版**：第四刀把 Unhealthy extent 停在"只计数
> 不触发"（deferred），本刀把**触发 + 队列的 extent 语义**补齐。生产改动三处：
> `metadata/pebble_store.go` 新增 `TriggerExtentRepair(ctx, extentID)`——先 `GetExtentMeta`
> 校验 extent 行（`ErrExtentNotFound` 快速失败，不给已被 GC 的 chunk 排队），再写
> `/repair/{id}` 队列（extent ID == chunk ID 不变式，key 复用），`Reason: "extent_unhealthy"`
> + `Priority: 1` 与心跳/rebalance 触发（`Reason: "triggered"`）可区分；`metadata/extent_scrub.go`
> 的 `Scan` 把 Unhealthy 分支（原 `result.Unhealthy++; return nil`）改为触发——`TriggerExtentRepair`
> 成功 `RepairTriggered++`、失败仅 `slog.Error` 不中止扫描，`Start` slog 加 `repair_triggered`；
> `cmd/metad/ops_repair.go` 的 `handleTriggerRepair` 同时接受 `chunk_id` 与 `extent_id`
> （extent 路径 `ErrExtentNotFound`→404，两者皆 0→400 "chunk_id or extent_id required"），
> `handleRepairQueue` 输出改为逐任务注解——`repairQueueEntry{RepairTask; IsExtent;
> ExtentLifecycle}`，经 `extentLifecycleName` switch 映射
> ready/ready_degraded/migrating/deleting/deleted/ec_converting/unknown，响应对 Go 消费者
> 向后兼容（未知字段忽略，`HTTPClient.GetRepairQueue` 照旧 decode `[]RepairTask`）。
> **触发缺口（本刀动机）**：心跳链 `markReplicaFailedAndRepair` 只在副本*迁移*到 Failed 时
> `TriggerRepair`——一个 chunk 未经历该迁移就落到全副本 Failed（如节点整体离线）会停在
> Unhealthy 永不入队。Scrubber 兜底入队，datanode RepairWorker 找到健康源即重复制，下一轮
> scrub 把全副本 Ready 的 extent 一起恢复回 Ready——与第四刀闭环。**Deferred**：① EC extent
> 仍跳过触发（分片健康归 EC healer）；② Dangling（无 backing chunk）不触发（无物可修）；
> ③ `TriggerExtentRepair` 只加 PebbleStore、不扩 `RepairService` 接口/ShardedStore/HTTPClient
> ——调用方仅 metad 内 `ExtentScrubber` 与 ops handler，无远程/跨 shard 调用方，避免死代码。

> 2026-08-16 完成（第六刀）。**bucket 配额按 extent 聚合**：快路径计数器
> （`UseBucketStats=true`）本就工作且有测试（V2 写经 `putWithBucketStats` 做 inode Size 差分，
> `gateway/s3/quota_test.go` 全覆盖）；真正 deferred 的重建路径——`ComputeAllBucketUsage`
> 慢路径 + `ensureBucketStats` 迁移种子（`ops_prometheus.go` 的指标源也走它）——此前只
> decode V1 `InodeMeta` 读 Size：对 V2 行靠 msgpack 字段名别名碰巧读对，但**不是按 extent
> 聚合**且零测试。本刀：慢路径改模型感知，新 helper `extentBytesForInode` 按布局聚合
> （inline→`InlineExtent.LogicalLen`，pages→`ResolveExtents` 逐 extent 读 `/extent-meta/` 的
> `LogicalLen` 求和、缺行回退 `in.Size` 防欠计；V1/空行回退 `in.Size`）。**顺带修了
> `Unlink`/`Link` 的 V2 行损坏 bug**：NLink>1 分支此前把共享 `/inode/` 行按 V1 `InodeMeta`
> 重编码，静默剥掉 Layout/InlineExtent/ExtentRoot——硬链接过的 V2 文件数据引用丢失；现镜像
> `UpdateInode` 模型感知（V2 行按 `InodeMetaV2` 写回保留布局，V1 行按 `InodeMeta` 写回保留
> ChunkMap），扣减改用 `v2.Size`。另修 `putWithBucketStats` 陈旧注释（AppendExtent 已计数，
> 只剩 PromoteToPages 用 `s.Put`）。测试 `metadata/bucket_usage_extent_test.go`：慢路径
> V1+inline+pages 混布聚合（Size 故意漂移到 9000/9999 证明按 extent 读而非计数器）、悬垂
> extent 回退 in.Size、`ensureBucketStats` 重开迁移种子、Unlink V2 扣减归零、Link/Unlink
> 跨硬链接保 layout（回归钩双验：慢路径退回读 Size 与 Link 退回 V1 重编码均红）。门禁
> `verify-docker -l fast` PASS（metadata 全包 macOS 直跑撞 EADDRNOTAVAIL，Linux 容器干净）。
> **已知未修**：慢路径对象计数经 `walkUsageTree` 对硬链接双计（与快路径唯一 inode 语义
> 不一致，既有设计取舍非本刀引入）；`AppendExtent` 是 restore/HTTP-only 面。

### 1.5 退出条件 ✅（2026-08-16 §1.5 收官）

- [x] 网关写入路径不再产生新 ChunkMap；ChunkMap 只读存量（阶段 4 删）。
  > 1.3c/§1.4 后三条写路径——单发 PUT 与 FUSE flush（`CommitChunkRefsModelAware`）
  > 与 S3 multipart complete（同一编排，见 §1.4）——全部落 V2 布局，网关写路径不再
  > 产生新 ChunkMap。§1.5 收官裁决三处残余写面（**只裁决不删**，删除归阶段 4）：
  > ① `AllocateChunksBatch`（`pebble_store.go:2193`）瞬态 append——每写发生、随后被
  > model-aware commit 原子改写为 extent 布局，**非持久** ChunkMap 生产者，保留；
  > ② `CommitChunkRefsModelAware` V1 fallback（`inode_v2_serving.go:326`）——仅 legacy
  > V1 行 / 空 ref 新文件触发（当前全接线 service 均实现 `ExtentInodeService`，V2 行
  > 不触发），防御性保留（回滚/parity 兼容）；③ `CreateObjectWithChunks`
  > （`pebble_store.go:2584`）——无 metad HTTP 路由、当前接线不可达的 V1 路径，
  > 阶段 4 删除候选。
- [x] 阶段 0 的 ❌/⚠️ 项全部闭环。
  > 见上方阶段 0 汇总表——§1.4 七刀全闭环；「存储 tier ⚠️」按代码事实判据重定义：
  > 机制真实承载 = `ChunkMeta.ECGroup`/`ECStripeID`（数据面）+ `ExtentMetaV2.Lifecycle`
  > （管理面），`StorageClass` 是写-闭环派生镜像，**不加人为消费者**——读路由改判
  > StorageClass 只会复制权威源、制造双源漂移风险（已评估后拒绝）；物理 `StorageTier`
  > 是独立活跃机制，非本 ⚠️ 指涉。
- [x] 小文件读改写、promote 后读写、稀疏文件、EC 转换、配额、备份全绿。
  > 测试矩阵：`datanode/v2_small_store_test.go`、`gateway/s3/dual_model_cross_test.go`、
  > `gateway/s3/multipart_dual_model_test.go`、`metadata/bucket_usage_extent_test.go`、
  > `metadata/backup_extent_test.go`、`metadata/ec_extent_marking_test.go`、smoke
  > `TestRepair_EndToEnd_EC`；门禁 `verify-docker -l fast` 全绿。

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

- [x] JSON/msgpack 双格式共存（`docs/architecture/data-organization.md:60`）→ 只留 msgpack
  > ✅（2026-08-17 收官）：**只写 msgpack**——5 处活 write 点翻 msgpack（`pebble_store.go:746`
  > initRootInode / `:3723,:3748,:4170` repair / `audit.go:211`），QueryAudit 直接 JSON 读者改
  > `unmarshalValue`（:257）；删死代码 `putJSON`/`putJSONBatch`/`EncodeSetJSON`/
  > `bucketNameByRoot`/`codecJSON` 常量及其分支（22 处测试 seed 同步翻 `putMsgpack`）；
  > 读嗅探保留（旧 raft 日志 / 旧库 / 旧备份 catalog 需 JSON 兼容），不做行级迁移工具。
  > 新增 `codec_test.go` 回归：msgpack 往返 + legacy JSON 行嗅探读兼容 + msgpack 首字节
  > 0x8n 与 `{`/`[` 无碰撞；legacy JSON 读兼容另由 `backup_verify_test.go` JSON fixture
  > 测试独立证明。验证：verify-docker -l fast PASS；`getJSON` 别名 33 处存活调用点保留
  > （重命名归下一刀归零）。
- [x] V1 引擎/EC 的 legacy 测试迁移或删除（~~`datanode/chunkstore_test.go` 等~~）
  > 阶段 2/3 已全删（`git log --diff-filter=D` 证实 20 个文件），其余现存测试逐文件核对
  > 均引用存活符号；本条剩 `-race` 全量绿验证，与 JSON/msgpack 全量验证一并做。
- [ ] 全仓 grep 清理 `storage-version`、`ECStripeID==""`、`ChunkMap` 写路径引用
  > §1.5 探子审计：Go 侧 `storage-version` 已净（flag 随阶段 2 删）；`ECStripeID==""`
  > 生产分支阶段 3 已退役（现仅 `!= ""` 判别符 + 测试断言 + 注释）；本刀清掉 6 个
  > deploy/soak 文件的 `--storage-version=v2.1` 残留（运行即 flag 解析退出）。
  > 剩余：`CreateObjectWithChunks`（V1 不可达路径）删除 + `ChunkMap` 写路径归零。

### 退出条件
- 全仓无 V1 引用；`-race` 全量绿。

---

## 依赖与风险

- **阶段 0 硬门禁 ✅（2026-08-16）**：§1.4 七刀全闭环（scrubber/修复/配额/备份/multipart/
  孤儿 GC 的 extent 版 + 心跳降级/存储类判据），阶段 0 汇总表已按代码事实更新。
- **上线门禁 ✅（2026-08-16 §1.5 收官）**：阶段 0 + 阶段 1 完成。③ ChunkMap 仍是网关
  存量读路径（阶段 4 删除前保留），三处残余写面裁决见 §1.5 注记（删除归阶段 4）。
- 阶段 2 的协议面等价抽查是硬门禁（V2.1 声称 parity ≠ 已验证）。
- 阶段 3 的 fallback 硬错误是行为变更，需显式决策。
- 顺序：**0 + 1（上线门禁）→ 2/3（可并行收尾）→ 4**。
