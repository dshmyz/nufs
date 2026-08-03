# V2.1 接入写路径 —— 影响面调研（只读，未改代码）

日期：2026-08-03
性质：可行性调研。未改任何代码，结论基于源码现状。
目的：澄清"把 V2.1 引擎接进写路径"的真实差距，供你决定下一步。

---

## 0. 一个必须先纠正的认知

我之前几次说"V2.1 存在但完全没接进生产路径"——**这个说法不准确**。读源码后发现：

**V2.1 引擎已经通过 `--storage-version=v2.1` 标志接到了 datanode 的真实 TCP 服务上。**

- `cmd/datanode/main.go`：`main()`(L29) 按 `cfg.StorageVersion`(L231) 分发
  - 默认 `v1` → `runDataNode`(L209) = 旧 ChunkStore 路径
  - `--storage-version=v2.1` → `runDataNodeV21`(L454) = V2.1 路径
- `runDataNodeV21`：每盘建一个 `segment.Store`，把 `stores[0]` 包成 `datanode.NewV2Store`([server_v2.go](nufs-core/datanode/server_v2.go))，跑真实 TCP `Server` + 心跳。
- [server_v2.go](nufs-core/datanode/server_v2.go) `V2Store` 把 `storage.Store` 适配成 `LocalChunkStore`（TCP server 的存储接口 [server.go:25](nufs-core/datanode/server.go:25)）。

所以诚实的现状是：**V2.1 是"flag 可切换 + MVP 适配器"地接上了 datanode 写服务，但不是默认，也不是完整 parity。**

---

## 1. 现状接线总览

```
默认(不传 --storage-version):                    可选(传 --storage-version=v2.1):
  gateway/FUSE ──► chunkstore.DatanodeChunkStore     同左(上层不变)
        │  ChunkID+Replicas[]/ECGroup                        │
        ▼                                                  ▼
  WritePipeline 扇出(副本/EC)  ──► 每个 datanode TCP      每个 datanode TCP
        │                           │                          │
        ▼                           ▼                          ▼
  同一 wire 协议 ReplicateChunk ──► server.dispatch           server.dispatch
                                     │ server.store =          │ server.store =
                                     ▼  legacy ChunkStore       ▼  V2Store(segment.Store)
```

**关键点**：datanode 侧 `server.dispatch` 只依赖 `LocalChunkStore` 接口（[server.go:25](nufs-core/datanode/server.go:25)），旧 `ChunkStore` 和新的 `V2Store` 都实现它。**上层（gateway/FUSE 扇出、副本/EC、metadata 的 ChunkID+replicas 模型）在两条路径下完全不变** —— 变的只是单节点落盘层。

---

## 2. 已经打通的部分

| 环节 | 状态 | 证据 |
|---|---|---|
| datanode TCP 服务抽象 | ✅ 有可插拔接口 | `LocalChunkStore`，新旧都实现 |
| V2.1 单节点落盘接入 TCP | ✅ 有 | `V2Store`：Write/Read/Delete/Stat → `segment.Store` |
| 每盘一个 segment.Store | ✅ 有 | `runDataNodeV21` 一个 disk 一个 store |
| 心跳让放置引擎选中该节点 | ✅ 有 | `runDataNodeV21` 起 `HeartbeatReporter` |
| 可通过 flag 切换 | ✅ 有 | `--storage-version=v2.1` |

---

## 3. 差距 / 需要补的部分（按严重度）

### A. MVP 适配器是"简化/桩"，非完整语义（server_v2.go）
- **Generation 固定 = 1**：`V2Store` 把 chunkID 映射成 extent、`Generation: 1` 硬编码。V2.1 的 generation fencing、幂等重写要靠 generation 变化，现在被锁死在 1 —— 覆盖重写同一 chunk 会命中 `ErrStaleGeneration` 或产生错误语义。
- **Seal = no-op**：V2.1 通过 commit log 原子提交 extent，所以 `Seal` 返回 0；但上层 `handleReplicate` 落盘后调用 `Seal` 期望设 CRC。行为差异需验证读路径是否照常校验（V2.1 逐帧 CRC 已内置，可能 OK，但要测）。
- **ListChunks / Stats / ChunkStateSnapshot / StateVersion / WriteErrorRate 都返回空**：心跳靠 `ChunkStateSnapshot`/`StateVersion` 报告副本状态。V2.1 心跳直接喂 `v2Store`，副本状态恒空 → **元数据的副本健康/状态可能失真**，影响放置与读failover。

### B. 只用了多盘里的第一个盘
`runDataNodeV21` 每盘建 store，但只 `NewV2Store(stores[0])`，后面几个盘没接进 TCP。多盘是"建了不用"，容量/失败隔离未打通。

### C. 跨节点副本 / EC / 放置仍在旧层
V2.1 引擎本身只做单节点落盘。跨节点写到多副本、EC 分片、placement 选取仍由上层 `chunkstore` + metadata(Task #24-#33 建的 Metadata V2) 承担。而 `V2Store` 走的是旧的 `ChunkID+replicas` 协议 —— **Metadata V2 的 extent page / placement group / epoch 并没有参与这条 serving 路径**。

### D. gateway / FUSE 侧完全没有 V2 分支
网关和 FUSE 仍只依赖 `chunkstore.ChunkStore` 接口（`WriteChunk/ReadChunk/ReadChunkRange`,传 `*metadata.ChunkMeta`）。要吃到 V2.1 的增量控制面 / EC 事务 / 小文件段收益，上层模型得换成 Metadata V2,这是另一个大的切开面（不是本调研的落盘层）。

---

## 4. 影响面清单（如果要推进）

### 落盘层（改动小，影响明确）
1. `datanode/server_v2.go` —— 补齐 generation 语义、Seal、Stats/ListChunks 至少给可用值；多盘聚合进一个 server。
2. `cmd/datanode/main.go` `runDataNodeV21` —— 多盘都接、默认值/flag 决策（要不要把 v2.1 设为默认）。
3. 端到端验证：`--storage-version=v2.1` 起 datanode + gateway 读写，确认副本状态、读 failover 正常。

### 上层模型（改动大，需要单独规划）
4. `chunkstore` / gateway / FUSE / metadata 从"ChunkID + replicas/ECGroup"切到"ExtentID + generation + placement/EC(Metadata V2)"。
5. EC 与副本的"由谁负责、何时转换"在 V2.1 语义下重排。

---

## 5. 建议的下一步（取决于你想切多深）

- **选项 1（最小）**：只把选项 A/B 补齐，把 `--storage-version=v2.1` 变成可靠可用的单节点落盘后端（仍走旧上层模型）。可以从 datanode 侧独立完成、独立过门禁。
- **选项 2（完整 parity，方案原文）**：落盘层 + Metadata V2 上层一起切，再删旧栈。工作量大，需先做上层影响面（本调研只覆盖到落盘层/协议层）。

需要我把选项 1 的偏差点（A/B)逐条验证成可执行计划，还是先继续把上层(chunkstore/gateway/metadata→Metadata V2)的影响面也摸清？
