# NUFS 文件存储方式详解

> 本文档描述 NUFS 分布式文件系统的文件存储内部机制，包括磁盘布局、数据格式、写入/读取/复制路径和元数据模型。

---

## 1. 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      客户端 (S3/FUSE/CLI)                        │
└──────────────────────────┬──────────────────────────────────────┘
                           │ HTTP (S3 API) / FUSE
┌──────────────────────────▼──────────────────────────────────────┐
│                    网关层 (gateway/s3, gateway/fuse)              │
│  解析请求、鉴权、缓存、将文件操作转为元数据+数据操作              │
└────┬───────────────────────────────────┬────────────────────────┘
     │ metadata.MetadataService (HTTP)   │ datanode.Client (TCP)
     ▼                                   ▼
┌──────────────────────┐   ┌──────────────────────────────────────┐
│   元数据服务 (metad)  │   │     数据节点 (datanode)               │
│   PebbleStore + Raft  │   │  ChunkStore + Replicator + Repair    │
│   管理:               │   │  管理:                               │
│   命名空间(目录树)    │   │  本地 chunk 文件                     │
│   Inode → ChunkMap    │   │  链复制 + 异步复制                   │
│   ChunkMeta/副本信息  │   │  反熵修复                            │
│   集群节点/放置策略   │   │  磁盘管理 + WAL                      │
│   修复队列/租约/GC    │   │  心跳上报                            │
└──────────────────────┘   └──────────────────────────────────────┘
     │ 内部通信: HTTP (metadata.HTTPClient)
     ▼
┌──────────────────────┐
│   datanode 通过       │
│   HTTPClient 连接     │
│   metad 做元数据操作   │
└──────────────────────┘
```

---

## 2. 元数据存储 (PebbleStore)

### 2.1 后端引擎

元数据唯一后端是 **Pebble**（CockroachDB 的 LSM-Tree KV 存储引擎），可选内嵌 **hashicorp/raft** 实现分布式共识。

### 2.2 键空间

所有元数据以 JSON 序列化后存储在 Pebble 中，键前缀用来区分数据类别：

```
Key                       Value (JSON)           说明
─────────────────────────────────────────────────────────
/bucket/{name}           BucketInfo              S3 Bucket 定义
/ns/{parent_id}/{name}   DirEntry                目录项: 父目录 → 子文件/目录
/inode/{id}              InodeMeta               Inode (文件/目录/符号链接)
/chunk/{chunk_id}        ChunkMeta               Chunk 元数据 (副本列表 + 状态)
/node/{node_id}          NodeInfo                数据节点注册信息
/policy/{bucket}         PlacementPolicy         放置策略 (副本数/拓扑约束)
/repair/{chunk_id}       RepairTask              修复队列任务
/inode/block/{block_id}  SmallFileBlockMeta      小文件块元数据
```

### 2.3 InodeMeta（文件/目录元数据）

每个文件、目录或符号链接都有一个 Inode，存储为 `/inode/{id}`：

```go
type InodeMeta struct {
    ID      InodeID      // 唯一 ID (uint64)
    Type    FileType     // 0=普通文件, 1=目录, 2=符号链接
    Size    int64        // 文件大小 (字节)
    NLink   uint32       // 硬链接计数
    UID     uint32       // POSIX 用户 ID
    GID     uint32       // POSIX 组 ID
    Mode    uint32       // POSIX 权限 (如 0644)
    CTime   int64        // 创建时间 (Unix ns)
    MTime   int64        // 修改时间 (Unix ns)
    ATime   int64        // 访问时间 (Unix ns)
    Chunks  []ChunkRef   // 有序 Chunk 引用列表 (大文件)
    Symlink string       // 符号链接目标路径
    XAttrs  map[string][]byte  // 扩展属性
}
```

`ChunkRef` 是文件的数据块索引：

```go
type ChunkRef struct {
    ID      ChunkID   // Chunk ID
    Offset  int64     // 在文件中的偏移量
    Length  int64     // Chunk 数据长度
    Version int64     // 版本戳 (用于一致性)
}
```

### 2.4 ChunkMeta（数据块元数据）

每个 Chunk 存储为 `/chunk/{chunk_id}`：

```go
type ChunkMeta struct {
    ID         ChunkID       // 唯一 ID
    Size       int64         // Chunk 数据大小
    State      ChunkState    // ChunkSealing/Sealed/Ready/Degraded/Orphan
    Replicas   []ReplicaInfo // 副本列表
    ECGroup    *ECGroupInfo  // 纠删码组 (可选)
    CreateTime int64         // 创建时间
    Checksum   uint32        // CRC32C 校验和
}
```

ReplicaInfo 记录每个副本所在的节点及状态：

```go
type ReplicaInfo struct {
    NodeID   NodeID       // 数据节点 ID
    Addr     string       // 节点 TCP 地址
    State    ReplicaState // Syncing/Ready/Stale/Failed
    DiskPath string       // 磁盘路径
}
```

### 2.5 Chunk 生命周期

```
ChunkSealing (0)   → 正在写入数据 (初始状态)
     ↓  写入完成 + CommitChunk
ChunkSealed  (1)   → 写入完成，等待副本确认
     ↓  所有副本 Ready
ChunkReady   (2)   → 全部就绪 (正常服务)
     ↓  副本丢失
ChunkDegraded(3)   → 副本不足，修复中
     ↓  GC 检测到无 Inode 引用
ChunkOrphan  (4)   → 孤儿 Chunk (GC 清理)
```

### 2.6 ReplicaState（副本状态）

```
ReplicaSyncing (0) → 初始写入/复制中
ReplicaReady   (1) → 同步完成，可用
ReplicaStale   (2) → 落后于主副本
ReplicaFailed  (3) → 损坏/不可达
```

### 2.7 NodeState（节点状态）

```
NodeOnline    (0) → 正常运行
NodeDraining  (1) → 正在下线 (迁移数据中)
NodeOffline   (2) → 心跳超时
NodeFailed    (3) → 不可恢复故障
```

---

## 3. 命名空间（目录树）

### 3.1 目录树实现

NUFS 使用传统的树形目录结构，以 InodeID = 1 为根目录：

```
/ (RootInodeID=1)
├── bucket-1/           (InodeID=101)
│   ├── dir/            (InodeID=102)
│   │   └── file.txt    (InodeID=103)
│   └── photo.jpg       (InodeID=104)
└── bucket-2/           (InodeID=105)
```

目录项的存储：

```
/ns/1/bucket-1      → DirEntry{InodeID:101, Type:Directory}
/ns/101/dir         → DirEntry{InodeID:102, Type:Directory}
/ns/101/photo.jpg   → DirEntry{InodeID:104, Type:Regular}
/ns/102/file.txt    → DirEntry{InodeID:103, Type:Regular}

/inode/101          → InodeMeta{Type:Directory, NLink:2, ...}
/inode/103          → InodeMeta{Type:Regular, Size:4096, Chunks:[...], ...}
```

**Lookup 流程**：查询 `/ns/{parent_inode}/{name}` 拿到 DirEntry（含 child inode ID），再查询 `/inode/{child_id}` 拿到完整的 InodeMeta。

**Rename 原子性**：通过 Pebble Batch 原子地创建新 `/ns/` 条目并删除旧条目，中间状态对外不可见。

### 3.2 Bucket 与目录的关系

S3 Bucket 本质上是一个特殊目录：

```go
type BucketInfo struct {
    Name         string         // Bucket 名称
    RootInode    InodeID        // 根目录 InodeID
    Policy       PlacementPolicy // 放置策略
    CreationDate time.Time
}
```

每个 Bucket 在创建时自动分配一个目录 Inode 作为其根。Bucket 的放置策略控制其文件的所有 Chunk 如何分布。

---

## 4. 数据节点磁盘布局

### 4.1 目录结构

```
{DataDir}（如 /var/lib/dfs/data）
├── chunks/                # Chunk 存储目录
│   ├── 00/               # Shard 0x00
│   │   ├── 0000000001.dat  # Chunk 数据文件
│   │   ├── 0000000001.meta # Chunk 元数据边车 (JSON, 可选)
│   │   ├── 0000000042.dat
│   │   └── ...
│   ├── 01/               # Shard 0x01
│   ├── 02/
│   ├── ...
│   └── ff/               # Shard 0xFF (256 个分片目录)
└── wal/                  # Write-Ahead Log 目录
    └── wal.log           # WAL 文件
```

**分片算法**：`shard = chunk_id % 256`，将 Chunk 均匀分布在 256 个目录下，避免单个目录文件过多（`MaxShards = 256`）。

### 4.2 Chunk 文件格式 (.dat)

每个 `.dat` 文件包含 20 字节的固定头部 + 原始数据：

```
偏移  大小  字段              说明
──────────────────────────────────────────
0     4     Magic "DFS\x01"  文件魔数 (ChunkFileMagic)
4     8     ChunkID          大端 uint64
12    4     DataLength       数据体长度（大端 uint32）
16    4     CRC32 Checksum   校验和（0 = 未 seal）
20    N     Data             原始 Chunk 数据

Header 共 20 字节
```

**最大 Chunk 大小**：64MB（`MaxChunkSize = 64 * 1024 * 1024`）。

### 4.3 元数据边车文件 (.meta)

可选的 JSON 边车文件，内容为 `LocalChunkInfo`，在启动时加速扫描（避免读取所有 `.dat` 头部）：

```json
{
  "chunk_id": 42,
  "state": 2,
  "size": 1048576,
  "checksum": 287454020,
  "access_time": 1700000000
}
```

### 4.4 Write-Ahead Log (WAL)

WAL 用于保证写入的崩溃恢复能力。位于 `{DataDir}/wal/wal.log`。

**日志条目格式**：

```
Magic:  "WAL1" (4B)
DataLen: uint32 (4B)
ChunkID: uint64 (8B)
Op:      byte   (1B)
CRC:     uint32 (4B)
Data:    [可变]
```

**操作类型**：

| Op | 名称 | 说明 |
|----|------|------|
| 0x01 | walOpWrite | 写入开始 |
| 0x02 | walOpDelete | 删除 Chunk |
| 0x03 | walOpCommit | 确认写入完成 |

**写入流程**：
1. 写入 WAL Entry (Op=Write) → fsync
2. 写入 Chunk 数据文件 → fsync
3. 写入 WAL Entry (Op=Commit) → fsync

**恢复流程**：启动时扫描 WAL，找到有 Write 但没有 Commit 的条目 → 这些是未完成的写入，可以清理或重做。

### 4.5 启动扫描

数据节点启动时：
1. 扫描全部 256 个 shard 目录，收集所有 `.dat` 文件名
2. 读取每个文件的 20 字节头部，解析 ChunkID、大小、校验和
3. 如果有 `.meta` 边车文件，读取补全信息
4. 重建内存索引 `map[ChunkID]*LocalChunkInfo`
5. 恢复 WAL：清理未 commit 的写入

---

## 5. 写入路径

### 5.1 端到端流程

```
Client (S3 PUT /bucket/key)
    │
    ├── 1. 网关层: CreateFile(父目录, key, 权限)
    │
    ├── 2. 元数据层: AllocateChunk(inodeID, offset, policy)
    │      ├── PlacementEngine.PlaceChunk() → 选择 N 个副本节点
    │      ├── 创建 ChunkMeta{State:ChunkSealing, Replicas:[N1,N2,N3]}
    │      └── 原子 Batch: 写入 ChunkMeta + 更新 Inode.ChunkMap
    │
    ├── 3. 并行写入所有数据节点 (TCP)
    │      ├── 连接 N1: WriteChunk(chunkID, data)
    │      ├── 连接 N2: WriteChunk(chunkID, data)
    │      └── 连接 N3: WriteChunk(chunkID, data)
    │
    ├── 4. 元数据层: CommitChunk(chunkID, checksum)
    │      └── ChunkMeta.State = ChunkSealed
    │
    ├── 5. 确认所有副本 Ready
    │      └── ChunkMeta.State = ChunkReady
    │
    └── 返回客户端
```

### 5.2 数据节点写入 (ChunkStore.Write)

```
ChunkStore.Write(chunkID, data)
    │
    ├── 1. 获取写信号量 (默认最多 64 并发写)
    │
    ├── 2. WAL.Write(Op=Write) → fsync（保证崩溃可恢复）
    │
    ├── 3. 计算 shard = chunkID % 256
    │
    ├── 4. 创建分片目录 (如 00/) 如果不存在
    │
    ├── 5. 写入 .dat 文件:
    │      ├── [DFS\x01]     魔数
    │      ├── [chunkID]     Chunk ID
    │      ├── [dataLen]     数据长度
    │      ├── [0]           checksum (暂为 0)
    │      └── [data]        原始数据
    │      └── fsync()
    │
    ├── 6. 更新内存索引 LocalChunkInfo{State:LocalWritten}
    │
    ├── 7. 写入 .meta 边车 (JSON)
    │
    ├── 8. WAL.Commit() → fsync（标记写入完成）
    │
    └── 9. 释放写信号量
```

### 5.3 Seal 流程

写入完成后，网关调用 `SealChunk`：

```
SealChunk(chunkID)
    │
    ├── 1. 读取整个 .dat 文件的数据体
    ├── 2. 计算 CRC32C 校验和
    ├── 3. 将校验和写入文件头部 (bytes 16-19)
    ├── 4. fsync()
    └── 5. 更新内存状态为 LocalSealed
```

---

## 6. 读取路径

### 6.1 端到端流程

```
Client (S3 GET /bucket/key)
    │
    ├── 1. 网关层: Lookup(父目录, key) → InodeMeta
    │
    ├── 2. 网关层: 从 InodeMeta.Chunks 获取 ChunkRef 列表
    │
    ├── 3. 对每个 ChunkRef:
    │      ├── GetChunk(chunkID) → ChunkMeta (含副本列表)
    │      ├── 选择读取节点:
    │      │   - 强一致模式: 选链尾节点 (Tail)
    │      │   - 低延迟模式: 选本地节点 (如果有)
    │      │   - 否则选第一个 Alive 副本
    │      └── TCP 连接: ReadChunk(chunkID, offset, length)
    │
    ├── 4. 数据节点读取:
    │      ├── 获取读信号量 (默认最多 256 并发读)
    │      ├── 打开 .dat 文件
    │      ├── 跳过 20 字节头部
    │      ├── 从 offset 处读取 length 字节 (0=全部)
    │      └── 返回数据 + CRC32 校验和
    │
    └── 5. 网关层写入客户端缓存 → 返回客户端
```

### 6.2 副本选择策略

```go
type ReadStrategy struct {
    localAddr         string
    PreferConsistency bool
}

// SelectReplica 选择最佳读取节点
func (rs *ReadStrategy) SelectReplica(chain *ReplicationChain) *ChainNode {
    // 强一致 → 选链尾 (Tail)
    // 低延迟 → 先找本地节点，再找第一个 Alive
}
```

---

## 7. 复制模型

### 7.1 链复制 (Chain Replication)

用于同步写入复制，保证强一致。

```
写入顺序: Head → Mid → Tail

Client
  │ Write
  ▼
Head (Node1)  ──Forward──►  Mid (Node2)  ──Forward──►  Tail (Node3)
  │ (本地写入)                  │ (本地写入)                 │ (本地写入)
  └─────────────────── ACK ◄────────────────── ACK ◄────────┘
                              ↗ Response to Client
```

**故障处理**：
- **Head 宕机**：客户端重试到新的第一个 Alive 节点
- **中间节点宕机**：绕过，直接转发给下一个 Alive 节点
- **Tail 宕机**：前一个节点成为新 Tail

### 7.2 异步复制 (Replicator)

用于后台数据复制和修复。使用 Worker 池（默认 4 个并发 Worker）：

```
Submit(task: {SourceAddr, TargetAddr, ChunkID})
    │
    ├── Worker 1: ReadChunk from Source → ReplicateChunk to Target → 校验 Checksum
    ├── Worker 2: ...
    ├── Worker 3: ...
    └── Worker 4: ...
```

**失败重试**：指数退避 1s → 2s → 4s，最多 3 次。

### 7.3 反熵修复 (Anti-Entropy)

定期（默认 30 分钟）扫描本地 Chunk，与元数据中的校验和对比：

```
AntiEntropy.Scan()
    │
    ├── 遍历本地所有 Chunk
    ├── 对每个 Chunk:
    │      ├── meta.GetChunk(chunkID) 获取元数据
    │      ├── 比较 localChecksum vs meta.Checksum
    │      └── 不一致时:
    │           ├── repairFromPeer(): 从健康副本拉取
    │           │      ├── TCP 连接健康节点
    │           │      ├── ReadChunk() 读取完整数据
    │           │      ├── ChunkStore.Write() + Seal() 覆盖本地
    │           │      └── ReportChunkState() 更新元数据
    │           └── 无健康副本时: TriggerRepair() 加入修复队列
    └── 统计: scanned / mismatches / repaired
```

### 7.4 修复队列 (RepairWorker)

从元数据修复队列消费任务：

```
RepairWorker.processRepairQueue()
    │
    ├── meta.GetRepairQueue() → 获取待修复的 Chunk 列表
    │
    └── 对每个 RepairTask:
            │
            ├── repairChunk():
            │      ├── 判断修复类型:
            │      │   needsNewReplica  → 副本少于预期
            │      │   localReplicaCorrupt → 本地副本损坏
            │      │   健康 → 删除旧修复任务
            │      │
            │      ├── repairByAddingReplica():
            │      │      ├── 找健康源副本 (ReplicaReady)
            │      │      ├── 找目标节点 (在线且不持有此 Chunk)
            │      │      ├── 从源读取 → 写入目标 (TCP)
            │      │      ├── UpdateChunk() 添加新副本到元数据
            │      │      └── ReportChunkState() 确认 Ready
            │      │
            │      └── repairByRefetchLocal():
            │             ├── 从健康源读取
            │             └── 覆盖本地副本
            │
            └── 完成后调用 RemoveRepairTask() 清理队列
```

---

## 8. 放置策略 (PlacementEngine)

### 8.1 节点评分算法

```
PlaceChunk(inodeID, offset, policy)

1. 过滤候选节点:
   - 排除 excludeNodes（已有副本的节点）
   - 仅 NodeOnline 状态的节点
   - 磁盘使用率 < 95%
   - 存储层级匹配策略

2. 评分公式:
   score = freeCapacity × 0.4
         + (1 - load)     × 0.3
         + tierMatch      × 0.3
         + jitter         × 0.05

   - freeCapacity: (capacity - used) / capacity
   - load: 磁盘 I/O 利用率 (0.0-1.0，通过心跳上报)
   - tierMatch: 节点层级匹配策略 = 1.0，否则 = 0.3
   - jitter: 0-0.05 随机抖动，避免惊群效应

3. 拓扑约束:
   - SpreadNode:  不同节点即可
   - SpreadRack:  不能在同一 Rack
   - SpreadZone:  不能在同一可用区
   - 无法满足时: 自动降级，允许同一故障域

4. 返回: [primary, secondary, tertiary, ...]
         按评分排序，第一个为主副本
```

### 8.2 PlacementPolicy

```go
type PlacementPolicy struct {
    ID                string           // 策略名称
    ReplicationFactor int              // 副本数 (如 3)
    ECConfig          *ECConfig        // 纠删码配置 (k+m)
    TopologySpread    TopologySpread   // 拓扑传播约束
    StorageTier       StorageTier      // 存储层级
}
```

每个 Bucket 可以配置独立的放置策略，存在 `/policy/{bucket}`。

---

## 9. ChunkID 生成 (Snowflake)

```
 41-bit 时间戳  |  10-bit 节点ID  |  13-bit 序列号
─────────────────┼────────────────┼────────────────
     毫秒级       │    metad 节点   │   每毫秒 8192 个
```

- 每个 metad 节点每毫秒可生成 8192 个唯一 ID
- 全局唯一，无需跨节点协调
- 编码了时间信息，便于按时间范围扫描

---

## 10. 小文件优化

文件 ≤ 64KB（`SmallFileThreshold`）时，不单独分配 Chunk，而是合并到 1MB 的 Block 中：

```
Block (1MB)
├── Header: file_count + index_offset
├── Index: [{name, offset, length, crc}, ...]
└── Data:
    ├── file1 (200B)
    ├── file2 (1KB)
    ├── file3 (50B)
    └── ...padding to 1MB
```

- 每个 Block 最多 256 个小文件
- 目录内 ≥ 50 个小文件时创建 Block
- Block 满后 Sealed，新建 Block
- 元数据减少 ~80%，磁盘利用率从 25% → ~90%

---

## 11. 心跳与健康监控

### 11.1 心跳上报 (HeartbeatReporter)

每 10 秒通过 HTTP 向 metad 上报：

```
POST /api/v1/nodes/{node_id}/heartbeat
{
    "used_gb": 500,
    "chunk_count": 12345,
    "disk_io": 0.3,           // I/O 利用率 (用于放置评分)
    "chunk_states": {          // 批量上报，非逐 chunk
        "chunk_id": "Ready",
        ...
    }
}
```

### 11.2 租约管理 (LeaseManager)

- 节点注册 → 获得 30s 租约
- 每 10s 心跳续租
- 超时未续租 → 标记 NodeOffline
- NodeOffline → Scrubber 检测 → 触发修复

---

## 12. 纠删码 (Reed-Solomon)

支持可选的 Reed-Solomon 纠删码（GF(256)）：

- **编码**：将数据分成 K 个数据片，计算 M 个校验片
- **解码**：任意 K 个片可恢复原始数据
- **验证**：重新计算校验片对比

配置方式（通过 PlacementPolicy）：
```go
type ECConfig struct {
    DataShards   int // K (数据片)
    ParityShards int // M (校验片)
}
```

---

## 13. 元数据内部通信

数据节点通过 `metadata.HTTPClient` 连接 metad：

```
datanode → HTTP → metad (PebbleStore)
            │
            ├── /api/v1/nodes           POST  → RegisterNode
            ├── /api/v1/nodes/{id}/heartbeat POST → Heartbeat
            ├── /api/v1/chunks/{id}     GET   → GetChunk
            ├── /api/v1/chunks/{id}     PUT   → UpdateChunk
            ├── /api/v1/chunks/report-state POST → ReportChunkState (批量)
            ├── /api/v1/repair/queue    GET   → GetRepairQueue
            └── /api/v1/repair/{id}     DELETE → RemoveRepairTask
```

所有元数据写入操作通过 Raft 达成共识（如果启用），保证线性一致性。

---

## 14. 关键数据流图

### 14.1 写入时序

```
Gateway           metad                  DataNode N1         DataNode N2         DataNode N3
  │                 │                       │                   │                   │
  │──CreateFile───►│                       │                   │                   │
  │──AllocateChunk►│                       │                   │                   │
  │                │──PlaceChunk()──►      │                   │                   │
  │                │◄─[N1,N2,N3]───        │                   │                   │
  │                │                       │                   │                   │
  │──WriteChunk───►│───TCP────────────────►│                   │                   │
  │                │                         │──WAL.Write()──  │                   │
  │                │                         │──Write .dat──   │                   │
  │                │                         │──WAL.Commit()─  │                   │
  │                │                         │─fsync()──       │                   │
  │──WriteChunk───►│─────────────────────────────────────────►│                   │
  │──WriteChunk───►│─────────────────────────────────────────────────────────────►│
  │                │                       │                   │                   │
  │──CommitChunk──►│                       │                   │                   │
  │                │──State→Sealed         │                   │                   │
  │                │                       │                   │                   │
  │──Seal─────────►│───TCP────────────────►│                   │                   │
  │                │                         │─calc CRC32C──   │                   │
  │                │                         │─write header──  │                   │
  │                │                         │─State→Sealed──  │                   │
  │◄──OK───────────│                       │                   │                   │
```

### 14.2 修复时序

```
Scrubber / LeaseManager      metad                    RepairWorker           Healthy Node      New Node
  │                            │                          │                    │                │
  │──检测到 ChunkDegraded───►│                          │                    │                │
  │                            │──TriggerRepair(chunk)──►│                    │                │
  │                            │                          │                    │                │
  │                            │──GetRepairQueue───►     │                    │                │
  │                            │◄──[chunk]──────────     │                    │                │
  │                            │                          │                    │                │
  │                            │──GetChunk(chunk)──►     │                    │                │
  │                            │◄──ChunkMeta──────       │                    │                │
  │                            │                          │──ReadChunk───────►│                │
  │                            │                          │◄──data──────────  │                │
  │                            │                          │──ReplicateChunk──────────────────►│
  │                            │                          │◄──OK─────────────                  │
  │                            │                          │                    │                │
  │                            │◄──UpdateChunk(+新副本)──│                    │                │
  │                            │◄──ReportChunkState(Ready)│                    │                │
  │                            │◄──RemoveRepairTask()─── │                    │                │
```

---

## 15. 配置项

### 15.1 metad

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--data-dir` | `/var/lib/dfs/metadata` | Pebble 数据目录 |
| `--ops-addr` | `0.0.0.0:8091` | HTTP API 监听地址 |
| `--raft-addr` | `0.0.0.0:7000` | Raft 共识端口 |
| `--raft` | `true` | 启用 Raft |
| `--lease-ttl` | `30s` | 数据节点租约 TTL |
| `--gc-interval` | `10m` | 孤儿 Chunk GC 周期 |
| `--scrub-interval` | `1h` | 数据清洗周期 |

### 15.2 datanode

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--listen` | `0.0.0.0:9100` | Chunk TCP 服务地址 |
| `--data-dir` | `/var/lib/dfs/data` | Chunk 存储根目录 |
| `--metadata` | `localhost:8091` | metad HTTP 地址 |
| `--capacity` | `1000` | 节点容量 (GB) |
| `--rack` | `rack-1` | 机架标识 |
| `--zone` | `zone-1` | 可用区标识 |
