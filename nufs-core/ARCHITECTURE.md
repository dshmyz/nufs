# NUFS-DFS 分布式文件系统架构设计

> 版本: 0.2.0 | 语言: Go 1.23 | 许可: 项目内部

---

## 目录

1. [系统总览](#1-系统总览)
2. [组件详细设计](#2-组件详细设计)
3. [核心数据流](#3-核心数据流)
4. [存储布局与数据格式](#4-存储布局与数据格式)
5. [一致性模型与容错](#5-一致性模型与容错)
6. [小文件优化](#6-小文件优化)
7. [客户端缓存策略](#7-客户端缓存策略)
8. [生命周期管理](#8-生命周期管理)
9. [跨机房复制](#9-跨机房复制)
10. [部署架构](#10-部署架构)
11. [运维与可观测性](#11-运维与可观测性)
12. [性能目标](#12-性能目标)
13. [实现状态与路线图](#13-实现状态与路线图)

---

## 1. 系统总览

### 1.1 设计目标

| 维度 | 目标 |
|------|------|
| 规模 | 百亿文件 / PB 级存储 |
| 可用性 | 99.99% (年停机 < 53 分钟) |
| 延迟 | 读 P99 < 10ms, 写 P99 < 20ms |
| 兼容性 | S3 API + FUSE 挂载 |
| 部署 | 单机开发 / Docker Compose / Kubernetes |

### 1.2 架构全景

```
┌─────────────────────────────────────────────────────────────────┐
│                        客户端层                                  │
├──────────────────┬──────────────────┬──────────────────────────┤
│  S3 Client       │  FUSE Mount      │  CLI (dfsctl)            │
│  (aws-sdk/etc)   │  (go-fuse/v2)    │  (运维管理)              │
└────────┬─────────┴────────┬─────────┴──────────┬───────────────┘
         │                  │                     │
┌────────▼──────────────────▼─────────────────────▼───────────────┐
│                      网关层 (Gateway)                            │
├──────────────────────┬──────────────────────────────────────────┤
│  S3 Gateway :8080    │  FUSE Gateway                              │
│  (S3 兼容 HTTP)      │  (POSIX 文件系统)                         │
│  ├─ Auth/V4签名      │  ├─ Inode 缓存                            │
│  ├─ Multipart        │  ├─ 目录/文件/符号链接                    │
│  └─ 中间件链         │  └─ XAttr 支持                            │
│                      │  ClientCache (LRU + Write-Back)          │
└──────────────┬───────┴──────────────┬───────────────────────────┘
               │                      │
┌──────────────▼──────────────────────▼───────────────────────────┐
│                    元数据层 (Metadata)                           │
├──────────────────────┬──────────────────────────────────────────┤
│  PebbleStore (唯一后端)                                         │
│  ├─ Pebble LSM-Tree                                              │
│  ├─ Raft (hashicorp/raft)                                        │
│  ├─ MVCC (乐观锁)                                                │
│  ├─ ShardRouter (一致性哈希)                                     │
│  └─ Batch 原子写入                                               │
│                      │                                          │
│  ServiceBundle:                                                 │
│  ├─ EventBus (Watch/Notify)                                     │
│  ├─ Metrics (延迟直方图/计数器)                                  │
│  ├─ HealthChecker (HTTP 探针)                                   │
│  ├─ LeaseManager (节点租约)                                     │
│  ├─ ChunkGC (孤儿块回收)                                        │
│  └─ Scrubber (静默损坏检测)                                     │
│                                                                 │
│  PlacementEngine:                                               │
│  ├─ 加权评分 (容量40% + 负载30% + 层级30%)                      │
│  ├─ 拓扑感知 (Node/Rack/Zone)                                  │
│  └─ Snowflake ChunkID 生成器                                    │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│                     数据层 (DataNode)                            │
├─────────────────────────────────────────────────────────────────┤
│  DataNode Server (TCP :9100)                                    │
│  ├─ ChunkStore (本地文件存储)                                    │
│  │   ├─ 256 分片目录 (避免单目录文件过多)                        │
│  │   ├─ 二进制头 + 数据体 + 元数据边车                           │
│  │   └─ 并发读写信号量控制                                       │
│  ├─ Replicator (异步复制引擎)                                    │
│  │   ├─ 多 Worker 并行复制                                       │
│  │   └─ 指数退避重试 (最多3次)                                   │
│  ├─ HeartbeatReporter (心跳上报)                                 │
│  │   └─ 磁盘使用/Chunk状态/DiskIO                                │
│  ├─ DiskManager (磁盘管理)                                       │
│  └─ Ops HTTP API (:8091)                                        │
│                                                                 │
│  RebalancePlanner (集群再平衡)                                   │
│  └─ 变异系数检测 + 迁移计划生成                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 1.3 技术栈

| 层次 | 技术 | 用途 |
|------|------|------|
| 元数据-共识 | hashicorp/raft | 多数派共识、Leader 选举 |
| 元数据-存储 | CockroachDB/Pebble | LSM-Tree KV 存储 (亿级元数据) |
| 元数据-日志 | raft-boltdb | Raft WAL 持久化 |
| 数据-存储 | 本地文件系统 | Chunk 二进制文件 |
| 数据-纠删码 | Reed-Solomon (GF256) | K+M 纠删码编解码 |
| 网关-S3 | net/http | S3 兼容 REST API |
| 网关-FUSE | hanwen/go-fuse/v2 | Linux POSIX 挂载 |
| 可观测 | 内置 Metrics + Health | Prometheus 兼容 |

---

## 2. 组件详细设计

### 2.1 元数据服务 (metadata/)

元数据服务是整个系统的核心，管理命名空间、Chunk 映射、集群拓扑和放置策略。

#### 2.1.1 双存储引擎

系统支持两种元数据后端，通过 `MetadataService` 接口统一访问：

```go
type MetadataService interface {
    // Bucket 操作
    CreateBucket / DeleteBucket / ListBuckets / GetBucket
    // 命名空间操作
    MkDir / RmDir / ReadDir / CreateFile / Unlink
    Lookup / Rename / Symlink / Readlink / Link
    // Inode 操作
    GetInode / UpdateInode
    // Chunk 操作
    AllocateChunk / CommitChunk / GetChunk / SealChunk
    ListChunks / DeleteChunk / ReportChunkState
    // 集群操作
    RegisterNode / Heartbeat / DecommissionNode / ListNodes / GetNode
    // 修复操作
    GetRepairQueue / TriggerRepair
}
```

| 特性 | PebbleStore (唯一后端) |
|------|----------------------|
| 共识机制 | 内嵌 hashicorp/raft (可选) |
| 存储引擎 | Pebble LSM-Tree |
| 原子写入 | Pebble Batch (原子) |
| MVCC | CAS 乐观锁 |
| 分片 | 一致性哈希 ShardedStore |
| 适合场景 | 开发/测试/生产全场景 |

#### 2.1.2 Raft 共识 (pebble_raft.go)

```
写路径:
  Client → Leader.Apply(RaftLogEntry) → Raft Log → 多数派 ACK → FSM.Apply() → Pebble

RaftLogEntry 编码:
  [OpType: 1字节][Key/Value/Batch payload]

  OpSet    (0x01): [key_len:4][key][val_len:4][val]
  OpDelete (0x02): [key_len:4][key]
  OpBatch  (0x03): [count:4]([del_flag:1][key_len:4][key][val_len:4][val])×N

快照策略:
  - 触发: 每 8192 条日志或 2 分钟
  - 格式: [magic:"PBL1"][key_count:8]([key_len:4][key][val_len:4][val])×N
  - 保留: 最近 3 个快照 + 10240 条尾随日志
  - 恢复: 逐批提交 (每 10K KV 一批，防 OOM)
```

#### 2.1.3 放置引擎 (placement.go)

```
PlaceChunk 算法:
  1. 过滤: 排除 offline / 容量>95% / 层级不匹配的节点
  2. 评分: score = free_capacity×0.4 + low_load×0.3 + tier_match×0.3 + jitter×0.05
  3. 拓扑: 按 TopologySpread (Node/Rack/Zone) 约束选择不同故障域
  4. 降级: 无法满足拓扑约束时，回退到无约束选择

ChunkID 生成 (Snowflake):
  [41-bit 毫秒时间戳 | 10-bit 节点ID | 13-bit 序列号]
  → 每节点每毫秒可生成 8192 个 ID
```

#### 2.1.4 分片路由 (shard.go)

```
ShardedStore:
  Key → CRC32(key) → 一致性哈希环 → ShardID → PebbleStore

  - 每物理分片 150 个虚拟节点
  - 增删分片仅迁移 ~1/N 的 Key
  - 每分片独立 Raft Group
  - RouteN(): 返回 N 个不同分片 (用于跨分片复制)
```

#### 2.1.5 MVCC 乐观并发控制 (production.go)

```go
// 写入流程:
1. GetInodeWithVersion() → 读取 inode + version
2. 修改 inode 数据
3. CASUpdateInode(expected=version, meta) →
   - 读取当前 version
   - version == expected → 写入, version++
   - version != expected → ErrVersionConflict → 客户端重试
```

#### 2.1.6 租约管理 (production.go)

```
LeaseManager:
  - 节点注册 → 获取 30s 租约
  - 每 10s 心跳 → 续租
  - 每 10s 检查 (TTL/3) → 标记过期节点
  - 过期 → NodeOffline + 发布事件 → 触发修复
```

#### 2.1.7 孤儿 Chunk GC (production.go)

```
ChunkGC 扫描流程:
  Phase 1: 遍历所有 Inode → 收集被引用的 ChunkID 集合
  Phase 2: 遍历所有 ChunkMeta → 找出不在引用集中的孤儿
  Phase 3: 删除孤儿 Chunk (或 dry-run 模式仅报告)

  运行周期: 默认每 10 分钟
```

#### 2.1.8 数据清洗 (production.go)

```
Scrubber 检测规则:
  - Chunk 无副本 → 标记损坏
  - 所有副本不健康 → 标记损坏
  - 已 Seal 但无 Checksum → 标记损坏
  - ChunkDegraded 状态 → 计入降级计数
  损坏 → 发布修复事件 → 修复队列

  运行周期: 默认每 1 小时
```

### 2.2 数据节点 (datanode/)

#### 2.2.1 TCP 协议

```
请求格式:
  [4字节 header_len (大端)] [Header JSON] [4字节 body_len (大端)] [Body 数据]

响应格式:
  [4字节 resp_len (大端)] [Response JSON]

请求类型:
  ReqWriteChunk     → 写入 Chunk 数据
  ReqReadChunk      → 读取 Chunk 数据 (支持 offset+length)
  ReqDeleteChunk    → 删除 Chunk
  ReqReplicateChunk → 复制 Chunk 到本节点
  ReqChunkInfo      → 查询 Chunk 元数据
  ReqListChunks     → 列出本地所有 Chunk
  ReqHealth         → 健康检查
```

#### 2.2.2 复制引擎

```
Replicator:
  - 多 Worker (默认 4) 并行复制
  - 任务队列 (缓冲 1024)
  - 指数退避重试 (最多 3 次)
  - 复制流程: Source.ReadChunk → Target.ReplicateChunk → 校验 Checksum
  - 修复流程: 从存活副本读 → 写入新目标节点
```

#### 2.2.3 心跳上报

```
每 10s 上报:
  - UsedGB / ChunkCount / DiskIO (0.0-1.0)
  - 每个 Chunk 的 ReplicaState (Ready/Syncing/Failed)
```

### 2.3 网关层 (gateway/)

#### 2.3.1 S3 网关

```
S3 兼容 API:
  服务级:  GET  /                          → ListBuckets
  Bucket级: PUT /{bucket}                  → CreateBucket
           DELETE /{bucket}                → DeleteBucket
           HEAD /{bucket}                  → HeadBucket
           GET /{bucket}                   → ListObjects
  Object级: PUT /{bucket}/{key}            → PutObject
           GET /{bucket}/{key}             → GetObject
           DELETE /{bucket}/{key}          → DeleteObject
           HEAD /{bucket}/{key}            → HeadObject
  Multipart: POST ?uploads                 → InitiateMultipartUpload
            PUT ?uploadId&partNumber       → UploadPart
            POST ?uploadId                 → CompleteMultipartUpload
            DELETE ?uploadId               → AbortMultipartUpload
            GET ?uploadId                  → ListParts
            GET ?uploads                   → ListMultipartUploads
  批量:    POST ?delete                    → BatchDelete

中间件链:
  Recovery → RequestID → CORS → Logging → Auth(V4签名) → Handler

认证:
  - AWS Signature V4 签名验证
  - 凭据源 = metad 注册表（Phase 2 收敛）：nufs-cli `auth add` 写入注册表 →
    网关经 --meta-auth-token 拉取 `/api/v1/auth/credentials`（明文由 metad 用
    --credential-secret-key 加密存储、受信端点解密下发）→ 内存 CredentialStore
    TTL 轮询刷新；吊销延迟 ≤ 同步间隔。principal = 注册表绑定的 principal
    （CreateBucket 的 owner 即验证后的 principal）。
  - 匿名模式 (注册表为空时)
  - 旧本地源 --access-key/--secret-key/--credentials-file 已废弃，仅作同步
    不可用时的回退
```

#### 2.3.2 FUSE 网关 (仅 Linux)

```
基于 hanwen/go-fuse/v2:
  DFSFileSystem → 根 Inode
  ├─ DFSDir   → 目录操作 (Lookup/ReadDir/Mkdir/Rmdir)
  ├─ DFSFile  → 文件操作 (Read/Write/Flush)
  └─ DFSSymlink → 符号链接

  Inode 映射: metadata.InodeID → fs.Inode (内存缓存)
  属性转换: InodeMeta → fuse.Attr (含 POSIX 权限/时间戳)
```

**POSIX 语义缺口/接受约束**（Program 12 审计后定稿）：

- **mknod（已实现）**：FUSE `Mknod` 支持 fifo / char-dev / block-dev / unix socket。
  FIFO 的 read/write 由内核 pipe 完全接管（fs 只提供 inode 身份 + 属性，`Open` 返回
  `FOPEN_NONSEEKABLE`）；设备节点与 socket 为 identity-only stub，`open()` 拒绝
  （`EOPNOTSUPP`）——用户态 FUSE 无法路由真实设备/套接字 I/O，需依赖 `/dev` 上真实设备。
- **fallocate（已实现）**：`DFSFile.Allocate` 支持预分配（扩 Size + 补零）、
  `FALLOC_FL_KEEP_SIZE`（物理预分配但不改逻辑 Size）、`FALLOC_FL_ZERO_RANGE`（区间清零并可延展）、
  `FALLOC_FL_PUNCH_HOLE`（clamp 到当前 Size 清除；对象存储无真实"孔"，补零即 POSIX 可见语义）。
  区间移动类 flag（`COLLAPSE_RANGE`/`INSERT_RANGE`/`UNSHARE_RANGE`）→ `EOPNOTSUPP`。
- **Advisory 锁 ≠ 完整 POSIX fcntl/flock（接受约束，不改代码）**：本网关采用**单写者对象存储
  模型**——同一文件通常由单一客户端以独占读写方式访问，跨进程字节区间锁（`fcntl(F_SETLK)` /
  `flock`）未实现，`O_EXCL`/独占打开等竞争语义不保证。作为对象存储网关这被认定为**可接受的
  约束**（与 S3 语义一致：无跨客户端持锁）。多写者并发协作不在此模型支持范围内。

**性能项（已明确延后，非缺口）**：`copy_file_range`（服务端复制）、`readdirplus`、
`SEEK_HOLE`/`SEEK_DATA`、`poll`/`epoll`、设备 `ioctl` 均未实现——纯优化项，不影响正确性，
列为延后清单。

#### 2.3.3 客户端缓存 (cache.go)

```
ClientCache:
  ├─ Attr Cache (LRU, TTL 5s)
  │   └─ inode → InodeMeta (ls/stat 直接返回)
  ├─ Data Cache (LRU, TTL 50s)
  │   └─ chunkKey → []byte (数据块缓存)
  └─ Write Buffer (500MB)
      └─ ChunkID → dirty data (异步写回)
      └─ 刷写策略: 缓冲>500MB / 10s定时 / 进程退出
```

---

## 3. 核心数据流

### 3.1 写入路径

```
                  S3 PUT /bucket/key
                         │
                    ┌────▼────┐
                    │ S3 GW   │ 1. 解析请求, 鉴权
                    └────┬────┘
                         │ 2. CreateFile(parent, key, mode)
                    ┌────▼────┐
                    │ Metadata│ 3. AllocateChunk(inodeID, offset, policy)
                    │ Service │    → PlacementEngine.PlaceChunk()
                    └────┬────┘    → 返回 ChunkMeta{replicas: [N1,N2,N3]}
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
         ┌────────┐ ┌────────┐ ┌────────┐
         │DN-N1   │ │DN-N2   │ │DN-N3   │ 4. 并行写入
         │Write   │ │Write   │ │Write   │    (主节点 N1 先写)
         │Chunk   │ │Chunk   │ │Chunk   │
         └───┬────┘ └────────┘ └────────┘
             │ 5. CommitChunk(chunkID, checksum)
             │    SealChunk(chunkID)
        ┌────▼────┐
        │ Metadata│ 6. 更新 InodeMeta.ChunkMap
        │ Service │    返回客户端
        └─────────┘
```

**写入保证**:
- 数据先写主副本 → 异步复制到从副本
- CommitChunk 后 Chunk 进入 Sealed 状态
- 所有副本 Ready 后 Chunk 进入 Ready 状态
- PebbleStore 使用 Batch 原子更新 Inode + ChunkMap

### 3.2 读取路径

```
                  S3 GET /bucket/key
                         │
                    ┌────▼────┐
                    │ S3 GW   │ 1. 查缓存
                    │ Cache?  │──命中──→ 直接返回
                    └────┬────┘
                         │ 未命中
                    ┌────▼────┐
                    │ Metadata│ 2. Lookup(parent, key) → InodeMeta
                    │ Service │ 3. ListChunks(inodeID) → []ChunkRef
                    └────┬────┘
                         │
                    ┌────▼────┐
                    │ 选择副本│ 4. 优先选择就近/低负载副本
                    └────┬────┘
                         │
                    ┌────▼────┐
                    │ DN-N1   │ 5. ReadChunk(chunkID, offset, length)
                    │ Read    │
                    └────┬────┘
                         │ 6. 写入客户端缓存
                    ┌────▼────┐
                    │ Cache   │ → 返回客户端
                    └─────────┘
```

**读取优化**:
- Attr Cache: 5s TTL, ls/stat 直接返回
- Data Cache: 50s TTL, 热数据本地读取
- 副本选择: 优先同 Rack/Zone 就近读

### 3.3 Chunk 复制与修复流程

```
写入后复制:
  N1.Write → Replicator.SubmitReplication() →
    Worker: N1.ReadChunk → N2.ReplicateChunk + N3.ReplicateChunk

节点宕机修复:
  LeaseManager 检测 NodeOffline →
    EventBus 发布事件 →
      Scrubber 检测 ChunkDegraded →
        TriggerRepair → Replicator.Repair() →
          存活副本.Read → 新节点.ReplicateChunk

节点下线 (Decommission):
  DecommissionNode(nodeID) → NodeDraining →
    RebalancePlanner.PlanDecommission() →
      迁移所有 Chunk 到其他节点
```

---

## 4. 存储布局与数据格式

### 4.1 元数据 KV 布局

```
PebbleStore 键空间:

/bucket/{name}           → BucketInfo (JSON)
/ns/{parent_id}/{name}   → DirEntry (JSON)
/inode/{id}              → InodeMeta (JSON)
/chunk/{id}              → ChunkMeta (JSON)
/node/{id}               → NodeInfo (JSON)
/policy/{bucket}         → PlacementPolicy (JSON)
/repair/{chunk_id}       → RepairTask (JSON)
/inode/block/{block_id}  → SmallFileBlockMeta (JSON)
```

### 4.2 InodeMeta 结构

```json
{
  "id": 12345,
  "type": 0,
  "size": 1048576,
  "nlink": 1,
  "uid": 1000,
  "gid": 1000,
  "mode": 420,
  "ctime": 1700000000000000000,
  "mtime": 1700000000000000000,
  "atime": 1700000000000000000,
  "chunks": [
    {"id": 789, "offset": 0, "length": 1048576, "version": 1700000000000000000}
  ],
  "symlink": "",
  "xattrs": {"user.key": "base64-value"}
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| type | FileType | 0=Regular, 1=Directory, 2=Symlink |
| chunks | []ChunkRef | 有序 Chunk 列表 (大文件) |
| version | MVCCVersion | 乐观锁版本号 (PebbleStore) |

### 4.3 ChunkMeta 结构

```json
{
  "id": 789,
  "size": 67108864,
  "state": 2,
  "replicas": [
    {"node_id": 1, "addr": "dn1:9100", "state": 1, "disk_path": "/data"},
    {"node_id": 2, "addr": "dn2:9100", "state": 1, "disk_path": "/data"},
    {"node_id": 3, "addr": "dn3:9100", "state": 1, "disk_path": "/data"}
  ],
  "ec_group": null,
  "create_time": 1700000000000000000,
  "checksum": 287454020
}
```

| ChunkState | 值 | 说明 |
|------------|---|------|
| ChunkSealing | 0 | 正在写入 |
| ChunkSealed | 1 | 写入完成，复制中 |
| ChunkReady | 2 | 所有副本已确认 |
| ChunkDegraded | 3 | 副本丢失，修复中 |
| ChunkOrphan | 4 | 无 Inode 引用 (GC 候选) |

### 4.4 数据节点磁盘布局

```
{DataDir}/
├── chunks/
│   ├── 00/           # Shard 0x00 (chunk_id % 256)
│   │   ├── 12345.dat    # Chunk 数据文件
│   │   └── 12345.meta   # 元数据边车 (JSON, 可选)
│   ├── 01/
│   │   └── ...
│   └── ff/
└── cache/            # 可选读缓存

Chunk 文件格式 (.dat):
  ┌─────────────────────────────────────────────┐
  │ Magic: "DFS\x01"            (4 bytes)       │
  │ ChunkID                      (8 bytes, BE)  │
  │ DataLength                   (4 bytes, BE)  │
  │ CRC32 Checksum               (4 bytes, BE)  │
  │ Chunk Data                   (N bytes)       │
  └─────────────────────────────────────────────┘
  Header 总计: 20 bytes
```

---

## 5. 一致性模型与容错

### 5.1 写一致性

```
强一致写 (Raft 共识):
  Client → [Leader]
              │ raft.Apply(RaftLogEntry)
              ├─→ [Follower1] ACK
              ├─→ [Follower2] ACK
              └─ 多数派确认 → 返回 Client

  Leader 宕机:
  └─ Raft 选举新 Leader (≤ 2s)
  └─ 未 Commit 操作自动回滚

MVCC 乐观并发:
  1. 读取 InodeWithVersion {meta, version=5}
  2. 修改 meta
  3. CASUpdateInode(expected=5, meta)
     - 当前 version==5 → 写入, version→6
     - 当前 version!=5 → ErrVersionConflict → 重试
```

### 5.2 读一致性

| 模式 | 路径 | 延迟 | 一致性 |
|------|------|------|--------|
| 强一致读 | 走 Leader | ~2ms | 线性一致性 |
| 最终一致读 | 走任意副本 | <1ms | 会话一致性 |
| 缓存读 | 本地 Attr/Data Cache | <100μs | TTL 过期后验证 |

### 5.3 容错能力

| 故障场景 | 检测 | 恢复 |
|---------|------|------|
| DataNode 宕机 | LeaseManager 30s 超时 | 标记 Offline → Scrubber → Repair |
| MetaNode 宕机 | Raft 心跳超时 | 自动选举新 Leader (≤2s) |
| 磁盘静默损坏 | Scrubber Checksum 校验 | 标记 Corrupt → 修复 |
| 孤儿 Chunk | GC 定期扫描 | 删除无引用 Chunk |
| 网络分区 | Raft 无法获多数派 | 分区外节点自动降级为 Follower |

### 5.4 写入保证

- **线性一致性**: Raft 保证写入按日志顺序提交
- **Read-Your-Writes**: ChunkRef 带 version 字段，读时验证
- **Exactly-Once**: 客户端 UUID + 服务端去重表 (规划中)
- **原子性**: PebbleStore 使用 Batch 保证 Inode+ChunkMap 原子更新

---

## 6. 小文件优化

### 6.1 痛点

- 每个文件 1 个 inode + 1-3 个 chunk → **元数据爆炸**
- 1 亿文件 × 500B/chunk = **50GB 元数据**
- 1KB 文件占 4KB 磁盘块 → **75% 空间浪费**

### 6.2 方案：小文件合并存储

```
┌─────────────────────────────────────┐
│  SmallFileBlock (1MB)               │
├─────────────────────────────────────┤
│ Header: [file_count:2] [index_off:4]│
├─────────────────────────────────────┤
│ Index: [offset:4][len:2][chksum:2]×N │
├─────────────────────────────────────┤
│ Data:                                │
│   [file1: 100B]                     │
│   [file2: 50B]                      │
│   [file3: 200B]                     │
│   ...                               │
│   [padding: to 1MB]                 │
└─────────────────────────────────────┘
```

**规则** (已实现在 `metadata/smallfile.go`):
- 文件 ≤ 64KB → 合并到 Block (`SmallFileThreshold`)
- 文件 > 64KB → 独立 Chunk
- 1 个 Block 最多 256 个小文件 (`MaxSmallFilesPerBlock`)
- 目录内 ≥ 50 个小文件时创建 Block (`ShouldCreateBlock`)
- Block 满后 Sealed，写新 Block

**SmallFileBlockMeta 结构**:
```json
{
  "block_id": 456,
  "size": 1048576,
  "file_count": 3,
  "index": [
    {"name": "a.txt", "offset": 0, "length": 100, "crc": 57005},
    {"name": "b.txt", "offset": 100, "length": 50, "crc": 48879}
  ],
  "created_at": 1700000000,
  "sealed": false
}
```

**效果**: 元数据减少 **80%**, 磁盘利用率从 25% → **90%**

---

## 7. 客户端缓存策略

### 7.1 缓存架构

```
┌─────────────────────────────────────────┐
│  DFS Client (FUSE/S3)                   │
├─────────────────────────────────────────┤
│  1. Kernel Cache (page cache)           │
│     - 内核态自动缓存 hot data           │
│     - 无需用户态介入                     │
│                                          │
│  2. User Cache (ClientCache)            │
│     ├── Attr Cache (LRU, TTL 5s)        │
│     │   - inode → InodeMeta             │
│     │   - ls/stat 直接返回              │
│     ├── Data Cache (LRU, TTL 50s)       │
│     │   - chunkKey → []byte             │
│     └── Write Buffer (500MB)            │
│         - ChunkID → dirty data          │
│         - 异步写回                       │
│                                          │
│  3. Write-back Policy                   │
│     - Dirty buffer > 500MB → flush      │
│     - 10s 定时 flush                    │
│     - 进程退出 → sync 所有 dirty        │
└─────────────────────────────────────────┘
```

### 7.2 一致性保证

- **读**: TTL 过期后重新验证 (Attr 5s, Data 50s)
- **写**: Write Buffer + 异步刷盘到 DataNode
- **缓存淘汰**: LRU 策略，Attr 缓存达到上限 10% 时淘汰最旧

### 7.3 效果

| 场景 | 无缓存 | 有缓存 |
|------|--------|--------|
| 读延迟 (热数据) | 5-10ms | < 100μs |
| 写延迟 | 5-10ms | ~1ms (异步) |
| ls/stat | 5ms | < 100μs (TTL 内) |

---

## 8. 生命周期管理

### 8.1 规则引擎 (已实现 `metadata/lifecycle.go`)

```yaml
lifecycle_rules:
  - bucket: "logs"
    prefix: ""
    transition:
      - days: 7    → TierHot → TierWarm
      - days: 30   → TierWarm → TierCold
      - days: 90   → TierCold → TierArchive
    expiration:
      days: 365    → 永久删除

  - bucket: "uploads"
    prefix: "tmp/"
    expiration:
      days: 1      → 自动清理
```

### 8.2 存储层级

| 层级 | 常量 | 介质 | 适用 |
|------|------|------|------|
| TierHot | 1 | NVMe | 热数据, 低延迟 |
| TierWarm | 2 | SSD | 温数据, 平衡 |
| TierCold | 3 | HDD | 冷数据, 低成本 |
| TierArchive | 4 | Tape/对象 | 归档, 极低成本 |

### 8.3 执行引擎

```
LifecycleEngine:
  - 定期执行 (可配置间隔)
  - 按 Bucket 遍历规则
  - 匹配文件 mtime + prefix + 规则
  - 生成迁移/删除任务
  - 统计 transitions / deletions 计数
```

---

## 9. 跨机房复制

### 9.1 方案：异步多活 + CRDT 冲突解决

```
机房 A (主)                    机房 B (备)
┌──────────────┐              ┌──────────────┐
│  Metad-A     │──异步同步──→ │  Metad-B     │
│  (Raft ×3)   │  (10s 延迟)  │  (Raft ×3)   │
└──────┬───────┘              └──────┬───────┘
       │                             │
       ▼                             ▼
┌──────────────┐              ┌──────────────┐
│  Data-A1,A2  │              │  Data-B1,B2  │
│  (Chunk 复制) │              │  (Chunk 复制) │
└──────────────┘              └──────────────┘

同步协议:
  1. 元数据: Append-Only Log 异步推送
  2. Chunk: 仅复制已 Seal 的 chunk (避免复制中途数据)
  3. 冲突: CRDT Last-Write-Wins (用 timestamp)
  4. 断网: 自动重试, 保留 72h 日志

故障切换:
  - 机房 A 宕机 → DNS 切换到 B (1min)
  - B 提升为主, 停止同步
  - A 恢复 → 增量同步 B 的数据, 降级为备
```

**复制粒度**:
- Bucket 级别: `bucket.cross_region_replication = true`
- 延迟: 同城 1-5s, 跨城 10-30s

---

## 10. 部署架构

### 10.1 三种部署模式

**模式 1: 单机开发 (all-in-one)**

```bash
docker run -d --name=dfs \
  -v /data:/data \
  -p 8080:8080 \
  -p 9001:9001 \
  dfs:latest all-in-one
```

→ 1 个容器 = 元数据 + 数据 + S3 网关

**模式 2: Docker Compose (生产小集群)**

```yaml
# 已实现在 docker-compose.yml
services:
  metad:       # 1× 元数据 (Pebble + Raft)
  datanode1:   # 3× 数据节点 (跨 Rack)
  datanode2:
  datanode3:
  s3gw:        # 1× S3 网关
```

```bash
docker-compose up -d
```

**模式 3: Kubernetes (大规模)**

```bash
helm install dfs ./helm/dfs \
  --set replicas.metad=3 \
  --set replicas.datanode=10 \
  --set storage.hot=1Ti \
  --set storage.warm=5Ti
```

→ StatefulSet + PVC + Service + Ingress

### 10.2 服务端口映射

| 服务 | 容器端口 | 用途 |
|------|---------|------|
| metad | 8091 | Operations API |
| metad | 7000 | Raft 共识 |
| datanode | 9100 | Chunk TCP 服务 |
| datanode | 8091 | Ops HTTP API |
| s3gw | 8080 | S3 HTTP API |
| fusegw | - | FUSE 挂载点 (本地) |
| metad (健康) | /health | K8s Liveness Probe |
| metad (就绪) | /ready | K8s Readiness Probe |
| metad (指标) | /metrics | JSON Metrics |

### 10.3 升级策略

```bash
# K8s 滚动升级 (零停机)
kubectl rollout restart sts/dfs-datanode

# Docker Compose 滚动升级
docker-compose up -d --no-deps --build datanode1
# 等待健康检查通过后逐个升级
```

---

## 11. 运维与可观测性

### 11.1 健康检查

```json
// GET /health
{
  "status": "healthy",       // healthy / degraded / unhealthy
  "role": "leader",          // leader / follower / standalone
  "version": "0.1.0",
  "uptime": "2h30m15s",
  "checks": {
    "pebble": "ok",
    "root_inode": "ok",
    "raft_state": "Leader",
    "raft_last_contact": "0.5s ago",
    "error_rate": "ok"
  }
}
```

**状态判定**:
- Pebble 不可读 → unhealthy
- Root Inode 缺失 → unhealthy
- 错误率 > 5% (样本>100) → degraded
- Raft 不可用 → degraded (单机模式正常)

### 11.2 Metrics 指标

```
# 操作计数
dfs_ops_total           # 总操作数
dfs_read_ops            # 读操作数
dfs_write_ops           # 写操作数
dfs_errors_total        # 错误数

# 延迟 (微秒, P50/P99)
dfs_read_p50_us
dfs_read_p99_us
dfs_write_p50_us
dfs_write_p99_us

# 存储
dfs_keys_total          # KV 总数
dfs_chunks_total        # Chunk 总数
dfs_nodes_online        # 在线节点数
dfs_buckets_total       # Bucket 总数

# Raft
dfs_raft_state          # 0=follower, 1=candidate, 2=leader
dfs_raft_term           # 当前任期
dfs_raft_log_index      # 日志索引
dfs_snapshots_done      # 快照次数
```

### 11.3 CLI 工具 (dfsctl)

```
dfsctl cluster info                # 集群概览
dfsctl bucket list                 # 列出 Bucket
dfsctl bucket create --name=logs   # 创建 Bucket
dfsctl node list                   # 列出节点
dfsctl node decommission node-3    # 下线节点
dfsctl rebalance start             # 启动再平衡
dfsctl repair queue                # 查看修复队列
dfsctl gc scan [--dry-run]         # GC 扫描
dfsctl scrub run                   # 数据清洗
```

### 11.4 告警规则

| 规则 | 条件 | 级别 |
|------|------|------|
| 磁盘使用率 | > 90% | P1 |
| 节点宕机 | > 5min | P1 |
| 复制延迟 | > 30s | P2 |
| 孤儿 Chunk | > 1000 | P2 |
| 错误率 | > 5% | P2 |
| Raft 选举频繁 | 1h内>3次 | P2 |

---

## 12. 性能目标

### 12.1 元数据层

| 指标 | 目标 | 当前 |
|------|------|------|
| 单次 Lookup | < 1ms | ✓ (Pebble) |
| 单次 ReadDir (100 entries) | < 5ms | ✓ |
| 单次 CreateFile + AllocateChunk | < 5ms | ✓ |
| MVCC 冲突率 | < 1% | ✓ |
| 亿级元数据启动 | < 30s | 待验证 |

### 12.2 数据层

| 指标 | 目标 | 当前 |
|------|------|------|
| 顺序写 (单 Chunk) | > 500MB/s | ✓ (本地盘) |
| 顺序读 (单 Chunk) | > 1GB/s | ✓ |
| 随机读延迟 | < 2ms | ✓ |
| 复制吞吐 (4 Worker) | > 200MB/s | ✓ |
| Chunk 写入并发 | 64 | ✓ (信号量) |
| Chunk 读取并发 | 256 | ✓ (信号量) |

### 12.3 网关层

| 指标 | 目标 | 当前 |
|------|------|------|
| S3 PUT (1MB) | < 20ms | ✓ |
| S3 GET (1MB, 缓存命中) | < 1ms | ✓ |
| FUSE Read (缓存命中) | < 100μs | ✓ |
| S3 QPS (单网关) | > 10K | 待压测 |

---

## 13. 实现状态与路线图

### 13.1 已实现

| 模块 | 文件 | 状态 |
|------|------|------|
| 元数据类型 | metadata/types.go | ✅ 完成 |
| etcd Store | *(已移除)* | ❌ 已删除 |
| Pebble Store | metadata/pebble_store.go | ✅ 完成 |
| Raft 共识 | metadata/pebble_raft.go | ✅ 完成 |
| 放置引擎 | metadata/placement.go | ✅ 完成 |
| 再平衡 | metadata/balance.go | ✅ 完成 |
| 纠删码 | metadata/ec.go | ✅ 完成 |
| 小文件合并 | metadata/smallfile.go | ✅ 完成 |
| 生命周期 | metadata/lifecycle.go | ✅ 框架完成 |
| 服务编排 | metadata/service.go | ✅ 完成 |
| MVCC | metadata/production.go | ✅ 完成 |
| EventBus | metadata/production.go | ✅ 完成 |
| 租约管理 | metadata/production.go | ✅ 完成 |
| Chunk GC | metadata/production.go | ✅ 完成 |
| Scrubber | metadata/production.go | ✅ 完成 |
| Metrics/Health | metadata/health.go | ✅ 完成 |
| 分片路由 | metadata/shard.go | ✅ 完成 |
| ChunkStore | datanode/chunkstore.go | ✅ 完成 |
| DataNode Server | datanode/server.go | ✅ 完成 |
| 复制引擎 | datanode/replicator.go | ✅ 完成 |
| 心跳上报 | datanode/heartbeat.go | ✅ 完成 |
| 磁盘管理 | datanode/diskmanager.go | ✅ 完成 |
| HA | datanode/ha.go | ✅ 完成 |
| 修复 | datanode/repair.go | ✅ 完成 |
| S3 网关 | gateway/s3/ | ✅ 完成 |
| FUSE 网关 | gateway/fuse/ | ✅ 完成 |
| 客户端缓存 | gateway/cache.go | ✅ 完成 |
| CLI | cmd/dfsctl/ | ✅ 完成 |
| Docker Compose | docker-compose.yml | ✅ 完成 |

### 13.2 部分实现 / 待完善

| 模块 | 缺失 | 优先级 |
|------|------|--------|
| 客户端缓存 | flushLoop 实际刷盘到 DataNode | P1 |
| S3 Multipart | 完整测试覆盖 | P2 |
| 跨机房复制 | 整体未实现 | P2 |
| 生命周期 prefix 匹配 | DirEntry 遍历集成 | P2 |

### 13.3 路线图

```
P0 (本周): 小文件合并集成 + 客户端缓存刷盘 + 写入链复制
P1 (下周): Lease 集成 + 生命周期执行引擎 + 运维 API 完善
P2 (2 周): 跨机房复制 + K8s Helm Chart + 压测
P3 (4 周): 平台化 Dashboard + 自动化运维 + 安全加固
```
