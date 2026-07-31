# chunkstore

`chunkstore` 是 nufs 的**分布式存储引擎 SDK**。任何需要读写 chunk 数据的接入层
（S3 gateway、FUSE 挂载器、CLI、外部服务）都通过这个包与 datanode 集群交互，
而不必关心复制、纠删码、连接池、写仲裁等分布式细节。

## 在架构中的位置

```
┌──────────────────────────────────────────────┐
│  接入层  gateway/s3 · gateway/fuse · 外部服务  │
└──────────────────────┬───────────────────────┘
                       │  ChunkStore 接口
┌──────────────────────▼───────────────────────┐
│  chunkstore (本包 / SDK)                       │
│  · 复制路径: WritePipeline 并发扇出            │
│  · 纠删码路径: K+M 分片编码 + 并发读写         │
│  · 连接池 · 健康度排序读 · 写仲裁              │
└──────────────────────┬───────────────────────┘
                       │  datanode TCP 二进制协议
┌──────────────────────▼───────────────────────┐
│  datanode  (chunk 存储 + server)              │
└──────────────────────────────────────────────┘
```

依赖方向是单向的：`接入层 → chunkstore → datanode → metadata`。`chunkstore` 不依赖
任何接入层，因此外部服务可以单独 `import` 它作为存储引擎。

## 快速开始

```go
import (
    "context"
    "github.com/example/dfs/chunkstore"
    "github.com/example/dfs/metadata"
)

cs := chunkstore.NewDatanodeChunkStore()
defer cs.Close() // 释放连接池

ctx := context.Background()

// 写入一个 3 副本 chunk（复制模式）
chunk := &metadata.ChunkMeta{
    ID:   42,
    Size: int32(len(data)),
    Replicas: []metadata.ReplicaInfo{
        {NodeID: 1, Addr: "10.0.0.1:9100", State: metadata.ReplicaReady},
        {NodeID: 2, Addr: "10.0.0.2:9100", State: metadata.ReplicaReady},
        {NodeID: 3, Addr: "10.0.0.3:9100", State: metadata.ReplicaReady},
    },
}
if err := cs.WriteChunk(ctx, chunk, data); err != nil {
    log.Fatal(err)
}

// 读回（自动从最健康的副本读）
got, err := cs.ReadChunk(ctx, chunk)

// 范围读 [offset, offset+length)
part, err := cs.ReadChunkRange(ctx, chunk, 1024, 4096)
```

`ChunkMeta` 通常由元数据服务的放置引擎（`PlacementEngine.PlaceChunk`）分配，
调用方只需把分配好的 `ChunkMeta` 连同数据交给 `WriteChunk`。

## ChunkStore 接口

```go
type ChunkStore interface {
    WriteChunk(ctx context.Context, chunk *metadata.ChunkMeta, data []byte) error
    ReadChunk(ctx context.Context, chunk *metadata.ChunkMeta) ([]byte, error)
    ReadChunkRange(ctx context.Context, chunk *metadata.ChunkMeta, offset int64, length int32) ([]byte, error)
}
```

`offset=0, length=0` 表示读整个 chunk。

## 两种数据路径

实现根据 `chunk.ECGroup` 自动路由，调用方无需感知：

| 模式 | 判定 | 写入 | 读取 |
|------|------|------|------|
| **复制** | `chunk.ECGroup == nil` | 同一份数据经 `WritePipeline` 并发扇出到所有副本 | 按健康度排序（Ready > Syncing > Stale > Failed）逐个尝试，首个成功即返回 |
| **纠删码** | `chunk.ECGroup != nil` | 编码成 K+M 个分片，每个分片并发写到对应 datanode，**K 个成功即视为写完成** | 并发从 K+M 个 datanode 拉取分片，收集到 ≥ K 个即可解码还原（含缺失分片重建） |

### 写仲裁（Quorum）

- **复制模式**：默认要求**所有副本**都成功（生产级持久化语义）。可通过
  `DatanodeChunkStore.MinReplicasPerWrite` 调低门槛（如单节点测试设为 1）。
- **纠删码模式**：固定要求 **K 个**数据分片写入成功。

## 纠删码（EC）

EC 使用 `klauspost/reedsolomon`（SIMD 加速，AVX2/NEON）。编码器按 `K-M` 缓存
（`GetECEncoder`），避免每次读写重新分配。

### EC 配置

EC 通过 `ChunkMeta.ECGroup` 描述一个分片组：

```go
chunk := &metadata.ChunkMeta{
    ID:   100,
    Size: int32(len(data)),
    ECGroup: &metadata.ECGroupInfo{
        GroupID:      "group-1",
        DataShards:   4,   // K
        ParityShards: 2,   // M  → 可容忍 M 个分片丢失
    },
    Replicas: []metadata.ReplicaInfo{
        // 每个副本持有一个分片，ShardIndex ∈ [0, K+M)
        {NodeID: 1, Addr: "10.0.0.1:9100", ShardIndex: 0, State: metadata.ReplicaReady},
        {NodeID: 2, Addr: "10.0.0.2:9100", ShardIndex: 1, State: metadata.ReplicaReady},
        {NodeID: 3, Addr: "10.0.0.3:9100", ShardIndex: 2, State: metadata.ReplicaReady},
        {NodeID: 4, Addr: "10.0.0.4:9100", ShardIndex: 3, State: metadata.ReplicaReady},
        {NodeID: 5, Addr: "10.0.0.5:9100", ShardIndex: 4, State: metadata.ReplicaReady}, // parity
        {NodeID: 6, Addr: "10.0.0.6:9100", ShardIndex: 5, State: metadata.ReplicaReady}, // parity
    },
}
```

- `DataShards`（K）：原始数据被切成 K 份；读时需要至少 K 个分片才能解码。
- `ParityShards`（M）：校验分片数；**最多可容忍 M 个分片丢失**仍可读。
- `ShardIndex`：该副本持有第几个分片（`0..K-1` 为数据分片，`K..K+M-1` 为校验分片）。
- `chunk.Size` **必须等于原始数据长度**：编码时会补齐到 K 的整数倍，解码后按 `Size` 截断还原。

### 直接使用编码器

如需脱离 datanode 单独做编码/解码（例如离线校验、迁移工具）：

```go
enc := chunkstore.GetECEncoder(4, 2)          // K=4, M=2，缓存命中
result, _ := enc.Encode(data)                  // -> DataShards + ParityShards
ok := enc.Verify(result)                       // 校验一致性
recovered, _ := enc.Decode(shards, present, len(data)) // 缺失分片用 nil，present[i]=false
```

## TLS

集群启用 mTLS 时，在首次 I/O 前配置：

```go
cs := chunkstore.NewDatanodeChunkStore()
cs.SetTLS(tlsutil.Config{
    CertFile: "/etc/nufs/datanode.crt",
    KeyFile:  "/etc/nufs/datanode.key",
    CAFile:   "/etc/nufs/ca.crt",
})
```

## 资源管理

`NewDatanodeChunkStore` 内部持有 TCP 连接池。长驻进程应 `defer cs.Close()` 释放连接，
否则 datanode 侧的空闲连接会等到对端关闭或请求超时才回收。

## 测试

测试用 `MemoryChunkStore` 替代真实 datanode，无需任何外部依赖：

```go
cs := chunkstore.NewMemoryChunkStore()

// 可注入钩子模拟失败
cs.WriteHook = func(id metadata.ChunkID, _ []byte) error {
    return errors.New("simulated quorum failure")
}
cs.WriteChunk(ctx, chunk, data) // -> 返回 hook 错误

// 校验落盘内容
data, ok := cs.Get(chunk.ID)
```

集成测试（`ec_integration_test.go`）会在进程内启动真实 datanode TCP server，
覆盖 EC 的完整数据路径：写读往返、缺失数据分片重建、纯校验分片丢失容忍、
写仲裁不足失败、范围读。
