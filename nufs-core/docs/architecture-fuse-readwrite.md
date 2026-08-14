# NUFS FUSE 读写路径分析

## 1. 写入流程

### 1.1 小文件写入（5 字节）

```
用户进程: write(fd, "hello", 0)
    │
    ▼
Linux 内核 VFS
    │
    ▼
内核 page cache: 标记 offset 0 的 4KiB 页为 dirty
    │
    ▼  （攒多次写，或 auto_sync_period=5秒，或 fsync/close）
内核 writeback: 发 FUSE Write(offset=0, len=5, data="hello") 给 go-fuse daemon
    │
    ▼
NUFS DFSFile.writeLocked():
    ├─ getChunk(base=0): make([]byte, 64MiB)   ← 分配64MiB
    ├─ copy(buf[0:5], "hello")                  ← 写5字节进buffer
    └─ dirtyBytes += 64MiB
    │
    ▼  （close/fsync/内核触发 Flush）
NUFS DFSFile.Flush():
    ├─ chunkLen = min(64MiB, fileSize=5) = 5    ← 只取实际数据
    ├─ chunkData = buf[0:5]                     ← 5字节
    └─ WriteChunk(chunk, "hello")               ← 写5字节到chunkstore
           │
           ▼
    datanode ChunkStore.Write():
        ├─ 写 header(16B) + data(5B) = 21B     ← 磁盘写21字节
        ├─ WAL: LogWrite(chunkID, 21B)          ← WAL日志
        └─ fsync()                              ← 刷盘
    │
    ▼  （replicated R=3）
3个datanode各写21字节（网络往返）
    │
    ▼
NUFS Flush 收尾：
    ├─ 写 inode ChunkMap[{off=0, chunkID, len=5}]
    └─ Raft commit（元数据一致性）
```

### 1.2 大文件顺序写入（128 MiB）

```
用户进程: write(fd, 128MiB_data, 0)
    │
    ▼
内核 page cache: 多个 4KiB 页连续 dirty
    │
    ▼
内核 writeback: 批量 FUSE Write（可能分多次调用）
    │
    ▼
DFSFile.writeLocked():
    ├─ base=0: getChunk(0) → 64MiB buffer → copy 前64MiB
    └─ base=64M: getChunk(64M) → 64MiB buffer → copy 后64MiB
    │
    ▼
DFSFile.Flush():
    ├─ base=0: chunkLen=64MiB, WriteChunk(chunkA, 64MiB)
    └─ base=64M: chunkLen=64MiB, WriteChunk(chunkB, 64MiB)
    │
    ▼
2个 datanode 写入 × R 副本 + Raft 提交
```

**写入量**：128 MiB → 写 128 MiB（1:1，无放大）

### 1.3 中间覆写（读-改-写）

```
已有128MiB文件，在 offset 64MiB+100 写入4字节:
    │
    ▼
DFSFile.writeLocked():
    ├─ base=64MiB → chunkBufs[64M] 不存在
    ├─ loadCommittedChunkLocked(64M)
    │  └─ ReadChunkRange(chunk, rel=100, len=4KiB) ← 精确range读
    ├─ getChunk(64M) → 64MiB buffer
    ├─ copy(buf[100:104], "new")
    └─ dirtyBytes += 64MiB
    │
    ▼
DFSFile.Flush():
    ├─ base=64M: chunkLen=64MiB（完整buffer）
    ├─ WriteChunk(newChunk, 64MiB)   ← 含committed+新写入的完整image
    └─ inode ChunkMap 更新 → Raft
```

**注意**：loadCommittedChunkLocked 用 range read 只拉需要的窗口（读放大已消除），但 buffer 仍分配 64MiB（内存开销不变）。

## 2. 读取流程

### 2.1 读已提交数据

```
用户进程: read(fd, 4KiB, offset=0)
    │
    ▼
DFSFile.Read():
    ├─ size = max(metaInode.Size, logicalSize)
    ├─ 4KiB buffer 全零
    │
    ├─ readChunkRange(0, 4KiB):
    │  ├─ chunkMap 遍历找到重叠 cref
    │  ├─ cache.Get(chunkID, 0) → 命中则用缓存
    │  └─ miss → ReadChunkRange(chunk, 0, 4KiB) ← 只拉4KiB窗口
    │
    └─ 返回 ReadResult（4KiB）
```

### 2.2 读放大已消除

**改造前**：
```
ReadChunkRange(chunk, 0, 4KiB) → ReadChunk(ctx, chunk) → 读整块64MiB
```

**改造后**：
```
ReadChunkRange(chunk, 0, 4KiB) → 只拉 [0, 4KiB) 窗口
```

分片缓存（chunkSliceKey）让同窗口二次读命中。

## 3. 放大分析

### 3.1 写放大

| 场景 | 实际数据 | 内存分配 | 磁盘/网络写入 | 总放大 |
|------|---------|---------|-------------|--------|
| 5字节小文件 | 5B | 64MiB（Flush后释放） | ~40B（含header+Raft+复制） | 8× |
| 64MiB 大文件 | 64MiB | 64MiB | 64MiB×R（复制） | R× |
| 中间覆写4B | 4B | 64MiB（Flush后释放） | 64MiB（完整chunk） | ~16M× |

**结论**：磁盘/网络写入放大不高（小文件8×，大文件R×），但**内存分配放大**对小文件是12,800,000×（5字节分配64MiB）。Flush 后内存释放。

### 3.2 读放大（已修复）

| 场景 | 改造前 | 改造后 |
|------|--------|--------|
| 4KiB 读（replicated） | 拉整块64MiB | 只拉4KiB窗口 |
| 4KiB 读（EC6+3） | 9×全shard读+解码64MiB | 6×shard读（最小集）+解码64MiB |

### 3.3 内存控制

| 机制 | 默认值 | 保护范围 |
|------|--------|---------|
| maxDirtyBytes（per-file） | 1 GiB | 单文件脏buffer上限，超过触发spill |
| globalDirtyBudget（跨文件） | 0（禁用） | 多文件并发写无全局保护 |
| spill 机制 | 内置 | 脏buffer溢出到磁盘staging |

## 4. 已知架构权衡

### 4.1 64 MiB chunk 大小

**优势**：
- 大文件连续写 1:1 放大（最优）
- 元数据条目数可控（64MiB/1GiB=16 chunks）
- 网络 round-trip 数可控

**劣势**：
- 小文件（<64MiB）每次写分配64MiB内存
- 随机小写跨 base 时每次新 base 分配64MiB
- 全局 dirty budget 默认禁用

### 4.2 海量小文件场景

**问题**：每个小文件首次写分配64MiB buffer，100万文件=100万×64MiB=64TiB（但受 dirty budget 限制会 spill）。

**当前保护**：
- per-file dirty budget = 1 GiB（spill 做背压）
- globalDirtyBudget = 0（跨文件无全局限制）

**潜在改进**：
- 启用 globalDirtyBudget（1行改动）
- 小文件内联（文件<阈值时数据存inode，不走chunk pipeline）
- 批量Flush（多文件合并一次Raft）

### 4.3 业界对比

| 方案 | daemon buffer 大小 | 小文件放大 |
|------|------------------|-----------|
| NUFS | 64 MiB | 64MiB/4KiB = 16384× 内存 |
| CephFS | 4 MiB（RADOS object） | 4MiB/4KiB = 1024× 内存 |
| SeaweedFS | ≤8MiB 内联到 metadata | 0×（不分配buffer） |
| GlusterFS | 128 KiB brick | 128KiB/4KiB = 32× 内存 |

## 5. 修复记录

### 5.1 FUSE 读放大消除

**Commit**: 3270913, ac4db9a

**改动**：readChunkRange / loadCommittedChunkLocked / zeroRange 三处 `ReadChunk`（整块64MiB）改为 `ReadChunkRange`（精确窗口）。分片缓存（chunkSliceKey{id,off}）让同窗口二次读命中。

**效果**：4KiB 读从拉64MiB降到拉4KiB（network），内存不变（仍需decode），缓存命中保留。

### 5.2 EC 读放大消除

**Commit**: 25a890d

**改动**：readECChunk 计算最小 shard 集（只读重叠 data shards + 必要 parity），ReadECShard RPC 加 offset/length，V2Store 用 ReadRangeFrames。

**效果**：9 shard 读降到 6（~33% 网络节省），解码内存不变。

### 5.3 Cache 线程安全 + 死循环防护

**Commit**: ac4db9a

**改动**：ChunkCache 加 sync.Mutex 保护所有 LRU 操作；readChunkRange 外层 while 检测 pos 未前进并 break。

## 6. 待改进项

| 项 | 类型 | 改动量 | 效果 |
|----|------|--------|------|
| 启用 globalDirtyBudget | 配置 | 1 行 | 防内存爆炸 |
| 小文件内联 | 架构 | 中等 | 小文件零chunk开销 |
| 批量 Flush | 优化 | 中等 | 减少Raft提交 |
| 可变 chunk 大小 | 架构 | 大 | 根本解决小文件放大 |
