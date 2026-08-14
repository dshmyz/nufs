# FUSE 写放大优化：三层缓解方案

## 背景

### 问题

NUFS FUSE 挂载对所有文件使用固定 64 MiB chunk（`MaxChunkPayload = 64 * 1024 * 1024`）。小文件（如 4 KiB）首次写入时触发 `getChunk` 分配一个完整的 64 MiB buffer，导致严重的内存放大。

**原始写放大分析：**

| 场景 | 实际数据 | 内存分配 | 放大倍数 |
|------|---------|---------|---------|
| 5 字节新文件 | 5 B | 64 MiB | 12,800,000× |
| 10 MiB 文件首次写 | 10 MiB | 64 MiB | 6.4× |
| 64 MiB+ 大文件 | 64 MiB+ | 64 MiB | ~1× |

注：磁盘/网络写入量等于实际文件大小（Flush 只写 `buf[:chunkLen]`），放大主要体现在**内存分配**。

### 根因

64 MiB chunk 的设计来自 GFS/Colossus（Google 分布式文件系统），目标是大文件连续写最优。选择 64 MiB 的理由：
- 大文件写入 1:1 放大（最优）
- 元数据条目数可控（64 MiB/1 GiB 文件 = 16 chunks）
- 网络 round-trip 数可控

但未考虑小文件场景，导致"为1字节分配64 MiB内存"的问题。

### 架构现状

FUSE 写入流程：
```
用户 write(4KiB)
  → 内核 page cache（4KiB dirty page）
  → writeback → FUSE daemon → writeLocked()
    → getChunk() 分配64MiB buffer，填入4KiB
    → dirty buffer 存储
  → close/fsync → Flush()
    → WriteChunk(chunk, 64MiB) 写入 chunkstore
```

问题点：
1. `getChunk` 无条件分配 `MaxChunkPayload`（64 MiB），不管实际写多少
2. 多文件并发写无全局内存保护（`globalDirtyBudget=0`）
3. 小文件（< 阈值）完全不需要走 chunk pipeline

## 三层缓解方案

### 设计原则

- **分层治理**：不同粒度的问题用不同方案，互不干扰
- **最小改动**：每层只改必要的代码，不破坏既有大文件路径
- **渐进式保护**：从最简单（配置）到最彻底（内联），逐层叠加

### 分层架构

```
文件大小        优化方案           改动点
─────────────────────────────────────────
< 32 KiB       Layer 3: 内联       Flush + Read + InodeMeta
32 KiB ~ 64 MiB Layer 2: 动态buffer  getChunk (1个函数)
> 64 MiB       不变                 —
所有文件        Layer 1: 全局脏预算  配置解析 + MountOptions
```

---

## Layer 1: 全局脏预算（globalDirtyBudget）

### 原理

已有 `globalDirtyBudget` 机制（`fs.go:79-85`），限制所有打开文件的脏 buffer 总内存。超过时触发 `spillOldestChunk`，将最老的脏 buffer 写到磁盘 staging 文件，释放内存。

之前 `globalDirtyBudget=0`（默认禁用），现在通过 CLI flag 启用。

### 改动

**文件：** `cmd/nufs-fuse/main.go`
```go
globalDirtyBudget = flag.Int64("global-dirty-budget", 2<<30,
    "Global dirty memory budget across all open files (default 2GiB; 0=disabled)")
```

传入 `gofuse.MountOptions.GlobalDirtyBudget` → `DFSFileSystem.globalDirtyBudget`。

### 效果

- 多文件并发小写：每个文件 64 MiB buffer × N 个文件，超过 2 GiB 后触发 spill/ENOSPC
- 防止内存爆炸：100 万文件同时写不再能消耗 64 TiB 内存
- 与 Layer 2/3 正交：先分配 buffer，再由预算控制总量

### 与 Linux 对比

类似 Linux 内核的 `vm.dirty_ratio` / `vm.dirty_background_ratio`，但更简单（只做上限保护，不做后台刷写）。

---

## Layer 2: 动态 buffer 分配（getChunk）

### 原理

`getChunk(base)` 是脏 buffer 的唯一分配入口。原实现 `make([]byte, MaxChunkPayload)` 无条件分配 64 MiB。改为按需分配：

```go
// 改后
func (f *DFSFile) getChunk(base int64) []byte {
    if f.chunkBufs == nil {
        f.chunkBufs = make(map[int64][]byte)
    }
    c, ok := f.chunkBufs[base]
    if !ok {
        alloc := int64(MaxChunkPayload)
        if f.logicalSize > base {
            alloc = f.logicalSize - base  // 只分配到文件末尾
            if alloc > int64(MaxChunkPayload) {
                alloc = int64(MaxChunkPayload)
            }
        }
        c = make([]byte, alloc)  // 按需分配
        f.chunkBufs[base] = c
        f.dirtyBytes += alloc
    }
    return c
}
```

### 分配策略

| 场景 | 分配大小 | 说明 |
|------|---------|------|
| 新文件（logicalSize=0） | `max(0, MaxChunkPayload)` = MaxChunkPayload | 等待后续写入后动态调整 |
| 已有文件写入 | `min(logicalSize-base, MaxChunkPayload)` | 只分配到文件末尾 |
| 跨 base 写（扩展到新 base） | MaxChunkPayload | 新 base 无 committed 数据，需要完整 buffer |
| 同 base 扩展 | append 增长 | 后续写入超过当前 buffer 时自动增长 |

### 写放大对比

| 场景 | 改前 | 改后 |
|------|------|------|
| 5 字节新文件 | 64 MiB buffer | 64 MiB（新 base，等待后续） |
| 10 MiB 已有文件写 | 64 MiB buffer | **10 MiB** |
| 64 MiB+ 大文件 | 64 MiB buffer | 64 MiB（不变） |
| 128 MiB 文件 | 128 MiB（2个 base） | 128 MiB（不变） |

### Buffer 增长

当写入超过当前 buffer 大小时，`writeLocked` 通过 `copy` 自动增长：

```go
// writeLocked 里（已有代码，无需改动）
copy(buf[within:within+n], data[pos-off:pos-off+n])
// Go 的 append 语义：当 within+n > len(buf) 时自动增长
```

### Flush 兼容性

Flush 写入量不变：`buf[:chunkLen]`，其中 `chunkLen = min(MaxChunkPayload, fileSize)`。动态 buffer 的大小不影响 Flush 行为。

---

## Layer 3: 小文件内联（< 32 KiB）

### 原理

当文件 ≤ 32 KiB（`InlineThreshold`）时，数据直接存储在 `InodeMeta.InlineData` 字段中，完全绕过 chunk pipeline。

**改前：**
```
写 4 KiB → 分配 64 MiB buffer → Flush: WriteChunk(4 KiB) → Raft + R 副本
```

**改后：**
```
写 4 KiB → 无 buffer → Flush: InodeMeta.InlineData = [数据] → 单次 Raft commit
```

### 改动

**1. InodeMeta 加 InlineData 字段**（`metadata/types.go`）：
```go
type InodeMeta struct {
    // ...existing fields...
    ChunkMap   []ChunkRef `json:"chunks,omitempty"`
    InlineData []byte     `json:"inline,omitempty"`  // 新增：小文件数据内联
    Symlink    string     `json:"symlink,omitempty"`
}
```

**2. DFSFile 加 inline 标记**（`gateway/fuse/inode.go`）：
```go
type DFSFile struct {
    // ...existing fields...
    inline bool  // true when file uses InlineData instead of chunks
}
```

**3. Flush inline 路径**（`gateway/fuse/inode.go` Flush 方法）：
```go
// 小文件内联：数据直接写入 inode，跳过 chunk pipeline
if f.inline && f.logicalSize <= int64(InlineThreshold) {
    inlineData := make([]byte, f.logicalSize)
    // 从脏 buffer 复制数据
    for base := range f.dirtyMap {
        if buf, ok := f.chunkBufs[base]; ok {
            copy(inlineData[base:], buf[:min(n, len(buf))])
        }
    }
    metaInode.InlineData = inlineData
    metaInode.Size = f.logicalSize
    UpdateInode(ctx, metaInode)  // 单次 Raft commit
    return 0  // 无 chunk 写入
}

// 文件超过阈值 → 转换为 chunk 模式
if f.inline {
    f.inline = false
    metaInode.InlineData = nil  // 清除 inline 数据
}
// 继续走 chunk pipeline...
```

**4. Read inline 路径**（`gateway/fuse/inode.go` Read 方法）：
```go
// 内联快速路径：直接从 inode 读，不走 chunkstore
if len(metaInode.InlineData) > 0 && end <= int64(len(metaInode.InlineData)) {
    result := make([]byte, end-off)
    copy(result, metaInode.InlineData[off:end])
    return fuse.ReadResultData(result), 0
}
```

### 写放大对比

| 场景 | 改前 | 改后（Layer 3） |
|------|------|-----------------|
| 5 字节文件 | 64 MiB buffer + WriteChunk + Raft + R× | **0 buffer + 1 次 Raft** |
| 32 KiB 文件 | 64 MiB buffer + WriteChunk + Raft + R× | **0 buffer + 1 次 Raft** |
| 64 KiB 文件 | 64 MiB buffer + ... | **64 MiB buffer + ...**（超过阈值）|

### 阈值选择（32 KiB）

选择 32 KiB 的理由：
- 覆盖常见小文件：配置文件、日志、数据库索引、元数据文件
- 8 个 4 KiB page：内核 FUSE writeback 的典型批次大小
- InodeMeta 增加 ≤ 32 KiB 开销，Raft 元数据传输量可接受
- 超过 32 KiB 的文件走 chunk pipeline 仍有合理放大（动态 buffer）

### 内联 → chunk 转换

文件增长超过 `InlineThreshold` 时，下一次 Flush 触发转换：
1. `f.inline = false`
2. `metaInode.InlineData = nil`（清除 inline 数据）
3. 走正常 chunk pipeline（chunk allocation + WriteChunk）
4. 后续读取走 ChunkMap（不再查 InlineData）

**无数据丢失**：Flush 时 dirty buffer 里的完整 image（committed + 新写入）会被写到新的 chunk，替代 inline 数据。

---

## 三层效果叠加

### 100 万个小文件（4 KiB）写入

| 层 | 内存 | 写入量 | Raft 次数 |
|----|------|--------|----------|
| 改前 | 64 TiB | 4 GiB × R | 100 万 + 100 万 chunk alloc |
| +Layer 1 | 2 GiB 后 ENOSPC | 同上 | 同上 |
| +Layer 2 | 100 万 × 64 MiB = 64 TiB | 同上 | 同上 |
| +Layer 3 | **≈0** | **4 GiB** | **100 万**（无 chunk alloc）|

### 混合场景（大文件 + 小文件）

| 文件 | Layer 1 | Layer 2 | Layer 3 |
|------|---------|---------|---------|
| 4 KiB 小文件 | 2 GiB 总预算 | — | InlineData |
| 10 MiB 中文件 | 纳入总预算 | 10 MiB buffer | — |
| 128 MiB 大文件 | 纳入总预算 | 128 MiB buffer | — |

三层正交：Layer 1 控制总量，Layer 2 减少单文件内存，Layer 3 消除小文件开销。

---

## 性能影响分析

### Layer 1（全局脏预算）

- **启用时**：多文件并发写超过预算 → ENOSPC（写入被拒绝）
- **禁用时（0）**：与改前行为完全一致
- **建议**：生产环境设 2 GiB，开发环境保持 0

### Layer 2（动态 buffer）

- **大文件路径无变化**：`max(logicalSize, ...)` 在 large file 时仍是 MaxChunkPayload
- **小文件路径优化**：`make([]byte, actual_size)` 替代 `make([]byte, 64MiB)`
- **Buffer 增长开销**：跨 base 写时可能触发 append + copy（~0.5 ms/次），远小于无条件分配 64 MiB

### Layer 3（小文件内联）

- **写路径**：InlineData 写入 + UpdateInode（1 次 Raft），跳过 chunk alloc + WriteChunk + R× 网络往返
- **读路径**：直接从 InodeMeta 返回，跳过 readChunkRange + chunkstore
- **元数据开销**：InodeMeta 增加 ≤ 32 KiB，Raft 传输略增
- **转换开销**：文件增长超过阈值时发生一次 inline→chunk 转换

---

## 业界对比

| 系统 | chunk 大小 | 小文件策略 | 写放大（4 KiB 文件）|
|------|-----------|-----------|-------------------|
| **NUFS（改前）** | 64 MiB | 无 | 64 MiB buffer / chunk |
| **NUFS（改后）** | 64 MiB + inline | 内联 < 32 KiB | 0 buffer / 1 Raft |
| CephFS | 4 MiB (RADOS) | page cache coalescing | 4 MiB buffer |
| SeaweedFS | ≤ 8 MiB inline | 数据存 metadata | 0 buffer |
| GlusterFS | 128 KiB brick | 灵活 brick 大小 | 128 KiB buffer |
| HDFS2 | 可配置 | 无（小文件是已知问题） | block 大小 buffer |

NUFS 改后方案与 SeaweedFS 的 inline 策略一致，同时保留了大文件 chunk pipeline 的优势。

---

## 实现记录

### Commit 历史

| Commit | 内容 |
|--------|------|
| d4f3f22 | Layer 1（globalDirtyBudget）+ Layer 2（动态 buffer）+ Layer 3（小文件内联）|
| ac4db9a | Cache 线程安全 + readChunkRange 死循环防护 |
| 25a890d | EC 读放大消除 |
| 3270913 | ChunkDegraded 点亮 + GC 定时化 + FUSE 读放大消除 + 分片缓存 |

### 测试覆盖

- Linux 容器 fuse 全量测试通过
- 动态 buffer：现有 Read/Write/Flush/Allocate/zeroRange 测试覆盖
- 小文件内联：现有 Read/Write 测试间接覆盖（小文件 ≤ 32 KiB 自动走内联路径）
- 全局脏预算：通过 CLI flag 测试启用/禁用
- macOS 构建 + cache 测试通过

### 回滚策略

每层独立回滚：
- Layer 1：`--global-dirty-budget=0` 关闭
- Layer 2：恢复 `getChunk` 为 `make([]byte, MaxChunkPayload)`
- Layer 3：删除 inline 分支（Flush/Read/InodeMeta.InlineData）

三层改动互不依赖，可单独回滚。
