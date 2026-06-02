# FUSE 网关功能补完：设计文档

> 状态：待审 | 适用范围：`nufs-core/gateway/fuse/` + `nufs-core/cmd/fusegw/` + `nufs-core/metadata/`

## 0. 背景

`nufs-fuse/` 在提交 `705e523` 中被整目录删除，原因是它用的是老 `bazil.org/fuse` + BadgerDB + MinIO S3 客户端，整套技术与新架构（`hanwen/go-fuse/v2` + Pebble + Raft + 自研元数据）不兼容。删除时已经把 5 个无 FUSE 依赖的工具模块（breaker/retry/lock/signals/metrics）lift 到了 `internal/resilience/`，但 **FUSE 协议层**和 **MinIO 凭证链路**没有补回。

`nufs-core/gateway/fuse/` 在 `9d93522 init` 提交里**重写**过，但只覆盖了 FUSE 协议层的最小集：
- `dfs.go` 根文件系统
- `dir.go` 目录操作（274 行）
- `inode.go` 文件 + 符号链接结构 + 句柄（230 行）

且**当前 `DFSFile.Read` 是占位实现**——返回零字节不读 datanode，**`DFSFile.Write` 写到内存 buffer 后只在 `Flush` 时改 inode size，datanode 上没有数据**。也就是说当前 fusegw 写文件后从 s3gw 读不到，是**坏的**。这个 bug 不在新功能里，但它阻塞了 cache 价值的体现——所以**先修这个 bug**，再做功能补完。

## 1. 范围与目标

### 1.1 要补回的 6 项功能

| # | 功能 | 旧位置 | 行数 |
|---|------|--------|------|
| F1 | readBufPool：128 KB `sync.Pool` 复用读缓冲 | `nufs-fuse/fs/file.go:23-29` | 30 |
| F2 | DFSSymlink 协议层补全：Access + Setattr（Readlink/Getattr 已有） | `nufs-fuse/fs/symlink.go` | 60 |
| F3 | xattr：4 个方法（Get/Set/List/Remove）+ FUSE 协议层 | `nufs-fuse/fs/xattr.go` | 150 |
| F4 | 配置 flag：read-only / scan TTL / metrics addr / max-read 等 | `nufs-fuse/fs/config.go` | 200 |
| F5 | 本地读穿透缓存：LRU + 配额淘汰 | `nufs-fuse/fs/filehandle.go` 的 cache 部分 | 250 |
| F6 | MinIO 兼容 `config.json` 凭证加载 | `nufs-fuse/fs/config.go` 凭证部分 | 200 |

### 1.2 顺手要修的 Bug

| 编号 | Bug | 影响 |
|------|-----|------|
| B1 | `DFSFile.Read` 返回零字节 | fusegw 读不到任何文件 |
| B2 | `DFSFile.Write` 不写 datanode | fusegw 写文件后外部读不到 |
| B3 | `Flush` 后 `f.buffer` 不清零，导致二次 Flush 时 size 计算错误 | 大小错误 |

B1-B3 必须在 F5（缓存）之前修，否则缓存就缓存的是"零字节"。

## 2. 架构变更

### 2.1 新增 `gateway/fuse/` 文件

```
nufs-core/gateway/fuse/
├── fs.go               (已存在) 不动
├── dir.go              (已存在) 不动
├── inode.go            (已存在) 改：补 readBufPool（B1+F1），重写 Write/Flush（B2+B3）
├── symlink.go          (新增 F2) DFSSymlink.Access + Setattr
├── xattr.go            (新增 F3) DFSXAttr Node + inode.XAttrs 操作
├── config.go           (新增 F4) MountConfig 结构 + flag 解析
├── cache.go            (新增 F5) LRU 缓存 + 磁盘配额
├── minfs_config.go     (新增 F6) 加载 /etc/dfs/minfs.json 的可选凭证
└── *_test.go           (新增) 每个文件配单元测试
```

### 2.2 新增 `metadata/` 改动

`MetadataService` 接口要加 4 个方法（`pebble_store.go` 实现）：

```go
GetXAttr(ctx, id, name) ([]byte, error)
SetXAttr(ctx, id, name, value) error
ListXAttr(ctx, id) (map[string][]byte, error)
RemoveXAttr(ctx, id, name) error
```

`InodeMeta.XAttrs` 字段已经存在（`types.go:49`），存储层只需 putJSON/getJSON。
**这 4 个方法会改 6 个 mock 实现**：`gateway/s3/handler_test.go`、`datanode/repair_test.go`、fuse 的 fake 等。

### 2.3 新增 `cmd/fusegw/main.go` 改动

把单文件入口**重写**为 5 个 flag + `gofuse.Mount` 调用。新增 flag：
- `-read-only`：禁用写操作
- `-scan-ttl`：目录列表缓存时间（默认 60s）
- `-metrics-addr`：metrics HTTP 监听（默认 `:9901`）
- `-cache-dir`：本地读缓存目录（默认 `/var/cache/dfs/fuse`）
- `-cache-quota`：缓存磁盘配额（默认 1 GiB，0 = 不限）
- `-minfs-config`：MinIO 兼容 config.json 路径（默认禁用）

入口行数预计 50 行左右，**不拆**——单一职责。

## 3. 6 个原子提交

按依赖排序，每个独立可编译可测。

### Commit 1: `fuse: real Read/Write path against datanode (B1+B2+B3)`

修最严重的 bug——fusegw 当前完全不能用。

**改**：
- `inode.go::DFSFile` 加 `chunkStore metadata.ChunkStore` 字段
- `DFSFile.Read` 通过 `meta.GetInode` → `ChunkMap` → `chunkStore.ReadChunk` 真实读
- `DFSFile.Write` 写到 `chunkStore.WriteChunk`（同步写，FS 语义保证）
- `Flush` 释放 buffer + 更新 `inode.Size` + 调用 `chunkStore.CommitChunk` + `SealChunk`
- `fuse_test.go` 加 `TestDFSFile_ReadEmptyFile`、`TestDFSFile_WriteUpdatesSize`
- 用 `mockMetaService`（fuse 包内新增 fake），**不依赖真实 datanode**

**风险**：`Flush` 时若 buffer 跨越多个 chunk，需要分块 `AllocateChunk`。第 1 步只支持**单 chunk**（buffer 长度 ≤ `MaxChunkSize`）；多 chunk 留到 cache commit 时做。

### Commit 2: `fuse: pool read buffers with sync.Pool (F1)`

`inode.go` 加 `readBufPool`：

```go
var readBufPool = sync.Pool{
    New: func() interface{} {
        buf := make([]byte, 128*1024)
        return &buf
    },
}
```

`Read` 改用 `pool.Get()` 取、`pool.Put()` 还。**测试**：`TestReadBufPool_Reuse` 跑 1000 次读，断言 pool 内对象数稳定。

### Commit 3: `fuse: add MetadataService xattr methods + DFSXAttr node (F3)`

**改**：
- `metadata/service.go` 加 4 个 xattr 方法
- `metadata/pebble_store.go` 实现这 4 个
- `metadata/keys.go` 加 `prefixXAttr` key prefix（不必要——`XAttrs` 字段在 inode JSON 里）
- 6 个 mock 加 4 个 stub
- `gateway/fuse/xattr.go` 加 `DFSXAttr` 节点 + `NodeGetxattrer` / `NodeSetxattrer` / `NodeListxattrer` / `NodeRemovexattrer` 接口
- `inode.go::DFSFile`、`DFSDir`、`DFSSymlink` 加 `OpenXAttr` 方法（hanwen 接口要求独立 handle）
- **测试**：`TestDFSXAttr_GetSetRoundTrip`、`TestDFSXAttr_ListMultiple`、`TestDFSXAttr_NotFound`

### Commit 4: `fuse: complete symlink protocol (Access + Setattr) (F2)`

**改**：
- `gateway/fuse/symlink.go`（从 inode.go 拆出来）
- `DFSSymlink.Access` 走 `fs.NodeAccesser`（默认 `0`）
- `DFSSymlink.Setattr` 走 `fs.NodeSetattrer`，更新 uid/gid/atime/mtime
- `DFSSymlink.Dirent` 用于 Readdir 输出（已经在 dir.go 里手动构造，不需要改）
- **测试**：`TestDFSSymlink_Readlink`、`TestDFSSymlink_SetattrUpdatesMode`

### Commit 5: `fuse: local read-through LRU cache with disk quota (F5)`

**改**：
- `gateway/fuse/cache.go` 新建 `ChunkCache` 类型
  - 内存层：`hashicorp/golang-lru`（已在 go.mod 间接依赖）做 inode → chunk payload LRU
  - 磁盘层：`<cacheDir>/<bucket>/<key>/<chunkID>` 落盘，目录总大小超 `quota` 时按 mtime 删最旧
  - 接口：`Get(ctx, chunkID) ([]byte, bool)`、`Put(ctx, chunkID, data) error`、`Stats() (hits, misses, bytesCached)`
- `inode.go::DFSFile` 加 `cache *ChunkCache` 字段
- `DFSFile.Read` 改为：cache miss → chunkStore.ReadChunk → cache.Put → 返回
- `cmd/fusegw/main.go` 加 `-cache-dir` / `-cache-quota` flag
- **测试**：`TestChunkCache_LRUEviction`（用临时目录）、`TestChunkCache_QuotaEnforcement`

### Commit 6: `fuse: configurable MountConfig + optional MinIO config.json (F4+F6)`

**改**：
- `gateway/fuse/config.go` 新建 `MountConfig` struct：
  - `ReadOnly bool`
  - `ScanTTL time.Duration`
  - `MaxReadAhead int`
  - `MetricsAddr string`
- `gateway/fuse/minfs_config.go` 新建 `LoadMinFSConfig(path string) (*MinFSConfig, error)`
  - 解析 `/etc/dfs/minfs.json`（`{accessKey, secretKey, secretToken, target URL}`）
  - 注入到 metadata 客户端的凭证
- `fs.go::Mount` 接受 `MountConfig` 参数
- `cmd/fusegw/main.go` 加 5 个 flag
- `gateway/fuse/metrics.go`（F4 子项）：用 `internal/resilience/metrics` 起一个 `/metrics` HTTP 服务
- **测试**：`TestMountConfig_ParsesFlags`、`TestLoadMinFSConfig_ValidJSON`、`TestLoadMinFSConfig_MissingFile`

## 4. 测试策略

**macOS 限制**：`gateway/fuse` 整个包是 `//go:build linux`，无法在 macOS 上跑挂载测试。处理：

- 所有**纯逻辑测试**（mock、不挂载）放 `fuse_unit_test.go`，**不带 build tag**——macOS 上能跑
- **挂载端到端测试**放 `fuse_e2e_test.go`，带 `//go:build linux && e2e`——CI 上跑
- 5 个新文件的单元测试覆盖 80%+ 逻辑
- B1+B2 修复的测试**必须**用真实 mock，跑通：
  - 创建一个 inode
  - DFSFile.Write 写 100 字节
  - DFSFile.Read 读回 100 字节一致
  - mockMetaService.GetInode 看到 size=100

## 5. 风险与备选

| 风险 | 缓解 |
|------|------|
| B1+B2 修复后 read 路径可能 panic（chunk 找不到等） | mockMetaService 完整覆盖；保留 nil-safe 行为 |
| 缓存 quota 用 mtime 删会误删正在读的 | 缓存用 mtime 时，Read 先 `O_RDONLY` 打开文件描述符再 stat；或换成"打开即 touch atime" |
| hanwen/go-fuse v2 与 v1 API 差异 | 当前代码已经在用 v2（看 import）；新加接口用 v2 风格 |
| Config.json 凭证覆盖 `gos3.NewCredentialStore` 时序 | 优先级：flag > env > config.json > anonymous |

## 6. 完成后要跑

- `go vet ./...`
- `go test ./...`（macOS 跳过 `gateway/fuse` 集成测试）
- `go test -race ./...`
- 提交后 git log 检查 6 个独立 commit

## 7. 不在本计划里的

- FUSE 性能基准（`dstat`/`fio` 对比 MinFS）—— 单独工作
- 异步写缓冲——目前用同步写保证 FS 一致性
- 写穿透缓存（write-back）—— 会破坏 POSIX 语义
- fuse 协议层对 xattr namespace 完整支持（`security.`、`user.`、`trusted.` 区分）—— 第 1 版只支持 `user.`，跟 MinFS 一致
