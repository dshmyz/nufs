# S3 Bucket FUSE Mount (s3fsgw)

## 1. 概述

将远端 S3 桶（MinIO、阿里 OSS、AWS S3 等）通过 FUSE 挂载为本地文件系统。不需要运行 NUFS metadata 服务或 datanode，S3 是唯一的数据源。

### 1.1 目标用户

- 需要本地文件系统接口访问 S3 数据的开发者
- 需要将 S3 桶作为共享存储挂载到多台机器的团队
- 需要 POSIX 兼容接口操作 S3 对象的应用

### 1.2 设计约束

- **S3 是唯一数据源**：不依赖 NUFS metadata/datanode
- **兼容 POSIX 语义**：在 S3 能力范围内提供完整的文件系统语义
- **本地元数据缓存**：使用 Pebble 缓存 S3 对象元数据，减少 S3 请求
- **异步写入**：写操作缓冲到本地缓存，Flush 时异步上传 S3
- **崩溃安全**：未完成的上传记录在本地，重启后重放
- **只读模式**：可选，禁用所有写操作

## 2. 架构

```
┌──────────────────────────────────────────────────────────────┐
│                     cmd/s3fsgw/main.go                       │
│                CLI flags + daemon startup                     │
└────────┬─────────────────────────────────────┬───────────────┘
         │                                     │
┌────────▼─────────────┐          ┌────────────▼───────────────┐
│    S3FileSystem      │          │      PebbleCache           │
│  (gateway/s3fs/)     │◄────────►│   (本地元数据缓存)           │
│                      │          │   inode + dir entries       │
│  - S3 客户端         │          │   scan TTL 刷新             │
│  - 异步上传队列      │          └────────────────────────────┘
│  - 路径锁            │
│  - 熔断器            │
│  - 指标收集          │
└────────┬─────────────┘
         │
┌────────▼─────────────────────────────────────────────────────┐
│              go-fuse v2 FUSE layer                           │
│  S3Dir / S3File / S3FileHandle / S3Symlink                   │
│  Lookup / Readdir / Open / Read / Write / Flush / Release    │
└────────┬─────────────────────────────────────────────────────┘
         │
┌────────▼─────────────────────────────────────────────────────┐
│                  minio-go v7 S3 client                       │
│  PutObject / GetObject / ListObjectsV2 /                     │
│  CopyObject / RemoveObject / HeadObject                      │
└────────┬─────────────────────────────────────────────────────┘
         │
┌────────▼─────────────────────────────────────────────────────┐
│       internal/resilience/ (复用已有模块)                      │
│  breaker / retry / metrics / lock                            │
└─────────────────────────────────────────────────────────────┘
```

## 3. 包结构

```
gateway/s3fs/
├── s3fs.go          # S3FileSystem 主结构体 + New() + Root() + Serve()
├── config.go        # Config + 功能选项
├── dir.go           # S3Dir: Lookup, Readdir, Create, Mkdir, Rename, Remove, Symlink, Link
├── file.go          # S3File: Open, Getattr, Setattr, Access
├── filehandle.go    # S3FileHandle: Read, Write, Flush, Release, Fsync
├── symlink.go       # S3Symlink: Readlink, Getattr, Setattr, Access
├── xattr.go         # S3File/S3Dir xattr 方法
├── scan.go          # S3 ListObjects → Pebble 缓存同步
├── cache.go         # Pebble 元数据缓存
├── sync.go          # 异步上传 worker pool + 崩溃恢复
├── ops.go           # MoveOperation / CopyOperation / PutOperation
├── lock.go          # 路径级文件锁
├── metrics.go       # Prometheus /metrics + /healthz

cmd/s3fsgw/
└── main.go          # CLI 入口
```

## 4. 数据模型

### 4.1 S3 → POSIX 映射

| S3 概念 | POSIX 概念 | 说明 |
|---------|-----------|------|
| Bucket | 挂载根目录 | 根 inode 的子目录 |
| Object key | 文件路径 | `/` 分隔符映射为目录层级 |
| Object (非 `/` 结尾) | 常规文件 | 大小 = Content-Length |
| Common Prefix (`/` 结尾) | 目录 | 虚拟目录，S3 无真实目录 |
| Object metadata | 文件属性 | mtime = Last-Modified, size = Content-Length |
| Content-Type | 文件类型 | 通过扩展名推断 |

### 4.2 本地缓存 inode 结构

```go
type CacheInode struct {
    ID       uint64
    IsDir    bool
    Name     string
    Size     uint64
    Mode     uint32
    UID      uint32
    GID      uint32
    Mtime    int64
    Ctime    int64
    Atime    int64
    ETag     string
    // 目录特有
    Children map[string]uint64  // name → child inode ID
    // Symlink 特有
    SymlinkTarget string
}
```

### 4.3 Pebble 键布局

```
inode:{inodeID}         → CacheInode JSON
dir:{parentID}:{name}   → childInodeID (uint64)
pending:{cacheBasename} → PendingUpload JSON
```

## 5. 核心流程

### 5.1 目录扫描 (scan)

```
Dir.scan(ctx)
  │
  ├─ 1. 检查 scanTTL：如果 lastScan + TTL > now，跳过
  │
  ├─ 2. S3 ListObjectsV2(Prefix=dirPath, Delimiter="/")
  │     返回：Objects (文件) + CommonPrefixes (子目录)
  │
  ├─ 3. 对每个 Object:
  │     ├─ 查缓存：GetInode(parentID, name)
  │     ├─ 存在 → 更新 size/mtime/etag
  │     ├─ 不存在 → 分配新 inode ID，创建 CacheInode
  │     └─ 标记 "已见"
  │
  ├─ 4. 对每个 CommonPrefix:
  │     ├─ 查缓存：GetInode(parentID, dirName)
  │     ├─ 不存在 → 分配新 inode ID，创建目录 CacheInode
  │     └─ 标记 "已见"
  │
  ├─ 5. 删除缓存中 "未见" 的条目（已删除的对象）
  │
  └─ 6. 更新 lastScan = now
```

### 5.2 文件读取 (Read)

```
S3FileHandle.Read(dest, off)
  │
  ├─ 1. 首次读取？→ ensureDownloaded()
  │     ├─ S3 GetObject(bucket, key)
  │     ├─ 写入本地缓存文件
  │     └─ 更新 inode size
  │
  ├─ 2. 从本地缓存文件 ReadAt(dest, off)
  │
  └─ 3. 返回 ReadResultData(dest[:n])
```

### 5.3 文件写入 (Write → Flush)

```
S3FileHandle.Write(data, off)
  │
  ├─ 1. 写入本地缓存文件
  ├─ 2. 标记 dirty = true
  └─ 3. 更新内存中的 inode size

S3FileHandle.Flush()
  │
  ├─ 1. 记录 pending upload (崩溃恢复)
  ├─ 2. sync(&PutOperation{cachePath, remotePath, size})
  │     → syncChan → syncWorker → PutObject
  ├─ 3. 等待上传完成
  ├─ 4. 清除 pending 记录
  ├─ 5. 更新本地缓存
  └─ 6. dirty = false, flushed = true
```

### 5.4 文件重命名 (Rename)

```
S3Dir.Rename(oldName, newParent, newName)
  │
  ├─ 1. 获取路径锁
  ├─ 2. 从缓存获取旧 inode
  ├─ 3. sync(&MoveOperation{oldPath, newPath})
  │     → syncChan → syncWorker:
  │       a. S3 CopyObject(bucket, oldPath, newPath)
  │       b. S3 RemoveObject(bucket, oldPath)
  ├─ 4. 等待完成
  ├─ 5. 更新本地缓存（删除旧条目，创建新条目）
  └─ 6. 释放路径锁
```

### 5.5 崩溃恢复

```
重启时 recoverPending():
  │
  ├─ 1. 遍历 Pebble "pending:" 前缀
  ├─ 2. 对每个 PendingUpload:
  │     ├─ 打开本地缓存文件
  │     ├─ 重新上传到 S3
  │     └─ 清除 pending 记录
  └─ 3. 删除孤立的缓存文件
```

## 6. S3 语义限制

| POSIX 语义 | S3 支持情况 | 处理方式 |
|-----------|------------|---------|
| 真实目录 | ❌ 不支持 | CommonPrefix 模拟，创建空对象 `prefix/` |
| Atomic rename | ❌ 不支持 | CopyObject + DeleteObject（非原子） |
| Hardlink | ❌ 不支持 | CopyObject 复制（独立副本） |
| Symlink | ❌ 不支持 | 本地 Pebble 存储，不上传 S3 |
| 文件锁 | ❌ 不支持 | 本地路径级互斥锁（进程内） |
| 权限 (chmod) | ❌ 不支持 | 本地缓存存储，不影响 S3 |
| 随机写入 | ⚠️ 部分支持 | 读-改-写整个对象（大文件性能差） |
| 并发写入 | ⚠️ 最终一致 | 路径锁 + Last-Write-Wins |

## 7. 配置

### 7.1 CLI 参数

```bash
s3fsgw [flags] <s3-endpoint/bucket/prefix> <mountpoint>

Flags:
  -cache-dir string       缓存目录 (default "/var/lib/s3fs/cache")
  -scan-ttl duration      目录扫描缓存 TTL (default 60s)
  -read-only              只读模式
  -cache-quota int        缓存磁盘配额 bytes (default 0=无限)
  -metrics-addr string    Prometheus 监听地址 (default ":9900")
  -insecure               跳过 TLS 验证
  -debug                  调试日志
  -uid uint               文件所有者 UID (default 0)
  -gid uint               文件所有者 GID (default 0)
```

### 7.2 环境变量

```bash
S3FS_ACCESS_KEY=xxx       # S3 access key
S3FS_SECRET_KEY=xxx       # S3 secret key
S3FS_SECRET_TOKEN=xxx     # S3 session token (可选)
```

### 7.3 凭证文件

`<cache-dir>/config.json`:
```json
{
  "version": "1",
  "accessKey": "xxx",
  "secretKey": "xxx",
  "secretToken": "xxx"
}
```

优先级：flag > 环境变量 > 凭证文件

## 8. 指标

### 8.1 Prometheus 指标

```
# FUSE 操作计数
s3fs_ops_total{op="open|read|write|release|lookup|readdir|mkdir|remove|rename|create|flush"}

# S3 后端操作
s3fs_s3_ops_total{op="get|put|copy|remove|list|errors|retries"}

# 延迟直方图
s3fs_s3_get_duration_seconds_bucket{le="..."}
s3fs_s3_put_duration_seconds_bucket{le="..."}
s3fs_s3_list_duration_seconds_bucket{le="..."}
s3fs_read_duration_seconds_bucket{le="..."}
s3fs_write_duration_seconds_bucket{le="..."}

# 状态
s3fs_active_handles
s3fs_circuit_breaker_state
s3fs_uptime_seconds
```

### 8.2 Health 端点

- `GET /healthz` — 返回 200 OK
- `GET /metrics` — Prometheus text format
- `GET /metrics/json` — JSON 格式

## 9. 测试策略

### 9.1 单元测试

- `cache_test.go` — Pebble 缓存 CRUD
- `scan_test.go` — 目录扫描逻辑（mock S3 client）
- `lock_test.go` — 路径锁并发
- `sync_test.go` — 上传队列

### 9.2 集成测试（`//go:build linux`）

- `s3fs_test.go` — 使用 MinIO testcontainer 或 mock S3
- 测试用例：
  - Lookup/Readdir 目录列表
  - Open/Read 文件读取
  - Create/Write/Flush 文件写入
  - Rename 文件重命名
  - Remove 文件删除
  - Symlink 符号链接
  - Mkdir 目录创建

### 9.3 E2E 测试

- 使用 `go-fuse/v2/fs/testing` 包的 `TestServer` 框架
- 或直接挂载后用 `os` 包测试

## 10. 实现顺序

| 阶段 | 内容 | 依赖 |
|------|------|------|
| **Phase 1** | 基础设施：config + cache + ops + lock + metrics | 无 |
| **Phase 2** | FUSE 层：s3fs + dir + file + filehandle + symlink + xattr | Phase 1 |
| **Phase 3** | S3 集成：scan + sync + breaker | Phase 1 + minio-go |
| **Phase 4** | 入口：cmd/s3fsgw/main.go + mount.go | Phase 1-3 |
| **Phase 5** | 测试：单元 + 集成 + E2E | Phase 1-4 |

## 11. 与现有 NUFS 的关系

- **独立包**：`gateway/s3fs/` 不依赖 `metadata/` 或 `gateway/fuse/`
- **共享基础设施**：复用 `internal/resilience/` 的 breaker/retry/metrics/lock
- **共享 S3 配置**：可与 `gateway/s3/` 共享凭证加载逻辑
- **互斥部署**：
  - `s3fsgw` — S3 桶挂载（本设计）
  - `fusegw` — NUFS 原生 FUSE（使用 metadata + datanode）
  - `s3gw` — NUFS S3 网关（暴露 S3 接口）

## 12. 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| S3 ListObjects 延迟高 | 大目录首次扫描慢 | scanTTL 缓存 + 增量扫描 |
| S3 最终一致性 | 列表可能不一致 | scanTTL 定期刷新 + 可配置一致性级别 |
| 大文件随机写性能差 | 每次写入需要读-改-写整个对象 | 本地缓存 + Flush 时批量上传 |
| 并发写入冲突 | 多客户端写同一文件 | 路径锁 + Last-Write-Wins |
| 缓存磁盘空间耗尽 | 写入失败 | cacheQuota + LRU 淘汰 |
| S3 不可用 | 读写失败 | 熔断器 + 重试 + 本地缓存兜底 |
