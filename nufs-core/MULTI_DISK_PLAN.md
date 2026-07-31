# datanode 进程内多盘（JBOD）改造设计

## 目标

把 datanode 从"一进程一盘"改成"一进程多盘（JBOD）"，对标 MinIO/HDFS DataNode。
- 运维成本：一台 N 盘机器从 N 个进程降到 1 个进程（1 份配置/端口/监控/metadata 心跳）
- 高可用：单盘故障不杀进程，标记该盘 bad 后其他盘继续服务；进程崩溃等价于整机宕（已被机器级拓扑打散吸收）
- 资源：共享一个 TCP server / metadata 连接 / 连接池，每盘独立 I/O 限流与 fd 缓存

## 核心设计原则

**外部 API 不变，全局索引不变，只拆 I/O 层。**

- `ChunkStore` 的导出方法签名（`Write/Read/ReadRange/Seal/Delete/Info/ListChunks/Stats/WriteAt/WriteBatch/VerifyChunkData/PerfSnapshot/WriteErrorRate/DrainWrites/Close`）全部保持不变。
- 全局 `chunks map[ChunkID]*LocalChunkInfo` 和全局原子计数器 `totalBytes/chunkCount` 保持不变（heartbeat/ops/anti-entropy 直接读它们，见 ha.go/ops.go）。
- 只把"文件落在哪块盘"相关的状态拆成 per-disk：路径、fdCache、writeSem/readSem/syncSem、WAL、磁盘健康。

这样 server.go / heartbeat.go / ops.go / ha.go 等调用方**零改动**。

## 数据结构

### LocalChunkInfo 增加 DiskIndex

```go
type LocalChunkInfo struct {
    ...现有字段...
    DiskIndex int  // 该 chunk 落在第几块盘（重建自扫描，不持久化）
}
```

**不持久化**：启动扫描时，扫到 disk i 目录下的文件就设 `DiskIndex=i`。盘归属由"文件在哪个盘的目录树"决定，无需改文件 header / .meta 格式。

### diskShard：每盘的 I/O 状态

```go
type diskShard struct {
    index     int           // 在 ChunkStore.disks 中的下标
    dataDir   string        // 该盘根目录
    chunksDir string        // {dataDir}/chunks
    writeSem  chan struct{}
    readSem   chan struct{}
    syncSem   chan struct{}
    wal       *WriteAheadLog   // per-disk WAL（见下）

    fdMu      sync.RWMutex
    fdCache   map[metadata.ChunkID]*os.File
    fdList    *fdLRU
    fdMax     int
}

func (d *diskShard) chunkPath(id metadata.ChunkID) string {
    shard := uint64(id) % MaxShards
    return filepath.Join(d.chunksDir, fmt.Sprintf("%02x", shard), fmt.Sprintf("%d.dat", id))
}
```

### ChunkStore：持有 []diskShard + 全局索引

```go
type ChunkStore struct {
    disks      []*diskShard                // N 块盘
    mu         sync.RWMutex                // 保护全局 chunks map（不变）
    chunks     map[ChunkID]*LocalChunkInfo // 全局索引（不变），info.DiskIndex 指向盘
    totalBytes atomic.Int64                // 全局（不变）
    chunkCount atomic.Int64                // 全局（不变）
    encryptor  *crypto.Encryptor           // 进程级共享
    disk       *DiskManager                // 现在管 N 块盘
    perf       chunkStorePerf
    scanDone   chan struct{}
    scanErr    error
}
```

## chunk -> 盘映射

**写入选盘**：`DiskManager.PickDisk()` 返回"最空闲的健康盘"（按 usedBytes/capacity 比例最少，且 state=Online）。
**读取定位**：`info := chunks[id]` -> `disk := disks[info.DiskIndex]` -> `disk.chunkPath(id)`。
**无 on-disk 格式变更**：盘归属由目录位置决定，扫描时重建 DiskIndex。

选盘策略用 least-used 而非 `chunkID % N`，因为后者在加盘时映射全变、无法弹性扩缩。

## 写路径（多盘）

```
Write(id, data):
  1. WaitForScan
  2. diskIdx := dm.PickDisk()          // 选最空闲健康盘
  3. dm.CanAdmitWrite(diskIdx, size)   // 该盘容量/健康检查
  4. disk := disks[diskIdx]
  5. disk.writeSem <- acquire
  6. disk.wal.LogWrite(id, len)        // per-disk WAL
  7. encrypt（进程级）
  8. os.Create(disk.chunkPath(id)) -> 写 header+data -> fsync(disk.syncSem)
  9. 全局 chunks[id] = info{DiskIndex: diskIdx, ...} + totalBytes/chunkCount
 10. disk.writeMetaSidecar(id, info)
 11. disk.wal.LogCommit(id)
```

## 读路径（多盘）

```
Read(id, off, len):
  1. WaitForScan
  2. info, ok := chunks[id]            // 全局索引
  3. disk := disks[info.DiskIndex]
  4. disk.readSem <- acquire
  5. f := disk.getFd(id, disk.chunkPath(id))
  6. 读 header + 范围/整读 + CRC
  7. decrypt
```

盘 FAILED 时读返回 error（该 chunk 本节点不可用，跨节点副本/EC 兜底）。

## WAL：per-disk

每块盘自己的 `{dataDir}/wal/wal.log`，只记录写该盘 chunk 的 intent/commit。
- **隔离**：一块盘的 WAL 故障不影响其他盘；盘挂了它的 WAL 一起挂，但 chunk 也一起没了，一致。
- **恢复**：每盘独立 Recover，清理该盘的孤儿 chunk（用该盘自己的 chunkPath）。
- group-commit 仍生效（每盘内批量 fsync）。
- `WriteAheadLog` 结构不变，只是每个 diskShard 各持有一个实例。

## DiskManager：per-disk 健康

```go
type DiskManager struct {
    disks []*diskHealth   // 与 ChunkStore.disks 一一对应
    ...
}
type diskHealth struct {
    index     int
    dataDir   string
    capacity  int64
    stats     DiskStats
    diskState atomic.Int64   // Online/Degraded/Failed
    ioErrors  atomic.Int64
    ...
}
func (dm *DiskManager) CanAdmitWrite(diskIdx int, size int64) error  // per-disk
func (dm *DiskManager) PickDisk() (int, error)                       // 最空闲健康盘
func (dm *DiskManager) MarkDiskFailed(diskIdx int)
func (dm *DiskManager) AggregateStats() DiskStats                    // 心跳用
```

- 每盘容量用 `Statfs` 自动探测（不再依赖单一 CapacityGB 配置）。
- 盘 I/O 错误累计超阈值 -> MarkDiskFailed -> 该盘停止写入、读返回错误，其他盘照常。
- 心跳报 `AggregateStats()`（汇总）+ 可选每盘明细。

## 启动扫描与恢复

`scanExisting` 改为：并发扫每块盘的 256 个 shard 目录，读 `.dat` header + `.meta`，合并进全局 `chunks` map，`DiskIndex` = 被扫描盘的下标。每盘的 WAL 各自 Recover 清理孤儿。`WaitForScan` 语义不变。

## Config & main.go 接线

```go
type Config struct {
    DataDirs []string   // 原 DataDir string
    ...
}
```

main.go：`--data-dirs /d1,/d2,/d3`（逗号分隔）。每盘建独立 WAL + chunkStore 共享：

```
for each dataDir in cfg.DataDirs:
    wal[i] = NewWriteAheadLog(dataDir/wal)
    diskShard[i] = newDiskShard(dataDir, wal[i], ...)
chunkStore = NewChunkStore(cfg.DataDirs, wal, ...)   // 内部建 []diskShard
diskManager = NewDiskManager(cfg.DataDirs, chunkStore, ...)
chunkStore.SetDiskManager(diskManager)
server = NewServer(cfg, chunkStore)   // 一个 TCP server
```

单盘部署：`DataDirs` 为单元素列表，等价于现状。

## 不变的部分（零改动）

- ChunkStore 导出 API 全部不变
- 全局 `chunks` map / `mu` / `totalBytes` / `chunkCount` 不变
- server.go / heartbeat.go / ops.go / ha.go（anti-entropy）/ repair / chainRepl 调用方不变
- 文件格式（header 20B + payload）、.meta sidecar、shard 目录布局（每盘内仍是 256 shard）不变
- 拓扑打散（MachineID / SpreadMachine）不变

## 范围与分期

**本次实现（核心 + 隔离）：**
1. Config.DataDirs + main.go 多盘接线
2. diskShard 结构 + ChunkStore 持有 []diskShard
3. LocalChunkInfo.DiskIndex（内存）
4. Write/Read/Delete/Seal/WriteAt/WriteBatch/VerifyChunkData 路由到 per-disk
5. ListChunks/Stats/Info 走全局 map（基本不变）
6. per-disk WAL
7. DiskManager 多盘健康 + PickDisk + CanAdmitWrite(diskIdx) + AggregateStats + Statfs 容量探测
8. scanExisting 多盘并发合并 + per-disk WAL Recover
9. 单盘故障隔离（FAILED 盘不写、读报错）

**延后（可选）：**
- 跨盘迁移 / 再均衡（加盘后搬数据均衡）—— 依赖跨节点副本保证耐用性，盘内不均衡只影响容量利用率，非阻塞
- 热加/热拔盘
- per-disk 分层存储（TierManager 现有逻辑适配多盘）

## 测试计划

1. **多盘写读往返**：2 盘，写一批 chunk，验证分布在两盘上（DiskIndex 有差异），读回正确
2. **单盘故障隔离**：3 盘，标记 disk[1] Failed，新写不再落 disk[1]，disk[1] 上的 chunk 读返回 error，disk[0]/disk[2] 正常读写
3. **least-used 选盘**：写满 disk[0] 到高水位，新写应倾向 disk[1]
4. **启动恢复**：多盘各写若干 chunk 后重启，scanExisting 重建全局索引、DiskIndex 正确
5. **per-disk WAL 恢复**：模拟某盘写后未 commit 崩溃，重启清理该盘孤儿、不影响其他盘
6. **聚合统计**：Stats()/ListChunks() 跨盘汇总正确
7. **单盘部署回归**：DataDirs 单元素，行为等价现状（现有 datanode 测试全过）

## 风险

- **DiskManager 与 ChunkStore 耦合**：diskmanager.go 直接读 `store.chunkCount/totalBytes`（全局，OK）和 TierManager 遍历 `store.chunks`（全局，OK）。writeMetaSidecar 需按 chunk.DiskIndex 路由到对应盘——这是主要改动点。
- **ha.go 直接读 store.chunks**：anti-entropy 遍历全局 map，DiskIndex 字段需在复制/对比时正确传递。
- 改动集中在 chunkstore.go / diskmanager.go / types.go(Config) / cmd/datanode/main.go 四处，调用方零改动，可控。
