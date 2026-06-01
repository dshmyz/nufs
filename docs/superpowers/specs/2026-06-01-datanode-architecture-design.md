# DataNode 架构设计：进程与磁盘模型

## 背景

NUFS 当前是 1 datanode 进程 = 1 磁盘目录的扁平模型。大规模集群下（10+ 机器，每台多块 NVMe），存在两个问题：

1. **拓扑盲区**：多进程在同一台机器上运行时，metad 的 PlacementEngine 不知道它们属于同一物理机器，可能将不同副本部署到同机不同盘上。
2. **运维成本高**：N 块盘需要手动启动 N 个进程、分配 N 个端口、管理 N 个 systemd unit，加盘需要改配置重启。

## 设计目标

- 磁盘故障隔离 —— 一块盘 IO hang 不波及其他盘
- 拓扑感知放置 —— placement 能避免同机副本
- 运维简单 —— 一条命令部署一台机器，加盘不改配置
- 保持现有 I/O 路径不变（标准 syscall `pread/write`，无 O_DIRECT 支持）
- 保持现有集中式元数据（metad + PebbleStore + 可选 Raft）不变

## 非目标

- 不重构 ChunkStore 或 I/O 层
- 不引入 O_DIRECT 或 io_uring
- 不去中心化元数据
- 不重写 chain replication

## 设计方案

### 1. 整体架构

```
Machine-A
┌──────────────────────────────────────────────┐
│ datanode（父进程 / supervisor）                │
│  ├── datanode (pid=1001, disk1):9100          │
│  │     ├── ChunkStore → /data/disk1/chunks    │
│  │     ├── DiskManager（独立状态机）            │
│  │     └── HeartbeatReporter                  │
│  ├── datanode (pid=1002, disk2):9101          │
│  ├── datanode (pid=1003, disk3):9102          │
│  └── datanode (pid=1004, disk4):9103          │
└──────────────────────────────────────────────┘
```

父进程（supervisor）:
- 读取启动配置 `--data-dirs`，为每个目录 fork 子进程
- 子进程是当前 datanode binary（`os.Exec` 自身），只需增加 CLI 参数解析，内部逻辑不变
- 父进程监听子进程退出 → 自动重启（指数退避，最多 3 次后停止）
- 转发 OS 信号（SIGTERM → 依次发到子进程，SIGUSR1 → status dump）

子进程（现有的 datanode）:
- 每个子进程独占一个端口（从 base-port 递增）
- 每个子进程注册独立的 NodeID 到 metad
- 心跳、TCP server、chain replication 完全不变

### 2. MachineID 拓扑保护

在 `metadata/types.go` 的 `NodeInfo` 中新增字段：

```go
type NodeInfo struct {
    ID         NodeID      `json:"id"`
    Addr       string      `json:"addr"`
    DataDir    string      `json:"data_dir"`
    Rack       string      `json:"rack"`
    Zone       string      `json:"zone"`
    Tier       StorageTier `json:"tier"`
    MachineID  string      `json:"machine_id"`  // 新增
    CapacityGB int64       `json:"capacity_gb"`
    UsedGB     int64       `json:"used_gb"`
    ChunkCount int64       `json:"chunk_count"`
    State      NodeState   `json:"state"`
    LastSeen   int64       `json:"last_seen"`
}
```

`MachineID` 由父进程在启动时从 `/etc/machine-id` 或 `/sys/class/dmi/id/product_uuid` 读取，附加到子进程的启动参数中。

在 `metadata/placement.go` 的 `spreadSelect` 中增加过滤逻辑（~5 行）：

```
对于 TopologySpread ≥ SpreadMachine（新增级别）：
  排除 MachineID 已被选中的节点
```

新增 `SpreadMachine` 级别（介于 `SpreadNode` 和 `SpreadRack` 之间）。

### 3. 运维命令

datanode binary 支持子命令模式：

```bash
# 启动一台机器（supervisor 模式）
datanode --data-dirs /data/nvme{0..7}n1 --machine-id $(cat /etc/machine-id)

# 查看状态
datanode status
# 输出：
# PID    DATA_DIR          PORT  STATE   UPTIME    CHUNKS  DISK_USED
# 1001   /data/nvme0n1     9100  running  12h      3,421    72%
# 1002   /data/nvme1n1     9101  running  12h      2,895    65%
# 1003   /data/nvme2n1     9102  running  12h      4,102    81%
# 1004   /data/nvme3n1     9103  crashed  4m        -        -

# 加盘 —— 向运行中的父进程发信号
datanode adopt /data/nvme8n1

# 移除盘（先 drain，再停进程）
datanode retire /data/nvme3n1

# 升级 —— 逐个重启子进程
datanode upgrade --binary /usr/local/bin/datanode-v2
```

子命令通过 Unix domain socket（`/var/run/datanode.sock`）与父进程通信，避免 signal 的并发和参数传递问题。`status` 读取只读状态，`adopt`/`retire` 发送操作指令。

### 4. 子进程端口分配

父进程自动选择端口：
- base-port = 9100（可通过 `--base-port` 覆盖）
- 按 data-dirs 列表顺序分配 `base-port, base-port+1, ...`
- 启动前检测端口占用，如果被占用则跳过（skipped，记录到日志）

### 5. 磁盘故障处理

子进程 = 独立进程，所以磁盘故障 = 进程退出：
- OS/磁盘故障 → `pread` hang → 进程不退出但不再心跳
- 父进程 monitor goroutine 检测子进程心跳超时（30s 无 metad 心跳成功）→ 发送 SIGQUIT → 强制退出 → 自动重启
- metad 的 LeaseManager 依赖心跳超时标记 NodeOffline
- RepairWorker 检测到 Offline chunks → 重建副本（已有逻辑）
- 重启子进程时自动重新注册 NodeInfo，metad 恢复 Online

### 6. 替换现有单进程模式

当前单进程模式仍支持：`datanode --data-dir /data --listen :9100`（没有 `--data-dirs` 参数时退化为单进程模式，不做 supervisor）。

### 7. 变更清单

| 范围 | 文件 | 变更 |
|------|------|------|
| types | `metadata/types.go` | `NodeInfo` 加 `MachineID string` |
| placement | `metadata/placement.go` | `SpreadMachine` 拓扑级别 + `spreadSelect` 过滤 |
| cmd | `cmd/datanode/main.go` | 新增 supervisor 模式（`--data-dirs` 分支）：读取配置 → fork 子进程 → monitor |
| cmd | `cmd/datanode/main.go` | 新增子命令：`status`, `adopt`, `retire`, `upgrade` |
| cmd | `cmd/datanode/main.go` | 新增 `--machine-id` 参数，透传到注册的 NodeInfo |
| client | `metadata/client.go` | 注册时携带 MachineID（无需改动，NodeInfo 序列化自动包含） |
| node 注册 | `cmd/datanode/main.go` / 心跳 | 注册时传递 MachineID |

### 8. 测试策略

- `TestSupervisorStartStop`：fork 2 个子进程，发送 SIGTERM 验证优雅退出
- `TestSupervisorCrashRestart`：kill 一个子进程，验证父进程自动重启（最多 N 次）
- `TestSupervisorAdoptDisk`：`adopt` 命令 → 验证新进程启动在正确端口
- `TestSupervisorRetireDisk`：`retire` 命令 → drain → 进程退出 → 端口释放
- `TestPlacementMachineSpread`：mock 同 MachineID 的节点，验证 spreadSelect 不选同机节点
- 现有 datanode 测试全部保持（子进程就是当前 main，不修改）

### 9. YAGNI 排除清单

以下场景暂不实现：
- 子进程自愈后的负载均衡（RepairWorker/Rebalance 已有）
- 多版本滚动升级的回滚（`upgrade` 失败回退留待后续）
- 跨机器的 supervisor 通信（一台机器的 supervisor 只管自己的盘）

## 设计决策记录

| 决策 | 选项 | 选择 | 理由 |
|------|------|------|------|
| 隔离方式 | 进程内多盘 vs 多进程 | 多进程 | Go 的同步 IO 无法隔离单盘 hang，Hadoop 经验支持 |
| 拓扑感知 | MachineID vs 机器名 hash | MachineID | 与 /etc/machine-id 对齐，去重逻辑简单 |
| 运维层 | 独立 agent binary vs binary 自管理 | binary 自管理 | 少一个二进制，少一个故障点，CLI 融入现有 datanode |
| supervisor 通信 | gRPC vs Unix signal vs Unix socket | Unix domain socket | 支持双向、可传递结构体、无端口冲突、自带权限控制 |
| 端口分配 | 自动递增 vs 配置文件 | 自动递增 | 无额外配置，约定即配置 |
