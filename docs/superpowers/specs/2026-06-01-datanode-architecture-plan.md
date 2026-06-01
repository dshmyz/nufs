# Implementation Plan: DataNode Architecture Redesign

## Overview

实现 DataNode 多进程 supervisor 模式 + MachineID 拓扑保护。

**设计文档**: `docs/superpowers/specs/2026-06-01-datanode-architecture-design.md`

## Tasks

### Task 1: MachineID 字段 + SpreadMachine 拓扑级别

**文件:** `metadata/types.go`, `metadata/placement.go`

- `NodeInfo` 增加 `MachineID string` 字段
- `TopologySpread` 枚举增加 `SpreadMachine` 常量（放在 `SpreadNode` 和 `SpreadRack` 之间）
- `PlacementEngine.getDomain()` 增加 `case SpreadMachine: return n.MachineID`

### Task 2: DataNode supervisor 模式

**文件:** `cmd/datanode/main.go`

#### 2.1 新增 flag
- `--data-dirs`（comma-separated paths, 与 `--data-dir` 互斥）
- `--base-port`（默认 9100）
- `--machine-id`（由父进程读取传给子进程）

#### 2.2 supervisor 入口
- 如果设置了 `--data-dirs` 进入 supervisor 模式
  - 读取 `/etc/machine-id`（或 `--machine-id` 参数）
  - 为每个 data-dir 准备子进程参数：
    - `cmd/datanode --data-dir=<dir> --listen=:<port> --node-id=<id> --machine-id=<machine-id>`
    - 端口从 `--base-port` 递增
    - NodeID 持久化到 `{dir}/.dfs-node-id`（避免重启后变化）
  - 对每个 dir fork 子进程，记录 PID

#### 2.3 supervisor watchdog
- 监控所有子进程
- 子进程退出 → 自动重启（最多 3 次，指数退避 1s/2s/4s）
- 父进程收到 SIGTERM → 依次向子进程发 SIGTERM，等待 10s，剩余发 SIGKILL
- 定期（30s）检查子进程心跳（子进程 stdout 输出的最后一行时间戳）

#### 2.4 Unix domain socket 管理接口
- 监听 `/var/run/datanode.sock`（或 `{data-dirs[0]}/.datanode.sock`）
- 请求格式：`{"cmd":"status"|"adopt"|"retire","path":"..."}`
- `status`: 返回所有子进程状态 JSON
- `adopt <path>`: 检查路径 → fork 新子进程 → 分配端口
- `retire <path>`: drain → SIGTERM → 等待退出 → 从列表移除

#### 2.5 CLI 子命令
- `datanode status`: 通过 socket 发送 status 请求，打印表格
- `datanode adopt /data/nvme5n1`: 通过 socket 发送 adopt 请求
- `datanode retire /data/nvme3n1`: 通过 socket 发送 retire 请求

### Task 3: DataNode 子进程侧适配

**文件:** `cmd/datanode/main.go`

- 新增 `--machine-id` flag，注册 NodeInfo 时设置 `MachineID` 字段
- 不支持 supervisor 模式下的 `--data-dir` 作为子进程启动

### Task 4: PlacementEngine 保护

**文件:** `metadata/placement.go`

- `spreadSelect` 对 `SpreadMachine` 使用 `n.MachineID` 作为 domain key
- 默认 `PlacementPolicy.TopologySpread` 设为 `SpreadMachine`（而非 `SpreadNode`）

### Task 5: 测试

- **`TestMachineIDPlacement`**: 注册同 MachineID 的节点 → 验证 spreadSelect 不部署双副本
- **`TestSupervisorForkAndStop`**: `--data-dirs /tmp/a,/tmp/b` → 验证启动 2 个进程 → SIGTERM → 验证优雅退出
- **`TestSupervisorCrashRestart`**: fork 子进程 → kill 子进程 → 验证父进程自动重启
- **`TestSupervisorSocketCommands`**: 启动 supervisor → socket 请求 status → 验证返回正确

## 验证

- `cd nufs-core && go build ./cmd/datanode/` 编译通过
- `go test ./metadata/... ./datanode/... -count=1` 全部通过
- `go test ./datanode/... -run TestSupervisor -v` supervisor 测试通过
- `go test ./metadata/... -run TestMachineID -v` placement 测试通过
