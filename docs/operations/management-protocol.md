# datanode 管理接口协议

datanode 在 `{dataDir}/.datanode.sock` 上监听 unix domain socket，提供进程内管理接口。
协议与旧版 supervisor 模式兼容（相同的 JSON 消息格式）。

## 连接方式

```bash
# CLI 方式（自动查找 socket）
datanode status --data-dir=/data1
datanode adopt /new-disk --data-dir=/data1
datanode retire /old-disk --data-dir=/data1

# 手动连接 socket
echo '{"cmd":"status"}' | socat - UNIX-CONNECT:/data1/.datanode.sock
```

## 消息格式

### 请求

```json
{
  "cmd": "status | adopt | retire",
  "path": "/optional/directory/path"
}
```

### 响应

```json
{
  "status": "ok | error",
  "error": "error message (only on error)",
  "data": { ... }
}
```

## 命令

### status

返回所有磁盘的状态和使用情况。

**响应 data**：

```json
{
  "disks": [
    {
      "index": 0,
      "dir": "/data1",
      "failed": false,
      "chunks": 15234,
      "bytes": 1073741824
    },
    {
      "index": 1,
      "dir": "/data2",
      "failed": false,
      "chunks": 14987,
      "bytes": 1048576000
    }
  ],
  "total_chunks": 30221,
  "total_bytes": 2122317824
}
```

### adopt

热加盘：将一个目录（新盘或从其他节点迁移来的盘）加入运行中的 datanode。

**请求**：`{"cmd": "adopt", "path": "/new-disk"}`

**响应**：`{"status": "ok", "data": {"dir": "/new-disk", "index": 2}}`

**行为**：
1. 创建新 diskShard，扫描目录中的 .dat 文件
2. 将已存在的 chunk 合并进全局索引（DiskIndex = 新盘下标）
3. 后续写入自动分布到新盘（least-used 策略）

### retire

热摘盘：将一块盘标记为下线。

**请求**：`{"cmd": "retire", "path": "/old-disk"}`

**响应**：`{"status": "ok"}`

**行为**：
1. 标记 `diskShard.failed = true`
2. PickDisk / CanAdmitWrite 跳过该盘
3. 该盘上的 chunk 仍然可读（通过其他副本或 EC 解码）
4. 如需迁移数据，可在 retire 前调用 `MigrateDisk` 将 chunk 搬到其他盘

## socket 查找顺序

CLI 工具按以下顺序查找 socket：

1. 命令行参数中的目录路径（`--data-dir`）
2. 环境变量 `DATA_DIRS` 中第一个目录
3. 默认位置 `/var/lib/nufs/data/.datanode.sock`
4. 系统临时目录 `/tmp/.datanode.sock`

## 兼容性

协议格式（`sockMsg` / `sockResp`）与旧版 supervisor 模式完全兼容。
现有的 `datanode status/adopt/retire` 命令无需修改即可使用。
