# nufs-cli 运维查询手册（Ops CLI）

本文档面向**上线后的排障 / 巡检**场景，说明如何用 `nufs-cli` 查询文件元数据、
浏览目录树、查元数据服务器（metad）的 KV，以及各类只读运维状态。
它是「上线验收清单 E3（观测）」与「D2（回归门禁）」在排障侧的可执行配套。

> 两种模式：
> - **local**：直接打开本机 metad 的 Pebble 元数据目录。注意 Pebble 打开即
>   **独占**（文件锁 + WAL replay/manifest 写入，目录会被修改）：metad 进程
>   必须先停止，且不要用不同版本的二进制去开生产目录（可能触发迁移）。
>   所有 CLI 命令本身只读，但"打开"这个动作不是。
> - **remote**：走 metad 的 ops HTTP API（`/api/v1/...`），读的是**一致的主副本**
>   （`requireLeader`），适合连到线上集群。
>
> 默认 `--mode=auto`：若 `--meta-dir` 存在则 local，否则 remote。
> 远程模式对受保护端点提供 `--auth-token <token>`（`Authorization: Bearer`）。
> 生产环境务必用 remote + token，不要直接在数据节点上开 Pebble（避免把数据目录
> 当可写路径误操作，且线上元数据目录通常不在本机）。

---

## 0. 快速定位：常见问题 → 命令

| 你想做什么 | 命令 |
|---|---|
| 查某个文件的完整元数据（inode/xattr/chunks） | `nufs-cli stat <bucket>/<dir>/<file>` |
| 看目录里有哪些对象 / 子目录 | `nufs-cli ns <bucket>/<dir>` |
| 按 inode id 查详情 | `nufs-cli inode <id>` |
| 查一个 inode 的所有 chunk | `nufs-cli chunks --inode <id>` |
| 查单个 chunk 的副本 / EC 状态 | `nufs-cli chunk <id>` |
| 直接读元数据 KV（原始键值） | `nufs-cli kv get <key>` / `kv scan <prefix>` |
| 看审计日志尾部 | `nufs-cli audit --limit N` |
| 看咨询锁 | `nufs-cli locks --inode <id>` |
| 看元数据备份状态 | `nufs-cli backups status` |
| 看写恢复（write-attempt）状态 | `nufs-cli write-attempts [--state S]` |
| 看集群/节点/容量 | `nufs-cli nodes` / `buckets` / `balance` / `health` |

---

## 1. 文件元数据查询：`stat`（推荐主命令）

`stat` 把 `<bucket>/<path>` 从桶根一路按目录分段 walk，最后返回目标 inode 的
元数据 + xattr + 关联的 chunk。这是「某文件在哪、多大、属主、哪几个 chunk、放
在哪些节点」的一站式入口。

```bash
# 远程（连线上 metad，只读主副本）
nufs-cli --mode=remote --meta-addr=metad-1:8091 --auth-token=<token> \
    stat mybucket/docs/2026/report.pdf

# 本地（直接读本机 Pebble 元数据目录）
nufs-cli --meta-dir=/var/lib/dfs/metadata stat mybucket/docs/2026/report.pdf
```

输出内容：
- **inode**：type（file/dir）、mode、size、mtime、owner。
- **chunks**：该文件由哪些 chunk 组成（offset/length/version）。
- 深入副本/EC 详情用 `chunk <id>`。

## 2. 目录树浏览：`ns`

`ns` 列出某桶路径下的目录条目（等价 `ls`），排查「目录里到底有什么」。

```bash
nufs-cli --mode=remote --meta-addr=metad-1:8091 --auth-token=<token> ns mybucket/docs
```

## 3. KV 查询：`kv`

直接读元数据服务器的原始 KV。key 前缀见 `metadata/keys.go`：

| 前缀 | 内容 |
|---|---|
| `/bucket/` | 桶定义 |
| `/bucket-by-root/` | 桶 root inode → 桶名索引 |
| `/inode/` | inode 元数据 |
| `/chunk/` | chunk / 副本 / EC 布局 |
| `/ns/` | 命名空间（父 inode → 子映射） |
| `/node/` | 数据节点注册 |
| `/repair/` | 修复队列 |
| `/quota/` | 桶配额 |
| `/audit/` | 审计日志 |
| `/write-attempt/` | 写恢复状态 |
| `/backup/...` | 备份相关 |

```bash
# 读单个原始键
nufs-cli --mode=remote --meta-addr=metad-1:8091 --auth-token=<token> kv get /inode/42

# 扫某个前缀下的所有键值（分页）
nufs-cli --mode=remote --meta-addr=metad-1:8091 --auth-token=<token> \
    kv scan /bucket/ --limit 100

# 本地直连
nufs-cli --meta-dir=/var/lib/dfs/metadata kv get /node/1
```

> **安全设计**
> - `kv` 默认**只读**（get / scan），不提供写/执行能力。
> - `scan` 的 `--prefix` 限 `metadata/keys.go` 里的已知前缀白名单，避免扫进系统内部键。
> - 远程模式下该端点**不在** `public` 白名单 → 配了 `--auth-token` 即强制 Bearer；
>   且 handler 再叠 `requireLeader`，保证读到的是一致主副本。

## 4. 运维状态查询

```bash
# inode 详情 + xattr
nufs-cli inode 42

# 某 inode 的 chunk 引用
nufs-cli chunks --inode 42

# 单 chunk 的副本/EC 详情
nufs-cli chunk 12345

# 审计日志尾（仅 remote）
nufs-cli audit --limit 500

# 咨询锁（仅 remote，且必须有 --inode）
nufs-cli locks --inode 42

# 备份状态 / 列表（仅 remote）
nufs-cli backups status

# 写恢复状态汇总，或按 state 看明细（仅 remote）
nufs-cli write-attempts
nufs-cli write-attempts --state failed
```

## 5. 与监控门禁的关系

- **`scripts/check-metrics.sh`**（已纳入 `make verify` 每一级）：机器校验
  `deploy/monitoring/alerting-rules.yaml` 与 `internal/slo/slo.go` 里引用的每个
  `nufs_*` 指标都**确实被某个 exporter 发出**（无死链），并对部署的告警规则做
  promtool 语法校验（本机装了 promtool 才跑，未装优雅 SKIP）。这给 E3 的
  「告警规则可查询、可触发」提供了静态门禁证据。
- **新指标**：`nufs_repair_tasks_queued` / `nufs_repair_oldest_timestamp`
  （repair 队列深度与最老任务时间戳，`ops_prometheus.go` 挂出），补上原来
  「规则写了但没人发」的 repair 维度可观测。
- 上完线后在真实 Prometheus 里做一次**告警触发链路**复验，把 E3 从 ⚠️ 升为 ✅。

## 6. 凭证管理：`auth`（mount auth Phase 1/2）

metad 是唯一认证权威；FUSE 挂载与 S3 网关的凭证都来自同一个注册表（`/cred/`，
raft 复制）。`nufs-cli auth` 即该注册表的唯一管理入口（替代旧的 S3 网关本地
YAML/flags）。

```bash
# 注册/更新一个凭证（S3 网关 + FUSE 共用；principal 缺省 = access key）
nufs-cli --mode=remote --meta-addr=metad-1:8091 --auth-token=<ops-token> \
    auth add app-server-1 --secret 's3cr3t' --principal app-server-1

# 吊销：S3 网关在下一个 credential-sync-interval（默认 60s）内失效该 key
nufs-cli --mode=remote --meta-addr=metad-1:8091 --auth-token=<ops-token> \
    auth del app-server-1

# 查看注册表（只回 access_key + principal，不回 secret）
nufs-cli --mode=remote --meta-addr=metad-1:8091 --auth-token=<ops-token> auth list
```

配套：
- metad 需配 `--token-signing-key`（FUSE token 交换）与 `--credential-secret-key`
  （32 字节 hex，用于加密存储明文 secret 供 S3 网关 SigV4 同步；缺省时凭证只存
  哈希，FUSE 可用但 S3 网关不可用）。
- S3 网关配 `--meta-auth-token=<ops-token>` 即启用注册表同步；`auth del` 的吊销
  延迟 ≤ `--credential-sync-interval`。
- 桶策略管理用 `nufs-cli acl get/set/delete <bucket>`（同样 remote + ops token）。

### 6.1 `--credential-secret-key` 轮换

`--credential-secret-key` 加密注册表里 S3 网关用的明文 secret。轮换它是破坏性
操作：**旧密钥加密的凭证在新密钥下全部无法解密**，注册表同步对 S3 网关会返回
空列表。网关侧 auth 是 pin 住的（`SetAuthMode(true)`），空列表 ≠ 匿名 —— 网关
会**拒绝所有请求（403）**并打出启动警告；若在运行中轮换，同步后同样全拒。

轮换步骤（避免服务中断）：

1. 在旧密钥下先增补一份同 principal 的新凭证（`nufs-cli auth add <name> --secret '<新secret>'`）。
2. 等 S3 网关一个 `--credential-sync-interval` 周期同步到新凭证（启动日志
   `count` ≥ 1），确认网关对新 secret 可正常签名访问。
3. 重启所有 metad，换 `--credential-secret-key=<新hex>`；旧凭证不再可解密，但
   第 1 步写入的新凭证仍在，网关同步结果保持非空。
4. 用旧 secret 的客户端切换到新 secret；之后按需 `auth del` 清理旧凭证。

若在步骤 3 前误轮换（网关 403 全拒）：恢复 metad 旧 `--credential-secret-key`
重启，网关下一个同步周期即恢复。

