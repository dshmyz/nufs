# NUFS — 分布式文件系统

NUFS 是一个 Go 编写的分布式文件系统，面向裸机 / 容器部署，核心组件：

| 二进制 | 角色 | 默认端口 |
|--------|------|---------|
| `metad` | 元数据服务（命名空间 / chunk 索引 / EC / PG） | ops :8091 |
| `datanode` | 数据节点（V2.1 多盘 JBOD，TCP 数据面） | :9103, ops :8092 |
| `nufs-s3` | S3 兼容网关 | :8081 |
| `nufs-fuse` | FUSE 挂载守护（**linux-only**，见下） | 挂载点 |
| `nufs-cli` | 命令行工具 | — |

> **`nufs-fuse` 是 `//go:build linux`**：只能在 Linux 上构建/运行。在 macOS 等非 Linux
> 上 `make build` 会告警跳过它（集群仍可用，仅挂载不可用）。

---

## 1. 快速开始（裸机，no-Docker）

发布/上线统一走一键安装（需 root，写 `/usr/local/bin` 与 `/sbin`）：

```bash
# 一键：make build → install 5 二进制到 /usr/local/bin → install mount.nufs 到 /sbin
sudo make deploy          # 等价于 sudo ./deploy/install.sh
```

等价的分步做法（也可单独用）：

```bash
make build                  # 编译全部二进制到 bin/
sudo make install           # 5 二进制 → /usr/local/bin
sudo make install-mount-helper   # /sbin/mount.nufs  ← 开启 mount -t nufs
```

`deploy/install.sh` 额外支持：

```bash
sudo ./deploy/install.sh --no-build    # 跳过编译，只装既有产物
sudo ./deploy/install.sh --bin PREFIX  # 覆盖二进制安装目录
./deploy/install.sh --dry-run          # 只打印动作
```

---

## 2. 挂载 FUSE

NUFS 支持标准 `mount(8)` 语法。装好 helper 后（`/sbin/mount.nufs`）：

```bash
# 挂载（普通用户即可；内部经 fusermount）
mkdir -p /mnt/nufs-fuse
mount -t nufs none /mnt/nufs-fuse [-o meta=host:port,log=level]

# 卸载
umount /mnt/nufs-fuse
```

`-o` 支持选项（也可用环境变量 `NUFS_META_ADDR` / `NUFS_LOG_LEVEL` / `NUFS_FUSE_BIN`…）：

| `-o` 键 | 含义 | 默认 |
|---------|------|------|
| `meta=host:port` | metad 地址 | `localhost:8091` |
| `metrics=addr` | DFS metrics/health 地址 | `:9901` |
| `log=level` | debug/info/warn/error | `info` |
| `allow_other` | 允许他用户访问（需 /etc/fuse.conf `user_allow_other`） | off |

`mount.nufs` 默认找 `FUSE_BIN=/usr/local/bin/nufs-fuse`，与 `make deploy` 安装位置对齐。

---

## 3. 常见部署路径

仓库有三套部署脚本/工具库，按场景选用：

| 入口 | 场景 | mount 实现 |
|------|------|-----------|
| `make deploy` / `deploy/install.sh` | **生产/发布**：装二进制 + helper | `mount -t nufs` → `/sbin/mount.nufs` |
| `deploy/host/cluster.sh mount` | **裸机测试**：一条龙启集群 + 挂载 | `host_mount()`（有 helper 则走 helper，无则内联） |
| `deploy/dfs-cluster.sh` / `mount.sh` | **Docker** 测试（compose + /etc/hosts） | `mount_fuse()` |

`deploy/host/cluster.sh` 常用：

```bash
./deploy/host/cluster.sh up        # 编译 + 起 metad/datanode(v2.1 JBOD)/s3
./deploy/host/cluster.sh mount     # 一条龙：起集群 + 挂载 FUSE
./deploy/host/cluster.sh unmount   # 卸载
./deploy/host/cluster.sh down      # 卸载 + 停全部服务
./deploy/host/cluster.sh status    # 各进程 + 挂载点
./deploy/host/cluster.sh logs s3   # 尾随某服务日志
```

---

## 4. 可靠性 / 疲劳测试

裸机版测试脚本（复用 `host_mount`，结果归档到 `NUFS_RESULTS_ROOT`，默认
`/var/log/nufs-tests`，带 `latest` 软链）：

```bash
scripts/fatigue-fuse-host.sh        # 长时可靠性：写/读/杀 datanode / 重启等
scripts/smallfile-fuse-host.sh      # 小文件批量测试
```

对应 Docker 版（`scripts/fatigue-fuse.sh` / `scripts/smallfile-fuse.sh`）走 compose 集群。

---

## 5. 开发

```bash
make build          # 编译 bin/
make test           # go test -race ./...
make build-linux    # 交叉编译 linux/amd64 到 bin/linux-amd64/
make lint / vet / fmt
make verify         # 上线验收回归门禁：make verify [LEVEL=fast|drill|full]
                    #   fast  = build/vet/fmt/全量单测（分包 -count=2 串行）
                    #   drill = fast + 三个故障 drill（leader-failover/metadata-restore/chaos-soak）
                    #   full  = drill + 长时/高 count P0 门禁（上线前跑）
                    # 详见 scripts/verify.sh；上线验收项见 docs/runbooks/production-readiness-checklist.md
make verify-docker  # 同一门禁，但跑在 Linux 容器里（macOS 宿主端口池太小，
                    #  分包 -count=2 会因 EADDRNOTAVAIL 端口耗尽假失败；容器内干净 PASS）
                    #  = scripts/verify-docker.sh，复用宿主 Go 缓存，无需网络
```

安全约束：`gateway/s3/auth.go` 的 SigV4 `providedSig` 不进入任何日志/错误串（保持该约束）。
Phase 2（S3 网关凭证收敛）在 `metadata/auth.go` 增加了 AES-GCM 加密存储（`SealSecret`/`OpenSecret`），
明文 secret 只通过受信端点 `/api/v1/auth/credentials`（ops bearer + leader gate）下发给 S3 网关，
网关仅内存持有。`deploy/install.sh` 与挂载脚本只做构建与文件安装，不触碰任何源码。

---

## 6. 运维、监控与数据组织（上线排障入口）

| 主题 | 文档 | 一句话 |
|------|------|--------|
| **运维查询** | [docs/runbooks/ops-cli.md](docs/runbooks/ops-cli.md) | `nufs-cli stat/ns/kv/inode/chunks/audit/backups` 查文件元数据、KV、运维状态（本地 + 远程鉴权） |
| **监控/告警** | [docs/runbooks/monitoring.md](docs/runbooks/monitoring.md) | Prometheus 抓取拓扑、指标命名空间、告警规则、grafana 面板、metrics 一致性门禁 |
| **数据组织/落盘** | [docs/architecture/data-organization.md](docs/architecture/data-organization.md) | 元数据（Pebble 键空间 + msgpack/JSON 编解码）与数据面 V2.1 segment 磁盘布局、字节格式 |
| 上线验收 | docs/runbooks/production-readiness-checklist.md | 逐项可勾选的门禁表（[E3 观测](#) 已含机器校验） |
| 故障 drill | docs/runbooks/leader-failover-drill.md、metadata-backup-restore-drill.md、object-write-recovery.md | 故障注入基线演练 |
| 通用运维 | docs/runbooks/bucket-quota.md、metadata-disaster-recovery.md | 配额与灾难恢复 |

`make verify`（开发一节）已含 metrics/alert 一致性门禁（死指标=0 + promtool 语法），保证
「告警规则引用的每个指标都真的被 exporter 发出」，避免规则写了却永远没数据的死链告警。

---

## 7. 其他设计文档

详见 [TODO.md](TODO.md)、[ARCHITECTURE.md](ARCHITECTURE.md)、[MULTI_DISK_PLAN.md](MULTI_DISK_PLAN.md)、
[STORAGE.md](STORAGE.md)（早期存储设计，V2.1 现状见 data-organization.md）。
