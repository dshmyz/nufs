# NUFS Admin — 多集群运维管理后台设计

> 日期: 2026-06-19
> 状态: 已批准，待实现

## 1. 背景与目标

### 1.1 问题

现有 NUFS 运维能力分散在三处：
- `cmd/metad` 有 HTML 模板页面 + JSON API（单集群视角，功能不全）
- `datanode/ops.go` 有独立的 Ops HTTP API
- `gateway/s3/admin.go` 有 `/admin/cluster/stats`

多机房部署时，运维人员需要分别打开每个集群的 metad 页面，无法统一查看全局状态，操作体验割裂。

### 1.2 目标

构建一个独立的 **admin-server** 多集群运维管理后台：
- **多机房统一看板**：聚合多个区域（北京/上海/深圳）的集群状态，一眼看出哪个机房有问题
- **运维功能齐全**：覆盖节点与磁盘、Bucket 管理、数据完整性、集群治理四大运维域
- **只看不操作（跨集群）**：跨集群层面只做监控统一，各集群内部独立运维
- **替代现有页面**：metad 的 HTML 模板页面废弃，现有 JSON API 保留作为 admin-server 的后端
- **不侵入集群**：admin-server 只调用各集群现有的 ops JSON API，metad/datanode 代码零改动

### 1.3 非目标

- 跨集群数据迁移 / bucket 跨区域复制 / 故障切换（用户明确排除）
- 多租户隔离 / 计费 / 配额管理（非多租户场景）

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        运维人员浏览器                                │
│                   React SPA (TypeScript + Vite)                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ HTTPS (JWT session)
┌──────────────────────────────▼──────────────────────────────────────┐
│                   admin-server (独立进程)                            │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────────┐  │
│  │  Auth 层     │  │ Cluster      │  │  Proxy/Aggregator          │  │
│  │  账号密码    │→ │ Registry     │→ │  · 并发拉取多集群           │  │
│  │  +JWT       │  │ (YAML 配置)  │  │  · 统一聚合/排序/分页       │  │
│  │  预留 SSO   │  │              │  │  · 缓存 10-15s             │  │
│  └─────────────┘  └──────────────┘  └────────────┬───────────────┘  │
│                                                   │                  │
│  ┌───────────────────────────────────────────────┴──────────────┐   │
│  │  REST API: /api/v1/clusters, /api/v1/clusters/:id/...        │   │
│  └──────────────────────────────────────────────────────────────┘   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ HTTP (各集群现有 ops API)
          ┌────────────────────┼────────────────────┐
          │                    │                    │
┌─────────▼─────────┐ ┌────────▼─────────┐ ┌────────▼─────────┐
│  机房 A (北京)     │ │  机房 B (上海)    │ │  机房 C (深圳)    │
│  metad ops :8091  │ │  metad ops :8091 │ │  metad ops :8091 │
│  datanode ops     │ │  datanode ops    │ │  datanode ops    │
└───────────────────┘ └───────────────────┘ └───────────────────┘
```

### 2.2 关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 后端部署形态 | 独立 admin-server 进程 | 职责清晰、故障隔离、部署灵活、不侵入 metad |
| 前端技术栈 | React + TypeScript | 生态成熟，适合复杂后台 |
| 与现有页面关系 | 新后台替代现有 metad HTML 页面 | 运维能力统一收敛，现有 JSON API 保留作后端 |
| 认证 | 账号密码 + JWT，预留 SSO | 内部运维简单够用，设计上预留 SSO 接入点 |
| 跨集群操作 | 只看不操作（监控统一） | 各集群独立运维，避免跨集群数据操作的复杂性 |

### 2.3 降级策略

- 某个集群不可达时，面板显示"离线"标记，其他集群正常展示
- 多集群总览并发拉取，单集群超时 3s，部分失败返回已成功的部分 + 标记失败集群
- 单集群操作遇超时返回 503，前端显示该集群离线

## 3. 运维功能模块

### 3.1 导航布局

- **左侧集群切换**：选择某个集群后右侧显示该集群运维面板；选"多集群总览"看全局聚合
- **顶部模块 Tab**：概览 / 节点 / Bucket / 数据完整性 / 集群治理

### 3.2 功能清单

#### 节点与磁盘
- 节点列表 · 在线状态 · 容量
- 磁盘健康监控（S.M.A.R.T）
- Decommission 节点
- 重启 / drain / 维护模式

#### Bucket 管理
- Bucket CRUD
- 配额设置 · 超额告警
- 放置策略（副本数 / EC）
- 用量统计 · 排行榜

#### 数据完整性
- Chunk 查询 · 完整性验证
- 修复队列 · 手动触发修复
- GC 扫描（孤儿块回收）
- Scrub 一致性校验
- 再平衡 · 触发 / 进度

#### 集群治理
- Raft 状态 · leader 查看
- Audit 日志查询
- 用户 / RBAC 管理
- 配置热更新

## 4. API 设计

### 4.1 数据流

以"查看北京集群节点列表"为例：

1. React SPA 发起 `GET /api/v1/clusters/bj-prod/nodes?status=online`
2. admin-server JWT 中间件校验 token
3. ClusterRegistry 查到 bj-prod 的 metad ops 地址: `10.0.1.3:8091`
4. Cache 检查（key: `bj-prod:nodes:online`）：
   - 命中 → 直接返回（TTL 10s）
   - 未命中 → 转发 `GET http://10.0.1.3:8091/api/v1/nodes`
5. admin-server 收到响应，写入缓存，附带 cluster 字段返回给 SPA
6. 如果 `10.0.1.3:8091` 不可达，返回 503 + `{error: "cluster_unreachable", cluster: "bj-prod"}`

### 4.2 REST API 端点

| Method | Path | 说明 | 后端调用 |
|--------|------|------|----------|
| GET | `/api/v1/clusters` | 列出所有已注册集群及健康状态 | 本地 ClusterRegistry |
| GET | `/api/v1/clusters/:id/overview` | 单集群概览（节点/容量/Bucket/修复队列） | 并发聚合 metad 多个端点 |
| GET | `/api/v1/clusters/:id/nodes` | 节点列表 | 代理 metad /api/v1/nodes |
| POST | `/api/v1/clusters/:id/nodes/:nid/decommission` | 下线节点 | 代理 metad（需 leader 转发） |
| GET | `/api/v1/clusters/:id/buckets` | Bucket 列表 + 用量 | 代理 metad /api/v1/buckets |
| POST | `/api/v1/clusters/:id/buckets` | 创建 Bucket（含策略） | 代理 metad |
| DELETE | `/api/v1/clusters/:id/buckets/:name` | 删除 Bucket | 代理 metad |
| GET | `/api/v1/clusters/:id/chunks/:cid` | 查询 chunk 元数据 + 副本状态 | 代理 metad /api/v1/chunks/:cid |
| POST | `/api/v1/clusters/:id/chunks/:cid/verify` | 触发 chunk 完整性验证 | 代理 datanode ops |
| POST | `/api/v1/clusters/:id/repair/trigger` | 手动触发修复 | 代理 metad /api/v1/repair/trigger |
| GET | `/api/v1/clusters/:id/repair/queue` | 修复队列状态 | 代理 metad /api/v1/repair/queue |
| POST | `/api/v1/clusters/:id/gc/scan` | 触发 GC 扫描 | 代理 datanode ops |
| POST | `/api/v1/clusters/:id/rebalance/trigger` | 触发再平衡 | 代理 metad |
| GET | `/api/v1/clusters/:id/raft/status` | Raft 状态 + leader 信息 | 代理 metad /api/v1/cluster/status |
| GET | `/api/v1/clusters/:id/audit` | 审计日志查询（分页） | 代理 metad /api/v1/audit |
| GET | `/api/v1/clusters/all/overview` | 多集群总览（并发拉取所有集群） | 并发聚合 + 部分失败容忍 |
| POST | `/api/v1/auth/login` | 登录，返回 JWT | 本地校验 |

### 4.3 缓存策略

- 读请求缓存 TTL 10s（节点/Bucket/概览），多集群总览缓存 TTL 15s
- 写请求（POST/DELETE/PUT）不缓存
- 缓存 key 格式：`{cluster}:{resource}:{params}`
- 实现：`sync.Map` + 过期时间戳，后台 goroutine 每 30s 清理过期项

### 4.4 聚合器降级

`aggregator.go` 用 `errgroup` 并发拉取所有集群，每个 goroutine 独立超时（3s）。任一失败不影响其他，最终返回：

```json
{
  "results": [...],
  "failures": [{"cluster": "sz-dr", "error": "timeout"}]
}
```

### 4.5 写操作转发

写请求直接代理到目标集群 metad。metad follower 会 307 重定向到 leader，admin-server 透传重定向。不做写缓存。

## 5. 项目结构

```
nufs-admin/                          # 新顶层目录，与 nufs-core 平级
├── cmd/
│   └── admin-server/main.go         # 入口：加载配置 → 启动 HTTP server
│
├── internal/
│   ├── config/                      # 配置定义与加载
│   │   └── config.go                # clusters.yaml 结构体 + 热加载
│   │
│   ├── cluster/                     # 集群注册表 + 代理客户端
│   │   ├── registry.go              # ClusterRegistry：name→endpoint 映射
│   │   ├── client.go                # ClusterClient：封装对单个集群的 HTTP 调用
│   │   └── health.go                # 后台健康探测（每 30s ping /health）
│   │
│   ├── proxy/                       # 请求代理层
│   │   ├── proxy.go                 # 通用代理：透传 GET/POST 到目标集群
│   │   └── aggregator.go            # 多集群聚合：并发拉取 + 部分失败容忍
│   │
│   ├── cache/                       # 响应缓存
│   │   └── cache.go                 # TTL 缓存（sync.Map + 过期清理）
│   │
│   ├── auth/                        # 认证
│   │   ├── auth.go                  # JWT 签发/校验
│   │   ├── users.go                 # 用户管理（bcrypt 存储）
│   │   └── middleware.go            # HTTP 中间件：校验 JWT
│   │
│   ├── api/                         # HTTP handler 层
│   │   ├── router.go                # 路由注册 + 中间件链
│   │   ├── handler_clusters.go      # /clusters 端点
│   │   ├── handler_nodes.go         # /clusters/:id/nodes 端点
│   │   ├── handler_buckets.go       # /clusters/:id/buckets 端点
│   │   ├── handler_chunks.go        # /clusters/:id/chunks 端点
│   │   ├── handler_repair.go        # 修复/GC/再平衡 端点
│   │   ├── handler_governance.go    # Raft/audit/RBAC 端点
│   │   └── handler_auth.go          # /auth/login 端点
│   │
│   └── server/                      # HTTP server 生命周期
│       └── server.go                # graceful shutdown + embed 静态资源
│
├── web/                             # 前端 React 项目
│   ├── package.json
│   ├── vite.config.ts
│   ├── src/
│   │   ├── main.tsx                 # 入口
│   │   ├── App.tsx                  # 路由 + 布局
│   │   ├── api/                     # API 客户端（axios 封装）
│   │   ├── components/              # 通用组件（表格/卡片/状态标签）
│   │   ├── pages/
│   │   │   ├── overview/            # 单集群概览页
│   │   │   ├── nodes/               # 节点管理页
│   │   │   ├── buckets/             # Bucket 管理页
│   │   │   ├── integrity/           # 数据完整性页
│   │   │   ├── governance/          # 集群治理页
│   │   │   └── global/              # 多集群总览页
│   │   ├── hooks/                   # 自定义 hooks（useClusters, useNodes）
│   │   └── types/                   # TypeScript 类型定义
│   └── dist/                        # 构建产物 → go:embed
│
├── deploy/
│   ├── clusters.example.yaml        # 集群配置示例
│   └── docker-compose.yml
│
└── Makefile                         # build-web / build-server / build-all
```

## 6. 核心数据结构

### 6.1 配置

```go
// config.go — 集群配置
type ClusterConfig struct {
    Name        string // "bj-prod"
    Region      string // "beijing"
    MetadOpsURL string // "http://10.0.1.3:8091"
    Description string
}

type Config struct {
    Clusters []ClusterConfig
    Server   struct {
        Listen    string // ":8090"
        JWTSecret string
    }
    Auth struct {
        UsersFile string // "users.yaml"
    }
}
```

### 6.2 集群客户端

```go
// cluster/client.go — 单集群客户端
type ClusterClient struct {
    name    string
    baseURL string
    http    *http.Client // 复用连接池
}

// GetNodes 代理 GET /api/v1/nodes
func (c *ClusterClient) GetNodes(ctx context.Context) ([]NodeInfo, error)

// TriggerRepair 代理 POST /api/v1/repair/trigger
func (c *ClusterClient) TriggerRepair(ctx context.Context) error

// 所有方法都有 ctx + 3s 超时
```

## 7. 关键实现细节

### 7.1 请求处理链路

1. JWT 中间件校验 token
2. 路由到对应 handler
3. handler 从 ClusterRegistry 取 client
4. 读请求：先查 cache，未命中调 client
5. 写请求：直接调 client（不缓存）
6. 多集群总览：aggregator 并发调所有 client

### 7.2 缓存实现

简单 `sync.Map` + 过期时间戳，后台 goroutine 每 30s 清理过期项。不做 LRU——TTL 10s 内条目数有限。缓存 value 存 JSON bytes，避免类型转换开销。

### 7.3 前端 embed

构建后 `web/dist/` 通过 Go `go:embed` 嵌入二进制。SPA fallback：所有非 `/api` 路径返回 `index.html`。开发时 Vite proxy 到 `:8090`。

### 7.4 配置热加载

收到 `SIGHUP` 重读 `clusters.yaml`，原子替换 Registry 内部 map。新增集群立即生效。删除集群时，Registry 标记该集群为 `draining` 状态，新的请求不再路由到该集群；进行中的请求通过 context cancel 被取消（每个代理请求的 ctx 都挂在 Registry 的 per-cluster context 上，替换时 cancel 旧 context）。

### 7.5 健康探测

后台 goroutine 每 30s 对每个集群 `GET /health`，更新 Registry 中的健康状态。前端侧边栏集群列表展示绿/黄状态点即来自此处。

### 7.6 认证

登录返回 JWT（有效期 12h），后续请求带 `Authorization: Bearer`。用户表本地存储（bcrypt），预留 SSO 接口 `/api/v1/auth/sso/callback`。

## 8. 部署

### 8.1 单二进制部署

`make build-all` 先 `npm run build` 生成 `web/dist/`，再 `go build` 把前端 embed 进二进制。产出单个 `admin-server` 可执行文件，只需 `clusters.yaml` + `users.yaml` 两个配置文件即可运行。

### 8.2 Docker

Docker 镜像同样单层：COPY 二进制 + 配置，`EXPOSE 8090`。

### 8.3 Kubernetes

Kubernetes 部署用现有 `deploy/helm` 目录的模式，加一个 admin-server Deployment + Service。

## 9. UI 设计

### 9.1 风格

浅色简洁风格，白底 + 蓝色主色调（#2563eb）。字体：Plus Jakarta Sans（UI）+ JetBrains Mono（数据/代码）。

### 9.2 布局

- 顶栏：Logo + 集群在线状态 + 告警数 + 用户信息
- 左侧栏：集群列表（带 region、节点数、健康状态点）+ 多集群总览入口
- 主内容区：标题 + 健康徽标 + 模块 Tab + 指标卡片 + 功能面板

### 9.3 指标卡片

概览页展示 4 个关键指标卡片：在线节点、容量使用、Bucket 数、修复队列。每个卡片含标签、数值、状态色、副信息。
