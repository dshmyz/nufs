# NUFS 集群部署指南（VM / 裸机，从零到可用）

本文档覆盖**完整部署流程**：准备 → 打包 → 部署 → 创建 bucket → 挂载使用 → 管理台。
3 节点拓扑（3 metad raft + 3 datanode + 1 网关 + 1 管理台），生产模式（认证开启）。

**先选部署方式：**

| 方式 | 适合 | 命令量 |
|---|---|---|
| **方式 A：Ansible（推荐）** | 有 3 台 VM，想一条命令部署 | 最少 |
| **方式 B：手动脚本** | 逐台 VM 操作，或没有 Ansible | 中等 |
| **方式 C：纯手动二进制** | 不用任何脚本，每条命令手敲（理解原理/排障/受限环境） | 最多，全部透明 |

---

## 0. 前置条件

**控制机（你的工作机）：**
- Go 1.25+、Node.js 18+（前端构建）、make
- 方式 A 需要：Ansible（`pip install ansible-core`），且能 SSH 免密到所有 VM
- 方式 A 的节点需要 Python 3（Ansible 目标机要求）

**3 台 VM（Linux，建议配置）：**
- 每台：2C4G 起步，磁盘按数据量规划（datanode 数据目录所在盘）
- 网络互通；以下端口未被占用：
  - metad：`18091/18092/18093`（ops HTTP）、`17001/17002/17003`（raft）
  - datanode：`9103/9104/9105`（chunk TCP）、`18096/18097/18098`（ops HTTP）
  - S3 网关：`8081`；管理台：`8090`
- SSH 用户有 sudo（root 或免密 sudo）

**本文约定拓扑**（每台 VM 同时跑 metad-N + datanode-N，网关/管理台挂节点 A）：

```
节点 A  10.0.0.11  → metad-1 + datanode-1 + S3 网关 + 管理台
节点 B  10.0.0.12  → metad-2 + datanode-2
节点 C  10.0.0.13  → metad-3 + datanode-3
```

> 也可以 metad 和 datanode 各用独立 VM（共 7 台），只是清单里 IP 不同，参数不变。

---

## 1. 构建部署包（控制机，一次）

```bash
cd nufs-core
make package
```

产出在 `dist/`：

```
nufs-vX.Y.Z-linux-amd64.tar.gz   ← 这个拷到 VM 用（含 linux 全部二进制）
```

包内容（解压即自包含）：

```
nufs-vX.Y.Z-linux-amd64/
├── bin/                 # metad datanode nufs-s3 nufs-fuse nufs-cli（Linux 静态二进制）
├── admin/               # 管理台：admin-server + web/dist 前端
├── deploy.sh            # 部署/启停脚本（自动用旁边的 bin/）
├── cluster.env.example  # 集群配置模板（方式 B 用）
└── config/              # 默认模块配置（config/ 目录模式用）
```

---

## 2. 方式 A：Ansible 一键部署（推荐）

### 2.1 写清单

```bash
cd nufs-core/deploy/ansible
cp inventory.ini.example inventory.ini
vi inventory.ini
```

把模板里的 `<NODE-A-IP>` 等占位符全部换成真实 IP（**必须是 IP，不能是域名**——deploy.sh 按 IP 识别角色）：

```ini
[metad_nodes]
nufs-a ansible_host=10.0.0.11 meta_ordinal=1
nufs-b ansible_host=10.0.0.12 meta_ordinal=2
nufs-c ansible_host=10.0.0.13 meta_ordinal=3

[datanode_nodes]
nufs-a ansible_host=10.0.0.11 dn_ordinal=1
nufs-b ansible_host=10.0.0.12 dn_ordinal=2
nufs-c ansible_host=10.0.0.13 dn_ordinal=3

[s3_gateway]
nufs-a ansible_host=10.0.0.11

[admin]
nufs-a ansible_host=10.0.0.11
```

要点：
- `meta_ordinal`/`dn_ordinal` 是节点编号（1/2/3），决定端口偏移和 raft owner（**metad-1 是 owner，必须先启动**——playbook 已处理顺序）
- `metad_nodes` 必须是**奇数台**（raft quorum）
- SSH 用户/密钥不是 root 的话，在主机行加 `ansible_user=xxx ansible_ssh_private_key_file=~/.ssh/id_rsa`

### 2.2 填业务凭据（唯一必改的变量）

```bash
vi group_vars/all.yml
```

```yaml
seed_credentials:
  - { access_key: "业务AK", secret_key: "业务SK" }   # 客户端（rclone/mc/自研 SDK）访问 S3 用
```

> 不填也能部署成功，但 S3 网关会 403（生产模式无凭据拒绝所有请求，设计如此）。

**其他常用变量**（都有默认，按需改）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `data_root` | `/var/lib/nufs` | 数据根目录 |
| `need_sudo` | `true` | 是否 sudo 执行 |
| `admin_port` | `8090`（admin.yml 里） | 管理台端口 |
| `admin_jwt_secret` | `change-me-in-production` | **生产必须换**（-e 传） |
| 端口四件套 | 18091/17001/9103/18096 | 按节点编号自动 +0/+1/+2 |

### 2.3 部署

```bash
cd nufs-core/deploy/ansible

# 全量：分发二进制 → gen-config → metad-1 先起等当选 → 其余起 → verify → seed 凭据
ansible-playbook -i inventory.ini site.yml

# 管理台（Web 控制台，可选但推荐）
ansible-playbook -i inventory.ini admin.yml
```

首次构建 admin 前端需要控制机有 node；密钥自动生成存 `secrets/`（勿提交、勿删——删了重跑会生成新密钥导致旧数据无法访问）。

### 2.4 验证

playbook 末尾自动跑 verify。手动验证：

```bash
# 在任意节点上（deploy_dir 默认 /opt/nufs）
/opt/nufs/deploy.sh verify
# 期望输出：quorum OK（1 个 leader）→ readiness 200 → datanode 3/3 注册 → 集群就绪
```

**方式 A 到此完成，跳到 §4 创建 bucket。**

---

## 3. 方式 B：手动脚本部署（逐台 VM）

### 3.1 分发包 + 生成配置

把 `dist/nufs-*-linux-amd64.tar.gz` 拷到**每台** VM：

```bash
scp dist/nufs-*-linux-amd64.tar.gz user@10.0.0.11:/opt/
# （12、13 同理）
```

**每台 VM** 上：

```bash
cd /opt && tar -xzf nufs-*-linux-amd64.tar.gz && cd nufs-*-linux-amd64
./deploy.sh gen-default-config     # 生成本机角色的默认配置（127.0.0.1）
```

### 3.2 改配置里的 IP（关键步骤）

`gen-default-config` 默认按 127.0.0.1 生成（单机演示用）。**多机部署必须改成真实 IP**：

```bash
vi config/*.yaml
```

需要改的地方（以节点 A 为例，B/C 同理只改自己的 IP 和对应文件）：

| 文件 | 改什么 |
|---|---|
| `config/metad1.yaml` | `ops_addr`/`advertise_ops_addr` → `10.0.0.11:18091`；`raft_advertise_addr` → `10.0.0.11:17001`；`raft_bootstrap_peers`/`raft_peer_ops` → **三台的 IP 各自替换**（三台要保持一致！）；data_dir/raft_dir 保持 |
| `config/metad2.yaml` | 同上，把 2 号相关地址改成 `10.0.0.12` |
| `config/metad3.yaml` | 同上，`10.0.0.13` |
| `config/datanode1.yaml` | `register_addr` → `10.0.0.11:9103`（必须填**能被网关路由到的地址**） |
| `config/datanode2/3.yaml` | 同上各自 IP |
| `config/s3.yaml` | 一般不用改（meta_addr 指向 metad-1 即可） |

> 提示：`sed -i 's/127.0.0.1/10.0.0.11/g' config/metad1.yaml config/datanode1.yaml` 一条命令就够，但 `raft_bootstrap_peers`/`raft_peer_ops` 里三个节点要手动分别替换成各自 IP。

**可选参数**（都在 YAML 里直接改）：

| 参数 | 默认 | 说明 |
|---|---|---|
| `capacity`（datanode） | 1000（GB） | 上报的节点容量 |
| `rack`/`zone` | rack-N / zone-1 | 拓扑标签，影响副本放置 |
| `data_dir`（datanode） | 单目录 | **多盘 JBOD**：改成 `"/mnt/disk1,/mnt/disk2"`，磁盘 ID 自动按 `NodeID×1000+序号` 编 |
| `log_format` | json | 改 `text` 可读性好些 |
| `log_level` | info | debug/info/warn/error |

**密钥**：gen-default-config 已自动生成三把密钥写进各 YAML（`auth_token`/`token_signing_key`/`credential_secret_key`）。**集群内所有组件的 `auth_token` 必须一致**——把 metad1.yaml 里的 `auth_token` 值复制到 datanode 和 s3 的 `*_auth_token`（gen-default-config 已经做了，手动改文件时别改岔）。

### 3.3 启动（顺序关键）

```bash
# ① 节点 A 先起（metad-1 是 raft owner，必须最先起）
./deploy.sh start

# ② 确认 metad-1 当选（is_leader=true 再继续）
curl -s -H "Authorization: Bearer <auth_token>" http://10.0.0.11:18091/api/v1/cluster/status

# ③ 节点 B、C 各自（2/3 会被 leader 自动拉入 raft，无需手工 join）
./deploy.sh start
```

> raft 加入机制：metad-1 自举为 1-voter 集群并当选；metad-2/3 以 follower 起，leader 的 reconcile 循环（启动即拉 + 每 5s 兜底）用 AddVoter 把它们拉入。**三台的 `raft_bootstrap_peers` 必须完全一致**，否则不收敛。

### 3.4 部署管理台（节点 A）

```bash
cd /opt/nufs-*-linux-amd64/admin
# clusters.yaml
TOK=$(grep '^auth_token:' ../config/metad1.yaml | sed -E 's/.*"([^"]+)".*/\1/')
cat > clusters.yaml <<EOF
clusters:
  - name: my-cluster
    region: default
    metad_ops_url: "http://10.0.0.11:18091"
    metad_token: "$TOK"
    description: "生产集群"
server:
  listen: ":8090"
  jwt_secret: "换成随机串 $(openssl rand -hex 16)"
auth:
  users_file: "/opt/nufs-admin/users.yaml"
database:
  dsn: ""
EOF
# 用户文件（默认 admin/password，生产务必换）
cat > users.yaml <<EOF
- username: admin
  password_hash: "$(<生成 bcrypt 哈希，见下)"
EOF
NUFS_ADMIN_CONFIG=$PWD/clusters.yaml nohup ./admin-server > admin.log 2>&1 &
echo $! > admin.pid
```

生成 bcrypt 哈希（改密码用）：`htpasswd -nbB admin 新密码` 取冒号后部分。

### 3.5 验证

```bash
./deploy.sh verify    # 在任意节点；方式 A 已自动做过
```

期望：
```
=== 1/3 校验 metad raft quorum ===
  meta-1  is_leader=true/false ...（三台看到同一 leader）
  quorum OK：1 个 leader=...，所有节点看到同一 leader
=== 2/3 校验 leader readiness ===   200 OK
=== 3/3 校验 datanode 注册 ===      已注册 datanode: 3（期望 3）
=== verify 通过：集群就绪 ===
```

---

## 3C. 方式 C：纯手动二进制部署（不用任何脚本）

不依赖 deploy.sh / Ansible / 配置文件——**只拿二进制，每条命令手敲**。适合理解原理、排障、或环境受限时最小化部署。命令里的 IP/密钥换成你自己的。

### C.1 分发二进制 + 建目录

只需要 3 个二进制（fuse/cli 不需要）：`metad`、`datanode`、`nufs-s3`。

```bash
# 每台 VM：放到 PATH 里并建数据目录
sudo install -m 0755 metad datanode nufs-s3 /usr/local/bin/
sudo mkdir -p /var/lib/nufs /var/log/nufs

# 节点 A 额外装挂载 helper（FUSE 可选）
sudo install -m 0755 mount.nufs /sbin/mount.nufs   # 只在要用 FUSE 的节点
```

### C.2 生成三把密钥（只做一次，记录好）

```bash
AUTH_TOKEN=$(openssl rand -hex 16)          # ops API Bearer，全集群共用
TOKEN_SIGNING_KEY=$(openssl rand -hex 16)   # 挂载认证 HMAC key
CRED_SECRET_KEY=$(openssl rand -hex 32)     # S3 凭据注册表加密（必须 32 字节 hex）
# 记下来（echo 到文件存好），后面每个进程都要用
```

### C.3 启动 3 个 metad（顺序：1 → 等 leader → 2、3）

**节点 A（metad-1，bootstrap owner）：**

```bash
AUTH_TOKEN=<上一步的值> TOKEN_SIGNING_KEY=<上一步的值> CRED_SECRET_KEY=<上一步的值>
nohup metad \
  --node-id=1 \
  --data-dir=/var/lib/nufs/meta1 \
  --ops-addr=10.0.0.11:18091 \
  --advertise-ops-addr=http://10.0.0.11:18091 \
  --raft=true \
  --raft-addr=0.0.0.0:17001 \
  --raft-advertise-addr=10.0.0.11:17001 \
  --raft-dir=/var/lib/nufs/raft1 \
  --raft-bootstrap=true \
  --raft-bootstrap-owner=meta-1 \
  --raft-bootstrap-peers="meta-1=10.0.0.11:17001,meta-2=10.0.0.12:17002,meta-3=10.0.0.13:17003" \
  --raft-peer-ops="meta-1=http://10.0.0.11:18091,meta-2=http://10.0.0.12:18092,meta-3=http://10.0.0.13:18093" \
  --auth-token=$AUTH_TOKEN \
  --token-signing-key=$TOKEN_SIGNING_KEY \
  --credential-secret-key=$CRED_SECRET_KEY \
  --log-format=json \
  > /var/log/nufs/metad1.log 2>&1 &
echo $! > /var/lib/nufs/metad1.pid
```

**等它当选**（必须成功再继续）：

```bash
curl -s -H "Authorization: Bearer $AUTH_TOKEN" \
  http://10.0.0.11:18091/api/v1/cluster/status
# 期望 "is_leader": true
```

**节点 B（metad-2）**——和 1 号完全一样，只改 4 处 + peers 原样照抄：

```bash
nohup metad \
  --node-id=2 \                                   # ← 改
  --data-dir=/var/lib/nufs/meta2 \                # ← 改
  --ops-addr=10.0.0.12:18092 \                    # ← 改（IP+端口）
  --advertise-ops-addr=http://10.0.0.12:18092 \   # ← 改
  --raft=true \
  --raft-addr=0.0.0.0:17002 \                     # ← 改（端口）
  --raft-advertise-addr=10.0.0.12:17002 \         # ← 改
  --raft-dir=/var/lib/nufs/raft2 \                # ← 改
  --raft-bootstrap=true \
  --raft-bootstrap-owner=meta-1 \                 # 不改！owner 固定是 meta-1
  --raft-bootstrap-peers="meta-1=10.0.0.11:17001,meta-2=10.0.0.12:17002,meta-3=10.0.0.13:17003" \  # 三台必须一字不差
  --raft-peer-ops="meta-1=http://10.0.0.11:18091,meta-2=http://10.0.0.12:18092,meta-3=http://10.0.0.13:18093" \
  --auth-token=$AUTH_TOKEN \
  --token-signing-key=$TOKEN_SIGNING_KEY \
  --credential-secret-key=$CRED_SECRET_KEY \
  --log-format=json \
  > /var/log/nufs/metad2.log 2>&1 &
```

> **metad-2/3 为什么不用"join"**：它们带 `--raft-bootstrap=true` 但 node-id 不是 owner，启动时自动跳过自举、以空配置当 follower；metad-1 当选后由 reconcile 循环（启动即拉 + 每 5s 兜底）把配置里列出的对端 AddVoter 拉进集群。**所以 peers 列表三台必须完全一致**，且写错不会立刻报错——只会选不出 leader。

**节点 C（metad-3）**：同 metad-2，把 `2`→`3`、`10.0.0.12`→`10.0.0.13`、端口 `18092`→`18093`、`17002`→`17003`。

**确认 raft 收敛**（三台都看到同一个 leader）：

```bash
for p in 18091 18092 18093; do
  curl -s -H "Authorization: Bearer $AUTH_TOKEN" http://10.0.0.11:$p/api/v1/cluster/status | grep -o '"leader_uri":"[^"]*"'
done
# 三行输出应相同（有且只有一个 is_leader=true）
```

### C.4 启动 3 个 datanode（可并行）

**节点 A（datanode-1）：**

```bash
nohup datanode \
  --node-id=1 \
  --listen=0.0.0.0:9103 \
  --register-addr=10.0.0.11:9103 \          # ← 必须是网关/其他节点能路由到的地址
  --ops-addr=0.0.0.0:18096 \
  --data-dir=/var/lib/nufs/datanode1 \      # 多盘 JBOD 换成：--data-dir=/mnt/disk1,/mnt/disk2
  --metadata=10.0.0.11:18091 \
  --metadata-auth-token=$AUTH_TOKEN \
  --ops-auth-token=$AUTH_TOKEN \
  --rack=rack-1 --zone=zone-1 --capacity=1000 \
  --log-format=json \
  > /var/log/nufs/datanode1.log 2>&1 &
echo $! > /var/lib/nufs/datanode1.pid
```

**节点 B（datanode-2）**：`node-id=2`、`listen=0.0.0.0:9104`、`register-addr=10.0.0.12:9104`、`ops-addr=0.0.0.0:18097`、`data-dir=.../datanode2`、`rack=rack-2`。
**节点 C（datanode-3）**：`node-id=3`、`9105`/`18098`/`datanode3`/`rack-3`，IP `10.0.0.13`。

**确认注册**：

```bash
curl -s -H "Authorization: Bearer $AUTH_TOKEN" http://10.0.0.11:18091/api/v1/nodes | grep -c '"id":'
# 期望 3
```

### C.5 启动 S3 网关（节点 A）

```bash
nohup nufs-s3 \
  --listen=:8081 \
  --meta-addr=10.0.0.11:18091 \
  --meta-auth-token=$AUTH_TOKEN \
  --part-dir=/var/lib/nufs/s3-parts \
  --log-format=json \
  > /var/log/nufs/s3.log 2>&1 &
echo $! > /var/lib/nufs/s3.pid
```

> 生产建议给网关一个稳定地址（keepalived VIP / 反向代理后挂三台 metad），否则 metad-1 宕机时网关写路径中断（即便 raft 正常切主）。

### C.6 seed S3 凭据（没凭据网关 403）

```bash
curl -X PUT http://10.0.0.11:18091/api/v1/auth/creds/业务AK \
  -H "Authorization: Bearer $AUTH_TOKEN" -H "content-type: application/json" \
  -d '{"secret_key":"业务SK","principal":"app"}'
```

### C.7 验证集群

```bash
# 1. raft：三台同一 leader（见 C.3 末尾）
# 2. readiness 200
curl -s -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $AUTH_TOKEN" \
  http://10.0.0.11:18091/api/v1/cluster/readiness
# 3. 建桶 + 读写
curl -X POST http://10.0.0.11:18091/api/v1/buckets \
  -H "Authorization: Bearer $AUTH_TOKEN" -H "content-type: application/json" \
  -d '{"name":"smoke"}'
rclone copyto /tmp/t.txt nufs-s3:smoke/t.txt && rclone cat nufs-s3:smoke/t.txt
```

### C.8 停止 / 重启（纯手动没有 down 命令）

```bash
# 停止顺序：s3 → datanode → metad（非 leader 的 metad 先）
kill $(cat /var/lib/nufs/s3.pid) /var/lib/nufs/datanode*.pid 里对应的 pid
kill $(cat /var/lib/nufs/metad2.pid) $(cat /var/lib/nufs/metad3.pid) $(cat /var/lib/nufs/metad1.pid)

# 重启：按 metad-1 → 2 → 3 → datanode → s3 顺序重新执行 C.3 起的命令
# raft 状态在 --raft-dir 持久化，重启幂等（数据不丢）
# 注意：kill 后等进程真正退出再重启（pebble 释放 DB 锁需要一两秒）
```

### C.9 生产化（手动部署别漏）

- **进程守护**：nohup 重启后不会自愈。正式环境把 C.3–C.5 的命令写成 systemd 单元（`Restart=always`），或改用方式 A/B
- **日志轮转**：nohup + 重定向不会轮转，配 logrotate 或用 `--log-format=json` 输出接采集器
- **备份**：metad 加 `--backup-enabled --cluster-id=<稳定ID> --backup-s3-bucket=<桶> ...`（见 §7）

---

## 4. 创建 bucket（两种方式）

> **网关不自动建桶**，先建桶才能写数据。

### 方式 1：管理台（最简单）

浏览器打开 `http://<管理台IP>:8090` → 登录（admin/password）→ 左侧选集群 → **Bucket** 页 → **创建 Bucket** → 输入名称 → 创建。同页可设置容量/对象配额。

### 方式 2：ops API（curl）

```bash
TOK=<你的 auth_token>       # config/metad1.yaml 里的 auth_token
curl -X POST http://10.0.0.11:18091/api/v1/buckets \
  -H "Authorization: Bearer $TOK" -H "content-type: application/json" \
  -d '{"name":"my-bucket"}'
# 期望：201 {"status":"created"}

# 确认
curl -s -H "Authorization: Bearer $TOK" http://10.0.0.11:18091/api/v1/buckets
```

### 验证 S3 读写（rclone）

```bash
# 部署时 seed 的凭据（group_vars/all.yml 里填的，或手动：见 §3.5 前的 seed-cred 命令）
rclone config create nufs s3 provider=Other \
  access_key_id=<业务AK> secret_access_key=<业务SK> \
  endpoint=http://10.0.0.11:8081 no_check_bucket=true

rclone copyto /tmp/test.txt nufs:my-bucket/obj.txt
rclone cat nufs:my-bucket/obj.txt        # 应输出 test.txt 内容
```

> 生产模式网关钉死认证：没 seed 凭据时所有请求 403（设计如此）。
> 手动 seed：`./deploy.sh seed-cred <AK> <SK>`（在任一有 deploy.sh 的节点）。

---

## 5. FUSE 挂载（可选，把集群挂成本地目录）

```bash
# 装挂载 helper（需 root，包里有 bin/nufs-fuse）
sudo make install-mount-helper    # 或手动：sudo cp deploy/mount.nufs /sbin/ && chmod 755

mkdir -p /mnt/nufs
sudo mount -t nufs none /mnt/nufs -o meta=10.0.0.11:18091
# 之后 /mnt/nufs 就是分布式文件系统；卸载：sudo umount /mnt/nufs
```

挂载认证走 metad 统一认证（token signing key 已在集群配置里）。

---

## 6. 部署后状态总览

| 服务 | 地址 | 用途 |
|---|---|---|
| S3 网关 | `http://<节点A>:8081` | 对象存储（SigV4） |
| 管理台（nufs-admin） | `http://<节点A>:8090` | **日常运维入口**：bucket/配额/治理/多集群 |
| metad 控制台（内嵌） | `http://<节点A>:18091/admin/` | 单集群深排障：chunk/namespace/备份/修复/再平衡 |
| datanode 控制台（内嵌） | `http://<节点N>:1809x/admin/` | 单节点：磁盘生命周期/GC/节点下线 |
| ops API | `http://<节点A>:18091/api/v1/...` | 自动化/脚本（Bearer = auth_token） |

> 三个控制台是"舰队→集群→节点"三层：日常开管理台，往下钻（管理台集群卡片的"metad 控制台 ↗"链接直达）。内嵌控制台首次打开需在页面右下/右上输入 ops token（auth_token），保存在浏览器本地。

## 7. 日常运维速查

```bash
/opt/nufs/deploy.sh status      # 本机进程
/opt/nufs/deploy.sh verify      # 全集群校验
/opt/nufs/deploy.sh down        # 停本机（s3/datanode 先，metad 后）
/opt/nufs/deploy.sh start       # 起（metad-1 所在节点最先）
```

- **换盘/加盘**：datanode 控制台 Disks 页（Adopt/Retire/Migrate），或改 `data_dir` 重启
- **升级**：控制机 `make package` → 重发包 → 各节点 `down && start`（metad-1 先）
- **监控**：`deploy/monitoring/` 有现成 prometheus/grafana/alerting 配置，指标端点 `:18091/api/v1/metrics`
- **生产必做**：开启 metad 备份（`--backup-enabled` + S3 仓库参数），并跑一次恢复演练

## 8. 常见问题

| 现象 | 原因 | 处理 |
|---|---|---|
| verify 报"没有任何 metad 是 leader" | metad-1 没先起，或三台 peers 列表不一致 | 核对三台 `raft_bootstrap_peers` 完全一致；metad-1 先 start |
| datanode 注册数 0 | `register_addr` 填了不可路由地址，或 auth_token 不一致 | register_addr 用真实 IP；对齐 auth_token |
| S3 全部 403 | 没 seed 凭据（生产模式设计如此） | 管理台/API seed 凭据，或 `seed-cred` |
| PUT 报 "bucket not found" | 网关不自动建桶 | 先建桶（§4） |
| 内嵌控制台打开 401 | 数据端点要 token | 页面内输入 auth_token（保存到浏览器本地） |
| down 后立刻 start 报 pebble 错 | 旧进程还没释放 DB 锁 | deploy.sh 已内置等待；若手动 kill 请等进程退出再 start |
