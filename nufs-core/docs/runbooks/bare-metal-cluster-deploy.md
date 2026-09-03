# Bare-Metal Cluster Deploy (Linux)

NUFS 裸机完整集群部署：3 metad（raft quorum）+ 3 datanode（RF=3）+ 1 S3 网关，生产模式（认证开启，无 --allow-insecure-dev）。

> **有现成自动化，嫌手工命令多直接用它**：
> - 部署脚本（按 IP 自动识别角色，支持 flag / `--config` 双模式）：[deploy/vm/deploy.sh](../deploy/vm/deploy.sh)（配置模板 [cluster.env.example](../deploy/vm/cluster.env.example)）
> - Ansible 一条命令部署：[deploy/ansible/site.yml](../deploy/ansible/site.yml)（清单 [inventory.ini](../deploy/ansible/inventory.ini)；默认复用 `make package` 产物，不现场构建）
>
> 下方保留手工步骤，用于理解/排障。

## 端口规划（单机多进程，或跨机改 IP）

| 组件 | ops | raft | data/chunk | datanode ops |
|---|---|---|---|---|
| metad-1/2/3 | 18091/18092/18093 | 17001/17002/17003 | - | - |
| datanode-1/2/3 | - | - | 9103/9104/9105 | 18096/18097/18098 |
| gateway | 8081 | - | - | - |

跨机部署时把 `127.0.0.1` 换成各机实际 IP；datanode 的 `--register-addr` 必须填 gateway 能路由到的地址。

## 1. 构建二进制

```bash
cd nufs-core
go build -o bin/metad ./cmd/metad
go build -o bin/datanode ./cmd/datanode
go build -o bin/nufs-s3 ./cmd/nufs-s3
```

## 2. 生成密钥（记好，后续要用）

```bash
AUTH_TOKEN=$(openssl rand -hex 16)
TOKEN_SIGNING_KEY=$(openssl rand -hex 16)
CRED_SECRET_KEY=$(openssl rand -hex 32)   # 必须 32 字节 hex
export AUTH_TOKEN TOKEN_SIGNING_KEY CRED_SECRET_KEY
```

## 3. 启动 3 个 metad（先起 metad-1，等它成为 leader 再起 2/3）

```bash
BASE=/var/lib/nufs

# metad-1（bootstrap owner）
bin/metad --node-id=meta-1 --data-dir=$BASE/meta1 --ops-addr=127.0.0.1:18091 \
  --raft=true --raft-bootstrap=true --raft-bootstrap-owner=meta-1 \
  --raft-addr=127.0.0.1:17001 --raft-advertise-addr=127.0.0.1:17001 \
  --raft-dir=$BASE/raft1 \
  --raft-bootstrap-peers="meta-1=127.0.0.1:17001,meta-2=127.0.0.1:17002,meta-3=127.0.0.1:17003" \
  --raft-peer-ops="meta-1=http://127.0.0.1:18091,meta-2=http://127.0.0.1:18092,meta-3=http://127.0.0.1:18093" \
  --advertise-ops-addr=http://127.0.0.1:18091 \
  --auth-token=$AUTH_TOKEN --token-signing-key=$TOKEN_SIGNING_KEY --credential-secret-key=$CRED_SECRET_KEY

# metad-2（node-id=meta-2, ops=18092, raft=17002, data=$BASE/meta2, raft-dir=$BASE/raft2）
# metad-3（node-id=meta-3, ops=18093, raft=17003, data=$BASE/meta3, raft-dir=$BASE/raft3）
# 2/3 参数与 metad-1 相同，只改 node-id/端口/目录
```

> 关键：`--raft-bootstrap-owner=meta-1` 只有 owner 自举，2/3 等 owner 拉入（AddVoter）——否则多节点 raft 不收敛。

## 4. 启动 3 个 datanode

```bash
for i in 1 2 3; do
  bin/datanode --node-id=$i \
    --listen=127.0.0.1:910$i --register-addr=127.0.0.1:910$i \
    --ops-addr=127.0.0.1:1809$((i+5)) \
    --data-dirs=$BASE/dn$i \
    --metadata=127.0.0.1:18091 \
    --metadata-auth-token=$AUTH_TOKEN --ops-auth-token=$AUTH_TOKEN &
done
```

## 5. 启动 S3 网关

```bash
bin/nufs-s3 --listen=:8081 --meta-addr=127.0.0.1:18091 \
  --meta-auth-token=$AUTH_TOKEN
```

## 6. 验证集群就绪

```bash
curl -H "Authorization: Bearer $AUTH_TOKEN" \
  http://127.0.0.1:18091/api/v1/cluster/status     # is_leader=true
curl -H "Authorization: Bearer $AUTH_TOKEN" \
  http://127.0.0.1:18091/api/v1/cluster/readiness  # 不是 not_ready
```

## 7. seed S3 凭据 + 访问

```bash
curl -X PUT http://127.0.0.1:18091/api/v1/auth/creds/TESTAK000000000000 \
  -H "Authorization: Bearer $AUTH_TOKEN" -H "content-type: application/json" \
  -d '{"secret_key":"test-secret-value","principal":"test"}'

# 用 SigV4 客户端访问 http://localhost:8081（rclone 示例）
rclone config create nufs s3 provider=Other \
  access_key_id=TESTAK000000000000 secret_access_key=test-secret-value \
  endpoint=http://localhost:8081 no_check_bucket=true
rclone copyto /tmp/test.txt nufs:test-bucket/obj.txt
rclone cat nufs:test-bucket/obj.txt
```

> 生产模式 gateway 钉死认证：没 seed 凭据时所有请求 403（设计如此，不是 bug）。

## 8. 验收

- [ ] 3 metad 形成 quorum，`/api/v1/cluster/status` is_leader=true
- [ ] datanode 全部注册、容量正常
- [ ] S3 PUT/GET hash 一致
- [ ] failover：kill leader metad 进程，写继续可用（秒级恢复）
- [ ] 容灾：kill 一个 datanode，RF=3 下读仍可用

## 运维

- 备份/回滚：生产模式建议启用 metad backup（`--backup-enabled` + S3 仓库），测试环境可跳过
- 日志：各进程 stdout 即日志，可重定向到文件；`--log-format=json` 输出结构化日志
- 停止：按 gateway → datanode → metad（非 leader 先）顺序 kill
- 重启：metad 按 1/2/3 顺序起（owner 先），raft 状态在 `--raft-dir` 持久化
