#!/usr/bin/env bash
#
# NUFS 多虚拟机集群部署脚本。
#
# 每台 VM 放同一份 deploy.sh + cluster.env，脚本按本机 IP 自动识别该机要跑
# 的角色（metad-1/2/3、datanode-1/2/3、s3），把裸机 runbook 里那一大串手写
# 参数从一份 IP 清单 + 三把密钥自动生成——只需要在 cluster.env 里填 IP 和密钥。
#
# 用法:
#   ./deploy.sh gen-keys                 # 生成三把密钥，粘贴进 cluster.env
#   ./deploy.sh install                  # 编译 + 安装 3 个二进制到 /usr/local/bin
#   ./deploy.sh gen-config               # 生成本机角色的 YAML 配置（$DATA_ROOT/config）
#   ./deploy.sh start                    # 启动本机服务：有 YAML 用 --config，否则用 flag 参数
#   ./deploy.sh status                   # 本机进程状态
#   ./deploy.sh verify                   # 全集群校验：raft quorum + datanode 注册
#   ./deploy.sh seed-cred [AK] [SK]      # 往 metad 注册一个 S3 凭据（生产模式必需）
#   ./deploy.sh down                     # 停本机服务
#   ./deploy.sh dump                     # 打印本机将执行的启动命令（排障用，不启动）
#   ./deploy.sh install-systemd          # 为角色生成 systemd 单元并启用（生产推荐）
#   ./deploy.sh systemd start|stop|status [role]   # 通过 systemd 管理本机服务
#
# 两种启动方式（二选一，可随时切换）：
#   flag 方式（默认，原始行为）：start 直接拼长参数列表启动，参数始终取自 cluster.env。
#   config 方式：先 gen-config 生成本机角色的 YAML 到 $DATA_ROOT/config/，start 检测到
#     该文件就改用 `metad --config=...` 启动——长参数列表消失，密钥也不出现在 ps(1)。
#     改 cluster.env 后需重跑 gen-config（或直接编辑 YAML）；NUFS_NO_CONFIG=1 强制走 flag。
#
# 部署顺序:
#   1) 每台 VM:  ./deploy.sh install
#   2) metad-1 所在 VM: ./deploy.sh gen-config && ./deploy.sh start   # 先起 owner，选举确定
#   3) 其余 VM:  ./deploy.sh gen-config && ./deploy.sh start
#   4) 任意一台: ./deploy.sh verify
#
# 环境变量可覆盖默认值：
#   NUFS_LOG_LEVEL=debug   NUFS_CAPACITY_GB=2000   NUFS_DATA_DIRS=/d1,/d2（JBOD）
#   NUFS_CORE=<源码路径>（install 编译用） NUFS_BIN=<二进制目录> NUFS_ENV_FILE=<env 路径>
#
# 关于 raft 成员加入（为什么 2/3 号不用单独"加入"）：
#   owner（meta-1）用 --raft-bootstrap-owner 自举成只有自己的 1-voter 集群并当选；
#   2/3 号带着同样的 --raft-bootstrap=true --raft-bootstrap-owner=meta-1 启动时，
#   因为 node-id 不是 owner，会跳过自举、以空配置从 follower 起。leader 启动后
#   对 --raft-bootstrap-peers 里每个还没入群的对端跑 AddVoter 拉入（reconcile
#   循环：启动即拉 + 每次当选 + 每 5s 兜底）。所以三台必须带同一份 peers 列表，
#   而 2/3 号只需要"开着、能被 leader 路由到"即可，不需要任何手工 join 命令。

set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${NUFS_ENV_FILE:-$DIR/cluster.env}"
CMD="${1:-help}"
# gen-keys/help 不需要 cluster.env（gen-keys 是第一步，此时还没建 env）
NEED_ENV=1
case "$CMD" in gen-keys|help|--help|-h) NEED_ENV=0 ;; esac
if [ "$NEED_ENV" = 1 ]; then
  [ -f "$ENV_FILE" ] || { echo "缺少 ${ENV_FILE}（先 cp cluster.env.example cluster.env 并填写）" >&2; exit 1; }
  # shellcheck source=cluster.env
  source "$ENV_FILE"
  LOG_DIR="$DATA_ROOT/log"
  # 网关/datanode 的 metad 访问地址。默认指向 metad-1（与 runbook 一致）；
  # 若要 metad 单机故障时网关不中断，设 METAD_SERVICE_ADDR=<VIP 或代理>:port，
  # 让 3 台 metad 挂在稳定地址后面（keepalived VIP / 反向代理 / DNS 轮询到存活节点）。
  METAD_ADDR="${METAD_SERVICE_ADDR:-${METAD_IPS[0]}:$METAD_OPS_PORT}"
  CONFIG_DIR="$DATA_ROOT/config"
fi

BIN="${NUFS_BIN:-/usr/local/bin}"
RUN_DIR="${NUFS_RUN_DIR:-/var/run/nufs}"

log() { printf '\033[1m[deploy]\033[0m %s\n' "$*"; }
die() { echo "FATAL: $*" >&2; exit 1; }

# ---------- 本机角色识别 ----------
local_ips() {
  local ips=""
  if command -v hostname >/dev/null 2>&1 && hostname -I >/dev/null 2>&1; then
    ips="$(hostname -I 2>/dev/null)"
  fi
  if [ -z "$ips" ] && command -v ipconfig >/dev/null 2>&1; then
    ips="$(ipconfig getifaddr en0 2>/dev/null || true) $(ipconfig getifaddr en1 2>/dev/null || true)"
  fi
  echo "127.0.0.1 $ips"
}
LOCAL_IPS="$(local_ips)"

is_local() { for ip in $LOCAL_IPS; do [ "$ip" = "$1" ] && return 0; done; return 1; }

detect_roles() {
  local roles=""
  local i
  for i in "${!METAD_IPS[@]}"; do
    is_local "${METAD_IPS[$i]}" && roles="$roles metad:$((i+1))"
  done
  for i in "${!DATANODE_IPS[@]}"; do
    is_local "${DATANODE_IPS[$i]}" && roles="$roles datanode:$((i+1))"
  done
  is_local "$S3_IP" && roles="$roles s3:"
  echo "$roles"
}

# ---------- 集群派生参数 ----------
metad_peers() {   # meta-1=ip:port,meta-2=...,meta-3=...
  local out=""
  for i in "${!METAD_IPS[@]}"; do
    [ -n "$out" ] && out="$out,"
    out="${out}meta-$((i+1))=${METAD_IPS[$i]}:$((METAD_RAFT_PORT+i))"
  done
  echo "$out"
}
metad_peer_ops() { # meta-1=http://ip:port,...
  local out=""
  for i in "${!METAD_IPS[@]}"; do
    [ -n "$out" ] && out="$out,"
    out="${out}meta-$((i+1))=http://${METAD_IPS[$i]}:$((METAD_OPS_PORT+i))"
  done
  echo "$out"
}

# ---------- 启动命令生成（start 与 dump 共用） ----------
CMD_ARGS=()
metad_args() {
  local n=$1 ip=$2
  local raft_port=$((METAD_RAFT_PORT+n-1)) ops_port=$((METAD_OPS_PORT+n-1))
  CMD_ARGS=("$BIN/metad" --node-id="$n"
    --data-dir="$DATA_ROOT/meta$n" --raft-dir="$DATA_ROOT/raft$n"
    --ops-addr="$ip:$ops_port" --advertise-ops-addr="http://$ip:$ops_port"
    --raft=true --raft-addr="0.0.0.0:$raft_port" --raft-advertise-addr="$ip:$raft_port"
    --raft-bootstrap=true --raft-bootstrap-owner=meta-1
    --raft-bootstrap-peers="$(metad_peers)" --raft-peer-ops="$(metad_peer_ops)"
    --auth-token="$AUTH_TOKEN" --token-signing-key="$TOKEN_SIGNING_KEY"
    --credential-secret-key="$CRED_SECRET_KEY"
    --log-level="${NUFS_LOG_LEVEL:-info}" --log-json=true --log-file="$LOG_DIR/metad$n.log")
}
datanode_args() {
  local n=$1 ip=$2
  local chunk_port=$((DATANODE_CHUNK_PORT+n-1)) ops_port=$((DATANODE_OPS_PORT+n-1))
  local data_arg=(--data-dir="$DATA_ROOT/datanode$n")
  [ -n "${NUFS_DATA_DIRS:-}" ] && data_arg=(--data-dirs="$NUFS_DATA_DIRS")
  CMD_ARGS=("$BIN/datanode" --node-id="$n"
    --listen="0.0.0.0:$chunk_port" --register-addr="$ip:$chunk_port"
    --ops-addr="0.0.0.0:$ops_port" "${data_arg[@]}"
    --metadata="$METAD_ADDR" --metadata-auth-token="$AUTH_TOKEN" --ops-auth-token="$AUTH_TOKEN"
    --rack="rack-$n" --zone="${NUFS_ZONE:-zone-1}" --capacity="${NUFS_CAPACITY_GB:-1000}"
    --log-level="${NUFS_LOG_LEVEL:-info}" --log-json=true)
}
s3_args() {
  CMD_ARGS=("$BIN/nufs-s3" --listen=":$S3_PORT"
    --meta-addr="$METAD_ADDR" --meta-auth-token="$AUTH_TOKEN"
    --part-dir="$DATA_ROOT/s3-parts"
    --log-level="${NUFS_LOG_LEVEL:-info}" --log-json=true)
}

# ---------- 进程管理（后台进程 + pidfile） ----------
proc_alive() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }

# ---------- 配置生成（gen-config）与启动方式选择 ----------
# 每个角色两种启动方式：
#   config 方式：$CONFIG_DIR/<name>.yaml 存在 → 用 `--config=<file>` 启动（参数进 YAML）
#   flag 方式（默认/原始）：直接拼长参数列表，参数实时取自 cluster.env
# NUFS_NO_CONFIG=1 强制 flag 方式，即使 YAML 存在。
MODE=flags
run_cmd() { # kind idx
  local kind=$1 idx=$2
  local name="${kind}${idx:-}"
  local cf="$CONFIG_DIR/$name.yaml"
  local binname=metad; case "$kind" in datanode) binname=datanode;; s3) binname=nufs-s3;; esac
  if [ "${NUFS_NO_CONFIG:-}" != 1 ] && [ -f "$cf" ]; then
    CMD_ARGS=("$BIN/$binname" --config="$cf")
    MODE=config
  else
    case "$kind" in
      metad)    metad_args "$2" "${METAD_IPS[$(( $2 - 1 ))]}" ;;
      datanode) datanode_args "$2" "${DATANODE_IPS[$(( $2 - 1 ))]}" ;;
      s3)       s3_args ;;
    esac
    MODE=flags
  fi
}

emit_metad_yaml() { # n ip -> stdout
  local n=$1 ip=$2
  local raft_port=$((METAD_RAFT_PORT+n-1)) ops_port=$((METAD_OPS_PORT+n-1))
  cat <<EOF
config_version: 1
node_id: $n
data_dir: "$DATA_ROOT/meta$n"
ops_addr: "$ip:$ops_port"
advertise_ops_addr: "http://$ip:$ops_port"
raft:
  enabled: true
  listen: "0.0.0.0:$raft_port"
  data_dir: "$DATA_ROOT/raft$n"
  bootstrap: true
raft_bootstrap_owner: "meta-1"
raft_bootstrap_peers: "$(metad_peers)"
raft_peer_ops: "$(metad_peer_ops)"
raft_advertise_addr: "$ip:$raft_port"
auth_token: "$AUTH_TOKEN"
token_signing_key: "$TOKEN_SIGNING_KEY"
credential_secret_key: "$CRED_SECRET_KEY"
log_level: "${NUFS_LOG_LEVEL:-info}"
log_json: true
log_file: "$LOG_DIR/metad$n.log"
EOF
}

emit_datanode_yaml() { # n ip -> stdout
  local n=$1 ip=$2
  local chunk_port=$((DATANODE_CHUNK_PORT+n-1)) ops_port=$((DATANODE_OPS_PORT+n-1))
  local data_key=data_dir data_val="$DATA_ROOT/datanode$n"
  [ -n "${NUFS_DATA_DIRS:-}" ] && { data_key=data_dirs; data_val="$NUFS_DATA_DIRS"; }
  cat <<EOF
config_version: 1
node_id: $n
listen: "0.0.0.0:$chunk_port"
register_addr: "$ip:$chunk_port"
ops_addr: "0.0.0.0:$ops_port"
$data_key: "$data_val"
metadata: "$METAD_ADDR"
metadata_auth_token: "$AUTH_TOKEN"
ops_auth_token: "$AUTH_TOKEN"
rack: "rack-$n"
zone: "${NUFS_ZONE:-zone-1}"
capacity: ${NUFS_CAPACITY_GB:-1000}
log_level: "${NUFS_LOG_LEVEL:-info}"
log_json: true
EOF
}

emit_s3_yaml() { # -> stdout
  cat <<EOF
config_version: 1
listen: ":$S3_PORT"
meta_addr: "$METAD_ADDR"
meta_auth_token: "$AUTH_TOKEN"
part_dir: "$DATA_ROOT/s3-parts"
log_level: "${NUFS_LOG_LEVEL:-info}"
log_json: true
EOF
}

start_role() {
  local kind=$1 idx=$2
  local name="${kind}${idx:-}"
  local pidfile="$RUN_DIR/${name}.pid"
  run_cmd "$kind" "$idx"
  proc_alive "$pidfile" && { log "$name 已在运行 (pid $(cat "$pidfile"))"; return 0; }
  mkdir -p "$DATA_ROOT" "$RUN_DIR" "$LOG_DIR"
  nohup "${CMD_ARGS[@]}" >>"$LOG_DIR/$name.log" 2>&1 &
  echo $! > "$pidfile"
  log "$name 已启动 (pid $!, $MODE, 日志 $LOG_DIR/$name.log)"
}

stop_role() {
  local kind=$1 idx=$2
  local name="${kind}${idx:-}"
  local pidfile="$RUN_DIR/${name}.pid"
  if proc_alive "$pidfile"; then
    local pid; pid="$(cat "$pidfile")"
    kill "$pid" 2>/dev/null || true
    # 等进程真正退出再返回：Pebble 关库需要时间，否则立刻 down && start 会撞 DB 锁
    local waited=0
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 15 ]; do sleep 1; waited=$((waited+1)); done
    if kill -0 "$pid" 2>/dev/null; then
      log "$name 优雅停机超时，强制 SIGKILL"
      kill -9 "$pid" 2>/dev/null || true
    else
      log "$name 已停止"
    fi
    rm -f "$pidfile"
  else
    rm -f "$pidfile"
    log "$name 未在运行"
  fi
}

# ---------- systemd ----------
gen_unit() { # kind idx -> 写 /etc/systemd/system/nufs-<name>.service
  local kind=$1 idx=$2
  local name="${kind}${idx:-}"
  run_cmd "$kind" "$idx"
  {
    echo "[Unit]"
    echo "Description=NUFS $name"
    echo "After=network.target"
    echo
    echo "[Service]"
    echo "Type=simple"
    echo "ExecStart=${CMD_ARGS[*]}"
    echo "Restart=always"
    echo "RestartSec=5"
    echo "LimitNOFILE=65536"
    echo
    echo "[Install]"
    echo "WantedBy=multi-user.target"
  } > "/etc/systemd/system/nufs-$name.service"
  chmod 600 "/etc/systemd/system/nufs-$name.service"   # 含密钥，收紧权限
}

# ---------- 校验 ----------
# metad 的 JSON 是 Go 缩进输出（冒号后有空格），grep 需兼容 "k": v 与 "k":v
json_bool() { grep -oE '"is_leader"[[:space:]]*:[[:space:]]*(true|false)' <<<"$1" | tail -1 | grep -oE '(true|false)$'; }
json_uri()  { grep -oE '"leader_uri"[[:space:]]*:[[:space:]]*"[^"]*"' <<<"$1" | tail -1 | sed -E 's/^.*"([^"]*)"$/\1/'; }

cmd_verify() {
  log "=== 1/3 校验 metad raft quorum ==="
  local leaders=0 leader_idx=-1 uri="" u body l i ip port
  for i in "${!METAD_IPS[@]}"; do
    ip="${METAD_IPS[$i]}"; port=$((METAD_OPS_PORT+i))
    if body="$(curl -fsS -H "Authorization: Bearer $AUTH_TOKEN" "http://$ip:$port/api/v1/cluster/status" 2>/dev/null)"; then
      l="$(json_bool "$body")"; u="$(json_uri "$body")"
    else
      l=false; u=""
    fi
    if [ "$l" = true ]; then leaders=$((leaders+1)); leader_idx=$i; fi
    [ -n "$u" ] && [ -z "$uri" ] && uri="$u"
    printf '  meta-%d  is_leader=%-5s leader_uri=%s\n' "$((i+1))" "$l" "$u"
    [ -n "$u" ] && [ "$u" != "$uri" ] && die "meta-$((i+1)) 与 meta-1 看到的 leader 不一致: $u vs $uri"
  done
  [ "$leaders" -ge 1 ] || die "没有任何 metad 是 leader（raft 未收敛，先确认 metad-1 所在 VM 已 start）"
  [ "$leaders" -le 1 ] || die "有 $leaders 个节点同时宣称是 leader（raft 分裂）"
  log "  quorum OK：1 个 leader=${uri}，所有节点看到同一 leader"

  log "=== 2/3 校验 leader readiness ==="
  local lip="${METAD_IPS[$leader_idx]}" lport=$((METAD_OPS_PORT+leader_idx))
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $AUTH_TOKEN" "http://$lip:$lport/api/v1/cluster/readiness")"
  [ "$code" = 200 ] || die "leader readiness 未通过（HTTP ${code}，期望 200）"
  log "  leader ($lip:$lport) readiness 200 OK"

  log "=== 3/3 校验 datanode 注册 ==="
  local nodes n
  nodes="$(curl -fsS -H "Authorization: Bearer $AUTH_TOKEN" "http://$lip:$lport/api/v1/nodes")" || { nodes=""; }
  n="$(grep -o '"id":' <<<"$nodes" | wc -l | tr -d ' ')"
  log "  已注册 datanode: ${n}（期望 ${#DATANODE_IPS[@]}）"
  [ "$n" -ge "${#DATANODE_IPS[@]}" ] || die "datanode 注册数不足"
  log "=== verify 通过：集群就绪 ==="
}

# ---------- 子命令 ----------
cmd_gen_keys() {
  cat <<EOF
粘贴到 cluster.env：
AUTH_TOKEN=$(openssl rand -hex 16)
TOKEN_SIGNING_KEY=$(openssl rand -hex 16)
CRED_SECRET_KEY=$(openssl rand -hex 32)
EOF
}

cmd_install() {
  local repo="${NUFS_CORE:-$DIR/../../}"
  (cd "$repo" && go build -o bin/metad ./cmd/metad \
    && go build -o bin/datanode ./cmd/datanode \
    && go build -o bin/nufs-s3 ./cmd/nufs-s3)
  for b in metad datanode nufs-s3; do
    install -m 0755 "$repo/bin/$b" "$BIN/$b" || die "install $b 失败（需要 root？）"
  done
  log "已安装 $BIN/{metad,datanode,nufs-s3}"
}

cmd_gen_config() {
  local roles; roles="$(detect_roles)"
  [ -z "$roles" ] && die "本机 IP 不在 cluster.env 任何角色清单里。本机 IP: $LOCAL_IPS"
  mkdir -p "$CONFIG_DIR"
  local kind idx name
  for r in $roles; do
    kind="${r%%:*}"; idx="${r##*:}"
    name="${kind}${idx:-}"
    case "$kind" in
      metad)    emit_metad_yaml "$idx" "${METAD_IPS[$((idx-1))]}" > "$CONFIG_DIR/$name.yaml" ;;
      datanode) emit_datanode_yaml "$idx" "${DATANODE_IPS[$((idx-1))]}" > "$CONFIG_DIR/$name.yaml" ;;
      s3)       emit_s3_yaml > "$CONFIG_DIR/$name.yaml" ;;
    esac
    chmod 600 "$CONFIG_DIR/$name.yaml"
    log "已生成 $CONFIG_DIR/$name.yaml（含密钥，权限 600）"
  done
  log "注意：start 会优先用这些 YAML；改 cluster.env 后需重跑 gen-config，或直接编辑 YAML"
}

cmd_start() {
  local roles; roles="$(detect_roles)"
  [ -z "$roles" ] && die "本机 IP 不在 cluster.env 任何角色清单里。本机 IP: $LOCAL_IPS"
  log "本机角色:$roles"
  # metad 先起（owner 机先跑 start 可保证选举确定），再 datanode/s3
  local kind idx
  for r in $roles; do
    kind="${r%%:*}"; idx="${r##*:}"
    [ "$kind" = metad ] && start_role "$kind" "$idx"
  done
  for r in $roles; do
    kind="${r%%:*}"; idx="${r##*:}"
    case "$kind" in datanode|s3) start_role "$kind" "$idx" ;; esac
  done
}

cmd_status() {
  local roles; roles="$(detect_roles)"
  [ -z "$roles" ] && { log "本机无角色（IP 不在清单里: ${LOCAL_IPS}）"; return 0; }
  local kind idx name pidfile
  for r in $roles; do
    kind="${r%%:*}"; idx="${r##*:}"
    name="${kind}${idx:-}"; pidfile="$RUN_DIR/${name}.pid"
    if proc_alive "$pidfile"; then
      printf '  %-10s running (pid %s)\n' "$name" "$(cat "$pidfile")"
    else
      printf '  %-10s not running\n' "$name"
    fi
  done
}

cmd_down() {
  local roles; roles="$(detect_roles)"
  local kind idx
  # 反序停：s3/datanode 先，metad 后
  for r in $roles; do
    kind="${r%%:*}"; idx="${r##*:}"
    case "$kind" in datanode|s3) stop_role "$kind" "$idx" ;; esac
  done
  for r in $roles; do
    kind="${r%%:*}"; idx="${r##*:}"
    [ "$kind" = metad ] && stop_role "$kind" "$idx"
  done
}

cmd_dump() {
  local roles; roles="$(detect_roles)"
  [ -z "$roles" ] && die "本机 IP 不在清单里"
  local kind idx
  for r in $roles; do
    kind="${r%%:*}"; idx="${r##*:}"
    run_cmd "$kind" "$idx"
    printf '%s\n' "--- ${kind}${idx:-} ---"
    printf '  %q\n' "${CMD_ARGS[@]}" | sed 's/^  //'
  done
}

cmd_seed_cred() {
  local ak="${1:-TESTAK000000000000}" sk="${2:-test-secret-value}"
  curl -fsS -X PUT "http://$METAD_ADDR/api/v1/auth/creds/$ak" \
    -H "Authorization: Bearer $AUTH_TOKEN" -H "content-type: application/json" \
    -d "{\"secret_key\":\"$sk\",\"principal\":\"deploy\"}" >/dev/null \
    && log "已注册凭据 $ak -> http://$METAD_ADDR"
}

cmd_install_systemd() {
  [ "$(id -u)" = 0 ] || die "install-systemd 需要 root"
  local roles; roles="$(detect_roles)"
  [ -z "$roles" ] && die "本机 IP 不在清单里"
  local kind idx name
  for r in $roles; do
    kind="${r%%:*}"; idx="${r##*:}"
    name="${kind}${idx:-}"
    gen_unit "$kind" "$idx"
    systemctl daemon-reload
    systemctl enable --now "nufs-$name" >/dev/null
    log "systemd 单元 nufs-$name 已启用"
  done
}

cmd_systemd() {
  local action=$1 role=${2:-}
  [ -n "$action" ] || die "用法: deploy.sh systemd start|stop|status [role]"
  local roles; roles="$(detect_roles)"
  [ -z "$roles" ] && die "本机 IP 不在清单里"
  local kind idx name
  for r in $roles; do
    kind="${r%%:*}"; idx="${r##*:}"
    name="${kind}${idx:-}"
    if [ -z "$role" ] || [ "$name" = "$role" ]; then
      systemctl "$action" "nufs-$name"
    fi
  done
}

usage() { sed -n '2,45p' "$0"; exit 0; }

case "${1:-help}" in
  gen-keys)        cmd_gen_keys ;;
  install)         cmd_install ;;
  gen-config)      cmd_gen_config ;;
  start)           cmd_start ;;
  status)          cmd_status ;;
  verify)          cmd_verify ;;
  down)            cmd_down ;;
  dump)            cmd_dump ;;
  seed-cred)       shift; cmd_seed_cred "$@" ;;
  install-systemd) cmd_install_systemd ;;
  systemd)         shift; cmd_systemd "$@" ;;
  help|--help|-h)  usage ;;
  *) echo "未知命令: $1"; usage ;;
esac
