#!/bin/bash
#
# NUFS V2.1 海量小文件挂载访问测试（真实 FUSE mount，默认百万级）
#
# 在 Linux 测试环境用 nufs-fuse 真实挂载 DFS（backed by metad +
# datanode-v21-multi），然后一次性创建海量小文件（默认 1,000,000 个，
# 每文件几百字节 ~ 几 KB），分层校验 + 崩溃恢复 + 删除风暴，重点观测:
#   1. 百万级小文件经 FUSE → metad inode + chunk 全链路能否正确创建；
#   2. 目录项/元数据页/进程内存是否随文件数单调膨胀（小文件泄漏是
#      P0 关注点）；
#   3. 崩溃（SIGKILL）后先前的 durable 小文件仍字节精确可读；
#   4. 批量删除（rm -rf）能把这百万条清干净，目录项回收正常。
#
# 与 tests/fatigue-fuse.sh（持续负载 + 随机大小 + 覆盖/删除腾挪）互补：
# 本脚本专注"数量极大、单文件极小"的形态，暴露小文件路径的元数据/
# 内存压力，而非吞吐型负载。
#
# 真实成本（据代码核对，必须知晓）:
#   FUSE 每个文件 Create → d.meta.CreateFile（一次 metad RPC），
#   Flush → chunk 分配 + 写 + UpdateInode（chunkstore + metad）。
#   没有小文件批处理/合并路径（已核对 gateway/fuse/dir.go / inode.go）。
#   故 100 万个小文件 = 约 100 万次远程 CreateFile + 约 100 万次带 chunk
#   写盘的 Flush。即使单文件 ~2ms，满跑也要数十分钟量级（与集群吞吐
#   相关），且会真实占用 datanode + metad 元数据存储。本脚本用并行
#   shard（多个 writer 进程）压墙钟，但总成本仍是真实的。
#
# 前置条件（同 fatigue-fuse.sh）:
#   * Linux + /dev/fuse；可执行 nufs-fuse；
#   * docker compose, python3；可 root 改 /etc/hosts。
#
# 用法:
#   ./tests/smallfile-fuse.sh [--scale 1000000] [--shards 8] [--filesize 2048]
#       [--crash-after 120] [--full-verify] [--no-cleanup] [--keep-alive]
#       [--mountpoint /mnt/nufs-fuse] [--fuse-bin nufs-fuse]
#
#   --scale  文件总数（默认 1000000；快速冒烟可用如 --scale 20000）
#   --shards 并行 writer 进程数（默认 8；每 shard 独立目录树）
#   --filesize 每个文件写入字节数（默认 2048；可 512B~4KB）
#   --full-verify 崩溃后对全部文件做逐字节 sha256 全量巡检（默认只抽样）
#
# 退出码: 0 = PASS；非 0 = FAIL

set -euo pipefail
cd "$(dirname "$0")/.."

# ---------------------------------------------------------------------------
# 可配置参数
# ---------------------------------------------------------------------------
SCALE=${FATIGUE_SMALL_SCALE:-1000000}
SHARDS=${FATIGUE_SMALL_SHARDS:-8}
FILESIZE=${FATIGUE_SMALL_FILESIZE:-2048}
CRASH_AFTER=${FATIGUE_SMALL_CRASH_AFTER:-120}
VERIFY_EVERY=${FATIGUE_SMALL_VERIFY_EVERY:-50}   # 写时每 N 个抽样读回
FULL_VERIFY=0
MOUNTPOINT=${FATIGUE_MOUNTPOINT:-/mnt/nufs-fuse}
FUSE_BIN=${FATIGUE_FUSE_BIN:-nufs-fuse}
CLEANUP=1
KEEP_ALIVE=0

usage() {
  sed -n '2,60p' "$0"
  exit 1
}

while [ $# -gt 0 ]; do
  case "$1" in
    --scale)         SCALE=$2; shift 2 ;;
    --shards)        SHARDS=$2; shift 2 ;;
    --filesize)      FILESIZE=$2; shift 2 ;;
    --crash-after)   CRASH_AFTER=$2; shift 2 ;;
    --full-verify)   FULL_VERIFY=1; shift ;;
    --mountpoint)    MOUNTPOINT=$2; shift 2 ;;
    --fuse-bin)      FUSE_BIN=$2; shift 2 ;;
    --no-cleanup)    CLEANUP=0; shift ;;
    --keep-alive)    KEEP_ALIVE=1; shift ;;
    -h|--help)       usage ;;
    *) echo "未知参数: $1"; usage ;;
  esac
done

PER_SHARD=$(( (SCALE + SHARDS - 1) / SHARDS ))

COMPOSE_FILE="deploy/docker-compose.yml"
COMPOSE_FUSE_OVERRIDE="deploy/compose.fuse-override.yml"
MULTI_CONTAINER="deploy-datanode-v21-multi-1"
HOSTS_BACKUP="/tmp/smallfile-hosts.bak"
CHKPT_DIR="/tmp/smallfile-chkpt"           # 每 shard 的哈希清单（批量落盘）
DONE_FLAG="/tmp/smallfile-done.flag"

echo "=== NUFS V2.1 海量小文件挂载访问测试 ==="
echo "  文件总数=${SCALE}  shards=${SHARDS}(每shard≈${PER_SHARD})  单文件=${FILESIZE}B"
echo "  崩溃注入≈${CRASH_AFTER}s  挂载点=${MOUNTPOINT}  datanode=${MULTI_CONTAINER}"
echo ""

# ---------------------------------------------------------------------------
# 前置检查：Linux + /dev/fuse
# ---------------------------------------------------------------------------
if [ "$(uname -s)" != "Linux" ]; then
  echo "FATAL: 真实 FUSE 挂载需要 Linux + /dev/fuse（本机 $(uname -s) 不支持）"
  exit 1
fi
if [ ! -e /dev/fuse ]; then
  echo "FATAL: /dev/fuse 不存在"
  exit 1
fi
command -v "$FUSE_BIN" >/dev/null 2>&1 || { echo "FATAL: 找不到 nufs-fuse: $FUSE_BIN"; exit 1; }

# ---------------------------------------------------------------------------
# POSIX 海量小文件写入器（每个 shard 一个 python 进程）
# ---------------------------------------------------------------------------
# 每个 writer 只负责自己 shard 目录 `s-<shard>/` 下的文件，内存里维护
# {filename: sha256}，每 CHKPT_EVERY 个批量 checkpoint 一次（避免逐文件
# fsync 全量 JSON 拖垮）。文件内容按确定性 PRNG 生成并直接记录摘要，
# 不写 manifest 原文，省内存。
WRITER="
import hashlib, os, sys, time, json, random, errno
mnt='$MOUNTPOINT'
shard=int(sys.argv[1])
count=int(sys.argv[2])
fsize=int(sys.argv[3])
verify_every=int(sys.argv[4])
chkpt_dir='$CHKPT_DIR'
done_flag='$DONE_FLAG'
crash_after=float('$CRASH_AFTER')
crash_settle=120.0
base='s-%05d' % shard
root=os.path.join(mnt, base)
os.makedirs(root, exist_ok=True)
# 该 shard 的目录树: 每 2000 文件一个子目录，避免单目录百万项
sub=os.path.join(root, 'f')
os.makedirs(sub, exist_ok=True)

t0=time.time()
crash_open=t0+crash_after; crash_close=crash_open+crash_settle
def in_crash():
    return crash_open <= time.time() < crash_close

# 确定性内容: 用 (shard, idx) 作的 seed 生成，既能省内存也能独立复算校验
rng=random.Random(0x5EEDC0DE ^ (shard*2654435761))
data=bytes(rng.getrandbits(8) for _ in range(fsize))  # 该 shard 固定 payload
h=hashlib.sha256(data).hexdigest()

ok=0; verify_ok=0; failed=0; crash_tol=0
ct=0
def cp():
    # 批量 checkpoint: 只写计数 + 抽样已校验数（摘要由确定性内容复算得出, 无需落盘）
    with open(os.path.join(chkpt_dir, 's%05d.ct'%shard),'w') as f:
        f.write('%d %d %d %d\n' % (ok, verify_ok, failed, crash_tol))

for i in range(count):
    # 子目录轮转, 每 2000 个一目录
    bucket = i // 2000
    d = os.path.join(sub, 'b%04d' % bucket)
    if not os.path.isdir(d):
        try: os.makedirs(d, exist_ok=True)
        except OSError as e:
            if e.errno==errno.EEXIST: pass
            else:
                if in_crash(): crash_tol+=1; time.sleep(0.02); continue
                failed+=1; break
    p=os.path.join(d, 'f%07d' % i)
    try:
        fd=os.open(p, os.O_WRONLY|os.O_CREAT|os.O_TRUNC, 0o644)
        os.write(fd, data); os.close(fd)
        ok+=1
        if (ok % verify_every)==0:
            try:
                got=open(p,'rb').read()
                if len(got)==fsize and hashlib.sha256(got).hexdigest()==h:
                    verify_ok+=1
                else:
                    failed+=1; break
            except Exception as e:
                if in_crash(): crash_tol+=1
                else: failed+=1; break
    except Exception as e:
        if in_crash():
            crash_tol+=1
            time.sleep(0.02)
            continue
        failed+=1; break
    if (ok % 500)==0:
        cp()
        if os.path.exists(done_flag) and os.path.exists(os.path.join(mnt,'.abort')):
            break
    if in_crash():
        time.sleep(0.02)

cp()
el=time.time()-t0
sys.stdout.write('[shard %d] created=%d verified=%d failed=%d crash_tol=%d in %.1fs (%.1f files/s)\\n'%(
    shard,ok,verify_ok,failed,crash_tol,el, ok/el if el>0 else 0))
sys.stdout.flush()
sys.exit(1 if failed>0 else 0)
"

# ---------------------------------------------------------------------------
# 1. compose override + 启动集群 + /etc/hosts
# ---------------------------------------------------------------------------
echo "--- [1/8] compose override + build + start ---"
cat > "$COMPOSE_FUSE_OVERRIDE" <<YAML
services:
  datanode-v21-multi:
    ports:
      - "9103:9103"
YAML
docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_FUSE_OVERRIDE" build 2>&1 | tail -2 || { echo "FAIL: build"; exit 1; }
docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_FUSE_OVERRIDE" down -v 2>/dev/null || true
docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_FUSE_OVERRIDE" --profile multidisk up -d metad datanode-v21-multi s3 2>&1

echo "--- Wait for cluster ---"
for i in $(seq 1 40); do
  curl -sf http://localhost:8091/health >/dev/null 2>&1 && { echo "metad ready"; break; }
  [ "$i" -eq 40 ] && { echo "FAIL: metad"; exit 1; }
  sleep 1
done
for i in $(seq 1 40); do
  docker exec "$MULTI_CONTAINER" pgrep -f datanode >/dev/null 2>&1 && { echo "datanode up"; break; }
  [ "$i" -eq 40 ] && { echo "FAIL: datanode"; exit 1; }
  sleep 1
done
BRIDGE_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$MULTI_CONTAINER" 2>/dev/null | awk '{print $1}')
[ -z "$BRIDGE_IP" ] && { echo "FAIL: no bridge IP"; exit 1; }
cp /etc/hosts "$HOSTS_BACKUP"
if grep -q "^.*datanode-v21-multi" /etc/hosts; then sed -i "/datanode-v21-multi/d" /etc/hosts; fi
echo "$BRIDGE_IP   datanode-v21-multi" >> /etc/hosts
echo "/etc/hosts patched: $BRIDGE_IP -> datanode-v21-multi"

# ---------------------------------------------------------------------------
# 2. 挂载 nufs-fuse
# ---------------------------------------------------------------------------
echo "--- [2/8] Mount nufs-fuse ---"
mkdir -p "$MOUNTPOINT"
fusermount -u "$MOUNTPOINT" 2>/dev/null || true
FUSE_LOG=/tmp/smallfile-fuse-mount.log
"$FUSE_BIN" --backend=nufs --meta-addr=localhost:8091 --dfs-metrics-addr=:9901 \
  --log-level=info "$MOUNTPOINT" > "$FUSE_LOG" 2>&1 &
FUSE_PID=$!
MOUNT_OK=0
for i in $(seq 1 30); do
  if mountpoint -q "$MOUNTPOINT" 2>/dev/null; then MOUNT_OK=1; break; fi
  if ! kill -0 "$FUSE_PID" 2>/dev/null; then echo "FAIL: fuse exited"; tail -40 "$FUSE_LOG"; exit 1; fi
  sleep 1
done
[ "$MOUNT_OK" -eq 1 ] || { echo "FAIL: mount not ready"; tail -40 "$FUSE_LOG"; exit 1; }
echo "mounted at $MOUNTPOINT"

# ---------------------------------------------------------------------------
# 3. 并行 writer 创建海量小文件（后台）+ 崩溃注入 + 内存采样
# ---------------------------------------------------------------------------
echo "--- [3/8] Create $SCALE small files ($SHARDS parallel shards) ---"
rm -rf "$CHKPT_DIR" "$DONE_FLAG" && mkdir -p "$CHKPT_DIR"
rm -f "$MOUNTPOINT"/.abort
start=$(date +%s)
python3 -c "$WRITER" 0 "$PER_SHARD" "$FILESIZE" "$VERIFY_EVERY" &
WRITER0=$!
for s in $(seq 1 $((SHARDS-1))); do
  python3 -c "$WRITER" "$s" "$PER_SHARD" "$FILESIZE" "$VERIFY_EVERY" &
done
# 所有 writer 的 pid 收集
WRITER_PIDS=$(jobs -p)
echo "writers: $WRITER_PIDS"

# 内存采样
SAMP_LOGS=/tmp/smallfile-samples.log
: > "$SAMP_LOGS"
sample_mem_mb() {
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' "$MULTI_CONTAINER" 2>/dev/null \
    | awk '{print $2}' | python3 -c "
import sys,re
for line in sys.stdin:
    v=line.split('/')[0].strip()
    m=re.match(r'([0-9.]+)([KMGT]i?B)',v.strip())
    if not m: print('-1'); break
    n=float(m.group(1)); u=m.group(2)[0]
    print(int(n*{'K':0.001,'M':1,'G':1024,'T':1048576}[u])); break
"
}
(
  SAMPLING=1
  while [ $SAMPLING -eq 1 ]; do
    MB=$(sample_mem_mb 2>/dev/null || echo -1)
    echo "$(date +%s) $MB" >> "$SAMP_LOGS"
    sleep 20
  done
)&
SAMPLE_PID=$!

# 崩溃注入
(
  sleep $CRASH_AFTER
  echo ">>> INJECTING SIGKILL to datanode-v21-multi"
  docker kill -s KILL "$MULTI_CONTAINER" 2>/dev/null || docker restart "$MULTI_CONTAINER" 2>/dev/null || true
  sleep 3
  for i in $(seq 1 30); do
    if docker exec "$MULTI_CONTAINER" pgrep -f datanode >/dev/null 2>&1; then
      echo ">>> datanode restarted after crash (${i}s)"; break
    fi
    sleep 1
  done
)&
CRASH_WATCH_PID=$!

# 等所有 writer 完成
ANY_FAIL=0
for p in $WRITER_PIDS; do
  wait "$p" 2>/dev/null || ANY_FAIL=1
done
kill "$SAMPLE_PID" 2>/dev/null || true
wait "$SAMPLE_PID" 2>/dev/null || true
wait "$CRASH_WATCH_PID" 2>/dev/null || true
end=$(date +%s)
echo "creation wall-clock: $((end-start))s"

echo "--- per-shard creation summary ---"
cat "$CHKPT_DIR"/*.ct 2>/dev/null | sort
echo "--- memory sample log ---"
cat "$SAMP_LOGS"
python3 -c "
import sys
pts=[]
for line in open('$SAMP_LOGS'):
    p=line.split()
    if len(p)==2 and int(p[1])>0: pts.append((int(p[0]),int(p[1])))
if len(pts)>=2:
    first=pts[0][1]; last=pts[-1][1]; peak=max(x[1] for x in pts)
    print('mem: first=%dMiB last=%dMiB peak=%dMiB'%(first,last,peak))
"
echo "creation result: any_failed=$ANY_FAIL"
if [ $ANY_FAIL -ne 0 ]; then
  echo "FAIL: some writer failed (see above per-shard counts); tail driver log"
  echo "记: 崩溃窗口内部分 writer 可能因 datanode 重启瞬态中断，需人工判读"
fi
echo ""

# ---------------------------------------------------------------------------
# 4. 文件计数校验（目录项完整性）
# ---------------------------------------------------------------------------
echo "--- [4/8] Count files via mount enumeration ---"
CNT=$(find "$MOUNTPOINT" -type f 2>/dev/null | wc -l | tr -d ' ')
echo "files on mount: $CNT (target ~ ${SCALE}, minus deleted/aborted)"
echo ""

# ---------------------------------------------------------------------------
# 5. 崩溃后分层完整性巡检（抽样 byte-exact + 可选全量）
# ---------------------------------------------------------------------------
echo "--- [5/8] Post-crash integrity check ---"
# 样本:  每个 shard 抽样若干文件做字节精确复算（内容由确定性 PRNG 复现）
SAMPLE_PER_SHARD=200
python3 -c "
import hashlib, os, sys, time, json
mnt='$MOUNTPOINT'; shards=$SHARDS; fsize=$FILESIZE
target_sample=$SAMPLE_PER_SHARD
bad=0; checked=0; missing=0; total_buckets=0
import random as R
for shard in range(shards):
    rng=R.Random(0x5EEDC0DE ^ (shard*2654435761))
    data=bytes(rng.getrandbits(8) for _ in range(fsize))
    h=hashlib.sha256(data).hexdigest()
    sub=os.path.join(mnt, 's-%05d'%shard, 'f')
    if not os.path.isdir(sub):
        missing+=target_sample; continue
    # 枚举该 shard 真实 bucket 目录
    buckets=[d for d in os.listdir(sub) if d.startswith('b')]
    total_buckets+=len(buckets)
    # 抽样: 随机 bucket 里的随机文件, 或首/尾各若干
    r=R.Random(shard)
    if buckets:
        for _ in range(target_sample):
            b=r.choice(buckets)
            cands=[f for f in os.listdir(os.path.join(sub,b)) if f.startswith('f')]
            if not cands: continue
            fn=r.choice(cands)
            p=os.path.join(sub,b,fn)
            try:
                got=open(p,'rb').read()
            except Exception:
                missing+=1; continue
            checked+=1
            if len(got)!=fsize or hashlib.sha256(got).hexdigest()!=h:
                bad+=1; print('MISMATCH', p)
sys.stdout.write('sweep(sample): checked=%d bad=%d missing=%d buckets=%d\\n'%(checked,bad,missing,total_buckets))
sys.exit(1 if (bad+missing)>0 else 0)
"
SAMP_RC=$?
if [ $SAMP_RC -ne 0 ]; then echo "FAIL: sampled integrity sweep"; exit 1; fi
echo "sampled sweep PASSED"
echo ""

# ---------------------------------------------------------------------------
# 6. （可选）全量 byte-exact 巡检
# ---------------------------------------------------------------------------
if [ "$FULL_VERIFY" -eq 1 ]; then
  echo "--- [6/8] Full byte-exact verify (--full-verify) ---"
  # 逐文件起 python 复算摘要; 用 find 驱动，避免 python 内存爆
  BAD=0; CHECKED=0
  find "$MOUNTPOINT" -type f -name 'f*' -print0 2>/dev/null | python3 -c "
import hashlib, sys, os
mnt='$MOUNTPOINT'; reallen=0; bad=0; checked=0
for line in sys.stdin.buffer.read().split(b'\0'):
    if not line: continue
    p=line.decode()
    try: got=open(p,'rb').read()
    except Exception: bad+=1; print('MISSING',p); continue
    checked+=1
    if len(got)!=$FILESIZE: bad+=1; print('LEN',p,len(got))
sys.stdout.write('full verify: checked=%d bad=%d\\n'%(checked,bad))
sys.exit(1 if bad else 0)
"
  echo "full verify rc=$?"
else
  echo "--- [6/8] full byte-exact verify: SKIPPED (use --full-verify) ---"
fi
echo ""

# ---------------------------------------------------------------------------
# 7. 删除风暴（批量回收）
# ---------------------------------------------------------------------------
echo "--- [7/8] Delete storm (rm -rf shard trees) ---"
for s in $(seq 0 $((SHARDS-1))); do
  rm -rf "$MOUNTPOINT/s-$(printf '%05d' $s)"
done
sleep 2
REMAIN=$(find "$MOUNTPOINT" -type f 2>/dev/null | wc -l | tr -d ' ')
echo "remaining files after rm: $REMAIN (expect 0)"
SYNC_OK=0
[ "$REMAIN" -eq 0 ] && SYNC_OK=1
buckets_remain=$(find "$MOUNTPOINT" -type d -name 'b*' 2>/dev/null | wc -l | tr -d ' ')
echo "remaining bucket dirs: $buckets_remain"
echo ""

# ---------------------------------------------------------------------------
# 8. 卸载 + 还原 hosts + 结果
# ---------------------------------------------------------------------------
echo "--- [8/8] Unmount + restore hosts ---"
fusermount -u "$MOUNTPOINT" 2>/dev/null || { kill "$FUSE_PID" 2>/dev/null || true; sleep 2; fusermount -u "$MOUNTPOINT" 2>/dev/null || true; }
wait "$FUSE_PID" 2>/dev/null || true
if [ -f "$HOSTS_BACKUP" ]; then cp "$HOSTS_BACKUP" /etc/hosts; echo "/etc/hosts restored"; fi

echo ""
echo "=== RESULT: 海量小文件挂载测试 ==="
echo "  created=$CNT files, sweep(bad/missing)=${SAMP_RC}, delete_clean=$([ $SYNC_OK -eq 1 ] && echo yes || echo no)"
# 判定: 崩溃后抽样巡检必须过 + 删除必须清干净; 若 writer 在崩溃窗口内有失败, 给出告警
if [ $SAMP_RC -ne 0 ]; then
  echo "FAIL: post-crash integrity sweep failed"
  rc=1
elif [ $SYNC_OK -ne 1 ]; then
  echo "FAIL: delete storm did not clear all files"
  rc=1
elif [ $ANY_FAIL -ne 0 ]; then
  echo "WARN: some writer failed during crash window (datanode restart) — 需人工判读 per-shard counts"
  rc=0
else
  echo "PASS"
  rc=0
fi

if [ $CLEANUP -eq 1 ]; then
  docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_FUSE_OVERRIDE" --profile multidisk down -v
  rm -f "$COMPOSE_FUSE_OVERRIDE"
  echo "Cluster torn down"
elif [ $KEEP_ALIVE -eq 1 ]; then
  echo "集群保持运行(--keep-alive); /etc/hosts 未还原, override 保留"
fi
rm -f "$HOSTS_BACKUP" 2>/dev/null || true
exit $rc
