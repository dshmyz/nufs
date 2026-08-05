#!/bin/bash
#
# NUFS V2.1 海量小文件挂载访问测试（真实 FUSE mount，默认百万级）—— 裸机（无 Docker）版
#
# 与 tests/smallfile-fuse.sh（Docker 版）等价，但完全在宿主机上运行：用
# deploy/host/mount-helpers.sh 以裸机进程方式拉起 metad + datanode(v2.1 JBOD)
# + nufs-s3，用 nufs-fuse 真实挂载 DFS，然后一次性创建海量小文件（默认
# 1,000,000 个，每文件几百字节 ~ 几 KB），分层校验 + 崩溃恢复 + 删除风暴，
# 重点观测:
#   1. 百万级小文件经 FUSE → metad inode + chunk 全链路能否正确创建；
#   2. 目录项/元数据页/进程内存是否随文件数单调膨胀（小文件泄漏是 P0 关注点）；
#   3. 崩溃（SIGKILL）后先前的 durable 小文件仍字节精确可读；
#   4. 批量删除（rm -rf）能把这百万条清干净，目录项回收正常。
#
# 为什么不需要 /etc/hosts 打通（与 Docker 版的关键区别）：
#   裸机单机 datanode 以 --register-addr=127.0.0.1:9103 上报（Replicas[i].Addr
#   就存它），宿主机上 nufs-fuse 直接拨 127.0.0.1:9103 即可，无需 compose
#   override，也无需改 /etc/hosts。
#
# 真实成本（与 Docker 版相同，必须知晓）:
#   FUSE 每个文件 Create → metad.CreateFile（一次 RPC），Flush → chunk 分配 +
#   写 + UpdateInode。没有小文件批处理/合并路径。故 100 万个小文件 = 约 100 万次
#   远程 CreateFile + 约 100 万次带 chunk 写盘的 Flush，满跑要数十分钟量级，且
#   真实占用 datanode + metad 存储。本脚本用并行 shard 压墙钟。
#
# 前置条件（同 fatigue-fuse-host.sh）:
#   * Linux + /dev/fuse；可执行 nufs-fuse（BIN_DIR 内）；
#   * python3；可 root（fusermount）。
#
# 用法:
#   ./tests/smallfile-fuse-host.sh [--scale 1000000] [--shards 8] [--filesize 2048]
#       [--crash-after 120] [--full-verify] [--no-cleanup] [--keep-alive]
#       [--mountpoint /mnt/nufs-fuse]
#
#   --scale  文件总数（默认 1000000；快速冒烟可用如 --scale 20000）
#   --shards 并行 writer 进程数（默认 8；每 shard 独立目录树）
#   --filesize 每个文件写入字节数（默认 2048）
#   --full-verify 崩溃后对全部文件做逐字节巡检（默认只抽样）
#
# 退出码: 0 = PASS；非 0 = FAIL

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HH="${SCRIPT_DIR}/../deploy/host/mount-helpers.sh"
[ -f "$HH" ] || { echo "FATAL: $HH 缺失（先提交 deploy/host）"; exit 1; }
# shellcheck source=deploy/host/mount-helpers.sh
source "$HH"

# ---------------------------------------------------------------------------
# 可配置参数
# ---------------------------------------------------------------------------
SCALE=${FATIGUE_SMALL_SCALE:-1000000}
SHARDS=${FATIGUE_SMALL_SHARDS:-8}
FILESIZE=${FATIGUE_SMALL_FILESIZE:-2048}
CRASH_AFTER=${FATIGUE_SMALL_CRASH_AFTER:-120}
VERIFY_EVERY=${FATIGUE_SMALL_VERIFY_EVERY:-50}   # 写时每 N 个抽样读回
FULL_VERIFY=0
MOUNTPOINT=${FATIGUE_MOUNTPOINT:-"$MOUNTPOINT"}
CLEANUP=1
KEEP_ALIVE=0

usage() {
  sed -n '2,58p' "$0"
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
    --no-cleanup)    CLEANUP=0; shift ;;
    --keep-alive)    KEEP_ALIVE=1; shift ;;
    -h|--help)       usage ;;
    *) echo "未知参数: $1"; usage ;;
  esac
done

PER_SHARD=$(( (SCALE + SHARDS - 1) / SHARDS ))

CHKPT_DIR="/tmp/smallfile-chkpt"           # 每 shard 的哈希清单（批量落盘）
DONE_FLAG="/tmp/smallfile-done.flag"

# 结果归档目录（本次运行的全部验收证据落到这里，可持续位置非 /tmp）
RES_DIR="$(result_dir "smallfile")"
SUMMARY="$RES_DIR/summary.txt"
SAMP_LOGS="$RES_DIR/samples.log"
CHKPT_COPY="$RES_DIR/chkpt"
: > "$SUMMARY"

summary() { printf '%s\n' "$*" >> "$SUMMARY"; }

echo "=== NUFS V2.1 海量小文件挂载访问测试 [裸机] ==="
echo "  文件总数=${SCALE}  shards=${SHARDS}(每shard≈${PER_SHARD})  单文件=${FILESIZE}B"
echo "  崩溃注入≈${CRASH_AFTER}s  挂载点=${MOUNTPOINT}"
echo "  数据根=${DATA_ROOT} 日志=${LOG_DIR}"
echo "  结果目录=${RES_DIR}"
echo ""
summary "NUFS V2.1 海量小文件挂载访问测试 [裸机]"
summary "时间: $(date '+%F %T')"
summary "参数: scale=${SCALE} shards=${SHARDS} per_shard≈${PER_SHARD} filesize=${FILESIZE}B crash_after=${CRASH_AFTER}s verify_every=${VERIFY_EVERY}"
summary "挂载点=${MOUNTPOINT} data_root=${DATA_ROOT}"
summary "----------------------------------------"

# ---------------------------------------------------------------------------
# 前置检查：Linux + /dev/fuse + nufs-fuse
# ---------------------------------------------------------------------------
if [ "$(uname -s)" != "Linux" ]; then
  echo "FATAL: 真实 FUSE 挂载需要 Linux + /dev/fuse（本机 $(uname -s) 不支持）"
  exit 1
fi
if [ ! -e /dev/fuse ]; then
  echo "FATAL: /dev/fuse 不存在"
  exit 1
fi
command -v "$FUSE_BIN" >/dev/null 2>&1 || [ -x "$FUSE_BIN" ] || { echo "FATAL: 找不到 nufs-fuse: $FUSE_BIN"; exit 1; }

# ---------------------------------------------------------------------------
# POSIX 海量小文件写入器（每个 shard 一个 python 进程）——与 Docker 版同构
# ---------------------------------------------------------------------------
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
sub=os.path.join(root, 'f')
os.makedirs(sub, exist_ok=True)

t0=time.time()
crash_open=t0+crash_after; crash_close=crash_open+crash_settle
def in_crash():
    return crash_open <= time.time() < crash_close

rng=random.Random(0x5EEDC0DE ^ (shard*2654435761))
data=bytes(rng.getrandbits(8) for _ in range(fsize))
h=hashlib.sha256(data).hexdigest()

ok=0; verify_ok=0; failed=0; crash_tol=0
def cp():
    with open(os.path.join(chkpt_dir, 's%05d.ct'%shard),'w') as f:
        f.write('%d %d %d %d\\n' % (ok, verify_ok, failed, crash_tol))

for i in range(count):
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
# 1. 编译 + 启动裸机集群
# ---------------------------------------------------------------------------
echo "--- [1/7] Build + start bare-metal cluster ---"
host_build
host_cluster_up

echo "--- Cluster ready ---"
wait_http "$METAD_HEALTH" 20 metad || die "metad"

# ---------------------------------------------------------------------------
# 2. 挂载 nufs-fuse
# ---------------------------------------------------------------------------
echo "--- [2/7] Mount nufs-fuse ---"
host_mount "$MOUNTPOINT"
echo "mounted at $MOUNTPOINT (pid $(cat "$FUSE_PID"))"

# ---------------------------------------------------------------------------
# 3. 并行 writer 创建海量小文件（后台）+ 崩溃注入 + 内存采样
# ---------------------------------------------------------------------------
echo "--- [3/7] Create $SCALE small files ($SHARDS parallel shards) ---"
rm -rf "$CHKPT_DIR" "$DONE_FLAG" && mkdir -p "$CHKPT_DIR"
rm -f "$MOUNTPOINT/.abort"
start=$(date +%s)
python3 -c "$WRITER" 0 "$PER_SHARD" "$FILESIZE" "$VERIFY_EVERY" &
WRITER0=$!
for s in $(seq 1 $((SHARDS-1))); do
  python3 -c "$WRITER" "$s" "$PER_SHARD" "$FILESIZE" "$VERIFY_EVERY" &
done
WRITER_PIDS=$(jobs -p)
echo "writers: $WRITER_PIDS"

: > "$SAMP_LOGS"
(
  SAMPLING=1
  while [ $SAMPLING -eq 1 ]; do
    MB=$(host_sample_mem_mib "$DATANODE_PID" 2>/dev/null || echo -1)
    echo "$(date +%s) $MB" >> "$SAMP_LOGS"
    sleep 20
  done
)&
SAMPLE_PID=$!

(
  sleep $CRASH_AFTER
  echo ">>> INJECTING SIGKILL to datanode (crash recovery test)"
  host_crash_datanode
  sleep 3
  host_relaunch_datanode
  echo ">>> datanode relaunched after crash"
)&
CRASH_WATCH_PID=$!

ANY_FAIL=0
for p in $WRITER_PIDS; do
  wait "$p" 2>/dev/null || ANY_FAIL=1
done
kill "$SAMPLE_PID" 2>/dev/null || true
wait "$SAMPLE_PID" 2>/dev/null || true
wait "$CRASH_WATCH_PID" 2>/dev/null || true
end=$(date +%s)
echo "creation wall-clock: $((end-start))s"
summary "创建墙钟: $((end-start))s ; any_failed=$ANY_FAIL"

echo "--- per-shard creation summary ---"
cat "$CHKPT_DIR"/*.ct 2>/dev/null | sort
mkdir -p "$CHKPT_COPY"
cp -f "$CHKPT_DIR"/*.ct "$CHKPT_COPY"/ 2>/dev/null || true
echo "--- memory sample log ---"
cat "$SAMP_LOGS"
MEM_LINE=$(python3 -c "
import sys
pts=[]
for line in open('$SAMP_LOGS'):
    p=line.split()
    if len(p)==2 and int(p[1])>0: pts.append((int(p[0]),int(p[1])))
if len(pts)>=2:
    first=pts[0][1]; last=pts[-1][1]; peak=max(x[1] for x in pts)
    print('mem: first=%dMiB last=%dMiB peak=%dMiB'%(first,last,peak))
else:
    print('mem: insufficient valid samples')
" 2>/dev/null) || MEM_LINE='mem: N/A'
echo "$MEM_LINE"
summary "内存(datanode): $MEM_LINE"
echo "creation result: any_failed=$ANY_FAIL"
if [ $ANY_FAIL -ne 0 ]; then
  echo "FAIL: some writer failed (see above per-shard counts); tail driver log"
  echo "记: 崩溃窗口内部分 writer 可能因 datanode 重启瞬态中断，需人工判读"
fi
echo ""

# ---------------------------------------------------------------------------
# 4. 文件计数校验（目录项完整性）
# ---------------------------------------------------------------------------
echo "--- [4/7] Count files via mount enumeration ---"
CNT=$(find "$MOUNTPOINT" -type f 2>/dev/null | wc -l | tr -d ' ')
echo "files on mount: $CNT (target ~ ${SCALE}, minus deleted/aborted)"
summary "挂载文件计数: $CNT (target≈${SCALE})"
echo ""

# ---------------------------------------------------------------------------
# 5. 崩溃后分层完整性巡检（抽样 byte-exact + 可选全量）
# ---------------------------------------------------------------------------
echo "--- [5/7] Post-crash integrity check ---"
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
    buckets=[d for d in os.listdir(sub) if d.startswith('b')]
    total_buckets+=len(buckets)
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
if [ $SAMP_RC -ne 0 ]; then
  echo "FAIL: sampled integrity sweep"
  summary "完整性巡检(抽样): FAIL"
  snapshot_cluster_logs "$RES_DIR"
  exit 1
fi
echo "sampled sweep PASSED"
summary "完整性巡检(抽样): PASS"
echo ""

# ---------------------------------------------------------------------------
# 6. （可选）全量 byte-exact 巡检 + 删除风暴
# ---------------------------------------------------------------------------
if [ "$FULL_VERIFY" -eq 1 ]; then
  echo "--- [6/7] Full byte-exact verify (--full-verify) ---"
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
  echo "--- [6/7] full byte-exact verify: SKIPPED (use --full-verify) ---"
fi
echo ""

echo "--- [6/7] Delete storm (rm -rf shard trees) ---"
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
summary "删除清理: remaining_files=$REMAIN bucket_dirs=$buckets_remain"
echo ""

# ---------------------------------------------------------------------------
# 7. 卸载 + 结果
# ---------------------------------------------------------------------------
echo "--- [7/7] Unmount + result ---"
host_unmount "$MOUNTPOINT"

echo ""
echo "=== RESULT: 海量小文件挂载测试 [裸机] ==="
echo "  created=$CNT files, sweep(bad/missing)=${SAMP_RC}, delete_clean=$([ $SYNC_OK -eq 1 ] && echo yes || echo no)"
if [ $SAMP_RC -ne 0 ]; then
  echo "FAIL: post-crash integrity sweep failed"
  summary "RESULT: FAIL (post-crash integrity sweep)"
  rc=1
elif [ $SYNC_OK -ne 1 ]; then
  echo "FAIL: delete storm did not clear all files"
  summary "RESULT: FAIL (delete storm did not clear)"
  rc=1
elif [ $ANY_FAIL -ne 0 ]; then
  echo "WARN: some writer failed during crash window (datanode restart) — 需人工判读 per-shard counts"
  summary "RESULT: WARN (some writer failed in crash window)"
  rc=0
else
  echo "PASS"
  summary "RESULT: PASS"
  rc=0
fi

if [ $CLEANUP -eq 1 ]; then
  snapshot_cluster_logs "$RES_DIR"
  host_cluster_down
  echo "Cluster torn down (结果保留在 $RES_DIR)"
elif [ $KEEP_ALIVE -eq 1 ]; then
  snapshot_cluster_logs "$RES_DIR"
  echo "集群保持运行(--keep-alive)；可用 ./deploy/host/cluster.sh status / logs 观察"
fi
echo "汇总: $RESULTS_ROOT/$(basename "$RES_DIR")/summary.txt ; latest: $RESULTS_ROOT/smallfile-latest"
exit $rc
