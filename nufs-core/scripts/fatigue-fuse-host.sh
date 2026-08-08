#!/bin/bash
#
# NUFS V2.1 挂载访问（真实 FUSE mount）疲劳 / 可靠性测试 —— 裸机（无 Docker）版
#
# 与 scripts/fatigue-fuse.sh（Docker 版）等价，但完全在宿主机上运行：用
# deploy/host/mount-helpers.sh 以裸机进程方式拉起 metad + datanode(v2.1 JBOD)
# + nufs-s3，用 nufs-fuse 真实挂载 DFS，然后对挂载点持续注入 POSIX 负载
# （写/读/追加/truncate/mkdir/symlink/删除 + 超过 64MiB 的多 chunk 文件），
# 并在负载中途对 datanode 做一次 SIGKILL 崩溃 + relaunch + 数据完整性确认
# （对应 P0 崩溃硬化 + FUSE 多 chunk Flush 验收），最后对全量挂载写入做字节
# 精确巡检。
#
# 为什么不需要 /etc/hosts 打通（与 Docker 版的关键区别）：
#   裸机单机 datanode 以 --register-addr=127.0.0.1:9103 上报（Replicas[i].Addr
#   就存它），宿主机上 nufs-fuse / nufs-s3 直接拨 127.0.0.1:9103 即可，
#   无需 compose override，也无需改 /etc/hosts。
#
# 前置条件（必须满足）:
#   * Linux 宿主机（真实 /dev/fuse），可用 root（fusermount）；
#   * python3；
#   * 可执行 nufs-fuse（裸机 BIN_DIR 内），用于真实挂载。
#
# 用法:
#   ./scripts/fatigue-fuse-host.sh --duration 600 [--rounds N] [--crash-after 120]
#       [--no-cleanup] [--keep-alive] [--mountpoint /mnt/nufs-fuse]
#
# 环境变量（继承 deploy/host/mount-helpers.sh）：NUFS_BIN_DIR/NUFS_DATA_ROOT/
# NUFS_MOUNTPOINT/NUFS_METAD_BIN/NUFS_DATANODE_BIN/NUFS_S3_BIN/NUFS_FUSE_BIN 等。
# 特别地：FATIGUE_FUSE_BIN 若设了则强制用该路径的 nufs-fuse（覆盖 NUFS_FUSE_BIN）。
#
# 退出码: 0 = PASS；非 0 = FAIL（含具体失败阶段）

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HH="${SCRIPT_DIR}/../deploy/host/mount-helpers.sh"
[ -f "$HH" ] || { echo "FATAL: $HH 缺失（先提交 deploy/host）"; exit 1; }
# shellcheck source=deploy/host/mount-helpers.sh
source "$HH"

# ---------------------------------------------------------------------------
# 可配置参数
# ---------------------------------------------------------------------------
DURATION=${FATIGUE_DURATION:-480}        # 总负载时长（秒）
ROUNDS=${FATIGUE_ROUNDS:-0}              # 0 = 由 DURATION 时间决定
CRASH_AFTER=${FATIGUE_CRASH_AFTER:-120}  # 负载开始后约多少秒注入 SIGKILL 崩溃
OBJECTS_BASE=${FATIGUE_OBJECTS:-16}      # 一轮内并发文件名基数
VERIFY_EVERY=${FATIGUE_VERIFY_EVERY:-3}  # 每 N 次写做一次字节精确读回
MOUNTPOINT=${FATIGUE_MOUNTPOINT:-"$MOUNTPOINT"}  # 继承 helper 默认 /mnt/nufs-fuse
CLEANUP=1
KEEP_ALIVE=0

if [ -n "${FATIGUE_FUSE_BIN:-}" ]; then
  FUSE_BIN="$FATIGUE_FUSE_BIN"   # 显式覆盖 helper 里的 nufs-fuse 路径
fi

usage() {
  sed -n '2,56p' "$0"
  exit 1
}

while [ $# -gt 0 ]; do
  case "$1" in
    --duration)     DURATION=$2; shift 2 ;;
    --rounds)       ROUNDS=$2; shift 2 ;;
    --crash-after)  CRASH_AFTER=$2; shift 2 ;;
    --objects)      OBJECTS_BASE=$2; shift 2 ;;
    --mountpoint)   MOUNTPOINT=$2; shift 2 ;;
    --no-cleanup)   CLEANUP=0; shift ;;
    --keep-alive)   KEEP_ALIVE=1; shift ;;
    -h|--help)      usage ;;
    *) echo "未知参数: $1"; usage ;;
  esac
done

META_ENDPOINT="http://localhost:8091"
S3_ENDPOINT="http://localhost:8081"
MANIFEST="/tmp/fatigue-fuse-manifest.json"   # 在挂载点内写入对象的路径 + 摘要/长度清单

# 结果归档目录（本次运行的全部验收证据落到这里，可持续位置非 /tmp）
RES_DIR="$(result_dir "fatigue")"
SUMMARY="$RES_DIR/summary.txt"
DRIVER_LOG="$RES_DIR/driver.log"
SAMP_LOGS="$RES_DIR/samples.log"
CLUSTER_LOG_COPY="$RES_DIR/cluster-logs"
: > "$SUMMARY"

summary() { printf '%s\n' "$*" >> "$SUMMARY"; }

echo "=== NUFS V2.1 挂载访问（FUSE mount）疲劳 / 可靠性测试 [裸机] ==="
echo "  时长=${DURATION}s  崩溃注入≈${CRASH_AFTER}s  文件名基数=${OBJECTS_BASE}"
echo "  挂载点=${MOUNTPOINT}  nufs-fuse=${FUSE_BIN}"
echo "  数据根=${DATA_ROOT} 日志=${LOG_DIR}"
echo "  结果目录=${RES_DIR}"
echo ""
summary "NUFS V2.1 挂载访问（FUSE mount）疲劳 / 可靠性测试 [裸机]"
summary "时间: $(date '+%F %T')"
summary "参数: duration=${DURATION}s crash_after=${CRASH_AFTER}s objects=${OBJECTS_BASE} verify_every=${VERIFY_EVERY}"
summary "挂载点=${MOUNTPOINT} fuse=${FUSE_BIN} data_root=${DATA_ROOT}"
summary "----------------------------------------"

# ---------------------------------------------------------------------------
# 前置检查：Linux + /dev/fuse + nufs-fuse
# ---------------------------------------------------------------------------
if [ "$(uname -s)" != "Linux" ]; then
  echo "FATAL: 真实 FUSE 挂载需要 Linux + /dev/fuse（本机 $(uname -s) 不支持）"
  exit 1
fi
if [ ! -e /dev/fuse ]; then
  echo "FATAL: /dev/fuse 不存在 —— 需要内核 FUSE 设备"
  exit 1
fi
command -v "$FUSE_BIN" >/dev/null 2>&1 || [ -x "$FUSE_BIN" ] || { echo "FATAL: 找不到 nufs-fuse: $FUSE_BIN（先 host_build 或 NUFS_FUSE_BIN 指定）"; exit 1; }

# ---------------------------------------------------------------------------
# POSIX 负载驱动（python，纯系统调用打挂载点）——与 Docker 版同构
# ---------------------------------------------------------------------------
DRIVER="
import hashlib, os, sys, time, random, json, errno, shutil
mnt='$MOUNTPOINT'
MANIFEST='$MANIFEST'
CRASH_AFTER=$CRASH_AFTER
CRASH_SETTLE=120
def now(): return time.time()

def restore_manifest():
    if os.path.exists(MANIFEST):
        try:
            with open(MANIFEST) as f: return json.load(f)
        except Exception: return []
    return []

def save_manifest(m):
    fd=os.open(MANIFEST, os.O_WRONLY|os.O_CREAT|os.O_TRUNC, 0o644)
    os.write(fd, json.dumps(m).encode()); os.fsync(fd); os.close(fd)

manifest=restore_manifest()
manifest_d={e['k']:e for e in manifest}
written=0; verified=0; failed=0
seq=0
t0=now(); deadline=t0+$DURATION
crash_open=t0+CRASH_AFTER; crash_close=crash_open+CRASH_SETTLE
crash_seen=0

def in_crash_window():
    return crash_open <= now() < crash_close

def crash_tolerate(action):
    global crash_seen
    if in_crash_window():
        crash_seen+=1
        if crash_seen<=5:
            sys.stdout.write('  [crash-window] tolerated %s\\n'%action); sys.stdout.flush()
        return True
    return False

def put_file(abs_path, rel, data):
    global written
    parent=os.path.dirname(abs_path)
    if parent and not os.path.isdir(parent):
        try: os.makedirs(parent, exist_ok=True)
        except Exception as e:
            if in_crash_window(): crash_seen+=1; return
            global failed
            failed+=1; sys.stdout.write('mkdir fail %s: %r\\n'%(parent,e)); sys.stdout.flush(); return
    try:
        with open(abs_path,'wb') as f:
            for i in range(0, len(data), 1024*1024):
                f.write(data[i:i+1024*1024])
                f.flush()
    except Exception as e:
        if crash_tolerate('put-file %s'%rel): return
        failed+=1; sys.stdout.write('WRITE fail %s: %r\\n'%(rel,e)); sys.stdout.flush(); return
    h=hashlib.sha256(data).hexdigest()
    manifest_d[rel]={'k':rel,'sha256':h,'size':len(data),'seq':seq}
    written+=1

def read_back(abs_path, rel, size, h):
    global verified
    try:
        with open(abs_path,'rb') as f: got=f.read()
    except Exception as e:
        if crash_tolerate('get-file %s'%rel): return
        global failed
        failed+=1; sys.stdout.write('READ fail %s: %r\\n'%(rel,e)); sys.stdout.flush(); return
    if len(got)!=size or hashlib.sha256(got).hexdigest()!=h:
        if crash_tolerate('read-verify %s'%rel): return
        failed+=1; sys.stdout.write('VERIFY FAIL %s len=%d/%d mismatched\\n'%(rel,len(got),size)); sys.stdout.flush(); return
    verified+=1

def unlink_path(rel):
    try:
        os.unlink(os.path.join(mnt,rel))
    except OSError as e:
        if e.errno==errno.ENOENT:
            pass
        elif not crash_tolerate('unlink %s'%rel):
            global failed
            failed+=1; sys.stdout.write('UNLINK fail %s: %r\\n'%(rel,e)); sys.stdout.flush()
        return
    if rel in manifest_d: del manifest_d[rel]

def one_round(j):
    global seq, written, verified, failed
    seq+=1
    i=random.randrange(0,int($OBJECTS_BASE))
    rel='w-%05d.dir/d-%03d/f-%05d.bin' % (j%97, random.randrange(0,8), i)
    sz_choice=random.random()
    if sz_choice < 0.50:
        sz=random.randint(1, 1024*512)
    elif sz_choice < 0.88:
        sz=random.randint(1024*1024, 32*1024*1024)
    else:
        sz=random.randint(64*1024*1024+4096, 4*64*1024*1024)
    data=os.urandom(sz)
    abs_path=os.path.join(mnt, rel)

    put_file(abs_path, rel, data)
    if (written % $VERIFY_EVERY)==0:
        e=manifest_d.get(rel)
        if e: read_back(abs_path, rel, e['size'], e['sha256'])

    if random.random() < 0.15:
        append=os.urandom(random.randint(1, 4*1024*1024))
        try:
            with open(abs_path,'ab') as f: f.write(append)
            if rel in manifest_d:
                try:
                    got=open(abs_path,'rb').read()
                    manifest_d[rel]={'k':rel,'sha256':hashlib.sha256(got).hexdigest(),'size':len(got),'seq':seq}
                except Exception as e2:
                    if not crash_tolerate('append-verify %s'%rel):
                        failed+=1; sys.stdout.write('APPEND re-read fail %s: %r\\n'%(rel,e2)); sys.stdout.flush()
        except Exception as e:
            if not crash_tolerate('append %s'%rel):
                failed+=1; sys.stdout.write('APPEND fail %s: %r\\n'%(rel,e)); sys.stdout.flush()

    if random.random() < 0.10:
        try:
            os.truncate(abs_path, random.randint(0, max(0, sz//2)))
            if rel in manifest_d:
                try:
                    got=open(abs_path,'rb').read()
                    manifest_d[rel]={'k':rel,'sha256':hashlib.sha256(got).hexdigest(),'size':len(got),'seq':seq}
                except Exception as e2:
                    if not crash_tolerate('trunc-verify %s'%rel):
                        failed+=1; sys.stdout.write('TRUNC re-read fail %s: %r\\n'%(rel,e2)); sys.stdout.flush()
        except Exception as e:
            if not crash_tolerate('truncate %s'%rel):
                failed+=1; sys.stdout.write('TRUNCATE fail %s: %r\\n'%(rel,e)); sys.stdout.flush()

    if random.random() < 0.2:
        unlink_path(rel)
        d=os.path.join(mnt, os.path.dirname(rel))
        try: os.rmdir(d)
        except OSError: pass

    save_manifest(list(manifest_d.values()))

    if (written % 25)==0:
        el=now()-t0
        sys.stdout.write('[%4ds] written=%d verified=%d failed=%d tput=%.1f obj/s crash_tol=%d\\n'%(
            int(el),written,verified,failed, written/el if el>0 else 0, crash_seen))
        sys.stdout.flush()
    return

if $ROUNDS > 0:
    for j in range($ROUNDS):
        one_round(j)
else:
    while now() < deadline:
        one_round(int(now()))
        if in_crash_window():
            time.sleep(0.05)

save_manifest(list(manifest_d.values()))
sys.stdout.write('fuse fatigue done: written=%d verified=%d failed=%d 存活对象=%d\\n'%(
    written,verified,failed,len(manifest_d)))
sys.stdout.flush()
sys.exit(1 if failed>0 else 0)
"

# ---------------------------------------------------------------------------
# 1. 编译 + 启动裸机集群（metad + datanode v2.1 JBOD + s3）
# ---------------------------------------------------------------------------
echo "--- [1/8] Build + start bare-metal cluster ---"
host_build
host_cluster_up

# ---------------------------------------------------------------------------
# 2. 等待集群就绪（helper host_cluster_up 已等 metad/datanode/s3）
# ---------------------------------------------------------------------------
echo "--- [2/8] Cluster ready check ---"
wait_http "$METAD_HEALTH" 20 metad || { echo "FAIL: metad not ready"; die "metad"; }
wait_http "$S3_HEALTH" 20 s3 2>/dev/null || echo "WARN: s3 health not reached (non-fatal for FUSE test)"

# ---------------------------------------------------------------------------
# 3. 挂载 nufs-fuse（真实内核挂载，复用 helper host_mount）
# ---------------------------------------------------------------------------
echo "--- [3/8] Mount nufs-fuse ---"
host_mount "$MOUNTPOINT"
echo "mounted at $MOUNTPOINT (pid $(cat "$FUSE_PID"))"
echo ""

# ---------------------------------------------------------------------------
# 4. 运行 POSIX 负载驱动（后台）+ 内存采样 + 崩溃注入
# ---------------------------------------------------------------------------
echo "--- [4/8] Run FUSE POSIX load driver (${DURATION}s) ---"
rm -f "$MANIFEST"
python3 -c "$DRIVER" > "$DRIVER_LOG" 2>&1 &
DRIVER_PID=$!
echo "driver pid=$DRIVER_PID -> $DRIVER_LOG"
DRIVER_FAILED=0

: > "$SAMP_LOGS"
MEM_GROW_MB_THRESHOLD=1024
MEM_MAX_MB_THRESHOLD=4096

(
  SAMPLING=1
  while [ $SAMPLING -eq 1 ]; do
    MB=$(host_sample_mem_mib "$DATANODE_PID" 2>/dev/null || echo -1)
    echo "$(date +%s) $MB" >> "$SAMP_LOGS"
    sleep 20
  done
)&
SAMPLE_PID=$!

# 崩溃注入定时器：到 CRASH_AFTER 秒时对 datanode 进程发 SIGKILL，等 relaunch
(
  sleep $CRASH_AFTER
  echo ">>> INJECTING SIGKILL to datanode (crash recovery test)"
  host_crash_datanode
  sleep 3
  host_relaunch_datanode
  echo ">>> datanode relaunched after crash"
)&
CRASH_WATCH_PID=$!

wait "$DRIVER_PID"; DRIVER_RC=$?
if [ $DRIVER_RC -ne 0 ]; then DRIVER_FAILED=1; fi
echo "driver exited rc=$DRIVER_RC"

kill "$SAMPLE_PID" 2>/dev/null || true
wait "$SAMPLE_PID" 2>/dev/null || true

echo "--- memory sample log (epoch_sec mem_mib) ---"
cat "$SAMP_LOGS"
if [ -s "$SAMP_LOGS" ]; then
  MEM_LINE=$(python3 -c "
import sys
pts=[]
with open('$SAMP_LOGS') as f:
    for line in f:
        parts=line.split()
        if len(parts)!=2: continue
        ts=int(parts[0]); mb=int(parts[1])
        if mb>0: pts.append((ts,mb))
if len(pts)>=2:
    first=pts[0][1]; last=pts[-1][1]; peak=max(p for _,p in pts)
    warn=''
    if last-first>$MEM_GROW_MB_THRESHOLD or peak>$MEM_MAX_MB_THRESHOLD:
        warn=' LEAK-WARN(growth=%dMiB peak=%dMiB)'%(last-first,peak)
    print('mem: first=%dMiB last=%dMiB peak=%dMiB%s'%(first,last,peak,warn))
else:
    print('mem: insufficient valid samples')
")
  echo "$MEM_LINE"
  summary "内存(datanode): $MEM_LINE"
fi

wait "$CRASH_WATCH_PID" 2>/dev/null || true
echo ""

# ---------------------------------------------------------------------------
# 5. 崩溃后完整性巡检：对清单中全部挂载写入对象字节精确验证（经挂载点读）
# ---------------------------------------------------------------------------
echo "--- [5/8] Post-load + post-crash integrity sweep (via mount) ---"
if [ ! -f "$MANIFEST" ] || ! python3 -c "import json;json.load(open('$MANIFEST'))"; then
  echo "WARN: no manifest for sweep — driver may have produced no durable objects"
else
  python3 -c "
import hashlib, json, sys, os, time
mnt='$MOUNTPOINT'
manifest=json.load(open('$MANIFEST'))
bad=0; checked=0; missing=0
for e in manifest:
    p=os.path.join(mnt, e['k'])
    checked+=1
    data=None
    for attempt in (1,2):
        try:
            with open(p,'rb') as f: data=f.read()
            break
        except Exception:
            if attempt==1: time.sleep(2); continue
    if data is None:
        missing+=1; print('SWEEP FAIL (missing)', e['k']); continue
    if len(data)!=e['size'] or hashlib.sha256(data).hexdigest()!=e['sha256']:
        bad+=1; print('SWEEP FAIL', e['k'], 'len=%d/%d'%(len(data),e['size']))
sys.stdout.write('sweep: checked=%d corrupt=%d missing=%d\\n'%(checked,bad,missing))
sys.exit(1 if (bad+missing)>0 else 0)
"
  SWEEP_RC=$?
  echo "integrity sweep rc=$SWEEP_RC"
  SWEEP_LINE=$(python3 -c "
import hashlib, json
m=json.load(open('$MANIFEST'))
bad=0
for e in m:
    bad+=0  # sentinel no-op; live object count below is the durable evidence
print('存活对象=%d' % len(m))
" 2>/dev/null) || SWEEP_LINE='存活对象=N/A'
  summary "完整性巡检: rc=$SWEEP_RC ; $SWEEP_LINE"
  snapshot_file "$MANIFEST" "$RES_DIR/manifest.json"
  if [ $SWEEP_RC -ne 0 ]; then
    echo "FAIL: post-crash integrity sweep found corruption/missing"
    summary "RESULT: FAIL (post-crash integrity sweep corruption/missing)"
    [ $CLEANUP -eq 1 ] && host_unmount "$MOUNTPOINT" && host_cluster_down
    snapshot_cluster_logs "$RES_DIR"
    exit 1
  fi
fi
echo ""

# ---------------------------------------------------------------------------
# 6. 多盘放置确认（datanode 数据落在两盘）
# ---------------------------------------------------------------------------
echo "--- [6/8] Verify multi-disk placement ---"
ready_dir
for d in "$DN_D0" "$DN_D1"; do
  SEG=$(find "$d/segments/data/active/" -maxdepth 1 -name '*.seg' 2>/dev/null | wc -l | tr -d ' ')
  echo "disk $d active segments: $SEG"
  summary "多盘放置: $d active_segments=$SEG"
done
echo "(放置仅作参考；FUSE 多盘写入的字节精确性已由第 5 步 sweep 硬门禁兜底)"
echo ""

# ---------------------------------------------------------------------------
# 7. 卸载
# ---------------------------------------------------------------------------
echo "--- [7/8] Unmount ---"
host_unmount "$MOUNTPOINT"
echo "unmounted + fuse daemon stopped"
echo ""

# ---------------------------------------------------------------------------
# 8. 结果与清理
# ---------------------------------------------------------------------------
echo "--- [8/8] Result ---"
if [ $DRIVER_FAILED -ne 0 ]; then
  echo "FAIL: FUSE driver reported errors (see $DRIVER_LOG)"
  tail -20 "$DRIVER_LOG"
  summary "RESULT: FAIL (driver reported errors)"
  if [ $CLEANUP -eq 1 ] && ! mountpoint -q "$MOUNTPOINT" 2>/dev/null; then host_cluster_down; fi
  snapshot_cluster_logs "$RES_DIR"
  exit 1
fi

summary "RESULT: PASS"
echo ""
echo "=== NUFS V2.1 挂载访问（FUSE mount）疲劳测试 [裸机] PASSED (duration=${DURATION}s) ==="
echo "结果目录: $RES_DIR"
echo "  driver:  $DRIVER_LOG"
echo "  samples: $SAMP_LOGS"
echo "  summary: $SUMMARY"
echo "  (集群日志已快照到 $CLUSTER_LOG_COPY/)"

if [ $CLEANUP -eq 1 ]; then
  echo "--- Cleanup ---"
  mountpoint -q "$MOUNTPOINT" 2>/dev/null && host_unmount "$MOUNTPOINT" || true
  snapshot_cluster_logs "$RES_DIR"
  host_cluster_down
  echo "Cluster torn down (结果保留在 $RES_DIR)"
elif [ $KEEP_ALIVE -eq 1 ]; then
  snapshot_cluster_logs "$RES_DIR"
  echo "集群保持运行(--keep-alive)；可用 ./deploy/host/cluster.sh status / logs 观察"
fi
echo ""
echo "汇总: $RESULTS_ROOT/$(basename "$RES_DIR")/summary.txt ; latest: $RESULTS_ROOT/fatigue-latest"
exit 0
