#!/bin/bash
#
# NUFS V2.1 挂载访问（真实 FUSE mount）疲劳 / 可靠性测试
#
# 在 Linux 测试环境部署完整 V2.1 多盘集群（metad + datanode-v21-multi +
# S3 gateway + prometheus/grafana），用 nufs-fuse 在宿主机真实挂载 DFS
# （backed by metad + datanode-v21-multi），然后对挂载点持续注入 POSIX
# 负载（写/读/追加/truncate/mkdir/symlink/删除 + 超过 64MiB 的多 chunk
# 文件），并在负载中途对 datanode 做一次 SIGKILL 崩溃 + 自动恢复 +
# 数据完整性确认（对应 P0 崩溃硬化 + FUSE 多 chunk Flush 验收），最后对
# 全量挂载写入做字节精确巡检。
#
# 与 scripts/fatigue-test.sh（S3 API 路径）互补：本脚本走"真实挂载访问"
# （内核 VFS → go-fuse → DFSFile/DFSDir → metad + datanode），验证 FUSE
# 层（含 64MiB 以上多 chunk Flush、truncate、O_APPEND）在持续负载 + 崩溃
# 恢复下的写读一致性。
#
# 前置条件（必须满足）:
#   * Linux 宿主机（真实 /dev/fuse，go-fuse 挂载需要内核），可用 root 改
#     /etc/hosts 并解析容器网络（见下方 PROVIDER 说明）；
#   * docker compose, python3;
#   * 本机可执行 nufs-fuse（宿主 NATIVE，非容器内），用于真实挂载。
#
# 挂载可达性（关键）:
#   datanode 在 metad 里注册的地址是容器内部 hostname `datanode-v21-multi:
#   9103`（`Replicas[i].Addr` 存的就是它）。宿主机上的 nufs-fuse 要能拨通
#   这个地址才能读写 chunk。因此本脚本:
#     1) 用一个 compose override 把 datanode-v21-multi 的 TCP 端口 9103
#        发布到宿主机；
#     2) 把 `datanode-v21-multi` 写进宿主机 /etc/hosts 指向该容器的 bridge
#        IP —— 这样 host-resident FUSE 就能解析并拨到它。
#   两项均由脚本管理并在退出时清理。
#
# 用法:
#   ./scripts/fatigue-fuse.sh --duration 600 [--rounds N] [--crash-after 120]
#       [--no-cleanup] [--keep-alive] [--mountpoint /mnt/nufs-fuse]
#       [--fuse-bin /usr/local/bin/nufs-fuse]
#
# 退出码: 0 = PASS；非 0 = FAIL（含具体失败阶段）

set -euo pipefail
cd "$(dirname "$0")/.."

# ---------------------------------------------------------------------------
# 可配置参数
# ---------------------------------------------------------------------------
DURATION=${FATIGUE_DURATION:-480}        # 总负载时长（秒）
ROUNDS=${FATIGUE_ROUNDS:-0}              # 0 = 由 DURATION 时间决定
CRASH_AFTER=${FATIGUE_CRASH_AFTER:-120}  # 负载开始后约多少秒注入 SIGKILL 崩溃
OBJECTS_BASE=${FATIGUE_OBJECTS:-16}      # 一轮内并发文件名基数
VERIFY_EVERY=${FATIGUE_VERIFY_EVERY:-3}  # 每 N 次写做一次字节精确读回
MOUNTPOINT=${FATIGUE_MOUNTPOINT:-/mnt/nufs-fuse}
FUSE_BIN=${FATIGUE_FUSE_BIN:-nufs-fuse}  # 宿主 NATIVE nufs-fuse（需在 PATH 或给绝对路径）
CLEANUP=1
KEEP_ALIVE=0

usage() {
  sed -n '2,50p' "$0"
  exit 1
}

while [ $# -gt 0 ]; do
  case "$1" in
    --duration)     DURATION=$2; shift 2 ;;
    --rounds)       ROUNDS=$2; shift 2 ;;
    --crash-after)  CRASH_AFTER=$2; shift 2 ;;
    --objects)      OBJECTS_BASE=$2; shift 2 ;;
    --mountpoint)   MOUNTPOINT=$2; shift 2 ;;
    --fuse-bin)     FUSE_BIN=$2; shift 2 ;;
    --no-cleanup)   CLEANUP=0; shift ;;
    --keep-alive)   KEEP_ALIVE=1; shift ;;
    -h|--help)      usage ;;
    *) echo "未知参数: $1"; usage ;;
  esac
done

COMPOSE_FILE="deploy/docker-compose.yml"
COMPOSE_FUSE_OVERRIDE="deploy/compose.fuse-override.yml"
META_ENDPOINT="http://localhost:8091"
MULTI_CONTAINER="deploy-datanode-v21-multi-1"
MANIFEST="/tmp/fatigue-fuse-manifest.json"   # 在挂载点内写入对象的路径 + 摘要/长度清单
HOSTS_BACKUP="/tmp/fatigue-fuse-hosts.bak"

echo "=== NUFS V2.1 挂载访问（FUSE mount）疲劳 / 可靠性测试 ==="
echo "  时长=${DURATION}s  崩溃注入≈${CRASH_AFTER}s  文件名基数=${OBJECTS_BASE}"
echo "  挂载点=${MOUNTPOINT}  nufs-fuse=${FUSE_BIN}  datanode=${MULTI_CONTAINER}"
echo ""

# ---------------------------------------------------------------------------
# 前置检查：本脚本必须在 Linux + /dev/fuse 上运行
# ---------------------------------------------------------------------------
if [ "$(uname -s)" != "Linux" ]; then
  echo "FATAL: 真实 FUSE 挂载需要 Linux + /dev/fuse（本机 $(uname -s) 不支持）"
  exit 1
fi
if [ ! -e /dev/fuse ]; then
  echo "FATAL: /dev/fuse 不存在 —— 需要内核 FUSE 设备（见脚本头注释）"
  exit 1
fi
command -v "$FUSE_BIN" >/dev/null 2>&1 || { echo "FATAL: 找不到 nufs-fuse 可执行文件: $FUSE_BIN"; exit 1; }

# ---------------------------------------------------------------------------
# POSIX 负载驱动（python，纯系统调用打挂载点）
# ---------------------------------------------------------------------------
# 直接对 MOUNTPOINT 做文件操作，把一个对象的相对路径 + sha256 + 长度记进
# 清单（fsync 落地），供崩溃后字节精确巡检与挂载层多 chunk 读回验证。
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
    # 确保父目录存在（目录操作本身也在测试覆盖里）
    parent=os.path.dirname(abs_path)
    if parent and not os.path.isdir(parent):
        try: os.makedirs(parent, exist_ok=True)
        except Exception as e:
            if in_crash_window(): crash_seen+=1; return
            global failed
            failed+=1; sys.stdout.write('mkdir fail %s: %r\\n'%(parent,e)); sys.stdout.flush(); return
    # 写入：分块写（避免一次 os.write 大 buffer；也是崩溃窗口瞬态重试的天然界面）
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
    # 整文件读回 + 摘要（触发整文件多 chunk Flush 读路径）
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
    # 目录结构（随机子目录）+ 相对路径
    rel='w-%05d.dir/d-%03d/f-%05d.bin' % (j%97, random.randrange(0,8), i)
    sz_choice=random.random()
    if sz_choice < 0.50:
        sz=random.randint(1, 1024*512)                 # 小文件（单 / 亚 chunk）
    elif sz_choice < 0.88:
        sz=random.randint(1024*1024, 32*1024*1024)     # 中（单 chunk 内）
    else:
        sz=random.randint(64*1024*1024+4096, 4*64*1024*1024)  # 大：>64MiB 多 chunk Flush
    data=os.urandom(sz)
    abs_path=os.path.join(mnt, rel)

    put_file(abs_path, rel, data)
    if (written % $VERIFY_EVERY)==0:
        e=manifest_d.get(rel)
        if e: read_back(abs_path, rel, e['size'], e['sha256'])

    # 追加（O_APPEND）路径：对已存在文件追加，验证偏移状态一致性
    if random.random() < 0.15:
        append=os.urandom(random.randint(1, 4*1024*1024))
        try:
            with open(abs_path,'ab') as f: f.write(append)
            if rel in manifest_d:
                # 追加会破坏原摘要：重新全读并把新摘要记入清单
                #（追加后原先的摘要不代表新内容）
                try:
                    got=open(abs_path,'rb').read()
                    manifest_d[rel]={'k':rel,'sha256':hashlib.sha256(got).hexdigest(),'size':len(got),'seq':seq}
                except Exception as e2:
                    if not crash_tolerate('append-verify %s'%rel):
                        failed+=1; sys.stdout.write('APPEND re-read fail %s: %r\\n'%(rel,e2)); sys.stdout.flush()
        except Exception as e:
            if not crash_tolerate('append %s'%rel):
                failed+=1; sys.stdout.write('APPEND fail %s: %r\\n'%(rel,e)); sys.stdout.flush()

    # truncate 路径：对现有文件截断到随机长度，验证空洞/截断读
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

    # 删除腾挪（~1/5）
    if random.random() < 0.2:
        unlink_path(rel)
        # 顺手 rmdir 空目录（目录一致性也在覆盖内）
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
# 1. 生成 compose override（发布 datanode-v21-multi 的 9103 端口到宿主机，
#    使 host-resident nufs-fuse 能经公网端口拨通容器内 datanode）
# ---------------------------------------------------------------------------
echo "--- [1/9] Write compose FUSE override (publish datanode 9103) ---"
cat > "$COMPOSE_FUSE_OVERRIDE" <<YAML
# Auto-generated by scripts/fatigue-fuse.sh — publishes the multidisk V2.1
# datanode's TCP port (9103) to the host so a host-resident nufs-fuse can
# dial chunk stores at <datanode-v21-multi:9103> (the address metad stores in
# Replicas[].Addr). Cleaned up when the script exits.
services:
  datanode-v21-multi:
    ports:
      - "9103:9103"
YAML
echo "override -> $COMPOSE_FUSE_OVERRIDE"
echo ""

# ---------------------------------------------------------------------------
# 2. 构建镜像 + 启动 compose 集群（含 override）
# ---------------------------------------------------------------------------
echo "--- [2/9] Build + start metad/datanode-v21-multi/s3 ---"
docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_FUSE_OVERRIDE" build 2>&1 | tail -2 || { echo "FAIL: build"; exit 1; }
docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_FUSE_OVERRIDE" down -v 2>/dev/null || true
docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_FUSE_OVERRIDE" --profile multidisk up -d metad datanode-v21-multi s3 2>&1
echo ""

# ---------------------------------------------------------------------------
# 3. 等待集群就绪 + 解析容器 bridge IP 写 /etc/hosts
# ---------------------------------------------------------------------------
echo "--- [3/9] Wait for cluster + patch /etc/hosts ---"
for i in $(seq 1 40); do
  curl -sf http://localhost:8091/health >/dev/null 2>&1 && { echo "metad ready after ${i}s"; break; }
  [ "$i" -eq 40 ] && { echo "FAIL: metad not ready"; docker compose -f "$COMPOSE_FILE" logs metad --tail 20; exit 1; }
  sleep 1
done
for i in $(seq 1 40); do
  docker exec "$MULTI_CONTAINER" pgrep -f datanode >/dev/null 2>&1 && { echo "datanode-v21-multi up after ${i}s"; break; }
  [ "$i" -eq 40 ] && { echo "FAIL: datanode not up"; docker compose -f "$COMPOSE_FILE" logs datanode-v21-multi --tail 30; exit 1; }
  sleep 1
done
for i in $(seq 1 40); do
  curl -sf http://localhost:8081/healthz >/dev/null 2>&1 && { echo "s3 ready after ${i}s"; break; }
  [ "$i" -eq 40 ] && { echo "FAIL: s3 not ready"; docker compose -f "$COMPOSE_FILE" logs s3 --tail 20; exit 1; }
  sleep 1
done
# 取容器 bridge IP 写入 /etc/hosts，让宿主机 nufs-fuse 能把 datanode-v21-multi
# 解析到容器（配合 override 发布的 9103 端口即可拨通数字存储）。
BRIDGE_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$MULTI_CONTAINER" 2>/dev/null | awk '{print $1}')
if [ -z "$BRIDGE_IP" ]; then
  echo "FAIL: cannot resolve container bridge IP for $MULTI_CONTAINER"
  exit 1
fi
echo "datanode-v21-multi bridge IP = $BRIDGE_IP"
# 备份 /etc/hosts 再追加（幂等），退出时还原
cp /etc/hosts "$HOSTS_BACKUP"
if grep -q "^.*datanode-v21-multi" /etc/hosts; then
  sed -i "/datanode-v21-multi/d" /etc/hosts
fi
echo "$BRIDGE_IP   datanode-v21-multi" >> /etc/hosts
echo "/etc/hosts patched: $BRIDGE_IP -> datanode-v21-multi"
echo ""

# ---------------------------------------------------------------------------
# 4. 挂载 nufs-fuse（真实内核挂载，后台运行）
# ---------------------------------------------------------------------------
echo "--- [4/9] Mount nufs-fuse ---"
mkdir -p "$MOUNTPOINT"
# 先清理可能残留的挂载点
fusermount -u "$MOUNTPOINT" 2>/dev/null || true
FUSE_LOG=/tmp/fatigue-fuse-mount.log
"$FUSE_BIN" --backend=nufs --meta-addr=localhost:8091 \
  --dfs-metrics-addr=:9901 --log-level=info "$MOUNTPOINT" > "$FUSE_LOG" 2>&1 &
FUSE_PID=$!
echo "nufs-fuse pid=$FUSE_PID"

MOUNT_OK=0
for i in $(seq 1 30); do
  if mountpoint -q "$MOUNTPOINT" 2>/dev/null; then MOUNT_OK=1; break; fi
  if ! kill -0 "$FUSE_PID" 2>/dev/null; then
    echo "FAIL: nufs-fuse exited before mount"
    tail -40 "$FUSE_LOG"; exit 1
  fi
  sleep 1
done
if [ "$MOUNT_OK" -ne 1 ]; then
  echo "FAIL: mountpoint not ready after 30s"
  tail -40 "$FUSE_LOG"
  exit 1
fi
echo "mounted at $MOUNTPOINT"
echo ""

# ---------------------------------------------------------------------------
# 5. 运行 POSIX 负载驱动（后台）+ 内存采样 + 崩溃注入
# ---------------------------------------------------------------------------
echo "--- [5/9] Run FUSE POSIX load driver (${DURATION}s) ---"
rm -f "$MANIFEST"
python3 -c "$DRIVER" > /tmp/fatigue-fuse-driver.log 2>&1 &
DRIVER_PID=$!
echo "driver pid=$DRIVER_PID"
DRIVER_FAILED=0

SAMP_LOGS=/tmp/fatigue-fuse-samples.log
: > "$SAMP_LOGS"
MEM_GROW_MB_THRESHOLD=1024
MEM_MAX_MB_THRESHOLD=4096

sample_mem_mb() {
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' "$MULTI_CONTAINER" 2>/dev/null \
    | awk '{print $2}' | python3 -c "
import sys,re
for line in sys.stdin:
    v=line.split('/')[0].strip()
    m=re.match(r'([0-9.]+)([KMGT]i?B)',v.strip())
    if not m: print('-1'); break
    n=float(m.group(1)); u=m.group(2)[0]
    mb=n*{'K':0.001,'M':1,'G':1024,'T':1048576}[u]
    print(int(mb)); break
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

# 崩溃注入定时器：到 CRASH_AFTER 秒时对 datanode 发 SIGKILL，等自动恢复
(
  sleep $CRASH_AFTER
  echo ">>> INJECTING SIGKILL to datanode-v21-multi (crash recovery test)"
  docker kill -s KILL "$MULTI_CONTAINER" 2>/dev/null || {
    docker restart "$MULTI_CONTAINER" 2>/dev/null || true
  }
  sleep 3
  for i in $(seq 1 30); do
    if docker exec "$MULTI_CONTAINER" pgrep -f datanode >/dev/null 2>&1; then
      echo ">>> datanode-v21-multi restarted after crash (${i}s)"
      break
    fi
    sleep 1
  done
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
  python3 -c "
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
    print('mem: first=%dMiB last=%dMiB peak=%dMiB'%(first,last,peak))
    if last-first>$MEM_GROW_MB_THRESHOLD or peak>$MEM_MAX_MB_THRESHOLD:
        print('LEAK-WARN: growth=%dMiB peak=%dMiB (见上方曲线，人工复核；崩溃重启会重置基线)'%(last-first,peak))
else:
    print('mem: insufficient valid samples')
"
fi

wait "$CRASH_WATCH_PID" 2>/dev/null || true
echo ""

# ---------------------------------------------------------------------------
# 6. 崩溃后完整性巡检：对清单中全部挂载写入对象字节精确验证（经挂载点读）
# ---------------------------------------------------------------------------
echo "--- [6/9] Post-load + post-crash integrity sweep (via mount) ---"
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
    # 崩溃后节点刚重启 → 带上一次重试窗口（读瞬时失败可能是节点仍恢复中）
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
  if [ $SWEEP_RC -ne 0 ]; then
    echo "FAIL: post-crash integrity sweep found corruption/missing"
    exit 1
  fi
fi
echo ""

# ---------------------------------------------------------------------------
# 7. 多盘放置确认（datanode 数据落在两盘）
# ---------------------------------------------------------------------------
echo "--- [7/9] Verify multi-disk placement ---"
for d in d0 d1; do
  SEG=$(docker exec "$MULTI_CONTAINER" find /var/lib/dfs/$d/segments/data/active/ -name '*.seg' 2>/dev/null | wc -l | tr -d ' ')
  echo "disk $d active segments: $SEG"
done
echo "(放置仅作参考；FUSE 多盘写入的字节精确性已由第 6 步 sweep 硬门禁兜底)"
echo ""

# ---------------------------------------------------------------------------
# 8. 卸载 + 还原 /etc/hosts + 清理 compose override
# ---------------------------------------------------------------------------
echo "--- [8/9] Unmount + restore /etc/hosts ---"
# 数据一致性最终确认：卸载前对挂载点内仍存活的 manifest 对象做一次读取，
# 若崩溃后节点尚未完全就绪此处会失败，故以第 6 步 sweep 为准，这里仅做
# 干净的卸载。
fusermount -u "$MOUNTPOINT" 2>/dev/null || {
  echo "WARN: fusermount -u failed; killing fuse pid for clean teardown"
  kill "$FUSE_PID" 2>/dev/null || true
  sleep 2
  fusermount -u "$MOUNTPOINT" 2>/dev/null || true
}
wait "$FUSE_PID" 2>/dev/null || true
echo "unmounted + fuse daemon stopped"

if [ -f "$HOSTS_BACKUP" ]; then
  cp "$HOSTS_BACKUP" /etc/hosts
  echo "/etc/hosts restored"
fi
echo ""

# ---------------------------------------------------------------------------
# 9. 结果与清理
# ---------------------------------------------------------------------------
echo "--- [9/9] Result ---"
if [ $DRIVER_FAILED -ne 0 ]; then
  echo "FAIL: FUSE driver reported errors (see /tmp/fatigue-fuse-driver.log)"
  tail -20 /tmp/fatigue-fuse-driver.log
  if [ $CLEANUP -eq 1 ]; then
    docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_FUSE_OVERRIDE" --profile multidisk down -v
    rm -f "$COMPOSE_FUSE_OVERRIDE"
  fi
  exit 1
fi

echo ""
echo "=== NUFS V2.1 挂载访问（FUSE mount）疲劳测试 PASSED (duration=${DURATION}s) ==="
echo "driver 日志: /tmp/fatigue-fuse-driver.log ; mount 日志: $FUSE_LOG ; 资源采样: $SAMP_LOGS"

if [ $CLEANUP -eq 1 ]; then
  echo "--- Cleanup ---"
  docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_FUSE_OVERRIDE" --profile multidisk down -v
  rm -f "$COMPOSE_FUSE_OVERRIDE"
  echo "Cluster torn down + override removed"
elif [ $KEEP_ALIVE -eq 1 ]; then
  echo "集群保持运行（--keep-alive）；可用 docker compose logs 观察"
  echo "注意：override 文件保留在 $COMPOSE_FUSE_OVERRIDE，/etc/hosts 未还原（--keep-alive）"
fi

rm -f "$HOSTS_BACKUP" 2>/dev/null || true
exit 0
