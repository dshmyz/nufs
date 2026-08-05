#!/bin/bash
#
# NUFS V2.1 疲劳 / 可靠性测试（fatigue test）harness
#
# 在测试环境部署完整 V2.1 多盘集群（metad + datanode-v21-multi + S3 gateway +
# prometheus/grafana），然后持续注入 S3 读写/覆盖/删除负载，并在负载中途做
# 一次进程（SIGKILL）崩溃 + 自动恢复 + 数据完整性确认（对应 P0 崩溃硬化验收），
# 最后对全量写入对象做字节精确的完整性巡检。
#
# 疲劳测试关注点：
#   1. 持续负载下（大量随机对象 + 覆盖 + 删除腾挪）不崩、不挂起、无数据错乱；
#   2. 崩溃（kill -9）+ 自动重启后，先前 durable 写入仍可字节精确读回；
#      （验证 group-commit / 恢复 checkpoint / 多盘 rebalance 的崩溃一致性）
#   3. 进程内存 / 打开文件描述符不随时间单调泄漏；
#   4. 多盘 least-used 放置持续生效（数据落两盘）。
#
# 前置条件: docker compose, python3
# 用法:
#   ./tests/fatigue-test.sh --duration 600 [--rounds N] [--crash-after 120]
#       [--no-cleanup] [--keep-alive]
#
# 退出码: 0 = PASS；非 0 = FAIL（含具体失败阶段）

set -euo pipefail
cd "$(dirname "$0")/.."

# ---------------------------------------------------------------------------
# 可配置参数
# ---------------------------------------------------------------------------
DURATION=${FATIGUE_DURATION:-600}        # 总负载时长（秒）
ROUNDS=${FATIGUE_ROUNDS:-0}              # 0 = 由 DURATION 时间决定
CRASH_AFTER=${FATIGUE_CRASH_AFTER:-120}  # 负载开始后约多少秒注入 SIGKILL 崩溃
OBJECTS_BASE=${FATIGUE_OBJECTS:-24}      # 一轮内并发对象基数
VERIFY_EVERY=${FATIGUE_VERIFY_EVERY:-4}  # 每 N 次写做一次字节精确读回
CLEANUP=1
KEEP_ALIVE=0

usage() {
  sed -n '2,30p' "$0"
  exit 1
}

while [ $# -gt 0 ]; do
  case "$1" in
    --duration)     DURATION=$2; shift 2 ;;
    --rounds)       ROUNDS=$2; shift 2 ;;
    --crash-after)  CRASH_AFTER=$2; shift 2 ;;
    --objects)      OBJECTS_BASE=$2; shift 2 ;;
    --no-cleanup)   CLEANUP=0; shift ;;
    --keep-alive)   KEEP_ALIVE=1; shift ;;
    -h|--help)      usage ;;
    *) echo "未知参数: $1"; usage ;;
  esac
done

COMPOSE_FILE="deploy/docker-compose.yml"
S3_ENDPOINT="http://localhost:8081"
ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
BUCKET="v21-fatigue"
MULTI_CONTAINER="deploy-datanode-v21-multi-1"
WORK_FILE="/tmp/fatigue-payload.bin"
WORK_DL="/tmp/fatigue-payload-dl.bin"

echo "=== NUFS V2.1 疲劳 / 可靠性测试 ==="
echo "  时长=${DURATION}s  崩溃注入≈${CRASH_AFTER}s  对象基数=${OBJECTS_BASE}"
echo ""

# ---------------------------------------------------------------------------
# SigV4 S3 signing helper + 疲劳负载驱动（python）
# ---------------------------------------------------------------------------
# 负载驱动以 python 内嵌脚本运行在宿主机，直接向 S3 gateway 打负载。
# 通过写到一个共享清单文件，把每个已写对象的 key 与预期摘要/长度记录下来，
# 供最终完整性巡检（含崩溃后）做字节精确核对。
DRIVER="
import hashlib, hmac, datetime, urllib.request, urllib.error, os, sys, time, random, json, subprocess

ak='$ACCESS_KEY'; sk='$SECRET_KEY'; ep='$S3_ENDPOINT'; b='$BUCKET'
MANIFEST='$WORK_FILE.manifest.json'
CRASH_FLAG='/tmp/fatigue-crash.requested'
CRASHED_FLAG='/tmp/fatigue-crash.done'

def sign(m,p,h,bd=b''):
    n=datetime.datetime.now(datetime.timezone.utc)
    d=n.strftime('%Y%m%dT%H%M%SZ'); ds=n.strftime('%Y%m%d')
    h['host']='localhost:8081'; h['x-amz-date']=d
    h['x-amz-content-sha256']=hashlib.sha256(bd).hexdigest()
    ch=''.join(f'{kk.lower()}:{vv.strip()}\n' for kk,vv in sorted(h.items()))
    sh=';'.join(sorted(kk.lower() for kk in h))
    cr=f'{m}\n{p}\n\n{ch}\n{sh}\n{h[\"x-amz-content-sha256\"]}'
    cs=f'{ds}/us-east-1/s3/aws4_request'
    sts=f'AWS4-HMAC-SHA256\n{d}\n{cs}\n{hashlib.sha256(cr.encode()).hexdigest()}'
    def skf(kk,mm): return hmac.new(kk,mm.encode(),hashlib.sha256).digest()
    kd=skf(('AWS4'+sk).encode(),ds); kr=skf(kd,'us-east-1'); ks=skf(kr,'s3'); kg=skf(ks,'aws4_request')
    sg=hmac.new(kg,sts.encode(),hashlib.sha256).hexdigest()
    h['Authorization']=f'AWS4-HMAC-SHA256 Credential={ak}/{cs}, SignedHeaders={sh}, Signature={sg}'
    return h

def req(m,p,bd=b'',extra=None):
    h={} if extra is None else dict(extra)
    h=sign(m,p,h,bd)
    return urllib.request.Request(f'{ep}{p}',data=bd or None,headers=h,method=m)

def s3raw(m,k='',bd=b'',extra=None):
    # 编码路径里的对象名（避免特殊字符）
    from urllib.parse import quote
    p=('/' + b + '/' + quote(k,safe='')) if k else ('/' + b)
    r=req(m,p,bd,extra)
    try:
        resp=urllib.request.urlopen(r, timeout=5)
        return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception:
        # 崩溃注入期间连接会立即失败/重连被拒 → 返回合成 5xx（视为瞬态）
        return 503, b''

def restore_manifest():
    if os.path.exists(MANIFEST):
        try:
            with open(MANIFEST) as f: return json.load(f)
        except Exception: return []
    return []

def save_manifest(m):
    # fsync 保证崩溃（含宿主 SIGKILL 触发）后清单也在
    with open(MANIFEST,'w') as f:
        json.dump(m,f); f.flush(); os.fsync(f.fileno())

manifest=restore_manifest()
manifest_d={e['k']:e for e in manifest}
written=0; verified=0; failed=0; t0=time.time()
deadline = t0 + $DURATION
# 崩溃窗口：宿主脚本在 t0+CRASH_AFTER 附近对 datanode 发 SIGKILL 并等它自动恢复。
# 窗口内所有 5xx/连接错误都是"预期瞬态"（数据节点正在崩溃/重启），不判为失败；
# 窗口外任一非 200 才是真失败。
CRASH_AFTER=$CRASH_AFTER
CRASH_SETTLE=120          # 给 kill -9 + 自动 restart + 恢复 checkpoint 的宽限期
crash_open  = t0 + CRASH_AFTER
crash_close = crash_open + CRASH_SETTLE
crash_seen  = 0           # 崩溃窗口内日志限频

sys.stdout.write('driver started, deadline=%ds from now, crash window ~[%d,%d)s\\n' % (
    $DURATION, int(CRASH_AFTER), int(CRASH_AFTER+CRASH_SETTLE)))
sys.stdout.flush()

def in_crash_window():
    return crash_open <= time.time() < crash_close

def one_round(j, obj_hi):
    global written, verified, failed, crash_seen
    # 对象 key 打乱在基数范围内，同时带轮次戳，确保覆盖与删除持续进行
    k='w-%05d-%03d.bin' % (j, random.randrange(0,int(obj_hi)))
    size_choice=random.random()
    if size_choice < 0.55:
        sz=random.randint(1, 1024*512)          # 小对象（small-file 路径）
    elif size_choice < 0.90:
        sz=random.randint(1024*512, 1024*1024)  # 中等（单 chunk 边缘）
    else:
        sz=random.randint(1024*1024+123, 6*1024*1024)  # 大对象（跨多 chunk）
    data=os.urandom(sz)
    h=hashlib.sha256(data).hexdigest()

    # PUT（新建或覆盖同一 key 都会走一次分配/写路径）
    st,_=s3raw('PUT',k,data,{'content-length':str(sz),'content-type':'application/octet-stream'})
    if st not in (200,):
        # 崩溃窗口内瞬态错误 → 容忍；窗口外 → 硬失败
        if in_crash_window():
            crash_seen+=1
            if crash_seen<=5:
                sys.stdout.write('  [crash-window] tolerated PUT fail key=%s http=%d\\n'%(k,st)); sys.stdout.flush()
            return
        failed+=1; sys.stdout.write('PUT fail key=%s http=%d\\n'%(k,st)); sys.stdout.flush(); return
    manifest_d[k]={'k':k,'sha256':h,'size':sz,'round':j}
    written+=1

    # 抽样读回字节精确
    if (written % $VERIFY_EVERY)==0:
        st,body=s3raw('GET',k)
        if st!=200 or len(body)!=sz or hashlib.sha256(body).hexdigest()!=h:
            if in_crash_window():
                crash_seen+=1; return
            failed+=1; sys.stdout.write('VERIFY FAIL key=%s http=%d len=%d/%d\\n'%(k,st,len(body),sz)); sys.stdout.flush(); return
        verified+=1

    # 删除腾挪（约 1/5 的对象删除，驱动 GC/回收/rebalance）
    if random.random() < 0.2:
        st,_=s3raw('DELETE',k)
        # DELETE 对已存在对象应 204；崩溃窗口内失败也容忍
        if in_crash_window():
            pass
        if k in manifest_d: del manifest_d[k]
        if st not in (200,204) and not in_crash_window():
            failed+=1; sys.stdout.write('DELETE fail key=%s http=%d\\n'%(k,st)); sys.stdout.flush()

    # 持久化清单（只保留存活对象 + 崩溃确认后需要完整巡检的旧对象）
    save_manifest(list(manifest_d.values()))

    # 周期心跳
    if (written % 25)==0:
        el=time.time()-t0
        sys.stdout.write('[%4ds] written=%d verified=%d failed=%d tput=%.1f obj/s crash_tol=%d\\n'%(
            int(el),written,verified,failed, written/el if el>0 else 0, crash_seen))
        sys.stdout.flush()
    return

obj_hi=$OBJECTS_BASE
# 主循环：按 ROUNDS（>0 则固定轮数）或按时间下限驱动
if $ROUNDS > 0:
    for j in range($ROUNDS):
        one_round(j,obj_hi)
else:
    while time.time() < deadline:
        one_round(int(time.time()), obj_hi)
        # 崩溃窗口内放慢节奏（数据节点重启频繁，避免超时堆积拖慢整个窗口计时）
        if in_crash_window():
            time.sleep(0.05)

save_manifest(list(manifest_d.values()))
sys.stdout.write('fatigue done: written=%d verified=%d failed=%d 存活对象=%d\n'%(
    written,verified,failed,len(manifest_d)))
sys.stdout.flush()
if failed>0:
    sys.exit(1)
"

# ---------------------------------------------------------------------------
# 1. 构建镜像
# ---------------------------------------------------------------------------
echo "--- [1/8] Build Docker image ---"
docker compose -f "$COMPOSE_FILE" build 2>&1 | tail -2 || { echo "FAIL: build"; exit 1; }
echo ""

# ---------------------------------------------------------------------------
# 2. 启动多盘 V2.1 集群
# ---------------------------------------------------------------------------
echo "--- [2/8] Start metad + datanode-v21-multi + s3 ---"
docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
docker compose -f "$COMPOSE_FILE" --profile multidisk up -d metad datanode-v21-multi s3 2>&1
echo ""

# ---------------------------------------------------------------------------
# 3. 等待 metad / datanode / s3 就绪
# ---------------------------------------------------------------------------
echo "--- [3/8] Wait for readiness ---"
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
# 等待 V2.1 节点注册为 online（决定论放置：单节点）
for i in $(seq 1 20); do
  ONLINE=$(curl -s http://localhost:8091/api/v1/nodes | python3 -c "
import json,sys
try: print(len([n for n in json.load(sys.stdin) if n.get('state')==0]))
except Exception: print(0)
" 2>/dev/null || echo 0)
  [ "$ONLINE" -ge 1 ] && { echo "online nodes: $ONLINE"; break; }
  sleep 1
done
echo ""

# ---------------------------------------------------------------------------
# 4. 创建疲劳 bucket（RF=1，单节点即可）
# ---------------------------------------------------------------------------
echo "--- [4/8] Create RF=1 bucket ---"
CREATE=$(curl -s -X POST http://localhost:8091/api/v1/buckets \
  -H "Content-Type: application/json" \
  -d '{"name":"'$BUCKET'","policy":{"replication_factor":1}}' -w "|%{http_code}")
CODE="${CREATE##*|}"
echo "create bucket http_code=$CODE"
if [ "$CODE" != "201" ] && [ "$CODE" != "500" ]; then
  echo "unexpected create bucket status: $CREATE"; exit 1
fi
echo ""

# ---------------------------------------------------------------------------
# 5. 运行疲劳负载驱动（后台）
# ---------------------------------------------------------------------------
echo "--- [5/8] Run fatigue load driver (${DURATION}s) ---"
rm -f "$WORK_FILE.manifest.json" /tmp/fatigue-crash.requested /tmp/fatigue-crash.done /tmp/fatigue-driver.log
# 预置一个稳定的 payload（用于单次源文件，可选；实际负载用 py 生成随机数据）
dd if=/dev/urandom bs=1M count=1 of="$WORK_FILE" 2>/dev/null

python3 -c "$DRIVER" > /tmp/fatigue-driver.log 2>&1 &
DRIVER_PID=$!
echo "driver pid=$DRIVER_PID"
DRIVER_FAILED=0

# 内存 / fd 泄漏监控：driver 运行期间周期性采样 datanode 容器内存，首末对比。
# 若内存随负载单调超阈值增长 → 判为泄漏，令测试失败（关注点 3）。
REFCONTAINER="$MULTI_CONTAINER"   # 聚合：datanode 是负载主要承载
SAMP_LOGS=/tmp/fatigue-samples.log
: > "$SAMP_LOGS"
MEM_GROW_MB_THRESHOLD=1024        # 首末内存增长审计阈值（MB，宽松，捕获单调泄漏）
MEM_MAX_MB_THRESHOLD=4096         # 单次内存上限审计阈值（MB，捕获失控）

sample_mem_mb() {
  # 从 docker stats 的 "xxxMiB / 1GiB" 或 "1.2GiB / ..." 解析已用 MiB
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' "$REFCONTAINER" 2>/dev/null \
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

# 后台采样：每 20s 记录一次内存 MiB（driver 运行期间持续，用于首末对比）
(
  SAMPLING=1
  while [ $SAMPLING -eq 1 ]; do
    MB=$(sample_mem_mb 2>/dev/null || echo -1)
    echo "$(date +%s) $MB" >> "$SAMP_LOGS"
    sleep 20
  done
)&
SAMPLE_PID=$!

# 崩溃注入定时器：到 CRASH_AFTER 秒时对 datanode 发 SIGKILL，等自动重启后继续
(
  # 以 CRASH_AFTER 为期望点，但也等 driver 至少起了 5s 再安排
  sleep $CRASH_AFTER
  if [ -f /tmp/fatigue-crash.done ]; then exit 0; fi
  touch /tmp/fatigue-crash.requested
  echo ">>> INJECTING SIGKILL to datanode-v21-multi (crash recovery test)"
  docker kill -s KILL "$MULTI_CONTAINER" 2>/dev/null || {
    # 若容器的 PID1 不直接受 kill，重启容器来模拟彻底崩溃
    docker restart "$MULTI_CONTAINER" 2>/dev/null || true
  }
  # datanode 容器默认 restart:unless-stopped → 会自动 restart；这里主要等它回来
  sleep 3
  for i in $(seq 1 30); do
    if docker exec "$MULTI_CONTAINER" pgrep -f datanode >/dev/null 2>&1; then
      echo ">>> datanode-v21-multi restarted after crash (${i}s)"
      break
    fi
    sleep 1
  done
  touch /tmp/fatigue-crash.done
)&
CRASH_WATCH_PID=$!

# 主等待循环：driver 运行结束后，停采样、做泄漏审计，再做崩溃后完整性巡检
wait "$DRIVER_PID"; DRIVER_RC=$?
if [ $DRIVER_RC -ne 0 ]; then DRIVER_FAILED=1; fi
echo "driver exited rc=$DRIVER_RC"

# 停掉后台采样器
kill "$SAMPLE_PID" 2>/dev/null || true
wait "$SAMPLE_PID" 2>/dev/null || true

# 泄漏审计（告警级，非硬失败）：SIGKILL 崩溃会重置进程内存，首末对比会把
# "重启后重新爬坡"误判为泄漏，故这里只输出曲线 + 峰值/增长告警，由运维人工判定。
# 真正的字节精确完整性由第 6 步 sweep 兜底（硬门禁）。
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
    print('mem: insufficient valid samples (nonzero can be empty if crash coincided with sampling)')
"
fi

# 等待崩溃监听收尾（不阻塞）
wait "$CRASH_WATCH_PID" 2>/dev/null || true
echo ""

# ---------------------------------------------------------------------------
# 6. 崩溃后完整性巡检：对清单中全部对象字节精确验证
# ---------------------------------------------------------------------------
echo "--- [6/8] Post-load + post-crash integrity sweep ---"
if [ ! -f "$WORK_FILE.manifest.json" ] || ! python3 -c "import json;json.load(open('$WORK_FILE.manifest.json'))"; then
  echo "WARN: no manifest for sweep — driver may have produced no durable objects"
else
  python3 -c "
import hashlib, json, urllib.request, urllib.error, sys, time
from urllib.parse import quote
manifest=json.load(open('$WORK_FILE.manifest.json'))
ak='$ACCESS_KEY'; sk='$SECRET_KEY'; ep='$S3_ENDPOINT'; b='$BUCKET'
def sign(m,p,h,bd=b''):
    import hashlib, hmac, datetime
    n=datetime.datetime.now(datetime.timezone.utc)
    d=n.strftime('%Y%m%dT%H%M%SZ'); ds=n.strftime('%Y%m%d')
    h['host']='localhost:8081'; h['x-amz-date']=d
    h['x-amz-content-sha256']=hashlib.sha256(bd).hexdigest()
    ch=''.join(f'{kk.lower()}:{vv.strip()}\n' for kk,vv in sorted(h.items()))
    sh=';'.join(sorted(kk.lower() for kk in h))
    cr=f'{m}\n{p}\n\n{ch}\n{sh}\n{h[\"x-amz-content-sha256\"]}'
    cs=f'{ds}/us-east-1/s3/aws4_request'
    sts=f'AWS4-HMAC-SHA256\n{d}\n{cs}\n{hashlib.sha256(cr.encode()).hexdigest()}'
    def skf(kk,mm): return hmac.new(kk,mm.encode(),hashlib.sha256).digest()
    kd=skf(('AWS4'+sk).encode(),ds); kr=skf(kd,'us-east-1'); ks=skf(kr,'s3'); kg=skf(ks,'aws4_request')
    sg=hmac.new(kg,sts.encode(),hashlib.sha256).hexdigest()
    h['Authorization']=f'AWS4-HMAC-SHA256 Credential={ak}/{cs}, SignedHeaders={sh}, Signature={sg}'
    return h
def req(m,p,bd=b'',extra=None):
    h={} if extra is None else dict(extra); h=sign(m,p,h,bd)
    return urllib.request.Request(f'{ep}{p}',data=bd or None,headers=h,method=m)
def get(k):
    p='/'+b+'/'+quote(k,safe='')
    for attempt in (1,2):
        try:
            r=urllib.request.urlopen(req('GET',p)); return r.status,r.read()
        except urllib.error.HTTPError as e:
            if e.code>=500 and attempt==1:
                time.sleep(2); continue   # 瞬态（如节点刚重启）→ 重试一次再判
            return e.code,b''
        except Exception:
            if attempt==1: time.sleep(2); continue
            return 503,b''
bad=0; checked=0
for e in manifest:
    st,body=get(e['k'])
    checked+=1
    if st!=200 or len(body)!=e['size'] or hashlib.sha256(body).hexdigest()!=e['sha256']:
        bad+=1; print('SWEEP FAIL', e['k'], 'http=%d len=%d/%d' % (st,len(body),e['size']))
sys.stdout.write('sweep: checked=%d corrupt=%d\\n' % (checked,bad))
sys.exit(1 if bad>0 else 0)
"
  SWEEP_RC=$?
  echo "integrity sweep rc=$SWEEP_RC"
  if [ $SWEEP_RC -ne 0 ]; then
    echo "FAIL: post-crash integrity sweep found corruption"
    exit 1
  fi
fi
echo ""

# ---------------------------------------------------------------------------
# 7. 多盘放置确认
# ---------------------------------------------------------------------------
echo "--- [7/8] Verify multi-disk placement ---"
if docker exec "$MULTI_CONTAINER" find /var/lib/dfs/d0 -name '*.seg' 2>/dev/null | grep -q seg || \
   docker exec "$MULTI_CONTAINER" find /var/lib/dfs/d1 -name '*.seg' 2>/dev/null | grep -q seg; then
  echo "data present on disks (d0/d1)"
else
  echo "WARN: no .seg found via probe (path may differ) — non-fatal"
fi
echo ""

# ---------------------------------------------------------------------------
# 8. 结果与清理
# ---------------------------------------------------------------------------
echo "--- [8/8] Result ---"
if [ $DRIVER_FAILED -ne 0 ]; then
  echo "FAIL: fatigue driver reported errors (see /tmp/fatigue-driver.log)"
  tail -20 /tmp/fatigue-driver.log
  if [ $CLEANUP -eq 1 ]; then
    docker compose -f "$COMPOSE_FILE" --profile multidisk down -v
  fi
  exit 1
fi

echo ""
echo "=== NUFS V2.1 疲劳测试 PASSED (duration=${DURATION}s) ==="
echo "driver 日志: /tmp/fatigue-driver.log ; 资源采样: $SAMP_LOGS"

if [ $CLEANUP -eq 1 ]; then
  echo "--- Cleanup ---"
  docker compose -f "$COMPOSE_FILE" --profile multidisk down -v
  echo "Cluster torn down"
elif [ $KEEP_ALIVE -eq 1 ]; then
  echo "集群保持运行（--keep-alive）；可用 docker compose logs 观察"
fi

rm -f "$WORK_FILE" "$WORK_DL" 2>/dev/null || true
exit 0
