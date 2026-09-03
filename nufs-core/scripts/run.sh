#!/bin/bash
#
# NUFS 测试脚本集
# 用法: ./scripts/run.sh <test-name> [options]
#
# 可用测试:
#   smoke        - 快速冒烟测试（S3 PUT/GET + kill datanode + JBOD，~30s）
#   regression   - 全量回归（datanode + chunkstore + gateway/s3，~90s）
#   load         - 负载测试（可配并发/时长）
#   benchmark    - 科学基准测试（90%读10%写，P50-P99.9）
#   chaos        - 混沌测试（随机 kill datanode，验证数据不丢）
#   ops-flow     - 端到端运维流程（adopt → migrate → decommission）
#   ec           - 纠删码集成测试
#   multidisk    - 多盘测试
#   full         - 全部测试（smoke + regression + benchmark，~180s）
#
# 环境变量:
#   NUFS_LOAD_DURATION   - 负载测试时长（默认 60s）
#   NUFS_LOAD_WORKERS    - 负载测试并发数（默认 8）
#   NUFS_LOAD_OBJ_SIZE   - 负载测试对象大小（默认 65536）
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CORE_DIR="$(dirname "$SCRIPT_DIR")"
MODULE_ROOT="$(dirname "$CORE_DIR")"   # 单模块合并后 go.mod 在仓库根
cd "$MODULE_ROOT"

export NUFS_RUN_SMOKE=1

# 默认参数
LOAD_DURATION="${NUFS_LOAD_DURATION:-60s}"
LOAD_WORKERS="${NUFS_LOAD_WORKERS:-8}"
LOAD_OBJ_SIZE="${NUFS_LOAD_OBJ_SIZE:-65536}"

run_smoke() {
    echo "=== 冒烟测试: S3 PUT/GET + kill datanode + JBOD + repair (~40s) ==="
    # TestFullStack = 主路径 + JBOD 多盘；TestRepair = 副本 repair + EC 修复。
    # TestRepair_EndToEnd_EC 特意用 4+2（6 节点）：验证分配尊重桶 ECConfig，
    # 非 6+3 桶有真实容错（kill 1 节点 → 5≥K=4 重建）。
    go test -v -count=1 \
        -run "TestFullStack|TestRepair" \
        -timeout=90s \
        ./nufs-core/tests/smoke/
}

run_regression() {
    echo "=== 全量回归测试 (~3min) ==="
    echo "--- datanode ---"
    # datanode/storage/segment 单包即需 ~150s（隔离跑实测），预算给足避免
    # ./nufs-core/datanode/... 并行时被 120s 误杀。
    go test -count=1 -timeout=300s ./nufs-core/datanode/... && echo "  ✅ datanode"
    echo "--- chunkstore ---"
    go test -count=1 -timeout=30s ./nufs-core/chunkstore/... && echo "  ✅ chunkstore"
    echo "--- gateway/s3 ---"
    go test -count=1 -timeout=30s ./nufs-core/gateway/s3/... && echo "  ✅ gateway/s3"
}

run_load() {
    echo "=== 负载测试: ${LOAD_WORKERS} workers, ${LOAD_DURATION}, ${LOAD_OBJ_SIZE}B 对象 ==="
    NUFS_LOAD_DURATION="$LOAD_DURATION" \
    NUFS_LOAD_WORKERS="$LOAD_WORKERS" \
    NUFS_LOAD_OBJ_SIZE="$LOAD_OBJ_SIZE" \
    go test -v -count=1 \
        -run "TestLoad_SustainedS3Ops" \
        -timeout=$(( ${LOAD_DURATION%s} + 120 ))s \
        ./nufs-core/tests/smoke/
}

run_benchmark() {
    echo "=== 科学基准测试 (90%读 10%写, ${LOAD_WORKERS} workers, ${LOAD_DURATION}) ==="
    NUFS_LOAD_DURATION="$LOAD_DURATION" \
    NUFS_LOAD_WORKERS="$LOAD_WORKERS" \
    go test -v -count=1 \
        -run "TestBenchmark_S3Workload" \
        -timeout=$(( ${LOAD_DURATION%s} + 120 ))s \
        ./nufs-core/tests/smoke/
}

run_chaos() {
    echo "=== 混沌测试: 随机 kill datanode (~90s) ==="
    go test -v -count=1 \
        -run "TestChaos_RandomDiskNodeKill" \
        -timeout=120s \
        ./nufs-core/tests/smoke/
}

run_ops_flow() {
    echo "=== 端到端运维流程: adopt → migrate → decommission (~15s) ==="
    go test -v -count=1 \
        -run "TestOpsFlow_AdoptMigrateDecommissionRestart" \
        -timeout=150s \
        ./nufs-core/tests/smoke/
}

run_ec() {
    echo "=== 纠删码集成测试 ==="
    go test -count=1 -v -run "TestEC" -timeout=30s ./nufs-core/chunkstore/... 2>&1 | grep -E "^(=== RUN|--- PASS|--- FAIL|PASS|ok)"
}

run_multidisk() {
    echo "=== 多盘测试 (V2.1 JBOD) ==="
    # V1 的 TestMultiDisk 已随 V1 引擎退役；V2.1 多盘端到端覆盖在 smoke 的
    # TestFullStack_MultiDiskJBOD（单节点双盘，S3 PUT/GET + 逐盘计数）。
    go test -count=1 -v -run "TestFullStack_MultiDiskJBOD" -timeout=60s ./nufs-core/tests/smoke/ 2>&1 | grep -E "^(=== RUN|--- PASS|--- FAIL|PASS|ok)"
}

run_full() {
    echo "========================================"
    echo "  NUFS 完整测试套件"
    echo "========================================"
    run_smoke
    echo ""
    run_regression
    echo ""
    run_benchmark
    echo ""
    run_ec
    echo ""
    run_multidisk
    echo ""
    echo "========================================"
    echo "  全部完成"
    echo "========================================"
}

# 主入口
case "${1:-help}" in
    smoke)      run_smoke ;;
    regression) run_regression ;;
    load)       run_load ;;
    benchmark)  run_benchmark ;;
    chaos)      run_chaos ;;
    ops-flow)   run_ops_flow ;;
    ec)         run_ec ;;
    multidisk)  run_multidisk ;;
    full)       run_full ;;
    *)
        echo "用法: $0 <test-name> [options]"
        echo ""
        echo "可用测试:"
        echo "  smoke        快速冒烟测试 (~30s)"
        echo "  regression   全量回归 (~90s)"
        echo "  load         负载测试 (可配 NUFS_LOAD_DURATION/WORKERS)"
        echo "  benchmark    科学基准测试 (90%读/10%写)"
        echo "  chaos        混沌测试 (~90s)"
        echo "  ops-flow     端到端运维流程 (~15s)"
        echo "  ec           纠删码测试 (~30s)"
        echo "  multidisk    多盘测试 (~30s)"
        echo "  full         全部测试 (~180s)"
        echo ""
        echo "示例:"
        echo "  ./scripts/run.sh smoke"
        echo "  NUFS_LOAD_WORKERS=4 NUFS_LOAD_DURATION=120s ./scripts/run.sh load"
        echo "  NUFS_LOAD_WORKERS=16 ./scripts/run.sh benchmark"
        ;;
esac
