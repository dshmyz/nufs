#!/usr/bin/env bash
# run-v21-network-faults.sh - E2 multi-machine network fault injection drill
# Tests V2.1 engine under network partitions, packet loss, and latency via S3 gateway
#
# Usage: ./scripts/soak/run-v21-network-faults.sh [clean]
#
# Prerequisites:
#   - Docker with compose plugin
#   - Images built: nufs-gateway, nufs-metad, nufs-datanode, nufs-netem
#   - Ports 8080, 8090, 8091 available

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DEPLOY_DIR="$REPO_ROOT/deploy"
NETEM_DIR="$REPO_ROOT/deploy/netem"
COMPOSE_FILE="$DEPLOY_DIR/docker-compose.e2e.yml"
TEST_BUCKET="network-fault-test"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:8180}"
RESULTS_DIR="${NUFS_NETWORK_RESULTS:-$REPO_ROOT/.drill-results/network-faults-$(date +%Y%m%dT%H%M%S)}"
FAILURES=0

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() { echo -e "${GREEN}[+]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
fail() { echo -e "${RED}[-]${NC} $*"; exit 1; }

require_command() { command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"; }

mkdir -p "$RESULTS_DIR"
require_command docker
require_command curl

# Check if docker compose file exists
if [ ! -f "$COMPOSE_FILE" ]; then
    fail "Docker compose file not found: $COMPOSE_FILE"
fi

# Build images
log "Building docker images..."
cd "$REPO_ROOT"

# Check if nufs:latest exists, otherwise build
if docker image inspect nufs:latest >/dev/null 2>&1; then
    log "Using existing nufs:latest image"
    docker tag nufs:latest nufs-s3:latest
    docker tag nufs:latest nufs-metad:latest
    docker tag nufs:latest nufs-datanode:latest
else
    log "Building nufs image..."
    docker build -t nufs:latest -f nufs-core/Dockerfile nufs-core
    docker tag nufs:latest nufs-s3:latest
    docker tag nufs:latest nufs-metad:latest
    docker tag nufs:latest nufs-datanode:latest
fi

# Build netem image
docker build -t nufs-netem:latest -f "$NETEM_DIR/Dockerfile" .

# Start stack
log "Starting docker compose stack..."
cd "$DEPLOY_DIR"
docker compose -f docker-compose.e2e.yml up -d

# Wait for services to be ready
log "Waiting for services to be ready..."
sleep 15

# Check if metad is ready
if ! curl -sf "$S3_ENDPOINT/metrics" > /dev/null 2>&1; then
    warn "Metad not ready yet, waiting longer..."
    sleep 15
fi

# Create test bucket
log "Creating test bucket..."
if ! curl -sf -X PUT "$S3_ENDPOINT/$TEST_BUCKET" > /dev/null 2>&1; then
    # A pre-existing bucket is fine, but an unavailable gateway is not.
    if ! curl -sf "$S3_ENDPOINT/$TEST_BUCKET" > /dev/null 2>&1; then
        fail "S3 gateway is unavailable at $S3_ENDPOINT"
    fi
fi

# Helper function: write test data using curl
write_test_data() {
    local prefix=$1
    local count=${2:-10}
    local start_time=$(date +%s%N)

    log "Writing test data to $prefix/ ($count objects)..."
    for i in $(seq 1 $count); do
        echo "test-data-$i-$(date +%s)" | curl -s -X PUT \
            -H "Content-Type: text/plain" \
            -d @- \
            "$S3_ENDPOINT/$TEST_BUCKET/$prefix/test-$i.txt" > /dev/null 2>&1
    done

    local end_time=$(date +%s%N)
    local duration_ms=$(( (end_time - start_time) / 1000000 ))
    echo $duration_ms
}

# Helper function: read test data using curl
read_test_data() {
    local prefix=$1
    local count=${2:-10}
    local errors=0

    for i in $(seq 1 $count); do
        if ! curl -sf "$S3_ENDPOINT/$TEST_BUCKET/$prefix/test-$i.txt" > /dev/null 2>&1; then
            errors=$((errors + 1))
        fi
    done
    echo $errors
}

# Helper function: check health
check_health() {
    local errors=0
    if ! curl -sf "http://localhost:8190/api/v1/health" > /dev/null 2>&1; then
        errors=$((errors + 1))
    fi
    if ! curl -sf "http://localhost:8191/api/v1/health" > /dev/null 2>&1; then
        errors=$((errors + 1))
    fi
    echo $errors
}

# Scenario 1: Partition Leader Metad
scenario_partition_leader() {
    log "=== Scenario 1: Partition Leader Metad ==="

    # Find leader
    log "Finding leader..."
    LEADER_PORT=""
    for port in 8190 8191; do
        stats=$(curl -sf "http://localhost:$port/api/v1/cluster/status" 2>/dev/null || echo "")
        log "  Port $port: $stats"
        if echo "$stats" | grep -q '"is_leader": true'; then
            LEADER_PORT=$port
            break
        fi
    done

    if [ -z "$LEADER_PORT" ]; then
        warn "Could not find leader, skipping scenario 1"
        return
    fi

    log "Leader is on port $LEADER_PORT"
    LEADER_CONTAINER="nufs-metad-$(($LEADER_PORT - 8189))"

    # Write baseline data
    write_test_data "scenario1-before" 10

    # Start netem sidecar to partition leader.
    # --cap-add=NET_ADMIN is required: the sidecar shares the metad netns and
    # `tc qdisc add` refuses to run without CAP_NET_ADMIN (Docker drops it by
    # default). Without it the partition never takes effect and the scenario
    # reports a vacuous PASS against a healthy cluster.
    log "Starting partition on leader..."
    docker run -d --cap-add=NET_ADMIN --name netem-partition-leader \
        --net container:"$LEADER_CONTAINER" \
        --pid container:"$LEADER_CONTAINER" \
        nufs-netem:latest partition

    # Wait for partition to take effect
    sleep 5

    # Try writes during partition
    log "Attempting writes during partition..."
    local start_time=$(date +%s%N)
    local success_count=0
    local error_count=0

    for i in $(seq 1 10); do
        if echo "partition-test-$i" | curl -s -X PUT \
            -H "Content-Type: text/plain" \
            -d @- \
            "$S3_ENDPOINT/$TEST_BUCKET/scenario1-during/test-$i.txt" > /dev/null 2>&1; then
            success_count=$((success_count + 1))
        else
            error_count=$((error_count + 1))
        fi
    done

    local end_time=$(date +%s%N)
    local duration_ms=$(( (end_time - start_time) / 1000000 ))

    # Check health during partition
    local health_errors=$(check_health)

    # Remove partition. The drop-100% rule lives in the metad's shared netns, so
    # first delete it explicitly via the sidecar (which still holds NET_ADMIN in
    # that netns), then stop the container. Both the sidecar's SIGTERM trap and
    # this explicit `reset` make the cleanup robust.
    log "Removing partition..."
    docker exec netem-partition-leader /scripts/netem-apply.sh reset 2>/dev/null || true
    docker stop netem-partition-leader 2>/dev/null || true
    docker rm netem-partition-leader 2>/dev/null || true

    # Wait for recovery
    sleep 10

    # Verify data after partition
    local read_errors=$(read_test_data "scenario1-before" 10)

    # Summary
    log "Scenario 1 Results:"
    log "  Writes during partition: $success_count success, $error_count errors"
    log "  Health checks during partition: $health_errors errors"
    log "  Read errors after recovery: $read_errors"
    log "  Duration: ${duration_ms}ms"

    if [ "$read_errors" -eq 0 ]; then
        log "  ✓ All baseline data preserved"
    else
        warn "  ✗ Some baseline data lost"
        return 1
    fi
}

# Scenario 2: Partition Minority Metad (1 node)
scenario_partition_minority() {
    log "=== Scenario 2: Partition Minority Metad ==="

    # Find non-leader
    log "Finding non-leader..."
    NON_LEADER_PORT=""
    for port in 8190 8191; do
        stats=$(curl -sf "http://localhost:$port/api/v1/cluster/status" 2>/dev/null || echo "")
        log "  Port $port: $stats"
        if echo "$stats" | grep -q '"is_leader": false'; then
            NON_LEADER_PORT=$port
            break
        fi
    done

    if [ -z "$NON_LEADER_PORT" ]; then
        warn "Could not find non-leader, skipping scenario 2"
        return
    fi

    log "Non-leader is on port $NON_LEADER_PORT"
    NON_LEADER_CONTAINER="nufs-metad-$(($NON_LEADER_PORT - 8189))"

    # Write baseline data
    write_test_data "scenario2-before" 10

    # Start netem sidecar to partition non-leader (see scenario 1 for why
    # --cap-add=NET_ADMIN is required).
    log "Starting partition on non-leader..."
    docker run -d --cap-add=NET_ADMIN --name netem-partition-minority \
        --net container:"$NON_LEADER_CONTAINER" \
        --pid container:"$NON_LEADER_CONTAINER" \
        nufs-netem:latest partition

    # Wait for partition to take effect
    sleep 5

    # Try writes during partition (should succeed via leader)
    log "Attempting writes during partition..."
    local start_time=$(date +%s%N)
    local success_count=0
    local error_count=0

    for i in $(seq 1 10); do
        if echo "partition-test-$i" | curl -s -X PUT \
            -H "Content-Type: text/plain" \
            -d @- \
            "$S3_ENDPOINT/$TEST_BUCKET/scenario2-during/test-$i.txt" > /dev/null 2>&1; then
            success_count=$((success_count + 1))
        else
            error_count=$((error_count + 1))
        fi
    done

    local end_time=$(date +%s%N)
    local duration_ms=$(( (end_time - start_time) / 1000000 ))

    # Check health during partition
    local health_errors=$(check_health)

    # Remove partition. The drop-100% rule lives in the non-leader's shared
    # netns; reset it explicitly before stopping the container.
    log "Removing partition..."
    docker exec netem-partition-minority /scripts/netem-apply.sh reset 2>/dev/null || true
    docker stop netem-partition-minority 2>/dev/null || true
    docker rm netem-partition-minority 2>/dev/null || true

    # Wait for recovery
    sleep 10

    # Verify data after partition
    local read_errors=$(read_test_data "scenario2-before" 10)

    # Summary
    log "Scenario 2 Results:"
    log "  Writes during partition: $success_count success, $error_count errors"
    log "  Health checks during partition: $health_errors errors"
    log "  Read errors after recovery: $read_errors"
    log "  Duration: ${duration_ms}ms"

    if [ "$read_errors" -eq 0 ]; then
        log "  ✓ All baseline data preserved"
    else
        warn "  ✗ Some baseline data lost"
        return 1
    fi
}

# Scenario 3: Packet Loss + Latency
scenario_loss_latency() {
    log "=== Scenario 3: Packet Loss + Latency ==="

    # Write baseline data
    write_test_data "scenario3-before" 10

    # Start netem sidecar on first datanode. Use the combined `losslat` command
    # so packet loss and latency are applied as ONE root qdisc — issuing loss and
    # delay as two separate `tc qdisc add ... root` calls fails with "File exists"
    # (a netns holds a single root qdisc), silently dropping the latency half.
    log "Starting packet loss + latency injection..."
    docker run -d --cap-add=NET_ADMIN --name netem-loss-latency \
        --net container:nufs-datanode-1 \
        --pid container:nufs-datanode-1 \
        nufs-netem:latest losslat 20 50ms 60

    # Wait for effects to take place
    sleep 5

    # Try writes during loss+latency
    log "Attempting writes during loss+latency..."
    local start_time=$(date +%s%N)
    local success_count=0
    local error_count=0

    for i in $(seq 1 10); do
        if echo "loss-latency-test-$i" | curl -s -X PUT \
            -H "Content-Type: text/plain" \
            -d @- \
            "$S3_ENDPOINT/$TEST_BUCKET/scenario3-during/test-$i.txt" > /dev/null 2>&1; then
            success_count=$((success_count + 1))
        else
            error_count=$((error_count + 1))
        fi
    done

    local end_time=$(date +%s%N)
    local duration_ms=$(( (end_time - start_time) / 1000000 ))

    # Check health during loss+latency
    local health_errors=$(check_health)

    # Remove injection. The qdisc lives in the datanode's shared netns; the
    # losslat command has a 60s auto-remove, but reset explicitly in case the
    # drill is still within that window.
    log "Removing packet loss + latency..."
    docker exec netem-loss-latency /scripts/netem-apply.sh reset 2>/dev/null || true
    docker stop netem-loss-latency 2>/dev/null || true
    docker rm netem-loss-latency 2>/dev/null || true

    # Wait for recovery
    sleep 10

    # Verify data after loss+latency
    local read_errors=$(read_test_data "scenario3-before" 10)

    # Summary
    log "Scenario 3 Results:"
    log "  Writes during loss+latency: $success_count success, $error_count errors"
    log "  Health checks during loss+latency: $health_errors errors"
    log "  Read errors after recovery: $read_errors"
    log "  Duration: ${duration_ms}ms"

    if [ "$read_errors" -eq 0 ]; then
        log "  ✓ All baseline data preserved"
    else
        warn "  ✗ Some baseline data lost"
        return 1
    fi
}

# Main execution
log "=== E2 Network Fault Injection Drill ==="
log "Starting at $(date)"

# Run scenarios
if ! scenario_partition_leader; then FAILURES=$((FAILURES + 1)); fi
if ! scenario_partition_minority; then FAILURES=$((FAILURES + 1)); fi
if ! scenario_loss_latency; then FAILURES=$((FAILURES + 1)); fi

# Final cleanup
log "Cleaning up..."
cd "$DEPLOY_DIR"
docker compose -f docker-compose.e2e.yml down -v

log "=== Drill Complete ==="
log "Finished at $(date)"
printf 'result=%s\nfailures=%s\nendpoint=%s\n' \
    "$([ "$FAILURES" -eq 0 ] && echo PASS || echo FAIL)" "$FAILURES" "$S3_ENDPOINT" \
    | tee "$RESULTS_DIR/REPORT.txt"
[ "$FAILURES" -eq 0 ]
