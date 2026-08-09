# Network Fault Injection Drill Runbook

## Overview

This drill tests the V2.1 engine under realistic multi-machine network failures:
- **Network partitions**: Isolate nodes from the cluster
- **Packet loss**: Simulate degraded network conditions
- **Latency injection**: Test timeout and retry behavior

All scenarios run through the S3 gateway to validate end-to-end data path resilience.

## Prerequisites

- Docker with compose plugin
- Images built: `nufs-gateway`, `nufs-metad`, `nufs-datanode`, `nufs-netem`
- Ports 8080, 8090, 8091 available
- AWS CLI configured with dummy credentials (for S3 operations)

## Quick Start

```bash
# From repo root
cd nufs-core

# Build all images
make docker

# Run the drill
bash scripts/soak/run-v21-network-faults.sh
```

## Drill Scenarios

### Scenario 1: Partition Leader Metad

**Goal**: Verify cluster continues operating when the leader is partitioned.

**What happens**:
1. Identify the current Raft leader
2. Inject 100% packet loss on the leader node
3. Attempt writes through S3 gateway
4. Verify health checks during partition
5. Remove partition and verify data recovery

**Expected behavior**:
- Writes may fail or succeed (depends on client timeout and leader election)
- After partition removal, cluster should elect new leader
- All pre-partition data should be readable

**Pass criteria**:
- No data loss for pre-partition writes
- Health endpoint responds within 5s after partition removal

### Scenario 2: Partition Minority Metad

**Goal**: Verify cluster continues operating when a non-leader is partitioned.

**What happens**:
1. Identify a non-leader node
2. Inject 100% packet loss on the non-leader
3. Attempt writes through S3 gateway
4. Verify health checks during partition
5. Remove partition and verify data recovery

**Expected behavior**:
- Writes should succeed (leader is still available)
- Partitioned node may trigger leader election attempts
- After partition removal, node should rejoin cluster

**Pass criteria**:
- All writes during partition succeed
- No data loss
- All nodes healthy after partition removal

### Scenario 3: Packet Loss + Latency

**Goal**: Verify data path resilience under degraded network conditions.

**What happens**:
1. Inject 20% packet loss + 50ms latency on a datanode
2. Attempt writes through S3 gateway
3. Verify health checks during injection
4. Remove injection and verify data recovery

**Expected behavior**:
- Writes may experience retries or timeouts
- S3 gateway should retry failed requests
- All pre-partition data should be readable

**Pass criteria**:
- No data loss for pre-partition writes
- At least 50% of writes succeed during injection
- Health endpoint responds within 2s after injection removal

## Troubleshooting

### Container won't start

**Symptom**: `docker: Error response from daemon: failed to create task for container`

**Fix**:
- Check if ports 8080, 8090, 8091 are available: `lsof -i :8080`
- Ensure Docker daemon is running: `docker info`
- Rebuild images: `make docker`

### Netem sidecar fails to inject

**Symptom**: `tc: qdisc add failed: No such file or directory`

**Fix**:
- Ensure `nufs-netem` image has `iproute2`: `docker run --rm nufs-netem:latest tc qdisc show`
- Check container has `NET_ADMIN` capability (added in docker-compose.e2e.yml)

### Writes fail during partition

**Symptom**: `aws: error: An error occurred (InternalError) when calling the PutObject operation`

**Expected**: This is normal during leader partition. The drill logs success/error counts.

### Data lost after drill

**Symptom**: Read errors in scenario summary

**Debug**:
1. Check metad logs: `docker logs nufs-metad-1`
2. Check datanode logs: `docker logs nufs-datanode-1`
3. Verify Raft cluster health: `curl http://localhost:8090/api/v1/ops/raft/stats`

## Integration with CI

The network-fault drill is integrated into `make verify` at the `drill` level:

```bash
# Run drill-level verification (includes network faults)
make verify LEVEL=drill

# Or run in Docker (macOS)
make verify-docker LEVEL=drill
```

## Extending the Drill

### Add new fault type

1. Edit `deploy/netem/scripts/netem-apply.sh` to add new command
2. Rebuild image: `docker build -t nufs-netem:latest -f deploy/netem/Dockerfile .`
3. Add new scenario in `scripts/soak/run-v21-network-faults.sh`

### Adjust parameters

- Packet loss percentage: Edit `loss 20` in scenario_loss_latency()
- Latency delay: Edit `delay 50ms` in scenario_loss_latency()
- Partition duration: Edit `sleep` commands or use duration parameter

## References

- [tc-netem documentation](https://man7.org/linux/man-pages/man8/tc-netem.8.html)
- [Docker network disconnect](https://docs.docker.com/engine/reference/commandline/network_disconnect/)
- [Hashicorp Raft故障处理](https://developer.hashicorp.com/raft#fault-tolerance)
