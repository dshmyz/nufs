# Leader Failover Drill Runbook

This runbook covers the recurring **metadata raft leader-failover drill**: a
throwaway multi-metad raft cluster is driven through a hard raft-leader kill
while read/write traffic keeps flowing, and the run is gated on the recovery
time objective (RTO) and graceful degradation. It is the machine-checked
implementation of the "定期故障注入演练" requirement for the 5-9 availability
tier, and the evidence source for the `metad_leader_failover_rto` SLO.

- Harness: `tests/soak/run-v21-leader-failover.sh`
- Gate: `tests/soak/run-v21-leader-failover.sh` PASS/FAIL
- SLO: `internal/slo/slo.go` — `metad_leader_failover_rto` (budget 15 s),
  alert `NUFSLeaderFailoverRTOExceeded`
- Unscheduled automation: `deploy/systemd/nufs-leader-failover-drill.{service,timer}`

## What the drill proves

1. **RTO**: time from a raft-leader SIGKILL until a new leader serves a
   successful metadata write must be ≤ `--rto-budget` (default 15 s).
2. **Graceful degradation**: sustained PUT/GET must not produce *out-of-window*
   client errors — only the tight leader-switch window (default `--window 20` s
   after the kill) may tolerate a transient error; anything outside it fails the
   run.
3. **Census**: after the kill, exactly the killed leader is down; every other
   metad and datanode must still be online.
4. **Byte-exact durability**: every durable object read back must match its
   written hash (the `verify_all` stage).

The drill launches its own cluster on dedicated host ports (3 metad raft +
6 datanode + 1 s3 gateway) and tears it down on exit — it never touches a
production cluster.

## Running the drill

Run from `nufs-core/` (Go build rules apply):

```sh
./tests/soak/run-v21-leader-failover.sh \
  --duration 300 \
  --failover-after 120 \
  --rto-budget 15 \
  --window 20 \
  --results /var/log/nufs-tests
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--metad N` | 3 | raft nodes (odd, ≥ 3) |
| `--nodes N` | 6 | datanodes (≥ 3) |
| `--duration` | 300 | sustained read/write seconds |
| `--failover-after` | 40% of duration | seconds before the leader is SIGKILLed |
| `--rto-budget` | 15 | max leader-failover RTO (seconds) |
| `--window` | 20 | post-kill tolerated-error window (seconds) |

Set `SOAK_EC=1` to build the measured bucket as RF=9 EC6+3 instead of the
default RF=3 replication. RF=3 is the stable default for measuring the metad
failover objective; see the note below on the EC direct-write path.

## Scheduled drill

The `nufs-leader-failover-drill.timer` runs the oneshot service weekly. Enable it
on a dedicated runner host (it needs Go buildability and spare ports):

```sh
sudo cp deploy/systemd/nufs-leader-failover-drill.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now nufs-leader-failover-drill.timer
sudo systemctl list-timers nufs-leader-failover-drill.timer
```

A run that exceeds the RTO budget or surfaces out-of-window errors exits
non-zero and the unit reports it.

## Interpreting the evidence

Evidence is archived per run under `<results>/leader-failover-<ts>/`:

- `REPORT.txt` — verdict, `leader_failover_rto_seconds`, `rto_budget_seconds`,
  census (`metad_alive`), `out_of_window_client_errors`.
- `rto.times` — every PUT/GET `(epoch_sec success http)`; RTO is derived from the
  first successful write after the kill epoch.
- `manifest.json` + `verify_all` output — byte-exact durability.
- `kill.epoch` / `disrupt.epoch` — chaos-injection anchors.
- `*.log` — metad / datanode / gateway logs for post-mortem.

A **PASS** line is printed only when RTO ≤ budget, zero out-of-window client
errors, census is intact, and `verify_all` is byte-exact. See
`metadata-disaster-recovery.md` for the complementary metadata backup / restore
coverage.

## Known limitation: EC direct-write on multi-metad

`SOAK_EC=0` (RF=3, the default) is the stable path for measuring the metad
failover objective. The V2.1 direct-EC write path (`writeECShardDirect`) is
currently unstable on a multi-metad raft cluster (allocation can 500 on an EC
bucket), which is a separate data-plane concern from raft leader failover; with
`SOAK_EC=1` the warmup/probe may not reach a clean 200. Tracked independently —
the drill's RTO/graceful-degradation objective is fully exercised by RF=3.
