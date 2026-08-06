#!/usr/bin/env bash
# run-bench.sh — Program 13 (P3) performance release gate.
#
# Runs the V2.1 §19 DataNode acceptance benchmark (cmd/storage-bench) plus the
# zero-copy read-path micro-benchmark added in Phase 3, and archives results to
# $NUFS_RESULTS_ROOT (default /var/log/nufs-tests) under a timestamped
# directory, following the same result-archival convention as
# deploy/host/mount-helpers.sh (result_dir).
#
# Exit status mirrors cmd/storage-bench: non-zero if any acceptance target
# (20k write ops/s, 50k cached read ops/s, p99 10ms) is missed.

set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root: nufs-core/

RESULTS_ROOT="${NUFS_RESULTS_ROOT:-/var/log/nufs-tests}"
ts="$(date +%Y%m%d-%H%M%S)"
res_dir="${RESULTS_ROOT}/bench-${ts}"
mkdir -p "$res_dir" || {
  res_dir="${TMPDIR:-/tmp}/nufs-bench-${ts}"
  mkdir -p "$res_dir"
  echo "WARN: $RESULTS_ROOT not writable; archiving to $res_dir" >&2
}
echo "benchmark archive: $res_dir"

bench_dir="${BENCH_DIR:-/tmp/nufs-storage-bench}"

fail=0

echo "==> cmd/storage-bench (V2.1 §19 acceptance: 20k write/s, 50k read/s, p99 10ms) =="
if ! go run ./cmd/storage-bench -dir "$bench_dir" | tee "$res_dir/storage-bench.txt"; then
  fail=1
fi

echo "==> zero-copy read-path micro-benchmark (segment, unencrypted/uncompressed) =="
if ! go test -run xxx -bench 'BenchmarkReadRangeFrames_Plain' -benchtime 3s \
    ./datanode/storage/segment/ 2>&1 | tee "$res_dir/read-zerocopy-bench.txt"; then
  fail=1
fi

echo "==> summary =="
grep -E "ns/op|ops/s|MB/s" "$res_dir"/*.txt || true

if [ "$fail" -ne 0 ]; then
  echo "BENCH GATE FAILED — see $res_dir" >&2
else
  echo "BENCH GATE PASSED — see $res_dir"
fi
exit "$fail"
