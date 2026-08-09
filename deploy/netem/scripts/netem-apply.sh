#!/usr/bin/env bash
# netem-apply.sh - Network fault injection via tc netem
# Usage: netem-apply.sh <command> [args]
#
# Commands:
#   loss <percentage> [duration_sec]  - Inject packet loss
#   latency <ms> [duration_sec]       - Inject latency
#   partition                          - Create partition (drop all traffic)
#   reset                              - Remove all tc rules

set -euo pipefail

command="${1:-help}"
shift || true

case "$command" in
    loss)
        percentage="${1:-50}"
        duration="${2:-0}"
        echo "[netem] Injecting ${percentage}% packet loss..."
        tc qdisc add dev eth0 root netem loss "${percentage}%"
        if [ "$duration" -gt 0 ]; then
            sleep "$duration"
            tc qdisc del dev eth0 root
            echo "[netem] Loss removed after ${duration}s"
        fi
        ;;
    latency)
        delay="${1:-100ms}"
        duration="${2:-0}"
        echo "[netem] Injecting ${delay} latency..."
        tc qdisc add dev eth0 root netem delay "$delay"
        if [ "$duration" -gt 0 ]; then
            sleep "$duration"
            tc qdisc del dev eth0 root
            echo "[netem] Latency removed after ${duration}s"
        fi
        ;;
    partition)
        echo "[netem] Creating partition (drop all traffic)..."
        tc qdisc add dev eth0 root netem drop 100%
        # Wait indefinitely until killed
        echo "[netem] Partition active - waiting for kill signal..."
        while true; do sleep 3600; done
        ;;
    reset)
        echo "[netem] Resetting tc rules..."
        tc qdisc del dev eth0 root 2>/dev/null || true
        echo "[netem] Reset complete"
        ;;
    help|*)
        echo "Usage: $0 <loss|latency|partition|reset> [args]"
        exit 1
        ;;
esac
