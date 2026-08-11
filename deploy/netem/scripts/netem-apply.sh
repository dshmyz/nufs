#!/usr/bin/env bash
# netem-apply.sh - Network fault injection via tc netem
# Usage: netem-apply.sh <command> [args]
#
# Commands:
#   loss <percentage> [duration_sec]  - Inject packet loss
#   latency <ms> [duration_sec]       - Inject latency
#   losslat <loss%> <ms> [duration]   - Inject combined packet loss + latency
#   partition                          - Create partition (drop all traffic)
#   reset                              - Remove all tc rules
#
# NOTE: running this requires CAP_NET_ADMIN in the target network namespace.
# The sidecar containers share the target's netns via --net container:<target>,
# so they MUST be launched with --cap-add=NET_ADMIN.

set -euo pipefail

command="${1:-help}"
shift || true

# apply_netem runs a single atomic tc qdisc add, replacing any existing root
# qdisc. Invoked from the cleanup trap we define at the end of the script.
apply_netem() {
    # shellcheck disable=SC2064
    trap cleanup_netem TERM INT HUP
    tc qdisc add dev eth0 root netem "$@"
}

cleanup_netem() {
    echo "[netem] Removing netem qdisc..."
    tc qdisc del dev eth0 root 2>/dev/null || true
    echo "[netem] Netem removed"
    exit 0
}

case "$command" in
    loss)
        percentage="${1:-50}"
        duration="${2:-0}"
        echo "[netem] Injecting ${percentage}% packet loss..."
        apply_netem loss "${percentage}%"
        if [ "$duration" -gt 0 ]; then
            sleep "$duration"
            cleanup_netem || true
            echo "[netem] Loss removed after ${duration}s"
        else
            # Hold indefinitely until killed (trap cleans up the qdisc).
            while true; do sleep 3600; done
        fi
        ;;
    latency)
        delay="${1:-100ms}"
        duration="${2:-0}"
        echo "[netem] Injecting ${delay} latency..."
        apply_netem delay "$delay"
        if [ "$duration" -gt 0 ]; then
            sleep "$duration"
            cleanup_netem || true
            echo "[netem] Latency removed after ${duration}s"
        else
            while true; do sleep 3600; done
        fi
        ;;
    losslat)
        percentage="${1:-50}"
        delay="${2:-100ms}"
        duration="${3:-0}"
        echo "[netem] Injecting ${percentage}% packet loss + ${delay} latency..."
        apply_netem loss "${percentage}%" delay "$delay"
        if [ "$duration" -gt 0 ]; then
            sleep "$duration"
            cleanup_netem || true
            echo "[netem] loss+latency removed after ${duration}s"
        else
            while true; do sleep 3600; done
        fi
        ;;
    partition)
        echo "[netem] Creating partition (drop all traffic)..."
        apply_netem drop 100%
        # Wait indefinitely until killed; the TERM trap above removes the qdisc.
        echo "[netem] Partition active - waiting for kill signal..."
        while true; do sleep 3600; done
        ;;
    reset)
        echo "[netem] Resetting tc rules..."
        tc qdisc del dev eth0 root 2>/dev/null || true
        echo "[netem] Reset complete"
        ;;
    help|*)
        echo "Usage: $0 <loss|latency|losslat|partition|reset> [args]"
        exit 1
        ;;
esac
