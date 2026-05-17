#!/usr/bin/env bash
# Wipe kubelet state between dev iterations.
#
# Phase 1 has no persistence: the kubelet assumes a clean state dir on
# startup. If a previous run was killed before graceful shutdown,
# libcontainer state files + orphan container processes can linger.
# This script clears them so the next kubelet run starts clean.

set -euo pipefail

STATE_DIR="${1:-/var/lib/my-k8s/state}"

echo "killing any orphan busybox processes..."
# All our containers run /bin/busybox (pause + apps). In a dedicated dev VM
# this is safe; outside, this would be too aggressive.
pkill -9 -f /bin/busybox 2>/dev/null || true

# Give the kernel a moment to reap them.
sleep 0.5

echo "wiping state dir: $STATE_DIR"
if [[ -d "$STATE_DIR" ]]; then
    rm -rf "$STATE_DIR"/*
fi

echo "clean."