#!/usr/bin/env bash
# myctl: read kubelet's debug snapshot. THROWAWAY — Phase 2 will replace
# this with proper API server endpoints + a real kubectl-shaped client.
#
# Usage:
#   myctl                                  # full snapshot
#   myctl '.pods[].name'                   # just pod names
#   myctl '.pods[] | select(.name=="web")' # one pod
#   myctl '.pods[].containers[].restart_count'

set -euo pipefail

DUMP="${MY_K8S_DEBUG:-/var/lib/my-k8s/state/debug.json}"

if [[ ! -f "$DUMP" ]]; then
    echo "no debug snapshot at $DUMP (kubelet not running, or hasn't completed first tick yet)" >&2
    exit 1
fi

filter="${1:-.}"
if command -v jq >/dev/null 2>&1; then
    sudo cat "$DUMP" | jq "$filter"
else
    sudo cat "$DUMP"
fi