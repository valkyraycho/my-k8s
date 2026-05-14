#!/usr/bin/env bash
# Builds a minimal OCI runtime bundle at $1 (default: /tmp/scratch-bundle)
# containing a busybox rootfs and a config.json that runs `/bin/echo`.
# Used by the Phase 0 scratch binary to smoke-test libcontainer integration.

set -euo pipefail

BUNDLE="${1:-/tmp/scratch-bundle}"
ROOTFS="$BUNDLE/rootfs"

if ! command -v busybox >/dev/null 2>&1; then
    echo "Installing busybox-static (one-time)..."
    sudo apt-get install -y -qq busybox-static
fi

rm -rf "$BUNDLE"
mkdir -p "$ROOTFS"/{bin,dev,proc,sys,tmp,etc}
cp "$(command -v busybox)" "$ROOTFS/bin/busybox"
ln -sf busybox "$ROOTFS/bin/sh"
ln -sf busybox "$ROOTFS/bin/echo"

cat > "$BUNDLE/config.json" <<'EOF'
{
  "ociVersion": "1.0.2",
  "process": {
    "terminal": false,
    "user": {"uid": 0, "gid": 0},
    "args": ["/bin/echo", "hello from libcontainer"],
    "env": ["PATH=/bin"],
    "cwd": "/",
    "noNewPrivileges": true,
    "capabilities": {
      "bounding": ["CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"],
      "effective": ["CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"],
      "permitted": ["CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"]
    }
  },
  "root": {"path": "rootfs", "readonly": true},
  "hostname": "scratch",
  "mounts": [
    {"destination": "/proc", "type": "proc", "source": "proc"},
    {"destination": "/dev", "type": "tmpfs", "source": "tmpfs",
     "options": ["nosuid", "strictatime", "mode=755", "size=65536k"]},
    {"destination": "/sys", "type": "sysfs", "source": "sysfs",
     "options": ["nosuid", "noexec", "nodev", "ro"]}
  ],
  "linux": {
    "namespaces": [
      {"type": "pid"}, {"type": "ipc"}, {"type": "uts"},
      {"type": "mount"}, {"type": "network"}
    ]
  }
}
EOF

echo "Bundle ready at $BUNDLE"
