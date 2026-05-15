#!/usr/bin/env bash
# Prepare the shared, read-only base rootfs used by every container in Phase 1.
#
# Idempotent: safe to re-run. Wipes the destination first.
#
# Usage: sudo bash scripts/prepare-rootfs.sh [TARGET]
#   TARGET defaults to /var/lib/my-k8s/rootfs-base

set -euo pipefail

TARGET="${1:-/var/lib/my-k8s/rootfs-base}"

# busybox-static gives us a statically-linked /bin/busybox with no libc deps.
if ! dpkg -l busybox-static >/dev/null 2>&1; then
    echo "Installing busybox-static..."
    apt-get update -qq
    apt-get install -y -qq busybox-static
fi

BUSYBOX="$(command -v busybox)"
if [[ -z "$BUSYBOX" ]]; then
    echo "ERROR: busybox not found on PATH after install" >&2
    exit 1
fi

echo "Building rootfs at $TARGET (using $BUSYBOX)"

# Wipe + recreate. mkdir -p tolerates the parent /var/lib/my-k8s not existing.
rm -rf "$TARGET"
mkdir -p "$TARGET"/{bin,dev,proc,sys,tmp,etc}

# Copy the binary itself, then symlink every applet name we want available
# inside containers as /bin/<name>.
cp "$BUSYBOX" "$TARGET/bin/busybox"

for applet in sh httpd sleep echo tail wget cat ls ps mkdir rm cp mv true false; do
    ln -sf busybox "$TARGET/bin/$applet"
done

# Minimal /etc — busybox is happy without much, but let's avoid weird DNS errors
# from containers that try to resolve hostnames.
cat > "$TARGET/etc/hosts" <<'EOF'
127.0.0.1   localhost
::1         localhost
EOF

cat > "$TARGET/etc/resolv.conf" <<'EOF'
nameserver 1.1.1.1
nameserver 8.8.8.8
EOF

echo "Done. Rootfs contents:"
ls -la "$TARGET/bin" | head -20
echo "Rootfs size: $(du -sh "$TARGET" | cut -f1)"