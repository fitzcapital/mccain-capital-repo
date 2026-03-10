#!/usr/bin/env bash
set -euo pipefail

APP_CONTAINER="${1:-mccain-capital-app}"
PERSIST_PATH="${2:-$(pwd)/persistent-data}"
MACHINE_RAW="$HOME/.local/share/containers/podman/machine/libkrun/podman-machine-default-arm64.raw"

echo "[podman] usage before cleanup"
podman system df
echo

echo "[podman] pruning unused images/containers/build cache"
podman image prune -a -f
podman container prune -f
podman builder prune -f
echo

echo "[podman] usage after cleanup"
podman system df
echo

if [ -f "$MACHINE_RAW" ]; then
  echo "[podman] machine raw disk file"
  ls -lh "$MACHINE_RAW"
  echo
fi

if [ -d "$PERSIST_PATH" ]; then
  echo "[app] persistent-data footprint"
  du -sh "$PERSIST_PATH" "$PERSIST_PATH/playwright-browsers" "$PERSIST_PATH/uploads" "$PERSIST_PATH/books" 2>/dev/null || true
  echo
fi

if podman ps --format '{{.Names}}' | rg -x "$APP_CONTAINER" >/dev/null 2>&1; then
  echo "[app] container mount + playwright env"
  podman inspect "$APP_CONTAINER" --format '{{range .Mounts}}{{println .Type "\t" .Source "\t" .Destination}}{{end}}'
  podman inspect "$APP_CONTAINER" --format '{{range .Config.Env}}{{println .}}{{end}}' | rg '^PLAYWRIGHT_BROWSERS_PATH=|^PLAYWRIGHT_INSTALL_ON_DEMAND=' || true
fi
