#!/usr/bin/env bash
set -euo pipefail

PLIST_PATH="/Library/LaunchDaemons/com.mccain.selfcontrolpf.plist"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run with sudo/root." >&2
  exit 1
fi

launchctl bootout system/com.mccain.selfcontrolpf >/dev/null 2>&1 || true
rm -f "${PLIST_PATH}"

echo "Uninstalled com.mccain.selfcontrolpf launch daemon."
