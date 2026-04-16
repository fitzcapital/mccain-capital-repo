#!/usr/bin/env bash
set -euo pipefail

PLIST_PATH="/Library/LaunchDaemons/com.mccain.selfcontrolhosts.plist"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run with sudo/root." >&2
  exit 1
fi

launchctl bootout system/com.mccain.selfcontrolhosts >/dev/null 2>&1 || true
rm -f "${PLIST_PATH}"

echo "Uninstalled com.mccain.selfcontrolhosts launch daemon."
