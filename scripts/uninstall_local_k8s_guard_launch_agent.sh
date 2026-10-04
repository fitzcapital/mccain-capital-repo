#!/usr/bin/env bash
set -euo pipefail

LABEL="com.mccaincapital.local-k8s-guard"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"

launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true
if [[ -f "$PLIST_PATH" ]]; then
  mv "$PLIST_PATH" "${HOME}/.Trash/${LABEL}.plist.$(date +%s)"
fi
echo "Uninstalled ${LABEL}; guard logs and state were retained."
