#!/usr/bin/env bash
set -euo pipefail

AGENT_LABEL="${AGENT_LABEL:-com.mccaincapital.caffeinate}"
AGENT_PATH="${HOME}/Library/LaunchAgents/${AGENT_LABEL}.plist"

/bin/launchctl bootout "gui/$(id -u)" "${AGENT_PATH}" >/dev/null 2>&1 || true
rm -f "${AGENT_PATH}"

echo "Removed ${AGENT_LABEL}"
echo "LaunchAgent deleted: ${AGENT_PATH}"
