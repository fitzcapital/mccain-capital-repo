#!/usr/bin/env bash
set -euo pipefail

AGENT_LABEL="${AGENT_LABEL:-com.mccaincapital.caffeinate}"
AGENT_PATH="${HOME}/Library/LaunchAgents/${AGENT_LABEL}.plist"

/bin/launchctl bootout "gui/$(id -u)" "${AGENT_PATH}" >/dev/null 2>&1 || true

echo "Stopped ${AGENT_LABEL} for the current session."
echo "The plist is still installed at ${AGENT_PATH}."
echo "If you log in again, launchd may load it again unless you remove the plist."
