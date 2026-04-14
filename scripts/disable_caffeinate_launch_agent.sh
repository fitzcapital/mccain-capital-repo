#!/usr/bin/env bash
set -euo pipefail

AGENT_LABEL="${AGENT_LABEL:-com.mccaincapital.caffeinate}"
AGENT_PATH="${HOME}/Library/LaunchAgents/${AGENT_LABEL}.plist"
GUI_DOMAIN="gui/$(id -u)"

/bin/launchctl disable "${GUI_DOMAIN}/${AGENT_LABEL}" >/dev/null 2>&1 || true
/bin/launchctl bootout "${GUI_DOMAIN}" "${AGENT_PATH}" >/dev/null 2>&1 || true

echo "Disabled ${AGENT_LABEL}"
echo "LaunchAgent remains installed at ${AGENT_PATH}"
echo "Re-enable it with scripts/install_caffeinate_launch_agent.sh"
