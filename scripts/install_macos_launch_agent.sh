#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
ROOT_DIR="$(cd "$(dirname "${SCRIPT_PATH}")/.." && pwd)"
AGENT_LABEL="${AGENT_LABEL:-com.mccaincapital.app}"
AGENT_PATH="${HOME}/Library/LaunchAgents/${AGENT_LABEL}.plist"
START_SCRIPT="${ROOT_DIR}/scripts/start_mccain_capital_on_login.sh"
LOG_DIR="${ROOT_DIR}/persistent-data/logs"

mkdir -p "${HOME}/Library/LaunchAgents" "${LOG_DIR}"

cat > "${AGENT_PATH}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>${AGENT_LABEL}</string>

    <key>ProgramArguments</key>
    <array>
      <string>/bin/bash</string>
      <string>${START_SCRIPT}</string>
    </array>

    <key>EnvironmentVariables</key>
    <dict>
      <key>PATH</key>
      <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    </dict>

    <key>RunAtLoad</key>
    <true/>

    <key>KeepAlive</key>
    <false/>

    <key>ProcessType</key>
    <string>Background</string>

    <key>WorkingDirectory</key>
    <string>${ROOT_DIR}</string>

    <key>StandardOutPath</key>
    <string>${LOG_DIR}/launchd.stdout.log</string>

    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/launchd.stderr.log</string>
  </dict>
</plist>
PLIST

chmod 644 "${AGENT_PATH}"
chmod +x "${START_SCRIPT}"

/bin/launchctl bootout "gui/$(id -u)" "${AGENT_PATH}" >/dev/null 2>&1 || true
/bin/launchctl bootstrap "gui/$(id -u)" "${AGENT_PATH}"
/bin/launchctl enable "gui/$(id -u)/${AGENT_LABEL}" >/dev/null 2>&1 || true
/bin/launchctl kickstart -k "gui/$(id -u)/${AGENT_LABEL}"

echo "Installed ${AGENT_LABEL}"
echo "LaunchAgent: ${AGENT_PATH}"
echo "Logs: ${LOG_DIR}/launchd.stdout.log and ${LOG_DIR}/launchd.stderr.log"
