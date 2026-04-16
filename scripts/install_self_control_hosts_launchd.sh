#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BLOCKER="${REPO_ROOT}/scripts/self_control_site_blocker.py"
PLIST_PATH="/Library/LaunchDaemons/com.mccain.selfcontrolhosts.plist"
STATE_PATH="${REPO_ROOT}/persistent-data/.self_control_enforcement_state.json"
STATUS_PATH="${REPO_ROOT}/persistent-data/.self_control_enforcement_status.json"
LOG_DIR="${REPO_ROOT}/persistent-data/logs"
OUT_LOG="${LOG_DIR}/self-control-hosts.out.log"
ERR_LOG="${LOG_DIR}/self-control-hosts.err.log"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run with sudo/root." >&2
  exit 1
fi

mkdir -p "${LOG_DIR}"

cat > "${PLIST_PATH}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.mccain.selfcontrolhosts</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/python3</string>
    <string>${BLOCKER}</string>
    <string>daemon</string>
    <string>--state-path</string>
    <string>${STATE_PATH}</string>
    <string>--status-path</string>
    <string>${STATUS_PATH}</string>
    <string>--no-notify</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>WorkingDirectory</key><string>${REPO_ROOT}</string>
  <key>StandardOutPath</key><string>${OUT_LOG}</string>
  <key>StandardErrorPath</key><string>${ERR_LOG}</string>
</dict>
</plist>
EOF

chmod 644 "${PLIST_PATH}"
launchctl bootout system/com.mccain.selfcontrolhosts >/dev/null 2>&1 || true
launchctl bootstrap system "${PLIST_PATH}"
launchctl enable system/com.mccain.selfcontrolhosts >/dev/null 2>&1 || true
launchctl kickstart -k system/com.mccain.selfcontrolhosts

echo "Installed launch daemon: ${PLIST_PATH}"
echo "State file: ${STATE_PATH}"
echo "Status file: ${STATUS_PATH}"
echo "Logs:"
echo "  ${OUT_LOG}"
echo "  ${ERR_LOG}"
