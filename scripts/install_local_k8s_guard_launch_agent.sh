#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="com.mccaincapital.local-k8s-guard"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"
GUARD_SCRIPT="${ROOT_DIR}/scripts/local_k8s_guard.sh"
STATE_DIR="${HOME}/Library/Application Support/McCain Capital/k8s-guard"

mkdir -p "${HOME}/Library/LaunchAgents" "$STATE_DIR"

cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array><string>/bin/bash</string><string>${GUARD_SCRIPT}</string></array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>StartInterval</key><integer>60</integer>
  <key>ProcessType</key><string>Background</string>
  <key>WorkingDirectory</key><string>${ROOT_DIR}</string>
  <key>StandardOutPath</key><string>${STATE_DIR}/launchd.stdout.log</string>
  <key>StandardErrorPath</key><string>${STATE_DIR}/launchd.stderr.log</string>
</dict>
</plist>
PLIST

chmod 755 "$GUARD_SCRIPT"
chmod 644 "$PLIST_PATH"
launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"
launchctl enable "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
launchctl kickstart -k "gui/$(id -u)/${LABEL}"

echo "Installed ${LABEL}"
echo "Guard status: ${GUARD_SCRIPT} --status"
