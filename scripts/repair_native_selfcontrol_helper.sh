#!/usr/bin/env bash
set -euo pipefail

APP_HELPER="/Applications/SelfControl.app/Contents/Library/LaunchServices/org.eyebeam.selfcontrold"
HELPER="/Library/PrivilegedHelperTools/org.eyebeam.selfcontrold"
PLIST="/Library/LaunchDaemons/org.eyebeam.selfcontrold.plist"
LABEL="org.eyebeam.selfcontrold"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run with sudo/root:"
  echo "  sudo ${0}"
  exit 1
fi

if [[ ! -r "${APP_HELPER}" ]]; then
  echo "SelfControl.app helper files were not found under /Applications/SelfControl.app." >&2
  exit 2
fi

install -o root -g wheel -m 0554 "${APP_HELPER}" "${HELPER}"
cat > "${PLIST}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>KeepAlive</key>
  <true/>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>MachServices</key>
  <dict>
    <key>${LABEL}</key>
    <true/>
  </dict>
  <key>Nice</key>
  <integer>5</integer>
  <key>Program</key>
  <string>${HELPER}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${HELPER}</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
</dict>
</plist>
EOF
chown root:wheel "${PLIST}"
chmod 0644 "${PLIST}"

launchctl bootout "system/${LABEL}" >/dev/null 2>&1 || true
launchctl enable "system/${LABEL}" >/dev/null 2>&1 || true
launchctl bootstrap system "${PLIST}"
launchctl kickstart -k "system/${LABEL}"

echo "Native SelfControl helper repaired and loaded:"
echo "  ${PLIST}"
echo "  ${HELPER}"
