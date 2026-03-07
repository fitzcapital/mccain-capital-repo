#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BLOCKER="${REPO_ROOT}/scripts/vanquish_pf_blocker.py"
SUDOERS_PATH="/etc/sudoers.d/com.mccain.vanquishpf"

INSTALL_SUDOERS=0
for arg in "$@"; do
  if [[ "$arg" == "--install-sudoers" ]]; then
    INSTALL_SUDOERS=1
  fi
done

TARGET_USER="${SUDO_USER:-${USER}}"
TARGET_UID="$(id -u "${TARGET_USER}")"
TARGET_HOME="$(dscl . -read "/Users/${TARGET_USER}" NFSHomeDirectory | awk '{print $2}')"
if [[ -z "${TARGET_HOME}" ]]; then
  TARGET_HOME="$(eval echo "~${TARGET_USER}")"
fi
PLIST_PATH="${TARGET_HOME}/Library/LaunchAgents/com.mccain.vanquishpf.plist"
LOG_DIR="${TARGET_HOME}/Library/Logs"
OUT_LOG="${LOG_DIR}/com.mccain.vanquishpf.out.log"
ERR_LOG="${LOG_DIR}/com.mccain.vanquishpf.err.log"

mkdir -p "${TARGET_HOME}/Library/LaunchAgents" "${LOG_DIR}"
chown "${TARGET_USER}" "${TARGET_HOME}/Library/LaunchAgents" "${LOG_DIR}" >/dev/null 2>&1 || true

if [[ ! -f "${BLOCKER}" ]]; then
  echo "missing blocker script: ${BLOCKER}" >&2
  exit 1
fi

cat > "${PLIST_PATH}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.mccain.vanquishpf</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/zsh</string>
    <string>-lc</string>
    <string>sudo -n /usr/bin/python3 "${BLOCKER}" once --no-notify</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>StartInterval</key><integer>10</integer>
  <key>StandardOutPath</key><string>${OUT_LOG}</string>
  <key>StandardErrorPath</key><string>${ERR_LOG}</string>
</dict>
</plist>
EOF
chown "${TARGET_USER}" "${PLIST_PATH}" >/dev/null 2>&1 || true
chmod 644 "${PLIST_PATH}"

if [[ "${INSTALL_SUDOERS}" -eq 1 ]]; then
  if [[ "$(id -u)" -ne 0 ]]; then
    echo "--install-sudoers requires sudo/root" >&2
    exit 1
  fi
  cat > "${SUDOERS_PATH}" <<EOF
${TARGET_USER} ALL=(root) NOPASSWD: /usr/bin/python3 ${BLOCKER} once --no-notify
${TARGET_USER} ALL=(root) NOPASSWD: /usr/bin/python3 ${BLOCKER} clear --no-notify
${TARGET_USER} ALL=(root) NOPASSWD: /usr/bin/python3 ${BLOCKER} status --no-notify
EOF
  chmod 440 "${SUDOERS_PATH}"
  visudo -cf "${SUDOERS_PATH}"
fi

if [[ "$(id -u)" -eq 0 ]]; then
  launchctl bootout "gui/${TARGET_UID}" com.mccain.vanquishpf >/dev/null 2>&1 || true
  launchctl bootstrap "gui/${TARGET_UID}" "${PLIST_PATH}"
  launchctl kickstart -k "gui/${TARGET_UID}/com.mccain.vanquishpf"
else
  launchctl bootout "gui/${TARGET_UID}" com.mccain.vanquishpf >/dev/null 2>&1 || true
  launchctl bootstrap "gui/${TARGET_UID}" "${PLIST_PATH}"
  launchctl kickstart -k "gui/${TARGET_UID}/com.mccain.vanquishpf"
fi

echo "Installed launch agent: ${PLIST_PATH}"
echo "Logs:"
echo "  ${OUT_LOG}"
echo "  ${ERR_LOG}"
if [[ "${INSTALL_SUDOERS}" -eq 0 ]]; then
  echo "NOTE: configure passwordless sudo for this command:"
  echo "  /usr/bin/python3 ${BLOCKER} once --no-notify"
  echo "Then rerun with: sudo ${SCRIPT_DIR}/install_vanquish_pf_launchd.sh --install-sudoers"
fi
