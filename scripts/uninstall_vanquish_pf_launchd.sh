#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BLOCKER="${REPO_ROOT}/scripts/vanquish_pf_blocker.py"
SUDOERS_PATH="/etc/sudoers.d/com.mccain.vanquishpf"

TARGET_USER="${SUDO_USER:-${USER}}"
TARGET_UID="$(id -u "${TARGET_USER}")"
TARGET_HOME="$(dscl . -read "/Users/${TARGET_USER}" NFSHomeDirectory | awk '{print $2}')"
if [[ -z "${TARGET_HOME}" ]]; then
  TARGET_HOME="$(eval echo "~${TARGET_USER}")"
fi
PLIST_PATH="${TARGET_HOME}/Library/LaunchAgents/com.mccain.vanquishpf.plist"

launchctl bootout "gui/${TARGET_UID}" com.mccain.vanquishpf >/dev/null 2>&1 || true
rm -f "${PLIST_PATH}"

if [[ "${1:-}" == "--remove-sudoers" ]]; then
  if [[ "$(id -u)" -ne 0 ]]; then
    echo "--remove-sudoers requires sudo/root" >&2
    exit 1
  fi
  rm -f "${SUDOERS_PATH}"
fi

if [[ "$(id -u)" -eq 0 ]]; then
  /usr/bin/python3 "${BLOCKER}" clear --no-notify || true
else
  sudo -n /usr/bin/python3 "${BLOCKER}" clear --no-notify || true
fi

echo "Uninstalled com.mccain.vanquishpf launch agent."
