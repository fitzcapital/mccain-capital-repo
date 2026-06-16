#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

HOSTS_LABEL="com.mccain.selfcontrolhosts"
PF_LABEL="com.mccain.selfcontrolpf"
SELFCONTROL_LABEL="org.eyebeam.selfcontrold"
HOSTS_PLIST="/Library/LaunchDaemons/${HOSTS_LABEL}.plist"
PF_PLIST="/Library/LaunchDaemons/${PF_LABEL}.plist"
SELFCONTROL_PLIST="/Library/LaunchDaemons/${SELFCONTROL_LABEL}.plist"
SELFCONTROL_HELPER="/Library/PrivilegedHelperTools/${SELFCONTROL_LABEL}"
SELFCONTROL_PF_ANCHOR="/etc/pf.anchors/org.eyebeam"
PF_CONF="/etc/pf.conf"
STATE_PATH="${REPO_ROOT}/persistent-data/.self_control_enforcement_state.json"
STATUS_PATH="${REPO_ROOT}/persistent-data/.self_control_enforcement_status.json"
DB_PATH="${REPO_ROOT}/persistent-data/journal.db"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run with sudo/root:"
  echo "  sudo ${0}"
  exit 1
fi

echo "Emergency stopping Self-Control Mode..."

launchctl bootout "system/${HOSTS_LABEL}" >/dev/null 2>&1 || true
launchctl disable "system/${HOSTS_LABEL}" >/dev/null 2>&1 || true
launchctl bootout "system/${PF_LABEL}" >/dev/null 2>&1 || true
launchctl disable "system/${PF_LABEL}" >/dev/null 2>&1 || true
launchctl bootout "system/${SELFCONTROL_LABEL}" >/dev/null 2>&1 || true
launchctl disable "system/${SELFCONTROL_LABEL}" >/dev/null 2>&1 || true
launchctl kill SIGKILL "system/${SELFCONTROL_LABEL}" >/dev/null 2>&1 || true

pkill -f "${SELFCONTROL_HELPER}" >/dev/null 2>&1 || true
pkill -f "SelfControl.app" >/dev/null 2>&1 || true

rm -f "${HOSTS_PLIST}" "${PF_PLIST}" "${SELFCONTROL_PLIST}" "${SELFCONTROL_HELPER}"

if [[ -r "${SCRIPT_DIR}/self_control_site_blocker.py" ]]; then
  /usr/bin/python3 "${SCRIPT_DIR}/self_control_site_blocker.py" clear \
    --state-path "${STATE_PATH}" \
    --status-path "${STATUS_PATH}" \
    --no-notify || true
fi

if [[ -r "${SCRIPT_DIR}/self_control_pf_blocker.py" ]]; then
  /usr/bin/python3 "${SCRIPT_DIR}/self_control_pf_blocker.py" clear \
    --state-path "${STATE_PATH}" \
    --status-path "${STATUS_PATH}" \
    --no-notify || true
fi

/usr/bin/python3 - /etc/hosts <<'PY'
from __future__ import annotations

from pathlib import Path
import sys

path = Path(sys.argv[1])
content = path.read_text(encoding="utf-8")
marker_pairs = [
    ("# BEGIN MCCAIN SELF CONTROL BLOCK", "# END MCCAIN SELF CONTROL BLOCK"),
    ("# BEGIN SELFCONTROL BLOCK", "# END SELFCONTROL BLOCK"),
]

changed = False
for begin, end in marker_pairs:
    while begin in content and end in content:
        start = content.find(begin)
        finish = content.find(end, start)
        if finish < 0:
            break
        finish += len(end)
        if finish < len(content) and content[finish : finish + 1] == "\n":
            finish += 1
        content = content[:start] + content[finish:]
        changed = True

if changed:
    path.write_text(content, encoding="utf-8")
PY

/usr/bin/python3 - "${PF_CONF}" "${SELFCONTROL_PF_ANCHOR}" <<'PY'
from __future__ import annotations

from pathlib import Path
import sys

pf_conf = Path(sys.argv[1])
anchor = Path(sys.argv[2])

if anchor.exists():
    anchor.write_text("# org.eyebeam SelfControl block cleared by emergency stop\n", encoding="utf-8")

content = pf_conf.read_text(encoding="utf-8")
lines = []
changed = False
skip_next = False
for line in content.splitlines(keepends=True):
    stripped = line.strip()
    if skip_next:
        skip_next = False
        changed = True
        continue
    if stripped == 'anchor "org.eyebeam"':
        skip_next = True
        changed = True
        continue
    if stripped == 'load anchor "org.eyebeam" from "/etc/pf.anchors/org.eyebeam"':
        changed = True
        continue
    lines.append(line)

if changed:
    pf_conf.write_text("".join(lines), encoding="utf-8")
PY

pfctl -f "${PF_CONF}" >/dev/null 2>&1 || true

mkdir -p "$(dirname "${STATE_PATH}")"
cat > "${STATE_PATH}" <<EOF
{
  "updated_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "active": false,
  "status": "emergency_stopped",
  "session_id": 0,
  "label": "",
  "strict_mode": false,
  "planned_end_at": "",
  "unlock_requirement": "",
  "blocked_domains": [],
  "blocked_categories": [],
  "provider_mode": "emergency_stop"
}
EOF

cat > "${STATUS_PATH}" <<EOF
{
  "updated_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "installed": false,
  "active": false,
  "mode": "emergency_stop",
  "managed_domains": [],
  "managed_count": 0,
  "session_id": 0,
  "session_label": "",
  "planned_end_at": "",
  "last_checked_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "last_error": ""
}
EOF

if [[ -f "${DB_PATH}" ]] && command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 "${DB_PATH}" <<'SQL'
UPDATE self_control_sessions
SET status = 'cancelled',
    ended_at = datetime('now'),
    cancel_reason = CASE
      WHEN trim(cancel_reason) = '' THEN 'Emergency fail-safe stop.'
      ELSE cancel_reason
    END,
    override_reason = CASE
      WHEN trim(override_reason) = '' THEN 'Emergency fail-safe stop.'
      ELSE override_reason
    END,
    updated_at = datetime('now')
WHERE status IN ('active', 'awaiting_journal_unlock');
SQL
fi

dscacheutil -flushcache >/dev/null 2>&1 || true
killall -HUP mDNSResponder >/dev/null 2>&1 || true

echo "Self-Control Mode emergency stop complete."
echo "LaunchDaemons removed:"
echo "  ${HOSTS_PLIST}"
echo "  ${PF_PLIST}"
echo "  ${SELFCONTROL_PLIST}"
echo "Privileged helper removed:"
echo "  ${SELFCONTROL_HELPER}"
echo "SelfControl PF anchor cleared:"
echo "  ${SELFCONTROL_PF_ANCHOR}"
echo "Network blocks cleared and active sessions marked cancelled."
