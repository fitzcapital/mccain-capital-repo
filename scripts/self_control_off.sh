#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
STATE_PATH="${REPO_ROOT}/persistent-data/.self_control_enforcement_state.json"
STATUS_PATH="${REPO_ROOT}/persistent-data/.self_control_enforcement_status.json"
DB_PATH="${REPO_ROOT}/persistent-data/journal.db"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run with sudo/root:"
  echo "  sudo ${0}"
  exit 1
fi

echo "Turning repo Self-Control block off..."

if [[ -r "${SCRIPT_DIR}/self_control_site_blocker.py" ]]; then
  /usr/bin/python3 "${SCRIPT_DIR}/self_control_site_blocker.py" clear \
    --state-path "${STATE_PATH}" \
    --status-path "${STATUS_PATH}" \
    --no-notify || true
fi

mkdir -p "$(dirname "${STATE_PATH}")"
cat > "${STATE_PATH}" <<EOF
{
  "updated_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "active": false,
  "status": "manual_off",
  "session_id": 0,
  "label": "",
  "strict_mode": false,
  "planned_end_at": "",
  "unlock_requirement": "",
  "blocked_domains": [],
  "blocked_categories": [],
  "provider_mode": "hosts"
}
EOF

if [[ -f "${DB_PATH}" ]] && command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 "${DB_PATH}" <<'SQL'
UPDATE self_control_sessions
SET status = 'cancelled',
    ended_at = datetime('now'),
    cancel_reason = CASE
      WHEN trim(cancel_reason) = '' THEN 'Manual Self-Control off.'
      ELSE cancel_reason
    END,
    override_reason = CASE
      WHEN trim(override_reason) = '' THEN 'Manual Self-Control off.'
      ELSE override_reason
    END,
    updated_at = datetime('now')
WHERE status IN ('active', 'awaiting_journal_unlock');
SQL
fi

if [[ -r "${SCRIPT_DIR}/self_control_site_blocker.py" ]]; then
  /usr/bin/python3 "${SCRIPT_DIR}/self_control_site_blocker.py" clear \
    --state-path "${STATE_PATH}" \
    --status-path "${STATUS_PATH}" \
    --no-notify || true
fi

dscacheutil -flushcache >/dev/null 2>&1 || true
killall -HUP mDNSResponder >/dev/null 2>&1 || true

echo "Repo Self-Control block is off. The daemon remains installed for future sessions."
