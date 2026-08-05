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

"${SCRIPT_DIR}/self_control_repair_state.sh" >/dev/null
"${SCRIPT_DIR}/install_self_control_hosts_launchd.sh"

/usr/bin/python3 - "${REPO_ROOT}" <<'PY'
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import sqlite3
import sys

root = Path(sys.argv[1])
db_path = root / "persistent-data" / "journal.db"
state_path = root / "persistent-data" / ".self_control_enforcement_state.json"

with sqlite3.connect(db_path) as conn:
    conn.row_factory = sqlite3.Row
    session = conn.execute(
        """
        SELECT *
        FROM self_control_sessions
        WHERE status IN ('active', 'awaiting_journal_unlock')
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()

if not session:
    print("No active Self-Control session found. Start one in the app or run self_control_on.sh.")
    raise SystemExit(3)

blocked_domains = json.loads(session["blocked_domains_json"] or "[]")
blocked_categories = json.loads(session["blocked_categories_json"] or "[]")
payload = {
    "updated_at": datetime.now(ZoneInfo("America/New_York")).isoformat(),
    "active": True,
    "status": session["status"],
    "session_id": int(session["id"]),
    "label": str(session["label"] or ""),
    "strict_mode": bool(session["strict_mode"]),
    "planned_end_at": str(session["planned_end_at"] or ""),
    "unlock_requirement": str(session["unlock_requirement"] or ""),
    "blocked_domains": blocked_domains,
    "blocked_categories": blocked_categories,
    "provider_mode": "hosts",
}
state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"Re-applied session {session['id']} ({session['label']})")
PY

if [[ -r "${SCRIPT_DIR}/self_control_site_blocker.py" ]]; then
  /usr/bin/python3 "${SCRIPT_DIR}/self_control_site_blocker.py" once \
    --state-path "${STATE_PATH}" \
    --status-path "${STATUS_PATH}" \
    --no-notify
fi

dscacheutil -flushcache >/dev/null 2>&1 || true
killall -HUP mDNSResponder >/dev/null 2>&1 || true

echo "Repo Self-Control block re-applied for the active app session."
