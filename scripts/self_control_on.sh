#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
STATE_PATH="${REPO_ROOT}/persistent-data/.self_control_enforcement_state.json"
DB_PATH="${REPO_ROOT}/persistent-data/journal.db"
MINUTES="${1:-60}"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run with sudo/root:"
  echo "  sudo ${0} [minutes]"
  exit 1
fi

if ! [[ "${MINUTES}" =~ ^[0-9]+$ ]] || [[ "${MINUTES}" -le 0 ]]; then
  echo "Usage: sudo ${0} [positive_minutes]" >&2
  exit 2
fi

"${SCRIPT_DIR}/self_control_repair_state.sh" >/dev/null
"${SCRIPT_DIR}/install_self_control_hosts_launchd.sh"

/usr/bin/python3 - "${REPO_ROOT}" "${MINUTES}" <<'PY'
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import sqlite3
import sys

root = Path(sys.argv[1])
minutes = int(sys.argv[2])
db_path = root / "persistent-data" / "journal.db"
state_path = root / "persistent-data" / ".self_control_enforcement_state.json"

now = datetime.now(ZoneInfo("America/New_York"))
end = now + timedelta(minutes=minutes)
domains = [
    "api.vanquishtrader.com",
    "app.vanquishtrader.com",
    "trade.vanquishtrader.com",
    "tradingview.com",
    "vanquishtrader.com",
    "www.trade.vanquishtrader.com",
    "www.tradingview.com",
    "www.vanquishtrader.com",
]
categories = ["Trading"]

with sqlite3.connect(db_path) as conn:
    active = conn.execute(
        """
        SELECT id, label, planned_end_at
        FROM self_control_sessions
        WHERE status IN ('active', 'awaiting_journal_unlock')
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if active:
        print(
            "Refusing to replace active Self-Control session "
            f"{active[0]} ({active[1]}, ends {active[2]})."
        )
        print("Use scripts/self_control_reapply.sh to re-apply the current session.")
        print("Use scripts/self_control_restart.sh [minutes] to cancel and restart intentionally.")
        raise SystemExit(3)

    cur = conn.execute(
        """
        INSERT INTO self_control_sessions (
            preset_slug, label, intent_note, status, strict_mode, started_at, planned_end_at,
            planned_minutes, completed_minutes, blocked_categories_json, blocked_domains_json,
            source_rule_slug, unlock_requirement, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "manual-on",
            "Manual Self-Control On",
            "Started from scripts/self_control_on.sh.",
            "active",
            1,
            now.isoformat(),
            end.isoformat(),
            minutes,
            0,
            json.dumps(categories),
            json.dumps(domains),
            "",
            "",
            now.isoformat(),
            now.isoformat(),
        ),
    )
    session_id = int(cur.lastrowid)
    conn.execute(
        """
        INSERT INTO self_control_events (session_id, event_type, event_at, detail_json)
        VALUES (?, ?, ?, ?)
        """,
        (
            session_id,
            "manual_on",
            now.isoformat(),
            json.dumps({"blocked_domains": domains, "planned_minutes": minutes}),
        ),
    )

payload = {
    "updated_at": now.isoformat(),
    "active": True,
    "status": "active",
    "session_id": session_id,
    "label": "Manual Self-Control On",
    "strict_mode": True,
    "planned_end_at": end.isoformat(),
    "unlock_requirement": "",
    "blocked_domains": domains,
    "blocked_categories": categories,
    "provider_mode": "hosts",
}
state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"Started Self-Control session {session_id} for {minutes} minutes, until {end.isoformat()}")
PY

if [[ -r "${SCRIPT_DIR}/self_control_site_blocker.py" ]]; then
  /usr/bin/python3 "${SCRIPT_DIR}/self_control_site_blocker.py" once \
    --state-path "${STATE_PATH}" \
    --status-path "${REPO_ROOT}/persistent-data/.self_control_enforcement_status.json" \
    --no-notify || true
fi

echo "Repo Self-Control block is on."
