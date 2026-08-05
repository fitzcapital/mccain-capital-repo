#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
STATE_PATH="${REPO_ROOT}/persistent-data/.self_control_enforcement_state.json"
STATUS_PATH="${REPO_ROOT}/persistent-data/.self_control_enforcement_status.json"
DB_PATH="${REPO_ROOT}/persistent-data/journal.db"

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
status_path = root / "persistent-data" / ".self_control_enforcement_status.json"
now = datetime.now(ZoneInfo("America/New_York"))


def parse_dt(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("America/New_York"))
    return dt.astimezone(ZoneInfo("America/New_York"))


completed: list[int] = []
awaiting: list[int] = []
with sqlite3.connect(db_path) as conn:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT *
        FROM self_control_sessions
        WHERE status IN ('active', 'awaiting_journal_unlock')
        ORDER BY id DESC
        """
    ).fetchall()
    for row in rows:
        if row["status"] != "active":
            continue
        planned_end = parse_dt(row["planned_end_at"])
        if not planned_end or planned_end > now:
            continue
        next_status = "awaiting_journal_unlock" if str(row["unlock_requirement"] or "").strip() else "completed"
        conn.execute(
            """
            UPDATE self_control_sessions
            SET status = ?,
                ended_at = ?,
                completed_minutes = planned_minutes,
                updated_at = ?
            WHERE id = ?
            """,
            (next_status, now.isoformat(), now.isoformat(), int(row["id"])),
        )
        conn.execute(
            """
            INSERT INTO self_control_events (session_id, event_type, event_at, detail_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                int(row["id"]),
                "script_recovered",
                now.isoformat(),
                json.dumps({"status": next_status, "completed_minutes": int(row["planned_minutes"] or 0)}),
            ),
        )
        if next_status == "awaiting_journal_unlock":
            awaiting.append(int(row["id"]))
        else:
            completed.append(int(row["id"]))

    active = conn.execute(
        """
        SELECT *
        FROM self_control_sessions
        WHERE status IN ('active', 'awaiting_journal_unlock')
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()

if active:
    blocked_domains = json.loads(active["blocked_domains_json"] or "[]")
    blocked_categories = json.loads(active["blocked_categories_json"] or "[]")
    state_payload = {
        "updated_at": now.isoformat(),
        "active": True,
        "status": active["status"],
        "session_id": int(active["id"]),
        "label": str(active["label"] or ""),
        "strict_mode": bool(active["strict_mode"]),
        "planned_end_at": str(active["planned_end_at"] or ""),
        "unlock_requirement": str(active["unlock_requirement"] or ""),
        "blocked_domains": blocked_domains,
        "blocked_categories": blocked_categories,
        "provider_mode": "hosts",
    }
else:
    state_payload = {
        "updated_at": now.isoformat(),
        "active": False,
        "status": "idle",
        "session_id": 0,
        "label": "",
        "strict_mode": False,
        "planned_end_at": "",
        "unlock_requirement": "",
        "blocked_domains": [],
        "blocked_categories": [],
        "provider_mode": "hosts",
    }

state_path.write_text(json.dumps(state_payload, ensure_ascii=False, indent=2), encoding="utf-8")

if status_path.exists() and not active:
    try:
        status_payload = json.loads(status_path.read_text(encoding="utf-8"))
    except Exception:
        status_payload = {}
    if isinstance(status_payload, dict):
        status_payload.update(
            {
                "updated_at": now.isoformat(),
                "active": False,
                "managed_domains": [],
                "managed_count": 0,
                "session_id": 0,
                "session_label": "",
                "planned_end_at": "",
                "last_checked_at": now.isoformat(),
            }
        )
        try:
            status_path.write_text(json.dumps(status_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except PermissionError:
            pass

if completed or awaiting:
    print(f"repaired completed={completed} awaiting_unlock={awaiting}")
else:
    print("repair no-op")
PY
