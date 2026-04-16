"""Self-control repository functions."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from mccain_capital.migrations import run_migrations
from mccain_capital.runtime import DB_PATH, db, now_iso


def ensure_self_control_schema() -> None:
    run_migrations(DB_PATH)


def _loads(raw: Any, default: Any) -> Any:
    text = str(raw or "").strip()
    if not text:
        return default
    try:
        parsed = json.loads(text)
    except Exception:
        return default
    return parsed if isinstance(parsed, type(default)) else default


def list_blocked_sites() -> List[Dict[str, Any]]:
    ensure_self_control_schema()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM self_control_blocked_sites
            ORDER BY category ASC, enabled DESC, domain ASC
            """
        ).fetchall()
    return [dict(r) for r in rows]


def create_blocked_site(domain: str, category: str, source: str = "custom") -> int:
    ensure_self_control_schema()
    created = now_iso()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO self_control_blocked_sites (
              domain, category, enabled, source, created_at, updated_at
            )
            VALUES (?, ?, 1, ?, ?, ?)
            ON CONFLICT(domain) DO UPDATE SET
              category = excluded.category,
              source = excluded.source,
              updated_at = excluded.updated_at
            """,
            (domain.strip().lower(), category.strip(), source.strip(), created, created),
        )
        row = conn.execute(
            "SELECT id FROM self_control_blocked_sites WHERE domain = ? LIMIT 1",
            (domain.strip().lower(),),
        ).fetchone()
    return int((row["id"] if row else cur.lastrowid) or 0)


def toggle_blocked_site(site_id: int) -> None:
    ensure_self_control_schema()
    with db() as conn:
        conn.execute(
            """
            UPDATE self_control_blocked_sites
            SET enabled = CASE WHEN enabled = 1 THEN 0 ELSE 1 END,
                updated_at = ?
            WHERE id = ?
            """,
            (now_iso(), int(site_id)),
        )


def delete_blocked_site(site_id: int) -> None:
    ensure_self_control_schema()
    with db() as conn:
        conn.execute("DELETE FROM self_control_blocked_sites WHERE id = ?", (int(site_id),))


def get_blocked_site(site_id: int) -> Optional[Dict[str, Any]]:
    ensure_self_control_schema()
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM self_control_blocked_sites WHERE id = ? LIMIT 1",
            (int(site_id),),
        ).fetchone()
    return dict(row) if row else None


def list_presets() -> List[Dict[str, Any]]:
    ensure_self_control_schema()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM self_control_presets
            WHERE enabled = 1
            ORDER BY seeded DESC, name ASC
            """
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["blocked_categories"] = _loads(item.get("blocked_categories_json"), [])
        item["blocked_domains"] = _loads(item.get("blocked_domains_json"), [])
        out.append(item)
    return out


def get_preset(slug: str) -> Optional[Dict[str, Any]]:
    ensure_self_control_schema()
    with db() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM self_control_presets
            WHERE slug = ? AND enabled = 1
            LIMIT 1
            """,
            (str(slug or "").strip(),),
        ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["blocked_categories"] = _loads(item.get("blocked_categories_json"), [])
    item["blocked_domains"] = _loads(item.get("blocked_domains_json"), [])
    return item


def list_rules() -> List[Dict[str, Any]]:
    ensure_self_control_schema()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM self_control_rules
            ORDER BY enabled DESC, name ASC
            """
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["trigger_config"] = _loads(item.get("trigger_config_json"), {})
        item["action_config"] = _loads(item.get("action_config_json"), {})
        out.append(item)
    return out


def get_rule(slug: str) -> Optional[Dict[str, Any]]:
    ensure_self_control_schema()
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM self_control_rules WHERE slug = ? LIMIT 1",
            (str(slug or "").strip(),),
        ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["trigger_config"] = _loads(item.get("trigger_config_json"), {})
    item["action_config"] = _loads(item.get("action_config_json"), {})
    return item


def toggle_rule(slug: str) -> None:
    ensure_self_control_schema()
    with db() as conn:
        conn.execute(
            """
            UPDATE self_control_rules
            SET enabled = CASE WHEN enabled = 1 THEN 0 ELSE 1 END,
                updated_at = ?
            WHERE slug = ?
            """,
            (now_iso(), str(slug or "").strip()),
        )


def get_active_session() -> Optional[Dict[str, Any]]:
    ensure_self_control_schema()
    with db() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM self_control_sessions
            WHERE status IN ('active', 'awaiting_journal_unlock')
            ORDER BY started_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
    return _session_row_to_dict(row)


def list_recent_sessions(limit: int = 20) -> List[Dict[str, Any]]:
    ensure_self_control_schema()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM self_control_sessions
            ORDER BY started_at DESC, id DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
    return [_session_row_to_dict(row) for row in rows]


def get_session(session_id: int) -> Optional[Dict[str, Any]]:
    ensure_self_control_schema()
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM self_control_sessions WHERE id = ? LIMIT 1",
            (int(session_id),),
        ).fetchone()
    return _session_row_to_dict(row)


def create_session(payload: Dict[str, Any]) -> int:
    ensure_self_control_schema()
    created = now_iso()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO self_control_sessions (
              preset_slug,
              label,
              intent_note,
              status,
              strict_mode,
              started_at,
              planned_end_at,
              ended_at,
              planned_minutes,
              completed_minutes,
              blocked_categories_json,
              blocked_domains_json,
              source_rule_slug,
              unlock_requirement,
              unlock_satisfied_at,
              cancel_reason,
              override_reason,
              created_at,
              updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(payload.get("preset_slug") or "").strip(),
                str(payload.get("label") or "").strip(),
                str(payload.get("intent_note") or "").strip(),
                str(payload.get("status") or "active").strip(),
                1 if payload.get("strict_mode") else 0,
                str(payload.get("started_at") or created).strip(),
                str(payload.get("planned_end_at") or "").strip(),
                str(payload.get("ended_at") or "").strip(),
                int(payload.get("planned_minutes") or 0),
                int(payload.get("completed_minutes") or 0),
                json.dumps(list(payload.get("blocked_categories") or [])),
                json.dumps(list(payload.get("blocked_domains") or [])),
                str(payload.get("source_rule_slug") or "").strip(),
                str(payload.get("unlock_requirement") or "").strip(),
                str(payload.get("unlock_satisfied_at") or "").strip(),
                str(payload.get("cancel_reason") or "").strip(),
                str(payload.get("override_reason") or "").strip(),
                created,
                created,
            ),
        )
    return int(cur.lastrowid)


def update_session(session_id: int, payload: Dict[str, Any]) -> None:
    ensure_self_control_schema()
    fields = []
    params: List[Any] = []
    mapping = {
        "preset_slug": "preset_slug",
        "label": "label",
        "intent_note": "intent_note",
        "status": "status",
        "strict_mode": "strict_mode",
        "started_at": "started_at",
        "planned_end_at": "planned_end_at",
        "ended_at": "ended_at",
        "planned_minutes": "planned_minutes",
        "completed_minutes": "completed_minutes",
        "source_rule_slug": "source_rule_slug",
        "unlock_requirement": "unlock_requirement",
        "unlock_satisfied_at": "unlock_satisfied_at",
        "cancel_reason": "cancel_reason",
        "override_reason": "override_reason",
    }
    for key, col in mapping.items():
        if key not in payload:
            continue
        value = payload.get(key)
        if key == "strict_mode":
            value = 1 if value else 0
        fields.append(f"{col} = ?")
        params.append(value)
    if "blocked_categories" in payload:
        fields.append("blocked_categories_json = ?")
        params.append(json.dumps(list(payload.get("blocked_categories") or [])))
    if "blocked_domains" in payload:
        fields.append("blocked_domains_json = ?")
        params.append(json.dumps(list(payload.get("blocked_domains") or [])))
    if not fields:
        return
    fields.append("updated_at = ?")
    params.append(now_iso())
    params.append(int(session_id))
    with db() as conn:
        conn.execute(
            f"UPDATE self_control_sessions SET {', '.join(fields)} WHERE id = ?",
            params,
        )


def log_event(session_id: Optional[int], event_type: str, detail: Dict[str, Any]) -> int:
    ensure_self_control_schema()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO self_control_events (
              session_id, event_type, event_at, detail_json
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                int(session_id) if session_id else None,
                str(event_type or "").strip(),
                now_iso(),
                json.dumps(detail or {}, ensure_ascii=False),
            ),
        )
    return int(cur.lastrowid)


def list_session_events(limit: int = 100) -> List[Dict[str, Any]]:
    ensure_self_control_schema()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM self_control_events
            ORDER BY event_at DESC, id DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["detail"] = _loads(item.get("detail_json"), {})
        out.append(item)
    return out


def list_enforcement_providers() -> List[Dict[str, Any]]:
    ensure_self_control_schema()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM self_control_enforcement_providers
            ORDER BY id ASC
            """
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["config"] = _loads(item.get("config_json"), {})
        out.append(item)
    return out


def _session_row_to_dict(row: Any) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    item = dict(row)
    item["blocked_categories"] = _loads(item.get("blocked_categories_json"), [])
    item["blocked_domains"] = _loads(item.get("blocked_domains_json"), [])
    return item
