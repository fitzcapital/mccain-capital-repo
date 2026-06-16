"""Trades repository functions."""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from mccain_capital.runtime import (
    db,
    get_setting_float,
    get_setting_value,
    now_iso,
    set_setting_value,
)

SOURCE_PLACEHOLDER_SETUPS = {
    "statement import",
    "live upload",
    "manual entry",
    "api",
    "balance snapshot",
    "imported",
}

DEFAULT_PROP_FIRM = "Vanquish"


def _trade_clock_to_minutes(value: Any) -> Optional[int]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%I:%M %p", "%I:%M:%S %p", "%H:%M"):
        try:
            parsed = datetime.strptime(text.upper(), fmt)
            return (parsed.hour * 60) + parsed.minute
        except ValueError:
            continue
    return None


def classify_time_block(value: Any) -> str:
    minute = _trade_clock_to_minutes(value)
    if minute is None:
        return "Unknown"
    if minute < (11 * 60):
        return "Open"
    if minute < (14 * 60):
        return "Midday"
    return "Power Hour"


def compute_hold_minutes(entry_time: Any, exit_time: Any) -> Optional[int]:
    entry_min = _trade_clock_to_minutes(entry_time)
    exit_min = _trade_clock_to_minutes(exit_time)
    if entry_min is None or exit_min is None or exit_min < entry_min:
        return None
    return exit_min - entry_min


def _normalize_filter_value(raw: Any) -> str:
    return str(raw or "").strip()


def normalize_broker_account_id(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if ":" in raw:
        prefix, suffix = raw.split(":", 1)
        cleaned_suffix = suffix.strip()
        if not cleaned_suffix:
            return ""
        return f"{prefix.strip() or 'default'}:{cleaned_suffix}"
    return f"default:{raw}"


def display_broker_account_id(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if ":" in raw:
        _prefix, suffix = raw.split(":", 1)
        return suffix.strip()
    return raw


def _continuity_setting_key(account_id: int) -> str:
    return f"account_continuity::{int(account_id)}"


def set_account_continuity(account_id: int, linked_account_ids: list[int]) -> None:
    active_id = int(account_id or 0)
    if active_id <= 0:
        return
    linked = [i for i in _clean_account_ids(linked_account_ids) if i != active_id]
    set_setting_value(_continuity_setting_key(active_id), json.dumps(linked))


def account_continuity_ids(account_id: int | None) -> list[int]:
    active_id = int(account_id or 0)
    if active_id <= 0:
        return []
    raw = str(get_setting_value(_continuity_setting_key(active_id), "[]") or "[]").strip()
    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = []
    linked = _clean_account_ids(parsed if isinstance(parsed, list) else [])
    return _clean_account_ids([*linked, active_id])


def account_continuity_label(account_id: int | None) -> str:
    ids = account_continuity_ids(account_id)
    if len(ids) <= 1:
        return ""
    accounts = [get_account(account_id) for account_id in ids]
    labels = [
        str(
            (account or {}).get("display_broker_account_id")
            or (account or {}).get("account_name")
            or ""
        )
        for account in accounts
    ]
    return " + ".join(label for label in labels if label)


def maybe_link_rollover_account(new_account_id: int, prior_account_id: int | None) -> bool:
    if not new_account_id or not prior_account_id or int(new_account_id) == int(prior_account_id):
        return False
    new_account = get_account(int(new_account_id))
    prior_account = get_account(int(prior_account_id))
    if not new_account or not prior_account:
        return False
    new_broker = display_broker_account_id(new_account.get("broker_account_id")).upper()
    prior_broker = display_broker_account_id(prior_account.get("broker_account_id")).upper()
    if not (new_broker.startswith("OPA") and prior_broker.startswith("OEV")):
        return False
    existing = [i for i in account_continuity_ids(int(new_account_id)) if i != int(new_account_id)]
    set_account_continuity(int(new_account_id), [*existing, int(prior_account_id)])
    return True


def clean_setup_label(raw: Any) -> str:
    value = str(raw or "").strip()
    return "" if value.lower() in SOURCE_PLACEHOLDER_SETUPS else value


def setup_display_label(row: Dict[str, Any]) -> str:
    return (
        clean_setup_label(
            row.get("setup_tag") or row.get("strategy_label") or row.get("resolved_setup_tag") or ""
        )
        or "Unknown"
    )


def normalize_trade_source(row: Dict[str, Any]) -> str:
    explicit = str(row.get("trade_source") or "").strip()
    if explicit:
        if explicit.lower() == "statement import":
            return "Live Upload"
        return explicit
    if str(row.get("import_batch_id") or "").strip():
        return "Live Upload"
    raw_line = str(row.get("raw_line") or "").strip().upper()
    if raw_line == "MANUAL ENTRY" or raw_line.startswith("DUPLICATE OF #"):
        return "Manual Entry"
    if "BALANCE SNAPSHOT" in raw_line:
        return "Balance Snapshot"
    return "Unknown"


def normalize_trade_filters(raw_filters: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    raw_filters = raw_filters or {}
    return {
        "setup": _normalize_filter_value(raw_filters.get("setup")),
        "session": _normalize_filter_value(raw_filters.get("session")),
        "outcome": _normalize_filter_value(raw_filters.get("outcome")).lower(),
        "time_block": _normalize_filter_value(raw_filters.get("time_block")),
        "mistake_tag": _normalize_filter_value(raw_filters.get("mistake_tag")),
    }


def _row_matches_trade_filters(row: Dict[str, Any], filters: Dict[str, str]) -> bool:
    setup = filters.get("setup", "")
    if setup and str(row.get("setup_display") or "") != setup:
        return False
    outcome = filters.get("outcome", "")
    net = row.get("net_pl")
    if outcome == "winner" and not (net is not None and float(net) > 0):
        return False
    if outcome == "loser" and not (net is not None and float(net) < 0):
        return False
    if outcome == "breakeven" and not (net is not None and float(net) == 0):
        return False

    time_block = filters.get("time_block", "")
    if time_block and str(row.get("time_block") or "") != time_block:
        return False

    mistake_tag = filters.get("mistake_tag", "").lower()
    if mistake_tag:
        tag_pool = ",".join(
            [
                str(row.get("mistake_tags") or ""),
                str(row.get("rule_break_tags") or ""),
            ]
        ).lower()
        if mistake_tag not in tag_pool:
            return False
    return True


def _enrich_trade_review_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    enriched = dict(row)
    enriched["trade_source"] = normalize_trade_source(row)
    enriched["setup_tag"] = clean_setup_label(
        row.get("setup_tag") or row.get("strategy_label") or row.get("resolved_setup_tag") or ""
    )
    enriched["setup_display"] = enriched["setup_tag"] or "Unknown"
    enriched["setup_missing"] = not bool(enriched["setup_tag"])
    enriched["time_block"] = classify_time_block(row.get("entry_time"))
    enriched["hold_minutes"] = compute_hold_minutes(row.get("entry_time"), row.get("exit_time"))
    planned_risk = row.get("planned_risk_dollars")
    net = row.get("net_pl")
    r_multiple = None
    try:
        planned_risk_float = float(planned_risk) if planned_risk not in (None, "") else None
        net_float = float(net) if net not in (None, "") else None
        if planned_risk_float and planned_risk_float > 0 and net_float is not None:
            r_multiple = net_float / planned_risk_float
    except (TypeError, ValueError):
        r_multiple = None
    enriched["r_multiple"] = r_multiple
    review_items = [
        enriched.get("setup_tag"),
        enriched.get("session_tag"),
        enriched.get("checklist_score"),
        enriched.get("mistake_tags"),
        enriched.get("planned_risk_dollars"),
        enriched.get("review_note"),
    ]
    completed = sum(1 for item in review_items if item not in (None, "", []))
    enriched["review_completion_pct"] = int(round((completed / len(review_items)) * 100.0))
    return enriched


def fetch_trades(
    d: str = "",
    q: str = "",
    filters: Optional[Dict[str, Any]] = None,
    *,
    account_id: int | None = None,
    account_ids: list[int] | tuple[int, ...] | None = None,
):
    d = (d or "").strip()
    q = (q or "").strip()
    normalized = normalize_trade_filters(filters)

    sql = """
        SELECT
            t.*,
            r.strategy_id,
            r.strategy_label,
            COALESCE(NULLIF(r.strategy_label, ''), NULLIF(s.title, ''), NULLIF(r.setup_tag, ''), '') AS resolved_setup_tag,
            r.session_tag,
            r.checklist_score,
            r.rule_break_tags,
            r.review_note,
            r.thesis_note,
            r.execution_grade,
            r.risk_grade,
            r.plan_grade,
            r.mistake_tags,
            r.planned_risk_dollars,
            r.size_rule_note,
            r.entry_quality_note,
            r.exit_quality_note,
            r.improvement_note,
            r.reviewed_stop_price,
            r.reviewed_target_price,
            r.reviewed_risk_dollars,
            r.reviewed_risk_percent,
            r.reviewed_execution_quality,
            r.reviewed_sizing_quality,
            r.reviewed_stop_discipline,
            r.reviewed_within_plan,
            r.manual_grade_score,
            r.manual_grade_letter,
            r.grade_override_reason,
            r.classification_override
        FROM trades t
        LEFT JOIN trade_reviews r ON r.trade_id = t.id
        LEFT JOIN strategies s ON s.id = r.strategy_id
    """
    where = []
    params: List[Any] = []

    if d:
        where.append("t.trade_date = ?")
        params.append(d)
    account_filter, account_params = _account_scope_filter(account_id, account_ids, prefix="t.")
    if account_filter:
        where.append(account_filter)
        params.extend(account_params)

    if q:
        where.append("(t.ticker LIKE ? OR t.opt_type LIKE ? OR t.raw_line LIKE ?)")
        like = f"%{q}%"
        params.extend([like, like, like])

    if normalized["setup"] and normalized["setup"].lower() != "unknown":
        where.append(
            "LOWER(COALESCE(NULLIF(r.strategy_label, ''), NULLIF(s.title, ''), NULLIF(r.setup_tag, ''), '')) = LOWER(?)"
        )
        params.append(normalized["setup"])

    if normalized["session"]:
        where.append("LOWER(COALESCE(r.session_tag, '')) = LOWER(?)")
        params.append(normalized["session"])

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY t.trade_date DESC, t.id DESC"

    with db() as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    enriched_rows = [_enrich_trade_review_fields(row) for row in rows]
    return [row for row in enriched_rows if _row_matches_trade_filters(row, normalized)]


def _active_account_record_id() -> int:
    raw = str(get_setting_value("active_account_record_id", "") or "").strip()
    return int(raw) if raw.isdigit() else 0


def _clean_account_ids(account_ids: Any) -> list[int]:
    cleaned: list[int] = []
    seen: set[int] = set()
    for raw in list(account_ids or []):
        try:
            account_id = int(raw)
        except (TypeError, ValueError):
            continue
        if account_id <= 0 or account_id in seen:
            continue
        cleaned.append(account_id)
        seen.add(account_id)
    return cleaned


def _account_scope_filter(
    account_id: int | None = None,
    account_ids: list[int] | tuple[int, ...] | None = None,
    *,
    prefix: str = "",
) -> tuple[str, list[Any]]:
    if account_id:
        return f"{prefix}account_id = ?", [int(account_id)]
    cleaned = _clean_account_ids(account_ids)
    if not cleaned:
        return "", []
    marks = ",".join(["?"] * len(cleaned))
    return f"{prefix}account_id IN ({marks})", list(cleaned)


def _trade_scope_clause(
    *,
    account_id: int | None = None,
    account_ids: list[int] | tuple[int, ...] | None = None,
    start_date: str = "",
    end_date: str = "",
    alias: str = "",
) -> tuple[list[str], list[Any]]:
    prefix = f"{alias}." if alias else ""
    where: list[str] = []
    params: list[Any] = []
    account_filter, account_params = _account_scope_filter(
        account_id,
        account_ids,
        prefix=prefix,
    )
    if account_filter:
        where.append(account_filter)
        params.extend(account_params)
    if start_date:
        where.append(f"{prefix}trade_date >= ?")
        params.append(str(start_date))
    if end_date:
        where.append(f"{prefix}trade_date < ?")
        params.append(str(end_date))
    return where, params


def _account_display_size(row: Dict[str, Any]) -> float:
    account_size = row.get("account_size")
    if account_size not in (None, ""):
        try:
            return float(account_size)
        except Exception:
            pass
    try:
        return float(row.get("starting_balance") or 0.0)
    except Exception:
        return 0.0


def get_account(account_id: int) -> Optional[Dict[str, Any]]:
    if not account_id:
        return None
    with db() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM accounts
            WHERE id = ? AND archived = 0
            LIMIT 1
            """,
            (int(account_id),),
        ).fetchone()
    if not row:
        return None
    account = dict(row)
    account["prop_firm"] = str(account.get("prop_firm") or "").strip() or DEFAULT_PROP_FIRM
    account["display_broker_account_id"] = display_broker_account_id(account.get("broker_account_id"))
    account["account_size"] = _account_display_size(account)
    account["current_balance"] = latest_balance_overall(
        account_id=int(account["id"]),
        starting_balance=float(account.get("starting_balance") or 0.0),
    )
    return account


def find_account_by_broker_account_id(
    broker_account_id: str, *, include_archived: bool = False
) -> Optional[Dict[str, Any]]:
    normalized = normalize_broker_account_id(broker_account_id)
    if not normalized:
        return None
    archived_filter = "" if include_archived else "AND archived = 0"
    with db() as conn:
        row = conn.execute(
            f"""
            SELECT *
            FROM accounts
            WHERE broker_account_id = ?
              {archived_filter}
            ORDER BY archived ASC, updated_at DESC, created_at DESC, id DESC
            LIMIT 1
            """,
            (normalized,),
        ).fetchone()
    if not row:
        return None
    account = dict(row)
    account["prop_firm"] = str(account.get("prop_firm") or "").strip() or DEFAULT_PROP_FIRM
    account["display_broker_account_id"] = display_broker_account_id(account.get("broker_account_id"))
    account["account_size"] = _account_display_size(account)
    account["current_balance"] = latest_balance_overall(
        account_id=int(account["id"]),
        starting_balance=float(account.get("starting_balance") or 0.0),
    )
    return account


def list_accounts(include_archived: bool = False) -> List[Dict[str, Any]]:
    where = "" if include_archived else "WHERE archived = 0"
    with db() as conn:
        rows = [
            dict(r)
            for r in conn.execute(
                f"""
                SELECT *
                FROM accounts
                {where}
                ORDER BY archived ASC, created_at DESC, id DESC
                """
            ).fetchall()
        ]
    accounts: List[Dict[str, Any]] = []
    for row in rows:
        row["prop_firm"] = str(row.get("prop_firm") or "").strip() or DEFAULT_PROP_FIRM
        row["display_broker_account_id"] = display_broker_account_id(row.get("broker_account_id"))
        row["account_size"] = _account_display_size(row)
        row["current_balance"] = latest_balance_overall(
            account_id=int(row["id"]),
            starting_balance=float(row.get("starting_balance") or 0.0),
        )
        accounts.append(row)
    return accounts


def create_account(
    *,
    account_name: str,
    starting_balance: float,
    prop_firm: str = "",
    broker_account_id: str = "",
    account_size: float | None = None,
    max_drawdown: float = 0.0,
) -> int:
    now = now_iso()
    account_name = str(account_name or "").strip()
    if not account_name:
        raise ValueError("account name required")
    starting = float(starting_balance)
    size_value = float(account_size if account_size is not None else starting)
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO accounts (
                prop_firm, account_name, broker_account_id, account_size,
                starting_balance, current_balance, max_drawdown, archived,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            """,
            (
                str(prop_firm or "").strip() or DEFAULT_PROP_FIRM,
                account_name,
                normalize_broker_account_id(broker_account_id),
                size_value,
                starting,
                starting,
                float(max_drawdown or 0.0),
                now,
                now,
            ),
        )
        account_id = int(cur.lastrowid)
        conn.commit()
    return account_id


def update_account(
    account_id: int,
    *,
    account_name: str,
    starting_balance: float,
    prop_firm: str = "",
    broker_account_id: str = "",
    account_size: float | None = None,
    max_drawdown: float = 0.0,
) -> None:
    now = now_iso()
    starting = float(starting_balance)
    size_value = float(account_size if account_size is not None else starting)
    with db() as conn:
        conn.execute(
            """
            UPDATE accounts
            SET prop_firm = ?,
                account_name = ?,
                broker_account_id = ?,
                account_size = ?,
                starting_balance = ?,
                max_drawdown = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                str(prop_firm or "").strip() or DEFAULT_PROP_FIRM,
                str(account_name or "").strip(),
                normalize_broker_account_id(broker_account_id),
                size_value,
                starting,
                float(max_drawdown or 0.0),
                now,
                int(account_id),
            ),
        )
        conn.commit()


def update_account_broker_metrics(
    account_id: int,
    *,
    broker_equity: float | None = None,
    broker_equity_peak: float | None = None,
    broker_remaining_drawdown: float | None = None,
    broker_max_loss: float | None = None,
    updated_at: str | None = None,
) -> None:
    if not account_id:
        return
    now = updated_at or now_iso()
    with db() as conn:
        conn.execute(
            """
            UPDATE accounts
            SET broker_equity = ?,
                broker_equity_peak = ?,
                broker_remaining_drawdown = ?,
                broker_max_loss = ?,
                broker_metrics_updated_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                broker_equity,
                broker_equity_peak,
                broker_remaining_drawdown,
                broker_max_loss,
                now,
                now,
                int(account_id),
            ),
        )
        conn.commit()


def update_account_broker_equity_from_statement(
    account_id: int,
    *,
    broker_equity: float,
    updated_at: str | None = None,
) -> None:
    if not account_id:
        return
    now = updated_at or now_iso()
    equity = float(broker_equity)
    with db() as conn:
        row = conn.execute(
            """
            SELECT broker_equity_peak
            FROM accounts
            WHERE id = ?
            LIMIT 1
            """,
            (int(account_id),),
        ).fetchone()
        existing_peak = row["broker_equity_peak"] if row else None
        broker_equity_peak = equity
        if existing_peak is not None:
            try:
                broker_equity_peak = max(float(existing_peak), equity)
            except (TypeError, ValueError):
                broker_equity_peak = equity
        conn.execute(
            """
            UPDATE accounts
            SET broker_equity = ?,
                broker_equity_peak = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                equity,
                broker_equity_peak,
                now,
                int(account_id),
            ),
        )
        conn.commit()


def archive_account(account_id: int) -> None:
    now = now_iso()
    with db() as conn:
        conn.execute(
            """
            UPDATE accounts
            SET archived = 1,
                updated_at = ?
            WHERE id = ?
            """,
            (now, int(account_id)),
        )
        conn.commit()


def restore_account(account_id: int) -> None:
    now = now_iso()
    with db() as conn:
        conn.execute(
            """
            UPDATE accounts
            SET archived = 0,
                updated_at = ?
            WHERE id = ?
            """,
            (now, int(account_id)),
        )
        conn.commit()


def set_active_account(account_id: int | None) -> None:
    if not account_id:
        set_setting_value("active_account_record_id", "")
        clear_account_scope()
        return
    account = get_account(int(account_id))
    if not account:
        set_setting_value("active_account_record_id", "")
        return
    set_setting_value("active_account_record_id", str(int(account["id"])))
    set_setting_value("active_account_id", str(account.get("broker_account_id") or "").strip())
    set_setting_value("active_account_name", str(account.get("account_name") or "").strip())
    set_setting_value("active_account_label", str(account.get("account_name") or "").strip())
    set_setting_value("active_account_type", str(account.get("prop_firm") or "").strip())
    set_setting_value(
        "active_account_start_balance",
        f"{float(account.get('starting_balance') or 0.0):.2f}",
    )
    set_setting_value("active_account_start_date", "")


def create_upload(
    *,
    account_id: int,
    filename: str,
    source: str,
    import_batch_id: str = "",
) -> int:
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO uploads (account_id, filename, source, import_batch_id, uploaded_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(account_id),
                str(filename or "").strip(),
                str(source or "").strip(),
                str(import_batch_id or "").strip(),
                now_iso(),
            ),
        )
        upload_id = int(cur.lastrowid)
        conn.commit()
    return upload_id


def fetch_latest_trade_date() -> str:
    with db() as conn:
        row = conn.execute(
            """
            SELECT trade_date
            FROM trades
            WHERE COALESCE(trade_date, '') <> ''
            ORDER BY trade_date DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
    return str(row["trade_date"] or "").strip() if row else ""


def fetch_trades_range(
    start_iso: str,
    end_iso: str,
    *,
    account_id: int | None = None,
    account_ids: list[int] | tuple[int, ...] | None = None,
):
    with db() as conn:
        where = ["trade_date >= ?", "trade_date < ?"]
        params: list[Any] = [start_iso, end_iso]
        account_filter, account_params = _account_scope_filter(account_id, account_ids)
        if account_filter:
            where.append(account_filter)
            params.extend(account_params)
        return list(
            conn.execute(
                """
            SELECT * FROM trades
            WHERE """
                + " AND ".join(where)
                + """
            ORDER BY trade_date ASC, id ASC
            """,
                params,
            ).fetchall()
        )


def fetch_open_positions(
    as_of: str = "", q: str = "", *, account_id: int | None = None
) -> List[Dict[str, Any]]:
    where: List[str] = [
        "(COALESCE(exit_time, '') = '' OR exit_price IS NULL OR net_pl IS NULL)",
        "COALESCE(contracts, 0) > 0",
    ]
    params: List[Any] = []
    if as_of:
        where.append("trade_date <= ?")
        params.append(as_of)
    if q:
        like = f"%{q.strip()}%"
        where.append("(ticker LIKE ? OR opt_type LIKE ? OR raw_line LIKE ?)")
        params.extend([like, like, like])
    if account_id:
        where.append("account_id = ?")
        params.append(int(account_id))

    sql = f"""
        SELECT *
        FROM trades
        WHERE {" AND ".join(where)}
        ORDER BY trade_date DESC, id DESC
    """
    with db() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_risk_controls() -> Dict[str, Any]:
    with db() as conn:
        row = conn.execute(
            "SELECT daily_max_loss, enforce_lockout, updated_at FROM risk_controls WHERE id = 1"
        ).fetchone()
    if not row:
        return {"daily_max_loss": 0.0, "enforce_lockout": 0, "updated_at": ""}
    return {
        "daily_max_loss": float(row["daily_max_loss"] or 0.0),
        "enforce_lockout": int(row["enforce_lockout"] or 0),
        "updated_at": row["updated_at"] or "",
    }


def save_risk_controls(daily_max_loss: float, enforce_lockout: int) -> None:
    with db() as conn:
        conn.execute(
            """
            INSERT INTO risk_controls (id, daily_max_loss, enforce_lockout, updated_at)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              daily_max_loss=excluded.daily_max_loss,
              enforce_lockout=excluded.enforce_lockout,
              updated_at=excluded.updated_at
            """,
            (abs(float(daily_max_loss or 0.0)), 1 if enforce_lockout else 0, now_iso()),
        )


def get_trade_review(trade_id: int) -> Optional[Dict[str, Any]]:
    with db() as conn:
        row = conn.execute(
            """
            SELECT
              trade_id,
              strategy_id,
              strategy_label,
              setup_tag,
              session_tag,
              checklist_score,
              rule_break_tags,
              review_note,
              thesis_note,
              execution_grade,
              risk_grade,
              plan_grade,
              mistake_tags,
              planned_risk_dollars,
              size_rule_note,
              entry_quality_note,
              exit_quality_note,
              improvement_note,
              reviewed_stop_price,
              reviewed_target_price,
              reviewed_risk_dollars,
              reviewed_risk_percent,
              reviewed_execution_quality,
              reviewed_sizing_quality,
              reviewed_stop_discipline,
              reviewed_within_plan,
              manual_grade_score,
              manual_grade_letter,
              grade_override_reason,
              classification_override
            FROM trade_reviews
            WHERE trade_id = ?
            """,
            (trade_id,),
        ).fetchone()
    return dict(row) if row else None


def _resolve_strategy_link(
    conn: Any,
    *,
    strategy_id: Optional[int],
    strategy_label: str,
    setup_tag: str,
) -> tuple[Optional[int], str]:
    clean_label = (strategy_label or setup_tag or "").strip()
    clean_id = int(strategy_id) if strategy_id not in (None, "", 0, "0") else None
    if clean_id:
        row = conn.execute("SELECT id, title FROM strategies WHERE id = ?", (clean_id,)).fetchone()
        if row:
            return int(row["id"]), str(row["title"] or "").strip()
    if not clean_label:
        return None, ""
    row = conn.execute(
        "SELECT id, title FROM strategies WHERE LOWER(TRIM(title)) = LOWER(TRIM(?)) LIMIT 1",
        (clean_label,),
    ).fetchone()
    if row:
        return int(row["id"]), str(row["title"] or "").strip()
    created = now_iso()
    cur = conn.execute(
        """
        INSERT INTO strategies (title, body, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            clean_label,
            "Auto-created from trade review/import flow. Add your execution rules here.",
            created,
            created,
        ),
    )
    return int(cur.lastrowid), clean_label


def upsert_trade_review(
    trade_id: int,
    strategy_id: Optional[int] = None,
    strategy_label: str = "",
    setup_tag: str = "",
    session_tag: str = "",
    checklist_score: Optional[int] = None,
    rule_break_tags: str = "",
    review_note: str = "",
    thesis_note: str = "",
    execution_grade: Optional[int] = None,
    risk_grade: Optional[int] = None,
    plan_grade: Optional[int] = None,
    mistake_tags: str = "",
    planned_risk_dollars: Optional[float] = None,
    size_rule_note: str = "",
    entry_quality_note: str = "",
    exit_quality_note: str = "",
    improvement_note: str = "",
    reviewed_stop_price: Optional[float] = None,
    reviewed_target_price: Optional[float] = None,
    reviewed_risk_dollars: Optional[float] = None,
    reviewed_risk_percent: Optional[float] = None,
    reviewed_execution_quality: str = "",
    reviewed_sizing_quality: str = "",
    reviewed_stop_discipline: str = "",
    reviewed_within_plan: Optional[int] = None,
    manual_grade_score: Optional[int] = None,
    manual_grade_letter: str = "",
    grade_override_reason: str = "",
    classification_override: str = "",
) -> None:
    now = now_iso()
    score_val = None if checklist_score is None else max(0, min(100, int(checklist_score)))
    execution_val = None if execution_grade is None else max(0, min(100, int(execution_grade)))
    risk_val = None if risk_grade is None else max(0, min(100, int(risk_grade)))
    plan_val = None if plan_grade is None else max(0, min(100, int(plan_grade)))
    risk_dollars_val = None
    if planned_risk_dollars not in (None, ""):
        risk_dollars_val = abs(float(planned_risk_dollars))
    reviewed_stop_val = None if reviewed_stop_price in (None, "") else float(reviewed_stop_price)
    reviewed_target_val = (
        None if reviewed_target_price in (None, "") else float(reviewed_target_price)
    )
    reviewed_risk_dollars_val = None
    if reviewed_risk_dollars not in (None, ""):
        reviewed_risk_dollars_val = abs(float(reviewed_risk_dollars))
    reviewed_risk_pct_val = None
    if reviewed_risk_percent not in (None, ""):
        reviewed_risk_pct_val = float(reviewed_risk_percent)
    reviewed_within_plan_val = None
    if reviewed_within_plan not in (None, ""):
        reviewed_within_plan_val = 1 if int(reviewed_within_plan) else 0
    manual_grade_score_val = None
    if manual_grade_score not in (None, ""):
        manual_grade_score_val = max(0, min(100, int(manual_grade_score)))
    with db() as conn:
        resolved_strategy_id, resolved_strategy_label = _resolve_strategy_link(
            conn,
            strategy_id=strategy_id,
            strategy_label=strategy_label,
            setup_tag=setup_tag,
        )
        conn.execute(
            """
            INSERT INTO trade_reviews
              (
                trade_id, strategy_id, strategy_label, setup_tag, session_tag, checklist_score,
                rule_break_tags, review_note, thesis_note, execution_grade, risk_grade, plan_grade,
                mistake_tags, planned_risk_dollars, size_rule_note, entry_quality_note,
                exit_quality_note, improvement_note, reviewed_stop_price, reviewed_target_price,
                reviewed_risk_dollars, reviewed_risk_percent, reviewed_execution_quality,
                reviewed_sizing_quality, reviewed_stop_discipline, reviewed_within_plan,
                manual_grade_score, manual_grade_letter, grade_override_reason,
                classification_override, created_at, updated_at
              )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_id) DO UPDATE SET
              strategy_id=excluded.strategy_id,
              strategy_label=excluded.strategy_label,
              setup_tag=excluded.setup_tag,
              session_tag=excluded.session_tag,
              checklist_score=excluded.checklist_score,
              rule_break_tags=excluded.rule_break_tags,
              review_note=excluded.review_note,
              thesis_note=excluded.thesis_note,
              execution_grade=excluded.execution_grade,
              risk_grade=excluded.risk_grade,
              plan_grade=excluded.plan_grade,
              mistake_tags=excluded.mistake_tags,
              planned_risk_dollars=excluded.planned_risk_dollars,
              size_rule_note=excluded.size_rule_note,
              entry_quality_note=excluded.entry_quality_note,
              exit_quality_note=excluded.exit_quality_note,
              improvement_note=excluded.improvement_note,
              reviewed_stop_price=excluded.reviewed_stop_price,
              reviewed_target_price=excluded.reviewed_target_price,
              reviewed_risk_dollars=excluded.reviewed_risk_dollars,
              reviewed_risk_percent=excluded.reviewed_risk_percent,
              reviewed_execution_quality=excluded.reviewed_execution_quality,
              reviewed_sizing_quality=excluded.reviewed_sizing_quality,
              reviewed_stop_discipline=excluded.reviewed_stop_discipline,
              reviewed_within_plan=excluded.reviewed_within_plan,
              manual_grade_score=excluded.manual_grade_score,
              manual_grade_letter=excluded.manual_grade_letter,
              grade_override_reason=excluded.grade_override_reason,
              classification_override=excluded.classification_override,
              updated_at=excluded.updated_at
            """,
            (
                trade_id,
                resolved_strategy_id,
                resolved_strategy_label,
                resolved_strategy_label,
                (session_tag or "").strip(),
                score_val,
                (rule_break_tags or "").strip(),
                (review_note or "").strip(),
                (thesis_note or "").strip(),
                execution_val,
                risk_val,
                plan_val,
                (mistake_tags or "").strip(),
                risk_dollars_val,
                (size_rule_note or "").strip(),
                (entry_quality_note or "").strip(),
                (exit_quality_note or "").strip(),
                (improvement_note or "").strip(),
                reviewed_stop_val,
                reviewed_target_val,
                reviewed_risk_dollars_val,
                reviewed_risk_pct_val,
                (reviewed_execution_quality or "").strip(),
                (reviewed_sizing_quality or "").strip(),
                (reviewed_stop_discipline or "").strip(),
                reviewed_within_plan_val,
                manual_grade_score_val,
                (manual_grade_letter or "").strip(),
                (grade_override_reason or "").strip(),
                (classification_override or "").strip(),
                now,
                now,
            ),
        )


def fetch_trade_reviews_map(trade_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    clean_ids = [
        int(i) for i in trade_ids if isinstance(i, int) or (isinstance(i, str) and str(i).isdigit())
    ]
    if not clean_ids:
        return {}
    marks = ",".join(["?"] * len(clean_ids))
    with db() as conn:
        rows = conn.execute(
            f"""
            SELECT
              trade_id,
              strategy_id,
              strategy_label,
              setup_tag,
              session_tag,
              checklist_score,
              rule_break_tags,
              review_note,
              thesis_note,
              execution_grade,
              risk_grade,
              plan_grade,
              mistake_tags,
              planned_risk_dollars,
              size_rule_note,
              entry_quality_note,
              exit_quality_note,
              improvement_note,
              reviewed_stop_price,
              reviewed_target_price,
              reviewed_risk_dollars,
              reviewed_risk_percent,
              reviewed_execution_quality,
              reviewed_sizing_quality,
              reviewed_stop_discipline,
              reviewed_within_plan,
              manual_grade_score,
              manual_grade_letter,
              grade_override_reason,
              classification_override
            FROM trade_reviews
            WHERE trade_id IN ({marks})
            """,
            clean_ids,
        ).fetchall()
    return {int(r["trade_id"]): dict(r) for r in rows}


def day_net_total(day_iso: str, *, account_id: int | None = None) -> float:
    with db() as conn:
        where = ["trade_date = ?"]
        params: list[Any] = [day_iso]
        if account_id:
            where.append("account_id = ?")
            params.append(int(account_id))
        row = conn.execute(
            "SELECT COALESCE(SUM(net_pl), 0) AS total FROM trades WHERE " + " AND ".join(where),
            params,
        ).fetchone()
    return float((row["total"] if row else 0.0) or 0.0)


def trade_lockout_state(
    day_iso: str, *, daily_max_loss: float, enforce_lockout: int, account_id: int | None = None
) -> Dict[str, Any]:
    day_net = day_net_total(day_iso, account_id=account_id)
    max_loss = abs(float(daily_max_loss or 0.0))
    locked = bool(enforce_lockout) and max_loss > 0 and day_net <= (-max_loss)
    return {
        "day": day_iso,
        "day_net": day_net,
        "daily_max_loss": max_loss,
        "enforce_lockout": int(enforce_lockout),
        "locked": locked,
    }


def last_balance_in_list(trades: List[object]) -> Optional[float]:
    for t in trades:
        b = t["balance"]
        if b is not None:
            try:
                return float(b)
            except Exception:
                return None
    return None


def trade_day_stats(trades: List[object]) -> Dict[str, Any]:
    total = 0.0
    wins = 0
    losses = 0
    for t in trades:
        net = t["net_pl"]
        if net is None:
            continue
        total += float(net)
        if net > 0:
            wins += 1
        elif net < 0:
            losses += 1

    total_trades = wins + losses
    win_rate = (wins / total_trades * 100.0) if total_trades else 0.0
    wl_ratio = (wins / losses) if losses else (float(wins) if wins else 0.0)
    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "total_trades": total_trades,
        "win_rate": win_rate,
        "wl_ratio": wl_ratio,
    }


def calc_consistency(trades: List[object]) -> Dict[str, Any]:
    if not trades:
        return {"ratio": None, "status": "—", "class": "", "biggest": 0.0, "denom": 0.0}

    net_vals: List[float] = []
    for t in trades:
        try:
            v = t["net_pl"]
        except Exception:
            v = t.get("net_pl") if isinstance(t, dict) else None
        if v is None:
            continue
        try:
            net_vals.append(float(v))
        except Exception:
            continue

    if not net_vals:
        return {"ratio": None, "status": "—", "class": "", "biggest": 0.0, "denom": 0.0}

    total_pnl = sum(net_vals)
    winners = [v for v in net_vals if v > 0]
    losers = [v for v in net_vals if v < 0]

    if total_pnl > 0:
        biggest = max(winners) if winners else 0.0
        denom = total_pnl
        ratio = (biggest / denom) if denom else None
    elif total_pnl < 0:
        biggest = max(abs(v) for v in losers) if losers else 0.0
        denom = abs(total_pnl)
        ratio = (biggest / denom) if denom else None
    else:
        return {"ratio": None, "status": "—", "class": "", "biggest": 0.0, "denom": 0.0}

    ok = (ratio is not None) and (ratio <= 0.30)
    return {
        "ratio": ratio,
        "status": "✅ Pass" if ok else "🚫 Fail",
        "class": "glow-green" if ok else "glow-red",
        "biggest": biggest,
        "denom": denom,
    }


def week_range_for(day_iso: Optional[str]) -> tuple[str, str]:
    if not day_iso:
        day_iso = datetime.now().date().isoformat()
    d = datetime.strptime(day_iso, "%Y-%m-%d").date()
    start = d - timedelta(days=d.weekday())
    end = start + timedelta(days=7)
    return start.isoformat(), end.isoformat()


def week_total_net(
    day_iso: str,
    *,
    account_id: int | None = None,
    account_ids: list[int] | tuple[int, ...] | None = None,
) -> float:
    start, end = week_range_for(day_iso)
    with db() as conn:
        where = ["trade_date >= ?", "trade_date < ?"]
        params: list[Any] = [start, end]
        account_filter, account_params = _account_scope_filter(account_id, account_ids)
        if account_filter:
            where.append(account_filter)
            params.extend(account_params)
        row = conn.execute(
            """
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE """
            + " AND ".join(where),
            params,
        ).fetchone()
    return float(row["net"] or 0.0)


def account_scope_snapshot() -> Dict[str, Any]:
    active_id = _active_account_record_id()
    if active_id:
        account = get_account(active_id)
        if account:
            return {
                "enabled": True,
                "start_date": "",
                "starting_balance": float(account.get("starting_balance") or 0.0),
                "label": str(account.get("account_name") or "").strip(),
                "account_id": str(account.get("id") or ""),
                "account_name": str(account.get("account_name") or "").strip(),
                "account_type": str(account.get("prop_firm") or "").strip(),
                "broker_account_id": str(account.get("broker_account_id") or "").strip(),
                "account_size": float(account.get("account_size") or 0.0),
                "current_balance": float(account.get("current_balance") or 0.0),
            }
    start_date = str(get_setting_value("active_account_start_date", "") or "").strip()
    label = str(get_setting_value("active_account_label", "") or "").strip()
    account_id = str(get_setting_value("active_account_id", "") or "").strip()
    account_name = str(get_setting_value("active_account_name", "") or "").strip() or label
    account_type = str(get_setting_value("active_account_type", "") or "").strip()
    if not start_date:
        return {
            "enabled": False,
            "start_date": "",
            "starting_balance": float(get_setting_float("starting_balance", 50000.0)),
            "label": label,
            "account_id": account_id,
            "account_name": account_name,
            "account_type": account_type,
            "broker_account_id": account_id,
            "account_size": float(get_setting_float("starting_balance", 50000.0)),
        }
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        return {
            "enabled": False,
            "start_date": "",
            "starting_balance": float(get_setting_float("starting_balance", 50000.0)),
            "label": label,
            "account_id": account_id,
            "account_name": account_name,
            "account_type": account_type,
            "broker_account_id": account_id,
            "account_size": float(get_setting_float("starting_balance", 50000.0)),
        }
    scoped_starting = float(
        get_setting_float(
            "active_account_start_balance", float(get_setting_float("starting_balance", 50000.0))
        )
    )
    return {
        "enabled": True,
        "start_date": start_date,
        "starting_balance": scoped_starting,
        "label": label,
        "account_id": account_id,
        "account_name": account_name,
        "account_type": account_type,
        "broker_account_id": account_id,
        "account_size": scoped_starting,
    }


def save_account_scope(start_date: str, starting_balance: float, label: str = "") -> None:
    active_id = _active_account_record_id()
    if active_id:
        account = get_account(active_id)
        if account:
            update_account(
                active_id,
                account_name=str(label or account.get("account_name") or "").strip()
                or str(account.get("account_name") or "").strip(),
                starting_balance=float(starting_balance),
                prop_firm=str(account.get("prop_firm") or "").strip(),
                broker_account_id=str(account.get("broker_account_id") or "").strip(),
                account_size=float(starting_balance),
                max_drawdown=float(account.get("max_drawdown") or 0.0),
            )
            set_active_account(active_id)
            return
    set_setting_value("active_account_start_date", str(start_date).strip())
    set_setting_value("active_account_start_balance", f"{float(starting_balance):.2f}")
    set_setting_value("active_account_label", str(label or "").strip())
    if str(label or "").strip():
        set_setting_value("active_account_name", str(label or "").strip())


def clear_account_scope() -> None:
    set_setting_value("active_account_record_id", "")
    set_setting_value("active_account_start_date", "")
    set_setting_value("active_account_start_balance", "")
    set_setting_value("active_account_label", "")


def clear_trades() -> None:
    with db() as conn:
        conn.execute("DELETE FROM trades")


def recompute_balances(starting_balance: float = 50000.0, *, account_id: int | None = None) -> None:
    with db() as conn:
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]
        except Exception:
            cols = []
        where, params = _trade_scope_clause(account_id=account_id)
        if "ticker" in cols:
            where.append("COALESCE(ticker, '') <> 'ACCT'")
        query = """
            SELECT id, net_pl
            FROM trades
        """
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY trade_date ASC, id ASC"
        rows = conn.execute(query, params).fetchall()
        bal = float(starting_balance)
        conn.execute("BEGIN")
        for r in rows:
            net = r["net_pl"]
            if net is not None:
                bal += float(net)
            conn.execute("UPDATE trades SET balance = ? WHERE id = ?", (bal, r["id"]))
        conn.commit()


def latest_trade_day() -> Optional[date]:
    with db() as conn:
        row = conn.execute(
            """
            SELECT trade_date
            FROM trades
            WHERE trade_date IS NOT NULL AND trade_date != ''
            ORDER BY trade_date DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
    if not row:
        return None
    try:
        return datetime.strptime(row["trade_date"], "%Y-%m-%d").date()
    except Exception:
        return None


def latest_balance_overall(
    as_of: str | None = None,
    *,
    account_id: int | None = None,
    account_ids: list[int] | tuple[int, ...] | None = None,
    start_date: str | None = None,
    starting_balance: float | None = None,
) -> float:
    starting = (
        float(starting_balance)
        if starting_balance is not None
        else float(get_setting_float("starting_balance", 50000.0))
    )
    with db() as conn:
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]
        except Exception:
            return float(starting)

        def _pick(existing: list[str], preferred: list[str]) -> str | None:
            for c in preferred:
                if c in existing:
                    return c
            return None

        pnl_col = _pick(
            cols,
            ["net_pl", "pnl", "profit_loss", "pl", "profit", "p_l", "net_pnl"],
        )
        if not pnl_col:
            return float(starting)
        date_col = _pick(cols, ["trade_date", "date", "day"])

        def _q(col: str) -> str:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col):
                raise ValueError(f"Unsafe column name: {col}")
            return f'"{col}"'

        try:
            pnl_q = _q(pnl_col)
            where: list[str] = []
            params: list[Any] = []
            scope_where, scope_params = _trade_scope_clause(
                account_id=account_id,
                account_ids=account_ids,
                start_date=str(start_date or ""),
            )
            where.extend(scope_where)
            params.extend(scope_params)
            if "ticker" in cols:
                where.append("COALESCE(ticker, '') <> 'ACCT'")
            if date_col and as_of:
                where.append(f"{_q(date_col)} <= ?")
                params.append(str(as_of))
            if where:
                row = conn.execute(
                    f"SELECT COALESCE(SUM(CAST({pnl_q} AS REAL)), 0) FROM trades WHERE {' AND '.join(where)}",
                    params,
                ).fetchone()
            else:
                row = conn.execute(
                    f"SELECT COALESCE(SUM(CAST({pnl_q} AS REAL)), 0) FROM trades"
                ).fetchone()
            total = float(row[0] or 0.0)
        except Exception:
            total = 0.0
    return float(starting + total)


def balance_integrity_snapshot(
    as_of: str | None = None,
    tolerance: float = 0.01,
    *,
    account_id: int | None = None,
    account_ids: list[int] | tuple[int, ...] | None = None,
    start_date: str | None = None,
    starting_balance: float | None = None,
) -> Dict[str, Any]:
    derived = float(
        latest_balance_overall(
            as_of=as_of,
            account_id=account_id,
            account_ids=account_ids,
            start_date=start_date,
            starting_balance=starting_balance,
        )
    )
    starting = (
        float(starting_balance)
        if starting_balance is not None
        else float(get_setting_float("starting_balance", 50000.0))
    )
    out: Dict[str, Any] = {
        "starting_balance": starting,
        "canonical_balance": derived,
        "source_label": "Derived ledger",
        "source_detail": (
            f"Scoped from {start_date}; start balance plus closed trade net P/L."
            if start_date
            else "Starting balance plus closed trade net P/L."
        ),
        "derived_balance": derived,
        "stored_balance": None,
        "stored_status_label": "No snapshot",
        "stored_status_tone": "neutral",
        "delta": None,
        "has_drift": False,
        "tolerance": float(tolerance),
    }
    if account_id or _clean_account_ids(account_ids):
        out["source_detail"] = (
            "Active account views use derived ledger math. "
            "Stored row balance drift is only comparable on all-history ledger rows."
        )
        out["stored_status_label"] = "Derived only"
        return out
    with db() as conn:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]

        def _pick(existing: list[str], preferred: list[str]) -> str | None:
            for c in preferred:
                if c in existing:
                    return c
            return None

        bal_col = _pick(cols, ["balance", "running_balance", "equity", "account_balance"])
        if not bal_col:
            return out
        date_col = _pick(cols, ["trade_date", "date", "day"])

        def _q(col: str) -> str:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col):
                raise ValueError(f"Unsafe column name: {col}")
            return f'"{col}"'

        acct_filter = "COALESCE(ticker, '') <> 'ACCT'" if "ticker" in cols else ""

        where: list[str] = [f"{_q(bal_col)} IS NOT NULL"]
        params: list[Any] = []
        if account_id:
            where.append("account_id = ?")
            params.append(int(account_id))
        if acct_filter:
            where.append(acct_filter)
        if date_col and start_date:
            where.append(f"{_q(date_col)} >= ?")
            params.append(str(start_date))
        if date_col and as_of:
            where.append(f"{_q(date_col)} <= ?")
            params.append(str(as_of))

        order_by = f"{_q(date_col)} DESC, id DESC" if date_col else "id DESC"
        row = conn.execute(
            f"""
            SELECT {_q(bal_col)} AS bal, id{f", {_q(date_col)} AS d" if date_col else ""}
            FROM trades
            WHERE {' AND '.join(where)}
            ORDER BY {order_by}
            LIMIT 1
            """,
            params,
        ).fetchone()

        # Stored balance rows are all-history cumulative. In active-account mode we
        # rebase them at the scope boundary so drift compares like-for-like.
        if row and row["bal"] is not None and start_date and date_col:
            pre_where: list[str] = [f"{_q(bal_col)} IS NOT NULL", f"{_q(date_col)} < ?"]
            pre_params: list[Any] = [str(start_date)]
            if account_id:
                pre_where.append("account_id = ?")
                pre_params.append(int(account_id))
            if acct_filter:
                pre_where.append(acct_filter)
            if as_of:
                pre_where.append(f"{_q(date_col)} <= ?")
                pre_params.append(str(as_of))
            pre_row = conn.execute(
                f"""
                SELECT {_q(bal_col)} AS bal
                FROM trades
                WHERE {' AND '.join(pre_where)}
                ORDER BY {_q(date_col)} DESC, id DESC
                LIMIT 1
                """,
                pre_params,
            ).fetchone()
            try:
                latest_raw = float(row["bal"])
                if pre_row and pre_row["bal"] is not None:
                    pre_raw = float(pre_row["bal"])
                    row = {"bal": starting + (latest_raw - pre_raw)}
                else:
                    global_start = float(get_setting_float("starting_balance", 50000.0))
                    row = {"bal": starting + (latest_raw - global_start)}
            except Exception:
                pass
    if not row or row["bal"] is None:
        return out
    try:
        stored = float(row["bal"])
    except Exception:
        return out
    delta = derived - stored
    out["stored_balance"] = stored
    out["stored_status_label"] = "Drift check"
    out["delta"] = delta
    out["has_drift"] = abs(delta) > float(tolerance)
    out["stored_status_tone"] = "critical" if out["has_drift"] else "healthy"
    if not out["has_drift"]:
        out["source_detail"] = "Stored trade balances match the derived ledger."
    return out


def month_heatmap(
    year: int,
    month: int,
    *,
    account_id: int | None = None,
    account_ids: list[int] | tuple[int, ...] | None = None,
    start_date: str = "",
    starting_balance: float | None = None,
) -> Dict[str, Any]:
    first = date(year, month, 1)
    nxt = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    days_in_month = (nxt - first).days
    with db() as conn:
        where = ["trade_date >= ?", "trade_date < ?"]
        params: list[Any] = [first.isoformat(), nxt.isoformat()]
        bal_where = ["trade_date >= ?", "trade_date < ?", "balance IS NOT NULL"]
        bal_params: list[Any] = [first.isoformat(), nxt.isoformat()]
        account_filter, account_params = _account_scope_filter(account_id, account_ids)
        if account_filter:
            where.append(account_filter)
            params.extend(account_params)
            bal_where.append(account_filter)
            bal_params.extend(account_params)
        if start_date:
            where.append("trade_date >= ?")
            params.append(str(start_date))
            bal_where.append("trade_date >= ?")
            bal_params.append(str(start_date))
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]
        except Exception:
            cols = []
        if "ticker" in cols:
            where.append("COALESCE(ticker, '') <> 'ACCT'")
            bal_where.append("COALESCE(ticker, '') <> 'ACCT'")
        rows = conn.execute(
            f"""
            SELECT
                trade_date,
                COALESCE(SUM(net_pl), 0) AS net,
                COUNT(*) AS trade_count,
                SUM(CASE WHEN COALESCE(net_pl, 0) > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN COALESCE(net_pl, 0) < 0 THEN 1 ELSE 0 END) AS losses
            FROM trades
            WHERE {' AND '.join(where)}
            GROUP BY trade_date
            """,
            params,
        ).fetchall()
        bal_rows = conn.execute(
            f"""
            SELECT trade_date, balance, id
            FROM trades
            WHERE {' AND '.join(bal_where)}
            ORDER BY trade_date ASC, id ASC
            """,
            bal_params,
        ).fetchall()
        pre_balance = None
        if start_date:
            pre_where = ["trade_date < ?", "balance IS NOT NULL"]
            pre_params: list[Any] = [str(start_date)]
            account_filter, account_params = _account_scope_filter(account_id, account_ids)
            if account_filter:
                pre_where.append(account_filter)
                pre_params.extend(account_params)
            if "ticker" in cols:
                pre_where.append("COALESCE(ticker, '') <> 'ACCT'")
            pre_row = conn.execute(
                f"""
                SELECT balance
                FROM trades
                WHERE {' AND '.join(pre_where)}
                ORDER BY trade_date DESC, id DESC
                LIMIT 1
                """,
                pre_params,
            ).fetchone()
            if pre_row and pre_row["balance"] is not None:
                try:
                    pre_balance = float(pre_row["balance"])
                except Exception:
                    pre_balance = None
    daily_stats = {
        r["trade_date"]: {
            "net": float(r["net"] or 0.0),
            "trade_count": int(r["trade_count"] or 0),
            "wins": int(r["wins"] or 0),
            "losses": int(r["losses"] or 0),
        }
        for r in rows
    }
    daily_balance: Dict[str, float] = {}
    scoped_start = (
        float(starting_balance)
        if starting_balance is not None
        else float(get_setting_float("starting_balance", 50000.0))
    )
    global_start = float(get_setting_float("starting_balance", 50000.0))
    for r in bal_rows:
        try:
            raw_balance = float(r["balance"])
            if start_date:
                baseline = pre_balance if pre_balance is not None else global_start
                daily_balance[r["trade_date"]] = scoped_start + (raw_balance - baseline)
            else:
                daily_balance[r["trade_date"]] = raw_balance
        except Exception:
            pass
    start_weekday = (first.weekday() + 1) % 7
    weekday_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    cells: List[Dict[str, Any]] = []
    max_abs = 0.0
    for _ in range(start_weekday):
        cells.append(
            {
                "daynum": None,
                "net": 0.0,
                "iso": "",
                "wd": None,
                "weekday_label": "",
                "trade_count": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "balance": None,
                "is_weekend": False,
                "has_trades": False,
                "outcome": "empty",
            }
        )
    for daynum in range(1, days_in_month + 1):
        iso = date(year, month, daynum).isoformat()
        stats = daily_stats.get(iso, {})
        net = float(stats.get("net", 0.0))
        wins = int(stats.get("wins", 0) or 0)
        losses = int(stats.get("losses", 0) or 0)
        trade_count = int(stats.get("trade_count", 0) or 0)
        total_decisions = wins + losses
        weekday = date(year, month, daynum).weekday()
        is_weekend = weekday >= 5
        if trade_count <= 0:
            outcome = "closed" if is_weekend else "flat"
        elif net > 0:
            outcome = "gain"
        elif net < 0:
            outcome = "loss"
        else:
            outcome = "flat"
        max_abs = max(max_abs, abs(net))
        cells.append(
            {
                "daynum": daynum,
                "net": net,
                "iso": iso,
                "wd": weekday,
                "weekday_label": weekday_names[(weekday + 1) % 7],
                "trade_count": trade_count,
                "wins": wins,
                "losses": losses,
                "win_rate": (wins / total_decisions * 100.0) if total_decisions else 0.0,
                "balance": daily_balance.get(iso),
                "is_weekend": is_weekend,
                "has_trades": trade_count > 0,
                "outcome": outcome,
            }
        )
    while len(cells) % 7 != 0:
        cells.append(
            {
                "daynum": None,
                "net": 0.0,
                "iso": "",
                "wd": None,
                "weekday_label": "",
                "trade_count": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "balance": None,
                "is_weekend": False,
                "has_trades": False,
                "outcome": "empty",
            }
        )
    weeks: List[Dict[str, Any]] = []
    for i in range(0, len(cells), 7):
        week_days = cells[i : i + 7]
        active_days = [d for d in week_days if d["daynum"] is not None]
        week_start = next((d for d in week_days if d["daynum"] is not None), None)
        week_end = next((d for d in reversed(week_days) if d["daynum"] is not None), None)
        weeks.append(
            {
                "days": week_days,
                "net": sum(float(d["net"]) for d in active_days),
                "trade_count": sum(int(d["trade_count"]) for d in active_days),
                "traded_days": sum(1 for d in active_days if d["trade_count"] > 0),
                "label": (
                    f"{week_start['iso']} to {week_end['iso']}"
                    if week_start and week_end
                    else "Out of month"
                ),
            }
        )
    return {
        "year": year,
        "month": month,
        "weeks": weeks,
        "max_abs": max_abs or 1.0,
        "daily_balance": daily_balance,
    }


def last_n_trading_day_totals(
    n: int = 20,
    since_date: str = "",
    *,
    account_id: int | None = None,
    account_ids: list[int] | tuple[int, ...] | None = None,
) -> List[float]:
    with db() as conn:
        where: list[str] = []
        params: list[Any] = []
        if since_date:
            where.append("trade_date >= ?")
            params.append(since_date)
        account_filter, account_params = _account_scope_filter(account_id, account_ids)
        if account_filter:
            where.append(account_filter)
            params.extend(account_params)
        rows = conn.execute(
            f"""
            SELECT trade_date, COALESCE(SUM(net_pl),0) AS net
            FROM trades
            {'WHERE ' + ' AND '.join(where) if where else ''}
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 200
            """,
            params,
        ).fetchall()
    out: List[float] = []
    for r in rows:
        try:
            d = datetime.strptime(r["trade_date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if d.weekday() >= 5:
            continue
        out.append(float(r["net"] or 0.0))
        if len(out) >= n:
            break
    return out


def projections_from_daily(
    daily_vals: List[float], base_balance: Optional[float]
) -> Dict[str, Any]:
    avg = (sum(daily_vals) / len(daily_vals)) if daily_vals else 0.0
    base = float(base_balance or 0.0)

    def _proj(days: int) -> Dict[str, Any]:
        est = avg * days
        return {"days": days, "daily_avg": avg, "est_pnl": est, "est_balance": base + est}

    return {"avg": avg, "base_balance": base, "p5": _proj(5), "p10": _proj(10), "p20": _proj(20)}


def month_total_net(
    year: int,
    month: int,
    *,
    account_id: int | None = None,
    account_ids: list[int] | tuple[int, ...] | None = None,
) -> float:
    first = date(year, month, 1)
    nxt = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    with db() as conn:
        where = ["trade_date >= ?", "trade_date < ?"]
        params: list[Any] = [first.isoformat(), nxt.isoformat()]
        account_filter, account_params = _account_scope_filter(account_id, account_ids)
        if account_filter:
            where.append(account_filter)
            params.extend(account_params)
        row = conn.execute(
            f"""
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE {' AND '.join(where)}
            """,
            params,
        ).fetchone()
    return float(row["net"] or 0.0)


def ytd_total_net(
    year: int,
    *,
    account_id: int | None = None,
    account_ids: list[int] | tuple[int, ...] | None = None,
) -> float:
    start = date(year, 1, 1)
    end = date(year + 1, 1, 1)
    with db() as conn:
        where = ["trade_date >= ?", "trade_date < ?"]
        params: list[Any] = [start.isoformat(), end.isoformat()]
        account_filter, account_params = _account_scope_filter(account_id, account_ids)
        if account_filter:
            where.append(account_filter)
            params.extend(account_params)
        row = conn.execute(
            f"""
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE {' AND '.join(where)}
            """,
            params,
        ).fetchone()
    return float(row["net"] or 0.0)


def month_trade_count(
    year: int,
    month: int,
    *,
    account_id: int | None = None,
    account_ids: list[int] | tuple[int, ...] | None = None,
) -> int:
    first = date(year, month, 1)
    nxt = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    with db() as conn:
        where = ["trade_date >= ?", "trade_date < ?"]
        params: list[Any] = [first.isoformat(), nxt.isoformat()]
        account_filter, account_params = _account_scope_filter(account_id, account_ids)
        if account_filter:
            where.append(account_filter)
            params.extend(account_params)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM trades
            WHERE {' AND '.join(where)}
            """,
            params,
        ).fetchone()
    return int(row["c"] or 0)


def ytd_trade_count(
    year: int,
    *,
    account_id: int | None = None,
    account_ids: list[int] | tuple[int, ...] | None = None,
) -> int:
    start = date(year, 1, 1)
    end = date(year + 1, 1, 1)
    with db() as conn:
        where = ["trade_date >= ?", "trade_date < ?"]
        params: list[Any] = [start.isoformat(), end.isoformat()]
        account_filter, account_params = _account_scope_filter(account_id, account_ids)
        if account_filter:
            where.append(account_filter)
            params.extend(account_params)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM trades
            WHERE {' AND '.join(where)}
            """,
            params,
        ).fetchone()
    return int(row["c"] or 0)
