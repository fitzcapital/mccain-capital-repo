"""Trades repository functions."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from mccain_capital import app_core as core


def fetch_trades(d: str = "", q: str = ""):
    d = (d or "").strip()
    q = (q or "").strip()

    sql = "SELECT * FROM trades"
    where = []
    params: List[Any] = []

    if d:
        where.append("trade_date = ?")
        params.append(d)

    if q:
        where.append("(ticker LIKE ? OR opt_type LIKE ? OR raw_line LIKE ?)")
        like = f"%{q}%"
        params.extend([like, like, like])

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY trade_date DESC, id DESC"

    with core.db() as conn:
        return list(conn.execute(sql, params).fetchall())


def fetch_trades_range(start_iso: str, end_iso: str):
    with core.db() as conn:
        return list(
            conn.execute(
                """
            SELECT * FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            ORDER BY trade_date ASC, id ASC
            """,
                (start_iso, end_iso),
            ).fetchall()
        )


def get_risk_controls() -> Dict[str, Any]:
    with core.db() as conn:
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
    with core.db() as conn:
        conn.execute(
            """
            INSERT INTO risk_controls (id, daily_max_loss, enforce_lockout, updated_at)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              daily_max_loss=excluded.daily_max_loss,
              enforce_lockout=excluded.enforce_lockout,
              updated_at=excluded.updated_at
            """,
            (abs(float(daily_max_loss or 0.0)), 1 if enforce_lockout else 0, core.now_iso()),
        )


def get_trade_review(trade_id: int) -> Optional[Dict[str, Any]]:
    with core.db() as conn:
        row = conn.execute(
            """
            SELECT trade_id, setup_tag, session_tag, checklist_score, rule_break_tags, review_note
            FROM trade_reviews
            WHERE trade_id = ?
            """,
            (trade_id,),
        ).fetchone()
    return dict(row) if row else None


def upsert_trade_review(
    trade_id: int,
    setup_tag: str = "",
    session_tag: str = "",
    checklist_score: Optional[int] = None,
    rule_break_tags: str = "",
    review_note: str = "",
) -> None:
    now = core.now_iso()
    score_val = None if checklist_score is None else max(0, min(100, int(checklist_score)))
    with core.db() as conn:
        conn.execute(
            """
            INSERT INTO trade_reviews
              (trade_id, setup_tag, session_tag, checklist_score, rule_break_tags, review_note, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_id) DO UPDATE SET
              setup_tag=excluded.setup_tag,
              session_tag=excluded.session_tag,
              checklist_score=excluded.checklist_score,
              rule_break_tags=excluded.rule_break_tags,
              review_note=excluded.review_note,
              updated_at=excluded.updated_at
            """,
            (
                trade_id,
                (setup_tag or "").strip(),
                (session_tag or "").strip(),
                score_val,
                (rule_break_tags or "").strip(),
                (review_note or "").strip(),
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
    with core.db() as conn:
        rows = conn.execute(
            f"""
            SELECT trade_id, setup_tag, session_tag, checklist_score, rule_break_tags, review_note
            FROM trade_reviews
            WHERE trade_id IN ({marks})
            """,
            clean_ids,
        ).fetchall()
    return {int(r["trade_id"]): dict(r) for r in rows}
