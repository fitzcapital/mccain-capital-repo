"""Journal repository functions."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from mccain_capital.runtime import db, now_iso, today_iso


def fetch_entries(q: str = "", d: str = "") -> List[object]:
    q = (q or "").strip()
    d = (d or "").strip()

    sql = "SELECT * FROM entries"
    where = []
    params: List[Any] = []

    if d:
        where.append("entry_date = ?")
        params.append(d)

    if q:
        where.append(
            "(notes LIKE ? OR market LIKE ? OR setup LIKE ? OR grade LIKE ? OR mood LIKE ?)"
        )
        like = f"%{q}%"
        params.extend([like, like, like, like, like])

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY entry_date DESC, updated_at DESC"

    with db() as conn:
        return list(conn.execute(sql, params).fetchall())


def get_entry(entry_id: int) -> Optional[object]:
    with db() as conn:
        return conn.execute("SELECT * FROM entries WHERE id = ?", (entry_id,)).fetchone()


def create_entry(data: Dict[str, Any]) -> int:
    created = now_iso()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO entries (entry_date, market, setup, grade, pnl, mood, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data.get("entry_date") or today_iso(),
                (data.get("market") or "").strip(),
                (data.get("setup") or "").strip(),
                (data.get("grade") or "").strip(),
                data.get("pnl"),
                (data.get("mood") or "").strip(),
                (data.get("notes") or "").strip(),
                created,
                created,
            ),
        )
        return int(cur.lastrowid)


def update_entry(entry_id: int, data: Dict[str, Any]) -> None:
    updated = now_iso()
    with db() as conn:
        conn.execute(
            """
            UPDATE entries
            SET entry_date = ?, market = ?, setup = ?, grade = ?, pnl = ?, mood = ?, notes = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                data.get("entry_date") or today_iso(),
                (data.get("market") or "").strip(),
                (data.get("setup") or "").strip(),
                (data.get("grade") or "").strip(),
                data.get("pnl"),
                (data.get("mood") or "").strip(),
                (data.get("notes") or "").strip(),
                updated,
                entry_id,
            ),
        )


def delete_entry(entry_id: int) -> None:
    with db() as conn:
        conn.execute("DELETE FROM entries WHERE id = ?", (entry_id,))
