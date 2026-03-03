"""Strategies repository functions."""

from __future__ import annotations

from typing import Optional

from mccain_capital.runtime import db, now_iso


def fetch_strategies():
    with db() as conn:
        return list(conn.execute("SELECT * FROM strategies ORDER BY updated_at DESC").fetchall())


def get_strategy(sid: int) -> Optional[object]:
    with db() as conn:
        return conn.execute("SELECT * FROM strategies WHERE id = ?", (sid,)).fetchone()


def get_strategy_by_title(title: str) -> Optional[object]:
    clean = (title or "").strip()
    if not clean:
        return None
    with db() as conn:
        return conn.execute(
            "SELECT * FROM strategies WHERE LOWER(TRIM(title)) = LOWER(TRIM(?)) LIMIT 1",
            (clean,),
        ).fetchone()


def create_strategy(title: str, body: str) -> int:
    created = now_iso()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO strategies (title, body, created_at, updated_at)
            VALUES (?,?,?,?)
            """,
            (title.strip(), body.strip(), created, created),
        )
        return int(cur.lastrowid)


def ensure_strategy(title: str, body: str = "") -> Optional[dict]:
    clean = (title or "").strip()
    if not clean:
        return None
    existing = get_strategy_by_title(clean)
    if existing:
        row = dict(existing)
        return {
            "id": int(row["id"]),
            "title": str(row["title"]).strip(),
            "body": str(row["body"] or ""),
        }
    sid = create_strategy(
        title=clean,
        body=(body or "").strip()
        or "Auto-created from trade review/import flow. Add your execution rules here.",
    )
    created = get_strategy(sid)
    if not created:
        return {"id": sid, "title": clean, "body": body}
    row = dict(created)
    return {
        "id": int(row["id"]),
        "title": str(row["title"]).strip(),
        "body": str(row["body"] or ""),
    }


def update_strategy(sid: int, title: str, body: str) -> None:
    updated = now_iso()
    with db() as conn:
        conn.execute(
            """
            UPDATE strategies
            SET title = ?, body = ?, updated_at = ?
            WHERE id = ?
            """,
            (title.strip(), body.strip(), updated, sid),
        )
        conn.execute(
            """
            UPDATE trade_reviews
            SET strategy_label = ?, setup_tag = ?, updated_at = ?
            WHERE strategy_id = ?
            """,
            (title.strip(), title.strip(), updated, sid),
        )


def delete_strategy(sid: int) -> None:
    with db() as conn:
        conn.execute("UPDATE trade_reviews SET strategy_id = NULL WHERE strategy_id = ?", (sid,))
        conn.execute("DELETE FROM strategies WHERE id = ?", (sid,))
