"""Strategies repository functions."""

from __future__ import annotations

from typing import Optional

from mccain_capital import app_core as core


def fetch_strategies():
    with core.db() as conn:
        return list(conn.execute("SELECT * FROM strategies ORDER BY updated_at DESC").fetchall())


def get_strategy(sid: int) -> Optional[object]:
    with core.db() as conn:
        return conn.execute("SELECT * FROM strategies WHERE id = ?", (sid,)).fetchone()


def create_strategy(title: str, body: str) -> int:
    created = core.now_iso()
    with core.db() as conn:
        cur = conn.execute(
            """
            INSERT INTO strategies (title, body, created_at, updated_at)
            VALUES (?,?,?,?)
            """,
            (title.strip(), body.strip(), created, created),
        )
        return int(cur.lastrowid)


def update_strategy(sid: int, title: str, body: str) -> None:
    updated = core.now_iso()
    with core.db() as conn:
        conn.execute(
            """
            UPDATE strategies
            SET title = ?, body = ?, updated_at = ?
            WHERE id = ?
            """,
            (title.strip(), body.strip(), updated, sid),
        )


def delete_strategy(sid: int) -> None:
    with core.db() as conn:
        conn.execute("DELETE FROM strategies WHERE id = ?", (sid,))

