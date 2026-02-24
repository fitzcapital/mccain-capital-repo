"""Goals repository functions."""

from __future__ import annotations

from typing import Any, Dict, Optional

from mccain_capital import app_core as core


def upsert_daily_goal(track_date: str, payload: Dict[str, Any]) -> None:
    now = core.now_iso()
    with core.db() as conn:
        conn.execute(
            """
            INSERT INTO daily_goals
              (track_date, debt_paid, debt_note, upwork_proposals, upwork_interviews,
               upwork_hours, upwork_earnings, other_income, notes, created_at, updated_at)
            VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(track_date) DO UPDATE SET
              debt_paid=excluded.debt_paid,
              debt_note=excluded.debt_note,
              upwork_proposals=excluded.upwork_proposals,
              upwork_interviews=excluded.upwork_interviews,
              upwork_hours=excluded.upwork_hours,
              upwork_earnings=excluded.upwork_earnings,
              other_income=excluded.other_income,
              notes=excluded.notes,
              updated_at=excluded.updated_at
            """,
            (
                track_date,
                payload.get("debt_paid", 0.0),
                payload.get("debt_note", ""),
                payload.get("upwork_proposals", 0),
                payload.get("upwork_interviews", 0),
                payload.get("upwork_hours", 0.0),
                payload.get("upwork_earnings", 0.0),
                payload.get("other_income", 0.0),
                payload.get("notes", ""),
                now,
                now,
            ),
        )


def fetch_daily_goals(start_iso: str, end_iso: str):
    with core.db() as conn:
        return conn.execute(
            """
            SELECT * FROM daily_goals
            WHERE track_date BETWEEN ? AND ?
            ORDER BY track_date DESC
            """,
            (start_iso, end_iso),
        ).fetchall()


def fetch_daily_goal(track_date: str) -> Optional[object]:
    with core.db() as conn:
        return conn.execute(
            "SELECT * FROM daily_goals WHERE track_date=? LIMIT 1", (track_date,)
        ).fetchone()

