"""Journal repository functions."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from mccain_capital.migrations import run_migrations
from mccain_capital.runtime import DB_PATH, db, now_iso, today_iso


def ensure_journal_schema() -> None:
    run_migrations(DB_PATH)


def fetch_entries(q: str = "", d: str = "") -> List[object]:
    ensure_journal_schema()
    q = (q or "").strip()
    d = (d or "").strip()

    sql = """
    SELECT
      e.*,
      (
        SELECT COUNT(*)
        FROM entry_trade_links l
        WHERE l.entry_id = e.id
      ) AS linked_trades
    FROM entries e
    """
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
    ensure_journal_schema()
    with db() as conn:
        return conn.execute("SELECT * FROM entries WHERE id = ?", (entry_id,)).fetchone()


def create_entry(data: Dict[str, Any]) -> int:
    ensure_journal_schema()
    created = now_iso()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO entries (
              entry_date, market, setup, grade, pnl, mood, notes, entry_type, template_payload, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data.get("entry_date") or today_iso(),
                (data.get("market") or "").strip(),
                (data.get("setup") or "").strip(),
                (data.get("grade") or "").strip(),
                data.get("pnl"),
                (data.get("mood") or "").strip(),
                (data.get("notes") or "").strip(),
                (data.get("entry_type") or "post_market").strip(),
                json.dumps(data.get("template_payload") or {}, ensure_ascii=False),
                created,
                created,
            ),
        )
        return int(cur.lastrowid)


def update_entry(entry_id: int, data: Dict[str, Any]) -> None:
    ensure_journal_schema()
    updated = now_iso()
    with db() as conn:
        conn.execute(
            """
            UPDATE entries
            SET entry_date = ?, market = ?, setup = ?, grade = ?, pnl = ?, mood = ?, notes = ?, entry_type = ?, template_payload = ?, updated_at = ?
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
                (data.get("entry_type") or "post_market").strip(),
                json.dumps(data.get("template_payload") or {}, ensure_ascii=False),
                updated,
                entry_id,
            ),
        )


def delete_entry(entry_id: int) -> None:
    ensure_journal_schema()
    with db() as conn:
        conn.execute("DELETE FROM entry_trade_links WHERE entry_id = ?", (entry_id,))
        conn.execute("DELETE FROM entries WHERE id = ?", (entry_id,))


def fetch_entry_trade_ids(entry_id: int) -> List[int]:
    ensure_journal_schema()
    with db() as conn:
        rows = conn.execute(
            "SELECT trade_id FROM entry_trade_links WHERE entry_id = ? ORDER BY trade_id",
            (entry_id,),
        ).fetchall()
    return [int(r["trade_id"]) for r in rows]


def set_entry_trade_links(entry_id: int, trade_ids: List[int]) -> None:
    ensure_journal_schema()
    clean = sorted(set(int(i) for i in trade_ids if int(i) > 0))
    with db() as conn:
        conn.execute("DELETE FROM entry_trade_links WHERE entry_id = ?", (entry_id,))
        for tid in clean:
            conn.execute(
                "INSERT INTO entry_trade_links (entry_id, trade_id, created_at) VALUES (?, ?, ?)",
                (entry_id, tid, now_iso()),
            )


def fetch_entries_range(start_date: str, end_date: str) -> List[object]:
    ensure_journal_schema()
    with db() as conn:
        return list(
            conn.execute(
                """
                SELECT * FROM entries
                WHERE entry_date BETWEEN ? AND ?
                ORDER BY entry_date ASC, id ASC
                """,
                (start_date, end_date),
            ).fetchall()
        )


def fetch_entry_day_rollups(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    ensure_journal_schema()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT
              entry_date,
              COUNT(*) AS entry_count,
              COALESCE(SUM(COALESCE(pnl, 0)), 0) AS pnl_total,
              GROUP_CONCAT(DISTINCT NULLIF(entry_type, '')) AS entry_types,
              GROUP_CONCAT(DISTINCT NULLIF(mood, '')) AS moods,
              GROUP_CONCAT(DISTINCT NULLIF(setup, '')) AS setups
            FROM entries
            WHERE entry_date BETWEEN ? AND ?
            GROUP BY entry_date
            ORDER BY entry_date ASC
            """,
            (start_date, end_date),
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "entry_date": str(row["entry_date"]),
                "entry_count": int(row["entry_count"] or 0),
                "pnl_total": float(row["pnl_total"] or 0.0),
                "entry_types": [v.strip() for v in str(row["entry_types"] or "").split(",") if v.strip()],
                "moods": [v.strip() for v in str(row["moods"] or "").split(",") if v.strip()],
                "setups": [v.strip() for v in str(row["setups"] or "").split(",") if v.strip()],
            }
        )
    return out


def weekly_setup_stats(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    ensure_journal_schema()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT
              COALESCE(NULLIF(r.strategy_label, ''), NULLIF(r.setup_tag, ''), 'Unlabeled') AS setup,
              COUNT(*) AS count,
              COALESCE(SUM(COALESCE(t.net_pl, 0)), 0) AS net,
              COALESCE(SUM(CASE WHEN COALESCE(t.net_pl,0) > 0 THEN 1 ELSE 0 END), 0) AS wins
            FROM entry_trade_links l
            JOIN entries e ON e.id = l.entry_id
            JOIN trades t ON t.id = l.trade_id
            LEFT JOIN trade_reviews r ON r.trade_id = t.id
            WHERE e.entry_date BETWEEN ? AND ?
            GROUP BY setup
            ORDER BY net DESC
            """,
            (start_date, end_date),
        ).fetchall()
    out = []
    for r in rows:
        c = int(r["count"] or 0)
        out.append(
            {
                "setup": r["setup"],
                "count": c,
                "net": float(r["net"] or 0.0),
                "win_rate": (float(r["wins"] or 0.0) / c * 100.0) if c else 0.0,
            }
        )
    return out


def weekly_mood_stats(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    ensure_journal_schema()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT
              COALESCE(NULLIF(mood,''), 'Unlabeled') AS mood,
              COUNT(*) AS count,
              AVG(COALESCE(pnl, 0)) AS avg_pnl,
              SUM(CASE WHEN COALESCE(pnl,0) > 0 THEN 1 ELSE 0 END) AS wins
            FROM entries
            WHERE entry_date BETWEEN ? AND ?
            GROUP BY mood
            ORDER BY count DESC, avg_pnl DESC
            """,
            (start_date, end_date),
        ).fetchall()
    out = []
    for r in rows:
        c = int(r["count"] or 0)
        out.append(
            {
                "mood": r["mood"],
                "count": c,
                "avg_pnl": float(r["avg_pnl"] or 0.0),
                "win_rate": (float(r["wins"] or 0.0) / c * 100.0) if c else 0.0,
            }
        )
    return out


def weekly_rule_break_tags(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    ensure_journal_schema()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT r.rule_break_tags
            FROM entry_trade_links l
            JOIN entries e ON e.id = l.entry_id
            LEFT JOIN trade_reviews r ON r.trade_id = l.trade_id
            WHERE e.entry_date BETWEEN ? AND ?
            """,
            (start_date, end_date),
        ).fetchall()
    counts: Dict[str, int] = {}
    for row in rows:
        tags = (row["rule_break_tags"] or "").strip()
        if not tags:
            continue
        for tag in [t.strip().lower() for t in tags.split(",") if t.strip()]:
            if tag == "ultra-short-hold":
                continue
            counts[tag] = counts.get(tag, 0) + 1
    return [
        {"tag": k, "count": v}
        for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:12]
    ]
