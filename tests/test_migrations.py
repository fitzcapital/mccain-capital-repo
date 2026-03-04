"""Database migration runner tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from mccain_capital.migrations import run_migrations


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_run_migrations_is_idempotent(tmp_path: Path):
    db_path = tmp_path / "migrate.db"

    first = run_migrations(str(db_path))
    assert "0001_baseline" in first
    assert "0002_journal_phase2" in first
    assert "0003_import_batches" in first
    assert "0004_strategy_links" in first

    second = run_migrations(str(db_path))
    assert second == []

    conn = sqlite3.connect(str(db_path))
    try:
        entries_cols = _table_columns(conn, "entries")
        assert "entry_type" in entries_cols
        assert "template_payload" in entries_cols

        links_cols = _table_columns(conn, "entry_trade_links")
        assert {"entry_id", "trade_id", "created_at"}.issubset(links_cols)

        applied = [
            r[0] for r in conn.execute("SELECT id FROM schema_migrations ORDER BY id").fetchall()
        ]
        assert applied == [
            "0001_baseline",
            "0002_journal_phase2",
            "0003_import_batches",
            "0004_strategy_links",
        ]
    finally:
        conn.close()
