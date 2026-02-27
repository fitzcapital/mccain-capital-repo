"""Lightweight SQLite migration runner."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Callable, List, Tuple

MigrationFn = Callable[[sqlite3.Connection], None]


def _migration_0001_baseline(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT NOT NULL,
            market TEXT DEFAULT '',
            setup TEXT DEFAULT '',
            grade TEXT DEFAULT '',
            pnl REAL,
            mood TEXT DEFAULT '',
            notes TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_entries_date ON entries(entry_date);

        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            entry_time TEXT DEFAULT '',
            exit_time TEXT DEFAULT '',
            ticker TEXT DEFAULT '',
            opt_type TEXT DEFAULT '',
            strike REAL,
            entry_price REAL,
            exit_price REAL,
            contracts INTEGER,
            total_spent REAL,
            stop_pct REAL,
            target_pct REAL,
            stop_price REAL,
            take_profit REAL,
            risk REAL,
            comm REAL,
            gross_pl REAL,
            net_pl REAL,
            result_pct REAL,
            balance REAL,
            raw_line TEXT DEFAULT '',
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(trade_date);
        CREATE INDEX IF NOT EXISTS idx_trades_ticker ON trades(ticker);

        CREATE TABLE IF NOT EXISTS trade_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INTEGER NOT NULL UNIQUE,
            setup_tag TEXT DEFAULT '',
            session_tag TEXT DEFAULT '',
            checklist_score INTEGER DEFAULT NULL,
            rule_break_tags TEXT DEFAULT '',
            review_note TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_trade_reviews_trade_id ON trade_reviews(trade_id);
        CREATE INDEX IF NOT EXISTS idx_trade_reviews_setup ON trade_reviews(setup_tag);

        CREATE TABLE IF NOT EXISTS risk_controls (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            daily_max_loss REAL DEFAULT 0,
            enforce_lockout INTEGER DEFAULT 0,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS strategies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_strategies_updated ON strategies(updated_at);

        CREATE TABLE IF NOT EXISTS daily_goals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_date TEXT NOT NULL UNIQUE,
            debt_paid REAL DEFAULT 0,
            debt_note TEXT DEFAULT '',
            upwork_proposals INTEGER DEFAULT 0,
            upwork_interviews INTEGER DEFAULT 0,
            upwork_hours REAL DEFAULT 0,
            upwork_earnings REAL DEFAULT 0,
            other_income REAL DEFAULT 0,
            notes TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_daily_goals_date ON daily_goals(track_date);
        """
    )
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT OR IGNORE INTO risk_controls (id, daily_max_loss, enforce_lockout, updated_at)
        VALUES (1, 0, 0, ?)
        """,
        (now,),
    )


def _migration_0002_journal_phase2(conn: sqlite3.Connection) -> None:
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(entries)").fetchall()]
    if "entry_type" not in cols:
        conn.execute("ALTER TABLE entries ADD COLUMN entry_type TEXT DEFAULT 'post_market'")
    if "template_payload" not in cols:
        conn.execute("ALTER TABLE entries ADD COLUMN template_payload TEXT DEFAULT '{}'")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entry_trade_links (
            entry_id INTEGER NOT NULL,
            trade_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (entry_id, trade_id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_entry_trade_links_entry ON entry_trade_links(entry_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_entry_trade_links_trade ON entry_trade_links(trade_id)"
    )


def _migration_0003_import_batches(conn: sqlite3.Connection) -> None:
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]
    if "import_batch_id" not in cols:
        conn.execute("ALTER TABLE trades ADD COLUMN import_batch_id TEXT DEFAULT ''")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_import_batch ON trades(import_batch_id)")


MIGRATIONS: List[Tuple[str, MigrationFn]] = [
    ("0001_baseline", _migration_0001_baseline),
    ("0002_journal_phase2", _migration_0002_journal_phase2),
    ("0003_import_batches", _migration_0003_import_batches),
]


def run_migrations(db_path: str) -> List[str]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
        """
    )

    applied = {
        r["id"] for r in conn.execute("SELECT id FROM schema_migrations ORDER BY id").fetchall()
    }
    new_applied: List[str] = []
    for mid, fn in MIGRATIONS:
        if mid in applied:
            continue
        fn(conn)
        conn.execute(
            "INSERT INTO schema_migrations (id, applied_at) VALUES (?, ?)",
            (mid, datetime.now().isoformat(timespec="seconds")),
        )
        new_applied.append(mid)
    conn.commit()
    conn.close()
    return new_applied
