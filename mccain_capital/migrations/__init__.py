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


def _migration_0004_strategy_links(conn: sqlite3.Connection) -> None:
    review_cols = [r["name"] for r in conn.execute("PRAGMA table_info(trade_reviews)").fetchall()]
    if "strategy_id" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN strategy_id INTEGER DEFAULT NULL")
    if "strategy_label" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN strategy_label TEXT DEFAULT ''")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_trade_reviews_strategy_id ON trade_reviews(strategy_id)"
    )

    now = datetime.now().isoformat(timespec="seconds")
    strategy_rows = conn.execute("SELECT id, title FROM strategies ORDER BY id").fetchall()
    strategy_map = {
        str(r["title"]).strip().lower(): (int(r["id"]), str(r["title"]).strip())
        for r in strategy_rows
    }

    review_rows = conn.execute(
        """
        SELECT trade_id, setup_tag, strategy_id, strategy_label
        FROM trade_reviews
        ORDER BY trade_id
        """
    ).fetchall()
    for row in review_rows:
        raw_label = str(row["strategy_label"] or "").strip() or str(row["setup_tag"] or "").strip()
        existing_id = row["strategy_id"]
        if existing_id:
            title_row = conn.execute(
                "SELECT title FROM strategies WHERE id = ?",
                (int(existing_id),),
            ).fetchone()
            if title_row:
                canonical = str(title_row["title"] or "").strip()
                conn.execute(
                    """
                    UPDATE trade_reviews
                    SET strategy_label = ?, setup_tag = ?
                    WHERE trade_id = ?
                    """,
                    (canonical, canonical, int(row["trade_id"])),
                )
                continue
        if not raw_label:
            continue
        strategy_key = raw_label.lower()
        strategy_id, canonical = strategy_map.get(strategy_key, (0, ""))
        if not strategy_id:
            cur = conn.execute(
                """
                INSERT INTO strategies (title, body, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    raw_label,
                    "Auto-created from existing trade review labels. Add your execution rules here.",
                    now,
                    now,
                ),
            )
            strategy_id = int(cur.lastrowid)
            canonical = raw_label
            strategy_map[strategy_key] = (strategy_id, canonical)
        conn.execute(
            """
            UPDATE trade_reviews
            SET strategy_id = ?, strategy_label = ?, setup_tag = ?
            WHERE trade_id = ?
            """,
            (strategy_id, canonical, canonical, int(row["trade_id"])),
        )


def _migration_0005_market_alerts(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            rule_type TEXT NOT NULL CHECK (rule_type IN ('above', 'below')),
            threshold REAL NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_alerts_symbol_enabled ON alerts(symbol, enabled);

        CREATE TABLE IF NOT EXISTS alert_fires (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            message TEXT NOT NULL,
            fired_at TEXT NOT NULL,
            UNIQUE(alert_id, fired_at)
        );
        CREATE INDEX IF NOT EXISTS idx_alert_fires_symbol_time ON alert_fires(symbol, fired_at DESC);
        """
    )


def _migration_0006_trade_review_rich_fields(conn: sqlite3.Connection) -> None:
    review_cols = [r["name"] for r in conn.execute("PRAGMA table_info(trade_reviews)").fetchall()]
    if "thesis_note" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN thesis_note TEXT DEFAULT ''")
    if "execution_grade" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN execution_grade INTEGER DEFAULT NULL")
    if "risk_grade" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN risk_grade INTEGER DEFAULT NULL")
    if "plan_grade" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN plan_grade INTEGER DEFAULT NULL")
    if "mistake_tags" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN mistake_tags TEXT DEFAULT ''")
    if "planned_risk_dollars" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN planned_risk_dollars REAL DEFAULT NULL")
    if "size_rule_note" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN size_rule_note TEXT DEFAULT ''")
    if "entry_quality_note" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN entry_quality_note TEXT DEFAULT ''")
    if "exit_quality_note" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN exit_quality_note TEXT DEFAULT ''")
    if "improvement_note" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN improvement_note TEXT DEFAULT ''")

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_trade_reviews_session ON trade_reviews(session_tag)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_trade_reviews_mistake_tags ON trade_reviews(mistake_tags)"
    )


def _migration_0007_trade_source(conn: sqlite3.Connection) -> None:
    trade_cols = [r["name"] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]
    if "trade_source" not in trade_cols:
        conn.execute("ALTER TABLE trades ADD COLUMN trade_source TEXT DEFAULT ''")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_trade_source ON trades(trade_source)")
    conn.execute(
        """
        UPDATE trades
        SET trade_source = CASE
            WHEN COALESCE(trade_source, '') <> '' THEN trade_source
            WHEN COALESCE(import_batch_id, '') <> '' THEN 'Statement Import'
            WHEN UPPER(COALESCE(raw_line, '')) = 'MANUAL ENTRY' THEN 'Manual Entry'
            WHEN UPPER(COALESCE(raw_line, '')) LIKE 'DUPLICATE OF #%' THEN 'Manual Entry'
            WHEN UPPER(COALESCE(raw_line, '')) LIKE '%BALANCE SNAPSHOT%' THEN 'Balance Snapshot'
            ELSE 'Unknown'
        END
        """
    )
    review_cols = [r["name"] for r in conn.execute("PRAGMA table_info(trade_reviews)").fetchall()]
    if "strategy_label" in review_cols and "strategy_id" in review_cols:
        conn.execute(
            """
            UPDATE trade_reviews
            SET strategy_id = NULL,
                strategy_label = CASE
                    WHEN LOWER(TRIM(COALESCE(strategy_label, ''))) = 'statement import' THEN ''
                    ELSE strategy_label
                END,
                setup_tag = CASE
                    WHEN LOWER(TRIM(COALESCE(setup_tag, ''))) = 'statement import' THEN ''
                    ELSE setup_tag
                END
            WHERE LOWER(TRIM(COALESCE(strategy_label, setup_tag, ''))) = 'statement import'
               OR LOWER(TRIM(COALESCE(setup_tag, ''))) = 'statement import'
            """
        )
    else:
        conn.execute(
            """
            UPDATE trade_reviews
            SET setup_tag = ''
            WHERE LOWER(TRIM(COALESCE(setup_tag, ''))) = 'statement import'
            """
        )


def _migration_0008_trade_review_workflow(conn: sqlite3.Connection) -> None:
    review_cols = [r["name"] for r in conn.execute("PRAGMA table_info(trade_reviews)").fetchall()]
    if "reviewed_stop_price" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN reviewed_stop_price REAL DEFAULT NULL")
    if "reviewed_target_price" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN reviewed_target_price REAL DEFAULT NULL")
    if "reviewed_risk_dollars" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN reviewed_risk_dollars REAL DEFAULT NULL")
    if "reviewed_risk_percent" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN reviewed_risk_percent REAL DEFAULT NULL")
    if "reviewed_execution_quality" not in review_cols:
        conn.execute(
            "ALTER TABLE trade_reviews ADD COLUMN reviewed_execution_quality TEXT DEFAULT ''"
        )
    if "reviewed_sizing_quality" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN reviewed_sizing_quality TEXT DEFAULT ''")
    if "reviewed_stop_discipline" not in review_cols:
        conn.execute(
            "ALTER TABLE trade_reviews ADD COLUMN reviewed_stop_discipline TEXT DEFAULT ''"
        )
    if "reviewed_within_plan" not in review_cols:
        conn.execute(
            "ALTER TABLE trade_reviews ADD COLUMN reviewed_within_plan INTEGER DEFAULT NULL"
        )
    if "manual_grade_score" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN manual_grade_score INTEGER DEFAULT NULL")
    if "manual_grade_letter" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN manual_grade_letter TEXT DEFAULT ''")
    if "grade_override_reason" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN grade_override_reason TEXT DEFAULT ''")
    if "classification_override" not in review_cols:
        conn.execute("ALTER TABLE trade_reviews ADD COLUMN classification_override TEXT DEFAULT ''")

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_trade_reviews_manual_grade ON trade_reviews(manual_grade_letter)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_trade_reviews_reviewed_execution ON trade_reviews(reviewed_execution_quality)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_trade_reviews_reviewed_stop ON trade_reviews(reviewed_stop_discipline)"
    )


MIGRATIONS: List[Tuple[str, MigrationFn]] = [
    ("0001_baseline", _migration_0001_baseline),
    ("0002_journal_phase2", _migration_0002_journal_phase2),
    ("0003_import_batches", _migration_0003_import_batches),
    ("0004_strategy_links", _migration_0004_strategy_links),
    ("0005_market_alerts", _migration_0005_market_alerts),
    ("0006_trade_review_rich_fields", _migration_0006_trade_review_rich_fields),
    ("0007_trade_source", _migration_0007_trade_source),
    ("0008_trade_review_workflow", _migration_0008_trade_review_workflow),
]


def run_migrations(db_path: str) -> List[str]:
    conn = sqlite3.connect(db_path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 10000")
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
    except sqlite3.DatabaseError:
        pass
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
        cur = conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (id, applied_at) VALUES (?, ?)",
            (mid, datetime.now().isoformat(timespec="seconds")),
        )
        # In multi-worker boot, another process may win the insert race.
        if int(cur.rowcount or 0) > 0:
            new_applied.append(mid)
        applied.add(mid)
    conn.commit()
    conn.close()
    return new_applied
