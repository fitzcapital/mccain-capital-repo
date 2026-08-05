"""Lightweight SQLite migration runner."""

from __future__ import annotations

import json
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


def _migration_0009_self_control_mode(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS self_control_blocked_sites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT NOT NULL UNIQUE,
            category TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            source TEXT NOT NULL DEFAULT 'seeded',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_self_control_blocked_sites_category
            ON self_control_blocked_sites(category, enabled);

        CREATE TABLE IF NOT EXISTS self_control_presets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            duration_minutes INTEGER NOT NULL,
            strict_mode INTEGER NOT NULL DEFAULT 0,
            intent_prompt TEXT NOT NULL DEFAULT '',
            blocked_categories_json TEXT NOT NULL DEFAULT '[]',
            blocked_domains_json TEXT NOT NULL DEFAULT '[]',
            auto_trigger_placeholder TEXT NOT NULL DEFAULT '',
            enabled INTEGER NOT NULL DEFAULT 1,
            seeded INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS self_control_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            enabled INTEGER NOT NULL DEFAULT 1,
            manual_only INTEGER NOT NULL DEFAULT 1,
            trigger_type TEXT NOT NULL,
            trigger_config_json TEXT NOT NULL DEFAULT '{}',
            action_type TEXT NOT NULL,
            action_config_json TEXT NOT NULL DEFAULT '{}',
            require_journal_before_unlock INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS self_control_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            preset_slug TEXT NOT NULL DEFAULT '',
            label TEXT NOT NULL,
            intent_note TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL,
            strict_mode INTEGER NOT NULL DEFAULT 0,
            started_at TEXT NOT NULL,
            planned_end_at TEXT NOT NULL,
            ended_at TEXT NOT NULL DEFAULT '',
            planned_minutes INTEGER NOT NULL,
            completed_minutes INTEGER NOT NULL DEFAULT 0,
            blocked_categories_json TEXT NOT NULL DEFAULT '[]',
            blocked_domains_json TEXT NOT NULL DEFAULT '[]',
            source_rule_slug TEXT NOT NULL DEFAULT '',
            unlock_requirement TEXT NOT NULL DEFAULT '',
            unlock_satisfied_at TEXT NOT NULL DEFAULT '',
            cancel_reason TEXT NOT NULL DEFAULT '',
            override_reason TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_self_control_sessions_status
            ON self_control_sessions(status, started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_self_control_sessions_started
            ON self_control_sessions(started_at DESC);

        CREATE TABLE IF NOT EXISTS self_control_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            event_type TEXT NOT NULL,
            event_at TEXT NOT NULL,
            detail_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_self_control_events_session
            ON self_control_events(session_id, event_at DESC);

        CREATE TABLE IF NOT EXISTS self_control_enforcement_providers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider_type TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            status TEXT NOT NULL,
            last_checked_at TEXT NOT NULL DEFAULT '',
            last_error TEXT NOT NULL DEFAULT '',
            config_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    now = datetime.now().isoformat(timespec="seconds")
    trading_blocked_sites = [
        ("trade.vanquishtrader.com", "Trading"),
        ("app.vanquishtrader.com", "Trading"),
        ("api.vanquishtrader.com", "Trading"),
        ("www.vanquishtrader.com", "Trading"),
        ("vanquishtrader.com", "Trading"),
        ("tradingview.com", "Trading"),
        ("www.tradingview.com", "Trading"),
    ]
    blocked_sites = [
        ("x.com", "Social"),
        ("twitter.com", "Social"),
        ("instagram.com", "Social"),
        ("facebook.com", "Social"),
        ("discord.com", "Social"),
        ("youtube.com", "Entertainment"),
        ("reddit.com", "News / Doomscroll"),
        *trading_blocked_sites,
    ]
    for domain, category in blocked_sites:
        conn.execute(
            """
            INSERT OR IGNORE INTO self_control_blocked_sites (
              domain, category, enabled, source, created_at, updated_at
            )
            VALUES (?, ?, 1, 'seeded', ?, ?)
            """,
            (domain, category, now, now),
        )

    presets = [
        (
            "market-open-lock",
            "Market Open Lock",
            "Cut off social drift while the opening rotation is still noisy.",
            60,
            1,
            "Protect the open and stop reactive scrolling.",
            '["Social","News / Doomscroll","Trading"]',
            "[]",
            "Future auto-trigger when session enters cash open.",
        ),
        (
            "midday-reset",
            "Midday Reset",
            "Short recovery block after the first wave of execution.",
            30,
            0,
            "Reset attention before the next trade cluster.",
            '["Social","Entertainment","Trading"]',
            "[]",
            "Future auto-trigger after trade-count threshold.",
        ),
        (
            "power-hour-focus",
            "Power Hour Focus",
            "Lock back in for the close and avoid re-entry noise.",
            45,
            1,
            "Keep the close clean and deliberate.",
            '["Social","Random distractions","Trading"]',
            "[]",
            "Future auto-trigger one hour before close.",
        ),
        (
            "journal-mode",
            "Journal Mode",
            "Block scroll loops while writing the debrief.",
            30,
            0,
            "Finish the journal before reopening the noise.",
            '["Social","Entertainment","Random distractions","Trading"]',
            "[]",
            "Future trigger after a completed trading session.",
        ),
        (
            "no-scroll-until-close",
            "No Scroll Until Close",
            "Carry a hard no-scroll posture through the session.",
            120,
            1,
            "Stay in execution mode until the close is done.",
            '["Social","News / Doomscroll","Entertainment","Trading"]',
            "[]",
            "Future all-day discipline mode.",
        ),
        (
            "post-loss-cooldown",
            "Post-Loss Cooldown",
            "Create friction after a loss or rule break.",
            45,
            1,
            "No reactive re-entry after a stop-out.",
            '["Social","Entertainment","News / Doomscroll","Random distractions","Trading"]',
            "[]",
            "Future auto-trigger from live trade loss hooks.",
        ),
    ]
    for preset in presets:
        conn.execute(
            """
            INSERT OR IGNORE INTO self_control_presets (
              slug, name, description, duration_minutes, strict_mode, intent_prompt,
              blocked_categories_json, blocked_domains_json, auto_trigger_placeholder,
              enabled, seeded, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?)
            """,
            (*preset, now, now),
        )

    rules = [
        (
            "post-loss-cooldown",
            "Trigger Post-Loss Cooldown",
            "Launch cooldown discipline after a losing trade.",
            1,
            1,
            "losing_trade",
            "{}",
            "start_preset",
            '{"preset_slug":"post-loss-cooldown"}',
            0,
        ),
        (
            "daily-max-loss-lock",
            "Trigger Lock After Daily Max Loss",
            "Lock the operator after daily max loss is reached.",
            1,
            1,
            "daily_max_loss",
            '{"threshold_source":"risk_controls.daily_max_loss"}',
            "start_duration",
            '{"duration_minutes":90,"strict_mode":true,"label":"Daily Max Loss Lock"}',
            0,
        ),
        (
            "require-debrief-before-reenable",
            "Require Debrief Before Re-Enable",
            "Cooldown completes only after a trading debrief is logged.",
            1,
            1,
            "cooldown_completion",
            "{}",
            "start_duration",
            '{"duration_minutes":15,"strict_mode":true,"label":"Debrief Gate","unlock_requirement":"trade_debrief_today_after_session_start"}',
            1,
        ),
        (
            "midday-reset-after-trade-count",
            "Trigger Midday Reset After X Trades",
            "Kick off a reset after a configurable number of trades.",
            1,
            1,
            "trade_count",
            '{"trade_count":5}',
            "start_preset",
            '{"preset_slug":"midday-reset"}',
            0,
        ),
        (
            "prevent-immediate-reentry",
            "Prevent Immediate Re-Entry After Stop-Out",
            "Force a short delay after a stop-out before re-entry.",
            1,
            1,
            "stop_out",
            '{"cooldown_minutes":15}',
            "start_duration",
            '{"duration_minutes":15,"strict_mode":true,"label":"Immediate Re-Entry Lock"}',
            0,
        ),
    ]
    for rule in rules:
        conn.execute(
            """
            INSERT OR IGNORE INTO self_control_rules (
              slug, name, description, enabled, manual_only, trigger_type,
              trigger_config_json, action_type, action_config_json,
              require_journal_before_unlock, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (*rule, now, now),
        )

    providers = [
        ("browser_extension", "Browser Extension", "not_connected"),
        ("local_helper", "Local Helper", "not_connected"),
        ("os_blocker", "OS Blocker", "not_connected"),
    ]
    for provider_type, display_name, status in providers:
        conn.execute(
            """
            INSERT OR IGNORE INTO self_control_enforcement_providers (
              provider_type, display_name, status, config_json, created_at, updated_at
            )
            VALUES (?, ?, ?, '{}', ?, ?)
            """,
            (provider_type, display_name, status, now, now),
        )


def _migration_0010_trading_blocked_sites(conn: sqlite3.Connection) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    blocked_sites = [
        ("trade.vanquishtrader.com", "Trading"),
        ("app.vanquishtrader.com", "Trading"),
        ("api.vanquishtrader.com", "Trading"),
        ("www.vanquishtrader.com", "Trading"),
        ("vanquishtrader.com", "Trading"),
        ("tradingview.com", "Trading"),
        ("www.tradingview.com", "Trading"),
    ]
    for domain, category in blocked_sites:
        conn.execute(
            """
            INSERT INTO self_control_blocked_sites (
              domain, category, enabled, source, created_at, updated_at
            )
            VALUES (?, ?, 1, 'seeded', ?, ?)
            ON CONFLICT(domain) DO UPDATE SET
              category = excluded.category,
              enabled = 1,
              source = CASE
                WHEN self_control_blocked_sites.source = 'seeded' THEN excluded.source
                ELSE self_control_blocked_sites.source
              END,
              updated_at = excluded.updated_at
            """,
            (domain, category, now, now),
        )


def _append_trading_to_seeded_presets(conn: sqlite3.Connection) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    rows = conn.execute(
        """
        SELECT id, blocked_categories_json
        FROM self_control_presets
        WHERE seeded = 1
        """
    ).fetchall()
    for row in rows:
        try:
            categories = json.loads(row["blocked_categories_json"] or "[]")
        except (TypeError, json.JSONDecodeError):
            categories = []
        if not isinstance(categories, list):
            categories = []
        normalized = [str(item or "").strip() for item in categories if str(item or "").strip()]
        if "Trading" in normalized:
            continue
        normalized.append("Trading")
        conn.execute(
            """
            UPDATE self_control_presets
            SET blocked_categories_json = ?, updated_at = ?
            WHERE id = ?
            """,
            (json.dumps(normalized, separators=(",", ":")), now, int(row["id"])),
        )


def _append_trading_to_active_sessions(conn: sqlite3.Connection) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    rows = conn.execute(
        """
        SELECT id, blocked_categories_json
        FROM self_control_sessions
        WHERE status IN ('active', 'awaiting_journal_unlock')
        """
    ).fetchall()
    for row in rows:
        try:
            categories = json.loads(row["blocked_categories_json"] or "[]")
        except (TypeError, json.JSONDecodeError):
            categories = []
        if not isinstance(categories, list):
            categories = []
        normalized = [str(item or "").strip() for item in categories if str(item or "").strip()]
        if "Trading" in normalized:
            continue
        normalized.append("Trading")
        conn.execute(
            """
            UPDATE self_control_sessions
            SET blocked_categories_json = ?, updated_at = ?
            WHERE id = ?
            """,
            (json.dumps(normalized, separators=(",", ":")), now, int(row["id"])),
        )


def _migration_0011_trading_scope_hardening(conn: sqlite3.Connection) -> None:
    _migration_0010_trading_blocked_sites(conn)
    _append_trading_to_seeded_presets(conn)
    _append_trading_to_active_sessions(conn)


def _migration_0012_full_trading_host_coverage(conn: sqlite3.Connection) -> None:
    _migration_0010_trading_blocked_sites(conn)


def _migration_0013_multi_account_ledgers(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prop_firm TEXT NOT NULL DEFAULT '',
            account_name TEXT NOT NULL DEFAULT '',
            broker_account_id TEXT NOT NULL DEFAULT '',
            account_size REAL NOT NULL DEFAULT 0,
            starting_balance REAL NOT NULL DEFAULT 0,
            current_balance REAL NOT NULL DEFAULT 0,
            max_drawdown REAL NOT NULL DEFAULT 0,
            archived INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            filename TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT '',
            import_batch_id TEXT NOT NULL DEFAULT '',
            uploaded_at TEXT NOT NULL
        );
        """
    )
    account_cols = {
        str(r["name"]): r for r in conn.execute("PRAGMA table_info(accounts)").fetchall()
    }
    if "archived" not in account_cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN archived INTEGER NOT NULL DEFAULT 0")
    if "current_balance" not in account_cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN current_balance REAL NOT NULL DEFAULT 0")
    if "max_drawdown" not in account_cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN max_drawdown REAL NOT NULL DEFAULT 0")
    if "created_at" not in account_cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN created_at TEXT NOT NULL DEFAULT ''")
    if "updated_at" not in account_cols:
        conn.execute("ALTER TABLE accounts ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''")
    conn.execute(
        "UPDATE accounts SET created_at = COALESCE(NULLIF(created_at, ''), ?), "
        "updated_at = COALESCE(NULLIF(updated_at, ''), ?)",
        (
            datetime.now().isoformat(timespec="seconds"),
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_accounts_archived_created "
        "ON accounts(archived, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_accounts_broker_size "
        "ON accounts(broker_account_id, starting_balance)"
    )
    upload_cols = {str(r["name"]): r for r in conn.execute("PRAGMA table_info(uploads)").fetchall()}
    if "import_batch_id" not in upload_cols:
        conn.execute("ALTER TABLE uploads ADD COLUMN import_batch_id TEXT NOT NULL DEFAULT ''")
    if "source" not in upload_cols:
        conn.execute("ALTER TABLE uploads ADD COLUMN source TEXT NOT NULL DEFAULT ''")
    if "filename" not in upload_cols:
        conn.execute("ALTER TABLE uploads ADD COLUMN filename TEXT NOT NULL DEFAULT ''")
    if "uploaded_at" not in upload_cols:
        conn.execute("ALTER TABLE uploads ADD COLUMN uploaded_at TEXT NOT NULL DEFAULT ''")
    conn.execute(
        "UPDATE uploads SET uploaded_at = COALESCE(NULLIF(uploaded_at, ''), ?)",
        (datetime.now().isoformat(timespec="seconds"),),
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_uploads_account_uploaded "
        "ON uploads(account_id, uploaded_at DESC)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_uploads_batch " "ON uploads(import_batch_id)")

    trade_cols = [r["name"] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]
    if "account_id" not in trade_cols:
        conn.execute("ALTER TABLE trades ADD COLUMN account_id INTEGER")
    if "upload_id" not in trade_cols:
        conn.execute("ALTER TABLE trades ADD COLUMN upload_id INTEGER")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_account_id ON trades(account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_upload_id ON trades(upload_id)")

    unowned_count = int(
        (
            conn.execute(
                "SELECT COUNT(*) AS c FROM trades WHERE account_id IS NULL OR upload_id IS NULL"
            ).fetchone()
            or {"c": 0}
        )["c"]
        or 0
    )
    if unowned_count <= 0:
        return

    now = datetime.now().isoformat(timespec="seconds")
    settings_rows = conn.execute("SELECT key, value FROM settings").fetchall()
    settings_map = {str(r["key"]): str(r["value"] or "") for r in settings_rows}
    account_name = (
        settings_map.get("active_account_name")
        or settings_map.get("active_account_label")
        or "Imported Account"
    ).strip() or "Imported Account"
    broker_account_id = (settings_map.get("active_account_id") or "").strip()
    prop_firm = (settings_map.get("active_account_type") or "").strip()
    try:
        starting_balance = float(
            settings_map.get("active_account_start_balance")
            or settings_map.get("starting_balance")
            or 0.0
        )
    except Exception:
        starting_balance = 0.0
    account_size = starting_balance
    existing = conn.execute(
        """
        SELECT id
        FROM accounts
        WHERE archived = 0
          AND COALESCE(account_name, '') = ?
          AND COALESCE(broker_account_id, '') = ?
          AND ABS(COALESCE(starting_balance, 0) - ?) < 0.0001
        ORDER BY id ASC
        LIMIT 1
        """,
        (account_name, broker_account_id, float(starting_balance)),
    ).fetchone()
    if existing:
        account_id = int(existing["id"])
    else:
        cur = conn.execute(
            """
            INSERT INTO accounts (
                prop_firm, account_name, broker_account_id, account_size,
                starting_balance, current_balance, max_drawdown, archived,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            """,
            (
                prop_firm,
                account_name,
                broker_account_id,
                float(account_size),
                float(starting_balance),
                float(starting_balance),
                0.0,
                now,
                now,
            ),
        )
        account_id = int(cur.lastrowid)

    cur = conn.execute(
        """
        INSERT INTO uploads (account_id, filename, source, import_batch_id, uploaded_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            account_id,
            "legacy-migrated-history",
            "migration",
            "legacy-migration",
            now,
        ),
    )
    upload_id = int(cur.lastrowid)
    conn.execute(
        """
        UPDATE trades
        SET account_id = COALESCE(account_id, ?),
            upload_id = COALESCE(upload_id, ?)
        WHERE account_id IS NULL OR upload_id IS NULL
        """,
        (account_id, upload_id),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO settings (key, value)
        VALUES ('active_account_record_id', ?)
        """,
        (str(account_id),),
    )


def _migration_0014_account_broker_metrics(conn: sqlite3.Connection) -> None:
    _migration_0013_multi_account_ledgers(conn)
    account_cols = {str(r["name"]) for r in conn.execute("PRAGMA table_info(accounts)").fetchall()}
    for col_name in (
        "broker_equity",
        "broker_equity_peak",
        "broker_remaining_drawdown",
        "broker_max_loss",
    ):
        if col_name not in account_cols:
            conn.execute(f"ALTER TABLE accounts ADD COLUMN {col_name} REAL")
    if "broker_metrics_updated_at" not in account_cols:
        conn.execute(
            "ALTER TABLE accounts ADD COLUMN broker_metrics_updated_at TEXT NOT NULL DEFAULT ''"
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
    ("0009_self_control_mode", _migration_0009_self_control_mode),
    ("0010_trading_blocked_sites", _migration_0010_trading_blocked_sites),
    ("0011_trading_scope_hardening", _migration_0011_trading_scope_hardening),
    ("0012_full_trading_host_coverage", _migration_0012_full_trading_host_coverage),
    ("0013_multi_account_ledgers", _migration_0013_multi_account_ledgers),
    ("0014_account_broker_metrics", _migration_0014_account_broker_metrics),
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
