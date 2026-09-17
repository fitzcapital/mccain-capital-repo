"""Database migration runner tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from mccain_capital.migrations import run_migrations
from mccain_capital.migrations import _migration_0019_market_pulse_setup_gamma_context


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_run_migrations_is_idempotent(tmp_path: Path):
    db_path = tmp_path / "migrate.db"

    first = run_migrations(str(db_path))
    assert "0001_baseline" in first
    assert "0002_journal_phase2" in first
    assert "0003_import_batches" in first
    assert "0004_strategy_links" in first
    assert "0005_market_alerts" in first

    second = run_migrations(str(db_path))
    assert second == []

    conn = sqlite3.connect(str(db_path))
    try:
        entries_cols = _table_columns(conn, "entries")
        assert "entry_type" in entries_cols
        assert "template_payload" in entries_cols

        links_cols = _table_columns(conn, "entry_trade_links")
        assert {"entry_id", "trade_id", "created_at"}.issubset(links_cols)
        alerts_cols = _table_columns(conn, "alerts")
        assert {"symbol", "rule_type", "threshold", "enabled"}.issubset(alerts_cols)
        fires_cols = _table_columns(conn, "alert_fires")
        assert {"alert_id", "symbol", "price", "message", "fired_at"}.issubset(fires_cols)
        account_cols = _table_columns(conn, "accounts")
        assert {
            "broker_equity",
            "broker_equity_peak",
            "broker_remaining_drawdown",
            "broker_max_loss",
            "broker_metrics_updated_at",
            "broker_equity_source",
        }.issubset(account_cols)
        setup_event_cols = _table_columns(conn, "market_pulse_setup_events")
        assert {
            "setup_event_id",
            "session_date",
            "signal_time",
            "family",
            "pattern_code",
            "outcome_state",
            "mfe",
            "mae",
            "target_progress_percent",
            "gamma_regime",
            "gamma_as_of",
            "gamma_source",
            "gamma_status",
        }.issubset(setup_event_cols)

        applied = [
            r[0] for r in conn.execute("SELECT id FROM schema_migrations ORDER BY id").fetchall()
        ]
        assert applied == [
            "0001_baseline",
            "0002_journal_phase2",
            "0003_import_batches",
            "0004_strategy_links",
            "0005_market_alerts",
            "0006_trade_review_rich_fields",
            "0007_trade_source",
            "0008_trade_review_workflow",
            "0009_self_control_mode",
            "0010_trading_blocked_sites",
            "0011_trading_scope_hardening",
            "0012_full_trading_host_coverage",
            "0013_multi_account_ledgers",
            "0014_account_broker_metrics",
            "0015_broker_equity_source",
            "0016_market_pulse_setup_events",
            "0017_market_pulse_reliability_events",
            "0018_market_pulse_reliability_history_index",
            "0019_market_pulse_setup_gamma_context",
        ]
    finally:
        conn.close()


def test_setup_gamma_migration_is_additive_for_existing_table(tmp_path: Path):
    path = tmp_path / "existing.db"
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            "CREATE TABLE market_pulse_setup_events "
            "(setup_event_id TEXT PRIMARY KEY, evidence_json TEXT NOT NULL DEFAULT '{}')"
        )
        conn.execute(
            "INSERT INTO market_pulse_setup_events (setup_event_id, evidence_json) VALUES (?, ?)",
            ("legacy", '{"kept":true}'),
        )
        _migration_0019_market_pulse_setup_gamma_context(conn)
        _migration_0019_market_pulse_setup_gamma_context(conn)
        row = conn.execute("SELECT * FROM market_pulse_setup_events").fetchone()
        indexes = {
            item["name"]
            for item in conn.execute("PRAGMA index_list(market_pulse_setup_events)").fetchall()
        }
    finally:
        conn.close()
    assert row["evidence_json"] == '{"kept":true}'
    assert row["gamma_regime"] == "unavailable"
    assert row["gamma_status"] == "unavailable"
    assert "idx_mp_setup_events_gamma_regime" in indexes
