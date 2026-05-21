"""Focused tests for dashboard balance summary semantics."""

from mccain_capital.runtime import db, now_iso, set_setting_value
from mccain_capital.services import core as core_service


def test_dashboard_balance_summary_uses_configured_start_for_all_history(app):
    set_setting_value("starting_balance", "50000")
    with db() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-26",
                "9:45 AM",
                "10:00 AM",
                "SPX",
                "CALL",
                6930.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                600.0,
                600.0,
                600.0,
                50434.40,
                "seed drift",
                now_iso(),
            ),
        )

    summary = core_service._dashboard_balance_summary(
        scope_active=False,
        scope_account_id=None,
        scope_start="",
        scope_starting_balance=0.0,
        selected_account=None,
    )

    assert summary["balance_integrity"]["starting_balance"] == 50000.0
    assert summary["overall_balance"] == 50600.0
    assert summary["overall_profit"] == 50600.0
    assert summary["trajectory_title"] == "All History Profit"


def test_dashboard_balance_summary_uses_scoped_account_balance_when_active(app):
    summary = core_service._dashboard_balance_summary(
        scope_active=True,
        scope_account_id=42,
        scope_start="2026-02-20",
        scope_starting_balance=52000.0,
        selected_account={"current_balance": 54571.0},
    )

    assert summary["overall_balance"] == 54571.0
    assert summary["overall_profit"] == 2571.0
    assert summary["trajectory_title"] == "Active Account Profit"
