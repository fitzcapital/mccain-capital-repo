"""Focused tests for dashboard balance summary semantics."""

from mccain_capital.repositories import trades as trades_repo
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


def test_dashboard_balance_summary_prefers_broker_equity_when_active(app):
    summary = core_service._dashboard_balance_summary(
        scope_active=True,
        scope_account_id=42,
        scope_start="2026-02-20",
        scope_starting_balance=50000.0,
        selected_account={"current_balance": 52309.40, "broker_equity": 52309.40},
    )

    assert summary["overall_balance"] == 52309.40
    assert round(summary["overall_profit"], 2) == 2309.40
    assert summary["balance_source"] == "broker_equity"


def test_dashboard_balance_summary_uses_statement_balance_when_broker_equity_is_stale(app):
    summary = core_service._dashboard_balance_summary(
        scope_active=True,
        scope_account_id=42,
        scope_start="2026-02-20",
        scope_starting_balance=50000.0,
        selected_account={"current_balance": 52544.0, "broker_equity": 52309.40},
    )

    assert summary["overall_balance"] == 52544.0
    assert round(summary["overall_profit"], 2) == 2544.0
    assert summary["balance_source"] == "account_balance"
    assert "stale" in summary["balance_source_detail"].lower()


def test_dashboard_balance_summary_normalizes_funded_account_equity_from_small_ledger_base(app):
    summary = core_service._dashboard_balance_summary(
        scope_active=True,
        scope_account_id=42,
        scope_start="2026-02-20",
        scope_starting_balance=7500.0,
        selected_account={
            "account_size": 75000.0,
            "starting_balance": 7500.0,
            "current_balance": 8959.50,
            "broker_equity": 75000.0,
        },
    )

    assert summary["overall_balance"] == 76459.50
    assert round(summary["overall_profit"], 2) == 1459.50
    assert summary["balance_source"] == "account_balance"
    assert "normalized" in summary["balance_source_detail"].lower()


def test_account_broker_metrics_normalizes_stale_equity_from_small_ledger_base(app):
    metrics = core_service._account_broker_metrics_viewmodel(
        {
            "id": 42,
            "account_size": 75000.0,
            "starting_balance": 7500.0,
            "current_balance": 8959.50,
            "broker_equity": 75000.0,
            "broker_remaining_drawdown": 3750.0,
        }
    )

    assert metrics["broker_equity"] == 76459.50
    assert metrics["broker_equity_source"] == "statement"
    assert metrics["broker_equity_is_stale"] is True


def test_account_broker_metrics_reports_drawdown_off_peak_from_max_drawdown(app):
    metrics = core_service._account_broker_metrics_viewmodel(
        {
            "id": 42,
            "max_drawdown": 3750.0,
            "broker_remaining_drawdown": 3696.50,
        }
    )

    assert metrics["drawdown_peak"] == 3750.0
    assert metrics["drawdown_off_peak"] == 53.50
    assert round(metrics["drawdown_peak_pct"], 2) == 98.57
    assert metrics["drawdown_off_peak_label"] == "$53.50 off peak"


def test_account_broker_metrics_clamps_drawdown_off_peak_at_zero(app):
    metrics = core_service._account_broker_metrics_viewmodel(
        {
            "id": 42,
            "max_drawdown": 3750.0,
            "broker_remaining_drawdown": 3800.0,
        }
    )

    assert metrics["drawdown_off_peak"] == 0.0
    assert metrics["drawdown_peak_pct"] == 100.0
    assert metrics["drawdown_off_peak_label"] == "$0.00 off peak"


def test_account_broker_metrics_hides_drawdown_off_peak_without_peak(app):
    metrics = core_service._account_broker_metrics_viewmodel(
        {
            "id": 42,
            "broker_remaining_drawdown": 2400.0,
        }
    )

    assert metrics["drawdown_peak"] is None
    assert metrics["drawdown_off_peak"] is None
    assert metrics["drawdown_peak_pct"] is None
    assert metrics["drawdown_off_peak_label"] == ""


def test_account_broker_metrics_falls_back_to_broker_max_loss_for_drawdown_peak(app):
    metrics = core_service._account_broker_metrics_viewmodel(
        {
            "id": 42,
            "max_drawdown": 0.0,
            "broker_max_loss": 2500.0,
            "broker_remaining_drawdown": 2100.0,
        }
    )

    assert metrics["drawdown_peak"] == 2500.0
    assert metrics["drawdown_off_peak"] == 400.0
    assert metrics["drawdown_peak_pct"] == 84.0
    assert metrics["drawdown_off_peak_label"] == "$400.00 off peak"


def test_remaining_drawdown_tone_thresholds():
    assert core_service._remaining_drawdown_tone(2251.0) == "positive"
    assert core_service._remaining_drawdown_tone(2250.0) == "warning"
    assert core_service._remaining_drawdown_tone(2000.0) == "warning"
    assert core_service._remaining_drawdown_tone(1500.0) == "warning"
    assert core_service._remaining_drawdown_tone(1499.0) == "negative"


def test_balance_integrity_snapshot_skips_stored_drift_for_active_account_scope(app):
    account_id = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Protect",
        broker_account_id="default:ACC111",
        account_size=50000.0,
        starting_balance=50000.0,
    )

    with db() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at, account_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                int(account_id),
            ),
        )

    snapshot = trades_repo.balance_integrity_snapshot(
        account_id=int(account_id),
        starting_balance=50000.0,
    )

    assert snapshot["canonical_balance"] == 50600.0
    assert snapshot["stored_balance"] is None
    assert snapshot["stored_status_label"] == "Derived only"
    assert snapshot["has_drift"] is False


def test_balance_integrity_snapshot_account_scope_uses_derived_only_even_with_other_accounts(app):
    set_setting_value("starting_balance", "50000")
    account_a = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Protect",
        broker_account_id="default:ACC111",
        account_size=55000.0,
        starting_balance=55000.0,
    )
    account_b = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Growth",
        broker_account_id="default:ACC222",
        account_size=30000.0,
        starting_balance=30000.0,
    )

    with db() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at, account_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-20",
                "9:35 AM",
                "10:00 AM",
                "SPX",
                "CALL",
                6925.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                500.0,
                500.0,
                500.0,
                55500.0,
                "account a baseline",
                now_iso(),
                int(account_a),
            ),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at, account_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-24",
                "9:35 AM",
                "10:00 AM",
                "SPX",
                "CALL",
                6925.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                399.0,
                399.0,
                399.0,
                55899.0,
                "account a trade 1",
                now_iso(),
                int(account_a),
            ),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at, account_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-25",
                "9:40 AM",
                "10:05 AM",
                "QQQ",
                "PUT",
                500.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                249.0,
                249.0,
                249.0,
                30249.0,
                "other account pre-scope row",
                now_iso(),
                int(account_b),
            ),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at, account_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-26",
                "9:40 AM",
                "10:05 AM",
                "SPX",
                "CALL",
                6920.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                3000.0,
                3000.0,
                3000.0,
                58899.0,
                "account a trade 2",
                now_iso(),
                int(account_a),
            ),
        )

    snapshot = trades_repo.balance_integrity_snapshot(
        account_id=int(account_a),
        start_date="2026-02-24",
        starting_balance=55000.0,
    )

    assert snapshot["canonical_balance"] == 58399.0
    assert snapshot["stored_balance"] is None
    assert snapshot["stored_status_label"] == "Derived only"
    assert snapshot["delta"] is None


def test_latest_balance_and_recompute_ignore_acct_snapshot_rows(app):
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
                "2026-02-24",
                "9:35 AM",
                "10:00 AM",
                "SPX",
                "CALL",
                6925.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                399.0,
                399.0,
                399.0,
                50000.0,
                "trade row",
                now_iso(),
            ),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-24",
                "11:00 AM",
                "11:01 AM",
                "ACCT",
                "",
                0.0,
                0.0,
                0.0,
                0,
                0.0,
                0.0,
                10000.0,
                10000.0,
                0.0,
                60399.0,
                "account snapshot row",
                now_iso(),
            ),
        )

    assert trades_repo.latest_balance_overall() == 50399.0

    trades_repo.recompute_balances(starting_balance=50000.0)

    snapshot = trades_repo.balance_integrity_snapshot(starting_balance=50000.0)
    assert snapshot["canonical_balance"] == 50399.0
    assert snapshot["stored_balance"] == 50399.0
    assert snapshot["delta"] == 0.0
