from datetime import date
from pathlib import Path

from mccain_capital.repositories import trades as trades_repo
from mccain_capital.runtime import db, now_iso
from mccain_capital.services.core import _dashboard_performance_hub_viewmodel


ROOT = Path(__file__).resolve().parents[1]


def test_performance_hub_uses_realized_trades_and_explicit_goal_basis():
    model = _dashboard_performance_hub_viewmodel(
        [{"net_pl": 300.0}, {"net_pl": -100.0}, {"net_pl": 200.0}],
        year=2026,
        month=8,
        anchor=date(2026, 8, 12),
        month_name="August 2026",
        mtd_net=400.0,
        milestone={"has_profit_goal": True, "profit_goal": 1000.0},
        selected_pace=100.0,
        scope_label="Active Account",
    )

    assert model["net"] == 400.0
    assert model["goal"] == 1000.0
    assert model["progress_pct"] == 40.0
    assert model["remaining"] == 600.0
    assert model["trade_count"] == 3
    assert round(model["win_rate"], 2) == 66.67
    assert model["profit_factor"] == 5.0
    assert round(model["expectancy"], 2) == 133.33
    assert "recorded-day pace" in model["projection_basis"]


def test_performance_hub_does_not_invent_goal_or_trade_quality():
    model = _dashboard_performance_hub_viewmodel(
        [],
        year=2026,
        month=7,
        anchor=date(2026, 8, 12),
        month_name="July 2026",
        mtd_net=0.0,
        milestone={"has_profit_goal": False, "profit_goal": 0.0},
        selected_pace=0.0,
        scope_label="All History",
    )

    assert model["goal"] is None
    assert model["projection"] is None
    assert model["win_rate"] is None
    assert model["profit_factor"] is None
    assert model["remaining_days"] == 0


def test_performance_hub_does_not_project_from_old_pace_without_current_month_trades():
    model = _dashboard_performance_hub_viewmodel(
        [],
        year=2026,
        month=8,
        anchor=date(2026, 8, 12),
        month_name="August 2026",
        mtd_net=0.0,
        milestone={"has_profit_goal": True, "profit_goal": 7500.0},
        selected_pace=500.0,
        scope_label="Active Account",
    )

    assert model["trade_count"] == 0
    assert model["projection"] is None
    assert model["projection_basis"] == (
        "Projection activates after the first closed trade in this period"
    )


def test_performance_hub_projects_through_custom_cross_month_end_date():
    model = _dashboard_performance_hub_viewmodel(
        [{"net_pl": 1000.0}, {"net_pl": 500.0}],
        year=2026,
        month=8,
        anchor=date(2026, 8, 13),
        month_name="Custom range",
        mtd_net=99999.0,
        milestone={"has_profit_goal": True, "profit_goal": 7500.0},
        selected_pace=500.0,
        scope_label="Active Account",
        range_start=date(2026, 7, 15),
        range_end=date(2026, 9, 4),
    )

    assert model["net"] == 1500.0
    assert model["remaining"] == 6000.0
    assert model["remaining_days"] == 16
    assert model["required_pace"] == 375.0
    assert model["projection"] == 9500.0


def test_dashboard_template_leads_with_performance_and_delegates_execution(client):
    body = client.get("/dashboard", follow_redirects=True).get_data(as_text=True)

    assert 'class="dashboardPerformanceHub"' in body
    assert "Trading Business Dashboard" in body
    assert "Net earned" in body
    assert "Daily pace" in body
    assert "Open Market Pulse" in body
    assert body.index("Trading Business Dashboard") < body.index("dashboardMarketCanvasRead")
    assert 'id="dashboardUpdatesLayer"' in body
    assert "Import trades" in body
    assert "Journal session" in body
    assert "Verify trades" in body
    assert "Review target" in body
    assert 'id="dashboardTargetConsistency"' in body
    assert "Target health" in body
    assert "Current-month projection" in body
    assert "Consistency" in body
    assert "≤30%" in body
    assert 'id="dashboardTargetEditor"' in body
    assert 'action="/dashboard/milestone"' in body
    assert 'action="/dashboard/pace"' in body
    assert 'name="milestone_profit_goal"' in body
    assert 'name="dashboard_pace_daily"' in body


def test_dashboard_compact_header_switches_account_scope(client):
    first_id = trades_repo.create_account(
        account_name="Protect",
        broker_account_id="OEV0035974",
        starting_balance=50000.0,
    )
    demo_id = trades_repo.create_account(
        account_name="Demo · August 10K / 35%",
        broker_account_id="DEMO-AUG-10K-35",
        starting_balance=50000.0,
    )

    body = client.get(
        f"/dashboard?y=2026&m=8&scope=active&account_id={demo_id}",
        follow_redirects=True,
    ).get_data(as_text=True)

    assert 'id="dashboard-account-switcher"' in body
    assert "Switch dashboard account" in body
    assert f"account_id={demo_id}" in body
    assert f"account_id={first_id}" in body
    assert 'class="is-selected" role="menuitem"' in body


def test_dashboard_custom_range_controls_and_account_links_preserve_dates(client):
    account_id = trades_repo.create_account(
        account_name="Range Demo",
        broker_account_id="RANGE-DEMO",
        starting_balance=50000.0,
    )

    body = client.get(
        f"/dashboard?scope=active&account_id={account_id}"
        "&range_start=2026-07-15&range_end=2026-09-04",
        follow_redirects=True,
    ).get_data(as_text=True)

    assert "Set projection window" in body
    assert 'name="range_start" value="2026-07-15"' in body
    assert 'name="range_end" value="2026-09-04"' in body
    assert "range_start=2026-07-15&amp;range_end=2026-09-04" in body


def test_dashboard_demo_account_reports_ten_thousand_and_35_percent_consistency(client):
    demo_id = trades_repo.create_account(
        account_name="Demo · August 10K / 35%",
        broker_account_id="DEMO-AUG-10K-35",
        starting_balance=50000.0,
    )
    with db() as conn:
        demo_results = (
            ("2026-08-03", 3500.0),
            ("2026-08-05", 3000.0),
            ("2026-08-07", 2500.0),
            ("2026-08-12", 2000.0),
            ("2026-08-14", -1000.0),
        )
        running_balance = 50000.0
        for trade_date, net_pl in demo_results:
            running_balance += net_pl
            conn.execute(
                """
                INSERT INTO trades (
                    trade_date, ticker, gross_pl, net_pl, balance, raw_line,
                    created_at, import_batch_id, trade_source, account_id
                ) VALUES (?, 'SPX', ?, ?, ?, 'DEMO AUGUST SCENARIO', ?, ?, 'demo', ?)
                """,
                (
                    trade_date,
                    net_pl,
                    net_pl,
                    running_balance,
                    now_iso(),
                    "DEMO-AUG-10K-35",
                    demo_id,
                ),
            )
        conn.commit()

    body = client.get(
        f"/dashboard?y=2026&m=8&scope=active&account_id={demo_id}",
        follow_redirects=True,
    ).get_data(as_text=True)

    assert "$10,000.00" in body
    assert "Account balance" in body
    assert "$60,000.00" in body
    assert "Drawdown cushion" in body
    assert "$2,500.00" in body
    assert "Calculated equity" not in body
    assert "11.00" in body
    assert "-$1,000.00" in body
    assert "35.0%" in body
    assert "Demo · August 10K / 35%" in body
    assert "Daily realized P&amp;L" in body
    assert "Closed-trade result by day" in body
    assert "Aug 3" in body
    assert "$3,500.00" in body


def test_dashboard_performance_signal_styles_are_explicit():
    css = (ROOT / "static/css/command_surfaces.css").read_text()

    assert '.dashboardPerformanceOutcome strong.is-positive{color:#70ffd2!important' in css
    assert "color:#70ffd2!important;text-shadow:none!important" in css
    assert ".dashboardTargetConsistencyMetric.is-danger" in css
    assert "rgba(255,100,124,.68)" in css


def test_global_navigation_exposes_primary_destinations_and_tools_menu(client):
    body = client.get("/dashboard", follow_redirects=True).get_data(as_text=True)
    nav = body[body.index('<div class="nav">') : body.index('<div id="moreMenu"')]

    for label in (
        "Executive",
        "Trading Dashboard",
        "Market Pulse",
        "Candle Opens",
        "Trades",
        "Journal",
    ):
        assert label in nav
    for label in ("Analytics", "Planner"):
        assert label not in nav
    assert "Tools ▾" in nav
