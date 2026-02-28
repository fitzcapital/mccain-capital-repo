"""Analytics repository metric tests."""

from mccain_capital.repositories import analytics as repo
from mccain_capital.runtime import db, now_iso


def _seed_trades():
    rows = [
        ("2026-01-02", "9:35 AM", 100.0, 50100.0, "ORB", "Open", 88, ""),
        ("2026-01-02", "10:20 AM", -50.0, 50050.0, "Fade", "Open", 62, "late-entry"),
        ("2026-01-03", "11:10 AM", 200.0, 50250.0, "ORB", "Midday", 91, ""),
        ("2026-01-03", "2:40 PM", -25.0, 50225.0, "Scalp", "Power Hour", 55, "oversized"),
        ("2026-01-04", "3:05 PM", 0.0, 50225.0, "ORB", "Power Hour", 70, ""),
    ]
    with db() as conn:
        for i, r in enumerate(rows, start=1):
            created = now_iso()
            conn.execute(
                """
                INSERT INTO trades (
                    trade_date, entry_time, exit_time, ticker, opt_type, strike,
                    entry_price, exit_price, contracts, total_spent, comm, gross_pl,
                    net_pl, result_pct, balance, raw_line, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    r[0],
                    r[1],
                    "",
                    "SPX",
                    "CALL",
                    5000.0,
                    1.0,
                    1.0,
                    1,
                    100.0,
                    1.0,
                    r[2] + 1.0,
                    r[2],
                    0.0,
                    r[3],
                    "seed",
                    created,
                ),
            )
            conn.execute(
                """
                INSERT INTO trade_reviews (
                    trade_id, setup_tag, session_tag, checklist_score, rule_break_tags, review_note, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (i, r[4], r[5], r[6], r[7], "seed", created, created),
            )


def test_performance_metrics(app):
    _seed_trades()
    rows = repo.fetch_analytics_rows()
    metrics = repo.performance_metrics(rows)

    assert metrics["total_trades"] == 5
    assert metrics["wins"] == 2
    assert metrics["losses"] == 2
    assert metrics["breakeven"] == 1
    assert metrics["profit_factor"] == 4.0
    assert metrics["expectancy"] == 45.0
    assert metrics["max_drawdown"] == 50.0
    assert metrics["max_win_streak"] == 1
    assert metrics["max_loss_streak"] == 1


def test_group_tables_and_rule_breaks(app):
    _seed_trades()
    rows = repo.fetch_analytics_rows()

    setup = repo.group_table(rows, "setup_tag")
    by_setup = {r["k"]: r for r in setup}
    assert by_setup["ORB"]["count"] == 3
    assert by_setup["ORB"]["expectancy"] == 100.0

    hours = repo.hour_bucket_table(rows)
    assert any(r["k"] == "09:00" for r in hours)

    breaks = repo.rule_break_counts(rows)
    by_tag = {r["tag"]: r["count"] for r in breaks}
    assert by_tag["late-entry"] == 1
    assert by_tag["oversized"] == 1


def test_correlation_drawdown_and_edge_over_time(app):
    _seed_trades()
    rows = repo.fetch_analytics_rows()

    dd = repo.drawdown_diagnostics(rows)
    assert dd["max_drawdown"] >= 0
    assert dd["max_drawdown_streak"] >= 0

    corr = repo.score_pnl_correlation(rows)
    assert corr["n"] >= 3
    assert corr["r"] is not None

    setup_trend = repo.edge_over_time(rows, "setup_tag", top_n=2)
    session_trend = repo.edge_over_time(rows, "session_tag", top_n=2)
    assert len(setup_trend) > 0
    assert len(session_trend) > 0


def test_chart_series_builders(app):
    _seed_trades()
    rows = repo.fetch_analytics_rows()

    equity = repo.equity_curve_series(rows)
    assert len(equity) == 5
    assert equity[0]["v"] == 50100.0
    assert equity[-1]["v"] == 50225.0

    drawdown = repo.drawdown_curve_series(rows)
    assert len(drawdown) == 5
    assert drawdown[0]["v"] == 0.0
    assert drawdown[1]["v"] == 50.0
    assert drawdown[-1]["v"] == 25.0

    expectancy = repo.expectancy_trend_series(rows)
    assert len(expectancy) == 1
    assert expectancy[0]["label"] == "2026-01"
    assert expectancy[0]["count"] == 5
    assert expectancy[0]["v"] == 45.0


def test_spx_benchmark_and_volatility_summary(app):
    rows = [
        {"id": 1, "trade_date": "2026-01-02", "ticker": "SPX", "net_pl": 100.0},
        {"id": 2, "trade_date": "2026-01-02", "ticker": "QQQ", "net_pl": -50.0},
        {"id": 3, "trade_date": "2026-01-03", "ticker": "SPX", "net_pl": 200.0},
        {"id": 4, "trade_date": "2026-01-03", "ticker": "QQQ", "net_pl": -25.0},
    ]
    series = repo.spx_benchmark_series(rows)
    assert len(series) == 4
    assert series[0]["v"] == 50100.0
    assert series[1]["v"] == 50100.0
    assert series[2]["v"] == 50300.0
    assert series[3]["v"] == 50300.0

    vol = repo.volatility_regime_summary(rows)
    assert vol["regime"] in {"LOW", "NORMAL", "HIGH"}
    assert isinstance(vol["current"], float)
    assert len(vol["series"]) == 2

    heat = repo.setup_expectancy_heatmap(rows, top_n_setups=3)
    assert isinstance(heat["setups"], list)
    assert isinstance(heat["rows"], list)
    assert len(heat["rows"]) >= 1


def test_analytics_performance_renders_benchmark_sections(client):
    _seed_trades()
    resp = client.get("/analytics?tab=performance", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Equity vs SPX Benchmark" in resp.data
    assert b"Day Volatility Regime" in resp.data
    assert b"Regime-Aware Sizing" in resp.data
    assert b"Explain This Day" in resp.data


def test_analytics_diagnostics_tab_renders(client):
    _seed_trades()
    resp = client.get("/analytics?tab=diagnostics", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Data Integrity Diagnostics" in resp.data
    assert b"/analytics?tab=diagnostics" in resp.data
    assert b"/analytics?tab=behavior" in resp.data


def test_analytics_behavior_renders_setup_heatmap(client):
    _seed_trades()
    resp = client.get("/analytics?tab=behavior", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Setup Expectancy Heatmap by Time Block" in resp.data


def test_session_replay_page_renders(client):
    _seed_trades()
    resp = client.get("/analytics/replay?date=2026-02-26", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Session Replay" in resp.data
    assert b"Timeline" in resp.data
    assert b"Create Journal Entry" in resp.data


def test_analytics_performance_renders_what_if_simulator(client):
    _seed_trades()
    resp = client.get(
        "/analytics?tab=performance&sim_max_trades=6&sim_win_rate=58&sim_avg_win=250&sim_avg_loss=140&sim_stop_loss_streak=2",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"What-If Day Simulator" in resp.data
    assert b"Guardrail-Adjusted" in resp.data


def test_analytics_expectancy_weekly_toggle_renders(client):
    _seed_trades()
    resp = client.get(
        "/analytics?tab=performance&expectancy_granularity=weekly", follow_redirects=True
    )
    assert resp.status_code == 200
    assert b"Expectancy Trend (WEEKLY)" in resp.data


def test_analytics_data_trust_shows_sync_failure_next_action(client, monkeypatch):
    from mccain_capital.services import analytics as analytics_svc

    monkeypatch.setattr(
        analytics_svc,
        "get_system_status",
        lambda: {
            "last_sync_status": "failed",
            "last_sync_stage": "submit_login",
            "last_sync_updated_human": "Feb 27, 2026 10:30 AM ET",
        },
    )

    resp = client.get("/analytics?tab=performance", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Data Trust" in resp.data
    assert b"Sync reliability is degraded." in resp.data
    assert b"/trades/upload/statement?ws=live" in resp.data


def test_analytics_behavior_empty_state_is_standardized(client):
    resp = client.get(
        "/analytics?tab=behavior&start=2020-01-01&end=2020-01-02", follow_redirects=True
    )
    assert resp.status_code == 200
    assert b"No heatmap data in this range." in resp.data
    assert b"Why this matters: time-block expectancy needs setup tags" in resp.data
    assert b"Next best action: widen date range or log more tagged trades." in resp.data
