"""Tests for payout unlock forecast helpers and /payouts rendering."""

from mccain_capital.runtime import db
from mccain_capital.services import goals as goals_svc


def test_required_profit_to_target_math():
    assert goals_svc._required_profit_to_target(2500.0, 2000.0) == 0.0
    assert goals_svc._required_profit_to_target(1500.0, 2000.0) == 500.0


def test_trading_day_quantiles_probabilities_and_ordering():
    out = goals_svc._trading_day_quantiles_to_goal(
        required_profit=1500.0,
        mu=140.0,
        sigma=60.0,
        runs=500,
        horizon=60,
        balance=56000.0,
        safe_floor=53000.0,
    )
    assert out["days_p50"] is not None
    assert out["days_p70"] is not None
    assert out["days_p90"] is not None
    assert int(out["days_p50"]) <= int(out["days_p70"]) <= int(out["days_p90"])
    for key in (
        "hit_prob_5d",
        "hit_prob_10d",
        "hit_prob_20d",
        "floor_breach_prob_at_target_horizon",
    ):
        assert 0.0 <= float(out[key]) <= 100.0


def test_build_unlock_forecast_sparse_data_fallback():
    out = goals_svc._build_unlock_forecast(
        safe_request=250.0,
        max_request=800.0,
        biweekly_goal=2000.0,
        overall_balance=55000.0,
        safe_floor=52500.0,
        daily20=[110.0, 120.0, 95.0, 130.0, 80.0],
        daily60=[120.0, 90.0, 100.0, 110.0, 85.0],
        risk_threshold=30.0,
    )
    assert out["method"] == "deterministic_low_confidence"
    assert out["warnings"]
    assert out["safe"]["days_p50"] is not None
    assert out["max"]["days_p50"] is not None


def test_build_unlock_forecast_negative_drift_marks_not_favorable():
    out = goals_svc._build_unlock_forecast(
        safe_request=0.0,
        max_request=0.0,
        biweekly_goal=2000.0,
        overall_balance=54000.0,
        safe_floor=52500.0,
        daily20=[-120.0] * 20,
        daily60=[-200.0, 40.0] * 30,
        risk_threshold=30.0,
    )
    assert "not statistically favorable" in str(out["safe"]["eta_note"]).lower()
    assert out["safe"]["risk_flag"] == "RISK"
    assert out["max"]["risk_flag"] == "RISK"


def test_risk_gate_changes_between_pass_and_risk():
    low_risk = goals_svc._build_unlock_forecast(
        safe_request=0.0,
        max_request=0.0,
        biweekly_goal=400.0,
        overall_balance=56000.0,
        safe_floor=52500.0,
        daily20=[200.0] * 20,
        daily60=[150.0] * 60,
        risk_threshold=30.0,
    )
    high_risk = goals_svc._build_unlock_forecast(
        safe_request=0.0,
        max_request=0.0,
        biweekly_goal=400.0,
        overall_balance=53100.0,
        safe_floor=53080.0,
        daily20=[120.0] * 20,
        daily60=[-900.0, 900.0] * 30,
        risk_threshold=30.0,
    )
    assert low_risk["safe"]["risk_flag"] == "PASS"
    assert high_risk["safe"]["risk_flag"] == "RISK"


def test_payouts_page_renders_unlock_forecast_and_scope_toggle(client):
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("active_account_start_date", "2026-03-01"),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("active_account_start_balance", "55000"),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("active_account_label", "Funded Account"),
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
                "2026-03-02",
                "09:30",
                "10:00",
                "SPX",
                "CALL",
                5100.0,
                2.0,
                2.4,
                5,
                1000.0,
                3.5,
                200.0,
                196.5,
                19.65,
                55196.5,
                "manual",
                "2026-03-02T10:00:00-05:00",
            ),
        )

    resp = client.get("/payouts?scope=active", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Payout Unlock Forecast" in resp.data
    assert b"Safe Withdrawal Path" in resp.data
    assert b"Max Withdrawal Path" in resp.data
    assert b"Active Account" in resp.data
    assert b"All History" in resp.data
