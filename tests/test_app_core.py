"""Core app behavior tests."""

from mccain_capital.runtime import db, now_iso
from werkzeug.security import generate_password_hash


def test_healthz_returns_ok_payload(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["status"] == "ok"
    assert payload["app"] == "mccain-capital"


def test_security_headers_applied(client):
    resp = client.get("/healthz")
    assert resp.headers.get("X-Content-Type-Options") == "nosniff"
    assert resp.headers.get("X-Frame-Options") == "SAMEORIGIN"
    assert resp.headers.get("Referrer-Policy") == "strict-origin-when-cross-origin"
    assert "Content-Security-Policy" in resp.headers


def test_core_pages_are_reachable(client):
    for path in [
        "/",
        "/dashboard",
        "/candle-opens",
        "/trades",
        "/journal",
        "/journal/review/weekly",
        "/calculator",
        "/payouts",
    ]:
        resp = client.get(path, follow_redirects=True)
        assert resp.status_code == 200, f"Expected 200 for {path}, got {resp.status_code}"


def test_calculator_shows_projected_balances_for_stop_and_target(client):
    resp = client.post(
        "/calculator",
        data={
            "entry": "10",
            "contracts": "1",
            "stop_pct": "20",
            "target_pct": "30",
            "fee_per_contract": "0.70",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"Balance If Stop Hits" in resp.data
    assert b"Balance If Target Hits" in resp.data
    assert b"Consistency If Stop Hits" in resp.data
    assert b"Consistency If Target Hits" in resp.data
    assert b"$49,799.30" in resp.data
    assert b"$50,299.30" in resp.data


def test_calculator_supports_async_json_updates(client):
    resp = client.post(
        "/calculator",
        data={
            "entry": "10",
            "contracts": "1",
            "stop_pct": "20",
            "target_pct": "30",
            "fee_per_contract": "0.70",
        },
        headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"},
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    assert payload["err"] is None
    assert "Plan updated" not in payload["results_html"]
    assert "Balance If Stop Hits" in payload["results_html"]
    assert "Consistency If Target Hits" in payload["results_html"]


def test_goals_and_payouts_render_new_planning_sections(client):
    goals_resp = client.get("/goals", follow_redirects=True)
    assert goals_resp.status_code == 200
    assert b"Goal-to-Execution Bridge" in goals_resp.data

    payouts_resp = client.get("/payouts", follow_redirects=True)
    assert payouts_resp.status_code == 200
    assert b"Payout Readiness Planner" in payouts_resp.data


def test_expected_endpoints_registered(app):
    endpoints = {rule.endpoint for rule in app.url_map.iter_rules()}
    expected = {
        "home",
        "healthz",
        "dashboard",
        "dashboard_recompute_balances",
        "candle_opens_page",
        "trades_page",
        "journal_home",
        "calculator",
        "payouts_page",
        "books_page",
    }
    assert expected.issubset(endpoints)


def test_candle_opens_page_renders_monthly_market_calendar(client):
    resp = client.get("/candle-opens?y=2026&m=2", follow_redirects=True)
    assert resp.status_code == 200
    assert b"February 2026 Candle Opens" in resp.data
    assert b"Presidents Day" in resp.data
    assert b"2D" in resp.data
    assert b"Trading Days" in resp.data


def test_trades_page_uses_derived_running_balance(client):
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("starting_balance", "50000"),
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
                50445.10,  # intentionally stale/incorrect row balance
                "seed 1",
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
                "2026-02-25",
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
                50434.40,  # intentionally stale/incorrect row balance
                "seed 2",
                now_iso(),
            ),
        )

    resp = client.get("/trades", follow_redirects=True)
    assert resp.status_code == 200
    # 50,000 + (399 + 3,000) = 53,399
    assert b"$53,399.00" in resp.data


def test_dashboard_shows_balance_basis_and_drift_signal(client):
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("starting_balance", "50000"),
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
                50434.40,  # stale row balance to trigger drift signal
                "seed drift",
                now_iso(),
            ),
        )

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Starting balance + cumulative net P/L" in resp.data
    assert b"Ledger drift detected" in resp.data
    assert b"Advanced Tools" in resp.data
    assert b"/ops/alerts" in resp.data


def test_dashboard_recompute_balances_endpoint_updates_stored_rows(client):
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_username", "owner"),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_password_hash", generate_password_hash("pass123")),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("starting_balance", "50000"),
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
                50000.0,  # stale/incorrect
                "seed 1",
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
                "2026-02-25",
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
                50000.0,  # stale/incorrect
                "seed 2",
                now_iso(),
            ),
        )

    with client.session_transaction() as sess:
        sess["auth_ok"] = True
        sess["auth_user"] = "owner"

    resp = client.post("/dashboard/recompute-balances", follow_redirects=True)
    assert resp.status_code == 200

    with db() as conn:
        rows = conn.execute("SELECT balance FROM trades ORDER BY trade_date ASC, id ASC").fetchall()
    assert len(rows) == 2
    assert float(rows[0]["balance"]) == 50399.0
    assert float(rows[1]["balance"]) == 53399.0


def test_dashboard_recompute_balances_requires_auth(client):
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("starting_balance", "50000"),
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
                "seed",
                now_iso(),
            ),
        )

    resp = client.post("/dashboard/recompute-balances", follow_redirects=True)
    assert resp.status_code == 200

    with db() as conn:
        row = conn.execute("SELECT balance FROM trades LIMIT 1").fetchone()
    assert float(row["balance"]) == 50000.0
