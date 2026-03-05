"""Core app behavior tests."""

from mccain_capital.runtime import db, now_iso
from mccain_capital.services import core as core_service
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


def test_market_pulse_includes_tesla_in_quotes_and_watchlist():
    labels = {item["label"] for item in core_service.MARKET_PULSE_SYMBOLS}
    assert "TSLA" in labels
    assert "TSLA" in set(core_service.MARKET_PULSE_WATCHLIST_NEWS_SYMBOLS)


def test_market_pulse_core_tape_renders_leader_tickers(client, monkeypatch):
    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Mar 2, 2026 09:45 AM ET",
            "source_label": "Yahoo Finance chart feed",
            "source_note": "",
            "quotes": [
                {
                    "label": "SPX",
                    "group": "core",
                    "price": 5100.0,
                    "change": 10.0,
                    "change_pct": 0.2,
                    "market_state": "Regular",
                    "day_range": "5000.00 to 5150.00",
                },
                {
                    "label": "TSLA",
                    "group": "leaders",
                    "price": 210.0,
                    "change": 2.0,
                    "change_pct": 0.96,
                    "market_state": "Regular",
                    "day_range": "205.00 to 212.00",
                },
            ],
        },
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda: {
            "available": False,
            "source_note": "",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
        },
    )

    resp = client.get("/market-pulse", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Core Tape" in resp.data
    assert b"SPX" in resp.data
    assert b"TSLA" in resp.data


def test_market_pulse_refresh_query_forces_snapshot_refresh(client, monkeypatch):
    force_flags = []

    def _fake_snapshot(*, force_refresh=False):
        force_flags.append(bool(force_refresh))
        return {
            "available": True,
            "fetched_at": "Mar 2, 2026 10:30 AM ET",
            "source_label": "Yahoo Finance chart feed",
            "source_note": "",
            "quotes": [],
        }

    monkeypatch.setattr(core_service, "_market_pulse_snapshot", _fake_snapshot)
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda: {
            "available": False,
            "source_note": "",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
        },
    )

    resp = client.get("/market-pulse?refresh=1", follow_redirects=True)
    assert resp.status_code == 200
    assert force_flags == [True]
    assert b"/market-pulse?refresh=1" in resp.data
    assert b'url.searchParams.set("refresh", "1")' in resp.data


def test_market_pulse_source_is_normalized_to_yahoo():
    out = core_service._market_pulse_force_yahoo_source(
        {
            "available": True,
            "fetched_at": "Mar 2, 2026 10:16 AM ET",
            "source_label": "Finnhub market feed",
            "source_note": "Live quotes and SPX candles are being served by Finnhub.",
            "quotes": [],
        }
    )
    assert out["source_label"] == "Yahoo Finance chart feed"
    assert "finnhub" not in str(out["source_note"]).lower()


def test_market_pulse_cached_payload_is_expanded_to_current_symbol_set():
    old_payload = {
        "available": True,
        "fetched_at": "Mar 2, 2026 10:16 AM ET",
        "source_label": "Finnhub market feed",
        "source_note": "legacy cached snapshot",
        "quotes": [
            {
                "label": "SPX",
                "symbol": "^GSPC",
                "price": 6878.88,
                "group": "core",
                "focus": "",
                "yahoo_href": "",
                "change": 0.0,
                "change_pct": 0.0,
                "volume": 0,
                "avg_volume": 0,
                "market_state": "At Close",
                "day_range": "—",
                "name": "SPX",
            },
            {
                "label": "META",
                "symbol": "META",
                "price": 649.54,
                "group": "leaders",
                "focus": "",
                "yahoo_href": "",
                "change": 0.0,
                "change_pct": 0.0,
                "volume": 0,
                "avg_volume": 0,
                "market_state": "Live",
                "day_range": "—",
                "name": "META",
            },
        ],
    }
    out = core_service._market_pulse_force_symbol_set(old_payload)
    labels = {q["label"] for q in out["quotes"]}
    assert out["source_label"] == "Yahoo Finance chart feed"
    assert "TSLA" in labels
    assert "SPX" in labels
    assert len(out["quotes"]) == len(core_service.MARKET_PULSE_SYMBOLS)


def test_market_pulse_stale_transition_and_alert_escalation():
    now_et = core_service.app_runtime.now_et()
    now_epoch = int(now_et.timestamp())
    base = [
        {
            "label": "SPY",
            "data_state": "live",
            "asof_epoch": now_epoch - 20,
            "mini_series": [1, 2, 3],
        },
        {
            "label": "QQQ",
            "data_state": "live",
            "asof_epoch": now_epoch - 120,
            "mini_series": [3, 2, 1],
        },
        {
            "label": "TSLA",
            "data_state": "cached",
            "asof_epoch": now_epoch - 400,
            "mini_series": [2, 2, 2],
        },
    ]
    enriched = core_service._market_pulse_enrich_quotes(base, now_et)
    by_label = {q["label"]: q for q in enriched}
    assert by_label["SPY"]["freshness_band"] == "live"
    assert by_label["QQQ"]["freshness_band"] == "warn"
    assert by_label["TSLA"]["freshness_band"] == "critical"
    alert = core_service._market_pulse_alert(enriched)
    assert alert["show"] is True
    assert alert["tone"] == "critical"


def test_market_pulse_guardrail_activates_on_threshold():
    quotes = [
        {"label": "SPY", "freshness_band": "critical"},
        {"label": "QQQ", "freshness_band": "critical"},
        {"label": "IWM", "freshness_band": "warn"},
    ]
    guard = core_service._market_pulse_guardrail(quotes)
    assert guard["active"] is True
    assert guard["critical_count"] >= guard["threshold"]


def test_market_pulse_market_hours_defaults_execution_mode(client, monkeypatch):
    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Mar 2, 2026 10:30:00 AM ET",
            "source_label": "Yahoo Finance chart feed",
            "source_note": "",
            "quotes": [],
            "integrity": {},
        },
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda: {
            "available": False,
            "source_note": "",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
        },
    )
    monkeypatch.setattr(core_service, "_market_pulse_market_hours", lambda now_et: True)

    resp = client.get("/market-pulse", follow_redirects=True)
    assert resp.status_code == 200
    assert b'data-market-hours="1"' in resp.data
    assert b'let mode = storedMode || (marketHours ? "execution" : "research");' in resp.data


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
        "stream_market",
        "stream_options_panel",
        "candle_opens_page",
        "trades_page",
        "journal_home",
        "calculator",
        "payouts_page",
        "books_page",
    }
    assert expected.issubset(endpoints)


def test_dashboard_renders_live_market_pulse_panel(client, monkeypatch):
    monkeypatch.setattr(
        core_service,
        "_load_dashboard_milestone_settings",
        lambda: {
            "name": "Profit Milestone",
            "profit_goal": 0.0,
            "target_balance": 0.0,
            "profit_source": "ytd",
        },
    )
    monkeypatch.setattr(
        core_service,
        "_dashboard_milestone_viewmodel",
        lambda *args, **kwargs: {
            "name": "Profit Milestone",
            "profit_source": "ytd",
            "profit_source_label": "YTD",
            "profit_current": 0.0,
            "profit_goal": 0.0,
            "profit_remaining": 0.0,
            "target_balance": 0.0,
            "balance_remaining": 0.0,
            "overall_progress_pct": 0.0,
            "profit_progress_pct": 0.0,
            "balance_progress_pct": 0.0,
            "profit_done": False,
            "balance_done": False,
            "has_profit_goal": False,
            "has_balance_goal": False,
            "avg_daily_profit": 0.0,
            "projected_days_profit": None,
            "projected_days_balance": None,
            "projected_days_overall": None,
        },
    )
    from mccain_capital.services import market_worker
    from mccain_capital.services import options_panel_service

    monkeypatch.setattr(
        market_worker, "get_market_snapshot", lambda: {"prices": {}, "alerts": [], "updated_at": ""}
    )
    monkeypatch.setattr(market_worker, "start_market_worker_once", lambda: None)
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {
            "asof": "",
            "symbols": {"SPX": {"underlying": {}, "gamma": {}, "contracts": []}},
        },
    )
    monkeypatch.setattr(options_panel_service, "start_options_worker_once", lambda: None)

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Live Market Pulse" in resp.data
    assert b"/stream/market" in resp.data
    assert b'EventSource("/stream/market")' in resp.data
    assert b"SPX Gamma" in resp.data
    assert b"/stream/options_panel" not in resp.data


def test_stream_market_sse_emits_json_payload(client, monkeypatch):
    from mccain_capital.services import market_worker
    from mccain_capital.services import options_panel_service

    monkeypatch.setattr(market_worker, "start_market_worker_once", lambda: None)
    monkeypatch.setattr(options_panel_service, "start_options_worker_once", lambda: None)
    monkeypatch.setattr(
        market_worker,
        "get_market_snapshot",
        lambda: {
            "prices": {
                "QQQ": {"price": 456.12, "pct_change": 0.42, "as_of": "2026-03-05T12:00:00"}
            },
            "alerts": ["QQQ crossed above 456.00 at 456.12"],
            "updated_at": "2026-03-05T12:00:00",
        },
    )
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {"asof": "2026-03-05T12:00:00-05:00", "symbols": {"SPX": {}}},
    )
    monkeypatch.setattr(core_service.time, "sleep", lambda _: None)

    resp = client.get("/stream/market", follow_redirects=True)
    assert resp.status_code == 200
    assert resp.headers.get("Content-Type", "").startswith("text/event-stream")
    assert b"data: " in resp.data
    assert b"QQQ" in resp.data
    assert b"options" in resp.data


def test_stream_options_panel_sse_emits_json_payload(client, monkeypatch):
    from mccain_capital.services import options_panel_service

    monkeypatch.setattr(options_panel_service, "start_options_worker_once", lambda: None)
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {
            "asof": "2026-03-05T12:00:00-05:00",
            "symbols": {
                "SPX": {
                    "underlying": {"price": 5120.35, "change_pct": 0.42, "source": "massive"},
                    "contracts": [
                        {
                            "label": "SPXW 2026-03-06 5125C",
                            "mid": 24.10,
                            "delta": 0.47,
                            "vol": 9200,
                            "oi": 18400,
                            "spread": 0.60,
                            "liq": "Tight",
                        }
                    ],
                    "gamma": {
                        "gamma_flip": 5110.0,
                        "call_wall": 5150.0,
                        "put_wall": 5050.0,
                        "net_gamma": "+2.1B",
                    },
                }
            },
        },
    )
    monkeypatch.setattr(core_service.time, "sleep", lambda _: None)

    resp = client.get("/stream/options_panel", follow_redirects=True)
    assert resp.status_code == 200
    assert resp.headers.get("Content-Type", "").startswith("text/event-stream")
    assert b"data: " in resp.data
    assert b"SPXW 2026-03-06 5125C" in resp.data
    assert b"gamma_flip" in resp.data


def test_candle_opens_page_renders_monthly_market_calendar(client):
    resp = client.get("/candle-opens?y=2026&m=2", follow_redirects=True)
    assert resp.status_code == 200
    assert b"February 2026 Candle Opens" in resp.data
    assert b"Presidents Day" in resp.data
    assert b"2D" in resp.data
    assert b"Trading Days" in resp.data
    assert b"Day reset" in resp.data
    assert b"candleWeekdayInline" in resp.data


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
    assert b"Start $50,000.00" in resp.data
    assert b"Ledger drift detected" in resp.data
    assert b"Daily P/L Calendar" in resp.data
    assert b"/ops/alerts" in resp.data


def test_dashboard_renders_calendar_week_cards_and_preview_metadata(client):
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
                "9:50 AM",
                "SPX",
                "CALL",
                6900.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                250.0,
                250.0,
                250.0,
                50250.0,
                "win 1",
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
                "10:05 AM",
                "10:20 AM",
                "SPX",
                "PUT",
                6895.0,
                1.5,
                1.0,
                1,
                150.0,
                1.0,
                -80.0,
                -80.0,
                -53.3,
                50170.0,
                "loss 1",
                now_iso(),
            ),
        )

    resp = client.get("/dashboard?y=2026&m=2", follow_redirects=True)
    assert resp.status_code == 200
    assert b"weekCardTitle" in resp.data
    assert b"2T" in resp.data
    assert b'data-wins="1"' in resp.data
    assert b'data-losses="1"' in resp.data
    assert b"calendarPreview" in resp.data
    assert b'aria-label="Preview 2026-02-24"' in resp.data


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
