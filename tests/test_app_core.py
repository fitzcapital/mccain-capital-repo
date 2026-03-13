"""Core app behavior tests."""

from mccain_capital.runtime import db, get_setting_value, now_iso, today_iso
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
        "/ops/system-check",
    ]:
        resp = client.get(path, follow_redirects=True)
        assert resp.status_code == 200, f"Expected 200 for {path}, got {resp.status_code}"


def test_base_shell_includes_market_pulse_transition_overlay(client):
    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"marketPulseLoadingOverlay" in resp.data
    assert b"showMarketPulseLoading" in resp.data


def test_dashboard_renders_daily_brief_card(client):
    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Daily Brief" in resp.data
    assert b"Plan A" in resp.data
    assert b"No-trade condition" in resp.data


def test_dashboard_brief_update_saves_daily_plan(client):
    resp = client.post(
        "/dashboard/brief",
        data={
            "brief_day": "2026-03-13",
            "brief_focus": "Protect A setups only.",
            "brief_plan_a": "Take continuation longs above flip.",
            "brief_plan_b": "Fade extremes only after rejection.",
            "brief_no_trade": "Stand down into CPI.",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert get_setting_value("dashboard_daily_brief::2026-03-13", "")


def test_dashboard_links_to_auto_debrief_draft_when_trades_exist(client):
    with db() as conn:
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
                today_iso(),
                "9:35 AM",
                "9:48 AM",
                "SPX",
                "CALL",
                5000.0,
                1.0,
                1.3,
                1,
                100.0,
                1.0,
                30.0,
                29.0,
                29.0,
                50029.0,
                "seed",
                created,
            ),
        )

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Auto Debrief Draft" in body
    assert "auto_draft=1" in body


def test_vanquish_blocklist_download_endpoint(client):
    resp = client.get("/ops/vanquish-blocklist")
    assert resp.status_code == 200
    assert "text/plain" in str(resp.content_type)
    body = resp.get_data(as_text=True)
    assert "trade.vanquishtrader.com" in body
    assert "www.vanquishtrader.com" in body


def test_vanquish_manual_lock_control_roundtrip(client):
    start = client.post(
        "/ops/vanquish-lock",
        data={"action": "start", "duration_minutes": "1", "next": "/dashboard"},
        follow_redirects=True,
    )
    assert start.status_code == 200
    assert b"Milestone" in start.data

    clear = client.post(
        "/ops/vanquish-lock",
        data={"action": "clear", "next": "/dashboard"},
        follow_redirects=True,
    )
    assert clear.status_code == 200
    assert b"Milestone" in clear.data


def test_vanquish_lock_state_endpoint(client):
    resp = client.get("/ops/vanquish-lock-state")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert isinstance(payload, dict)
    assert "active" in payload
    assert "goal" in payload
    assert "day_net" in payload
    assert "unlock_label" in payload


def test_dashboard_trading_window_banner_uses_test_mode_stop_state(client):
    from mccain_capital.runtime import set_setting_value

    set_setting_value("trading_window_enabled", "1")
    set_setting_value("trading_window_start_et", "09:30")
    set_setting_value("trading_window_done_by_et", "11:30")
    set_setting_value("trading_window_hard_stop_et", "12:00")
    set_setting_value("trading_window_test_mode", "1")
    set_setting_value("trading_window_test_date", "2026-03-12")
    set_setting_value("trading_window_test_time_et", "12:45")

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Trading Window" in resp.data
    assert b"Test Mode" in resp.data
    assert b"STOP TRADING" in resp.data
    assert b"Done By 11:30 ET" in resp.data


def test_trading_window_config_endpoint_saves_times(client):
    resp = client.post(
        "/ops/trading-window",
        data={
            "tw_enabled": "1",
            "tw_start_et": "09:35",
            "tw_done_by_et": "11:20",
            "tw_hard_stop_et": "11:45",
            "tw_test_mode": "1",
            "tw_test_date": "2026-03-12",
            "tw_test_time_et": "10:15",
            "next": "/dashboard",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert resp.headers.get("Location", "").endswith("/dashboard")

    assert str(get_setting_value("trading_window_enabled", "")) == "1"
    assert str(get_setting_value("trading_window_start_et", "")) == "09:35"
    assert str(get_setting_value("trading_window_done_by_et", "")) == "11:20"
    assert str(get_setting_value("trading_window_hard_stop_et", "")) == "11:45"
    assert str(get_setting_value("trading_window_test_mode", "")) == "1"
    assert str(get_setting_value("trading_window_test_date", "")) == "2026-03-12"
    assert str(get_setting_value("trading_window_test_time_et", "")) == "10:15"


def test_candle_opens_news_includes_placeholder_weeks(monkeypatch):
    monkeypatch.setattr(
        core_service,
        "get_forex_factory_month_feed",
        lambda: [
            {
                "country": "USD",
                "impact": "High",
                "title": "NFP",
                "date": "2026-03-06T08:30:00-05:00",
            }
        ],
    )
    monkeypatch.setattr(core_service, "get_forex_factory_feed", lambda: [])
    monkeypatch.setattr(core_service, "get_forex_factory_next_week_feed", lambda: [])
    out = core_service._forex_factory_usd_window_events(
        core_service.date(2026, 3, 1), core_service.date(2026, 3, 31)
    )
    assert bool(out.get("available"))
    assert "2026-03-11" in set((out.get("events_by_day") or {}).keys())


def test_candle_opens_includes_known_march_fallback_markers(monkeypatch):
    monkeypatch.setattr(core_service, "get_forex_factory_month_feed", lambda: [])
    monkeypatch.setattr(core_service, "get_forex_factory_feed", lambda: [])
    monkeypatch.setattr(core_service, "get_forex_factory_next_week_feed", lambda: [])
    out = core_service._forex_factory_usd_window_events(
        core_service.date(2026, 3, 1), core_service.date(2026, 3, 31)
    )
    assert bool(out.get("fallback_used"))
    assert int(out.get("fallback_count") or 0) >= 11
    assert "fallback marker" in str(out.get("summary") or "").lower()
    event_days = set((out.get("events_by_day") or {}).keys())
    for expected in (
        "2026-03-11",
        "2026-03-12",
        "2026-03-13",
        "2026-03-16",
        "2026-03-17",
        "2026-03-18",
        "2026-03-19",
        "2026-03-24",
        "2026-03-26",
        "2026-03-27",
        "2026-03-31",
    ):
        assert expected in event_days


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
    assert b"marketPulseStreamStatus" in resp.data
    assert b"marketPulseLoadingOverlay" in resp.data
    assert b"Loading Market Pulse" in resp.data
    assert b"autoRefreshToggle" not in resp.data


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
    assert b'url.searchParams.delete("refresh")' in resp.data


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
    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Trading Command Deck" in resp.data
    assert b"Milestone" in resp.data
    assert b"Live Market Pulse" not in resp.data


def test_dashboard_live_tape_compact_labels_and_guardrails(client, monkeypatch):
    from mccain_capital.services import market_worker
    from mccain_capital.services import market_data_service

    monkeypatch.setattr(market_worker, "start_market_worker_once", lambda: None)
    monkeypatch.setattr(
        market_worker,
        "get_market_snapshot",
        lambda: {
            "prices": {
                "SPX": {
                    "price": 6775.80,
                    "pct_change": -0.09,
                    "provider": "tradier",
                    "reason": "tradier_live_quote",
                    "as_of": "2026-03-05T12:00:30-05:00",
                },
                "VIX": {
                    "price": 24.23,
                    "pct_change": -2.81,
                    "provider": "tradier",
                    "reason": "tradier_live_quote",
                    "as_of": "2026-03-05T12:00:30-05:00",
                },
            },
            "series_points": {},
            "series": {},
            "updated_at": "2026-03-05T12:00:30-05:00",
        },
    )
    monkeypatch.setattr(
        market_data_service,
        "get_watchlist_tradier",
        lambda symbols: {
            "SPX": {
                "price": 6775.80,
                "pct_change": -0.09,
                "provider": "tradier",
                "reason": "tradier_live_quote",
                "as_of": "2026-03-05T12:00:30-05:00",
            },
            "VIX": {
                "price": 24.23,
                "pct_change": -2.81,
                "provider": "tradier",
                "reason": "tradier_live_quote",
                "as_of": "2026-03-05T12:00:30-05:00",
            },
        },
    )
    monkeypatch.setattr(market_data_service, "get_watchlist", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        market_data_service,
        "get_intraday",
        lambda symbol: (
            [{"close": 6773.42}, {"close": 6775.80}]
            if symbol == "SPX"
            else [{"close": 24.21}, {"close": 24.58}]
        ),
    )

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"dashboardCoreTapeCard" in resp.data
    assert b"dashboardCoreTapeRow" in resp.data
    assert b"dashboardCoreTapeStat" in resp.data
    assert b"dashboardTapeStreamStatus" in resp.data
    assert b"dashboardGapLine" in resp.data
    assert b"Gap O/N:" in resp.data
    assert b"-8.48 (-0.13%)" in resp.data
    assert b"Range: 6773.42-6775.80" in resp.data
    assert b"Volatility regime and gamma proxy anchor." in resp.data


def test_stream_market_sse_emits_json_payload(client, monkeypatch):
    from mccain_capital.services import market_worker
    from mccain_capital.services import options_panel_service
    from mccain_capital.services import gamma_map_service

    monkeypatch.setattr(market_worker, "start_market_worker_once", lambda: None)
    monkeypatch.setattr(options_panel_service, "start_options_worker_once", lambda: None)
    monkeypatch.setattr(gamma_map_service, "start_gamma_worker_once", lambda: None)
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
    monkeypatch.setattr(
        gamma_map_service,
        "get_gamma_snapshot",
        lambda: {
            "asof": "2026-03-05T12:00:00-05:00",
            "spot": 5120.35,
            "regime": "positive",
            "net_gex": 2100000000.0,
            "gamma_flip": 5110.0,
            "call_wall": 5150.0,
            "put_wall": 5050.0,
            "gamma_walls_top3": [5150.0, 5125.0, 5100.0],
            "void_zone": {"start": 5060.0, "end": 5090.0},
            "bias": "buy_dips_above_flip",
            "paths": {
                "csv": "/app/persistent/uploads/gamma_data.csv",
                "png": "/app/persistent/uploads/gamma_map.png",
            },
        },
    )
    monkeypatch.setattr(core_service.time, "sleep", lambda _: None)

    resp = client.get("/stream/market", follow_redirects=True)
    assert resp.status_code == 200
    assert resp.headers.get("Content-Type", "").startswith("text/event-stream")
    assert b"data: " in resp.data
    assert b"QQQ" in resp.data
    assert b"options" in resp.data
    assert b"gamma_map" in resp.data


def test_stream_market_ws_requires_upgrade(client):
    resp = client.get("/ws/market", follow_redirects=True)
    assert resp.status_code in {400, 501}


def test_trades_sync_live_get_redirects_to_upload_workspace(client):
    resp = client.get("/trades/sync/live", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/trades/upload/statement")


def test_trades_sync_auto_config_get_redirects_to_upload_workspace(client):
    resp = client.get("/trades/sync/auto/config", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/trades/upload/statement")


def test_market_pulse_renders_spx_gamma_details(client, monkeypatch):
    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Mar 2, 2026 10:30 AM ET",
            "source_label": "Massive market feed",
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
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import options_panel_service

    monkeypatch.setattr(gamma_map_service, "start_gamma_worker_once", lambda: None)
    monkeypatch.setattr(options_panel_service, "start_options_worker_once", lambda: None)
    monkeypatch.setattr(
        gamma_map_service,
        "get_gamma_snapshot",
        lambda: {
            "asof": "2026-03-05T12:00:00-05:00",
            "spot": 5120.35,
            "regime": "positive",
            "net_gex": 2100000000.0,
            "net_gamma_label": "+2.10B",
            "gamma_flip": 5110.0,
            "call_wall": 5150.0,
            "put_wall": 5050.0,
            "gamma_walls_top3": [5150.0, 5125.0, 5100.0],
            "void_zone": {"start": 5060.0, "end": 5090.0},
            "bias": "buy_dips_above_flip",
        },
    )
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {
            "asof": "2026-03-05T12:00:00-05:00",
            "symbols": {"SPX": {"contracts": [{"label": "SPXW 2026-03-06 5125C", "liq": "Tight"}]}},
        },
    )

    resp = client.get("/market-pulse", follow_redirects=True)
    assert resp.status_code == 200
    assert b"SPX Priority" in resp.data
    assert b"Gamma Flip" in resp.data
    assert b"Best Contracts" in resp.data
    assert b"SPXW 2026-03-06 5125C" in resp.data


def test_market_pulse_renders_source_health_and_degraded_banner(client, monkeypatch):
    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Mar 7, 2026 10:30 AM ET",
            "source_label": "Tradier market feed",
            "source_note": "cached fallback",
            "quotes": [],
            "integrity": {"live_count": 0, "delayed_count": 2, "missing_count": 3},
        },
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda: {
            "available": True,
            "source_note": "Live + cached merge",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
        },
    )
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import options_panel_service

    monkeypatch.setattr(gamma_map_service, "start_gamma_worker_once", lambda: None)
    monkeypatch.setattr(options_panel_service, "start_options_worker_once", lambda: None)
    monkeypatch.setattr(
        gamma_map_service,
        "get_gamma_snapshot",
        lambda: {"asof": "", "regime": "unavailable", "bias": "insufficient_data"},
    )
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {"asof": "", "symbols": {"SPX": {"contracts": []}}},
    )

    resp = client.get("/market-pulse", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Source Health" in resp.data
    assert b"Degraded Mode" in resp.data


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
