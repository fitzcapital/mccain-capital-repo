from datetime import datetime

from mccain_capital.runtime import set_setting_value
from mccain_capital.services import core
from werkzeug.security import generate_password_hash


def test_get_playbook_ticker_context_defaults_to_spy():
    context = core.get_playbook_ticker_context(None)

    assert context["ticker"] == "SPY"
    assert context["alternate_ticker"] == "SPX"
    assert tuple(context["supported_tickers"]) == ("SPX", "SPY", "QQQ")
    assert context["storage_key"] == "mc_market_pulse_ticker"


def test_profile_defaults_drive_dashboard_and_market_pulse_when_no_query(client):
    set_setting_value("auth_username", "admin")
    set_setting_value("auth_password_hash", generate_password_hash("password123"))
    set_setting_value(
        "auth_user_profiles",
        '{"users":{"admin":{"display_name":"Fitz","title":"Owner","market_pulse_default_ticker":"QQQ","dashboard_default_ticker":"SPX","is_admin":true}}}',
    )
    with client.session_transaction() as session:
        session["auth_ok"] = True
        session["auth_user"] = "admin"

    dashboard_resp = client.get("/dashboard", follow_redirects=True)
    market_resp = client.get("/market-pulse", follow_redirects=True)

    assert dashboard_resp.status_code == 200
    assert market_resp.status_code == 200
    assert 'data-selected-ticker="SPX"' in dashboard_resp.get_data(as_text=True)
    assert "QQQ PLAYBOOK" in market_resp.get_data(as_text=True)


def test_dashboard_defaults_to_spy_switcher_and_market_pulse_links(client):
    resp = client.get("/dashboard", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'data-selected-ticker="SPY"' in body
    assert 'data-dashboard-ticker-switch="SPY"' in body
    assert 'data-dashboard-ticker-switch="SPX"' in body
    assert 'href="/market-pulse?ticker=SPY"' in body
    assert 'data-dashboard-ticker-switch="QQQ"' in body
    assert 'href="/dashboard?ticker=QQQ' in body
    assert 'href="/dashboard?ticker=SPY' in body


def test_dashboard_honors_qqq_ticker_in_switcher_and_links(client):
    resp = client.get("/dashboard?ticker=QQQ", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'data-selected-ticker="QQQ"' in body
    assert 'href="/market-pulse?ticker=QQQ"' in body
    assert 'data-dashboard-ticker-switch="QQQ"' in body


def test_dashboard_honors_spy_ticker_in_switcher_and_links(client):
    resp = client.get("/dashboard?ticker=SPY", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'data-selected-ticker="SPY"' in body
    assert 'href="/market-pulse?ticker=SPY"' in body
    assert 'href="/dashboard?ticker=QQQ' in body
    assert 'href="/dashboard?ticker=SPX' in body


def test_dashboard_honors_spx_ticker_in_switcher_and_links(client):
    resp = client.get("/dashboard?ticker=SPX", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'data-selected-ticker="SPX"' in body
    assert 'href="/market-pulse?ticker=SPX"' in body
    assert 'href="/dashboard?ticker=QQQ' in body
    assert 'href="/dashboard?ticker=SPY' in body


def test_dashboard_daily_brief_localizes_to_selected_ticker():
    brief = core._dashboard_daily_brief_viewmodel(
        now_et=datetime.fromisoformat("2026-04-10T09:37:54-04:00"),
        ticker="SPY",
        dashboard_spx={"price": 548.0, "pct_change": -0.4, "day_open": 550.0},
        dashboard_instrument={"price": 548.0, "pct_change": -0.4, "day_open": 550.0},
        dashboard_vix={"price": 18.2},
        gamma_snapshot={
            "gamma_flip": 551.0,
            "local_flip": 545.0,
            "call_wall": 552.0,
            "put_wall": 541.0,
        },
        market_structure_snapshot={
            "spot": 548.0,
            "main_flip": 551.0,
            "local_flip": 545.0,
            "call_wall": 552.0,
            "put_wall": 541.0,
        },
        news_snapshot={"macro_events": []},
        today_count=0,
        today_net=0.0,
    )

    assert "SPY is below gamma flip 551" in brief["bias_summary"]
    assert "Generated from live SPY" in brief["source_detail"]
