from datetime import date
from pathlib import Path

from mccain_capital.runtime import get_setting_value
from mccain_capital.services import core as core_service


def _pace_viewmodel(buffer: float):
    return core_service._dashboard_pace_viewmodel(
        {
            "avg": 0.0,
            "base_balance": 75000.0,
            "p5": {"days": 5},
            "p10": {"days": 10},
            "p20": {"days": 20},
        },
        {},
        {
            "custom_daily": 500.0,
            "custom_enabled": True,
            "pass_buffer": buffer,
            "pass_buffer_enabled": buffer > 0.0,
            "target_date": "2026-08-10",
        },
        anchor_day=date(2026, 8, 3),
        pace_scope={"key": "m", "label": "Month", "suffix": "/month", "factor": 20},
    )


def test_forward_pace_viewmodel_keeps_projected_and_available_balances_distinct():
    pace = _pace_viewmodel(7500.0)

    assert pace["nodes"][0]["est_pnl"] == "-$5,000.00"
    assert pace["nodes"][0]["est_balance"] == "$77,500.00"
    assert pace["nodes"][0]["available_balance"] == "$70,000.00"
    assert pace["nodes"][2]["est_pnl"] == "$2,500.00"
    assert pace["nodes"][2]["est_balance"] == "$85,000.00"
    assert pace["nodes"][2]["available_balance"] == "$77,500.00"
    assert pace["target"]["sessions"] == 5
    assert pace["target"]["est_pnl"] == "-$5,000.00"
    assert pace["target"]["est_balance"] == "$77,500.00"
    assert pace["target"]["available_balance"] == "$70,000.00"


def test_forward_pace_viewmodel_handles_zero_and_larger_than_profit_buffers_once():
    no_buffer = _pace_viewmodel(0.0)
    large_buffer = _pace_viewmodel(20000.0)

    assert no_buffer["nodes"][0]["est_pnl"] == "$2,500.00"
    assert no_buffer["nodes"][0]["est_balance"] == "$77,500.00"
    assert no_buffer["nodes"][0]["available_balance"] == "$77,500.00"
    assert no_buffer["target"]["available_balance"] == "$77,500.00"
    assert large_buffer["nodes"][0]["est_pnl"] == "-$17,500.00"
    assert large_buffer["nodes"][0]["est_balance"] == "$77,500.00"
    assert large_buffer["nodes"][0]["available_balance"] == "$57,500.00"
    assert large_buffer["target"]["est_pnl"] == "-$17,500.00"
    assert large_buffer["target"]["available_balance"] == "$57,500.00"


def test_dashboard_forward_pace_inline_save_returns_authoritative_fragment(client):
    response = client.post(
        "/dashboard/pace",
        data={
            "dashboard_pace_daily": "500",
            "dashboard_pace_buffer": "7500",
            "dashboard_projection_start_date": "2026-08-03",
            "dashboard_projection_target_date": "2026-09-11",
            "y": "2026",
            "m": "8",
            "scope": "all",
            "ticker": "SPY",
            "pace_tf": "m",
        },
        headers={"X-Dashboard-Partial": "forward-pace", "Accept": "application/json"},
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["message"] == "Forward pace updated."
    assert 'id="dashboardForwardPaceCard"' in payload["fragment"]
    assert 'value="500.00"' in payload["fragment"]
    assert 'value="7500.00"' in payload["fragment"]
    assert "Projected Profit After Buffer" in payload["fragment"]
    assert "Available Balance After Buffer" in payload["fragment"]
    assert get_setting_value("dashboard_pace_daily", "") == "500.00"
    assert get_setting_value("dashboard_pace_buffer", "") == "7500.00"


def test_dashboard_forward_pace_inline_reset_supports_consecutive_update(client):
    headers = {"X-Dashboard-Partial": "forward-pace", "Accept": "application/json"}
    first = client.post(
        "/dashboard/pace",
        data={"dashboard_pace_daily": "500", "dashboard_pace_buffer": "7500"},
        headers=headers,
    )
    second = client.post(
        "/dashboard/pace",
        data={"dashboard_pace_daily": "500", "dashboard_pace_buffer": "7500", "pace_reset": "1"},
        headers=headers,
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.get_json()["message"] == "Forward pace reset to live trading pace."
    assert get_setting_value("dashboard_pace_daily", "missing") == ""
    assert get_setting_value("dashboard_pace_buffer", "missing") == ""


def test_dashboard_forward_pace_inline_rejects_invalid_dates_without_saving(client):
    response = client.post(
        "/dashboard/pace",
        data={
            "dashboard_pace_daily": "500",
            "dashboard_pace_buffer": "7500",
            "dashboard_projection_target_date": "not-a-date",
        },
        headers={"X-Dashboard-Partial": "forward-pace", "Accept": "application/json"},
    )

    assert response.status_code == 400
    assert response.get_json()["ok"] is False
    assert "not saved" in response.get_json()["error"]
    assert get_setting_value("dashboard_pace_daily", "") == ""
    assert get_setting_value("dashboard_pace_buffer", "") == ""


def test_dashboard_forward_pace_keeps_non_json_redirect_fallback(client):
    response = client.post(
        "/dashboard/pace",
        data={"dashboard_pace_daily": "500", "dashboard_pace_buffer": "7500"},
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("pace_tf=m")


def test_dashboard_forward_pace_frontend_contract(client):
    body = client.get("/dashboard", follow_redirects=True).get_data(as_text=True)
    script = (Path(__file__).parents[1] / "static/js/dashboard_interactions.js").read_text()

    for hook in (
        'id="dashboardForwardPaceCard"',
        'id="dashboardForwardPaceForm"',
        "data-forward-pace-card",
        "data-forward-pace-form",
        "data-forward-pace-status",
        'aria-live="polite"',
    ):
        assert hook in body

    for contract in (
        'document.addEventListener("submit"',
        'form.dataset.submitting === "1"',
        '"X-Dashboard-Partial": "forward-pace"',
        "card.replaceWith(replacement)",
        "window.scrollTo(0, scrollTop)",
        '        "error",',
    ):
        assert contract in script
