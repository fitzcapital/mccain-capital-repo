from mccain_capital.services import core
from mccain_capital.services import tradier_hero_chart_service as hero_service


def test_hero_levels_api_uses_service_payload(client, monkeypatch):
    monkeypatch.setattr(
        core,
        "get_or_build_market_pulse_snapshot",
        lambda force_refresh=False, now_et=None, **kwargs: {
            "quotes": [{"symbol": "SPX", "provider": "market_snapshot"}],
            "spx_quote": {"symbol": "SPX", "provider": "market_snapshot"},
            "execution_chart": {},
            "execution_model": {"posture_summary": ""},
            "market_structure_snapshot": {
                "spot": 6611.83,
                "snapshot_timestamp": "2026-04-07T15:55:00-04:00",
                "session_mode": "regular",
                "session_mode_label": "Regular",
                "levels_source": "live_session_snapshot",
                "levels_source_label": "Live session snapshot",
                "gamma_data_status": "fresh_valid",
                "gamma_data_status_label": "Fresh valid",
                "main_flip": 6785.0,
                "local_flip": 6593.0,
                "call_wall": 6600.0,
                "put_wall": 6575.0,
                "next_call_wall": 6620.0,
                "next_put_wall": 6590.0,
                "gamma_regime": "negative",
                "gamma_regime_label": "Negative Gamma",
                "gamma_regime_subtitle": "Trend / momentum active",
                "regime_confidence": "high",
                "regime_confidence_label": "High confidence",
                "execution_regime": "trend_momentum_active",
                "execution_regime_label": "Trend momentum active",
                "planning_bias": "above_call_wall_extension_risk",
                "planning_bias_label": "Above Call Wall · extension risk toward 6620",
                "bias_state": "above_local",
                "bias_context": "ABOVE LOCAL FLIP",
                "bias_label": "BUY DIPS",
                "bias": "Above Call Wall · extension risk toward 6620",
                "tradeability": "Trend momentum active",
                "session": "Regular · High confidence",
                "trade_state": "NO_TRADE",
                "trade_state_label": "NO TRADE",
                "current_read": "Above Call Wall",
                "pullback_level": "CW 6600",
                "next_destination": "NCW 6620",
                "plan_note": "Wait for pullback into call wall",
                "best_look": "Wait for pullback into Call Wall",
                "required_trigger": "Sweep + reclaim + 2-2 + volume",
                "invalidation": "Lose LF 6593",
                "spot_meta": {"value": 6611.83, "source_label": "Live Session"},
                "local_flip_meta": {"value": 6593.0, "source": "live_session"},
                "level_meta": {},
                "gamma_regime_meta": {"value": "negative"},
                "chart_meta": {"chart_state": "live_session"},
            },
        },
    )

    response = client.get("/api/hero/levels")

    assert response.status_code == 200
    assert response.get_json()["spot"] == 6611.83


def test_hero_bars_api_returns_normalized_bars(client, monkeypatch):
    monkeypatch.setattr(
        hero_service,
        "get_intraday_bars",
        lambda symbol="SPX", interval="5min": {
            "symbol": symbol,
            "interval": interval,
            "bars": [{"time": 1712601900, "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 1200}],
            "opening_session_mode": True,
            "live_session_bar_count": 2,
            "opening_threshold": 10,
        },
    )

    response = client.get("/api/hero/bars")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["symbol"] == "SPX"
    assert payload["bars"][0]["close"] == 1.5
    assert payload["opening_session_mode"] is True
    assert payload["live_session_bar_count"] == 2
