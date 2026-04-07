from mccain_capital.services import tradier_hero_chart_service as hero_service


def test_hero_levels_api_uses_service_payload(client, monkeypatch):
    monkeypatch.setattr(
        hero_service,
        "get_hero_levels",
        lambda symbol="SPX": {"symbol": symbol, "spot": 6611.83, "state": "NO_TRADE"},
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
