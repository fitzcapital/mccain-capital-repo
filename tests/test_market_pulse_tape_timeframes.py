from datetime import datetime, timedelta, timezone

from mccain_capital.services.market_pulse_tape import timeframe_payloads


def _row(timestamp: datetime, close: float) -> dict[str, object]:
    return {
        "ts": timestamp.isoformat(),
        "open": close - 0.25,
        "high": close + 0.5,
        "low": close - 0.5,
        "close": close,
    }


def test_timeframe_payloads_use_ts_timestamps_for_distinct_windows() -> None:
    current = datetime(2026, 7, 16, 13, 40, tzinfo=timezone.utc)
    prior_session = [
        _row(current - timedelta(hours=23, minutes=55), 100.0),
        _row(current - timedelta(hours=23, minutes=45), 101.0),
    ]
    current_session = [
        _row(current - timedelta(minutes=10), 110.0),
        _row(current, 112.0),
    ]

    payloads = timeframe_payloads(prior_session + current_session, symbol="SPX")

    assert payloads["15M"]["change"] == 2.0
    assert payloads["1H"]["change"] == 2.0
    assert payloads["24H"]["change"] == 12.0
    assert payloads["24H"]["candles"][0]["close"] == 100.0
    assert payloads["1H"]["candles"][0]["close"] == 110.0


def test_dashboard_tape_refresh_includes_prior_session_for_24h(client, monkeypatch) -> None:
    from mccain_capital.services import market_data_service, market_worker

    current = datetime(2026, 7, 16, 13, 40, tzinfo=timezone.utc)
    current_rows = [
        _row(current - timedelta(minutes=10), 110.0),
        _row(current, 112.0),
    ]
    prior_rows = [
        _row(current - timedelta(hours=23, minutes=55), 100.0),
        _row(current - timedelta(hours=23, minutes=45), 101.0),
    ]

    monkeypatch.setattr(market_worker, "start_market_worker_once", lambda: None)
    monkeypatch.setattr(
        market_worker,
        "get_market_snapshot",
        lambda: {
            "updated_at": current.isoformat(),
            "prices": {
                symbol: {
                    "symbol": symbol,
                    "label": symbol,
                    "price": 112.0,
                    "pct_change": 1.0,
                    "mini_series": [109.0, 110.0, 111.0, 112.0],
                }
                for symbol in ("SPX", "VIX")
            },
        },
    )
    monkeypatch.setattr(market_data_service, "get_intraday", lambda _symbol: current_rows)
    monkeypatch.setattr(
        market_data_service,
        "get_prior_session_intraday",
        lambda _symbol: prior_rows,
    )

    response = client.get("/api/dashboard/tape?symbols=SPX,VIX")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["timeframes"]["SPX"]["1H"]["change"] == 2.0
    assert payload["timeframes"]["SPX"]["24H"]["change"] == 12.0
    assert payload["timeframes"]["VIX"]["1H"]["change"] == 2.0
    assert payload["timeframes"]["VIX"]["24H"]["change"] == 12.0
