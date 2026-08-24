from mccain_capital.services.core import _market_radar_viewmodel


def _quote(symbol, pct, *, state="Mixed", price=100.0, low=90.0, high=110.0):
    return {
        "label": symbol,
        "price": price,
        "change_pct": pct,
        "watch_state": state,
        "freshness_label": "Live",
        "day_low": low,
        "day_high": high,
    }


def test_market_radar_deduplicates_and_separates_indexes():
    radar = _market_radar_viewmodel(
        [_quote("SPX", 0.1), _quote("SPX", 9.9), _quote("NVDA", 1.2, state="Strong")]
    )

    assert [item["radar_symbol"] for item in radar["index_pulse"]] == ["SPX"]
    assert [item["radar_symbol"] for item in radar["watchlist"]] == ["NVDA"]


def test_market_radar_ranks_conviction_then_move_and_handles_invalid_range():
    radar = _market_radar_viewmodel(
        [
            _quote("AAPL", 4.0, state="Mixed", low=100.0, high=100.0),
            _quote("AMD", 0.5, state="Strong"),
            _quote("TSLA", -2.0, state="Weak"),
        ]
    )

    assert [item["radar_symbol"] for item in radar["watchlist"]] == ["TSLA", "AMD", "AAPL"]
    assert [item["radar_rank"] for item in radar["watchlist"]] == [1, 2, 3]
    assert radar["watchlist"][2]["range_position"] is None
