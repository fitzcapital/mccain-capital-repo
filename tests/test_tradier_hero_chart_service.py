from mccain_capital.services import tradier_hero_chart_service as svc


def test_normalize_tradier_timesales_aggregates_to_five_minute_bars():
    rows = [
        {"ts": "2026-04-06T09:31:00-04:00", "open": 6600.0, "high": 6601.0, "low": 6599.5, "close": 6600.5, "volume": 100},
        {"ts": "2026-04-06T09:34:00-04:00", "open": 6600.5, "high": 6602.0, "low": 6600.0, "close": 6601.5, "volume": 150},
        {"ts": "2026-04-06T09:36:00-04:00", "open": 6601.5, "high": 6603.0, "low": 6601.0, "close": 6602.5, "volume": 200},
    ]

    bars = svc.normalize_tradier_timesales(rows)

    assert len(bars) == 2
    assert bars[0]["open"] == 6600.0
    assert bars[0]["high"] == 6602.0
    assert bars[0]["low"] == 6599.5
    assert bars[0]["close"] == 6601.5
    assert bars[0]["volume"] == 250.0
    assert bars[1]["open"] == 6601.5
    assert bars[1]["close"] == 6602.5


def test_derive_hero_state_above_call_wall_prefers_next_call_wall():
    payload = svc.derive_hero_state(6611.83, 6571, 6605, 6550, 6615, 6570)

    assert payload["state"] == "NO_TRADE"
    assert payload["current_read"] == "Above Call Wall"
    assert payload["pullback_level"] == "CW 6,605"
    assert payload["next_destination"] == "NCW 6,615"


def test_derive_hero_state_below_put_wall_uses_next_put_wall():
    payload = svc.derive_hero_state(6548, 6571, 6605, 6550, 6615, 6535)

    assert payload["state"] == "WAIT"
    assert payload["current_read"] == "Below Local Flip"
    assert payload["next_destination"] == "NPW 6,535"
