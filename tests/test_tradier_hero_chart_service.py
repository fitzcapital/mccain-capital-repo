from datetime import datetime
from datetime import timedelta

from mccain_capital.services import core
from mccain_capital.services import tradier_hero_chart_service as svc


def _ts(year: int, month: int, day: int, hour: int, minute: int) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=svc.app_runtime.TZ).timestamp())


def test_normalize_tradier_timesales_aggregates_to_five_minute_bars():
    rows = [
        {
            "ts": "2026-04-06T09:31:00-04:00",
            "open": 6600.0,
            "high": 6601.0,
            "low": 6599.5,
            "close": 6600.5,
            "volume": 100,
        },
        {
            "ts": "2026-04-06T09:34:00-04:00",
            "open": 6600.5,
            "high": 6602.0,
            "low": 6600.0,
            "close": 6601.5,
            "volume": 150,
        },
        {
            "ts": "2026-04-06T09:36:00-04:00",
            "open": 6601.5,
            "high": 6603.0,
            "low": 6601.0,
            "close": 6602.5,
            "volume": 200,
        },
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


def test_opening_session_carryover_prepends_prior_session_context():
    current_bars = [
        {
            "time": _ts(2026, 4, 7, 9, 30),
            "open": 6600,
            "high": 6602,
            "low": 6598,
            "close": 6601,
            "volume": 1000,
        },
        {
            "time": _ts(2026, 4, 7, 9, 35),
            "open": 6601,
            "high": 6603,
            "low": 6600,
            "close": 6602,
            "volume": 1100,
        },
    ]
    prior_bars = [
        {
            "time": _ts(2026, 4, 6, 14, 30),
            "open": 6560,
            "high": 6561,
            "low": 6558,
            "close": 6559,
            "volume": 900,
        },
        {
            "time": _ts(2026, 4, 6, 14, 35),
            "open": 6559,
            "high": 6560,
            "low": 6557,
            "close": 6558,
            "volume": 850,
        },
    ]

    payload = svc._opening_session_carryover_bars(
        current_bars=current_bars,
        prior_bars=prior_bars,
        session_day=svc.date(2026, 4, 7),
        interval="5min",
    )

    assert payload["opening_session_mode"] is True
    assert payload["live_session_bar_count"] == 2
    assert payload["carryover_bar_count"] == 2
    assert len(payload["bars"]) == 4
    assert payload["bars"][0]["time"] == prior_bars[0]["time"]
    assert payload["bars"][-1]["time"] == current_bars[-1]["time"]


def test_opening_session_carryover_disables_once_threshold_is_met():
    current_bars = []
    start = datetime(2026, 4, 7, 9, 30, tzinfo=svc.app_runtime.TZ)
    for index in range(10):
        current_bars.append(
            {
                "time": int((start + timedelta(minutes=index * 5)).timestamp()),
                "open": 6600 + index,
                "high": 6601 + index,
                "low": 6599 + index,
                "close": 6600.5 + index,
                "volume": 1000,
            }
        )

    payload = svc._opening_session_carryover_bars(
        current_bars=current_bars,
        prior_bars=[],
        session_day=svc.date(2026, 4, 7),
        interval="5min",
    )

    assert payload["opening_session_mode"] is False
    assert payload["live_session_bar_count"] == 10
    assert payload["carryover_bar_count"] == 0
    assert len(payload["bars"]) == 10


def test_get_hero_levels_uses_shared_snapshot_regime_without_reclassification(monkeypatch):
    now_et = datetime(2026, 4, 8, 18, 0, 0, tzinfo=svc.app_runtime.TZ)
    monkeypatch.setattr(svc.app_runtime, "now_et", lambda: now_et)
    monkeypatch.setattr(
        core,
        "get_or_build_market_pulse_snapshot",
        lambda force_refresh=False, now_et=None: {
            "quotes": [{"symbol": "SPX", "provider": "market_snapshot"}],
            "spx_quote": {"symbol": "SPX", "provider": "market_snapshot"},
            "execution_model": {"posture_summary": "Shared snapshot summary"},
            "market_structure_snapshot": {
                "snapshot_timestamp": "2026-04-08T15:55:00-04:00",
                "spot": 6782.81,
                "gamma_regime": "negative",
                "gamma_regime_label": "Negative Gamma",
                "gamma_regime_subtitle": "Trend / momentum active",
                "regime_confidence": "high",
                "regime_confidence_label": "High confidence",
                "levels_source": "live_session_snapshot",
                "levels_source_label": "Live session snapshot",
                "gamma_data_status": "fresh_valid",
                "gamma_data_status_label": "Fresh valid",
                "session_mode": "regular",
                "session_mode_label": "Regular",
                "main_flip": 6805.0,
                "local_flip": 6805.0,
                "call_wall": 6775.0,
                "put_wall": 6525.0,
                "next_call_wall": 6785.0,
                "next_put_wall": 6575.0,
                "bias": "Above Call Wall · extension risk toward 6785",
                "tradeability": "FULL_MODE",
                "session": "Regular · High confidence",
                "trade_state": "PLANNING_ONLY",
                "trade_state_label": "PLANNING ONLY",
                "current_read": "Above Call Wall",
                "pullback_level": "CW 6775",
                "next_destination": "NCW 6785",
                "plan_note": "Wait for pullback into Call Wall",
                "best_look": "Wait for pullback into Call Wall",
                "required_trigger": "Next live session retest and hold at Call Wall",
                "invalidation": "CW 6775",
            },
        },
    )

    payload = svc.get_hero_levels(symbol="SPX")

    assert payload["gamma_regime"] == "negative"
    assert payload["gamma_regime_label"] == "Negative Gamma"
    assert payload["pullback_level"] == "CW 6775"
    assert payload["provider"] == "market_snapshot"
    assert payload["posture_summary"] == "Shared snapshot summary"
