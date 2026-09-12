from datetime import datetime
from datetime import timedelta
from zoneinfo import ZoneInfo

from mccain_capital.services.market_pulse_freshness import build_canonical_freshness


ET = ZoneInfo("America/New_York")


def test_generation_is_stable_for_identical_canonical_content():
    now = datetime(2026, 8, 10, 10, 0, tzinfo=ET)
    timestamps = {
        name: (now - timedelta(seconds=5)).isoformat()
        for name in ("spot", "bars", "gamma", "options", "strategy")
    }
    first = build_canonical_freshness(
        now=now, market_open=True, timestamps=timestamps, fingerprint={"spot": 7000.0}
    )
    second = build_canonical_freshness(
        now=now, market_open=True, timestamps=timestamps, fingerprint={"spot": 7000.0}
    )
    assert first["generation_id"] == second["generation_id"]
    assert first["execution_locked"] is False


def test_generation_changes_with_execution_content():
    now = datetime(2026, 8, 10, 10, 0, tzinfo=ET)
    timestamps = {
        name: now.isoformat() for name in ("spot", "bars", "gamma", "options", "strategy")
    }
    first = build_canonical_freshness(
        now=now, market_open=True, timestamps=timestamps, fingerprint={"spot": 7000.0}
    )
    second = build_canonical_freshness(
        now=now, market_open=True, timestamps=timestamps, fingerprint={"spot": 7001.0}
    )
    assert first["generation_id"] != second["generation_id"]


def test_stale_required_component_locks_execution_without_masking_age():
    now = datetime(2026, 8, 10, 10, 0, tzinfo=ET)
    payload = build_canonical_freshness(
        now=now,
        market_open=True,
        timestamps={
            "spot": now.isoformat(),
            "bars": now.isoformat(),
            "gamma": (now - timedelta(seconds=361)).isoformat(),
            "options": now.isoformat(),
            "strategy": now.isoformat(),
        },
        fingerprint={},
    )
    assert payload["execution_locked"] is True
    assert payload["stale_required_components"] == ["gamma"]
    assert payload["components"]["gamma"]["age_seconds"] == 361


def test_missing_timestamp_is_explicitly_unavailable():
    now = datetime(2026, 8, 10, 10, 0, tzinfo=ET)
    payload = build_canonical_freshness(now=now, market_open=True, timestamps={}, fingerprint={})
    assert payload["components"]["spot"]["status"] == "unavailable"
    assert payload["execution_locked"] is True


def test_completed_five_minute_bar_has_one_cycle_of_delivery_grace():
    now = datetime(2026, 8, 10, 10, 9, tzinfo=ET)
    payload = build_canonical_freshness(
        now=now,
        market_open=True,
        timestamps={
            "spot": now.isoformat(),
            "bars": (now - timedelta(minutes=4)).isoformat(),
            "gamma": now.isoformat(),
        },
        fingerprint={},
    )

    assert payload["components"]["bars"]["threshold_seconds"] == 360
    assert payload["components"]["bars"]["status"] == "current"
    assert payload["execution_locked"] is False


def test_diagnostics_metadata_distinguishes_required_and_optional_components():
    now = datetime(2026, 8, 10, 10, 0, tzinfo=ET)
    payload = build_canonical_freshness(
        now=now,
        market_open=True,
        timestamps={name: now.isoformat() for name in ("spot", "bars", "gamma")},
        fingerprint={},
    )

    assert payload["components"]["spot"]["required"] is True
    assert payload["components"]["bars"]["required"] is True
    assert payload["components"]["gamma"]["required"] is True
    assert payload["components"]["options"]["required"] is False
    assert payload["components"]["strategy"]["required"] is False
    assert payload["stale_required_components"] == []
    assert payload["execution_locked"] is False
    assert payload["sync_interval_seconds"] == 15


def test_future_dated_required_component_locks_execution():
    now = datetime(2026, 8, 20, 11, 0, tzinfo=ET)
    payload = build_canonical_freshness(
        now=now,
        market_open=True,
        timestamps={
            "spot": (now + timedelta(minutes=2)).isoformat(),
            "bars": now.isoformat(),
            "gamma": now.isoformat(),
        },
        fingerprint={},
        symbol="SPX",
        session_id="2026-08-20",
    )

    assert payload["components"]["spot"]["status"] == "future"
    assert payload["components"]["spot"]["future_dated"] is True
    assert payload["execution_locked"] is True
    assert payload["symbol"] == "SPX"
    assert payload["session_id"] == "2026-08-20"


def test_each_required_component_failure_is_named_and_fails_closed():
    now = datetime(2026, 8, 20, 11, 0, tzinfo=ET)

    for failed_component in ("spot", "bars", "gamma"):
        timestamps = {name: now.isoformat() for name in ("spot", "bars", "gamma")}
        timestamps[failed_component] = ""
        payload = build_canonical_freshness(
            now=now,
            market_open=True,
            timestamps=timestamps,
            fingerprint={},
            symbol="SPX",
            session_id="2026-08-20",
        )

        assert payload["components"][failed_component]["status"] == "unavailable"
        assert payload["execution_locked"] is True
        assert payload["stale_required_components"] == [failed_component]
