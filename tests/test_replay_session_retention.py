from datetime import datetime
import re
import shutil
import subprocess

import pytest

from mccain_capital.services.market_pulse_setup_replay import resolve_replay_session


def bar(stamp, **extra):
    return {"ts": stamp, "open": 100, "high": 102, "low": 99, "close": 101, **extra}


@pytest.mark.parametrize(
    "now",
    [
        "2026-08-30T12:00:00-04:00",
        "2026-08-31T08:30:00-04:00",
        "2026-08-31T09:34:59-04:00",
        "2026-08-31T02:00:00+00:00",
    ],
)
def test_retains_friday_until_new_completed_candle(now):
    rows = [bar("2026-08-28T13:15:00-04:00"), bar("2026-08-31T09:30:00-04:00")]
    result = resolve_replay_session(rows, now=datetime.fromisoformat(now))
    assert result["session_date"] == "2026-08-28"
    assert result["evaluated_through"] == rows[0]["ts"]
    assert result["review_only"] is True


def test_first_completed_new_session_candle_switches_without_setup():
    result = resolve_replay_session(
        [bar("2026-08-28T13:15:00-04:00"), bar("2026-08-31T09:30:00-04:00")],
        now=datetime.fromisoformat("2026-08-31T09:35:00-04:00"),
    )
    assert result["session_date"] == "2026-08-31"
    assert len(result["bars"]) == 1
    assert result["review_only"] is False


def test_holiday_rejects_non_session_and_malformed_rows():
    result = resolve_replay_session(
        [
            bar("2026-09-04T15:55:00-04:00"),
            bar("2026-09-07T10:00:00-04:00"),
            bar("bad"),
            bar("2026-09-04T16:05:00-04:00"),
            bar("2026-09-04T15:50:00-04:00", complete=False),
        ],
        now=datetime.fromisoformat("2026-09-07T12:00:00-04:00"),
    )
    assert result["session_date"] == "2026-09-04"
    assert len(result["bars"]) == 1


def test_early_close_bounds_and_utc_normalization():
    result = resolve_replay_session(
        [bar("2026-11-27T17:55:00Z"), bar("2026-11-27T18:00:00Z")],
        now=datetime.fromisoformat("2026-11-28T12:00:00-05:00"),
    )
    assert result["evaluated_through"] == "2026-11-27T12:55:00-05:00"
    assert len(result["bars"]) == 1


def test_explicit_missing_session_does_not_fall_back():
    result = resolve_replay_session(
        [bar("2026-08-28T13:15:00-04:00")],
        now=datetime.fromisoformat("2026-08-30T12:00:00-04:00"),
        requested_session="2026-08-27",
    )
    assert result["session_date"] == "2026-08-27"
    assert result["available"] is False
    assert result["bars"] == []


def test_weekend_endpoint_scopes_level_reads_and_never_evaluates_live_alerts(client, monkeypatch):
    from mccain_capital.services import core

    monkeypatch.setattr(
        core.app_runtime, "now_et", lambda: datetime.fromisoformat("2026-08-30T12:00:00-04:00")
    )
    snapshot = {
        "ticker": "SPX",
        "execution_chart": {
            "strategy_bars_5m": [
                bar("2026-08-28T13:05:00-04:00", high=100, low=98, close=99),
                bar("2026-08-28T13:10:00-04:00", high=102, low=98.5, close=101),
                bar("2026-08-28T13:15:00-04:00", high=101, low=97, close=99),
            ]
        },
        "playbook_quote": {"prior_day_high": 101, "prior_day_low": 90},
    }
    monkeypatch.setattr(
        core, "_market_pulse_setup_replay_source_snapshot", lambda *a, **k: snapshot
    )
    observed_dates = []
    monkeypatch.setattr(
        core,
        "_market_pulse_durable_level_observations",
        lambda ticker, session_date: observed_dates.append(session_date) or [],
    )

    def no_alerts(**kwargs):
        raise AssertionError("Replay must not evaluate Live alerts")

    monkeypatch.setattr(core, "evaluate_live_setup_monitor", no_alerts)
    first = client.get("/api/market-pulse/setup-replay?ticker=SPX").get_json()["payload"]
    second = client.get("/api/market-pulse/setup-replay?ticker=SPX").get_json()["payload"]
    assert first == second
    assert first["session_date"] == "2026-08-28"
    assert first["setup_count"] > 0
    assert first["review_only"] is True
    assert observed_dates == ["2026-08-28", "2026-08-28"]
    assert first["evaluated_through"].endswith("13:15:00-04:00")
    assert client.get("/api/market-pulse/setup-replay?session_date=bad").status_code == 400


def test_source_uses_retained_disk_snapshot_without_live_cache_age_permission(client, monkeypatch):
    from mccain_capital.services import core

    snapshot = {
        "ticker": "SPX",
        "canonical_freshness": {"session_id": "2026-08-28"},
        "execution_chart": {"strategy_bars_5m": [bar("2026-08-28T13:15:00-04:00")]},
    }
    monkeypatch.setattr(core, "_market_pulse_cached_playbook_snapshot", lambda *a, **k: None)
    monkeypatch.setattr(core, "_market_pulse_context_response_cache", {})
    monkeypatch.setattr(
        core, "_load_market_pulse_playbook_disk_cache", lambda: {"payload": snapshot}
    )
    with client.application.app_context():
        result = core._market_pulse_setup_replay_source_snapshot(
            datetime.fromisoformat("2026-08-30T12:00:00-04:00"), ticker="SPX"
        )
    assert result == snapshot


def test_replay_template_labels_and_inline_javascript_compile(client):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node is required for rendered JavaScript syntax checks")
    response = client.get("/market-pulse?ticker=SPX")
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "replayPayload.session_label" in body
    assert 'replayPayload.review_only ? "Review only"' in body
    assert "replayPayload.evaluated_through" in body
    checked = 0
    for attrs, script in re.findall(r"<script\b([^>]*)>(.*?)</script>", body, re.S):
        if "application/json" in attrs or not script.strip():
            continue
        result = subprocess.run([node, "--check"], input=script, text=True, capture_output=True)
        assert result.returncode == 0, result.stderr
        checked += 1
    assert checked > 0
