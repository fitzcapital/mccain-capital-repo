from datetime import datetime
import threading
from zoneinfo import ZoneInfo

from flask import Flask

from mccain_capital.services import core
from mccain_capital.services import market_pulse_setup_monitor_runtime as monitor_runtime


ET = ZoneInfo("America/New_York")
NOW = datetime(2026, 9, 8, 11, 30, tzinfo=ET)


def setup_function():
    monitor_runtime.reset_server_setup_monitor_for_tests()


def teardown_function():
    monitor_runtime.reset_server_setup_monitor_for_tests()


def test_one_cycle_updates_heartbeat_and_completed_candle():
    app = Flask(__name__)

    result = monitor_runtime.run_server_setup_monitor_cycle(
        app,
        now=NOW,
        evaluator=lambda **_kwargs: {
            "status": "healthy",
            "last_evaluated_candle": "2026-09-08T11:25:00-04:00",
            "generation_id": "g1",
        },
    )
    state = monitor_runtime.get_server_setup_monitor_state(now=NOW)

    assert result["status"] == "healthy"
    assert state["heartbeat_age_seconds"] == 0
    assert state["last_evaluated_candle"].endswith("11:25:00-04:00")
    assert state["consecutive_failures"] == 0


def test_clock_discontinuity_is_forwarded_and_marks_recovery():
    app = Flask(__name__)
    observed = {}

    def evaluator(**kwargs):
        observed.update(kwargs)
        return {"status": "degraded", "error": "clock_revalidation_required"}

    monitor_runtime.run_server_setup_monitor_cycle(
        app, now=NOW, clock_discontinuous=True, evaluator=evaluator
    )
    state = monitor_runtime.get_server_setup_monitor_state(now=NOW)

    assert observed["clock_discontinuous"] is True
    assert state["clock_status"] == "recovering"
    assert state["consecutive_failures"] == 1


def test_duplicate_start_keeps_one_process_owner(monkeypatch):
    app = Flask(__name__)
    app.config["TESTING"] = True
    started = []

    class FakeThread:
        def __init__(self, **kwargs):
            started.append(kwargs)

        def start(self):
            return None

        def is_alive(self):
            return False

    monkeypatch.setattr(threading, "Thread", FakeThread)

    assert monitor_runtime.start_server_setup_monitor_once(app, force=True) is True
    assert monitor_runtime.start_server_setup_monitor_once(app, force=True) is False
    assert len(started) == 1


def test_core_cycle_sleeps_outside_session(monkeypatch):
    app = Flask(__name__)
    monkeypatch.setattr(
        core, "_market_pulse_refresh_contract", lambda *_args: {"market_phase": "closed"}
    )

    with app.app_context():
        result = core.run_market_pulse_server_setup_monitor_cycle(now_et=NOW)

    assert result == {"status": "sleeping", "last_evaluated_candle": ""}


def test_core_cycle_skips_unchanged_completed_candle(monkeypatch):
    app = Flask(__name__)
    candle = "2026-09-08T11:25:00-04:00"
    snapshot = {
        "canonical_freshness": {"generation_id": "g1", "execution_locked": False},
        "execution_chart": {"strategy_bars_5m": [{"completed_at": candle}]},
    }
    monkeypatch.setattr(
        core, "_market_pulse_refresh_contract", lambda *_args: {"market_phase": "open"}
    )
    monkeypatch.setattr(core, "get_or_build_market_pulse_snapshot", lambda **_kwargs: snapshot)
    monkeypatch.setattr(
        "mccain_capital.services.market_pulse_runtime.ensure_market_pulse_runtime_started",
        lambda: None,
    )

    with app.app_context():
        result = core.run_market_pulse_server_setup_monitor_cycle(
            now_et=NOW, previous_candle=candle
        )

    assert result["status"] == "unchanged"
    assert result["last_evaluated_candle"] == candle


def test_core_cycle_uses_shared_monitor_without_claiming_delivery(monkeypatch):
    app = Flask(__name__)
    candle = "2026-09-08T11:25:00-04:00"
    snapshot = {
        "canonical_freshness": {"generation_id": "g2", "execution_locked": False},
        "execution_chart": {"strategy_bars_5m": [{"completed_at": candle}]},
        "playbook_view": {},
        "market_structure_snapshot": {},
        "playbook_quote": {},
        "gamma_snapshot": {},
    }
    observed = {}
    monkeypatch.setattr(
        core, "_market_pulse_refresh_contract", lambda *_args: {"market_phase": "open"}
    )
    monkeypatch.setattr(core, "get_or_build_market_pulse_snapshot", lambda **_kwargs: snapshot)
    monkeypatch.setattr(
        "mccain_capital.services.market_pulse_runtime.ensure_market_pulse_runtime_started",
        lambda: None,
    )

    def evaluate(**kwargs):
        observed.update(kwargs)
        return {"persistence": {"healthy": True}, "recent": []}

    monkeypatch.setattr(core, "_market_pulse_live_setup_monitor", evaluate)

    with app.app_context():
        result = core.run_market_pulse_server_setup_monitor_cycle(now_et=NOW)

    assert result["status"] == "healthy"
    assert observed["claim_delivery"] is False


def test_core_cycle_forces_completed_bar_recovery_when_cached_bars_are_stale(monkeypatch):
    app = Flask(__name__)
    stale = {
        "canonical_freshness": {
            "generation_id": "g1",
            "execution_locked": True,
            "stale_required_components": ["bars"],
        },
        "execution_chart": {"strategy_bars_5m": []},
    }
    recovered = {
        "canonical_freshness": {"generation_id": "g2", "execution_locked": False},
        "execution_chart": {"strategy_bars_5m": [{"ts": "2026-09-08T11:25:00-04:00"}]},
    }
    builds = []
    monkeypatch.setattr(
        core, "_market_pulse_refresh_contract", lambda *_args: {"market_phase": "open"}
    )
    monkeypatch.setattr(
        "mccain_capital.services.market_pulse_runtime.ensure_market_pulse_runtime_started",
        lambda: None,
    )
    monkeypatch.setattr(core, "_market_pulse_snapshot", lambda **_kwargs: {"fresh": True})
    monkeypatch.setattr("mccain_capital.services.gamma_map_service.get_gamma_snapshot", lambda: {})

    def build(**kwargs):
        builds.append(kwargs)
        return stale if len(builds) == 1 else recovered

    monkeypatch.setattr(core, "get_or_build_market_pulse_snapshot", build)
    monkeypatch.setattr(
        core,
        "_market_pulse_live_setup_monitor",
        lambda **_kwargs: {"persistence": {"healthy": True}, "recent": []},
    )

    with app.app_context():
        result = core.run_market_pulse_server_setup_monitor_cycle(now_et=NOW)

    assert result["status"] == "healthy"
    assert result["generation_id"] == "g2"
    assert len(builds) == 2
    assert builds[1]["preloaded_snapshot"] == {"fresh": True}


def test_quote_enrichment_preserves_timestamped_strategy_candles():
    candles = [
        {
            "ts": f"2026-09-08T11:{minute:02d}:00-04:00",
            "v": 7700.0 + minute,
            "open": 7699.0 + minute,
            "high": 7701.0 + minute,
            "low": 7698.0 + minute,
            "close": 7700.0 + minute,
        }
        for minute in range(8)
    ]
    quote = {
        "symbol": "SPX",
        "label": "SPX",
        "data_state": "live",
        "asof_epoch": int(NOW.timestamp()),
        "series": candles,
    }

    enriched = core._market_pulse_enrich_quotes([quote], NOW)[0]

    assert enriched["series"] == candles
    assert enriched["series"][-1]["ts"].startswith("2026-09-08")


def test_cached_minutes_become_only_completed_five_minute_bars():
    points = [
        {
            "ts": f"2026-09-08T11:{minute:02d}:00",
            "open": 7700.0 + minute,
            "high": 7701.0 + minute,
            "low": 7699.0 + minute,
            "close": 7700.5 + minute,
            "v": 7700.5 + minute,
            "volume": 10,
        }
        for minute in range(11)
    ]

    bars = core._market_pulse_completed_five_minute_bars(
        points, datetime(2026, 9, 8, 11, 10, 4, tzinfo=ET)
    )

    assert [bar["ts"] for bar in bars] == [
        "2026-09-08T11:00:00-04:00",
        "2026-09-08T11:05:00-04:00",
    ]
    assert bars[0]["open"] == 7700.0
    assert bars[0]["close"] == 7704.5
    assert bars[0]["high"] == 7705.0
    assert bars[0]["low"] == 7699.0
