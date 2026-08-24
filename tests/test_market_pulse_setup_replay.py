from datetime import datetime
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from mccain_capital.services.market_pulse_setup_replay import build_intraday_setup_replay


ET = ZoneInfo("America/New_York")


def _bar(clock: str, *, open_: float, high: float, low: float, close: float):
    stamp = datetime.fromisoformat(f"2026-08-19T{clock}:00").replace(tzinfo=ET)
    return {
        "ts": stamp.isoformat(),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
    }


def _replay(
    bars,
    *,
    gamma_as_of="2026-08-19T09:30:00-04:00",
    include_rejected=False,
    levels=None,
):
    replay_levels = levels or [
        {"key": "local_flip", "label": "Local Flip", "value": 100},
        {"key": "put_wall", "label": "Put Wall", "value": 95},
        {"key": "call_wall", "label": "Call Wall", "value": 105},
    ]
    replay_levels = [
        {
            **row,
            **(
                {"as_of": gamma_as_of}
                if row.get("key")
                in {
                    "gamma_flip",
                    "local_flip",
                    "call_wall",
                    "put_wall",
                    "new_call_wall",
                    "new_put_wall",
                }
                else {}
            ),
        }
        for row in replay_levels
    ]
    return build_intraday_setup_replay(
        ticker="SPX",
        session_date="2026-08-19",
        bars=bars,
        levels=replay_levels,
        gamma_regime="negative_gamma",
        gamma_as_of=gamma_as_of,
        include_rejected=include_rejected,
    )


def test_replay_qualifies_first_point_in_time_confirmation_and_later_outcome():
    result = _replay(
        [
            _bar("09:30", open_=100, high=101, low=99, close=100),
            _bar("09:35", open_=100, high=102, low=99.5, close=99.8),
            _bar("09:40", open_=99.8, high=101.5, low=99.6, close=99.7),
            _bar("09:45", open_=99.7, high=100.5, low=98, close=99),
            _bar("09:50", open_=99, high=99, low=94, close=95),
        ]
    )

    setup = next(row for row in result["setups"] if row["strat_pattern"]["code"] == "2-1-2D")
    assert setup["signal_time"].endswith("09:50:00-04:00")
    assert setup["target"]["value"] == 95
    assert setup["outcome"]["state"] == "open"
    assert setup["strat_pattern"]["code"] == "2-1-2D"
    assert setup["data_availability"]["gamma_available_at_signal"] is True
    assert result["read_only"] is True


def test_future_bars_change_outcome_without_changing_signal_eligibility():
    signal_bars = [
        _bar("09:30", open_=100, high=101, low=99, close=100),
        _bar("09:35", open_=100, high=102, low=99.5, close=99.8),
        _bar("09:40", open_=99.8, high=101.5, low=99.6, close=99.7),
        _bar("09:45", open_=99.7, high=100.5, low=98, close=99),
    ]
    before = _replay(signal_bars)
    after = _replay(signal_bars + [_bar("09:50", open_=98.5, high=99, low=94, close=95)])
    assert not [row for row in before["setups"] if row["strat_pattern"]["code"] == "2-1-2D"]
    after_setup = next(
        row for row in after["setups"] if row["strat_pattern"]["code"] == "2-1-2D"
    )

    assert after_setup["signal_time"].endswith("09:50:00-04:00")
    assert after_setup["outcome"]["state"] == "open"


def test_replay_keeps_cdh_creation_pattern_when_live_retest_gate_is_incomplete():
    result = _replay(
        [
            _bar("11:45", open_=99, high=100, low=96, close=98),
            _bar("11:50", open_=98, high=105, low=97, close=104),
            _bar("11:55", open_=104, high=104.5, low=98, close=100),
            _bar("12:00", open_=100, high=101, low=95, close=96),
        ],
        levels=[
            {"key": "current_day_high", "label": "Current-Day High", "value": 105},
            {"key": "put_wall", "label": "Put Wall", "value": 95},
        ],
    )

    assert not [row for row in result["setups"] if row["strat_pattern"]["code"] == "2-1-2D"]


def test_replay_rejects_random_212_that_only_intersects_a_wall():
    result = _replay(
        [
            _bar("11:45", open_=100, high=101, low=99, close=100),
            _bar("11:50", open_=100, high=106, low=99, close=105),
            _bar("11:55", open_=105, high=105.5, low=101, close=103),
            _bar("12:00", open_=103, high=104, low=98, close=99),
            _bar("12:05", open_=99, high=100, low=96, close=97),
        ],
        levels=[
            {"key": "call_wall", "label": "Call Wall", "value": 106},
            {"key": "put_wall", "label": "Put Wall", "value": 95},
        ],
        include_rejected=True,
    )

    assert not any(
        row["strat_pattern"]["code"] == "2-1-2D" and row["level"]["key"] == "call_wall"
        for row in result["setups"]
    )
    assert any("No CDH/CDL creation or ordered liquidity sweep" in row["reason"] for row in result["rejected"])


def test_replay_accepts_212_after_ordered_liquidity_sweep_inside_pattern():
    result = _replay(
        [
            _bar("11:45", open_=99, high=100, low=96, close=98),
            _bar("11:50", open_=98, high=106, low=97, close=104),
            _bar("11:55", open_=104, high=104.5, low=101, close=103),
            _bar("12:00", open_=103, high=104, low=98, close=99),
            _bar("12:05", open_=99, high=100, low=96, close=97),
        ],
        levels=[
            {"key": "call_wall", "label": "Call Wall", "value": 105},
            {"key": "put_wall", "label": "Put Wall", "value": 95},
        ],
    )

    setup = next(
        row
        for row in result["setups"]
        if row["strat_pattern"]["code"] == "2-1-2D" and row["level"]["key"] == "call_wall"
    )
    assert setup["location_event"] == "liquidity_swept"
    assert setup["family"] == "failed_high"
    assert len(
        [
            row
            for row in result["setups"]
            if row["strat_pattern"]["code"] == "2-1-2D"
            and row["level"]["key"] == "call_wall"
        ]
    ) == 1


def test_replay_does_not_project_dynamic_level_before_observation_time():
    result = _replay(
        [
            _bar("11:45", open_=99, high=100, low=96, close=98),
            _bar("11:50", open_=98, high=106, low=97, close=104),
            _bar("11:55", open_=104, high=104.5, low=101, close=103),
            _bar("12:00", open_=103, high=104, low=98, close=99),
            _bar("12:05", open_=99, high=100, low=96, close=97),
        ],
        gamma_as_of="2026-08-19T13:00:00-04:00",
        levels=[{"key": "call_wall", "label": "Call Wall", "value": 105}],
    )

    assert result["setups"] == []


def test_later_high_does_not_erase_earlier_current_day_high_212_down():
    result = _replay(
        [
            _bar("09:30", open_=100, high=101, low=99, close=100),
            _bar("09:35", open_=100, high=103, low=99.5, close=102),
            _bar("09:40", open_=102, high=103, low=100, close=102),
            _bar("09:45", open_=102, high=103, low=98, close=99),
            _bar("09:50", open_=99, high=110, low=98.5, close=109),
            _bar("09:55", open_=109, high=109.5, low=97, close=98),
        ],
        levels=[
            {"key": "current_day_high", "label": "Current-Day High", "value": 110},
            {"key": "put_wall", "label": "Put Wall", "value": 95},
        ],
    )

    setup = next(
        row
        for row in result["setups"]
        if row["strat_pattern"]["code"] == "2-1-2D"
        and row["level"]["key"] == "current_day_high"
    )
    assert setup["signal_time"].endswith("09:55:00-04:00")
    assert setup["level"]["value"] == 103
    assert setup["target"]["value"] == 95


def test_gamma_observation_after_signal_is_not_used_retroactively():
    result = _replay(
        [
            _bar("09:30", open_=100, high=101, low=99, close=100),
            _bar("09:35", open_=100, high=102, low=99.5, close=99.8),
            _bar("09:40", open_=99.8, high=101.5, low=99.6, close=99.7),
            _bar("09:45", open_=99.7, high=100.5, low=98, close=99),
            _bar("09:50", open_=99, high=99.5, low=96, close=97),
        ],
        gamma_as_of="2026-08-19T10:00:00-04:00",
        levels=[
            {"key": "prior_day_high", "label": "Prior-Day High", "value": 100},
            {"key": "prior_day_low", "label": "Prior-Day Low", "value": 95},
        ],
    )

    setup = next(row for row in result["setups"] if row["strat_pattern"]["code"] == "2-1-2D")
    assert setup["data_availability"]["gamma_available_at_signal"] is False


def test_replay_endpoint_and_template_are_read_only_and_discoverable(client):
    response = client.get("/api/market-pulse/setup-replay?ticker=SPX")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["payload"]["read_only"] is True

    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)
    assert 'id="marketPulseSetupReplay"' in body
    assert 'id="marketPulseReplayList"' in body
    assert '"setup_replay_api_url"' in body
    assert "Known at signal" in body
    assert "What happened next" in body

    chart = (Path(__file__).resolve().parents[1] / "static/js/spx_hero_chart.js").read_text(
        encoding="utf-8"
    )
    assert "market-pulse-setup-replay-updated" in chart
    assert "setupReplayMarkers" in chart


def test_replay_is_full_width_collapsible_and_ranks_signal_quality(client):
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)
    replay_start = body.index('id="marketPulseSetupReplay"')
    scenario_end = body.index("</section>", replay_start)

    assert replay_start < scenario_end
    assert 'class="marketPulseSetupReplayBody"' in body
    assert "Highest signal quality first" in body
    assert "Number(right.score || 0) - Number(left.score || 0)" in body
    assert "REPLAY_REQUEST_TIMEOUT_MS = 10000" in body
    assert "window.setTimeout(loadSetupReplay, 250)" in body
    assert "Retry replay" in body
    assert "signal: controller.signal" in body

    styles = (Path(__file__).resolve().parents[1] / "static/css/market_pulse.css").read_text(
        encoding="utf-8"
    )
    assert "grid-column:1 / -1" in styles
    assert "max-height:min(52vh,560px)" in styles
    assert "overflow:auto" in styles
    assert ".marketPulseReplayUnavailable" in styles


def test_replay_results_are_sorted_best_to_least_then_by_signal_time():
    result = _replay(
        [
            _bar("09:35", open_=101, high=103, low=99, close=102),
            _bar("09:40", open_=102, high=104, low=98, close=99),
            _bar("09:45", open_=99, high=101, low=96, close=97),
            _bar("09:50", open_=97, high=100, low=94, close=95),
            _bar("09:55", open_=95, high=98, low=93, close=96),
        ]
    )

    scores = [int(row["score"]) for row in result["setups"]]
    assert scores == sorted(scores, reverse=True)


def test_replay_excludes_entry_confirmations_after_330_pm():
    qualifying = [
        _bar("15:10", open_=100, high=101, low=99, close=100),
        _bar("15:15", open_=100, high=103, low=99.5, close=102),
        _bar("15:20", open_=102, high=102.5, low=100, close=101),
        _bar("15:25", open_=101, high=101.5, low=98, close=99),
        _bar("15:30", open_=99, high=99.5, low=97, close=98),
    ]
    after_cutoff = [
        *qualifying[:-1],
        _bar("15:35", open_=99, high=99.5, low=97, close=98),
    ]

    allowed = _replay(qualifying)
    excluded = _replay(after_cutoff)

    assert allowed["entry_cutoff_label"] == "3:30 PM ET"
    assert all(
        datetime.fromisoformat(row["signal_time"]).astimezone(ET).time()
        <= datetime.strptime("15:30", "%H:%M").time()
        for row in allowed["setups"]
    )
    assert excluded["setup_count"] == 0


def test_market_pulse_header_uses_completed_candle_and_compact_controls(client):
    root = Path(__file__).resolve().parents[1]
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)
    styles = (root / "static/css/market_pulse.css").read_text(encoding="utf-8")
    gamma_context = (root / "static/js/market_pulse_gamma_context.js").read_text(
        encoding="utf-8"
    )
    chart = (root / "static/js/spx_hero_chart.js").read_text(encoding="utf-8")

    assert "Last candle ·" in body
    assert "last_completed_candle_time_label" in gamma_context
    assert "Last candle · ${snapshotLabel}" in chart
    assert body.count('id="marketPulseRefreshCountdown"') == 1
    assert body.count("marketPulseUtilityIconButton") == 3
    actions_start = body.index("marketPulsePlaybookActions marketPulseCockpitUtility")
    countdown_start = body.index("marketPulsePollCountdown", actions_start)
    sticky_start = body.index("marketPulsePlaybookPinToggle", actions_start)
    assert actions_start < countdown_start < sticky_start
    assert "marketPulseRefreshSpin" in styles
    assert "Entries after 3:30 PM ET excluded" in body


def test_context_refresh_uses_single_flight_and_returns_cached_generation(client):
    from mccain_capital.services import core

    core._market_pulse_context_response_cache.clear()
    first = client.get("/api/market-pulse/context?ticker=SPX")
    assert first.status_code == 200
    assert first.get_json()["refresh_outcome"]["status"] in {"promoted", "partial"}

    assert core._market_pulse_context_refresh_lock.acquire(blocking=False)
    try:
        second = client.get("/api/market-pulse/context?ticker=SPX")
    finally:
        core._market_pulse_context_refresh_lock.release()

    assert second.status_code == 200
    payload = second.get_json()
    assert payload["refresh_outcome"]["status"] == "unchanged"
    assert payload["payload"]["ticker"] == "SPX"


def test_context_refresh_returns_not_modified_without_provider_work(client, monkeypatch):
    from mccain_capital.services import core

    core._market_pulse_context_response_cache.clear()
    first = client.get("/api/market-pulse/context?ticker=SPX")
    assert first.status_code == 200
    generation = first.get_json()["payload"]["canonical_freshness"]["generation_id"]

    monkeypatch.setattr(
        core,
        "_market_pulse_context_api_impl",
        lambda: (_ for _ in ()).throw(AssertionError("automatic check forced provider work")),
    )
    current = client.get(
        "/api/market-pulse/context?ticker=SPX",
        headers={"If-None-Match": f'"{generation}"'},
    )

    assert current.status_code == 304
    assert current.headers["X-Market-Pulse-Outcome"] == "unchanged"
    assert int(current.headers["X-Market-Pulse-Next-Check"]) > 0


def test_after_hours_automatic_refresh_returns_retained_generation_without_provider_work(
    client, monkeypatch
):
    from mccain_capital import runtime as app_runtime
    from mccain_capital.services import core

    closed_at = datetime(2026, 8, 20, 20, 0, tzinfo=ET)
    monkeypatch.setattr(app_runtime, "now_et", lambda: closed_at)
    core._market_pulse_context_response_cache.clear()
    first = client.get("/api/market-pulse/context?ticker=SPX")
    assert first.status_code == 200
    generation = first.get_json()["payload"]["canonical_freshness"]["generation_id"]

    monkeypatch.setattr(
        core,
        "_market_pulse_context_api_impl",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("after-hours automatic check reached provider refresh")
        ),
    )
    retained = client.get(
        f"/api/market-pulse/context?ticker=SPX&automatic=1&generation={generation}"
    )

    assert retained.status_code == 200
    body = retained.get_json()
    assert retained.headers["X-Market-Pulse-Outcome"] == "session_paused"
    assert retained.headers["X-Market-Pulse-Next-Check"] == "0"
    assert body["payload"]["refresh_contract"] == {
        "automatic_refresh_enabled": False,
        "canonical_interval_seconds": 15,
        "market_phase": "closed",
        "mode": "session_paused",
        "next_check_seconds": 0,
        "next_session_open_at": "2026-08-21T09:30:00-04:00",
    }


def test_market_pulse_refresh_contract_enables_only_regular_session():
    from mccain_capital.services import core

    live = core._market_pulse_refresh_contract(
        datetime(2026, 8, 20, 11, 0, tzinfo=ET), {"sync_interval_seconds": 15}
    )
    closed = core._market_pulse_refresh_contract(
        datetime(2026, 8, 21, 20, 0, tzinfo=ET), {"sync_interval_seconds": 15}
    )

    assert live["automatic_refresh_enabled"] is True
    assert live["next_check_seconds"] == 15
    assert closed["automatic_refresh_enabled"] is False
    assert closed["next_check_seconds"] == 0
    assert closed["next_session_open_at"] == "2026-08-24T09:30:00-04:00"


def test_after_hours_automatic_refresh_without_cache_still_avoids_provider_work(
    client, monkeypatch
):
    from mccain_capital import runtime as app_runtime
    from mccain_capital.services import core

    monkeypatch.setattr(
        app_runtime,
        "now_et",
        lambda: datetime(2026, 8, 20, 20, 0, tzinfo=ET),
    )
    core._market_pulse_context_response_cache.clear()
    monkeypatch.setattr(
        core,
        "_market_pulse_context_api_impl",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("closed automatic request reached provider refresh")
        ),
    )

    response = client.get("/api/market-pulse/context?ticker=SPX&automatic=1")

    assert response.status_code == 200
    assert response.headers["X-Market-Pulse-Outcome"] == "session_paused"
    assert response.get_json()["payload"]["refresh_contract"]["automatic_refresh_enabled"] is False


def test_after_hours_manual_refresh_remains_one_shot(client, monkeypatch):
    from mccain_capital import runtime as app_runtime
    from mccain_capital.services import core

    monkeypatch.setattr(
        app_runtime,
        "now_et",
        lambda: datetime(2026, 8, 20, 20, 0, tzinfo=ET),
    )
    core._market_pulse_context_response_cache.clear()
    calls = []

    def fake_impl(**_kwargs):
        calls.append(True)
        return core.jsonify(
            {
                "ok": True,
                "refresh_outcome": {"status": "unchanged"},
                "payload": {
                    "ticker": "SPX",
                    "refresh_contract": core._market_pulse_refresh_contract(app_runtime.now_et()),
                },
            }
        )

    monkeypatch.setattr(core, "_market_pulse_context_api_impl", fake_impl)
    response = client.get("/api/market-pulse/context?ticker=SPX&refresh=1")

    assert response.status_code == 200
    assert calls == [True]
    assert response.get_json()["payload"]["refresh_contract"]["automatic_refresh_enabled"] is False


def test_market_pulse_stream_client_respects_refresh_contract():
    script = (
        Path(__file__).resolve().parents[1] / "static/js/market_pulse_gamma_context.js"
    ).read_text(encoding="utf-8")

    assert "automaticRefreshAllowed" in script
    assert "if (!automaticRefreshAllowed())" in script
    assert "Live stream paused · last valid session retained" in script


def test_context_refresh_starts_bounded_recovery_when_locked_generation_is_overdue(
    client, monkeypatch
):
    from mccain_capital import runtime as app_runtime
    from mccain_capital.services import core

    live_at = datetime(2026, 8, 20, 11, 29, tzinfo=ET)
    monkeypatch.setattr(app_runtime, "now_et", lambda: live_at)
    core._market_pulse_context_response_cache.clear()
    core._market_pulse_context_recovery_attempts.clear()
    first = client.get("/api/market-pulse/context?ticker=SPX")
    assert first.status_code == 200
    cached = core._market_pulse_context_response_cache["SPX"]
    freshness = cached["canonical_freshness"]
    generation = freshness["generation_id"]
    freshness["generated_at"] = (app_runtime.now_et() - timedelta(minutes=20)).isoformat()
    freshness["execution_locked"] = True
    freshness["stale_required_components"] = ["gamma"]
    calls = []

    def recover(**kwargs):
        calls.append(kwargs)
        return core.jsonify(
            {
                "ok": True,
                "refresh_outcome": {"status": "partial"},
                "payload": cached,
            }
        )

    monkeypatch.setattr(core, "_market_pulse_context_api_impl", recover)
    response = client.get(
        f"/api/market-pulse/context?ticker=SPX&automatic=1&generation={generation}",
        headers={"If-None-Match": f'"{generation}"'},
    )

    assert response.status_code == 200
    assert calls == [{"recover_stale": True}]


def test_context_refresh_uses_short_retry_during_automatic_recovery_cooldown(
    client, monkeypatch
):
    from mccain_capital import runtime as app_runtime
    from mccain_capital.services import core

    live_at = datetime(2026, 8, 20, 11, 29, tzinfo=ET)
    monkeypatch.setattr(app_runtime, "now_et", lambda: live_at)
    core._market_pulse_context_response_cache.clear()
    core._market_pulse_context_recovery_attempts.clear()
    first = client.get("/api/market-pulse/context?ticker=SPX")
    cached = core._market_pulse_context_response_cache["SPX"]
    freshness = cached["canonical_freshness"]
    generation = freshness["generation_id"]
    freshness["generated_at"] = (live_at - timedelta(minutes=20)).isoformat()
    freshness["execution_locked"] = True
    freshness["stale_required_components"] = ["gamma"]
    core._market_pulse_context_recovery_attempts["SPX"] = live_at
    monkeypatch.setattr(
        core,
        "_market_pulse_context_api_impl",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("cooldown poll repeated provider recovery")
        ),
    )

    response = client.get(
        f"/api/market-pulse/context?ticker=SPX&automatic=1&generation={generation}"
    )

    assert first.status_code == 200
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["refresh_outcome"]["retry_classification"] == (
        "automatic_recovery_cooldown"
    )
    assert payload["refresh_outcome"]["next_retry_seconds"] == 15


def test_setup_replay_prefers_canonical_context_when_playbook_snapshot_has_no_bars(
    client, monkeypatch
):
    from mccain_capital.services import core

    bars = [
        _bar("09:30", open_=100, high=101, low=99, close=100),
        _bar("09:35", open_=100, high=102, low=99.5, close=99.8),
        _bar("09:40", open_=99.8, high=101.5, low=99.6, close=99.7),
        _bar("09:45", open_=99.7, high=100.5, low=98, close=99),
        _bar("09:50", open_=99, high=99, low=94, close=95),
    ]
    canonical = {
        "ticker": "SPX",
        "canonical_freshness": {
            "generation_id": "context-with-bars",
            "symbol": "SPX",
            "session_id": "2026-08-19",
            "generated_at": "2026-08-19T10:00:00-04:00",
            "components": {
                name: {"as_of": "2026-08-19T10:00:00-04:00"}
                for name in ("spot", "bars", "gamma")
            },
        },
        "market_structure_snapshot": {
            "local_flip": 100,
            "put_wall": 95,
            "call_wall": 105,
            "gamma_regime": "negative_gamma",
        },
        "playbook_quote": {"day_high": 102, "day_low": 94},
        "gamma_snapshot": {"computed_at": "2026-08-19T09:30:00-04:00"},
        "execution_chart": {"strategy_bars_5m": bars},
    }
    empty_snapshot = dict(canonical)
    empty_snapshot["execution_chart"] = {"strategy_bars_5m": []}
    core._market_pulse_context_response_cache.clear()
    core._market_pulse_context_response_cache["SPX"] = canonical
    monkeypatch.setattr(
        core,
        "_market_pulse_cached_playbook_snapshot",
        lambda *_args, **_kwargs: empty_snapshot,
    )

    response = client.get(
        "/api/market-pulse/setup-replay?ticker=SPX&session_date=2026-08-19"
    )

    assert response.status_code == 200
    payload = response.get_json()["payload"]
    assert payload["bar_count"] == len(bars)
    assert any(row["strat_pattern"]["code"] == "2-1-2D" for row in payload["setups"])
    core._market_pulse_context_response_cache.clear()


def test_cached_context_current_rejects_live_generation_past_refresh_window():
    from mccain_capital.services import core

    now = datetime(2026, 8, 20, 11, 29, tzinfo=ET)
    freshness = {
        "generated_at": (now - timedelta(minutes=20)).isoformat(),
        "sync_interval_seconds": 15,
        "execution_locked": False,
        "stale_required_components": [],
    }

    assert core._market_pulse_cached_context_is_current(freshness, now) is False


def test_context_refresh_promotes_newer_cached_generation_without_provider_work(
    client, monkeypatch
):
    from mccain_capital.services import core

    core._market_pulse_context_response_cache.clear()
    first = client.get("/api/market-pulse/context?ticker=SPX")
    assert first.status_code == 200
    current_generation = first.get_json()["payload"]["canonical_freshness"]["generation_id"]

    monkeypatch.setattr(
        core,
        "_market_pulse_context_api_impl",
        lambda: (_ for _ in ()).throw(AssertionError("cached promotion forced provider work")),
    )
    stale_tab = client.get(
        "/api/market-pulse/context?ticker=SPX&generation=older-generation",
        headers={"If-None-Match": '"older-generation"'},
    )

    assert stale_tab.status_code == 200
    payload = stale_tab.get_json()
    assert payload["refresh_outcome"]["status"] == "promoted"
    assert payload["payload"]["canonical_freshness"]["generation_id"] == current_generation


def test_requested_context_refresh_rebuilds_instead_of_reusing_cached_playbook(client, monkeypatch):
    from mccain_capital.services import core

    core._market_pulse_context_response_cache.clear()
    cached = {
        "ticker": "SPX",
        "market_structure_snapshot": {
            "spot": 100.0,
            "main_flip": 110.0,
            "local_flip": 100.0,
            "call_wall": 105.0,
            "put_wall": 95.0,
        },
        "playbook_quote": {"price": 100.0},
        "canonical_freshness": {"generation_id": "cached"},
    }
    rebuilt = dict(cached)
    rebuilt["server_ts"] = "rebuilt-on-request"
    calls = []

    monkeypatch.setattr(
        core,
        "_market_pulse_cached_playbook_snapshot",
        lambda *_args, **_kwargs: cached,
    )

    def rebuild(**kwargs):
        calls.append(kwargs)
        return rebuilt

    monkeypatch.setattr(core, "get_or_build_market_pulse_snapshot", rebuild)
    response = client.get("/api/market-pulse/context?ticker=SPX&refresh=1")

    assert response.status_code == 200
    assert len(calls) == 1
    assert calls[0]["force_refresh"] is True


def test_market_pulse_refresh_coordinator_and_countdown_contract(client):
    root = Path(__file__).resolve().parents[1]
    coordinator = (root / "static/js/market_pulse_refresh_coordinator.js").read_text(
        encoding="utf-8"
    )
    chart = (root / "static/js/spx_hero_chart.js").read_text(encoding="utf-8")
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)

    assert "marketPulseRefreshCoordinator" in coordinator
    assert "const runDue" in coordinator
    assert "lane.inFlight" in coordinator
    assert "market_pulse_refresh_coordinator.js" in body
    assert 'id="marketPulseRefreshCountdown"' in body
    assert "`Retry ${seconds}s`" in body
    assert "`${seconds}s`" in body
    assert 'nextCanonicalCheckMode === "retry" ? "Retry" : "Refresh"' in body
    assert "Math.min(30000, Number(delaySeconds || 15) * 1000)" in body
    assert "armCanonicalRefreshWatchdog" in body
    assert "deadline < now - 2000" in body
    assert "deadline > now + 30000" in body
    assert "market-pulse-component-updated" in chart
    assert "refreshCoordinator.register(refreshLaneNames.bars" in chart
    assert "refreshCoordinator.register(refreshLaneNames.levels" in chart
    assert "refreshCoordinator.register(refreshLaneNames.quote" in chart


def test_setup_replay_marker_and_legend_are_historical_not_live(client):
    root = Path(__file__).resolve().parents[1]
    chart = (root / "static/js/spx_hero_chart.js").read_text(encoding="utf-8")
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)

    assert 'text: "◇"' in chart
    assert 'text: "S"' not in chart
    assert "Replay setup · historical, not a live entry" in body


def test_tape_endpoint_does_not_create_per_request_provider_pool():
    source = (Path(__file__).resolve().parents[1] / "mccain_capital/services/core.py").read_text(
        encoding="utf-8"
    )
    assert "ThreadPoolExecutor" not in source
    assert "_market_pulse_tape_refresh_lock" in source
