from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from mccain_capital.services.market_pulse_scenarios import (
    CONFLUENCE_WEIGHTS,
    confluence_grade,
    normalize_levels,
    rank_market_scenarios,
)


ET = ZoneInfo("America/New_York")


def _bar(clock: str, *, high: float, low: float, close: float, volume: float = 100):
    stamp = datetime.fromisoformat(f"2026-08-11T{clock}:00").replace(tzinfo=ET)
    return {
        "ts": stamp.isoformat(),
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


@pytest.mark.parametrize(
    ("score", "grade"),
    [(100, "A+"), (93, "A"), (85, "A-"), (70, "B-"), (61, "C"), (55, "C-"), (40, "D-"), (30, "F")],
)
def test_confluence_grade_uses_the_same_score(score, grade):
    assert confluence_grade(score) == grade


def test_levels_are_normalized_and_deduplicated_stably():
    levels = normalize_levels(
        [
            {"key": "local_flip", "value": 7750},
            {"key": "call_wall", "value": "7750"},
            {"key": "put_wall", "value": None},
        ]
    )

    assert [(row.key, row.value) for row in levels] == [("local_flip", 7750.0)]


def test_scenario_labels_describe_price_action_instead_of_generic_breaks():
    result = rank_market_scenarios(
        spot=7730,
        levels=[
            {"key": "current_day_low", "value": 7717},
            {"key": "put_wall", "value": 7715},
            {"key": "prior_day_low", "value": 7743},
            {"key": "local_flip", "value": 7749},
        ],
        bars=[],
    )
    labels = {row["family"]: row["family_label"] for row in result["candidates"]}

    assert labels["failed_low"] == "Sweep and Recover Low"
    assert labels["breakdown"] == "Acceptance Below"
    assert labels["breakout"] == "Reclaim Above"
    assert labels["local_flip_loss"] == "Failed Reclaim Below Local Flip"
    assert "Breakout" not in labels.values()
    assert "Breakdown" not in labels.values()


def test_confirmed_local_flip_breakdown_outranks_distant_high():
    levels = [
        {"key": "local_flip", "value": 7750},
        {"key": "current_day_high", "value": 7800},
        {"key": "put_wall", "value": 7725},
    ]
    bars = [
        _bar("09:55", high=7755, low=7745, close=7750),
        _bar("10:00", high=7756, low=7746, close=7749),
        _bar("10:05", high=7755, low=7747, close=7749),
        _bar("10:10", high=7754, low=7740, close=7744),
    ]
    result = rank_market_scenarios(
        spot=7744,
        levels=levels,
        bars=bars,
        gamma_regime="negative_gamma",
    )

    assert result["primary"]["family"] == "local_flip_loss"
    assert result["primary"]["lane"] == "alternative"
    assert result["primary"]["state"] == "trigger_armed"
    assert result["primary"]["grade"] == ""
    assert result["primary"]["strat_pattern"]["code"] == "2-1-2D"
    assert result["primary"]["plan"]["target"] == "Set after direction confirms"
    assert any(row["level"]["key"] == "current_day_high" for row in result["dormant"])


def test_quality_grade_is_published_only_after_later_trigger_break():
    bars = [
        _bar("09:55", high=7755, low=7745, close=7750),
        _bar("10:00", high=7756, low=7746, close=7749),
        _bar("10:05", high=7755, low=7747, close=7749),
        _bar("10:10", high=7754, low=7740, close=7744),
    ]
    levels = [{"key": "local_flip", "value": 7750}, {"key": "put_wall", "value": 7725}]
    armed = rank_market_scenarios(
        spot=7744, levels=levels, bars=bars, gamma_regime="negative_gamma"
    )["primary"]
    triggered = rank_market_scenarios(
        spot=7738,
        levels=levels,
        bars=[*bars, _bar("10:15", high=7745, low=7738, close=7740)],
        gamma_regime="negative_gamma",
    )["primary"]

    assert armed["state"] == "trigger_armed"
    assert armed["quality_score"] is None
    assert armed["grade"] == ""
    assert triggered["state"] == "triggered"
    assert triggered["quality_score"] == triggered["score"]
    assert triggered["grade"]
    assert triggered["trigger_evidence"]["triggered_at"].endswith("10:15:00-04:00")


def test_score_is_fixed_at_one_hundred_and_cannot_unlock_candidate():
    assert sum(CONFLUENCE_WEIGHTS.values()) == 100
    result = rank_market_scenarios(
        spot=7751,
        levels=[{"key": "local_flip", "value": 7750}, {"key": "call_wall", "value": 7780}],
        bars=[],
        gamma_regime="positive_gamma",
    )

    assert result["primary"]["score_max"] == 100
    assert result["primary"]["lane"] != "active_now"
    assert result["primary"]["plan"]["action"] == "No entry until the trigger confirms"


def test_locked_candidates_never_become_active():
    result = rank_market_scenarios(
        spot=7744,
        levels=[{"key": "local_flip", "value": 7750}, {"key": "put_wall", "value": 7725}],
        bars=[
            _bar("10:00", high=7752, low=7744, close=7747),
            _bar("10:05", high=7750, low=7740, close=7744),
        ],
        gamma_regime="negative_gamma",
        locked=True,
    )

    assert result["active"] == []
    assert all(row["state"] == "locked" for row in result["candidates"])


def test_score_components_exclude_vwap_and_still_total_one_hundred():
    result = rank_market_scenarios(
        spot=7751,
        levels=[{"key": "local_flip", "value": 7750}],
        bars=[],
    )

    candidate = result["candidates"][0]
    assert all(component["key"] != "vwap" for component in candidate["score_components"])
    assert sum(component["possible"] for component in candidate["score_components"]) == 100


def test_market_pulse_renders_ranked_lanes_without_vwap_contract(client):
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)

    for element_id in (
        "marketPulseScenarioLanes",
        "marketPulseAlternativeTitle",
        "marketPulseDormantScenarios",
    ):
        assert f'id="{element_id}"' in body
    assert '"scenario_rankings"' in body
    assert '"primary_scenario"' in body
    assert '"vwap"' not in body.lower()
    assert "canonicalScenarioPlan" in body
    assert "Confluence Grade" in body
    assert "Grade and score measure confluence—not permission" in body
    assert "marketPulseVwap" not in body
    assert "data-vwap-coverage" not in body

    css = (Path(__file__).resolve().parents[1] / "static/css/market_pulse.css").read_text(
        encoding="utf-8"
    )
    scenario_rule = css.split("body.page-market-pulse .marketPulseScenarioLanes{", 1)[1].split(
        "}", 1
    )[0]
    assert "grid-column:1 / -1" in scenario_rule
    assert "width:100%" in scenario_rule


def test_chart_excludes_vwap_and_contains_boundary_annotations():
    chart = (Path(__file__).resolve().parents[1] / "static/js/spx_hero_chart.js").read_text(
        encoding="utf-8"
    )
    assert "vwap" not in chart.lower()
    assert "const priceScaleWidth = 112" in chart
    assert "const DEFAULT_RIGHT_OFFSET_BARS = 6" in chart
    assert 'nearLeftEdge ? "0%" : nearRightEdge ? "100%" : "50%"' in chart
    assert '"current-day low": "CDL"' in chart
    assert '"local flip": "LF"' in chart
    assert 'overlay.roles.join("/")' in chart
    assert "distance.toFixed(1)" in chart
