from datetime import datetime
from pathlib import Path

from mccain_capital.services.core import (
    _market_pulse_cached_context_is_current,
    _market_pulse_chart_source_viewmodel,
)
from mccain_capital.services.tradier_hero_chart_service import get_hero_levels


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "mccain_capital/templates/core/market_pulse.html"
CSS = ROOT / "static/css/market_pulse.css"
GAMMA_CONTEXT = ROOT / "static/js/market_pulse_gamma_context.js"


def test_watchdog_is_server_owned_and_part_of_atomic_commit():
    body = TEMPLATE.read_text(encoding="utf-8")
    assert 'id="marketPulseOperationalHealth"' in body
    assert "'operational_health': ops_health" in body
    commit = body[body.index("const applyCanonicalMarketPulsePayload") :]
    assert "renderOperationalHealth(staged.payload.operational_health || {})" in commit
    assert 'id="marketPulseOperationalSetupCoverage"' in body
    assert "health.server_setup_monitor || {}" in body
    assert "applyCanonicalDiagnostics(staged.payload)" in commit


def test_watchdog_has_accessible_native_disclosure_and_responsive_contract():
    body = TEMPLATE.read_text(encoding="utf-8")
    css = CSS.read_text(encoding="utf-8")
    assert '<details class="marketPulseWatchdog' in body
    assert '<summary>' in body
    assert 'class="marketPulseWatchdogExpand"' in body
    assert "watchdog.open = true" not in body
    assert ".marketPulseWatchdogDetails" in css
    assert "@media (max-width:800px)" in css


def test_header_timestamp_always_describes_the_last_completed_candle():
    body = TEMPLATE.read_text(encoding="utf-8")
    gamma_context = GAMMA_CONTEXT.read_text(encoding="utf-8")

    assert "const headerSnapshotPresentation" in gamma_context
    assert 'label: "Last candle"' in gamma_context
    assert "structure.last_completed_candle_time_label" in gamma_context
    patch_spot = gamma_context[
        gamma_context.index("const patchHeaderLiveSpot") : gamma_context.index(
            "const renderSimpleBadges"
        )
    ]
    assert "headerSnapshotPresentation(current, asOf)" in patch_spot
    assert "`Live · ${formatEtLabel" not in patch_spot

    canonical_commit = body[
        body.index("const applyCanonicalMarketPulsePayload") : body.index(
            "const refreshMarketPulseContext"
        )
    ]
    assert 'setCanonicalText("marketPulseHeaderSnapshot", `Last candle · ${updatedAt}`)' in canonical_commit
    assert ': "Planning"' in canonical_commit
    assert '.replace(/^(Live|Last valid)\\s*·\\s*/i, "Last candle · ")' in body


def test_initial_session_label_uses_current_market_hours_not_cached_chart_mode():
    body = TEMPLATE.read_text(encoding="utf-8")
    gamma_context = GAMMA_CONTEXT.read_text(encoding="utf-8")

    assert "{% set market_closed = not market_hours %}" in body
    assert "execution_chart.mode != 'live_session'" not in body
    assert "const deriveSessionPhase" in gamma_context
    assert 'sessionPhase === "open" ? "live" : "replay"' in gamma_context
    assert 'String(refreshContract.market_phase || "").toLowerCase() === "open"' in body
    assert "Live execution" not in body
    assert 'staged.actionCode === "LOCKED" ? "Locked" : "Market open"' in body
    assert '"Waiting on Gamma"' in body
    assert '"Synchronizing data"' in body


def test_header_timestamp_has_canonical_completed_bar_fallbacks():
    body = TEMPLATE.read_text(encoding="utf-8")
    canonical_commit = body[
        body.index("const applyCanonicalMarketPulsePayload") : body.index(
            "const refreshMarketPulseContext"
        )
    ]

    assert "structure.last_completed_candle_time_label" in canonical_commit
    assert "structure.last_completed_candle_time" in canonical_commit
    assert "structure.bars_as_of" in canonical_commit


def test_partial_refresh_advances_safe_completed_candle_label():
    body = TEMPLATE.read_text(encoding="utf-8")

    assert 'if (outcomeStatus === "partial")' in body
    assert "const partialStructure = data.payload.market_structure_snapshot || {};" in body
    assert "partialStructure.last_completed_candle_time" in body
    assert "`Last candle · ${partialCompletedCandleLabel}`" in body


def test_chart_metadata_uses_completed_strategy_bar_when_display_points_are_empty():
    completed_at = "2026-08-25T13:55:00-04:00"

    result = _market_pulse_chart_source_viewmodel(
        execution_chart={
            "mode": "live_session",
            "points": [],
            "strategy_bars_5m": [{"ts": completed_at, "close": 7667.25}],
        },
        session_mode="regular",
    )

    assert result["bars_as_of"] == completed_at
    assert result["chart_state"] == "live_session"


def test_levels_poll_preserves_canonical_completed_candle_timestamp():
    completed_at = "2026-08-25T13:55:00-04:00"
    result = get_hero_levels(
        symbol="SPX",
        playbook_snapshot={
            "market_structure_snapshot": {
                "last_completed_candle_time": completed_at,
                "last_completed_candle_time_label": "Aug 25, 2026 01:55:00 PM ET",
            }
        },
    )

    assert result["last_completed_candle_time"] == completed_at
    assert result["last_completed_candle_time_label"] == "Aug 25, 2026 01:55:00 PM ET"


def test_automatic_poll_rejects_cache_after_a_new_five_minute_bar_completes():
    now_et = datetime.fromisoformat("2026-08-25T14:10:10-04:00")
    freshness = {
        "generated_at": "2026-08-25T14:10:05-04:00",
        "sync_interval_seconds": 15,
        "execution_locked": False,
        "stale_required_components": [],
    }
    stale = {
        "market_structure_snapshot": {
            "last_completed_candle_time": "2026-08-25T14:00:00-04:00"
        }
    }
    current = {
        "market_structure_snapshot": {
            "last_completed_candle_time": "2026-08-25T14:05:00-04:00"
        }
    }

    assert not _market_pulse_cached_context_is_current(freshness, now_et, stale)
    assert _market_pulse_cached_context_is_current(freshness, now_et, current)


def test_automatic_poll_allows_brief_provider_grace_at_candle_boundary():
    now_et = datetime.fromisoformat("2026-08-25T14:10:04-04:00")
    freshness = {
        "generated_at": "2026-08-25T14:10:02-04:00",
        "sync_interval_seconds": 15,
        "execution_locked": False,
        "stale_required_components": [],
    }
    context = {
        "market_structure_snapshot": {
            "last_completed_candle_time": "2026-08-25T14:00:00-04:00"
        }
    }

    assert _market_pulse_cached_context_is_current(freshness, now_et, context)
