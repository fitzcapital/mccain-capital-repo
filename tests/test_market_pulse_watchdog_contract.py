from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "mccain_capital/templates/core/market_pulse.html"
CSS = ROOT / "static/css/market_pulse.css"
GAMMA_CONTEXT = ROOT / "static/js/market_pulse_gamma_context.js"
HERO_CHART = ROOT / "static/js/spx_hero_chart.js"


def test_watchdog_is_server_owned_and_part_of_atomic_commit():
    body = TEMPLATE.read_text(encoding="utf-8")
    assert 'id="marketPulseOperationalHealth"' in body
    assert "'operational_health': ops_health" in body
    commit = body[body.index("const applyCanonicalMarketPulsePayload") :]
    assert "renderOperationalHealth(staged.payload.operational_health || {})" in commit
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
    chart = HERO_CHART.read_text(encoding="utf-8")
    gamma_context = GAMMA_CONTEXT.read_text(encoding="utf-8")

    assert "{% set market_closed = not market_hours %}" in body
    assert "execution_chart.mode != 'live_session'" not in body
    assert "const deriveSessionPhase" in gamma_context
    assert 'sessionPhase === "open" ? "live" : "replay"' in gamma_context
    assert 'String(refreshContract.market_phase || "").toLowerCase() === "open"' in body
