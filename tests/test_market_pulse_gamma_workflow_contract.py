import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def test_market_pulse_uses_gamma_first_workflow(client):
    response = client.get("/market-pulse")
    assert response.status_code == 200
    body = response.get_data(as_text=True)

    ordered_anchors = [
        'id="marketPulseStatusBar"',
        'id="marketPulseGammaCockpit"',
        'id="marketPulseGammaLevelDeck"',
        'id="marketPulseStructureMapPrimary"',
        'id="marketPulseTradeReadCard"',
        'id="marketPulseEntryChecklist"',
        'id="marketPulseGammaLadderCard"',
        'id="marketPulseCoreTape"',
    ]
    positions = [body.index(anchor) for anchor in ordered_anchors]
    assert positions == sorted(positions)
    assert body.count('id="marketPulseStatusBar"') == 1
    assert body.count('id="marketPulseTradeReadCard"') == 1


def test_spx_playbook_renders_failed_sweep_decision_and_six_step_checklist(client):
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)

    assert "Wait · Location" in body
    assert "Trigger" in body
    assert "Act" in body
    assert "Cancel" in body
    assert "Reversal Checklist" in body
    for label in (
        "Location reached",
        "Liquidity swept",
        "Five-minute close back inside",
        "Five-minute 2-2 confirmed",
        "Trigger broken",
        "Setup ready",
    ):
        assert label in body
    assert body.count('data-trigger-step="') == 6
    assert "Risk Calculator" not in body
    assert "Daily Loss Panel" not in body
    assert "Account Locking" not in body
    assert "Contract Sizing" not in body


def test_market_pulse_exposes_decision_narrative_and_support_disclosures(client):
    body = client.get("/market-pulse").get_data(as_text=True)

    assert 'id="marketPulseDecisionNarrative"' in body
    for label in (
        "Execution Read",
        "Active Level",
        "Next Evidence",
        "Invalidation",
    ):
        assert label in body
    for duplicate_label in (
        "Spot / Availability",
        "Permission / State",
        "Component Freshness",
        "Completed candles only",
        "Exit the thesis when this condition fails",
    ):
        assert duplicate_label not in body
    assert body.index('id="marketPulseDecisionNarrative"') < body.index(
        'id="marketPulseGammaCockpit"'
    )
    assert '<details class="marketPulseSupportFold marketPulseSection"' in body
    assert 'id="marketPulseStructureContextFold"' in body
    assert 'id="marketPulseCoreTapeFold"' in body


def test_market_pulse_preserves_primary_controls_and_hooks(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    required_hooks = [
        "data-playbook-symbol-search-control",
        'id="marketPulseContextRefreshBtn"',
        'data-hero-chart-interval="1min"',
        'data-hero-chart-interval="5min"',
        'id="marketPulseHeroToggleDraw"',
        'id="marketPulseHeroUndoDraw"',
        'id="marketPulseHeroClearDraw"',
        'id="marketPulseHeroToggleMarkers"',
        'id="marketPulseHeroToggleLevels"',
        'id="marketPulseHeroToggleDayLevels"',
        'id="marketPulseHeroMicroTapeStatus"',
        'id="marketPulseFastTapeChart"',
        "Connecting · visual only",
        "js/market_pulse_micro_tape.js",
        "data-gamma-symbol-search",
        'data-gamma-window-pill="standard"',
        "data-gamma-refresh",
        'data-open-modal="marketPulseLevelsModal"',
        'href="/candle-opens"',
    ]
    for hook in required_hooks:
        assert hook in body


def test_five_second_tape_reuses_shared_stream_with_poll_fallback_and_stays_isolated():
    chart_script = (ROOT / "static/js/spx_hero_chart.js").read_text(encoding="utf-8")
    context_script = (ROOT / "static/js/market_pulse_gamma_context.js").read_text(
        encoding="utf-8"
    )
    workflow_script = (ROOT / "static/js/market_pulse_gamma_workflow.js").read_text(
        encoding="utf-8"
    )

    assert "acceptMicroTapeQuote(payload, price)" in chart_script
    assert "microTapeSeries.setData(microTapeState.points)" in chart_script
    assert "micro_tape_max_points" in chart_script
    assert 'addEventListener("market-pulse-stream-payload"' in chart_script
    assert "LightweightCharts.createChart(fastTapeCanvas" in chart_script
    assert "fastTapeChart.timeScale().fitContent()" in chart_script
    assert "chart.timeScale().applyOptions({secondsVisible: false})" in chart_script
    assert "new EventSource" not in chart_script
    assert "micro-tape" not in chart_script
    assert "MicroTape" not in workflow_script
    assert "payload.refresh_contract" in chart_script
    assert "marketPhase: nextPhase" in chart_script
    assert 'addEventListener("market-pulse-canonical-update", syncStreamLifecycle)' in context_script
    assert 'dispatchStreamStatus("Market closed"' in context_script


def test_market_radar_groups_indexes_and_exposes_decision_hierarchy(client):
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)

    assert "Market Radar" in body
    assert "Index Pulse" in body
    assert "Ranked Watchlist" in body
    assert "Relative strength" in body
    assert "Relative weakness" in body
    assert "data-market-radar-index" in body
    assert "data-market-radar-watchlist" in body
    assert 'data-role="range-position"' in body
    assert "Selected timeframe candles · white bull" not in body


def test_playbook_uses_one_labeled_canonical_refresh_control(client):
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)

    assert body.count("data-market-pulse-context-refresh") == 2  # button + JS selector
    assert 'aria-label="Refresh data"' in body
    assert "Refresh data" in body
    assert "Refresh Market Pulse" not in body
    assert 'id="marketPulseRefreshFeedback"' in body
    assert 'id="marketPulseDiagnosticsHeading"' in body
    assert 'blockerLabels.length ? "Why execution is locked" : "Data health"' in body
    assert 'label.textContent = isLoading ? "Refreshing…" : "Refresh data"' not in body
    assert "Refresh delayed · showing last valid data" in body
    assert "Auto-refresh delayed · showing last valid data" not in body
    assert "AUTO_REFRESH_INTERVAL_MS = 15000" in body
    assert "INITIAL_CANONICAL_VALIDATION_DELAY_SECONDS = 1" in body
    assert (
        'scheduleCanonicalCheck(INITIAL_CANONICAL_VALIDATION_DELAY_SECONDS, "refresh")'
        in body
    )
    assert "bootstrapRefreshDelaySeconds" not in body
    assert 'document.addEventListener("visibilitychange"' in body
    assert 'window.addEventListener("focus"' in body
    assert 'window.addEventListener("pageshow"' in body
    assert "if (!event.persisted) return" in body
    assert "suspendCanonicalRefresh()" in body
    assert 'resumeCanonicalRefresh({ force: true, reason: "Page restored" })' in body
    assert "Math.min(wakeAt - Date.now(), 2147483000)" in body
    assert "6 * 60 * 60 * 1000" not in body
    assert "refreshCountdownTimer = null" in body
    assert "liveSetupCountdownTimer = null" in body
    assert "nextGenerationId === currentGenerationId" in body
    assert "nextGenerationAtMs < currentGenerationAtMs" in body
    assert "Already showing newer data" in body
    assert "applyCanonicalMarketPulsePayload(data.payload)" in body
    assert 'new CustomEvent("market-pulse-canonical-update"' in body
    assert "window.location.assign(nextUrl.toString())" not in body
    assert "saveCanonicalUiState" not in body
    assert "refreshMarketPulseContext(true, false)" in body
    assert 'if (automatic && currentGenerationId) url.searchParams.set("generation", currentGenerationId)' in body
    assert '"If-None-Match"' in body
    assert "response.status === 304" in body
    assert body.index("stageCanonicalPayload(payload)") < body.index(
        'setCanonicalText("marketPulseStatusSpot", staged.spot)'
    )
    assert body.index('setCanonicalText("marketPulseStatusSpot", staged.spot)') < body.index(
        "currentGenerationId = applyCanonicalMarketPulsePayload(data.payload)"
    )
    assert body.index("nextGenerationAtMs < currentGenerationAtMs") < body.index(
        "currentGenerationId = applyCanonicalMarketPulsePayload(data.payload)"
    )


def test_playbook_canonical_refresh_has_stable_in_place_targets(client):
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)

    for target_id in (
        "marketPulseCanonicalDecision",
        "marketPulseCanonicalDecisionDetail",
        "marketPulseCanonicalActiveLevel",
        "marketPulseCanonicalDistance",
        "marketPulseCanonicalNextEvidence",
        "marketPulseCanonicalInvalidation",
    ):
        assert f'id="{target_id}"' in body


def test_playbook_data_lock_diagnostics_are_collapsed_canonical_and_noncompeting(client):
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)

    assert '<details class="marketPulseDiagnostics marketPulseTrustCenter marketPulseSection' in body
    assert 'id="marketPulseDataLockDiagnostics"' in body
    assert 'id="marketPulseDataLockDiagnostics" open' not in body
    diagnostics = body[
        body.index('id="marketPulseDataLockDiagnostics"') : body.index(
            'aria-label="Market Pulse session and data state"'
        )
    ]
    assert diagnostics.count('data-diagnostic-component="') == 5
    for label in (
        "Trust Center",
        "Completed Bars",
        "Age / Limit",
        "Last Success",
        "Source / Cache",
        "Canonical success",
        "Retry cadence",
    ):
        assert label in body
    assert "data-market-pulse-context-refresh" not in diagnostics
    assert "applyCanonicalDiagnostics(staged.payload)" in body
    assert '"marketPulseDiagnosticsSummaryText"' in body
    assert "stale_required_components" in body
    assert "Sync delayed · retry ≤" in body
    assert 'value === null || value === undefined || value === ""' in body
    assert "Provider completed-bar cache" in body


def test_playbook_template_prioritizes_data_lock_over_actionable_copy():
    template = (ROOT / "mccain_capital/templates/core/market_pulse.html").read_text(
        encoding="utf-8"
    )

    assert "'Data locked' if guardrail.active" in template
    assert "guardrail.message if guardrail.active" in template


def test_playbook_sticky_summary_is_accessible_persistent_and_default_off(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    assert "data-playbook-pin-toggle" in body
    assert 'aria-pressed="false"' in body
    assert 'aria-label="Toggle sticky summary"' in body
    assert "Sticky summary: Off" in body
    assert 'id="marketPulseStatusRegime"' in body
    assert 'return state.includes("strong") ? "Strong Positive Ⲅ" : "Positive Ⲅ"' in body
    assert 'return state.includes("strong") ? "Strong Negative Ⲅ" : "Negative Ⲅ"' in body

    workflow = (ROOT / "static/js/market_pulse_gamma_workflow.js").read_text(encoding="utf-8")
    assert "mccain.marketPulse.playbookPinned.v1" in workflow
    assert "is-playbook-pinned" in workflow
    assert 'Sticky summary: ${pinned ? "On" : "Off"}' in workflow

    styles = (ROOT / "static/css/market_pulse.css").read_text(encoding="utf-8")
    status_metric_rule = styles[
        styles.index("body.page-market-pulse .marketPulseStatusMetric strong{") :
        styles.index("body.page-market-pulse .marketPulseStatusMetric.is-spot{")
    ]
    assert "line-height:1.8;" in status_metric_rule
    assert "line-height:1.1;" not in status_metric_rule
    assert "grid-template-columns:repeat(4, minmax(116px, 1fr))" not in styles
    assert "grid-template-columns:repeat(3, minmax(132px, 1fr))" in styles
    assert "body.page-market-pulse #marketPulseStatusBar{\n  position:relative" in styles
    assert "body.page-market-pulse.is-playbook-pinned .marketPulseExecutionStrip" in styles


def test_canonical_refresh_rebinds_regime_from_complete_fallback_chain(client):
    body = client.get("/market-pulse").get_data(as_text=True)

    assert "const canonicalGammaRegime = (payload = {})" in body
    assert "playbook.gamma_regime" in body
    assert "structure.gamma_regime" in body
    assert "gamma.regime" in body
    assert '"marketPulseStatusRegime",\n      canonicalRegimeLabel' in body
    assert 'setCanonicalText("marketPulseHeaderGammaLabel", canonicalRegimeLabel)' in body
    assert 'headerGammaCard.dataset.gammaState = canonicalRegime.value' in body


def test_playbook_header_search_popover_is_unclipped_and_header_is_compact(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    assert "marketPulseTickerSearchPopover" in body
    assert "data-symbol-search-popover" in body

    styles = (ROOT / "static/css/market_pulse.css").read_text(encoding="utf-8")
    correction = styles[styles.index("Playbook header popover and density correction") :]
    assert "overflow:visible !important" in correction
    assert "grid-template-areas:" in correction
    assert '"kicker kicker"' in correction
    assert "top:calc(100% + 8px)" in correction


def test_gamma_ladder_exposes_location_sections_and_structural_marker_hooks():
    controller = (ROOT / "static/js/gamma_ladder.js").read_text(encoding="utf-8")
    for hook in (
        "data-gamma-location-section",
        "gamma-ladder-nodeMarker--spot",
        "gamma-ladder-nodeMarker--flip",
        "gamma-ladder-nodeMarker--dominant",
    ):
        assert hook in controller


def test_gamma_ladder_uses_quiet_minor_roles_and_one_structural_badge(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    board = body[body.index('id="gammaLadderDepthBoard"') :]
    assert "<span>Signal</span>" not in board

    controller = (ROOT / "static/js/gamma_ladder.js").read_text(encoding="utf-8")
    assert "data-gamma-role-text" in controller
    assert "data-gamma-structural-badge" in controller
    assert "gamma-ladder-row__micro" not in controller
    assert "importance.label" in controller[controller.index("const detailPayload") :]


def test_gamma_ladder_uses_compact_command_bar_and_accessible_settings_popover(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    required = [
        "data-gamma-command-bar",
        'id="gammaLadderSettingsToggle"',
        "data-gamma-settings-label",
        'aria-controls="gammaLadderSettingsPopover"',
        'aria-expanded="false"',
        'id="gammaLadderSettingsPopover"',
        "data-gamma-settings-popover",
    ]
    for hook in required:
        assert hook in body

    popover = body[body.index('id="gammaLadderSettingsPopover"') :]
    assert popover.index('data-gamma-window-pill="standard"') < popover.index(
        'data-gamma-dte-pill="0"'
    )

    controller = (ROOT / "static/js/gamma_ladder.js").read_text(encoding="utf-8")
    for hook in (
        "updateSettingsLabel",
        "setSettingsPopoverOpen",
        'event.key === "Escape"',
        "settingsToggle.focus()",
    ):
        assert hook in controller


def _run_workflow_function(expression: str):
    script_path = ROOT / "static/js/market_pulse_gamma_workflow.js"
    command = (
        f"const api=require({json.dumps(str(script_path))});"
        f"process.stdout.write(JSON.stringify({expression}));"
    )
    result = subprocess.run(
        ["node", "-e", command],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def test_gamma_workflow_normalizes_valid_and_missing_levels():
    valid = _run_workflow_function(
        "api.normalizeLevel({key:'main_flip',price:'5750',classification:'flip'}, 'SPY')"
    )
    assert valid == {
        "key": "main_flip",
        "price": 5750,
        "classification": "flip",
        "symbol": "SPY",
        "valid": True,
    }

    missing = _run_workflow_function("api.normalizeLevel({key:'local_flip',price:''}, 'SPY')")
    assert missing["key"] == "local_flip"
    assert missing["price"] is None
    assert missing["valid"] is False


def test_gamma_workflow_rejects_invalid_stale_and_mismatched_events():
    cases = _run_workflow_function(
        "["
        "api.shouldAcceptEvent({symbol:'SPY',valid:true,price:5750},'SPY',1000,2000),"
        "api.shouldAcceptEvent({symbol:'QQQ',valid:true,price:5750},'SPY',1000,2000),"
        "api.shouldAcceptEvent({symbol:'SPY',valid:false,price:null},'SPY',1000,2000),"
        "api.shouldAcceptEvent({symbol:'SPY',valid:true,price:5750,timestamp:500},'SPY',1000,2000)"
        "]"
    )
    assert cases == [True, False, False, False]


def test_gamma_workflow_restores_pin_after_preview_and_ignores_unavailable_levels():
    states = _run_workflow_function(
        "(() => {"
        "let state={pinnedKey:'',previewKey:''};"
        "state=api.nextSelectionState(state,{type:'pin-toggle',key:'main_flip'});"
        "const pinned={...state};"
        "state=api.nextSelectionState(state,{type:'preview-start',key:'call_wall'});"
        "const preview={...state};"
        "state=api.nextSelectionState(state,{type:'preview-end'});"
        "const restored={...state};"
        "state=api.nextSelectionState(state,{type:'pin-toggle',key:'put_wall',valid:false});"
        "return {pinned,preview,restored,unavailable:state};"
        "})()"
    )
    assert states == {
        "pinned": {"pinnedKey": "main_flip", "previewKey": ""},
        "preview": {"pinnedKey": "main_flip", "previewKey": "call_wall"},
        "restored": {"pinnedKey": "main_flip", "previewKey": ""},
        "unavailable": {"pinnedKey": "main_flip", "previewKey": ""},
    }


def test_chart_consumes_gamma_selection_without_resetting_on_timeframe_change():
    chart_script = (ROOT / "static/js/spx_hero_chart.js").read_text(encoding="utf-8")
    assert 'document.addEventListener("market-pulse:gamma-level-selected"' in chart_script
    assert "gammaSelectionLine = candleSeries.createPriceLine" in chart_script
    interval_handler = chart_script[chart_script.index("const bindIntervalToggles") :]
    assert "clearGammaSelectionLine" not in interval_handler.split("loadDisplayPrefs", 1)[0]


def test_chart_keeps_distant_levels_in_rail_without_flattening_candles():
    chart_script = (ROOT / "static/js/spx_hero_chart.js").read_text(encoding="utf-8")

    assert "const OFF_CHART_LEVEL_PCT = 0.01" in chart_script
    assert "const levelFitsActiveRange" in chart_script
    add_level_line = chart_script[
        chart_script.index("const addLevelLine") : chart_script.index("const addSpotPriceLine")
    ]
    assert "levelFitsActiveRange(numeric, activeBars, anchor)" in add_level_line
    frame_bounds = chart_script[
        chart_script.index("const applyFrameBounds") : chart_script.index(
            "const activeBarsForPayload"
        )
    ]
    assert "levelFitsActiveRange(numeric, bars, spot)" in frame_bounds


def test_chart_reconciles_canonical_strategy_overlays_without_chart_reset():
    chart_script = (ROOT / "static/js/spx_hero_chart.js").read_text(encoding="utf-8")

    assert 'window.addEventListener("market-pulse-canonical-update"' in chart_script
    assert "verdict.invalidation_level" in chart_script
    assert "verdict.primary_target" in chart_script
    assert 'state === "REVERSAL_READY"' in chart_script
    assert 'state === "CONTINUATION_ACTIVE"' in chart_script
    assert '"BEAR INVALIDATION"' in chart_script
    assert '"BULL INVALIDATION"' in chart_script
    assert '"REVERSAL TARGET"' in chart_script
    assert '"CONTINUATION TARGET"' in chart_script
    assert "buildCanonicalStrategyOverlays" in chart_script
    assert "canonicalStrategyLines" in chart_script
    assert "marketPulseCanonicalChartOverlayState" in chart_script
    canonical_handler = chart_script[
        chart_script.index("const applyCanonicalStrategyOverlays") : chart_script.index(
            "const handleGammaLevelSelection"
        )
    ]
    assert "fitContent" not in canonical_handler
    assert "setVisibleLogicalRange" not in canonical_handler
    assert "clearGammaSelectionLine" not in canonical_handler
    assert "priceLines" not in canonical_handler
    assert "dayLevelLines" not in canonical_handler


def test_market_pulse_busts_chart_cache_for_canonical_overlays(client):
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)

    assert "market-pulse-ladder-20260810c" in body


def test_chart_uses_closed_session_review_mode_and_stops_component_polling():
    template = (ROOT / "mccain_capital/templates/core/market_pulse.html").read_text(
        encoding="utf-8"
    )
    chart_script = (ROOT / "static/js/spx_hero_chart.js").read_text(encoding="utf-8")

    assert "SESSION CLOSED · REVIEW ONLY" in template
    assert 'polling?.automatic_refresh_enabled !== false' in chart_script
    assert "if (!marketIsOpenForPolling()) return;" in chart_script
    start_polling = chart_script[
        chart_script.index("const startPolling") : chart_script.index("const boot")
    ]
    assert "if (!marketIsOpenForPolling())" in start_polling
    assert "renderSummary(lastLevelsPayload)" in start_polling
    assert "market-pulse-session-contract-updated" in chart_script
    assert "automatic_refresh_enabled: contract.automatic_refresh_enabled === true" in chart_script


def test_market_pulse_lifecycle_revalidates_server_boundaries_without_local_phase_guessing():
    template = (ROOT / "mccain_capital/templates/core/market_pulse.html").read_text(
        encoding="utf-8"
    )

    assert "next_transition_at" in template
    assert "serverClockOffsetMs" in template
    assert "forceBoundaryValidation" in template
    assert "automaticRefreshEnabled = true" not in template
    initial_validation_start = template.rindex("registerCanonicalRefreshLane();")
    initial_validation = template[
        initial_validation_start : template.index("armRefreshCountdown();", initial_validation_start)
    ]
    assert "if (automaticRefreshEnabled)" in initial_validation
    assert "INITIAL_CANONICAL_VALIDATION_DELAY_SECONDS" in initial_validation
    assert "else pauseCanonicalRefresh()" in initial_validation
    assert "refreshMarketPulseContext(false, false, true)" in template
    assert 'if (forceRefresh) url.searchParams.set("refresh", "1")' in template
    assert 'if (automatic) url.searchParams.set("automatic", "1")' in template
    assert 'currentGenerationId && automatic ? {"If-None-Match"' in template
    for event_name in ("visibilitychange", "online", "focus", "pageshow", "pagehide"):
        assert event_name in template


def test_trade_decision_uses_an_explicit_five_step_sequence(client):
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)
    decision = body[body.index('id="marketPulseTradeReadCard"') : body.index('id="spxPrioritySpotPanel"')]

    for label in ("Wait · Location", "Trigger", "Act", "Target", "Cancel"):
        assert label in decision
    for element_id in (
        "marketPulseDecisionWait",
        "marketPulseNeed",
        "marketPulseBestLook",
        "marketPulseExecutionStatus",
        "marketPulseIfThen",
    ):
        assert f'id="{element_id}"' in decision
        assert f'setCanonicalText("{element_id}"' in body
    assert ">Active level<" not in decision
    assert ">Interaction<" not in decision
    assert ">Path / Direction<" not in decision
    assert "No entry until the trigger confirms" in body


def test_market_pulse_refresh_rejects_contradictory_execution_generations(client):
    body = client.get("/market-pulse?ticker=SPX", follow_redirects=True).get_data(as_text=True)

    assert "authoritative_action_state" in body
    assert "Execution permission conflicts with canonical state." in body
    assert "Execution guide belongs to a different state generation." in body
    assert "Actionable state lacks required canonical confirmation." in body
    assert '["triggered", "confirmed"].includes' in body
    assert "Gamma Ladder belongs to a different generation." in body
    assert "Gamma Ladder spot diverged from the canonical SPX quote." in body
    assert 'setCanonicalText("marketPulseHeroStateChip", staged.actionCode)' in body
    assert "Execution locked · ${reason}" in body
