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


def test_market_pulse_exposes_decision_narrative_and_support_disclosures(client):
    body = client.get("/market-pulse").get_data(as_text=True)

    assert 'id="marketPulseDecisionNarrative"' in body
    for label in (
        "Execution Read",
        "Regime",
        "Spot / Availability",
        "Permission",
        "Freshness",
        "Trigger",
        "Invalidation",
    ):
        assert label in body
    assert body.index('id="marketPulseDecisionNarrative"') < body.index(
        'id="marketPulseGammaCockpit"'
    )
    assert '<details class="marketPulseSupportFold marketPulseSection"' in body
    assert 'id="marketPulseStructureContextFold"' in body
    assert 'id="marketPulseCoreTapeFold"' in body


def test_market_pulse_preserves_primary_controls_and_hooks(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    required_hooks = [
        'data-playbook-symbol-search-control',
        'id="marketPulseContextRefreshBtn"',
        'data-hero-chart-interval="1min"',
        'data-hero-chart-interval="5min"',
        'id="marketPulseHeroToggleDraw"',
        'id="marketPulseHeroUndoDraw"',
        'id="marketPulseHeroClearDraw"',
        'id="marketPulseHeroToggleMarkers"',
        'id="marketPulseHeroToggleLevels"',
        'id="marketPulseHeroToggleDayLevels"',
        'data-gamma-symbol-search',
        'data-gamma-window-pill="standard"',
        'data-gamma-refresh',
        'data-open-modal="marketPulseLevelsModal"',
        'href="/candle-opens"',
    ]
    for hook in required_hooks:
        assert hook in body


def test_playbook_header_pin_is_accessible_persistent_and_default_off(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    assert 'data-playbook-pin-toggle' in body
    assert 'aria-pressed="false"' in body
    assert "Pin header" in body

    workflow = (ROOT / "static/js/market_pulse_gamma_workflow.js").read_text(encoding="utf-8")
    assert "mccain.marketPulse.playbookPinned.v1" in workflow
    assert "is-playbook-pinned" in workflow

    styles = (ROOT / "static/css/market_pulse.css").read_text(encoding="utf-8")
    assert "body.page-market-pulse #marketPulseStatusBar{\n  position:relative" in styles
    assert "body.page-market-pulse.is-playbook-pinned #marketPulseStatusBar" in styles


def test_playbook_header_search_popover_is_unclipped_and_header_is_compact(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    assert "marketPulseTickerSearchPopover" in body
    assert "data-symbol-search-popover" in body

    styles = (ROOT / "static/css/market_pulse.css").read_text(encoding="utf-8")
    correction = styles[styles.index("Playbook header popover and density correction"):]
    assert "overflow:visible !important" in correction
    assert 'grid-template-areas:' in correction
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
    board = body[body.index('id="gammaLadderDepthBoard"'):]
    assert "<span>Signal</span>" not in board

    controller = (ROOT / "static/js/gamma_ladder.js").read_text(encoding="utf-8")
    assert "data-gamma-role-text" in controller
    assert "data-gamma-structural-badge" in controller
    assert "gamma-ladder-row__micro" not in controller
    assert "importance.label" in controller[controller.index("const detailPayload"):]


def test_gamma_ladder_uses_compact_command_bar_and_accessible_settings_popover(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    required = [
        'data-gamma-command-bar',
        'id="gammaLadderSettingsToggle"',
        'data-gamma-settings-label',
        'aria-controls="gammaLadderSettingsPopover"',
        'aria-expanded="false"',
        'id="gammaLadderSettingsPopover"',
        'data-gamma-settings-popover',
    ]
    for hook in required:
        assert hook in body

    popover = body[body.index('id="gammaLadderSettingsPopover"'):]
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

    missing = _run_workflow_function(
        "api.normalizeLevel({key:'local_flip',price:''}, 'SPY')"
    )
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
    interval_handler = chart_script[chart_script.index("const bindIntervalToggles"):]
    assert "clearGammaSelectionLine" not in interval_handler.split("loadDisplayPrefs", 1)[0]
