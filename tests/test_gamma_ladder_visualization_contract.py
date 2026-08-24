import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def test_gamma_ladder_uses_institutional_depth_hierarchy(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    anchors = [
        'id="gammaLadderStatus"',
        "data-gamma-execution-map",
        'id="gammaLadderDepthBoard"',
        'id="gammaLadderSelectedInspector"',
        'id="gammaLadderGuide"',
    ]
    positions = [body.index(anchor) for anchor in anchors]
    assert positions == sorted(positions)
    assert body.count('id="gammaLadderSelectedInspector"') == 1
    assert body.count("data-gamma-execution-map") == 1
    assert "data-gamma-key-levels" not in body
    assert "data-gamma-decision-panels" not in body
    assert "<summary" in body[body.index('id="gammaLadderGuide"') :]


def test_gamma_ladder_preserves_controls_hooks_and_script_order(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    required = [
        'id="marketPulseGammaLadderCard"',
        "data-gamma-symbol-search",
        'data-gamma-symbol-pill="SPX"',
        'data-gamma-window-pill="standard"',
        'data-gamma-dte-pill="0"',
        "data-gamma-refresh",
        "data-gamma-loading",
        "data-gamma-error",
        "data-gamma-board",
        "data-gamma-rows",
        "data-gamma-depth-toggle",
        "data-gamma-hidden-count",
        "data-gamma-tooltip",
        "data-gamma-legend",
    ]
    for hook in required:
        assert hook in body
    presentation = body.index("js/gamma_ladder_presentation.js")
    controller = body.index("js/gamma_ladder.js")
    assert presentation < controller


def test_gamma_ladder_uses_shared_refresh_and_local_progressive_disclosure():
    controller = (ROOT / "static/js/gamma_ladder.js").read_text()
    assert 'document.querySelector("[data-market-pulse-context-refresh]")' in controller
    assert 'window.addEventListener("market-pulse-canonical-update"' in controller
    assert 'url.searchParams.set("refresh", "1")' in controller
    assert "window.location.reload" not in controller
    assert "showFullRows = !showFullRows" in controller
    assert "nearby hidden" in controller


def test_gamma_ladder_exposes_live_guidance_scope_and_lineage(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    controller = (ROOT / "static/js/gamma_ladder.js").read_text()
    stylesheet = (ROOT / "static/css/market_pulse.css").read_text()

    for hook in (
        "data-gamma-scope",
        "data-gamma-lineage",
        "data-gamma-command-sequence",
        "data-gamma-command-now",
        "data-gamma-command-confirm",
        "data-gamma-command-target",
        "data-gamma-command-fail",
        "data-gamma-priority-strip",
    ):
        assert hook in body
    assert 'data-canonical-generation="' in body
    assert 'data-canonical-gamma-generation="' in body
    assert 'symbol === "SPX"' in controller
    assert '"Reference only"' in controller
    assert '"Revalidating lineage"' in controller
    assert "gammaMatches" in controller
    assert "alignmentRecoveryGammaId" in controller
    assert 'document.querySelector("[data-market-pulse-context-refresh]")?.click()' in controller
    assert ".gamma-command-sequence" in stylesheet
    assert ".gamma-priority-strip" in stylesheet


def test_gamma_ladder_guidance_obeys_canonical_permission_and_explicit_freshness(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    controller = (ROOT / "static/js/gamma_ladder.js").read_text()

    for attribute in (
        "data-canonical-permission",
        "data-canonical-action-state",
        "data-canonical-execution-locked",
    ):
        assert attribute in body
    assert "const explicitSourceOverall" in controller
    assert 'explicitSourceOverall === "current"' in controller
    assert 'sourceOverall === "current" || payload.data_state === "live"' not in controller
    assert "const canonicalAllowsGuidance" in controller
    assert "&& canonicalAllowsGuidance" in controller
    assert "Execution locked by canonical playbook" in controller
    assert "Resolve the named execution blocker" in controller
    assert "root.dataset.canonicalGeneration = currentCanonicalGenerationId" in controller
    assert "root.dataset.canonicalGammaGeneration = currentCanonicalGammaGenerationId" in controller
    assert "root.dataset.canonicalPermission = currentCanonicalPermission" in controller
    assert "root.dataset.canonicalActionState = currentCanonicalActionState" in controller
    assert 'root.dataset.canonicalExecutionLocked = currentCanonicalExecutionLocked ? "true" : "false"' in controller


def test_market_pulse_bootstrap_embeds_the_aligned_gamma_generation(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    service = (ROOT / "mccain_capital/services/core.py").read_text()
    template = (ROOT / "mccain_capital/templates/core/market_pulse.html").read_text()

    assert '"gamma_ladder":' in body
    assert "'gamma_ladder': gamma_ladder" in template
    assert "gamma_ladder=gamma_ladder" in service
    assert service.count("_market_pulse_build_aligned_gamma_ladder(") == 3


def test_gamma_ladder_controller_prefers_matching_canonical_bootstrap():
    controller = (ROOT / "static/js/gamma_ladder.js").read_text()

    assert 'document.getElementById("spxPriorityBasePayload")' in controller
    assert "bootstrapPayload?.gamma_ladder" in controller
    assert "const bootstrapMatches" in controller
    assert "lastAcceptedPayload = bootstrapLadder" in controller
    assert "renderGammaLadderRows(bootstrapLadder)" in controller
    assert "if (bootstrapMatches)" in controller
    assert "const acceptedPayloads = new Map()" in controller
    assert "cacheAcceptedPayload(bootstrapLadder)" in controller
    assert "if (!restoreAcceptedPayload(nextSymbol)) fetchGammaLadder(nextSymbol)" in controller


def test_aligned_gamma_ladder_helper_promotes_one_shared_generation(app, monkeypatch):
    from mccain_capital.services import core
    from mccain_capital.services import gamma_map_service

    timestamp = "2026-08-20T11:30:00-04:00"
    ladder = {
        "ok": True,
        "symbol": "SPX",
        "spot": 7700.0,
        "updated_at": timestamp,
        "gamma_generation_id": "gamma-source-generation",
        "rows": [{"strike": 7700, "net_gex": 1.0}],
    }
    monkeypatch.setattr(gamma_map_service, "build_gamma_ladder", lambda *args, **kwargs: ladder)
    monkeypatch.setitem(app.config, "TESTING", False)
    canonical = {
        "generation_id": "market-source-generation",
        "execution_locked": False,
        "components": {"gamma": {"status": "current"}},
    }
    verdict = {"permission": "wait"}
    guide = {"permission": "wait", "action_state": "WAIT"}
    playbook = {
        "live_execution_guide": dict(guide),
        "scenario_rankings": {"generation_id": "market-source-generation"},
    }
    structure = {"scenario_rankings": {"generation_id": "market-source-generation"}}

    with app.app_context():
        result, aligned_guide = core._market_pulse_build_aligned_gamma_ladder(
            "SPX",
            playbook_quote={"price": 7700.0, "as_of": timestamp},
            spx_quote={"price": 7700.0, "as_of": timestamp},
            canonical_freshness=canonical,
            verdict=verdict,
            playbook_view=playbook,
            market_structure_snapshot=structure,
            live_execution_guide=guide,
        )

    shared_generation = canonical["generation_id"]
    assert shared_generation not in {"market-source-generation", "gamma-source-generation"}
    assert canonical["gamma_generation_id"] == "gamma-source-generation"
    assert result["canonical_generation_id"] == shared_generation
    assert verdict["generation_id"] == shared_generation
    assert playbook["scenario_rankings"]["generation_id"] == shared_generation
    assert structure["scenario_rankings"]["generation_id"] == shared_generation
    assert aligned_guide["generation_id"] == shared_generation


def test_aligned_gamma_ladder_revalidates_stale_cache_before_locking(app, monkeypatch):
    from mccain_capital.services import core
    from mccain_capital.services import gamma_map_service

    timestamp = "2026-08-20T11:30:00-04:00"
    stale_ladder = {
        "ok": True,
        "symbol": "SPX",
        "spot": 7680.0,
        "updated_at": "2026-08-20T11:10:00-04:00",
        "gamma_generation_id": "stale-gamma-generation",
        "rows": [],
    }
    current_ladder = {
        "ok": True,
        "symbol": "SPX",
        "spot": 7700.0,
        "updated_at": timestamp,
        "gamma_generation_id": "current-gamma-generation",
        "rows": [{"strike": 7700, "net_gex": 1.0}],
    }
    calls = []

    def build_ladder(*args, **kwargs):
        calls.append(bool(kwargs.get("force_refresh")))
        return current_ladder if kwargs.get("force_refresh") else stale_ladder

    monkeypatch.setattr(gamma_map_service, "build_gamma_ladder", build_ladder)
    monkeypatch.setattr(core, "_market_pulse_market_hours", lambda _now: True)
    monkeypatch.setitem(app.config, "TESTING", False)
    canonical = {
        "generation_id": "market-source-generation",
        "execution_locked": False,
        "stale_required_components": [],
        "components": {"gamma": {"status": "current"}},
    }
    verdict = {"permission": "wait"}
    guide = {"permission": "wait", "action_state": "WAIT"}
    playbook = {
        "strategy": {},
        "primary_scenario": {},
        "live_execution_guide": dict(guide),
        "scenario_rankings": {"generation_id": "market-source-generation"},
    }
    structure = {"scenario_rankings": {"generation_id": "market-source-generation"}}

    with app.app_context():
        result, aligned_guide = core._market_pulse_build_aligned_gamma_ladder(
            "SPX",
            playbook_quote={"price": 7700.0, "as_of": timestamp},
            spx_quote={"price": 7700.0, "as_of": timestamp},
            canonical_freshness=canonical,
            verdict=verdict,
            playbook_view=playbook,
            market_structure_snapshot=structure,
            live_execution_guide=guide,
        )

    assert calls == [False, True]
    assert result["gamma_generation_id"] == "current-gamma-generation"
    assert result.get("coherence_blocked") is not True
    assert canonical["execution_locked"] is False
    assert canonical["stale_required_components"] == []
    assert aligned_guide["action_state"] == "WAIT"


def test_aligned_gamma_ladder_stays_locked_when_revalidation_diverges(app, monkeypatch):
    from mccain_capital.services import core
    from mccain_capital.services import gamma_map_service

    divergent = {
        "ok": True,
        "symbol": "SPX",
        "spot": 7680.0,
        "updated_at": "2026-08-20T11:10:00-04:00",
        "gamma_generation_id": "divergent-gamma-generation",
        "rows": [],
    }
    monkeypatch.setattr(gamma_map_service, "build_gamma_ladder", lambda *args, **kwargs: divergent)
    monkeypatch.setattr(core, "_market_pulse_market_hours", lambda _now: True)
    monkeypatch.setitem(app.config, "TESTING", False)
    canonical = {
        "generation_id": "market-source-generation",
        "execution_locked": False,
        "stale_required_components": [],
        "components": {"gamma": {"status": "current"}},
    }
    verdict = {"permission": "wait"}
    guide = {"permission": "wait", "action_state": "WAIT"}
    playbook = {
        "strategy": {},
        "primary_scenario": {},
        "live_execution_guide": dict(guide),
        "scenario_rankings": {"generation_id": "market-source-generation"},
    }
    structure = {"scenario_rankings": {"generation_id": "market-source-generation"}}

    with app.app_context():
        result, aligned_guide = core._market_pulse_build_aligned_gamma_ladder(
            "SPX",
            playbook_quote={"price": 7700.0, "as_of": "2026-08-20T11:30:00-04:00"},
            spx_quote={"price": 7700.0, "as_of": "2026-08-20T11:30:00-04:00"},
            canonical_freshness=canonical,
            verdict=verdict,
            playbook_view=playbook,
            market_structure_snapshot=structure,
            live_execution_guide=guide,
        )

    assert result["coherence_blocked"] is True
    assert canonical["execution_locked"] is True
    assert canonical["stale_required_components"] == ["gamma"]
    assert aligned_guide["action_state"] == "LOCKED"


def test_gamma_ladder_guidance_is_ranked_selected_and_plain_language():
    controller = (ROOT / "static/js/gamma_ladder.js").read_text()
    template = (ROOT / "mccain_capital/templates/core/market_pulse.html").read_text()

    assert "const rankedPriorityRows" in controller
    assert ".slice(0, 3)" in controller
    assert "const nearestPriority = rankedPriorityRows(payload)[0]" in controller
    assert "if (nearestRow) updateSelectedInspector(nearestRow)" in controller
    assert "5m close ${movingUp ? \"above\" : \"below\"}" in controller
    assert "Loss of ${formatNumber(decision, 0)}" in controller
    assert "Crossed ${Number(payload.spot)" in controller
    assert "No prior Gamma snapshot is available." in controller
    assert "Unavailable choices do not have a valid snapshot." in template


def test_market_pulse_humanizes_machine_states_at_initial_and_refresh_boundaries(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    template = (ROOT / "mccain_capital/templates/core/market_pulse.html").read_text()
    context_controller = (ROOT / "static/js/market_pulse_gamma_context.js").read_text()
    hero_controller = (ROOT / "static/js/spx_hero_chart.js").read_text()

    assert "Market Pulse" in body
    assert "macro mp_state_label" in template
    assert 'LEVEL_BEING_TESTED: "Testing active level"' in body
    assert "humanizeMarketState(strategy.state)" in body
    assert "humanizeMarketState(strategy?.state" in context_controller
    assert "humanizeMarketState(levels.decision_label" in hero_controller
    assert (
        '"Positive Gamma stabilization"'
        in (ROOT / "mccain_capital/services/gamma_map_service.py").read_text()
    )


def test_positive_gamma_status_uses_regime_specific_glow():
    template = (ROOT / "mccain_capital/templates/core/market_pulse.html").read_text()
    stylesheet = (ROOT / "static/css/market_pulse.css").read_text()

    assert "is-regime-positive" in template
    assert 'canonicalRegimeKey.includes("positive")' in template
    assert "grid-template-columns:repeat(3,minmax(132px,1fr))" in stylesheet
    assert "0 0 24px rgba(93,242,166,.13)" in stylesheet
    assert "text-shadow:none" in stylesheet
    assert 'id="marketPulseStatusUpdated"' not in template
    assert "Last candle ·" in template


def test_execution_read_has_legend_and_quiet_refresh_indicator():
    template = (ROOT / "mccain_capital/templates/core/market_pulse.html").read_text()
    stylesheet = (ROOT / "static/css/market_pulse.css").read_text()

    assert 'class="marketPulseStateLegend"' in template
    assert "Data locked</b> Required market data is stale or unavailable" in template
    assert "Planning only</b> Structure is available" in template
    assert "Actionable</b> Required data and price evidence are current" in template
    assert 'class="marketPulseDiagnosticsSyncIndicator"' in template
    assert "diagnosticsSyncState.title = message" in template
    assert ".marketPulseDecisionNarrativeLead.is-locked" in stylesheet
    assert ".marketPulseDiagnosticsSyncIndicator i" in stylesheet


def test_execution_rail_and_chart_use_semantic_visual_contract():
    template = (ROOT / "mccain_capital/templates/core/market_pulse.html").read_text()
    stylesheet = (ROOT / "static/css/market_pulse.css").read_text()
    chart = (ROOT / "static/js/spx_hero_chart.js").read_text()

    for role in ("permission", "level", "evidence", "risk"):
        assert f'data-decision-role="{role}"' in template
    assert "Can I act, plan, or wait?" in template
    assert "What proves the thesis wrong?" in template
    assert "grid-template-columns: repeat(4, minmax(0, 1fr))" in stylesheet
    assert 'bull: "#F4F6FA"' in chart
    assert 'bullBorder: "#B9C1CD"' in chart
    assert 'bullWick: "#D7DCE5"' in chart
    assert 'bear: "#1F4ACB"' in chart
    assert 'bearBorder: "#2F6BFF"' in chart
    assert 'bearWick: "#D7DCE5"' in chart
    assert 'stratUp: "#F4F6FA"' in chart
    assert 'stratDown: "#2F6BFF"' in chart
    assert 'text: "3"' in chart
    assert 'text: "2"' in chart
    assert 'text: "1"' in chart
    assert 'id="spxExecutionHeroSessionShade"' in template
    assert "Math.abs(distance) <= 5" in chart


def test_gamma_ladder_uses_truthful_expiry_net_strength_and_focused_rows():
    template = (ROOT / "mccain_capital/templates/core/market_pulse.html").read_text()
    controller = (ROOT / "static/js/gamma_ladder.js").read_text()

    assert "Requested ${dteDisplayLabel(currentDtePreset)} → Using" in controller
    assert "setDisplayedDtePreset(effectiveDte)" in controller
    assert "const maxNet = rows.reduce" in controller
    assert "buildGammaDecisionModel({ ...payload, rows }, rowMeta)" in controller
    assert 'label: "Dominant"' in controller
    assert 'label: "Major"' in controller
    assert 'label: "Minor"' in controller
    assert 'class="gamma-ladder-row__tone"' not in controller
    assert "gamma-ladder-row__laneLabel--negative" not in controller
    assert "data-gamma-selected-behavior" in template
    assert "data-gamma-selected-failure" in template
    assert "data-gamma-selected-chart" in template
    assert "market-pulse-ladder-20260810c" in template


def test_gamma_ladder_uses_spot_centered_price_spine_and_robust_depth_scale():
    controller = (ROOT / "static/js/gamma_ladder.js").read_text()
    stylesheet = (ROOT / "static/css/market_pulse.css").read_text()

    assert "data-gamma-spot-rail" in controller
    assert "gamma-ladder-nodeMarker--decision" in controller
    assert "data-gamma-row-decision" in controller
    assert "Math.ceil(sideValues.length * 0.9)" in controller
    assert "rows[index] === nearestUpside" in controller
    assert "rows[index] === nearestDownside" in controller
    assert ".gamma-ladder-spotRail" in stylesheet
    assert ".gamma-ladder-row.is-decision" in stylesheet
    assert "rgba(66, 221, 180, .94)" in stylesheet
    assert "rgba(255, 100, 124, .94)" in stylesheet
    assert 'return `Decision ${roleBaseLabel(row)}`' in controller
    assert 'row.level.type === "current" ? "Spot Interaction"' in controller
    assert 'return "Accepted Below"' in controller
    assert 'return "Accepted Above"' in controller
    assert 'return "Expansion Active"' in controller


def _run_presentation(expression: str):
    path = ROOT / "static/js/gamma_ladder_presentation.js"
    command = (
        f"const api=require({json.dumps(str(path))});"
        f"process.stdout.write(JSON.stringify({expression}));"
    )
    result = subprocess.run(
        ["node", "-e", command],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def test_gamma_ladder_diff_covers_value_order_and_structural_changes():
    result = _run_presentation(
        "(() => {"
        "const context={symbol:'SPY',dte:'0',expiration:'2026-07-17',window:'standard'};"
        "const before=api.createPresentationSnapshot({spot:100,regime:'positive_gamma',rows:["
        "{strike:105,call_gex:10,put_gex:-2,net_gex:8,is_strongest:true},"
        "{strike:100,call_gex:5,put_gex:-5,net_gex:0,is_flip:true},"
        "{strike:95,call_gex:2,put_gex:-8,net_gex:-6}"
        "]},context);"
        "const after=api.createPresentationSnapshot({spot:101,regime:'negative_gamma',rows:["
        "{strike:110,call_gex:7,put_gex:-1,net_gex:6},"
        "{strike:100,call_gex:4,put_gex:-8,net_gex:-4,is_flip:true},"
        "{strike:105,call_gex:12,put_gex:-2,net_gex:10,is_strongest:true}"
        "]},context);"
        "return api.diffPresentationSnapshots(before,after);"
        "})()"
    )
    assert result["unchanged"] is False
    assert result["inserted"] == ["110"]
    assert result["removed"] == ["95"]
    assert set(result["reordered"]) == {"105"}
    assert set(result["changed"]) == {"100", "105"}
    assert result["spotMoved"] is True
    assert result["regimeChanged"] is True
    assert result["strongestChanged"] is False


def test_gamma_ladder_diff_detects_crossings_and_stays_idle_when_unchanged():
    result = _run_presentation(
        "(() => {"
        "const context={symbol:'SPY',dte:'0',expiration:'2026-07-17',window:'standard'};"
        "const payload={spot:99,regime:'mixed_gamma',rows:["
        "{strike:100,call_gex:5,put_gex:-5,net_gex:0,is_flip:true},"
        "{strike:95,call_gex:2,put_gex:-8,net_gex:-6,is_strongest:true}"
        "]};"
        "const before=api.createPresentationSnapshot(payload,context);"
        "const same=api.diffPresentationSnapshots(before,api.createPresentationSnapshot(payload,context));"
        "const crossed=api.diffPresentationSnapshots(before,api.createPresentationSnapshot({...payload,spot:101},context));"
        "return {same,crossed};"
        "})()"
    )
    assert result["same"]["unchanged"] is True
    assert result["same"]["changed"] == []
    assert result["crossed"]["crossed"] == ["100"]


def test_gamma_ladder_diff_detects_classification_only_changes():
    result = _run_presentation(
        "(() => {"
        "const context={symbol:'SPY',dte:'0',expiration:'2026-07-17',window:'standard'};"
        "const before=api.createPresentationSnapshot({spot:100,rows:["
        "{strike:100,call_gex:5,put_gex:-2,net_gex:3,classification:'positive'}"
        "]},context);"
        "const after=api.createPresentationSnapshot({spot:100,rows:["
        "{strike:100,call_gex:5,put_gex:-2,net_gex:3,classification:'strong-positive'}"
        "]},context);"
        "return api.diffPresentationSnapshots(before,after);"
        "})()"
    )
    assert result["changed"] == ["100"]
    assert result["unchanged"] is False


def test_gamma_ladder_rejects_stale_mismatched_and_reduced_motion_animation():
    result = _run_presentation(
        "({"
        "accepted:api.shouldAcceptPayload({requestId:2,latestRequestId:2,symbol:'SPY',activeSymbol:'SPY',timestamp:200,lastTimestamp:100}),"
        "stale:api.shouldAcceptPayload({requestId:1,latestRequestId:2,symbol:'SPY',activeSymbol:'SPY',timestamp:200,lastTimestamp:100}),"
        "mismatch:api.shouldAcceptPayload({requestId:2,latestRequestId:2,symbol:'QQQ',activeSymbol:'SPY',timestamp:200,lastTimestamp:100}),"
        "old:api.shouldAcceptPayload({requestId:2,latestRequestId:2,symbol:'SPY',activeSymbol:'SPY',timestamp:50,lastTimestamp:100}),"
        "contextMismatch:api.shouldAcceptPayload({requestId:2,latestRequestId:2,symbol:'SPY',activeSymbol:'SPY',contextKey:'SPY|0|2026-07-17|tight',activeContextKey:'SPY|0|2026-07-17|wide'}),"
        "motion:api.shouldAnimate({accepted:true,reducedMotion:false,unchanged:false}),"
        "reduced:api.shouldAnimate({accepted:true,reducedMotion:true,unchanged:false}),"
        "unchanged:api.shouldAnimate({accepted:true,reducedMotion:false,unchanged:true})"
        "})"
    )
    assert result == {
        "accepted": True,
        "stale": False,
        "mismatch": False,
        "old": False,
        "contextMismatch": False,
        "motion": True,
        "reduced": False,
        "unchanged": False,
    }
