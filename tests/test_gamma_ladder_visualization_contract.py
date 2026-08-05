import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def test_gamma_ladder_uses_institutional_depth_hierarchy(client):
    body = client.get("/market-pulse").get_data(as_text=True)
    anchors = [
        'id="gammaLadderStatus"',
        'id="gammaLadderKeyLevels"',
        'id="gammaLadderDepthBoard"',
        'id="gammaLadderSelectedInspector"',
        'id="gammaLadderGuide"',
    ]
    positions = [body.index(anchor) for anchor in anchors]
    assert positions == sorted(positions)
    assert body.count('id="gammaLadderSelectedInspector"') == 1
    assert '<summary' in body[body.index('id="gammaLadderGuide"'):]


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
        "data-gamma-tooltip",
        "data-gamma-legend",
    ]
    for hook in required:
        assert hook in body
    presentation = body.index("js/gamma_ladder_presentation.js")
    controller = body.index("js/gamma_ladder.js")
    assert presentation < controller


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
