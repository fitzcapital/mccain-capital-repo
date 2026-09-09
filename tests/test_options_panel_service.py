import json
import threading
import time

from mccain_capital.services import options_panel_service as svc


def test_parse_option_ticker_compact_format():
    out = svc.parse_option_ticker("O:SPXW260306C05125000")
    assert out["root"] == "SPXW"
    assert out["expiration"] == "2026-03-06"
    assert out["cp"] == "C"
    assert out["strike"] == 5125.0


def test_format_contract_label_human_readable():
    label = svc.format_contract_label("SPXW", "2026-03-06", 5125.0, "C")
    assert label == "SPXW 2026-03-06 5125C"


def test_liquidity_badge_logic():
    assert svc.liquidity_badge(0.60, 9200) == "Tight"
    assert svc.liquidity_badge(1.20, 800) == "OK"
    assert svc.liquidity_badge(2.10, 400) == "Wide"


def test_contract_shortlist_keeps_near_money_call_and_put():
    contracts = [
        {
            "label": "far call",
            "strike": 7700.0,
            "cp": "C",
            "mid": 7.5,
            "vol": 9000,
            "spread": 0.2,
            "_root_rank": 0,
            "_liq_rank": 0,
            "_dte": 0,
        },
        {
            "label": "ntm call",
            "strike": 7780.0,
            "cp": "C",
            "mid": 3.04,
            "vol": 500,
            "spread": 0.5,
            "_root_rank": 0,
            "_liq_rank": 0,
            "_dte": 0,
        },
        {
            "label": "ntm put",
            "strike": 7780.0,
            "cp": "P",
            "mid": 3.2,
            "vol": 400,
            "spread": 0.5,
            "_root_rank": 0,
            "_liq_rank": 0,
            "_dte": 0,
        },
    ]
    selected = svc._contract_shortlist(contracts, 7781.0)
    assert [row["label"] for row in selected[:2]] == ["ntm call", "ntm put"]


def test_shared_snapshot_is_visible_after_process_local_cache_is_cleared(tmp_path, monkeypatch):
    monkeypatch.setattr(svc.app_runtime, "PERSISTENT_DATA_DIR", str(tmp_path))
    snapshot = {
        "asof": "2026-09-02T15:00:00+00:00",
        "symbols": {
            "SPX": {
                "underlying": {"price": 7781.0},
                "contracts": [{"label": "SPXW 2026-09-02 7780C"}],
            }
        },
    }
    svc._write_shared_snapshot(snapshot)
    with svc._LOCK:
        svc._CACHE.clear()
    assert svc.get_options_snapshot() == snapshot


def test_failed_refresh_preserves_last_good_shared_snapshot(tmp_path, monkeypatch):
    monkeypatch.setattr(svc.app_runtime, "PERSISTENT_DATA_DIR", str(tmp_path))
    snapshot = {
        "asof": "2026-09-02T15:00:00+00:00",
        "symbols": {
            "SPX": {
                "underlying": {"price": 7781.0},
                "contracts": [{"label": "SPXW 2026-09-02 7780C"}],
            }
        },
    }
    svc._write_shared_snapshot(snapshot)
    monkeypatch.setattr(svc.market_data_service, "get_watchlist", lambda symbols: {"SPX": {}})
    monkeypatch.setattr(svc, "_fetch_spx_contracts", lambda spot=None: [])
    svc._poll_once()
    with open(svc._snapshot_path(), encoding="utf-8") as handle:
        assert json.load(handle) == snapshot


def test_refresh_lock_serializes_writers(tmp_path, monkeypatch):
    monkeypatch.setattr(svc.app_runtime, "PERSISTENT_DATA_DIR", str(tmp_path))
    state = {"active": 0, "maximum": 0}
    state_lock = threading.Lock()

    def refresh():
        with state_lock:
            state["active"] += 1
            state["maximum"] = max(state["maximum"], state["active"])
        time.sleep(0.02)
        with state_lock:
            state["active"] -= 1

    monkeypatch.setattr(svc, "_poll_once", refresh)
    threads = [threading.Thread(target=svc._run_locked_refresh) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert state["maximum"] == 1
