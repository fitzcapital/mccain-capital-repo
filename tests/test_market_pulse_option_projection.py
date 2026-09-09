from datetime import datetime, timedelta, timezone

from mccain_capital.services import market_pulse_option_projection as projection


NOW = datetime(2026, 9, 2, 15, 0, tzinfo=timezone.utc)


def snapshot(contracts, *, age_seconds=5, spot=7782.0):
    return {
        "asof": (NOW - timedelta(seconds=age_seconds)).isoformat(),
        "symbols": {"SPX": {"underlying": {"price": spot}, "contracts": contracts}},
    }


def contract(label, *, mid=7.5, delta=0.4, spread=0.5, liq="Tight", vol=1000, oi=2000):
    return {
        "label": label,
        "mid": mid,
        "delta": delta,
        "spread": spread,
        "liq": liq,
        "vol": vol,
        "oi": oi,
    }


def test_selects_directional_current_tradier_anchor_deterministically():
    data = snapshot(
        [
            contract("SPXW 2026-09-02 7780P", delta=-0.38),
            contract("SPXW 2026-09-02 7780C", delta=0.42),
            contract("SPX 2026-09-03 7780C", delta=0.40),
        ]
    )
    bullish = projection.premium_anchor("bullish", snapshot=data, now=NOW)
    bearish = projection.premium_anchor("bearish", snapshot=data, now=NOW)
    assert bullish["contract_label"] == "SPXW 2026-09-02 7780C"
    assert bearish["contract_label"] == "SPXW 2026-09-02 7780P"
    assert bullish["pricing_mode"] == "tradier_current_quote"
    assert bullish["contract_cost"] == 750.0 and bullish["absolute_delta"] == 0.42
    assert bullish["strike"] == 7780.0
    assert bullish["spot_distance"] == 2.0


def test_near_money_strike_outranks_far_contract_with_target_premium():
    data = snapshot(
        [
            contract("SPXW 2026-09-02 7780C", mid=3.04, delta=0.52),
            contract("SPXW 2026-09-02 7740C", mid=7.50, delta=0.40),
        ],
        spot=7781.0,
    )
    anchor = projection.premium_anchor("bullish", snapshot=data, now=NOW)
    assert anchor["contract_label"] == "SPXW 2026-09-02 7780C"
    assert anchor["contract_cost"] == 304.0
    assert anchor["spot_distance"] == 1.0


def test_stale_crossed_missing_delta_and_empty_snapshots_fall_back():
    stale = projection.premium_anchor(
        "bullish", snapshot=snapshot([contract("SPXW 2026-09-02 7780C")], age_seconds=60), now=NOW
    )
    crossed = projection.premium_anchor(
        "bullish", snapshot=snapshot([contract("SPXW 2026-09-02 7780C", spread=-0.1)]), now=NOW
    )
    missing_delta = projection.premium_anchor(
        "bullish", snapshot=snapshot([contract("SPXW 2026-09-02 7780C", delta=None)]), now=NOW
    )
    empty = projection.premium_anchor("bullish", snapshot=snapshot([]), now=NOW)
    mixed = projection.premium_anchor("mixed", snapshot=snapshot([]), now=NOW)
    assert stale["fallback_reason"] == "quote_stale"
    assert {crossed["pricing_mode"], missing_delta["pricing_mode"], empty["pricing_mode"]} == {
        "fallback_estimate"
    }
    assert mixed["fallback_reason"] == "mixed_direction_selection"
    assert all(
        row["contract_cost"] == 750.0 for row in (stale, crossed, missing_delta, empty, mixed)
    )


def test_anchor_reads_cached_snapshot_without_refresh(monkeypatch):
    calls = []
    monkeypatch.setattr(
        projection,
        "get_options_snapshot",
        lambda: calls.append("read") or snapshot([contract("SPXW 2026-09-02 7780C")]),
    )
    projection.premium_anchor("bullish", now=NOW)
    assert calls == ["read"]


def test_anchor_pair_requests_one_empty_cache_recovery(monkeypatch):
    calls = []
    data = snapshot(
        [
            contract("SPXW 2026-09-02 7780C"),
            contract("SPXW 2026-09-02 7780P", delta=-0.4),
        ]
    )
    monkeypatch.setattr(
        projection,
        "get_options_snapshot",
        lambda **kwargs: calls.append(kwargs) or data,
    )
    anchors = projection.premium_anchors(now=NOW)
    assert calls == [{"recover_if_empty": True}]
    assert anchors["bullish"]["pricing_mode"] == "tradier_current_quote"
    assert anchors["bearish"]["pricing_mode"] == "tradier_current_quote"
