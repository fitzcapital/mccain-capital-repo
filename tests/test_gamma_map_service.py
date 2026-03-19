from datetime import datetime

import pandas as pd

from mccain_capital import runtime as app_runtime
from mccain_capital.services import gamma_map_service as svc


def _base_df():
    return pd.DataFrame(
        [
            {
                "expiration": "2026-03-05",
                "strike": 5100.0,
                "call_oi": 1000,
                "put_oi": 900,
                "call_gamma": 0.012,
                "put_gamma": 0.010,
                "call_vega": 0.15,
                "put_vega": 0.14,
                "call_delta": 0.52,
                "put_delta": -0.48,
            },
            {
                "expiration": "2026-03-05",
                "strike": 5125.0,
                "call_oi": 1400,
                "put_oi": 700,
                "call_gamma": 0.014,
                "put_gamma": 0.009,
                "call_vega": 0.16,
                "put_vega": 0.13,
                "call_delta": 0.56,
                "put_delta": -0.42,
            },
            {
                "expiration": "2026-03-06",
                "strike": 5150.0,
                "call_oi": 600,
                "put_oi": 1900,
                "call_gamma": 0.010,
                "put_gamma": 0.018,
                "call_vega": 0.11,
                "put_vega": 0.19,
                "call_delta": 0.45,
                "put_delta": -0.58,
            },
        ]
    )


def test_compute_exposures_matches_contract_formula():
    spot = 5120.0
    expo = svc.compute_exposures(_base_df(), spot)
    row = expo[expo["strike"] == 5100.0].iloc[0]

    expected_gex = (1000 * 0.012 * 100 * spot) - (900 * 0.010 * 100 * spot)
    expected_vex = (1000 * 0.15 * 100) + (900 * 0.14 * 100)
    expected_dex = (1000 * 0.52 * 100) - (900 * -0.48 * 100)

    assert abs(float(row["gex"]) - expected_gex) < 1e-6
    assert abs(float(row["vex"]) - expected_vex) < 1e-6
    assert abs(float(row["dex"]) - expected_dex) < 1e-6


def test_identify_levels_finds_walls_void_and_flip():
    spot = 5120.0
    expo = svc.compute_exposures(_base_df(), spot)
    levels = svc.identify_levels(expo, spot)

    assert len(levels["gamma_walls_top3"]) >= 1
    assert levels["call_wall"] is not None
    assert levels["put_wall"] is not None
    assert levels["gamma_flip"] is not None
    assert "start" in levels["void_zone"]
    assert "end" in levels["void_zone"]


def test_build_summary_bias_and_regime_positive():
    summary = svc.build_summary({"gamma_flip": 5110.0}, spot=5120.0, net_gex=100.0)
    assert summary["regime"] == "positive"
    assert summary["bias"] == "buy_dips_above_flip"


def test_build_summary_bias_and_regime_negative():
    summary = svc.build_summary({"gamma_flip": 5110.0}, spot=5100.0, net_gex=-100.0)
    assert summary["regime"] == "negative"
    assert summary["bias"] == "sell_rips_below_flip"


def test_parse_cboe_option_symbol():
    out = svc._parse_cboe_option_symbol("SPX260320C00200000")
    assert out["expiration"] == "2026-03-20"
    assert out["cp"] == "C"
    assert out["strike"] == 200.0


def test_identify_levels_prefers_split_call_put_walls_around_spot():
    df = pd.DataFrame(
        [
            {"strike": 5090.0, "gex": -10.0, "call_side_gex": 20.0, "put_side_gex": 80.0},
            {"strike": 5100.0, "gex": 5.0, "call_side_gex": 200.0, "put_side_gex": 250.0},
            {"strike": 5110.0, "gex": 12.0, "call_side_gex": 300.0, "put_side_gex": 150.0},
        ]
    )
    levels = svc.identify_levels(df, spot=5102.0)
    assert levels["call_wall"] == 5110.0
    assert levels["put_wall"] == 5100.0


def test_identify_levels_keeps_put_wall_strength_in_sync_when_wall_is_reassigned():
    df = pd.DataFrame(
        [
            {"strike": 5100.0, "gex": 20.0, "call_side_gex": 300.0, "put_side_gex": 280.0},
            {"strike": 5095.0, "gex": -12.0, "call_side_gex": 40.0, "put_side_gex": 190.0},
        ]
    )

    levels = svc.identify_levels(df, spot=5099.0)

    assert levels["call_wall"] == 5100.0
    assert levels["put_wall"] == 5095.0
    assert levels["put_wall_gamma_per_point"] == 190.0


def test_gamma_poll_seconds_is_faster_during_market_hours():
    current = datetime(2026, 3, 16, 10, 0, tzinfo=app_runtime.TZ)
    assert svc._gamma_poll_seconds(current) == svc.ACTIVE_POLL_SECONDS


def test_gamma_poll_seconds_is_slower_on_weekends():
    current = datetime(2026, 3, 14, 10, 0, tzinfo=app_runtime.TZ)
    assert svc._gamma_poll_seconds(current) == svc.WEEKEND_POLL_SECONDS


def test_fetch_spx_chain_for_expiries_does_not_fallback_when_tradier_is_empty(monkeypatch):
    monkeypatch.setattr(svc, "_fetch_spx_chain_from_tradier", lambda expiries: pd.DataFrame())
    monkeypatch.setattr(
        svc,
        "_massive_json",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("polygon fallback should not run")),
    )
    monkeypatch.setattr(
        svc,
        "_fetch_spx_chain_from_cboe",
        lambda expiries: (_ for _ in ()).throw(AssertionError("cboe fallback should not run")),
    )

    out = svc.fetch_spx_chain_for_expiries(["2026-03-17"])

    assert out.empty


def test_eod_gamma_notification_context_opens_after_curb_close(monkeypatch):
    monkeypatch.setattr(svc, "_load_gamma_notify_state", lambda: {})
    snapshot = {"diagnostics": {"expirations": ["2026-03-17", "2026-03-18"]}}

    current = datetime(2026, 3, 17, 17, 10, tzinfo=app_runtime.TZ)
    context = svc._eod_gamma_notification_context(snapshot, now_et=current)

    assert context is not None
    assert context["target_expiry"] == "2026-03-18"


def test_eod_gamma_notification_context_skips_before_window(monkeypatch):
    monkeypatch.setattr(svc, "_load_gamma_notify_state", lambda: {})
    snapshot = {"diagnostics": {"expirations": ["2026-03-17", "2026-03-18"]}}

    current = datetime(2026, 3, 17, 16, 59, tzinfo=app_runtime.TZ)

    assert svc._eod_gamma_notification_context(snapshot, now_et=current) is None


def test_maybe_emit_eod_gamma_notification_sends_once_per_target(monkeypatch):
    state = {}
    sent: list[dict] = []

    monkeypatch.setattr(svc, "_load_gamma_notify_state", lambda: dict(state))

    def _save(payload):
        state.clear()
        state.update(payload)

    monkeypatch.setattr(svc, "_save_gamma_notify_state", _save)
    monkeypatch.setattr(
        svc,
        "_send_eod_gamma_notification",
        lambda snapshot, context: sent.append({"snapshot": snapshot, "context": context}),
    )
    snapshot = {
        "spot": 5662.25,
        "gamma_flip": 5655.0,
        "call_wall": 5700.0,
        "put_wall": 5625.0,
        "asof": "2026-03-17T21:10:00+00:00",
        "diagnostics": {"expirations": ["2026-03-17", "2026-03-18"]},
    }
    current = datetime(2026, 3, 17, 17, 15, tzinfo=app_runtime.TZ)

    svc._maybe_emit_eod_gamma_notification(snapshot, now_et=current)
    svc._maybe_emit_eod_gamma_notification(snapshot, now_et=current)

    assert len(sent) == 1
    assert state["last_target_expiry"] == "2026-03-18"
