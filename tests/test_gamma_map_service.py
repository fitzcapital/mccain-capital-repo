from datetime import datetime

import pandas as pd
import pytest

from mccain_capital import runtime as app_runtime
from mccain_capital.services import gamma_map_service as svc


def _raw_basket_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "expiration": "2026-03-19",
                "strike": 5100.0,
                "call_oi": 1000,
                "put_oi": 100,
                "call_gamma": 0.010,
                "put_gamma": 0.003,
                "call_vega": 0.15,
                "put_vega": 0.08,
                "call_delta": 0.52,
                "put_delta": 0.20,
            },
            {
                "expiration": "2026-03-20",
                "strike": 5100.0,
                "call_oi": 500,
                "put_oi": 50,
                "call_gamma": 0.012,
                "put_gamma": 0.002,
                "call_vega": 0.12,
                "put_vega": 0.07,
                "call_delta": 0.48,
                "put_delta": 0.15,
            },
            {
                "expiration": "2026-03-19",
                "strike": 5125.0,
                "call_oi": 150,
                "put_oi": 1200,
                "call_gamma": 0.004,
                "put_gamma": 0.016,
                "call_vega": 0.10,
                "put_vega": 0.17,
                "call_delta": 0.25,
                "put_delta": 0.61,
            },
            {
                "expiration": "2026-03-20",
                "strike": 5125.0,
                "call_oi": 50,
                "put_oi": 800,
                "call_gamma": 0.003,
                "put_gamma": 0.013,
                "call_vega": 0.09,
                "put_vega": 0.16,
                "call_delta": 0.18,
                "put_delta": 0.54,
            },
            {
                "expiration": "2026-03-19",
                "strike": 5150.0,
                "call_oi": 900,
                "put_oi": 300,
                "call_gamma": 0.014,
                "put_gamma": 0.005,
                "call_vega": 0.18,
                "put_vega": 0.11,
                "call_delta": 0.61,
                "put_delta": 0.28,
            },
        ]
    )


def _aggregated_df(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if "gex" not in frame.columns and "net_gex" in frame.columns:
        frame["gex"] = frame["net_gex"]
    if "call_gex" not in frame.columns and "call_side_gex" in frame.columns:
        frame["call_gex"] = frame["call_side_gex"]
    if "put_gex" not in frame.columns and "put_side_gex" in frame.columns:
        frame["put_gex"] = -frame["put_side_gex"].abs()
    if "call_side_gex" not in frame.columns and "call_gex" in frame.columns:
        frame["call_side_gex"] = frame["call_gex"]
    if "put_side_gex" not in frame.columns and "put_gex" in frame.columns:
        frame["put_side_gex"] = frame["put_gex"].abs()
    for col in ("call_oi", "put_oi", "vex", "dex"):
        if col not in frame.columns:
            frame[col] = 0.0
    return frame


def _stub_snapshot_io(monkeypatch) -> None:
    monkeypatch.setattr(svc, "render_gex_chart", lambda *args, **kwargs: svc._FallbackFigure("gex"))
    monkeypatch.setattr(svc, "render_vex_chart", lambda *args, **kwargs: svc._FallbackFigure("vex"))
    monkeypatch.setattr(svc, "export_outputs", lambda *args, **kwargs: {"csv": "", "png": ""})
    monkeypatch.setattr(svc, "_maybe_emit_eod_gamma_notification", lambda *_args, **_kwargs: None)


def test_compute_row_gex_matches_configured_formula():
    out = svc.compute_row_gex(0.01, 1000, 5100.0, "call", contract_multiplier=100, scaler=0.01)
    expected = 0.01 * 1000 * 100 * (5100.0**2) * 0.01
    assert out == expected
    assert svc.compute_row_gex(0.01, 1000, 5100.0, "put") == -expected


def test_classify_gamma_regime_positive_threshold():
    assert (
        svc.classify_gamma_regime(
            75_000_000.0,
            positive_threshold=50_000_000.0,
            negative_threshold=-50_000_000.0,
        )
        == "positive"
    )


def test_classify_gamma_regime_negative_threshold():
    assert (
        svc.classify_gamma_regime(
            -80_000_000.0,
            positive_threshold=50_000_000.0,
            negative_threshold=-50_000_000.0,
        )
        == "negative"
    )


def test_classify_gamma_regime_neutral_band():
    assert (
        svc.classify_gamma_regime(
            10_000_000.0,
            positive_threshold=50_000_000.0,
            negative_threshold=-50_000_000.0,
        )
        == "neutral"
    )


def test_duplicate_strike_across_two_expiries_groups_into_one_strike():
    spot = 5120.0
    expo = svc.compute_exposures(_raw_basket_df(), spot)
    grouped = svc.aggregate_gex_by_strike(expo)

    strike_5100 = grouped[grouped["strike"] == 5100.0]
    assert len(strike_5100.index) == 1

    raw_5100 = expo[expo["strike"] == 5100.0]
    assert float(strike_5100.iloc[0]["net_gex"]) == pytest.approx(float(raw_5100["net_gex"].sum()))


def test_false_zero_crossing_is_prevented_by_grouped_strike_map():
    raw_like = _aggregated_df(
        [
            {"strike": 5000.0, "net_gex": -10.0},
            {"strike": 5000.0, "net_gex": 6.0},
            {"strike": 5010.0, "net_gex": -9.0},
            {"strike": 5010.0, "net_gex": 3.0},
        ]
    )
    grouped = svc.aggregate_gex_by_strike(raw_like)

    assert svc.compute_gamma_flip(grouped, spot=5005.0) is None


def test_exact_zero_net_gex_strike_returns_that_strike_for_flip():
    grouped = _aggregated_df(
        [
            {"strike": 5000.0, "net_gex": -10.0},
            {"strike": 5005.0, "net_gex": 0.0},
            {"strike": 5010.0, "net_gex": 15.0},
        ]
    )

    assert svc.compute_gamma_flip(grouped, spot=5004.0) == 5005.0


def test_interpolated_flip_uses_linear_interpolation():
    grouped = _aggregated_df(
        [
            {"strike": 5000.0, "net_gex": -10.0},
            {"strike": 5010.0, "net_gex": 10.0},
        ]
    )

    assert svc.compute_gamma_flip(grouped, spot=5007.0) == pytest.approx(5005.0)


def test_no_sign_change_returns_none_for_flip():
    grouped = _aggregated_df(
        [
            {"strike": 5000.0, "net_gex": -10.0},
            {"strike": 5010.0, "net_gex": -4.0},
        ]
    )

    assert svc.compute_gamma_flip(grouped, spot=5007.0) is None


def test_call_wall_selection_uses_strongest_positive_grouped_strike():
    grouped = _aggregated_df(
        [
            {"strike": 5100.0, "net_gex": 10.0},
            {"strike": 5125.0, "net_gex": 22.0},
            {"strike": 5150.0, "net_gex": 18.0},
        ]
    )

    assert svc.compute_call_wall(grouped, spot=5130.0) == {"strike": 5125.0, "net_gex": 22.0}


def test_put_wall_selection_uses_most_negative_grouped_strike():
    grouped = _aggregated_df(
        [
            {"strike": 5100.0, "net_gex": -10.0},
            {"strike": 5075.0, "net_gex": -22.0},
            {"strike": 5050.0, "net_gex": -18.0},
        ]
    )

    assert svc.compute_put_wall(grouped, spot=5080.0) == {"strike": 5075.0, "net_gex": -22.0}


def test_regime_consistency_matches_sign_of_total_net_gex():
    grouped = _aggregated_df(
        [
            {"strike": 5100.0, "net_gex": 80_000_000.0},
            {"strike": 5125.0, "net_gex": -5_000_000.0},
        ]
    )
    total = svc.compute_net_gex_total(grouped)
    summary = svc.build_summary({"gamma_flip": 5110.0}, spot=5120.0, net_gex=total)

    assert total > 0
    assert svc.classify_gamma_regime(total) == "positive"
    assert summary["regime"] == "positive"


def test_tie_breaking_prefers_nearest_spot_then_higher_call_and_lower_put():
    grouped = _aggregated_df(
        [
            {"strike": 5110.0, "net_gex": 20.0},
            {"strike": 5130.0, "net_gex": 20.0},
            {"strike": 5070.0, "net_gex": -25.0},
            {"strike": 5090.0, "net_gex": -25.0},
        ]
    )

    assert svc.compute_call_wall(grouped, spot=5120.0) == {"strike": 5130.0, "net_gex": 20.0}
    assert svc.compute_put_wall(grouped, spot=5080.0) == {"strike": 5070.0, "net_gex": -25.0}


def test_empty_dataset_returns_safe_warning_state(monkeypatch):
    _stub_snapshot_io(monkeypatch)

    snapshot = svc.assemble_market_pulse_snapshot(
        raw=pd.DataFrame(),
        requested_expiries=["2026-03-19", "2026-03-20"],
        source_timestamp="2026-03-19T12:00:00+00:00",
        source_file_path="",
        spot_price=5100.0,
        spot_source_name="tradier",
        spot_source_timestamp="2026-03-19T12:00:00+00:00",
        contracts_seen=0,
    )

    assert snapshot["gamma_flip"] is None
    assert snapshot["call_wall"] is None
    assert snapshot["put_wall"] is None
    assert "empty_source" in snapshot["stale_flags"]
    assert snapshot["warnings"]


def test_snapshot_atomicity_keeps_last_good_snapshot_on_bad_recompute(monkeypatch):
    _stub_snapshot_io(monkeypatch)
    monkeypatch.setattr(svc, "_CACHE", svc._json_clone(svc._CACHE))
    monkeypatch.setattr(
        svc,
        "_RUNTIME_STATE",
        {"last_attempted_at": "", "last_error": "", "last_refresh_ms": 0, "status": "waiting", "cache_status": "cold"},
    )
    monkeypatch.setattr(
        svc.market_data_service,
        "get_price_snapshot",
        lambda symbol: {
            "symbol": "SPX",
            "value": 5120.0,
            "source_name": "tradier",
            "source_timestamp": "2026-03-19T12:00:00+00:00",
            "raw": {},
        },
    )
    monkeypatch.setattr(
        svc,
        "load_gamma_source",
        lambda expiries=None: {
            "raw": _raw_basket_df(),
            "requested_expiries": ["2026-03-19", "2026-03-20"],
            "source_timestamp": "2026-03-19T12:00:00+00:00",
            "contracts_seen": 5,
            "source_file_path": "",
        },
    )

    good = svc.run_gamma_refresh_once()
    assert good["total_unique_strikes"] > 0

    monkeypatch.setattr(
        svc,
        "load_gamma_source",
        lambda expiries=None: {
            "raw": pd.DataFrame(),
            "requested_expiries": ["2026-03-19", "2026-03-20"],
            "source_timestamp": "2026-03-19T12:05:00+00:00",
            "contracts_seen": 0,
            "source_file_path": "",
        },
    )

    with pytest.raises(RuntimeError):
        svc.run_gamma_refresh_once()

    after = svc.get_gamma_snapshot()
    assert after["gamma_flip"] == good["gamma_flip"]
    assert after["call_wall"] == good["call_wall"]
    assert after["put_wall"] == good["put_wall"]


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
