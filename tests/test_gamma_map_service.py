from datetime import datetime

import pandas as pd
import pytest

from mccain_capital import runtime as app_runtime
from mccain_capital.services import gamma_map_service as svc
from mccain_capital.services.gamma_snapshot_models import SnapshotStatus
from mccain_capital.services.gamma_snapshot_models import ValidationError
from mccain_capital.services.gamma_snapshot_models import validate_market_pulse_snapshot


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


def test_local_flip_prefers_exact_zero_inside_local_band():
    grouped = _aggregated_df(
        [
            {"strike": 4990.0, "net_gex": -12.0},
            {"strike": 5000.0, "net_gex": 0.0},
            {"strike": 5010.0, "net_gex": 9.0},
        ]
    )

    result = svc.compute_local_gamma_flip(grouped, spot=5004.0, pct_band=0.02, strike_window=5)

    assert result["found"] is True
    assert result["value"] == 5000.0
    assert result["distance_from_spot"] == pytest.approx(4.0)
    assert result["window_used"]["mode"] == "spot_pct_band"


def test_local_flip_interpolates_nearest_sign_change_inside_local_band():
    grouped = _aggregated_df(
        [
            {"strike": 4980.0, "net_gex": 14.0},
            {"strike": 5000.0, "net_gex": -10.0},
            {"strike": 5010.0, "net_gex": 10.0},
            {"strike": 5040.0, "net_gex": -12.0},
        ]
    )

    result = svc.compute_local_gamma_flip(grouped, spot=5008.0, pct_band=0.02, strike_window=5)

    assert result["found"] is True
    assert result["value"] == pytest.approx(5005.0)
    assert result["distance_from_spot"] == pytest.approx(3.0)
    assert result["candidate_count"] >= 1


def test_local_flip_returns_none_when_local_band_has_no_sign_change():
    grouped = _aggregated_df(
        [
            {"strike": 4950.0, "net_gex": -14.0},
            {"strike": 5000.0, "net_gex": 8.0},
            {"strike": 5010.0, "net_gex": 10.0},
            {"strike": 5020.0, "net_gex": 12.0},
            {"strike": 5150.0, "net_gex": -8.0},
        ]
    )

    result = svc.compute_local_gamma_flip(grouped, spot=5010.0, pct_band=0.01, strike_window=5)

    assert result["found"] is False
    assert result["value"] is None
    assert result["distance_from_spot"] is None
    assert result["candidate_count"] == 0


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

    assert snapshot["gamma_flip_combined_basket"] is None
    assert snapshot["call_wall_aggregated_gamma"] is None
    assert snapshot["put_wall_aggregated_gamma"] is None
    assert snapshot["warning_state"]["snapshot_status"] == SnapshotStatus.INVALID.value
    assert "empty_source" in snapshot["warning_state"]["stale_flags"]
    assert snapshot["warning_state"]["warnings"]


def test_complete_basket_snapshot_is_healthy(monkeypatch):
    _stub_snapshot_io(monkeypatch)

    snapshot = svc.assemble_market_pulse_snapshot(
        raw=_raw_basket_df(),
        requested_expiries=["2026-03-19", "2026-03-20"],
        source_timestamp="2026-03-19T12:00:00+00:00",
        source_file_path="",
        spot_price=5120.0,
        spot_source_name="tradier",
        spot_source_timestamp="2026-03-19T12:00:00+00:00",
        contracts_seen=5,
    )

    assert snapshot["warning_state"]["snapshot_status"] == SnapshotStatus.HEALTHY.value
    assert snapshot["source_metadata"]["included_expiries"] == ["2026-03-19", "2026-03-20"]
    assert snapshot["local_flip_found"] is True
    assert snapshot["local_flip_aggregated_gamma"] is not None
    assert snapshot["local_flip_distance_from_spot"] is not None
    assert snapshot["local_flip_window_used"]["rows_considered"] >= 2
    assert snapshot["local_flip_expiries_used"] == ["2026-03-19", "2026-03-20"]


def test_single_expiry_snapshot_is_degraded(monkeypatch):
    _stub_snapshot_io(monkeypatch)

    degraded_raw = _raw_basket_df()[_raw_basket_df()["expiration"] == "2026-03-19"].copy()
    snapshot = svc.assemble_market_pulse_snapshot(
        raw=degraded_raw,
        requested_expiries=["2026-03-19", "2026-03-20"],
        source_timestamp="2026-03-19T12:00:00+00:00",
        source_file_path="",
        spot_price=5120.0,
        spot_source_name="tradier",
        spot_source_timestamp="2026-03-19T12:00:00+00:00",
        contracts_seen=3,
    )

    assert snapshot["warning_state"]["snapshot_status"] == SnapshotStatus.DEGRADED.value
    assert snapshot["source_metadata"]["included_expiries"] == ["2026-03-19"]
    assert "missing_expiries" in snapshot["warning_state"]["stale_flags"]


def test_snapshot_atomicity_keeps_last_good_snapshot_on_bad_recompute(monkeypatch):
    _stub_snapshot_io(monkeypatch)
    monkeypatch.setattr(svc, "_CACHE", {})
    monkeypatch.setattr(
        svc.app_runtime,
        "now_et",
        lambda: datetime(2026, 3, 19, 12, 1, tzinfo=app_runtime.TZ),
    )
    monkeypatch.setattr(
        svc,
        "_RUNTIME_STATE",
        {
            "last_attempted_at": "",
            "last_error": "",
            "last_refresh_ms": 0,
            "status": "waiting",
            "cache_status": "cold",
        },
    )
    monkeypatch.setattr(
        svc.market_data_service,
        "get_price_snapshot",
        lambda symbol: {
            "symbol": "SPX",
            "value": 5120.0,
            "source_name": "tradier",
            "source_timestamp": "2026-03-19T16:00:00+00:00",
            "raw": {},
        },
    )
    monkeypatch.setattr(
        svc,
        "load_gamma_source",
        lambda expiries=None: {
            "raw": _raw_basket_df(),
            "requested_expiries": ["2026-03-19", "2026-03-20"],
            "source_timestamp": "2026-03-19T16:00:00+00:00",
            "source_fetch_timestamp": "2026-03-19T16:00:00+00:00",
            "source_effective_timestamp": "2026-03-19T16:00:00+00:00",
            "source_effective_timestamp_source": "fetch_fallback",
            "source_effective_timestamp_note": "Exchange-native chain timestamp unavailable; using fetch timestamp.",
            "exchange_timestamp_available": False,
            "contracts_seen": 5,
            "source_file_path": "",
        },
    )

    good = svc.run_gamma_refresh_once()
    assert good["total_unique_strikes"] > 0
    assert good["snapshot_status"] == SnapshotStatus.HEALTHY.value

    monkeypatch.setattr(
        svc,
        "load_gamma_source",
        lambda expiries=None: {
            "raw": pd.DataFrame(),
            "requested_expiries": ["2026-03-19", "2026-03-20"],
            "source_timestamp": "2026-03-19T12:05:00+00:00",
            "source_fetch_timestamp": "2026-03-19T12:05:00+00:00",
            "source_effective_timestamp": "2026-03-19T12:05:00+00:00",
            "source_effective_timestamp_source": "fetch_fallback",
            "source_effective_timestamp_note": "Exchange-native chain timestamp unavailable; using fetch timestamp.",
            "exchange_timestamp_available": False,
            "contracts_seen": 0,
            "source_file_path": "",
        },
    )

    stale = svc.run_gamma_refresh_once()

    after = svc.get_gamma_snapshot()
    assert stale["snapshot_status"] == SnapshotStatus.STALE.value
    assert after["snapshot_status"] == SnapshotStatus.STALE.value
    assert after["gamma_flip"] == good["gamma_flip"]
    assert after["call_wall"] == good["call_wall"]
    assert after["put_wall"] == good["put_wall"]


def test_stale_source_without_last_good_returns_invalid_snapshot(monkeypatch):
    _stub_snapshot_io(monkeypatch)
    monkeypatch.setattr(svc, "_CACHE", {})
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
            "source_fetch_timestamp": "2026-03-19T12:00:00+00:00",
            "source_effective_timestamp": "2026-03-19T12:00:00+00:00",
            "source_effective_timestamp_source": "fetch_fallback",
            "source_effective_timestamp_note": "Exchange-native chain timestamp unavailable; using fetch timestamp.",
            "exchange_timestamp_available": False,
            "contracts_seen": 5,
            "source_file_path": "",
        },
    )
    monkeypatch.setattr(
        svc.app_runtime,
        "now_et",
        lambda: datetime(2026, 3, 19, 12, 30, tzinfo=app_runtime.TZ),
    )

    snapshot = svc.run_gamma_refresh_once()

    assert snapshot["snapshot_status"] == SnapshotStatus.INVALID.value
    assert snapshot["gamma_flip"] is None
    assert (
        "stale" in snapshot["snapshot_status_label"].lower()
        or "invalid" in snapshot["snapshot_status_label"].lower()
    )


def test_pydantic_validation_rejects_missing_required_timestamp():
    with pytest.raises(ValidationError):
        validate_market_pulse_snapshot(
            {
                "asof": "2026-03-19T12:00:00+00:00",
                "computed_at": "2026-03-19T12:00:00+00:00",
                "last_successful_compute": "",
                "source_metadata": {
                    "requested_expiries": ["2026-03-19", "2026-03-20"],
                    "included_expiries": ["2026-03-19", "2026-03-20"],
                    "missing_expiries": [],
                    "source_file_path": "",
                    "source_fetch_timestamp": "2026-03-19T12:00:00+00:00",
                    "source_effective_timestamp": "2026-03-19T12:00:00+00:00",
                    "source_effective_timestamp_source": "fetch_fallback",
                    "source_effective_timestamp_note": "",
                    "exchange_timestamp_available": False,
                },
                "warning_state": {
                    "warnings": [],
                    "stale_flags": [],
                    "snapshot_status": "healthy",
                    "snapshot_status_label": "Healthy Gamma Basket: 2 of 2 expiries available",
                    "snapshot_status_detail": "ok",
                },
                "spot": 5120.0,
                "spot_price_used": 5120.0,
                "spot_source_name": "tradier",
                "spot_source_timestamp": "2026-03-19T12:00:00+00:00",
                "contract_multiplier": 100,
                "total_rows_before_filter": 1,
                "total_rows_after_filter": 1,
                "total_unique_strikes": 1,
                "regime": "positive",
                "net_gex": 1.0,
                "net_gex_total": 1.0,
                "net_gamma_label": "+1",
                "gamma_flip_combined_basket": 5120.0,
                "call_wall_aggregated_gamma": 5130.0,
                "put_wall_aggregated_gamma": 5110.0,
                "call_wall_gamma_per_point": 1.0,
                "put_wall_gamma_per_point": 1.0,
                "gamma_walls_top3": [5130.0],
                "top_positive_strikes": [5130.0],
                "top_negative_strikes": [5110.0],
                "top_3_positive_gamma_strikes": [5130.0],
                "top_3_negative_gamma_strikes": [5110.0],
                "nearest_positive_gamma_above_spot": 5130.0,
                "nearest_negative_gamma_below_spot": 5110.0,
                "gamma_range_estimate": 10.0,
                "gamma_range_high": 5130.0,
                "gamma_range_low": 5110.0,
                "next_call_wall_above": 5130.0,
                "next_put_wall_below": 5110.0,
                "void_zone": {"start": None, "end": None},
                "bias": "buy_dips_above_flip",
                "cache_status": "fresh",
                "refresh_mode": "in_process",
                "field_labels": {},
                "paths": {"csv": "", "png": ""},
                "diagnostics": {
                    "status": "ok",
                    "contracts_seen": 1,
                    "contracts_used": 1,
                    "duplicate_raw_records": 0,
                    "nan_gamma_rows": 0,
                    "zero_open_interest_rows": 0,
                    "rows_dropped": 0,
                    "expirations": ["2026-03-19", "2026-03-20"],
                    "refresh_ms": 0,
                    "error": "",
                },
                "narrative": {
                    "what_matters": "",
                    "trader_live_quote": "",
                    "trade_bias": "",
                    "environment_note": "",
                    "auto_read": [],
                    "warning_badges": [],
                },
                "chart_json": {"gex": None, "vex": None},
                "grouped_strike_rows": [],
                "validation_result": {
                    "total_rows_before_filter": 1,
                    "rows_dropped": 0,
                    "duplicate_raw_records": 0,
                    "nan_gamma_rows": 0,
                    "zero_open_interest_rows": 0,
                    "available_expiries": ["2026-03-19", "2026-03-20"],
                    "warnings": [],
                    "stale_flags": [],
                },
            }
        )


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
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("polygon fallback should not run")
        ),
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
