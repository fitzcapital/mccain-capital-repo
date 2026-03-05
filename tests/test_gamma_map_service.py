import pandas as pd

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
