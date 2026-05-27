from mccain_capital.services import core


def test_get_supported_playbook_ticker_defaults_to_qqq():
    assert core.get_supported_playbook_ticker(None) == "QQQ"
    assert core.get_supported_playbook_ticker("") == "QQQ"
    assert core.get_supported_playbook_ticker("spx") == "SPX"
    assert core.get_supported_playbook_ticker("qqq") == "QQQ"
    assert core.get_supported_playbook_ticker("SPY") == "SPY"
    assert core.get_supported_playbook_ticker("nvda") == "NVDA"
    assert core.get_supported_playbook_ticker("BRK.B") == "BRK.B"
    assert core.get_supported_playbook_ticker("bad!") == "QQQ"


def test_market_pulse_renders_spx_ticker_switch(client):
    resp = client.get("/market-pulse?ticker=SPX", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'href="/market-pulse?ticker=SPX"' in body
    assert 'data-playbook-ticker-switch="SPX"' in body
    assert "data-playbook-ticker-search" in body
    assert "data-playbook-ticker-input" in body
    assert 'class="marketPulseTickerSwitch is-active"' in body
    assert "SPX PLAYBOOK" in body


def test_market_pulse_accepts_custom_ticker_search(client):
    resp = client.get("/market-pulse?ticker=NVDA", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "NVDA PLAYBOOK" in body
    assert 'value="NVDA"' in body
    assert 'data-playbook-ticker-switch="QQQ"' in body
    assert 'class="marketPulseTickerSwitch is-active"' not in body


def test_scaled_gamma_snapshot_scales_levels_and_localizes_warning():
    payload = core._market_pulse_scaled_gamma_snapshot(
        ticker="SPY",
        base_gamma_snapshot={
            "spot": 5000.0,
            "spot_price_used": 5000.0,
            "gamma_flip_combined_basket": 5020.0,
            "local_flip_aggregated_gamma": 5010.0,
            "call_wall_aggregated_gamma": 5030.0,
            "put_wall_aggregated_gamma": 4980.0,
            "next_call_wall_above": 5040.0,
            "next_put_wall_below": 4970.0,
            "gamma_range_estimate": 25.0,
            "gamma_range_high": 5025.0,
            "gamma_range_low": 4975.0,
            "gamma_walls_top3": [5030.0, 5040.0],
            "void_zone": {"start": 4992.0, "end": 5006.0},
            "warnings": ["SPX snapshot warning"],
        },
        base_quote={"price": 5000.0},
        target_quote={"price": 500.0, "provider": "tradier", "as_of": "2026-04-23T15:55:00-04:00"},
    )

    assert payload["symbol"] == "SPY"
    assert payload["spot"] == 500.0
    assert payload["gamma_flip_combined_basket"] == 502.0
    assert payload["call_wall_aggregated_gamma"] == 503.0
    assert payload["put_wall_aggregated_gamma"] == 498.0
    assert payload["gamma_range_estimate"] == 2.5
    assert payload["void_zone"] == {"start": 499.2, "end": 500.6}
    assert payload["warnings"][0] == "SPY snapshot warning"
    assert any("SPY playbook levels are currently scaled" in item for item in payload["warnings"])
