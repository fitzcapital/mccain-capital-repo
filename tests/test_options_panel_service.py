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
