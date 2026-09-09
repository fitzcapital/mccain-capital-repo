from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_fast_tape_status_wraps_inside_the_metadata_column():
    styles = (ROOT / "static/css/market_pulse.css").read_text(encoding="utf-8")
    rule = styles.split(
        "body.page-market-pulse .marketPulseFastTape .marketPulseMicroTapeStatus{", 1
    )[1].split("}", 1)[0]

    assert "max-width:100%;" in rule
    assert "overflow-wrap:anywhere;" in rule
    assert "white-space:normal;" in rule
