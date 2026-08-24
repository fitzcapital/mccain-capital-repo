from datetime import datetime
from datetime import timedelta
from zoneinfo import ZoneInfo

from mccain_capital.services.market_pulse_reliability import (
    choose_newest_context,
    load_shared_context,
    promote_shared_context,
    validate_context_payload,
)


ET = ZoneInfo("America/New_York")


def _payload(generated_at: datetime, generation: str, *, ticker: str = "SPX") -> dict:
    stamp = generated_at.isoformat()
    return {
        "ticker": ticker,
        "canonical_freshness": {
            "generation_id": generation,
            "generated_at": stamp,
            "symbol": ticker,
            "session_id": generated_at.date().isoformat(),
            "components": {
                name: {"as_of": stamp, "status": "current"}
                for name in ("spot", "bars", "gamma")
            },
        },
    }


def test_context_validation_rejects_symbol_and_missing_component_timestamp():
    now = datetime(2026, 8, 20, 11, 0, tzinfo=ET)
    payload = _payload(now, "generation-a", ticker="QQQ")
    valid, problems = validate_context_payload(payload, ticker="SPX")
    assert valid is False
    assert "symbol" in problems

    payload = _payload(now, "generation-b")
    payload["canonical_freshness"]["components"]["gamma"]["as_of"] = ""
    valid, problems = validate_context_payload(payload, ticker="SPX")
    assert valid is False
    assert "gamma" in problems


def test_shared_context_promotion_is_atomic_and_monotonic(tmp_path):
    now = datetime(2026, 8, 20, 11, 0, tzinfo=ET)
    path = str(tmp_path / "context.json")
    newest = _payload(now, "generation-new")
    older = _payload(now - timedelta(minutes=1), "generation-old")

    assert promote_shared_context(path, newest, ticker="SPX") is True
    assert promote_shared_context(path, older, ticker="SPX") is False
    assert load_shared_context(path, ticker="SPX") == newest


def test_worker_selection_adopts_newest_valid_generation():
    now = datetime(2026, 8, 20, 11, 0, tzinfo=ET)
    older = _payload(now - timedelta(seconds=20), "generation-old")
    newer = _payload(now, "generation-new")

    assert choose_newest_context(older, newer, ticker="SPX") == newer


def test_invalid_candidate_cannot_replace_last_verified_generation():
    now = datetime(2026, 8, 20, 11, 0, tzinfo=ET)
    verified = _payload(now, "generation-verified")
    invalid = _payload(now + timedelta(seconds=20), "generation-invalid")
    invalid["canonical_freshness"]["components"]["gamma"]["as_of"] = ""

    assert choose_newest_context(verified, invalid, ticker="SPX") == verified
