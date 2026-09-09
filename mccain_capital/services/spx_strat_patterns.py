"""Exact completed-candle Strat patterns anchored to canonical SPX levels."""

from __future__ import annotations

import math
from typing import Any, Iterable, Mapping


SPX_KEY_LEVEL_PROXIMITY_POINTS = 0.25


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def classify_strat_bar(previous: Mapping[str, Any], current: Mapping[str, Any]) -> str | None:
    """Classify one completed candle relative to its immediate predecessor."""

    prior_high = _number(previous.get("high"))
    prior_low = _number(previous.get("low"))
    high = _number(current.get("high"))
    low = _number(current.get("low"))
    if None in {prior_high, prior_low, high, low}:
        return None
    breaks_high = high > prior_high
    breaks_low = low < prior_low
    if breaks_high and breaks_low:
        return "3"
    if breaks_high:
        return "2U"
    if breaks_low:
        return "2D"
    return "1"


def bar_is_at_key_level(bar: Mapping[str, Any], level: float) -> bool:
    """Return whether an SPX candle reaches or narrowly tests a key level."""

    high = _number(bar.get("high"))
    low = _number(bar.get("low"))
    return (
        high is not None
        and low is not None
        and low - SPX_KEY_LEVEL_PROXIMITY_POINTS
        <= level
        <= high + SPX_KEY_LEVEL_PROXIMITY_POINTS
    )


def _pattern_payload(
    *,
    code: str,
    family: str,
    direction: str,
    bars: list[Mapping[str, Any]],
    classifications: list[str],
    level_key: str,
    level_label: str,
    level_value: float,
) -> dict[str, Any]:
    return {
        "code": code,
        "family": family,
        "direction": direction,
        "timeframe": "5m",
        "completed_at": str(bars[-1].get("ts") or bars[-1].get("timestamp") or ""),
        "bar_timestamps": [str(bar.get("ts") or bar.get("timestamp") or "") for bar in bars],
        "classifications": classifications,
        "anchor_level": {
            "key": level_key,
            "label": level_label,
            "value": level_value,
        },
        "provenance": "5m_completed_candles",
    }


def _detect_ending_key_level_pattern(
    rows: Iterable[Mapping[str, Any]],
    *,
    level_key: str,
    level_label: str,
    level_value: Any,
    direction: str | None = None,
) -> dict[str, Any] | None:
    """Return a supported pattern only when it ends on the final supplied candle."""

    bars = [dict(row) for row in rows if isinstance(row, Mapping)]
    level = _number(level_value)
    wanted_direction = str(direction or "").lower()
    if level is None or len(bars) < 3:
        return None

    if len(bars) >= 4:
        window = bars[-4:]
        classifications = [
            classify_strat_bar(window[index - 1], window[index]) for index in range(1, 4)
        ]
        final_type = classifications[-1]
        if (
            classifications[0] in {"2U", "2D"}
            and classifications[1] == "1"
            and final_type in {"2U", "2D"}
        ):
            pattern_bars = window[1:]
            pattern_direction = "bullish" if final_type == "2U" else "bearish"
            if (not wanted_direction or wanted_direction == pattern_direction) and any(
                bar_is_at_key_level(bar, level) for bar in pattern_bars
            ):
                return _pattern_payload(
                    code="2-1-2U" if pattern_direction == "bullish" else "2-1-2D",
                    family="2-1-2",
                    direction=pattern_direction,
                    bars=pattern_bars,
                    classifications=[str(value) for value in classifications],
                    level_key=level_key,
                    level_label=level_label,
                    level_value=level,
                )

    window = bars[-3:]
    classifications = [classify_strat_bar(window[index - 1], window[index]) for index in (1, 2)]
    reversal_direction = ""
    if classifications == ["2U", "2D"]:
        reversal_direction = "bearish"
    elif classifications == ["2D", "2U"]:
        reversal_direction = "bullish"
    pattern_bars = window[1:]
    if (
        reversal_direction
        and (not wanted_direction or wanted_direction == reversal_direction)
        and any(bar_is_at_key_level(bar, level) for bar in pattern_bars)
    ):
        return _pattern_payload(
            code="2-2 REV D" if reversal_direction == "bearish" else "2-2 REV U",
            family="2-2-reversal",
            direction=reversal_direction,
            bars=pattern_bars,
            classifications=[str(value) for value in classifications],
            level_key=level_key,
            level_label=level_label,
            level_value=level,
        )
    return None


def detect_latest_key_level_pattern(
    rows: Iterable[Mapping[str, Any]],
    *,
    level_key: str,
    level_label: str,
    level_value: Any,
    direction: str | None = None,
) -> dict[str, Any] | None:
    """Return the most recent supported completed pattern at one canonical level."""

    bars = [dict(row) for row in rows if isinstance(row, Mapping)]
    for end in range(len(bars), 2, -1):
        pattern = _detect_ending_key_level_pattern(
            bars[:end],
            level_key=level_key,
            level_label=level_label,
            level_value=level_value,
            direction=direction,
        )
        if pattern:
            return pattern
    return None


def detect_ending_key_level_pattern(
    rows: Iterable[Mapping[str, Any]],
    *,
    level_key: str,
    level_label: str,
    level_value: Any,
    direction: str | None = None,
) -> dict[str, Any] | None:
    """Return a supported pattern only when it ends on the latest completed candle."""

    return _detect_ending_key_level_pattern(
        rows,
        level_key=level_key,
        level_label=level_label,
        level_value=level_value,
        direction=direction,
    )
