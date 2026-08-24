"""Derive failed-sweep evidence from completed, ordered OHLC candles."""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any, Iterable, Mapping

from mccain_capital.services.spx_strat_patterns import detect_latest_key_level_pattern


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and number > 0 else None


def _timestamp(value: Any, now: datetime) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").strip().replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=now.tzinfo)
    return parsed.astimezone(now.tzinfo)


def normalize_completed_bars(
    rows: Iterable[Mapping[str, Any]] | None,
    *,
    now: datetime,
    timeframe_minutes: int,
    session_date: str,
) -> list[dict[str, Any]]:
    """Normalize finite, completed, same-session bars in chronological order."""

    normalized: dict[str, dict[str, Any]] = {}
    for raw in rows or []:
        if not isinstance(raw, Mapping):
            continue
        stamp = _timestamp(raw.get("ts") or raw.get("timestamp") or raw.get("time"), now)
        values = {
            key: _number(raw.get(key) if raw.get(key) is not None else raw.get("v"))
            for key in ("open", "high", "low", "close")
        }
        if stamp is None or any(value is None for value in values.values()):
            continue
        if session_date and stamp.date().isoformat() != session_date:
            continue
        if stamp + timedelta(minutes=timeframe_minutes) > now:
            continue
        if values["high"] < max(values["open"], values["close"], values["low"]):
            continue
        if values["low"] > min(values["open"], values["close"], values["high"]):
            continue
        normalized[stamp.isoformat()] = {"ts": stamp.isoformat(), **values}
    return [normalized[key] for key in sorted(normalized)]


def _confirmed(bar: Mapping[str, Any] | None, timeframe: str) -> dict[str, Any] | None:
    if not bar:
        return None
    return {"confirmed": True, "timestamp": str(bar["ts"]), "provenance": timeframe}


def derive_completed_candle_evidence(
    *,
    rows_5m: Iterable[Mapping[str, Any]] | None,
    rows_15m: Iterable[Mapping[str, Any]] | None,
    now: datetime,
    ticker: str,
    session_date: str,
    level_key: str,
    level_value: Any,
    interaction: str,
) -> dict[str, Any]:
    """Derive conservative ordered evidence; ambiguity remains unavailable."""

    level = _number(level_value)
    identity = {
        "ticker": str(ticker or "").upper(),
        "session_date": session_date,
        "level_key": str(level_key or ""),
        "level_value": level,
        "interaction": interaction,
    }
    if level is None or interaction not in {"from_below", "from_above"}:
        return {"identity": identity, "bars_as_of": "", "evidence": {}}

    bars = normalize_completed_bars(
        rows_5m, now=now, timeframe_minutes=5, session_date=session_date
    )
    bars_15 = normalize_completed_bars(
        rows_15m, now=now, timeframe_minutes=15, session_date=session_date
    )
    bearish = interaction == "from_below"
    location = next(
        (bar for bar in bars if (bar["high"] >= level if bearish else bar["low"] <= level)),
        None,
    )
    start = bars.index(location) if location else len(bars)
    sweep = next(
        (bar for bar in bars[start:] if (bar["high"] > level if bearish else bar["low"] < level)),
        None,
    )
    sweep_index = bars.index(sweep) if sweep else len(bars)
    reclaim = next(
        (
            bar
            for bar in bars[sweep_index:]
            if (bar["close"] < level if bearish else bar["close"] > level)
        ),
        None,
    )
    reclaim_index = bars.index(reclaim) if reclaim else len(bars)
    pattern = None
    for index in range(max(2, reclaim_index), len(bars)):
        candidate = detect_latest_key_level_pattern(
            bars[: index + 1],
            level_key=level_key,
            level_label=level_key.replace("_", " ").title(),
            level_value=level,
            direction="bearish" if bearish else "bullish",
        )
        completed_index = next(
            (
                bar_index
                for bar_index, bar in enumerate(bars)
                if bar["ts"] == str((candidate or {}).get("completed_at") or "")
            ),
            -1,
        )
        if candidate and completed_index >= reclaim_index:
            pattern = candidate
            break
    reversal = None
    if pattern and pattern["family"] == "2-2-reversal":
        reversal = next(
            (bar for bar in bars if bar["ts"] == pattern["completed_at"]),
            None,
        )
    # A completed opposing 2 is the 2-2 reversal trigger; do not require a later candle.
    trigger = reversal

    beyond = [
        bar for bar in bars[start:] if (bar["close"] > level if bearish else bar["close"] < level)
    ]
    acceptance = beyond[1] if len(beyond) >= 2 and not reclaim else None
    retest = (
        next(
            (
                bar
                for bar in bars[bars.index(acceptance) + 1 :]
                if (
                    bar["low"] <= level <= bar["close"]
                    if bearish
                    else bar["high"] >= level >= bar["close"]
                )
            ),
            None,
        )
        if acceptance
        else None
    )

    runner = None
    if trigger:
        for index in range(2, len(bars_15)):
            if bars_15[index]["ts"] <= trigger["ts"]:
                continue
            runner_pattern = detect_latest_key_level_pattern(
                bars_15[: index + 1],
                level_key=level_key,
                level_label=level_key.replace("_", " ").title(),
                level_value=level,
                direction="bearish" if bearish else "bullish",
            )
            if runner_pattern and runner_pattern["family"] == "2-2-reversal":
                runner = bars_15[index]
                break

    evidence = {
        "location": _confirmed(location, "5m_completed_candle"),
        "sweep": _confirmed(sweep, "5m_completed_candle"),
        "close_back_inside_5m": _confirmed(reclaim, "5m_completed_candle"),
        "reversal_2_2_5m": _confirmed(reversal, "5m_completed_candle"),
        "strat_pattern_5m": pattern,
        "trigger_break": _confirmed(trigger, "5m_completed_candle"),
        "acceptance": _confirmed(acceptance, "5m_completed_candle"),
        "continuation_retest": _confirmed(retest, "5m_completed_candle"),
        "runner_2_2_15m": _confirmed(runner, "15m_completed_candle"),
    }
    return {
        "identity": identity,
        "bars_as_of": bars[-1]["ts"] if bars else "",
        "evidence": {key: value for key, value in evidence.items() if value is not None},
    }
