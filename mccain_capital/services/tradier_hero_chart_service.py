"""Tradier-backed SPX hero chart service.

This module keeps the hero chart plumbing isolated from the Market Pulse page
template so the frontend can consume a small, stable contract:

- initial bars
- current levels/state
- polling configuration for phase 1 live updates
"""

from __future__ import annotations

from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
import logging
from typing import Any, Dict, List, Optional

from mccain_capital import runtime as app_runtime
from mccain_capital.services import market_data_service

LOGGER = logging.getLogger(__name__)

DEFAULT_SYMBOL = "SPX"
DEFAULT_INTERVAL = "5min"
DEFAULT_BARS_LIMIT = 480
LEVEL_UNAVAILABLE = "Unavailable"
OPENING_SESSION_BAR_THRESHOLD = 10
OPENING_SESSION_CARRYOVER_MINUTES = 390
OPENING_SESSION_RIGHT_OFFSET_BARS = 6
HERO_CHART_TIMEZONE = "America/New_York"


def _as_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except Exception:
        return None


def _session_phase(now_et: datetime) -> str:
    if int(now_et.weekday()) >= 5:
        return "closed"
    minute_of_day = (int(now_et.hour) * 60) + int(now_et.minute)
    if (9 * 60 + 30) <= minute_of_day < (16 * 60):
        return "open"
    if (4 * 60) <= minute_of_day < (9 * 60 + 30):
        return "premarket"
    if (16 * 60) <= minute_of_day < (20 * 60):
        return "afterhours"
    return "closed"


def _session_label(now_et: datetime, gamma_regime: str) -> str:
    phase = _session_phase(now_et)
    if phase == "open":
        return "Regular Session"
    if phase == "premarket":
        return "Premarket"
    if phase == "afterhours":
        return "After Hours"
    if "negative" in gamma_regime.lower():
        return "Closed · Expansion risk"
    if "positive" in gamma_regime.lower():
        return "Closed · Mean reversion context"
    return "Closed · Replay context"


def _bias_label(price: Optional[float], local_flip: Optional[float]) -> str:
    if price is None or local_flip is None:
        return "Bias unavailable"
    if price > local_flip:
        return f"Bullish above Local Flip {local_flip:.0f}"
    if price < local_flip:
        return f"Bearish below Local Flip {local_flip:.0f}"
    return f"At Local Flip {local_flip:.0f}"


def _tradeability_label(gamma_regime: str, state: str) -> str:
    regime = str(gamma_regime or "").lower()
    if state == "NO_TRADE":
        return "Blocked / extension risk"
    if "negative" in regime:
        return "Trend / momentum active"
    if "positive" in regime:
        return "Responsive / mean reversion"
    return "Two-way / mixed"


def _format_level(prefix: str, value: Optional[float], *, digits: int = 0) -> str:
    numeric = _as_float(value)
    if numeric is None:
        return LEVEL_UNAVAILABLE
    return f"{prefix} {numeric:,.{digits}f}"


def _parse_ts(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=app_runtime.TZ)
    return parsed.astimezone(app_runtime.TZ)


def _interval_minutes(interval: str) -> int:
    return 5 if str(interval or DEFAULT_INTERVAL).strip().lower() == "5min" else 1


def _regular_session_target_bar_count(interval: str) -> int:
    interval_minutes = max(1, _interval_minutes(interval))
    return int(390 / interval_minutes) + 1


def _is_regular_session_dt(dt: datetime) -> bool:
    if int(dt.weekday()) >= 5:
        return False
    clock = dt.timetz().replace(tzinfo=None)
    return time(9, 30) <= clock < time(16, 0)


def _previous_trading_day(anchor_day: date) -> date:
    out = anchor_day - timedelta(days=1)
    while out.weekday() >= 5:
        out -= timedelta(days=1)
    return out


def _bars_for_session_day(
    bars: List[Dict[str, Any]], *, session_day: date, regular_only: bool = True
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for bar in bars:
        ts = _as_float(bar.get("time"))
        if ts is None:
            continue
        dt = datetime.fromtimestamp(int(ts), tz=app_runtime.TZ)
        if dt.date() != session_day:
            continue
        if regular_only and not _is_regular_session_dt(dt):
            continue
        out.append(dict(bar))
    return out


def _regular_session_bars_for_anchor_day(
    *,
    current_bars: List[Dict[str, Any]],
    prior_bars: List[Dict[str, Any]],
    anchor_day: date,
) -> List[Dict[str, Any]]:
    """Return the best available cash-session-only bar set for the hero chart.

    SPX does not have actionable extended-hours trading for this view, so the
    hero chart should always anchor to regular-session bars in New York market
    time. Prefer the current session day when it has regular bars; otherwise
    fall back to the prior valid trading session.
    """

    current_regular = _bars_for_session_day(current_bars, session_day=anchor_day, regular_only=True)
    if current_regular:
        return current_regular
    prior_session_day = _previous_trading_day(anchor_day)
    return _bars_for_session_day(prior_bars, session_day=prior_session_day, regular_only=True)


def _two_session_regular_bars(
    *,
    current_bars: List[Dict[str, Any]],
    prior_bars: List[Dict[str, Any]],
    anchor_day: date,
    interval: str = DEFAULT_INTERVAL,
) -> Dict[str, Any]:
    """Return exactly the prior regular session plus the current regular session.

    The hero chart is an execution view, not a multi-day replay. We keep one
    prior day for gap/session context and the current session for live decision
    making. When the current regular session has not started yet, the payload
    falls back to the prior regular session only.
    """

    prior_session_day = _previous_trading_day(anchor_day)
    prior_regular = _bars_for_session_day(prior_bars, session_day=prior_session_day, regular_only=True)
    current_regular = _bars_for_session_day(current_bars, session_day=anchor_day, regular_only=True)
    combined = [dict(bar) for bar in prior_regular] + [dict(bar) for bar in current_regular]
    if not combined:
        combined = list(current_regular or prior_regular)
    return {
        "bars": combined,
        "previous_session_bar_count": len(prior_regular),
        "current_session_bar_count": len(current_regular),
        "previous_session_day": prior_session_day.isoformat() if prior_regular else "",
        "current_session_day": anchor_day.isoformat() if current_regular else "",
        "visible_window_bars": len(combined),
        "session_target_bar_count": _regular_session_target_bar_count(interval) if current_regular else 0,
    }


def _opening_session_carryover_bars(
    *,
    current_bars: List[Dict[str, Any]],
    prior_bars: List[Dict[str, Any]],
    session_day: date,
    interval: str,
) -> Dict[str, Any]:
    # During the opening minutes, prepend the last slice of the prior regular
    # session so the hero chart has context instead of auto-fitting to 1-2 bars.
    threshold = OPENING_SESSION_BAR_THRESHOLD
    carryover_target = max(
        1,
        int(OPENING_SESSION_CARRYOVER_MINUTES / max(1, _interval_minutes(interval))),
    )
    current_regular = _bars_for_session_day(current_bars, session_day=session_day, regular_only=True)
    live_count = len(current_regular)
    if live_count <= 0 or live_count >= threshold:
        return {
            "bars": list(current_regular or current_bars),
            "opening_session_mode": False,
            "live_session_bar_count": live_count,
            "opening_threshold": threshold,
            "carryover_bar_count": 0,
            "visible_window_bars": len(current_regular or current_bars),
            "right_offset_bars": OPENING_SESSION_RIGHT_OFFSET_BARS,
            "previous_session_bar_count": 0,
            "current_session_bar_count": live_count,
            "previous_session_day": "",
            "current_session_day": session_day.isoformat() if current_regular else "",
            "session_target_bar_count": _regular_session_target_bar_count(interval) if current_regular else 0,
        }

    prior_session_day = _previous_trading_day(session_day)
    prior_regular = _bars_for_session_day(prior_bars, session_day=prior_session_day, regular_only=True)
    carryover = prior_regular[-carryover_target:] if prior_regular else []
    combined = [dict(bar) for bar in carryover] + [dict(bar) for bar in current_regular]
    return {
        "bars": combined,
        "opening_session_mode": True,
        "live_session_bar_count": live_count,
        "opening_threshold": threshold,
        "carryover_bar_count": len(carryover),
        "visible_window_bars": max(len(combined), threshold + len(carryover) + OPENING_SESSION_RIGHT_OFFSET_BARS),
        "right_offset_bars": OPENING_SESSION_RIGHT_OFFSET_BARS,
        "previous_session_bar_count": len(carryover),
        "current_session_bar_count": live_count,
        "previous_session_day": prior_session_day.isoformat() if carryover else "",
        "current_session_day": session_day.isoformat() if current_regular else "",
        "session_target_bar_count": _regular_session_target_bar_count(interval) if current_regular else 0,
    }


def _synthetic_quote_bar(
    *,
    price: Optional[float],
    now_et: datetime,
    interval: str,
) -> List[Dict[str, Any]]:
    numeric = _as_float(price)
    if numeric is None or numeric <= 0:
        return []
    interval_minutes = max(1, _interval_minutes(interval))
    floored_minute = now_et.minute - (now_et.minute % interval_minutes)
    bucket_dt = now_et.replace(minute=floored_minute, second=0, microsecond=0)
    return [
        {
            "time": int(bucket_dt.timestamp()),
            "open": numeric,
            "high": numeric,
            "low": numeric,
            "close": numeric,
            "volume": 0.0,
        }
    ]


def normalize_tradier_timesales(
    rows: List[Dict[str, Any]], *, interval: str = DEFAULT_INTERVAL, limit: int = DEFAULT_BARS_LIMIT
) -> List[Dict[str, Any]]:
    """Aggregate provider rows into Lightweight Charts bar format.

    Existing market_data_service rows already come from Tradier timesales.
    We floor timestamps to 5-minute buckets for a stable frontend contract.
    """

    interval_minutes = _interval_minutes(interval)
    bucketed: Dict[int, Dict[str, Any]] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue
        dt = _parse_ts(row.get("ts"))
        if dt is None:
            continue
        floored_minute = dt.minute - (dt.minute % interval_minutes)
        bucket_dt = dt.replace(minute=floored_minute, second=0, microsecond=0)
        bucket_ts = int(bucket_dt.timestamp())

        open_ = _as_float(row.get("open"))
        high = _as_float(row.get("high"))
        low = _as_float(row.get("low"))
        close = _as_float(row.get("close"))
        volume = _as_float(row.get("volume")) or 0.0
        if open_ is None or high is None or low is None or close is None:
            continue

        current = bucketed.get(bucket_ts)
        if current is None:
            bucketed[bucket_ts] = {
                "time": bucket_ts,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
            continue

        current["high"] = max(float(current["high"]), high)
        current["low"] = min(float(current["low"]), low)
        current["close"] = close
        current["volume"] = float(current.get("volume") or 0.0) + volume

    bars = [bucketed[key] for key in sorted(bucketed.keys())]
    if limit > 0:
        bars = bars[-limit:]
    return bars


def get_intraday_bars(symbol: str = DEFAULT_SYMBOL, interval: str = DEFAULT_INTERVAL) -> Dict[str, Any]:
    try:
        rows = market_data_service.get_intraday(symbol)
    except Exception as exc:
        LOGGER.warning("hero chart intraday fetch failed for %s: %s", symbol, exc)
        rows = []
    normalized_current = normalize_tradier_timesales(list(rows or []), interval=interval)
    now_et = app_runtime.now_et()
    symbol_name = str(symbol or DEFAULT_SYMBOL).strip().upper() or DEFAULT_SYMBOL
    payload: Dict[str, Any] = {
        "symbol": symbol_name,
        "interval": interval,
        "bars": normalized_current,
        "opening_session_mode": False,
        "live_session_bar_count": 0,
        "opening_threshold": OPENING_SESSION_BAR_THRESHOLD,
        "carryover_bar_count": 0,
        "visible_window_bars": len(normalized_current),
        "right_offset_bars": OPENING_SESSION_RIGHT_OFFSET_BARS,
        "time_zone": HERO_CHART_TIMEZONE,
        "previous_session_bar_count": 0,
        "current_session_bar_count": 0,
        "previous_session_day": "",
        "current_session_day": "",
        "session_target_bar_count": 0,
    }

    try:
        prior_rows = market_data_service.get_prior_session_intraday(symbol_name, anchor_session_day=now_et.date())
    except Exception as exc:
        LOGGER.warning("hero chart prior-session fetch failed for %s: %s", symbol_name, exc)
        prior_rows = []
    normalized_prior = normalize_tradier_timesales(list(prior_rows or []), interval=interval)

    if _session_phase(now_et) != "open":
        two_session_payload = _two_session_regular_bars(
            current_bars=normalized_current,
            prior_bars=normalized_prior,
            anchor_day=now_et.date(),
            interval=interval,
        )
        payload.update(two_session_payload)
        return payload

    if not normalized_current:
        quote_price = _as_float(get_live_quote(symbol_name).get("price"))
        synthetic_current = _synthetic_quote_bar(price=quote_price, now_et=now_et, interval=interval)
        payload.update(
            _two_session_regular_bars(
                current_bars=synthetic_current,
                prior_bars=normalized_prior,
                anchor_day=now_et.date(),
                interval=interval,
            )
        )
        return payload

    framing = _opening_session_carryover_bars(
        current_bars=normalized_current,
        prior_bars=normalized_prior,
        session_day=now_et.date(),
        interval=interval,
    )
    payload.update(framing)
    return payload


def get_live_quote(symbol: str = DEFAULT_SYMBOL) -> Dict[str, Any]:
    try:
        quote = dict((market_data_service.get_watchlist_tradier([symbol]).get(symbol) or {}))
    except Exception as exc:
        LOGGER.warning("hero chart quote fetch failed for %s: %s", symbol, exc)
        quote = {}
    return {
        "symbol": str(symbol or DEFAULT_SYMBOL).strip().upper() or DEFAULT_SYMBOL,
        "price": _as_float(quote.get("price")),
        "pct_change": _as_float(quote.get("pct_change")),
        "provider": str(quote.get("provider") or "tradier"),
        "reason": str(quote.get("reason") or ""),
        "as_of": str(quote.get("as_of") or ""),
    }


def derive_hero_state(
    price: Optional[float],
    local_flip: Optional[float],
    call_wall: Optional[float],
    put_wall: Optional[float],
    ncw: Optional[float] = None,
    npw: Optional[float] = None,
) -> Dict[str, str]:
    """Single source of truth for hero state in phase 1.

    JS renders this payload; it does not re-derive the state.
    """

    price = _as_float(price)
    local_flip = _as_float(local_flip)
    call_wall = _as_float(call_wall)
    put_wall = _as_float(put_wall)
    ncw = _as_float(ncw)
    npw = _as_float(npw)

    if price is None:
        return {
            "state": "WATCH",
            "current_read": "Transition",
            "pullback_level": "Nearest level",
            "next_destination": "Await structure",
            "plan_note": "Spot is unavailable. Wait for live price and levels.",
            "best_look": "Wait for live structure",
            "required_trigger": "Await data",
            "invalidation": "No active structure",
        }

    if call_wall is not None and price > call_wall:
        return {
            "state": "NO_TRADE",
            "current_read": "Above Call Wall",
            "pullback_level": _format_level("CW", call_wall),
            "next_destination": _format_level("NCW", ncw) if ncw is not None else "Expansion zone",
            "plan_note": "Above call wall. Momentum can continue, but the only valid long is a confirmed retest.",
            "best_look": "Wait for pullback into Call Wall",
            "required_trigger": "Sweep + reclaim + 2-2 + volume",
            "invalidation": _format_level("Lose LF", local_flip) if local_flip is not None else _format_level("Lose CW", call_wall),
        }

    if local_flip is not None and call_wall is not None and local_flip < price <= call_wall:
        state = "READY" if abs(price - local_flip) <= 6 else "WAIT"
        return {
            "state": state,
            "current_read": "Above Local Flip",
            "pullback_level": _format_level("LF", local_flip),
            "next_destination": _format_level("CW", call_wall),
            "plan_note": "Bullish above local flip. Use dip + confirmation. Do not chase.",
            "best_look": "Buy dip after sweep + reclaim",
            "required_trigger": "Sweep + reclaim + 2-2 + volume",
            "invalidation": _format_level("Lose LF", local_flip),
        }

    if local_flip is not None and price < local_flip:
        state = "READY" if abs(price - local_flip) <= 6 else "WAIT"
        destination = (
            _format_level("PW", put_wall)
            if put_wall is not None and price > put_wall
            else _format_level("NPW", npw)
            if npw is not None
            else "Downside expansion"
        )
        return {
            "state": state,
            "current_read": "Below Local Flip",
            "pullback_level": _format_level("LF", local_flip),
            "next_destination": destination,
            "plan_note": "Below local flip. Look for failed bounce or downside continuation.",
            "best_look": "Sell failed bounce below Local Flip",
            "required_trigger": "Pop + fail + 2-2 + volume",
            "invalidation": _format_level("Reclaim LF", local_flip),
        }

    return {
        "state": "WATCH",
        "current_read": "Transition",
        "pullback_level": "Nearest level",
        "next_destination": "Await structure",
        "plan_note": "Wait for clearer structure.",
        "best_look": "Wait for clean structure",
        "required_trigger": "Confirmation required",
        "invalidation": "Reset if level logic breaks",
    }


def get_hero_levels(
    symbol: str = DEFAULT_SYMBOL,
    *,
    playbook_snapshot: Optional[Dict[str, Any]] = None,
    now_et: Optional[datetime] = None,
) -> Dict[str, Any]:
    resolved_symbol = str(symbol or DEFAULT_SYMBOL).strip().upper() or DEFAULT_SYMBOL
    current_now = now_et or app_runtime.now_et()

    if playbook_snapshot is None:
        from mccain_capital.services import core

        playbook_snapshot = core.get_or_build_market_pulse_snapshot(
            force_refresh=False,
            now_et=current_now,
        )

    snapshot = dict(playbook_snapshot or {})
    quotes = list(snapshot.get("quotes") or [])
    spx_quote = next(
        (
            q
            for q in quotes
            if str(q.get("symbol") or q.get("label") or "").upper() == resolved_symbol
        ),
        dict(snapshot.get("spx_quote") or {}),
    )
    structure_snapshot = dict(snapshot.get("market_structure_snapshot") or {})

    return {
        "symbol": resolved_symbol,
        "as_of": structure_snapshot.get("snapshot_timestamp") or app_runtime.now_iso(),
        "spot": structure_snapshot.get("spot"),
        "session_mode": structure_snapshot.get("session_mode"),
        "session_mode_label": structure_snapshot.get("session_mode_label"),
        "levels_source": structure_snapshot.get("levels_source"),
        "levels_source_label": structure_snapshot.get("levels_source_label"),
        "gamma_data_status": structure_snapshot.get("gamma_data_status"),
        "gamma_data_status_label": structure_snapshot.get("gamma_data_status_label"),
        "main_flip": structure_snapshot.get("main_flip"),
        "local_flip": structure_snapshot.get("local_flip"),
        "call_wall": structure_snapshot.get("call_wall"),
        "put_wall": structure_snapshot.get("put_wall"),
        "next_call_wall": structure_snapshot.get("next_call_wall"),
        "next_put_wall": structure_snapshot.get("next_put_wall"),
        "gamma_regime": structure_snapshot.get("gamma_regime"),
        "gamma_regime_label": structure_snapshot.get("gamma_regime_label"),
        "gamma_regime_subtitle": structure_snapshot.get("gamma_regime_subtitle"),
        "regime_confidence": structure_snapshot.get("regime_confidence"),
        "regime_confidence_label": structure_snapshot.get("regime_confidence_label"),
        "execution_regime": structure_snapshot.get("execution_regime"),
        "execution_regime_label": structure_snapshot.get("execution_regime_label"),
        "planning_bias": structure_snapshot.get("planning_bias"),
        "planning_bias_label": structure_snapshot.get("planning_bias_label"),
        "bias_state": structure_snapshot.get("bias_state"),
        "bias_context": structure_snapshot.get("bias_context"),
        "bias_label": structure_snapshot.get("bias_label"),
        "bias": structure_snapshot.get("bias"),
        "tradeability": structure_snapshot.get("tradeability"),
        "context_grade": structure_snapshot.get("context_grade"),
        "context_score": structure_snapshot.get("context_score"),
        "context_score_pct": structure_snapshot.get("context_score_pct"),
        "context_tone": structure_snapshot.get("context_tone"),
        "context_status": structure_snapshot.get("context_status"),
        "app_state": structure_snapshot.get("app_state"),
        "app_state_label": structure_snapshot.get("app_state_label"),
        "spot_meta": structure_snapshot.get("spot_meta"),
        "spot_source_short_label": structure_snapshot.get("spot_source_short_label"),
        "local_flip_meta": structure_snapshot.get("local_flip_meta"),
        "level_meta": structure_snapshot.get("level_meta"),
        "gamma_regime_meta": structure_snapshot.get("gamma_regime_meta"),
        "chart_meta": structure_snapshot.get("chart_meta"),
        "session": structure_snapshot.get("session"),
        "state": structure_snapshot.get("trade_state"),
        "trade_state_label": structure_snapshot.get("trade_state_label"),
        "current_read": structure_snapshot.get("current_read"),
        "pullback_level": structure_snapshot.get("pullback_level"),
        "next_destination": structure_snapshot.get("next_destination"),
        "plan_note": structure_snapshot.get("plan_note"),
        "best_look": structure_snapshot.get("best_look"),
        "required_trigger": structure_snapshot.get("required_trigger"),
        "invalidation": structure_snapshot.get("invalidation"),
        "trigger_validation": structure_snapshot.get("trigger_validation"),
        "provider": str(spx_quote.get("provider") or "market_snapshot"),
        "snapshot_timestamp": structure_snapshot.get("snapshot_timestamp"),
        "snapshot_timestamp_label": structure_snapshot.get("snapshot_timestamp_label"),
        "last_valid_snapshot_time": structure_snapshot.get("last_valid_snapshot_time"),
        "last_valid_snapshot_time_label": structure_snapshot.get("last_valid_snapshot_time_label"),
        "last_valid_snapshot_usable": structure_snapshot.get("last_valid_snapshot_usable"),
        "last_valid_snapshot_reason": structure_snapshot.get("last_valid_snapshot_reason"),
        "last_valid_snapshot_age_seconds": structure_snapshot.get("last_valid_snapshot_age_seconds"),
        "posture_summary": dict(snapshot.get("execution_model") or {}).get("posture_summary") or "",
    }


def get_stream_session_payload() -> Dict[str, Any]:
    """Phase 1 uses polling instead of direct browser streaming."""

    return {
        "mode": "polling",
        "enabled": False,
        "symbol": DEFAULT_SYMBOL,
        "bars_interval_ms": 30000,
        "levels_interval_ms": 10000,
    }
