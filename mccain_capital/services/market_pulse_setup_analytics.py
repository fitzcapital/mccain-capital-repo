"""Durable, read-only analytics for canonical Market Pulse setup events."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
import json
import math
import statistics
import threading
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from mccain_capital.runtime import db
from mccain_capital.services.market_pulse_option_projection import premium_anchors
from mccain_capital.services.market_session_calendar import is_session_day

ET = ZoneInfo("America/New_York")
TERMINAL_OUTCOMES = {"target_reached", "invalidated", "ambiguous"}
VALID_OUTCOMES = TERMINAL_OUTCOMES | {"open", "unavailable"}
OUTCOME_RANK = {"unavailable": 0, "open": 1, "target_reached": 2, "invalidated": 2, "ambiguous": 2}
VALID_DIRECTIONS = {"bullish", "bearish"}
VALID_SORTS = {
    "signal_desc": "signal_time DESC",
    "signal_asc": "signal_time ASC",
    "score_desc": "score DESC, signal_time DESC",
    "mfe_desc": "mfe DESC, signal_time DESC",
}
MAX_RANGE_DAYS = 180
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
ESTIMATED_CONTRACT_COST = 750.0
ESTIMATED_ABSOLUTE_DELTA = 0.40
OPTION_CONTRACT_MULTIPLIER = 100
PLANNED_TP_RETURN_RANGE = (15, 20)
VALID_PRESETS = {
    "today",
    "last_3_sessions",
    "this_week",
    "last_20_sessions",
    "all_history",
    "custom",
}
_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, dict[str, Any]] = {}


class AnalyticsFilterError(ValueError):
    """Raised when an analytics filter is invalid."""


def _text(value: Any) -> str:
    return str(value or "").strip()


def _float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _timestamp(value: Any) -> datetime | None:
    raw = _text(value)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ET)
    return parsed.astimezone(ET)


def _mapping(*values: Any) -> dict[str, Any]:
    for value in values:
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _outcome_payload(source: Mapping[str, Any]) -> dict[str, Any]:
    outcome = _mapping(source.get("outcome"))
    state = _text(outcome.get("state") or source.get("outcome_state") or "unavailable").lower()
    return {
        "state": state if state in VALID_OUTCOMES else "unavailable",
        "at": _text(outcome.get("at") or source.get("resolution_time")),
        "evaluated_through": _text(
            outcome.get("evaluated_through") or source.get("evaluated_through")
        ),
        "mfe": _float(outcome.get("mfe") if "mfe" in outcome else source.get("mfe")),
        "mae": _float(outcome.get("mae") if "mae" in outcome else source.get("mae")),
        "target_progress_percent": _float(
            outcome.get("target_progress_percent")
            if "target_progress_percent" in outcome
            else source.get("target_progress_percent")
        ),
    }


def canonical_record(source: Mapping[str, Any]) -> dict[str, Any] | None:
    event_id = _text(source.get("setup_event_id"))
    signal_time = _text(source.get("signal_time") or source.get("evidence_at"))
    signal_dt = _timestamp(signal_time)
    if not event_id or signal_dt is None:
        return None
    level = _mapping(source.get("level"))
    target = _mapping(source.get("target"), source.get("target_level"))
    pattern = _mapping(source.get("strat_pattern"))
    outcome = _outcome_payload(source)
    evidence = {
        "entry_basis": source.get("entry_basis"),
        "confirmation": source.get("confirmation") or source.get("trigger"),
        "invalidation": source.get("invalidation"),
        "data_availability": source.get("data_availability") or {},
        "trigger_evidence": source.get("trigger_evidence") or {},
        "supporting_levels": source.get("supporting_levels") or [],
        "location_event": source.get("location_event"),
    }
    return {
        "setup_event_id": event_id,
        "ticker": _text(source.get("ticker") or "SPX").upper(),
        "session_date": _text(source.get("session_date") or signal_dt.date().isoformat()),
        "signal_time": signal_dt.isoformat(),
        "signal_candle_time": _text(source.get("signal_candle_time")),
        "resolution_time": outcome["at"],
        "evaluated_through": outcome["evaluated_through"],
        "family": _text(source.get("family")),
        "family_label": _text(source.get("family_label")),
        "direction": _text(source.get("direction")).lower(),
        "pattern_code": _text(pattern.get("code")),
        "pattern_family": _text(pattern.get("family")),
        "level_key": _text(level.get("key")),
        "level_label": _text(level.get("label")),
        "level_value": _float(level.get("value")),
        "entry_value": _float(source.get("entry_zone") or source.get("entry_value")),
        "target_key": _text(target.get("key")),
        "target_label": _text(target.get("label")),
        "target_value": _float(target.get("value")),
        "score": int(_float(source.get("score")) or 0),
        "grade": _text(source.get("grade")),
        "outcome_state": outcome["state"],
        "mfe": outcome["mfe"],
        "mae": outcome["mae"],
        "target_progress_percent": outcome["target_progress_percent"],
        "evidence_json": json.dumps(evidence, separators=(",", ":"), default=str),
        "source_revision": int(_float(source.get("revision")) or 1),
    }


def upsert_records(sources: Iterable[Mapping[str, Any]]) -> int:
    records = [record for source in sources if (record := canonical_record(source))]
    if not records:
        return 0
    changed = 0
    now = datetime.now(ET).isoformat()
    with db() as conn:
        for record in records:
            existing = conn.execute(
                "SELECT outcome_state, source_revision FROM market_pulse_setup_events "
                "WHERE setup_event_id = ?",
                (record["setup_event_id"],),
            ).fetchone()
            if existing is None:
                columns = [*record, "created_at", "updated_at"]
                values = [record[key] for key in record] + [now, now]
                placeholders = ",".join("?" for _ in columns)
                conn.execute(
                    f"INSERT INTO market_pulse_setup_events ({','.join(columns)}) "
                    f"VALUES ({placeholders})",
                    values,
                )
                changed += 1
                continue
            previous_state = _text(existing["outcome_state"]).lower()
            next_state = record["outcome_state"]
            if previous_state in TERMINAL_OUTCOMES and next_state != previous_state:
                continue
            if OUTCOME_RANK.get(next_state, 0) < OUTCOME_RANK.get(previous_state, 0):
                continue
            conn.execute(
                """
                UPDATE market_pulse_setup_events
                SET resolution_time = CASE WHEN ? != '' THEN ? ELSE resolution_time END,
                    evaluated_through = CASE WHEN ? != '' THEN ? ELSE evaluated_through END,
                    outcome_state = ?, mfe = COALESCE(?, mfe), mae = COALESCE(?, mae),
                    target_progress_percent = COALESCE(?, target_progress_percent),
                    source_revision = ?, updated_at = ?
                WHERE setup_event_id = ?
                """,
                (
                    record["resolution_time"],
                    record["resolution_time"],
                    record["evaluated_through"],
                    record["evaluated_through"],
                    next_state,
                    record["mfe"],
                    record["mae"],
                    record["target_progress_percent"],
                    max(int(existing["source_revision"] or 1), record["source_revision"]),
                    now,
                    record["setup_event_id"],
                ),
            )
            changed += 1
    if changed:
        with _CACHE_LOCK:
            _CACHE.clear()
    return changed


def backfill_ledger(path: str) -> int:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, TypeError, ValueError):
        return 0
    sources = [
        row
        for row in dict(payload.get("setups") or {}).values()
        if isinstance(row, Mapping) and row.get("setup_event_id")
    ]
    return upsert_records(sources)


def _parse_date(value: Any, label: str) -> date | None:
    raw = _text(value)
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise AnalyticsFilterError(f"Invalid {label}.") from exc


def _parse_clock(value: Any, label: str) -> time | None:
    raw = _text(value)
    if not raw:
        return None
    try:
        return time.fromisoformat(raw)
    except ValueError as exc:
        raise AnalyticsFilterError(f"Invalid {label}.") from exc


def _session_start(end: date, count: int) -> date:
    found: list[date] = []
    cursor = end
    for _ in range(370):
        if is_session_day(cursor):
            found.append(cursor)
            if len(found) >= count:
                return cursor
        cursor -= timedelta(days=1)
    raise AnalyticsFilterError("Unable to resolve exchange sessions.")


def _preset_dates(preset: str, today: date | None = None) -> tuple[date, date]:
    current = today or datetime.now(ET).date()
    if preset == "last_3_sessions":
        return _session_start(current, 3), current
    if preset == "last_20_sessions":
        return _session_start(current, 20), current
    if preset == "this_week":
        return current - timedelta(days=current.weekday()), current
    if preset == "all_history":
        return date(2000, 1, 1), current
    return current, current


def normalize_filters(values: Mapping[str, Any]) -> dict[str, Any]:
    requested_preset = _text(values.get("preset")).lower()
    has_explicit_dates = bool(_text(values.get("start_date")) or _text(values.get("end_date")))
    preset = requested_preset or ("custom" if has_explicit_dates else "today")
    if preset not in VALID_PRESETS:
        raise AnalyticsFilterError("Invalid study preset.")
    if preset == "custom":
        end = _parse_date(values.get("end_date"), "end date") or datetime.now(ET).date()
        start = _parse_date(values.get("start_date"), "start date") or end
    else:
        start, end = _preset_dates(preset)
    if start > end:
        raise AnalyticsFilterError("Start date must not be after end date.")
    if preset != "all_history" and (end - start).days > MAX_RANGE_DAYS:
        raise AnalyticsFilterError(f"Date range cannot exceed {MAX_RANGE_DAYS} days.")
    start_time = _parse_clock(values.get("start_time"), "start time") or time(9, 30)
    end_time = _parse_clock(values.get("end_time"), "end time") or time(16, 0)
    if start_time > end_time:
        raise AnalyticsFilterError("Start time must not be after end time.")
    direction = _text(values.get("direction")).lower()
    if direction and direction not in VALID_DIRECTIONS:
        raise AnalyticsFilterError("Invalid direction.")
    outcome = _text(values.get("outcome")).lower()
    if outcome and outcome not in VALID_OUTCOMES:
        raise AnalyticsFilterError("Invalid outcome.")
    sort = _text(values.get("sort") or "signal_desc")
    if sort not in VALID_SORTS:
        raise AnalyticsFilterError("Invalid sort.")
    try:
        page = max(1, int(values.get("page") or 1))
        page_size = int(values.get("page_size") or DEFAULT_PAGE_SIZE)
    except (TypeError, ValueError) as exc:
        raise AnalyticsFilterError("Invalid pagination.") from exc
    if page_size < 1 or page_size > MAX_PAGE_SIZE:
        raise AnalyticsFilterError(f"Page size must be between 1 and {MAX_PAGE_SIZE}.")
    return {
        "ticker": _text(values.get("ticker") or "SPX").upper(),
        "preset": preset,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "start_time": start_time.strftime("%H:%M"),
        "end_time": end_time.strftime("%H:%M"),
        "family": _text(values.get("family")),
        "pattern": _text(values.get("pattern")),
        "direction": direction,
        "level": _text(values.get("level")),
        "grade": _text(values.get("grade")),
        "outcome": outcome,
        "sort": sort,
        "page": page,
        "page_size": page_size,
    }


def _median(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return {"value": statistics.median(values) if values else None, "sample_size": len(values)}


def _bucket_label(stamp: str) -> str:
    parsed = _timestamp(stamp)
    if parsed is None:
        return "Unavailable"
    minute = 30 if parsed.minute >= 30 else 0
    start = parsed.replace(minute=minute, second=0, microsecond=0)
    end = start + timedelta(minutes=29)
    return f"{start.strftime('%-I:%M')}–{end.strftime('%-I:%M %p')}"


def _bucket_sort_key(label: str) -> datetime:
    raw = label.split("–", 1)[0].strip()
    parsed = datetime.strptime(raw, "%I:%M")
    if parsed.hour < 8:
        parsed = parsed.replace(hour=parsed.hour + 12)
    return parsed


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator * 100, 1) if denominator else None


def _ranked_insight(rows: list[dict[str, Any]], label_key: str) -> dict[str, Any] | None:
    eligible = [row for row in rows if row["evaluated_count"]]
    if not eligible:
        return None
    best = max(
        eligible,
        key=lambda row: (
            row["target_reached_rate"],
            row["evaluated_count"],
            row["total_occurrences"],
        ),
    )
    return {
        "label": best[label_key],
        "target_reached_rate": best["target_reached_rate"],
        "target_reached_count": best["target_reached_count"],
        "evaluated_count": best["evaluated_count"],
        "total_occurrences": best["total_occurrences"],
        "early_evidence": best["evaluated_count"] < 3,
    }


def _row_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    try:
        payload["evidence"] = json.loads(_text(payload.pop("evidence_json", "{}")) or "{}")
    except ValueError:
        payload["evidence"] = {}
    payload["replay_url"] = (
        f"/market-pulse?ticker={payload['ticker']}&session_date={payload['session_date']}"
        f"#marketPulseSetupReplay"
    )
    return payload


def analytics_payload(values: Mapping[str, Any]) -> dict[str, Any]:
    filters = normalize_filters(values)
    anchors = premium_anchors()
    clauses = ["ticker = ?", "session_date BETWEEN ? AND ?"]
    params: list[Any] = [filters["ticker"], filters["start_date"], filters["end_date"]]
    for field, column in (
        ("family", "family"),
        ("pattern", "pattern_code"),
        ("direction", "direction"),
        ("level", "level_key"),
        ("grade", "grade"),
        ("outcome", "outcome_state"),
    ):
        if filters[field]:
            clauses.append(f"{column} = ?")
            params.append(filters[field])
    where = " AND ".join(clauses)
    with db() as conn:
        revision = conn.execute(
            "SELECT COUNT(*) AS n, COALESCE(MAX(updated_at), '') AS updated "
            "FROM market_pulse_setup_events"
        ).fetchone()
        cache_key = json.dumps(filters, sort_keys=True)
        anchor_revision = ":".join(
            str(anchors[key].get("as_of") or anchors[key].get("fallback_reason") or "")
            for key in ("bullish", "bearish")
        )
        cache_revision = f"{revision['n']}:{revision['updated']}:{anchor_revision}"
        with _CACHE_LOCK:
            cached = _CACHE.get(cache_key)
            if cached and cached.get("revision") == cache_revision:
                return dict(cached["payload"])
        raw_rows = [
            dict(row)
            for row in conn.execute(
                f"SELECT * FROM market_pulse_setup_events WHERE {where} "
                f"ORDER BY {VALID_SORTS[filters['sort']]}",
                params,
            ).fetchall()
        ]
    rows = []
    for row in raw_rows:
        parsed = _timestamp(row["signal_time"])
        if parsed is None:
            continue
        clock = parsed.strftime("%H:%M")
        if filters["start_time"] <= clock <= filters["end_time"]:
            rows.append(row)
    total = len(rows)
    sessions = sorted({row["session_date"] for row in rows})
    terminal = [row for row in rows if row["outcome_state"] in TERMINAL_OUTCOMES]
    target_count = sum(row["outcome_state"] == "target_reached" for row in terminal)
    invalidated_count = sum(row["outcome_state"] == "invalidated" for row in terminal)
    by_session = Counter(row["session_date"] for row in rows)
    by_time: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_time[_bucket_label(row["signal_time"])].append(row)
        by_family[row["family_label"] or row["family"]].append(row)

    def comparison(label: str, group: list[dict[str, Any]], label_key: str) -> dict[str, Any]:
        evaluated = [row for row in group if row["outcome_state"] in TERMINAL_OUTCOMES]
        targets = sum(row["outcome_state"] == "target_reached" for row in evaluated)
        invalidated = sum(row["outcome_state"] == "invalidated" for row in evaluated)
        return {
            label_key: label,
            "total_occurrences": len(group),
            "evaluated_count": len(evaluated),
            "target_reached_count": targets,
            "invalidated_count": invalidated,
            "open_count": sum(row["outcome_state"] == "open" for row in group),
            "unavailable_count": sum(row["outcome_state"] == "unavailable" for row in group),
            "direction": next(iter({row["direction"] for row in group}), "mixed")
            if len({row["direction"] for row in group}) == 1
            else "mixed",
            "target_reached_rate": _rate(targets, len(evaluated)),
            "outcome_coverage_percent": _rate(len(evaluated), len(group)),
            "median_mfe": _median(evaluated, "mfe"),
            "median_mae": _median(evaluated, "mae"),
        }

    time_heatmap = [
        comparison(label, by_time[label], "bucket")
        for label in sorted(by_time, key=_bucket_sort_key)
    ]
    family_comparison = [comparison(label, group, "family") for label, group in by_family.items()]
    for item in family_comparison:
        latest = sorted(by_family[item["family"]], key=lambda row: row["signal_time"], reverse=True)
        item["occurrences"] = [
            {key: row[key] for key in (
                "setup_event_id", "signal_time", "direction", "pattern_code", "level_label",
                "level_value", "entry_value", "target_label", "target_value", "outcome_state",
                "resolution_time",
            )}
            for row in latest[:5]
        ]
    family_comparison.sort(
        key=lambda row: (row["evaluated_count"], row["total_occurrences"]), reverse=True
    )
    page_start = (filters["page"] - 1) * filters["page_size"]
    page_rows = rows[page_start : page_start + filters["page_size"]]
    metrics = {
        "total_setups": total,
        "covered_sessions": len(sessions),
        "setups_per_session": round(total / len(sessions), 2) if sessions else None,
        "terminal_sample_size": len(terminal),
        "target_reached_count": target_count,
        "target_reached_rate": _rate(target_count, len(terminal)),
        "invalidated_count": invalidated_count,
        "invalidation_rate": _rate(invalidated_count, len(terminal)),
        "open_count": sum(row["outcome_state"] == "open" for row in rows),
        "unavailable_count": sum(row["outcome_state"] == "unavailable" for row in rows),
        "median_mfe": _median(rows, "mfe"),
        "median_mae": _median(rows, "mae"),
        "median_target_progress": _median(rows, "target_progress_percent"),
    }
    payload = {
        "filters": filters,
        "metrics": metrics,
        "charts": {
            "occurrences_by_session": [
                {"session_date": key, "count": by_session[key]} for key in sorted(by_session)
            ],
            "outcomes_by_time_bucket": [
                {
                    "bucket": item["bucket"],
                    "target_reached": item["target_reached_count"],
                    "invalidated": item["invalidated_count"],
                    "open": item["open_count"],
                    "unavailable": item["unavailable_count"],
                }
                for item in time_heatmap
            ],
            "outcomes_by_family": [
                {
                    "family": item["family"],
                    "target_reached": item["target_reached_count"],
                    "invalidated": item["invalidated_count"],
                    "open": item["open_count"],
                    "unavailable": item["unavailable_count"],
                }
                for item in family_comparison
            ],
            "mfe_vs_mae": [
                {
                    "setup_event_id": row["setup_event_id"],
                    "signal_time": row["signal_time"],
                    "family": row["family_label"] or row["family"],
                    "outcome": row["outcome_state"],
                    "mfe": row["mfe"],
                    "mae": row["mae"],
                }
                for row in rows
                if row["mfe"] is not None and row["mae"] is not None
            ],
        },
        "time_heatmap": time_heatmap,
        "family_comparison": family_comparison,
        "profit_estimate_assumptions": {
            "contract_cost": int(ESTIMATED_CONTRACT_COST),
            "absolute_delta": ESTIMATED_ABSOLUTE_DELTA,
            "multiplier": OPTION_CONTRACT_MULTIPLIER,
            "planned_return_range": list(PLANNED_TP_RETURN_RANGE),
            "planned_profit_range": [
                round(ESTIMATED_CONTRACT_COST * percent / 100, 2)
                for percent in PLANNED_TP_RETURN_RANGE
            ],
            "anchors": anchors,
        },
        "insights": {
            "best_time_bucket": _ranked_insight(time_heatmap, "bucket"),
            "strongest_family": _ranked_insight(family_comparison, "family"),
        },
        "ledger": {
            "rows": [_row_payload(row) for row in page_rows],
            "page": filters["page"],
            "page_size": filters["page_size"],
            "total": total,
            "pages": math.ceil(total / filters["page_size"]) if total else 0,
        },
        "options": {
            "families": sorted({row["family"] for row in rows if row["family"]}),
            "patterns": sorted({row["pattern_code"] for row in rows if row["pattern_code"]}),
            "levels": sorted({row["level_key"] for row in rows if row["level_key"]}),
            "grades": sorted({row["grade"] for row in rows if row["grade"]}),
        },
        "coverage": {
            "session_count": len(sessions),
            "earliest_signal_time": min((row["signal_time"] for row in rows), default=""),
            "latest_signal_time": max((row["signal_time"] for row in rows), default=""),
            "evaluated_through": max((row["evaluated_through"] for row in rows), default=""),
            "missing_outcome_count": metrics["unavailable_count"],
            "complete_outcome_count": len(terminal),
            "outcome_coverage_percent": _rate(len(terminal), total),
            "missing_excursion_count": sum(
                row["mfe"] is None or row["mae"] is None for row in rows
            ),
        },
        "empty": total == 0,
        "interpretation": (
            "SPX setup outcomes measure underlying price movement. Potential-profit figures are "
            "planning estimates, not option fills, actual contract returns, or realized profit."
        ),
    }
    with _CACHE_LOCK:
        _CACHE[cache_key] = {"revision": cache_revision, "payload": payload}
    return payload
