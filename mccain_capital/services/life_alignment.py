"""Life Alignment page and JSON-backed habit storage."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from flask import jsonify, render_template, request

from mccain_capital import runtime as app_runtime
from mccain_capital.repositories import journal as journal_repo
from mccain_capital.runtime import now_iso, today_iso
from mccain_capital.services.ui import render_page

_STORE_FILE = "life_alignment.json"
_MOODS = {"low", "neutral", "good", "strong"}
_NUMERIC_FIELDS = {
    "water_oz": float,
    "water_goal_oz": float,
    "pushups": int,
    "squats": int,
    "walk_minutes": int,
    "steps": int,
    "sleep_hours": float,
}
_BOOLEAN_FIELDS = ("workout_completed", "devotion_completed", "journal_completed")
_WORKOUT_TYPES = {"Push-ups / Squats", "Walk", "Full Body", "Mobility", "Rest / Recovery"}
_FOLLOWED_RULES = {"yes", "no", "not_yet"}


def life_alignment_page():
    today = today_iso()
    content = render_template(
        "life_alignment.html",
        today=today,
        journal_preview=_today_life_journal_preview(today),
    )
    return render_page(content, active="life-alignment", title="McCain Capital · Life Alignment")


def api_today():
    store = _read_store()
    entry = _entry_for_date(store.get("entries", []), today_iso()) or _default_entry(today_iso())
    return jsonify({"ok": True, "entry": _normalize_entry(entry)})


def api_save_today():
    payload = request.get_json(silent=True) or request.form.to_dict()
    day = today_iso()
    store = _read_store()
    entries = [_normalize_entry(entry) for entry in store.get("entries", [])]
    existing = _entry_for_date(entries, day)
    action = str(payload.get("action") or "").strip().lower()
    if existing and existing.get("locked") and action not in {"unlock", "lock"}:
        return jsonify({"ok": False, "error": "Day is locked. Unlock it before editing."}), 409

    entry = _coerce_entry(payload, day=day, existing=existing)
    if action == "lock":
        entry["locked"] = True
        entry["locked_at"] = now_iso()
    elif action == "unlock":
        entry["locked"] = False
        entry["locked_at"] = ""

    replaced = False
    next_entries: List[Dict[str, Any]] = []
    for old_entry in entries:
        if str(old_entry.get("date")) == day:
            next_entries.append(entry)
            replaced = True
        else:
            next_entries.append(old_entry)
    if not replaced:
        next_entries.append(entry)

    store["entries"] = _sort_entries(next_entries)
    _write_store(store)
    return jsonify({"ok": True, "entry": entry, "analytics": _build_analytics(store["entries"])})


def api_history():
    entries = _last_n_calendar_entries(_read_store().get("entries", []), days=30)
    return jsonify({"ok": True, "entries": entries})


def api_analytics():
    entries = [_normalize_entry(entry) for entry in _read_store().get("entries", [])]
    return jsonify({"ok": True, "analytics": _build_analytics(entries)})


def _storage_path() -> str:
    return os.path.join(str(app_runtime.PERSISTENT_DATA_DIR), _STORE_FILE)


def _read_store() -> Dict[str, Any]:
    path = _storage_path()
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return {"entries": []}
    except (json.JSONDecodeError, OSError):
        return {"entries": []}

    if isinstance(data, list):
        return {"entries": data}
    if isinstance(data, dict) and isinstance(data.get("entries"), list):
        return data
    return {"entries": []}


def _write_store(store: Dict[str, Any]) -> None:
    path = _storage_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(store, handle, indent=2, sort_keys=True)
    os.replace(tmp_path, path)


def _default_entry(day: str) -> Dict[str, Any]:
    return {
        "date": day,
        "water_oz": 0,
        "water_goal_oz": 100,
        "workout_completed": False,
        "workout_type": "Push-ups / Squats",
        "pushups": 0,
        "squats": 0,
        "walk_minutes": 0,
        "steps": 0,
        "sleep_hours": 0,
        "devotion_completed": False,
        "journal_completed": False,
        "mood": "neutral",
        "discipline_score": 0,
        "notes": "",
        "created_at": "",
        "updated_at": "",
        "locked": False,
        "locked_at": "",
        "followed_rules": "not_yet",
    }


def _coerce_entry(
    payload: Dict[str, Any], *, day: str, existing: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    base = _default_entry(day)
    if existing:
        base.update(_normalize_entry(existing))

    for field, field_type in _NUMERIC_FIELDS.items():
        base[field] = _coerce_number(payload.get(field, base.get(field, 0)), field_type)

    for field in _BOOLEAN_FIELDS:
        base[field] = _coerce_bool(payload.get(field, base.get(field, False)))

    workout_type = str(payload.get("workout_type", base.get("workout_type")) or "").strip()
    base["workout_type"] = workout_type if workout_type in _WORKOUT_TYPES else "Push-ups / Squats"

    mood = str(payload.get("mood", base.get("mood")) or "neutral").strip().lower()
    base["mood"] = mood if mood in _MOODS else "neutral"
    followed_rules = str(payload.get("followed_rules", base.get("followed_rules")) or "not_yet").strip().lower()
    base["followed_rules"] = followed_rules if followed_rules in _FOLLOWED_RULES else "not_yet"
    base["locked"] = _coerce_bool(payload.get("locked", base.get("locked", False)))
    base["locked_at"] = str(payload.get("locked_at", base.get("locked_at")) or "").strip()
    base["notes"] = str(payload.get("notes", base.get("notes")) or "").strip()
    base["date"] = day
    base["discipline_score"] = _calculate_score(base)
    stamp = now_iso()
    base["created_at"] = str(base.get("created_at") or stamp)
    base["updated_at"] = stamp
    return _normalize_entry(base)


def _normalize_entry(raw: Dict[str, Any]) -> Dict[str, Any]:
    day = str(raw.get("date") or today_iso()).strip() or today_iso()
    entry = _default_entry(day)
    entry.update({k: raw.get(k, v) for k, v in entry.items()})
    for field, field_type in _NUMERIC_FIELDS.items():
        entry[field] = _coerce_number(entry.get(field), field_type)
    for field in _BOOLEAN_FIELDS:
        entry[field] = _coerce_bool(entry.get(field))
    mood = str(entry.get("mood") or "neutral").strip().lower()
    entry["mood"] = mood if mood in _MOODS else "neutral"
    followed_rules = str(entry.get("followed_rules") or "not_yet").strip().lower()
    entry["followed_rules"] = followed_rules if followed_rules in _FOLLOWED_RULES else "not_yet"
    entry["locked"] = _coerce_bool(entry.get("locked"))
    entry["locked_at"] = str(entry.get("locked_at") or "")
    entry["notes"] = str(entry.get("notes") or "")
    entry["discipline_score"] = _calculate_score(entry)
    entry.update(_entry_status(entry))
    return entry


def _coerce_number(value: Any, field_type: type) -> Any:
    try:
        number = field_type(value)
    except (TypeError, ValueError):
        number = field_type(0)
    if number < 0:
        number = field_type(0)
    return number


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "checked"}


def _calculate_score(entry: Dict[str, Any]) -> int:
    score = 0
    if float(entry.get("water_goal_oz") or 0) > 0 and float(entry.get("water_oz") or 0) >= float(
        entry.get("water_goal_oz") or 0
    ):
        score += 20
    if _coerce_bool(entry.get("workout_completed")):
        score += 20
    if int(entry.get("walk_minutes") or 0) >= 20:
        score += 15
    if float(entry.get("sleep_hours") or 0) >= 7:
        score += 15
    if _coerce_bool(entry.get("devotion_completed")):
        score += 15
    if _coerce_bool(entry.get("journal_completed")):
        score += 15
    score = min(100, score)
    if str(entry.get("followed_rules") or "not_yet") == "no":
        return min(score, 69)
    return score


def _entry_status(entry: Dict[str, Any]) -> Dict[str, Any]:
    score = int(entry.get("discipline_score") or 0)
    missing = _missing_today(entry)
    if score >= 80:
        today_mode = "LOCKED IN"
    elif score >= 50:
        today_mode = "IN PROGRESS"
    elif score > 0:
        today_mode = "OFF TRACK"
    else:
        today_mode = "NOT STARTED"

    if not str(entry.get("created_at") or "").strip():
        daily_status = "NOT LOGGED"
    elif not missing:
        daily_status = "COMPLETE"
    else:
        daily_status = "STARTED"

    if score <= 39:
        score_state = "off-track"
        score_label = "OFF TRACK"
    elif score <= 69:
        score_state = "building"
        score_label = "BUILDING"
    else:
        score_state = "locked-in"
        score_label = "LOCKED IN"

    if score == 0:
        score_message = "Start with one small win."
    elif score <= 39:
        score_message = "Get back in alignment."
    elif score <= 69:
        score_message = "Keep stacking."
    elif score <= 99:
        score_message = "Strong day forming."
    else:
        score_message = "Ready to lock the day."

    rule_message = ""
    if entry.get("followed_rules") == "no":
        rule_message = "Rule break logged. Review it. Don't hide it."
    elif entry.get("followed_rules") == "yes" and score >= 70:
        rule_message = "Clean execution day."

    return {
        "today_mode": today_mode,
        "daily_status": daily_status,
        "missing_today": missing,
        "score_state": score_state,
        "score_label": score_label,
        "score_message": score_message,
        "rule_message": rule_message,
    }


def _missing_today(entry: Dict[str, Any]) -> List[str]:
    missing: List[str] = []
    if not (
        float(entry.get("water_goal_oz") or 0) > 0
        and float(entry.get("water_oz") or 0) >= float(entry.get("water_goal_oz") or 0)
    ):
        missing.append("Water")
    if not _coerce_bool(entry.get("workout_completed")):
        missing.append("Workout")
    if int(entry.get("walk_minutes") or 0) < 20:
        missing.append("Walk")
    if not _coerce_bool(entry.get("devotion_completed")):
        missing.append("Devotion")
    if not _coerce_bool(entry.get("journal_completed")):
        missing.append("Journal")
    return missing


def _entry_for_date(entries: Iterable[Dict[str, Any]], day: str) -> Optional[Dict[str, Any]]:
    for entry in entries:
        if str(entry.get("date")) == day:
            return entry
    return None


def _sort_entries(entries: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted((_normalize_entry(entry) for entry in entries), key=lambda entry: entry["date"])


def _last_n_calendar_entries(entries: Iterable[Dict[str, Any]], *, days: int) -> List[Dict[str, Any]]:
    by_date = {entry["date"]: entry for entry in _sort_entries(entries)}
    today = datetime.strptime(today_iso(), "%Y-%m-%d").date()
    start = today - timedelta(days=days - 1)
    return [
        by_date.get((start + timedelta(days=offset)).isoformat(), _default_entry((start + timedelta(days=offset)).isoformat()))
        for offset in range(days)
    ]


def _build_analytics(entries: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    normalized = _sort_entries(entries)
    last_30 = _last_n_calendar_entries(normalized, days=30)
    last_7 = last_30[-7:]
    existing_last_30 = [entry for entry in last_30 if str(entry.get("created_at") or "").strip()]
    today = datetime.strptime(today_iso(), "%Y-%m-%d").date()
    month_prefix = today.strftime("%Y-%m")
    month_entries = [entry for entry in normalized if str(entry.get("date", "")).startswith(month_prefix)]

    def workout_predicate(entry):
        return entry["workout_completed"]

    def water_predicate(entry):
        return float(entry["water_goal_oz"] or 0) > 0 and float(entry["water_oz"] or 0) >= float(
            entry["water_goal_oz"] or 0
        )

    def journal_predicate(entry):
        return entry["journal_completed"]

    def devotion_predicate(entry):
        return entry["devotion_completed"]

    def walk_predicate(entry):
        return int(entry["walk_minutes"] or 0) >= 20

    def sleep_predicate(entry):
        return float(entry["sleep_hours"] or 0) >= 7

    weekly_hit_rates = {
        "water": _hit_rate(last_7, water_predicate),
        "workout": _hit_rate(last_7, workout_predicate),
        "walk": _hit_rate(last_7, walk_predicate),
        "sleep": _hit_rate(last_7, sleep_predicate),
        "journal": _hit_rate(last_7, journal_predicate),
        "devotion": _hit_rate(last_7, devotion_predicate),
    }
    weekly_completion = round(sum(entry["discipline_score"] for entry in last_7) / max(len(last_7), 1))
    monthly_completion = round(sum(entry["discipline_score"] for entry in last_30) / max(len(last_30), 1))
    insights = _build_insights(last_7, weekly_hit_rates, weekly_completion)

    return {
        "current_workout_streak": _current_streak(normalized, workout_predicate),
        "current_water_goal_streak": _current_streak(normalized, water_predicate),
        "current_journal_streak": _current_streak(normalized, journal_predicate),
        "current_devotion_streak": _current_streak(normalized, devotion_predicate),
        "best_workout_streak": _best_streak(normalized, workout_predicate),
        "best_water_streak": _best_streak(normalized, water_predicate),
        "best_journal_streak": _best_streak(normalized, journal_predicate),
        "best_devotion_streak": _best_streak(normalized, devotion_predicate),
        "weekly_completion_percentage": weekly_completion,
        "monthly_completion_percentage": monthly_completion,
        "average_water_intake": round(_average(existing_last_30, "water_oz"), 1),
        "average_sleep": round(_average(existing_last_30, "sleep_hours"), 1),
        "total_workouts_this_month": sum(1 for entry in month_entries if entry["workout_completed"]),
        "total_walking_minutes_this_month": sum(int(entry["walk_minutes"] or 0) for entry in month_entries),
        "average_discipline_score": round(_average(existing_last_30, "discipline_score")),
        "weekly_habit_hit_rates": weekly_hit_rates,
        "accountability_insights": insights,
        "score_trend": [int(entry["discipline_score"] or 0) for entry in last_30],
        "locked_days_count": sum(1 for entry in normalized if entry.get("locked")),
        "rule_break_count": sum(1 for entry in normalized if entry.get("followed_rules") == "no"),
    }


def _current_streak(entries: Iterable[Dict[str, Any]], predicate) -> int:
    by_date = {entry["date"]: entry for entry in _sort_entries(entries)}
    cursor = datetime.strptime(today_iso(), "%Y-%m-%d").date()
    streak = 0
    while True:
        entry = by_date.get(cursor.isoformat())
        if not entry or not predicate(entry):
            return streak
        streak += 1
        cursor -= timedelta(days=1)


def _best_streak(entries: Iterable[Dict[str, Any]], predicate) -> int:
    best = 0
    current = 0
    for entry in _sort_entries(entries):
        if predicate(entry):
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def _hit_rate(entries: List[Dict[str, Any]], predicate) -> int:
    if not entries:
        return 0
    return round((sum(1 for entry in entries if predicate(entry)) / len(entries)) * 100)


def _build_insights(
    last_7: List[Dict[str, Any]], weekly_hit_rates: Dict[str, int], weekly_completion: int
) -> List[str]:
    labels = {
        "water": "Water",
        "workout": "Workout",
        "walk": "Walking",
        "sleep": "Sleep",
        "journal": "Journal",
        "devotion": "Devotion",
    }
    logged_days = [entry for entry in last_7 if str(entry.get("created_at") or "").strip()]
    if len(logged_days) < 3:
        return ["Not enough data yet. Stack 3 clean days."]

    values = list(weekly_hit_rates.values())
    if len(set(values)) == 1:
        insights = ["Habits are currently flat - build momentum."]
    else:
        best_key = max(weekly_hit_rates, key=lambda key: (weekly_hit_rates[key], labels[key]))
        weakest_candidates = [
            key for key, value in weekly_hit_rates.items() if value == min(weekly_hit_rates.values())
        ]
        weakest_key = next((key for key in weakest_candidates if key != best_key), weakest_candidates[0])
        insights = [
            f"Best habit this week: {labels[best_key]}",
            f"Weakest habit this week: {labels[weakest_key]}",
        ]
    average_sleep = _average(last_7, "sleep_hours")
    if average_sleep < 6.5:
        insights.append("Needs attention: Sleep is below target.")
    if weekly_hit_rates.get("water", 0) < 70:
        insights.append("Water consistency is below 70%.")
    if sum(1 for entry in last_7 if entry.get("workout_completed")) < 3:
        insights.append("Workout consistency is slipping - get 3 sessions this week.")
    if weekly_completion >= 80:
        insights.append("Strong week forming. Keep the standard.")
    return insights


def _average(entries: List[Dict[str, Any]], field: str) -> float:
    if not entries:
        return 0.0
    return sum(float(entry.get(field) or 0) for entry in entries) / len(entries)


def _today_life_journal_preview(day: str) -> Dict[str, str]:
    try:
        entries = [dict(row) for row in journal_repo.fetch_entries_by_type("life_note", d=day)]
    except Exception:
        return {"logged": False, "text": "No journal reflection logged yet.", "href": "/journal/life"}
    if not entries:
        return {"logged": False, "text": "No journal reflection logged yet.", "href": "/journal/life"}

    entry = entries[0]
    title = str(entry.get("setup") or "Today reflection").strip()
    notes = str(entry.get("notes") or "").strip()
    preview = notes[:180].strip()
    if len(notes) > 180:
        preview += "..."
    return {
        "logged": True,
        "text": preview or title,
        "title": title,
        "href": f"/journal/life?d={day}",
    }
