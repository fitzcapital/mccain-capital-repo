"""Trade review scoring helpers.

This module powers the Trades page, Review page, and replay workflow with one
shared grade model. Outcome has the lightest weight so disciplined losses can
still grade well, while sloppy wins can still fail review.
"""

from __future__ import annotations

from typing import Any


GRADE_COMPONENTS = (
    ("setup_quality", "Setup Quality", 20),
    ("entry_quality", "Entry Quality", 15),
    ("confirmation_quality", "Confirmation Quality", 15),
    ("risk_management", "Risk Management", 20),
    ("execution_discipline", "Execution Discipline", 15),
    ("trade_management", "Trade Management", 10),
    ("outcome", "Outcome", 5),
)

GRADE_BREAKPOINTS = (
    (90, "A"),
    (75, "B"),
    (60, "C"),
    (40, "D"),
    (0, "F"),
)


def _safe_float(value: Any) -> float:
    try:
        return float(value) if value not in (None, "") else 0.0
    except Exception:
        return 0.0


def _float_or_none(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _int_or_none(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except Exception:
        return None


def _text_present(*values: Any) -> bool:
    return any(bool(str(value or "").strip()) for value in values)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _normalize_choice(value: Any) -> str:
    return str(value or "").strip()


def _resolve_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    try:
        return bool(int(value))
    except Exception:
        pass
    text = str(value).strip().lower()
    if text in {"true", "yes", "y", "on"}:
        return True
    if text in {"false", "no", "n", "off"}:
        return False
    return None


def grade_from_score(score: int | float | None) -> str:
    numeric = max(0, min(100, int(round(float(score or 0)))))
    for threshold, grade in GRADE_BREAKPOINTS:
        if numeric >= threshold:
            return grade
    return "F"


def classification_from_grade(grade: str, outcome_label: str) -> str:
    clean_grade = str(grade or "").strip().upper()
    if outcome_label == "Scratch":
        return "Scratch"
    if outcome_label == "Win":
        return "Good Win" if clean_grade in {"A", "B"} else "Bad Win"
    if outcome_label == "Loss":
        return "Good Loss" if clean_grade in {"A", "B"} else "Bad Loss"
    return "Unclassified"


def _format_r_multiple(value: float | None) -> str:
    if value is None:
        return "—"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}R"


def _review_state(review_pct: int) -> tuple[str, str]:
    if review_pct >= 84:
        return "Fully Reviewed", "positive"
    if review_pct >= 42:
        return "Partially Reviewed", "info"
    return "Not Reviewed", "warn"


def _review_completion_items(
    *,
    setup_identified: bool,
    thesis_present: bool,
    review_present: bool,
    risk_captured: bool,
    stop_captured: bool,
    target_captured: bool,
    execution_reviewed: bool,
    final_grade_present: bool,
    classification_present: bool,
    plan_verdict_present: bool,
) -> list[dict[str, Any]]:
    return [
        {
            "key": "setup",
            "label": "Setup identified",
            "done": setup_identified,
            "hint": "Choose a valid setup tag.",
        },
        {
            "key": "thesis",
            "label": "Thesis present",
            "done": thesis_present,
            "hint": "Explain why the setup existed.",
        },
        {
            "key": "reflection",
            "label": "Review reflection present",
            "done": review_present,
            "hint": "Add review or improvement notes.",
        },
        {
            "key": "risk",
            "label": "Risk captured",
            "done": risk_captured,
            "hint": "Log planned or reviewed risk.",
        },
        {
            "key": "stop",
            "label": "Stop captured",
            "done": stop_captured,
            "hint": "Log a reviewed or planned stop.",
        },
        {
            "key": "target",
            "label": "Target captured",
            "done": target_captured,
            "hint": "Log a reviewed or planned target.",
        },
        {
            "key": "execution",
            "label": "Execution reviewed",
            "done": execution_reviewed,
            "hint": "Score execution or add entry/exit review.",
        },
        {
            "key": "final_grade",
            "label": "Final grade present",
            "done": final_grade_present,
            "hint": "Keep the derived final grade or override it.",
        },
        {
            "key": "classification",
            "label": "Classification present",
            "done": classification_present,
            "hint": "Keep the derived class or set an override.",
        },
        {
            "key": "plan_verdict",
            "label": "Sizing / plan verdict present",
            "done": plan_verdict_present,
            "hint": "Capture sizing, stop discipline, or within-plan verdict.",
        },
    ]


def _review_missing_summary(items: list[dict[str, Any]]) -> str:
    missing = [str(item.get("label") or "") for item in items if not bool(item.get("done"))]
    if not missing:
        return "All core review checks logged."
    short_map = {
        "Setup identified": "setup",
        "Thesis present": "thesis",
        "Review reflection present": "review note",
        "Risk captured": "risk",
        "Stop captured": "stop",
        "Target captured": "target",
        "Execution reviewed": "execution review",
        "Final grade present": "final grade",
        "Classification present": "classification",
        "Sizing / plan verdict present": "plan verdict",
    }
    short = [short_map.get(label, label.lower()) for label in missing[:2]]
    summary = " + ".join(short)
    if len(missing) > 2:
        summary += f" + {len(missing) - 2} more"
    return f"Missing {summary}"


def _weighted_score(raw_score: float, max_points: int, *, floor: float = 0.0) -> float:
    return _clamp(raw_score / 100.0, floor, 1.0) * max_points


def compute_trade_review_foundation(
    row: dict[str, Any],
    *,
    risk_median: float | None = None,
    risk_avg: float | None = None,
    spend_median: float | None = None,
) -> dict[str, Any]:
    setup_label = _normalize_choice(row.get("setup_tag") or row.get("setup_display"))
    session_tag = _normalize_choice(row.get("session_tag"))
    checklist_score = _int_or_none(row.get("checklist_score")) or 0
    execution_grade = _int_or_none(row.get("execution_grade")) or 0
    risk_grade = _int_or_none(row.get("risk_grade")) or 0
    plan_grade = _int_or_none(row.get("plan_grade")) or 0

    thesis_present = _text_present(row.get("thesis_note"))
    review_present = _text_present(row.get("review_note"), row.get("improvement_note"))
    entry_reviewed = _text_present(row.get("entry_quality_note"))
    exit_reviewed = _text_present(row.get("exit_quality_note"))
    size_note_present = _text_present(row.get("size_rule_note"))
    rule_breaks = _normalize_choice(row.get("rule_break_tags"))
    mistake_tags = _normalize_choice(row.get("mistake_tags"))

    planned_risk = _float_or_none(row.get("reviewed_risk_dollars"))
    if planned_risk in (None, 0):
        planned_risk = _float_or_none(row.get("planned_risk_dollars"))
    if planned_risk in (None, 0):
        planned_risk = _float_or_none(row.get("risk"))

    spend_value = _float_or_none(row.get("total_spent"))
    risk_inferred = False
    if planned_risk in (None, 0) and spend_value not in (None, 0):
        planned_risk = spend_value * 0.20
        risk_inferred = True

    balance_value = _float_or_none(row.get("balance"))
    reviewed_risk_pct = _float_or_none(row.get("reviewed_risk_percent"))
    net = _safe_float(row.get("net_pl"))
    risk_pct = reviewed_risk_pct
    if risk_pct is None and planned_risk and balance_value and balance_value > 0:
        risk_pct = (planned_risk / balance_value) * 100.0
    r_multiple = (net / planned_risk) if planned_risk and planned_risk > 0 else None

    stop_price = _float_or_none(row.get("reviewed_stop_price"))
    if stop_price in (None, 0):
        stop_price = _float_or_none(row.get("stop_price"))
    stop_pct = _float_or_none(row.get("stop_pct"))
    target_price = _float_or_none(row.get("reviewed_target_price"))
    if target_price in (None, 0):
        target_price = _float_or_none(row.get("take_profit"))
    target_pct = _float_or_none(row.get("target_pct"))
    entry_price = _float_or_none(row.get("entry_price"))
    exit_price = _float_or_none(row.get("exit_price"))

    implicit_stop = False
    if (
        stop_price in (None, 0)
        and stop_pct in (None, 0)
        and (planned_risk not in (None, 0) or spend_value not in (None, 0))
    ):
        stop_pct = 20.0
        implicit_stop = True

    stop_present = bool(stop_price or stop_pct)
    target_present = bool(target_price or target_pct)

    reward_risk = None
    if entry_price and stop_price and target_price and abs(entry_price - stop_price) > 0:
        reward_risk = abs(target_price - entry_price) / abs(entry_price - stop_price)

    loss_exceeded = bool(planned_risk and net < 0 and abs(net) > (planned_risk * 1.05))
    exited_early = bool(
        net > 0
        and target_price
        and exit_price
        and target_price > 0
        and exit_price < (target_price * 0.92)
    )

    oversized = bool(
        (
            planned_risk
            and risk_avg
            and planned_risk > max(risk_avg * 1.45, (risk_median or risk_avg) * 1.35)
        )
        or (risk_pct and risk_pct >= 2.0)
        or (spend_value and spend_median and spend_value > spend_median * 1.6)
    )
    undersized = bool(
        planned_risk and risk_median and risk_median > 0 and planned_risk < (risk_median * 0.55)
    )

    auto_stop_discipline = "Stop Missing"
    if stop_present and loss_exceeded:
        auto_stop_discipline = "Loss Exceeded Planned Risk"
    elif stop_present and exited_early:
        auto_stop_discipline = "Exited Early"
    elif stop_present:
        auto_stop_discipline = "Within Risk Plan" if implicit_stop else "Stop Respected"

    if rule_breaks or (execution_grade and execution_grade < 60):
        auto_execution_quality = "Rule Break"
    elif execution_grade >= 80 and not mistake_tags:
        auto_execution_quality = "Clean"
    else:
        auto_execution_quality = "Minor Error"

    if oversized:
        auto_sizing_quality = "Oversized"
    elif undersized:
        auto_sizing_quality = "Undersized"
    else:
        auto_sizing_quality = "Proper"

    reviewed_execution_quality = _normalize_choice(row.get("reviewed_execution_quality"))
    reviewed_sizing_quality = _normalize_choice(row.get("reviewed_sizing_quality"))
    reviewed_stop_discipline = _normalize_choice(row.get("reviewed_stop_discipline"))
    reviewed_within_plan = _resolve_bool(row.get("reviewed_within_plan"))

    execution_quality = reviewed_execution_quality or auto_execution_quality
    sizing_quality = reviewed_sizing_quality or auto_sizing_quality
    stop_discipline = reviewed_stop_discipline or auto_stop_discipline
    within_plan = (
        reviewed_within_plan
        if reviewed_within_plan is not None
        else (
            sizing_quality != "Oversized"
            and stop_discipline not in {"Stop Missing", "Loss Exceeded Planned Risk"}
            and execution_quality != "Rule Break"
        )
    )

    outcome_label = "Scratch"
    if net > 0:
        outcome_label = "Win"
    elif net < 0:
        outcome_label = "Loss"

    # Weighted 100-point grade engine.
    setup_quality = 5.0
    if setup_label and setup_label != "Unknown":
        setup_quality += 8.0
    if session_tag:
        setup_quality += 3.0
    if thesis_present:
        setup_quality += 2.0
    if checklist_score:
        setup_quality += _weighted_score(checklist_score, 2, floor=0.15)
    setup_quality = _clamp(setup_quality, 0.0, 20.0)

    entry_quality = 3.0
    if entry_reviewed:
        entry_quality += 4.0
    if execution_grade:
        entry_quality += _weighted_score(execution_grade, 5, floor=0.10)
    if checklist_score:
        entry_quality += _weighted_score(checklist_score, 3, floor=0.10)
    entry_quality = _clamp(entry_quality, 0.0, 15.0)

    confirmation_quality = 3.0
    if thesis_present:
        confirmation_quality += 2.0
    if session_tag:
        confirmation_quality += 2.0
    confirmation_quality += _weighted_score(checklist_score, 8, floor=0.10)
    confirmation_quality = _clamp(confirmation_quality, 0.0, 15.0)

    risk_management = 4.0
    if planned_risk:
        risk_management += 5.0 if not risk_inferred else 3.5
    if stop_present:
        risk_management += 4.0
    if target_present:
        risk_management += 1.5
    if risk_grade:
        risk_management += _weighted_score(risk_grade, 3, floor=0.10)
    if plan_grade:
        risk_management += _weighted_score(plan_grade, 2.5, floor=0.10)
    if risk_pct is not None and risk_pct <= 2.0:
        risk_management += 2.0
    if not oversized:
        risk_management += 1.0
    if not loss_exceeded:
        risk_management += 1.0
    risk_management = _clamp(risk_management, 0.0, 20.0)

    execution_discipline = 3.0
    if execution_quality == "Clean":
        execution_discipline += 7.0
    elif execution_quality == "Minor Error":
        execution_discipline += 4.0
    if not rule_breaks:
        execution_discipline += 2.0
    if execution_grade:
        execution_discipline += _weighted_score(execution_grade, 3, floor=0.10)
    execution_discipline = _clamp(execution_discipline, 0.0, 15.0)

    trade_management = 2.0
    if target_present:
        trade_management += 2.0
    if exit_reviewed:
        trade_management += 2.0
    if reward_risk is not None:
        trade_management += 2.0 if reward_risk >= 1.5 else 1.0
    if stop_discipline in {"Stop Respected", "Within Risk Plan"}:
        trade_management += 2.0
    elif stop_discipline == "Exited Early":
        trade_management += 1.0
    if size_note_present:
        trade_management += 1.0
    if review_present:
        trade_management += 1.0
    trade_management = _clamp(trade_management, 0.0, 10.0)

    if outcome_label == "Win" and within_plan and execution_quality == "Clean":
        outcome_score = 5.0
    elif outcome_label == "Win":
        outcome_score = 2.0
    elif outcome_label == "Loss" and within_plan:
        outcome_score = 4.0
    elif outcome_label == "Loss" and not loss_exceeded:
        outcome_score = 2.0
    elif outcome_label == "Scratch":
        outcome_score = 3.0
    else:
        outcome_score = 0.0

    auto_score = int(
        round(
            setup_quality
            + entry_quality
            + confirmation_quality
            + risk_management
            + execution_discipline
            + trade_management
            + outcome_score
        )
    )
    auto_grade = grade_from_score(auto_score)

    manual_grade_score = _int_or_none(row.get("manual_grade_score"))
    manual_grade_letter = _normalize_choice(row.get("manual_grade_letter")).upper()
    if manual_grade_score is not None and not manual_grade_letter:
        manual_grade_letter = grade_from_score(manual_grade_score)
    if manual_grade_letter and manual_grade_score is None:
        for threshold, grade in GRADE_BREAKPOINTS:
            if grade == manual_grade_letter:
                manual_grade_score = threshold
                break

    final_score = manual_grade_score if manual_grade_score is not None else auto_score
    final_grade = manual_grade_letter if manual_grade_letter else auto_grade
    classification_override = _normalize_choice(row.get("classification_override"))
    classification = classification_override or classification_from_grade(
        final_grade, outcome_label
    )

    review_completion_items = _review_completion_items(
        setup_identified=bool(setup_label and setup_label != "Unknown"),
        thesis_present=thesis_present,
        review_present=review_present,
        risk_captured=bool(planned_risk or risk_pct is not None),
        stop_captured=bool(stop_present or stop_pct or stop_price),
        target_captured=bool(target_present or target_price or target_pct),
        execution_reviewed=bool(
            entry_reviewed or exit_reviewed or execution_grade or reviewed_execution_quality
        ),
        final_grade_present=bool(final_grade),
        classification_present=classification not in {"", "Unclassified"},
        plan_verdict_present=bool(
            size_note_present
            or reviewed_sizing_quality
            or reviewed_stop_discipline
            or reviewed_within_plan is not None
        ),
    )
    review_checks = [bool(item.get("done")) for item in review_completion_items]
    review_pct = int(round((sum(1 for item in review_checks if item) / len(review_checks)) * 100.0))
    review_label, review_tone = _review_state(review_pct)
    review_missing_summary = _review_missing_summary(review_completion_items)

    grade_components = {
        "setup_quality": round(setup_quality, 1),
        "entry_quality": round(entry_quality, 1),
        "confirmation_quality": round(confirmation_quality, 1),
        "risk_management": round(risk_management, 1),
        "execution_discipline": round(execution_discipline, 1),
        "trade_management": round(trade_management, 1),
        "outcome": round(outcome_score, 1),
    }
    breakdown_items = [
        {
            "key": key,
            "label": label,
            "score": grade_components[key],
            "max": max_points,
        }
        for key, label, max_points in GRADE_COMPONENTS
    ]
    strongest_components = sorted(
        breakdown_items, key=lambda item: (item["score"] / item["max"]), reverse=True
    )[:2]
    weakest_components = sorted(breakdown_items, key=lambda item: (item["score"] / item["max"]))[:2]

    return {
        "setup_missing": not bool(setup_label and setup_label != "Unknown"),
        "risk_dollars": planned_risk,
        "risk_pct": risk_pct,
        "r_multiple": r_multiple,
        "r_multiple_display": _format_r_multiple(r_multiple),
        "risk_inferred": risk_inferred,
        "reward_risk_at_entry": reward_risk,
        "stop_present": stop_present,
        "target_present": target_present,
        "stop_value": stop_price,
        "target_value": target_price,
        "loss_exceeded_planned_risk": loss_exceeded,
        "oversized": oversized,
        "undersized": undersized,
        "within_plan": within_plan,
        "stop_discipline": stop_discipline,
        "stop_discipline_label": stop_discipline,
        "execution_quality": execution_quality,
        "execution_quality_label": execution_quality,
        "sizing_quality": sizing_quality,
        "sizing_quality_label": sizing_quality,
        "outcome_label": outcome_label,
        "auto_grade_score": auto_score,
        "auto_grade_letter": auto_grade,
        "manual_grade_score": manual_grade_score,
        "manual_grade_letter": manual_grade_letter,
        "final_grade_score": final_score,
        "final_grade_letter": final_grade,
        "trade_score": final_score,
        "trade_grade": final_grade,
        "classification": classification,
        "trade_classification": classification,
        "grade_override_reason": _normalize_choice(row.get("grade_override_reason")),
        "review_completion_pct": review_pct,
        "review_state": {
            "label": review_label,
            "tone": review_tone,
            "meta": f"{review_pct}% complete",
        },
        "review_completion_items": review_completion_items,
        "review_missing_summary": review_missing_summary,
        "grade_components": grade_components,
        "grade_breakdown_items": breakdown_items,
        "weakest_components": weakest_components,
        "strongest_components": strongest_components,
    }
