"""Forward Pace projection planner and PDF export."""

from __future__ import annotations

import io
import json
import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable

from flask import jsonify, render_template, request, send_file

from mccain_capital.runtime import today_iso
from mccain_capital.services.ui import render_page

_STANDARD_DEDUCTION_2026 = {
    "single": 16100.0,
    "married_joint": 32200.0,
    "head_household": 24150.0,
}

_FEDERAL_BRACKETS_2026 = {
    "single": [
        (0, 0.10),
        (12400, 0.12),
        (50400, 0.22),
        (105700, 0.24),
        (201775, 0.32),
        (256225, 0.35),
        (640600, 0.37),
    ],
    "married_joint": [
        (0, 0.10),
        (24800, 0.12),
        (100800, 0.22),
        (211400, 0.24),
        (403550, 0.32),
        (512450, 0.35),
        (768700, 0.37),
    ],
    "head_household": [
        (0, 0.10),
        (17700, 0.12),
        (67450, 0.22),
        (105700, 0.24),
        (201775, 0.32),
        (256200, 0.35),
        (640600, 0.37),
    ],
}

# Planning-rate estimates by state. These are intentionally conservative effective-rate inputs,
# not a substitute for filing software or a CPA review.
_STATE_TAX_RATES = {
    "AL": 0.05,
    "AK": 0.0,
    "AZ": 0.025,
    "AR": 0.039,
    "CA": 0.093,
    "CO": 0.044,
    "CT": 0.055,
    "DC": 0.085,
    "DE": 0.066,
    "FL": 0.0,
    "GA": 0.0519,
    "HI": 0.0825,
    "IA": 0.038,
    "ID": 0.058,
    "IL": 0.0495,
    "IN": 0.03,
    "KS": 0.052,
    "KY": 0.04,
    "LA": 0.0425,
    "MA": 0.05,
    "MD": 0.0575,
    "ME": 0.0715,
    "MI": 0.0425,
    "MN": 0.0785,
    "MO": 0.048,
    "MS": 0.047,
    "MT": 0.059,
    "NC": 0.0425,
    "ND": 0.025,
    "NE": 0.055,
    "NH": 0.0,
    "NJ": 0.0637,
    "NM": 0.049,
    "NV": 0.0,
    "NY": 0.0645,
    "OH": 0.035,
    "OK": 0.0475,
    "OR": 0.0875,
    "PA": 0.0307,
    "RI": 0.0475,
    "SC": 0.064,
    "SD": 0.0,
    "TN": 0.0,
    "TX": 0.0,
    "UT": 0.0455,
    "VA": 0.0575,
    "VT": 0.066,
    "WA": 0.0,
    "WI": 0.053,
    "WV": 0.047,
    "WY": 0.0,
}

_STATES = [
    ("AL", "Alabama"),
    ("AK", "Alaska"),
    ("AZ", "Arizona"),
    ("AR", "Arkansas"),
    ("CA", "California"),
    ("CO", "Colorado"),
    ("CT", "Connecticut"),
    ("DC", "District of Columbia"),
    ("DE", "Delaware"),
    ("FL", "Florida"),
    ("GA", "Georgia"),
    ("HI", "Hawaii"),
    ("IA", "Iowa"),
    ("ID", "Idaho"),
    ("IL", "Illinois"),
    ("IN", "Indiana"),
    ("KS", "Kansas"),
    ("KY", "Kentucky"),
    ("LA", "Louisiana"),
    ("MA", "Massachusetts"),
    ("MD", "Maryland"),
    ("ME", "Maine"),
    ("MI", "Michigan"),
    ("MN", "Minnesota"),
    ("MO", "Missouri"),
    ("MS", "Mississippi"),
    ("MT", "Montana"),
    ("NC", "North Carolina"),
    ("ND", "North Dakota"),
    ("NE", "Nebraska"),
    ("NH", "New Hampshire"),
    ("NJ", "New Jersey"),
    ("NM", "New Mexico"),
    ("NV", "Nevada"),
    ("NY", "New York"),
    ("OH", "Ohio"),
    ("OK", "Oklahoma"),
    ("OR", "Oregon"),
    ("PA", "Pennsylvania"),
    ("RI", "Rhode Island"),
    ("SC", "South Carolina"),
    ("SD", "South Dakota"),
    ("TN", "Tennessee"),
    ("TX", "Texas"),
    ("UT", "Utah"),
    ("VA", "Virginia"),
    ("VT", "Vermont"),
    ("WA", "Washington"),
    ("WI", "Wisconsin"),
    ("WV", "West Virginia"),
    ("WY", "Wyoming"),
]


def forward_pace_page():
    content = render_template("forward_pace.html", today=today_iso(), states=_STATES)
    return render_page(content, active="forward-pace", title="McCain Capital · Forward Pace")


def api_projection():
    payload = request.get_json(silent=True) or {}
    try:
        return jsonify({"ok": True, "projection": build_projection(payload)})
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


def download_pdf():
    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    if "payload" in payload:
        payload = json.loads(payload.get("payload") or "{}")
    try:
        projection = build_projection(payload)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    pdf = _build_pdf(projection)
    return send_file(
        io.BytesIO(pdf),
        mimetype="application/pdf",
        as_attachment=True,
        download_name="forward-pace-projection.pdf",
    )


def build_projection(raw: Dict[str, Any]) -> Dict[str, Any]:
    account_phase = str(raw.get("account_phase") or "evaluation").lower()
    if account_phase not in {"evaluation", "performance"}:
        account_phase = "evaluation"
    base_balance = _money(raw.get("base_balance"), 50000)
    current_balance = _money(raw.get("current_balance"), base_balance)
    daily_profit = _money(raw.get("daily_profit", raw.get("gross_payout")), 250)
    trading_days_week = _bounded_int(
        raw.get("trading_days_week", raw.get("payouts_per_week")), 5, 1, 5
    )
    gross_payout = daily_profit
    payouts_per_week = trading_days_week
    weeks = _bounded_int(raw.get("weeks"), 12, 1, 104)
    fixed_buffer = _money(raw.get("fixed_buffer"), 5000)
    buffer_rate = _bounded_float(raw.get("buffer_rate"), 10, 0, 90) / 100
    state = str(raw.get("state") or "GA").upper()
    filing_status = str(raw.get("filing_status") or "single")
    if filing_status not in _FEDERAL_BRACKETS_2026:
        filing_status = "single"
    start_date = _parse_date(raw.get("start_date"))
    horizon_mode = str(raw.get("horizon_mode") or "weeks").strip().lower()
    if horizon_mode not in {"weeks", "date"}:
        horizon_mode = "weeks"
    if horizon_mode == "date":
        target_date = _parse_date(raw.get("target_date"), required=True)
        if target_date < start_date:
            raise ValueError("Target date must be on or after the start date.")
        if (target_date - start_date).days > 731:
            raise ValueError("Date projections are limited to two years.")
    else:
        target_date = start_date + timedelta(days=(weeks * 7) - 1)
    weekdays = _weekdays_between(start_date, target_date)
    if horizon_mode == "weeks":
        session_count = weeks * 5
    else:
        session_count = len(weekdays)
    if not session_count:
        raise ValueError("The selected date window must include at least one weekday.")
    equivalent_weeks = session_count / 5
    target_balance = _money(raw.get("target_balance"), 0)
    evaluation_target = base_balance * 1.10
    performance_buffer = _money(raw.get("performance_buffer"), 52875 if base_balance == 50000 else 0)
    current_loss_limit = _money(raw.get("current_loss_limit"), 48500 if base_balance == 50000 else 0)
    fixed_loss_limit = _money(raw.get("fixed_loss_limit"), 50375 if base_balance == 50000 else 0)
    safety_cushion = _money(raw.get("safety_cushion"), 1000)
    proposed_payout = _money(raw.get("proposed_payout"), 0)
    next_account_cost = _money(raw.get("next_account_cost"), 0)

    weekly_gross = gross_payout * payouts_per_week
    annual_gross = weekly_gross * 52
    federal_annual = _federal_tax(annual_gross, filing_status)
    state_rate = _STATE_TAX_RATES.get(state, 0)
    state_annual = max(0.0, annual_gross * state_rate)
    weekly_federal = federal_annual / 52
    weekly_state = state_annual / 52
    weekly_tax = weekly_federal + weekly_state
    weekly_buffer = weekly_gross * buffer_rate
    weekly_net = max(0.0, weekly_gross - weekly_tax - weekly_buffer)
    # Broker-account progress uses trading profit directly. Tax and reserves apply only to
    # withdrawn payout income and never reduce evaluation or performance milestones.
    weekly_account_profit = daily_profit * trading_days_week

    daily_gross = weekly_gross / 5
    daily_federal = weekly_federal / 5
    daily_state = weekly_state / 5
    daily_buffer = weekly_buffer / 5
    daily_net = daily_profit

    schedule = []
    balance = current_balance
    net_total = 0.0
    gross_total = 0.0
    tax_total = 0.0
    buffer_total = fixed_buffer
    grouped_days: list[tuple[date, date, int]] = []
    if horizon_mode == "weeks":
        grouped_days = [
            (start_date + timedelta(days=idx * 7), start_date + timedelta(days=idx * 7 + 6), 5)
            for idx in range(weeks)
        ]
    else:
        buckets: list[list[date]] = []
        for day in weekdays:
            if not buckets or buckets[-1][0].isocalendar()[:2] != day.isocalendar()[:2]:
                buckets.append([])
            buckets[-1].append(day)
        grouped_days = [(days[0], days[-1], len(days)) for days in buckets]
    for idx, (row_start, row_end, sessions) in enumerate(grouped_days):
        row_gross = daily_gross * sessions
        row_federal = daily_federal * sessions
        row_state = daily_state * sessions
        row_buffer = daily_buffer * sessions
        row_net = daily_net * sessions
        balance += row_net
        net_total += row_net
        gross_total += row_gross
        tax_total += row_federal + row_state
        buffer_total += row_buffer
        schedule.append(
            {
                "week": idx + 1,
                "start": row_start.isoformat(),
                "end": row_end.isoformat(),
                "sessions": sessions,
                "partial": sessions < 5,
                "gross": round(row_gross, 2),
                "federal_tax": round(row_federal, 2),
                "state_tax": round(row_state, 2),
                "buffer": round(row_buffer, 2),
                "net": round(row_net, 2),
                "projected_balance": round(balance, 2),
            }
        )

    phase_target = evaluation_target if account_phase == "evaluation" else performance_buffer
    target_balance = target_balance or phase_target
    target_remaining = max(0.0, target_balance - current_balance)
    target_active = target_balance > base_balance
    required_daily = target_remaining / session_count if target_active else 0.0
    required_weekly = required_daily * 5
    pace_ratio = daily_net / required_daily if required_daily else 0.0
    target_status = "neutral"
    if target_active:
        target_status = "ahead" if pace_ratio > 1.02 else "behind" if pace_ratio < 0.98 else "on_track"
    projected_gap = balance - target_balance if target_active else 0.0
    weekly_adjustment = required_weekly - weekly_account_profit if target_active else 0.0
    scenarios = []
    for key, label, multiplier in (
        ("conservative", "Conservative", 0.75),
        ("base", "Base", 1.0),
        ("stretch", "Stretch", 1.25),
    ):
        scenario_net = daily_net * session_count * multiplier
        scenario_daily = daily_net * multiplier
        sessions_to_target = (
            math.ceil(target_remaining / scenario_daily)
            if target_active and target_remaining > 0 and scenario_daily > 0
            else 0
        )
        completion_date = (
            _advance_weekdays(start_date, sessions_to_target).isoformat()
            if sessions_to_target
            else ""
        )
        scenarios.append(
            {
                "key": key,
                "label": label,
                "multiplier": multiplier,
                "net": round(scenario_net, 2),
                "projected_balance": round(current_balance + scenario_net, 2),
                "gap_to_target": round(current_balance + scenario_net - target_balance, 2)
                if target_active
                else 0.0,
                "sessions_to_target": sessions_to_target,
                "completion_date": completion_date,
            }
        )

    return {
        "inputs": {
            "base_balance": round(base_balance, 2),
            "current_balance": round(current_balance, 2),
            "account_phase": account_phase,
            "daily_profit": round(daily_profit, 2),
            "trading_days_week": trading_days_week,
            "gross_payout": round(gross_payout, 2),
            "payouts_per_week": payouts_per_week,
            "weeks": weeks,
            "horizon_mode": horizon_mode,
            "fixed_buffer": round(fixed_buffer, 2),
            "buffer_rate": round(buffer_rate * 100, 2),
            "state": state,
            "filing_status": filing_status,
            "start_date": start_date.isoformat(),
            "target_date": target_date.isoformat(),
            "target_balance": round(target_balance, 2),
        },
        "tax": {
            "annual_gross": round(annual_gross, 2),
            "taxable_federal_income": round(
                max(0.0, annual_gross - _STANDARD_DEDUCTION_2026[filing_status]), 2
            ),
            "federal_annual": round(federal_annual, 2),
            "state_annual": round(state_annual, 2),
            "state_rate": round(state_rate * 100, 3),
            "effective_tax_rate": (
                round((federal_annual + state_annual) / annual_gross * 100, 2)
                if annual_gross
                else 0
            ),
        },
        "totals": {
            "gross": round(gross_total, 2),
            "tax": round(tax_total, 2),
            "buffer": round(buffer_total, 2),
            "net": round(net_total, 2),
            "projected_balance": round(balance, 2),
        },
        "window": {
            "start_date": start_date.isoformat(),
            "target_date": target_date.isoformat(),
            "calendar_days": (target_date - start_date).days + 1,
            "sessions": session_count,
            "equivalent_weeks": round(equivalent_weeks, 2),
            "schedule_weeks": len(schedule),
            "assumption": "Monday-Friday weekdays; weekends excluded, market holidays not excluded.",
        },
        "target": {
            "active": target_active,
            "balance": round(target_balance, 2),
            "remaining": round(target_remaining, 2),
            "required_daily": round(required_daily, 2),
            "required_weekly": round(required_weekly, 2),
            "pace_ratio": round(pace_ratio, 3),
            "status": target_status,
            "projected_gap": round(projected_gap, 2),
            "weekly_adjustment": round(weekly_adjustment, 2),
        },
        "lifecycle": _build_lifecycle(
            phase=account_phase,
            plan_size=base_balance,
            current_balance=current_balance,
            projected_balance=balance,
            evaluation_target=evaluation_target,
            performance_buffer=performance_buffer,
            current_loss_limit=current_loss_limit,
            fixed_loss_limit=fixed_loss_limit,
            safety_cushion=safety_cushion,
            proposed_payout=proposed_payout,
            next_account_cost=next_account_cost,
            start_date=start_date,
            daily_profit=daily_profit,
        ),
        "scenarios": scenarios,
        "weekly": {
            "gross": round(weekly_gross, 2),
            "federal_tax": round(weekly_federal, 2),
            "state_tax": round(weekly_state, 2),
            "buffer": round(weekly_buffer, 2),
            "net": round(weekly_account_profit, 2),
        },
        "daily": {
            "gross": round(daily_gross, 2),
            "tax": round(daily_federal + daily_state, 2),
            "buffer": round(daily_buffer, 2),
            "net": round(daily_profit, 2),
        },
        "schedule": schedule,
        "notes": [
            "Planning estimate only; verify with tax software or a CPA before filing.",
            "State tax uses a conservative planning rate so projections work across every state.",
            "Session counts use Monday-Friday weekdays; market holidays are not excluded.",
        ],
    }


def _federal_tax(annual_gross: float, filing_status: str) -> float:
    taxable = max(0.0, annual_gross - _STANDARD_DEDUCTION_2026[filing_status])
    brackets = _FEDERAL_BRACKETS_2026[filing_status]
    tax = 0.0
    for idx, (floor, rate) in enumerate(brackets):
        next_floor = brackets[idx + 1][0] if idx + 1 < len(brackets) else math.inf
        if taxable <= floor:
            break
        tax += (min(taxable, next_floor) - floor) * rate
    return max(0.0, tax)


def _milestone_date(start: date, current: float, target: float, daily_profit: float) -> str:
    if target <= current:
        return start.isoformat()
    if daily_profit <= 0:
        return ""
    sessions = math.ceil((target - current) / daily_profit)
    return _advance_weekdays(start, sessions).isoformat()


def _build_lifecycle(
    *,
    phase: str,
    plan_size: float,
    current_balance: float,
    projected_balance: float,
    evaluation_target: float,
    performance_buffer: float,
    current_loss_limit: float,
    fixed_loss_limit: float,
    safety_cushion: float,
    proposed_payout: float,
    next_account_cost: float,
    start_date: date,
    daily_profit: float,
) -> Dict[str, Any]:
    buffer_reached = bool(performance_buffer and current_balance >= performance_buffer)
    applicable_loss_limit = fixed_loss_limit if buffer_reached else current_loss_limit
    protected_floor = applicable_loss_limit + safety_cushion
    theoretical_capacity = max(0.0, current_balance - applicable_loss_limit)
    protected_capacity = max(0.0, current_balance - protected_floor)
    projected_protected_capacity = max(0.0, projected_balance - protected_floor)
    post_payout_balance = current_balance - proposed_payout
    post_payout_cushion = post_payout_balance - applicable_loss_limit
    payout_safe = proposed_payout <= protected_capacity and proposed_payout > 0
    recommendation = _build_recommendation(
        phase=phase,
        current_balance=current_balance,
        projected_balance=projected_balance,
        evaluation_target=evaluation_target,
        performance_buffer=performance_buffer,
        protected_floor=protected_floor,
        fixed_protected_floor=fixed_loss_limit + safety_cushion,
        protected_capacity=protected_capacity,
        proposed_payout=proposed_payout,
        start_date=start_date,
        daily_profit=daily_profit,
    )
    next_milestone = evaluation_target if phase == "evaluation" else performance_buffer
    next_label = "Pass evaluation" if phase == "evaluation" else (
        "Build protected payout" if buffer_reached else "Reach performance buffer"
    )
    if phase == "performance" and buffer_reached:
        next_milestone = protected_floor + max(proposed_payout, next_account_cost)
    scenario_dates = []
    for key, label, multiplier in (
        ("conservative", "Conservative", 0.75),
        ("expected", "Expected", 1.0),
        ("stretch", "Stretch", 1.25),
    ):
        pace = daily_profit * multiplier
        scenario_dates.append(
            {
                "key": key,
                "label": label,
                "multiplier": multiplier,
                "daily_profit": round(pace, 2),
                "evaluation_date": _milestone_date(
                    start_date, current_balance, evaluation_target, pace
                ),
                "buffer_date": _milestone_date(
                    start_date, current_balance, performance_buffer, pace
                )
                if performance_buffer
                else "",
                "protected_payout_date": _milestone_date(
                    start_date,
                    current_balance,
                    protected_floor + max(proposed_payout, next_account_cost),
                    pace,
                )
                if applicable_loss_limit
                else "",
            }
        )
    if phase == "evaluation":
        milestones = [
            {"key": "start", "label": "Starting Balance", "value": round(plan_size, 2)},
            {
                "key": "evaluation",
                "label": "Passing Target",
                "value": round(evaluation_target, 2),
            },
        ]
        steps = [
            {
                "key": "start",
                "label": "Evaluation Started",
                "value": round(plan_size, 2),
                "state": "complete",
            },
            {
                "key": "current",
                "label": "Current Balance",
                "value": round(current_balance, 2),
                "state": "active",
            },
            {
                "key": "evaluation",
                "label": "Pass Evaluation",
                "value": round(evaluation_target, 2),
                "state": "complete" if current_balance >= evaluation_target else "next",
            },
        ]
    else:
        milestones = [
            {"key": "buffer", "label": "Performance Buffer", "value": round(performance_buffer, 2)},
            {"key": "loss", "label": "Applicable Loss Limit", "value": round(applicable_loss_limit, 2)},
            {"key": "protected", "label": "Protected Floor", "value": round(protected_floor, 2)},
        ]
        steps = [
            {
                "key": "start",
                "label": "Performance Account",
                "value": round(plan_size, 2),
                "state": "complete",
            },
            {
                "key": "buffer",
                "label": "Reach Profit Buffer",
                "value": round(performance_buffer, 2),
                "state": "complete" if buffer_reached else "active",
            },
            {
                "key": "fixed",
                "label": "Fix Loss Limit",
                "value": round(fixed_loss_limit, 2),
                "state": "complete" if buffer_reached else "locked",
            },
            {
                "key": "payout",
                "label": "Protected Payout",
                "value": round(protected_capacity, 2),
                "state": "active" if buffer_reached else "locked",
            },
        ]
    return {
        "phase": phase,
        "phase_label": "Evaluation" if phase == "evaluation" else "Performance",
        "evaluation_target": round(evaluation_target, 2),
        "evaluation_remaining": round(max(0.0, evaluation_target - current_balance), 2),
        "performance_buffer": round(performance_buffer, 2),
        "buffer_reached": buffer_reached,
        "current_loss_limit": round(current_loss_limit, 2),
        "fixed_loss_limit": round(fixed_loss_limit, 2),
        "applicable_loss_limit": round(applicable_loss_limit, 2),
        "loss_limit_state": "Fixed" if buffer_reached else "Trailing / current",
        "safety_cushion": round(safety_cushion, 2),
        "protected_floor": round(protected_floor, 2),
        "theoretical_capacity": round(theoretical_capacity, 2),
        "protected_capacity": round(protected_capacity, 2),
        "projected_protected_capacity": round(projected_protected_capacity, 2),
        "proposed_payout": round(proposed_payout, 2),
        "post_payout_balance": round(post_payout_balance, 2),
        "post_payout_cushion": round(post_payout_cushion, 2),
        "payout_safe": payout_safe,
        "recommendation": recommendation,
        "next_account_cost": round(next_account_cost, 2),
        "next_label": next_label,
        "next_milestone": round(next_milestone, 2),
        "next_remaining": round(max(0.0, next_milestone - current_balance), 2),
        "next_date": _milestone_date(start_date, current_balance, next_milestone, daily_profit),
        "milestones": milestones,
        "steps": steps,
        "scenarios": scenario_dates,
        "rule_source": (
            "Evaluation target: Vanquish Basic Options Plan, 10% of starting balance. "
            "The $50,000 performance defaults use the documented $52,875 buffer and "
            "$50,375 fixed loss-limit example; verify current account rules before acting."
        ),
    }


def _build_recommendation(
    *,
    phase: str,
    current_balance: float,
    projected_balance: float,
    evaluation_target: float,
    performance_buffer: float,
    protected_floor: float,
    fixed_protected_floor: float,
    protected_capacity: float,
    proposed_payout: float,
    start_date: date,
    daily_profit: float,
) -> Dict[str, Any]:
    if phase == "evaluation":
        passed_now = current_balance >= evaluation_target
        passed_by_end = projected_balance >= evaluation_target
        remaining = max(0.0, evaluation_target - current_balance)
        return {
            "action": "qualified" if passed_now else "continue",
            "eyebrow": "Evaluation passed" if passed_now else "Keep building",
            "title": (
                "Evaluation target is already reached."
                if passed_now
                else f"Keep trading until the balance reaches ${evaluation_target:,.0f}."
            ),
            "detail": (
                "Move to Performance planning after the account transition is confirmed."
                if passed_now
                else (
                    "The selected pace reaches the target within this projection."
                    if passed_by_end
                    else "The selected projection ends before the passing balance is reached."
                )
            ),
            "decision_amount": round(evaluation_target, 2),
            "required_balance": round(evaluation_target, 2),
            "additional_profit": round(remaining, 2),
            "sessions_to_ready": math.ceil(remaining / daily_profit) if remaining and daily_profit else 0,
            "ready_date": _milestone_date(
                start_date, current_balance, evaluation_target, daily_profit
            ),
            "post_action_balance": round(current_balance, 2),
            "projected_status": "Passes by end date" if passed_by_end else "Below target at end date",
        }

    buffer_reached = bool(performance_buffer and current_balance >= performance_buffer)
    desired_payout = proposed_payout
    payout_required_balance = (
        fixed_protected_floor + desired_payout if desired_payout > 0 else 0.0
    )
    if not buffer_reached:
        required_balance = max(performance_buffer, payout_required_balance)
        action = "wait"
        decision_amount = desired_payout
        if desired_payout > 0:
            title = f"Build to ${required_balance:,.0f}, then take the ${desired_payout:,.0f} payout."
            detail = "That first clears the buffer, then preserves the selected cushion after payout."
        else:
            title = f"Do not withdraw yet. Build the balance to ${performance_buffer:,.0f}."
            detail = "Reach the performance buffer first so the fixed-limit payout plan applies."
    elif desired_payout > 0 and protected_capacity >= desired_payout:
        required_balance = protected_floor + desired_payout
        action = "withdraw"
        title = f"A ${desired_payout:,.0f} payout fits the protected plan now."
        detail = "The selected safety cushion remains above the applicable loss limit."
        decision_amount = desired_payout
    elif desired_payout > 0:
        required_balance = protected_floor + desired_payout
        action = "wait"
        title = f"Wait until ${required_balance:,.0f} before taking ${desired_payout:,.0f}."
        detail = "That balance funds the payout while preserving the selected safety cushion."
        decision_amount = desired_payout
    else:
        recommended = math.floor(protected_capacity / 100) * 100
        if recommended >= 100:
            required_balance = current_balance
            action = "withdraw"
            title = f"Up to ${recommended:,.0f} is protected by the current cushion."
            detail = "Enter a desired payout to compare a specific withdrawal plan."
            decision_amount = recommended
        else:
            required_balance = protected_floor + 100
            action = "wait"
            title = f"Wait until ${required_balance:,.0f} before taking a payout."
            detail = "The account needs room above the protected floor first."
            decision_amount = 100.0

    additional_profit = max(0.0, required_balance - current_balance)
    sessions_to_ready = (
        math.ceil(additional_profit / daily_profit) if additional_profit and daily_profit else 0
    )
    return {
        "action": action,
        "eyebrow": "Take payout" if action == "withdraw" else "Wait for protection",
        "title": title,
        "detail": detail,
        "decision_amount": round(decision_amount, 2),
        "required_balance": round(required_balance, 2),
        "additional_profit": round(additional_profit, 2),
        "sessions_to_ready": sessions_to_ready,
        "ready_date": _milestone_date(
            start_date, current_balance, required_balance, daily_profit
        ),
        "post_action_balance": round(
            (current_balance if action == "withdraw" else required_balance) - decision_amount,
            2,
        ),
        "projected_status": (
            "Ready by end date" if projected_balance >= required_balance else "Not ready by end date"
        ),
    }


def _build_pdf(projection: Dict[str, Any]) -> bytes:
    lines = [
        "McCain Capital - Forward Pace Projection",
        f"State: {projection['inputs']['state']}    Filing: {projection['inputs']['filing_status']}",
        f"Window: {projection['window']['start_date']} to {projection['window']['target_date']}",
        f"Sessions: {projection['window']['sessions']}    Equivalent weeks: {projection['window']['equivalent_weeks']}",
        "",
        f"Base Balance: {_fmt_money(projection['inputs']['base_balance'])}",
        f"Weekly Gross: {_fmt_money(projection['weekly']['gross'])}",
        f"Weekly Tax: {_fmt_money(projection['weekly']['federal_tax'] + projection['weekly']['state_tax'])}",
        f"Weekly Buffer: {_fmt_money(projection['weekly']['buffer'])}",
        f"Weekly Net Pace: {_fmt_money(projection['weekly']['net'])}",
        f"Projected Balance: {_fmt_money(projection['totals']['projected_balance'])}",
        f"Target Balance: {_fmt_money(projection['target']['balance']) if projection['target']['active'] else 'Not set'}",
        f"Required Daily Pace: {_fmt_money(projection['target']['required_daily']) if projection['target']['active'] else 'Not set'}",
        "",
        "Scenarios",
        *[
            f"{row['label']} ({row['multiplier']:.0%}) - Balance {_fmt_money(row['projected_balance'])}"
            for row in projection["scenarios"]
        ],
        "",
        "Weekly Schedule",
    ]
    for row in projection["schedule"]:
        lines.append(
            f"W{row['week']:02d} {row['start']} to {row['end']} ({row['sessions']} sessions) - "
            f"Gross {_fmt_money(row['gross'])} | "
            f"Tax {_fmt_money(row['federal_tax'] + row['state_tax'])} | "
            f"Buffer {_fmt_money(row['buffer'])} | Net {_fmt_money(row['net'])} | "
            f"Balance {_fmt_money(row['projected_balance'])}"
        )
    lines.extend(["", *projection["notes"]])
    return _simple_pdf(lines)


def _simple_pdf(lines: Iterable[str]) -> bytes:
    escaped_lines = [_pdf_escape(line) for line in lines]
    chunks = [escaped_lines[idx : idx + 42] for idx in range(0, len(escaped_lines), 42)] or [[]]
    page_refs = [f"{4 + idx * 2} 0 R" for idx in range(len(chunks))]
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        f"<< /Type /Pages /Kids [{' '.join(page_refs)}] /Count {len(chunks)} >>".encode(),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    for idx, chunk in enumerate(chunks):
        content_ref = 5 + idx * 2
        text_ops = ["BT", "/F1 12 Tf", "50 760 Td", "16 TL"]
        for line in chunk:
            text_ops.append(f"({line}) Tj")
            text_ops.append("T*")
        text_ops.append("ET")
        stream = "\n".join(text_ops).encode("latin-1", "replace")
        objects.append(
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_ref} 0 R >>"
            ).encode()
        )
        objects.append(
            b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream"
        )
    out = io.BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets = [0]
    for idx, obj in enumerate(objects, start=1):
        offsets.append(out.tell())
        out.write(f"{idx} 0 obj\n".encode())
        out.write(obj)
        out.write(b"\nendobj\n")
    xref = out.tell()
    out.write(f"xref\n0 {len(objects) + 1}\n".encode())
    out.write(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        out.write(f"{offset:010d} 00000 n \n".encode())
    out.write(
        f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode()
    )
    return out.getvalue()


def _pdf_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")[:110]


def _weekdays_between(start: date, end: date) -> list[date]:
    return [
        start + timedelta(days=offset)
        for offset in range((end - start).days + 1)
        if (start + timedelta(days=offset)).weekday() < 5
    ]


def _advance_weekdays(start: date, sessions: int) -> date:
    if sessions <= 1:
        return start
    day = start
    remaining = sessions - 1 if start.weekday() < 5 else sessions
    while remaining:
        day += timedelta(days=1)
        if day.weekday() < 5:
            remaining -= 1
    return day


def _parse_date(value: Any, *, required: bool = False) -> date:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        if required:
            raise ValueError("Enter a valid target date.") from None
        return date.fromisoformat(today_iso())


def _money(value: Any, default: float) -> float:
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return default


def _bounded_int(value: Any, default: int, low: int, high: int) -> int:
    try:
        return min(high, max(low, int(value)))
    except (TypeError, ValueError):
        return default


def _bounded_float(value: Any, default: float, low: float, high: float) -> float:
    try:
        return min(high, max(low, float(value)))
    except (TypeError, ValueError):
        return default


def _fmt_money(value: float) -> str:
    return f"${value:,.2f}"
