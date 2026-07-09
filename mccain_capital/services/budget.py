"""Budget Command Center page and JSON-backed personal finance storage."""

from __future__ import annotations

import json
import os
import uuid
from calendar import monthrange
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional

from flask import jsonify, redirect, request, url_for

from mccain_capital import runtime as app_runtime
from mccain_capital.runtime import now_iso, today_iso

_STORE_FILE = "budget.json"
_INCOME_TYPES = {"job", "trading", "business", "side_hustle", "other"}
_FREQUENCIES = {"weekly", "biweekly", "semimonthly", "monthly", "one_time"}
_BUDGET_ITEM_TYPES = {"bill", "debt", "subscription", "food", "gas", "savings", "other"}
_PAYCHECK_ALLOCATIONS = {"first_check", "second_check", "split", "any"}
_FIXED_BUDGET_CATEGORIES = {
    "housing",
    "debt",
    "utilities",
    "insurance",
    "auto",
    "subscriptions",
    "irs",
}
_VARIABLE_BUDGET_TYPES = {"food", "gas", "other"}
_VARIABLE_BUDGET_CATEGORIES = {"food", "gas", "other", "household", "entertainment"}
_CHARGE_CATEGORIES = {
    "food",
    "gas",
    "household",
    "subscriptions",
    "entertainment",
    "trading",
    "debt",
    "savings",
    "giving",
    "other",
}
_NEED_WANT = {"need", "want", "investment", "leak"}
_PAYMENT_METHODS = {"debit", "credit", "cash", "bank", "other"}
_PRIORITIES = {"low", "medium", "high"}


def budget_page():
    return redirect(url_for("executive_dashboard"), code=302)


def api_summary():
    store = _read_store()
    return jsonify({"ok": True, "summary": _build_summary(store)})


def api_data():
    return jsonify({"ok": True, "data": _read_store()})


def api_analytics():
    store = _read_store()
    return jsonify({"ok": True, "analytics": _build_analytics(store)})


def api_profile():
    payload = request.get_json(silent=True) or {}
    store = _read_store()
    profile = _normalize_profile({**store.get("profile", {}), **payload})
    store["profile"] = profile
    _write_store(store)
    return jsonify({"ok": True, "profile": profile, "summary": _build_summary(store)})


def api_upsert_income():
    return _upsert_item("income_sources", request.get_json(silent=True) or {}, _normalize_income)


def api_delete_income(item_id: str):
    return _delete_item("income_sources", item_id)


def api_upsert_bill():
    return _upsert_item("bills", request.get_json(silent=True) or {}, _normalize_bill)


def api_delete_bill(item_id: str):
    return _delete_item("bills", item_id)


def api_upsert_charge():
    return _upsert_item("charges", request.get_json(silent=True) or {}, _normalize_charge)


def api_delete_charge(item_id: str):
    return _delete_item("charges", item_id)


def api_upsert_debt():
    return _upsert_item("debts", request.get_json(silent=True) or {}, _normalize_debt)


def api_delete_debt(item_id: str):
    return _delete_item("debts", item_id)


def api_upsert_goal():
    return _upsert_item("goals", request.get_json(silent=True) or {}, _normalize_goal)


def api_delete_goal(item_id: str):
    return _delete_item("goals", item_id)


def api_monthly_review():
    payload = request.get_json(silent=True) or {}
    store = _read_store()
    summary = _build_summary(store)
    analytics = _build_analytics(store)
    review = {
        "id": str(uuid.uuid4()),
        "month": str(payload.get("month") or _month_key(_today())),
        "created_at": now_iso(),
        "total_income": summary["projected_monthly_income"],
        "total_bills": summary["fixed_bills_total"],
        "total_charges": summary["variable_spending_total"],
        "total_saved": summary["savings_goal_total"],
        "total_debt_paid": summary["debt_minimums_total"],
        "cash_left": summary["projected_cash_left"],
        "biggest_leak": summary["biggest_leak_category"],
        "best_category": _best_category(analytics["spending_by_category"]),
        "adjustment": str(payload.get("adjustment") or _closeout_adjustment(summary)).strip(),
    }
    store.setdefault("monthly_reviews", []).append(review)
    _write_store(store)
    return jsonify({"ok": True, "review": review, "summary": summary})


def _storage_path() -> str:
    return os.path.join(str(app_runtime.PERSISTENT_DATA_DIR), _STORE_FILE)


def _read_store() -> Dict[str, Any]:
    path = _storage_path()
    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except FileNotFoundError:
        return _default_store()
    except (json.JSONDecodeError, OSError):
        return _default_store()

    if not isinstance(raw, dict):
        return _default_store()
    return _normalize_store(raw)


def _write_store(store: Dict[str, Any]) -> None:
    normalized = _normalize_store(store)
    path = _storage_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(normalized, handle, indent=2, sort_keys=True)
    os.replace(tmp_path, path)


def _default_store() -> Dict[str, Any]:
    return {
        "profile": _normalize_profile({}),
        "income_sources": [],
        "bills": [],
        "charges": [],
        "debts": [],
        "goals": [],
        "monthly_reviews": [],
    }


def _normalize_store(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "profile": _normalize_profile(raw.get("profile", {})),
        "income_sources": [_normalize_income(item) for item in _list(raw.get("income_sources"))],
        "bills": [_normalize_bill(item) for item in _list(raw.get("bills"))],
        "charges": [_normalize_charge(item) for item in _list(raw.get("charges"))],
        "debts": [_normalize_debt(item) for item in _list(raw.get("debts"))],
        "goals": [_normalize_goal(item) for item in _list(raw.get("goals"))],
        "monthly_reviews": _list(raw.get("monthly_reviews")),
    }


def _normalize_profile(raw: Dict[str, Any]) -> Dict[str, Any]:
    pay_frequency = str(raw.get("pay_frequency") or "biweekly").strip().lower()
    return {
        "monthly_take_home": _money(raw.get("monthly_take_home", 7200)),
        "hourly_after_tax": _money(raw.get("hourly_after_tax", 46.20)),
        "workday_income": _money(raw.get("workday_income", 372)),
        "target_extra_monthly_income": _money(raw.get("target_extra_monthly_income", 4000)),
        "pay_frequency": pay_frequency if pay_frequency in _FREQUENCIES else "biweekly",
        "paycheck_amount": _nullable_money(raw.get("paycheck_amount")),
        "paycheck_dates": [str(item)[:10] for item in raw.get("paycheck_dates", []) if str(item).strip()][:4]
        if isinstance(raw.get("paycheck_dates"), list)
        else [],
        "currency": str(raw.get("currency") or "USD").strip().upper()[:3] or "USD",
    }


def _normalize_income(raw: Dict[str, Any]) -> Dict[str, Any]:
    item_type = str(raw.get("type") or "job").strip().lower()
    frequency = str(raw.get("frequency") or "monthly").strip().lower()
    return {
        "id": _item_id(raw),
        "name": str(raw.get("name") or "Income Source").strip()[:120],
        "type": item_type if item_type in _INCOME_TYPES else "other",
        "amount": _money(raw.get("amount")),
        "frequency": frequency if frequency in _FREQUENCIES else "monthly",
        "pay_day": str(raw.get("pay_day") or "").strip()[:80],
        "active": _bool(raw.get("active", True)),
        "notes": str(raw.get("notes") or "").strip()[:500],
    }


def _normalize_bill(raw: Dict[str, Any]) -> Dict[str, Any]:
    category = str(raw.get("category") or _infer_category(raw.get("name"))).strip().lower()[:60]
    item_type = str(raw.get("type") or _infer_type(raw.get("name"), category)).strip().lower()
    allocation = str(raw.get("paycheck_allocation") or "any").strip().lower()
    stamp = now_iso()
    return {
        "id": _item_id(raw),
        "name": str(raw.get("name") or "Bill").strip()[:120],
        "category": category,
        "type": item_type if item_type in _BUDGET_ITEM_TYPES else "bill",
        "amount": _money(raw.get("amount")),
        "due_day": _optional_day(raw.get("due_day")),
        "frequency": "monthly",
        "paycheck_allocation": allocation if allocation in _PAYCHECK_ALLOCATIONS else "any",
        "autopay": _bool(raw.get("autopay")),
        "paid": _bool(raw.get("paid")),
        "essential": _bool(raw.get("essential", True)),
        "active": _bool(raw.get("active", True)),
        "notes": str(raw.get("notes") or "").strip()[:500],
        "created_at": str(raw.get("created_at") or stamp),
        "updated_at": stamp,
    }


def _normalize_charge(raw: Dict[str, Any]) -> Dict[str, Any]:
    category = str(raw.get("category") or "other").strip().lower()
    method = str(raw.get("payment_method") or "debit").strip().lower()
    need_or_want = str(raw.get("need_or_want") or "need").strip().lower()
    return {
        "id": _item_id(raw),
        "date": _date_text(raw.get("date") or today_iso()),
        "name": str(raw.get("name") or "Charge").strip()[:120],
        "category": category if category in _CHARGE_CATEGORIES else "other",
        "amount": _money(raw.get("amount")),
        "payment_method": method if method in _PAYMENT_METHODS else "other",
        "need_or_want": need_or_want if need_or_want in _NEED_WANT else "need",
        "notes": str(raw.get("notes") or "").strip()[:500],
    }


def _normalize_debt(raw: Dict[str, Any]) -> Dict[str, Any]:
    priority = str(raw.get("priority") or "medium").strip().lower()
    return {
        "id": _item_id(raw),
        "name": str(raw.get("name") or "Debt").strip()[:120],
        "balance": _money(raw.get("balance")),
        "minimum_payment": _money(raw.get("minimum_payment")),
        "interest_rate": _rate(raw.get("interest_rate")),
        "due_day": _day(raw.get("due_day")),
        "priority": priority if priority in _PRIORITIES else "medium",
        "notes": str(raw.get("notes") or "").strip()[:500],
    }


def _normalize_goal(raw: Dict[str, Any]) -> Dict[str, Any]:
    priority = str(raw.get("priority") or "medium").strip().lower()
    target = _money(raw.get("target_amount"))
    current = min(_money(raw.get("current_amount")), target) if target else _money(raw.get("current_amount"))
    return {
        "id": _item_id(raw),
        "name": str(raw.get("name") or "Savings Goal").strip()[:120],
        "target_amount": target,
        "current_amount": current,
        "target_date": _date_text(raw.get("target_date") or ""),
        "monthly_contribution": _money(raw.get("monthly_contribution")),
        "priority": priority if priority in _PRIORITIES else "medium",
        "notes": str(raw.get("notes") or "").strip()[:500],
    }


def _upsert_item(key: str, payload: Dict[str, Any], normalizer) -> Any:
    store = _read_store()
    item = normalizer(payload)
    replaced = False
    next_items = []
    for existing in store.get(key, []):
        same_id = str(existing.get("id")) == item["id"]
        same_bill = key == "bills" and _bill_identity(existing) == _bill_identity(item)
        if same_id or same_bill:
            if same_bill and not same_id:
                item["id"] = existing["id"]
                item["created_at"] = existing.get("created_at", item.get("created_at", ""))
            item["updated_at"] = now_iso()
            next_items.append(item)
            replaced = True
        else:
            next_items.append(existing)
    if not replaced:
        next_items.append(item)
    store[key] = next_items
    _write_store(store)
    return jsonify({"ok": True, "item": item, "summary": _build_summary(store), "analytics": _build_analytics(store)})


def _delete_item(key: str, item_id: str) -> Any:
    store = _read_store()
    store[key] = [item for item in store.get(key, []) if str(item.get("id")) != str(item_id)]
    _write_store(store)
    return jsonify({"ok": True, "summary": _build_summary(store), "analytics": _build_analytics(store)})


def _build_summary(store: Dict[str, Any]) -> Dict[str, Any]:
    store = _normalize_store(store)
    profile = store["profile"]
    income_sources_total = sum(_monthly_income(item) for item in store["income_sources"] if item["active"])
    monthly_income = profile["monthly_take_home"] or income_sources_total
    fixed_bills = sum(float(item["amount"]) for item in store["bills"] if _is_fixed_item(item))
    planned_variable = sum(float(item["amount"]) for item in store["bills"] if _is_variable_item(item))
    charges_total = sum(float(item["amount"]) for item in _current_month_items(store["charges"], "date"))
    debt_minimums = sum(float(item["minimum_payment"]) for item in store["debts"])
    savings = sum(float(item["monthly_contribution"]) for item in store["goals"])
    total_planned_outflow = fixed_bills + planned_variable + savings
    projected_cash_left = monthly_income - total_planned_outflow
    actual_cash_left = monthly_income - fixed_bills - charges_total - savings
    current_month_spent = fixed_bills + charges_total + savings
    wants_leaks = sum(
        float(item["amount"])
        for item in _current_month_items(store["charges"], "date")
        if item["need_or_want"] in {"want", "leak"}
    )
    upcoming = _upcoming_bills(store["bills"])
    overdue = _overdue_bills(store["bills"])
    category_totals = _spending_by_category(store["charges"])
    leak_totals = _spending_by_category([c for c in store["charges"] if c["need_or_want"] == "leak"])
    summary = {
        "monthly_take_home": round(monthly_income, 2),
        "monthly_income": round(monthly_income, 2),
        "projected_monthly_income": round(monthly_income, 2),
        "fixed_bills_total": round(fixed_bills, 2),
        "planned_variable_total": round(planned_variable, 2),
        "variable_spending_total": round(charges_total, 2),
        "actual_charges_total": round(charges_total, 2),
        "debt_minimums_total": round(debt_minimums, 2),
        "savings_goal_total": round(savings, 2),
        "total_planned_outflow": round(total_planned_outflow, 2),
        "projected_cash_left": round(projected_cash_left, 2),
        "actual_cash_left": round(actual_cash_left, 2),
        "cashflow_health_score": _cashflow_score(
            take_home=monthly_income,
            projected_cash_left=projected_cash_left,
            fixed_bills=fixed_bills,
            wants_leaks=wants_leaks,
            savings=savings,
            debt_minimums=debt_minimums,
            overdue=bool(overdue),
        ),
        "current_month_spent": round(current_month_spent, 2),
        "current_month_remaining": round(monthly_income - current_month_spent, 2),
        "top_spending_category": _top_category(category_totals),
        "biggest_leak_category": _top_category(leak_totals),
        "upcoming_bills_next_14_days": upcoming,
        "upcoming_bills_total": round(sum(float(item["amount"]) for item in upcoming), 2),
        "overdue_bills_count": len(overdue),
        "income_gap_to_goal": round(profile["target_extra_monthly_income"] - _extra_income_this_month(store), 2),
        "paycheck_allocation": _paycheck_allocation(store, monthly_income),
        "leak_analysis": _leak_analysis(store, projected_cash_left),
        "goal_allocation": _goal_allocation(projected_cash_left),
    }
    summary["score_state"] = _score_state(summary["cashflow_health_score"])
    return summary


def _build_analytics(store: Dict[str, Any]) -> Dict[str, Any]:
    store = _normalize_store(store)
    charges = _current_month_items(store["charges"], "date")
    spending_by_category = _planned_by_category(store["bills"]) or _spending_by_category(charges)
    need_vs_want = _need_breakdown(charges)
    leaks_total = round(sum(float(item["amount"]) for item in charges if item["need_or_want"] == "leak"), 2)
    summary = _build_summary(store)
    analytics = {
        "spending_by_category": spending_by_category,
        "cashflow_by_week": _cashflow_by_week(store, summary),
        "bills_by_due_date": _bills_by_due_date(store["bills"]),
        "need_vs_want_breakdown": need_vs_want,
        "leaks_total": leaks_total,
        "monthly_income_trend": _monthly_income_trend(store),
        "goal_progress": [_goal_progress(goal) for goal in store["goals"]],
        "debt_summary": _debt_summary(store["debts"]),
        "budget_warnings": _budget_warnings(store, summary, leaks_total),
        "budget_recommendations": _budget_recommendations(store, summary, spending_by_category),
    }
    return analytics


def _cashflow_score(
    *, take_home: float, projected_cash_left: float, fixed_bills: float, wants_leaks: float,
    savings: float, debt_minimums: float, overdue: bool
) -> int:
    score = 0
    if projected_cash_left > 0:
        score += 25
    if take_home and fixed_bills <= take_home * 0.5:
        score += 20
    if take_home and wants_leaks <= take_home * 0.15:
        score += 20
    if savings > 0:
        score += 15
    if take_home and debt_minimums <= take_home * 0.15:
        score += 10
    if not overdue:
        score += 10
    if projected_cash_left < 0:
        score = min(score, 49)
    if overdue:
        score = min(score, 69)
    return int(max(0, min(100, score)))


def _score_state(score: int) -> str:
    if score <= 39:
        return "danger"
    if score <= 69:
        return "tight"
    if score <= 84:
        return "stable"
    return "strong"


def _monthly_income(item: Dict[str, Any]) -> float:
    amount = float(item.get("amount") or 0)
    frequency = str(item.get("frequency") or "monthly")
    if frequency == "weekly":
        return amount * 52 / 12
    if frequency == "biweekly":
        return amount * 26 / 12
    return amount


def _extra_income_this_month(store: Dict[str, Any]) -> float:
    return sum(
        _monthly_income(item)
        for item in store.get("income_sources", [])
        if item.get("active") and item.get("type") in {"trading", "business", "side_hustle"}
    )


def _upcoming_bills(bills: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    today = _today()
    end = today + timedelta(days=14)
    upcoming = []
    for bill in bills:
        if bill.get("paid") or not bill.get("active", True) or bill.get("due_day") is None:
            continue
        due = _due_date_for_day(int(bill.get("due_day") or 1), today)
        if due < today:
            due = _due_date_for_day(int(bill.get("due_day") or 1), today.replace(day=1) + timedelta(days=32))
        if today <= due <= end:
            upcoming.append({**bill, "due_date": due.isoformat()})
    return sorted(upcoming, key=lambda item: item["due_date"])


def _overdue_bills(bills: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    today = _today()
    overdue = []
    for bill in bills:
        if not bill.get("active", True) or bill.get("due_day") is None:
            continue
        due = _due_date_for_day(int(bill.get("due_day") or 1), today)
        if not bill.get("paid") and due < today:
            overdue.append({**bill, "due_date": due.isoformat()})
    return overdue


def _due_date_for_day(day_num: int, cursor: date) -> date:
    month_start = date(cursor.year, cursor.month, 1)
    last_day = monthrange(month_start.year, month_start.month)[1]
    return date(month_start.year, month_start.month, min(max(day_num, 1), last_day))


def _current_month_items(items: Iterable[Dict[str, Any]], date_key: str) -> List[Dict[str, Any]]:
    month = _month_key(_today())
    return [item for item in items if str(item.get(date_key) or "").startswith(month)]


def _spending_by_category(charges: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    totals: Dict[str, float] = {}
    for charge in charges:
        category = str(charge.get("category") or "other")
        totals[category] = round(totals.get(category, 0) + float(charge.get("amount") or 0), 2)
    return dict(sorted(totals.items(), key=lambda item: item[1], reverse=True))


def _planned_by_category(bills: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    totals: Dict[str, float] = {}
    for bill in bills:
        if not bill.get("active", True):
            continue
        category = str(bill.get("category") or "other")
        totals[category] = round(totals.get(category, 0) + float(bill.get("amount") or 0), 2)
    return dict(sorted(totals.items(), key=lambda item: item[1], reverse=True))


def _need_breakdown(charges: Iterable[Dict[str, Any]]) -> Dict[str, float]:
    totals = {key: 0.0 for key in ["need", "want", "investment", "leak"]}
    for charge in charges:
        key = str(charge.get("need_or_want") or "need")
        totals[key] = round(totals.get(key, 0) + float(charge.get("amount") or 0), 2)
    return totals


def _cashflow_by_week(store: Dict[str, Any], summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    charges = _current_month_items(store.get("charges", []), "date")
    today = _today()
    weeks = [{"week": f"W{i}", "cash_left": summary["projected_monthly_income"]} for i in range(1, 6)]
    for charge in charges:
        parsed = _parse_date(charge.get("date")) or today
        idx = min(4, max(0, (parsed.day - 1) // 7))
        for week in weeks[idx:]:
            week["cash_left"] = round(float(week["cash_left"]) - float(charge["amount"]), 2)
    return weeks


def _bills_by_due_date(bills: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {"due_day": bill["due_day"], "name": bill["name"], "amount": bill["amount"], "paid": bill["paid"]}
        for bill in sorted(bills, key=lambda item: int(item.get("due_day") or 99))
        if bill.get("active", True)
    ]


def _monthly_income_trend(store: Dict[str, Any]) -> List[Dict[str, Any]]:
    current = round(sum(_monthly_income(item) for item in store["income_sources"] if item["active"]), 2)
    if current <= 0:
        current = store["profile"]["monthly_take_home"]
    return [{"month": _month_key(_today()), "income": current}]


def _goal_progress(goal: Dict[str, Any]) -> Dict[str, Any]:
    target = float(goal.get("target_amount") or 0)
    current = float(goal.get("current_amount") or 0)
    pct = round((current / target) * 100, 1) if target else 0
    months_left = _months_until(goal.get("target_date"))
    needed = round(max(target - current, 0) / months_left, 2) if months_left else 0
    return {**goal, "progress_pct": pct, "monthly_needed": needed}


def _debt_summary(debts: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    debts = list(debts)
    total_balance = round(sum(float(item.get("balance") or 0) for item in debts), 2)
    total_minimums = round(sum(float(item.get("minimum_payment") or 0) for item in debts), 2)
    focus = sorted(debts, key=lambda item: (item.get("priority") != "high", -float(item.get("interest_rate") or 0)))
    return {
        "total_balance": total_balance,
        "total_minimums": total_minimums,
        "focus_debt": focus[0]["name"] if focus else "",
    }


def _budget_warnings(store: Dict[str, Any], summary: Dict[str, Any], leaks_total: float) -> List[str]:
    warnings = []
    planned = _planned_by_category(store.get("bills", []))
    if summary["projected_cash_left"] < 0:
        warnings.append("Projected cash left is negative. Cut wants or increase income.")
    if summary["upcoming_bills_total"] > 0:
        warnings.append(f"Next 14 days: ${summary['upcoming_bills_total']:,.2f} in unpaid bills due.")
    if leaks_total > max(summary["monthly_take_home"] * 0.05, 100):
        warnings.append(f"Leak spending is ${leaks_total:,.2f} this month.")
    if summary["debt_minimums_total"] > summary["monthly_take_home"] * 0.15:
        warnings.append("Debt minimums are taking too much cashflow.")
    if store["goals"] and summary["savings_goal_total"] <= 0:
        warnings.append("Savings goals exist but are not funded monthly.")
    if planned.get("subscriptions", 0) > 100:
        warnings.append("Subscriptions are above $100. Review what can be cut.")
    if planned.get("food", 0) > 1000:
        warnings.append("Food budget is high. Track actual spend closely.")
    if summary["projected_cash_left"] < 1000:
        warnings.append("Cash buffer is tight. Reduce wants or increase income.")
    return warnings or ["Budget is stable. Keep tracking every charge."]


def _budget_recommendations(
    store: Dict[str, Any], summary: Dict[str, Any], spending_by_category: Dict[str, float]
) -> List[str]:
    recommendations = []
    planned = _planned_by_category(store.get("bills", []))
    fixed_pct = (summary["fixed_bills_total"] / summary["monthly_take_home"] * 100) if summary["monthly_take_home"] else 0
    recommendations.append(f"Fixed bills are {fixed_pct:.0f}% of take-home.")
    if summary["income_gap_to_goal"] > 0:
        recommendations.append(f"You are ${summary['income_gap_to_goal']:,.2f} away from the extra income goal.")
    top = _top_category(spending_by_category)
    if top != "None":
        recommendations.append(f"{top.title()} is your biggest planned category.")
    if summary["projected_cash_left"] >= 1500:
        recommendations.append("Healthy cash buffer. Assign part of this to goals.")
    if planned.get("subscriptions", 0) > 100:
        recommendations.append(f"Subscriptions are ${planned['subscriptions']:,.2f}/month. Audit them.")
    if planned.get("food", 0) > 1000:
        recommendations.append("Food is over $1,000 planned. Compare every actual charge against the plan.")
    for goal in store.get("goals", []):
        progress = _goal_progress(goal)
        if progress["monthly_needed"] > 0:
            recommendations.append(
                f"{goal['name']} needs ${progress['monthly_needed']:,.2f}/month to hit the target."
            )
            break
    return recommendations


def _top_category(totals: Dict[str, float]) -> str:
    if not totals:
        return "None"
    return max(totals.items(), key=lambda item: item[1])[0]


def _normalize_name(value: Any) -> str:
    return " ".join(str(value or "").lower().replace("&", "and").split())


def _bill_identity(item: Dict[str, Any]) -> tuple[str, float]:
    return (_normalize_name(item.get("name")), _money(item.get("amount")))


def _optional_day(value: Any) -> Optional[int]:
    if value in (None, "", "null", "None"):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    if number < 1 or number > 31:
        return None
    return number


def _infer_category(name: Any) -> str:
    text = _normalize_name(name)
    if "rent" in text:
        return "housing"
    if "car note" in text:
        return "auto"
    if any(key in text for key in ["chase", "amex", "capital one", "concord", "irs"]):
        return "debt"
    if any(key in text for key in ["power", "verizon", "att", "internet"]):
        return "utilities"
    if any(key in text for key in ["progressive", "renter", "life insurance", "insurance"]):
        return "insurance"
    if "subscription" in text:
        return "subscriptions"
    if "food" in text:
        return "food"
    if "gas" in text:
        return "gas"
    return "other"


def _infer_type(name: Any, category: Any) -> str:
    category_text = str(category or "").lower()
    if category_text in {"food", "gas"}:
        return category_text
    if category_text == "subscriptions":
        return "subscription"
    if category_text == "debt":
        return "debt"
    if category_text == "savings":
        return "savings"
    if category_text == "other" and any(key in _normalize_name(name) for key in ["food", "gas"]):
        return category_text
    return "bill"


def _is_variable_item(item: Dict[str, Any]) -> bool:
    if not item.get("active", True):
        return False
    item_type = str(item.get("type") or "")
    category = str(item.get("category") or "")
    return item_type in _VARIABLE_BUDGET_TYPES or category in _VARIABLE_BUDGET_CATEGORIES


def _is_fixed_item(item: Dict[str, Any]) -> bool:
    return bool(item.get("active", True)) and not _is_variable_item(item)


def _split_note_amounts(notes: Any) -> Optional[tuple[float, float]]:
    text = str(notes or "")
    import re

    match = re.search(r"\$?(\d+(?:\.\d{1,2})?)\s*/\s*\$?(\d+(?:\.\d{1,2})?)", text)
    if not match:
        return None
    return (_money(match.group(1)), _money(match.group(2)))


def _paycheck_allocation(store: Dict[str, Any], monthly_income: float) -> Dict[str, Any]:
    profile = store.get("profile", {})
    pay_frequency = profile.get("pay_frequency") or "biweekly"
    paycheck_amount = profile.get("paycheck_amount") or (monthly_income / 2 if pay_frequency in {"biweekly", "semimonthly"} else monthly_income)
    first = {"income": round(paycheck_amount, 2), "items": [], "set_asides": [], "bills_total": 0.0, "set_asides_total": 0.0}
    second = {"income": round(paycheck_amount, 2), "items": [], "set_asides": [], "bills_total": 0.0, "set_asides_total": 0.0}
    flexible = {"items": [], "total": 0.0}

    def add_bill(bucket: Dict[str, Any], item: Dict[str, Any], amount: Optional[float] = None) -> None:
        row = {**item, "amount": round(float(amount if amount is not None else item.get("amount") or 0), 2)}
        bucket["items"].append(row)
        bucket["bills_total"] = round(bucket["bills_total"] + row["amount"], 2)

    def add_set_aside(bucket: Dict[str, Any], item: Dict[str, Any], amount: float) -> None:
        row = {**item, "amount": round(amount, 2)}
        bucket["set_asides"].append(row)
        bucket["set_asides_total"] = round(bucket["set_asides_total"] + row["amount"], 2)

    for item in store.get("bills", []):
        if not item.get("active", True):
            continue
        amount = float(item.get("amount") or 0)
        allocation = item.get("paycheck_allocation") or "any"
        due_day = item.get("due_day")
        note_split = _split_note_amounts(item.get("notes"))
        if allocation == "split" or note_split:
            first_amount, second_amount = note_split or (round(amount / 2, 2), round(amount / 2, 2))
            add_set_aside(first, item, first_amount)
            add_set_aside(second, item, second_amount)
            remainder = round(max(amount - first_amount - second_amount, 0), 2)
            if remainder:
                flexible["items"].append({**item, "amount": remainder, "notes": f"{item.get('notes', '')} remaining buffer".strip()})
                flexible["total"] = round(flexible["total"] + remainder, 2)
            continue
        if allocation == "first_check":
            add_bill(first, item)
        elif allocation == "second_check":
            add_bill(second, item)
        elif due_day is None:
            flexible["items"].append(item)
            flexible["total"] = round(flexible["total"] + amount, 2)
        elif int(due_day) <= 14:
            add_bill(first, item)
        else:
            add_bill(second, item)

    for bucket in (first, second):
        bucket["cash_left"] = round(bucket["income"] - bucket["bills_total"] - bucket["set_asides_total"], 2)

    return {
        "pay_frequency": pay_frequency,
        "paycheck_amount": round(paycheck_amount, 2),
        "first_check": first,
        "second_check": second,
        "flexible": flexible,
    }


def _leak_analysis(store: Dict[str, Any], projected_cash_left: float) -> List[str]:
    planned = _planned_by_category(store.get("bills", []))
    charges = _current_month_items(store.get("charges", []), "date")
    messages = []
    if planned.get("subscriptions", 0) > 100:
        messages.append("Subscriptions are above $100. Review what can be cut.")
    if planned.get("food", 0) > 1000:
        messages.append("Food budget is high. Track actual spend closely.")
    unplanned = sum(float(item.get("amount") or 0) for item in charges if item.get("need_or_want") in {"want", "leak"})
    if unplanned:
        messages.append(f"Unplanned wants/leaks are ${unplanned:,.2f} this month.")
    if projected_cash_left < 1000:
        messages.append("Cash buffer is tight. Reduce wants or increase income.")
    if projected_cash_left >= 1500:
        messages.append("Healthy cash buffer. Assign part of this to goals.")
    return messages or ["No major leaks detected yet. Keep tracking actual charges."]


def _goal_allocation(projected_cash_left: float) -> Dict[str, float]:
    if projected_cash_left <= 0:
        return {"buffer": 0.0, "debt_payoff": 0.0, "flex": 0.0}
    return {
        "buffer": round(projected_cash_left * 0.50, 2),
        "debt_payoff": round(projected_cash_left * 0.30, 2),
        "flex": round(projected_cash_left * 0.20, 2),
    }


def _best_category(totals: Dict[str, float]) -> str:
    positive = {key: value for key, value in totals.items() if value > 0}
    if not positive:
        return "None"
    return min(positive.items(), key=lambda item: item[1])[0]


def _closeout_adjustment(summary: Dict[str, Any]) -> str:
    if summary["projected_cash_left"] < 0:
        return "Cut leak spending before adding new obligations."
    if summary["income_gap_to_goal"] > 0:
        return "Add one extra income action before next closeout."
    return "Keep tracking charges daily."


def _item_id(raw: Dict[str, Any]) -> str:
    item_id = str(raw.get("id") or "").strip()
    return item_id if item_id else str(uuid.uuid4())


def _money(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = 0.0
    return round(max(number, 0.0), 2)


def _nullable_money(value: Any) -> Optional[float]:
    if value in (None, "", "null", "None"):
        return None
    return _money(value)


def _rate(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = 0.0
    return round(min(max(number, 0.0), 100.0), 2)


def _day(value: Any) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = 1
    return min(max(number, 1), 31)


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "checked"}


def _list(value: Any) -> List[Dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _date_text(value: Any) -> str:
    parsed = _parse_date(value)
    return parsed.isoformat() if parsed else ""


def _parse_date(value: Any) -> Optional[date]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _today() -> date:
    return date.fromisoformat(today_iso())


def _month_key(day: date) -> str:
    return day.strftime("%Y-%m")


def _month_label(day: date) -> str:
    return day.strftime("%B %Y")


def _months_until(value: Any) -> int:
    target = _parse_date(value)
    if not target:
        return 0
    today = _today()
    return max(1, (target.year - today.year) * 12 + target.month - today.month)
