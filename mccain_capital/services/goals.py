"""Goals and payouts domain service functions."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import math
import random
import statistics
from flask import flash, redirect, render_template, request, url_for

from mccain_capital.repositories import goals as repo
from mccain_capital.repositories import trades as trades_repo
from mccain_capital.runtime import (
    BASE_MONTHLY_INCOME,
    DEFAULT_PROTECT_BUFFER,
    money,
    month_bounds,
    now_et,
    parse_float,
    parse_int,
    payout_summary,
    projections_from_daily,
    today_iso,
    db,
)
from mccain_capital.services.ui import render_page
from mccain_capital.services.viewmodels import balance_state_badges

# Compatibility aliases used by extracted route bodies.
upsert_daily_goal = repo.upsert_daily_goal
fetch_daily_goals = repo.fetch_daily_goals
fetch_daily_goal = repo.fetch_daily_goal
_month_bounds = month_bounds


def _goal_execution_bridge(anchor_day):
    start = anchor_day - timedelta(days=anchor_day.weekday())
    end = start + timedelta(days=6)
    prev_start = start - timedelta(days=7)
    prev_end = start - timedelta(days=1)
    with db() as conn:
        goals_rows = conn.execute(
            """
            SELECT track_date, upwork_proposals, upwork_interviews
            FROM daily_goals
            WHERE track_date >= ? AND track_date <= ?
            ORDER BY track_date ASC
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchall()
        trade_rows = conn.execute(
            """
            SELECT t.trade_date, t.net_pl, r.checklist_score
            FROM trades t
            LEFT JOIN trade_reviews r ON r.trade_id = t.id
            WHERE t.trade_date >= ? AND t.trade_date <= ?
            ORDER BY t.trade_date ASC, t.id ASC
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchall()
        prev_trade_rows = conn.execute(
            """
            SELECT t.trade_date, t.net_pl, r.checklist_score
            FROM trades t
            LEFT JOIN trade_reviews r ON r.trade_id = t.id
            WHERE t.trade_date >= ? AND t.trade_date <= ?
            ORDER BY t.trade_date ASC, t.id ASC
            """,
            (prev_start.isoformat(), prev_end.isoformat()),
        ).fetchall()
    goal_days = {str(r["track_date"]) for r in goals_rows}
    trade_days = {str(r["trade_date"]) for r in trade_rows}
    aligned_days = len(goal_days & trade_days)
    proposals = sum(int(r["upwork_proposals"] or 0) for r in goals_rows)
    interviews = sum(int(r["upwork_interviews"] or 0) for r in goals_rows)
    checklist_scores = [
        float(r["checklist_score"]) for r in trade_rows if r["checklist_score"] is not None
    ]
    avg_score = (sum(checklist_scores) / len(checklist_scores)) if checklist_scores else 0.0
    planned_days = len(goal_days)
    proposal_target = max(1, planned_days * 3)
    interview_target = max(1, planned_days // 2)
    align_ratio = (aligned_days / planned_days) if planned_days else 0.0
    proposal_ratio = min(1.0, proposals / proposal_target)
    interview_ratio = min(1.0, interviews / interview_target)
    score_ratio = min(1.0, avg_score / 100.0)
    compliance = round(
        (align_ratio * 0.45 + proposal_ratio * 0.20 + interview_ratio * 0.10 + score_ratio * 0.25)
        * 100.0,
        1,
    )
    prev_scores = [
        float(r["checklist_score"]) for r in prev_trade_rows if r["checklist_score"] is not None
    ]
    prev_avg = (sum(prev_scores) / len(prev_scores)) if prev_scores else 0.0
    prev_trade_days = {str(r["trade_date"]) for r in prev_trade_rows}
    prev_align = (len(prev_trade_days) / max(1, len(prev_trade_days))) if prev_trade_days else 0.0
    prev_compliance = round((prev_align * 0.45 + (prev_avg / 100.0) * 0.25) * 100.0, 1)
    drift = compliance - prev_compliance
    drift_flag = "improving" if drift >= 5 else "slipping" if drift <= -5 else "stable"
    return {
        "week_start": start.isoformat(),
        "week_end": end.isoformat(),
        "planned_days": planned_days,
        "trade_days": len(trade_days),
        "aligned_days": aligned_days,
        "proposals": proposals,
        "proposal_target": proposal_target,
        "interviews": interviews,
        "interview_target": interview_target,
        "avg_checklist_score": avg_score,
        "compliance_score": compliance,
        "drift_vs_prev_week": drift,
        "drift_flag": drift_flag,
    }


def _payout_readiness_planner(
    *,
    daily_vals,
    balance: float,
    safe_floor: float,
    biweekly_goal: float,
):
    sample = [float(v) for v in daily_vals if isinstance(v, (int, float))]
    if not sample:
        sample = [0.0]
    mu = statistics.mean(sample)
    sigma = statistics.pstdev(sample) if len(sample) > 1 else abs(mu) * 0.6
    sigma = max(sigma, 1.0)
    target_balance = float(balance) + max(0.0, biweekly_goal)

    def simulate(days: int, runs: int = 600):
        hits_target = 0
        hits_floor = 0
        pnls = []
        for _ in range(runs):
            pnl = 0.0
            bal = float(balance)
            breached = False
            for _d in range(days):
                step = random.gauss(mu, sigma)
                pnl += step
                bal += step
                if bal < safe_floor:
                    breached = True
            pnls.append(pnl)
            if bal >= target_balance:
                hits_target += 1
            if breached:
                hits_floor += 1
        pnls.sort()
        p10 = pnls[max(0, int(len(pnls) * 0.10) - 1)]
        p50 = pnls[max(0, int(len(pnls) * 0.50) - 1)]
        p90 = pnls[max(0, int(len(pnls) * 0.90) - 1)]
        return {
            "days": days,
            "target_hit_prob": round((hits_target / runs) * 100.0, 1),
            "floor_breach_prob": round((hits_floor / runs) * 100.0, 1),
            "p10_pnl": p10,
            "p50_pnl": p50,
            "p90_pnl": p90,
            "p50_balance": balance + p50,
        }

    return {
        "mu": mu,
        "sigma": sigma,
        "h5": simulate(5),
        "h10": simulate(10),
        "h20": simulate(20),
        "target_balance": target_balance,
    }


def _required_profit_to_target(current_withdrawable: float, payout_goal: float) -> float:
    return max(0.0, float(payout_goal) - float(current_withdrawable))


def _quantile_int(values: list[int], q: float) -> int | None:
    if not values:
        return None
    ordered = sorted(int(v) for v in values)
    idx = max(0, min(len(ordered) - 1, int(math.ceil(q * len(ordered)) - 1)))
    return int(ordered[idx])


def _trading_day_quantiles_to_goal(
    required_profit: float,
    mu: float,
    sigma: float,
    *,
    runs: int = 1000,
    horizon: int = 60,
    balance: float = 0.0,
    safe_floor: float | None = None,
    seed: int = 11,
) -> dict[str, float | int | None]:
    req = float(required_profit)
    if req <= 0:
        return {
            "days_p50": 0,
            "days_p70": 0,
            "days_p90": 0,
            "hit_prob_5d": 100.0,
            "hit_prob_10d": 100.0,
            "hit_prob_20d": 100.0,
            "floor_breach_prob_at_target_horizon": 0.0,
        }

    vol = max(0.0, float(sigma))
    rng = random.Random(seed)
    reached_days: list[int] = []
    breach_days: list[int | None] = []

    for _ in range(max(1, int(runs))):
        pnl = 0.0
        bal = float(balance)
        reached_day: int | None = None
        breach_day: int | None = None
        for day in range(1, int(horizon) + 1):
            step = rng.gauss(float(mu), vol) if vol > 0 else float(mu)
            pnl += step
            bal += step
            if breach_day is None and safe_floor is not None and bal < float(safe_floor):
                breach_day = day
            if reached_day is None and pnl >= req:
                reached_day = day
        if reached_day is not None:
            reached_days.append(reached_day)
        breach_days.append(breach_day)

    def _hit_prob(days: int) -> float:
        hits = sum(1 for d in reached_days if d <= days)
        return round((hits / max(1, runs)) * 100.0, 1)

    p50 = _quantile_int(reached_days, 0.50)
    p70 = _quantile_int(reached_days, 0.70)
    p90 = _quantile_int(reached_days, 0.90)
    target_horizon = p70 or p90 or int(horizon)
    floor_hits = sum(
        1
        for d in breach_days
        if d is not None and d <= int(target_horizon)
    )
    floor_prob = round((floor_hits / max(1, runs)) * 100.0, 1)

    return {
        "days_p50": p50,
        "days_p70": p70,
        "days_p90": p90,
        "hit_prob_5d": _hit_prob(5),
        "hit_prob_10d": _hit_prob(10),
        "hit_prob_20d": _hit_prob(20),
        "floor_breach_prob_at_target_horizon": floor_prob,
    }


def _build_unlock_forecast(
    *,
    safe_request: float,
    max_request: float,
    biweekly_goal: float,
    overall_balance: float,
    safe_floor: float,
    daily20: list[float],
    daily60: list[float],
    risk_threshold: float = 30.0,
) -> dict[str, object]:
    near = [float(v) for v in daily20 if isinstance(v, (int, float))]
    stable = [float(v) for v in daily60 if isinstance(v, (int, float))]
    sample_count = len(stable) if stable else len(near)
    mu_20 = statistics.mean(near) if near else 0.0
    sigma_60 = statistics.pstdev(stable) if len(stable) > 1 else (abs(mu_20) * 0.6)
    sigma_60 = max(0.0, float(sigma_60))

    warnings: list[str] = []
    low_confidence = sample_count < 10
    if low_confidence:
        warnings.append("Low confidence: fewer than 10 trading days in scope.")
    if mu_20 <= 0:
        warnings.append("Drift is non-positive; payout unlock ETA is not statistically favorable.")

    method = "deterministic_low_confidence" if low_confidence else "probabilistic_bands"

    def _build_path(label: str, current_withdrawable: float) -> dict[str, object]:
        required = _required_profit_to_target(current_withdrawable, biweekly_goal)
        path: dict[str, object] = {
            "label": label,
            "required_profit": required,
            "can_unlock_now": required <= 0.0,
            "days_p50": 0 if required <= 0.0 else None,
            "days_p70": 0 if required <= 0.0 else None,
            "days_p90": 0 if required <= 0.0 else None,
            "hit_prob_5d": 100.0 if required <= 0.0 else 0.0,
            "hit_prob_10d": 100.0 if required <= 0.0 else 0.0,
            "hit_prob_20d": 100.0 if required <= 0.0 else 0.0,
            "floor_breach_prob": 0.0,
            "risk_flag": "PASS",
            "risk_tone": "ok",
            "eta_note": "Unlocked now." if required <= 0.0 else "",
        }
        if required <= 0.0:
            return path

        if low_confidence:
            if mu_20 > 0:
                deterministic_days = int(math.ceil(required / mu_20))
                path.update(
                    {
                        "days_p50": deterministic_days,
                        "days_p70": deterministic_days,
                        "days_p90": deterministic_days,
                        "eta_note": "Deterministic fallback from average daily P/L.",
                    }
                )
            else:
                path.update(
                    {
                        "eta_note": "Not statistically favorable (non-positive drift).",
                        "risk_flag": "RISK",
                        "risk_tone": "critical",
                    }
                )
            return path

        quantiles = _trading_day_quantiles_to_goal(
            required,
            mu_20,
            sigma_60,
            runs=1000,
            horizon=60,
            balance=float(overall_balance),
            safe_floor=float(safe_floor),
        )
        path.update(
            {
                "days_p50": quantiles["days_p50"],
                "days_p70": quantiles["days_p70"],
                "days_p90": quantiles["days_p90"],
                "hit_prob_5d": quantiles["hit_prob_5d"],
                "hit_prob_10d": quantiles["hit_prob_10d"],
                "hit_prob_20d": quantiles["hit_prob_20d"],
                "floor_breach_prob": quantiles["floor_breach_prob_at_target_horizon"],
            }
        )
        floor_prob = float(path["floor_breach_prob"] or 0.0)
        if floor_prob > float(risk_threshold) or mu_20 <= 0:
            path["risk_flag"] = "RISK"
            path["risk_tone"] = "critical"
        if mu_20 <= 0:
            path["days_p50"] = None
            path["days_p70"] = None
            path["days_p90"] = None
            path["eta_note"] = "Not statistically favorable (non-positive drift)."
        return path

    return {
        "method": method,
        "risk_threshold": float(risk_threshold),
        "model": {
            "mu_20": mu_20,
            "sigma_60": sigma_60,
            "sample_20": len(near),
            "sample_60": len(stable),
        },
        "warnings": warnings,
        "safe": _build_path("Safe Withdrawal Path", safe_request),
        "max": _build_path("Max Withdrawal Path", max_request),
    }


def _sum_net_between(*, start_date: str, end_date: str, scope_start: str = "") -> float:
    where = ["trade_date >= ?", "trade_date < ?"]
    params: list[object] = [start_date, end_date]
    if scope_start:
        where.append("trade_date >= ?")
        params.append(scope_start)
    with db() as conn:
        row = conn.execute(
            f"""
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE {' AND '.join(where)}
            """,
            tuple(params),
        ).fetchone()
    return float(row["net"] or 0.0)


def goals_tracker():
    # which day are we viewing/editing?
    d_iso = (request.args.get("date") or today_iso()).strip()
    try:
        d_obj = datetime.strptime(d_iso, "%Y-%m-%d").date()
    except Exception:
        d_obj = now_et().date()
        d_iso = d_obj.isoformat()

    # daily form defaults
    row = fetch_daily_goal(d_iso)
    vals = {
        "track_date": d_iso,
        "debt_paid": str(row["debt_paid"]) if row else "0",
        "debt_note": row["debt_note"] if row else "",
        "upwork_proposals": str(row["upwork_proposals"]) if row else "0",
        "upwork_interviews": str(row["upwork_interviews"]) if row else "0",
        "upwork_hours": str(row["upwork_hours"]) if row else "0",
        "upwork_earnings": str(row["upwork_earnings"]) if row else "0",
        "other_income": str(row["other_income"]) if row else "0",
        "notes": row["notes"] if row else "",
    }

    if request.method == "POST":
        f = request.form
        vals["track_date"] = (f.get("track_date") or d_iso).strip() or d_iso
        vals["debt_paid"] = (f.get("debt_paid") or "0").strip()
        vals["debt_note"] = (f.get("debt_note") or "").strip()
        vals["upwork_proposals"] = (f.get("upwork_proposals") or "0").strip()
        vals["upwork_interviews"] = (f.get("upwork_interviews") or "0").strip()
        vals["upwork_hours"] = (f.get("upwork_hours") or "0").strip()
        vals["upwork_earnings"] = (f.get("upwork_earnings") or "0").strip()
        vals["other_income"] = (f.get("other_income") or "0").strip()
        vals["notes"] = (f.get("notes") or "").strip()

        payload = {
            "debt_paid": parse_float(vals["debt_paid"]) or 0.0,
            "debt_note": vals["debt_note"],
            "upwork_proposals": parse_int(vals["upwork_proposals"]) or 0,
            "upwork_interviews": parse_int(vals["upwork_interviews"]) or 0,
            "upwork_hours": parse_float(vals["upwork_hours"]) or 0.0,
            "upwork_earnings": parse_float(vals["upwork_earnings"]) or 0.0,
            "other_income": parse_float(vals["other_income"]) or 0.0,
            "notes": vals["notes"],
        }
        upsert_daily_goal(vals["track_date"], payload)
        flash("Saved ✅", "goals_ok")
        return redirect(url_for("goals_tracker", date=vals["track_date"]))

    # month summary + projections
    m_first, m_last = _month_bounds(d_obj)
    rows = fetch_daily_goals(m_first.isoformat(), m_last.isoformat())

    # sums this month
    sum_debt = sum(float(r["debt_paid"] or 0) for r in rows)
    sum_upwork = sum(float(r["upwork_earnings"] or 0) for r in rows)
    sum_other = sum(float(r["other_income"] or 0) for r in rows)

    # projection based on recorded days (not calendar days) to avoid lying to you
    recorded_days = len(
        [
            r
            for r in rows
            if (
                r["upwork_earnings"] is not None
                or r["other_income"] is not None
                or r["debt_paid"] is not None
            )
        ]
    )
    recorded_days = max(recorded_days, 1)
    upwork_daily_avg = sum_upwork / recorded_days
    other_daily_avg = sum_other / recorded_days
    projected_upwork = round(upwork_daily_avg * 30, 2)
    projected_other = round(other_daily_avg * 30, 2)
    bridge = _goal_execution_bridge(d_obj)

    # scenario inputs (GET so it doesn't overwrite your daily log)
    s = request.args
    hourly_rate = parse_float(s.get("hourly_rate") or "") or 0.0
    hours_per_week = parse_float(s.get("hours_per_week") or "") or 0.0
    fixed_deals = parse_int(s.get("fixed_deals") or "") or 0
    avg_deal = parse_float(s.get("avg_deal") or "") or 0.0
    trading_monthly = parse_float(s.get("trading_monthly") or "") or 0.0
    other_monthly = parse_float(s.get("other_monthly") or "") or 0.0

    upwork_from_hourly = (
        round(hourly_rate * hours_per_week * 4.33, 2) if hourly_rate and hours_per_week else 0.0
    )
    upwork_from_fixed = round(fixed_deals * avg_deal, 2) if fixed_deals and avg_deal else 0.0
    upwork_scenario = round(upwork_from_hourly + upwork_from_fixed, 2)

    total_scenario = round(
        BASE_MONTHLY_INCOME + upwork_scenario + trading_monthly + other_monthly, 2
    )
    gap_15 = round(max(15000 - total_scenario, 0), 2)
    gap_20 = round(max(20000 - total_scenario, 0), 2)

    content = render_template(
        "goals/index.html",
        vals=vals,
        rows=rows,
        m_first=m_first.isoformat(),
        m_last=m_last.isoformat(),
        sum_debt=sum_debt,
        sum_upwork=sum_upwork,
        sum_other=sum_other,
        recorded_days=recorded_days,
        projected_upwork=projected_upwork,
        projected_other=projected_other,
        bridge=bridge,
        base_income=BASE_MONTHLY_INCOME,
        hourly_rate=hourly_rate,
        hours_per_week=hours_per_week,
        fixed_deals=fixed_deals,
        avg_deal=avg_deal,
        upwork_from_hourly=upwork_from_hourly,
        upwork_from_fixed=upwork_from_fixed,
        upwork_scenario=upwork_scenario,
        trading_monthly=trading_monthly,
        other_monthly=other_monthly,
        total_scenario=total_scenario,
        gap_15=gap_15,
        gap_20=gap_20,
        money=money,
    )
    return render_page(content, active="goals")


def payouts_page():
    protect = DEFAULT_PROTECT_BUFFER
    biweekly_goal = 2000.0

    if request.method == "POST":
        protect = parse_float(request.form.get("protect_buffer", "")) or protect
        biweekly_goal = parse_float(request.form.get("biweekly_goal", "")) or biweekly_goal
    else:
        protect = parse_float(request.args.get("protect", "")) or protect
        biweekly_goal = parse_float(request.args.get("goal", "")) or biweekly_goal

    scope = trades_repo.account_scope_snapshot()
    scope_enabled = bool(scope.get("enabled"))
    scope_mode_raw = (request.args.get("scope") or "").strip().lower()
    scope_active = scope_enabled and scope_mode_raw != "all"
    scope_start = str(scope.get("start_date") or "")
    scope_starting_balance = float(scope.get("starting_balance") or 50000.0)

    balance_integrity = trades_repo.balance_integrity_snapshot(
        start_date=scope_start if scope_active else None,
        starting_balance=scope_starting_balance if scope_active else None,
    )
    balance_badges = balance_state_badges(balance_integrity)
    overall_balance = float(balance_integrity.get("canonical_balance") or 0.0)
    ps = payout_summary(overall_balance, protect)

    today = now_et().date()
    m_first = date(today.year, today.month, 1).isoformat()
    m_next = date(today.year + (today.month == 12), 1 if today.month == 12 else today.month + 1, 1).isoformat()
    mtd = _sum_net_between(start_date=m_first, end_date=m_next, scope_start=scope_start if scope_active else "")
    last30_start = (today - timedelta(days=30)).isoformat()
    last30_end = (today + timedelta(days=1)).isoformat()
    last30 = _sum_net_between(
        start_date=last30_start,
        end_date=last30_end,
        scope_start=scope_start if scope_active else "",
    )

    daily20 = trades_repo.last_n_trading_day_totals(20, since_date=scope_start if scope_active else "")
    daily60 = trades_repo.last_n_trading_day_totals(60, since_date=scope_start if scope_active else "")
    proj = projections_from_daily(daily20, overall_balance)
    readiness = _payout_readiness_planner(
        daily_vals=daily60,
        balance=float(overall_balance),
        safe_floor=float(ps["safe_floor"]),
        biweekly_goal=float(biweekly_goal),
    )
    unlock_forecast = _build_unlock_forecast(
        safe_request=float(ps["safe_request"]),
        max_request=float(ps["max_request"]),
        biweekly_goal=float(biweekly_goal),
        overall_balance=float(overall_balance),
        safe_floor=float(ps["safe_floor"]),
        daily20=daily20,
        daily60=daily60,
        risk_threshold=30.0,
    )

    can_take_biweekly_now = ps["safe_request"] >= biweekly_goal

    content = render_template(
        "goals/payouts.html",
        ps=ps,
        protect=protect,
        biweekly_goal=biweekly_goal,
        mtd=mtd,
        last30=last30,
        proj=proj,
        readiness=readiness,
        unlock_forecast=unlock_forecast,
        balance_badges=balance_badges,
        can_take_biweekly_now=can_take_biweekly_now,
        account_scope=scope,
        scope_mode=("active" if scope_active else "all"),
        scope_active_href=f"/payouts?scope=active&protect={protect}&goal={biweekly_goal}",
        scope_all_href=f"/payouts?scope=all&protect={protect}&goal={biweekly_goal}",
        money=money,
    )
    return render_page(content, active="payouts")


# ============================================================
# Boot
# ============================================================
# DB init moved to package startup (mccain_capital.create_app).
