"""Goals and payouts domain service functions."""

from __future__ import annotations

from datetime import datetime, timedelta
import random
import statistics
from flask import flash, get_flashed_messages, redirect, render_template_string, request, url_for

from mccain_capital.repositories import goals as repo
from mccain_capital.repositories import trades as trades_repo
from mccain_capital.runtime import (
    BASE_MONTHLY_INCOME,
    DEFAULT_PROTECT_BUFFER,
    last_30d_total_net,
    last_n_trading_day_totals,
    latest_balance_overall,
    money,
    month_bounds,
    month_total_net,
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

    content = render_template_string(
        """
        <div class="card pageHero">
          <div class="toolbar">
            <div class="pageHeroHead">
              <div>
                <div class="pill">🎯 Goals Workspace</div>
                <h2 class="pageTitle">Income & Discipline Tracker</h2>
                <div class="pageSub">Track daily input actions, project income scenarios, and keep your execution aligned with monthly targets.</div>
              </div>
              <div class="actionRow">
                <a class="btn" href="/payouts">💸 Payouts</a>
                <a class="btn" href="/calculator">🧮 Calculator</a>
              </div>
            </div>
          </div>
        </div>

        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div class="pill">🎯 Daily Goals Tracker</div>
            <div class="tiny stack10 line16">
              Track *inputs* daily: debt actions + Upwork pipeline. Then let the math bully you into consistency 😈
            </div>

            {% with messages = get_flashed_messages(with_categories=true, category_filter=['goals_ok']) %}
              {% if messages %}
                <div class="hr"></div>
                {% for cat,msg in messages %}
                  <div class="tiny metaGreen">• {{ msg }}</div>
                {% endfor %}
              {% endif %}
            {% endwith %}

            <div class="hr"></div>
            <form method="post">
              <div class="row">
                <div>
                  <label>📆 Date</label>
                  <input type="date" name="track_date" value="{{ vals.track_date }}">
                </div>
                <div>
                  <label>💳 Debt Paid Today</label>
                  <input name="debt_paid" inputmode="decimal" value="{{ vals.debt_paid }}">
                </div>
              </div>

              <div class="stack10">
                <label>🧾 Debt Notes (what did you resolve?)</label>
                <input name="debt_note" value="{{ vals.debt_note }}" placeholder="e.g. called CBNA, negotiated, paid Zip installment...">
              </div>

              <div class="row stack10">
                <div>
                  <label>🧲 Upwork Proposals</label>
                  <input name="upwork_proposals" inputmode="numeric" value="{{ vals.upwork_proposals }}">
                </div>
                <div>
                  <label>🗣️ Upwork Interviews</label>
                  <input name="upwork_interviews" inputmode="numeric" value="{{ vals.upwork_interviews }}">
                </div>
                <div>
                  <label>⏱️ Upwork Hours</label>
                  <input name="upwork_hours" inputmode="decimal" value="{{ vals.upwork_hours }}">
                </div>
                <div>
                  <label>💵 Upwork Earnings</label>
                  <input name="upwork_earnings" inputmode="decimal" value="{{ vals.upwork_earnings }}">
                </div>
                <div>
                  <label>➕ Other Income</label>
                  <input name="other_income" inputmode="decimal" value="{{ vals.other_income }}">
                </div>
              </div>

              <div class="stack10">
                <label>📝 Notes</label>
                <input name="notes" value="{{ vals.notes }}" placeholder="What moved the needle today?">
              </div>

              <div class="hr"></div>
              <div class="rightActions">
                <button class="btn primary" type="submit">💾 Save</button>
                <a class="btn" href="/calculator">🧮 Calc</a>
                <a class="btn" href="/dashboard">📊 Calendar</a>
              </div>
            </form>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">📅 This Month: {{ m_first }} → {{ m_last }}</div>

            <div class="calcGrid stack10">
              <div class="calcCard"><div class="k">💳 Debt Paid</div><div class="v">{{ money(sum_debt) }}</div></div>
              <div class="calcCard"><div class="k">💵 Upwork Earned</div><div class="v">{{ money(sum_upwork) }}</div></div>
              <div class="calcCard"><div class="k">➕ Other Income</div><div class="v">{{ money(sum_other) }}</div></div>
              <div class="calcCard"><div class="k">🧠 Recorded Days</div><div class="v">{{ recorded_days }}</div></div>
              <div class="calcCard"><div class="k">📈 Projected Upwork (30d)</div><div class="v">{{ money(projected_upwork) }}</div></div>
              <div class="calcCard"><div class="k">📈 Projected Other (30d)</div><div class="v">{{ money(projected_other) }}</div></div>
            </div>

            <div class="hr"></div>
            <div class="tiny line16">
              Projection is based on <b>days you actually recorded</b> — not wishful calendar math. Record daily. ✅
            </div>
          </div></div>
        </div>

        <div class="twoCol stack12">
          <div class="card"><div class="toolbar">
            <div class="pill">📈 Income Projection (Goal: $15k–$20k)</div>
            <div class="tiny stack10 line16">
              Your baseline income is set to <b>{{ money(base_income) }}</b> monthly.
              You need <b>{{ money(15000 - base_income) }}</b> to reach $15k and <b>{{ money(20000 - base_income) }}</b> to reach $20k.
            </div>

            <div class="hr"></div>
            <form method="get">
              <input type="hidden" name="date" value="{{ vals.track_date }}">
              <div class="row">
                <div>
                  <label>💼 Upwork Hourly Rate</label>
                  <input name="hourly_rate" inputmode="decimal" value="{{ hourly_rate }}" placeholder="e.g. 75">
                </div>
                <div>
                  <label>⏱️ Billable Hours / Week</label>
                  <input name="hours_per_week" inputmode="decimal" value="{{ hours_per_week }}" placeholder="e.g. 10">
                </div>
                <div>
                  <label>📦 Fixed Deals / Month</label>
                  <input name="fixed_deals" inputmode="numeric" value="{{ fixed_deals }}" placeholder="e.g. 2">
                </div>
                <div>
                  <label>💰 Avg Deal Value</label>
                  <input name="avg_deal" inputmode="decimal" value="{{ avg_deal }}" placeholder="e.g. 1500">
                </div>
              </div>

              <div class="row stack10">
                <div>
                  <label>📊 Trading (Monthly)</label>
                  <input name="trading_monthly" inputmode="decimal" value="{{ trading_monthly }}" placeholder="e.g. 3000">
                </div>
                <div>
                  <label>➕ Other (Monthly)</label>
                  <input name="other_monthly" inputmode="decimal" value="{{ other_monthly }}" placeholder="e.g. 500">
                </div>
              </div>

              <div class="hr"></div>
              <div class="rightActions">
                <button class="btn primary" type="submit">⚡ Project</button>
                <a class="btn" href="/goals">Reset</a>
              </div>
            </form>

            <div class="hr"></div>
            <div class="calcGrid">
              <div class="calcCard"><div class="k">🧱 Base Income</div><div class="v">{{ money(base_income) }}</div></div>
              <div class="calcCard"><div class="k">🧑‍💻 Upwork (Hourly)</div><div class="v">{{ money(upwork_from_hourly) }}</div></div>
              <div class="calcCard"><div class="k">📦 Upwork (Fixed)</div><div class="v">{{ money(upwork_from_fixed) }}</div></div>
              <div class="calcCard"><div class="k">🔥 Upwork Total</div><div class="v">{{ money(upwork_scenario) }}</div></div>
              <div class="calcCard"><div class="k">📊 Trading</div><div class="v">{{ money(trading_monthly) }}</div></div>
              <div class="calcCard"><div class="k">➕ Other</div><div class="v">{{ money(other_monthly) }}</div></div>
              <div class="calcCard"><div class="k">✅ Total Projected</div><div class="v">{{ money(total_scenario) }}</div></div>
              <div class="calcCard"><div class="k">⬆️ Gap to $15k</div><div class="v">{{ money(gap_15) }}</div></div>
              <div class="calcCard"><div class="k">⬆️ Gap to $20k</div><div class="v">{{ money(gap_20) }}</div></div>
            </div>

            <div class="hr"></div>
            <div class="tiny line16">
              Reality check: if the gap is big, don't "motivate" yourself — <b>engineer a system</b>:
              proposals/day, follow-ups/day, and a weekly billable-hours target. ✅
            </div>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">🗓️ Month Log</div>
            <div class="tiny stack10 line16">
              Click a date to edit it.
            </div>
            <div class="hr"></div>

            <div class="tableWrap desktopOnly">
              <table class="tableDense">
                <thead>
                  <tr>
                    <th>📆 Date</th>
                    <th>💳 Debt</th>
                    <th>💵 Upwork</th>
                    <th>🧲 Props</th>
                    <th>🗣️ Intv</th>
                    <th>➕ Other</th>
                  </tr>
                </thead>
                <tbody>
                  {% for r in rows %}
                    <tr>
                      <td><a class="btn btnCompact" href="/goals?date={{ r.track_date }}">{{ r.track_date }}</a></td>
                      <td>{{ money(r.debt_paid) }}</td>
                      <td>{{ money(r.upwork_earnings) }}</td>
                      <td>{{ r.upwork_proposals }}</td>
                      <td>{{ r.upwork_interviews }}</td>
                      <td>{{ money(r.other_income) }}</td>
                    </tr>
                  {% endfor %}
                  {% if not rows %}
                    <tr><td colspan="6" class="tiny">No entries yet for this month.</td></tr>
                  {% endif %}
                </tbody>
              </table>
            </div>
            <div class="mobileOnly">
              <div class="grid">
                {% for r in rows %}
                  <div class="card"><div class="toolbar">
                    <div class="pill">📆 {{ r.track_date }}</div>
                    <div class="metaRow">
                      <span class="meta">💳 Debt: <b>{{ money(r.debt_paid) }}</b></span>
                      <span class="meta">💵 Upwork: <b>{{ money(r.upwork_earnings) }}</b></span>
                      <span class="meta">🧲 Props: <b>{{ r.upwork_proposals }}</b></span>
                      <span class="meta">🗣️ Intv: <b>{{ r.upwork_interviews }}</b></span>
                      <span class="meta">➕ Other: <b>{{ money(r.other_income) }}</b></span>
                    </div>
                    <div class="stack10">
                      <a class="btn btnCompact" href="/goals?date={{ r.track_date }}">Open Day</a>
                    </div>
                  </div></div>
                {% endfor %}
                {% if not rows %}
                  <div class="card"><div class="toolbar"><div class="tiny">No entries yet for this month.</div></div></div>
                {% endif %}
              </div>
            </div>
          </div></div>
        </div>

        <div class="card"><div class="toolbar">
          <div class="pill">🧭 Goal-to-Execution Bridge (Weekly)</div>
          <div class="tiny stack10 line16">Connect planned actions to actual trade execution quality and weekly drift.</div>
          <div class="hr"></div>
          <div class="statRow">
            <div class="stat"><div class="k">Week</div><div class="v">{{ bridge.week_start }} → {{ bridge.week_end }}</div></div>
            <div class="stat"><div class="k">Compliance</div><div class="v">{{ '%.1f'|format(bridge.compliance_score) }}%</div></div>
            <div class="stat"><div class="k">Planned vs Traded Days</div><div class="v">{{ bridge.planned_days }} / {{ bridge.trade_days }}</div></div>
            <div class="stat"><div class="k">Aligned Days</div><div class="v">{{ bridge.aligned_days }}</div></div>
            <div class="stat"><div class="k">Avg Checklist Score</div><div class="v">{{ '%.1f'|format(bridge.avg_checklist_score) }}</div></div>
            <div class="stat"><div class="k">Weekly Drift</div><div class="v">{{ '%.1f'|format(bridge.drift_vs_prev_week) }} ({{ bridge.drift_flag }})</div></div>
          </div>
          <div class="hr"></div>
          <div class="tiny line16">
            Pipeline: proposals {{ bridge.proposals }}/{{ bridge.proposal_target }} · interviews {{ bridge.interviews }}/{{ bridge.interview_target }}.
          </div>
        </div></div>
        """,
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
        get_flashed_messages=get_flashed_messages,
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

    balance_integrity = trades_repo.balance_integrity_snapshot()
    balance_badges = balance_state_badges(balance_integrity)
    overall_balance = float(balance_integrity.get("canonical_balance") or 0.0)
    ps = payout_summary(overall_balance, protect)

    today = now_et().date()
    mtd = month_total_net(today.year, today.month)
    last30 = last_30d_total_net()

    daily20 = last_n_trading_day_totals(20)
    daily60 = last_n_trading_day_totals(60)
    proj = projections_from_daily(daily20, overall_balance)
    readiness = _payout_readiness_planner(
        daily_vals=daily60,
        balance=float(overall_balance),
        safe_floor=float(ps["safe_floor"]),
        biweekly_goal=float(biweekly_goal),
    )

    can_take_biweekly_now = ps["safe_request"] >= biweekly_goal

    content = render_template_string(
        """
        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div class="pill">Payouts</div>
            <div class="tiny stack10 line15">
              Safe payout preserves a cushion above the fixed loss limit.
            </div>

            <div class="hr"></div>
            <form method="post" class="row">
              <div>
                <label>Protect Buffer ($)</label>
                <input name="protect_buffer" inputmode="decimal" value="{{ protect }}" />
              </div>
              <div>
                <label>Bi-Weekly Goal ($)</label>
                <input name="biweekly_goal" inputmode="decimal" value="{{ biweekly_goal }}" />
              </div>
              <div class="actionRow">
                <button class="btn primary" type="submit">Update</button>
                <a class="btn" href="/payouts">Reset</a>
              </div>
            </form>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">Rule Snapshot (50K)</div>
            <div class="tiny stack10 line16">
              • Buffer reached at: <b>{{ money(ps.profit_buffer_level) }}</b><br>
              • Fixed loss limit after buffer: <b>{{ money(ps.fixed_loss_limit) }}</b><br>
              • Safe floor (loss limit + cushion): <b>{{ money(ps.safe_floor) }}</b><br>
              <div class="hr"></div>
              {% if ps.buffer_reached %}
                ✅ Buffer reached — payouts can be calculated.
              {% else %}
                ⛔ Buffer NOT reached — eligibility = $0.00 until you pass <b>{{ money(ps.profit_buffer_level) }}</b>.
              {% endif %}
            </div>
          </div></div>
        </div>

        <div class="card stack12"><div class="toolbar">
          <div class="pill">Current Totals</div>
          <div class="statusBadgeStrip">
            {% for badge in balance_badges %}
              <div class="statusBadge is-{{ badge.tone }}" title="{{ badge.title }}">
                <span class="statusBadgeLabel">{{ badge.label }}</span>
                <strong>{{ badge.value }}</strong>
              </div>
            {% endfor %}
          </div>
          <div class="hr"></div>

          <div class="statRow">
            <div class="stat"><div class="k">Balance</div><div class="v">{{ money(ps.balance) }}</div></div>
            <div class="stat"><div class="k">MTD Net</div><div class="v">{{ money(mtd) }}</div></div>
            <div class="stat"><div class="k">Last 30 Days</div><div class="v">{{ money(last30) }}</div></div>

            <div class="stat {% if ps.safe_request > 0 %}glow-green{% endif %}">
              <div class="k">Safe Withdraw (now)</div>
              <div class="v">{{ money(ps.safe_request) }}</div>
            </div>

            <div class="stat {% if ps.max_request > 0 %}glow-green{% endif %}">
              <div class="k">Max Withdraw (no cushion)</div>
              <div class="v">{{ money(ps.max_request) }}</div>
            </div>

            <div class="stat {% if can_take_biweekly_now %}glow-green{% else %}glow-red{% endif %}">
              <div class="k">${{ '%.0f'|format(biweekly_goal) }} Bi-Weekly?</div>
              <div class="v">{% if can_take_biweekly_now %}Yes{% else %}Not yet{% endif %}</div>
              <div class="tiny">Needs Safe ≥ {{ money(biweekly_goal) }}</div>
            </div>
          </div>

          <div class="hr"></div>
          <div class="pill">Projections</div>
          <div class="hr"></div>
          <div class="statRow">
            <div class="stat"><div class="k">Daily Avg (recent)</div><div class="v">{{ money(proj.avg) }}</div></div>
            <div class="stat"><div class="k">5D Est Bal</div><div class="v">{{ money(proj.p5.est_balance) }}</div></div>
            <div class="stat"><div class="k">10D Est Bal</div><div class="v">{{ money(proj.p10.est_balance) }}</div></div>
            <div class="stat"><div class="k">20D Est Bal</div><div class="v">{{ money(proj.p20.est_balance) }}</div></div>
          </div>

          <div class="hr"></div>
          <div class="tiny">
            Respect risk. One impulsive day can erase progress.
          </div>
        </div></div>
        <div class="card stack12"><div class="toolbar">
          <div class="pill">Payout Readiness Planner</div>
          <div class="tiny stack10 line16">Probability bands based on your recent daily expectancy/volatility profile.</div>
          <div class="hr"></div>
          <div class="statRow">
            <div class="stat"><div class="k">Drift (μ/day)</div><div class="v">{{ money(readiness.mu) }}</div></div>
            <div class="stat"><div class="k">Vol (σ/day)</div><div class="v">{{ money(readiness.sigma) }}</div></div>
            <div class="stat"><div class="k">Target Balance</div><div class="v">{{ money(readiness.target_balance) }}</div></div>
          </div>
          <div class="hr"></div>
          <div class="tableWrap"><table class="tableDense">
            <thead><tr><th>Horizon</th><th>Target Hit %</th><th>Floor Breach %</th><th>P10 PnL</th><th>P50 PnL</th><th>P90 PnL</th></tr></thead>
            <tbody>
              <tr><td>5D</td><td>{{ readiness.h5.target_hit_prob }}%</td><td>{{ readiness.h5.floor_breach_prob }}%</td><td>{{ money(readiness.h5.p10_pnl) }}</td><td>{{ money(readiness.h5.p50_pnl) }}</td><td>{{ money(readiness.h5.p90_pnl) }}</td></tr>
              <tr><td>10D</td><td>{{ readiness.h10.target_hit_prob }}%</td><td>{{ readiness.h10.floor_breach_prob }}%</td><td>{{ money(readiness.h10.p10_pnl) }}</td><td>{{ money(readiness.h10.p50_pnl) }}</td><td>{{ money(readiness.h10.p90_pnl) }}</td></tr>
              <tr><td>20D</td><td>{{ readiness.h20.target_hit_prob }}%</td><td>{{ readiness.h20.floor_breach_prob }}%</td><td>{{ money(readiness.h20.p10_pnl) }}</td><td>{{ money(readiness.h20.p50_pnl) }}</td><td>{{ money(readiness.h20.p90_pnl) }}</td></tr>
            </tbody>
          </table></div>
        </div></div>
        """,
        ps=ps,
        protect=protect,
        biweekly_goal=biweekly_goal,
        mtd=mtd,
        last30=last30,
        proj=proj,
        readiness=readiness,
        balance_badges=balance_badges,
        can_take_biweekly_now=can_take_biweekly_now,
        money=money,
    )
    return render_page(content, active="payouts")


# ============================================================
# Boot
# ============================================================
# DB init moved to package startup (mccain_capital.create_app).
