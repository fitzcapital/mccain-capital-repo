"""Goals and payouts domain service functions."""

from __future__ import annotations

from datetime import datetime
from flask import flash, get_flashed_messages, redirect, render_template_string, request, url_for

from mccain_capital import app_core as core

# Compatibility aliases used by extracted route bodies.
today_iso = core.today_iso
now_et = core.now_et
parse_float = core.parse_float
parse_int = core.parse_int
upsert_daily_goal = core.upsert_daily_goal
fetch_daily_goals = core.fetch_daily_goals
fetch_daily_goal = core.fetch_daily_goal
_month_bounds = core._month_bounds
BASE_MONTHLY_INCOME = core.BASE_MONTHLY_INCOME
money = core.money
render_page = core.render_page
DEFAULT_PROTECT_BUFFER = core.DEFAULT_PROTECT_BUFFER
latest_balance_overall = core.latest_balance_overall
payout_summary = core.payout_summary
month_total_net = core.month_total_net
last_30d_total_net = core.last_30d_total_net
last_n_trading_day_totals = core.last_n_trading_day_totals
projections_from_daily = core.projections_from_daily

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
        flash("Saved ✅", "ok")
        return redirect(url_for("goals_tracker", date=vals["track_date"]))

    # month summary + projections
    m_first, m_last = _month_bounds(d_obj)
    rows = fetch_daily_goals(m_first.isoformat(), m_last.isoformat())

    # sums this month
    sum_debt = sum(float(r["debt_paid"] or 0) for r in rows)
    sum_upwork = sum(float(r["upwork_earnings"] or 0) for r in rows)
    sum_other = sum(float(r["other_income"] or 0) for r in rows)

    # projection based on recorded days (not calendar days) to avoid lying to you
    recorded_days = len([r for r in rows if (
                r["upwork_earnings"] is not None or r["other_income"] is not None or r["debt_paid"] is not None)])
    recorded_days = max(recorded_days, 1)
    upwork_daily_avg = sum_upwork / recorded_days
    other_daily_avg = sum_other / recorded_days
    projected_upwork = round(upwork_daily_avg * 30, 2)
    projected_other = round(other_daily_avg * 30, 2)

    # scenario inputs (GET so it doesn't overwrite your daily log)
    s = request.args
    hourly_rate = parse_float(s.get("hourly_rate") or "") or 0.0
    hours_per_week = parse_float(s.get("hours_per_week") or "") or 0.0
    fixed_deals = parse_int(s.get("fixed_deals") or "") or 0
    avg_deal = parse_float(s.get("avg_deal") or "") or 0.0
    trading_monthly = parse_float(s.get("trading_monthly") or "") or 0.0
    other_monthly = parse_float(s.get("other_monthly") or "") or 0.0

    upwork_from_hourly = round(hourly_rate * hours_per_week * 4.33, 2) if hourly_rate and hours_per_week else 0.0
    upwork_from_fixed = round(fixed_deals * avg_deal, 2) if fixed_deals and avg_deal else 0.0
    upwork_scenario = round(upwork_from_hourly + upwork_from_fixed, 2)

    total_scenario = round(BASE_MONTHLY_INCOME + upwork_scenario + trading_monthly + other_monthly, 2)
    gap_15 = round(max(15000 - total_scenario, 0), 2)
    gap_20 = round(max(20000 - total_scenario, 0), 2)

    content = render_template_string(
        """
        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div class="pill">🎯 Daily Goals Tracker</div>
            <div class="tiny" style="margin-top:10px; line-height:1.6">
              Track *inputs* daily: debt actions + Upwork pipeline. Then let the math bully you into consistency 😈
            </div>

            {% with messages = get_flashed_messages(with_categories=true) %}
              {% if messages %}
                <div class="hr"></div>
                {% for cat,msg in messages %}
                  <div class="tiny" style="color:#b8f7c9">• {{ msg }}</div>
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

              <div style="margin-top:10px">
                <label>🧾 Debt Notes (what did you resolve?)</label>
                <input name="debt_note" value="{{ vals.debt_note }}" placeholder="e.g. called CBNA, negotiated, paid Zip installment...">
              </div>

              <div class="row" style="margin-top:10px">
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

              <div style="margin-top:10px">
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

            <div class="calcGrid" style="margin-top:10px">
              <div class="calcCard"><div class="k">💳 Debt Paid</div><div class="v">{{ money(sum_debt) }}</div></div>
              <div class="calcCard"><div class="k">💵 Upwork Earned</div><div class="v">{{ money(sum_upwork) }}</div></div>
              <div class="calcCard"><div class="k">➕ Other Income</div><div class="v">{{ money(sum_other) }}</div></div>
              <div class="calcCard"><div class="k">🧠 Recorded Days</div><div class="v">{{ recorded_days }}</div></div>
              <div class="calcCard"><div class="k">📈 Projected Upwork (30d)</div><div class="v">{{ money(projected_upwork) }}</div></div>
              <div class="calcCard"><div class="k">📈 Projected Other (30d)</div><div class="v">{{ money(projected_other) }}</div></div>
            </div>

            <div class="hr"></div>
            <div class="tiny" style="line-height:1.6">
              Projection is based on <b>days you actually recorded</b> — not wishful calendar math. Record daily. ✅
            </div>
          </div></div>
        </div>

        <div class="twoCol" style="margin-top:12px">
          <div class="card"><div class="toolbar">
            <div class="pill">📈 Income Projection (Goal: $15k–$20k)</div>
            <div class="tiny" style="margin-top:10px; line-height:1.6">
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

              <div class="row" style="margin-top:10px">
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
            <div class="tiny" style="line-height:1.6">
              Reality check: if the gap is big, don't "motivate" yourself — <b>engineer a system</b>:
              proposals/day, follow-ups/day, and a weekly billable-hours target. ✅
            </div>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">🗓️ Month Log</div>
            <div class="tiny" style="margin-top:10px; line-height:1.6">
              Click a date to edit it.
            </div>
            <div class="hr"></div>

            <div style="overflow:auto">
              <table>
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
                      <td><a class="btn" style="padding:6px 10px" href="/goals?date={{ r.track_date }}">{{ r.track_date }}</a></td>
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
          </div></div>
        </div>
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

    overall_balance = latest_balance_overall() or 0.0
    ps = payout_summary(overall_balance, protect)

    today = now_et().date()
    mtd = month_total_net(today.year, today.month)
    last30 = last_30d_total_net()

    daily20 = last_n_trading_day_totals(20)
    proj = projections_from_daily(daily20, overall_balance)

    can_take_biweekly_now = ps["safe_request"] >= biweekly_goal

    content = render_template_string(
        """
        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div class="pill">💸 Payouts</div>
            <div class="tiny" style="margin-top:10px; line-height:1.5">
              Safe payout = protects cushion above the fixed loss limit 🛡️
            </div>

            <div class="hr"></div>
            <form method="post" class="row">
              <div>
                <label>🛡️ Protect Buffer ($)</label>
                <input name="protect_buffer" inputmode="decimal" value="{{ protect }}" />
              </div>
              <div>
                <label>🎯 Bi-Weekly Goal ($)</label>
                <input name="biweekly_goal" inputmode="decimal" value="{{ biweekly_goal }}" />
              </div>
              <div style="display:flex; gap:10px; flex-wrap:wrap">
                <button class="btn primary" type="submit">🔄 Update</button>
                <a class="btn" href="/payouts">♻️ Reset</a>
              </div>
            </form>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">📌 Rule Snapshot (50K)</div>
            <div class="tiny" style="margin-top:10px; line-height:1.6">
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

        <div class="card" style="margin-top:12px"><div class="toolbar">
          <div class="pill">📊 Current Totals</div>
          <div class="hr"></div>

          <div class="statRow">
            <div class="stat"><div class="k">🏦 Balance</div><div class="v">{{ money(ps.balance) }}</div></div>
            <div class="stat"><div class="k">🗓️ MTD Net</div><div class="v">{{ money(mtd) }}</div></div>
            <div class="stat"><div class="k">📆 Last 30 Days</div><div class="v">{{ money(last30) }}</div></div>

            <div class="stat {% if ps.safe_request > 0 %}glow-green{% endif %}">
              <div class="k">🛡️ Safe Withdraw (now)</div>
              <div class="v">{{ money(ps.safe_request) }}</div>
            </div>

            <div class="stat {% if ps.max_request > 0 %}glow-green{% endif %}">
              <div class="k">⚠️ Max Withdraw (no cushion)</div>
              <div class="v">{{ money(ps.max_request) }}</div>
            </div>

            <div class="stat {% if can_take_biweekly_now %}glow-green{% else %}glow-red{% endif %}">
              <div class="k">🎯 ${{ '%.0f'|format(biweekly_goal) }} Bi-Weekly?</div>
              <div class="v">{% if can_take_biweekly_now %}✅ Yes{% else %}⛔ Not yet{% endif %}</div>
              <div class="tiny">Needs Safe ≥ {{ money(biweekly_goal) }}</div>
            </div>
          </div>

          <div class="hr"></div>
          <div class="pill">📈 Projections</div>
          <div class="hr"></div>
          <div class="statRow">
            <div class="stat"><div class="k">📊 Daily Avg (recent)</div><div class="v">{{ money(proj.avg) }}</div></div>
            <div class="stat"><div class="k">5D Est Bal</div><div class="v">{{ money(proj.p5.est_balance) }}</div></div>
            <div class="stat"><div class="k">10D Est Bal</div><div class="v">{{ money(proj.p10.est_balance) }}</div></div>
            <div class="stat"><div class="k">20D Est Bal</div><div class="v">{{ money(proj.p20.est_balance) }}</div></div>
          </div>

          <div class="hr"></div>
          <div class="tiny">
            Respect risk. One impulsive day can erase progress.
          </div>
        </div></div>
        """,
        ps=ps,
        protect=protect,
        biweekly_goal=biweekly_goal,
        mtd=mtd,
        last30=last30,
        proj=proj,
        can_take_biweekly_now=can_take_biweekly_now,
        money=money,
    )
    return render_page(content, active="payouts")


# ============================================================
# Boot
# ============================================================
# DB init moved to package startup (mccain_capital.create_app).
