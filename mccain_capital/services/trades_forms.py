"""Trades edit/review/risk-control form handlers."""

from __future__ import annotations

from urllib.parse import urlencode

from mccain_capital.repositories import analytics as analytics_repo
from mccain_capital.services import trades as legacy


def trades_edit(trade_id: int):
    row = legacy.get_trade(trade_id)
    if not row:
        legacy.abort(404)

    d = legacy.request.args.get("d", "")
    q = legacy.request.args.get("q", "")
    review_filters = analytics_repo.normalize_trade_filters(
        {
            "setup": legacy.request.args.get("setup", ""),
            "session": legacy.request.args.get("session", ""),
            "outcome": legacy.request.args.get("outcome", ""),
            "time_block": legacy.request.args.get("time_block", ""),
            "mistake_tag": legacy.request.args.get("mistake_tag", ""),
        }
    )
    back_query = urlencode(
        {
            "d": d,
            "q": q,
            **{key: value for key, value in review_filters.items() if value},
        }
    )

    if legacy.request.method == "POST":
        f = legacy.request.form

        trade_date = (f.get("trade_date") or legacy.today_iso()).strip()
        entry_time = (f.get("entry_time") or "").strip()
        exit_time = (f.get("exit_time") or "").strip()

        ticker = (f.get("ticker") or "").strip().upper()
        opt_type = legacy.normalize_opt_type(f.get("opt_type") or "")
        strike = legacy.parse_float(f.get("strike") or "")

        contracts = legacy.parse_int(f.get("contracts") or "") or 0
        entry_price = legacy.parse_float(f.get("entry_price") or "")
        exit_price = legacy.parse_float(f.get("exit_price") or "")
        comm = legacy.parse_float(f.get("comm") or "") or 0.0

        if (
            not ticker
            or opt_type not in ("CALL", "PUT")
            or contracts <= 0
            or entry_price is None
            or exit_price is None
        ):
            return legacy.render_page(
                legacy.simple_msg("Missing required fields (ticker/type/contracts/entry/exit)."),
                active="trades",
            )

        gross_pl = (exit_price - entry_price) * 100.0 * contracts
        net_pl = gross_pl - comm
        total_spent = entry_price * 100.0 * contracts
        result_pct = (net_pl / total_spent * 100.0) if total_spent > 0 else None

        with legacy.db() as conn:
            conn.execute(
                """
                UPDATE trades
                SET trade_date=?, entry_time=?, exit_time=?, ticker=?, opt_type=?, strike=?,
                    entry_price=?, exit_price=?, contracts=?, comm=?,
                    total_spent=?, gross_pl=?, net_pl=?, result_pct=?
                WHERE id=?
                """,
                (
                    trade_date,
                    entry_time,
                    exit_time,
                    ticker,
                    opt_type,
                    strike,
                    entry_price,
                    exit_price,
                    contracts,
                    comm,
                    total_spent,
                    gross_pl,
                    net_pl,
                    result_pct,
                    trade_id,
                ),
            )

        legacy.repo.recompute_balances()
        legacy.flash("Trade updated.", "success")
        if back_query:
            return legacy.redirect(f"/trades?{back_query}")
        return legacy.redirect(legacy.url_for("trades_page", d=trade_date))

    t = dict(row)
    content = legacy.render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">✏️ Edit Trade #{{ t.id }}</div>
          <div class="hr"></div>

          <form method="post" action="/trades/edit/{{ t.id }}?{{ back_query }}">
            <div class="row">
              <div><label>📆 Date</label><input type="date" name="trade_date" value="{{ t.trade_date }}"/></div>
              <div><label>⏱️ Entry Time</label><input name="entry_time" value="{{ t.entry_time or '' }}"/></div>
              <div><label>⏱️ Exit Time</label><input name="exit_time" value="{{ t.exit_time or '' }}"/></div>
            </div>

            <div class="row stack10">
              <div><label>🏷️ Ticker</label><input name="ticker" value="{{ t.ticker or '' }}"/></div>
              <div>
                <label>📌 Type</label>
                <select name="opt_type">
                  <option value="CALL" {% if (t.opt_type or '')=='CALL' %}selected{% endif %}>CALL</option>
                  <option value="PUT"  {% if (t.opt_type or '')=='PUT' %}selected{% endif %}>PUT</option>
                </select>
              </div>
              <div><label>❌ Strike</label><input name="strike" inputmode="decimal" value="{{ '' if t.strike is none else t.strike }}"/></div>
            </div>

            <div class="row stack10">
              <div><label>🧾 Contracts</label><input name="contracts" inputmode="numeric" value="{{ t.contracts or 1 }}"/></div>
              <div><label>💰 Entry</label><input name="entry_price" inputmode="decimal" value="{{ '' if t.entry_price is none else t.entry_price }}"/></div>
              <div><label>💰 Exit</label><input name="exit_price" inputmode="decimal" value="{{ '' if t.exit_price is none else t.exit_price }}"/></div>
            </div>

            <div class="row stack10">
              <div><label>💵 Fees (total)</label><input name="comm" inputmode="decimal" value="{{ t.comm or 0.70 }}"/></div>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save</button>
              <a class="btn" href="/trades{% if back_query %}?{{ back_query }}{% endif %}">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        t=t,
        d=d,
        q=q,
        back_query=back_query,
    )
    return legacy.render_page(content, active="trades")


def trades_review(trade_id: int):
    row = legacy.get_trade(trade_id)
    if not row:
        legacy.abort(404)

    d = legacy.request.args.get("d", "")
    q = legacy.request.args.get("q", "")
    review_filters = analytics_repo.normalize_trade_filters(
        {
            "setup": legacy.request.args.get("setup", ""),
            "session": legacy.request.args.get("session", ""),
            "outcome": legacy.request.args.get("outcome", ""),
            "time_block": legacy.request.args.get("time_block", ""),
            "mistake_tag": legacy.request.args.get("mistake_tag", ""),
        }
    )
    back_query = urlencode(
        {
            "d": d,
            "q": q,
            **{key: value for key, value in review_filters.items() if value},
        }
    )
    rv = legacy.repo.get_trade_review(trade_id) or {}
    trade_row = dict(row)
    trade_metrics = {
        "net_pl": float(row["net_pl"] or 0.0) if row["net_pl"] is not None else 0.0,
        "hold_minutes": legacy.repo.compute_hold_minutes(
            trade_row.get("entry_time"), trade_row.get("exit_time")
        ),
    }
    planned_risk_existing = legacy.parse_float(str(rv.get("planned_risk_dollars") or ""))
    trade_metrics["r_multiple"] = (
        (trade_metrics["net_pl"] / planned_risk_existing)
        if planned_risk_existing and planned_risk_existing > 0
        else None
    )

    if legacy.request.method == "POST":
        f = legacy.request.form
        strategy_label = (f.get("strategy_label") or f.get("setup_tag") or "").strip()
        session_tag = (f.get("session_tag") or "").strip()
        score_raw = (f.get("checklist_score") or "").strip()
        checklist_score = legacy.parse_int(score_raw) if score_raw else None
        execution_grade = legacy.parse_int((f.get("execution_grade") or "").strip() or "")
        risk_grade = legacy.parse_int((f.get("risk_grade") or "").strip() or "")
        plan_grade = legacy.parse_int((f.get("plan_grade") or "").strip() or "")
        rule_break_tags = (f.get("rule_break_tags") or "").strip()
        rule_break_tags = legacy._merge_auto_rule_break_tags(
            entry_price=legacy.parse_float(
                str(row["entry_price"]) if row["entry_price"] is not None else ""
            ),
            exit_price=legacy.parse_float(
                str(row["exit_price"]) if row["exit_price"] is not None else ""
            ),
            existing_tags=rule_break_tags,
        )
        thesis_note = (f.get("thesis_note") or "").strip()
        mistake_tags = (f.get("mistake_tags") or "").strip()
        planned_risk_dollars = legacy.parse_float((f.get("planned_risk_dollars") or "").strip())
        size_rule_note = (f.get("size_rule_note") or "").strip()
        entry_quality_note = (f.get("entry_quality_note") or "").strip()
        exit_quality_note = (f.get("exit_quality_note") or "").strip()
        review_note = (f.get("review_note") or "").strip()
        improvement_note = (f.get("improvement_note") or "").strip()
        legacy.repo.upsert_trade_review(
            trade_id=trade_id,
            strategy_id=rv.get("strategy_id"),
            strategy_label=strategy_label,
            setup_tag=strategy_label,
            session_tag=session_tag,
            checklist_score=checklist_score,
            rule_break_tags=rule_break_tags,
            review_note=review_note,
            thesis_note=thesis_note,
            execution_grade=execution_grade,
            risk_grade=risk_grade,
            plan_grade=plan_grade,
            mistake_tags=mistake_tags,
            planned_risk_dollars=planned_risk_dollars,
            size_rule_note=size_rule_note,
            entry_quality_note=entry_quality_note,
            exit_quality_note=exit_quality_note,
            improvement_note=improvement_note,
        )
        legacy.flash("Trade review saved.", "success")
        return legacy.redirect(f"/trades?{back_query}" if back_query else legacy.url_for("trades_page"))

    strategy_options = [dict(r) for r in legacy.strategies_repo.fetch_strategies()]
    content = legacy.render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🧠 Trade Review #{{ t.id }}</div>
          <div class="tiny stack8">{{ t.trade_date }} · {{ t.ticker }} {{ t.opt_type }}</div>
          <div class="trendChips">
            <span class="trendChip {% if metrics.net_pl > 0 %}positive{% elif metrics.net_pl < 0 %}negative{% endif %}">Net {{ money(metrics.net_pl) }}</span>
            <span class="trendChip">Hold {{ metrics.hold_minutes if metrics.hold_minutes is not none else '—' }} min</span>
            <span class="trendChip">Time Block {{ repo.classify_time_block(t.entry_time) }}</span>
            <span class="trendChip">R {% if metrics.r_multiple is not none %}{{ '%.2f'|format(metrics.r_multiple) }}R{% else %}—{% endif %}</span>
          </div>
          <div class="hr"></div>
          <form method="post" action="/trades/review/{{ t.id }}?{{ back_query }}">
            <div class="pill">Setup</div>
            <div class="row">
              <div>
                <label>Strategy</label>
                <input name="strategy_label" list="strategy-options" value="{{ rv.get('strategy_label','') or rv.get('setup_tag','') }}" placeholder="FVG, ORB, Fade, Breakout">
              </div>
              <div>
                <label>Session Tag</label>
                <select name="session_tag">
                  {% set s = rv.get('session_tag','') %}
                  <option value="" {% if s=='' %}selected{% endif %}>—</option>
                  <option value="Open" {% if s=='Open' %}selected{% endif %}>Open</option>
                  <option value="Midday" {% if s=='Midday' %}selected{% endif %}>Midday</option>
                  <option value="Power Hour" {% if s=='Power Hour' %}selected{% endif %}>Power Hour</option>
                  <option value="After Hours" {% if s=='After Hours' %}selected{% endif %}>After Hours</option>
                </select>
              </div>
              <div><label>Checklist Score (0-100)</label><input name="checklist_score" inputmode="numeric" value="{{ '' if rv.get('checklist_score') is none else rv.get('checklist_score') }}"></div>
            </div>
            <div class="stack10">
              <label>Thesis Note</label>
              <textarea name="thesis_note" placeholder="What was the read before entry?">{{ rv.get('thesis_note','') }}</textarea>
            </div>
            <div class="hr"></div>
            <div class="pill">Risk</div>
            <div class="row">
              <div><label>Planned Risk ($)</label><input name="planned_risk_dollars" inputmode="decimal" value="{{ '' if rv.get('planned_risk_dollars') is none else rv.get('planned_risk_dollars') }}"></div>
              <div><label>Risk Grade (0-100)</label><input name="risk_grade" inputmode="numeric" value="{{ '' if rv.get('risk_grade') is none else rv.get('risk_grade') }}"></div>
              <div><label>Plan Grade (0-100)</label><input name="plan_grade" inputmode="numeric" value="{{ '' if rv.get('plan_grade') is none else rv.get('plan_grade') }}"></div>
            </div>
            <div class="stack10">
              <label>Size Rule Note</label>
              <textarea name="size_rule_note" placeholder="Was size aligned with the plan?">{{ rv.get('size_rule_note','') }}</textarea>
            </div>
            <div class="hr"></div>
            <div class="pill">Execution</div>
            <div class="row">
              <div><label>Execution Grade (0-100)</label><input name="execution_grade" inputmode="numeric" value="{{ '' if rv.get('execution_grade') is none else rv.get('execution_grade') }}"></div>
              <div><label>Entry Quality Note</label><input name="entry_quality_note" value="{{ rv.get('entry_quality_note','') }}" placeholder="Patience, location, confirmation"></div>
              <div><label>Exit Quality Note</label><input name="exit_quality_note" value="{{ rv.get('exit_quality_note','') }}" placeholder="Scale, target, stop management"></div>
            </div>
            <div class="hr"></div>
            <div class="pill">Mistakes</div>
            <div class="row stack10">
              <div>
                <label>Mistake Tags (comma separated)</label>
                <input name="mistake_tags" value="{{ rv.get('mistake_tags','') }}" placeholder="late entry, overtrade, size creep">
              </div>
            </div>
            <div class="row stack10">
              <div>
                <label>Rule-Break Tags (comma separated)</label>
                <input name="rule_break_tags" value="{{ rv.get('rule_break_tags','') }}" placeholder="oversized, late entry, no stop, revenge trade">
              </div>
            </div>
            <datalist id="strategy-options">
              {% for strategy in strategy_options %}
                <option value="{{ strategy['title'] }}"></option>
              {% endfor %}
            </datalist>
            <div class="hr"></div>
            <div class="pill">Lesson</div>
            <div class="stack10">
              <label>Review Note</label>
              <textarea name="review_note" placeholder="What to repeat, what to remove next session">{{ rv.get('review_note','') }}</textarea>
            </div>
            <div class="stack10">
              <label>Improvement Note</label>
              <textarea name="improvement_note" placeholder="What changes on the next clean rep?">{{ rv.get('improvement_note','') }}</textarea>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save Review</button>
              <a class="btn" href="/analytics?tab=edge&setup={{ (rv.get('strategy_label','') or rv.get('setup_tag',''))|urlencode }}">Setup Analytics</a>
              <a class="btn" href="/journal/new?d={{ t.trade_date }}&entry_type=trade_debrief">Journal Draft</a>
              <a class="btn" href="/trades{% if back_query %}?{{ back_query }}{% endif %}">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        t=trade_row,
        rv=rv,
        d=d,
        q=q,
        back_query=back_query,
        strategy_options=strategy_options,
        metrics=trade_metrics,
        money=legacy.money,
        repo=legacy.repo,
    )
    return legacy.render_page(content, active="trades")


def trades_risk_controls():
    if legacy.request.method == "POST":
        daily_max_loss = legacy.parse_float(legacy.request.form.get("daily_max_loss", "")) or 0.0
        enforce_lockout = 1 if legacy.request.form.get("enforce_lockout") == "1" else 0
        legacy.repo.save_risk_controls(daily_max_loss, enforce_lockout)
        legacy.flash("Risk controls saved.", "success")
        return legacy.redirect(legacy.url_for("trades_risk_controls"))

    rc = legacy.repo.get_risk_controls()
    state = legacy.trade_lockout_state(legacy.today_iso())
    content = legacy.render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🛡️ Risk Controls</div>
          <div class="tiny stack8">
            Today's net: {{ money(state.day_net) }} · Max loss: {{ money(state.daily_max_loss) }} ·
            Status: {% if state.locked %}<b class="statusLock">LOCKED</b>{% else %}<b class="statusActive">ACTIVE</b>{% endif %}
          </div>
          <div class="hr"></div>
          <form method="post">
            <div class="row">
              <div><label>Daily Max Loss ($)</label><input name="daily_max_loss" inputmode="decimal" value="{{ rc.daily_max_loss }}"></div>
              <div>
                <label>Enforce Lockout</label>
                <select name="enforce_lockout">
                  <option value="0" {% if not rc.enforce_lockout %}selected{% endif %}>Off</option>
                  <option value="1" {% if rc.enforce_lockout %}selected{% endif %}>On</option>
                </select>
              </div>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">Save Risk Controls</button>
              <a class="btn" href="/trades">Trades</a>
            </div>
          </form>
        </div></div>
        """,
        rc=rc,
        state=state,
        money=legacy.money,
    )
    return legacy.render_page(content, active="trades")
