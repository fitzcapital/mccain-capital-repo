"""Trades edit/review/risk-control form handlers."""

from __future__ import annotations

from datetime import datetime
from urllib.parse import urlencode

from mccain_capital.repositories import analytics as analytics_repo
from mccain_capital.services import market_data_service
from mccain_capital.services.trade_review_scoring import (
    compute_trade_review_foundation,
    grade_from_score,
)
from mccain_capital.services import trades as legacy


def _trade_back_query() -> str:
    return urlencode(
        {key: value for key, value in legacy.request.args.items() if value not in (None, "", [])}
    )


def _merge_trade_review_payload(trade_row: dict, review_row: dict | None) -> dict:
    payload = dict(trade_row)
    payload.update(review_row or {})
    payload["setup_display"] = (
        str(payload.get("strategy_label") or payload.get("setup_tag") or "").strip() or "Unknown"
    )
    foundation = compute_trade_review_foundation(payload)
    payload.update(foundation)
    return payload


def _intraday_points_for_day(symbol: str, day: str) -> list[dict]:
    try:
        rows = market_data_service.get_intraday(symbol)
    except Exception:
        rows = []
    points: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ts_raw = str(row.get("ts") or "").strip()
        close_v = row.get("close")
        if not ts_raw or not isinstance(close_v, (int, float)):
            continue
        try:
            dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        dt_et = (
            dt.astimezone(legacy.app_runtime.TZ)
            if dt.tzinfo
            else dt.replace(tzinfo=legacy.app_runtime.TZ)
        )
        if dt_et.date().isoformat() != day:
            continue
        points.append(
            {
                "ts": dt_et.isoformat(),
                "label": dt_et.strftime("%I:%M %p").lstrip("0"),
                "minute": (dt_et.hour * 60) + dt_et.minute,
                "close": float(close_v),
            }
        )
    return points


def _minute_from_label(value: str) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    for fmt in ("%I:%M %p", "%H:%M"):
        try:
            parsed = datetime.strptime(text.upper(), fmt)
            return (parsed.hour * 60) + parsed.minute
        except ValueError:
            continue
    return 0


def _trade_replay_payload(trade: dict, review: dict) -> dict:
    trade_date = str(trade.get("trade_date") or "").strip()
    symbol = str(trade.get("ticker") or "SPX").strip().upper() or "SPX"
    intraday = _intraday_points_for_day(symbol, trade_date)
    entry_minute = _minute_from_label(str(trade.get("entry_time") or ""))
    exit_minute = _minute_from_label(str(trade.get("exit_time") or "")) or entry_minute + 30
    series: list[dict] = []
    mode = "reference"
    if intraday:
        filtered = [
            point
            for point in intraday
            if point["minute"] >= max(0, entry_minute - 20)
            and point["minute"] <= min((24 * 60), exit_minute + 20)
        ]
        series = filtered or intraday[-30:]
        mode = "intraday"
    else:
        entry_price = legacy.parse_float(str(trade.get("entry_price") or ""))
        exit_price = legacy.parse_float(str(trade.get("exit_price") or ""))
        series = [
            {
                "label": str(trade.get("entry_time") or "Entry"),
                "minute": entry_minute,
                "close": float(entry_price or 0),
            },
            {
                "label": str(trade.get("exit_time") or "Exit"),
                "minute": exit_minute,
                "close": float(exit_price or entry_price or 0),
            },
        ]
    return {
        "mode": mode,
        "series": series,
        "entry_minute": entry_minute,
        "exit_minute": exit_minute,
        "entry_price": legacy.parse_float(str(trade.get("entry_price") or "")),
        "exit_price": legacy.parse_float(str(trade.get("exit_price") or "")),
        "stop_price": review.get("stop_value"),
        "target_price": review.get("target_value"),
        "summary": (
            "Trade-window replay using same-day intraday prints."
            if mode == "intraday"
            else "Reference replay built from the trade’s saved execution levels."
        ),
    }


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

    back_query = _trade_back_query()
    rv = legacy.repo.get_trade_review(trade_id) or {}
    trade_row = dict(row)
    review_payload = _merge_trade_review_payload(trade_row, rv)
    trade_metrics = {
        "net_pl": float(row["net_pl"] or 0.0) if row["net_pl"] is not None else 0.0,
        "hold_minutes": legacy.repo.compute_hold_minutes(
            trade_row.get("entry_time"), trade_row.get("exit_time")
        ),
        "r_multiple": review_payload.get("r_multiple"),
    }

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
        reviewed_stop_price = legacy.parse_float((f.get("reviewed_stop_price") or "").strip())
        reviewed_target_price = legacy.parse_float((f.get("reviewed_target_price") or "").strip())
        reviewed_risk_dollars = legacy.parse_float((f.get("reviewed_risk_dollars") or "").strip())
        reviewed_risk_percent = legacy.parse_float((f.get("reviewed_risk_percent") or "").strip())
        reviewed_execution_quality = (f.get("reviewed_execution_quality") or "").strip()
        reviewed_sizing_quality = (f.get("reviewed_sizing_quality") or "").strip()
        reviewed_stop_discipline = (f.get("reviewed_stop_discipline") or "").strip()
        reviewed_within_plan_raw = (f.get("reviewed_within_plan") or "").strip()
        reviewed_within_plan = (
            1 if reviewed_within_plan_raw == "1" else 0 if reviewed_within_plan_raw == "0" else None
        )
        manual_grade_score = legacy.parse_int((f.get("manual_grade_score") or "").strip() or "")
        manual_grade_letter = (f.get("manual_grade_letter") or "").strip().upper()
        if manual_grade_score is not None and not manual_grade_letter:
            manual_grade_letter = grade_from_score(manual_grade_score)
        if manual_grade_letter and manual_grade_score is None:
            score_floor = {"A": 90, "B": 75, "C": 60, "D": 40, "F": 0}.get(manual_grade_letter)
            manual_grade_score = score_floor
        classification_override = (f.get("classification_override") or "").strip()
        grade_override_reason = (f.get("grade_override_reason") or "").strip()
        if f.get("use_auto_grade") == "1":
            manual_grade_score = None
            manual_grade_letter = ""
            classification_override = ""
            grade_override_reason = ""
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
            reviewed_stop_price=reviewed_stop_price,
            reviewed_target_price=reviewed_target_price,
            reviewed_risk_dollars=reviewed_risk_dollars,
            reviewed_risk_percent=reviewed_risk_percent,
            reviewed_execution_quality=reviewed_execution_quality,
            reviewed_sizing_quality=reviewed_sizing_quality,
            reviewed_stop_discipline=reviewed_stop_discipline,
            reviewed_within_plan=reviewed_within_plan,
            manual_grade_score=manual_grade_score,
            manual_grade_letter=manual_grade_letter,
            grade_override_reason=grade_override_reason,
            classification_override=classification_override,
        )
        legacy.flash("Trade review saved.", "success")
        if f.get("back_to_trades") == "1":
            return legacy.redirect(
                f"/trades?{back_query}" if back_query else legacy.url_for("trades_page")
            )
        review_href = f"/trades/review/{trade_id}"
        if back_query:
            review_href += f"?{back_query}"
        return legacy.redirect(review_href)

    strategy_options = [dict(r) for r in legacy.strategies_repo.fetch_strategies()]
    content = legacy.render_template(
        "trades/review.html",
        t=trade_row,
        rv=rv,
        review=review_payload,
        back_query=back_query,
        strategy_options=strategy_options,
        metrics=trade_metrics,
        money=legacy.money,
        repo=legacy.repo,
    )
    return legacy.render_page(content, active="trades")


def trades_replay(trade_id: int):
    row = legacy.get_trade(trade_id)
    if not row:
        legacy.abort(404)
    back_query = _trade_back_query()
    trade_row = dict(row)
    review_row = legacy.repo.get_trade_review(trade_id) or {}
    review_payload = _merge_trade_review_payload(trade_row, review_row)
    replay_chart = _trade_replay_payload(trade_row, review_payload)
    content = legacy.render_template(
        "trades/replay.html",
        t=trade_row,
        rv=review_row,
        review=review_payload,
        replay_chart=replay_chart,
        back_query=back_query,
        money=legacy.money,
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
