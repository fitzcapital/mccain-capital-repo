"""Trades page view assembly."""

from __future__ import annotations

from mccain_capital.services import trades as legacy
from mccain_capital.services import trades_balance as trades_balance_svc
from mccain_capital.services.viewmodels import (
    balance_state_badges,
    sync_state_badges,
    trades_data_trust,
)
from mccain_capital.runtime import (
    get_setting_float,
    money,
    next_trading_day_iso,
    parse_int,
    pct,
    prev_trading_day_iso,
    today_iso,
)


def trades_page():
    d = legacy.request.args.get("d", "")
    active_day = d or today_iso()

    prev_day = prev_trading_day_iso(active_day)
    next_day = next_trading_day_iso(active_day)

    q = legacy.request.args.get("q", "")
    page = max(1, parse_int(legacy.request.args.get("page") or "1") or 1)
    per = parse_int(legacy.request.args.get("per") or "50") or 50
    per = max(25, min(200, per))
    scope_state = trades_balance_svc.scope_state_for_day(active_day)
    account_scope = scope_state["account_scope"]

    raw_trades = legacy.fetch_trades(d=d, q=q)
    trades = [dict(r) for r in raw_trades]
    review_map = legacy.fetch_trade_reviews_map(
        [int(t["id"]) for t in trades if t.get("id") is not None]
    )
    for t in trades:
        rv = review_map.get(int(t["id"]), {})
        t["setup_tag"] = rv.get("strategy_label", "") or rv.get("setup_tag", "")
        t["session_tag"] = rv.get("session_tag", "")
        t["checklist_score"] = rv.get("checklist_score", None)
        t["rule_break_tags"] = rv.get("rule_break_tags", "")
    derived_balances = trades_balance_svc.derived_balance_map(
        as_of=active_day,
        start_date=scope_state["scope_start"] if scope_state["scope_active"] else "",
        starting_balance=(
            scope_state["scope_starting_balance"] if scope_state["scope_active"] else None
        ),
    )
    for t in trades:
        trade_id = t.get("id")
        if trade_id in derived_balances:
            t["balance"] = derived_balances[trade_id]
    total_rows = len(trades)
    page_count = max(1, (total_rows + per - 1) // per)
    if page > page_count:
        page = page_count
    row_start = (page - 1) * per
    row_end = row_start + per
    page_trades = trades[row_start:row_end]

    stats = legacy.trade_day_stats(trades)
    cons = legacy.calc_consistency(trades)
    guardrail = legacy.trade_lockout_state(active_day)
    sync_status = legacy._load_last_sync_status() or {}
    history_starting_balance = float(get_setting_float("starting_balance", 50000.0))
    balance_integrity = trades_balance_svc.balance_integrity_for_day(active_day, scope_state)
    balance_badges = balance_state_badges(balance_integrity)
    data_trust = trades_data_trust(
        sync_status, guardrail_locked=bool(guardrail.get("locked")), active_day=active_day
    )
    sync_badges = sync_state_badges(
        sync_status,
        status_key="status",
        stage_key="stage",
        updated_key="updated_at_human",
    )

    week_total = legacy.week_total_net(d or None)
    running_balance = trades_balance_svc.running_balance_for_day(active_day, scope_state)
    totals = trades_balance_svc.summary_totals_for_day(active_day, scope_state)
    ytd_net = float(totals["ytd_net"] or 0.0)
    all_time_net = float(totals["all_time_net"] or 0.0)
    prior_eod_balance = totals["prior_eod_balance"]
    day_net = float(
        (stats["total"] if isinstance(stats, dict) else getattr(stats, "total", 0.0)) or 0.0
    )
    win_rate = float(
        (stats["win_rate"] if isinstance(stats, dict) else getattr(stats, "win_rate", 0.0)) or 0.0
    )
    trades_count = len(trades)
    avg_net = (day_net / trades_count) if trades_count else 0.0
    if trades_count == 0:
        execution_msg = (
            "No trades logged for the current filter. Start with one clean, rules-based setup."
        )
    elif win_rate >= 60 and day_net >= 0:
        execution_msg = "Execution quality is stable today. Keep sizing disciplined and avoid late-session forcing."
    elif day_net < 0:
        execution_msg = "P/L is under pressure. Prioritize A+ entries and reduce pace until process quality improves."
    else:
        execution_msg = (
            "Mixed session so far. Focus on setup clarity and post-trade review accuracy."
        )

    if guardrail.get("locked"):
        risk_msg = "Guardrail is locked. New trades should pause until next session or risk controls are adjusted."
    else:
        risk_msg = (
            f"Guardrail active with day net at {money(guardrail.get('day_net') or 0)}. "
            "Current risk posture is tradable."
        )

    next_action_msg = (
        "Tag every trade with setup/session and complete missing review scores before day end."
        if trades_count
        else "Import statement or add first trade, then complete setup/session review tags."
    )
    is_day_view = bool(d)
    primary_net_label = (
        f"💰 Day Net ({d})" if is_day_view else "💰 Filtered Net (All Visible Trades)"
    )
    primary_net_sub = (
        "Net for the selected trading day"
        if is_day_view
        else "Net across the current filter (all dates when no date is set)"
    )
    secondary_total_label = "📅 Week Total" if is_day_view else "🏁 All-Time Net"
    secondary_total_value = week_total if is_day_view else all_time_net

    content = legacy.render_template(
        "trades/index.html",
        trades=trades,
        page_trades=page_trades,
        total_rows=total_rows,
        page=page,
        page_count=page_count,
        per=per,
        d=d,
        q=q,
        stats=stats,
        cons=cons,
        week_total=week_total,
        running_balance=running_balance,
        ytd_net=ytd_net,
        all_time_net=all_time_net,
        prior_eod_balance=prior_eod_balance,
        money=money,
        pct=pct,
        prev_day=prev_day,
        next_day=next_day,
        day_net=day_net,
        win_rate=win_rate,
        trades_count=trades_count,
        avg_net=avg_net,
        execution_msg=execution_msg,
        risk_msg=risk_msg,
        next_action_msg=next_action_msg,
        guardrail=guardrail,
        data_trust=data_trust,
        balance_integrity=balance_integrity,
        balance_badges=balance_badges,
        sync_badges=sync_badges,
        account_scope=account_scope,
        history_starting_balance=history_starting_balance,
        primary_net_label=primary_net_label,
        primary_net_sub=primary_net_sub,
        secondary_total_label=secondary_total_label,
        secondary_total_value=secondary_total_value,
    )

    return legacy.render_page(content, active="trades")
