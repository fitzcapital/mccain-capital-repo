"""Dashboard endpoint service extractions."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, List

from flask import jsonify, render_template, request, url_for

from mccain_capital.services import core as core_svc


def dashboard_calendar_fragment():
    from mccain_capital.repositories import trades as trades_repo

    requested_account_raw = str(request.args.get("account_id") or "").strip().lower()
    if requested_account_raw == "all":
        trades_repo.set_active_account(None)
    else:
        try:
            requested_account_id = int(requested_account_raw) if requested_account_raw else None
        except ValueError:
            requested_account_id = None
        if requested_account_id and trades_repo.get_account(requested_account_id):
            trades_repo.set_active_account(requested_account_id)
    scope = trades_repo.account_scope_snapshot()
    scope_enabled = bool(scope.get("enabled"))
    scope_mode_raw = (request.args.get("scope") or "").strip().lower()
    scope_active = scope_enabled and scope_mode_raw != "all"
    try:
        scope_account_id = int(scope.get("account_id") or 0) or None
    except Exception:
        scope_account_id = None
    scoped_ledger_account_ids = (
        trades_repo.account_continuity_ids(scope_account_id)
        if scope_active and scope_account_id
        else []
    )
    scope_start = str(scope.get("start_date") or "")
    scope_starting_balance = float(scope.get("starting_balance") or 0.0)
    anchor = core_svc.app_runtime.now_et().date()
    year = int(request.args.get("y") or anchor.year)
    month = max(1, min(12, int(request.args.get("m") or anchor.month)))
    calendar_scope_label = (
        "Continuity Ledger"
        if scope_active and len(scoped_ledger_account_ids) > 1
        else "Active Account" if scope_active else "All History"
    )
    calendar_payload = core_svc._dashboard_calendar_payload(
        year=year,
        month=month,
        scope_active=scope_active,
        scope_account_id=scope_account_id,
        scope_start=scope_start,
        scope_starting_balance=scope_starting_balance,
        scope_label=calendar_scope_label,
        scope_account_ids=scoped_ledger_account_ids if scoped_ledger_account_ids else None,
    )
    return render_template(
        "dashboard/_calendar_panel.html",
        heat=calendar_payload["heat"],
        month_name=calendar_payload["month_name"],
        calendar_state=calendar_payload["calendar_state"],
        money=core_svc.app_runtime.money,
        money_compact=core_svc._money_compact,
    )


def dashboard_planning_refresh_api():
    from mccain_capital.repositories import analytics as analytics_repo
    from mccain_capital.repositories import trades as trades_repo
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_worker

    if core_svc.auth_enabled() and not core_svc.is_authenticated():
        return jsonify({"ok": False, "error": "auth_required"}), 401

    ticker_context = core_svc.get_playbook_ticker_context(request.args.get("ticker"))
    selected_ticker = str(ticker_context["ticker"])
    force_refresh = (request.args.get("force") or "").strip() == "1"
    scope_mode = (request.args.get("scope") or "all").strip().lower()
    scope = trades_repo.account_scope_snapshot()
    scope_active = bool(scope.get("enabled")) and scope_mode != "all"
    try:
        scope_account_id = int(scope.get("account_id") or 0) or None
    except Exception:
        scope_account_id = None
    scoped_account_id = scope_account_id if scope_active else None
    scoped_account_ids = (
        trades_repo.account_continuity_ids(scope_account_id)
        if scope_active and scope_account_id
        else []
    )

    try:
        market_worker.start_market_worker_once()
    except Exception:
        pass

    now_et = core_svc.app_runtime.now_et()
    today_key = core_svc.app_runtime.today_iso()
    anchor = trades_repo.latest_trade_day() or now_et.date()
    year = int(request.args.get("y") or anchor.year)
    month = max(1, min(12, int(request.args.get("m") or anchor.month)))

    today_rows = [
        dict(r)
        for r in trades_repo.fetch_trades(
            d=today_key,
            q="",
            account_id=scoped_account_id if not scoped_account_ids else None,
            account_ids=scoped_account_ids or None,
        )
    ]
    today_stats = trades_repo.trade_day_stats(today_rows)
    today_net = float(today_stats.get("total", 0.0))
    today_wins = int(today_stats.get("wins", 0) or 0)
    today_losses = int(today_stats.get("losses", 0) or 0)
    today_count = len(today_rows)

    ytd_trades_list = [
        dict(r)
        for r in trades_repo.fetch_trades_range(
            date(year, 1, 1).isoformat(),
            date(year + 1, 1, 1).isoformat(),
            account_id=scoped_account_id if not scoped_account_ids else None,
            account_ids=scoped_account_ids or None,
        )
    ]
    ytd_cons = trades_repo.calc_consistency(ytd_trades_list)
    recent_start = max(date(year, month, 1), anchor - timedelta(days=45))
    recent_rows = analytics_repo.fetch_analytics_rows(recent_start.isoformat(), anchor.isoformat())
    recent_rule_breaks = analytics_repo.rule_break_counts(recent_rows)
    top_rule_break = recent_rule_breaks[0] if recent_rule_breaks else None

    risk_posture_title = (
        "Attack window"
        if today_count
        and today_net > 0
        and (ytd_cons.get("ratio") is None or ytd_cons.get("ratio", 1.0) <= 0.30)
        else "Protect capital" if today_count and today_net < 0 else "Wait for clean signal"
    )
    risk_posture_detail = (
        f"Today {today_wins}W/{today_losses}L · Consistency "
        + (f"{float(ytd_cons['ratio']) * 100.0:.1f}%" if ytd_cons.get("ratio") is not None else "—")
        + "."
    )
    pattern_watch = (
        f"Most common breach: {str(top_rule_break['tag']).replace('-', ' ').title()} ({top_rule_break['count']})."
        if top_rule_break
        else "No recurring rule-break tag is dominating recent sessions."
    )

    tape_snapshot = market_worker.get_market_snapshot()
    tape_prices = dict(tape_snapshot.get("prices") or {})
    dashboard_instrument = dict(tape_prices.get(selected_ticker) or {})
    dashboard_vix = dict(tape_prices.get("VIX") or {})

    cached_playbook_snapshot = core_svc._market_pulse_cached_playbook_snapshot(
        now_et,
        ticker=selected_ticker,
    )
    if cached_playbook_snapshot and not force_refresh:
        playbook_snapshot = cached_playbook_snapshot
        gamma_snapshot = dict(playbook_snapshot.get("gamma_snapshot") or {})
    else:
        try:
            gamma_snapshot = gamma_map_service.get_gamma_snapshot()
        except Exception:
            gamma_snapshot = {}
        playbook_snapshot = core_svc.get_or_build_market_pulse_snapshot(
            ticker=selected_ticker,
            force_refresh=force_refresh,
            now_et=now_et,
            preloaded_gamma_snapshot=gamma_snapshot,
            preloaded_macro_events=[],
        )

    try:
        news_snapshot = core_svc._market_news_snapshot(page_type="dashboard", macro_events=[])
    except Exception:
        news_snapshot = {"macro_events": []}
    dashboard_playbook_view = dict(playbook_snapshot.get("playbook_view") or {})
    dashboard_market_structure_snapshot = dict(
        playbook_snapshot.get("market_structure_snapshot") or {}
    )
    refreshed_gamma_snapshot = dict(playbook_snapshot.get("gamma_snapshot") or gamma_snapshot or {})
    daily_brief = core_svc._dashboard_daily_brief_viewmodel(
        now_et=now_et,
        dashboard_spx=dashboard_instrument,
        dashboard_vix=dashboard_vix,
        ticker=selected_ticker,
        dashboard_instrument=dict(playbook_snapshot.get("playbook_quote") or dashboard_instrument),
        gamma_snapshot=refreshed_gamma_snapshot,
        market_structure_snapshot=dashboard_market_structure_snapshot,
        news_snapshot=news_snapshot,
        today_count=today_count,
        today_net=today_net,
    )
    readiness = core_svc._dashboard_readiness_viewmodel(
        [],
        brief_ready=True,
        today_count=today_count,
        data_trust={},
    )
    gamma_strip = core_svc._dashboard_gamma_strip_viewmodel(
        playbook_view=dashboard_playbook_view,
        market_structure_snapshot=dashboard_market_structure_snapshot,
    )
    decision_panel = core_svc._dashboard_decision_viewmodel(
        daily_brief=daily_brief,
        risk_posture_title=risk_posture_title,
        risk_posture_detail=risk_posture_detail,
        data_trust={},
        readiness=readiness,
        dashboard_vix=dashboard_vix,
        playbook_view=dashboard_playbook_view,
        gamma_strip=gamma_strip,
        market_structure_snapshot=dashboard_market_structure_snapshot,
    )
    brief_html = render_template(
        "dashboard/_brief_card.html",
        selected_ticker=selected_ticker,
        daily_brief=daily_brief,
        decision_panel=decision_panel,
        dashboard_year=year,
        dashboard_month=month,
        scope_mode=("active" if scope_mode == "active" else "all"),
        market_pulse_href=url_for("market_pulse_page", ticker=selected_ticker),
    )
    return jsonify(
        {
            "ok": True,
            "decision_panel": decision_panel,
            "dashboard_gamma": gamma_strip,
            "market_structure_snapshot": dashboard_market_structure_snapshot,
            "pattern_watch": pattern_watch,
            "brief_html": brief_html,
            "refreshed_at": core_svc.app_runtime.now_iso(),
        }
    )


def dashboard_tape_refresh_api():
    from mccain_capital.services import market_data_service
    from mccain_capital.services import market_worker

    if core_svc.auth_enabled() and not core_svc.is_authenticated():
        return jsonify({"ok": False, "error": "auth_required"}), 401

    ticker_context = core_svc.get_playbook_ticker_context(request.args.get("ticker"))
    symbols = core_svc._dashboard_tape_symbols(str(ticker_context["ticker"]))
    try:
        market_worker.start_market_worker_once()
    except Exception:
        pass

    snapshot = market_worker.get_market_snapshot()
    prices = dict(snapshot.get("prices") or {})
    needs_fill = [sym for sym in symbols if not isinstance((prices.get(sym) or {}).get("price"), (int, float))]

    if needs_fill:
        try:
            direct_quotes = market_data_service.get_watchlist_tradier(needs_fill)
        except Exception:
            direct_quotes = {}
        for sym in needs_fill:
            quote = dict((direct_quotes or {}).get(sym) or {})
            if isinstance(quote.get("price"), (int, float)):
                prices[sym] = quote

    series_points: Dict[str, List[Dict[str, float]]] = {}
    for sym in symbols:
        quote = dict(prices.get(sym) or {})
        quote.setdefault("symbol", sym)
        quote.setdefault("label", sym)
        spark_values = core_svc._market_pulse_resolve_sparkline_values(quote)
        if len(spark_values) < 4 and sym in {"VIX", "^VIX"}:
            try:
                rows = market_data_service.get_intraday(sym)
            except Exception:
                rows = []
            vix_values = [
                float(row.get("close"))
                for row in rows[-120:]
                if isinstance(row, dict) and isinstance(row.get("close"), (int, float))
            ]
            deduped_vix: List[float] = []
            for value in vix_values:
                if not deduped_vix or abs(deduped_vix[-1] - value) > 0.001:
                    deduped_vix.append(value)
            if len(deduped_vix) >= 4:
                spark_values = deduped_vix[-40:]
                prices[sym] = {
                    **quote,
                    "mini_series": spark_values,
                    "series": [{"v": value, "close": value} for value in spark_values],
                }
        if len(spark_values) >= 4:
            series_points[sym] = [
                {"v": float(value), "close": float(value)}
                for value in spark_values[-40:]
                if isinstance(value, (int, float))
            ]

    updated_raw = str(snapshot.get("updated_at") or core_svc.app_runtime.now_iso())
    updated_label = core_svc._format_iso_et_label(updated_raw)
    if updated_label:
        parts = updated_label.split(" ", 3)
        if len(parts) >= 4:
            updated_label = parts[3]

    return jsonify(
        {
            "ok": True,
            "quotes": {sym: dict(prices.get(sym) or {}) for sym in symbols},
            "series_points": series_points,
            "updated_at": updated_raw,
            "updated_label": updated_label or "—",
        }
    )
