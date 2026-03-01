"""Core domain service gateway.

Core routes still rely on legacy implementations in ``app_core``. This module
keeps that dependency localized behind explicit delegator functions.
"""

from __future__ import annotations

from calendar import Calendar
from datetime import date
from datetime import timedelta
import json
import os
import shutil
import tempfile
import zipfile
from typing import Any, Dict, List, Optional, Tuple

from flask import abort, flash, jsonify, redirect, render_template, request, send_file, url_for

from mccain_capital.auth import auth_enabled, effective_username, is_authenticated
from mccain_capital import runtime as app_runtime
from mccain_capital.services.ui import get_system_status, render_page, simple_msg
from mccain_capital.services.viewmodels import dashboard_data_trust

MULTIPLIER = 100
DEFAULT_STOP_PCT = 20.0
DEFAULT_TARGET_PCT = 30.0
DEFAULT_FEE_PER_CONTRACT = 0.70
DAY_OPEN_INTERVALS = tuple(range(2, 13))
WEEK_OPEN_INTERVALS = (2, 3, 4, 5)


def _legacy():
    from mccain_capital import app_core

    return app_core


def home():
    return _legacy().home()


def setup_page():
    from mccain_capital.services import auth as auth_svc

    return auth_svc.setup_page()


def login_page():
    from mccain_capital.services import auth as auth_svc

    return auth_svc.login_page()


def logout_page():
    from mccain_capital.services import auth as auth_svc

    return auth_svc.logout_page()


def healthz():
    return _legacy().healthz()


def favicon():
    return _legacy().favicon()


def dashboard():
    from mccain_capital.repositories import analytics as analytics_repo
    from mccain_capital.repositories import trades as trades_repo

    anchor = trades_repo.latest_trade_day() or app_runtime.now_et().date()
    year = int(request.args.get("y") or anchor.year)
    month = max(1, min(12, int(request.args.get("m") or anchor.month)))

    heat = trades_repo.month_heatmap(year, month)
    prev_y, prev_m = (year, month - 1)
    next_y, next_m = (year, month + 1)
    if prev_m == 0:
        prev_m = 12
        prev_y -= 1
    if next_m == 13:
        next_m = 1
        next_y += 1

    month_name = date(year, month, 1).strftime("%B %Y")
    overall_balance = trades_repo.latest_balance_overall()
    balance_integrity = trades_repo.balance_integrity_snapshot()
    sync_status = get_system_status()
    data_trust = dashboard_data_trust(sync_status, balance_integrity)
    admin_recompute_allowed = auth_enabled() and is_authenticated()

    week_anchor = (
        anchor.isoformat()
        if (year == anchor.year and month == anchor.month)
        else date(year, month, 1).isoformat()
    )
    this_week_total = trades_repo.week_total_net(week_anchor)
    mtd_net = trades_repo.month_total_net(year, month)
    ytd_net = trades_repo.ytd_total_net(year)
    mtd_trades = trades_repo.month_trade_count(year, month)
    ytd_trades = trades_repo.ytd_trade_count(year)
    proj = trades_repo.projections_from_daily(
        trades_repo.last_n_trading_day_totals(20),
        overall_balance,
    )

    ytd_trades_list = [
        dict(r)
        for r in trades_repo.fetch_trades_range(
            date(year, 1, 1).isoformat(), date(year + 1, 1, 1).isoformat()
        )
    ]
    ytd_stats = trades_repo.trade_day_stats(ytd_trades_list)
    ytd_cons = trades_repo.calc_consistency(ytd_trades_list)
    ytd_wins = int(ytd_stats.get("wins", 0) or 0)
    ytd_losses = int(ytd_stats.get("losses", 0) or 0)
    ytd_win_rate = float(ytd_stats.get("win_rate", 0.0))
    today_rows = [dict(r) for r in trades_repo.fetch_trades(d=app_runtime.today_iso(), q="")]
    today_stats = trades_repo.trade_day_stats(today_rows)
    today_net = float(today_stats.get("total", 0.0))
    today_win_rate = float(today_stats.get("win_rate", 0.0))
    today_wins = int(today_stats.get("wins", 0) or 0)
    today_losses = int(today_stats.get("losses", 0) or 0)
    today_count = len(today_rows)
    capital_pulse = max(8.0, min(100.0, 50.0 + ((mtd_net / 3000.0) * 50.0)))
    discipline_pulse = max(8.0, min(100.0, today_win_rate if today_count else 18.0))
    discipline_label = (
        "Locked in"
        if today_win_rate >= 60 and today_net >= 0
        else "Stabilize process" if today_count else "No session logged"
    )
    recent_start = max(date(year, month, 1), anchor - timedelta(days=45))
    recent_rows = analytics_repo.fetch_analytics_rows(recent_start.isoformat(), anchor.isoformat())
    recent_rule_breaks = analytics_repo.rule_break_counts(recent_rows)
    recent_setup_rows = [
        row
        for row in analytics_repo.group_table(recent_rows, "setup_tag")
        if str(row.get("k") or "").strip() and str(row.get("k") or "").strip() != "Unlabeled"
    ]
    top_rule_break = recent_rule_breaks[0] if recent_rule_breaks else None
    top_setup = recent_setup_rows[0] if recent_setup_rows else None
    payout_focus = (
        f"5-day pace projects {app_runtime.money(proj['p5']['est_balance'])}."
        if proj.get("p5")
        else "Need more daily history for payout pace."
    )
    payout_focus_detail = (
        f"10-day estimate {app_runtime.money(proj['p10']['est_balance'])} · Avg day {app_runtime.money(proj['avg'])}."
        if proj.get("p10")
        else "Upload more trades to stabilize projections."
    )
    risk_posture_title = (
        "Attack window"
        if today_count and today_net > 0 and (ytd_cons.get("ratio") is None or ytd_cons.get("ratio", 1.0) <= 0.30)
        else "Protect capital"
        if today_count and today_net < 0
        else "Wait for clean signal"
    )
    risk_posture_detail = (
        f"Today {today_wins}W/{today_losses}L · Consistency "
        + (
            f"{float(ytd_cons['ratio']) * 100.0:.1f}%"
            if ytd_cons.get("ratio") is not None
            else "—"
        )
        + "."
    )
    pattern_watch = (
        f"Most common breach: {str(top_rule_break['tag']).replace('-', ' ').title()} ({top_rule_break['count']})."
        if top_rule_break
        else "No recurring rule-break tag is dominating recent sessions."
    )
    setup_focus = (
        f"Lead setup {top_setup['k']} · {top_setup['count']} trades · {app_runtime.money(top_setup['net'])}."
        if top_setup
        else "No dominant labeled setup yet."
    )

    content = render_template(
        "dashboard.html",
        heat=heat,
        prev_y=prev_y,
        prev_m=prev_m,
        next_y=next_y,
        next_m=next_m,
        month_name=month_name,
        overall_balance=overall_balance,
        balance_integrity=balance_integrity,
        sync_status=sync_status,
        data_trust=data_trust,
        admin_recompute_allowed=admin_recompute_allowed,
        this_week_total=this_week_total,
        mtd_net=mtd_net,
        ytd_net=ytd_net,
        mtd_trades=mtd_trades,
        ytd_trades=ytd_trades,
        ytd_wins=ytd_wins,
        ytd_losses=ytd_losses,
        ytd_win_rate=ytd_win_rate,
        ytd_cons=ytd_cons,
        cons_threshold=0.30,
        today_net=today_net,
        today_win_rate=today_win_rate,
        today_wins=today_wins,
        today_losses=today_losses,
        today_count=today_count,
        capital_pulse=capital_pulse,
        discipline_pulse=discipline_pulse,
        discipline_label=discipline_label,
        payout_focus=payout_focus,
        payout_focus_detail=payout_focus_detail,
        risk_posture_title=risk_posture_title,
        risk_posture_detail=risk_posture_detail,
        pattern_watch=pattern_watch,
        setup_focus=setup_focus,
        proj=proj,
        money=app_runtime.money,
        money_compact=_money_compact,
    )
    return render_page(content, active="dashboard")


def command_calendar_page():
    from mccain_capital.repositories import analytics as analytics_repo
    from mccain_capital.repositories import goals as goals_repo
    from mccain_capital.repositories import journal as journal_repo
    from mccain_capital.repositories import trades as trades_repo

    anchor = trades_repo.latest_trade_day() or app_runtime.now_et().date()
    year = int(request.args.get("y") or anchor.year)
    month = max(1, min(12, int(request.args.get("m") or anchor.month)))
    first = date(year, month, 1)
    next_month = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    month_end = next_month - timedelta(days=1)

    heat = trades_repo.month_heatmap(year, month)
    journal_rows = journal_repo.fetch_entry_day_rollups(first.isoformat(), month_end.isoformat())
    goal_rows = goals_repo.fetch_daily_goals(first.isoformat(), month_end.isoformat())
    analytics_rows = analytics_repo.fetch_analytics_rows(first.isoformat(), month_end.isoformat())

    journal_map = {str(row["entry_date"]): row for row in journal_rows}
    goal_map = {str(row["track_date"]): dict(row) for row in goal_rows}
    analytics_map = _analytics_rows_by_day(analytics_rows)

    activity_days = 0
    journal_days = 0
    project_days = 0
    project_signals = 0
    debrief_count = 0
    state_rollup: Dict[str, int] = {}
    mistake_rollup: Dict[str, int] = {}

    for week in heat["weeks"]:
        for day in week["days"]:
            iso = str(day.get("iso") or "")
            if not iso:
                continue
            journal = journal_map.get(iso) or {}
            goals = goal_map.get(iso) or {}
            day_analytics = analytics_map.get(iso) or []
            goal_signal_count = _goal_signal_count(goals)
            if day.get("has_trades") or journal or goal_signal_count:
                activity_days += 1
            if journal:
                journal_days += 1
                debrief_count += int(journal.get("entry_count") or 0)
            if goal_signal_count:
                project_days += 1
                project_signals += goal_signal_count
            day["journal"] = journal
            day["goals"] = goals
            day["goal_signal_count"] = goal_signal_count
            day["has_projects"] = goal_signal_count > 0
            day["activity_level"] = sum(
                [
                    1 if day.get("has_trades") else 0,
                    1 if journal else 0,
                    1 if goal_signal_count else 0,
                ]
            )
            day["focus_label"] = _day_focus_label(day, journal, goals)
            day["project_summary"] = _project_summary(goals)
            day["journal_summary"] = _journal_summary(journal)
            day["mistake_summary"] = _day_mistake_summary(day_analytics)
            day["day_state"] = _day_state(day, journal, goals, day_analytics)
            day["day_state_label"] = _day_state_label(day["day_state"])
            state_rollup[day["day_state"]] = int(state_rollup.get(day["day_state"], 0)) + 1
            if day["mistake_summary"]:
                mistake_rollup[day["mistake_summary"]] = int(mistake_rollup.get(day["mistake_summary"], 0)) + 1

    month_net = trades_repo.month_total_net(year, month)
    month_trade_count = trades_repo.month_trade_count(year, month)
    overall_balance = trades_repo.latest_balance_overall()
    month_name = first.strftime("%B %Y")
    prev_y, prev_m = (year, month - 1)
    next_y, next_m = (year, month + 1)
    if prev_m == 0:
        prev_m = 12
        prev_y -= 1
    if next_m == 13:
        next_m = 1
        next_y += 1

    content = render_template(
        "core/command_calendar.html",
        heat=heat,
        month_name=month_name,
        month_net=month_net,
        month_trade_count=month_trade_count,
        overall_balance=overall_balance,
        prev_y=prev_y,
        prev_m=prev_m,
        next_y=next_y,
        next_m=next_m,
        activity_days=activity_days,
        journal_days=journal_days,
        debrief_count=debrief_count,
        project_days=project_days,
        project_signals=project_signals,
        state_rollup=state_rollup,
        top_mistake=max(mistake_rollup.items(), key=lambda kv: kv[1])[0] if mistake_rollup else "",
        money=app_runtime.money,
        money_compact=_money_compact,
    )
    return render_page(content, active="calendar", title=f"{month_name} Calendar")


def dashboard_recompute_balances():
    if not auth_enabled():
        flash("Enable authentication to use admin recompute actions.", "warn")
        return redirect(url_for("dashboard"))
    if not is_authenticated():
        abort(403)

    from mccain_capital.repositories import trades as trades_repo

    starting = float(app_runtime.get_setting_float("starting_balance", 50000.0))
    trades_repo.recompute_balances(starting_balance=starting)
    try:
        from mccain_capital.services.trades import record_admin_audit

        record_admin_audit(
            "dashboard_recompute_balances",
            {"starting_balance": starting},
            actor=effective_username(),
        )
    except Exception:
        pass
    flash("Stored trade balances recomputed from canonical ledger math.", "success")
    return redirect(url_for("dashboard"))


def candle_opens_page():
    anchor = app_runtime.now_et().date()
    year = int(request.args.get("y") or anchor.year)
    month = max(1, min(12, int(request.args.get("m") or anchor.month)))
    model = _build_candle_open_calendar(year, month)
    content = render_template("core/candle_opens.html", **model)
    return render_page(
        content,
        active="candle-opens",
        title=f"{model['month_name']} Candle Opens",
    )


def analytics_page():
    from mccain_capital.services import analytics as analytics_svc

    return analytics_svc.analytics_page()


def session_replay_page():
    from mccain_capital.services import analytics as analytics_svc

    return analytics_svc.session_replay_page()


def calculator():
    context = _calculator_context(request.form if request.method == "POST" else None)

    if request.method == "POST" and request.headers.get("X-Requested-With") == "XMLHttpRequest":
        results_html = render_template(
            "calculator_results.html",
            out=context["out"],
            money=app_runtime.money,
        )
        return jsonify(
            {
                "ok": context["err"] is None,
                "err": context["err"],
                "results_html": results_html,
            }
        )

    content = render_template(
        "calculator.html",
        out=context["out"],
        err=context["err"],
        vals=context["vals"],
        money=app_runtime.money,
        current_balance=context["current_balance"],
        current_consistency=context["current_consistency"],
    )
    return render_page(content, active="calc")


def links_page():
    content = render_template("core/links.html")
    return render_page(content, active="links")


def export_json():
    return _legacy().export_json()


def backup_data():
    stamp = app_runtime.now_et().strftime("%Y%m%d_%H%M%S")
    fd, out_path = tempfile.mkstemp(prefix="mccain_backup_", suffix=".zip")
    os.close(fd)
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if os.path.exists(app_runtime.DB_PATH):
            zf.write(str(app_runtime.DB_PATH), arcname="data/journal.db")

        if os.path.isdir(app_runtime.UPLOAD_DIR):
            for root, _, files in os.walk(str(app_runtime.UPLOAD_DIR)):
                for name in files:
                    full = os.path.join(root, name)
                    rel = os.path.relpath(full, str(app_runtime.UPLOAD_DIR))
                    zf.write(full, arcname=f"data/uploads/{rel}")

        meta = {
            "exported_at": app_runtime.now_iso(),
            "db_path": str(app_runtime.DB_PATH),
            "upload_dir": str(app_runtime.UPLOAD_DIR),
            "app": "mccain-capital",
        }
        zf.writestr("data/meta.json", json.dumps(meta, ensure_ascii=False, indent=2))

    try:
        from mccain_capital.services.trades import record_admin_audit

        record_admin_audit(
            "manual_backup_downloaded",
            {"file": f"mccain_capital_backup_{stamp}.zip"},
            actor=(
                _legacy()._effective_username()
                if _legacy().auth_enabled()
                else _legacy().APP_USERNAME
            ),
        )
    except Exception:
        pass
    return send_file(
        out_path,
        as_attachment=True,
        download_name=f"mccain_capital_backup_{stamp}.zip",
        mimetype="application/zip",
    )


def restore_data():
    if request.method == "GET":
        content = render_template(
            "core/restore_backup.html",
            db_path=str(app_runtime.DB_PATH),
            upload_dir=str(app_runtime.UPLOAD_DIR),
        )
        return render_page(content, active="dashboard")

    f = request.files.get("backup_zip")
    if not f or not f.filename:
        return render_page(simple_msg("Please choose a backup zip file."), active="dashboard")

    try:
        with zipfile.ZipFile(f.stream) as zf:
            names = zf.namelist()
            if not names:
                return render_page(simple_msg("Backup zip is empty."), active="dashboard")

            allowed_prefixes = ("data/journal.db", "data/uploads/", "data/meta.json")
            for n in names:
                if n.startswith("/") or ".." in n:
                    return render_page(
                        simple_msg("Backup zip contains unsafe paths."), active="dashboard"
                    )
                if not any(n == p or n.startswith(p) for p in allowed_prefixes):
                    return render_page(
                        simple_msg("Backup zip contains unsupported files."), active="dashboard"
                    )

            db_member = "data/journal.db"
            if db_member in names:
                db_path = str(app_runtime.DB_PATH)
                os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
                db_dir = os.path.dirname(db_path) or "."
                fd, tmp_db = tempfile.mkstemp(prefix="restore_db_", suffix=".tmp", dir=db_dir)
                os.close(fd)
                try:
                    with zf.open(db_member) as src, open(tmp_db, "wb") as dst:
                        shutil.copyfileobj(src, dst, length=1024 * 1024)
                    os.replace(tmp_db, db_path)
                finally:
                    if os.path.exists(tmp_db):
                        os.unlink(tmp_db)

            upload_dir = str(app_runtime.UPLOAD_DIR)
            os.makedirs(upload_dir, exist_ok=True)
            for n in names:
                if not n.startswith("data/uploads/") or n.endswith("/"):
                    continue
                rel = n[len("data/uploads/") :]
                out_path = os.path.join(upload_dir, rel)
                out_dir = os.path.dirname(out_path)
                if out_dir:
                    os.makedirs(out_dir, exist_ok=True)
                with zf.open(n) as src, open(out_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
    except zipfile.BadZipFile:
        return render_page(simple_msg("Invalid zip file."), active="dashboard")
    except Exception as e:
        return render_page(simple_msg(f"Restore failed: {e}"), active="dashboard")

    try:
        from mccain_capital.services.trades import record_admin_audit

        record_admin_audit(
            "manual_backup_restored",
            {"source_filename": f.filename if f else ""},
            actor=(
                _legacy()._effective_username()
                if _legacy().auth_enabled()
                else _legacy().APP_USERNAME
            ),
        )
    except Exception:
        pass
    return render_page(simple_msg("Backup restore completed."), active="dashboard")


def strat_page():
    from mccain_capital.services import strat as strat_svc

    return strat_svc.strat_page()


def _money_compact(val: Any) -> str:
    if val is None or val == "":
        return ""
    try:
        n = float(val)
    except Exception:
        return ""
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 10000:
        return f"{sign}${n / 1000:.0f}k"
    if n >= 1000:
        return f"{sign}${n / 1000:.1f}k"
    if n >= 100:
        return f"{sign}${n:.0f}"
    return f"{sign}${n:.2f}"


def _calc_stop_takeprofit(entry: float, stop_pct: float, target_pct: float) -> Tuple[float, float]:
    stop_price = round(entry * (1 - stop_pct / 100.0), 2)
    tp_price = round(entry * (1 + target_pct / 100.0), 2)
    return stop_price, tp_price


def _calc_risk_reward(
    entry: float, contracts: int, stop_price: float, tp_price: float, fee_per_contract: float
) -> Dict[str, float]:
    fees = round(contracts * fee_per_contract, 2)
    risk_gross = (entry - stop_price) * MULTIPLIER * contracts
    reward_gross = (tp_price - entry) * MULTIPLIER * contracts
    risk_net = round(risk_gross + fees, 2)
    reward_net = round(reward_gross - fees, 2)
    rr = round((reward_net / risk_net), 2) if risk_net > 0 else 0.0
    return {"fees": fees, "risk_net": risk_net, "reward_net": reward_net, "rr": rr}


def _calculator_context(form_data: Optional[Any] = None) -> Dict[str, Any]:
    from mccain_capital.repositories import trades as trades_repo

    current_balance = trades_repo.latest_balance_overall() or 50000.0
    base_trades = trades_repo.fetch_trades(d="", q="")
    current_consistency = trades_repo.calc_consistency(base_trades)
    vals = {
        "entry": "",
        "contracts": "1",
        "stop_pct": str(DEFAULT_STOP_PCT),
        "target_pct": str(DEFAULT_TARGET_PCT),
        "fee_per_contract": str(DEFAULT_FEE_PER_CONTRACT),
    }
    out = None
    err = None
    if form_data is not None:
        vals["entry"] = (form_data.get("entry") or "").strip()
        vals["contracts"] = (form_data.get("contracts") or "1").strip()
        vals["stop_pct"] = (form_data.get("stop_pct") or str(DEFAULT_STOP_PCT)).strip()
        vals["target_pct"] = (form_data.get("target_pct") or str(DEFAULT_TARGET_PCT)).strip()
        vals["fee_per_contract"] = (
            form_data.get("fee_per_contract") or str(DEFAULT_FEE_PER_CONTRACT)
        ).strip()

        entry = app_runtime.parse_float(vals["entry"])
        contracts = app_runtime.parse_int(vals["contracts"]) or 1
        stop_pct = app_runtime.parse_float(vals["stop_pct"]) or DEFAULT_STOP_PCT
        target_pct = app_runtime.parse_float(vals["target_pct"]) or DEFAULT_TARGET_PCT
        fee = app_runtime.parse_float(vals["fee_per_contract"]) or DEFAULT_FEE_PER_CONTRACT

        if not entry or entry <= 0:
            err = "Entry premium must be > 0."
        elif contracts <= 0:
            err = "Contracts must be >= 1."
        else:
            stop_price, tp_price = _calc_stop_takeprofit(entry, stop_pct, target_pct)
            rr = _calc_risk_reward(entry, contracts, stop_price, tp_price, fee)
            ladder = []
            for p in range(10, 101, 10):
                ladder_tp = round(entry * (1 + p / 100.0), 2)
                ladder_rr = _calc_risk_reward(entry, contracts, stop_price, ladder_tp, fee)
                ladder.append({"pct": p, "tp": ladder_tp, "net": ladder_rr["reward_net"]})

            out = {
                "entry": entry,
                "contracts": contracts,
                "total_spend": round(entry * MULTIPLIER * contracts + (fee * contracts), 2),
                "stop_pct": stop_pct,
                "target_pct": target_pct,
                "fee": fee,
                "stop_price": stop_price,
                "tp_price": tp_price,
                "current_balance": float(current_balance),
                "balance_if_stop": round(float(current_balance) - float(rr["risk_net"]), 2),
                "balance_if_target": round(float(current_balance) + float(rr["reward_net"]), 2),
                "consistency_current": current_consistency,
                "consistency_if_stop": trades_repo.calc_consistency(
                    list(base_trades) + [{"net_pl": -float(rr["risk_net"])}]
                ),
                "consistency_if_target": trades_repo.calc_consistency(
                    list(base_trades) + [{"net_pl": float(rr["reward_net"])}]
                ),
                "risk_pct_balance": round((float(rr["risk_net"]) / float(current_balance) * 100.0), 2)
                if current_balance
                else 0.0,
                "reward_pct_balance": round((float(rr["reward_net"]) / float(current_balance) * 100.0), 2)
                if current_balance
                else 0.0,
                "profit_pct": round((float(rr["reward_net"]) / float(entry * MULTIPLIER * contracts + (fee * contracts)) * 100.0), 1)
                if (entry * MULTIPLIER * contracts + (fee * contracts))
                else 0.0,
                "plan_state": (
                    "Sharp"
                    if rr["rr"] >= 2.0 and float(rr["risk_net"]) <= float(current_balance) * 0.01
                    else "Manageable"
                    if rr["rr"] >= 1.5 and float(rr["risk_net"]) <= float(current_balance) * 0.02
                    else "Too hot"
                ),
                **rr,
                "ladder": ladder,
            }

    return {
        "out": out,
        "err": err,
        "vals": vals,
        "current_balance": current_balance,
        "current_consistency": current_consistency,
    }


def _build_candle_open_calendar(year: int, month: int) -> Dict[str, Any]:
    cal = Calendar(firstweekday=6)
    session_index = _trading_day_index_map(year)
    week_index, week_open_dates = _trading_week_index_map(year)

    prev_y, prev_m = (year, month - 1)
    next_y, next_m = (year, month + 1)
    if prev_m == 0:
        prev_m = 12
        prev_y -= 1
    if next_m == 13:
        next_m = 1
        next_y += 1

    weeks = []
    total_signals = 0
    trading_days = 0
    for week in cal.monthdatescalendar(year, month):
        cells = []
        for day in week:
            in_month = day.month == month
            holiday_name = _market_holiday_name(day)
            is_weekend = day.weekday() >= 5
            is_holiday = bool(holiday_name)
            is_trading = in_month and not is_weekend and not is_holiday
            day_labels = []
            week_labels = []
            if is_trading:
                trading_days += 1
                idx = session_index.get(day)
                if idx is not None:
                    day_labels = [f"{span}D" for span in DAY_OPEN_INTERVALS if idx % span == 1]
                if day in week_open_dates:
                    widx = week_index.get(day)
                    if widx is not None:
                        week_labels = [f"{span}W" for span in WEEK_OPEN_INTERVALS if widx % span == 1]
                total_signals += len(day_labels) + len(week_labels)
            cells.append(
                {
                    "day": day.day,
                    "iso": day.isoformat(),
                    "weekday_label": day.strftime("%a"),
                    "in_month": in_month,
                    "is_weekend": is_weekend,
                    "is_holiday": is_holiday,
                    "is_trading": is_trading,
                    "holiday_name": holiday_name,
                    "day_labels": day_labels,
                    "week_labels": week_labels,
                    "labels": day_labels + week_labels,
                }
            )
        weeks.append(cells)

    month_name = date(year, month, 1).strftime("%B %Y")
    return {
        "month_name": month_name,
        "year": year,
        "month": month,
        "weeks": weeks,
        "prev_y": prev_y,
        "prev_m": prev_m,
        "next_y": next_y,
        "next_m": next_m,
        "trading_days": trading_days,
        "signal_count": total_signals,
        "day_legend": ", ".join(f"{span}D" for span in DAY_OPEN_INTERVALS),
        "week_legend": ", ".join(f"{span}W" for span in WEEK_OPEN_INTERVALS),
    }


def _goal_signal_count(goal_row: Dict[str, Any]) -> int:
    if not goal_row:
        return 0
    count = 0
    count += 1 if float(goal_row.get("debt_paid") or 0.0) > 0 else 0
    count += 1 if int(goal_row.get("upwork_proposals") or 0) > 0 else 0
    count += 1 if int(goal_row.get("upwork_interviews") or 0) > 0 else 0
    count += 1 if float(goal_row.get("upwork_hours") or 0.0) > 0 else 0
    count += 1 if float(goal_row.get("upwork_earnings") or 0.0) > 0 else 0
    count += 1 if float(goal_row.get("other_income") or 0.0) > 0 else 0
    count += 1 if str(goal_row.get("notes") or "").strip() else 0
    return count


def _project_summary(goal_row: Dict[str, Any]) -> List[str]:
    if not goal_row:
        return []
    items: List[str] = []
    proposals = int(goal_row.get("upwork_proposals") or 0)
    interviews = int(goal_row.get("upwork_interviews") or 0)
    hours = float(goal_row.get("upwork_hours") or 0.0)
    debt_paid = float(goal_row.get("debt_paid") or 0.0)
    other_income = float(goal_row.get("other_income") or 0.0)
    if proposals:
        items.append(f"{proposals} proposals")
    if interviews:
        items.append(f"{interviews} interviews")
    if hours:
        items.append(f"{hours:.1f}h outside work")
    if debt_paid:
        items.append(f"Debt {app_runtime.money(debt_paid)}")
    if other_income:
        items.append(f"Other {app_runtime.money(other_income)}")
    if str(goal_row.get("notes") or "").strip():
        items.append("project note")
    return items[:3]


def _journal_summary(journal_row: Dict[str, Any]) -> List[str]:
    if not journal_row:
        return []
    items: List[str] = []
    entry_count = int(journal_row.get("entry_count") or 0)
    if entry_count:
        items.append(f"{entry_count} debrief{'s' if entry_count != 1 else ''}")
    moods = list(journal_row.get("moods") or [])
    if moods:
        items.append(moods[0].title())
    setups = list(journal_row.get("setups") or [])
    if setups:
        items.append(setups[0])
    return items[:3]


def _day_focus_label(
    day_row: Dict[str, Any], journal_row: Dict[str, Any], goal_row: Dict[str, Any]
) -> str:
    if day_row.get("has_trades") and journal_row and goal_row:
        return "Full stack day"
    if day_row.get("has_trades") and journal_row:
        return "Traded and debriefed"
    if day_row.get("has_trades") and goal_row:
        return "Traded and built"
    if journal_row and goal_row:
        return "Review and project push"
    if day_row.get("has_trades"):
        return "Trading session"
    if journal_row:
        return "Debrief day"
    if goal_row:
        return "Project day"
    return "No signal"


def _analytics_rows_by_day(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        iso = str(row.get("trade_date") or "")
        if not iso:
            continue
        out.setdefault(iso, []).append(row)
    return out


def _day_mistake_summary(rows: List[Dict[str, Any]]) -> str:
    counts: Dict[str, int] = {}
    for row in rows:
        tags = str(row.get("rule_break_tags") or "")
        for tag in [part.strip().lower() for part in tags.split(",") if part.strip()]:
            counts[tag] = int(counts.get(tag, 0)) + 1
    if not counts:
        return ""
    return max(counts.items(), key=lambda kv: kv[1])[0][0:36]


def _day_state(day_row: Dict[str, Any], journal_row: Dict[str, Any], goal_row: Dict[str, Any], analytics_rows: List[Dict[str, Any]]) -> str:
    has_trades = bool(day_row.get("has_trades"))
    net = float(day_row.get("net") or 0.0)
    has_journal = bool(journal_row)
    has_projects = bool(goal_row)
    mistake = _day_mistake_summary(analytics_rows)
    if has_trades and net > 0 and has_journal and not mistake:
        return "clean_win"
    if has_trades and net > 0:
        return "sloppy_win" if mistake else "green_day"
    if has_trades and net < 0:
        return "impulsive_loss" if mistake else "controlled_loss"
    if has_trades and net == 0:
        return "flat_session"
    if has_journal and has_projects:
        return "review_build"
    if has_journal:
        return "debrief_day"
    if has_projects:
        return "project_day"
    return "quiet_day"


def _day_state_label(value: str) -> str:
    labels = {
        "clean_win": "Clean win",
        "sloppy_win": "Review win",
        "green_day": "Green day",
        "controlled_loss": "Controlled loss",
        "impulsive_loss": "Impulsive loss",
        "flat_session": "Flat session",
        "review_build": "Review + build",
        "debrief_day": "Debrief",
        "project_day": "Project",
        "quiet_day": "",
    }
    return labels.get(value, "Day state")


def _trading_day_index_map(year: int) -> Dict[date, int]:
    idx = 0
    out: Dict[date, int] = {}
    cursor = date(year, 1, 1)
    end = date(year, 12, 31)
    while cursor <= end:
        if _is_market_session(cursor):
            idx += 1
            out[cursor] = idx
        cursor += timedelta(days=1)
    return out


def _trading_week_index_map(year: int) -> Tuple[Dict[date, int], set[date]]:
    idx = 0
    out: Dict[date, int] = {}
    week_open_dates: set[date] = set()
    current_week_key = None
    cursor = date(year, 1, 1)
    end = date(year, 12, 31)
    while cursor <= end:
        if _is_market_session(cursor):
            week_key = cursor - timedelta(days=cursor.weekday())
            if week_key != current_week_key:
                current_week_key = week_key
                idx += 1
                week_open_dates.add(cursor)
            out[cursor] = idx
        cursor += timedelta(days=1)
    return out, week_open_dates


def _is_market_session(day: date) -> bool:
    return day.weekday() < 5 and not _market_holiday_name(day)


def _market_holiday_name(day: date) -> str:
    return _market_holidays(day.year).get(day, "")


def _market_holidays(year: int) -> Dict[date, str]:
    easter = _easter_sunday(year)
    holidays = {
        _observed_fixed_holiday(year, 1, 1): "New Years Day",
        _nth_weekday_of_month(year, 1, 0, 3): "Martin Luther King Jr. Day",
        _nth_weekday_of_month(year, 2, 0, 3): "Presidents Day",
        easter - timedelta(days=2): "Good Friday",
        _last_weekday_of_month(year, 5, 0): "Memorial Day",
        _observed_fixed_holiday(year, 6, 19): "Juneteenth",
        _observed_fixed_holiday(year, 7, 4): "Independence Day",
        _nth_weekday_of_month(year, 9, 0, 1): "Labor Day",
        _nth_weekday_of_month(year, 11, 3, 4): "Thanksgiving",
        _observed_fixed_holiday(year, 12, 25): "Christmas Day",
    }
    return holidays


def _observed_fixed_holiday(year: int, month: int, day_num: int) -> date:
    holiday = date(year, month, day_num)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return first + timedelta(days=delta + ((n - 1) * 7))


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if month == 12:
        cursor = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        cursor = date(year, month + 1, 1) - timedelta(days=1)
    while cursor.weekday() != weekday:
        cursor -= timedelta(days=1)
    return cursor


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    weekday_offset = (32 + (2 * e) + (2 * i) - h - k) % 7
    m = (a + (11 * h) + (22 * weekday_offset)) // 451
    month = (h + weekday_offset - (7 * m) + 114) // 31
    day_num = ((h + weekday_offset - (7 * m) + 114) % 31) + 1
    return date(year, month, day_num)
