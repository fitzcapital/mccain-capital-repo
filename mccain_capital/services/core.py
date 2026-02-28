"""Core domain service gateway.

Core routes still rely on legacy implementations in ``app_core``. This module
keeps that dependency localized behind explicit delegator functions.
"""

from __future__ import annotations

from datetime import date
import json
import os
import shutil
import tempfile
import zipfile
from typing import Any, Dict, Optional, Tuple

from flask import abort, flash, jsonify, redirect, render_template, request, send_file, url_for

from mccain_capital.auth import auth_enabled, effective_username, is_authenticated
from mccain_capital import runtime as app_runtime
from mccain_capital.services.ui import APP_TITLE, get_system_status, render_page, simple_msg
from mccain_capital.services.viewmodels import dashboard_data_trust

MULTIPLIER = 100
DEFAULT_STOP_PCT = 20.0
DEFAULT_TARGET_PCT = 30.0
DEFAULT_FEE_PER_CONTRACT = 0.70


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
    ytd_cons = trades_repo.calc_consistency(ytd_trades_list)
    today_rows = [dict(r) for r in trades_repo.fetch_trades(d=app_runtime.today_iso(), q="")]
    today_stats = trades_repo.trade_day_stats(today_rows)
    today_net = float(today_stats.get("total", 0.0))
    today_win_rate = float(today_stats.get("win_rate", 0.0))
    today_count = len(today_rows)
    capital_pulse = max(8.0, min(100.0, 50.0 + ((mtd_net / 3000.0) * 50.0)))
    discipline_pulse = max(8.0, min(100.0, today_win_rate if today_count else 18.0))
    discipline_label = (
        "Locked in"
        if today_win_rate >= 60 and today_net >= 0
        else "Stabilize process" if today_count else "No session logged"
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
        ytd_cons=ytd_cons,
        cons_threshold=0.30,
        today_net=today_net,
        today_win_rate=today_win_rate,
        today_count=today_count,
        capital_pulse=capital_pulse,
        discipline_pulse=discipline_pulse,
        discipline_label=discipline_label,
        proj=proj,
        money=app_runtime.money,
        money_compact=_money_compact,
    )
    return render_page(content, active="dashboard")


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
