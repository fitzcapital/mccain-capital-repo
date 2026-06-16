"""Trades mutation and bulk-action handlers."""

from __future__ import annotations

from datetime import datetime
import sqlite3
from typing import Any, List

from mccain_capital.repositories import trades as trades_repo
from mccain_capital.services import trades as legacy

QUICK_SETUP_CHOICES = {"Sweep", "Reversal", "Continuation", "Chop", "Other"}


def _parse_ids_from_request() -> List[int]:
    ids: Any = None
    if legacy.request.is_json:
        payload = legacy.request.get_json(silent=True) or {}
        ids = payload.get("ids")
    if ids is None:
        ids = legacy.request.form.getlist("ids") or legacy.request.form.get("ids")

    if isinstance(ids, str):
        raw = [x.strip() for x in ids.split(",") if x.strip()]
    elif isinstance(ids, list):
        raw = ids
    else:
        raw = []

    clean: List[int] = []
    for x in raw:
        try:
            clean.append(int(x))
        except Exception:
            continue

    seen = set()
    out: List[int] = []
    for i in clean:
        if i not in seen:
            out.append(i)
            seen.add(i)
    return out


def _trades_table_columns(conn: sqlite3.Connection) -> List[str]:
    return [r["name"] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]


def _parse_setup_from_request() -> str:
    payload = legacy.request.get_json(silent=True) or {} if legacy.request.is_json else {}
    raw = payload.get("setup") if legacy.request.is_json else legacy.request.form.get("setup")
    setup = str(raw or "").strip()
    if setup not in QUICK_SETUP_CHOICES:
        return ""
    return setup


def _preserve_review_with_setup(review: dict[str, Any], setup: str) -> dict[str, Any]:
    return {
        "strategy_id": None,
        "strategy_label": setup,
        "setup_tag": setup,
        "session_tag": review.get("session_tag", ""),
        "checklist_score": review.get("checklist_score"),
        "rule_break_tags": review.get("rule_break_tags", ""),
        "review_note": review.get("review_note", ""),
        "thesis_note": review.get("thesis_note", ""),
        "execution_grade": review.get("execution_grade"),
        "risk_grade": review.get("risk_grade"),
        "plan_grade": review.get("plan_grade"),
        "mistake_tags": review.get("mistake_tags", ""),
        "planned_risk_dollars": review.get("planned_risk_dollars"),
        "size_rule_note": review.get("size_rule_note", ""),
        "entry_quality_note": review.get("entry_quality_note", ""),
        "exit_quality_note": review.get("exit_quality_note", ""),
        "improvement_note": review.get("improvement_note", ""),
    }


def trades_duplicate(trade_id: int):
    src_row = legacy.get_trade(trade_id)
    if not src_row:
        legacy.abort(404)
    src = dict(src_row)

    net_pl = float(src["net_pl"] or 0.0)
    account_id = src.get("account_id")
    try:
        scoped_account_id = int(account_id) if account_id not in (None, "") else None
    except (TypeError, ValueError):
        scoped_account_id = None
    starting_balance = None
    if scoped_account_id:
        account = trades_repo.get_account(scoped_account_id)
        if account:
            starting_balance = float(account.get("starting_balance") or 50000.0)
    new_balance = (
        trades_repo.latest_balance_overall(
            account_id=scoped_account_id,
            starting_balance=starting_balance,
        )
        or 50000.0
    ) + net_pl

    with legacy.db() as conn:
        columns = _trades_table_columns(conn)
        insert_columns = [
            "trade_date",
            "entry_time",
            "exit_time",
            "ticker",
            "opt_type",
            "strike",
            "entry_price",
            "exit_price",
            "contracts",
            "total_spent",
            "stop_pct",
            "target_pct",
            "stop_price",
            "take_profit",
            "risk",
            "comm",
            "gross_pl",
            "net_pl",
            "result_pct",
            "balance",
            "raw_line",
            "created_at",
            "trade_source",
        ]
        values = [
            src["trade_date"],
            src["entry_time"] or "",
            src["exit_time"] or "",
            src["ticker"] or "",
            src["opt_type"] or "",
            src["strike"],
            src["entry_price"],
            src["exit_price"],
            src["contracts"],
            src["total_spent"],
            src["stop_pct"],
            src["target_pct"],
            src["stop_price"],
            src["take_profit"],
            src["risk"],
            src["comm"],
            src["gross_pl"],
            src["net_pl"],
            src["result_pct"],
            new_balance,
            f"DUPLICATE OF #{trade_id}",
            legacy.now_iso(),
            str(src.get("trade_source") or "Manual Entry"),
        ]
        if "account_id" in columns:
            insert_columns.append("account_id")
            values.append(scoped_account_id)
        qmarks = ",".join(["?"] * len(insert_columns))
        conn.execute(
            f"INSERT INTO trades ({','.join(insert_columns)}) VALUES ({qmarks})",
            values,
        )

    d = legacy.request.args.get("d", "") or (src["trade_date"] or "")
    q = legacy.request.args.get("q", "")
    return legacy.redirect(legacy.url_for("trades_page", d=d, q=q))


def trades_delete(trade_id: int):
    with legacy.db() as conn:
        conn.execute("DELETE FROM trades WHERE id = ?", (trade_id,))
    d = legacy.request.args.get("d", "")
    q = legacy.request.args.get("q", "")
    return legacy.redirect(legacy.url_for("trades_page", d=d, q=q))


def trades_delete_many():
    ids = _parse_ids_from_request()
    if not ids:
        if legacy.request.is_json:
            return legacy.jsonify({"ok": True, "deleted": 0})
        legacy.flash("No trades selected.", "warning")
        return legacy.redirect(
            legacy.url_for(
                "trades_page",
                d=legacy.request.args.get("d", ""),
                q=legacy.request.args.get("q", ""),
            )
        )

    placeholders = ",".join(["?"] * len(ids))
    with legacy.db() as conn:
        cur = conn.execute(f"DELETE FROM trades WHERE id IN ({placeholders})", ids)
        deleted = cur.rowcount if cur.rowcount is not None else 0

    if legacy.request.is_json:
        return legacy.jsonify({"ok": True, "deleted": int(deleted)})
    legacy.flash(f"Deleted {deleted} trade(s).", "success")
    return legacy.redirect(
        legacy.url_for(
            "trades_page",
            d=legacy.request.args.get("d", ""),
            q=legacy.request.args.get("q", ""),
        )
    )


def trades_copy_many():
    ids = _parse_ids_from_request()
    target_date = None
    if legacy.request.is_json:
        payload = legacy.request.get_json(silent=True) or {}
        target_date = payload.get("target_date")
    if not target_date:
        target_date = legacy.request.form.get("target_date")

    if not ids:
        if legacy.request.is_json:
            return legacy.jsonify({"ok": True, "copied": 0})
        legacy.flash("No trades selected.", "warning")
        return legacy.redirect(
            legacy.url_for(
                "trades_page",
                d=legacy.request.args.get("d", ""),
                q=legacy.request.args.get("q", ""),
            )
        )

    try:
        datetime.strptime(str(target_date), "%Y-%m-%d")
    except Exception:
        if legacy.request.is_json:
            return (
                legacy.jsonify({"ok": False, "error": "Invalid target_date. Use YYYY-MM-DD."}),
                400,
            )
        legacy.flash("Invalid target date (use YYYY-MM-DD).", "danger")
        return legacy.redirect(
            legacy.url_for(
                "trades_page",
                d=legacy.request.args.get("d", ""),
                q=legacy.request.args.get("q", ""),
            )
        )

    with legacy.db() as conn:
        cols = _trades_table_columns(conn)
        insert_cols = [c for c in cols if c != "id"]
        select_cols = ",".join([f"{c}" for c in insert_cols])
        placeholders = ",".join(["?"] * len(ids))
        rows = conn.execute(
            f"SELECT {select_cols} FROM trades WHERE id IN ({placeholders}) ORDER BY trade_date, id",
            ids,
        ).fetchall()

        copied = 0
        if rows:
            now_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            qmarks = ",".join(["?"] * len(insert_cols))
            insert_sql = f"INSERT INTO trades ({','.join(insert_cols)}) VALUES ({qmarks})"
            for r in rows:
                data = dict(r)
                data["trade_date"] = str(target_date)
                if "created_at" in data:
                    data["created_at"] = now_iso
                if "balance" in data:
                    data["balance"] = None
                values = [data.get(c) for c in insert_cols]
                conn.execute(insert_sql, values)
                copied += 1

    if legacy.request.is_json:
        return legacy.jsonify({"ok": True, "copied": copied})
    legacy.flash(f"Copied {copied} trade(s) to {target_date}.", "success")
    return legacy.redirect(
        legacy.url_for("trades_page", d=str(target_date), q=legacy.request.args.get("q", ""))
    )


def trades_set_setup(trade_id: int):
    setup = _parse_setup_from_request()
    if not setup:
        return (legacy.jsonify({"ok": False, "error": "Invalid setup."}), 400)
    review = legacy.repo.get_trade_review(trade_id) or {}
    legacy.repo.upsert_trade_review(
        trade_id=trade_id,
        **_preserve_review_with_setup(review, setup),
    )
    return legacy.jsonify({"ok": True, "setup": setup})


def trades_set_setup_many():
    setup = _parse_setup_from_request()
    ids = _parse_ids_from_request()
    if not setup:
        return (legacy.jsonify({"ok": False, "error": "Invalid setup."}), 400)
    if not ids:
        return (legacy.jsonify({"ok": False, "error": "No trades selected."}), 400)
    updated = 0
    for trade_id in ids:
        review = legacy.repo.get_trade_review(trade_id) or {}
        legacy.repo.upsert_trade_review(
            trade_id=trade_id,
            **_preserve_review_with_setup(review, setup),
        )
        updated += 1
    return legacy.jsonify({"ok": True, "updated": updated, "setup": setup})


def trades_clear():
    legacy.repo.clear_trades()
    return legacy.redirect(legacy.url_for("trades_page"))
