"""Trades mutation and bulk-action handlers."""

from __future__ import annotations

from datetime import datetime
import sqlite3
from typing import Any, List

from mccain_capital.services import trades as legacy


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


def trades_duplicate(trade_id: int):
    src = legacy.get_trade(trade_id)
    if not src:
        legacy.abort(404)

    net_pl = float(src["net_pl"] or 0.0)
    new_balance = (legacy.latest_balance_overall() or 50000.0) + net_pl

    with legacy.db() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent,
                stop_pct, target_pct, stop_price, take_profit,
                risk, comm, gross_pl, net_pl, result_pct, balance,
                raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
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
            ),
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


def trades_clear():
    legacy.repo.clear_trades()
    return legacy.redirect(legacy.url_for("trades_page"))
