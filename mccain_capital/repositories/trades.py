"""Trades repository functions."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from mccain_capital.runtime import (
    db,
    get_setting_float,
    get_setting_value,
    now_iso,
    set_setting_value,
)


def fetch_trades(d: str = "", q: str = ""):
    d = (d or "").strip()
    q = (q or "").strip()

    sql = "SELECT * FROM trades"
    where = []
    params: List[Any] = []

    if d:
        where.append("trade_date = ?")
        params.append(d)

    if q:
        where.append("(ticker LIKE ? OR opt_type LIKE ? OR raw_line LIKE ?)")
        like = f"%{q}%"
        params.extend([like, like, like])

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY trade_date DESC, id DESC"

    with db() as conn:
        return list(conn.execute(sql, params).fetchall())


def fetch_trades_range(start_iso: str, end_iso: str):
    with db() as conn:
        return list(
            conn.execute(
                """
            SELECT * FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            ORDER BY trade_date ASC, id ASC
            """,
                (start_iso, end_iso),
            ).fetchall()
        )


def fetch_open_positions(as_of: str = "", q: str = "") -> List[Dict[str, Any]]:
    where: List[str] = [
        "(COALESCE(exit_time, '') = '' OR exit_price IS NULL OR net_pl IS NULL)",
        "COALESCE(contracts, 0) > 0",
    ]
    params: List[Any] = []
    if as_of:
        where.append("trade_date <= ?")
        params.append(as_of)
    if q:
        like = f"%{q.strip()}%"
        where.append("(ticker LIKE ? OR opt_type LIKE ? OR raw_line LIKE ?)")
        params.extend([like, like, like])

    sql = f"""
        SELECT *
        FROM trades
        WHERE {" AND ".join(where)}
        ORDER BY trade_date DESC, id DESC
    """
    with db() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_risk_controls() -> Dict[str, Any]:
    with db() as conn:
        row = conn.execute(
            "SELECT daily_max_loss, enforce_lockout, updated_at FROM risk_controls WHERE id = 1"
        ).fetchone()
    if not row:
        return {"daily_max_loss": 0.0, "enforce_lockout": 0, "updated_at": ""}
    return {
        "daily_max_loss": float(row["daily_max_loss"] or 0.0),
        "enforce_lockout": int(row["enforce_lockout"] or 0),
        "updated_at": row["updated_at"] or "",
    }


def save_risk_controls(daily_max_loss: float, enforce_lockout: int) -> None:
    with db() as conn:
        conn.execute(
            """
            INSERT INTO risk_controls (id, daily_max_loss, enforce_lockout, updated_at)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              daily_max_loss=excluded.daily_max_loss,
              enforce_lockout=excluded.enforce_lockout,
              updated_at=excluded.updated_at
            """,
            (abs(float(daily_max_loss or 0.0)), 1 if enforce_lockout else 0, now_iso()),
        )


def get_trade_review(trade_id: int) -> Optional[Dict[str, Any]]:
    with db() as conn:
        row = conn.execute(
            """
            SELECT trade_id, strategy_id, strategy_label, setup_tag, session_tag, checklist_score, rule_break_tags, review_note
            FROM trade_reviews
            WHERE trade_id = ?
            """,
            (trade_id,),
        ).fetchone()
    return dict(row) if row else None


def _resolve_strategy_link(
    conn: Any,
    *,
    strategy_id: Optional[int],
    strategy_label: str,
    setup_tag: str,
) -> tuple[Optional[int], str]:
    clean_label = (strategy_label or setup_tag or "").strip()
    clean_id = int(strategy_id) if strategy_id not in (None, "", 0, "0") else None
    if clean_id:
        row = conn.execute("SELECT id, title FROM strategies WHERE id = ?", (clean_id,)).fetchone()
        if row:
            return int(row["id"]), str(row["title"] or "").strip()
    if not clean_label:
        return None, ""
    row = conn.execute(
        "SELECT id, title FROM strategies WHERE LOWER(TRIM(title)) = LOWER(TRIM(?)) LIMIT 1",
        (clean_label,),
    ).fetchone()
    if row:
        return int(row["id"]), str(row["title"] or "").strip()
    created = now_iso()
    cur = conn.execute(
        """
        INSERT INTO strategies (title, body, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            clean_label,
            "Auto-created from trade review/import flow. Add your execution rules here.",
            created,
            created,
        ),
    )
    return int(cur.lastrowid), clean_label


def upsert_trade_review(
    trade_id: int,
    strategy_id: Optional[int] = None,
    strategy_label: str = "",
    setup_tag: str = "",
    session_tag: str = "",
    checklist_score: Optional[int] = None,
    rule_break_tags: str = "",
    review_note: str = "",
) -> None:
    now = now_iso()
    score_val = None if checklist_score is None else max(0, min(100, int(checklist_score)))
    with db() as conn:
        resolved_strategy_id, resolved_strategy_label = _resolve_strategy_link(
            conn,
            strategy_id=strategy_id,
            strategy_label=strategy_label,
            setup_tag=setup_tag,
        )
        conn.execute(
            """
            INSERT INTO trade_reviews
              (trade_id, strategy_id, strategy_label, setup_tag, session_tag, checklist_score, rule_break_tags, review_note, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_id) DO UPDATE SET
              strategy_id=excluded.strategy_id,
              strategy_label=excluded.strategy_label,
              setup_tag=excluded.setup_tag,
              session_tag=excluded.session_tag,
              checklist_score=excluded.checklist_score,
              rule_break_tags=excluded.rule_break_tags,
              review_note=excluded.review_note,
              updated_at=excluded.updated_at
            """,
            (
                trade_id,
                resolved_strategy_id,
                resolved_strategy_label,
                resolved_strategy_label,
                (session_tag or "").strip(),
                score_val,
                (rule_break_tags or "").strip(),
                (review_note or "").strip(),
                now,
                now,
            ),
        )


def fetch_trade_reviews_map(trade_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    clean_ids = [
        int(i) for i in trade_ids if isinstance(i, int) or (isinstance(i, str) and str(i).isdigit())
    ]
    if not clean_ids:
        return {}
    marks = ",".join(["?"] * len(clean_ids))
    with db() as conn:
        rows = conn.execute(
            f"""
            SELECT trade_id, strategy_id, strategy_label, setup_tag, session_tag, checklist_score, rule_break_tags, review_note
            FROM trade_reviews
            WHERE trade_id IN ({marks})
            """,
            clean_ids,
        ).fetchall()
    return {int(r["trade_id"]): dict(r) for r in rows}


def day_net_total(day_iso: str) -> float:
    with db() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(net_pl), 0) AS total FROM trades WHERE trade_date = ?",
            (day_iso,),
        ).fetchone()
    return float((row["total"] if row else 0.0) or 0.0)


def trade_lockout_state(
    day_iso: str, *, daily_max_loss: float, enforce_lockout: int
) -> Dict[str, Any]:
    day_net = day_net_total(day_iso)
    max_loss = abs(float(daily_max_loss or 0.0))
    locked = bool(enforce_lockout) and max_loss > 0 and day_net <= (-max_loss)
    return {
        "day": day_iso,
        "day_net": day_net,
        "daily_max_loss": max_loss,
        "enforce_lockout": int(enforce_lockout),
        "locked": locked,
    }


def last_balance_in_list(trades: List[object]) -> Optional[float]:
    for t in trades:
        b = t["balance"]
        if b is not None:
            try:
                return float(b)
            except Exception:
                return None
    return None


def trade_day_stats(trades: List[object]) -> Dict[str, Any]:
    total = 0.0
    wins = 0
    losses = 0
    for t in trades:
        net = t["net_pl"]
        if net is None:
            continue
        total += float(net)
        if net > 0:
            wins += 1
        elif net < 0:
            losses += 1

    total_trades = wins + losses
    win_rate = (wins / total_trades * 100.0) if total_trades else 0.0
    wl_ratio = (wins / losses) if losses else (float(wins) if wins else 0.0)
    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "total_trades": total_trades,
        "win_rate": win_rate,
        "wl_ratio": wl_ratio,
    }


def calc_consistency(trades: List[object]) -> Dict[str, Any]:
    if not trades:
        return {"ratio": None, "status": "—", "class": "", "biggest": 0.0, "denom": 0.0}

    net_vals: List[float] = []
    for t in trades:
        try:
            v = t["net_pl"]
        except Exception:
            v = t.get("net_pl") if isinstance(t, dict) else None
        if v is None:
            continue
        try:
            net_vals.append(float(v))
        except Exception:
            continue

    if not net_vals:
        return {"ratio": None, "status": "—", "class": "", "biggest": 0.0, "denom": 0.0}

    total_pnl = sum(net_vals)
    winners = [v for v in net_vals if v > 0]
    losers = [v for v in net_vals if v < 0]

    if total_pnl > 0:
        biggest = max(winners) if winners else 0.0
        denom = total_pnl
        ratio = (biggest / denom) if denom else None
    elif total_pnl < 0:
        biggest = max(abs(v) for v in losers) if losers else 0.0
        denom = abs(total_pnl)
        ratio = (biggest / denom) if denom else None
    else:
        return {"ratio": None, "status": "—", "class": "", "biggest": 0.0, "denom": 0.0}

    ok = (ratio is not None) and (ratio <= 0.30)
    return {
        "ratio": ratio,
        "status": "✅ Pass" if ok else "🚫 Fail",
        "class": "glow-green" if ok else "glow-red",
        "biggest": biggest,
        "denom": denom,
    }


def week_range_for(day_iso: Optional[str]) -> tuple[str, str]:
    if not day_iso:
        day_iso = datetime.now().date().isoformat()
    d = datetime.strptime(day_iso, "%Y-%m-%d").date()
    start = d - timedelta(days=d.weekday())
    end = start + timedelta(days=7)
    return start.isoformat(), end.isoformat()


def week_total_net(day_iso: str) -> float:
    start, end = week_range_for(day_iso)
    with db() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            """,
            (start, end),
        ).fetchone()
    return float(row["net"] or 0.0)


def account_scope_snapshot() -> Dict[str, Any]:
    start_date = str(get_setting_value("active_account_start_date", "") or "").strip()
    label = str(get_setting_value("active_account_label", "") or "").strip()
    if not start_date:
        return {
            "enabled": False,
            "start_date": "",
            "starting_balance": float(get_setting_float("starting_balance", 50000.0)),
            "label": label,
        }
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        return {
            "enabled": False,
            "start_date": "",
            "starting_balance": float(get_setting_float("starting_balance", 50000.0)),
            "label": label,
        }
    scoped_starting = float(
        get_setting_float(
            "active_account_start_balance", float(get_setting_float("starting_balance", 50000.0))
        )
    )
    return {
        "enabled": True,
        "start_date": start_date,
        "starting_balance": scoped_starting,
        "label": label,
    }


def save_account_scope(start_date: str, starting_balance: float, label: str = "") -> None:
    set_setting_value("active_account_start_date", str(start_date).strip())
    set_setting_value("active_account_start_balance", f"{float(starting_balance):.2f}")
    set_setting_value("active_account_label", str(label or "").strip())


def clear_account_scope() -> None:
    set_setting_value("active_account_start_date", "")
    set_setting_value("active_account_start_balance", "")
    set_setting_value("active_account_label", "")


def clear_trades() -> None:
    with db() as conn:
        conn.execute("DELETE FROM trades")


def recompute_balances(starting_balance: float = 50000.0) -> None:
    with db() as conn:
        rows = conn.execute(
            """
            SELECT id, net_pl
            FROM trades
            ORDER BY trade_date ASC, id ASC
            """
        ).fetchall()
        bal = float(starting_balance)
        conn.execute("BEGIN")
        for r in rows:
            net = r["net_pl"]
            if net is not None:
                bal += float(net)
            conn.execute("UPDATE trades SET balance = ? WHERE id = ?", (bal, r["id"]))
        conn.commit()


def latest_trade_day() -> Optional[date]:
    with db() as conn:
        row = conn.execute(
            """
            SELECT trade_date
            FROM trades
            WHERE trade_date IS NOT NULL AND trade_date != ''
            ORDER BY trade_date DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
    if not row:
        return None
    try:
        return datetime.strptime(row["trade_date"], "%Y-%m-%d").date()
    except Exception:
        return None


def latest_balance_overall(
    as_of: str | None = None,
    *,
    start_date: str | None = None,
    starting_balance: float | None = None,
) -> float:
    starting = (
        float(starting_balance)
        if starting_balance is not None
        else float(get_setting_float("starting_balance", 50000.0))
    )
    with db() as conn:
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]
        except Exception:
            return float(starting)

        def _pick(existing: list[str], preferred: list[str]) -> str | None:
            for c in preferred:
                if c in existing:
                    return c
            return None

        pnl_col = _pick(
            cols,
            ["net_pl", "pnl", "profit_loss", "pl", "profit", "p_l", "net_pnl"],
        )
        if not pnl_col:
            return float(starting)
        date_col = _pick(cols, ["trade_date", "date", "day"])

        def _q(col: str) -> str:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col):
                raise ValueError(f"Unsafe column name: {col}")
            return f'"{col}"'

        try:
            pnl_q = _q(pnl_col)
            where: list[str] = []
            params: list[Any] = []
            if date_col and start_date:
                where.append(f"{_q(date_col)} >= ?")
                params.append(str(start_date))
            if date_col and as_of:
                where.append(f"{_q(date_col)} <= ?")
                params.append(str(as_of))
            if where:
                row = conn.execute(
                    f"SELECT COALESCE(SUM(CAST({pnl_q} AS REAL)), 0) FROM trades WHERE {' AND '.join(where)}",
                    params,
                ).fetchone()
            else:
                row = conn.execute(
                    f"SELECT COALESCE(SUM(CAST({pnl_q} AS REAL)), 0) FROM trades"
                ).fetchone()
            total = float(row[0] or 0.0)
        except Exception:
            total = 0.0
    return float(starting + total)


def balance_integrity_snapshot(
    as_of: str | None = None,
    tolerance: float = 0.01,
    *,
    start_date: str | None = None,
    starting_balance: float | None = None,
) -> Dict[str, Any]:
    derived = float(
        latest_balance_overall(
            as_of=as_of, start_date=start_date, starting_balance=starting_balance
        )
    )
    starting = (
        float(starting_balance)
        if starting_balance is not None
        else float(get_setting_float("starting_balance", 50000.0))
    )
    out: Dict[str, Any] = {
        "starting_balance": starting,
        "canonical_balance": derived,
        "source_label": "Derived ledger",
        "source_detail": (
            f"Scoped from {start_date}; start balance plus closed trade net P/L."
            if start_date
            else "Starting balance plus closed trade net P/L."
        ),
        "derived_balance": derived,
        "stored_balance": None,
        "delta": None,
        "has_drift": False,
        "tolerance": float(tolerance),
    }
    with db() as conn:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]

        def _pick(existing: list[str], preferred: list[str]) -> str | None:
            for c in preferred:
                if c in existing:
                    return c
            return None

        bal_col = _pick(cols, ["balance", "running_balance", "equity", "account_balance"])
        if not bal_col:
            return out
        date_col = _pick(cols, ["trade_date", "date", "day"])

        def _q(col: str) -> str:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col):
                raise ValueError(f"Unsafe column name: {col}")
            return f'"{col}"'

        acct_filter = " AND COALESCE(ticker, '') <> 'ACCT'" if "ticker" in cols else ""
        if as_of and date_col:
            params: list[Any] = []
            where = [f"{_q(bal_col)} IS NOT NULL", f"{_q(date_col)} <= ?{acct_filter}"]
            params.append(str(as_of))
            if start_date:
                where.insert(1, f"{_q(date_col)} >= ?")
                params.insert(0, str(start_date))
            row = conn.execute(
                f"""
                SELECT {_q(bal_col)}
                FROM trades
                WHERE {' AND '.join(where)}
                ORDER BY {_q(date_col)} DESC, id DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
        else:
            where = [f"{_q(bal_col)} IS NOT NULL{acct_filter}"]
            params: list[Any] = []
            if start_date and date_col:
                where.append(f"{_q(date_col)} >= ?")
                params.append(str(start_date))
            row = conn.execute(
                f"""
                SELECT {_q(bal_col)}
                FROM trades
                WHERE {' AND '.join(where)}
                ORDER BY id DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
    if not row or row[0] is None:
        return out
    try:
        stored = float(row[0])
    except Exception:
        return out
    delta = derived - stored
    out["stored_balance"] = stored
    out["delta"] = delta
    out["has_drift"] = abs(delta) > float(tolerance)
    if not out["has_drift"]:
        out["source_detail"] = "Stored trade balances match the derived ledger."
    return out


def month_heatmap(year: int, month: int) -> Dict[str, Any]:
    first = date(year, month, 1)
    nxt = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    days_in_month = (nxt - first).days
    with db() as conn:
        rows = conn.execute(
            """
            SELECT
                trade_date,
                COALESCE(SUM(net_pl), 0) AS net,
                COUNT(*) AS trade_count,
                SUM(CASE WHEN COALESCE(net_pl, 0) > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN COALESCE(net_pl, 0) < 0 THEN 1 ELSE 0 END) AS losses
            FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            GROUP BY trade_date
            """,
            (first.isoformat(), nxt.isoformat()),
        ).fetchall()
        bal_rows = conn.execute(
            """
            SELECT trade_date, balance, id
            FROM trades
            WHERE trade_date >= ? AND trade_date < ? AND balance IS NOT NULL
            ORDER BY trade_date ASC, id ASC
            """,
            (first.isoformat(), nxt.isoformat()),
        ).fetchall()
    daily_stats = {
        r["trade_date"]: {
            "net": float(r["net"] or 0.0),
            "trade_count": int(r["trade_count"] or 0),
            "wins": int(r["wins"] or 0),
            "losses": int(r["losses"] or 0),
        }
        for r in rows
    }
    daily_balance: Dict[str, float] = {}
    for r in bal_rows:
        try:
            daily_balance[r["trade_date"]] = float(r["balance"])
        except Exception:
            pass
    start_weekday = (first.weekday() + 1) % 7
    weekday_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    cells: List[Dict[str, Any]] = []
    max_abs = 0.0
    for _ in range(start_weekday):
        cells.append(
            {
                "daynum": None,
                "net": 0.0,
                "iso": "",
                "wd": None,
                "weekday_label": "",
                "trade_count": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "balance": None,
                "is_weekend": False,
                "has_trades": False,
                "outcome": "empty",
            }
        )
    for daynum in range(1, days_in_month + 1):
        iso = date(year, month, daynum).isoformat()
        stats = daily_stats.get(iso, {})
        net = float(stats.get("net", 0.0))
        wins = int(stats.get("wins", 0) or 0)
        losses = int(stats.get("losses", 0) or 0)
        trade_count = int(stats.get("trade_count", 0) or 0)
        total_decisions = wins + losses
        weekday = date(year, month, daynum).weekday()
        is_weekend = weekday >= 5
        if trade_count <= 0:
            outcome = "closed" if is_weekend else "flat"
        elif net > 0:
            outcome = "gain"
        elif net < 0:
            outcome = "loss"
        else:
            outcome = "flat"
        max_abs = max(max_abs, abs(net))
        cells.append(
            {
                "daynum": daynum,
                "net": net,
                "iso": iso,
                "wd": weekday,
                "weekday_label": weekday_names[(weekday + 1) % 7],
                "trade_count": trade_count,
                "wins": wins,
                "losses": losses,
                "win_rate": (wins / total_decisions * 100.0) if total_decisions else 0.0,
                "balance": daily_balance.get(iso),
                "is_weekend": is_weekend,
                "has_trades": trade_count > 0,
                "outcome": outcome,
            }
        )
    while len(cells) % 7 != 0:
        cells.append(
            {
                "daynum": None,
                "net": 0.0,
                "iso": "",
                "wd": None,
                "weekday_label": "",
                "trade_count": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "balance": None,
                "is_weekend": False,
                "has_trades": False,
                "outcome": "empty",
            }
        )
    weeks: List[Dict[str, Any]] = []
    for i in range(0, len(cells), 7):
        week_days = cells[i : i + 7]
        active_days = [d for d in week_days if d["daynum"] is not None]
        week_start = next((d for d in week_days if d["daynum"] is not None), None)
        week_end = next((d for d in reversed(week_days) if d["daynum"] is not None), None)
        weeks.append(
            {
                "days": week_days,
                "net": sum(float(d["net"]) for d in active_days),
                "trade_count": sum(int(d["trade_count"]) for d in active_days),
                "traded_days": sum(1 for d in active_days if d["trade_count"] > 0),
                "label": (
                    f"{week_start['iso']} to {week_end['iso']}"
                    if week_start and week_end
                    else "Out of month"
                ),
            }
        )
    return {
        "year": year,
        "month": month,
        "weeks": weeks,
        "max_abs": max_abs or 1.0,
        "daily_balance": daily_balance,
    }


def last_n_trading_day_totals(n: int = 20, since_date: str = "") -> List[float]:
    with db() as conn:
        if since_date:
            rows = conn.execute(
                """
                SELECT trade_date, COALESCE(SUM(net_pl),0) AS net
                FROM trades
                WHERE trade_date >= ?
                GROUP BY trade_date
                ORDER BY trade_date DESC
                LIMIT 200
                """,
                (since_date,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT trade_date, COALESCE(SUM(net_pl),0) AS net
                FROM trades
                GROUP BY trade_date
                ORDER BY trade_date DESC
                LIMIT 200
                """
            ).fetchall()
    out: List[float] = []
    for r in rows:
        try:
            d = datetime.strptime(r["trade_date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if d.weekday() >= 5:
            continue
        out.append(float(r["net"] or 0.0))
        if len(out) >= n:
            break
    return out


def projections_from_daily(
    daily_vals: List[float], base_balance: Optional[float]
) -> Dict[str, Any]:
    avg = (sum(daily_vals) / len(daily_vals)) if daily_vals else 0.0
    base = float(base_balance or 0.0)

    def _proj(days: int) -> Dict[str, Any]:
        est = avg * days
        return {"days": days, "daily_avg": avg, "est_pnl": est, "est_balance": base + est}

    return {"avg": avg, "base_balance": base, "p5": _proj(5), "p10": _proj(10), "p20": _proj(20)}


def month_total_net(year: int, month: int) -> float:
    first = date(year, month, 1)
    nxt = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    with db() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            """,
            (first.isoformat(), nxt.isoformat()),
        ).fetchone()
    return float(row["net"] or 0.0)


def ytd_total_net(year: int) -> float:
    start = date(year, 1, 1)
    end = date(year + 1, 1, 1)
    with db() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchone()
    return float(row["net"] or 0.0)


def month_trade_count(year: int, month: int) -> int:
    first = date(year, month, 1)
    nxt = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    with db() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            """,
            (first.isoformat(), nxt.isoformat()),
        ).fetchone()
    return int(row["c"] or 0)


def ytd_trade_count(year: int) -> int:
    start = date(year, 1, 1)
    end = date(year + 1, 1, 1)
    with db() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchone()
    return int(row["c"] or 0)
