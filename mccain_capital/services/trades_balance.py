"""Balance and scope helpers for Trades page."""

from __future__ import annotations

from typing import Any, Dict, Optional

from mccain_capital.repositories import trades as repo
from mccain_capital.runtime import db, get_setting_float


def scope_state_for_day(active_day: str) -> Dict[str, Any]:
    account_scope = repo.account_scope_snapshot()
    scope_enabled = bool(account_scope.get("enabled"))
    account_id = None
    try:
        if account_scope.get("account_id") not in (None, "", "0"):
            account_id = int(account_scope.get("account_id"))
    except Exception:
        account_id = None
    scope_start = str(account_scope.get("start_date") or "").strip()
    scope_starting_balance = float(account_scope.get("starting_balance") or 50000.0)
    scope_active = scope_enabled and (
        bool(account_id) or (bool(scope_start) and active_day >= scope_start)
    )
    return {
        "account_scope": account_scope,
        "account_id": account_id,
        "scope_enabled": scope_enabled,
        "scope_start": scope_start,
        "scope_starting_balance": scope_starting_balance,
        "scope_active": scope_active,
    }


def derived_balance_map(
    as_of: Optional[str] = None,
    *,
    start_date: str = "",
    starting_balance: Optional[float] = None,
    account_id: int | None = None,
) -> Dict[int, float]:
    starting = (
        float(starting_balance)
        if starting_balance is not None
        else float(get_setting_float("starting_balance", 50000.0))
    )
    with db() as conn:
        where: list[str] = ["net_pl IS NOT NULL"]
        params: list[Any] = []
        if as_of:
            where.append("trade_date <= ?")
            params.append(as_of)
        if start_date:
            where.append("trade_date >= ?")
            params.append(start_date)
        if account_id:
            where.append("account_id = ?")
            params.append(int(account_id))
        rows = conn.execute(
            f"""
            SELECT id, net_pl
            FROM trades
            WHERE {' AND '.join(where)}
            ORDER BY trade_date ASC, id ASC
            """,
            params,
        ).fetchall()
    out: Dict[int, float] = {}
    bal = starting
    for r in rows:
        bal += float(r["net_pl"] or 0.0)
        out[int(r["id"])] = float(bal)
    return out


def balance_integrity_for_day(active_day: str, scope_state: Dict[str, Any]) -> Dict[str, Any]:
    if scope_state["scope_active"]:
        return repo.balance_integrity_snapshot(
            as_of=active_day,
            start_date=scope_state["scope_start"],
            starting_balance=scope_state["scope_starting_balance"],
            account_id=scope_state.get("account_id"),
        )
    return repo.balance_integrity_snapshot(as_of=active_day, starting_balance=0.0)


def running_balance_for_day(active_day: str, scope_state: Dict[str, Any]) -> float:
    if scope_state["scope_active"]:
        return float(
            repo.latest_balance_overall(
                as_of=active_day,
                start_date=scope_state["scope_start"],
                starting_balance=scope_state["scope_starting_balance"],
                account_id=scope_state.get("account_id"),
            )
        )
    return float(repo.latest_balance_overall(as_of=active_day, starting_balance=0.0))


def summary_totals_for_day(active_day: str, scope_state: Dict[str, Any]) -> Dict[str, float | None]:
    starting_balance = (
        float(scope_state["scope_starting_balance"]) if scope_state["scope_active"] else 0.0
    )
    with db() as conn:
        y_start = f"{active_day[:4]}-01-01"
        ytd_where = ["trade_date >= ?", "trade_date <= ?"]
        ytd_params: list[Any] = [y_start, active_day]
        if scope_state.get("account_id"):
            ytd_where.append("account_id = ?")
            ytd_params.append(int(scope_state["account_id"]))
        ytd_row = conn.execute(
            f"""
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE {' AND '.join(ytd_where)}
            """,
            ytd_params,
        ).fetchone()
        if scope_state["scope_active"] and scope_state.get("account_id"):
            prior_eod_row = conn.execute(
                """
                SELECT COALESCE(SUM(net_pl), 0) AS net, COUNT(*) AS cnt
                FROM trades
                WHERE trade_date < ? AND trade_date >= ? AND net_pl IS NOT NULL AND account_id = ?
                """,
                (active_day, scope_state["scope_start"], int(scope_state["account_id"])),
            ).fetchone()
        elif scope_state["scope_active"]:
            prior_eod_row = conn.execute(
                """
                SELECT COALESCE(SUM(net_pl), 0) AS net, COUNT(*) AS cnt
                FROM trades
                WHERE trade_date < ? AND trade_date >= ? AND net_pl IS NOT NULL
                """,
                (active_day, scope_state["scope_start"]),
            ).fetchone()
        else:
            prior_eod_row = conn.execute(
                """
                SELECT COALESCE(SUM(net_pl), 0) AS net, COUNT(*) AS cnt
                FROM trades
                WHERE trade_date < ? AND net_pl IS NOT NULL
                """,
                (active_day,),
            ).fetchone()
        if scope_state.get("account_id"):
            all_time_row = conn.execute(
                """
                SELECT COALESCE(SUM(net_pl), 0) AS net
                FROM trades
                WHERE account_id = ?
                """,
                (int(scope_state["account_id"]),),
            ).fetchone()
        else:
            all_time_row = conn.execute(
                """
                SELECT COALESCE(SUM(net_pl), 0) AS net
                FROM trades
                """
            ).fetchone()
    ytd_net = float(ytd_row["net"] or 0.0)
    all_time_net = float(all_time_row["net"] or 0.0)
    prior_eod_balance = (
        starting_balance + float(prior_eod_row["net"] or 0.0)
        if prior_eod_row and int(prior_eod_row["cnt"] or 0) > 0
        else None
    )
    return {
        "starting_balance": starting_balance,
        "ytd_net": ytd_net,
        "all_time_net": all_time_net,
        "prior_eod_balance": prior_eod_balance,
    }
