"""Account-scoped Broker Equity validation, resolution, and persistence."""

from __future__ import annotations

import math
import re
from typing import Any, Callable

from mccain_capital.repositories import trades as repo
from mccain_capital.runtime import now_iso


def parse_equity(raw: Any) -> float:
    cleaned = re.sub(r"[^0-9.\-]+", "", str(raw or "").strip())
    try:
        value = float(cleaned)
    except (TypeError, ValueError) as exc:
        raise ValueError("Enter a valid Broker Equity amount.") from exc
    if not math.isfinite(value) or value < 0:
        raise ValueError("Enter a finite Broker Equity amount of zero or more.")
    return round(value, 2)


def manual_update(
    account_id: int,
    raw_equity: Any,
    *,
    audit: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    account = repo.get_account(int(account_id or 0))
    if not account or int(account.get("archived") or 0):
        raise ValueError("Select a valid active account before updating Broker Equity.")
    value = parse_equity(raw_equity)
    existing_peak = account.get("broker_equity_peak")
    peak = max(float(existing_peak), value) if existing_peak is not None else value
    timestamp = now_iso()
    repo.update_account_broker_metrics(
        int(account_id),
        broker_equity=value,
        broker_equity_peak=peak,
        broker_equity_source="manual",
        updated_at=timestamp,
    )
    if audit:
        audit(
            "broker_equity_manual_update",
            {
                "account_id": int(account_id),
                "source": "manual",
                "value": value,
                "updated_at": timestamp,
            },
        )
    return {
        "account_id": int(account_id),
        "source": "manual",
        "value": value,
        "updated_at": timestamp,
    }


def ledger_equity_view(account: dict[str, Any] | None) -> dict[str, Any]:
    """Describe transaction-derived equity from opening capital and recorded net P&L."""
    if not account:
        return {
            "available": False,
            "opening_balance": None,
            "realized_pnl": None,
            "ledger_equity": None,
        }
    opening = _valid_value(account.get("starting_balance"))
    current = _valid_value(account.get("current_balance"))
    if opening is None or current is None:
        return {
            "available": False,
            "opening_balance": opening,
            "realized_pnl": None,
            "ledger_equity": current,
        }
    realized_pnl = round(current - opening, 2)
    account_size = _valid_value(account.get("account_size"))
    display_opening = (
        account_size if account_size is not None and account_size > opening * 1.5 else opening
    )
    return {
        "available": True,
        "opening_balance": display_opening,
        "realized_pnl": realized_pnl,
        "ledger_equity": round(display_opening + realized_pnl, 2),
    }


def resolve_refresh(
    *,
    account_id: int,
    requested_broker_account_id: str,
    metrics: dict[str, Any] | None,
    metrics_meta: dict[str, Any] | None,
    statement_balance: float | None,
    statement_trusted: bool,
) -> dict[str, Any]:
    account = repo.get_account(int(account_id or 0))
    attempted = ["broker_dashboard", "statement"]
    if not account:
        return _result(
            "missing", "missing", None, False, "Selected account is unavailable.", attempted, ""
        )
    selected_broker = repo.normalize_broker_account_id(account.get("broker_account_id"))
    requested_broker = repo.normalize_broker_account_id(requested_broker_account_id)
    if not selected_broker or selected_broker != requested_broker:
        return _preserved(
            account, "Broker account mismatch; no equity value was changed.", attempted
        )

    dashboard_value = _valid_value((metrics or {}).get("broker_equity"))
    if dashboard_value is not None:
        timestamp = now_iso()
        existing_peak = _valid_value(account.get("broker_equity_peak"))
        supplied_peak = _valid_value((metrics or {}).get("broker_equity_peak"))
        peak = max(
            value for value in (dashboard_value, existing_peak, supplied_peak) if value is not None
        )
        updates: dict[str, Any] = {
            "broker_equity": dashboard_value,
            "broker_equity_peak": peak,
            "broker_equity_source": "broker_dashboard",
            "updated_at": timestamp,
        }
        for key in ("broker_remaining_drawdown", "broker_max_loss"):
            value = _valid_value((metrics or {}).get(key))
            if value is not None:
                updates[key] = value
        repo.update_account_broker_metrics(int(account_id), **updates)
        return _result(
            "updated",
            "broker_dashboard",
            dashboard_value,
            True,
            "Broker dashboard equity refreshed.",
            attempted,
            timestamp,
        )

    statement_value = _valid_value(statement_balance) if statement_trusted else None
    if statement_value is not None:
        timestamp = now_iso()
        repo.update_account_broker_equity_from_statement(
            int(account_id), broker_equity=statement_value, source="statement", updated_at=timestamp
        )
        return _result(
            "updated",
            "statement",
            statement_value,
            True,
            "Trusted statement ending balance used.",
            attempted,
            timestamp,
        )

    reason = _failure_reason(metrics_meta, statement_trusted, statement_balance)
    return _preserved(account, reason, attempted)


def _valid_value(raw: Any) -> float | None:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return round(value, 2) if math.isfinite(value) and value >= 0 else None


def _failure_reason(
    meta: dict[str, Any] | None, statement_trusted: bool, statement_balance: Any
) -> str:
    payload = meta or {}
    status = str(payload.get("status") or payload.get("reason") or "").strip().lower()
    final_url = str(payload.get("final_url") or "").lower()
    if status == "auth_required" or "/login" in final_url or "/signup" in final_url:
        return "Broker authentication required; sign in to Vanquish or save equity manually."
    if statement_balance is not None and not statement_trusted:
        return "Statement equity was not trusted because the requested range was not applied."
    return "Broker dashboard equity and a trustworthy statement ending balance were unavailable."


def _preserved(account: dict[str, Any], reason: str, attempted: list[str]) -> dict[str, Any]:
    value = _valid_value(account.get("broker_equity"))
    status = "preserved" if value is not None else "missing"
    source = str(
        (account.get("broker_equity_source") or "stored") if value is not None else "missing"
    )
    return _result(
        status,
        source,
        value,
        False,
        reason,
        attempted,
        str(account.get("broker_metrics_updated_at") or ""),
    )


def _result(
    status: str,
    source: str,
    value: float | None,
    updated: bool,
    reason: str,
    attempted: list[str],
    timestamp: str,
) -> dict[str, Any]:
    return {
        "status": status,
        "source": source,
        "value": value,
        "updated": updated,
        "reason": reason,
        "attempted_sources": attempted,
        "updated_at": timestamp,
    }
