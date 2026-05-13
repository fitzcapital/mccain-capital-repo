"""Trades page view assembly."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from statistics import median
from urllib.parse import urlencode

from mccain_capital.repositories import analytics as analytics_repo
from mccain_capital.repositories import trades as trades_repo
from mccain_capital.services.trade_review_scoring import compute_trade_review_foundation
from mccain_capital.services import trades as legacy
from mccain_capital.services import trades_balance as trades_balance_svc
from mccain_capital.services.viewmodels import (
    StateBadgeViewModel,
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


def _safe_float(value):
    try:
        return float(value) if value is not None else 0.0
    except Exception:
        return 0.0


def _float_or_none(value):
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _trade_day_label(value: str) -> str:
    try:
        return datetime.fromisoformat(str(value)).strftime("%a")
    except Exception:
        return ""


def _compact_note(row: dict) -> str:
    candidates = [
        row.get("improvement_note"),
        row.get("review_note"),
        row.get("thesis_note"),
        row.get("mistake_tags"),
        row.get("entry_quality_note"),
        row.get("exit_quality_note"),
    ]
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            return text
    if not str(row.get("setup_tag") or "").strip():
        return "Tag the setup first so the review stays decision-grade."
    return "Add one clear line on entry quality, exit quality, or the main mistake."


def _format_trade_pct(value) -> str:
    pct_value = _float_or_none(value)
    if pct_value is None:
        return "—"
    sign = "+" if pct_value > 0 else ""
    return f"{sign}{pct_value:.1f}%"


def _format_r_multiple(value) -> str:
    r_value = _float_or_none(value)
    if r_value is None:
        return "—"
    sign = "+" if r_value > 0 else ""
    return f"{sign}{r_value:.2f}R"


def _risk_context(row: dict) -> str:
    planned = _float_or_none(row.get("planned_risk_dollars"))
    if planned is not None and planned > 0:
        return f"Risk {money(planned)}"
    fallback = _float_or_none(row.get("total_spent"))
    if fallback is not None and fallback > 0:
        return f"Risk {money(fallback * 0.20)} est."
    return "Risk n/a"


def _stop_context(row: dict) -> str:
    stop_price = _float_or_none(row.get("stop_price"))
    stop_pct = _float_or_none(row.get("stop_pct"))
    if stop_price is not None and stop_price > 0:
        return f"Stop {money(stop_price)}"
    if stop_pct is not None and stop_pct > 0:
        return f"Stop {stop_pct:.1f}%"
    if _float_or_none(row.get("planned_risk_dollars")) not in (None, 0) or _float_or_none(
        row.get("total_spent")
    ) not in (None, 0):
        return "Stop 20.0%"
    return "Stop n/a"


def _trade_risk_pct_display(row: dict) -> str:
    planned = _float_or_none(row.get("planned_risk_dollars"))
    if planned in (None, 0):
        planned = _float_or_none(row.get("risk_dollars"))
    spend = _float_or_none(row.get("total_spent"))
    if planned not in (None, 0) and spend not in (None, 0):
        return f"{(planned / spend) * 100.0:.1f}%"
    stop_pct = _float_or_none(row.get("stop_pct"))
    if stop_pct not in (None, 0):
        return f"{stop_pct:.1f}%"
    return "20.0%"


def _review_queue(rows: list[dict]) -> list[dict]:
    counts = {"not_reviewed": 0, "partial": 0, "full": 0}
    for row in rows:
        label = str((row.get("review_state") or {}).get("label") or "").lower()
        if label == "fully reviewed":
            counts["full"] += 1
        elif label == "partially reviewed":
            counts["partial"] += 1
        else:
            counts["not_reviewed"] += 1
    return [
        {"label": "Not Reviewed", "value": counts["not_reviewed"], "tone": "warn"},
        {"label": "Partially Reviewed", "value": counts["partial"], "tone": "info"},
        {"label": "Fully Reviewed", "value": counts["full"], "tone": "positive"},
    ]


def _parse_date_or_blank(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return datetime.fromisoformat(text).strftime("%Y-%m-%d")
    except Exception:
        return ""


def _matches_search(row: dict, query: str) -> bool:
    query = str(query or "").strip().lower()
    if not query:
        return True
    haystack = " ".join(
        [
            str(row.get("ticker") or ""),
            str(row.get("opt_type") or ""),
            str(row.get("raw_line") or ""),
            str(row.get("trade_source") or ""),
        ]
    ).lower()
    return query in haystack


def _matches_base_review_filters(row: dict, filters: dict[str, str]) -> bool:
    setup = str(filters.get("setup") or "").strip()
    if setup and str(row.get("setup_label") or row.get("setup_display") or "Unknown") != setup:
        return False
    session = str(filters.get("session") or "").strip()
    if session and str(row.get("session_tag") or "").strip() != session:
        return False
    outcome = str(filters.get("outcome") or "").strip().lower()
    net = _safe_float(row.get("net_pl"))
    if outcome == "winner" and not (net > 0):
        return False
    if outcome == "loser" and not (net < 0):
        return False
    if outcome == "breakeven" and not (net == 0):
        return False
    time_block = str(filters.get("time_block") or "").strip()
    if time_block and str(row.get("time_block") or "").strip() != time_block:
        return False
    mistake_tag = str(filters.get("mistake_tag") or "").strip().lower()
    if mistake_tag:
        tags = ",".join(
            [
                str(row.get("mistake_tags") or ""),
                str(row.get("rule_break_tags") or ""),
            ]
        ).lower()
        if mistake_tag not in tags:
            return False
    return True


def _resolve_scope_selection(
    *,
    requested_mode: str,
    start_date: str,
    end_date: str,
    scope_preset: str,
    account_scope: dict,
    history_starting_balance: float,
) -> dict:
    account_enabled = bool(account_scope.get("enabled"))
    account_start = str(account_scope.get("start_date") or "").strip()
    account_label = (
        str(account_scope.get("account_name") or "").strip()
        or str(account_scope.get("label") or "").strip()
        or "Current Account"
    )
    account_type = str(account_scope.get("account_type") or "").strip()
    default_mode = "current" if account_enabled and account_start else "all"
    mode = str(requested_mode or "").strip().lower() or default_mode
    if mode not in {"all", "current", "custom"}:
        mode = default_mode
    if mode == "current" and not (account_enabled and account_start):
        mode = "all"

    effective_start = ""
    effective_end = ""
    scope_label = "All History"
    scope_detail = "All trades in the dataset."
    scope_starting_balance = float(history_starting_balance)

    if mode == "current":
        effective_start = account_start
        scope_label = "Current Account"
        scope_starting_balance = float(
            account_scope.get("starting_balance") or history_starting_balance
        )
        detail_parts = [account_label]
        if account_type:
            detail_parts.append(account_type)
        detail_parts.append(f"from {account_start}")
        scope_detail = " · ".join(part for part in detail_parts if part)
    elif mode == "custom":
        effective_start = start_date
        effective_end = end_date
        scope_label = "Custom Range"
        if effective_start or effective_end:
            scope_detail = f"{effective_start or '…'} → {effective_end or '…'}"
        else:
            scope_detail = "No explicit range selected. Falling back to all available trades."
        if effective_start:
            try:
                prior_day = prev_trading_day_iso(effective_start)
                scope_starting_balance = float(trades_repo.latest_balance_overall(as_of=prior_day))
            except Exception:
                scope_starting_balance = float(history_starting_balance)
    return {
        "mode": mode,
        "default_mode": default_mode,
        "effective_start": effective_start,
        "effective_end": effective_end,
        "label": scope_label,
        "detail": scope_detail,
        "starting_balance": float(scope_starting_balance),
        "account_name": account_label,
        "account_id": str(account_scope.get("account_id") or "").strip(),
        "account_type": account_type,
        "account_start_date": account_start,
    }


def _apply_scope_preset(preset: str) -> tuple[str, str]:
    preset = str(preset or "").strip().lower()
    today = today_iso()
    try:
        today_dt = datetime.fromisoformat(today)
    except Exception:
        return "", ""
    if preset == "7d":
        return ((today_dt - timedelta(days=6)).strftime("%Y-%m-%d"), today)
    if preset == "30d":
        return ((today_dt - timedelta(days=29)).strftime("%Y-%m-%d"), today)
    if preset == "90d":
        return ((today_dt - timedelta(days=89)).strftime("%Y-%m-%d"), today)
    if preset == "ytd":
        return (today_dt.strftime("%Y-01-01"), today)
    return "", ""


def _risk_tier(risk_pct: float | None, cohort_median: float | None) -> str:
    if risk_pct is None and cohort_median is None:
        return "Unknown"
    if risk_pct is not None:
        if risk_pct >= 2.0:
            return "High"
        if risk_pct >= 1.0:
            return "Medium"
    if (
        cohort_median is not None
        and cohort_median > 0
        and risk_pct is not None
        and risk_pct <= cohort_median * 0.65
    ):
        return "Low"
    if risk_pct is not None and risk_pct <= 0.75:
        return "Low"
    return "Medium"


def _trade_bucket_match(row: dict, bucket: str) -> bool:
    bucket = str(bucket or "").strip().lower()
    if not bucket:
        return True
    classification = str(row.get("trade_classification") or "")
    oversized = bool(row.get("oversized"))
    missing_stop = not bool(row.get("stop_present"))
    if bucket == "good_wins":
        return classification == "Good Win"
    if bucket == "bad_wins":
        return classification == "Bad Win"
    if bucket == "good_losses":
        return classification == "Good Loss"
    if bucket == "bad_losses":
        return classification == "Bad Loss"
    if bucket == "oversized":
        return oversized
    if bucket == "missing_stop":
        return missing_stop
    if bucket == "rule_breaks":
        return row.get("execution_quality_label") == "Rule Break" or bool(
            str(row.get("rule_break_tags") or "").strip()
        )
    if bucket == "best_grades":
        return str(row.get("trade_grade") or "") in {"A", "B"}
    if bucket == "worst_grades":
        return str(row.get("trade_grade") or "") in {"D", "F"}
    if bucket == "best_r":
        return (
            _float_or_none(row.get("r_multiple")) is not None
            and float(row.get("r_multiple")) >= 1.0
        )
    if bucket == "worst_discipline":
        return (
            row.get("stop_discipline_label") in {"Loss Exceeded Planned Risk", "Stop Missing"}
            or row.get("execution_quality_label") == "Rule Break"
        )
    return True


def _suggest_setup(row: dict) -> str:
    hold_minutes = row.get("hold_minutes")
    time_block = str(row.get("time_block") or "").strip()
    session_tag = str(row.get("session_tag") or "").strip()
    net_pl = _safe_float(row.get("net_pl"))
    result_pct = _safe_float(row.get("result_pct"))
    if time_block == "Midday":
        return "Chop"
    if (
        hold_minutes is not None
        and hold_minutes <= 6
        and (result_pct is not None and abs(result_pct) >= 18)
    ):
        return "Sweep"
    if session_tag == "Open" and hold_minutes is not None and hold_minutes <= 20:
        return "Reversal"
    if hold_minutes is not None and hold_minutes >= 28:
        return "Continuation"
    if net_pl is not None and net_pl < 0 and time_block == "Power Hour":
        return "Reversal"
    return "Other"


def _build_daily_pnl(rows: list[dict], *, limit: int = 14) -> list[dict]:
    daily: dict[str, float] = defaultdict(float)
    for row in rows:
        day = str(row.get("trade_date") or "").strip()
        if not day:
            continue
        daily[day] += _safe_float(row.get("net_pl"))
    points = []
    for day in sorted(daily.keys())[-limit:]:
        points.append(
            {
                "date": day,
                "label": _trade_day_label(day) or day[5:],
                "value": round(float(daily[day]), 2),
            }
        )
    return points


def _build_win_rate_trend(rows: list[dict], *, limit: int = 14) -> list[dict]:
    daily_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"wins": 0, "closed": 0})
    for row in rows:
        day = str(row.get("trade_date") or "").strip()
        if not day:
            continue
        net = row.get("net_pl")
        if net is None:
            continue
        net_value = _safe_float(net)
        daily_counts[day]["closed"] += 1
        if net_value > 0:
            daily_counts[day]["wins"] += 1
    cumulative_closed = 0
    cumulative_wins = 0
    points = []
    for day in sorted(daily_counts.keys())[-limit:]:
        cumulative_closed += daily_counts[day]["closed"]
        cumulative_wins += daily_counts[day]["wins"]
        pct_value = (cumulative_wins / cumulative_closed * 100.0) if cumulative_closed else 0.0
        points.append(
            {
                "date": day,
                "label": _trade_day_label(day) or day[5:],
                "value": round(pct_value, 1),
            }
        )
    return points


def _build_equity_curve(
    rows: list[dict], *, starting_balance: float = 0.0, limit: int = 24
) -> list[dict]:
    ordered = sorted(
        rows,
        key=lambda row: (
            str(row.get("trade_date") or ""),
            int(row.get("id") or 0),
        ),
    )
    grouped: dict[str, dict] = {}
    for row in ordered:
        trade_date = str(row.get("trade_date") or "").strip()
        if not trade_date:
            continue
        bucket = grouped.setdefault(
            trade_date,
            {
                "date": trade_date,
                "label": _trade_day_label(trade_date) or trade_date[5:],
                "net": 0.0,
                "close_balance": None,
            },
        )
        bucket["net"] += _safe_float(row.get("net_pl"))
        balance_value = _float_or_none(row.get("balance"))
        if balance_value is not None:
            bucket["close_balance"] = balance_value

    days = [grouped[key] for key in sorted(grouped.keys())[-limit:]]
    if not days:
        return []

    running_balance = starting_balance
    first_close = _float_or_none(days[0].get("close_balance"))
    if first_close is not None:
        running_balance = first_close - _safe_float(days[0].get("net"))

    peak = running_balance
    cumulative_net = 0.0
    cumulative_r = 0.0
    peak_r = 0.0
    points: list[dict] = []
    for day in days:
        close_balance = _float_or_none(day.get("close_balance"))
        day_net = _safe_float(day.get("net"))
        if close_balance is None:
            running_balance += day_net
            close_balance = running_balance
        else:
            running_balance = close_balance
        peak = max(peak, close_balance)
        drawdown = close_balance - peak
        cumulative_net += day_net
        day_r = 0.0
        for row in rows:
            if str(row.get("trade_date") or "").strip() != day["date"]:
                continue
            r_multiple = _float_or_none(row.get("r_multiple"))
            if r_multiple is not None:
                day_r += r_multiple
        cumulative_r += day_r
        peak_r = max(peak_r, cumulative_r)
        cumulative_pct = (
            ((close_balance - starting_balance) / starting_balance * 100.0)
            if starting_balance
            else 0.0
        )
        peak_pct = (
            ((peak - starting_balance) / starting_balance * 100.0) if starting_balance else 0.0
        )
        drawdown_pct = ((close_balance - peak) / peak * 100.0) if peak else 0.0
        points.append(
            {
                "date": day["date"],
                "label": day["label"],
                "value": round(close_balance, 2),
                "peak": round(peak, 2),
                "drawdown": round(drawdown, 2),
                "drawdown_abs": round(abs(drawdown), 2),
                "drawdown_pct": round(drawdown_pct, 2),
                "drawdown_pct_abs": round(abs(drawdown_pct), 2),
                "day_net": round(day_net, 2),
                "cum_net": round(cumulative_net, 2),
                "cum_r": round(cumulative_r, 2),
                "peak_r": round(peak_r, 2),
                "cum_pct": round(cumulative_pct, 2),
                "peak_pct": round(peak_pct, 2),
            }
        )
    return points


def _build_setup_performance(setup_scorecards: list[dict], *, limit: int = 5) -> list[dict]:
    candidates = [
        {
            "label": str(card.get("setup") or "").strip(),
            "value": round(float(card.get("expectancy") or 0.0), 2),
            "net": round(float(card.get("net") or 0.0), 2),
            "count": int(card.get("count") or 0),
            "win_rate": round(float(card.get("win_rate") or 0.0), 1),
        }
        for card in setup_scorecards
        if str(card.get("setup") or "").strip() and str(card.get("setup") or "") != "Unknown"
    ]
    candidates.sort(key=lambda item: (item["value"], item["net"], item["count"]), reverse=True)
    return candidates[:limit]


def _build_time_of_day_performance(rows: list[dict]) -> list[dict]:
    order = ["Open", "Midday", "Power Hour", "After Hours", "Unknown"]
    grouped: dict[str, dict[str, float]] = defaultdict(
        lambda: {"net": 0.0, "count": 0.0, "wins": 0.0}
    )
    for row in rows:
        label = str(row.get("time_block") or "Unknown").strip() or "Unknown"
        net = _safe_float(row.get("net_pl"))
        grouped[label]["net"] += net
        grouped[label]["count"] += 1
        if net > 0:
            grouped[label]["wins"] += 1
    points = []
    for label in order:
        if label not in grouped:
            continue
        count = int(grouped[label]["count"] or 0)
        win_rate = (grouped[label]["wins"] / count * 100.0) if count else 0.0
        points.append(
            {
                "label": label,
                "value": round(grouped[label]["net"], 2),
                "count": count,
                "win_rate": round(win_rate, 1),
            }
        )
    return points


def _equity_takeaway(points: list[dict]) -> dict:
    if not points:
        return {
            "title": "No equity curve yet",
            "body": "Closed trades will populate the running curve and drawdown context.",
        }
    current = points[-1]
    peak = max(float(point.get("peak") or 0.0) for point in points)
    gap = peak - float(current.get("value") or 0.0)
    max_drawdown = max(abs(float(point.get("drawdown") or 0.0)) for point in points)
    if gap <= 1:
        return {
            "title": "Equity is pressing the current peak",
            "body": f"Max drawdown in this window is {money(max_drawdown)}. Protect the high-water mark.",
        }
    return {
        "title": f"Curve is {money(gap)} below the peak",
        "body": f"Max drawdown reached {money(max_drawdown)} in this review window. Recovery still needs clean follow-through.",
    }


def _setup_takeaway(points: list[dict]) -> dict:
    if not points:
        return {
            "title": "No setup edge read yet",
            "body": "Tag more trades so the setup breakdown separates real edge from noise.",
        }
    leader = points[0]
    trailer = min(points, key=lambda item: float(item.get("value") or 0.0))
    if float(trailer.get("value") or 0.0) < 0 and trailer["label"] != leader["label"]:
        body = (
            f"{leader['label']} is leading at {money(leader['value'])} per trade, while "
            f"{trailer['label']} is leaking {money(abs(trailer['value']))} per trade."
        )
    else:
        body = f"{leader['label']} is leading at {money(leader['value'])} expectancy across {leader['count']} tagged trades."
    return {"title": "Setup quality is separating cleanly", "body": body}


def _time_takeaway(points: list[dict]) -> dict:
    if not points:
        return {
            "title": "No time-of-day pattern yet",
            "body": "More closed trades are needed before the session windows become actionable.",
        }
    leader = max(points, key=lambda item: float(item.get("value") or 0.0))
    trailer = min(points, key=lambda item: float(item.get("value") or 0.0))
    if leader["label"] == trailer["label"]:
        body = f"{leader['label']} is the only meaningful session window in scope so far."
    else:
        body = (
            f"{leader['label']} is the cleanest window at {money(leader['value'])}, while "
            f"{trailer['label']} is dragging at {money(trailer['value'])}."
        )
    return {"title": "Session timing is driving the quality split", "body": body}


def _build_risk_by_day(rows: list[dict], *, limit: int = 16) -> list[dict]:
    grouped: dict[str, float] = defaultdict(float)
    for row in rows:
        trade_date = str(row.get("trade_date") or "").strip()
        if not trade_date:
            continue
        grouped[trade_date] += float(_float_or_none(row.get("risk_dollars")) or 0.0)
    return [
        {
            "date": day,
            "label": _trade_day_label(day) or day[5:],
            "value": round(grouped[day], 2),
        }
        for day in sorted(grouped.keys())[-limit:]
    ]


def _build_stop_respect_by_setup(rows: list[dict], *, limit: int = 6) -> list[dict]:
    grouped: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0.0, "respected": 0.0})
    for row in rows:
        label = str(row.get("setup_label") or "Unknown").strip() or "Unknown"
        grouped[label]["count"] += 1
        if str(row.get("stop_discipline_label") or "") in {"Stop Respected", "Within Risk Plan"}:
            grouped[label]["respected"] += 1
    points = []
    for label, values in grouped.items():
        count = int(values["count"] or 0)
        pct_value = (values["respected"] / count * 100.0) if count else 0.0
        points.append(
            {
                "label": label,
                "value": round(pct_value, 1),
                "count": count,
                "win_rate": round(pct_value, 1),
            }
        )
    points.sort(key=lambda item: (item["value"], item["count"]), reverse=True)
    return points[:limit]


def _risk_kpi_strip(rows: list[dict], max_drawdown: float) -> list[dict]:
    risks = [
        float(v)
        for v in (_float_or_none(row.get("risk_dollars")) for row in rows)
        if v not in (None, 0)
    ]
    spends = [
        float(v)
        for v in (_float_or_none(row.get("total_spent")) for row in rows)
        if v not in (None, 0)
    ]
    rs = [
        float(v) for v in (_float_or_none(row.get("r_multiple")) for row in rows) if v is not None
    ]
    wins = [
        float(_safe_float(row.get("net_pl"))) for row in rows if _safe_float(row.get("net_pl")) > 0
    ]
    losses = [
        abs(float(_safe_float(row.get("net_pl"))))
        for row in rows
        if _safe_float(row.get("net_pl")) < 0
    ]
    stop_present_rows = [row for row in rows if row.get("stop_present")]
    stop_respected_count = sum(
        1
        for row in stop_present_rows
        if str(row.get("stop_discipline_label") or "") in {"Stop Respected", "Within Risk Plan"}
    )
    oversized_count = sum(1 for row in rows if row.get("oversized"))
    return [
        {
            "label": "Avg Risk",
            "value": money(sum(risks) / len(risks)) if risks else "—",
            "meta": "per trade",
        },
        {
            "label": "Largest Risk",
            "value": money(max(risks)) if risks else "—",
            "meta": "largest planned",
        },
        {
            "label": "Avg R",
            "value": f"{(sum(rs) / len(rs)):.2f}R" if rs else "—",
            "meta": "average realized",
        },
        {
            "label": "Median R",
            "value": f"{median(rs):.2f}R" if rs else "—",
            "meta": "middle result",
        },
        {"label": "Max DD", "value": money(max_drawdown), "meta": "equity drawdown"},
        {
            "label": "Stop Respected",
            "value": (
                f"{(stop_respected_count / len(stop_present_rows) * 100.0):.0f}%"
                if stop_present_rows
                else "—"
            ),
            "meta": "of trades with stop",
        },
        {"label": "Oversized", "value": str(oversized_count), "meta": "flagged trades"},
        {
            "label": "Avg Spend",
            "value": money(sum(spends) / len(spends)) if spends else "—",
            "meta": "position spend",
        },
        {
            "label": "Avg Winner",
            "value": money(sum(wins) / len(wins)) if wins else "—",
            "meta": "positive trades",
        },
        {
            "label": "Avg Loser",
            "value": money(sum(losses) / len(losses)) if losses else "—",
            "meta": "absolute loss",
        },
    ]


def _account_health_strip(
    *,
    resolved_scope: dict,
    rows: list[dict],
    net_pl: float,
    max_drawdown: float,
    current_drawdown: float,
) -> list[dict]:
    active_days = len(
        {
            str(row.get("trade_date") or "").strip()
            for row in rows
            if str(row.get("trade_date") or "").strip()
        }
    )
    scope_mode = str(resolved_scope.get("mode") or "all").strip().lower()
    if scope_mode == "current":
        account_name = str(resolved_scope.get("account_name") or "").strip() or "Current Account"
        start_label = str(resolved_scope.get("account_start_date") or "").strip() or "—"
        scope_mode_label = "Current Account"
        context_meta = str(resolved_scope.get("account_type") or "").strip() or "Active account"
    elif scope_mode == "custom":
        account_name = "Custom Review Window"
        start_label = f"{resolved_scope.get('effective_start') or '…'} → {resolved_scope.get('effective_end') or '…'}"
        scope_mode_label = "Custom Range"
        context_meta = "Selected date range"
    else:
        account_name = "All Accounts"
        start_label = "Lifetime ledger"
        scope_mode_label = "All History"
        context_meta = "Full dataset"
    return [
        {"label": "Account Name", "value": account_name, "meta": context_meta},
        {
            "label": "Scope Mode",
            "value": scope_mode_label,
            "meta": str(resolved_scope.get("detail") or "").strip() or "All visible trades",
        },
        {"label": "Start Date", "value": start_label, "meta": "Scope anchor"},
        {"label": "Net P/L", "value": money(net_pl), "meta": f"{len(rows)} trades in scope"},
        {"label": "Max Drawdown", "value": money(max_drawdown), "meta": "Peak to trough"},
        {
            "label": "Current Drawdown",
            "value": money(current_drawdown),
            "meta": "Below high-water mark",
        },
        {"label": "Days Active", "value": str(active_days), "meta": "Trading days in scope"},
    ]


def _circular_stat_spotlight(
    *,
    win_rate: float,
    stop_respected_pct: float | None,
    review_completion_pct: float,
    recovery_pct: float,
) -> list[dict]:
    def tone_for_pct(value: float) -> str:
        if value >= 75:
            return "positive"
        if value >= 50:
            return "info"
        if value >= 35:
            return "warning"
        return "negative"

    stop_pct = float(stop_respected_pct or 0.0)
    items = [
        {
            "label": "Win Rate",
            "value": f"{float(win_rate or 0.0):.0f}%",
            "pct": max(0.0, min(100.0, float(win_rate or 0.0))),
            "meta": "Closed trade hit rate",
            "tone": tone_for_pct(float(win_rate or 0.0)),
        },
        {
            "label": "Stop Respected",
            "value": f"{stop_pct:.0f}%" if stop_respected_pct is not None else "—",
            "pct": max(0.0, min(100.0, stop_pct)),
            "meta": "With stop context",
            "tone": tone_for_pct(stop_pct),
        },
        {
            "label": "Review Complete",
            "value": f"{float(review_completion_pct or 0.0):.0f}%",
            "pct": max(0.0, min(100.0, float(review_completion_pct or 0.0))),
            "meta": "Tagged and reviewed",
            "tone": tone_for_pct(float(review_completion_pct or 0.0)),
        },
        {
            "label": "Recovery",
            "value": f"{float(recovery_pct or 0.0):.0f}%",
            "pct": max(0.0, min(100.0, float(recovery_pct or 0.0))),
            "meta": "Recovered from max DD",
            "tone": tone_for_pct(float(recovery_pct or 0.0)),
        },
    ]
    return items


def _best_time_block(rows: list[dict]) -> dict | None:
    grouped: dict[str, dict[str, float]] = defaultdict(lambda: {"net": 0.0, "count": 0.0})
    for row in rows:
        block = str(row.get("time_block") or "").strip()
        if not block:
            continue
        grouped[block]["net"] += _safe_float(row.get("net_pl"))
        grouped[block]["count"] += 1
    ranked = [
        {"label": label, "net": values["net"], "count": int(values["count"])}
        for label, values in grouped.items()
        if values["count"] > 0
    ]
    ranked.sort(key=lambda item: (item["net"], item["count"]), reverse=True)
    return ranked[0] if ranked and ranked[0]["net"] > 0 else None


def _worst_time_block(rows: list[dict]) -> dict | None:
    grouped: dict[str, dict[str, float]] = defaultdict(lambda: {"net": 0.0, "count": 0.0})
    for row in rows:
        block = str(row.get("time_block") or "").strip()
        if not block:
            continue
        grouped[block]["net"] += _safe_float(row.get("net_pl"))
        grouped[block]["count"] += 1
    ranked = [
        {"label": label, "net": values["net"], "count": int(values["count"])}
        for label, values in grouped.items()
        if values["count"] > 0
    ]
    ranked.sort(key=lambda item: (item["net"], -item["count"]))
    return ranked[0] if ranked and ranked[0]["net"] < 0 else None


def _trade_insights(
    *,
    trades_count: int,
    best_setup: dict | None,
    biggest_leak: dict | None,
    review_coverage: dict,
    rows: list[dict],
) -> tuple[list[str], list[str]]:
    working: list[str] = []
    not_working: list[str] = []
    if best_setup:
        working.append(
            f"{best_setup['setup']} is leading with {money(best_setup['net'])} net across {best_setup['count']} trades."
        )
    best_block = _best_time_block(rows)
    if best_block:
        working.append(
            f"{best_block['label']} is the cleanest window at {money(best_block['net'])} over {best_block['count']} trades."
        )
    if trades_count and review_coverage["completion_pct"] >= 70:
        working.append(
            f"Review coverage is {review_coverage['completion_pct']:.0f}%, which is good enough to trust the setup read."
        )
    if not working:
        working.append(
            "No durable edge is standing out yet. Keep tagging setups and reviews so the signal can emerge."
        )

    if biggest_leak:
        not_working.append(
            f"{biggest_leak['tag'].replace('-', ' ').title()} is costing {money(biggest_leak['loss_cost'])} across {biggest_leak['count']} tagged trades."
        )
    worst_block = _worst_time_block(rows)
    if worst_block:
        not_working.append(
            f"{worst_block['label']} is dragging results at {money(worst_block['net'])}; tighten entries in that window."
        )
    if trades_count and review_coverage["fully_reviewed"] < trades_count:
        not_working.append(
            f"{trades_count - review_coverage['fully_reviewed']} trades still need full review tags before the insights are decision-grade."
        )
    if not not_working:
        not_working.append("No obvious recurring leak is dominating this scope right now.")
    return working[:3], not_working[:3]


def _tagging_status(rows: list[dict], review_coverage: dict) -> dict:
    tagged = 0
    imported = 0
    imported_untagged = 0
    suggestions_ready = 0
    for row in rows:
        source = str(row.get("trade_source") or "").strip()
        has_setup = bool(str(row.get("setup_tag") or "").strip())
        if has_setup:
            tagged += 1
        if source in {"Statement Import", "Live Upload"}:
            imported += 1
            if not has_setup:
                imported_untagged += 1
        if not has_setup:
            suggestions_ready += 1
    total = len(rows)
    return {
        "tagged": tagged,
        "untagged": max(total - tagged, 0),
        "imported": imported,
        "imported_untagged": imported_untagged,
        "review_completion_pct": float(review_coverage.get("completion_pct") or 0.0),
        "suggestions_ready": suggestions_ready,
    }


def trades_page():
    d = legacy.request.args.get("d", "")
    start_date = _parse_date_or_blank(legacy.request.args.get("start_date", ""))
    end_date = _parse_date_or_blank(legacy.request.args.get("end_date", ""))
    scope_preset = str(legacy.request.args.get("scope_preset", "") or "").strip().lower()
    if scope_preset and scope_preset != "custom":
        preset_start, preset_end = _apply_scope_preset(scope_preset)
        start_date = preset_start or start_date
        end_date = preset_end or end_date
    if d and not start_date and not end_date:
        start_date = d
        end_date = d
    active_day = end_date or d or today_iso()
    history_starting_balance = float(get_setting_float("starting_balance", 50000.0))
    account_scope = trades_repo.account_scope_snapshot()
    account_scope_mode = (
        str(legacy.request.args.get("account_scope_mode", "") or "").strip().lower()
    )
    resolved_scope = _resolve_scope_selection(
        requested_mode=account_scope_mode,
        start_date=start_date,
        end_date=end_date,
        scope_preset=scope_preset,
        account_scope=account_scope,
        history_starting_balance=history_starting_balance,
    )
    account_scope_mode = resolved_scope["mode"]
    scope_effective_start = resolved_scope["effective_start"]
    scope_effective_end = resolved_scope["effective_end"]
    scope_as_of_day = scope_effective_end or active_day

    prev_day = prev_trading_day_iso(active_day)
    next_day = next_trading_day_iso(active_day)

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
    extended_filters = {
        "day_of_week": str(legacy.request.args.get("day_of_week", "") or "").strip(),
        "instrument": str(legacy.request.args.get("instrument", "") or "").strip(),
        "side": str(legacy.request.args.get("side", "") or "").strip(),
        "review_state": str(legacy.request.args.get("review_state", "") or "").strip(),
        "trade_grade": str(legacy.request.args.get("trade_grade", "") or "").strip(),
        "classification": str(legacy.request.args.get("classification", "") or "").strip(),
        "risk_tier": str(legacy.request.args.get("risk_tier", "") or "").strip(),
        "stop_discipline": str(legacy.request.args.get("stop_discipline", "") or "").strip(),
        "execution_quality": str(legacy.request.args.get("execution_quality", "") or "").strip(),
        "outcome_quality": str(legacy.request.args.get("outcome_quality", "") or "").strip(),
        "risk_gt": _float_or_none(legacy.request.args.get("risk_gt", "")),
        "risk_pct_gt": _float_or_none(legacy.request.args.get("risk_pct_gt", "")),
        "r_lt_zero": str(legacy.request.args.get("r_lt_zero", "") or "").strip(),
        "oversized_only": str(legacy.request.args.get("oversized_only", "") or "").strip(),
        "missing_stop_only": str(legacy.request.args.get("missing_stop_only", "") or "").strip(),
        "loss_exceeded_only": str(legacy.request.args.get("loss_exceeded_only", "") or "").strip(),
        "high_spend_only": str(legacy.request.args.get("high_spend_only", "") or "").strip(),
        "rule_break_only": str(legacy.request.args.get("rule_break_only", "") or "").strip(),
        "bucket": str(legacy.request.args.get("bucket", "") or "").strip(),
    }
    advanced_filters_active = any(
        bool(value) for key, value in extended_filters.items() if key not in {"bucket"}
    )
    page = max(1, parse_int(legacy.request.args.get("page") or "1") or 1)
    per = parse_int(legacy.request.args.get("per") or "50") or 50
    per = max(25, min(200, per))
    scope_state = {
        "account_scope": account_scope,
        "scope_enabled": account_scope_mode in {"current", "custom"}
        and bool(scope_effective_start),
        "scope_start": scope_effective_start,
        "scope_starting_balance": float(resolved_scope["starting_balance"]),
        "scope_active": account_scope_mode in {"current", "custom"} and bool(scope_effective_start),
    }

    raw_trades = legacy.fetch_trades(d="", q="", filters={})
    trades = [dict(r) for r in raw_trades]
    derived_balances = trades_balance_svc.derived_balance_map(
        as_of=scope_as_of_day,
        start_date=scope_effective_start if scope_state["scope_active"] else "",
        starting_balance=(
            scope_state["scope_starting_balance"] if scope_state["scope_active"] else None
        ),
    )
    for t in trades:
        trade_id = t.get("id")
        if trade_id in derived_balances:
            t["balance"] = derived_balances[trade_id]
        t["day_of_week"] = _trade_day_label(t.get("trade_date"))
        t["key_note"] = _compact_note(t)
        t["setup_label"] = str(t.get("setup_display") or t.get("setup_tag") or "Unknown")
        t["source_label"] = str(t.get("trade_source") or "Unknown")
        t["setup_missing"] = not bool(str(t.get("setup_tag") or "").strip())
        t["setup_suggestion"] = _suggest_setup(t) if t["setup_missing"] else ""
        t["result_pct_display"] = _format_trade_pct(t.get("result_pct"))
        t["r_multiple_display"] = _format_r_multiple(t.get("r_multiple"))
        t["risk_context"] = _risk_context(t)
        t["stop_context"] = _stop_context(t)
        t["trade_risk_pct_display"] = _trade_risk_pct_display(t)
        t["stop_rule_display"] = t["trade_risk_pct_display"]
        t["detail_tags"] = [
            value
            for value in [
                str(t.get("session_tag") or "").strip(),
                str(t.get("time_block") or "").strip(),
                str(t.get("mistake_tags") or "").strip(),
            ]
            if value
        ]
    scoped_trades = []
    for t in trades:
        trade_date = str(t.get("trade_date") or "").strip()
        if scope_effective_start and trade_date and trade_date < scope_effective_start:
            continue
        if scope_effective_end and trade_date and trade_date > scope_effective_end:
            continue
        scoped_trades.append(t)
    trades = scoped_trades
    cohort_risks = [
        float(t["planned_risk_dollars"])
        for t in trades
        if _float_or_none(t.get("planned_risk_dollars")) not in (None, 0)
    ]
    risk_median = median(cohort_risks) if cohort_risks else None
    risk_avg = (sum(cohort_risks) / len(cohort_risks)) if cohort_risks else None
    spends = [_float_or_none(t.get("total_spent")) for t in trades]
    spend_values = [float(v) for v in spends if v not in (None, 0)]
    spend_median = median(spend_values) if spend_values else None

    for t in trades:
        review_foundation = compute_trade_review_foundation(
            t,
            risk_median=risk_median,
            risk_avg=risk_avg,
            spend_median=spend_median,
        )
        t.update(review_foundation)
        t["risk_tier"] = _risk_tier(t.get("risk_pct"), risk_median)
        t["mae_display"] = "n/a"
        t["mfe_display"] = "n/a"

    filtered_trades = []
    for t in trades:
        if not _matches_search(t, q):
            continue
        if not _matches_base_review_filters(t, review_filters):
            continue
        if (
            extended_filters["day_of_week"]
            and str(t.get("day_of_week") or "") != extended_filters["day_of_week"]
        ):
            continue
        if (
            extended_filters["instrument"]
            and extended_filters["instrument"].lower() not in str(t.get("ticker") or "").lower()
        ):
            continue
        if (
            extended_filters["side"]
            and extended_filters["side"].lower() != str(t.get("opt_type") or "").lower()
        ):
            continue
        if (
            extended_filters["review_state"]
            and extended_filters["review_state"].lower()
            not in str(t.get("review_state", {}).get("label") or "").lower()
        ):
            continue
        if extended_filters["trade_grade"] and extended_filters["trade_grade"] != str(
            t.get("trade_grade") or ""
        ):
            continue
        if extended_filters["classification"] and extended_filters["classification"] != str(
            t.get("trade_classification") or ""
        ):
            continue
        if extended_filters["risk_tier"] and extended_filters["risk_tier"] != t.get("risk_tier"):
            continue
        if extended_filters["stop_discipline"] and extended_filters["stop_discipline"] != t.get(
            "stop_discipline_label"
        ):
            continue
        if extended_filters["execution_quality"] and extended_filters["execution_quality"] != t.get(
            "execution_quality_label"
        ):
            continue
        if extended_filters["outcome_quality"] and extended_filters["outcome_quality"] != t.get(
            "outcome_label"
        ):
            continue
        if extended_filters["risk_gt"] is not None and (
            t.get("risk_dollars") is None
            or float(t.get("risk_dollars")) <= extended_filters["risk_gt"]
        ):
            continue
        if extended_filters["risk_pct_gt"] is not None and (
            t.get("risk_pct") is None or float(t.get("risk_pct")) <= extended_filters["risk_pct_gt"]
        ):
            continue
        if extended_filters["r_lt_zero"] and not (
            _float_or_none(t.get("r_multiple")) is not None and float(t.get("r_multiple")) < 0
        ):
            continue
        if extended_filters["oversized_only"] and not t.get("oversized"):
            continue
        if extended_filters["missing_stop_only"] and t.get("stop_present"):
            continue
        if extended_filters["loss_exceeded_only"] and not t.get("loss_exceeded_planned_risk"):
            continue
        if extended_filters["high_spend_only"] and not (
            spend_median
            and _float_or_none(t.get("total_spent"))
            and float(t.get("total_spent")) > spend_median * 1.6
        ):
            continue
        if extended_filters["rule_break_only"] and not str(t.get("rule_break_tags") or "").strip():
            continue
        if not _trade_bucket_match(t, extended_filters["bucket"]):
            continue
        filtered_trades.append(t)
    trades = filtered_trades
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
    balance_integrity = trades_balance_svc.balance_integrity_for_day(scope_as_of_day, scope_state)
    balance_badges = balance_state_badges(balance_integrity)
    data_trust = trades_data_trust(
        sync_status, guardrail_locked=bool(guardrail.get("locked")), active_day=scope_as_of_day
    )
    sync_badges = sync_state_badges(
        sync_status,
        status_key="status",
        stage_key="stage",
        updated_key="updated_at_human",
    )

    week_total = legacy.week_total_net(scope_as_of_day or d or None)
    running_balance = trades_balance_svc.running_balance_for_day(scope_as_of_day, scope_state)
    totals = trades_balance_svc.summary_totals_for_day(scope_as_of_day, scope_state)
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
    review_coverage = analytics_repo.review_coverage(trades)
    setup_scorecards = analytics_repo.setup_scorecards(trades)
    setup_scorecards = [
        card
        for card in setup_scorecards
        if str(card.get("setup") or "").strip()
        and str(card.get("setup") or "") != "Unlabeled"
        and str(card.get("setup") or "") != "Unknown"
    ]
    mistake_costs = analytics_repo.mistake_costs(trades)
    best_setup = setup_scorecards[0] if setup_scorecards else None
    biggest_leak = mistake_costs[0] if mistake_costs else None
    insight_working, insight_not_working = _trade_insights(
        trades_count=trades_count,
        best_setup=best_setup,
        biggest_leak=biggest_leak,
        review_coverage=review_coverage,
        rows=trades,
    )
    tagging_status = _tagging_status(trades, review_coverage)
    chart_start_balance = (
        float(resolved_scope["starting_balance"])
        if account_scope_mode in {"current", "custom"}
        else history_starting_balance
    )
    performance = analytics_repo.performance_metrics(trades, starting_balance=chart_start_balance)
    equity_curve_points = _build_equity_curve(trades, starting_balance=chart_start_balance)
    equity_current_balance = (
        float(equity_curve_points[-1]["value"])
        if equity_curve_points
        else float(running_balance or 0.0)
    )
    equity_peak_balance = max(
        (float(point.get("peak") or 0.0) for point in equity_curve_points),
        default=equity_current_balance,
    )
    max_drawdown = max(
        (abs(float(point.get("drawdown") or 0.0)) for point in equity_curve_points),
        default=0.0,
    )
    current_drawdown = max(
        0.0, float(equity_peak_balance or 0.0) - float(equity_current_balance or 0.0)
    )
    max_drawdown_pct = max(
        (abs(float(point.get("drawdown_pct") or 0.0)) for point in equity_curve_points),
        default=0.0,
    )
    current_drawdown_pct = (
        (current_drawdown / float(equity_peak_balance) * 100.0)
        if float(equity_peak_balance or 0.0) > 0
        else 0.0
    )
    recovery_pct = (
        max(0.0, min(100.0, (1.0 - (current_drawdown / max_drawdown)) * 100.0))
        if max_drawdown > 0
        else 100.0
    )
    hero_chart_stats = {
        "peak_equity": round(float(equity_peak_balance or 0.0), 2),
        "current_equity": round(float(equity_current_balance or 0.0), 2),
        "max_drawdown": round(float(max_drawdown or 0.0), 2),
        "current_drawdown": round(float(current_drawdown or 0.0), 2),
        "max_drawdown_pct": round(float(max_drawdown_pct or 0.0), 2),
        "current_drawdown_pct": round(float(current_drawdown_pct or 0.0), 2),
        "recovery_pct": round(float(recovery_pct or 0.0), 1),
    }
    setup_performance_points = _build_setup_performance(setup_scorecards)
    time_of_day_points = _build_time_of_day_performance(trades)
    risk_by_day_points = _build_risk_by_day(trades)
    stop_respect_points = _build_stop_respect_by_setup(trades)
    equity_takeaway = _equity_takeaway(equity_curve_points)
    setup_takeaway = _setup_takeaway(setup_performance_points)
    time_takeaway = _time_takeaway(time_of_day_points)
    risk_strip = _risk_kpi_strip(trades, max_drawdown)
    stop_respected_item = next(
        (item for item in risk_strip if item["label"] == "Stop Respected"), None
    )
    stop_respected_pct = None
    if stop_respected_item:
        try:
            stop_respected_pct = float(str(stop_respected_item["value"]).replace("%", "").strip())
        except Exception:
            stop_respected_pct = None
    circular_stat_spotlight = _circular_stat_spotlight(
        win_rate=win_rate,
        stop_respected_pct=stop_respected_pct,
        review_completion_pct=float(review_coverage.get("completion_pct") or 0.0),
        recovery_pct=recovery_pct,
    )
    account_health_strip = _account_health_strip(
        resolved_scope=resolved_scope,
        rows=trades,
        net_pl=day_net,
        max_drawdown=max_drawdown,
        current_drawdown=current_drawdown,
    )
    day_of_week_options = []
    seen_days = set()
    for row in trades:
        label = str(row.get("day_of_week") or "").strip()
        if label and label not in seen_days:
            seen_days.add(label)
            day_of_week_options.append(label)
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

    if trades_count == 0:
        next_action_msg = (
            "Import statement or add first trade, then complete setup/session review tags."
        )
    elif review_coverage["fully_reviewed"] < trades_count:
        next_action_msg = f"Complete structured reviews on {trades_count - review_coverage['fully_reviewed']} trade(s) so the analytics layer stays decision-grade."
    elif biggest_leak:
        next_action_msg = f"Review {biggest_leak['tag']} first. It has logged {legacy.money(biggest_leak['loss_cost'])} in preventable loss cost."
    else:
        next_action_msg = "Review is current. Use Analytics to validate the strongest setup before adding more size."
    if guardrail.get("locked"):
        hero_title = "Protect Capital and Review"
        hero_blurb = "The session is in protection mode. Audit the tape, lock in lessons, and avoid new risk."
    elif trades_count == 0:
        hero_title = "Prime"
        hero_blurb = "Open with a clean record: import, tag, and define the first valid setup before pace increases."
    elif day_net > 0 and win_rate >= 60:
        hero_title = "Protect"
        hero_blurb = "Results are working. Keep the quality bar high and avoid giving back edge through boredom."
    elif day_net < 0:
        hero_title = "Tighten Risk and Review Fast"
        hero_blurb = "Pressure is rising. Shrink the decision tree, fix the misses, and only keep A-grade intent on."
    else:
        hero_title = "Focus"
        hero_blurb = "The read is mixed. Stay on the brief, cut the noise, and wait for the cleanest setup."

    trades_status_badges = [
        StateBadgeViewModel(
            label="Execution + sync",
            value=("Review only" if guardrail.get("locked") else "Execution live"),
            tone=("critical" if guardrail.get("locked") else "healthy"),
            title="Current execution mode for the trades surface.",
        ),
        StateBadgeViewModel(
            label="In Scope",
            value=(f"{trades_count} trades" if trades_count else "No trades"),
            tone=("healthy" if trades_count else "caution"),
            title="Visible trades in the current date/search filter.",
        ),
        StateBadgeViewModel(
            label="Confidence",
            value=(
                f"{review_coverage['completion_pct']:.0f}% complete" if trades_count else "Stand by"
            ),
            tone=(
                "healthy"
                if trades_count and review_coverage["fully_reviewed"] == trades_count
                else "caution" if trades_count else "neutral"
            ),
            title="Trade review tags should be completed before day end.",
        ),
    ]
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
    summary_bar = [
        {"label": primary_net_label, "value": money(day_net), "meta": primary_net_sub},
        {
            "label": "Avg R",
            "value": (
                f"{(sum(float(t['r_multiple']) for t in trades if _float_or_none(t.get('r_multiple')) is not None) / max(1, len([t for t in trades if _float_or_none(t.get('r_multiple')) is not None]))):.2f}R"
                if any(_float_or_none(t.get("r_multiple")) is not None for t in trades)
                else "—"
            ),
            "meta": "realized R",
        },
        {
            "label": "Profit Factor",
            "value": (
                f"{float(performance.get('profit_factor') or 0.0):.2f}"
                if performance.get("profit_factor") is not None
                else "—"
            ),
            "meta": "gross profit / gross loss",
        },
        {
            "label": "Avg Risk",
            "value": next(
                (item["value"] for item in risk_strip if item["label"] == "Avg Risk"), "—"
            ),
            "meta": "planned per trade",
        },
        {
            "label": "Avg Winner",
            "value": next(
                (item["value"] for item in risk_strip if item["label"] == "Avg Winner"), "—"
            ),
            "meta": "positive trade",
        },
        {
            "label": "Avg Loser",
            "value": next(
                (item["value"] for item in risk_strip if item["label"] == "Avg Loser"), "—"
            ),
            "meta": "absolute loss",
        },
    ]
    context_query = urlencode(
        {key: value for key, value in legacy.request.args.items() if value not in (None, "", [])}
    )
    base_query = {
        key: value
        for key, value in legacy.request.args.items()
        if value not in (None, "", []) and key not in {"page", "per"}
    }
    filter_query = urlencode(base_query)
    pagination_query_prefix = (filter_query + "&") if filter_query else ""
    bucket_base_query = {key: value for key, value in base_query.items() if key != "bucket"}
    clear_bucket_query = urlencode(bucket_base_query)
    scope_link_base = {
        key: value
        for key, value in base_query.items()
        if key not in {"start_date", "end_date", "scope_preset", "d", "account_scope_mode"}
    }

    def _build_scope_query(**overrides):
        payload = dict(scope_link_base)
        for key, value in overrides.items():
            if value in (None, "", False):
                payload.pop(key, None)
            else:
                payload[key] = value
        return urlencode(payload)

    scope_links = {
        "today": _build_scope_query(
            account_scope_mode="custom",
            start_date=today_iso(),
            end_date=today_iso(),
            scope_preset="custom",
        ),
        "all": _build_scope_query(
            account_scope_mode="all", scope_preset=None, start_date=None, end_date=None
        ),
        "current": _build_scope_query(
            account_scope_mode="current",
            scope_preset=None,
            start_date=None,
            end_date=None,
        ),
        "custom": _build_scope_query(account_scope_mode="custom"),
        "7d": _build_scope_query(
            account_scope_mode="custom", scope_preset="7d", start_date=None, end_date=None
        ),
        "30d": _build_scope_query(
            account_scope_mode="custom", scope_preset="30d", start_date=None, end_date=None
        ),
        "90d": _build_scope_query(
            account_scope_mode="custom", scope_preset="90d", start_date=None, end_date=None
        ),
        "ytd": _build_scope_query(
            account_scope_mode="custom", scope_preset="ytd", start_date=None, end_date=None
        ),
    }
    active_filter_chips = []
    active_filter_chips.append({"label": "Scope", "value": resolved_scope["label"]})
    if account_scope_mode == "custom" and start_date and end_date and start_date == end_date:
        active_filter_chips.append({"label": "Date", "value": start_date})
    elif account_scope_mode == "custom" and (start_date or end_date):
        active_filter_chips.append(
            {"label": "Range", "value": f"{start_date or '…'} → {end_date or '…'}"}
        )
    if q:
        active_filter_chips.append({"label": "Search", "value": q})
    if review_filters.get("setup"):
        active_filter_chips.append({"label": "Setup", "value": review_filters["setup"]})
    if review_filters.get("session"):
        active_filter_chips.append({"label": "Session", "value": review_filters["session"]})
    if review_filters.get("outcome"):
        active_filter_chips.append({"label": "Outcome", "value": review_filters["outcome"].title()})
    if review_filters.get("time_block"):
        active_filter_chips.append({"label": "Window", "value": review_filters["time_block"]})
    if review_filters.get("mistake_tag"):
        active_filter_chips.append({"label": "Mistake", "value": review_filters["mistake_tag"]})
    for key, label in [
        ("day_of_week", "Day"),
        ("instrument", "Instrument"),
        ("side", "Side"),
        ("review_state", "Review"),
        ("trade_grade", "Grade"),
        ("classification", "Class"),
        ("risk_tier", "Risk Tier"),
        ("stop_discipline", "Stop"),
        ("execution_quality", "Execution"),
        ("outcome_quality", "Outcome"),
    ]:
        if extended_filters.get(key):
            active_filter_chips.append({"label": label, "value": str(extended_filters[key])})
    if extended_filters.get("bucket"):
        active_filter_chips.append(
            {"label": "Bucket", "value": str(extended_filters["bucket"]).replace("_", " ").title()}
        )
    review_queue = _review_queue(trades)

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
        review_filters=review_filters,
        review_coverage=review_coverage,
        insight_working=insight_working,
        insight_not_working=insight_not_working,
        tagging_status=tagging_status,
        best_setup=best_setup,
        biggest_leak=biggest_leak,
        summary_bar=summary_bar,
        circular_stat_spotlight=circular_stat_spotlight,
        account_health_strip=account_health_strip,
        risk_strip=risk_strip,
        hero_chart_stats=hero_chart_stats,
        equity_curve_points=equity_curve_points,
        equity_current_balance=equity_current_balance,
        equity_peak_balance=equity_peak_balance,
        setup_performance_points=setup_performance_points,
        time_of_day_points=time_of_day_points,
        risk_by_day_points=risk_by_day_points,
        stop_respect_points=stop_respect_points,
        equity_takeaway=equity_takeaway,
        setup_takeaway=setup_takeaway,
        time_takeaway=time_takeaway,
        max_drawdown=max_drawdown,
        current_drawdown=current_drawdown,
        max_drawdown_pct=max_drawdown_pct,
        current_drawdown_pct=current_drawdown_pct,
        recovery_pct=recovery_pct,
        day_of_week_options=day_of_week_options,
        guardrail=guardrail,
        data_trust=data_trust,
        trades_status_badges=trades_status_badges,
        balance_integrity=balance_integrity,
        balance_badges=balance_badges,
        sync_badges=sync_badges,
        account_scope=account_scope,
        history_starting_balance=history_starting_balance,
        primary_net_label=primary_net_label,
        primary_net_sub=primary_net_sub,
        secondary_total_label=secondary_total_label,
        secondary_total_value=secondary_total_value,
        hero_title=hero_title,
        hero_blurb=hero_blurb,
        active_filter_chips=active_filter_chips,
        today_scope_date=today_iso(),
        review_queue=review_queue,
        filter_query=context_query,
        pagination_query_prefix=pagination_query_prefix,
        clear_bucket_query=clear_bucket_query,
        scope_links=scope_links,
        account_scope_mode=account_scope_mode,
        resolved_scope=resolved_scope,
        advanced_filters_active=advanced_filters_active,
    )

    return legacy.render_page(content, active="trades")
