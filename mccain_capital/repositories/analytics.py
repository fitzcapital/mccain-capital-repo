"""Analytics repository and metric helpers."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from mccain_capital.runtime import db, get_setting_float


def fetch_analytics_rows(start_date: str = "", end_date: str = "") -> List[Dict[str, Any]]:
    where: List[str] = []
    params: List[Any] = []
    if start_date:
        where.append("t.trade_date >= ?")
        params.append(start_date)
    if end_date:
        where.append("t.trade_date <= ?")
        params.append(end_date)

    sql = """
        SELECT
          t.id,
          t.trade_date,
          t.entry_time,
          t.ticker,
          t.net_pl,
          t.balance,
          COALESCE(NULLIF(r.strategy_label, ''), NULLIF(s.title, ''), NULLIF(r.setup_tag, ''), '') AS setup_tag,
          r.session_tag,
          r.checklist_score,
          r.rule_break_tags
        FROM trades t
        LEFT JOIN trade_reviews r ON r.trade_id = t.id
        LEFT JOIN strategies s ON s.id = r.strategy_id
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY t.trade_date ASC, t.id ASC"

    with db() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _streaks(net_values: Iterable[float]) -> tuple[int, int]:
    win_streak = 0
    loss_streak = 0
    max_win_streak = 0
    max_loss_streak = 0
    for n in net_values:
        if n > 0:
            win_streak += 1
            loss_streak = 0
        elif n < 0:
            loss_streak += 1
            win_streak = 0
        else:
            win_streak = 0
            loss_streak = 0
        max_win_streak = max(max_win_streak, win_streak)
        max_loss_streak = max(max_loss_streak, loss_streak)
    return max_win_streak, max_loss_streak


def _derived_equity_curve(rows: List[Dict[str, Any]]) -> List[float]:
    running = float(get_setting_float("starting_balance", 50000.0))
    curve: List[float] = []
    for r in rows:
        n = _safe_float(r.get("net_pl"))
        if n is not None:
            running += n
        curve.append(running)
    return curve


def _max_drawdown(rows: List[Dict[str, Any]]) -> float:
    series = _derived_equity_curve(rows)
    if not series:
        return 0.0

    peak = float("-inf")
    max_dd = 0.0
    for v in series:
        peak = max(peak, v)
        max_dd = max(max_dd, peak - v)
    return max_dd


def performance_metrics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    net_values = [n for n in (_safe_float(r.get("net_pl")) for r in rows) if n is not None]
    total_trades = len(net_values)
    wins = [n for n in net_values if n > 0]
    losses = [n for n in net_values if n < 0]
    breakeven = len([n for n in net_values if n == 0])

    gross_profit = sum(wins)
    gross_loss_abs = abs(sum(losses))
    total_net = sum(net_values)
    profit_factor = (gross_profit / gross_loss_abs) if gross_loss_abs > 0 else None
    expectancy = (total_net / total_trades) if total_trades else 0.0
    avg_win = (sum(wins) / len(wins)) if wins else 0.0
    avg_loss_abs = (abs(sum(losses)) / len(losses)) if losses else 0.0
    max_win = max(wins) if wins else 0.0
    max_loss = min(losses) if losses else 0.0
    win_rate = (len(wins) / total_trades * 100.0) if total_trades else 0.0
    max_win_streak, max_loss_streak = _streaks(net_values)
    max_drawdown = _max_drawdown(rows)

    return {
        "total_trades": total_trades,
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": breakeven,
        "win_rate": win_rate,
        "total_net": total_net,
        "gross_profit": gross_profit,
        "gross_loss_abs": gross_loss_abs,
        "profit_factor": profit_factor,
        "expectancy": expectancy,
        "avg_win": avg_win,
        "avg_loss_abs": avg_loss_abs,
        "max_win": max_win,
        "max_loss": max_loss,
        "max_win_streak": max_win_streak,
        "max_loss_streak": max_loss_streak,
        "max_drawdown": max_drawdown,
    }


def group_table(rows: List[Dict[str, Any]], key_name: str) -> List[Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        key = (r.get(key_name) or "").strip() or "Unlabeled"
        out.setdefault(key, {"count": 0, "wins": 0, "net": 0.0, "scores": []})
        out[key]["count"] += 1
        net = _safe_float(r.get("net_pl")) or 0.0
        out[key]["net"] += net
        if net > 0:
            out[key]["wins"] += 1
        score = _safe_float(r.get("checklist_score"))
        if score is not None:
            out[key]["scores"].append(score)

    table: List[Dict[str, Any]] = []
    for k, v in out.items():
        c = v["count"] or 1
        expectancy = v["net"] / c if c else 0.0
        table.append(
            {
                "k": k,
                "count": v["count"],
                "net": v["net"],
                "win_rate": (v["wins"] / c) * 100.0,
                "expectancy": expectancy,
                "avg_score": (sum(v["scores"]) / len(v["scores"])) if v["scores"] else None,
            }
        )
    table.sort(key=lambda x: x["net"], reverse=True)
    return table


def hour_bucket_table(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def key_fn(r: Dict[str, Any]) -> str:
        s = (r.get("entry_time") or "").strip().upper()
        m = datetime.strptime(s, "%I:%M %p") if s else None
        if not m:
            return "Unknown"
        return f"{m.hour:02d}:00"

    enriched: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        try:
            rr["hour_bucket"] = key_fn(r)
        except Exception:
            rr["hour_bucket"] = "Unknown"
        enriched.append(rr)
    return group_table(enriched, "hour_bucket")


def rule_break_counts(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    c: Counter[str] = Counter()
    for r in rows:
        tags = (r.get("rule_break_tags") or "").strip()
        if not tags:
            continue
        for tag in [t.strip().lower() for t in tags.split(",") if t.strip()]:
            if tag == "ultra-short-hold":
                continue
            c[tag] += 1
    return [{"tag": k, "count": v} for k, v in c.most_common(12)]


def drawdown_diagnostics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    curve = _derived_equity_curve(rows)

    peak = float("-inf")
    current_dd = 0.0
    max_dd = 0.0
    current_dd_streak = 0
    max_dd_streak = 0
    for v in curve:
        if v >= peak:
            peak = v
            current_dd = 0.0
            current_dd_streak = 0
        else:
            current_dd = peak - v
            current_dd_streak += 1
            max_dd = max(max_dd, current_dd)
            max_dd_streak = max(max_dd_streak, current_dd_streak)

    return {
        "current_drawdown": current_dd,
        "max_drawdown": max_dd,
        "current_drawdown_streak": current_dd_streak,
        "max_drawdown_streak": max_dd_streak,
    }


def score_pnl_correlation(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    pairs: List[tuple[float, float]] = []
    for r in rows:
        score = _safe_float(r.get("checklist_score"))
        net = _safe_float(r.get("net_pl"))
        if score is None or net is None:
            continue
        pairs.append((score, net))

    n = len(pairs)
    if n < 3:
        return {"n": n, "r": None, "label": "Not enough scored trades"}

    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    cov = sum((x - mean_x) * (y - mean_y) for x, y in pairs)
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    if var_x <= 0 or var_y <= 0:
        return {"n": n, "r": None, "label": "Flat data"}
    r = cov / ((var_x * var_y) ** 0.5)
    if r >= 0.6:
        label = "Strong positive"
    elif r >= 0.3:
        label = "Moderate positive"
    elif r > -0.3:
        label = "Weak / none"
    elif r > -0.6:
        label = "Moderate negative"
    else:
        label = "Strong negative"
    return {"n": n, "r": r, "label": label}


def edge_over_time(
    rows: List[Dict[str, Any]], key_name: str, top_n: int = 3
) -> List[Dict[str, Any]]:
    grouped = group_table(rows, key_name)
    top_keys = [r["k"] for r in grouped[:top_n]]
    if not top_keys:
        return []

    period_map: Dict[tuple[str, str], Dict[str, Any]] = {}
    for r in rows:
        key = (r.get(key_name) or "").strip() or "Unlabeled"
        if key not in top_keys:
            continue
        trade_date = (r.get("trade_date") or "").strip()
        period = trade_date[:7] if len(trade_date) >= 7 else "Unknown"
        bucket_key = (key, period)
        entry = period_map.setdefault(
            bucket_key, {"key": key, "period": period, "count": 0, "wins": 0, "net": 0.0}
        )
        net = _safe_float(r.get("net_pl")) or 0.0
        entry["count"] += 1
        entry["net"] += net
        if net > 0:
            entry["wins"] += 1

    out: List[Dict[str, Any]] = []
    for e in period_map.values():
        c = int(e["count"] or 0)
        out.append(
            {
                "key": e["key"],
                "period": e["period"],
                "count": c,
                "net": float(e["net"] or 0.0),
                "expectancy": (float(e["net"] or 0.0) / c) if c else 0.0,
                "win_rate": ((int(e["wins"] or 0) / c) * 100.0) if c else 0.0,
            }
        )
    out.sort(key=lambda x: (x["key"], x["period"]))
    return out


def equity_curve_series(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    curve = _derived_equity_curve(rows)
    out: List[Dict[str, Any]] = []
    for idx, (r, v) in enumerate(zip(rows, curve), start=1):
        out.append(
            {
                "i": idx,
                "label": f"{r.get('trade_date', '')} #{r.get('id', idx)}",
                "v": v,
            }
        )
    return out


def drawdown_curve_series(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    curve = _derived_equity_curve(rows)

    out: List[Dict[str, Any]] = []
    peak = float("-inf")
    for idx, v in enumerate(curve, start=1):
        peak = max(peak, v)
        out.append({"i": idx, "label": str(idx), "v": peak - v})
    return out


def expectancy_trend_series(
    rows: List[Dict[str, Any]], granularity: str = "monthly"
) -> List[Dict[str, Any]]:
    g = (granularity or "monthly").strip().lower()
    if g not in {"monthly", "weekly"}:
        g = "monthly"

    def _period(d: str) -> str:
        if len(d) < 10:
            return "Unknown"
        if g == "monthly":
            return d[:7]
        try:
            dt = datetime.strptime(d[:10], "%Y-%m-%d")
            iso = dt.isocalendar()
            return f"{iso.year}-W{iso.week:02d}"
        except Exception:
            return "Unknown"

    agg: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        d = str(r.get("trade_date") or "")
        period = _period(d)
        bucket = agg.setdefault(period, {"count": 0, "net": 0.0})
        bucket["count"] += 1
        bucket["net"] += _safe_float(r.get("net_pl")) or 0.0

    out: List[Dict[str, Any]] = []
    for period in sorted(agg.keys()):
        c = int(agg[period]["count"] or 0)
        n = float(agg[period]["net"] or 0.0)
        out.append({"label": period, "v": (n / c) if c else 0.0, "count": c})
    return out


def spx_benchmark_series(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Benchmark curve: only applies P/L from SPX ticker rows, keeping other rows flat.
    This provides a simple in-book benchmark overlay without external market data.
    """
    running = float(get_setting_float("starting_balance", 50000.0))
    out: List[Dict[str, Any]] = []
    for idx, r in enumerate(rows, start=1):
        ticker = str(r.get("ticker") or "").strip().upper()
        if ticker == "SPX":
            n = _safe_float(r.get("net_pl"))
            if n is not None:
                running += n
        out.append(
            {
                "i": idx,
                "label": f"{r.get('trade_date', '')} #{r.get('id', idx)}",
                "v": running,
            }
        )
    return out


def volatility_regime_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Day volatility proxy from internal book data:
      - daily_abs = abs(sum(net_pl) per day)
      - rolling_5d_abs = 5-day rolling average of daily_abs
      - regime = LOW / NORMAL / HIGH using 33/66 percentile thresholds
    """
    by_day: Dict[str, float] = {}
    for r in rows:
        d = str(r.get("trade_date") or "").strip()
        if not d:
            continue
        by_day[d] = by_day.get(d, 0.0) + float(_safe_float(r.get("net_pl")) or 0.0)
    if not by_day:
        return {
            "regime": "NO DATA",
            "current": 0.0,
            "p33": 0.0,
            "p66": 0.0,
            "series": [],
        }

    days = sorted(by_day.keys())
    abs_vals = [abs(float(by_day[d])) for d in days]
    rolling: List[float] = []
    for i in range(len(abs_vals)):
        start = max(0, i - 4)
        window = abs_vals[start : i + 1]
        rolling.append(sum(window) / len(window))

    sorted_roll = sorted(rolling)
    i33 = int((len(sorted_roll) - 1) * 0.33)
    i66 = int((len(sorted_roll) - 1) * 0.66)
    p33 = float(sorted_roll[i33])
    p66 = float(sorted_roll[i66])
    current = float(rolling[-1])
    if current <= p33:
        regime = "LOW"
    elif current <= p66:
        regime = "NORMAL"
    else:
        regime = "HIGH"

    series = [{"label": d, "v": float(v)} for d, v in zip(days, rolling)]
    return {"regime": regime, "current": current, "p33": p33, "p66": p66, "series": series}


def _is_fitz_22_rev_setup(tag: str) -> bool:
    t = (tag or "").strip().lower()
    if not t:
        return False
    has_22 = ("2-2" in t) or ("2 2" in t) or (t == "22") or (" 22 " in f" {t} ")
    has_rev = ("reversal" in t) or ("rev" in t)
    # Keep matching broad enough for short trader shorthand like "2-2".
    return has_22 and (has_rev or t in {"2-2", "22"})


def fitz_22_rev_indicator(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    tagged = [r for r in rows if _is_fitz_22_rev_setup(str(r.get("setup_tag") or ""))]
    net_values = [float(_safe_float(r.get("net_pl")) or 0.0) for r in tagged]
    trades = len(net_values)
    wins = len([n for n in net_values if n > 0])
    total_net = sum(net_values)
    win_rate = (wins / trades * 100.0) if trades else 0.0
    expectancy = (total_net / trades) if trades else 0.0

    recent_slice = net_values[-10:]
    recent_trades = len(recent_slice)
    recent_wins = len([n for n in recent_slice if n > 0])
    recent_net = sum(recent_slice)
    recent_win_rate = (recent_wins / recent_trades * 100.0) if recent_trades else 0.0

    if trades < 5:
        status = "BUILD SAMPLE"
        tone = "neutral"
        note = "Log more tagged 2-2 reversal trades before trusting the signal."
    elif expectancy > 0 and recent_net >= 0 and win_rate >= 50.0:
        status = "IN PLAY"
        tone = "positive"
        note = "2-2 reversal edge is currently healthy. Keep execution quality high."
    elif expectancy > 0:
        status = "WATCH"
        tone = "neutral"
        note = "Edge is positive but unstable. Tighten entry quality and risk discipline."
    else:
        status = "COOL OFF"
        tone = "negative"
        note = "Recent 2-2 reversal outcomes are weak. Reduce size and wait for clean structure."

    return {
        "trades": trades,
        "win_rate": win_rate,
        "expectancy": expectancy,
        "total_net": total_net,
        "recent_trades": recent_trades,
        "recent_win_rate": recent_win_rate,
        "recent_net": recent_net,
        "status": status,
        "tone": tone,
        "note": note,
    }


def integrity_diagnostics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    missing_setup = 0
    missing_session = 0
    missing_score = 0
    sig_counts: Dict[tuple[str, str, str, str], int] = {}

    curve = _derived_equity_curve(rows)
    stale_balance_rows = 0
    for idx, r in enumerate(rows):
        setup = str(r.get("setup_tag") or "").strip()
        session = str(r.get("session_tag") or "").strip()
        score = _safe_float(r.get("checklist_score"))
        if not setup:
            missing_setup += 1
        if not session:
            missing_session += 1
        if score is None:
            missing_score += 1

        sig = (
            str(r.get("trade_date") or ""),
            str(r.get("ticker") or ""),
            str(r.get("entry_time") or ""),
            str(r.get("net_pl") or ""),
        )
        sig_counts[sig] = sig_counts.get(sig, 0) + 1

        bal = _safe_float(r.get("balance"))
        if bal is not None and idx < len(curve) and abs(float(bal) - float(curve[idx])) > 0.01:
            stale_balance_rows += 1

    duplicate_candidates = sum(1 for v in sig_counts.values() if v > 1)
    return {
        "missing_setup": missing_setup,
        "missing_session": missing_session,
        "missing_score": missing_score,
        "duplicate_candidates": duplicate_candidates,
        "stale_balance_rows": stale_balance_rows,
    }


def _entry_time_to_block(entry_time: str) -> str:
    s = (entry_time or "").strip().upper()
    if not s:
        return "Other / Unknown"
    try:
        t = datetime.strptime(s, "%I:%M %p")
    except Exception:
        return "Other / Unknown"
    minutes = t.hour * 60 + t.minute
    if 570 <= minutes < 600:  # 9:30-10:00
        return "09:30-10:00"
    if 600 <= minutes < 660:
        return "10:00-11:00"
    if 660 <= minutes < 720:
        return "11:00-12:00"
    if 720 <= minutes < 780:
        return "12:00-13:00"
    if 780 <= minutes < 840:
        return "13:00-14:00"
    if 840 <= minutes < 900:
        return "14:00-15:00"
    if 900 <= minutes < 960:
        return "15:00-16:00"
    return "Other / Unknown"


def setup_expectancy_heatmap(rows: List[Dict[str, Any]], top_n_setups: int = 5) -> Dict[str, Any]:
    setup_stats: Dict[str, Dict[str, float]] = {}
    for r in rows:
        setup = (r.get("setup_tag") or "").strip() or "Unlabeled"
        net = float(_safe_float(r.get("net_pl")) or 0.0)
        entry = setup_stats.setdefault(setup, {"count": 0.0, "net": 0.0})
        entry["count"] += 1.0
        entry["net"] += net
    top_setups = [
        k
        for k, _ in sorted(
            setup_stats.items(), key=lambda kv: (kv[1]["count"], kv[1]["net"]), reverse=True
        )[:top_n_setups]
    ]
    if not top_setups:
        return {"setups": [], "rows": [], "max_abs_exp": 0.0}

    block_order = [
        "09:30-10:00",
        "10:00-11:00",
        "11:00-12:00",
        "12:00-13:00",
        "13:00-14:00",
        "14:00-15:00",
        "15:00-16:00",
        "Other / Unknown",
    ]
    agg: Dict[tuple[str, str], Dict[str, float]] = {}
    for r in rows:
        setup = (r.get("setup_tag") or "").strip() or "Unlabeled"
        if setup not in top_setups:
            continue
        block = _entry_time_to_block(str(r.get("entry_time") or ""))
        net = float(_safe_float(r.get("net_pl")) or 0.0)
        key = (block, setup)
        bucket = agg.setdefault(key, {"count": 0.0, "net": 0.0})
        bucket["count"] += 1.0
        bucket["net"] += net

    out_rows: List[Dict[str, Any]] = []
    max_abs_exp = 0.0
    for block in block_order:
        cells: List[Dict[str, Any]] = []
        for setup in top_setups:
            bucket = agg.get((block, setup), {"count": 0.0, "net": 0.0})
            count = int(bucket["count"])
            expectancy = (bucket["net"] / count) if count else 0.0
            max_abs_exp = max(max_abs_exp, abs(expectancy))
            cells.append(
                {
                    "setup": setup,
                    "count": count,
                    "net": float(bucket["net"]),
                    "expectancy": float(expectancy),
                }
            )
        out_rows.append({"block": block, "cells": cells})
    return {"setups": top_setups, "rows": out_rows, "max_abs_exp": max_abs_exp}
