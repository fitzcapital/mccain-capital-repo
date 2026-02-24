"""Analytics repository and metric helpers."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from mccain_capital.runtime import db


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
          t.net_pl,
          t.balance,
          r.setup_tag,
          r.session_tag,
          r.checklist_score,
          r.rule_break_tags
        FROM trades t
        LEFT JOIN trade_reviews r ON r.trade_id = t.id
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


def _max_drawdown(rows: List[Dict[str, Any]], net_values: List[float]) -> float:
    balances = [_safe_float(r.get("balance")) for r in rows]
    if any(v is not None for v in balances):
        series = [v for v in balances if v is not None]
    else:
        running = 0.0
        series = []
        for n in net_values:
            running += n
            series.append(running)

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
    max_drawdown = _max_drawdown(rows, net_values)

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
    net_values = [n for n in (_safe_float(r.get("net_pl")) for r in rows) if n is not None]
    balances = [_safe_float(r.get("balance")) for r in rows]
    if any(v is not None for v in balances):
        curve = [v for v in balances if v is not None]
    else:
        running = 0.0
        curve = []
        for n in net_values:
            running += n
            curve.append(running)

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
