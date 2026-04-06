"""Analytics service module."""

from __future__ import annotations

import html
from datetime import datetime
import re
from urllib.parse import urlencode
from typing import Any, Dict, List

from flask import jsonify, render_template, request

from mccain_capital.repositories import analytics as repo
from mccain_capital.repositories import journal as journal_repo
from mccain_capital.repositories import trades as trades_repo
from mccain_capital import runtime as app_runtime
from mccain_capital.runtime import money
from mccain_capital.services import market_data_service
from mccain_capital.services.ui import get_system_status, render_page
from mccain_capital.services.viewmodels import (
    analytics_data_trust,
    balance_state_badges,
    sync_state_badges,
)


def _series_day_from_label(label: str) -> str:
    raw = str(label or "")
    m = re.search(r"\d{4}-\d{2}-\d{2}", raw)
    return m.group(0) if m else ""


def _chart_empty_state() -> str:
    return """
    <div class="chartEmpty">
      <div class="chartEmptyTitle">Not enough data to render chart.</div>
      <div class="chartEmptySub">Need at least 2 data points in this range. Try a wider range or switch to Weekly granularity.</div>
      <div class="chartEmptyActions">
        <a class="btn ctaSecondary" href="/trades/upload/statement">Upload Statement</a>
        <a class="btn ctaLink" href="/analytics?tab=performance&expectancy_granularity=weekly">Use Weekly View</a>
      </div>
    </div>
    """


def _minute_from_label(value: str) -> int:
    raw = str(value or "").strip()
    if not raw:
        return 24 * 60
    for fmt in ("%I:%M %p", "%I:%M:%S %p", "%H:%M"):
        try:
            parsed = datetime.strptime(raw.upper(), fmt)
            return (parsed.hour * 60) + parsed.minute
        except ValueError:
            continue
    return 24 * 60


def _format_compact_clock(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "—"
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=app_runtime.TZ)
    return dt.astimezone(app_runtime.TZ).strftime("%I:%M %p ET").lstrip("0")


def _intraday_points_for_day(symbol: str, day: str) -> List[Dict[str, Any]]:
    try:
        rows = market_data_service.get_intraday(symbol)
    except Exception:
        rows = []
    points: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ts_raw = str(row.get("ts") or "").strip()
        close_v = row.get("close")
        if not ts_raw or not isinstance(close_v, (int, float)):
            continue
        try:
            dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=app_runtime.TZ)
        dt_et = dt.astimezone(app_runtime.TZ)
        if dt_et.date().isoformat() != day:
            continue
        points.append(
            {
                "minute": (dt_et.hour * 60) + dt_et.minute,
                "ts": dt_et.isoformat(),
                "label": dt_et.strftime("%I:%M %p ET").lstrip("0"),
                "close": float(close_v),
            }
        )
    return points


def _nearest_intraday_snapshot(points: List[Dict[str, Any]], minute: int) -> Dict[str, Any] | None:
    if not points or minute >= 24 * 60:
        return None
    return min(points, key=lambda row: abs(int(row.get("minute") or 0) - minute))


def _market_arc_summary(day: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for symbol in ("SPX", "VIX"):
        points = _intraday_points_for_day(symbol, day)
        if not points:
            rows.append(
                {
                    "label": symbol,
                    "available": False,
                    "summary": "No same-day intraday curve available.",
                    "detail": "Provider did not return a usable same-day curve.",
                }
            )
            continue
        start = float(points[0]["close"])
        end = float(points[-1]["close"])
        highs = [float(p["close"]) for p in points]
        low = min(highs)
        high = max(highs)
        delta = end - start
        pct = (delta / start * 100.0) if abs(start) > 1e-9 else 0.0
        rows.append(
            {
                "label": symbol,
                "available": True,
                "summary": f"{start:,.2f} open to {end:,.2f} close · {delta:+.2f} ({pct:+.2f}%)",
                "detail": f"Session range {low:,.2f}-{high:,.2f} across {len(points)} intraday points.",
            }
        )
    return rows


def _macro_timeline_items(day: str) -> List[Dict[str, Any]]:
    try:
        from mccain_capital.services import core as core_svc
    except Exception:
        return []
    try:
        anchor = datetime.strptime(day, "%Y-%m-%d").date()
    except ValueError:
        return []
    overlay = core_svc._forex_factory_usd_window_events(anchor, anchor)
    items: List[Dict[str, Any]] = []
    for event in list((overlay.get("events_by_day") or {}).get(day, [])):
        if not isinstance(event, dict):
            continue
        impact = str(event.get("impact") or "").strip().lower()
        items.append(
            {
                "kind": "macro",
                "sort_minute": _minute_from_label(str(event.get("time_label") or "")),
                "time_label": str(event.get("time_label") or "Macro window"),
                "headline": str(event.get("title") or "USD macro event"),
                "summary": str(event.get("tooltip") or event.get("impact") or "Calendar event"),
                "tone": "negative" if impact == "high" else "warm",
                "impact_label": str(event.get("impact") or "Scheduled"),
            }
        )
    return items


def _line_chart_svg(series: List[Dict[str, Any]], stroke: str, y_prefix: str = "$") -> str:
    if len(series) < 2:
        return _chart_empty_state()

    width = 820.0
    height = 200.0
    pad = 18.0
    inner_height = height - (2 * pad)

    values = [float(p.get("v") or 0.0) for p in series]
    min_v = min(values)
    max_v = max(values)
    if abs(max_v - min_v) < 1e-9:
        max_v = min_v + 1.0

    def sx(i: int) -> float:
        return pad + (i / (len(values) - 1)) * (width - (2 * pad))

    def sy(v: float) -> float:
        return height - pad - ((v - min_v) / (max_v - min_v)) * (height - (2 * pad))

    points = " ".join(f"{sx(i):.2f},{sy(v):.2f}" for i, v in enumerate(values))
    area_points = (
        f"{pad:.2f},{height - pad:.2f} " + points + f" {width - pad:.2f},{height - pad:.2f}"
    )
    zero_in_range = min_v <= 0.0 <= max_v
    zero_y = sy(0.0) if zero_in_range else None
    latest_label = html.escape(str(series[-1].get("label") or "latest"))
    gradient_id = f"chartArea{re.sub(r'[^a-zA-Z0-9]+', '', stroke)}"
    y_steps = [pad + (inner_height * frac) for frac in (0.0, 0.33, 0.66, 1.0)]
    grid_lines = "".join(
        f'<line x1="{pad:.2f}" y1="{y:.2f}" x2="{width - pad:.2f}" y2="{y:.2f}" '
        f'class="chartGridLine" />'
        for y in y_steps
    )

    points_markup = []
    for i, p in enumerate(series):
        label = html.escape(str(p.get("label") or f"#{i+1}"))
        value = float(p.get("v") or 0.0)
        day = _series_day_from_label(str(p.get("label") or ""))
        points_markup.append(
            (
                f'<circle class="chartPoint" cx="{sx(i):.2f}" cy="{sy(values[i]):.2f}" r="5.0" '
                f'fill="transparent" data-label="{label}" data-value="{value:.2f}" data-prefix="{html.escape(y_prefix)}" '
                f'data-day="{html.escape(day)}" />'
            )
        )
    return f"""
    <svg viewBox="0 0 {int(width)} {int(height)}" role="img" aria-label="analytics line chart" style="width:100%;height:auto;display:block">
      <defs>
        <linearGradient id="{gradient_id}" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="{stroke}" stop-opacity="0.22" />
          <stop offset="100%" stop-color="{stroke}" stop-opacity="0.02" />
        </linearGradient>
      </defs>
      <rect x="0" y="0" width="{int(width)}" height="{int(height)}" fill="rgba(4,10,20,.26)" rx="10" />
      {grid_lines}
      {f'<line x1="{pad}" y1="{zero_y:.2f}" x2="{width - pad}" y2="{zero_y:.2f}" class="chartZeroLine" />' if zero_y is not None else ""}
      <polygon class="chartArea" fill="url(#{gradient_id})" points="{area_points}" />
      <polyline class="chartLine" fill="none" stroke="{stroke}" stroke-width="3" points="{points}" />
      <circle class="chartLastPoint" cx="{sx(len(values) - 1):.2f}" cy="{sy(values[-1]):.2f}" r="4.5" fill="{stroke}" />
      {''.join(points_markup)}
    </svg>
    <div class="chartMeta">
      <span>Range: {y_prefix}{min_v:,.2f} → {y_prefix}{max_v:,.2f}</span>
      <span>Latest: {latest_label} ({y_prefix}{values[-1]:,.2f})</span>
    </div>
    """


def _multi_line_chart_svg(series_list: List[Dict[str, Any]], y_prefix: str = "$") -> str:
    active = [s for s in series_list if isinstance(s.get("series"), list) and s.get("series")]
    if not active:
        return _chart_empty_state()
    if all(len(s["series"]) < 2 for s in active):
        return _chart_empty_state()

    width = 820.0
    height = 210.0
    pad = 18.0
    inner_height = height - (2 * pad)

    all_vals: List[float] = []
    for s in active:
        all_vals.extend(float(p.get("v") or 0.0) for p in s["series"])
    min_v = min(all_vals)
    max_v = max(all_vals)
    if abs(max_v - min_v) < 1e-9:
        max_v = min_v + 1.0

    def sx(i: int, n: int) -> float:
        den = max(1, n - 1)
        return pad + (i / den) * (width - (2 * pad))

    def sy(v: float) -> float:
        return height - pad - ((v - min_v) / (max_v - min_v)) * (height - (2 * pad))

    lines: List[str] = []
    dots: List[str] = []
    point_hits: List[str] = []
    legend_items: List[str] = []
    y_steps = [pad + (inner_height * frac) for frac in (0.0, 0.33, 0.66, 1.0)]
    grid_lines = "".join(
        f'<line x1="{pad:.2f}" y1="{y:.2f}" x2="{width - pad:.2f}" y2="{y:.2f}" '
        f'class="chartGridLine" />'
        for y in y_steps
    )
    for s in active:
        color = str(s.get("color") or "#35d4ff")
        name = html.escape(str(s.get("name") or "Series"))
        vals = [float(p.get("v") or 0.0) for p in s["series"]]
        pts = " ".join(f"{sx(i, len(vals)):.2f},{sy(v):.2f}" for i, v in enumerate(vals))
        lines.append(
            f'<polyline class="chartLine" fill="none" stroke="{color}" stroke-width="3" points="{pts}" />'
        )
        dots.append(
            f'<circle cx="{sx(len(vals)-1, len(vals)):.2f}" cy="{sy(vals[-1]):.2f}" r="4.5" fill="{color}" />'
        )
        for i, p in enumerate(s["series"]):
            label = html.escape(str(p.get("label") or f"#{i+1}"))
            day = _series_day_from_label(str(p.get("label") or ""))
            point_hits.append(
                (
                    f'<circle class="chartPoint" cx="{sx(i, len(vals)):.2f}" cy="{sy(vals[i]):.2f}" r="5.0" '
                    f'fill="transparent" data-label="{label}" data-value="{vals[i]:.2f}" data-prefix="{html.escape(y_prefix)}" '
                    f'data-series="{name}" data-day="{html.escape(day)}" />'
                )
            )
        legend_items.append(
            f'<span class="trendChip"><span style="display:inline-block;width:10px;height:10px;border-radius:999px;background:{color};margin-right:6px;"></span>{name}: {y_prefix}{vals[-1]:,.2f}</span>'
        )

    return f"""
    <svg viewBox="0 0 {int(width)} {int(height)}" role="img" aria-label="analytics multi line chart" style="width:100%;height:auto;display:block">
      <rect x="0" y="0" width="{int(width)}" height="{int(height)}" fill="rgba(4,10,20,.26)" rx="10" />
      {grid_lines}
      {''.join(lines)}
      {''.join(f'<g class="chartLastPoint">{dot}</g>' for dot in dots)}
      {''.join(point_hits)}
    </svg>
    <div class="trendChips">{''.join(legend_items)}</div>
    <div class="chartMeta"><span>Range: {y_prefix}{min_v:,.2f} → {y_prefix}{max_v:,.2f}</span></div>
    """


def _series_story(
    series: List[Dict[str, Any]], *, favorable_direction: str = "up"
) -> Dict[str, Any]:
    if not series:
        return {
            "latest": 0.0,
            "prev": None,
            "delta": 0.0,
            "pct": None,
            "direction": "flat",
            "tone": "neutral",
            "label": "No data",
        }

    latest = float(series[-1].get("v") or 0.0)
    prev = float(series[-2].get("v") or 0.0) if len(series) > 1 else None
    delta = latest - prev if prev is not None else 0.0
    pct = ((delta / abs(prev)) * 100.0) if prev not in (None, 0.0) else None
    if delta > 0:
        direction = "up"
    elif delta < 0:
        direction = "down"
    else:
        direction = "flat"

    tone = "neutral"
    if direction != "flat":
        improved = direction == favorable_direction
        tone = "positive" if improved else "negative"

    label = str(series[-1].get("label") or "latest")
    return {
        "latest": latest,
        "prev": prev,
        "delta": delta,
        "pct": pct,
        "direction": direction,
        "tone": tone,
        "label": label,
    }


def _insight_panels(
    perf: Dict[str, Any], dd: Dict[str, Any], corr: Dict[str, Any]
) -> Dict[str, str]:
    expectancy = float(perf.get("expectancy") or 0.0)
    win_rate = float(perf.get("win_rate") or 0.0)
    drawdown_live = float(dd.get("current_drawdown") or 0.0)
    drawdown_streak = int(dd.get("current_drawdown_streak") or 0)
    corr_value = corr.get("r")

    if expectancy > 0 and win_rate >= 50:
        changed = "Positive expectancy with >=50% win rate. Edge is paying with current execution."
    elif expectancy > 0:
        changed = "Expectancy is positive even with mixed hit-rate. Size and loss control are doing heavy lifting."
    else:
        changed = (
            "Expectancy is flat/negative. Recent trade selection or exits are suppressing edge."
        )

    if drawdown_live > 0:
        risk_now = (
            f"Live drawdown is {money(drawdown_live)} over {drawdown_streak} trade(s). "
            "Prioritize A+ setups and cap size until recovery."
        )
    else:
        risk_now = "No active drawdown streak. Risk posture is stable for planned sizing."

    if corr_value is None:
        next_action = "Score more trades consistently to unlock quality-vs-PnL feedback loops."
    elif corr_value >= 0.3:
        next_action = (
            "Lean into high-score setups and sessions; quality currently aligns with outcomes."
        )
    else:
        next_action = "Quality score is not aligned with PnL yet. Rebuild review tags and tighten setup/session definitions."

    return {"changed": changed, "risk_now": risk_now, "next_action": next_action}


def _explain_day(rows: List[Dict[str, Any]], day_iso: str = "") -> Dict[str, Any]:
    if not rows:
        return {
            "day": day_iso or "—",
            "title": "No data",
            "pnl_driver": "No trades available in this range.",
            "risk_driver": "Risk narrative needs at least one completed trade day.",
            "edge_driver": "Edge shift unavailable without day samples.",
        }
    target_day = day_iso or str(max(str(r.get("trade_date") or "") for r in rows))
    day_rows = [r for r in rows if str(r.get("trade_date") or "") == target_day]
    if not day_rows:
        day_rows = rows[-5:]
        target_day = str(day_rows[-1].get("trade_date") or "—")
    nets = [float(r.get("net_pl") or 0.0) for r in day_rows]
    net_total = sum(nets)
    wins = len([n for n in nets if n > 0])
    losses = len([n for n in nets if n < 0])
    biggest_win = max([n for n in nets if n > 0], default=0.0)
    biggest_loss = min([n for n in nets if n < 0], default=0.0)
    by_setup: Dict[str, float] = {}
    for r in day_rows:
        setup = str(r.get("setup_display") or "").strip() or "Unknown"
        by_setup[setup] = by_setup.get(setup, 0.0) + float(r.get("net_pl") or 0.0)
    top_setup = "Unknown"
    top_setup_net = 0.0
    if by_setup:
        top_setup, top_setup_net = sorted(by_setup.items(), key=lambda kv: kv[1], reverse=True)[0]
    pnl_driver = (
        f"Net {money(net_total)} across {len(day_rows)} trades ({wins}W/{losses}L). "
        f"Top setup: {top_setup} ({money(top_setup_net)})."
    )
    risk_driver = (
        f"Largest win {money(biggest_win)} vs largest loss {money(biggest_loss)}. "
        "Concentration in one outlier signals elevated day variance."
    )
    edge_driver = (
        "Edge strengthened on this day."
        if net_total > 0 and wins >= losses
        else "Edge weakened on this day. Tighten setup quality and pace."
    )
    return {
        "day": target_day,
        "title": f"Explain This Day ({target_day})",
        "pnl_driver": pnl_driver,
        "risk_driver": risk_driver,
        "edge_driver": edge_driver,
    }


def _regime_sizing_suggestion(
    *,
    perf: Dict[str, Any],
    dd: Dict[str, Any],
    vol_summary: Dict[str, Any],
    setup_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    regime = str(vol_summary.get("regime") or "NORMAL")
    current_dd = float(dd.get("current_drawdown") or 0.0)
    expectancy = float(perf.get("expectancy") or 0.0)
    base = 1.0
    if regime == "HIGH":
        base *= 0.65
    elif regime == "NORMAL":
        base *= 0.85
    else:
        base *= 1.0
    if current_dd > 0:
        base *= 0.75
    if expectancy < 0:
        base *= 0.80
    top_setup = setup_rows[0] if setup_rows else None
    if (
        top_setup
        and float(top_setup.get("expectancy") or 0.0) > 0
        and float(top_setup.get("win_rate") or 0.0) >= 55.0
    ):
        base *= 1.08
    size_pct = int(round(max(30.0, min(120.0, base * 100.0))))
    if size_pct <= 60:
        action = "Defensive size"
    elif size_pct <= 90:
        action = "Baseline reduced size"
    else:
        action = "Normal size"
    return {
        "regime": regime,
        "size_pct": size_pct,
        "action": action,
        "note": (
            f"Regime {regime}, live drawdown {money(current_dd)}, expectancy {money(expectancy)}. "
            f"Suggested size: {size_pct}% of your normal per-trade risk unit."
        ),
    }


def _what_if_day_simulator(
    rows: List[Dict[str, Any]],
    args: Dict[str, str],
    perf: Dict[str, Any],
    *,
    scope_start: str = "",
    scope_starting_balance: float | None = None,
) -> Dict[str, Any]:
    try:
        max_trades = int(args.get("sim_max_trades") or 5)
    except (TypeError, ValueError):
        max_trades = 5
    try:
        stop_loss_streak = int(args.get("sim_stop_loss_streak") or 2)
    except (TypeError, ValueError):
        stop_loss_streak = 2

    max_trades = max(1, min(30, max_trades))
    stop_loss_streak = max(1, min(8, stop_loss_streak))

    consistency_win_rate = max(0.0, min(100.0, float(perf.get("win_rate") or 0.0)))
    consistency_avg_win = max(0.0, float(perf.get("avg_win") or 0.0))
    consistency_avg_loss = max(0.0, float(perf.get("avg_loss_abs") or 0.0))
    if consistency_avg_win <= 0 and consistency_avg_loss <= 0:
        consistency_avg_win = 1.0

    current_balance = float(
        trades_repo.latest_balance_overall(
            start_date=(scope_start or None),
            starting_balance=scope_starting_balance,
        )
    )

    p_win = consistency_win_rate / 100.0
    p_loss = 1.0 - p_win
    expectancy = (p_win * consistency_avg_win) - (p_loss * consistency_avg_loss)
    projected_full = max_trades * expectancy
    streak_prob = 1.0 - (
        (1.0 - (p_loss**stop_loss_streak)) ** max(1, max_trades - stop_loss_streak + 1)
    )
    projected_with_guardrail = (projected_full * (1.0 - streak_prob)) + (
        (-stop_loss_streak * consistency_avg_loss) * streak_prob
    )

    by_day: Dict[str, Dict[str, float]] = {}
    for r in rows:
        d = str(r.get("trade_date") or "")
        if not d:
            continue
        day = by_day.setdefault(d, {"net": 0.0, "wins": 0.0, "count": 0.0})
        net = float(r.get("net_pl") or 0.0)
        day["net"] += net
        day["count"] += 1.0
        if net > 0:
            day["wins"] += 1.0
    day_stats = list(by_day.values())
    real_avg_day_net = (
        sum(float(x["net"]) for x in day_stats) / len(day_stats) if day_stats else 0.0
    )
    real_avg_win_rate = (
        (
            sum(float(x["wins"]) for x in day_stats)
            / max(1.0, sum(float(x["count"]) for x in day_stats))
        )
        * 100.0
        if day_stats
        else 0.0
    )
    real_avg_trades = (
        sum(float(x["count"]) for x in day_stats) / len(day_stats) if day_stats else 0.0
    )

    return {
        "max_trades": max_trades,
        "current_balance": current_balance,
        "consistency_win_rate": consistency_win_rate,
        "consistency_avg_win": consistency_avg_win,
        "consistency_avg_loss": consistency_avg_loss,
        "stop_loss_streak": stop_loss_streak,
        "expectancy": expectancy,
        "projected_full": projected_full,
        "projected_end_balance": current_balance + projected_full,
        "streak_prob": streak_prob,
        "projected_with_guardrail": projected_with_guardrail,
        "projected_guardrail_end_balance": current_balance + projected_with_guardrail,
        "real_avg_day_net": real_avg_day_net,
        "real_avg_win_rate": real_avg_win_rate,
        "real_avg_trades": real_avg_trades,
        "delta_vs_real": projected_with_guardrail - real_avg_day_net,
    }


def _tone_from_value(value: float, *, inverse: bool = False) -> str:
    if value > 0:
        return "negative" if inverse else "positive"
    if value < 0:
        return "positive" if inverse else "negative"
    return "neutral"


def _progress_value(value: float, *, floor: float = 0.0, ceiling: float = 100.0) -> float:
    return max(floor, min(ceiling, float(value)))


def _series_points(series: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    points: List[Dict[str, Any]] = []
    for idx, point in enumerate(series, start=1):
        label = str(point.get("label") or f"#{idx}")
        points.append(
            {
                "x": label,
                "y": float(point.get("v") or 0.0),
                "day": _series_day_from_label(label),
            }
        )
    return points


def _daily_net_series(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_day: Dict[str, float] = {}
    for row in rows:
        day = str(row.get("trade_date") or "").strip()
        if not day:
            continue
        by_day[day] = by_day.get(day, 0.0) + float(row.get("net_pl") or 0.0)
    return [{"x": day, "y": by_day[day]} for day in sorted(by_day.keys())]


def _daily_review_completion_series(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_day: Dict[str, Dict[str, float]] = {}
    for row in rows:
        day = str(row.get("trade_date") or "").strip()
        if not day:
            continue
        bucket = by_day.setdefault(day, {"total": 0.0, "full": 0.0})
        bucket["total"] += 1.0
        is_full = all(
            [
                bool(str(row.get("setup_tag") or "").strip()),
                bool(str(row.get("session_tag") or "").strip()),
                row.get("checklist_score") not in (None, ""),
                bool(str(row.get("mistake_tags") or "").strip()),
                row.get("planned_risk_dollars") not in (None, ""),
            ]
        )
        if is_full:
            bucket["full"] += 1.0
    return [
        {
            "x": day,
            "y": ((bucket["full"] / bucket["total"]) * 100.0) if bucket["total"] else 0.0,
        }
        for day, bucket in sorted(by_day.items())
    ]


def _daily_trust_series(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_day: Dict[str, Dict[str, float]] = {}
    for row in rows:
        day = str(row.get("trade_date") or "").strip()
        if not day:
            continue
        bucket = by_day.setdefault(day, {"total": 0.0, "healthy": 0.0})
        bucket["total"] += 1.0
        core_complete = all(
            [
                bool(str(row.get("setup_tag") or "").strip()),
                bool(str(row.get("session_tag") or "").strip()),
                row.get("checklist_score") not in (None, ""),
            ]
        )
        if core_complete:
            bucket["healthy"] += 1.0
    return [
        {
            "x": day,
            "y": ((bucket["healthy"] / bucket["total"]) * 100.0) if bucket["total"] else 0.0,
        }
        for day, bucket in sorted(by_day.items())
    ]


def _compact_breakdown(
    items: List[Dict[str, Any]],
    *,
    label_key: str,
    value_key: str,
    top_n: int,
    total_label: str,
    empty_title: str,
    empty_detail: str,
) -> Dict[str, Any]:
    cleaned: List[Dict[str, Any]] = []
    for item in items:
        label = str(item.get(label_key) or "").strip()
        value = float(item.get(value_key) or 0.0)
        if not label or value <= 0:
            continue
        cleaned.append({"label": label, "value": value})
    if not cleaned:
        return {
            "empty": True,
            "labels": [],
            "series": [],
            "center_primary": "0",
            "center_secondary": total_label,
            "dominant": "",
            "empty_title": empty_title,
            "empty_detail": empty_detail,
        }

    top = cleaned[:top_n]
    if len(cleaned) > top_n:
        top.append(
            {
                "label": "Other",
                "value": sum(float(item["value"]) for item in cleaned[top_n:]),
            }
        )
    total = sum(float(item["value"]) for item in cleaned)
    dominant = cleaned[0]["label"]
    return {
        "empty": False,
        "labels": [str(item["label"]) for item in top],
        "series": [float(item["value"]) for item in top],
        "center_primary": str(int(round(total))) if abs(total - round(total)) < 1e-9 else f"{total:.1f}",
        "center_secondary": total_label,
        "dominant": dominant,
        "empty_title": empty_title,
        "empty_detail": empty_detail,
    }


def _regime_breakdown(vol_summary: Dict[str, Any]) -> Dict[str, Any]:
    p33 = float(vol_summary.get("p33") or 0.0)
    p66 = float(vol_summary.get("p66") or 0.0)
    counts = {"Low": 0.0, "Normal": 0.0, "High": 0.0}
    for row in list(vol_summary.get("series") or []):
        value = float(row.get("v") or 0.0)
        if value <= p33:
            counts["Low"] += 1.0
        elif value <= p66:
            counts["Normal"] += 1.0
        else:
            counts["High"] += 1.0
    items = [{"label": label, "value": value} for label, value in counts.items()]
    return _compact_breakdown(
        items,
        label_key="label",
        value_key="value",
        top_n=3,
        total_label="Days",
        empty_title="No regime mix yet",
        empty_detail="Need a wider date range before regime distribution becomes useful.",
    )


def _filter_chips(
    *,
    review_filters: Dict[str, str],
    start_date: str,
    end_date: str,
    scope_mode: str,
    expectancy_granularity: str,
) -> List[str]:
    chips = [f"Scope {scope_mode.title()}", f"{expectancy_granularity.title()} expectancy"]
    if start_date:
        chips.append(f"From {start_date}")
    if end_date:
        chips.append(f"To {end_date}")
    if review_filters.get("setup"):
        chips.append(f"Setup {review_filters['setup']}")
    if review_filters.get("session"):
        chips.append(f"Session {review_filters['session']}")
    if review_filters.get("outcome"):
        chips.append(str(review_filters["outcome"]).title())
    if review_filters.get("time_block"):
        chips.append(f"Block {review_filters['time_block']}")
    if review_filters.get("mistake_tag"):
        chips.append(f"Mistake {review_filters['mistake_tag']}")
    return chips


def _serialize_analytics_payload(context: Dict[str, Any]) -> Dict[str, Any]:
    perf = context["perf"]
    dd = context["dd"]
    corr = context["corr"]
    rows = context["rows"]
    setup_rows = context["setup_rows"]
    mistake_cost_rows = context["mistake_cost_rows"]
    review_coverage = context["review_coverage"]
    vol_summary = context["vol_summary"]
    data_trust = context["data_trust"]
    balance_integrity = context["balance_integrity"]
    best_setup_card = context["best_setup_card"]
    biggest_leak_card = context["biggest_leak_card"]
    day_story = context["day_story"]
    sizing = context["sizing"]
    fitz_22 = context["fitz_22"]
    equity_series = context["equity_series"]
    drawdown_series = context["drawdown_series"]
    expectancy_series = context["expectancy_series"]
    spx_benchmark_series = context["spx_benchmark_series"]
    integrity_issue_count = int(context["integrity_issue_count"] or 0)
    scope = context["account_scope"]
    scope_mode = context["scope_mode"]

    total_trades = int(perf.get("total_trades") or 0)
    expectancy = float(perf.get("expectancy") or 0.0)
    avg_win = float(perf.get("avg_win") or 0.0)
    avg_loss_abs = float(perf.get("avg_loss_abs") or 0.0)
    expectancy_scale = max(abs(avg_win), abs(avg_loss_abs), 100.0)
    expectancy_progress = _progress_value(50.0 + ((expectancy / expectancy_scale) * 50.0))
    drawdown_now = float(dd.get("current_drawdown") or 0.0)
    max_drawdown = float(dd.get("max_drawdown") or 0.0)
    control_progress = _progress_value(
        100.0 - ((drawdown_now / max(max_drawdown, 250.0)) * 100.0)
    )
    review_progress = _progress_value(float(review_coverage.get("completion_pct") or 0.0))
    trust_score = _progress_value(
        100.0 - ((integrity_issue_count / max(1.0, float(max(total_trades, 1) * 3))) * 100.0)
    )
    corr_value = corr.get("r")
    corr_progress = 50.0 if corr_value is None else _progress_value(50.0 + (float(corr_value) * 50.0))
    profit_factor_value = perf.get("profit_factor")
    profit_factor_display = (
        f"{float(profit_factor_value):.2f}" if profit_factor_value is not None else "∞"
    )
    outcome_breakdown = _compact_breakdown(
        [
            {"label": "Wins", "value": perf.get("wins", 0)},
            {"label": "Losses", "value": perf.get("losses", 0)},
            {"label": "Breakeven", "value": perf.get("breakeven", 0)},
        ],
        label_key="label",
        value_key="value",
        top_n=3,
        total_label="Trades",
        empty_title="No outcomes yet",
        empty_detail="Log trades inside this range to unlock the outcome mix.",
    )
    setup_breakdown = _compact_breakdown(
        [{"label": row.get("k"), "value": row.get("count")} for row in setup_rows],
        label_key="label",
        value_key="value",
        top_n=5,
        total_label="Trades",
        empty_title="No setup mix yet",
        empty_detail="Tag setups during review to reveal the composition.",
    )
    mistake_breakdown = _compact_breakdown(
        [
            {"label": str(row.get("tag") or "").replace("-", " ").title(), "value": row.get("count")}
            for row in mistake_cost_rows
        ],
        label_key="label",
        value_key="value",
        top_n=5,
        total_label="Flags",
        empty_title="No mistakes logged",
        empty_detail="Review tags are clean in this range. Keep logging behavior drift when it shows up.",
    )
    benchmark_has_data = any(
        str(row.get("ticker") or "").strip().upper() == "SPX" for row in rows
    )
    scope_label = ""
    if scope and scope.get("enabled") and scope_mode == "active":
        scope_label = str(scope.get("start_date") or "")
        if scope.get("label"):
            scope_label = f"{scope_label} · {scope.get('label')}"

    return {
        "meta": {
            "tab": context["tab"],
            "scope": scope_mode,
            "start_date": context["start_date"],
            "end_date": context["end_date"],
            "explain_day": context["explain_day"],
            "expectancy_granularity": context["expectancy_granularity"],
            "filter_chips": _filter_chips(
                review_filters=context["review_filters"],
                start_date=context["start_date"],
                end_date=context["end_date"],
                scope_mode=scope_mode,
                expectancy_granularity=context["expectancy_granularity"],
            ),
            "scope_label": scope_label,
        },
        "summary": {
            "balance_display": money(float(balance_integrity.get("canonical_balance") or 0.0)),
            "net_display": money(float(perf.get("total_net") or 0.0)),
            "net_tone": _tone_from_value(float(perf.get("total_net") or 0.0)),
            "win_rate_display": f"{float(perf.get('win_rate') or 0.0):.1f}%",
            "expectancy_display": money(expectancy),
            "drawdown_display": money(drawdown_now),
            "drawdown_tone": "negative" if drawdown_now > 0 else "positive",
            "total_trades": total_trades,
            "profit_factor_display": profit_factor_display,
        },
        "kpis": {
            "win_rate": {
                "progress": _progress_value(float(perf.get("win_rate") or 0.0)),
                "center": f"{float(perf.get('win_rate') or 0.0):.1f}%",
                "subtitle": "Win rate",
                "footnote": f"{int(perf.get('wins') or 0)}W / {int(perf.get('losses') or 0)}L / {int(perf.get('breakeven') or 0)}BE",
            },
            "expectancy": {
                "progress": expectancy_progress,
                "center": money(expectancy),
                "subtitle": "Expectancy",
                "footnote": f"Profit factor {profit_factor_display}",
            },
            "risk_control": {
                "progress": control_progress,
                "center": (
                    "Healthy" if drawdown_now <= 0 else "Caution" if drawdown_now < max(max_drawdown * 0.5, 150.0) else "Stress"
                ),
                "subtitle": "Risk control state",
                "footnote": f"Live DD {money(drawdown_now)}",
            },
            "review_coverage": {
                "progress": review_progress,
                "center": f"{review_progress:.0f}%",
                "subtitle": "Review coverage",
                "footnote": f"{int(review_coverage.get('fully_reviewed') or 0)} fully reviewed",
            },
        },
        "coach": {
            "changed": context["insights"]["changed"],
            "risk_now": context["insights"]["risk_now"],
            "next_action": context["insights"]["next_action"],
            "best_setup_headline": (
                f"{best_setup_card['setup']} · {money(float(best_setup_card.get('expectancy') or 0.0))} expectancy"
                if best_setup_card
                else "Need labeled setup sample"
            ),
            "best_setup_body": (
                f"{int(best_setup_card.get('count') or 0)} trades · {float(best_setup_card.get('win_rate') or 0.0):.1f}% win rate · avg winner {money(float(best_setup_card.get('avg_winner') or 0.0))}."
                if best_setup_card
                else "Add setup tags on reviewed trades so edge detection has enough signal."
            ),
            "biggest_leak_headline": (
                f"{str(biggest_leak_card['tag']).replace('-', ' ').title()} · {money(float(biggest_leak_card.get('loss_cost') or 0.0))} cost"
                if biggest_leak_card
                else "No behavior leak isolated"
            ),
            "biggest_leak_body": (
                f"{int(biggest_leak_card.get('count') or 0)} tagged trades · average impact {money(float(biggest_leak_card.get('avg_impact') or 0.0))}."
                if biggest_leak_card
                else "Tag mistakes during reviews so the behavior layer can rank what is actually expensive."
            ),
            "data_trust_message": data_trust.message,
            "data_trust_tone": data_trust.tone,
            "data_trust_href": str(data_trust.primary_href or ""),
            "data_trust_label": str(data_trust.primary_label or ""),
            "day_title": day_story["title"],
            "day_lines": [day_story["pnl_driver"], day_story["risk_driver"], day_story["edge_driver"]],
            "sizing_regime": sizing["regime"],
            "sizing_action": sizing["action"],
            "sizing_note": sizing["note"],
            "fitz_status": str(fitz_22.get("status") or "BUILD SAMPLE"),
        },
        "charts": {
            "equity": {
                "points": _series_points(equity_series),
                "subtitle": f"Cumulative equity · {money(float(perf.get('total_net') or 0.0))} net",
            },
            "drawdown": {
                "points": _series_points(drawdown_series),
                "subtitle": f"Max drawdown {money(max_drawdown)}",
            },
            "benchmark": {
                "strategy": _series_points(equity_series),
                "benchmark": _series_points(spx_benchmark_series) if benchmark_has_data else [],
                "has_benchmark": benchmark_has_data,
                "empty_title": "Benchmark unavailable",
                "empty_detail": "No SPX trades in this range yet, so the benchmark overlay has nothing honest to compare against.",
            },
            "pnl_day": {
                "points": _daily_net_series(rows),
                "subtitle": "Net PnL by trade day",
            },
            "breakdowns": {
                "outcomes": outcome_breakdown,
                "setups": setup_breakdown,
                "mistakes": mistake_breakdown,
                "regimes": _regime_breakdown(vol_summary),
            },
            "review_spark": _daily_review_completion_series(rows),
            "trust_spark": _daily_trust_series(rows),
            "edge_spark": [{"x": point["x"], "y": point["y"]} for point in _series_points(expectancy_series)],
        },
        "micro": {
            "review": {
                "label": "Review Quality",
                "value": f"{review_progress:.0f}%",
                "note": f"{int(review_coverage.get('fully_reviewed') or 0)} of {total_trades} trades fully reviewed.",
                "progress": review_progress,
            },
            "trust": {
                "label": "Trust Signal",
                "value": f"{trust_score:.0f}/100",
                "note": data_trust.message,
                "progress": trust_score,
            },
            "edge": {
                "label": "Edge Alignment",
                "value": f"{float(corr_value):.2f}" if corr_value is not None else "—",
                "note": str(corr.get("label") or "Not enough scored trades"),
                "progress": corr_progress,
            },
        },
    }


def _build_analytics_context(args: Any) -> Dict[str, Any]:
    start_date = (args.get("start") or "").strip()
    end_date = (args.get("end") or "").strip()
    explain_day = (args.get("explain_day") or "").strip()
    review_filters = repo.normalize_trade_filters(
        {
            "setup": args.get("setup", ""),
            "session": args.get("session", ""),
            "outcome": args.get("outcome", ""),
            "time_block": args.get("time_block", ""),
            "mistake_tag": args.get("mistake_tag", ""),
        }
    )
    expectancy_granularity = ((args.get("expectancy_granularity") or "monthly").strip().lower())
    if expectancy_granularity not in {"monthly", "weekly"}:
        expectancy_granularity = "monthly"
    tab = (args.get("tab") or "performance").strip().lower()
    if tab not in {"performance", "behavior", "edge", "diagnostics"}:
        tab = "performance"

    scope = trades_repo.account_scope_snapshot()
    scope_enabled = bool(scope.get("enabled"))
    scope_mode_raw = (args.get("scope") or "").strip().lower()
    scope_active = scope_enabled and scope_mode_raw != "all"
    scope_start = str(scope.get("start_date") or "")
    scope_starting_balance = float(scope.get("starting_balance") or 50000.0)
    if scope_active and scope_start:
        if not start_date or start_date < scope_start:
            start_date = scope_start

    rows = repo.fetch_analytics_rows(
        start_date=start_date, end_date=end_date, filters=review_filters
    )
    perf = repo.performance_metrics(
        rows, starting_balance=scope_starting_balance if scope_active else None
    )
    dd = repo.drawdown_diagnostics(
        rows, starting_balance=scope_starting_balance if scope_active else None
    )
    corr = repo.score_pnl_correlation(rows)
    setup_rows = repo.group_table(rows, "setup_tag")
    session_rows = repo.group_table(rows, "session_tag")
    setup_scorecards = repo.setup_scorecards(rows)
    mistake_cost_rows = repo.mistake_costs(rows)
    review_coverage = repo.review_coverage(rows)
    best_setup_card = next(
        (
            row
            for row in setup_scorecards
            if str(row.get("setup") or "").strip()
            and str(row.get("setup") or "").strip() != "Unlabeled"
            and str(row.get("setup") or "").strip() != "Unknown"
        ),
        None,
    )
    biggest_leak_card = mistake_cost_rows[0] if mistake_cost_rows else None
    setup_trend_rows = repo.edge_over_time(rows, "setup_tag", top_n=3)
    session_trend_rows = repo.edge_over_time(rows, "session_tag", top_n=3)
    hour_rows = repo.hour_bucket_table(rows)
    rule_breaks = repo.rule_break_counts(rows)
    equity_series = repo.equity_curve_series(
        rows, starting_balance=scope_starting_balance if scope_active else None
    )
    drawdown_series = repo.drawdown_curve_series(
        rows, starting_balance=scope_starting_balance if scope_active else None
    )
    expectancy_series = repo.expectancy_trend_series(rows, granularity=expectancy_granularity)
    expectancy_auto_switched = False
    if expectancy_granularity == "monthly" and len(expectancy_series) < 2:
        expectancy_granularity = "weekly"
        expectancy_series = repo.expectancy_trend_series(rows, granularity=expectancy_granularity)
        expectancy_auto_switched = True
    spx_benchmark_series = repo.spx_benchmark_series(
        rows, starting_balance=scope_starting_balance if scope_active else None
    )
    vol_summary = repo.volatility_regime_summary(rows)
    heatmap = repo.setup_expectancy_heatmap(rows, top_n_setups=5)
    fitz_22 = repo.fitz_22_rev_indicator(rows)
    integrity = repo.integrity_diagnostics(
        rows, starting_balance=scope_starting_balance if scope_active else None
    )
    integrity_issue_count = int(
        (integrity.get("missing_setup") or 0)
        + (integrity.get("missing_session") or 0)
        + (integrity.get("missing_score") or 0)
        + (integrity.get("duplicate_candidates") or 0)
        + (integrity.get("stale_balance_rows") or 0)
    )
    sync_status = get_system_status() or {}
    balance_integrity = trades_repo.balance_integrity_snapshot(
        start_date=scope_start if scope_active else None,
        starting_balance=scope_starting_balance if scope_active else None,
    )
    balance_badges = balance_state_badges(balance_integrity)
    sync_badges = sync_state_badges(
        sync_status,
        status_key="last_sync_status",
        stage_key="last_sync_stage",
        updated_key="last_sync_updated_human",
    )
    data_trust = analytics_data_trust(sync_status, integrity_issue_count=integrity_issue_count)
    if data_trust.primary_href == "/analytics?tab=diagnostics":
        data_trust = data_trust.__class__(
            status_label=data_trust.status_label,
            stage_label=data_trust.stage_label,
            updated_label=data_trust.updated_label,
            tone=data_trust.tone,
            message=data_trust.message,
            primary_href=(
                f"/analytics?tab=diagnostics&start={start_date}&end={end_date}"
                f"&explain_day={explain_day}&expectancy_granularity={expectancy_granularity}"
            ),
            primary_label=data_trust.primary_label,
            secondary_href=data_trust.secondary_href,
            secondary_label=data_trust.secondary_label,
        )
    day_story = _explain_day(rows, day_iso=explain_day)
    sizing = _regime_sizing_suggestion(
        perf=perf, dd=dd, vol_summary=vol_summary, setup_rows=setup_rows
    )
    sim = _what_if_day_simulator(
        rows,
        {
            "sim_max_trades": (request.args.get("sim_max_trades") or "").strip(),
            "sim_stop_loss_streak": (request.args.get("sim_stop_loss_streak") or "").strip(),
        },
        perf=perf,
        scope_start=scope_start if scope_enabled else "",
        scope_starting_balance=scope_starting_balance if scope_enabled else None,
    )
    insights = _insight_panels(perf, dd, corr)
    context = {
        "rows": rows,
        "perf": perf,
        "dd": dd,
        "corr": corr,
        "insights": insights,
        "setup_rows": setup_rows,
        "session_rows": session_rows,
        "setup_scorecards": setup_scorecards,
        "mistake_cost_rows": mistake_cost_rows,
        "review_coverage": review_coverage,
        "best_setup_card": best_setup_card,
        "biggest_leak_card": biggest_leak_card,
        "setup_trend_rows": setup_trend_rows,
        "session_trend_rows": session_trend_rows,
        "hour_rows": hour_rows,
        "rule_breaks": rule_breaks,
        "equity_series": equity_series,
        "drawdown_series": drawdown_series,
        "expectancy_series": expectancy_series,
        "spx_benchmark_series": spx_benchmark_series,
        "vol_summary": vol_summary,
        "fitz_22": fitz_22,
        "integrity": integrity,
        "integrity_issue_count": integrity_issue_count,
        "balance_integrity": balance_integrity,
        "balance_badges": balance_badges,
        "sync_status": sync_status,
        "sync_badges": sync_badges,
        "data_trust": data_trust,
        "start_date": start_date,
        "end_date": end_date,
        "explain_day": explain_day,
        "review_filters": review_filters,
        "expectancy_granularity": expectancy_granularity,
        "expectancy_auto_switched": expectancy_auto_switched,
        "tab": tab,
        "account_scope": scope,
        "scope_mode": ("active" if scope_active else "all"),
        "scope_active_href": (
            "/analytics?"
            + urlencode(
                {
                    "tab": tab,
                    "start": start_date,
                    "end": end_date,
                    "explain_day": explain_day,
                    "expectancy_granularity": expectancy_granularity,
                    "setup": review_filters["setup"],
                    "session": review_filters["session"],
                    "outcome": review_filters["outcome"],
                    "time_block": review_filters["time_block"],
                    "mistake_tag": review_filters["mistake_tag"],
                    "scope": "active",
                }
            )
        ),
        "scope_all_href": (
            "/analytics?"
            + urlencode(
                {
                    "tab": tab,
                    "start": start_date,
                    "end": end_date,
                    "explain_day": explain_day,
                    "expectancy_granularity": expectancy_granularity,
                    "setup": review_filters["setup"],
                    "session": review_filters["session"],
                    "outcome": review_filters["outcome"],
                    "time_block": review_filters["time_block"],
                    "mistake_tag": review_filters["mistake_tag"],
                    "scope": "all",
                }
            )
        ),
        "day_story": day_story,
        "sizing": sizing,
        "sim": sim,
        "heatmap": heatmap,
        "analytics_api_url": "/api/analytics/dashboard",
    }
    context["analytics_payload"] = _serialize_analytics_payload(context)
    return context


def analytics_page():
    context = _build_analytics_context(request.args)
    content = render_template("analytics/index.html", money=money, **context)
    return render_page(content, active="analytics", title="McCain Capital · Analytics")


def analytics_dashboard_api():
    context = _build_analytics_context(request.args)
    return jsonify(context["analytics_payload"])


def session_replay_page():
    day = (request.args.get("date") or "").strip()
    rows = repo.fetch_analytics_rows()
    if not day:
        day = str(max((str(r.get("trade_date") or "") for r in rows), default=""))
    day_rows = [r for r in rows if str(r.get("trade_date") or "") == day]
    day_rows.sort(key=lambda r: int(r.get("id") or 0))
    running = 0.0
    timeline: List[Dict[str, Any]] = []
    spx_intraday = _intraday_points_for_day("SPX", day)
    vix_intraday = _intraday_points_for_day("VIX", day)
    for idx, r in enumerate(day_rows, start=1):
        net = float(r.get("net_pl") or 0.0)
        running += net
        entry_time = str(r.get("entry_time") or "—")
        minute = _minute_from_label(entry_time)
        spx_snap = _nearest_intraday_snapshot(spx_intraday, minute)
        vix_snap = _nearest_intraday_snapshot(vix_intraday, minute)
        timeline.append(
            {
                "step": idx,
                "id": int(r.get("id") or 0),
                "entry_time": entry_time,
                "exit_time": str(r.get("exit_time") or "—"),
                "ticker": str(r.get("ticker") or ""),
                "opt_type": str(r.get("opt_type") or ""),
                "setup_tag": str(r.get("setup_display") or r.get("setup_tag") or "Unknown"),
                "session_tag": str(r.get("session_tag") or ""),
                "rule_break_tags": str(r.get("rule_break_tags") or ""),
                "checklist_score": r.get("checklist_score"),
                "net_pl": net,
                "equity_delta": running,
                "spx_snapshot": spx_snap,
                "vix_snapshot": vix_snap,
            }
        )
    wins = len([t for t in timeline if float(t["net_pl"]) > 0])
    losses = len([t for t in timeline if float(t["net_pl"]) < 0])
    day_net = sum(float(t["net_pl"]) for t in timeline)
    key_wins = [t for t in timeline if float(t["net_pl"]) > 0][:3]
    key_losses = sorted(
        [t for t in timeline if float(t["net_pl"]) < 0], key=lambda x: float(x["net_pl"])
    )[:3]
    score_values = [
        float(t["checklist_score"])
        for t in timeline
        if t.get("checklist_score") is not None and str(t.get("checklist_score")).strip() != ""
    ]
    avg_score = (sum(score_values) / len(score_values)) if score_values else None
    setup_rows = repo.group_table(day_rows, "setup_tag")
    session_rows = repo.group_table(day_rows, "session_tag")
    dominant_setup = setup_rows[0] if setup_rows else None
    dominant_session = session_rows[0] if session_rows else None
    rule_breaks = sorted(
        {
            p.strip()
            for t in timeline
            for p in str(t.get("rule_break_tags") or "").split(",")
            if p.strip()
        }
    )
    rule_break_rows = repo.rule_break_counts(day_rows)
    journal_entries = [dict(r) for r in journal_repo.fetch_entries(d=day)]
    for entry in journal_entries:
        entry["linked_trade_ids"] = journal_repo.fetch_entry_trade_ids(int(entry["id"]))
    journal_summary = journal_entries[0] if journal_entries else None
    replay_status = (
        "No trades logged"
        if not timeline
        else (
            "Controlled green day"
            if day_net > 0 and not rule_break_rows
            else "Review behavior" if day_net < 0 or rule_break_rows else "Logged and stable"
        )
    )
    first_trade = timeline[0] if timeline else None
    last_trade = timeline[-1] if timeline else None
    intraday_arc = (
        f"{first_trade['entry_time']} first print -> {last_trade['exit_time']} close"
        if first_trade and last_trade
        else "No intraday sequence recorded."
    )
    notes_lines: List[str] = [
        f"Session replay for {day}: {len(timeline)} trades, net {money(day_net)}, wins/losses {wins}/{losses}.",
        "Key wins:",
    ]
    if key_wins:
        notes_lines.extend(
            [
                f"- #{t['id']} {t['ticker']} {t['opt_type']} {t['entry_time']}->{t['exit_time']}: {money(float(t['net_pl']))}"
                for t in key_wins
            ]
        )
    else:
        notes_lines.append("- None")
    notes_lines.append("Key losses:")
    if key_losses:
        notes_lines.extend(
            [
                f"- #{t['id']} {t['ticker']} {t['opt_type']} {t['entry_time']}->{t['exit_time']}: {money(float(t['net_pl']))}"
                for t in key_losses
            ]
        )
    else:
        notes_lines.append("- None")
    notes_lines.append(
        "Primary mistakes/rule breaks: "
        + (", ".join(rule_breaks) if rule_breaks else "none logged")
    )
    notes_lines.append("Action plan for next session:")
    notes_lines.append("- Keep A+ setups only in strongest time block.")
    notes_lines.append("- Respect max size and stop-after-streak guardrails.")
    replay_rail = list(_macro_timeline_items(day))
    for t in timeline:
        spx_snap = t.get("spx_snapshot") or {}
        vix_snap = t.get("vix_snapshot") or {}
        market_context_bits = []
        if isinstance(spx_snap.get("close"), (int, float)):
            market_context_bits.append(f"SPX {float(spx_snap['close']):,.2f}")
        if isinstance(vix_snap.get("close"), (int, float)):
            market_context_bits.append(f"VIX {float(vix_snap['close']):,.2f}")
        replay_rail.append(
            {
                "kind": "trade",
                "sort_minute": _minute_from_label(str(t.get("entry_time") or "")),
                "time_label": str(t.get("entry_time") or "Trade"),
                "headline": f"Trade #{t['id']} · {t['ticker']} {t['opt_type']}",
                "summary": (
                    f"{t.get('setup_tag') or 'Unknown'} · {t.get('session_tag') or 'No session tag'}"
                ),
                "tone": (
                    "positive"
                    if float(t["net_pl"]) > 0
                    else "negative" if float(t["net_pl"]) < 0 else "neutral"
                ),
                "impact_label": money(float(t["net_pl"])),
                "market_context": (
                    " · ".join(market_context_bits)
                    if market_context_bits
                    else "Market snapshot unavailable"
                ),
                "detail": (
                    f"Checklist {t['checklist_score']}"
                    if t.get("checklist_score") is not None
                    else "Checklist not scored"
                ),
                "rule_breaks": str(t.get("rule_break_tags") or "").strip(),
            }
        )
    replay_rail.sort(
        key=lambda item: (
            int(item.get("sort_minute") or 0),
            0 if item.get("kind") == "macro" else 1,
        )
    )
    market_arc = _market_arc_summary(day)
    replay_journal_href = "/new?" + urlencode(
        {
            "prefill": "replay",
            "d": day,
            "entry_type": "trade_debrief",
            "link_all_day": "1",
            "auto_draft": "1",
            "pnl": f"{day_net:.2f}",
            "notes": "\n".join(notes_lines),
            "template_notes": "Replay-linked debrief generated from Session Replay.",
            "setup": "Session Replay Debrief",
            "grade": "TBD",
        }
    )
    content = render_template(
        "analytics/session_replay.html",
        day=day,
        timeline=timeline,
        day_net=day_net,
        wins=wins,
        losses=losses,
        avg_score=avg_score,
        dominant_setup=dominant_setup,
        dominant_session=dominant_session,
        journal_entries=journal_entries,
        journal_summary=journal_summary,
        rule_break_rows=rule_break_rows,
        replay_status=replay_status,
        intraday_arc=intraday_arc,
        replay_rail=replay_rail,
        market_arc=market_arc,
        replay_journal_href=replay_journal_href,
        money=money,
    )
    return render_page(content, active="analytics", title="McCain Capital · Session Replay")
