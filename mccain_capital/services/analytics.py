"""Analytics service module."""

from __future__ import annotations

import html
import re
from urllib.parse import urlencode
from typing import Any, Dict, List

from flask import render_template_string, request

from mccain_capital.repositories import analytics as repo
from mccain_capital.runtime import get_setting_float, money
from mccain_capital.services.ui import render_page


def _series_day_from_label(label: str) -> str:
    raw = str(label or "")
    m = re.search(r"\d{4}-\d{2}-\d{2}", raw)
    return m.group(0) if m else ""


def _line_chart_svg(series: List[Dict[str, Any]], stroke: str, y_prefix: str = "$") -> str:
    if len(series) < 2:
        return '<div class="chartEmpty">Not enough data to render chart.</div>'

    width = 820.0
    height = 200.0
    pad = 18.0

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
    zero_in_range = min_v <= 0.0 <= max_v
    zero_y = sy(0.0) if zero_in_range else None
    latest_label = html.escape(str(series[-1].get("label") or "latest"))

    points_markup = []
    for i, p in enumerate(series):
        label = html.escape(str(p.get("label") or f"#{i+1}"))
        value = float(p.get("v") or 0.0)
        day = _series_day_from_label(str(p.get("label") or ""))
        points_markup.append(
            (
                f'<circle class="chartPoint" cx="{sx(i):.2f}" cy="{sy(values[i]):.2f}" r="6.5" '
                f'fill="transparent" data-label="{label}" data-value="{value:.2f}" data-prefix="{html.escape(y_prefix)}" '
                f'data-day="{html.escape(day)}" />'
            )
        )
    return f"""
    <svg viewBox="0 0 {int(width)} {int(height)}" role="img" aria-label="analytics line chart" style="width:100%;height:auto;display:block">
      <rect x="0" y="0" width="{int(width)}" height="{int(height)}" fill="rgba(4,10,20,.35)" rx="10" />
      {f'<line x1="{pad}" y1="{zero_y:.2f}" x2="{width - pad}" y2="{zero_y:.2f}" stroke="rgba(255,255,255,.2)" stroke-dasharray="4 4" />' if zero_y is not None else ""}
      <polyline class="chartLine" fill="none" stroke="{stroke}" stroke-width="3" points="{points}" />
      <circle cx="{sx(len(values) - 1):.2f}" cy="{sy(values[-1]):.2f}" r="4.5" fill="{stroke}" />
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
        return '<div class="chartEmpty">Not enough data to render chart.</div>'
    if all(len(s["series"]) < 2 for s in active):
        return '<div class="chartEmpty">Not enough data to render chart.</div>'

    width = 820.0
    height = 210.0
    pad = 18.0

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
                    f'<circle class="chartPoint" cx="{sx(i, len(vals)):.2f}" cy="{sy(vals[i]):.2f}" r="6.0" '
                    f'fill="transparent" data-label="{label}" data-value="{vals[i]:.2f}" data-prefix="{html.escape(y_prefix)}" '
                    f'data-series="{name}" data-day="{html.escape(day)}" />'
                )
            )
        legend_items.append(
            f'<span class="trendChip"><span style="display:inline-block;width:10px;height:10px;border-radius:999px;background:{color};margin-right:6px;"></span>{name}: {y_prefix}{vals[-1]:,.2f}</span>'
        )

    return f"""
    <svg viewBox="0 0 {int(width)} {int(height)}" role="img" aria-label="analytics multi line chart" style="width:100%;height:auto;display:block">
      <rect x="0" y="0" width="{int(width)}" height="{int(height)}" fill="rgba(4,10,20,.35)" rx="10" />
      {''.join(lines)}
      {''.join(dots)}
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
        setup = (r.get("setup_tag") or "").strip() or "Unlabeled"
        by_setup[setup] = by_setup.get(setup, 0.0) + float(r.get("net_pl") or 0.0)
    top_setup = "Unlabeled"
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
    if top_setup and float(top_setup.get("expectancy") or 0.0) > 0 and float(
        top_setup.get("win_rate") or 0.0
    ) >= 55.0:
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
    rows: List[Dict[str, Any]], args: Dict[str, str], perf: Dict[str, Any]
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

    current_balance = float(get_setting_float("starting_balance", 50000.0))
    for r in reversed(rows):
        try:
            bal = float(r.get("balance")) if r.get("balance") is not None else None
        except (TypeError, ValueError):
            bal = None
        if bal is not None:
            current_balance = bal
            break
    else:
        current_balance += float(perf.get("total_net") or 0.0)

    p_win = consistency_win_rate / 100.0
    p_loss = 1.0 - p_win
    expectancy = (p_win * consistency_avg_win) - (p_loss * consistency_avg_loss)
    projected_full = max_trades * expectancy
    streak_prob = 1.0 - ((1.0 - (p_loss**stop_loss_streak)) ** max(1, max_trades - stop_loss_streak + 1))
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
        (sum(float(x["wins"]) for x in day_stats) / max(1.0, sum(float(x["count"]) for x in day_stats))) * 100.0
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


def analytics_page():
    start_date = (request.args.get("start") or "").strip()
    end_date = (request.args.get("end") or "").strip()
    explain_day = (request.args.get("explain_day") or "").strip()
    expectancy_granularity = (request.args.get("expectancy_granularity") or "monthly").strip().lower()
    if expectancy_granularity not in {"monthly", "weekly"}:
        expectancy_granularity = "monthly"
    tab = (request.args.get("tab") or "performance").strip().lower()
    if tab not in {"performance", "behavior", "edge", "diagnostics"}:
        tab = "performance"

    rows = repo.fetch_analytics_rows(start_date=start_date, end_date=end_date)
    perf = repo.performance_metrics(rows)
    dd = repo.drawdown_diagnostics(rows)
    corr = repo.score_pnl_correlation(rows)
    setup_rows = repo.group_table(rows, "setup_tag")
    session_rows = repo.group_table(rows, "session_tag")
    setup_trend_rows = repo.edge_over_time(rows, "setup_tag", top_n=3)
    session_trend_rows = repo.edge_over_time(rows, "session_tag", top_n=3)
    hour_rows = repo.hour_bucket_table(rows)
    rule_breaks = repo.rule_break_counts(rows)
    equity_series = repo.equity_curve_series(rows)
    drawdown_series = repo.drawdown_curve_series(rows)
    expectancy_series = repo.expectancy_trend_series(rows, granularity=expectancy_granularity)
    expectancy_auto_switched = False
    if expectancy_granularity == "monthly" and len(expectancy_series) < 2:
        expectancy_granularity = "weekly"
        expectancy_series = repo.expectancy_trend_series(rows, granularity=expectancy_granularity)
        expectancy_auto_switched = True
    spx_benchmark_series = repo.spx_benchmark_series(rows)
    vol_summary = repo.volatility_regime_summary(rows)
    heatmap = repo.setup_expectancy_heatmap(rows, top_n_setups=5)
    vol_series = vol_summary.get("series") or []
    equity_chart = _line_chart_svg(equity_series, stroke="#35d4ff", y_prefix="$")
    drawdown_chart = _line_chart_svg(drawdown_series, stroke="#ff5c7a", y_prefix="$")
    expectancy_chart = _line_chart_svg(expectancy_series, stroke="#8cff66", y_prefix="$")
    benchmark_chart = _multi_line_chart_svg(
        [
            {"name": "Strategy", "color": "#35d4ff", "series": equity_series},
            {"name": "SPX Benchmark", "color": "#f7c65f", "series": spx_benchmark_series},
        ],
        y_prefix="$",
    )
    vol_chart = _line_chart_svg(vol_series, stroke="#ffa14d", y_prefix="$")
    equity_story = _series_story(equity_series, favorable_direction="up")
    drawdown_story = _series_story(drawdown_series, favorable_direction="down")
    expectancy_story = _series_story(expectancy_series, favorable_direction="up")
    fitz_22 = repo.fitz_22_rev_indicator(rows)
    integrity = repo.integrity_diagnostics(rows)
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
    )
    insights = _insight_panels(perf, dd, corr)
    edge_pulse = max(8.0, min(100.0, 50.0 + (float(perf.get("expectancy") or 0.0) * 8.0)))
    drawdown_now = float(dd.get("current_drawdown") or 0.0)
    control_pulse = max(8.0, min(100.0, 100.0 - (drawdown_now / 30.0)))

    content = render_template_string(
        """
        <div class="card pageHero">
          <div class="toolbar">
            <div class="pageHeroHead">
              <div>
                <div class="pill">📈 Analytics Workspace</div>
                <h2 class="pageTitle">Performance Intelligence</h2>
                <div class="pageSub">Track edge quality over time with expectancy, drawdown, behavior, and setup/session diagnostics.</div>
              </div>
              <div class="actionRow">
                <a class="btn" href="/trades">📅 Trades</a>
                <a class="btn" href="/journal/review/weekly">📘 Weekly Review</a>
                <a class="btn" href="/analytics/replay">🎬 Session Replay</a>
                <a class="btn" href="/trades/playbook">📘 Playbook</a>
              </div>
            </div>
          </div>
        </div>

        <div class="showcaseGrid">
          <div class="showcaseCard">
            <div class="showcaseHead">
              <div class="showcaseTitle">
                <svg class="mcIcon"><use href="#mc-crown"></use></svg>
                Edge Intensity
              </div>
              <span class="trendChip">{{ perf.total_trades }} trades</span>
            </div>
            <div class="showcaseValue">{{ money(perf.expectancy) }}</div>
            <div class="showcaseMeta">
              Profit factor {% if perf.profit_factor is not none %}{{ '%.2f'|format(perf.profit_factor) }}{% else %}∞{% endif %}
              · Win rate {{ '%.1f'|format(perf.win_rate) }}%.
            </div>
            <div class="showcasePulse" style="--pulse-w: {{ '%.1f'|format(edge_pulse) }}%">
              <div class="showcasePulseFill"></div>
              <div class="showcaseWave"></div>
            </div>
          </div>
          <div class="showcaseCard">
            <div class="showcaseHead">
              <div class="showcaseTitle">
                <svg class="mcIcon"><use href="#mc-crest"></use></svg>
                Risk Control State
              </div>
              <span class="trendChip">DD streak {{ dd.current_drawdown_streak }}</span>
            </div>
            <div class="showcaseValue">{{ money(dd.current_drawdown) }}</div>
            <div class="showcaseMeta">
              Max DD {{ money(dd.max_drawdown) }} · Current streak {{ dd.current_drawdown_streak }}.
            </div>
            <div class="showcasePulse" style="--pulse-w: {{ '%.1f'|format(control_pulse) }}%">
              <div class="showcasePulseFill"></div>
              <div class="showcaseWave"></div>
            </div>
          </div>
        </div>

        <div class="metricStrip">
          <div class="metric"><div class="label">Trades</div><div class="value">{{ perf.total_trades }}</div><div class="sub">Reviewed sample in selected range</div></div>
          <div class="metric"><div class="label">Win Rate</div><div class="value">{{ '%.1f'|format(perf.win_rate) }}%</div><div class="sub">Execution hit-rate across reviewed trades</div></div>
          <div class="metric"><div class="label">Expectancy</div><div class="value">{{ money(perf.expectancy) }}</div><div class="sub">Average outcome per trade</div></div>
          <div class="metric"><div class="label">Profit Factor</div><div class="value">{{ '%.2f'|format(perf.profit_factor) if perf.profit_factor is not none else '∞' }}</div><div class="sub">Gross wins divided by gross losses</div></div>
          <div class="metric"><div class="label">Max Drawdown</div><div class="value">{{ money(perf.max_drawdown) }}</div><div class="sub">Largest peak-to-trough decline</div></div>
          <div class="metric"><div class="label">DD Streak (Max)</div><div class="value">{{ dd.max_drawdown_streak }}</div><div class="sub">Longest drawdown trade streak</div></div>
          <div class="metric"><div class="label">DD Streak (Current)</div><div class="value">{{ dd.current_drawdown_streak }}</div><div class="sub">Active drawdown streak right now</div></div>
          <div class="metric"><div class="label">Quality ↔ PnL Corr</div><div class="value">{% if corr.r is not none %}{{ '%.2f'|format(corr.r) }}{% else %}—{% endif %}</div><div class="sub">Checklist score alignment with outcome</div></div>
        </div>

        <div class="insightGrid stack12">
          <div class="insightCard">
            <div class="insightTitle">🎯 Fitz 2-2 REV Indicator</div>
            <div class="insightBody">
              <div class="trendChips">
                <span class="trendChip {{ fitz_22.tone }}">{{ fitz_22.status }}</span>
                <span class="trendChip">{{ fitz_22.trades }} tagged trades</span>
                <span class="trendChip">WR {{ '%.1f'|format(fitz_22.win_rate) }}%</span>
                <span class="trendChip">Exp {{ money(fitz_22.expectancy) }}</span>
              </div>
              <div class="meta">Recent 10: {{ money(fitz_22.recent_net) }} · {{ '%.1f'|format(fitz_22.recent_win_rate) }}% win rate</div>
              <div class="stack8">{{ fitz_22.note }}</div>
            </div>
          </div>
          <div class="insightCard">
            <div class="insightTitle">🔎 What Changed</div>
            <div class="insightBody">{{ insights.changed }}</div>
          </div>
          <div class="insightCard">
            <div class="insightTitle">🛡️ Risk Now</div>
            <div class="insightBody">{{ insights.risk_now }}</div>
          </div>
          <div class="insightCard">
            <div class="insightTitle">➡️ Next Action</div>
            <div class="insightBody">{{ insights.next_action }}</div>
          </div>
        </div>

        <div class="card"><div class="toolbar">
          <div class="actionRow">
            <a class="btn {% if tab == 'performance' %}primary{% endif %}" href="/analytics?tab=performance&start={{ start_date }}&end={{ end_date }}&explain_day={{ explain_day }}&expectancy_granularity={{ expectancy_granularity }}">Performance</a>
            <a class="btn {% if tab == 'behavior' %}primary{% endif %}" href="/analytics?tab=behavior&start={{ start_date }}&end={{ end_date }}&explain_day={{ explain_day }}&expectancy_granularity={{ expectancy_granularity }}">Behavior</a>
            <a class="btn {% if tab == 'edge' %}primary{% endif %}" href="/analytics?tab=edge&start={{ start_date }}&end={{ end_date }}&explain_day={{ explain_day }}&expectancy_granularity={{ expectancy_granularity }}">Edge</a>
            <a class="btn {% if tab == 'diagnostics' %}primary{% endif %}" href="/analytics?tab=diagnostics&start={{ start_date }}&end={{ end_date }}&explain_day={{ explain_day }}&expectancy_granularity={{ expectancy_granularity }}">Diagnostics</a>
          </div>
          <div class="hr"></div>
          <form method="get" class="row">
            <input type="hidden" name="tab" value="{{ tab }}">
            <div><label>Start</label><input type="date" name="start" value="{{ start_date }}"></div>
            <div><label>End</label><input type="date" name="end" value="{{ end_date }}"></div>
            <div>
              <label>Explain Day</label>
              <input type="date" name="explain_day" value="{{ explain_day }}" />
            </div>
            <div>
              <label>Expectancy View</label>
              <select name="expectancy_granularity">
                <option value="monthly" {% if expectancy_granularity == 'monthly' %}selected{% endif %}>Monthly</option>
                <option value="weekly" {% if expectancy_granularity == 'weekly' %}selected{% endif %}>Weekly</option>
              </select>
            </div>
            <div class="actionRow">
              <button class="btn" type="submit">Apply</button>
              <a class="btn" href="/analytics">Reset</a>
            </div>
          </form>
        </div></div>

        {% if tab == 'performance' %}
        <div class="twoCol stack12">
          <div class="card"><div class="toolbar">
            <div class="pill">📈 Equity Curve</div>
            <div class="hr"></div>
            <div class="trendChips">
              <span class="trendChip {{ equity_story.tone }}">Latest {{ money(equity_story.latest) }}</span>
              <span class="trendChip {{ equity_story.tone }}">
                Δ {{ money(equity_story.delta) }}
                {% if equity_story.pct is not none %}({{ '%.1f'|format(equity_story.pct) }}%){% endif %}
              </span>
              <span class="trendChip">{{ equity_story.label }}</span>
            </div>
            {{ equity_chart|safe }}
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">📉 Drawdown Curve</div>
            <div class="hr"></div>
            <div class="trendChips">
              <span class="trendChip {{ drawdown_story.tone }}">Latest {{ money(drawdown_story.latest) }}</span>
              <span class="trendChip {{ drawdown_story.tone }}">
                Δ {{ money(drawdown_story.delta) }}
                {% if drawdown_story.pct is not none %}({{ '%.1f'|format(drawdown_story.pct) }}%){% endif %}
              </span>
              <span class="trendChip">{{ drawdown_story.label }}</span>
            </div>
            {{ drawdown_chart|safe }}
          </div></div>
        </div>
        <div class="twoCol stack12">
          <div class="card"><div class="toolbar">
            <div class="pill">🧭 Equity vs SPX Benchmark</div>
            <div class="tiny stack8 line15">SPX benchmark applies only SPX ticker outcomes across the same trade timeline.</div>
            <div class="hr"></div>
            {{ benchmark_chart|safe }}
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">🌪️ Day Volatility Regime (5D)</div>
            <div class="trendChips">
              <span class="trendChip">Regime {{ vol_summary.regime }}</span>
              <span class="trendChip">Current {{ money(vol_summary.current) }}</span>
              <span class="trendChip">P33 {{ money(vol_summary.p33) }}</span>
              <span class="trendChip">P66 {{ money(vol_summary.p66) }}</span>
            </div>
            <div class="hr"></div>
            {{ vol_chart|safe }}
          </div></div>
        </div>
        <div class="twoCol stack12">
          <div class="card"><div class="toolbar">
            <div class="pill">🧠 {{ day_story.title }}</div>
            <div class="hr"></div>
            <div class="tiny stack8 line16">• {{ day_story.pnl_driver }}</div>
            <div class="tiny stack8 line16">• {{ day_story.risk_driver }}</div>
            <div class="tiny stack8 line16">• {{ day_story.edge_driver }}</div>
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">📐 Regime-Aware Sizing</div>
            <div class="trendChips">
              <span class="trendChip">Regime {{ sizing.regime }}</span>
              <span class="trendChip">{{ sizing.action }}</span>
              <span class="trendChip">Size {{ sizing.size_pct }}%</span>
            </div>
            <div class="hr"></div>
            <div class="tiny stack8 line16">{{ sizing.note }}</div>
          </div></div>
        </div>
        <div class="card"><div class="toolbar">
          <div class="pill">🧪 What-If Day Simulator</div>
          <div class="tiny stack8 line15">Project your ending balance from current balance using your real consistency profile.</div>
          <div class="tiny stack8 line16">Why this matters: plan quality is measured by expected ending balance, not isolated trade outcomes.</div>
          <div class="tiny stack8 line16">Next best action: set max trades + stop streak, then compare projected end balance vs your real average day.</div>
          <div class="hr"></div>
          <form method="get" class="row">
            <input type="hidden" name="tab" value="{{ tab }}">
            <input type="hidden" name="start" value="{{ start_date }}">
            <input type="hidden" name="end" value="{{ end_date }}">
            <input type="hidden" name="explain_day" value="{{ explain_day }}">
            <div><label>Max Trades</label><input type="number" min="1" max="30" name="sim_max_trades" value="{{ sim.max_trades }}"></div>
            <div><label>Stop After Loss Streak</label><input type="number" min="1" max="8" name="sim_stop_loss_streak" value="{{ sim.stop_loss_streak }}"></div>
            <div class="actionRow"><button class="btn" type="submit">Simulate</button></div>
          </form>
          <div class="hr"></div>
          <div class="statRow stack10">
            <div class="stat"><div class="k">Current Balance</div><div class="v">{{ money(sim.current_balance) }}</div></div>
            <div class="stat"><div class="k">Projected Expectancy</div><div class="v">{{ money(sim.expectancy) }}</div></div>
            <div class="stat"><div class="k">Projected Net (plan)</div><div class="v">{{ money(sim.projected_full) }}</div></div>
            <div class="stat"><div class="k">Projected End Balance</div><div class="v">{{ money(sim.projected_end_balance) }}</div></div>
            <div class="stat"><div class="k">Guardrail-Adjusted End</div><div class="v">{{ money(sim.projected_guardrail_end_balance) }}</div></div>
            <div class="stat"><div class="k">Loss-Streak Trigger Prob</div><div class="v">{{ '%.1f'|format(sim.streak_prob * 100.0) }}%</div></div>
          </div>
          <div class="tiny stack8 line16">
            Consistency profile in selected range: WR {{ '%.1f'|format(sim.consistency_win_rate) }}% · Avg Win {{ money(sim.consistency_avg_win) }} · Avg Loss {{ money(sim.consistency_avg_loss) }}.<br>
            Real avg day: {{ money(sim.real_avg_day_net) }} · Real avg WR: {{ '%.1f'|format(sim.real_avg_win_rate) }}% · Real avg trades/day: {{ '%.1f'|format(sim.real_avg_trades) }}.<br>
            Delta vs real avg day: {{ money(sim.delta_vs_real) }}.
          </div>
        </div></div>
        <div class="card stack12"><div class="toolbar">
          <div class="pill">🧠 Expectancy Trend ({{ expectancy_granularity|upper }})</div>
          {% if expectancy_auto_switched %}
            <div class="tiny stack8 line15">Auto-switched to weekly because monthly has too few points.</div>
          {% endif %}
          <div class="hr"></div>
          <div class="trendChips">
            <span class="trendChip {{ expectancy_story.tone }}">Latest {{ money(expectancy_story.latest) }}</span>
            <span class="trendChip {{ expectancy_story.tone }}">
              Δ {{ money(expectancy_story.delta) }}
              {% if expectancy_story.pct is not none %}({{ '%.1f'|format(expectancy_story.pct) }}%){% endif %}
            </span>
            <span class="trendChip">{{ expectancy_story.label }}</span>
          </div>
          {{ expectancy_chart|safe }}
        </div></div>
        <div class="twoCol stack12">
          <div class="card"><div class="toolbar">
            <div class="pill">📈 Performance Summary</div>
            <div class="statRow stack10">
              <div class="stat"><div class="k">Total Net</div><div class="v">{{ money(perf.total_net) }}</div></div>
              <div class="stat"><div class="k">Gross Profit</div><div class="v">{{ money(perf.gross_profit) }}</div></div>
              <div class="stat"><div class="k">Gross Loss</div><div class="v">{{ money(perf.gross_loss_abs) }}</div></div>
            </div>
            <details class="syncDetails stack10">
              <summary>Open detailed performance table</summary>
              <div class="hr"></div>
              <div class="tableWrap"><table class="tableDense kvTable">
                <tbody>
                  <tr><td>Total Net</td><td>{{ money(perf.total_net) }}</td></tr>
                  <tr><td>Gross Profit</td><td>{{ money(perf.gross_profit) }}</td></tr>
                  <tr><td>Gross Loss</td><td>{{ money(perf.gross_loss_abs) }}</td></tr>
                  <tr><td>Avg Win</td><td>{{ money(perf.avg_win) }}</td></tr>
                  <tr><td>Avg Loss</td><td>{{ money(perf.avg_loss_abs) }}</td></tr>
                  <tr><td>Largest Win</td><td>{{ money(perf.max_win) }}</td></tr>
                  <tr><td>Largest Loss</td><td>{{ money(perf.max_loss) }}</td></tr>
                </tbody>
              </table></div>
            </details>
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">🔥 Streaks</div>
            <div class="statRow stack10">
              <div class="stat"><div class="k">Longest Win Streak</div><div class="v">{{ perf.max_win_streak }}</div></div>
              <div class="stat"><div class="k">Longest Loss Streak</div><div class="v">{{ perf.max_loss_streak }}</div></div>
              <div class="stat"><div class="k">Wins / Losses</div><div class="v">{{ perf.wins }} / {{ perf.losses }}</div></div>
            </div>
            <details class="syncDetails stack10">
              <summary>Open detailed streak table</summary>
              <div class="hr"></div>
              <div class="tableWrap"><table class="tableDense kvTable">
                <tbody>
                  <tr><td>Longest Win Streak</td><td>{{ perf.max_win_streak }}</td></tr>
                  <tr><td>Longest Loss Streak</td><td>{{ perf.max_loss_streak }}</td></tr>
                  <tr><td>Wins</td><td>{{ perf.wins }}</td></tr>
                  <tr><td>Losses</td><td>{{ perf.losses }}</td></tr>
                  <tr><td>Breakeven</td><td>{{ perf.breakeven }}</td></tr>
                </tbody>
              </table></div>
            </details>
          </div></div>
        </div>
        {% elif tab == 'behavior' %}
        <div class="twoCol stack12">
          <div class="card"><div class="toolbar">
            <div class="pill">🕒 Session Breakdown</div>
            {% set s_top = session_rows[0] if session_rows|length else none %}
            <div class="statRow stack10">
              <div class="stat"><div class="k">Top Session</div><div class="v">{% if s_top %}{{ s_top.k }}{% else %}—{% endif %}</div></div>
              <div class="stat"><div class="k">Top Session Net</div><div class="v">{% if s_top %}{{ money(s_top.net) }}{% else %}—{% endif %}</div></div>
              <div class="stat"><div class="k">Top Session Win Rate</div><div class="v">{% if s_top %}{{ '%.1f'|format(s_top.win_rate) }}%{% else %}—{% endif %}</div></div>
            </div>
            <details class="syncDetails stack10">
              <summary>Open session breakdown table</summary>
              <div class="hr"></div>
              <div class="tableWrap desktopOnly"><table class="tableDense">
                <thead><tr><th>Session</th><th>Trades</th><th>Win Rate</th><th>Net</th><th>Expectancy</th><th>Score</th></tr></thead>
                <tbody>
                {% for r in session_rows %}
                  <tr><td>{{ r.k }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.net) }}</td><td>{{ money(r.expectancy) }}</td><td>{{ '%.1f'|format(r.avg_score) if r.avg_score is not none else '—' }}</td></tr>
                {% endfor %}
                </tbody>
              </table></div>
              <div class="mobileOnly">
                <div class="grid">
                  {% for r in session_rows %}
                    <div class="card"><div class="toolbar">
                      <div class="pill">{{ r.k }}</div>
                      <div class="metaRow">
                        <span class="meta">Trades: <b>{{ r.count }}</b></span>
                        <span class="meta">Win: <b>{{ '%.1f'|format(r.win_rate) }}%</b></span>
                        <span class="meta">Net: <b>{{ money(r.net) }}</b></span>
                        <span class="meta">Exp: <b>{{ money(r.expectancy) }}</b></span>
                      </div>
                    </div></div>
                  {% endfor %}
                </div>
              </div>
            </details>
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">⚠️ Rule Break Tags</div>
            {% set rb_top = rule_breaks[0] if rule_breaks|length else none %}
            <div class="statRow stack10">
              <div class="stat"><div class="k">Top Rule Break</div><div class="v">{% if rb_top %}{{ rb_top.tag }}{% else %}None logged{% endif %}</div></div>
              <div class="stat"><div class="k">Occurrences</div><div class="v">{% if rb_top %}{{ rb_top.count }}{% else %}0{% endif %}</div></div>
            </div>
            <details class="syncDetails stack10">
              <summary>Open full rule-break list</summary>
              <div class="hr"></div>
              <div class="tableWrap desktopOnly"><table class="tableDense">
                <thead><tr><th>Tag</th><th>Count</th></tr></thead>
                <tbody>
                {% for r in rule_breaks %}
                  <tr><td>{{ r.tag }}</td><td>{{ r.count }}</td></tr>
                {% endfor %}
                {% if rule_breaks|length == 0 %}
                  <tr><td colspan="2">No rule-break tags logged.</td></tr>
                {% endif %}
                </tbody>
              </table></div>
              <div class="mobileOnly">
                <div class="grid">
                  {% for r in rule_breaks %}
                    <div class="card"><div class="toolbar">
                      <div class="pill">{{ r.tag }}</div>
                      <div class="meta">Count: <b>{{ r.count }}</b></div>
                    </div></div>
                  {% endfor %}
                  {% if rule_breaks|length == 0 %}
                    <div class="card"><div class="toolbar"><div class="tiny">No rule-break tags logged.</div></div></div>
                  {% endif %}
                </div>
              </div>
            </details>
          </div></div>
        </div>
        <div class="card stack12"><div class="toolbar">
          <div class="pill">🗺️ Setup Expectancy Heatmap by Time Block</div>
          <div class="tiny stack8 line15">Color intensity reflects expectancy magnitude for top setups across intraday blocks.</div>
          <div class="hr"></div>
          {% if heatmap.setups|length == 0 %}
            <div class="tiny">No setup heatmap data in this range.</div>
          {% else %}
            <div class="tableWrap desktopOnly">
              <table class="tableDense">
                <thead>
                  <tr>
                    <th>Time Block</th>
                    {% for s in heatmap.setups %}<th>{{ s }}</th>{% endfor %}
                  </tr>
                </thead>
                <tbody>
                  {% for row in heatmap.rows %}
                  <tr>
                    <td>{{ row.block }}</td>
                    {% for c in row.cells %}
                      {% set ratio = ((c.expectancy|abs) / (heatmap.max_abs_exp if heatmap.max_abs_exp else 1.0)) %}
                      {% set alpha = 0.08 + (0.42 * ratio) %}
                      {% if c.expectancy > 0 %}
                        <td style="background: rgba(46, 204, 113, {{ alpha }});">{{ money(c.expectancy) }} · {{ c.count }}</td>
                      {% elif c.expectancy < 0 %}
                        <td style="background: rgba(231, 76, 60, {{ alpha }});">{{ money(c.expectancy) }} · {{ c.count }}</td>
                      {% else %}
                        <td style="background: rgba(255,255,255,.03);">{{ money(c.expectancy) }} · {{ c.count }}</td>
                      {% endif %}
                    {% endfor %}
                  </tr>
                  {% endfor %}
                </tbody>
              </table>
            </div>
            <div class="mobileOnly">
              {% for row in heatmap.rows %}
                <details class="syncDetails stack10">
                  <summary>{{ row.block }}</summary>
                  <div class="tiny stack8 line16">
                    {% for c in row.cells %}
                      • {{ c.setup }}: {{ money(c.expectancy) }} ({{ c.count }})<br>
                    {% endfor %}
                  </div>
                </details>
              {% endfor %}
            </div>
          {% endif %}
        </div></div>
        {% elif tab == 'diagnostics' %}
        <div class="card stack12"><div class="toolbar">
          <div class="pill">🧪 Data Integrity Diagnostics</div>
          <div class="tiny stack8 line15">Find data quality issues that can distort analytics and decision quality.</div>
          <div class="hr"></div>
          <div class="statRow">
            <div class="stat"><div class="k">Missing Setup Tags</div><div class="v">{{ integrity.missing_setup }}</div></div>
            <div class="stat"><div class="k">Missing Session Tags</div><div class="v">{{ integrity.missing_session }}</div></div>
            <div class="stat"><div class="k">Missing Scores</div><div class="v">{{ integrity.missing_score }}</div></div>
            <div class="stat"><div class="k">Duplicate Candidates</div><div class="v">{{ integrity.duplicate_candidates }}</div></div>
            <div class="stat"><div class="k">Stale Balance Rows</div><div class="v">{{ integrity.stale_balance_rows }}</div></div>
          </div>
          <div class="hr"></div>
          <div class="tiny stack8 line15">
            Priority actions:
            {% if integrity.stale_balance_rows > 0 %}
              • Recompute stored balances from dashboard drift action.<br>
            {% endif %}
            {% if integrity.missing_setup or integrity.missing_session %}
              • Rebuild/fill trade reviews to restore setup/session analytics.<br>
            {% endif %}
            {% if integrity.missing_score > 0 %}
              • Backfill checklist scores for quality-vs-PnL correlation.<br>
            {% endif %}
            {% if integrity.duplicate_candidates > 0 %}
              • Review recent imports for duplicate signatures.<br>
            {% endif %}
            {% if not (integrity.stale_balance_rows or integrity.missing_setup or integrity.missing_session or integrity.missing_score or integrity.duplicate_candidates) %}
              • No major integrity issues detected in this range.
            {% endif %}
          </div>
        </div></div>
        {% else %}
        <div class="twoCol stack12">
          <div class="card"><div class="toolbar">
            <div class="pill">📌 Setup Edge</div>
            {% set setup_top = setup_rows[0] if setup_rows|length else none %}
            <div class="statRow stack10">
              <div class="stat"><div class="k">Top Setup</div><div class="v">{% if setup_top %}{{ setup_top.k }}{% else %}—{% endif %}</div></div>
              <div class="stat"><div class="k">Top Setup Net</div><div class="v">{% if setup_top %}{{ money(setup_top.net) }}{% else %}—{% endif %}</div></div>
              <div class="stat"><div class="k">Top Setup Win Rate</div><div class="v">{% if setup_top %}{{ '%.1f'|format(setup_top.win_rate) }}%{% else %}—{% endif %}</div></div>
            </div>
            <details class="syncDetails stack10">
              <summary>Open setup edge table</summary>
              <div class="hr"></div>
              <div class="tableWrap desktopOnly"><table class="tableDense">
                <thead><tr><th>Setup</th><th>Trades</th><th>Win Rate</th><th>Net</th><th>Expectancy</th><th>Score</th></tr></thead>
                <tbody>
                {% for r in setup_rows %}
                  <tr><td>{{ r.k }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.net) }}</td><td>{{ money(r.expectancy) }}</td><td>{{ '%.1f'|format(r.avg_score) if r.avg_score is not none else '—' }}</td></tr>
                {% endfor %}
                </tbody>
              </table></div>
              <div class="mobileOnly">
                <div class="grid">
                  {% for r in setup_rows %}
                    <div class="card"><div class="toolbar">
                      <div class="pill">{{ r.k }}</div>
                      <div class="metaRow">
                        <span class="meta">Trades: <b>{{ r.count }}</b></span>
                        <span class="meta">Win: <b>{{ '%.1f'|format(r.win_rate) }}%</b></span>
                        <span class="meta">Net: <b>{{ money(r.net) }}</b></span>
                        <span class="meta">Exp: <b>{{ money(r.expectancy) }}</b></span>
                      </div>
                    </div></div>
                  {% endfor %}
                </div>
              </div>
            </details>
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">⏱️ Time of Day Edge</div>
            {% set hour_top = hour_rows[0] if hour_rows|length else none %}
            <div class="statRow stack10">
              <div class="stat"><div class="k">Top Hour</div><div class="v">{% if hour_top %}{{ hour_top.k }}{% else %}—{% endif %}</div></div>
              <div class="stat"><div class="k">Top Hour Net</div><div class="v">{% if hour_top %}{{ money(hour_top.net) }}{% else %}—{% endif %}</div></div>
              <div class="stat"><div class="k">Top Hour Win Rate</div><div class="v">{% if hour_top %}{{ '%.1f'|format(hour_top.win_rate) }}%{% else %}—{% endif %}</div></div>
            </div>
            <details class="syncDetails stack10">
              <summary>Open time-of-day edge table</summary>
              <div class="hr"></div>
              <div class="tableWrap desktopOnly"><table class="tableDense">
                <thead><tr><th>Hour</th><th>Trades</th><th>Win Rate</th><th>Net</th><th>Expectancy</th><th>Score</th></tr></thead>
                <tbody>
                {% for r in hour_rows %}
                  <tr><td>{{ r.k }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.net) }}</td><td>{{ money(r.expectancy) }}</td><td>{{ '%.1f'|format(r.avg_score) if r.avg_score is not none else '—' }}</td></tr>
                {% endfor %}
                </tbody>
              </table></div>
              <div class="mobileOnly">
                <div class="grid">
                  {% for r in hour_rows %}
                    <div class="card"><div class="toolbar">
                      <div class="pill">{{ r.k }}</div>
                      <div class="metaRow">
                        <span class="meta">Trades: <b>{{ r.count }}</b></span>
                        <span class="meta">Win: <b>{{ '%.1f'|format(r.win_rate) }}%</b></span>
                        <span class="meta">Net: <b>{{ money(r.net) }}</b></span>
                      </div>
                    </div></div>
                  {% endfor %}
                </div>
              </div>
            </details>
          </div></div>
        </div>

        <div class="twoCol stack12">
          <div class="card"><div class="toolbar">
            <div class="pill">📈 Setup Edge Over Time (Monthly)</div>
            {% set setup_trend_top = setup_trend_rows[0] if setup_trend_rows|length else none %}
            <div class="statRow stack10">
              <div class="stat"><div class="k">Top Setup Trend</div><div class="v">{% if setup_trend_top %}{{ setup_trend_top.key }}{% else %}—{% endif %}</div></div>
              <div class="stat"><div class="k">Latest Period</div><div class="v">{% if setup_trend_top %}{{ setup_trend_top.period }}{% else %}—{% endif %}</div></div>
              <div class="stat"><div class="k">Latest Net</div><div class="v">{% if setup_trend_top %}{{ money(setup_trend_top.net) }}{% else %}—{% endif %}</div></div>
            </div>
            <details class="syncDetails stack10">
              <summary>Open setup trend table</summary>
              <div class="hr"></div>
              <div class="tableWrap desktopOnly"><table class="tableDense">
                <thead><tr><th>Setup</th><th>Period</th><th>Trades</th><th>Win Rate</th><th>Net</th><th>Expectancy</th></tr></thead>
                <tbody>
                {% for r in setup_trend_rows %}
                  <tr><td>{{ r.key }}</td><td>{{ r.period }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.net) }}</td><td>{{ money(r.expectancy) }}</td></tr>
                {% endfor %}
                {% if setup_trend_rows|length == 0 %}
                  <tr><td colspan="6">No setup trend data in range.</td></tr>
                {% endif %}
                </tbody>
              </table></div>
              <div class="mobileOnly">
                <div class="grid">
                  {% for r in setup_trend_rows %}
                    <div class="card"><div class="toolbar">
                      <div class="pill">{{ r.key }} · {{ r.period }}</div>
                      <div class="metaRow">
                        <span class="meta">Trades: <b>{{ r.count }}</b></span>
                        <span class="meta">Win: <b>{{ '%.1f'|format(r.win_rate) }}%</b></span>
                        <span class="meta">Net: <b>{{ money(r.net) }}</b></span>
                        <span class="meta">Exp: <b>{{ money(r.expectancy) }}</b></span>
                      </div>
                    </div></div>
                  {% endfor %}
                  {% if setup_trend_rows|length == 0 %}
                    <div class="card"><div class="toolbar"><div class="tiny">No setup trend data in range.</div></div></div>
                  {% endif %}
                </div>
              </div>
            </details>
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">📈 Session Edge Over Time (Monthly)</div>
            {% set session_trend_top = session_trend_rows[0] if session_trend_rows|length else none %}
            <div class="statRow stack10">
              <div class="stat"><div class="k">Top Session Trend</div><div class="v">{% if session_trend_top %}{{ session_trend_top.key }}{% else %}—{% endif %}</div></div>
              <div class="stat"><div class="k">Latest Period</div><div class="v">{% if session_trend_top %}{{ session_trend_top.period }}{% else %}—{% endif %}</div></div>
              <div class="stat"><div class="k">Latest Net</div><div class="v">{% if session_trend_top %}{{ money(session_trend_top.net) }}{% else %}—{% endif %}</div></div>
            </div>
            <details class="syncDetails stack10">
              <summary>Open session trend table</summary>
              <div class="hr"></div>
              <div class="tableWrap desktopOnly"><table class="tableDense">
                <thead><tr><th>Session</th><th>Period</th><th>Trades</th><th>Win Rate</th><th>Net</th><th>Expectancy</th></tr></thead>
                <tbody>
                {% for r in session_trend_rows %}
                  <tr><td>{{ r.key }}</td><td>{{ r.period }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.net) }}</td><td>{{ money(r.expectancy) }}</td></tr>
                {% endfor %}
                {% if session_trend_rows|length == 0 %}
                  <tr><td colspan="6">No session trend data in range.</td></tr>
                {% endif %}
                </tbody>
              </table></div>
              <div class="mobileOnly">
                <div class="grid">
                  {% for r in session_trend_rows %}
                    <div class="card"><div class="toolbar">
                      <div class="pill">{{ r.key }} · {{ r.period }}</div>
                      <div class="metaRow">
                        <span class="meta">Trades: <b>{{ r.count }}</b></span>
                        <span class="meta">Win: <b>{{ '%.1f'|format(r.win_rate) }}%</b></span>
                        <span class="meta">Net: <b>{{ money(r.net) }}</b></span>
                        <span class="meta">Exp: <b>{{ money(r.expectancy) }}</b></span>
                      </div>
                    </div></div>
                  {% endfor %}
                  {% if session_trend_rows|length == 0 %}
                    <div class="card"><div class="toolbar"><div class="tiny">No session trend data in range.</div></div></div>
                  {% endif %}
                </div>
              </div>
            </details>
          </div></div>
        </div>
        {% endif %}
        <script>
          (function(){
            const points = Array.from(document.querySelectorAll(".chartPoint"));
            if(!points.length){ return; }
            const tip = document.createElement("div");
            tip.style.position = "fixed";
            tip.style.zIndex = "9999";
            tip.style.pointerEvents = "none";
            tip.style.padding = "8px 10px";
            tip.style.borderRadius = "10px";
            tip.style.border = "1px solid rgba(53,212,255,.35)";
            tip.style.background = "rgba(4,12,20,.95)";
            tip.style.color = "#dff8ff";
            tip.style.fontSize = "12px";
            tip.style.lineHeight = "1.35";
            tip.style.boxShadow = "0 10px 24px rgba(0,0,0,.45)";
            tip.style.display = "none";
            document.body.appendChild(tip);

            function fmt(prefix, raw){
              const v = Number(raw || 0);
              if(prefix === "%"){ return `${v.toFixed(2)}%`; }
              if(prefix === "$"){ return `$${v.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}`; }
              return `${prefix || ""}${v.toFixed(2)}`;
            }
            function showTip(el, evt){
              const label = el.dataset.label || "Point";
              const series = el.dataset.series ? `${el.dataset.series} · ` : "";
              const prefix = el.dataset.prefix || "$";
              const value = fmt(prefix, el.dataset.value);
              const day = el.dataset.day || "";
              const drill = day ? `<div style="opacity:.8;margin-top:4px">Click to open ${day} trades</div>` : "";
              tip.innerHTML = `<div style="font-weight:800">${series}${label}</div><div>${value}</div>${drill}`;
              tip.style.display = "block";
              tip.style.left = `${evt.clientX + 12}px`;
              tip.style.top = `${evt.clientY + 12}px`;
            }
            function hideTip(){ tip.style.display = "none"; }

            points.forEach((pt)=>{
              pt.style.cursor = "pointer";
              pt.addEventListener("mouseenter", (e)=>showTip(pt, e));
              pt.addEventListener("mousemove", (e)=>showTip(pt, e));
              pt.addEventListener("mouseleave", hideTip);
              pt.addEventListener("click", ()=>{
                const day = (pt.dataset.day || "").trim();
                if(day){
                  window.location.href = `/trades?d=${encodeURIComponent(day)}`;
                }
              });
            });
          })();
        </script>
        """,
        perf=perf,
        dd=dd,
        corr=corr,
        insights=insights,
        setup_rows=setup_rows,
        session_rows=session_rows,
        setup_trend_rows=setup_trend_rows,
        session_trend_rows=session_trend_rows,
        hour_rows=hour_rows,
        rule_breaks=rule_breaks,
        equity_chart=equity_chart,
        drawdown_chart=drawdown_chart,
        expectancy_chart=expectancy_chart,
        benchmark_chart=benchmark_chart,
        vol_chart=vol_chart,
        equity_story=equity_story,
        drawdown_story=drawdown_story,
        expectancy_story=expectancy_story,
        vol_summary=vol_summary,
        fitz_22=fitz_22,
        integrity=integrity,
        edge_pulse=edge_pulse,
        control_pulse=control_pulse,
        start_date=start_date,
        end_date=end_date,
        explain_day=explain_day,
        expectancy_granularity=expectancy_granularity,
        expectancy_auto_switched=expectancy_auto_switched,
        tab=tab,
        money=money,
        day_story=day_story,
        sizing=sizing,
        sim=sim,
        heatmap=heatmap,
    )
    return render_page(content, active="analytics", title="McCain Capital 🏛️ · Analytics")


def session_replay_page():
    day = (request.args.get("date") or "").strip()
    rows = repo.fetch_analytics_rows()
    if not day:
        day = str(max((str(r.get("trade_date") or "") for r in rows), default=""))
    day_rows = [r for r in rows if str(r.get("trade_date") or "") == day]
    day_rows.sort(key=lambda r: int(r.get("id") or 0))
    running = 0.0
    timeline: List[Dict[str, Any]] = []
    for idx, r in enumerate(day_rows, start=1):
        net = float(r.get("net_pl") or 0.0)
        running += net
        timeline.append(
            {
                "step": idx,
                "id": int(r.get("id") or 0),
                "entry_time": str(r.get("entry_time") or "—"),
                "exit_time": str(r.get("exit_time") or "—"),
                "ticker": str(r.get("ticker") or ""),
                "opt_type": str(r.get("opt_type") or ""),
                "setup_tag": str(r.get("setup_tag") or ""),
                "session_tag": str(r.get("session_tag") or ""),
                "rule_break_tags": str(r.get("rule_break_tags") or ""),
                "checklist_score": r.get("checklist_score"),
                "net_pl": net,
                "equity_delta": running,
            }
        )
    wins = len([t for t in timeline if float(t["net_pl"]) > 0])
    losses = len([t for t in timeline if float(t["net_pl"]) < 0])
    day_net = sum(float(t["net_pl"]) for t in timeline)
    key_wins = [t for t in timeline if float(t["net_pl"]) > 0][:3]
    key_losses = sorted(
        [t for t in timeline if float(t["net_pl"]) < 0], key=lambda x: float(x["net_pl"])
    )[:3]
    rule_breaks = sorted(
        {
            p.strip()
            for t in timeline
            for p in str(t.get("rule_break_tags") or "").split(",")
            if p.strip()
        }
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
    notes_lines.append("Primary mistakes/rule breaks: " + (", ".join(rule_breaks) if rule_breaks else "none logged"))
    notes_lines.append("Action plan for next session:")
    notes_lines.append("- Keep A+ setups only in strongest time block.")
    notes_lines.append("- Respect max size and stop-after-streak guardrails.")
    replay_journal_href = "/new?" + urlencode(
        {
            "prefill": "replay",
            "d": day,
            "entry_type": "trade_debrief",
            "link_all_day": "1",
            "pnl": f"{day_net:.2f}",
            "notes": "\n".join(notes_lines),
            "template_notes": "Replay-linked debrief generated from Session Replay.",
            "setup": "Session Replay Debrief",
            "grade": "TBD",
        }
    )
    content = render_template_string(
        """
        <div class="card pageHero"><div class="toolbar">
          <div class="pageHeroHead">
            <div>
              <div class="pill">🎬 Session Replay</div>
              <h2 class="pageTitle">Day Reconstruction</h2>
              <div class="pageSub">Replay execution sequence, equity steps, and rule-break context for one day.</div>
            </div>
            <div class="actionRow">
              <a class="btn primary" href="{{ replay_journal_href }}">📝 Create Journal Entry</a>
              <a class="btn" href="/analytics">📈 Back Analytics</a>
            </div>
          </div>
          <div class="hr"></div>
          <form method="get" class="row">
            <div><label>Date</label><input type="date" name="date" value="{{ day }}" /></div>
            <div class="actionRow"><button class="btn primary" type="submit">Replay</button></div>
          </form>
        </div></div>
        <div class="metricStrip">
          <div class="metric"><div class="label">Trades</div><div class="value">{{ timeline|length }}</div></div>
          <div class="metric"><div class="label">Day Net</div><div class="value">{{ money(day_net) }}</div></div>
          <div class="metric"><div class="label">Wins / Losses</div><div class="value">{{ wins }} / {{ losses }}</div></div>
        </div>
        <div class="card"><div class="toolbar">
          <div class="pill">🧭 Timeline</div>
          <div class="tableWrap"><table class="tableDense">
            <thead><tr><th>#</th><th>Trade</th><th>Setup/Session</th><th>Score</th><th>Rule Breaks</th><th>Net</th><th>Equity Step</th></tr></thead>
            <tbody>
            {% for t in timeline %}
              <tr>
                <td>{{ t.step }}</td>
                <td>#{{ t.id }} · {{ t.entry_time }} → {{ t.exit_time }} · {{ t.ticker }} {{ t.opt_type }}</td>
                <td>{{ t.setup_tag or '—' }} / {{ t.session_tag or '—' }}</td>
                <td>{% if t.checklist_score is not none %}{{ t.checklist_score }}{% else %}—{% endif %}</td>
                <td>{{ t.rule_break_tags or '—' }}</td>
                <td>{{ money(t.net_pl) }}</td>
                <td>{{ money(t.equity_delta) }}</td>
              </tr>
            {% else %}
              <tr><td colspan="7">No trades found for selected day.</td></tr>
            {% endfor %}
            </tbody>
          </table></div>
        </div></div>
        """,
        day=day,
        timeline=timeline,
        day_net=day_net,
        wins=wins,
        losses=losses,
        replay_journal_href=replay_journal_href,
        money=money,
    )
    return render_page(content, active="analytics", title="McCain Capital 🏛️ · Session Replay")
