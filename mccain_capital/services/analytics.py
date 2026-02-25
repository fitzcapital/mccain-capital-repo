"""Analytics service module."""

from __future__ import annotations

import html
from typing import Any, Dict, List

from flask import render_template_string, request

from mccain_capital.repositories import analytics as repo
from mccain_capital.runtime import money
from mccain_capital.services.ui import render_page


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

    return f"""
    <svg viewBox="0 0 {int(width)} {int(height)}" role="img" aria-label="analytics line chart" style="width:100%;height:auto;display:block">
      <rect x="0" y="0" width="{int(width)}" height="{int(height)}" fill="rgba(4,10,20,.35)" rx="10" />
      {f'<line x1="{pad}" y1="{zero_y:.2f}" x2="{width - pad}" y2="{zero_y:.2f}" stroke="rgba(255,255,255,.2)" stroke-dasharray="4 4" />' if zero_y is not None else ""}
      <polyline fill="none" stroke="{stroke}" stroke-width="3" points="{points}" />
      <circle cx="{sx(len(values) - 1):.2f}" cy="{sy(values[-1]):.2f}" r="4.5" fill="{stroke}" />
    </svg>
    <div class="chartMeta">
      <span>Range: {y_prefix}{min_v:,.2f} → {y_prefix}{max_v:,.2f}</span>
      <span>Latest: {latest_label} ({y_prefix}{values[-1]:,.2f})</span>
    </div>
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


def analytics_page():
    start_date = (request.args.get("start") or "").strip()
    end_date = (request.args.get("end") or "").strip()
    tab = (request.args.get("tab") or "performance").strip().lower()
    if tab not in {"performance", "behavior", "edge"}:
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
    expectancy_series = repo.expectancy_trend_series(rows)
    equity_chart = _line_chart_svg(equity_series, stroke="#35d4ff", y_prefix="$")
    drawdown_chart = _line_chart_svg(drawdown_series, stroke="#ff5c7a", y_prefix="$")
    expectancy_chart = _line_chart_svg(expectancy_series, stroke="#8cff66", y_prefix="$")
    equity_story = _series_story(equity_series, favorable_direction="up")
    drawdown_story = _series_story(drawdown_series, favorable_direction="down")
    expectancy_story = _series_story(expectancy_series, favorable_direction="up")
    fitz_22 = repo.fitz_22_rev_indicator(rows)
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
          <div class="metric"><div class="label">Trades</div><div class="value">{{ perf.total_trades }}</div></div>
          <div class="metric"><div class="label">Win Rate</div><div class="value">{{ '%.1f'|format(perf.win_rate) }}%</div></div>
          <div class="metric"><div class="label">Expectancy</div><div class="value">{{ money(perf.expectancy) }}</div></div>
          <div class="metric"><div class="label">Profit Factor</div><div class="value">{{ '%.2f'|format(perf.profit_factor) if perf.profit_factor is not none else '∞' }}</div></div>
          <div class="metric"><div class="label">Max Drawdown</div><div class="value">{{ money(perf.max_drawdown) }}</div></div>
          <div class="metric"><div class="label">DD Streak (Max)</div><div class="value">{{ dd.max_drawdown_streak }}</div></div>
          <div class="metric"><div class="label">DD Streak (Current)</div><div class="value">{{ dd.current_drawdown_streak }}</div></div>
          <div class="metric"><div class="label">Quality ↔ PnL Corr</div><div class="value">{% if corr.r is not none %}{{ '%.2f'|format(corr.r) }}{% else %}—{% endif %}</div></div>
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
          <form method="get" class="row">
            <div><label>Start</label><input type="date" name="start" value="{{ start_date }}"></div>
            <div><label>End</label><input type="date" name="end" value="{{ end_date }}"></div>
            <div>
              <label>View</label>
              <select name="tab">
                <option value="performance" {% if tab == 'performance' %}selected{% endif %}>Performance</option>
                <option value="behavior" {% if tab == 'behavior' %}selected{% endif %}>Behavior</option>
                <option value="edge" {% if tab == 'edge' %}selected{% endif %}>Edge</option>
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
        <div class="card stack12"><div class="toolbar">
          <div class="pill">🧠 Expectancy Trend (Monthly)</div>
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
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">🔥 Streaks</div>
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
          </div></div>
        </div>
        {% elif tab == 'behavior' %}
        <div class="twoCol stack12">
          <div class="card"><div class="toolbar">
            <div class="pill">🕒 Session Breakdown</div>
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
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">⚠️ Rule Break Tags</div>
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
          </div></div>
        </div>
        {% else %}
        <div class="twoCol stack12">
          <div class="card"><div class="toolbar">
            <div class="pill">📌 Setup Edge</div>
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
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">⏱️ Time of Day Edge</div>
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
          </div></div>
        </div>

        <div class="twoCol stack12">
          <div class="card"><div class="toolbar">
            <div class="pill">📈 Setup Edge Over Time (Monthly)</div>
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
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">📈 Session Edge Over Time (Monthly)</div>
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
          </div></div>
        </div>
        {% endif %}
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
        equity_story=equity_story,
        drawdown_story=drawdown_story,
        expectancy_story=expectancy_story,
        fitz_22=fitz_22,
        edge_pulse=edge_pulse,
        control_pulse=control_pulse,
        start_date=start_date,
        end_date=end_date,
        tab=tab,
        money=money,
    )
    return render_page(content, active="analytics", title="McCain Capital 🏛️ · Analytics")
