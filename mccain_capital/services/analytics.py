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
    equity_chart = _line_chart_svg(repo.equity_curve_series(rows), stroke="#35d4ff", y_prefix="$")
    drawdown_chart = _line_chart_svg(
        repo.drawdown_curve_series(rows), stroke="#ff5c7a", y_prefix="$"
    )
    expectancy_chart = _line_chart_svg(
        repo.expectancy_trend_series(rows), stroke="#8cff66", y_prefix="$"
    )

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
            {{ equity_chart|safe }}
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">📉 Drawdown Curve</div>
            <div class="hr"></div>
            {{ drawdown_chart|safe }}
          </div></div>
        </div>
        <div class="card stack12"><div class="toolbar">
          <div class="pill">🧠 Expectancy Trend (Monthly)</div>
          <div class="hr"></div>
          {{ expectancy_chart|safe }}
        </div></div>
        <div class="twoCol stack12">
          <div class="card"><div class="toolbar">
            <div class="pill">📈 Performance Summary</div>
            <div class="hr"></div>
            <div class="tableWrap"><table class="tableDense">
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
            <div class="tableWrap"><table class="tableDense">
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
            <div class="tableWrap"><table class="tableDense">
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
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">📈 Session Edge Over Time (Monthly)</div>
            <div class="hr"></div>
            <div class="tableWrap"><table class="tableDense">
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
          </div></div>
        </div>
        {% endif %}
        """,
        perf=perf,
        dd=dd,
        corr=corr,
        setup_rows=setup_rows,
        session_rows=session_rows,
        setup_trend_rows=setup_trend_rows,
        session_trend_rows=session_trend_rows,
        hour_rows=hour_rows,
        rule_breaks=rule_breaks,
        equity_chart=equity_chart,
        drawdown_chart=drawdown_chart,
        expectancy_chart=expectancy_chart,
        start_date=start_date,
        end_date=end_date,
        tab=tab,
        money=money,
    )
    return render_page(content, active="analytics", title="McCain Capital 🏛️ · Analytics")
