"""Analytics service module."""

from __future__ import annotations

from flask import render_template_string, request

from mccain_capital.repositories import analytics as repo
from mccain_capital.runtime import money
from mccain_capital.services.ui import render_page


def analytics_page():
    start_date = (request.args.get("start") or "").strip()
    end_date = (request.args.get("end") or "").strip()
    tab = (request.args.get("tab") or "performance").strip().lower()
    if tab not in {"performance", "behavior", "edge"}:
        tab = "performance"

    rows = repo.fetch_analytics_rows(start_date=start_date, end_date=end_date)
    perf = repo.performance_metrics(rows)
    setup_rows = repo.group_table(rows, "setup_tag")
    session_rows = repo.group_table(rows, "session_tag")
    hour_rows = repo.hour_bucket_table(rows)
    rule_breaks = repo.rule_break_counts(rows)

    content = render_template_string(
        """
        <div class="metricStrip">
          <div class="metric"><div class="label">Trades</div><div class="value">{{ perf.total_trades }}</div></div>
          <div class="metric"><div class="label">Win Rate</div><div class="value">{{ '%.1f'|format(perf.win_rate) }}%</div></div>
          <div class="metric"><div class="label">Expectancy</div><div class="value">{{ money(perf.expectancy) }}</div></div>
          <div class="metric"><div class="label">Profit Factor</div><div class="value">{{ '%.2f'|format(perf.profit_factor) if perf.profit_factor is not none else '∞' }}</div></div>
          <div class="metric"><div class="label">Max Drawdown</div><div class="value">{{ money(perf.max_drawdown) }}</div></div>
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
            <div style="display:flex;gap:10px;flex-wrap:wrap">
              <button class="btn" type="submit">Apply</button>
              <a class="btn" href="/analytics">Reset</a>
            </div>
          </form>
        </div></div>

        {% if tab == 'performance' %}
        <div class="twoCol" style="margin-top:12px">
          <div class="card"><div class="toolbar">
            <div class="pill">📈 Performance Summary</div>
            <div class="hr"></div>
            <table>
              <tbody>
                <tr><td>Total Net</td><td>{{ money(perf.total_net) }}</td></tr>
                <tr><td>Gross Profit</td><td>{{ money(perf.gross_profit) }}</td></tr>
                <tr><td>Gross Loss</td><td>{{ money(perf.gross_loss_abs) }}</td></tr>
                <tr><td>Avg Win</td><td>{{ money(perf.avg_win) }}</td></tr>
                <tr><td>Avg Loss</td><td>{{ money(perf.avg_loss_abs) }}</td></tr>
                <tr><td>Largest Win</td><td>{{ money(perf.max_win) }}</td></tr>
                <tr><td>Largest Loss</td><td>{{ money(perf.max_loss) }}</td></tr>
              </tbody>
            </table>
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">🔥 Streaks</div>
            <div class="hr"></div>
            <table>
              <tbody>
                <tr><td>Longest Win Streak</td><td>{{ perf.max_win_streak }}</td></tr>
                <tr><td>Longest Loss Streak</td><td>{{ perf.max_loss_streak }}</td></tr>
                <tr><td>Wins</td><td>{{ perf.wins }}</td></tr>
                <tr><td>Losses</td><td>{{ perf.losses }}</td></tr>
                <tr><td>Breakeven</td><td>{{ perf.breakeven }}</td></tr>
              </tbody>
            </table>
          </div></div>
        </div>
        {% elif tab == 'behavior' %}
        <div class="twoCol" style="margin-top:12px">
          <div class="card"><div class="toolbar">
            <div class="pill">🕒 Session Breakdown</div>
            <div class="hr"></div>
            <table>
              <thead><tr><th>Session</th><th>Trades</th><th>Win Rate</th><th>Net</th><th>Score</th></tr></thead>
              <tbody>
              {% for r in session_rows %}
                <tr><td>{{ r.k }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.net) }}</td><td>{{ '%.1f'|format(r.avg_score) if r.avg_score is not none else '—' }}</td></tr>
              {% endfor %}
              </tbody>
            </table>
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">⚠️ Rule Break Tags</div>
            <div class="hr"></div>
            <table>
              <thead><tr><th>Tag</th><th>Count</th></tr></thead>
              <tbody>
              {% for r in rule_breaks %}
                <tr><td>{{ r.tag }}</td><td>{{ r.count }}</td></tr>
              {% endfor %}
              {% if rule_breaks|length == 0 %}
                <tr><td colspan="2">No rule-break tags logged.</td></tr>
              {% endif %}
              </tbody>
            </table>
          </div></div>
        </div>
        {% else %}
        <div class="twoCol" style="margin-top:12px">
          <div class="card"><div class="toolbar">
            <div class="pill">📌 Setup Edge</div>
            <div class="hr"></div>
            <table>
              <thead><tr><th>Setup</th><th>Trades</th><th>Win Rate</th><th>Net</th><th>Score</th></tr></thead>
              <tbody>
              {% for r in setup_rows %}
                <tr><td>{{ r.k }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.net) }}</td><td>{{ '%.1f'|format(r.avg_score) if r.avg_score is not none else '—' }}</td></tr>
              {% endfor %}
              </tbody>
            </table>
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">⏱️ Time of Day Edge</div>
            <div class="hr"></div>
            <table>
              <thead><tr><th>Hour</th><th>Trades</th><th>Win Rate</th><th>Net</th><th>Score</th></tr></thead>
              <tbody>
              {% for r in hour_rows %}
                <tr><td>{{ r.k }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.net) }}</td><td>{{ '%.1f'|format(r.avg_score) if r.avg_score is not none else '—' }}</td></tr>
              {% endfor %}
              </tbody>
            </table>
          </div></div>
        </div>
        {% endif %}
        """,
        perf=perf,
        setup_rows=setup_rows,
        session_rows=session_rows,
        hour_rows=hour_rows,
        rule_breaks=rule_breaks,
        start_date=start_date,
        end_date=end_date,
        tab=tab,
        money=money,
    )
    return render_page(content, active="analytics", title="McCain Capital 🏛️ · Analytics")
