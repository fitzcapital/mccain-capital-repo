"""Trades domain service functions."""

from __future__ import annotations

import os
import sqlite3
import json
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError
from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import (
    abort,
    flash,
    jsonify,
    redirect,
    render_template_string,
    request,
    send_file,
    url_for,
)
from werkzeug.utils import secure_filename

from mccain_capital.repositories import trades as repo
from mccain_capital.runtime import (
    UPLOAD_DIR,
    db,
    detect_paste_format,
    latest_balance_overall,
    money,
    next_trading_day_iso,
    now_iso,
    normalize_opt_type,
    parse_float,
    parse_int,
    pct,
    prev_trading_day_iso,
    today_iso,
)
from mccain_capital.services import trades_importing as importing
from mccain_capital.services import vanquish_live_sync
from mccain_capital.services.ui import render_page, simple_msg

# Compatibility aliases used by extracted route bodies.
fetch_trades = repo.fetch_trades
fetch_trade_reviews_map = repo.fetch_trade_reviews_map
trade_day_stats = repo.trade_day_stats
calc_consistency = repo.calc_consistency
week_total_net = repo.week_total_net
last_balance_in_list = repo.last_balance_in_list

BROKER_SYNC_CONFIG_PATH = os.path.join(UPLOAD_DIR, ".vanquish_sync.json")
BROKER_DEBUG_DIR = os.path.join(UPLOAD_DIR, "vanquish_debug")


def _load_broker_sync_config() -> Dict[str, str]:
    defaults = {
        "base_url": os.environ.get(
            "VANQUISH_STATEMENT_URL", "https://trade.vanquishtrader.com/account/statement/"
        ),
        "wl": os.environ.get("VANQUISH_WL", "vanquishtrader"),
        "account": os.environ.get("VANQUISH_ACCOUNT", ""),
        "time_zone": os.environ.get("VANQUISH_TIME_ZONE", "America/New_York"),
        "date_locale": os.environ.get("VANQUISH_DATE_LOCALE", "en-US"),
        "report_locale": os.environ.get("VANQUISH_REPORT_LOCALE", "en"),
        "token": os.environ.get("VANQUISH_TOKEN", ""),
    }
    try:
        with open(BROKER_SYNC_CONFIG_PATH, "r", encoding="utf-8") as f:
            parsed = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return defaults
    for key in defaults:
        val = parsed.get(key, defaults[key])
        defaults[key] = str(val).strip() if val is not None else defaults[key]
    return defaults


def _save_broker_sync_config(data: Dict[str, str]) -> None:
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    with open(BROKER_SYNC_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _debug_relative(path: str) -> str:
    rel = os.path.relpath(path, UPLOAD_DIR)
    return rel.replace("\\", "/")


def _debug_safe_path(rel: str) -> str:
    clean = (rel or "").replace("\\", "/").lstrip("/")
    abs_path = os.path.abspath(os.path.join(UPLOAD_DIR, clean))
    root = os.path.abspath(UPLOAD_DIR)
    if not abs_path.startswith(root + os.sep) and abs_path != root:
        raise ValueError("unsafe path")
    return abs_path


def _render_live_debug_result(
    *,
    folder_rel: str,
    artifacts_rel: List[str],
    warns: List[str],
    error: str = "",
):
    return render_page(
        render_template_string(
            """
            <div class="card"><div class="toolbar">
              <div class="pill">🧪 Live Sync Debug Artifacts</div>
              <div class="tiny stack10 line15">Captured from headless run. Use these files to map selectors and flow states.</div>
              <div class="hr"></div>
              {% if error %}
                <div class="tiny metaRed line16">Error: {{ error }}</div>
                <div class="hr"></div>
              {% endif %}
              {% if warns %}
                <div class="tiny metaBlue line16">{% for w in warns %}• {{ w }}<br>{% endfor %}</div>
                <div class="hr"></div>
              {% endif %}
              <div class="tiny"><b>Folder:</b> {{ folder_rel }}</div>
              <div class="hr"></div>
              <div class="stack10">
                {% for rel in artifacts_rel %}
                  <div><a class="btn" href="/trades/sync/debug/{{ rel }}" target="_blank" rel="noopener">{{ rel }}</a></div>
                {% endfor %}
              </div>
              <div class="hr"></div>
              <div class="rightActions">
                <a class="btn primary" href="/trades/upload/statement">Back to Sync</a>
              </div>
            </div></div>
            """,
            folder_rel=folder_rel,
            artifacts_rel=artifacts_rel,
            warns=warns,
            error=error,
        ),
        active="trades",
    )


def _normalize_iso_date(raw: str, fallback: str) -> str:
    v = (raw or "").strip()
    if not v:
        return fallback
    try:
        return datetime.strptime(v, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return fallback


def _fetch_statement_html(
    *,
    base_url: str,
    token: str,
    wl: str,
    from_date: str,
    to_date: str,
    time_zone: str,
    account: str,
    date_locale: str,
    report_locale: str,
) -> str:
    query = urllib.parse.urlencode(
        {
            "token": token,
            "wl": wl,
            "format": "html",
            "from": from_date,
            "to": to_date,
            "timeZone": time_zone,
            "account": account,
            "dateLocale": date_locale,
            "reportLocale": report_locale,
        }
    )
    url = f"{base_url.rstrip('/')}?{query}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "McCainCapitalBrokerSync/1.0",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = resp.read()
    return payload.decode("utf-8", errors="replace")


def _handle_statement_html_import(path: str, mode: str, source_label: str):
    paste_text, balance_val, warns = importing.parse_statement_html_to_broker_paste(path)

    if mode == "broker":
        if not paste_text:
            return render_page(
                render_template_string(
                    """
                    <div class="card"><div class="toolbar">
                      <div class="pill">⛔ HTML parsed, but no trade rows found</div>
                      <div class="hr"></div>
                      <div class="tiny metaBlue line16">
                        {% for m in warns %}• {{ m }}<br>{% endfor %}
                      </div>
                      <div class="hr"></div>
                      <a class="btn" href="/trades/upload/statement">Back</a>
                    </div></div>
                    """,
                    warns=warns or [],
                ),
                active="trades",
            )

        inserted, errors, report = importing.insert_trades_from_broker_paste_with_report(
            paste_text, ending_balance=balance_val
        )
        reconciliation_html = _reconciliation_block(report)
        msgs = (warns or []) + (errors or [])

        return render_page(
            render_template_string(
                """
                <div class="card"><div class="toolbar">
                  <div class="pill">🧾 HTML → Trades ✅</div>
                  <div class="stack10">Inserted <b>{{ inserted }}</b> trade{{ '' if inserted==1 else 's' }}.</div>
                  {% if msgs %}
                    <div class="hr"></div>
                    <div class="tiny metaBlue line16">
                      {% for m in msgs %}• {{ m }}<br>{% endfor %}
                    </div>
                  {% endif %}
                  {{ reconciliation_html|safe }}
                  <div class="hr"></div>
                  <a class="btn primary" href="/trades">Trades 📅</a>
                  <a class="btn" href="/trades/upload/statement">Upload Another</a>
                </div></div>
                """,
                inserted=inserted,
                msgs=msgs,
                reconciliation_html=reconciliation_html,
            ),
            active="trades",
        )

    if balance_val is None:
        return render_page(
            render_template_string(
                """
                <div class="card"><div class="toolbar">
                  <div class="pill">⛔ Balance not found in HTML</div>
                  <div class="hr"></div>
                  <div class="tiny metaBlue line16">
                    {% for m in warns %}• {{ m }}<br>{% endfor %}
                  </div>
                  <div class="hr"></div>
                  <a class="btn" href="/trades/upload/statement">Back</a>
                </div></div>
                """,
                warns=warns or [],
            ),
            active="trades",
        )

    importing.insert_balance_snapshot(today_iso(), balance_val, raw_line=source_label)
    return redirect(url_for("trades_page"))


def _reconciliation_block(report: Optional[dict]) -> str:
    if not report:
        return ""
    return render_template_string(
        """
        <div class="hr"></div>
        <div class="pill">🧾 Import Reconciliation</div>
        <div class="hr"></div>
        <table>
          <tbody>
            <tr><td>Fills Parsed</td><td>{{ report.fills_parsed }}</td></tr>
            <tr><td>Round-Trips Paired</td><td>{{ report.pairs_completed }}</td></tr>
            <tr><td>Inserted Trades</td><td>{{ report.inserted_trades }}</td></tr>
            <tr><td>Duplicates Skipped</td><td>{{ report.duplicates_skipped }}</td></tr>
            <tr><td>Open Contracts Remaining</td><td>{{ report.open_contracts }}</td></tr>
            <tr><td>Errors / Warnings</td><td>{{ report.errors_count }} / {{ report.warnings_count }}</td></tr>
            <tr>
              <td>Statement Ending Balance</td>
              <td>
                {% if report.statement_ending_balance is not none %}{{ money(report.statement_ending_balance) }}{% else %}Not provided{% endif %}
              </td>
            </tr>
            <tr>
              <td>Ledger Ending Balance</td>
              <td>
                {% if report.ledger_ending_balance is not none %}{{ money(report.ledger_ending_balance) }}{% else %}Not available{% endif %}
              </td>
            </tr>
            <tr>
              <td>Balance Delta (Ledger - Statement)</td>
              <td>
                {% if report.balance_delta is not none %}{{ money(report.balance_delta) }}{% else %}Not computed{% endif %}
              </td>
            </tr>
          </tbody>
        </table>
      """,
        report=report,
        money=money,
    )


def trade_lockout_state(day_iso: str):
    rc = repo.get_risk_controls()
    return repo.trade_lockout_state(
        day_iso,
        daily_max_loss=float(rc.get("daily_max_loss", 0.0) or 0.0),
        enforce_lockout=int(rc.get("enforce_lockout", 0) or 0),
    )


def trades_page():
    d = request.args.get("d", "")
    active_day = d or today_iso()

    prev_day = prev_trading_day_iso(active_day)
    next_day = next_trading_day_iso(active_day)

    q = request.args.get("q", "")

    # ✅ Convert sqlite3.Row -> dict so Jinja can use .get() and ['key']
    raw_trades = fetch_trades(d=d, q=q)
    trades = [dict(r) for r in raw_trades]
    review_map = fetch_trade_reviews_map([int(t["id"]) for t in trades if t.get("id") is not None])
    for t in trades:
        rv = review_map.get(int(t["id"]), {})
        t["setup_tag"] = rv.get("setup_tag", "")
        t["session_tag"] = rv.get("session_tag", "")
        t["checklist_score"] = rv.get("checklist_score", None)
        t["rule_break_tags"] = rv.get("rule_break_tags", "")

    stats = trade_day_stats(trades)  # likely dict
    cons = calc_consistency(trades)  # dict-like expected
    guardrail = trade_lockout_state(active_day)

    week_total = week_total_net(d or None)
    bal_in_day = last_balance_in_list(trades)
    overall_bal = latest_balance_overall(as_of=active_day)
    display_balance = bal_in_day if bal_in_day is not None else overall_bal
    day_net = float(
        (stats["total"] if isinstance(stats, dict) else getattr(stats, "total", 0.0)) or 0.0
    )
    win_rate = float(
        (stats["win_rate"] if isinstance(stats, dict) else getattr(stats, "win_rate", 0.0)) or 0.0
    )
    trades_count = len(trades)
    avg_net = (day_net / trades_count) if trades_count else 0.0
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

    next_action_msg = (
        "Tag every trade with setup/session and complete missing review scores before day end."
        if trades_count
        else "Import statement or add first trade, then complete setup/session review tags."
    )

    content = render_template_string(
        """
        <div class="card pageHero">
          <div class="toolbar">
            <div class="pageHeroHead">
              <div>
                <div class="pill">📅 Trades Workspace</div>
                <h2 class="pageTitle">Execution Log</h2>
                <div class="pageSub">Log, review, and import trades with fast context on risk, consistency, and current exposure.</div>
              </div>
              <div class="actionRow">
                <a class="btn primary" href="/trades/new">➕ Add Trade</a>
                <a class="btn" href="/trades/open-positions">📂 Open Positions</a>
                <a class="btn" href="/trades/reviews/rebuild">🛠️ Rebuild Reviews</a>
              </div>
            </div>
          </div>
        </div>

        <div class="metricStrip">
          <div class="metric">
            <div class="label">Day Net</div>
            <div class="value">{{ money(day_net) }}</div>
            <div class="sub">Filtered by selected day/search</div>
          </div>
          <div class="metric">
            <div class="label">Win Rate</div>
            <div class="value">{{ '%.1f'|format(win_rate) }}%</div>
            <div class="sub">Execution quality for this day</div>
          </div>
          <div class="metric">
            <div class="label">Trades Logged</div>
            <div class="value">{{ trades_count }}</div>
            <div class="sub">Disciplined volume beats overtrading</div>
          </div>
          <div class="metric">
            <div class="label">Displayed Balance</div>
            <div class="value">{{ money(display_balance) }}</div>
            <div class="sub">End-of-day if available, otherwise latest</div>
          </div>
        </div>

        <div class="insightGrid stack12">
          <div class="insightCard">
            <div class="insightTitle">🔎 Execution Read</div>
            <div class="insightBody">{{ execution_msg }}</div>
          </div>
          <div class="insightCard">
            <div class="insightTitle">🛡️ Risk State</div>
            <div class="insightBody">{{ risk_msg }}</div>
          </div>
          <div class="insightCard">
            <div class="insightTitle">➡️ Next Action</div>
            <div class="insightBody">{{ next_action_msg }}</div>
          </div>
        </div>

        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <form method="get" action="/trades" class="row">
              <div class="fieldGrow2">
                <label for="search">🔎 Search Trades 🎯</label>
                <input id="search" name="q" value="{{ q }}" placeholder="SPX, CALL, PUT…" />
              </div>
              <div class="fieldGrow1">
                <label>📆 Date</label>
                <input type="date" name="d" value="{{ d }}" />
              </div>
              <div class="actionRow">
                <a class="btn" href="/trades?d={{ prev_day }}&q={{ q }}">⬅️ Prev</a>
                <a class="btn" href="/trades?d={{ next_day }}&q={{ q }}">Next ➡️</a>
                <button class="btn" type="submit">🧲 Filter</button>
                <a class="btn" href="/trades">♻️ Reset</a>
                <a class="btn primary" href="/trades/new">➕ Manual Add</a>
                <a class="btn primary" href="/trades/paste">📋 Table Paste</a>
                <a class="btn primary" href="/trades/upload/statement">📄 Upload Statement</a>
              </div>
            </form>
            <div class="tradesMobileActions">
              <div class="mobileActionGrid">
                <a class="btn primary" href="/trades/new">➕ Add</a>
                <a class="btn primary" href="/trades/paste">📋 Paste</a>
                <a class="btn primary" href="/trades/upload/statement">📄 Upload</a>
                <a class="btn" href="/trades/open-positions">📂 Open</a>
              </div>
            </div>

            <div class="hr"></div>
            <div class="statRow">
              <div class="stat {% if guardrail.locked %}glow-red{% else %}glow-green{% endif %}">
                <div class="k">🛡️ Guardrail Status</div>
                <div class="v">{% if guardrail.locked %}LOCKED{% else %}ACTIVE{% endif %}</div>
                <div class="tiny">
                  Day Net {{ money(guardrail.day_net) }} / Max Loss {{ money(guardrail.daily_max_loss) }}
                </div>
              </div>
              <div class="stat">
                <div class="k">⚙️ Risk Controls</div>
                <div class="v"><a class="btn primary btnCompact" href="/trades/risk-controls">Configure</a></div>
              </div>
              <div class="stat">
                <div class="k">📈 Edge Analytics</div>
                <div class="v"><a class="btn primary btnCompact" href="/analytics">Open</a></div>
              </div>
            </div>

            <div class="hr"></div>
            <div class="statRow">
              <div class="stat">
                <div class="k">💰 Day Net (filtered)</div>
                <div class="v">{{ money(stats['total'] if stats is mapping else stats.total) }}</div>
              </div>

              <div class="stat {% if week_total > 0 %}glow-green{% elif week_total < 0 %}glow-red{% endif %}">
                <div class="k">📅 Week Total</div>
                <div class="v">{{ money(week_total) }}</div>
              </div>

              <div class="stat">
                <div class="k">🏦 Balance</div>
                <div class="v">{{ money(display_balance) }}</div>
              </div>

              <div class="stat">
                <div class="k">✅ Wins</div>
                <div class="v">{{ stats['wins'] if stats is mapping else stats.wins }}</div>
              </div>

              <div class="stat">
                <div class="k">❌ Losses</div>
                <div class="v">{{ stats['losses'] if stats is mapping else stats.losses }}</div>
              </div>

              <div class="stat">
                <div class="k">🎯 Win Rate</div>
                <div class="v">
                  {{ '%.1f'|format((stats['win_rate'] if stats is mapping else stats.win_rate)) }}%
                </div>
              </div>

              <div class="stat">
                <div class="k">⚖️ W/L Ratio</div>
                <div class="v">
                  {{ '%.2f'|format((stats['wl_ratio'] if stats is mapping else stats.wl_ratio)) }}
                </div>
              </div>

              <div class="stat {{ cons.class }}">
                <div class="k">🎯 Consistency</div>
                <div class="v">
                  {% if cons.ratio is none %}
                    —
                  {% else %}
                    {{ '%.1f'|format(cons.ratio * 100) }}%
                  {% endif %}
                </div>
                <div class="tiny">
                  Max: {{ money(cons.biggest) }} / {{ money(cons.denom) }}
                </div>
              </div>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <form id="clear-trades" method="post" action="/trades/clear" class="inlineForm"></form>
              <button class="btn danger" type="button" onclick="confirmClear('clear-trades')">🧼 Clear</button>
              <a class="btn" href="/trades/open-positions">📂 Open Positions</a>
              <a class="btn" href="/trades/reviews/rebuild">🛠️ Rebuild Reviews</a>
              <a class="btn" href="/dashboard">📊 Calendar</a>
              <a class="btn" href="/calculator">🧮 Calculator</a>
            </div>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">🧠 Paste Format</div>
            <div class="tiny stack10 line15">
              Table paste = tab-delimited rows. Broker paste = "instrument | dt | side | qty | price | fee". ✅
            </div>
          </div></div>
        </div>

        <div class="card stack12"><div class="toolbar">
          <div class="pill">🧾 Trades ({{ trades|length }})</div>
          <div class="hr"></div>

          <!-- Bulk actions: multi-select delete / copy -->
          <div class="row bulkActions">
            <label class="pill bulkSelectLabel">
              <input type="checkbox" id="selectAll" />
              Select all
            </label>
            <span class="meta bulkCount" id="selectedCount">0 selected</span>
            <button class="btn danger" id="bulkDeleteBtn" disabled>🗑️ Delete selected</button>

            <span class="hr bulkSpacer"></span>

            <label class="meta bulkCopyLabel">
              Copy to:
              <input type="date" id="bulkCopyDate" class="bulkCopyDate" value="{{ d }}" />
            </label>
            <button class="btn" id="bulkCopyBtn" disabled>📋 Copy selected</button>
          </div>

          <div class="tradesDesktop tableWrap">
            <table class="tableDense">
              <thead>
                <tr>
                  <th class="thSelect"></th>
                  <th>📆 Date</th>
                  <th>⏱️ Time</th>
                  <th>🏷️</th>
                  <th>📌</th>
                  <th>🧠 Setup</th>
                  <th>✅ Score</th>
                  <th>❌ Strike</th>
                  <th>🧾 C</th>
                  <th>💳 Spend</th>
                  <th>💰 Entry</th>
                  <th>💰 Exit</th>
                  <th>🛑20% Risk</th>
                  <th>💵 Net</th>
                  <th>📊%</th>
                  <th>🏦 Bal</th>
                  <th class="thActions">Actions</th>

                </tr>
              </thead>
              <tbody>
                {% for t in trades %}
                <tr>
                  <td><input type="checkbox" class="tradeCheckbox" data-id="{{ t['id'] }}" aria-label="Select trade {{ t['id'] }}"></td>
                  <td>{{ t['trade_date'] }}</td>

                  <td>
                    {% set et = t.get('entry_time') %}
                    {% set xt = t.get('exit_time') %}
                    {% if et and xt %}
                      {{ et }} → {{ xt }}
                    {% elif et %}
                      {{ et }}
                    {% elif xt %}
                      {{ xt }}
                    {% else %}
                      —
                    {% endif %}
                  </td>

                  <td><b>{{ t['ticker'] }}</b></td>
                  <td>{{ t['opt_type'] }}</td>
                  <td>{{ t.get('setup_tag') or '—' }}</td>
                  <td>{% if t.get('checklist_score') is not none %}{{ t.get('checklist_score') }}{% else %}—{% endif %}</td>
                  <td>{{ '' if t['strike'] is none else t['strike'] }}</td>
                  <td>{{ '' if t['contracts'] is none else t['contracts'] }}</td>
                  <td>{{ money(t['total_spent']) }}</td>
                  <td>{{ money(t['entry_price']) }}</td>
                  <td>{{ money(t['exit_price']) }}</td>
                  <td><span class="cell-red">{{ money((t['total_spent'] or 0) * 0.20) }}</span></td>
                  <td>
  {% set n = t.get('net_pl') %}
  {% if n is none %}
    <span class="pl-zero">—</span>
  {% elif n > 0 %}
    <span class="pl-pos">{{ money(n) }}</span>
  {% elif n < 0 %}
    <span class="pl-neg">{{ money(n) }}</span>
  {% else %}
    <span class="pl-zero">{{ money(n) }}</span>
  {% endif %}
</td>
                  {% set rp = t.get('result_pct') %}
<td>
  {% if rp is none %}
    <span class="muted">–</span>
  {% elif rp < 10 %}
    <span class="cell-red">{{ pct(rp) }}</span>
  {% elif rp > 20 %}
    <span class="cell-green">{{ pct(rp) }}</span>
  {% elif rp > 15 %}
    <span class="cell-orange">{{ pct(rp) }}</span>
  {% else %}
    <span class="muted">{{ pct(rp) }}</span>
  {% endif %}
</td>

                  <td>{{ money(t['balance']) }}</td>
                  <td class="tdActions">
                    <div class="rowActions" id="rowActions-{{ t['id'] }}">
                      <button type="button" class="rowMoreBtn" onclick="toggleRowMenu('{{ t['id'] }}', event)" aria-label="Trade actions">▾</button>
                      <div class="rowMoreMenu" id="rowMenu-{{ t['id'] }}">
                        <a class="rowMenuItem" href="/trades/edit/{{ t['id'] }}?d={{ d }}&q={{ q }}">✏️ Edit</a>
                        <a class="rowMenuItem" href="/trades/review/{{ t['id'] }}?d={{ d }}&q={{ q }}">🧠 Review</a>
                        <form method="post" action="/trades/duplicate/{{ t['id'] }}?d={{ d }}&q={{ q }}">
  <button class="rowMenuItem" type="submit">📄 Duplicate</button>
</form>

                        <form id="del-t-{{ t['id'] }}" method="post"
                              action="/trades/delete/{{ t['id'] }}?d={{ d }}&q={{ q }}"
                              onsubmit="return confirm('Delete this trade?');">
                          <button class="rowMenuItem danger" type="submit">🗑️ Delete</button>
                        </form>
                      </div>
                    </div>
                  </td>
                </tr>
              {% endfor %}

              {% if trades|length == 0 %}
                <tr><td colspan="17" class="meta">No trades yet. Click <b>📋 Paste</b> and feed the beast 😈</td></tr>
              {% endif %}
              </tbody>
            </table>
          </div>

          <div class="tradesMobileList">
            {% for t in trades %}
              {% set n = t.get('net_pl') %}
              {% set rp = t.get('result_pct') %}
              <article class="tradeCard">
                <div class="tradeCardHead">
                  <div class="pill">{{ t['trade_date'] }}</div>
                  <div class="pill">{{ t['ticker'] }} {{ t['opt_type'] or '' }}</div>
                </div>
                <div class="tradeCardGrid">
                  <div><span class="meta">Time</span><div>{{ (t.get('entry_time') or '—') ~ (' → ' ~ t.get('exit_time') if t.get('exit_time') else '') }}</div></div>
                  <div><span class="meta">Strike / C</span><div>{{ '' if t['strike'] is none else t['strike'] }} / {{ '' if t['contracts'] is none else t['contracts'] }}</div></div>
                  <div><span class="meta">Setup / Session</span><div>{{ t.get('setup_tag') or '—' }} / {{ t.get('session_tag') or '—' }}</div></div>
                  <div><span class="meta">Checklist Score</span><div>{% if t.get('checklist_score') is not none %}{{ t.get('checklist_score') }}{% else %}—{% endif %}</div></div>
                  <div><span class="meta">Entry → Exit</span><div>{{ money(t['entry_price']) }} → {{ money(t['exit_price']) }}</div></div>
                  <div><span class="meta">Risk (20%)</span><div class="cell-red">{{ money((t['total_spent'] or 0) * 0.20) }}</div></div>
                  <div><span class="meta">Net</span>
                    <div>
                      {% if n is none %}<span class="pl-zero">—</span>
                      {% elif n > 0 %}<span class="pl-pos">{{ money(n) }}</span>
                      {% elif n < 0 %}<span class="pl-neg">{{ money(n) }}</span>
                      {% else %}<span class="pl-zero">{{ money(n) }}</span>{% endif %}
                    </div>
                  </div>
                  <div><span class="meta">Result / Balance</span>
                    <div>
                      {% if rp is none %}<span class="muted">–</span>
                      {% elif rp < 10 %}<span class="cell-red">{{ pct(rp) }}</span>
                      {% elif rp > 20 %}<span class="cell-green">{{ pct(rp) }}</span>
                      {% elif rp > 15 %}<span class="cell-orange">{{ pct(rp) }}</span>
                      {% else %}<span class="muted">{{ pct(rp) }}</span>{% endif %}
                      · {{ money(t['balance']) }}
                    </div>
                  </div>
                </div>
                <div class="mobileActionGrid stack10">
                  <a class="btn" href="/trades/edit/{{ t['id'] }}?d={{ d }}&q={{ q }}">✏️ Edit</a>
                  <a class="btn" href="/trades/review/{{ t['id'] }}?d={{ d }}&q={{ q }}">🧠 Review</a>
                  <form method="post" action="/trades/duplicate/{{ t['id'] }}?d={{ d }}&q={{ q }}" class="inlineForm">
                    <button class="btn" type="submit">📄 Duplicate</button>
                  </form>
                  <form method="post" action="/trades/delete/{{ t['id'] }}?d={{ d }}&q={{ q }}" onsubmit="return confirm('Delete this trade?');" class="inlineForm">
                    <button class="btn danger" type="submit">🗑️ Delete</button>
                  </form>
                </div>
              </article>
            {% endfor %}
          </div>
        </div></div>

<script>
  function closeAllRowMenus() {
  document.querySelectorAll('.rowMoreMenu.open').forEach(menu => closeRowMenu(menu));
}

function closeRowMenu(menu) {
  menu.classList.remove('open');
  menu.style.position = '';
  menu.style.left = '';
  menu.style.top = '';
  menu.style.visibility = '';
  const originId = menu.dataset.origin;
  if (originId) {
    const origin = document.getElementById(originId);
    if (origin) origin.appendChild(menu);
  }
}

function openRowMenu(tradeId, btn, menu) {
  // Remember where the menu lives so we can put it back
  const origin = btn.closest('.rowActions');
  if (origin && origin.id) menu.dataset.origin = origin.id;

  // Move to <body> so it isn't clipped by scroll/overflow containers
  document.body.appendChild(menu);
  menu.classList.add('open');

  // Position near the button (fixed)
  const rect = btn.getBoundingClientRect();

  // Measure after open so width is accurate
  menu.style.visibility = 'hidden';
  const w = menu.offsetWidth || 180;
  const h = menu.offsetHeight || 120;

  let left = rect.right - w;
  let top  = rect.bottom + 6;

  const pad = 8;
  left = Math.max(pad, Math.min(left, window.innerWidth - w - pad));
  top  = Math.max(pad, Math.min(top,  window.innerHeight - h - pad));

  menu.style.position = 'fixed';
  menu.style.left = left + 'px';
  menu.style.top = top + 'px';
  menu.style.visibility = '';
}

// Close on outside click / escape
document.addEventListener('click', function(e){
  if (e.target.closest('.rowMoreMenu') || e.target.closest('.rowMoreBtn')) return;
  closeAllRowMenus();
});
document.addEventListener('keydown', function(e){
  if (e.key === 'Escape') closeAllRowMenus();
});
window.addEventListener('resize', closeAllRowMenus);
window.addEventListener('scroll', closeAllRowMenus, true);

// -----------------------------
// Bulk select / delete / copy
// -----------------------------
const selCountEl = document.getElementById('selectedCount');
const bulkDeleteBtn = document.getElementById('bulkDeleteBtn');
const bulkCopyBtn = document.getElementById('bulkCopyBtn');
const bulkCopyDate = document.getElementById('bulkCopyDate');
const selectAll = document.getElementById('selectAll');

function getSelectedIds() {
  return Array.from(document.querySelectorAll('.tradeCheckbox:checked'))
    .map(cb => parseInt(cb.dataset.id || '', 10))
    .filter(n => Number.isFinite(n));
}

function refreshBulkUi() {
  const ids = getSelectedIds();
  if (selCountEl) selCountEl.textContent = ids.length + ' selected';
  if (bulkDeleteBtn) bulkDeleteBtn.disabled = ids.length === 0;
  if (bulkCopyBtn) bulkCopyBtn.disabled = ids.length === 0;

  if (selectAll) {
    const all = document.querySelectorAll('.tradeCheckbox');
    const checked = document.querySelectorAll('.tradeCheckbox:checked');
    const allCount = all.length;
    const checkedCount = checked.length;
    selectAll.checked = allCount > 0 && checkedCount === allCount;
    selectAll.indeterminate = checkedCount > 0 && checkedCount < allCount;
  }
}

if (selectAll) {
  selectAll.addEventListener('change', () => {
    const on = selectAll.checked;
    document.querySelectorAll('.tradeCheckbox').forEach(cb => { cb.checked = on; });
    refreshBulkUi();
  });
}
document.querySelectorAll('.tradeCheckbox').forEach(cb => cb.addEventListener('change', refreshBulkUi));
refreshBulkUi();

if (bulkDeleteBtn) {
  bulkDeleteBtn.addEventListener('click', async () => {
    const ids = getSelectedIds();
    if (!ids.length) return;
    if (!confirm(`Delete ${ids.length} selected trade(s)? This can't be undone.`)) return;
    const r = await fetch(`/trades/delete_many?d={{ d }}&q={{ q|urlencode }}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ids })
    });
    if (r.ok) {
      location.reload();
    } else {
      alert('Delete failed: ' + (await r.text()));
    }
  });
}

if (bulkCopyBtn) {
  bulkCopyBtn.addEventListener('click', async () => {
    const ids = getSelectedIds();
    if (!ids.length) return;
    const target_date = (bulkCopyDate && bulkCopyDate.value) ? bulkCopyDate.value : '{{ d }}';
    const r = await fetch(`/trades/copy_many?d={{ d }}&q={{ q|urlencode }}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ids, target_date })
    });
    if (r.ok) {
      // jump to the day you copied into
      window.location.href = `/trades?d=${encodeURIComponent(target_date)}`;
    } else {
      alert('Copy failed: ' + (await r.text()));
    }
  });
}
</script>
""",
        trades=trades,
        d=d,
        q=q,
        stats=stats,
        cons=cons,  # ✅ THIS was missing and caused your crash
        week_total=week_total,
        display_balance=display_balance,
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
        guardrail=guardrail,
    )

    return render_page(content, active="trades")


def get_trade(trade_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()


def _parse_ids_from_request() -> List[int]:
    """Parse a list of trade ids from JSON or form data."""
    ids: Any = None
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        ids = payload.get("ids")
    if ids is None:
        ids = request.form.getlist("ids") or request.form.get("ids")

    if isinstance(ids, str):
        raw = [x.strip() for x in ids.split(",") if x.strip()]
    elif isinstance(ids, list):
        raw = ids
    else:
        raw = []

    clean: List[int] = []
    for x in raw:
        try:
            clean.append(int(x))
        except Exception:
            continue

    seen = set()
    out: List[int] = []
    for i in clean:
        if i not in seen:
            out.append(i)
            seen.add(i)
    return out


def _trades_table_columns(conn: sqlite3.Connection) -> List[str]:
    """Return the current trades table columns (sqlite)."""
    return [r["name"] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]


def trades_duplicate(trade_id: int):
    """Clone a trade row (useful for scaling in/out or repeating a similar fill)."""
    src = get_trade(trade_id)
    if not src:
        abort(404)

    net_pl = float(src["net_pl"] or 0.0)
    new_balance = (latest_balance_overall() or 50000.0) + net_pl

    with db() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent,
                stop_pct, target_pct, stop_price, take_profit,
                risk, comm, gross_pl, net_pl, result_pct, balance,
                raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                src["trade_date"],
                src["entry_time"] or "",
                src["exit_time"] or "",
                src["ticker"] or "",
                src["opt_type"] or "",
                src["strike"],
                src["entry_price"],
                src["exit_price"],
                src["contracts"],
                src["total_spent"],
                src["stop_pct"],
                src["target_pct"],
                src["stop_price"],
                src["take_profit"],
                src["risk"],
                src["comm"],
                src["gross_pl"],
                src["net_pl"],
                src["result_pct"],
                new_balance,
                f"DUPLICATE OF #{trade_id}",
                now_iso(),
            ),
        )

    d = request.args.get("d", "") or (src["trade_date"] or "")
    q = request.args.get("q", "")
    return redirect(url_for("trades_page", d=d, q=q))


def trades_delete(trade_id: int):
    with db() as conn:
        conn.execute("DELETE FROM trades WHERE id = ?", (trade_id,))
    d = request.args.get("d", "")
    q = request.args.get("q", "")
    return redirect(url_for("trades_page", d=d, q=q))


def trades_delete_many():
    ids = _parse_ids_from_request()
    if not ids:
        if request.is_json:
            return jsonify({"ok": True, "deleted": 0})
        flash("No trades selected.", "warning")
        return redirect(
            url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", ""))
        )

    placeholders = ",".join(["?"] * len(ids))
    with db() as conn:
        cur = conn.execute(f"DELETE FROM trades WHERE id IN ({placeholders})", ids)
        deleted = cur.rowcount if cur.rowcount is not None else 0

    if request.is_json:
        return jsonify({"ok": True, "deleted": int(deleted)})
    flash(f"Deleted {deleted} trade(s).", "success")
    return redirect(
        url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", ""))
    )


def trades_copy_many():
    ids = _parse_ids_from_request()
    target_date = None
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        target_date = payload.get("target_date")
    if not target_date:
        target_date = request.form.get("target_date")

    if not ids:
        if request.is_json:
            return jsonify({"ok": True, "copied": 0})
        flash("No trades selected.", "warning")
        return redirect(
            url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", ""))
        )

    try:
        datetime.strptime(str(target_date), "%Y-%m-%d")
    except Exception:
        if request.is_json:
            return jsonify({"ok": False, "error": "Invalid target_date. Use YYYY-MM-DD."}), 400
        flash("Invalid target date (use YYYY-MM-DD).", "danger")
        return redirect(
            url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", ""))
        )

    with db() as conn:
        cols = _trades_table_columns(conn)
        insert_cols = [c for c in cols if c != "id"]
        select_cols = ",".join([f"{c}" for c in insert_cols])
        placeholders = ",".join(["?"] * len(ids))
        rows = conn.execute(
            f"SELECT {select_cols} FROM trades WHERE id IN ({placeholders}) ORDER BY trade_date, id",
            ids,
        ).fetchall()

        copied = 0
        if rows:
            now_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            qmarks = ",".join(["?"] * len(insert_cols))
            insert_sql = f"INSERT INTO trades ({','.join(insert_cols)}) VALUES ({qmarks})"
            for r in rows:
                data = dict(r)
                data["trade_date"] = str(target_date)
                if "created_at" in data:
                    data["created_at"] = now_iso
                if "balance" in data:
                    data["balance"] = None
                values = [data.get(c) for c in insert_cols]
                conn.execute(insert_sql, values)
                copied += 1

    if request.is_json:
        return jsonify({"ok": True, "copied": copied})
    flash(f"Copied {copied} trade(s) to {target_date}.", "success")
    return redirect(url_for("trades_page", d=str(target_date), q=request.args.get("q", "")))


def trades_edit(trade_id: int):
    row = get_trade(trade_id)
    if not row:
        abort(404)

    d = request.args.get("d", "")
    q = request.args.get("q", "")

    if request.method == "POST":
        f = request.form

        trade_date = (f.get("trade_date") or today_iso()).strip()
        entry_time = (f.get("entry_time") or "").strip()
        exit_time = (f.get("exit_time") or "").strip()

        ticker = (f.get("ticker") or "").strip().upper()
        opt_type = normalize_opt_type(f.get("opt_type") or "")
        strike = parse_float(f.get("strike") or "")

        contracts = parse_int(f.get("contracts") or "") or 0
        entry_price = parse_float(f.get("entry_price") or "")
        exit_price = parse_float(f.get("exit_price") or "")
        comm = parse_float(f.get("comm") or "") or 0.0

        if (
            not ticker
            or opt_type not in ("CALL", "PUT")
            or contracts <= 0
            or entry_price is None
            or exit_price is None
        ):
            return render_page(
                simple_msg("Missing required fields (ticker/type/contracts/entry/exit)."),
                active="trades",
            )

        gross_pl = (exit_price - entry_price) * 100.0 * contracts
        net_pl = gross_pl - comm
        total_spent = entry_price * 100.0 * contracts
        result_pct = (net_pl / total_spent * 100.0) if total_spent > 0 else None

        with db() as conn:
            conn.execute(
                """
                UPDATE trades
                SET trade_date=?, entry_time=?, exit_time=?, ticker=?, opt_type=?, strike=?,
                    entry_price=?, exit_price=?, contracts=?, comm=?,
                    total_spent=?, gross_pl=?, net_pl=?, result_pct=?
                WHERE id=?
                """,
                (
                    trade_date,
                    entry_time,
                    exit_time,
                    ticker,
                    opt_type,
                    strike,
                    entry_price,
                    exit_price,
                    contracts,
                    comm,
                    total_spent,
                    gross_pl,
                    net_pl,
                    result_pct,
                    trade_id,
                ),
            )

        repo.recompute_balances()
        return redirect(
            url_for("trades_page", d=d, q=q) if (d or q) else url_for("trades_page", d=trade_date)
        )

    t = dict(row)
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">✏️ Edit Trade #{{ t.id }}</div>
          <div class="hr"></div>

          <form method="post" action="/trades/edit/{{ t.id }}?d={{ d }}&q={{ q }}">
            <div class="row">
              <div><label>📆 Date</label><input type="date" name="trade_date" value="{{ t.trade_date }}"/></div>
              <div><label>⏱️ Entry Time</label><input name="entry_time" value="{{ t.entry_time or '' }}"/></div>
              <div><label>⏱️ Exit Time</label><input name="exit_time" value="{{ t.exit_time or '' }}"/></div>
            </div>

            <div class="row stack10">
              <div><label>🏷️ Ticker</label><input name="ticker" value="{{ t.ticker or '' }}"/></div>
              <div>
                <label>📌 Type</label>
                <select name="opt_type">
                  <option value="CALL" {% if (t.opt_type or '')=='CALL' %}selected{% endif %}>CALL</option>
                  <option value="PUT"  {% if (t.opt_type or '')=='PUT' %}selected{% endif %}>PUT</option>
                </select>
              </div>
              <div><label>❌ Strike</label><input name="strike" inputmode="decimal" value="{{ '' if t.strike is none else t.strike }}"/></div>
            </div>

            <div class="row stack10">
              <div><label>🧾 Contracts</label><input name="contracts" inputmode="numeric" value="{{ t.contracts or 1 }}"/></div>
              <div><label>💰 Entry</label><input name="entry_price" inputmode="decimal" value="{{ '' if t.entry_price is none else t.entry_price }}"/></div>
              <div><label>💰 Exit</label><input name="exit_price" inputmode="decimal" value="{{ '' if t.exit_price is none else t.exit_price }}"/></div>
            </div>

            <div class="row stack10">
              <div><label>💵 Fees (total)</label><input name="comm" inputmode="decimal" value="{{ t.comm or 0.70 }}"/></div>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save</button>
              <a class="btn" href="/trades?d={{ d }}&q={{ q }}">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        t=t,
        d=d,
        q=q,
    )
    return render_page(content, active="trades")


def trades_review(trade_id: int):
    row = get_trade(trade_id)
    if not row:
        abort(404)

    d = request.args.get("d", "")
    q = request.args.get("q", "")
    rv = repo.get_trade_review(trade_id) or {}

    if request.method == "POST":
        f = request.form
        setup_tag = (f.get("setup_tag") or "").strip()
        session_tag = (f.get("session_tag") or "").strip()
        score_raw = (f.get("checklist_score") or "").strip()
        checklist_score = parse_int(score_raw) if score_raw else None
        rule_break_tags = (f.get("rule_break_tags") or "").strip()
        review_note = (f.get("review_note") or "").strip()
        repo.upsert_trade_review(
            trade_id=trade_id,
            setup_tag=setup_tag,
            session_tag=session_tag,
            checklist_score=checklist_score,
            rule_break_tags=rule_break_tags,
            review_note=review_note,
        )
        return redirect(url_for("trades_page", d=d, q=q) if (d or q) else url_for("trades_page"))

    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🧠 Trade Review #{{ t.id }}</div>
          <div class="tiny stack8">{{ t.trade_date }} · {{ t.ticker }} {{ t.opt_type }}</div>
          <div class="hr"></div>
          <form method="post" action="/trades/review/{{ t.id }}?d={{ d }}&q={{ q }}">
            <div class="row">
              <div><label>Setup Tag</label><input name="setup_tag" value="{{ rv.get('setup_tag','') }}" placeholder="FVG, ORB, Fade, Breakout"></div>
              <div>
                <label>Session Tag</label>
                <select name="session_tag">
                  {% set s = rv.get('session_tag','') %}
                  <option value="" {% if s=='' %}selected{% endif %}>—</option>
                  <option value="Open" {% if s=='Open' %}selected{% endif %}>Open</option>
                  <option value="Midday" {% if s=='Midday' %}selected{% endif %}>Midday</option>
                  <option value="Power Hour" {% if s=='Power Hour' %}selected{% endif %}>Power Hour</option>
                  <option value="After Hours" {% if s=='After Hours' %}selected{% endif %}>After Hours</option>
                </select>
              </div>
              <div><label>Checklist Score (0-100)</label><input name="checklist_score" inputmode="numeric" value="{{ '' if rv.get('checklist_score') is none else rv.get('checklist_score') }}"></div>
            </div>
            <div class="row stack10">
              <div>
                <label>Rule-Break Tags (comma separated)</label>
                <input name="rule_break_tags" value="{{ rv.get('rule_break_tags','') }}" placeholder="oversized, late entry, no stop, revenge trade">
              </div>
            </div>
            <div class="stack10">
              <label>Review Note</label>
              <textarea name="review_note" placeholder="What to repeat, what to remove next session">{{ rv.get('review_note','') }}</textarea>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save Review</button>
              <a class="btn" href="/trades?d={{ d }}&q={{ q }}">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        t=dict(row),
        rv=rv,
        d=d,
        q=q,
    )
    return render_page(content, active="trades")


def trades_risk_controls():
    if request.method == "POST":
        daily_max_loss = parse_float(request.form.get("daily_max_loss", "")) or 0.0
        enforce_lockout = 1 if request.form.get("enforce_lockout") == "1" else 0
        repo.save_risk_controls(daily_max_loss, enforce_lockout)
        return redirect(url_for("trades_risk_controls"))

    rc = repo.get_risk_controls()
    state = trade_lockout_state(today_iso())
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🛡️ Risk Controls</div>
          <div class="tiny stack8">
            Today's net: {{ money(state.day_net) }} · Max loss: {{ money(state.daily_max_loss) }} ·
            Status: {% if state.locked %}<b class="statusLock">LOCKED</b>{% else %}<b class="statusActive">ACTIVE</b>{% endif %}
          </div>
          <div class="hr"></div>
          <form method="post">
            <div class="row">
              <div><label>Daily Max Loss ($)</label><input name="daily_max_loss" inputmode="decimal" value="{{ rc.daily_max_loss }}"></div>
              <div>
                <label>Enforce Lockout</label>
                <select name="enforce_lockout">
                  <option value="0" {% if not rc.enforce_lockout %}selected{% endif %}>Off</option>
                  <option value="1" {% if rc.enforce_lockout %}selected{% endif %}>On</option>
                </select>
              </div>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">Save Controls</button>
              <a class="btn" href="/trades">Back to Trades</a>
            </div>
          </form>
        </div></div>
        """,
        rc=rc,
        state=state,
        money=money,
    )
    return render_page(content, active="trades")


def trades_clear():
    repo.clear_trades()
    return redirect(url_for("trades_page"))


def trades_paste():
    if request.method == "POST":
        guardrail = trade_lockout_state(today_iso())
        if guardrail["locked"]:
            return render_page(
                simple_msg(
                    f"Daily max-loss guardrail is active for {guardrail['day']}. "
                    f"Day net {money(guardrail['day_net'])} reached limit {money(guardrail['daily_max_loss'])}. "
                    "Unlock in Risk Controls to continue."
                ),
                active="trades",
            )
        text = request.form.get("text", "")
        fmt = detect_paste_format(text)

        reconciliation_html = ""
        if fmt == "broker":
            inserted, errors, report = importing.insert_trades_from_broker_paste_with_report(text)
            reconciliation_html = _reconciliation_block(report)
        else:
            inserted, errors = importing.insert_trades_from_paste(text)

        content = render_template_string(
            """
            <div class="card"><div class="toolbar">
              <div class="pill">📋 Paste Trades</div>
              <div class="stack10">Inserted <b>{{ inserted }}</b> trade{{ '' if inserted==1 else 's' }} ✅</div>
              {% if errors %}
                <div class="hr"></div>
                <div class="tiny metaRed">
                  {% for e in errors %}• {{ e }}<br/>{% endfor %}
                </div>
              {% endif %}
              {{ reconciliation_html|safe }}
              <div class="hr"></div>
              <div class="rightActions">
                <a class="btn primary" href="/trades">Trades 📅</a>
                <a class="btn" href="/dashboard">Calendar 📊</a>
                <a class="btn" href="/calculator">Calculator 🧮</a>
                <a class="btn" href="/trades/paste">Paste More 🔁</a>
              </div>
            </div></div>
            """,
            inserted=inserted,
            errors=errors,
            reconciliation_html=reconciliation_html,
        )
        return render_page(content, active="trades")

    example = "1/29\t9:35 AM\t9:37 AM\tSPX\tPUT\t6940\t$6.20\t$7.30\t3\t$1,860.00\t20\t30\t$4.96\t$8.06\t$374.10\t$2.10\t$330.00\t$327.90\t17.74%\t$50,924.40"
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">📋 Paste Trades (tabs please ✅)</div>
          <div class="tiny stack10 line15">
            Pro tip: copy straight from your sheet/log, keep the tabs.
            <div class="hr"></div>
            Example:<br/><code class="preWrapMuted">{{ example }}</code>
          </div>
          <div class="hr"></div>
          <form method="post">
            <div class="stack12">
              <label>📎 Paste here</label>
              <textarea name="text" placeholder="Paste your trade rows here…"></textarea>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">🚀 Import</button>
              <a class="btn" href="/trades">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        example=example,
    )
    return render_page(content, active="trades")


def trades_new_manual():
    if request.method == "POST":
        f = request.form
        trade_date = (f.get("trade_date") or today_iso()).strip()
        guardrail = trade_lockout_state(trade_date)
        if guardrail["locked"]:
            return render_page(
                simple_msg(
                    f"Daily max-loss lockout active for {trade_date}. "
                    f"Day net {money(guardrail['day_net'])} hit limit {money(guardrail['daily_max_loss'])}."
                ),
                active="trades",
            )
        entry_time = (f.get("entry_time") or "").strip()
        exit_time = (f.get("exit_time") or "").strip()
        ticker = (f.get("ticker") or "").strip().upper()
        opt_type = normalize_opt_type(f.get("opt_type") or "")
        strike = parse_float(f.get("strike") or "")
        contracts = parse_int(f.get("contracts") or "") or 0
        entry_price = parse_float(f.get("entry_price") or "")
        exit_price = parse_float(f.get("exit_price") or "")
        comm = parse_float(f.get("comm") or "") or 0.0

        if (
            not ticker
            or opt_type not in ("CALL", "PUT")
            or contracts <= 0
            or entry_price is None
            or exit_price is None
        ):
            return render_page(
                simple_msg("Missing required fields (ticker/type/contracts/entry/exit)."),
                active="trades",
            )

        gross_pl = (exit_price - entry_price) * 100.0 * contracts
        net_pl = gross_pl - comm
        total_spent = entry_price * 100.0 * contracts
        result_pct = (net_pl / total_spent * 100.0) if total_spent > 0 else None
        balance = (latest_balance_overall() or 50000.0) + net_pl

        with db() as conn:
            conn.execute(
                """
                INSERT INTO trades (
                    trade_date, entry_time, exit_time, ticker, opt_type, strike,
                    entry_price, exit_price, contracts, total_spent,
                    comm, gross_pl, net_pl, result_pct, balance,
                    raw_line, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    trade_date,
                    entry_time,
                    exit_time,
                    ticker,
                    opt_type,
                    strike,
                    entry_price,
                    exit_price,
                    contracts,
                    total_spent,
                    comm,
                    gross_pl,
                    net_pl,
                    result_pct,
                    balance,
                    "MANUAL ENTRY",
                    now_iso(),
                ),
            )
        return redirect(url_for("trades_page", d=trade_date))

    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">➕ Manual Trade Entry</div>
          <div class="hr"></div>
          <form method="post">
            <div class="row">
              <div><label>📆 Date</label><input type="date" name="trade_date" value="{{ today }}"/></div>
              <div><label>⏱️ Entry Time</label><input name="entry_time" placeholder="9:45 AM"/></div>
              <div><label>⏱️ Exit Time</label><input name="exit_time" placeholder="10:05 AM"/></div>
            </div>
            <div class="row stack10">
              <div><label>🏷️ Ticker</label><input name="ticker" placeholder="SPX"/></div>
              <div>
                <label>📌 Type</label>
                <select name="opt_type"><option>CALL</option><option>PUT</option></select>
              </div>
              <div><label>❌ Strike</label><input name="strike" inputmode="decimal" placeholder="6940"/></div>
            </div>
            <div class="row stack10">
              <div><label>🧾 Contracts</label><input name="contracts" inputmode="numeric" value="1"/></div>
              <div><label>💰 Entry</label><input name="entry_price" inputmode="decimal" placeholder="6.20"/></div>
              <div><label>💰 Exit</label><input name="exit_price" inputmode="decimal" placeholder="7.30"/></div>
            </div>
            <div class="row stack10">
              <div><label>💵 Commission/Fees (total)</label><input name="comm" inputmode="decimal" value="0.70"/></div>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save Trade</button>
              <a class="btn" href="/trades">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        today=today_iso(),
    )
    return render_page(content, active="trades")


def trades_paste_broker():
    if request.method == "POST":
        guardrail = trade_lockout_state(today_iso())
        if guardrail["locked"]:
            return render_page(
                simple_msg(
                    f"Daily max-loss guardrail is active for {guardrail['day']}. "
                    f"Day net {money(guardrail['day_net'])} reached limit {money(guardrail['daily_max_loss'])}."
                ),
                active="trades",
            )
        text = request.form.get("text", "")
        inserted, errors, report = importing.insert_trades_from_broker_paste_with_report(text)
        reconciliation_html = _reconciliation_block(report)
        content = render_template_string(
            """
            <div class="card"><div class="toolbar">
              <div class="pill">🏦 Broker Paste Import</div>
              <div class="stack10">Inserted <b>{{ inserted }}</b> round-trip trade{{ '' if inserted==1 else 's' }} ✅</div>
              {% if errors %}
                <div class="hr"></div><div class="tiny metaRed">{% for e in errors %}• {{ e }}<br/>{% endfor %}</div>
              {% endif %}
              {{ reconciliation_html|safe }}
              <div class="hr"></div>
              <div class="rightActions">
                <a class="btn primary" href="/trades">Trades 📅</a>
                <a class="btn" href="/dashboard">Calendar 📊</a>
                <a class="btn" href="/trades/paste/broker">Paste More 🔁</a>
              </div>
            </div></div>
            """,
            inserted=inserted,
            errors=errors,
            reconciliation_html=reconciliation_html,
        )
        return render_page(content, active="trades")

    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🏦 Paste Broker Fills (BUY/SELL legs)</div>
          <div class="tiny stack10 line15">
            Paste the raw fills. This importer pairs BUY+SELL into one completed trade (FIFO). ✅
          </div>
          <div class="hr"></div>
          <form method="post">
            <div class="stack12">
              <label>📎 Paste here</label>
              <textarea name="text" placeholder="SPX JAN/30/26 6935 PUT | 1/30/26, 10:30 AM | SELL | 2 | 18.90 | 0.70"></textarea>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">🚀 Convert + Import</button>
              <a class="btn" href="/trades">← Back</a>
            </div>
          </form>
        </div></div>
        """
    )
    return render_page(content, active="trades")


def trades_upload_pdf():
    if request.method == "POST":
        guardrail = trade_lockout_state(today_iso())
        if guardrail["locked"]:
            return render_page(
                simple_msg(
                    f"Daily max-loss guardrail is active for {guardrail['day']}. "
                    f"Day net {money(guardrail['day_net'])} reached limit {money(guardrail['daily_max_loss'])}."
                ),
                active="trades",
            )
        f = request.files.get("pdf")
        mode = (request.form.get("mode") or "broker").strip()  # broker | balance

        if not f or not f.filename:
            return render_page(simple_msg("Please upload a file."), active="trades")

        filename = secure_filename(f.filename)
        _, ext = os.path.splitext(filename.lower())

        if ext not in {".pdf", ".html", ".htm"}:
            return render_page(simple_msg("Please upload a .pdf or .html file."), active="trades")

        path = os.path.join(UPLOAD_DIR, filename)
        f.save(path)

        # ✅ HTML path (no OCR)
        if ext in (".html", ".htm"):
            return _handle_statement_html_import(
                path, mode=mode, source_label="STATEMENT HTML UPLOAD"
            )

        # --- PDF path (keep your OCR behavior for now) ---
        if mode == "broker":
            paste_text, ocr_warns = importing.ocr_pdf_to_broker_paste(path)
            if not paste_text:
                stitched = []
                try:
                    convert_from_path, pytesseract, _, _, _, dep_error = importing.load_ocr_deps()
                    if dep_error:
                        raise RuntimeError(dep_error)
                    pages = convert_from_path(path, dpi=250)
                    all_lines = []
                    for page_img in pages:
                        img = importing.prep_for_ocr(page_img)
                        txt = pytesseract.image_to_string(img, config="--oem 3 --psm 6")
                        all_lines.extend(
                            [
                                importing.normalize_ocr(ln)
                                for ln in txt.splitlines()
                                if importing.normalize_ocr(ln)
                            ]
                        )
                    stitched = importing.stitch_ocr_rows("\n".join(all_lines))
                except Exception as e:
                    ocr_warns = (ocr_warns or []) + [f"OCR debug error: {e}"]

                return render_page(
                    render_template_string(
                        """
                        <div class="card"><div class="toolbar">
                          <div class="pill">⛔ OCR rows not parseable</div>
                          <div class="hr"></div>
                          <div class="tiny metaBlue line16">
                            {% for m in warns %}• {{ m }}<br>{% endfor %}
                          </div>
                          <div class="hr"></div>
                          <div class="tiny">Stitched rows (first 30):</div>
                          <pre class="preWrapMuted">{{ dump }}</pre>
                          <div class="hr"></div>
                          <a class="btn" href="/trades/upload/statement">Back</a>
                        <a class="btn" href="/trades/upload/statement">Upload Another</a>

                        </div></div>
                        """,
                        warns=ocr_warns,
                        dump="\n".join(stitched[:30]),
                    ),
                    active="trades",
                )

            inserted, errors, report = importing.insert_trades_from_broker_paste_with_report(
                paste_text
            )
            reconciliation_html = _reconciliation_block(report)
            msgs = (ocr_warns or []) + (errors or [])
            return render_page(
                render_template_string(
                    """
                    <div class="card"><div class="toolbar">
                      <div class="pill">📄 PDF → OCR → Trades ✅</div>
                      <div class="stack10">Inserted <b>{{ inserted }}</b> trade{{ '' if inserted==1 else 's' }}.</div>
                      {% if msgs %}
                        <div class="hr"></div>
                        <div class="tiny metaBlue line16">
                          {% for m in msgs %}• {{ m }}<br>{% endfor %}
                        </div>
                      {% endif %}
                      {{ reconciliation_html|safe }}
                      <div class="hr"></div>
                      <a class="btn primary" href="/trades">Trades 📅</a>
                     <a class="btn" href="/trades/upload/statement">Upload Another</a>
                    </div></div>
                    """,
                    inserted=inserted,
                    msgs=msgs,
                    reconciliation_html=reconciliation_html,
                ),
                active="trades",
            )

        # mode == balance (PDF OCR)
        text, warns = importing.ocr_pdf_to_text(path)
        bal = importing.extract_statement_balance(text)
        if bal is None:
            return render_page(
                render_template_string(
                    """<div class="card"><div class="toolbar">
                       <div class="pill">⛔ Could not find ending balance</div>
                       <div class="hr"></div>
                       <div class="tiny">Dump (first 1200 chars):</div>
                       <pre class="preWrapMuted">{{ dump }}</pre>
                       <div class="hr"></div>
                       <a class="btn" href="/trades/upload/statement">Back</a>
                       </div></div>""",
                    dump=(text or "")[:1200],
                ),
                active="trades",
            )

        importing.insert_balance_snapshot(today_iso(), bal, raw_line="STATEMENT PDF UPLOAD")
        return redirect(url_for("trades_page"))

    # GET
    broker_cfg = _load_broker_sync_config()
    default_day = today_iso()
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">📄 Upload Statement (PDF / HTML)</div>
          <div class="hr"></div>
          <form method="post" enctype="multipart/form-data">
            <div class="row">
              <div>
                <label>Mode</label>
                <select name="mode">
                  <option value="broker">🏦 Broker fills → trades</option>
                  <option value="balance">🏁 Statement → ending balance snapshot</option>
                </select>
              </div>
            </div>

            <div class="stack12">
              <label>📎 File</label>
              <input type="file" name="pdf" accept="application/pdf,text/html" />
              <div class="tiny stack8">Upload the Vanquish Account Statement HTML if you have it — it’s cleaner than OCR ✅</div>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">🚀 Process</button>
              <a class="btn" href="/trades">← Back</a>
            </div>
          </form>
        </div></div>

        <div class="card"><div class="toolbar">
          <div class="pill">🔗 Broker Sync (Vanquish Statement URL)</div>
          <div class="tiny stack10 line15">One-click fetches statement HTML using your token, then imports with the same HTML parser.</div>
          <div class="hr"></div>
          <form method="post" action="/trades/sync/statement">
            <div class="row">
              <div>
                <label>Mode</label>
                <select name="mode">
                  <option value="broker">🏦 Broker fills → trades</option>
                  <option value="balance">🏁 Statement → ending balance snapshot</option>
                </select>
              </div>
              <div>
                <label>From</label>
                <input type="date" name="from_date" value="{{ default_day }}" />
              </div>
              <div>
                <label>To</label>
                <input type="date" name="to_date" value="{{ default_day }}" />
              </div>
            </div>
            <div class="row">
              <div class="fieldGrow2">
                <label>Token (session or static)</label>
                <input name="token" placeholder="Paste Vanquish statement token" />
              </div>
              <div class="fieldGrow2">
                <label>Account</label>
                <input name="account" value="{{ broker_cfg.account }}" placeholder="default:OEXXXXXXXX" />
              </div>
            </div>
            <div class="row">
              <div class="fieldGrow2">
                <label>Base URL</label>
                <input name="base_url" value="{{ broker_cfg.base_url }}" />
              </div>
              <div>
                <label>Whitelist</label>
                <input name="wl" value="{{ broker_cfg.wl }}" />
              </div>
              <div>
                <label>Timezone</label>
                <input name="time_zone" value="{{ broker_cfg.time_zone }}" />
              </div>
            </div>
            <div class="row">
              <div>
                <label>Date Locale</label>
                <input name="date_locale" value="{{ broker_cfg.date_locale }}" />
              </div>
              <div>
                <label>Report Locale</label>
                <input name="report_locale" value="{{ broker_cfg.report_locale }}" />
              </div>
              <div class="stack12">
                <label>Token Storage</label>
                <label><input type="checkbox" name="remember_token" value="1" {% if broker_cfg.token %}checked{% endif %}/> Remember token on this computer</label>
              </div>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">⚡ Sync Statement</button>
              <a class="btn" href="/trades/upload/statement">Reset</a>
            </div>
          </form>
        </div></div>

        <div class="card"><div class="toolbar">
          <div class="pill">🔐 Live Login Sync (Auto Generate Statement)</div>
          <div class="tiny stack10 line15">Logs into Vanquish, opens statement, clicks Generate Statement, then imports HTML output.</div>
          <div class="hr"></div>
          <form method="post" action="/trades/sync/live">
            <div class="row">
              <div>
                <label>Mode</label>
                <select name="mode">
                  <option value="broker">🏦 Broker fills → trades</option>
                  <option value="balance">🏁 Statement → ending balance snapshot</option>
                </select>
              </div>
              <div>
                <label>From</label>
                <input type="date" name="from_date" value="{{ default_day }}" />
              </div>
              <div>
                <label>To</label>
                <input type="date" name="to_date" value="{{ default_day }}" />
              </div>
            </div>
            <div class="row">
              <div class="fieldGrow2">
                <label>Username</label>
                <input name="username" placeholder="Vanquish username/email" />
              </div>
              <div class="fieldGrow2">
                <label>Password</label>
                <input type="password" name="password" placeholder="Vanquish password" />
              </div>
            </div>
            <div class="row">
              <div class="fieldGrow2">
                <label>Base Origin</label>
                <input name="base_url" value="{{ broker_cfg.base_url }}" />
              </div>
              <div class="fieldGrow2">
                <label>Account</label>
                <input name="account" value="{{ broker_cfg.account }}" placeholder="default:OEXXXXXXXX" />
              </div>
            </div>
            <div class="row">
              <div>
                <label>Whitelist</label>
                <input name="wl" value="{{ broker_cfg.wl }}" />
              </div>
              <div>
                <label>Timezone</label>
                <input name="time_zone" value="{{ broker_cfg.time_zone }}" />
              </div>
              <div>
                <label>Date Locale</label>
                <input name="date_locale" value="{{ broker_cfg.date_locale }}" />
              </div>
              <div>
                <label>Report Locale</label>
                <input name="report_locale" value="{{ broker_cfg.report_locale }}" />
              </div>
            </div>
            <div class="row">
              <div class="stack12">
                <label>Browser Mode</label>
                <label><input type="checkbox" name="headless" value="1" checked /> Headless</label>
              </div>
              <div class="stack12">
                <label>Diagnostics</label>
                <label><input type="checkbox" name="debug_capture" value="1" checked /> Capture debug artifacts</label>
                <label><input type="checkbox" name="debug_only" value="1" /> Debug only (no import)</label>
              </div>
              <div class="stack12">
                <label>Config</label>
                <label><input type="checkbox" name="remember_connection" value="1" /> Remember account/base settings locally</label>
              </div>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">🤖 Login + Generate + Import</button>
              <a class="btn" href="/trades/upload/statement">Reset</a>
            </div>
          </form>
        </div></div>
        """,
        broker_cfg=broker_cfg,
        default_day=default_day,
    )
    return render_page(content, active="trades")


def trades_sync_statement():
    if request.method != "POST":
        return redirect(url_for("trades_upload_pdf"))

    mode = (request.form.get("mode") or "broker").strip()
    guardrail = trade_lockout_state(today_iso())
    if guardrail["locked"] and mode == "broker":
        return render_page(
            simple_msg(
                f"Daily max-loss guardrail is active for {guardrail['day']}. "
                f"Day net {money(guardrail['day_net'])} reached limit {money(guardrail['daily_max_loss'])}."
            ),
            active="trades",
        )

    cfg = _load_broker_sync_config()
    token = (request.form.get("token") or "").strip() or cfg.get("token", "")
    base_url = (request.form.get("base_url") or "").strip() or cfg.get("base_url", "")
    account = (request.form.get("account") or "").strip() or cfg.get("account", "")
    wl = (request.form.get("wl") or "").strip() or cfg.get("wl", "vanquishtrader")
    time_zone = (request.form.get("time_zone") or "").strip() or cfg.get(
        "time_zone", "America/New_York"
    )
    date_locale = (request.form.get("date_locale") or "").strip() or cfg.get("date_locale", "en-US")
    report_locale = (request.form.get("report_locale") or "").strip() or cfg.get(
        "report_locale", "en"
    )
    remember_token = request.form.get("remember_token") == "1"

    if not token:
        return render_page(
            simple_msg("Broker token is required. Paste token once or enable remember token."),
            active="trades",
        )
    if not base_url or not account:
        return render_page(
            simple_msg("Base URL and account are required for broker sync."), active="trades"
        )

    from_date = _normalize_iso_date(request.form.get("from_date") or "", today_iso())
    to_date = _normalize_iso_date(request.form.get("to_date") or "", today_iso())
    if from_date > to_date:
        from_date, to_date = to_date, from_date

    if remember_token:
        cfg.update(
            {
                "base_url": base_url,
                "wl": wl,
                "account": account,
                "time_zone": time_zone,
                "date_locale": date_locale,
                "report_locale": report_locale,
                "token": token,
            }
        )
        _save_broker_sync_config(cfg)

    try:
        html_text = _fetch_statement_html(
            base_url=base_url,
            token=token,
            wl=wl,
            from_date=from_date,
            to_date=to_date,
            time_zone=time_zone,
            account=account,
            date_locale=date_locale,
            report_locale=report_locale,
        )
    except HTTPError as e:
        return render_page(
            simple_msg(
                f"Broker sync failed with HTTP {e.code}. Token may be expired or account is invalid."
            ),
            active="trades",
        )
    except URLError as e:
        return render_page(simple_msg(f"Broker sync network error: {e.reason}"), active="trades")
    except Exception as e:
        return render_page(simple_msg(f"Broker sync failed: {e}"), active="trades")

    if "<html" not in html_text.lower():
        return render_page(
            simple_msg(
                "Broker response was not HTML statement content. Verify token/account and try again."
            ),
            active="trades",
        )

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"vanquish_statement_sync_{from_date}_{to_date}_{stamp}.html"
    path = os.path.join(UPLOAD_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_text)

    return _handle_statement_html_import(path, mode=mode, source_label="BROKER SYNC HTML")


def trades_sync_live():
    if request.method != "POST":
        return redirect(url_for("trades_upload_pdf"))

    mode = (request.form.get("mode") or "broker").strip()
    guardrail = trade_lockout_state(today_iso())
    if guardrail["locked"] and mode == "broker":
        return render_page(
            simple_msg(
                f"Daily max-loss guardrail is active for {guardrail['day']}. "
                f"Day net {money(guardrail['day_net'])} reached limit {money(guardrail['daily_max_loss'])}."
            ),
            active="trades",
        )

    cfg = _load_broker_sync_config()
    username = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "").strip()
    base_url = (request.form.get("base_url") or "").strip() or cfg.get("base_url", "")
    account = (request.form.get("account") or "").strip() or cfg.get("account", "")
    wl = (request.form.get("wl") or "").strip() or cfg.get("wl", "vanquishtrader")
    time_zone = (request.form.get("time_zone") or "").strip() or cfg.get(
        "time_zone", "America/New_York"
    )
    date_locale = (request.form.get("date_locale") or "").strip() or cfg.get("date_locale", "en-US")
    report_locale = (request.form.get("report_locale") or "").strip() or cfg.get(
        "report_locale", "en"
    )
    headless = request.form.get("headless") == "1"
    debug_capture = request.form.get("debug_capture") == "1"
    debug_only = request.form.get("debug_only") == "1"
    remember_connection = request.form.get("remember_connection") == "1"

    if not username or not password:
        return render_page(
            simple_msg("Username and password are required for live login sync."),
            active="trades",
        )
    if not base_url or not account:
        return render_page(
            simple_msg("Base origin and account are required for live login sync."),
            active="trades",
        )

    from_date = _normalize_iso_date(request.form.get("from_date") or "", today_iso())
    to_date = _normalize_iso_date(request.form.get("to_date") or "", today_iso())
    if from_date > to_date:
        from_date, to_date = to_date, from_date

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    debug_dir = (
        os.path.join(BROKER_DEBUG_DIR, f"live_{from_date}_{to_date}_{stamp}")
        if debug_capture
        else None
    )
    artifacts_rel: List[str] = []

    if remember_connection:
        cfg.update(
            {
                "base_url": base_url,
                "wl": wl,
                "account": account,
                "time_zone": time_zone,
                "date_locale": date_locale,
                "report_locale": report_locale,
            }
        )
        _save_broker_sync_config(cfg)

    try:
        html_text, warns, artifacts_abs = vanquish_live_sync.fetch_statement_html_via_login(
            base_origin=base_url,
            username=username,
            password=password,
            from_date=from_date,
            to_date=to_date,
            account=account,
            wl=wl,
            time_zone=time_zone,
            date_locale=date_locale,
            report_locale=report_locale,
            headless=headless,
            debug_dir=debug_dir,
        )
        artifacts_rel = [_debug_relative(p) for p in artifacts_abs]
    except Exception as e:
        if debug_dir and os.path.isdir(debug_dir):
            artifacts_rel = [
                _debug_relative(os.path.join(debug_dir, n))
                for n in sorted(os.listdir(debug_dir))
                if os.path.isfile(os.path.join(debug_dir, n))
            ]
        if artifacts_rel:
            return _render_live_debug_result(
                folder_rel=_debug_relative(debug_dir or ""),
                artifacts_rel=artifacts_rel,
                warns=[],
                error=f"Live login sync failed: {e}",
            )
        return render_page(simple_msg(f"Live login sync failed: {e}"), active="trades")

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    filename = f"vanquish_statement_live_{from_date}_{to_date}_{stamp}.html"
    path = os.path.join(UPLOAD_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_text)
    artifacts_rel = artifacts_rel + [_debug_relative(path)]

    if debug_only and debug_dir:
        return _render_live_debug_result(
            folder_rel=_debug_relative(debug_dir),
            artifacts_rel=artifacts_rel,
            warns=warns,
            error="",
        )

    response = _handle_statement_html_import(path, mode=mode, source_label="LIVE LOGIN HTML")
    if warns:
        flash("Live sync note(s): " + " | ".join(warns), "warn")
    if artifacts_rel and debug_dir:
        flash(
            "Live sync debug artifacts: "
            + " | ".join(f"/trades/sync/debug/{rel}" for rel in artifacts_rel[:6]),
            "warn",
        )
    return response


def trades_sync_debug_file(name: str):
    try:
        path = _debug_safe_path(name)
    except ValueError:
        abort(400)
    if not os.path.isfile(path):
        abort(404)
    return send_file(path, as_attachment=False)


def _fetch_trades_for_rebuild(start_date: str = "", end_date: str = "") -> List[Dict[str, Any]]:
    where: List[str] = []
    params: List[Any] = []
    if start_date:
        where.append("trade_date >= ?")
        params.append(start_date)
    if end_date:
        where.append("trade_date <= ?")
        params.append(end_date)
    sql = "SELECT * FROM trades"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY trade_date ASC, id ASC"
    with db() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def trades_open_positions():
    as_of = (request.args.get("as_of") or "").strip()
    q = (request.args.get("q") or "").strip()
    rows = repo.fetch_open_positions(as_of=as_of, q=q)

    grouped: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        ticker = (r.get("ticker") or "—").strip() or "—"
        opt_type = (r.get("opt_type") or "—").strip() or "—"
        strike = r.get("strike")
        strike_label = (
            "—"
            if strike is None
            else str(int(strike)) if float(strike).is_integer() else f"{float(strike):.2f}"
        )
        key = f"{ticker} {opt_type} {strike_label}"
        g = grouped.setdefault(
            key,
            {
                "symbol": key,
                "trades": 0,
                "contracts": 0,
                "total_spent": 0.0,
                "latest_date": "",
            },
        )
        g["trades"] += 1
        g["contracts"] += int(r.get("contracts") or 0)
        g["total_spent"] += float(r.get("total_spent") or 0.0)
        g["latest_date"] = max(g["latest_date"], str(r.get("trade_date") or ""))

    grouped_rows = sorted(
        grouped.values(), key=lambda x: (x["latest_date"], x["symbol"]), reverse=True
    )
    total_contracts = sum(int(r["contracts"]) for r in grouped_rows)
    total_spent = sum(float(r["total_spent"]) for r in grouped_rows)

    content = render_template_string(
        """
        <div class="metricStrip">
          <div class="metric"><div class="label">Open Buckets</div><div class="value">{{ grouped_rows|length }}</div></div>
          <div class="metric"><div class="label">Open Contracts</div><div class="value">{{ total_contracts }}</div></div>
          <div class="metric"><div class="label">Capital In Open Lots</div><div class="value">{{ money(total_spent) }}</div></div>
          <div class="metric"><div class="label">Candidate Rows</div><div class="value">{{ rows|length }}</div></div>
        </div>

        <div class="card"><div class="toolbar">
          <div class="pill">📂 Open Positions (Unmatched / Incomplete)</div>
          <div class="tiny stack10 line15">Derived from trades missing close info (no exit time, no exit price, or no net P/L).</div>
          <div class="hr"></div>
          <form method="get" class="row">
            <div><label>As of Date</label><input type="date" name="as_of" value="{{ as_of }}"></div>
            <div class="fieldGrow2"><label>Filter</label><input name="q" value="{{ q }}" placeholder="SPX, CALL, raw note..."></div>
            <div class="actionRow">
              <button class="btn" type="submit">Apply</button>
              <a class="btn" href="/trades/open-positions">Reset</a>
              <a class="btn" href="/trades">Back Trades</a>
            </div>
          </form>
        </div></div>

        <div class="card stack12"><div class="toolbar">
          <div class="pill">🧾 Position Summary</div>
          <div class="hr"></div>
          <div class="tableWrap"><table class="tableDense">
            <thead><tr><th>Symbol</th><th>Open Trades</th><th>Contracts</th><th>Capital</th><th>Latest</th></tr></thead>
            <tbody>
            {% for r in grouped_rows %}
              <tr>
                <td>{{ r.symbol }}</td>
                <td>{{ r.trades }}</td>
                <td>{{ r.contracts }}</td>
                <td>{{ money(r.total_spent) }}</td>
                <td>{{ r.latest_date }}</td>
              </tr>
            {% endfor %}
            {% if grouped_rows|length == 0 %}
              <tr><td colspan="5">No open-position candidates found.</td></tr>
            {% endif %}
            </tbody>
          </table></div>
        </div></div>
        """,
        grouped_rows=grouped_rows,
        total_contracts=total_contracts,
        total_spent=total_spent,
        rows=rows,
        as_of=as_of,
        q=q,
        money=money,
    )
    return render_page(content, active="trades")


def trades_rebuild_reviews():
    start_date = (request.values.get("start_date") or "").strip()
    end_date = (request.values.get("end_date") or "").strip()
    scope = (request.values.get("scope") or "missing").strip().lower()
    if scope not in {"missing", "all"}:
        scope = "missing"
    preserve_manual = (request.values.get("preserve_manual") or "1") == "1"

    if request.method == "POST":
        trades = _fetch_trades_for_rebuild(start_date=start_date, end_date=end_date)
        review_map = repo.fetch_trade_reviews_map(
            [int(t["id"]) for t in trades if t.get("id") is not None]
        )
        rebuilt = 0
        skipped_existing = 0

        for t in trades:
            tid = int(t["id"])
            existing = review_map.get(tid)
            if scope == "missing" and existing:
                skipped_existing += 1
                continue

            payload = importing._auto_review_payload(t)
            if preserve_manual and existing:
                payload["setup_tag"] = (existing.get("setup_tag") or "").strip() or payload[
                    "setup_tag"
                ]
                payload["session_tag"] = (existing.get("session_tag") or "").strip() or payload[
                    "session_tag"
                ]
                if existing.get("checklist_score") is not None:
                    payload["checklist_score"] = int(existing["checklist_score"])
                payload["rule_break_tags"] = (
                    existing.get("rule_break_tags") or ""
                ).strip() or payload["rule_break_tags"]
                payload["review_note"] = (existing.get("review_note") or "").strip() or payload[
                    "review_note"
                ]

            repo.upsert_trade_review(
                trade_id=tid,
                setup_tag=payload.get("setup_tag", ""),
                session_tag=payload.get("session_tag", ""),
                checklist_score=payload.get("checklist_score"),
                rule_break_tags=payload.get("rule_break_tags", ""),
                review_note=payload.get("review_note", ""),
            )
            rebuilt += 1

        flash(
            f"Rebuild complete: updated {rebuilt} review(s), skipped {skipped_existing} existing review(s).",
            "success",
        )
        return redirect(
            url_for(
                "trades_rebuild_reviews",
                start_date=start_date,
                end_date=end_date,
                scope=scope,
                preserve_manual="1" if preserve_manual else "0",
            )
        )

    preview = _fetch_trades_for_rebuild(start_date=start_date, end_date=end_date)
    preview_reviews = repo.fetch_trade_reviews_map(
        [int(t["id"]) for t in preview if t.get("id") is not None]
    )
    preview_missing = sum(1 for t in preview if int(t["id"]) not in preview_reviews)

    content = render_template_string(
        """
        <div class="metricStrip">
          <div class="metric"><div class="label">Trades In Scope</div><div class="value">{{ preview|length }}</div></div>
          <div class="metric"><div class="label">Existing Reviews</div><div class="value">{{ preview_reviews|length }}</div></div>
          <div class="metric"><div class="label">Missing Reviews</div><div class="value">{{ preview_missing }}</div></div>
          <div class="metric"><div class="label">Mode</div><div class="value">{{ 'Missing Only' if scope == 'missing' else 'All Trades' }}</div></div>
        </div>

        <div class="card"><div class="toolbar">
          <div class="pill">🛠️ Admin: Rebuild Reviews</div>
          <div class="tiny stack10 line15">Bulk regenerate review metadata (setup/session/score/tags) from trade rows.</div>
          <div class="hr"></div>
          <form method="post" class="row">
            <div><label>Start Date</label><input type="date" name="start_date" value="{{ start_date }}"></div>
            <div><label>End Date</label><input type="date" name="end_date" value="{{ end_date }}"></div>
            <div>
              <label>Scope</label>
              <select name="scope">
                <option value="missing" {% if scope == 'missing' %}selected{% endif %}>Only Missing Reviews</option>
                <option value="all" {% if scope == 'all' %}selected{% endif %}>All Trades (Overwrite)</option>
              </select>
            </div>
            <div>
              <label>Preserve Manual Fields</label>
              <select name="preserve_manual">
                <option value="1" {% if preserve_manual %}selected{% endif %}>Yes (safer)</option>
                <option value="0" {% if not preserve_manual %}selected{% endif %}>No (fully regenerate)</option>
              </select>
            </div>
            <div class="actionRow">
              <button class="btn primary" type="submit">Run Rebuild</button>
              <a class="btn" href="/trades/reviews/rebuild">Reset</a>
              <a class="btn" href="/trades">Back Trades</a>
            </div>
          </form>
        </div></div>
        """,
        preview=preview,
        preview_reviews=preview_reviews,
        preview_missing=preview_missing,
        start_date=start_date,
        end_date=end_date,
        scope=scope,
        preserve_manual=preserve_manual,
    )
    return render_page(content, active="trades")
