"""Trades domain service functions."""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any, List, Optional

from flask import abort, flash, jsonify, redirect, render_template_string, request, url_for
from werkzeug.utils import secure_filename

from mccain_capital import app_core as core

# Compatibility aliases used by extracted route bodies.
today_iso = core.today_iso
prev_trading_day_iso = core.prev_trading_day_iso
next_trading_day_iso = core.next_trading_day_iso
fetch_trades = core.fetch_trades
fetch_trade_reviews_map = core.fetch_trade_reviews_map
trade_day_stats = core.trade_day_stats
calc_consistency = core.calc_consistency
trade_lockout_state = core.trade_lockout_state
week_total_net = core.week_total_net
last_balance_in_list = core.last_balance_in_list
latest_balance_overall = core.latest_balance_overall
render_page = core.render_page
money = core.money
pct = core.pct
default_starting_balance = core.default_starting_balance
detect_paste_format = core.detect_paste_format
insert_trades_from_broker_paste = core.insert_trades_from_broker_paste
insert_trades_from_paste = core.insert_trades_from_paste
parse_float = core.parse_float
UPLOAD_DIR = core.UPLOAD_DIR
parse_statement_html_to_broker_paste = core.parse_statement_html_to_broker_paste
insert_balance_snapshot = core.insert_balance_snapshot
ocr_pdf_to_broker_paste = core.ocr_pdf_to_broker_paste
_load_ocr_deps = core._load_ocr_deps
_prep_for_ocr = core._prep_for_ocr
normalize_ocr = core.normalize_ocr
stitch_ocr_rows = core.stitch_ocr_rows
ocr_pdf_to_text = core.ocr_pdf_to_text
extract_statement_balance = core.extract_statement_balance
_simple_msg = core._simple_msg


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
    day_net = float((stats["total"] if isinstance(stats, dict) else getattr(stats, "total", 0.0)) or 0.0)
    win_rate = float((stats["win_rate"] if isinstance(stats, dict) else getattr(stats, "win_rate", 0.0)) or 0.0)
    trades_count = len(trades)

    content = render_template_string(
        """
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

        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <form method="get" action="/trades" class="row">
              <div style="flex:2 1 260px">
                <label for="search">🔎 Search Trades 🎯</label>
                <input id="search" name="q" value="{{ q }}" placeholder="SPX, CALL, PUT…" />
              </div>
              <div style="flex:1 1 160px">
                <label>📆 Date</label>
                <input type="date" name="d" value="{{ d }}" />
              </div>
              <div style="display:flex; gap:10px; flex-wrap:wrap">
                <a class="btn" href="/trades?d={{ prev_day }}&q={{ q }}">⬅️ Prev</a>
                <a class="btn" href="/trades?d={{ next_day }}&q={{ q }}">Next ➡️</a>
                <button class="btn" type="submit">🧲 Filter</button>
                <a class="btn" href="/trades">♻️ Reset</a>
                <a class="btn primary" href="/trades/new">➕ Manual Add</a>
                <a class="btn primary" href="/trades/paste">📋 Table Paste</a>
                <a class="btn primary" href="/trades/upload/statement">📄 Upload Statement</a>
              </div>
            </form>

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
                <div class="v"><a class="btn primary" style="padding:8px 10px" href="/trades/risk-controls">Configure</a></div>
              </div>
              <div class="stat">
                <div class="k">📈 Edge Analytics</div>
                <div class="v"><a class="btn primary" style="padding:8px 10px" href="/analytics">Open</a></div>
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
              <form id="clear-trades" method="post" action="/trades/clear" style="display:inline"></form>
              <button class="btn danger" type="button" onclick="confirmClear('clear-trades')">🧼 Clear</button>
              <a class="btn" href="/dashboard">📊 Calendar</a>
              <a class="btn" href="/calculator">🧮 Calculator</a>
            </div>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">🧠 Paste Format</div>
            <div class="tiny" style="margin-top:10px; line-height:1.5">
              Table paste = tab-delimited rows. Broker paste = "instrument | dt | side | qty | price | fee". ✅
            </div>
          </div></div>
        </div>

        <div class="card" style="margin-top:12px"><div class="toolbar">
          <div class="pill">🧾 Trades ({{ trades|length }})</div>
          <div class="hr"></div>

          <!-- Bulk actions: multi-select delete / copy -->
          <div class="row bulkActions">
            <label class="pill bulkSelectLabel">
              <input type="checkbox" id="selectAll" style="margin:0;" />
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

          <div class="tradesDesktop" style="overflow:auto">
            <table>
              <thead>
                <tr>
                  <th style="width:42px"></th>
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
                  <th style="width:90px; text-align:right;">Actions</th>

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
                  <td style="text-align:right; white-space:nowrap;">
                    <div class="rowActions" id="rowActions-{{ t['id'] }}">
                      <button type="button" class="rowMoreBtn" onclick="toggleRowMenu('{{ t['id'] }}', event)" aria-label="Trade actions">▾</button>
                      <div class="rowMoreMenu" id="rowMenu-{{ t['id'] }}">
                        <a class="rowMenuItem" href="/trades/edit/{{ t['id'] }}?d={{ d }}&q={{ q }}">✏️ Edit</a>
                        <a class="rowMenuItem" href="/trades/review/{{ t['id'] }}?d={{ d }}&q={{ q }}">🧠 Review</a>
                        <form method="post" action="/trades/duplicate/{{ t['id'] }}?d={{ d }}&q={{ q }}" style="margin:0;">
  <button class="rowMenuItem" type="submit">📄 Duplicate</button>
</form>

                        <form id="del-t-{{ t['id'] }}" method="post"
                              action="/trades/delete/{{ t['id'] }}?d={{ d }}&q={{ q }}"
                              onsubmit="return confirm('Delete this trade?');"
                              style="margin:0;">
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
                <div class="rightActions" style="margin-top:10px;">
                  <a class="btn" href="/trades/edit/{{ t['id'] }}?d={{ d }}&q={{ q }}">✏️ Edit</a>
                  <a class="btn" href="/trades/review/{{ t['id'] }}?d={{ d }}&q={{ q }}">🧠 Review</a>
                  <form method="post" action="/trades/duplicate/{{ t['id'] }}?d={{ d }}&q={{ q }}" style="display:inline;">
                    <button class="btn" type="submit">📄 Duplicate</button>
                  </form>
                  <form method="post" action="/trades/delete/{{ t['id'] }}?d={{ d }}&q={{ q }}" onsubmit="return confirm('Delete this trade?');" style="display:inline;">
                    <button class="btn danger" type="submit">🗑️ Delete</button>
                  </form>
                </div>
              </article>
            {% endfor %}
          </div>
        </div></div>

<style>
/* Net P/L coloring */
.pl-pos { color: rgb(var(--green)); font-weight: 900; }
.pl-neg { color: rgb(var(--red));   font-weight: 900; }
.pl-zero{ color: var(--muted);      font-weight: 900; }

/* cell emphasis */
.cell-red { color: #dc2626; font-weight: 700; }
.cell-orange { color: #f97316; font-weight: 700; }
.cell-green { color: #16a34a; font-weight: 700; }

/* optional: make it pop a bit more without coloring the whole row */
.cell-red, .cell-orange, .cell-green { white-space: nowrap; }
.tradesMobileList{ display:none; gap:10px; }
.tradeCard{
  border:1px solid rgba(0,229,255,.18);
  background: linear-gradient(180deg, rgba(0,229,255,.07), rgba(255,255,255,.01));
  border-radius:14px;
  padding:12px;
}
.tradeCardHead{ display:flex; gap:8px; align-items:center; justify-content:space-between; margin-bottom:10px; }
.tradeCardGrid{ display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:8px 12px; }
.bulkActions{ align-items:center; flex-wrap:wrap; gap:10px; margin:10px 0 2px; }
.bulkSelectLabel{ cursor:pointer; gap:8px; white-space:nowrap; }
.bulkCount{ white-space:nowrap; font-weight:700; }
.bulkSpacer{ flex:1; min-width:40px; }
.bulkCopyLabel{ display:inline-flex; align-items:center; gap:8px; white-space:nowrap; }
.bulkCopyDate{ width:180px; max-width:180px; }
.tradesDesktop{
  overflow:auto;
  -webkit-overflow-scrolling: touch;
}
.tradesDesktop table{ min-width:1400px; }
.tradesDesktop table thead th{
  position: sticky;
  top: 0;
  background: rgba(8,15,24,.96);
  z-index: 5;
}
@media (max-width: 1200px){
  .bulkSpacer{ display:none; }
}
@media (max-width: 860px){
  .tradesDesktop{ display:none; }
  .tradesMobileList{ display:grid; }
}

  .rowActions{ position:relative; display:inline-block; }
  .rowMoreBtn{
    background: transparent; border: 0; cursor: pointer;
    padding: 6px 10px; border-radius: 10px;
    color: inherit; font-weight: 800;
  }
  .rowMoreBtn:hover{ background: rgba(255,255,255,0.06); }
  .rowMoreMenu{
    position:absolute; right:0; top: calc(100% + 6px);
    min-width: 140px;
    max-width:140px;
    background: rgba(10,10,10,0.92);
    border: 1px solid rgba(255,255,255,0.14);
    border-radius: 14px;
    padding: 8px;
    box-shadow: 0 18px 40px rgba(0,0,0,0.45);
    display:none;
    z-index: 50;
    backdrop-filter: blur(10px);
  }
  .rowMoreMenu.open{ display:block; }
  .rowMenuItem{
    display:block;
    width: 100%;
    text-align:left;
    padding: 10px 10px;
    border-radius: 12px;
    text-decoration:none;
    background: transparent;
    border: 0;
    color: inherit;
    font-weight: 700;
    cursor:pointer;
  }
  .rowMenuItem:hover{ background: rgba(255,255,255,0.06); }
  .rowMenuItem.danger{ color: #ff6b6b; }

/* ===== FIX: Allow row action dropdowns to escape table/card ===== */

/* The card that wraps the trades table */
.tradesCard,
.tradesTableWrap,
.tableWrap,
.tableContainer {
  overflow: visible !important;
}

/* If you rely on horizontal scroll */
.tradesTableWrap {
  overflow-x: auto !important;
  overflow-y: visible !important;
}

/* Ensure table + cells do not clip */
table,
thead,
tbody,
tr,
td,
th {
  overflow: visible !important;
}

/* Anchor for dropdown */
.rowActions {
  position: relative;
  overflow: visible !important;
}

/* Dropdown menu itself */
.rowMoreMenu {
  position: absolute;
  right: 0;
  top: calc(100% + 6px);
  z-index: 999999;
}



</style>

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
        guardrail=guardrail,
    )

    return render_page(content, active="trades")

def get_trade(trade_id: int) -> Optional[sqlite3.Row]:
    with core.db() as conn:
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
    new_balance = (core.latest_balance_overall() or 50000.0) + net_pl

    with core.db() as conn:
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
                core.now_iso(),
            ),
        )

    d = request.args.get("d", "") or (src["trade_date"] or "")
    q = request.args.get("q", "")
    return redirect(url_for("trades_page", d=d, q=q))


def trades_delete(trade_id: int):
    with core.db() as conn:
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
        return redirect(url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", "")))

    placeholders = ",".join(["?"] * len(ids))
    with core.db() as conn:
        cur = conn.execute(f"DELETE FROM trades WHERE id IN ({placeholders})", ids)
        deleted = cur.rowcount if cur.rowcount is not None else 0

    if request.is_json:
        return jsonify({"ok": True, "deleted": int(deleted)})
    flash(f"Deleted {deleted} trade(s).", "success")
    return redirect(url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", "")))


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
        return redirect(url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", "")))

    try:
        datetime.strptime(str(target_date), "%Y-%m-%d")
    except Exception:
        if request.is_json:
            return jsonify({"ok": False, "error": "Invalid target_date. Use YYYY-MM-DD."}), 400
        flash("Invalid target date (use YYYY-MM-DD).", "danger")
        return redirect(url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", "")))

    with core.db() as conn:
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

        trade_date = (f.get("trade_date") or core.today_iso()).strip()
        entry_time = (f.get("entry_time") or "").strip()
        exit_time = (f.get("exit_time") or "").strip()

        ticker = (f.get("ticker") or "").strip().upper()
        opt_type = core.normalize_opt_type(f.get("opt_type") or "")
        strike = core.parse_float(f.get("strike") or "")

        contracts = core.parse_int(f.get("contracts") or "") or 0
        entry_price = core.parse_float(f.get("entry_price") or "")
        exit_price = core.parse_float(f.get("exit_price") or "")
        comm = core.parse_float(f.get("comm") or "") or 0.0

        if not ticker or opt_type not in ("CALL", "PUT") or contracts <= 0 or entry_price is None or exit_price is None:
            return core.render_page(core._simple_msg("Missing required fields (ticker/type/contracts/entry/exit)."),
                                    active="trades")

        gross_pl = (exit_price - entry_price) * 100.0 * contracts
        net_pl = gross_pl - comm
        total_spent = entry_price * 100.0 * contracts
        result_pct = (net_pl / total_spent * 100.0) if total_spent > 0 else None

        with core.db() as conn:
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

        core.recompute_balances()
        return redirect(url_for("trades_page", d=d, q=q) if (d or q) else url_for("trades_page", d=trade_date))

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

            <div class="row" style="margin-top:10px">
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

            <div class="row" style="margin-top:10px">
              <div><label>🧾 Contracts</label><input name="contracts" inputmode="numeric" value="{{ t.contracts or 1 }}"/></div>
              <div><label>💰 Entry</label><input name="entry_price" inputmode="decimal" value="{{ '' if t.entry_price is none else t.entry_price }}"/></div>
              <div><label>💰 Exit</label><input name="exit_price" inputmode="decimal" value="{{ '' if t.exit_price is none else t.exit_price }}"/></div>
            </div>

            <div class="row" style="margin-top:10px">
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
    return core.render_page(content, active="trades")


def trades_review(trade_id: int):
    row = get_trade(trade_id)
    if not row:
        abort(404)

    d = request.args.get("d", "")
    q = request.args.get("q", "")
    rv = core.get_trade_review(trade_id) or {}

    if request.method == "POST":
        f = request.form
        setup_tag = (f.get("setup_tag") or "").strip()
        session_tag = (f.get("session_tag") or "").strip()
        score_raw = (f.get("checklist_score") or "").strip()
        checklist_score = core.parse_int(score_raw) if score_raw else None
        rule_break_tags = (f.get("rule_break_tags") or "").strip()
        review_note = (f.get("review_note") or "").strip()
        core.upsert_trade_review(
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
          <div class="tiny" style="margin-top:8px">{{ t.trade_date }} · {{ t.ticker }} {{ t.opt_type }}</div>
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
            <div class="row" style="margin-top:10px">
              <div>
                <label>Rule-Break Tags (comma separated)</label>
                <input name="rule_break_tags" value="{{ rv.get('rule_break_tags','') }}" placeholder="oversized, late entry, no stop, revenge trade">
              </div>
            </div>
            <div style="margin-top:10px">
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
    return core.render_page(content, active="trades")


def trades_risk_controls():
    if request.method == "POST":
        daily_max_loss = core.parse_float(request.form.get("daily_max_loss", "")) or 0.0
        enforce_lockout = 1 if request.form.get("enforce_lockout") == "1" else 0
        core.save_risk_controls(daily_max_loss, enforce_lockout)
        return redirect(url_for("trades_risk_controls"))

    rc = core.get_risk_controls()
    state = core.trade_lockout_state(core.today_iso())
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🛡️ Risk Controls</div>
          <div class="tiny" style="margin-top:8px">
            Today's net: {{ money(state.day_net) }} · Max loss: {{ money(state.daily_max_loss) }} ·
            Status: {% if state.locked %}<b style="color:#ff8f8f">LOCKED</b>{% else %}<b style="color:#7ee2ae">ACTIVE</b>{% endif %}
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
        money=core.money,
    )
    return core.render_page(content, active="trades")


def trades_clear():
    core.clear_trades()
    return redirect(url_for("trades_page"))


def trades_paste():
    if request.method == "POST":
        guardrail = core.trade_lockout_state(core.today_iso())
        if guardrail["locked"]:
            return core.render_page(
                core._simple_msg(
                    f"Daily max-loss guardrail is active for {guardrail['day']}. "
                    f"Day net {core.money(guardrail['day_net'])} reached limit {core.money(guardrail['daily_max_loss'])}. "
                    "Unlock in Risk Controls to continue."
                ),
                active="trades",
            )
        text = request.form.get("text", "")
        starting_balance = core.parse_float(request.form.get("starting_balance", "")) or core.default_starting_balance()
        fmt = core.detect_paste_format(text)

        if fmt == "broker":
            inserted, errors = core.insert_trades_from_broker_paste(text, starting_balance=starting_balance)
        else:
            inserted, errors = core.insert_trades_from_paste(text)

        content = render_template_string(
            """
            <div class="card"><div class="toolbar">
              <div class="pill">📋 Paste Trades</div>
              <div style="margin-top:10px">Inserted <b>{{ inserted }}</b> trade{{ '' if inserted==1 else 's' }} ✅</div>
              {% if errors %}
                <div class="hr"></div>
                <div class="tiny" style="color:#ff8f8f">
                  {% for e in errors %}• {{ e }}<br/>{% endfor %}
                </div>
              {% endif %}
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
        )
        return core.render_page(content, active="trades")

    example = "1/29\t9:35 AM\t9:37 AM\tSPX\tPUT\t6940\t$6.20\t$7.30\t3\t$1,860.00\t20\t30\t$4.96\t$8.06\t$374.10\t$2.10\t$330.00\t$327.90\t17.74%\t$50,924.40"
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">📋 Paste Trades (tabs please ✅)</div>
          <div class="tiny" style="margin-top:10px; line-height:1.5">
            Pro tip: copy straight from your sheet/log, keep the tabs.
            <div class="hr"></div>
            Example:<br/><code style="font-size:12px; color:var(--muted)">{{ example }}</code>
          </div>
          <div class="hr"></div>
          <form method="post">
            <div class="row">
              <div>
                <label>🏁 Starting Balance (for Broker paste)</label>
                <input name="starting_balance" inputmode="decimal" value="50000" />
              </div>
            </div>
            <div style="margin-top:12px">
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
    return core.render_page(content, active="trades")


def trades_new_manual():
    if request.method == "POST":
        f = request.form
        trade_date = (f.get("trade_date") or core.today_iso()).strip()
        guardrail = core.trade_lockout_state(trade_date)
        if guardrail["locked"]:
            return core.render_page(
                core._simple_msg(
                    f"Daily max-loss lockout active for {trade_date}. "
                    f"Day net {core.money(guardrail['day_net'])} hit limit {core.money(guardrail['daily_max_loss'])}."
                ),
                active="trades",
            )
        entry_time = (f.get("entry_time") or "").strip()
        exit_time = (f.get("exit_time") or "").strip()
        ticker = (f.get("ticker") or "").strip().upper()
        opt_type = core.normalize_opt_type(f.get("opt_type") or "")
        strike = core.parse_float(f.get("strike") or "")
        contracts = core.parse_int(f.get("contracts") or "") or 0
        entry_price = core.parse_float(f.get("entry_price") or "")
        exit_price = core.parse_float(f.get("exit_price") or "")
        comm = core.parse_float(f.get("comm") or "") or 0.0

        if not ticker or opt_type not in ("CALL", "PUT") or contracts <= 0 or entry_price is None or exit_price is None:
            return core.render_page(core._simple_msg("Missing required fields (ticker/type/contracts/entry/exit)."),
                                    active="trades")

        gross_pl = (exit_price - entry_price) * 100.0 * contracts
        net_pl = gross_pl - comm
        total_spent = entry_price * 100.0 * contracts
        result_pct = (net_pl / total_spent * 100.0) if total_spent > 0 else None
        balance = (core.latest_balance_overall() or 50000.0) + net_pl

        with core.db() as conn:
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
                    trade_date, entry_time, exit_time, ticker, opt_type, strike,
                    entry_price, exit_price, contracts, total_spent,
                    comm, gross_pl, net_pl, result_pct, balance,
                    "MANUAL ENTRY",
                    core.now_iso(),
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
            <div class="row" style="margin-top:10px">
              <div><label>🏷️ Ticker</label><input name="ticker" placeholder="SPX"/></div>
              <div>
                <label>📌 Type</label>
                <select name="opt_type"><option>CALL</option><option>PUT</option></select>
              </div>
              <div><label>❌ Strike</label><input name="strike" inputmode="decimal" placeholder="6940"/></div>
            </div>
            <div class="row" style="margin-top:10px">
              <div><label>🧾 Contracts</label><input name="contracts" inputmode="numeric" value="1"/></div>
              <div><label>💰 Entry</label><input name="entry_price" inputmode="decimal" placeholder="6.20"/></div>
              <div><label>💰 Exit</label><input name="exit_price" inputmode="decimal" placeholder="7.30"/></div>
            </div>
            <div class="row" style="margin-top:10px">
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
        today=core.today_iso(),
    )
    return core.render_page(content, active="trades")


def trades_paste_broker():
    if request.method == "POST":
        guardrail = core.trade_lockout_state(core.today_iso())
        if guardrail["locked"]:
            return core.render_page(
                core._simple_msg(
                    f"Daily max-loss guardrail is active for {guardrail['day']}. "
                    f"Day net {core.money(guardrail['day_net'])} reached limit {core.money(guardrail['daily_max_loss'])}."
                ),
                active="trades",
            )
        text = request.form.get("text", "")
        starting_balance = core.parse_float(request.form.get("starting_balance", "")) or core.default_starting_balance()
        inserted, errors = core.insert_trades_from_broker_paste(text, starting_balance=starting_balance)
        content = render_template_string(
            """
            <div class="card"><div class="toolbar">
              <div class="pill">🏦 Broker Paste Import</div>
              <div style="margin-top:10px">Inserted <b>{{ inserted }}</b> round-trip trade{{ '' if inserted==1 else 's' }} ✅</div>
              {% if errors %}
                <div class="hr"></div><div class="tiny" style="color:#ff8f8f">{% for e in errors %}• {{ e }}<br/>{% endfor %}</div>
              {% endif %}
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
        )
        return core.render_page(content, active="trades")

    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🏦 Paste Broker Fills (BUY/SELL legs)</div>
          <div class="tiny" style="margin-top:10px; line-height:1.5">
            Paste the raw fills. This importer pairs BUY+SELL into one completed trade (FIFO). ✅
          </div>
          <div class="hr"></div>
          <form method="post">
            <div class="row"><div><label>🏁 Starting Balance</label><input name="starting_balance" inputmode="decimal" value="50000" /></div></div>
            <div style="margin-top:12px">
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
    return core.render_page(content, active="trades")


def trades_upload_pdf():
    if request.method == "POST":
        guardrail = trade_lockout_state(today_iso())
        if guardrail["locked"]:
            return render_page(
                _simple_msg(
                    f"Daily max-loss guardrail is active for {guardrail['day']}. "
                    f"Day net {money(guardrail['day_net'])} reached limit {money(guardrail['daily_max_loss'])}."
                ),
                active="trades",
            )
        f = request.files.get("pdf")
        mode = (request.form.get("mode") or "broker").strip()  # broker | balance
        starting_balance = parse_float(request.form.get("starting_balance", "")) or 50000.0

        if not f or not f.filename:
            return render_page(_simple_msg("Please upload a file."), active="trades")

        filename = secure_filename(f.filename)
        _, ext = os.path.splitext(filename.lower())

        if ext not in {".pdf", ".html", ".htm"}:
            return render_page(_simple_msg("Please upload a .pdf or .html file."), active="trades")

        path = os.path.join(UPLOAD_DIR, filename)
        f.save(path)

        # ✅ HTML path (no OCR)
        if ext in (".html", ".htm"):
            paste_text, balance_val, warns = parse_statement_html_to_broker_paste(path)

            if mode == "broker":
                if not paste_text:
                    return render_page(
                        render_template_string(
                            """
                            <div class="card"><div class="toolbar">
                              <div class="pill">⛔ HTML parsed, but no trade rows found</div>
                              <div class="hr"></div>
                              <div class="tiny" style="color:#9fd6ff; line-height:1.6">
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

                inserted, errors = insert_trades_from_broker_paste(paste_text, starting_balance=starting_balance)
                msgs = (warns or []) + (errors or [])

                return render_page(
                    render_template_string(
                        """
                        <div class="card"><div class="toolbar">
                          <div class="pill">🧾 HTML → Trades ✅</div>
                          <div style="margin-top:10px">Inserted <b>{{ inserted }}</b> trade{{ '' if inserted==1 else 's' }}.</div>
                          {% if msgs %}
                            <div class="hr"></div>
                            <div class="tiny" style="color:#9fd6ff; line-height:1.6">
                              {% for m in msgs %}• {{ m }}<br>{% endfor %}
                            </div>
                          {% endif %}
                          <div class="hr"></div>
                          <a class="btn primary" href="/trades">Trades 📅</a>
                          <a class="btn" href="/trades/upload/statement">Upload Another</a>
                        </div></div>
                        """,
                        inserted=inserted,
                        msgs=msgs,
                    ),
                    active="trades",
                )

            # mode == "balance"
            if balance_val is None:
                return render_page(
                    render_template_string(
                        """
                        <div class="card"><div class="toolbar">
                          <div class="pill">⛔ Balance not found in HTML</div>
                          <div class="hr"></div>
                          <div class="tiny" style="color:#9fd6ff; line-height:1.6">
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

            insert_balance_snapshot(today_iso(), balance_val, raw_line="STATEMENT HTML UPLOAD")
            return redirect(url_for("trades_page"))

        # --- PDF path (keep your OCR behavior for now) ---
        if mode == "broker":
            paste_text, ocr_warns = ocr_pdf_to_broker_paste(path)
            if not paste_text:
                stitched = []
                try:
                    convert_from_path, pytesseract, _, _, _, dep_error = _load_ocr_deps()
                    if dep_error:
                        raise RuntimeError(dep_error)
                    pages = convert_from_path(path, dpi=250)
                    all_lines = []
                    for page_img in pages:
                        img = _prep_for_ocr(page_img)
                        txt = pytesseract.image_to_string(img, config="--oem 3 --psm 6")
                        all_lines.extend([normalize_ocr(ln) for ln in txt.splitlines() if normalize_ocr(ln)])
                    stitched = stitch_ocr_rows("\n".join(all_lines))
                except Exception as e:
                    ocr_warns = (ocr_warns or []) + [f"OCR debug error: {e}"]

                return render_page(
                    render_template_string(
                        """
                        <div class="card"><div class="toolbar">
                          <div class="pill">⛔ OCR rows not parseable</div>
                          <div class="hr"></div>
                          <div class="tiny" style="color:#9fd6ff; line-height:1.6">
                            {% for m in warns %}• {{ m }}<br>{% endfor %}
                          </div>
                          <div class="hr"></div>
                          <div class="tiny">Stitched rows (first 30):</div>
                          <pre style="white-space:pre-wrap; font-size:12px; color:var(--muted)">{{ dump }}</pre>
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

            inserted, errors = insert_trades_from_broker_paste(paste_text, starting_balance=starting_balance)
            msgs = (ocr_warns or []) + (errors or [])
            return render_page(
                render_template_string(
                    """
                    <div class="card"><div class="toolbar">
                      <div class="pill">📄 PDF → OCR → Trades ✅</div>
                      <div style="margin-top:10px">Inserted <b>{{ inserted }}</b> trade{{ '' if inserted==1 else 's' }}.</div>
                      {% if msgs %}
                        <div class="hr"></div>
                        <div class="tiny" style="color:#9fd6ff; line-height:1.6">
                          {% for m in msgs %}• {{ m }}<br>{% endfor %}
                        </div>
                      {% endif %}
                      <div class="hr"></div>
                      <a class="btn primary" href="/trades">Trades 📅</a>
                     <a class="btn" href="/trades/upload/statement">Upload Another</a>
                    </div></div>
                    """,
                    inserted=inserted,
                    msgs=msgs,
                ),
                active="trades",
            )

        # mode == balance (PDF OCR)
        text, warns = ocr_pdf_to_text(path)
        bal = extract_statement_balance(text)
        if bal is None:
            return render_page(
                render_template_string(
                    """<div class="card"><div class="toolbar">
                       <div class="pill">⛔ Could not find ending balance</div>
                       <div class="hr"></div>
                       <div class="tiny">Dump (first 1200 chars):</div>
                       <pre style="white-space:pre-wrap; font-size:12px; color:var(--muted)">{{ dump }}</pre>
                       <div class="hr"></div>
                       <a class="btn" href="/trades/upload/statement">Back</a>
                       </div></div>""",
                    dump=(text or "")[:1200],
                ),
                active="trades",
            )

        insert_balance_snapshot(today_iso(), bal, raw_line="STATEMENT PDF UPLOAD")
        return redirect(url_for("trades_page"))

    # GET
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
              <div>
                <label>🏁 Starting Balance (broker mode)</label>
                <input name="starting_balance" inputmode="decimal" value="50000" />
              </div>
            </div>

            <div style="margin-top:12px">
              <label>📎 File</label>
              <input type="file" name="pdf" accept="application/pdf,text/html" />
              <div class="tiny" style="margin-top:6px">Upload the Vanquish Account Statement HTML if you have it — it’s cleaner than OCR ✅</div>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">🚀 Process</button>
              <a class="btn" href="/trades">← Back</a>
            </div>
          </form>
        </div></div>
        """
    )
    return render_page(content, active="trades")
