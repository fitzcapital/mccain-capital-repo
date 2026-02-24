"""Journal domain service functions."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from flask import abort, jsonify, redirect, render_template_string, request, url_for

from mccain_capital.repositories import journal as repo
from mccain_capital.repositories import trades as trades_repo
from mccain_capital.runtime import money, parse_float, today_iso
from mccain_capital.services.ui import render_page


def _entry_form(
    mode: str,
    values: Dict[str, Any],
    entry_id: Optional[int] = None,
    errors: Optional[List[str]] = None,
    available_trades: Optional[List[Dict[str, Any]]] = None,
    selected_trade_ids: Optional[List[int]] = None,
) -> str:
    errors = errors or []
    available_trades = available_trades or []
    selected_trade_ids = selected_trade_ids or []
    selected_trade_ids_set = {int(i) for i in selected_trade_ids if int(i) > 0}
    action = "/new" if mode == "new" else f"/edit/{entry_id}"
    title = "➕ New Entry" if mode == "new" else f"✏️ Edit Entry #{entry_id}"
    return render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">{{ title }}</div>
          <div class="tiny" style="margin-top:10px; line-height:1.6">
            Document observations, execution, and lessons with clarity.
          </div>

          {% if errors %}
            <div class="hr"></div>
            <div class="tiny" style="color:#ff8f8f">{% for e in errors %}• {{ e }}<br/>{% endfor %}</div>
          {% endif %}

          <div class="hr"></div>
          <form method="post" action="{{ action }}">
            <div class="row">
              <div>
                <label>📆 Date</label>
                <input id="journal-entry-date" type="date" name="entry_date" value="{{ values.get('entry_date','') }}">
              </div>
              <div>
                <label>🏷️ Market</label>
                <input name="market" value="{{ values.get('market','') }}" placeholder="SPX / QQQ / NQ...">
              </div>
              <div>
                <label>📌 Setup</label>
                <input name="setup" value="{{ values.get('setup','') }}" placeholder="Midday CE Strike...">
              </div>
            </div>

            <div class="row" style="margin-top:10px">
              <div>
                <label>🧠 Grade</label>
                <input name="grade" value="{{ values.get('grade','') }}" placeholder="A / B / C...">
              </div>
              <div>
                <label>😶‍🌫️ Mood</label>
                <input name="mood" value="{{ values.get('mood','') }}" placeholder="Calm / anxious / revenge...">
              </div>
              <div>
                <label>💰 PnL</label>
                <input name="pnl" inputmode="decimal" value="{{ values.get('pnl','') }}" placeholder="e.g. 327.90">
              </div>
            </div>

            <div class="row" style="margin-top:10px">
              <div>
                <label>🧩 Entry Type</label>
                <select name="entry_type">
                  {% set et = values.get('entry_type','post_market') %}
                  <option value="pre_market" {% if et == 'pre_market' %}selected{% endif %}>Pre-Market Plan</option>
                  <option value="trade_debrief" {% if et == 'trade_debrief' %}selected{% endif %}>Trade Debrief</option>
                  <option value="post_market" {% if et == 'post_market' %}selected{% endif %}>Post-Market Review</option>
                </select>
              </div>
              <div>
                <label>🔁 Day Link Mode</label>
                <label style="display:flex;gap:8px;align-items:center;margin-top:8px">
                  <input id="journal-link-all-day" type="checkbox" name="link_all_day" value="1" {% if values.get('link_all_day') == '1' %}checked{% endif %}>
                  Link all trades for selected date
                </label>
              </div>
              <div style="flex:2 1 260px">
                <label>🔗 Linked Trade IDs (comma separated)</label>
                <input name="linked_trade_ids" value="{{ values.get('linked_trade_ids','') }}" placeholder="e.g. 101,102">
              </div>
            </div>

            <div style="margin-top:12px">
              <label>📌 Linked Trades (multi-select)</label>
              {% if available_trades|length == 0 %}
                <div class="tiny" style="margin-bottom:6px">
                  No trades found for <b>{{ values.get('entry_date','(no date)') }}</b>. Change date to a trading day or save with Day Link Mode after setting the date.
                </div>
              {% else %}
                <div class="tiny" style="margin-bottom:6px">
                  Showing {{ available_trades|length }} trade{{ '' if available_trades|length == 1 else 's' }} for <b>{{ values.get('entry_date','') }}</b>.
                </div>
              {% endif %}
              <select id="journal-linked-trades" name="linked_trade_ids_multi" multiple size="8">
                {% for t in available_trades %}
                  <option value="{{ t['id'] }}" {% if t['id'] in selected_trade_ids_set %}selected{% endif %}>
                    #{{ t['id'] }} • {{ t['trade_date'] }} {{ t.get('entry_time') or '' }} • {{ t.get('ticker') or '—' }} {{ t.get('opt_type') or '' }} • Net {{ money(t.get('net_pl') or 0) }}
                  </option>
                {% endfor %}
              </select>
              <div class="tiny" style="margin-top:6px">
                Tip: Hold Cmd/Ctrl to select multiple trades. For Post-Market Review, enable Day Link Mode to auto-link everything from that date.
              </div>
            </div>

            <div style="margin-top:12px">
              <label>🗂️ Template Notes</label>
              <textarea name="template_notes" placeholder="Planned levels, risk model, follow-up checklist...">{{ values.get('template_notes','') }}</textarea>
            </div>

            <div style="margin-top:12px">
              <label>📝 Notes</label>
              <textarea name="notes" placeholder="Capture context, execution, and improvement plan...">{{ values.get('notes','') }}</textarea>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save</button>
              <a class="btn" href="/journal">← Back</a>
            </div>
          </form>
        </div></div>
        <script>
          (function(){
            const dateInput = document.getElementById("journal-entry-date");
            const tradeSelect = document.getElementById("journal-linked-trades");
            const linkAllDay = document.getElementById("journal-link-all-day");
            if(!dateInput || !tradeSelect){ return; }

            function fmtMoney(v){
              const n = Number(v || 0);
              return n.toLocaleString(undefined, {style:"currency", currency:"USD"});
            }

            async function refreshTradesForDate(){
              const d = (dateInput.value || "").trim();
              if(!d){ tradeSelect.innerHTML = ""; return; }
              const resp = await fetch(`/journal/trades-for-date?d=${encodeURIComponent(d)}`);
              if(!resp.ok){ return; }
              const data = await resp.json();
              tradeSelect.innerHTML = "";
              (data.trades || []).forEach((t)=>{
                const opt = document.createElement("option");
                opt.value = String(t.id);
                opt.textContent = `#${t.id} • ${t.trade_date} ${t.entry_time || ""} • ${t.ticker || "—"} ${t.opt_type || ""} • Net ${fmtMoney(t.net_pl || 0)}`;
                if(linkAllDay && linkAllDay.checked){
                  opt.selected = true;
                }
                tradeSelect.appendChild(opt);
              });
            }

            dateInput.addEventListener("change", refreshTradesForDate);
            if(linkAllDay){
              linkAllDay.addEventListener("change", ()=>{
                const on = linkAllDay.checked;
                Array.from(tradeSelect.options).forEach((o)=>{ o.selected = on; });
              });
            }
          })();
        </script>
        """,
        title=title,
        action=action,
        values=values,
        errors=errors,
        available_trades=available_trades,
        selected_trade_ids_set=selected_trade_ids_set,
        money=money,
    )


def journal_home():
    q = request.args.get("q", "")
    d = request.args.get("d", "")
    entries = [dict(r) for r in repo.fetch_entries(q=q, d=d)]

    content = render_template_string(
        """
        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <form method="get" action="/journal" class="row">
              <div style="flex:2 1 260px">
                <label for="search">🔎 Search Journal 🧠</label>
                <input id="search" name="q" value="{{ q }}" placeholder="notes, setup, mood…" />
              </div>
              <div style="flex:1 1 160px">
                <label>📆 Date</label>
                <input type="date" name="d" value="{{ d }}" />
              </div>
              <div style="display:flex; gap:10px; flex-wrap:wrap">
                <button class="btn" type="submit">🧲 Filter</button>
                <a class="btn" href="/journal">♻️ Reset</a>
                <a class="btn primary" href="{{ url_for('new_entry') }}">➕ New Entry</a>
                <a class="btn" href="{{ url_for('journal_weekly_review') }}">📅 Weekly Review</a>
              </div>
            </form>
            <div class="hr"></div>
            <div class="meta">🧾 {{ entries|length }} entr{{ 'y' if entries|length==1 else 'ies' }} found</div>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">🎯 Daily Focus</div>
            <div style="margin-top:10px; color:var(--muted); line-height:1.5">
              <div>✅ Rules first (or it’s gambling 🎰).</div>
              <div>✅ Confirmation > Hope 👀</div>
              <div>✅ Size + stop respected 🛑</div>
              <div style="margin-top:10px">Journal: <b>what you saw</b> → <b>what you did</b> → <b>what you learned</b> 🧱</div>
            </div>
          </div></div>
        </div>

        <div class="grid">
          {% for e in entries %}
            <div class="card entry">
              <div class="entryTop">
                <div>
                  <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap">
                    <div class="pill">📆 {{ e['entry_date'] }}</div>
                    <div class="pill">🧩 {{ (e.get('entry_type') or 'post_market').replace('_',' ').title() }}</div>
                    {% if e['market'] %}<div class="meta">🏷️ Market: <b>{{ e['market'] }}</b></div>{% endif %}
                    {% if e['setup'] %}<div class="meta">📌 Setup: <b>{{ e['setup'] }}</b></div>{% endif %}
                  </div>
                  <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:6px">
                    {% if e['grade'] %}<span class="meta">🧠 Grade: <b>{{ e['grade'] }}</b></span>{% endif %}
                    {% if e['mood'] %}<span class="meta">😶‍🌫️ Mood: <b>{{ e['mood'] }}</b></span>{% endif %}
                    {% if e['pnl'] is not none %}<span class="meta">💰 PnL: <b>{{ money(e['pnl']) }}</b></span>{% endif %}
                    {% if e.get('linked_trades', 0) > 0 %}<span class="meta">🔗 Trades: <b>{{ e.get('linked_trades') }}</b></span>{% endif %}
                    <span class="meta">🕒 Updated: {{ e['updated_at'] }}</span>
                  </div>
                </div>

                <div class="rightActions">
                  <a class="btn" href="{{ url_for('edit_entry', entry_id=e['id']) }}">✏️ Edit</a>
                  <form id="del-e-{{ e['id'] }}" method="post" action="{{ url_for('delete_entry_route', entry_id=e['id']) }}" style="display:inline"></form>
                  <button class="btn danger" type="button" onclick="confirmDelete('del-e-{{ e['id'] }}')">🗑️</button>
                </div>
              </div>

              <div class="notes">{{ e['notes'] }}</div>
            </div>
          {% endfor %}

          {% if entries|length == 0 %}
            <div class="card entry"><div class="meta">No journal entries yet. Hit <b>New Entry</b>. 📝</div></div>
          {% endif %}
        </div>
        """,
        q=q,
        d=d,
        entries=entries,
        money=money,
    )
    return render_page(content, active="journal")


def journal_trades_for_date():
    day = (request.args.get("d") or "").strip()
    if not day:
        return jsonify({"trades": []})
    rows = _trade_options_for_date(day)
    payload = []
    for r in rows:
        payload.append(
            {
                "id": int(r["id"]),
                "trade_date": r.get("trade_date", ""),
                "entry_time": r.get("entry_time", ""),
                "ticker": r.get("ticker", ""),
                "opt_type": r.get("opt_type", ""),
                "net_pl": float(r.get("net_pl") or 0.0),
            }
        )
    return jsonify({"trades": payload})


def new_entry():
    if request.method == "POST":
        f = request.form
        entry_date = (f.get("entry_date") or today_iso()).strip()
        pnl = parse_float(f.get("pnl", ""))
        notes = (f.get("notes") or "").strip()
        linked_ids = _linked_trade_ids_from_form(entry_date, f)
        entry_type = (f.get("entry_type") or "post_market").strip()
        template_notes = (f.get("template_notes") or "").strip()
        if not notes:
            values = dict(f)
            values["entry_date"] = entry_date
            return render_page(
                _entry_form(
                    "new",
                    values,
                    errors=["Notes is required."],
                    available_trades=_trade_options_for_date(entry_date),
                    selected_trade_ids=linked_ids,
                ),
                active="journal",
            )

        entry_id = repo.create_entry(
            {
                "entry_date": entry_date,
                "market": f.get("market"),
                "setup": f.get("setup"),
                "grade": f.get("grade"),
                "pnl": pnl,
                "mood": f.get("mood"),
                "notes": notes,
                "entry_type": entry_type,
                "template_payload": {"template_notes": template_notes},
            }
        )
        repo.set_entry_trade_links(entry_id, linked_ids)
        return redirect(url_for("edit_entry", entry_id=entry_id))

    entry_date = _default_entry_date_for_journal()
    initial_values = {"entry_date": entry_date, "entry_type": "post_market", "link_all_day": "1"}
    return render_page(
        _entry_form(
            "new",
            initial_values,
            errors=[],
            available_trades=_trade_options_for_date(entry_date),
            selected_trade_ids=_trade_ids_for_date(entry_date),
        ),
        active="journal",
    )


def edit_entry(entry_id: int):
    row = repo.get_entry(entry_id)
    if not row:
        abort(404)

    if request.method == "POST":
        f = request.form
        entry_date = (f.get("entry_date") or today_iso()).strip()
        pnl = parse_float(f.get("pnl", ""))
        notes = (f.get("notes") or "").strip()
        linked_ids = _linked_trade_ids_from_form(entry_date, f)
        entry_type = (f.get("entry_type") or "post_market").strip()
        template_notes = (f.get("template_notes") or "").strip()
        if not notes:
            values = dict(f)
            values["entry_date"] = entry_date
            return render_page(
                _entry_form(
                    "edit",
                    values,
                    entry_id=entry_id,
                    errors=["Notes is required."],
                    available_trades=_trade_options_for_date(entry_date),
                    selected_trade_ids=linked_ids,
                ),
                active="journal",
            )

        repo.update_entry(
            entry_id,
            {
                "entry_date": entry_date,
                "market": f.get("market"),
                "setup": f.get("setup"),
                "grade": f.get("grade"),
                "pnl": pnl,
                "mood": f.get("mood"),
                "notes": notes,
                "entry_type": entry_type,
                "template_payload": {"template_notes": template_notes},
            },
        )
        repo.set_entry_trade_links(entry_id, linked_ids)
        return redirect(url_for("journal_home"))

    values = dict(row)
    if values.get("pnl") is None:
        values["pnl"] = ""
    payload = _safe_template_payload(values.get("template_payload"))
    values["template_notes"] = payload.get("template_notes", "")
    linked_ids = repo.fetch_entry_trade_ids(entry_id)
    values["linked_trade_ids"] = ",".join(str(i) for i in linked_ids)
    values["link_all_day"] = "0"
    entry_date = (values.get("entry_date") or today_iso()).strip()
    return render_page(
        _entry_form(
            "edit",
            values,
            entry_id=entry_id,
            errors=[],
            available_trades=_trade_options_for_date(entry_date),
            selected_trade_ids=linked_ids,
        ),
        active="journal",
    )


def delete_entry_route(entry_id: int):
    repo.delete_entry(entry_id)
    return redirect(url_for("journal_home"))


def journal_weekly_review():
    week_start = (request.args.get("week_start") or "").strip()
    if not week_start:
        today = datetime.strptime(today_iso(), "%Y-%m-%d").date()
        week_start_date = today - timedelta(days=today.weekday())
        week_start = week_start_date.isoformat()
    try:
        start = datetime.strptime(week_start, "%Y-%m-%d").date()
    except Exception:
        start = datetime.strptime(today_iso(), "%Y-%m-%d").date()
        start = start - timedelta(days=start.weekday())
        week_start = start.isoformat()
    end = start + timedelta(days=6)
    week_end = end.isoformat()

    entries = repo.fetch_entries_range(week_start, week_end)
    setup_stats = repo.weekly_setup_stats(week_start, week_end)
    mood_stats = repo.weekly_mood_stats(week_start, week_end)
    rule_breaks = repo.weekly_rule_break_tags(week_start, week_end)

    content = render_template_string(
        """
        <div class="metricStrip">
          <div class="metric"><div class="label">Week</div><div class="value">{{ week_start }} → {{ week_end }}</div></div>
          <div class="metric"><div class="label">Journal Entries</div><div class="value">{{ entries|length }}</div></div>
          <div class="metric"><div class="label">Setups Tracked</div><div class="value">{{ setup_stats|length }}</div></div>
          <div class="metric"><div class="label">Rule-Break Tags</div><div class="value">{{ rule_breaks|length }}</div></div>
        </div>

        <div class="card"><div class="toolbar">
          <form method="get" class="row">
            <div><label>Week Start</label><input type="date" name="week_start" value="{{ week_start }}"></div>
            <div style="display:flex; gap:10px; flex-wrap:wrap">
              <button class="btn" type="submit">Apply</button>
              <a class="btn" href="/journal/review/weekly">Current Week</a>
              <a class="btn" href="/journal">Back Journal</a>
            </div>
          </form>
        </div></div>

        <div class="twoCol" style="margin-top:12px">
          <div class="card"><div class="toolbar">
            <div class="pill">📌 Best Setups (linked trades)</div>
            <div class="hr"></div>
            <table>
              <thead><tr><th>Setup</th><th>Trades</th><th>Win Rate</th><th>Net</th></tr></thead>
              <tbody>
              {% for r in setup_stats %}
                <tr><td>{{ r.setup }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.net) }}</td></tr>
              {% endfor %}
              {% if setup_stats|length == 0 %}<tr><td colspan="4">No linked-trade setup data this week.</td></tr>{% endif %}
              </tbody>
            </table>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">😶‍🌫️ Mood vs PnL Pattern</div>
            <div class="hr"></div>
            <table>
              <thead><tr><th>Mood</th><th>Entries</th><th>Win Rate</th><th>Avg PnL</th></tr></thead>
              <tbody>
              {% for r in mood_stats %}
                <tr><td>{{ r.mood }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.avg_pnl) }}</td></tr>
              {% endfor %}
              {% if mood_stats|length == 0 %}<tr><td colspan="4">No mood data this week.</td></tr>{% endif %}
              </tbody>
            </table>
          </div></div>
        </div>

        <div class="card" style="margin-top:12px"><div class="toolbar">
          <div class="pill">⚠️ Repeated Rule Breaks</div>
          <div class="hr"></div>
          <table>
            <thead><tr><th>Tag</th><th>Count</th></tr></thead>
            <tbody>
            {% for r in rule_breaks %}
              <tr><td>{{ r.tag }}</td><td>{{ r.count }}</td></tr>
            {% endfor %}
            {% if rule_breaks|length == 0 %}<tr><td colspan="2">No rule-break tags for linked trades this week.</td></tr>{% endif %}
            </tbody>
          </table>
        </div></div>
        """,
        week_start=week_start,
        week_end=week_end,
        entries=entries,
        setup_stats=setup_stats,
        mood_stats=mood_stats,
        rule_breaks=rule_breaks,
        money=money,
    )
    return render_page(content, active="journal")


def _parse_linked_trade_ids(raw: str) -> List[int]:
    out: List[int] = []
    for token in (raw or "").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            val = int(token)
        except Exception:
            continue
        if val > 0 and val not in out:
            out.append(val)
    return out


def _trade_options_for_date(day_iso: str) -> List[Dict[str, Any]]:
    rows = trades_repo.fetch_trades(d=day_iso, q="")
    out: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        if item.get("id") is None:
            continue
        out.append(item)
    return out


def _trade_ids_for_date(day_iso: str) -> List[int]:
    return [int(r["id"]) for r in _trade_options_for_date(day_iso)]


def _default_entry_date_for_journal() -> str:
    today = today_iso()
    if _trade_ids_for_date(today):
        return today

    all_trades = trades_repo.fetch_trades(d="", q="")
    if not all_trades:
        return today
    return str(dict(all_trades[0]).get("trade_date") or today)


def _linked_trade_ids_from_form(entry_date: str, form: Any) -> List[int]:
    if (form.get("link_all_day") or "").strip() == "1":
        return _trade_ids_for_date(entry_date)

    multi_raw = form.getlist("linked_trade_ids_multi")
    multi_ids = [int(v) for v in multi_raw if str(v).isdigit() and int(v) > 0]
    comma_ids = _parse_linked_trade_ids(form.get("linked_trade_ids", ""))
    return sorted(set(multi_ids + comma_ids))


def _safe_template_payload(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    if not v:
        return {}
    try:
        parsed = json.loads(str(v))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}
