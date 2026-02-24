"""Journal domain service functions."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from flask import abort, redirect, render_template_string, request, url_for

from mccain_capital import app_core as core

fetch_entries = core.fetch_entries
get_entry = core.get_entry
create_entry = core.create_entry
update_entry = core.update_entry
delete_entry = core.delete_entry


def _entry_form(
    mode: str,
    values: Dict[str, Any],
    entry_id: Optional[int] = None,
    errors: Optional[List[str]] = None,
) -> str:
    errors = errors or []
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
                <input type="date" name="entry_date" value="{{ values.get('entry_date','') }}">
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
        """,
        title=title,
        action=action,
        values=values,
        errors=errors,
    )


def journal_home():
    q = request.args.get("q", "")
    d = request.args.get("d", "")
    entries = fetch_entries(q=q, d=d)

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
                    {% if e['market'] %}<div class="meta">🏷️ Market: <b>{{ e['market'] }}</b></div>{% endif %}
                    {% if e['setup'] %}<div class="meta">📌 Setup: <b>{{ e['setup'] }}</b></div>{% endif %}
                  </div>
                  <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:6px">
                    {% if e['grade'] %}<span class="meta">🧠 Grade: <b>{{ e['grade'] }}</b></span>{% endif %}
                    {% if e['mood'] %}<span class="meta">😶‍🌫️ Mood: <b>{{ e['mood'] }}</b></span>{% endif %}
                    {% if e['pnl'] is not none %}<span class="meta">💰 PnL: <b>{{ money(e['pnl']) }}</b></span>{% endif %}
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
        money=core.money,
    )
    return core.render_page(content, active="journal")


def new_entry():
    if request.method == "POST":
        f = request.form
        pnl = core.parse_float(f.get("pnl", ""))
        notes = (f.get("notes") or "").strip()
        if not notes:
            return core.render_page(_entry_form("new", dict(f), errors=["Notes is required."]), active="journal")

        entry_id = create_entry(
            {
                "entry_date": (f.get("entry_date") or core.today_iso()).strip(),
                "market": f.get("market"),
                "setup": f.get("setup"),
                "grade": f.get("grade"),
                "pnl": pnl,
                "mood": f.get("mood"),
                "notes": notes,
            }
        )
        return redirect(url_for("edit_entry", entry_id=entry_id))

    return core.render_page(_entry_form("new", {"entry_date": core.today_iso()}, errors=[]), active="journal")


def edit_entry(entry_id: int):
    row = get_entry(entry_id)
    if not row:
        abort(404)

    if request.method == "POST":
        f = request.form
        pnl = core.parse_float(f.get("pnl", ""))
        notes = (f.get("notes") or "").strip()
        if not notes:
            return core.render_page(_entry_form("edit", dict(f), entry_id=entry_id, errors=["Notes is required."]),
                                    active="journal")

        update_entry(
            entry_id,
            {
                "entry_date": (f.get("entry_date") or core.today_iso()).strip(),
                "market": f.get("market"),
                "setup": f.get("setup"),
                "grade": f.get("grade"),
                "pnl": pnl,
                "mood": f.get("mood"),
                "notes": notes,
            },
        )
        return redirect(url_for("journal_home"))

    values = dict(row)
    if values.get("pnl") is None:
        values["pnl"] = ""
    return core.render_page(_entry_form("edit", values, entry_id=entry_id, errors=[]), active="journal")


def delete_entry_route(entry_id: int):
    delete_entry(entry_id)
    return redirect(url_for("journal_home"))
