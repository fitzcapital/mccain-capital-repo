"""Strategies domain service functions."""

from __future__ import annotations

from typing import List

from flask import abort, redirect, render_template_string, request, url_for

from mccain_capital import app_core as core
from mccain_capital.repositories import strategies as repo


def _strategy_form(title: str, t: str, body: str, errors: List[str]) -> str:
    return render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">📌 {{ title }}</div>
          <div class="tiny" style="margin-top:10px; line-height:1.6">
            Keep it executable. If it’s too complex, you won’t follow it. ✅
          </div>

          {% if errors %}
            <div class="hr"></div>
            <div class="tiny" style="color:#ff8f8f">{% for e in errors %}• {{ e }}<br/>{% endfor %}</div>
          {% endif %}

          <div class="hr"></div>
          <form method="post">
            <div class="row">
              <div style="flex:2 1 320px">
                <label>Title</label>
                <input name="title" value="{{ t }}" placeholder="e.g. Fitz Midday CE Strike">
              </div>
            </div>

            <div style="margin-top:12px">
              <label>Body</label>
              <textarea name="body" placeholder="Entry trigger… Invalidation… Size… Stops… Targets…">{{ body }}</textarea>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save</button>
              <a class="btn" href="/strategies">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        title=title,
        t=t,
        body=body,
        errors=errors,
    )


def strategies_page():
    items = repo.fetch_strategies()
    content = render_template_string(
        """
        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div class="pill">📌 Strategies</div>
            <div class="tiny" style="margin-top:10px; line-height:1.5">
              Build your playbook here. Add / edit anytime. ✅
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <a class="btn primary" href="/strategies/new">➕ New Strategy</a>
              <a class="btn" href="/dashboard">📊 Calendar</a>
            </div>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">Rules</div>
            <div class="tiny" style="margin-top:10px; line-height:1.6">
              • One page per setup<br>
              • Include: Entry trigger, invalidation, size rule, exit plan<br>
              • Keep it simple enough to execute under pressure
            </div>
          </div></div>
        </div>

        <div class="grid">
          {% for s in items %}
            <div class="card entry">
              <div class="entryTop">
                <div>
                  <div class="pill">📌 {{ s['title'] }}</div>
                  <div class="meta" style="margin-top:6px">🕒 Updated: {{ s['updated_at'] }}</div>
                </div>
                <div class="rightActions">
                  <a class="btn" href="/strategies/edit/{{ s['id'] }}">✏️ Edit</a>
                  <form id="del-s-{{ s['id'] }}" method="post" action="/strategies/delete/{{ s['id'] }}" style="display:inline"></form>
                  <button class="btn danger" type="button" onclick="confirmDelete('del-s-{{ s['id'] }}')">🗑️</button>
                </div>
              </div>
              <div class="notes">{{ s['body'] }}</div>
            </div>
          {% endfor %}

          {% if items|length == 0 %}
            <div class="card entry"><div class="meta">No strategies yet. Hit <b>New Strategy</b>. 📌</div></div>
          {% endif %}
        </div>
        """,
        items=items,
    )
    return core.render_page(content, active="strategies")


def strategies_new():
    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        body = (request.form.get("body") or "").strip()
        if not title or not body:
            return core.render_page(_strategy_form("New Strategy", title, body, ["Title and body required."]),
                                    active="strategies")
        repo.create_strategy(title=title, body=body)
        return redirect(url_for("strategies_page"))
    return core.render_page(_strategy_form("New Strategy", "", "", []), active="strategies")


def strategies_edit(sid: int):
    row = repo.get_strategy(sid=sid)
    if not row:
        abort(404)

    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        body = (request.form.get("body") or "").strip()
        if not title or not body:
            return core.render_page(_strategy_form("Edit Strategy", title, body, ["Title and body required."]),
                                    active="strategies")
        repo.update_strategy(sid=sid, title=title, body=body)
        return redirect(url_for("strategies_page"))

    return core.render_page(_strategy_form("Edit Strategy", row["title"], row["body"], []), active="strategies")


def strategies_delete(sid: int):
    repo.delete_strategy(sid=sid)
    return redirect(url_for("strategies_page"))


def strat_page():
    return core.strat_page()
