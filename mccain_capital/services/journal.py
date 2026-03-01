"""Journal domain service functions."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from flask import (
    abort,
    jsonify,
    redirect,
    render_template,
    render_template_string,
    request,
    url_for,
)

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
    action = url_for("new_entry") if mode == "new" else url_for("edit_entry", entry_id=entry_id)
    title = "➕ New Entry" if mode == "new" else f"✏️ Edit Entry #{entry_id}"
    return render_template(
        "journal/entry_form.html",
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
    for entry in entries:
        entry["entry_date_display"] = _format_entry_date(entry.get("entry_date"))
        entry["updated_at_display"] = _format_updated_timestamp(entry.get("updated_at"))

    content = render_template(
        "journal/home.html",
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

    prefill_date = (request.args.get("d") or "").strip()
    entry_date = prefill_date or _default_entry_date_for_journal()
    initial_values = {
        "entry_date": entry_date,
        "entry_type": (request.args.get("entry_type") or "post_market").strip() or "post_market",
        "link_all_day": "1" if (request.args.get("link_all_day") or "1").strip() == "1" else "0",
        "market": (request.args.get("market") or "").strip(),
        "setup": (request.args.get("setup") or "").strip(),
        "grade": (request.args.get("grade") or "").strip(),
        "mood": (request.args.get("mood") or "").strip(),
        "pnl": (request.args.get("pnl") or "").strip(),
        "notes": (request.args.get("notes") or "").strip(),
        "template_notes": (request.args.get("template_notes") or "").strip(),
    }
    selected_ids = _trade_ids_for_date(entry_date) if initial_values["link_all_day"] == "1" else []
    scaffold = _build_debrief_scaffold(
        entry_date,
        initial_values["entry_type"],
        selected_ids,
    )
    if not initial_values["template_notes"]:
        initial_values["template_notes"] = scaffold["template_notes"]
    if not initial_values["notes"]:
        initial_values["notes"] = scaffold["notes"]
    if not initial_values["pnl"] and scaffold["pnl"] is not None:
        initial_values["pnl"] = f"{float(scaffold['pnl']):.2f}"
    return render_page(
        _entry_form(
            "new",
            initial_values,
            errors=[],
            available_trades=_trade_options_for_date(entry_date),
            selected_trade_ids=selected_ids,
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
    top_setup = setup_stats[0]["setup"] if setup_stats else "No setup data"
    top_setup_net = float(setup_stats[0]["net"] or 0.0) if setup_stats else 0.0
    top_mood = mood_stats[0]["mood"] if mood_stats else "No mood data"
    top_mood_avg = float(mood_stats[0]["avg_pnl"] or 0.0) if mood_stats else 0.0
    top_break = rule_breaks[0]["tag"] if rule_breaks else "No repeated tags"
    top_break_count = int(rule_breaks[0]["count"] or 0) if rule_breaks else 0

    content = render_template_string(
        """
        <div class="metricStrip">
          <div class="metric"><div class="label">Week</div><div class="value">{{ week_start }} → {{ week_end }}</div></div>
          <div class="metric"><div class="label">Journal Entries</div><div class="value">{{ entries|length }}</div></div>
          <div class="metric"><div class="label">Setups Tracked</div><div class="value">{{ setup_stats|length }}</div></div>
          <div class="metric"><div class="label">Rule-Break Tags</div><div class="value">{{ rule_breaks|length }}</div></div>
        </div>

        <div class="card pageHero">
          <div class="toolbar">
            <div class="pageHeroHead">
              <div>
                <div class="pill">📘 Weekly Review</div>
                <h2 class="pageTitle">Behavior Snapshot</h2>
                <div class="pageSub">Review setup quality, mood outcomes, and repeated rule breaks for the selected week window.</div>
              </div>
            </div>
          </div>
        </div>

        <div class="card"><div class="toolbar">
          <form method="get" class="row">
            <div><label>Week Start</label><input type="date" name="week_start" value="{{ week_start }}"></div>
            <div class="actionRow">
              <button class="btn" type="submit">Apply</button>
              <a class="btn" href="/journal/review/weekly">Current Week</a>
              <a class="btn" href="/journal">Back Journal</a>
            </div>
          </form>
        </div></div>

        <div class="insightGrid stack12">
          <div class="insightCard">
            <div class="insightTitle">🏁 Best Setup (Week)</div>
            <div class="insightBody">{{ top_setup }} · Net {{ money(top_setup_net) }}</div>
          </div>
          <div class="insightCard">
            <div class="insightTitle">😶‍🌫️ Mood Signal</div>
            <div class="insightBody">{{ top_mood }} · Avg {{ money(top_mood_avg) }}</div>
          </div>
          <div class="insightCard">
            <div class="insightTitle">⚠️ Most Repeated Break</div>
            <div class="insightBody">{{ top_break }}{% if top_break_count %} · {{ top_break_count }}x{% endif %}</div>
          </div>
        </div>

        <div class="twoCol stack12">
          <div class="card"><div class="toolbar">
            <div class="pill">📌 Best Setups (linked trades)</div>
            <div class="hr"></div>
            <div class="tableWrap desktopOnly"><table class="tableDense">
              <thead><tr><th>Setup</th><th>Trades</th><th>Win Rate</th><th>Net</th></tr></thead>
              <tbody>
              {% for r in setup_stats %}
                <tr><td>{{ r.setup }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.net) }}</td></tr>
              {% endfor %}
              {% if setup_stats|length == 0 %}<tr><td colspan="4">No linked-trade setup data this week.</td></tr>{% endif %}
              </tbody>
            </table></div>
            <div class="mobileOnly">
              <div class="grid">
                {% for r in setup_stats %}
                  <div class="card"><div class="toolbar">
                    <div class="pill">{{ r.setup }}</div>
                    <div class="metaRow">
                      <span class="meta">Trades: <b>{{ r.count }}</b></span>
                      <span class="meta">Win: <b>{{ '%.1f'|format(r.win_rate) }}%</b></span>
                      <span class="meta">Net: <b>{{ money(r.net) }}</b></span>
                    </div>
                  </div></div>
                {% endfor %}
                {% if setup_stats|length == 0 %}
                  <div class="card"><div class="toolbar"><div class="tiny">No linked-trade setup data this week.</div></div></div>
                {% endif %}
              </div>
            </div>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">😶‍🌫️ Mood vs PnL Pattern</div>
            <div class="hr"></div>
            <div class="tableWrap desktopOnly"><table class="tableDense">
              <thead><tr><th>Mood</th><th>Entries</th><th>Win Rate</th><th>Avg PnL</th></tr></thead>
              <tbody>
              {% for r in mood_stats %}
                <tr><td>{{ r.mood }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.avg_pnl) }}</td></tr>
              {% endfor %}
              {% if mood_stats|length == 0 %}<tr><td colspan="4">No mood data this week.</td></tr>{% endif %}
              </tbody>
            </table></div>
            <div class="mobileOnly">
              <div class="grid">
                {% for r in mood_stats %}
                  <div class="card"><div class="toolbar">
                    <div class="pill">{{ r.mood }}</div>
                    <div class="metaRow">
                      <span class="meta">Entries: <b>{{ r.count }}</b></span>
                      <span class="meta">Win: <b>{{ '%.1f'|format(r.win_rate) }}%</b></span>
                      <span class="meta">Avg: <b>{{ money(r.avg_pnl) }}</b></span>
                    </div>
                  </div></div>
                {% endfor %}
                {% if mood_stats|length == 0 %}
                  <div class="card"><div class="toolbar"><div class="tiny">No mood data this week.</div></div></div>
                {% endif %}
              </div>
            </div>
          </div></div>
        </div>

        <div class="card stack12"><div class="toolbar">
          <div class="pill">⚠️ Repeated Rule Breaks</div>
          <div class="hr"></div>
          <div class="tableWrap desktopOnly"><table class="tableDense">
            <thead><tr><th>Tag</th><th>Count</th></tr></thead>
            <tbody>
            {% for r in rule_breaks %}
              <tr><td>{{ r.tag }}</td><td>{{ r.count }}</td></tr>
            {% endfor %}
            {% if rule_breaks|length == 0 %}<tr><td colspan="2">No rule-break tags for linked trades this week.</td></tr>{% endif %}
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
                <div class="card"><div class="toolbar"><div class="tiny">No rule-break tags for linked trades this week.</div></div></div>
              {% endif %}
            </div>
          </div>
        </div></div>
        """,
        week_start=week_start,
        week_end=week_end,
        entries=entries,
        setup_stats=setup_stats,
        mood_stats=mood_stats,
        rule_breaks=rule_breaks,
        top_setup=top_setup,
        top_setup_net=top_setup_net,
        top_mood=top_mood,
        top_mood_avg=top_mood_avg,
        top_break=top_break,
        top_break_count=top_break_count,
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


def _format_entry_date(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        return datetime.strptime(raw, "%Y-%m-%d").strftime("%b %d, %Y")
    except ValueError:
        return raw


def _format_updated_timestamp(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    normalized = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        return dt.strftime("%b %d, %Y %I:%M %p")
    except ValueError:
        return raw


def _linked_trade_ids_from_form(entry_date: str, form: Any) -> List[int]:
    if (form.get("link_all_day") or "").strip() == "1":
        return _trade_ids_for_date(entry_date)

    multi_raw = form.getlist("linked_trade_ids_multi")
    multi_ids = [int(v) for v in multi_raw if str(v).isdigit() and int(v) > 0]
    comma_ids = _parse_linked_trade_ids(form.get("linked_trade_ids", ""))
    return sorted(set(multi_ids + comma_ids))


def _build_debrief_scaffold(entry_date: str, entry_type: str, linked_trade_ids: List[int]) -> Dict[str, str]:
    entry_kind = (entry_type or "").strip() or "post_market"
    if entry_kind == "pre_market":
        return {
            "template_notes": "Planned levels, catalysts, invalidation, and risk limits for the session.",
            "notes": "",
            "pnl": None,
        }

    trades = _trade_options_for_date(entry_date)
    if linked_trade_ids:
        wanted = set(int(tid) for tid in linked_trade_ids)
        trades = [t for t in trades if int(t.get("id") or 0) in wanted]
    stats = trades_repo.trade_day_stats(trades)
    review_map = trades_repo.fetch_trade_reviews_map(
        [int(t["id"]) for t in trades if t.get("id") is not None]
    )
    tickers = sorted({str(t.get("ticker") or "").strip() for t in trades if str(t.get("ticker") or "").strip()})
    strategy_labels = sorted(
        {
            str((review_map.get(int(t["id"]), {}) or {}).get("strategy_label")
                or (review_map.get(int(t["id"]), {}) or {}).get("setup_tag")
                or "").strip()
            for t in trades
            if t.get("id") is not None
        }
        - {""}
    )
    template_lines = [
        f"Session date: {entry_date}",
        f"Trades linked: {len(trades)}",
        f"Wins / Losses: {int(stats.get('wins', 0) or 0)} / {int(stats.get('losses', 0) or 0)}",
        f"Net P/L: {money(float(stats.get('total', 0.0) or 0.0))}",
        "Tickers: " + (", ".join(tickers) if tickers else "None linked yet"),
        "Strategies: " + (", ".join(strategy_labels) if strategy_labels else "Unlabeled"),
    ]
    notes_lines = [
        "What I saw:",
        "- Day context / market behavior:",
        "- Best setup or read:",
        "",
        "What I did:",
        f"- Risk and execution summary ({int(stats.get('wins', 0) or 0)}W/{int(stats.get('losses', 0) or 0)}L, {money(float(stats.get('total', 0.0) or 0.0))} net):",
        "- Rule adherence / mistakes:",
        "",
        "What I learned:",
        "- Keep doing:",
        "- Stop doing:",
        "- Next session adjustment:",
    ]
    return {
        "template_notes": "\n".join(template_lines),
        "notes": "\n".join(notes_lines),
        "pnl": float(stats.get("total", 0.0) or 0.0),
    }


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
