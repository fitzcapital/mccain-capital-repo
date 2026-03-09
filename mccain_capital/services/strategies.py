"""Strategies domain service functions."""

from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Dict, List

from flask import (
    abort,
    jsonify,
    redirect,
    render_template,
    render_template_string,
    request,
    url_for,
)

from mccain_capital.services import core as core_svc
from mccain_capital.services.ui import render_page
from mccain_capital.runtime import money, now_et, get_setting_value, set_setting_value
from mccain_capital.repositories import strategies as repo
from mccain_capital.repositories import analytics as analytics_repo

ACTIVE_STRATEGY_ID_KEY = "active_strategy_id"
ACTIVE_STRATEGY_DATE_KEY = "active_strategy_date"
ACTIVE_STRATEGY_CHECKS_KEY = "active_strategy_checks_json"


def _strategy_form(title: str, t: str, body: str, checklist: str, errors: List[str]) -> str:
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
            <div style="margin-top:12px">
              <label>Checklist (one item per line)</label>
              <textarea name="checklist" placeholder="Only A+ setup context&#10;Risk size confirmed&#10;Entry trigger present&#10;Stop and invalidation mapped">{{ checklist }}</textarea>
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
        checklist=checklist,
        errors=errors,
    )


def strategies_page():
    items = [dict(r) for r in repo.fetch_strategies()]
    analytics_rows = analytics_repo.fetch_analytics_rows()
    scorecards = _build_strategy_scorecards(items, analytics_rows)
    stats_map = {str(r.get("title") or "").strip(): r for r in scorecards}
    for item in items:
        stat = stats_map.get(str(item.get("title") or "").strip(), {})
        item["trade_count"] = int(stat.get("count") or 0)
        item["expectancy"] = float(stat.get("expectancy") or 0.0)
        item["win_rate"] = float(stat.get("win_rate") or 0.0)
        item["net"] = float(stat.get("net") or 0.0)
        item["checklist_items"] = _parse_checklist(str(item.get("checklist") or ""))
    headline = scorecards[0] if scorecards else None
    active_state = _active_strategy_state(items)
    content = render_template(
        "strategies/index.html",
        items=items,
        scorecards=scorecards,
        headline=headline,
        active_state=active_state,
        money=money,
    )
    return render_page(content, active="strategies")


def strategies_new():
    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        body = (request.form.get("body") or "").strip()
        checklist = (request.form.get("checklist") or "").strip()
        if not title or not body:
            return render_page(
                _strategy_form(
                    "New Strategy", title, body, checklist, ["Title and body required."]
                ),
                active="strategies",
            )
        repo.create_strategy(title=title, body=body, checklist=checklist)
        return redirect(url_for("strategies_page"))
    return render_page(_strategy_form("New Strategy", "", "", "", []), active="strategies")


def strategies_edit(sid: int):
    row = repo.get_strategy(sid=sid)
    if not row:
        abort(404)

    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        body = (request.form.get("body") or "").strip()
        checklist = (request.form.get("checklist") or "").strip()
        if not title or not body:
            return render_page(
                _strategy_form(
                    "Edit Strategy", title, body, checklist, ["Title and body required."]
                ),
                active="strategies",
            )
        repo.update_strategy(sid=sid, title=title, body=body, checklist=checklist)
        return redirect(url_for("strategies_page"))

    return render_page(
        _strategy_form(
            "Edit Strategy",
            row["title"],
            row["body"],
            str(row["checklist"] or ""),
            [],
        ),
        active="strategies",
    )


def strategies_delete(sid: int):
    repo.delete_strategy(sid=sid)
    return redirect(url_for("strategies_page"))


def strategies_activate():
    strategy_id = int(request.form.get("strategy_id") or 0)
    next_url = (request.form.get("next") or "").strip() or url_for("strategies_page")
    set_setting_value(ACTIVE_STRATEGY_ID_KEY, str(strategy_id))
    set_setting_value(ACTIVE_STRATEGY_DATE_KEY, now_et().date().isoformat())
    set_setting_value(ACTIVE_STRATEGY_CHECKS_KEY, "[]")
    return redirect(next_url)


def strategies_checklist_update():
    items = [dict(r) for r in repo.fetch_strategies()]
    state = _active_strategy_state(items)
    next_url = (request.form.get("next") or "").strip() or url_for("strategies_page")
    checklist_rows = list(state.get("rows") or [])
    checked_indexes = {str(v) for v in request.form.getlist("done_idx")}
    checks = [str(idx) in checked_indexes for idx in range(len(checklist_rows))]
    set_setting_value(ACTIVE_STRATEGY_DATE_KEY, now_et().date().isoformat())
    set_setting_value(ACTIVE_STRATEGY_CHECKS_KEY, json.dumps(checks))
    complete = len([v for v in checks if v])
    total = len(checks)
    progress_pct = (complete / total * 100.0) if total > 0 else 0.0
    if _wants_json():
        return jsonify(
            {
                "ok": True,
                "complete": complete,
                "total": total,
                "progress_pct": progress_pct,
            }
        )
    return redirect(next_url)


def strat_page():
    return core_svc.strat_page()


def _parse_checklist(raw: str) -> List[str]:
    lines: List[str] = []
    for line in str(raw or "").splitlines():
        clean = line.strip().lstrip("-").strip()
        if clean:
            lines.append(clean)
    return lines


def _active_strategy_state(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    item_map = {int(i["id"]): i for i in items if int(i.get("id") or 0) > 0}
    active_id = int(get_setting_value(ACTIVE_STRATEGY_ID_KEY, 0) or 0)
    if active_id not in item_map and items:
        active_id = int(items[0].get("id") or 0)
    active = item_map.get(active_id) if active_id else None

    checklist_items = _parse_checklist(str((active or {}).get("checklist") or ""))
    today_iso = now_et().date().isoformat()
    saved_date = str(get_setting_value(ACTIVE_STRATEGY_DATE_KEY, "") or "")
    saved_checks_raw = str(get_setting_value(ACTIVE_STRATEGY_CHECKS_KEY, "[]") or "[]")
    if saved_date != today_iso:
        checks: List[bool] = []
    else:
        try:
            parsed = json.loads(saved_checks_raw)
            checks = [bool(v) for v in parsed] if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            checks = []
    checks = (checks + [False] * len(checklist_items))[: len(checklist_items)]
    rows = [
        {"idx": idx, "label": label, "done": checks[idx] if idx < len(checks) else False}
        for idx, label in enumerate(checklist_items)
    ]
    complete = len([r for r in rows if r["done"]])
    total = len(rows)
    progress = (complete / total * 100.0) if total > 0 else 0.0
    return {
        "active_id": int(active_id or 0),
        "active_title": str((active or {}).get("title") or "No active strategy"),
        "rows": rows,
        "complete": complete,
        "total": total,
        "progress_pct": progress,
    }


def _wants_json() -> bool:
    if str(request.headers.get("X-Requested-With") or "").lower() == "xmlhttprequest":
        return True
    accept = str(request.headers.get("Accept") or "").lower()
    return "application/json" in accept


def _build_strategy_scorecards(
    items: List[Dict[str, Any]], analytics_rows: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {
        str(item.get("title") or "").strip(): [] for item in items
    }
    for row in analytics_rows:
        key = str(row.get("setup_tag") or "").strip()
        if key in grouped:
            grouped[key].append(row)

    scorecards: List[Dict[str, Any]] = []
    for item in items:
        title = str(item.get("title") or "").strip()
        rows = grouped.get(title, [])
        perf = analytics_repo.performance_metrics(rows)
        recent_rows = rows[-10:]
        recent_perf = analytics_repo.performance_metrics(recent_rows)
        avg_score_values = [
            float(r["checklist_score"])
            for r in rows
            if r.get("checklist_score") is not None and str(r.get("checklist_score")).strip() != ""
        ]
        updated_label = str(item.get("updated_at") or "")
        try:
            updated_label = datetime.fromisoformat(updated_label.replace("Z", "+00:00")).strftime(
                "%b %d, %Y"
            )
        except Exception:
            pass
        scorecards.append(
            {
                **item,
                "count": perf["total_trades"],
                "win_rate": perf["win_rate"],
                "expectancy": perf["expectancy"],
                "net": perf["total_net"],
                "avg_win": perf["avg_win"],
                "avg_loss_abs": perf["avg_loss_abs"],
                "profit_factor": perf["profit_factor"],
                "max_drawdown": perf["max_drawdown"],
                "recent_net": recent_perf["total_net"],
                "recent_win_rate": recent_perf["win_rate"],
                "avg_score": (
                    (sum(avg_score_values) / len(avg_score_values)) if avg_score_values else None
                ),
                "status": _strategy_status(perf, recent_perf),
                "status_tone": _strategy_status_tone(perf, recent_perf),
                "updated_label": updated_label,
            }
        )
    scorecards.sort(key=lambda x: (x["count"], x["net"]), reverse=True)
    return scorecards


def _strategy_status(perf: Dict[str, Any], recent_perf: Dict[str, Any]) -> str:
    if perf["total_trades"] == 0:
        return "Build sample"
    if recent_perf["total_trades"] >= 3 and recent_perf["total_net"] < 0:
        return "Review now"
    if perf["win_rate"] >= 55.0 and perf["expectancy"] > 0:
        return "Trade"
    return "Tighten"


def _strategy_status_tone(perf: Dict[str, Any], recent_perf: Dict[str, Any]) -> str:
    status = _strategy_status(perf, recent_perf)
    if status == "Trade":
        return "metaGreen"
    if status == "Review now":
        return "metaRed"
    return "metaAmber"
