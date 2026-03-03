"""Strategies domain service functions."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from flask import abort, redirect, render_template, render_template_string, request, url_for

from mccain_capital.services import core as core_svc
from mccain_capital.services.ui import render_page
from mccain_capital.runtime import money
from mccain_capital.repositories import strategies as repo
from mccain_capital.repositories import analytics as analytics_repo


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
    headline = scorecards[0] if scorecards else None
    content = render_template(
        "strategies/index.html",
        items=items,
        scorecards=scorecards,
        headline=headline,
        money=money,
    )
    return render_page(content, active="strategies")


def strategies_new():
    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        body = (request.form.get("body") or "").strip()
        if not title or not body:
            return render_page(
                _strategy_form("New Strategy", title, body, ["Title and body required."]),
                active="strategies",
            )
        repo.create_strategy(title=title, body=body)
        return redirect(url_for("strategies_page"))
    return render_page(_strategy_form("New Strategy", "", "", []), active="strategies")


def strategies_edit(sid: int):
    row = repo.get_strategy(sid=sid)
    if not row:
        abort(404)

    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        body = (request.form.get("body") or "").strip()
        if not title or not body:
            return render_page(
                _strategy_form("Edit Strategy", title, body, ["Title and body required."]),
                active="strategies",
            )
        repo.update_strategy(sid=sid, title=title, body=body)
        return redirect(url_for("strategies_page"))

    return render_page(
        _strategy_form("Edit Strategy", row["title"], row["body"], []), active="strategies"
    )


def strategies_delete(sid: int):
    repo.delete_strategy(sid=sid)
    return redirect(url_for("strategies_page"))


def strat_page():
    return core_svc.strat_page()


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
