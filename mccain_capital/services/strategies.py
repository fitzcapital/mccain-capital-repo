"""Strategies domain service functions."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from flask import abort, flash, redirect, render_template, request, url_for

from mccain_capital.services import core as core_svc
from mccain_capital.services.ui import render_page
from mccain_capital.runtime import money
from mccain_capital.repositories import strategies as repo
from mccain_capital.repositories import analytics as analytics_repo
from mccain_capital.services.viewmodels import StateBadgeViewModel


def _strategy_form(title: str, t: str, body: str, errors: List[str]) -> str:
    return render_template(
        "strategies/form.html",
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
    total_trades = sum(int(card.get("count") or 0) for card in scorecards)
    active_cards = sum(1 for card in scorecards if str(card.get("status") or "") == "Trade")
    if headline and str(headline.get("status") or "") == "Trade":
        hero_title = "Trade the Proven Seats"
        hero_blurb = "The playbook has at least one setup earning its place. Keep size aligned with the cards actually paying."
    elif headline:
        hero_title = "Tighten the Playbook Before You Add Size"
        hero_blurb = "There is data on the board, but the best seat still needs review before it deserves more risk."
    else:
        hero_title = "Build the Playbook From Real Edge"
        hero_blurb = (
            "Write one executable card at a time and let expectancy decide whether it survives."
        )

    strategy_status_badges = [
        StateBadgeViewModel(
            label="Confidence",
            value=("High" if total_trades >= 20 else "Mixed"),
            tone=("healthy" if total_trades >= 20 else "caution"),
            title="Confidence in the playbook based on tracked strategy sample size.",
        ),
        StateBadgeViewModel(
            label="Cards",
            value=f"{len(scorecards)} active",
            tone=("healthy" if scorecards else "neutral"),
            title="Strategy scorecards currently being tracked.",
        ),
        StateBadgeViewModel(
            label="Trade Seats",
            value=(f"{active_cards} ready" if active_cards else "Review only"),
            tone=("healthy" if active_cards else "caution"),
            title="How many strategy cards are currently in tradeable shape.",
        ),
        StateBadgeViewModel(
            label="Sample",
            value=(f"{total_trades} trades" if total_trades else "No sample"),
            tone=("healthy" if total_trades else "neutral"),
            title="Total trades matched to strategy scorecards.",
        ),
    ]
    content = render_template(
        "strategies/index.html",
        items=items,
        scorecards=scorecards,
        headline=headline,
        hero_title=hero_title,
        hero_blurb=hero_blurb,
        strategy_status_badges=strategy_status_badges,
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
        flash("Strategy saved.", "success")
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
        flash("Strategy updated.", "success")
        return redirect(url_for("strategies_page"))

    return render_page(
        _strategy_form("Edit Strategy", row["title"], row["body"], []), active="strategies"
    )


def strategies_delete(sid: int):
    repo.delete_strategy(sid=sid)
    flash("Strategy deleted.", "success")
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
