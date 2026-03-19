"""Journal domain service functions."""

from __future__ import annotations

import json
import os
import secrets
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from flask import (
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from werkzeug.utils import secure_filename

from mccain_capital.repositories import journal as repo
from mccain_capital.repositories import trades as trades_repo
from mccain_capital import runtime as app_runtime
from mccain_capital.runtime import money, parse_float, today_iso
from mccain_capital.services.ui import render_page
from mccain_capital.services.viewmodels import StateBadgeViewModel

_CAPTURE_ALLOWED_EXTS = {".png", ".jpg", ".jpeg", ".webp"}


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
        payload = _safe_template_payload(entry.get("template_payload"))
        screenshot_path = str(payload.get("capture_screenshot_path") or "").strip()
        entry["capture_screenshot_path"] = screenshot_path
        entry["capture_screenshot_href"] = _capture_href(screenshot_path)
        entry["capture_screenshot_note"] = str(payload.get("capture_screenshot_note") or "").strip()
        note_view = _render_note_sections(str(entry.get("notes") or "").strip())
        entry["note_sections"] = note_view["sections"]
        entry["note_plain"] = note_view["plain"]

    latest_entry = entries[0] if entries else {}
    linked_trades_total = sum(int(entry.get("linked_trades") or 0) for entry in entries)
    grade_count = sum(1 for entry in entries if str(entry.get("grade") or "").strip())
    latest_day = str(
        latest_entry.get("entry_date_display") or latest_entry.get("entry_date") or ""
    ).strip()
    latest_mood = str(latest_entry.get("mood") or "").strip()
    latest_setup = str(latest_entry.get("setup") or "").strip()
    latest_grade = str(latest_entry.get("grade") or "").strip()

    if not entries:
        hero_title = "Capture Today While It Is Fresh"
        hero_blurb = "A fast note beats a perfect memory. Log what you saw, what you did, and what it taught you."
        review_focus_lead = "No journal entries are in scope yet."
        review_focus_body = "Use Quick Capture right after the next setup or create a full debrief before the close."
        review_focus_meta = "One clean entry unlocks weekly review and pattern tracking."
        capture_lead = "The review loop is empty."
        capture_body = "Start with one short note, one mood tag, and one linked trade so the debrief system has something real to learn from."
        capture_meta = "Next move: open Quick Capture from phone or desktop."
    elif d:
        hero_title = "Close the Loop on This Session"
        hero_blurb = "Review the selected day cleanly before the next session adds more noise."
        review_focus_lead = f"{latest_day or 'Selected day'} is the active review window."
        review_focus_body = (
            f"Latest note: {latest_setup or 'Context only'}"
            + (f" · Mood {latest_mood}" if latest_mood else "")
            + (f" · Grade {latest_grade}" if latest_grade else "")
        )
        review_focus_meta = (
            "Refine the lesson, link the missing trades, then lock the weekly narrative."
        )
        capture_lead = (
            f"{len(entries)} entr{'y' if len(entries) == 1 else 'ies'} in this day filter."
        )
        capture_body = "Keep the selected session tight: finish the main debrief, add any screenshot evidence, then stop adding duplicate notes."
        capture_meta = "Next move: edit the strongest entry or add one missing post-trade note."
    else:
        hero_title = "Close the Learning Loop"
        hero_blurb = "Use the journal as a review system, not a note dump. Keep the lesson sharp and the next rule obvious."
        review_focus_lead = f"Latest review: {latest_day or 'Recent session'}" + (
            f" · {latest_setup}" if latest_setup else ""
        )
        review_focus_body = f"Most recent note carries {latest_mood or 'neutral'} tone" + (
            f" with grade {latest_grade}." if latest_grade else "."
        )
        review_focus_meta = (
            "Promote the cleanest lesson into weekly review instead of stacking similar entries."
        )
        capture_lead = f"{len(entries)} entr{'y' if len(entries) == 1 else 'ies'} in scope · {linked_trades_total} linked trades."
        capture_body = "Keep capture fast: one clear lesson, one emotion tag, one linked trade cluster. Save the long reflection for the weekly review."
        capture_meta = "Next move: use Quick Capture for the next live observation, then write one fuller debrief."

    journal_status_badges = [
        StateBadgeViewModel(
            label="Entries",
            value=f"{len(entries)} in scope",
            tone=("healthy" if entries else "caution"),
            title="Journal entries visible in the current filter.",
        ),
        StateBadgeViewModel(
            label="Linked Trades",
            value=(str(linked_trades_total) if linked_trades_total else "0"),
            tone=("healthy" if linked_trades_total else "neutral"),
            title="Trade rows linked into these journal entries.",
        ),
        StateBadgeViewModel(
            label="Graded",
            value=(f"{grade_count} tagged" if grade_count else "Not graded"),
            tone=("healthy" if grade_count else "caution"),
            title="Entries with an explicit grade in the current scope.",
        ),
        StateBadgeViewModel(
            label="Last Update",
            value=(str(latest_entry.get("updated_at_display") or "No entries")),
            tone=("healthy" if latest_entry else "neutral"),
            title="Most recent update timestamp in the current filter.",
        ),
    ]

    content = render_template(
        "journal/home.html",
        q=q,
        d=d,
        entries=entries,
        money=money,
        hero_title=hero_title,
        hero_blurb=hero_blurb,
        review_focus_lead=review_focus_lead,
        review_focus_body=review_focus_body,
        review_focus_meta=review_focus_meta,
        capture_lead=capture_lead,
        capture_body=capture_body,
        capture_meta=capture_meta,
        journal_status_badges=journal_status_badges,
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
        screenshot_path = _save_capture_upload(request.files.get("capture_screenshot"), entry_date)
        if not screenshot_path:
            screenshot_path = (f.get("existing_capture_screenshot_path") or "").strip()
        screenshot_note = (f.get("capture_screenshot_note") or "").strip()
        if not notes:
            values = dict(f)
            values["entry_date"] = entry_date
            values["capture_screenshot_path"] = screenshot_path
            values["capture_screenshot_href"] = _capture_href(screenshot_path)
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
                "template_payload": _entry_template_payload(
                    template_notes, screenshot_path, screenshot_note
                ),
            }
        )
        repo.set_entry_trade_links(entry_id, linked_ids)
        flash("Journal entry saved.", "success")
        return redirect(url_for("journal_home", d=entry_date))

    prefill_date = (request.args.get("d") or "").strip()
    entry_date = prefill_date or _default_entry_date_for_journal()
    auto_draft = (request.args.get("auto_draft") or "0").strip() == "1"
    link_all_arg = (request.args.get("link_all_day") or "").strip()
    link_all_day = "1" if link_all_arg == "1" or (not link_all_arg and auto_draft) else "0"
    hydrate_trade_context = link_all_day == "1" or auto_draft
    initial_values = {
        "entry_date": entry_date,
        "entry_type": (request.args.get("entry_type") or "post_market").strip() or "post_market",
        "auto_draft": "1" if auto_draft else "0",
        "quick_capture": "1" if (request.args.get("quick_capture") or "0").strip() == "1" else "0",
        "link_all_day": link_all_day,
        "market": (request.args.get("market") or "").strip(),
        "setup": (request.args.get("setup") or "").strip(),
        "grade": (request.args.get("grade") or "").strip(),
        "mood": (request.args.get("mood") or "").strip(),
        "pnl": (request.args.get("pnl") or "").strip(),
        "notes": (request.args.get("notes") or "").strip(),
        "template_notes": (request.args.get("template_notes") or "").strip(),
        "capture_screenshot_path": (request.args.get("capture_screenshot_path") or "").strip(),
        "capture_screenshot_note": (request.args.get("capture_screenshot_note") or "").strip(),
    }
    selected_ids = _trade_ids_for_date(entry_date) if link_all_day == "1" else []
    scaffold = _build_debrief_scaffold(
        entry_date,
        initial_values["entry_type"],
        selected_ids,
        auto_draft=auto_draft,
        hydrate_from_trades=hydrate_trade_context,
        include_market_context=hydrate_trade_context,
    )
    if not initial_values["template_notes"]:
        initial_values["template_notes"] = scaffold["template_notes"]
    if not initial_values["notes"]:
        initial_values["notes"] = scaffold["notes"]
    if not initial_values["pnl"] and scaffold["pnl"] is not None:
        initial_values["pnl"] = f"{float(scaffold['pnl']):.2f}"
    if not initial_values["market"] and scaffold.get("market"):
        initial_values["market"] = str(scaffold["market"])
    if not initial_values["setup"] and scaffold.get("setup"):
        initial_values["setup"] = str(scaffold["setup"])
    if not initial_values["mood"] and scaffold.get("mood"):
        initial_values["mood"] = str(scaffold["mood"])
    initial_values["capture_screenshot_href"] = _capture_href(
        initial_values["capture_screenshot_path"]
    )
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
        screenshot_path = _save_capture_upload(request.files.get("capture_screenshot"), entry_date)
        if not screenshot_path:
            screenshot_path = (f.get("existing_capture_screenshot_path") or "").strip()
        screenshot_note = (f.get("capture_screenshot_note") or "").strip()
        if not notes:
            values = dict(f)
            values["entry_date"] = entry_date
            values["capture_screenshot_path"] = screenshot_path
            values["capture_screenshot_href"] = _capture_href(screenshot_path)
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
                "template_payload": _entry_template_payload(
                    template_notes, screenshot_path, screenshot_note
                ),
            },
        )
        repo.set_entry_trade_links(entry_id, linked_ids)
        flash("Journal entry saved.", "success")
        return redirect(url_for("journal_home", d=entry_date))

    values = dict(row)
    if values.get("pnl") is None:
        values["pnl"] = ""
    payload = _safe_template_payload(values.get("template_payload"))
    values["template_notes"] = payload.get("template_notes", "")
    values["capture_screenshot_path"] = str(payload.get("capture_screenshot_path") or "").strip()
    values["capture_screenshot_href"] = _capture_href(values["capture_screenshot_path"])
    values["capture_screenshot_note"] = str(payload.get("capture_screenshot_note") or "").strip()
    linked_ids = repo.fetch_entry_trade_ids(entry_id)
    values["linked_trade_ids"] = ",".join(str(i) for i in linked_ids)
    values["link_all_day"] = "0"
    values["quick_capture"] = "0"
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
    flash("Journal entry deleted.", "success")
    return redirect(url_for("journal_home"))


def journal_capture_asset(name: str):
    rel = str(name or "").strip().lstrip("/")
    if not rel:
        abort(404)
    ext = os.path.splitext(rel)[1].lower()
    if ext not in _CAPTURE_ALLOWED_EXTS:
        abort(404)
    root = os.path.abspath(app_runtime.upload_path("journal-captures"))
    path = os.path.abspath(app_runtime.upload_path(rel))
    if not path.startswith(root + os.sep):
        abort(404)
    if not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=False)


def _entry_template_payload(
    template_notes: str, screenshot_path: str, screenshot_note: str = ""
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"template_notes": template_notes}
    if screenshot_path:
        payload["capture_screenshot_path"] = screenshot_path
    if screenshot_note:
        payload["capture_screenshot_note"] = screenshot_note
    return payload


def _normalize_note_heading(text: str) -> str:
    clean = " ".join(str(text or "").strip().replace("_", " ").split())
    lower = clean.lower().rstrip(":")
    aliases = {
        "what i saw": "What I Saw",
        "context": "What I Saw",
        "read": "What I Saw",
        "what i did": "What I Did",
        "execution": "What I Did",
        "what happened": "What Happened",
        "lesson": "Lesson",
        "takeaway": "Lesson",
        "next time": "Next Time",
        "next action": "Next Time",
        "rule": "Rule",
        "plan": "Plan",
    }
    return aliases.get(lower, clean.rstrip(":"))


def _render_note_sections(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {"sections": [], "plain": ""}
    blocks = [block.strip() for block in raw.split("\n\n") if block.strip()]
    sections: List[Dict[str, str]] = []
    for block in blocks:
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        if not lines:
            continue
        first = lines[0]
        title = ""
        body = ""
        if ":" in first:
            maybe_title, maybe_body = first.split(":", 1)
            if len(maybe_title.strip()) <= 24:
                title = _normalize_note_heading(maybe_title)
                tail = [maybe_body.strip()] if maybe_body.strip() else []
                if len(lines) > 1:
                    tail.extend(lines[1:])
                body = "\n".join([line for line in tail if line])
        elif len(lines) >= 2 and len(first) <= 24:
            title = _normalize_note_heading(first)
            body = "\n".join(lines[1:])
        if title and body:
            sections.append({"title": title, "body": body})
    if sections:
        return {"sections": sections, "plain": ""}
    return {"sections": [], "plain": raw}


def _capture_href(relpath: str) -> str:
    rel = str(relpath or "").strip()
    if not rel:
        return ""
    return url_for("journal_capture_asset", name=rel)


def _save_capture_upload(upload: Any, entry_date: str) -> str:
    if not upload or not getattr(upload, "filename", ""):
        return ""
    filename = secure_filename(str(upload.filename or ""))
    ext = os.path.splitext(filename)[1].lower()
    if ext not in _CAPTURE_ALLOWED_EXTS:
        return ""
    day = str(entry_date or today_iso()).strip() or today_iso()
    rel_dir = os.path.join("journal-captures", day)
    abs_dir = app_runtime.upload_path(rel_dir)
    os.makedirs(abs_dir, exist_ok=True)
    stored = f"{day.replace('-', '')}-{secrets.token_hex(6)}{ext}"
    abs_path = app_runtime.upload_path(rel_dir, stored)
    upload.save(abs_path)
    return os.path.join(rel_dir, stored).replace("\\", "/")


def _weekly_coach_rule(top_break: str, strongest_setup: str, strongest_session: str) -> str:
    tag = str(top_break or "").strip().lower()
    if tag in {"revenge", "revenge-trading"}:
        return "After a red trade, pause and re-state the setup before any next entry."
    if tag in {"oversized", "size creep"}:
        return "Cut size before you cut standards. Keep size fixed until the A setup is obvious."
    if tag in {"late-entry", "early entry"}:
        return "Let confirmation print first. Missed is cheaper than forcing the first touch."
    if strongest_setup and strongest_setup != "No setup data":
        if strongest_session:
            return f"Press {strongest_setup} only when the tape matches your {strongest_session} conditions."
        return f"Repeat {strongest_setup} only when structure is clean enough to name before entry."
    return "If the setup is not named and the invalidation is not obvious, do not take it."


def _weekly_coach_summary(
    setup_stats: List[Dict[str, Any]],
    mood_stats: List[Dict[str, Any]],
    rule_breaks: List[Dict[str, Any]],
    day_rollups: List[Dict[str, Any]],
) -> Dict[str, str]:
    strongest = next((row for row in setup_stats if float(row.get("net") or 0.0) > 0), None)
    leak = rule_breaks[0] if rule_breaks else None
    weakest_mood = None
    mood_candidates = [row for row in mood_stats if int(row.get("count") or 0) > 0]
    if mood_candidates:
        weakest_mood = min(mood_candidates, key=lambda row: float(row.get("avg_pnl") or 0.0))
    best_day = max(day_rollups, key=lambda row: float(row.get("pnl_total") or 0.0), default=None)
    worst_day = min(day_rollups, key=lambda row: float(row.get("pnl_total") or 0.0), default=None)
    week_net = sum(float(row.get("pnl_total") or 0.0) for row in day_rollups)
    active_days = len(day_rollups)
    green_days = len([row for row in day_rollups if float(row.get("pnl_total") or 0.0) > 0])
    strongest_setup = str((strongest or {}).get("setup") or "")
    return {
        "headline": "Press the edge, cut the leak, and keep the week small enough to control.",
        "strongest_setup": (
            f"{strongest_setup} · {money(float((strongest or {}).get('net') or 0.0))}"
            if strongest
            else "No setup edge identified yet."
        ),
        "biggest_leak": (
            f"{str(leak.get('tag') or '').replace('-', ' ')} · {int(leak.get('count') or 0)}x"
            if leak
            else (
                f"Mood drift: {weakest_mood.get('mood')} averaged {money(float(weakest_mood.get('avg_pnl') or 0.0))}"
                if weakest_mood and float(weakest_mood.get("avg_pnl") or 0.0) < 0
                else "No repeated leak tagged yet."
            )
        ),
        "one_rule": _weekly_coach_rule(
            str((leak or {}).get("tag") or ""),
            strongest_setup,
            str((strongest or {}).get("session") or ""),
        ),
        "consistency": (
            f"{green_days}/{active_days} green day{'s' if active_days != 1 else ''} · {money(week_net)} net"
            if active_days
            else "No active journal days this week."
        ),
        "best_day": (
            f"{best_day.get('entry_date')} · {money(float(best_day.get('pnl_total') or 0.0))}"
            if best_day
            else "No best day yet."
        ),
        "worst_day": (
            f"{worst_day.get('entry_date')} · {money(float(worst_day.get('pnl_total') or 0.0))}"
            if worst_day
            else "No worst day yet."
        ),
    }


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
    day_rollups = repo.fetch_entry_day_rollups(week_start, week_end)
    setup_stats = repo.weekly_setup_stats(week_start, week_end)
    mood_stats = repo.weekly_mood_stats(week_start, week_end)
    rule_breaks = repo.weekly_rule_break_tags(week_start, week_end)
    top_setup = setup_stats[0]["setup"] if setup_stats else "No setup data"
    top_setup_net = float(setup_stats[0]["net"] or 0.0) if setup_stats else 0.0
    top_mood = mood_stats[0]["mood"] if mood_stats else "No mood data"
    top_mood_avg = float(mood_stats[0]["avg_pnl"] or 0.0) if mood_stats else 0.0
    top_break = rule_breaks[0]["tag"] if rule_breaks else "No repeated tags"
    top_break_count = int(rule_breaks[0]["count"] or 0) if rule_breaks else 0
    coach = _weekly_coach_summary(setup_stats, mood_stats, rule_breaks, day_rollups)
    week_net = sum(float(row.get("pnl_total") or 0.0) for row in day_rollups)
    active_days = len(day_rollups)
    green_days = len([row for row in day_rollups if float(row.get("pnl_total") or 0.0) > 0])
    content = render_template(
        "journal/weekly_review.html",
        week_start=week_start,
        week_end=week_end,
        entries=entries,
        day_rollups=day_rollups,
        setup_stats=setup_stats,
        mood_stats=mood_stats,
        rule_breaks=rule_breaks,
        top_setup=top_setup,
        top_setup_net=top_setup_net,
        top_mood=top_mood,
        top_mood_avg=top_mood_avg,
        top_break=top_break,
        top_break_count=top_break_count,
        coach=coach,
        week_net=week_net,
        active_days=active_days,
        green_days=green_days,
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

    latest_trade_day = trades_repo.fetch_latest_trade_date()
    if not latest_trade_day:
        return today
    return latest_trade_day


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


def _build_debrief_scaffold(
    entry_date: str,
    entry_type: str,
    linked_trade_ids: List[int],
    *,
    auto_draft: bool = False,
    hydrate_from_trades: bool = True,
    include_market_context: bool = True,
) -> Dict[str, str]:
    entry_kind = (entry_type or "").strip() or "post_market"
    if entry_kind == "pre_market":
        return {
            "template_notes": "Planned levels, catalysts, invalidation, and risk limits for the session.",
            "notes": "",
            "pnl": None,
        }

    if not hydrate_from_trades:
        return {
            "template_notes": "",
            "notes": "\n".join(
                [
                    "What I saw:",
                    "- Day context / market behavior:",
                    "- Best setup or read:",
                    "",
                    "What I did:",
                    "- Risk and execution summary:",
                    "- Rule adherence / mistakes:",
                    "",
                    "What I learned:",
                    "- Keep doing:",
                    "- Stop doing:",
                    "- Next session adjustment:",
                ]
            ),
            "pnl": None,
            "market": "",
            "setup": "",
            "mood": "",
        }

    trades = _trade_options_for_date(entry_date)
    if linked_trade_ids:
        wanted = set(int(tid) for tid in linked_trade_ids)
        trades = [t for t in trades if int(t.get("id") or 0) in wanted]
    stats = trades_repo.trade_day_stats(trades)
    review_map = trades_repo.fetch_trade_reviews_map(
        [int(t["id"]) for t in trades if t.get("id") is not None]
    )
    tickers = sorted(
        {str(t.get("ticker") or "").strip() for t in trades if str(t.get("ticker") or "").strip()}
    )
    strategy_labels = sorted(
        {
            str(
                (review_map.get(int(t["id"]), {}) or {}).get("strategy_label")
                or (review_map.get(int(t["id"]), {}) or {}).get("setup_tag")
                or ""
            ).strip()
            for t in trades
            if t.get("id") is not None
        }
        - {""}
    )
    dominant_setup = strategy_labels[0] if strategy_labels else ""
    session_tags = sorted(
        {
            str((review_map.get(int(t["id"]), {}) or {}).get("session_tag") or "").strip()
            for t in trades
            if t.get("id") is not None
        }
        - {""}
    )
    rule_breaks = sorted(
        {
            tag.strip()
            for t in trades
            if t.get("id") is not None
            for tag in str(
                (review_map.get(int(t["id"]), {}) or {}).get("rule_break_tags") or ""
            ).split(",")
            if tag.strip()
        }
    )
    sorted_trades = sorted(
        [dict(t) for t in trades],
        key=lambda row: float(row.get("net_pl") or 0.0),
        reverse=True,
    )
    best_trade = sorted_trades[0] if sorted_trades else None
    worst_trade = sorted_trades[-1] if sorted_trades else None
    market_context = _debrief_market_context(entry_date) if include_market_context else {}
    template_lines = [
        f"Session date: {entry_date}",
        f"Trades linked: {len(trades)}",
        f"Wins / Losses: {int(stats.get('wins', 0) or 0)} / {int(stats.get('losses', 0) or 0)}",
        f"Net P/L: {money(float(stats.get('total', 0.0) or 0.0))}",
        "Tickers: " + (", ".join(tickers) if tickers else "None linked yet"),
        "Strategies: " + (", ".join(strategy_labels) if strategy_labels else "Unlabeled"),
    ]
    if market_context.get("headline"):
        template_lines.extend(
            [
                f"Market pulse: {market_context['headline']}",
                f"Key levels: {market_context['levels']}",
                f"Macro watch: {market_context['macro']}",
            ]
        )
    if dominant_setup:
        template_lines.append(f"Dominant setup: {dominant_setup}")
    if session_tags:
        template_lines.append("Session tags: " + ", ".join(session_tags))
    if rule_breaks:
        template_lines.append("Rule breaks: " + ", ".join(rule_breaks))

    if auto_draft:
        notes_lines = [
            "Auto Debrief Draft",
            "",
            "Session scorecard:",
            f"- Result: {int(stats.get('wins', 0) or 0)}W / {int(stats.get('losses', 0) or 0)}L · {money(float(stats.get('total', 0.0) or 0.0))} net",
            f"- Tickers traded: {', '.join(tickers) if tickers else 'None linked yet'}",
            f"- Dominant setup: {dominant_setup or 'Unlabeled'}",
            f"- Session focus: {', '.join(session_tags) if session_tags else 'Not tagged yet'}",
            "",
            "Market context:",
            f"- {market_context.get('headline') or 'Market context unavailable'}",
            f"- Key levels: {market_context.get('levels') or 'Not available'}",
            f"- Macro watch: {market_context.get('macro') or 'No macro catalyst loaded'}",
            "",
            "Execution read:",
            f"- Best trade: {_trade_line(best_trade) if best_trade else 'None'}",
            f"- Worst trade: {_trade_line(worst_trade) if worst_trade else 'None'}",
            f"- Rule breaks logged: {', '.join(rule_breaks) if rule_breaks else 'none logged'}",
            "",
            "Prompt yourself:",
            "- What did the market offer that actually matched the plan?",
            "- Where did execution slip versus what the setup required?",
            "- What one adjustment matters most next session?",
        ]
    else:
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
        "market": market_context.get("market") or "",
        "setup": dominant_setup,
        "mood": _draft_mood_label(
            float(stats.get("total", 0.0) or 0.0),
            int(stats.get("wins", 0) or 0),
            int(stats.get("losses", 0) or 0),
        ),
    }


def _trade_line(row: Optional[Dict[str, Any]]) -> str:
    if not isinstance(row, dict):
        return ""
    return (
        f"#{int(row.get('id') or 0)} "
        f"{str(row.get('ticker') or '').strip()} {str(row.get('opt_type') or '').strip()} "
        f"{str(row.get('entry_time') or '—')}->{str(row.get('exit_time') or '—')} "
        f"{money(float(row.get('net_pl') or 0.0))}"
    ).strip()


def _draft_mood_label(day_net: float, wins: int, losses: int) -> str:
    if wins and day_net > 0:
        return "Confident but controlled"
    if losses and day_net < 0:
        return "Frustrated - review discipline"
    return "Neutral / process review"


def _debrief_market_context(entry_date: str) -> Dict[str, str]:
    try:
        from mccain_capital.services import core as core_svc
        from mccain_capital.services import gamma_map_service
    except Exception:
        return {"headline": "", "levels": "", "macro": "", "market": ""}

    now_et = app_runtime.now_et()
    try:
        snapshot = core_svc._market_pulse_snapshot()
        quotes = core_svc._market_pulse_enrich_quotes(list(snapshot.get("quotes") or []), now_et)
        context = core_svc._market_pulse_context(quotes)
    except Exception:
        quotes = []
        context = {}
    try:
        gamma_snapshot = gamma_map_service.get_gamma_snapshot()
    except Exception:
        gamma_snapshot = {}
    try:
        news_snapshot = core_svc._market_news_snapshot()
    except Exception:
        news_snapshot = {"macro_events": []}

    spx_quote = next((q for q in quotes if str(q.get("label") or "") == "SPX"), {})
    vix_quote = next((q for q in quotes if str(q.get("label") or "") == "VIX"), {})
    spot = float(spx_quote.get("price") or 0.0) if spx_quote.get("price") is not None else None
    vix = float(vix_quote.get("price") or 0.0) if vix_quote.get("price") is not None else None
    gamma_flip = gamma_snapshot.get("gamma_flip")
    call_wall = gamma_snapshot.get("call_wall")
    put_wall = gamma_snapshot.get("put_wall")

    structure = str(context.get("structure") or context.get("market_posture") or "").strip()
    if spot is not None:
        headline = f"SPX {spot:.2f}"
        if structure:
            headline += f" · {structure}"
        if vix is not None:
            headline += f" · VIX {vix:.2f}"
    else:
        headline = structure or "Market context unavailable"

    level_parts = []
    if put_wall is not None:
        level_parts.append(f"Put wall {float(put_wall):.0f}")
    if gamma_flip is not None:
        level_parts.append(f"Gamma flip {float(gamma_flip):.0f}")
    if call_wall is not None:
        level_parts.append(f"Call wall {float(call_wall):.0f}")
    levels = " · ".join(level_parts)

    macro_rows = list(news_snapshot.get("macro_events") or [])[:2]
    macro = " / ".join(
        f"{str(row.get('headline') or 'Macro event')} ({str(row.get('published_label') or 'time TBD')})"
        for row in macro_rows
        if isinstance(row, dict)
    )

    market = "SPX"
    if vix is not None:
        market = f"SPX / VIX {vix:.2f}"
    return {
        "headline": headline,
        "levels": levels,
        "macro": macro,
        "market": market,
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
