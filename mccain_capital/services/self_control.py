"""Self-control discipline console services."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import json
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from flask import flash, jsonify, redirect, render_template, request, url_for

from mccain_capital import runtime as app_runtime
from mccain_capital.repositories import journal as journal_repo
from mccain_capital.repositories import self_control as repo
from mccain_capital.repositories import trades as trades_repo
from mccain_capital.runtime import db
from mccain_capital.services.ui import render_page

SELF_CONTROL_DURATIONS = (15, 30, 45, 60, 90, 120)
SELF_CONTROL_CATEGORIES = (
    "Social",
    "Entertainment",
    "News / Doomscroll",
    "Trading",
    "Random distractions",
)
SELF_CONTROL_STATE_PATH = os.path.join(
    app_runtime.PERSISTENT_DATA_DIR, ".self_control_enforcement_state.json"
)
SELF_CONTROL_STATUS_PATH = os.path.join(
    app_runtime.PERSISTENT_DATA_DIR, ".self_control_enforcement_status.json"
)


def self_control_page():
    session = _recover_stale_session()
    _write_enforcement_state_snapshot(session)
    model = _self_control_page_model(active_session=session)
    content = render_template("self_control/index.html", **model)
    return render_page(content, active="self-control")


def self_control_state_api():
    session = _recover_stale_session()
    _write_enforcement_state_snapshot(session)
    model = _self_control_page_model(active_session=session, include_history=False)
    return jsonify(
        {
            "ok": True,
            "now_iso": app_runtime.now_iso(),
            "session": model["active_session"],
            "metrics": model["daily_metrics"],
            "unlock_ready": bool(model["unlock_ready"]),
        }
    )


def self_control_session_start():
    session = _recover_stale_session()
    if session and session.get("status") in {"active", "awaiting_journal_unlock"}:
        flash("A self-control session is already active.", "warning")
        return redirect(url_for("self_control_page"))

    if str(request.form.get("start_confirmed") or "").strip() != "1":
        flash("Session start requires confirmation.", "warning")
        return redirect(url_for("self_control_page"))

    now_et = app_runtime.now_et()
    preset_slug = str(request.form.get("preset_slug") or "").strip()
    custom_duration = int(app_runtime.parse_float(request.form.get("custom_duration") or "") or 0)
    duration = int(app_runtime.parse_float(request.form.get("duration_minutes") or "") or 0)
    if custom_duration > 0:
        duration = custom_duration
    if duration <= 0:
        flash("Choose a valid session duration.", "warning")
        return redirect(url_for("self_control_page"))

    preset = repo.get_preset(preset_slug) if preset_slug else None
    label = str(request.form.get("label") or "").strip() or str(
        (preset or {}).get("name") or f"{duration}m Focus Session"
    )
    intent_note = str(request.form.get("intent_note") or "").strip()
    strict_mode = bool((preset or {}).get("strict_mode")) or str(
        request.form.get("strict_mode") or ""
    ).strip() in {"1", "true", "on", "yes"}
    selected_categories = request.form.getlist("blocked_categories")
    if preset:
        selected_categories = list(preset.get("blocked_categories") or [])
    elif not selected_categories:
        selected_categories = list(SELF_CONTROL_CATEGORIES)
    blocked_scope = _resolve_block_scope(
        categories=selected_categories,
        explicit_domains=list((preset or {}).get("blocked_domains") or []),
    )
    unlock_requirement = str(request.form.get("unlock_requirement") or "").strip()
    session_id = _create_focus_session(
        {
            "preset_slug": preset_slug,
            "label": label,
            "intent_note": intent_note,
            "strict_mode": strict_mode,
            "planned_minutes": duration,
            "blocked_categories": blocked_scope["categories"],
            "blocked_domains": blocked_scope["domains"],
            "unlock_requirement": unlock_requirement,
            "event_type": "started",
            "event_detail": {
                "preset_slug": preset_slug,
                "strict_mode": strict_mode,
                "planned_minutes": duration,
                "blocked_categories": blocked_scope["categories"],
                "blocked_domains": blocked_scope["domains"],
            },
        },
        started_at=now_et,
    )
    if session_id <= 0:
        flash("Unable to start self-control session.", "warning")
        return redirect(url_for("self_control_page"))
    flash("Self-Control Mode activated.", "success")
    return redirect(url_for("self_control_page"))


def self_control_preset_start(slug: str):
    preset = repo.get_preset(slug)
    if not preset:
        flash("Preset not found.", "warning")
        return redirect(url_for("self_control_page"))
    if str(request.form.get("start_confirmed") or "").strip() != "1":
        flash("Session start requires confirmation.", "warning")
        return redirect(url_for("self_control_page"))
    session = _recover_stale_session()
    if session and session.get("status") in {"active", "awaiting_journal_unlock"}:
        flash("A self-control session is already active.", "warning")
        return redirect(url_for("self_control_page"))

    now_et = app_runtime.now_et()
    blocked_scope = _resolve_block_scope(
        categories=list(preset.get("blocked_categories") or []),
        explicit_domains=list(preset.get("blocked_domains") or []),
    )
    session_id = _create_focus_session(
        {
            "preset_slug": slug,
            "label": str(preset.get("name") or "").strip(),
            "intent_note": str(request.form.get("intent_note") or "").strip(),
            "strict_mode": bool(preset.get("strict_mode")),
            "planned_minutes": int(preset.get("duration_minutes") or 0),
            "blocked_categories": blocked_scope["categories"],
            "blocked_domains": blocked_scope["domains"],
            "unlock_requirement": str(request.form.get("unlock_requirement") or "").strip(),
            "event_type": "started",
            "event_detail": {
                "preset_slug": slug,
                "strict_mode": bool(preset.get("strict_mode")),
                "planned_minutes": int(preset.get("duration_minutes") or 0),
                "blocked_categories": blocked_scope["categories"],
                "blocked_domains": blocked_scope["domains"],
            },
        },
        started_at=now_et,
    )
    if session_id <= 0:
        flash("Unable to start preset session.", "warning")
        return redirect(url_for("self_control_page"))
    flash(f"{preset.get('name')} started.", "success")
    return redirect(url_for("self_control_page"))


def self_control_session_cancel():
    session = _recover_stale_session()
    if not session:
        flash("No active self-control session to cancel.", "warning")
        return redirect(url_for("self_control_page"))
    if session.get("status") != "active":
        flash("Only active sessions can be cancelled.", "warning")
        return redirect(url_for("self_control_page"))
    if session.get("strict_mode"):
        flash("Strict mode sessions cannot be cancelled early.", "warning")
        return redirect(url_for("self_control_page"))
    reason = str(request.form.get("cancel_reason") or "").strip()
    if not reason:
        flash("Cancellation requires a reason.", "warning")
        return redirect(url_for("self_control_page"))
    now_et = app_runtime.now_et()
    repo.update_session(
        int(session["id"]),
        {
            "status": "cancelled",
            "ended_at": now_et.isoformat(),
            "completed_minutes": _completed_minutes(session, now_et),
            "cancel_reason": reason,
            "override_reason": reason,
        },
    )
    repo.log_event(
        int(session["id"]),
        "cancelled_early",
        {"cancel_reason": reason, "completed_minutes": _completed_minutes(session, now_et)},
    )
    _write_enforcement_state_snapshot(None)
    flash("Self-Control session cancelled and logged.", "warning")
    return redirect(url_for("self_control_page"))


def self_control_session_acknowledge_unlock():
    session = _recover_stale_session()
    if not session or session.get("status") != "awaiting_journal_unlock":
        flash("No journal-locked session is waiting for unlock.", "warning")
        return redirect(url_for("self_control_page"))
    if not _journal_unlock_satisfied(session):
        flash("Trading debrief not found yet for this unlock gate.", "warning")
        return redirect(url_for("self_control_page"))
    now_et = app_runtime.now_et()
    repo.update_session(
        int(session["id"]),
        {
            "status": "completed",
            "unlock_satisfied_at": now_et.isoformat(),
        },
    )
    repo.log_event(int(session["id"]), "unlock_satisfied", {"at": now_et.isoformat()})
    _write_enforcement_state_snapshot(None)
    flash("Journal unlock requirement satisfied.", "success")
    return redirect(url_for("self_control_page"))


def self_control_site_add():
    session = _recover_stale_session()
    if session and session.get("strict_mode"):
        flash("Blocked-site edits are locked during strict mode.", "warning")
        return redirect(url_for("self_control_page"))
    raw_domain = str(request.form.get("domain") or "").strip()
    category = str(request.form.get("category") or "Random distractions").strip()
    domain = _normalize_domain(raw_domain)
    if not domain:
        flash("Enter a valid domain to block.", "warning")
        return redirect(url_for("self_control_page"))
    repo.create_blocked_site(domain, category, source="custom")
    flash("Blocked site saved.", "success")
    return redirect(url_for("self_control_page"))


def self_control_site_toggle(site_id: int):
    session = _recover_stale_session()
    if session and session.get("strict_mode"):
        flash("Blocked-site edits are locked during strict mode.", "warning")
        return redirect(url_for("self_control_page"))
    if not repo.get_blocked_site(site_id):
        flash("Blocked site not found.", "warning")
        return redirect(url_for("self_control_page"))
    repo.toggle_blocked_site(site_id)
    flash("Blocked site updated.", "success")
    return redirect(url_for("self_control_page"))


def self_control_site_delete(site_id: int):
    session = _recover_stale_session()
    if session and session.get("strict_mode"):
        flash("Blocked-site edits are locked during strict mode.", "warning")
        return redirect(url_for("self_control_page"))
    site = repo.get_blocked_site(site_id)
    if not site:
        flash("Blocked site not found.", "warning")
        return redirect(url_for("self_control_page"))
    repo.delete_blocked_site(site_id)
    flash("Blocked site removed.", "success")
    return redirect(url_for("self_control_page"))


def self_control_rule_toggle(slug: str):
    rule = repo.get_rule(slug)
    if not rule:
        flash("Rule not found.", "warning")
        return redirect(url_for("self_control_page"))
    repo.toggle_rule(slug)
    flash("Discipline rule updated.", "success")
    return redirect(url_for("self_control_page"))


def self_control_rule_trigger(slug: str):
    session = _recover_stale_session()
    if session and session.get("status") in {"active", "awaiting_journal_unlock"}:
        flash("Finish the current self-control session before triggering another rule.", "warning")
        return redirect(url_for("self_control_page"))
    rule = repo.get_rule(slug)
    if not rule:
        flash("Rule not found.", "warning")
        return redirect(url_for("self_control_page"))
    action = dict(rule.get("action_config") or {})
    now_et = app_runtime.now_et()
    if str(rule.get("slug") or "") == "daily-max-loss-lock":
        threshold = _daily_max_loss_threshold()
        if threshold <= 0:
            flash("Daily max loss threshold is not configured yet.", "warning")
            return redirect(url_for("self_control_page"))
    if str(rule.get("action_type") or "") == "start_preset":
        preset = repo.get_preset(str(action.get("preset_slug") or "").strip())
        if not preset:
            flash("Preset for this rule is unavailable.", "warning")
            return redirect(url_for("self_control_page"))
        duration = int(preset.get("duration_minutes") or 0)
        label = str(preset.get("name") or rule.get("name") or "Rule Trigger").strip()
        strict_mode = bool(preset.get("strict_mode"))
        blocked_scope = _resolve_block_scope(
            categories=list(preset.get("blocked_categories") or []),
            explicit_domains=list(preset.get("blocked_domains") or []),
        )
        unlock_requirement = (
            "trade_debrief_today_after_session_start"
            if rule.get("require_journal_before_unlock")
            else ""
        )
    else:
        duration = int(app_runtime.parse_float(action.get("duration_minutes") or 0) or 0)
        label = str(action.get("label") or rule.get("name") or "Rule Trigger").strip()
        strict_mode = bool(action.get("strict_mode"))
        blocked_scope = _resolve_block_scope(categories=list(SELF_CONTROL_CATEGORIES), explicit_domains=[])
        unlock_requirement = str(action.get("unlock_requirement") or "").strip()
    if duration <= 0:
        flash("This rule is missing a valid duration.", "warning")
        return redirect(url_for("self_control_page"))
    session_id = _create_focus_session(
        {
            "preset_slug": str(action.get("preset_slug") or "").strip(),
            "label": label,
            "intent_note": f"Triggered by rule: {rule.get('name')}",
            "strict_mode": strict_mode,
            "planned_minutes": duration,
            "blocked_categories": blocked_scope["categories"],
            "blocked_domains": blocked_scope["domains"],
            "source_rule_slug": str(rule.get("slug") or "").strip(),
            "unlock_requirement": unlock_requirement,
            "event_type": "rule_triggered",
            "event_detail": {
                "rule_slug": rule.get("slug"),
                "label": label,
                "planned_minutes": duration,
                "strict_mode": strict_mode,
            },
        },
        started_at=now_et,
    )
    if session_id <= 0:
        flash("Unable to trigger discipline rule.", "warning")
        return redirect(url_for("self_control_page"))
    flash(f"{rule.get('name')} triggered.", "success")
    return redirect(url_for("self_control_page"))


def _self_control_page_model(
    *, active_session: Optional[Dict[str, Any]] = None, include_history: bool = True
) -> Dict[str, Any]:
    now_et = app_runtime.now_et()
    active_session = active_session if active_session is not None else _recover_stale_session()
    blocked_sites = repo.list_blocked_sites()
    presets = repo.list_presets()
    rules = repo.list_rules()
    providers = _enrich_provider_cards(repo.list_enforcement_providers(), active_session)
    sessions = repo.list_recent_sessions(30) if include_history else []
    events = repo.list_session_events(60) if include_history else []
    enriched_session = _enrich_session(active_session) if active_session else None
    history = [_enrich_session(row) for row in sessions]
    metrics = _daily_metrics(history, events, now_et)
    trade_signals = _trade_signals(now_et)
    unlock_ready = bool(enriched_session and _journal_unlock_satisfied(enriched_session))
    grouped_sites = _group_sites(blocked_sites)
    preset_cards = [_preset_card(preset, blocked_sites) for preset in presets]
    rule_cards = [_rule_card(rule, trade_signals, enriched_session) for rule in rules]
    provider_cards = [_provider_card(provider) for provider in providers]
    status = _discipline_status(enriched_session, unlock_ready, metrics)
    event_map: Dict[int, List[Dict[str, Any]]] = {}
    for event in events:
        session_id = int(event.get("session_id") or 0)
        if session_id <= 0:
            continue
        event_map.setdefault(session_id, []).append(event)
    for item in history:
        item["events"] = event_map.get(int(item["id"]), [])[:4]
    return {
        "active_session": enriched_session,
        "blocked_site_groups": grouped_sites,
        "presets": preset_cards,
        "rules": rule_cards,
        "providers": provider_cards,
        "history": history,
        "daily_metrics": metrics,
        "trade_signals": trade_signals,
        "self_control_status": status,
        "unlock_ready": unlock_ready,
        "duration_options": list(SELF_CONTROL_DURATIONS),
        "categories": list(SELF_CONTROL_CATEGORIES),
        "now_iso": now_et.isoformat(),
    }


def _recover_stale_session() -> Optional[Dict[str, Any]]:
    session = repo.get_active_session()
    if not session:
        return None
    now_et = app_runtime.now_et()
    if str(session.get("status") or "") == "awaiting_journal_unlock":
        if _journal_unlock_satisfied(session):
            repo.update_session(
                int(session["id"]),
                {
                    "status": "completed",
                    "unlock_satisfied_at": now_et.isoformat(),
                },
            )
            repo.log_event(int(session["id"]), "unlock_satisfied", {"at": now_et.isoformat()})
            _write_enforcement_state_snapshot(None)
            return None
        return session
    if str(session.get("status") or "") != "active":
        return session
    planned_end = _parse_dt(session.get("planned_end_at"))
    if not planned_end or planned_end > now_et:
        return session
    completed_minutes = int(session.get("planned_minutes") or 0)
    unlock_requirement = str(session.get("unlock_requirement") or "").strip()
    next_status = "awaiting_journal_unlock" if unlock_requirement else "completed"
    repo.update_session(
        int(session["id"]),
        {
            "status": next_status,
            "ended_at": now_et.isoformat(),
            "completed_minutes": completed_minutes,
        },
    )
    repo.log_event(
        int(session["id"]),
        "auto_recovered",
        {"status": next_status, "completed_minutes": completed_minutes},
    )
    _write_enforcement_state_snapshot(repo.get_active_session() if next_status == "awaiting_journal_unlock" else None)
    return repo.get_active_session() if next_status == "awaiting_journal_unlock" else None


def _enrich_session(session: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not session:
        return None
    item = dict(session)
    started = _parse_dt(item.get("started_at"))
    planned_end = _parse_dt(item.get("planned_end_at"))
    ended = _parse_dt(item.get("ended_at"))
    now_et = app_runtime.now_et()
    item["started_label"] = _format_et(started)
    item["planned_end_label"] = _format_et(planned_end)
    item["ended_label"] = _format_et(ended)
    item["status_label"] = _status_label(item.get("status"))
    item["status_tone"] = _status_tone(item.get("status"))
    item["strict_label"] = "Strict" if item.get("strict_mode") else "Flexible"
    item["remaining_seconds"] = max(
        0,
        int((planned_end - now_et).total_seconds()) if planned_end and str(item.get("status")) == "active" else 0,
    )
    item["completed_minutes_label"] = str(item.get("completed_minutes") or 0)
    item["blocked_scope_label"] = (
        f"{len(item.get('blocked_domains') or [])} sites across {len(item.get('blocked_categories') or [])} groups"
    )
    item["requires_journal_unlock"] = (
        str(item.get("unlock_requirement") or "").strip() == "trade_debrief_today_after_session_start"
    )
    return item


def _group_sites(sites: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {name: [] for name in SELF_CONTROL_CATEGORIES}
    for site in sites:
        category = str(site.get("category") or "Random distractions").strip()
        buckets.setdefault(category, []).append(site)
    return [
        {
            "name": category,
            "items": sorted(items, key=lambda item: (0 if item.get("enabled") else 1, str(item.get("domain") or ""))),
            "enabled_count": sum(1 for item in items if item.get("enabled")),
        }
        for category, items in buckets.items()
    ]


def _resolve_block_scope(categories: List[str], explicit_domains: List[str]) -> Dict[str, List[str]]:
    active_sites = [site for site in repo.list_blocked_sites() if int(site.get("enabled") or 0) == 1]
    normalized_categories = [str(item or "").strip() for item in categories if str(item or "").strip()]
    domain_set = {
        str(site.get("domain") or "").strip().lower()
        for site in active_sites
        if str(site.get("category") or "").strip() in normalized_categories
    }
    for domain in explicit_domains or []:
        normalized = _normalize_domain(domain)
        if normalized:
            domain_set.add(normalized)
    return {
        "categories": normalized_categories,
        "domains": sorted(domain_set),
    }


def _preset_card(preset: Dict[str, Any], sites: List[Dict[str, Any]]) -> Dict[str, Any]:
    item = dict(preset)
    scope = _resolve_block_scope(
        categories=list(item.get("blocked_categories") or []),
        explicit_domains=list(item.get("blocked_domains") or []),
    )
    item["scope"] = scope
    item["scope_preview"] = ", ".join(scope["domains"][:4]) or "No sites"
    item["duration_label"] = f"{int(item.get('duration_minutes') or 0)}m"
    return item


def _rule_card(
    rule: Dict[str, Any], trade_signals: Dict[str, Any], session: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    item = dict(rule)
    slug = str(item.get("slug") or "")
    signal = "Manual trigger ready."
    tone = "info"
    if slug == "post-loss-cooldown":
        if trade_signals["latest_trade_loss"]:
            signal = "Latest trade is a loss."
            tone = "negative"
    elif slug == "daily-max-loss-lock":
        if trade_signals["daily_max_loss_hit"]:
            signal = "Daily max loss threshold is hit."
            tone = "negative"
        elif trade_signals["daily_max_loss_threshold"] <= 0:
            signal = "Daily max loss threshold not configured."
            tone = "warning"
    elif slug == "midday-reset-after-trade-count":
        if trade_signals["today_trade_count"] >= int(item.get("trigger_config", {}).get("trade_count") or 0):
            signal = "Trade-count threshold reached."
            tone = "warning"
    elif slug == "prevent-immediate-reentry":
        signal = "Manual stop-out cooldown trigger."
    elif slug == "require-debrief-before-reenable":
        signal = (
            "Current unlock gate is waiting on debrief."
            if session and session.get("unlock_requirement")
            else "Use this to force a debrief gate."
        )
    item["signal"] = signal
    item["signal_tone"] = tone
    return item


def _provider_card(provider: Dict[str, Any]) -> Dict[str, Any]:
    item = dict(provider)
    tone_map = {
        "not_connected": "warning",
        "available": "positive",
        "active": "positive",
        "degraded": "warning",
        "error": "negative",
    }
    item["tone"] = tone_map.get(str(item.get("status") or ""), "info")
    item["status_label"] = str(item.get("status") or "not_connected").replace("_", " ").title()
    return item


def _enrich_provider_cards(
    providers: List[Dict[str, Any]], active_session: Optional[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    status_payload = _read_enforcement_status()
    helper_active = bool(status_payload.get("active"))
    helper_connected = bool(status_payload.get("installed") or status_payload.get("last_checked_at"))
    managed_mode = str(status_payload.get("mode") or "").strip().lower()
    last_error = str(status_payload.get("last_error") or "").strip()
    out: List[Dict[str, Any]] = []
    for provider in providers:
        item = dict(provider)
        provider_type = str(item.get("provider_type") or "").strip()
        if provider_type == "local_helper":
            if last_error:
                item["status"] = "error"
            elif helper_active:
                item["status"] = "active"
            elif helper_connected:
                item["status"] = "available"
            else:
                item["status"] = "not_connected"
        elif provider_type == "os_blocker":
            if last_error:
                item["status"] = "error"
            elif managed_mode == "hosts" and helper_active:
                item["status"] = "active"
            elif managed_mode == "hosts" and helper_connected:
                item["status"] = "available"
            else:
                item["status"] = "not_connected"
        out.append(_provider_card(item))
    return out


def _create_focus_session(payload: Dict[str, Any], *, started_at: Optional[datetime] = None) -> int:
    start = started_at or app_runtime.now_et()
    planned_minutes = int(payload.get("planned_minutes") or 0)
    if planned_minutes <= 0:
        return 0
    session_id = repo.create_session(
        {
            "preset_slug": str(payload.get("preset_slug") or "").strip(),
            "label": str(payload.get("label") or "").strip(),
            "intent_note": str(payload.get("intent_note") or "").strip(),
            "status": "active",
            "strict_mode": bool(payload.get("strict_mode")),
            "started_at": start.isoformat(),
            "planned_end_at": (start + timedelta(minutes=planned_minutes)).isoformat(),
            "planned_minutes": planned_minutes,
            "completed_minutes": 0,
            "blocked_categories": list(payload.get("blocked_categories") or []),
            "blocked_domains": list(payload.get("blocked_domains") or []),
            "source_rule_slug": str(payload.get("source_rule_slug") or "").strip(),
            "unlock_requirement": str(payload.get("unlock_requirement") or "").strip(),
        }
    )
    event_type = str(payload.get("event_type") or "").strip()
    if session_id > 0 and event_type:
        repo.log_event(session_id, event_type, dict(payload.get("event_detail") or {}))
        _write_enforcement_state_snapshot(repo.get_session(session_id))
    return session_id


def _write_enforcement_state_snapshot(session: Optional[Dict[str, Any]]) -> None:
    app_runtime.ensure_storage_dirs()
    os.makedirs(os.path.dirname(SELF_CONTROL_STATE_PATH) or ".", exist_ok=True)
    active = bool(session and str(session.get("status") or "") in {"active", "awaiting_journal_unlock"})
    blocked_categories = list((session or {}).get("blocked_categories") or [])
    blocked_domains = list((session or {}).get("blocked_domains") or [])
    if active:
        blocked_scope = _resolve_block_scope(
            categories=blocked_categories,
            explicit_domains=blocked_domains,
        )
        blocked_categories = blocked_scope["categories"]
        blocked_domains = blocked_scope["domains"]
    payload = {
        "updated_at": app_runtime.now_et().isoformat(),
        "active": active,
        "status": str((session or {}).get("status") or "idle"),
        "session_id": int((session or {}).get("id") or 0),
        "label": str((session or {}).get("label") or "").strip(),
        "strict_mode": bool((session or {}).get("strict_mode")),
        "planned_end_at": str((session or {}).get("planned_end_at") or "").strip(),
        "unlock_requirement": str((session or {}).get("unlock_requirement") or "").strip(),
        "blocked_domains": blocked_domains,
        "blocked_categories": blocked_categories,
        "provider_mode": "hosts",
    }
    tmp_path = f"{SELF_CONTROL_STATE_PATH}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    os.replace(tmp_path, SELF_CONTROL_STATE_PATH)


def _read_enforcement_status() -> Dict[str, Any]:
    try:
        with open(SELF_CONTROL_STATUS_PATH, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _daily_metrics(
    sessions: List[Dict[str, Any]], events: List[Dict[str, Any]], now_et: datetime
) -> Dict[str, Any]:
    today = now_et.date().isoformat()
    sessions_today = [item for item in sessions if _session_day(item) == today]
    completed_today = [item for item in sessions_today if str(item.get("status") or "") == "completed"]
    focus_minutes_today = sum(int(item.get("completed_minutes") or 0) for item in sessions_today)
    if any(str(item.get("status") or "") == "active" for item in sessions_today):
        focus_minutes_today += 0
    recent = [item for item in sessions if _days_old(_session_day(item), today) <= 14]
    started_recent = len(recent)
    completed_recent = sum(1 for item in recent if str(item.get("status") or "") == "completed")
    last_broken = next(
        (
            item
            for item in sessions
            if str(item.get("status") or "") == "cancelled"
            or str(item.get("override_reason") or "").strip()
        ),
        None,
    )
    streak = _completed_streak(sessions, today)
    last_broken_event = next(
        (
            event
            for event in events
            if str(event.get("event_type") or "") in {"cancelled_early", "override"}
        ),
        None,
    )
    return {
        "sessions_completed_today": len(completed_today),
        "focus_minutes_today": focus_minutes_today,
        "focus_time_today_label": _minutes_label(focus_minutes_today),
        "current_streak": streak,
        "last_broken_session_label": (
            f"{last_broken.get('label')} · {last_broken.get('started_label')}"
            if last_broken
            else (
                _format_et(_parse_dt(last_broken_event.get("event_at"))) if last_broken_event else "None"
            )
        ),
        "compliance_rate": int(round((completed_recent / started_recent) * 100.0)) if started_recent else 100,
    }


def _completed_streak(sessions: List[Dict[str, Any]], today_iso: str) -> int:
    day_map: Dict[str, Dict[str, int]] = {}
    for item in sessions:
        day = _session_day(item)
        if not day:
            continue
        bucket = day_map.setdefault(day, {"completed": 0, "cancelled": 0})
        if str(item.get("status") or "") == "completed":
            bucket["completed"] += 1
        if str(item.get("status") or "") == "cancelled":
            bucket["cancelled"] += 1
    cursor = date.fromisoformat(today_iso)
    streak = 0
    while True:
        if cursor.weekday() >= 5:
            cursor -= timedelta(days=1)
            continue
        bucket = day_map.get(cursor.isoformat())
        if not bucket or bucket["completed"] <= 0 or bucket["cancelled"] > 0:
            break
        streak += 1
        cursor -= timedelta(days=1)
    return streak


def _trade_signals(now_et: datetime) -> Dict[str, Any]:
    day = now_et.date().isoformat()
    rows = [dict(r) for r in trades_repo.fetch_trades(d=day, q="")]
    stats = trades_repo.trade_day_stats(rows)
    threshold = _daily_max_loss_threshold()
    latest = rows[0] if rows else {}
    latest_pl = float(latest.get("net_pl") or 0.0) if latest else 0.0
    today_net = float(stats.get("total") or 0.0)
    return {
        "today_trade_count": len(rows),
        "today_net": today_net,
        "latest_trade_loss": bool(rows and latest_pl < 0),
        "daily_max_loss_threshold": threshold,
        "daily_max_loss_hit": bool(threshold > 0 and today_net <= (-1.0 * threshold)),
    }


def _daily_max_loss_threshold() -> float:
    with db() as conn:
        row = conn.execute(
            "SELECT daily_max_loss FROM risk_controls WHERE id = 1 LIMIT 1"
        ).fetchone()
    return float((row["daily_max_loss"] if row else 0.0) or 0.0)


def _journal_unlock_satisfied(session: Dict[str, Any]) -> bool:
    if str(session.get("unlock_requirement") or "").strip() != "trade_debrief_today_after_session_start":
        return True
    started = _parse_dt(session.get("started_at"))
    if not started:
        return False
    rows = [dict(r) for r in journal_repo.fetch_entries_by_type("trade_debrief", d=started.date().isoformat())]
    for row in rows:
        updated = _parse_dt(row.get("updated_at")) or _parse_dt(row.get("created_at"))
        if updated and updated >= started:
            return True
    return False


def _discipline_status(
    session: Optional[Dict[str, Any]], unlock_ready: bool, metrics: Dict[str, Any]
) -> Dict[str, Any]:
    if session and str(session.get("status") or "") == "awaiting_journal_unlock":
        return {
            "title": "Journal Unlock Required",
            "detail": "Cooldown completed. Trading remains locked until the debrief is logged and acknowledged.",
            "tone": "negative",
            "chip": "Awaiting Debrief",
        }
    if session and str(session.get("status") or "") == "active":
        return {
            "title": "Discipline Mode Active",
            "detail": "App-enforced discipline mode is live. External browser/system blocking integration coming next.",
            "tone": "positive",
            "chip": "Focus Locked",
        }
    return {
        "title": "No Active Lock",
        "detail": (
            f"{metrics.get('sessions_completed_today', 0)} sessions completed today. "
            "Use a preset or custom session to re-enter discipline mode."
        ),
        "tone": "info",
        "chip": "Ready",
    }


def _status_label(status: Any) -> str:
    return str(status or "idle").replace("_", " ").title()


def _status_tone(status: Any) -> str:
    raw = str(status or "")
    if raw == "completed":
        return "positive"
    if raw in {"cancelled", "awaiting_journal_unlock"}:
        return "negative"
    if raw == "active":
        return "positive"
    return "info"


def _format_et(value: Optional[datetime]) -> str:
    if not value:
        return "—"
    return value.astimezone(app_runtime.TZ).strftime("%b %d %I:%M %p ET")


def _parse_dt(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=app_runtime.TZ)
    return dt.astimezone(app_runtime.TZ)


def _normalize_domain(raw: str) -> str:
    text = str(raw or "").strip().lower()
    if not text:
        return ""
    if "://" in text:
        parsed = urlparse(text)
        text = parsed.netloc or parsed.path
    text = text.split("/")[0].split("?")[0].split("#")[0].strip()
    text = text.lstrip(".")
    if text.startswith("www."):
        text = text[4:]
    if not text or "." not in text or " " in text:
        return ""
    return text


def _completed_minutes(session: Dict[str, Any], now_et: datetime) -> int:
    started = _parse_dt(session.get("started_at"))
    if not started:
        return 0
    elapsed = max(0, int((now_et - started).total_seconds() // 60))
    return min(int(session.get("planned_minutes") or 0), elapsed)


def _session_day(session: Dict[str, Any]) -> str:
    started = _parse_dt(session.get("started_at"))
    return started.date().isoformat() if started else ""


def _minutes_label(minutes: int) -> str:
    total = max(0, int(minutes or 0))
    hours = total // 60
    rem = total % 60
    if hours and rem:
        return f"{hours}h {rem}m"
    if hours:
        return f"{hours}h"
    return f"{rem}m"


def _days_old(day_iso: str, today_iso: str) -> int:
    if not day_iso:
        return 999
    try:
        day_value = date.fromisoformat(day_iso)
        today_value = date.fromisoformat(today_iso)
    except ValueError:
        return 999
    return max(0, (today_value - day_value).days)
