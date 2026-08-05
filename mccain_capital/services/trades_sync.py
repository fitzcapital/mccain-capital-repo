"""Live sync and auto-sync orchestration for trades."""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import flash, jsonify, redirect, request, url_for

from mccain_capital.services import trades as legacy
from mccain_capital.services import trades_ops
from mccain_capital.services.job_presenters import job_response_payload

_AUTO_SYNC_THREAD_STARTED = False
_AUTO_SYNC_THREAD_LOCK = threading.Lock()
_AUTO_BACKUP_THREAD_STARTED = False
_AUTO_BACKUP_THREAD_LOCK = threading.Lock()


def _wants_async_json() -> bool:
    return (request.args.get("async") or "").strip() == "1" or (
        "application/json" in str(request.headers.get("Accept") or "").lower()
    )


def _json_error(message: str, *, status_code: int = 400):
    return jsonify({"ok": False, "message": str(message or "Request failed.")}), status_code


def prepare_sync_runtime_state() -> None:
    legacy._reconcile_sync_runtime_state()


def _active_sync_conflict():
    active_job = legacy._latest_active_sync_job()
    if not active_job:
        return None
    message = (
        f"Another sync is already active: {active_job.get('title') or 'Sync'} "
        f"({str(active_job.get('stage') or 'start').replace('_', ' ')}). "
        "Cancel it, or use Force Reset Lane if the broker login is hung."
    )
    payload = {
        "ok": False,
        "message": message,
        "job": job_response_payload(active_job, humanize_timestamp=legacy._humanize_et_timestamp),
    }
    return active_job, payload


def _human_sync_stage(stage: str) -> str:
    label = str(stage or "").strip().replace("_", " ")
    return label.title() if label else "Standby"


def _parse_et_date(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    except Exception:
        return ""


def _format_last_sync_at_et(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(
            ZoneInfo("America/New_York")
        )
        hour = dt.strftime("%I").lstrip("0") or "0"
        return f"{dt.strftime('%b %d, %Y')} · {hour}{dt.strftime(':%M %p ET')}"
    except Exception:
        return legacy._humanize_et_timestamp(text)


def _broker_credential_context(username: str = "", cfg=None):
    loaded_cfg = dict(cfg or legacy._load_broker_sync_config() or {})
    resolved_username = str(username or loaded_cfg.get("username") or "").strip()
    if resolved_username:
        loaded_cfg["username"] = resolved_username
    stored_password = legacy._get_auto_sync_password(loaded_cfg) if resolved_username else ""
    return loaded_cfg, resolved_username, stored_password


def _credential_save_result(*, remember_credentials: bool, username: str, password: str, cfg):
    if not remember_credentials:
        return {
            "requested": False,
            "status": "not_requested",
            "detail": "Credential save was not requested on the last run.",
        }
    if not username:
        return {
            "requested": True,
            "status": "failed",
            "detail": "Username is missing, so reusable credentials could not be saved.",
        }
    if not password:
        _, _, stored_password = _broker_credential_context(username=username, cfg=cfg)
        if stored_password:
            return {
                "requested": True,
                "status": "reused",
                "detail": "Saved credentials are already reusable for dashboard quick run.",
            }
        return {
            "requested": True,
            "status": "failed",
            "detail": "No password was provided and no reusable saved credential is available.",
        }
    if str(cfg.get("password_enc") or "").strip():
        return {
            "requested": True,
            "status": "fallback_saved",
            "detail": "Credentials saved in encrypted fallback storage and ready for dashboard quick run.",
        }
    _, _, stored_password = _broker_credential_context(username=username, cfg=cfg)
    if stored_password:
        return {
            "requested": True,
            "status": "saved",
            "detail": "Credentials saved securely and ready for dashboard quick run.",
        }
    return {
        "requested": True,
        "status": "failed",
        "detail": "Credential save did not complete. Re-enter the password and run again.",
    }


_ACTIVE_SYNC_STATUSES = {"queued", "running"}
_FAILED_SYNC_STATUSES = {"failed", "error", "blocked"}
_RECOVERY_SYNC_STAGES = {
    "auth_required",
    "capture_statement_html",
    "reset_required",
    "stale",
    "storage_io",
    "system_resource",
}


def _selected_broker_account(selected_account: dict | None) -> str:
    account = selected_account or {}
    return legacy.repo.normalize_broker_account_id(
        account.get("display_broker_account_id")
        or legacy.repo.display_broker_account_id(account.get("broker_account_id"))
        or account.get("broker_account_id")
        or ""
    )


def _masked_broker_account(account: str) -> str:
    normalized = legacy.repo.normalize_broker_account_id(account)
    if len(normalized) <= 4:
        return normalized or "Not configured"
    return f"••••{normalized[-4:]}"


def _sync_outcome(payload: dict, *, active: bool = False) -> str:
    status = str(payload.get("status") or "idle").strip().lower() or "idle"
    stage = str(payload.get("stage") or "").strip().lower()
    requested = payload.get("requested") if isinstance(payload.get("requested"), dict) else {}
    if active or status in _ACTIVE_SYNC_STATUSES:
        return "running"
    if status == "debug_only" or bool(requested.get("debug_only")) and status == "success":
        return "diagnostic_only"
    if status == "success":
        inserted = payload.get("inserted")
        if inserted is None and isinstance(payload.get("summary"), dict):
            inserted = payload["summary"].get("inserted")
        return "no_new_trades" if inserted == 0 else "completed"
    if status == "cancelled":
        return "cancelled"
    if status == "stale" or stage in _RECOVERY_SYNC_STAGES:
        return "needs_recovery"
    if status in _FAILED_SYNC_STATUSES:
        return "failed"
    if status in {"", "idle", "ready"}:
        return "ready"
    return "needs_recovery"


def _next_auto_run(auto_cfg: dict, *, now_et: datetime) -> str:
    if not bool(auto_cfg.get("enabled")):
        return ""
    raw_time = str(auto_cfg.get("run_time_et") or "16:15").strip()
    try:
        hour, minute = [int(part) for part in raw_time.split(":", 1)]
    except (TypeError, ValueError):
        hour, minute = 16, 15
    candidate = now_et.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now_et:
        candidate += timedelta(days=1)
    if not bool(auto_cfg.get("run_weekends")):
        while candidate.weekday() >= 5:
            candidate += timedelta(days=1)
    return candidate.isoformat()


def _last_successful_import(last_status: dict, history: list[dict]) -> dict:
    if _sync_outcome(last_status) in {"completed", "no_new_trades"}:
        return last_status
    for row in reversed(history):
        requested = row.get("requested") if isinstance(row.get("requested"), dict) else {}
        status = str(row.get("status") or "").strip().lower()
        if status == "success" and not bool(requested.get("debug_only")):
            return row
    return {}


def _build_sync_today_preflight(
    *,
    selected_account: dict | None = None,
    cfg: dict | None = None,
    requested: dict | None = None,
    active_job: dict | None = None,
    today_et: str | None = None,
) -> dict:
    selected = selected_account if selected_account is not None else legacy._selected_account()
    saved_request = requested or {}
    loaded_cfg, username, stored_password = _broker_credential_context(
        username=str(saved_request.get("username") or ""),
        cfg=cfg,
    )
    account = _selected_broker_account(selected)
    disabled_reason = ""
    if active_job:
        disabled_reason = "A live sync is already running."
    elif not selected:
        disabled_reason = "Select a local account ledger before syncing."
    elif not account:
        disabled_reason = "The selected ledger needs a broker account ID before syncing."
    elif not username or not stored_password:
        disabled_reason = "Saved live-sync credentials are missing."
    elif not str(loaded_cfg.get("base_url") or "").strip():
        disabled_reason = "The broker base URL is missing."
    day = today_et or legacy.today_iso()
    return {
        "selected_account_id": int(selected["id"]) if selected else None,
        "ledger_name": str((selected or {}).get("account_name") or "No ledger selected"),
        "broker_account": account,
        "broker_account_masked": _masked_broker_account(account),
        "from_date": day,
        "to_date": day,
        "mode": "broker",
        "mode_label": "Broker fills → trades",
        "debug_only": False,
        "intent_label": "Normal import",
        "username": username,
        "credentials_ready": bool(username and stored_password),
        "base_url": str(loaded_cfg.get("base_url") or "").strip(),
        "can_run": not bool(disabled_reason),
        "disabled_reason": disabled_reason,
    }


def _canonical_live_sync_state(
    *,
    last_status: dict,
    active_job: dict,
    history: list[dict],
    auto_cfg: dict,
    preflight: dict,
    now_et: datetime | None = None,
) -> dict:
    now = now_et or datetime.now(ZoneInfo("America/New_York"))
    today = now.date().isoformat()
    effective = active_job or last_status
    outcome = _sync_outcome(effective, active=bool(active_job))
    last_success = _last_successful_import(last_status, history)
    attempt_at = str(effective.get("updated_at") or last_status.get("updated_at") or "").strip()
    success_at = str(last_success.get("updated_at") or "").strip()
    attempted_today = _parse_et_date(attempt_at) == today
    import_completed_today = _parse_et_date(success_at) == today
    labels = {
        "ready": ("READY", "Ready for today's import", "ready"),
        "running": ("SYNC RUNNING", "Live broker import in progress", "running"),
        "completed": ("IMPORT COMPLETE", "Trades imported successfully", "success"),
        "no_new_trades": ("NO NEW TRADES", "Import completed; no new fills found", "success"),
        "diagnostic_only": (
            "DIAGNOSTIC COMPLETE",
            "Test finished; nothing was imported",
            "warning",
        ),
        "cancelled": ("SYNC CANCELLED", "Today's import is still pending", "warning"),
        "failed": ("SYNC FAILED", "Today's import is still pending", "warning"),
        "needs_recovery": (
            "RECOVERY NEEDED",
            "Review the failure evidence before retrying",
            "warning",
        ),
    }
    state_label, current_label, tone = labels[outcome]
    if outcome == "ready" and not preflight.get("can_run"):
        current_label = str(preflight.get("disabled_reason") or "Sync setup is incomplete.")
        tone = "warning"
    message = str(effective.get("message") or last_status.get("message") or "").strip()
    if outcome == "running":
        detail = message or "Running now. Controls unlock when complete."
    elif import_completed_today:
        detail = "Today's broker import is complete."
    elif outcome in {"cancelled", "failed", "needs_recovery", "diagnostic_only"}:
        detail = message or current_label
    else:
        detail = "Today's broker import is pending."
    return {
        "outcome": outcome,
        "status": str(effective.get("status") or "idle").strip().lower() or "idle",
        "stage": str(effective.get("stage") or "").strip().lower(),
        "state_label": state_label,
        "current_sync_label": current_label,
        "tone": tone,
        "detail": detail,
        "message": message,
        "attempted_today": attempted_today,
        "import_completed_today": import_completed_today,
        "last_attempt_at": attempt_at,
        "last_attempt_at_et": _format_last_sync_at_et(attempt_at),
        "last_successful_import_at": success_at,
        "last_successful_import_at_et": _format_last_sync_at_et(success_at),
        "today_status": (
            "running"
            if outcome == "running"
            else (
                "completed"
                if import_completed_today
                else (
                    "failed"
                    if attempted_today and outcome in {"failed", "needs_recovery"}
                    else (
                        "cancelled"
                        if attempted_today and outcome == "cancelled"
                        else (
                            "diagnostic"
                            if attempted_today and outcome == "diagnostic_only"
                            else "pending"
                        )
                    )
                )
            )
        ),
        "recommended_next_action": (
            "Wait for the active sync to finish."
            if outcome == "running"
            else (
                "No manual sync is needed today."
                if import_completed_today
                else (
                    "Review recovery guidance before retrying."
                    if outcome == "needs_recovery"
                    else "Run today's normal import when ready."
                )
            )
        ),
        "automation_enabled": bool(auto_cfg.get("enabled")),
        "automation_next_run_at": _next_auto_run(auto_cfg, now_et=now),
        "automation_run_time_et": str(auto_cfg.get("run_time_et") or "16:15"),
        "preflight": preflight,
    }


def dashboard_live_sync_state() -> dict:
    last_status = legacy._load_last_sync_status() or {}
    active_job = legacy._latest_active_sync_job() or {}
    active_job_payload = (
        job_response_payload(active_job, humanize_timestamp=legacy._humanize_et_timestamp)
        if active_job
        else {}
    )
    requested = (
        last_status.get("requested") if isinstance(last_status.get("requested"), dict) else {}
    )
    cfg = legacy._load_broker_sync_config() or {}
    selected_account = legacy._selected_account()
    preflight = _build_sync_today_preflight(
        selected_account=selected_account,
        cfg=cfg,
        requested=requested,
        active_job=active_job_payload,
    )
    canonical = _canonical_live_sync_state(
        last_status=last_status,
        active_job=active_job_payload,
        history=legacy._load_sync_history(),
        auto_cfg=legacy._load_auto_sync_config(),
        preflight=preflight,
    )
    has_last_request = bool(requested)
    credentials_ready = bool(preflight["credentials_ready"])
    credential_save_status = str(requested.get("credential_save_status") or "").strip().lower()
    credential_save_detail = str(requested.get("credential_save_detail") or "").strip()
    active_job_id = str(active_job_payload.get("id") or "").strip()
    disabled_reason = str(preflight["disabled_reason"] or "")

    return {
        **canonical,
        "updated_human": canonical["last_attempt_at_et"],
        "updated_at_raw": canonical["last_attempt_at"],
        "last_sync_at_et": canonical["last_attempt_at_et"],
        "sync_ran_today": canonical["attempted_today"],
        "sync_status_today": canonical["today_status"],
        "has_last_request": has_last_request,
        "credentials_ready": credentials_ready,
        "credential_save_status": credential_save_status,
        "credential_save_detail": credential_save_detail,
        "can_run": bool(preflight["can_run"]),
        "disabled_reason": disabled_reason,
        "active_job_id": active_job_id,
        "active_job": active_job_payload,
        "last_requested": requested,
        "run_endpoint": url_for("trades_sync_live_last_run"),
    }


def live_sync_monitoring_state() -> dict:
    """Return a passive, redacted projection of the canonical Live Sync state."""
    state = dashboard_live_sync_state()
    return {
        "outcome": state["outcome"],
        "stage": state["stage"],
        "attempted_today": state["attempted_today"],
        "import_completed_today": state["import_completed_today"],
        "last_attempt_at": state["last_attempt_at"],
        "last_successful_import_at": state["last_successful_import_at"],
        "automation_enabled": state["automation_enabled"],
        "automation_next_run_at": state["automation_next_run_at"],
    }


def trades_sync_live():
    if request.method != "POST":
        return redirect(url_for("trades_upload_pdf"))
    conflict = _active_sync_conflict()
    if conflict:
        active_job, payload = conflict
        if _wants_async_json():
            return jsonify(payload), 409
        flash(payload["message"], "warn")
        return redirect(url_for("trades_upload_pdf", ws="live", job=active_job["id"]))

    mode = (request.form.get("mode") or "broker").strip()
    guardrail = legacy.trade_lockout_state(legacy.today_iso())
    if guardrail["locked"] and mode == "broker":
        message = (
            f"Daily max-loss guardrail is active for {guardrail['day']}. "
            f"Day net {legacy.money(guardrail['day_net'])} reached limit "
            f"{legacy.money(guardrail['daily_max_loss'])}."
        )
        if _wants_async_json():
            return _json_error(message, status_code=409)
        return legacy.render_page(
            legacy.simple_msg(message),
            active="trades",
        )

    cfg = legacy._load_broker_sync_config()
    remembered_username = str(cfg.get("username") or "").strip()
    username = (request.form.get("username") or "").strip() or remembered_username
    password = (request.form.get("password") or "").strip()
    remember_credentials = request.form.get("remember_credentials") == "1"
    clear_saved_credentials = request.form.get("clear_saved_credentials") == "1"
    base_url = (request.form.get("base_url") or "").strip() or cfg.get("base_url", "")
    account = (request.form.get("account") or "").strip() or cfg.get("account", "")
    wl = (request.form.get("wl") or "").strip() or cfg.get("wl", "vanquishtrader")
    time_zone = (request.form.get("time_zone") or "").strip() or cfg.get(
        "time_zone", "America/New_York"
    )
    date_locale = (request.form.get("date_locale") or "").strip() or cfg.get("date_locale", "en-US")
    report_locale = (request.form.get("report_locale") or "").strip() or cfg.get(
        "report_locale", "en"
    )
    headless = request.form.get("headless") == "1"
    debug_capture = request.form.get("debug_capture") == "1"
    debug_only = request.form.get("debug_only") == "1"
    remember_connection = request.form.get("remember_connection") == "1"
    _, _, stored_password = _broker_credential_context(username=username, cfg=cfg)

    if clear_saved_credentials and username:
        legacy._clear_auto_sync_password(username)
        cfg["username"] = ""
        cfg["password"] = ""
        cfg["password_enc"] = ""
        cfg["password_stored"] = False
        legacy._save_broker_sync_config(cfg)
        flash("Saved live sync credentials cleared.", "success")
        return redirect(url_for("trades_upload_pdf", ws="live"))

    password_for_run = password or stored_password

    if not username or not password_for_run:
        message = "Username and password are required for live login sync."
        if _wants_async_json():
            return _json_error(message)
        return legacy.render_page(
            legacy.simple_msg(message),
            active="trades",
        )
    if not base_url or not account:
        message = "Base origin and account are required for live login sync."
        if _wants_async_json():
            return _json_error(message)
        return legacy.render_page(
            legacy.simple_msg(message),
            active="trades",
        )

    from_date = legacy._normalize_iso_date(request.form.get("from_date") or "", legacy.today_iso())
    to_date = legacy._normalize_iso_date(request.form.get("to_date") or "", legacy.today_iso())
    if from_date > to_date:
        from_date, to_date = to_date, from_date
    selected_account = legacy._sync_account(request.form.get("selected_account_id"), account)
    selected_broker_account = _selected_broker_account(selected_account)
    requested_broker_account = legacy.repo.normalize_broker_account_id(account)
    if not selected_account:
        message = "Select a local account ledger before syncing."
        if _wants_async_json():
            return _json_error(message, status_code=409)
        flash(message, "warn")
        return redirect(url_for("trades_upload_pdf", ws="live"))
    if not selected_broker_account:
        message = "The selected ledger needs a broker account ID before syncing."
        if _wants_async_json():
            return _json_error(message, status_code=409)
        flash(message, "warn")
        return redirect(url_for("trades_upload_pdf", ws="live"))
    if requested_broker_account and requested_broker_account != selected_broker_account:
        message = "Selected ledger and broker account do not match. Update the account selection."
        if _wants_async_json():
            return _json_error(message, status_code=409)
        flash(message, "warn")
        return redirect(url_for("trades_upload_pdf", ws="live"))
    account = selected_broker_account

    requested = legacy._sync_requested_payload(
        source="manual_live",
        mode=mode,
        from_date=from_date,
        to_date=to_date,
        base_url=base_url,
        account=account,
        wl=wl,
        time_zone=time_zone,
        date_locale=date_locale,
        report_locale=report_locale,
        headless=headless,
        debug_capture=debug_capture,
        debug_only=debug_only,
        username=username,
    )
    requested["remember_connection"] = remember_connection
    requested["remember_credentials"] = remember_credentials
    requested["stored_password_reused"] = bool(not password and stored_password)

    if remember_connection or remember_credentials:
        cfg.update(
            {
                "base_url": base_url,
                "wl": wl,
                "account": account,
                "time_zone": time_zone,
                "date_locale": date_locale,
                "report_locale": report_locale,
                "username": username if remember_credentials else remembered_username,
            }
        )
        if remember_credentials and password:
            saved = legacy._set_auto_sync_password(username, password)
            if not saved and legacy.AUTO_SYNC_PASSWORD_FALLBACK:
                cfg["password_enc"] = legacy._encrypt_fallback_password(password)
                cfg["password"] = ""
                cfg["password_stored"] = True
            else:
                cfg.pop("password_enc", None)
                cfg["password_stored"] = saved
        legacy._save_broker_sync_config(cfg)
    credential_result = _credential_save_result(
        remember_credentials=remember_credentials,
        username=username,
        password=password,
        cfg=cfg,
    )
    requested["credential_save_requested"] = credential_result["requested"]
    requested["credential_save_status"] = credential_result["status"]
    requested["credential_save_detail"] = credential_result["detail"]
    job = legacy._start_sync_job(
        selected_account_id=(int(selected_account["id"]) if selected_account else None),
        title="Live Sync",
        source_label="LIVE LOGIN HTML",
        record_source="LIVE LOGIN HTML",
        mode=mode,
        username=username,
        password=password_for_run,
        base_url=base_url,
        account=account,
        wl=wl,
        time_zone=time_zone,
        date_locale=date_locale,
        report_locale=report_locale,
        from_date=from_date,
        to_date=to_date,
        headless=headless,
        debug_capture=debug_capture,
        debug_only=debug_only,
        requested=requested,
    )
    job_payload = job_response_payload(job, humanize_timestamp=legacy._humanize_et_timestamp)
    if str(job.get("status") or "").strip().lower() == "failed":
        message = str(job.get("message") or "Live sync could not start.")
        if _wants_async_json():
            return jsonify({"ok": False, "message": message, "job": job_payload}), 503
        flash(message, "warn")
        return redirect(url_for("trades_upload_pdf", ws="live", job=job["id"]))
    if _wants_async_json():
        return jsonify({"ok": True, "job": job_payload})
    flash("Live sync started. Progress and result will update below.", "success")
    return redirect(url_for("trades_upload_pdf", ws="live", job=job["id"]))


def trades_sync_live_last_run():
    if request.method != "POST":
        return redirect(url_for("dashboard"))
    conflict = _active_sync_conflict()
    if conflict:
        active_job, payload = conflict
        if _wants_async_json():
            return jsonify(payload), 409
        flash(payload["message"], "warn")
        return redirect(url_for("dashboard"))

    last_status = legacy._load_last_sync_status() or {}
    requested_last = (
        last_status.get("requested") if isinstance(last_status.get("requested"), dict) else {}
    )
    cfg = legacy._load_broker_sync_config() or {}
    mode = "broker"
    guardrail = legacy.trade_lockout_state(legacy.today_iso())
    if guardrail["locked"] and mode == "broker":
        message = (
            f"Daily max-loss guardrail is active for {guardrail['day']}. "
            f"Day net {legacy.money(guardrail['day_net'])} reached limit "
            f"{legacy.money(guardrail['daily_max_loss'])}."
        )
        if _wants_async_json():
            return _json_error(message, status_code=409)
        flash(message, "warn")
        return redirect(url_for("dashboard"))

    cfg, username, stored_password = _broker_credential_context(
        username=str(requested_last.get("username") or ""),
        cfg=cfg,
    )
    if not username or not stored_password:
        message = (
            str(requested_last.get("credential_save_detail") or "").strip()
            or "Saved live-sync credentials are required before quick run is available."
        )
        if _wants_async_json():
            return _json_error(message, status_code=409)
        flash(message, "warn")
        return redirect(url_for("dashboard"))

    selected_account = legacy._selected_account()
    preflight = _build_sync_today_preflight(
        selected_account=selected_account,
        cfg=cfg,
        requested=requested_last,
    )
    if not preflight["can_run"]:
        message = str(preflight["disabled_reason"] or "Sync Today is not ready.")
        if _wants_async_json():
            return _json_error(message, status_code=409)
        flash(message, "warn")
        return redirect(url_for("dashboard"))
    base_url = str(preflight["base_url"])
    account = str(preflight["broker_account"])
    from_date = str(preflight["from_date"])
    to_date = str(preflight["to_date"])

    requested = legacy._sync_requested_payload(
        source="dashboard_sync_today",
        mode=mode,
        from_date=from_date,
        to_date=to_date,
        base_url=base_url,
        account=account,
        wl=str(requested_last.get("wl") or cfg.get("wl") or "vanquishtrader"),
        time_zone=str(
            requested_last.get("time_zone") or cfg.get("time_zone") or "America/New_York"
        ),
        date_locale=str(requested_last.get("date_locale") or cfg.get("date_locale") or "en-US"),
        report_locale=str(requested_last.get("report_locale") or cfg.get("report_locale") or "en"),
        headless=bool(cfg.get("headless", True)),
        debug_capture=bool(cfg.get("debug_capture", True)),
        debug_only=False,
        username=username,
    )
    requested["remember_connection"] = bool(requested_last.get("remember_connection"))
    requested["remember_credentials"] = bool(requested_last.get("remember_credentials"))
    requested["stored_password_reused"] = True
    requested["credential_save_requested"] = bool(requested_last.get("credential_save_requested"))
    requested["credential_save_status"] = str(
        requested_last.get("credential_save_status") or ""
    ).strip()
    requested["credential_save_detail"] = str(
        requested_last.get("credential_save_detail") or ""
    ).strip()

    job = legacy._start_sync_job(
        selected_account_id=int(selected_account["id"]),
        title="Live Sync",
        source_label="LIVE LOGIN HTML",
        record_source="DASHBOARD SYNC TODAY",
        mode=mode,
        username=username,
        password=stored_password,
        base_url=base_url,
        account=account,
        wl=str(requested.get("wl") or "vanquishtrader"),
        time_zone=str(requested.get("time_zone") or "America/New_York"),
        date_locale=str(requested.get("date_locale") or "en-US"),
        report_locale=str(requested.get("report_locale") or "en"),
        from_date=from_date,
        to_date=to_date,
        headless=bool(requested.get("headless")),
        debug_capture=bool(requested.get("debug_capture")),
        debug_only=bool(requested.get("debug_only")),
        requested=requested,
    )
    job_payload = job_response_payload(job, humanize_timestamp=legacy._humanize_et_timestamp)
    if str(job.get("status") or "").strip().lower() == "failed":
        message = str(job.get("message") or "Live sync could not start.")
        if _wants_async_json():
            return jsonify({"ok": False, "message": message, "job": job_payload}), 503
        flash(message, "warn")
        return redirect(url_for("dashboard"))
    if _wants_async_json():
        return jsonify({"ok": True, "job": job_payload, "sync": dashboard_live_sync_state()})
    flash("Live sync started from the dashboard. Status will update inline.", "success")
    return redirect(url_for("dashboard"))


def trades_sync_auto_config():
    if request.method != "POST":
        return redirect(url_for("trades_upload_pdf"))
    cfg = legacy._load_auto_sync_config()
    cfg["enabled"] = request.form.get("auto_enabled") == "1"
    cfg["run_weekends"] = request.form.get("auto_run_weekends") == "1"
    cfg["run_time_et"] = (request.form.get("auto_run_time_et") or "").strip() or "16:15"
    cfg["mode"] = (request.form.get("auto_mode") or "broker").strip() or "broker"
    username = (request.form.get("auto_username") or "").strip()
    old_username = str(cfg.get("username") or "")
    cfg["username"] = username
    new_password = (request.form.get("auto_password") or "").strip()
    clear_password = request.form.get("auto_clear_password") == "1"
    cfg["base_url"] = (request.form.get("auto_base_url") or "").strip() or cfg.get(
        "base_url", "https://trade.vanquishtrader.com"
    )
    cfg["account"] = (request.form.get("auto_account") or "").strip()
    cfg["wl"] = (request.form.get("auto_wl") or "").strip() or cfg.get("wl", "vanquishtrader")
    cfg["time_zone"] = (request.form.get("auto_time_zone") or "").strip() or cfg.get(
        "time_zone", "America/New_York"
    )
    cfg["date_locale"] = (request.form.get("auto_date_locale") or "").strip() or cfg.get(
        "date_locale", "en-US"
    )
    cfg["report_locale"] = (request.form.get("auto_report_locale") or "").strip() or cfg.get(
        "report_locale", "en"
    )
    cfg["headless"] = request.form.get("auto_headless") == "1"
    cfg["debug_capture"] = request.form.get("auto_debug_capture") == "1"
    if clear_password:
        target_user = username or old_username
        cfg["password"] = ""
        cfg["password_enc"] = ""
        if target_user and legacy._clear_auto_sync_password(target_user):
            flash("Auto sync password cleared from OS keychain.", "success")
        elif target_user:
            flash("Could not clear keychain password (or it was not present).", "warn")
    elif new_password:
        if not username:
            cfg["password"] = ""
            cfg["password_enc"] = ""
            flash("Set username before saving password to keychain.", "warn")
        elif legacy._set_auto_sync_password(username, new_password):
            cfg["password"] = ""
            cfg["password_enc"] = ""
            flash("Auto sync password stored in OS keychain.", "success")
        elif legacy.AUTO_SYNC_PASSWORD_FALLBACK:
            enc = legacy._encrypt_fallback_password(new_password)
            if enc:
                cfg["password_enc"] = enc
                cfg["password"] = ""
                flash(
                    "OS keychain unavailable. Stored encrypted password in container fallback.",
                    "warn",
                )
            else:
                cfg["password"] = ""
                cfg["password_enc"] = ""
                flash(
                    "OS keychain unavailable and fallback encryption is not ready. "
                    "Set SECRET_KEY or AUTO_SYNC_PASSWORD_FALLBACK_KEY.",
                    "warn",
                )
        else:
            cfg["password"] = ""
            cfg["password_enc"] = ""
            flash(
                "OS keychain unavailable. Install/use a keyring backend before enabling auto sync.",
                "warn",
            )
    else:
        cfg["password"] = str(cfg.get("password") or "")
        cfg["password_enc"] = str(cfg.get("password_enc") or "")
    legacy._save_auto_sync_config(cfg)
    if cfg.get("enabled") and not legacy._get_auto_sync_password(cfg):
        flash(
            "Auto sync is enabled but no keychain password is stored yet.",
            "warn",
        )
    flash("Auto sync schedule saved.", "success")
    return redirect(url_for("trades_upload_pdf", ws="live"))


def trades_sync_auto_run_now():
    conflict = _active_sync_conflict()
    if conflict:
        active_job, payload = conflict
        if _wants_async_json():
            return jsonify(payload), 409
        flash(payload["message"], "warn")
        return redirect(url_for("trades_upload_pdf", ws="live", job=active_job["id"]))

    cfg = legacy._load_auto_sync_config()
    selected_account = legacy._selected_account()
    if not selected_account:
        message = "Please select an account before uploading trades."
        if _wants_async_json():
            return _json_error(message)
        flash(message, "warn")
        return redirect(url_for("trades_upload_pdf", ws="live"))
    auto_password = legacy._get_auto_sync_password(cfg)
    if not cfg.get("username") or not auto_password:
        message = "Auto sync credentials are missing. Save username and password in the Live Sync workspace first."
        if _wants_async_json():
            return _json_error(message)
        flash(
            message,
            "warn",
        )
        return redirect(url_for("trades_upload_pdf", ws="live"))
    today = legacy.today_iso()
    requested = legacy._sync_requested_payload(
        source="manual_auto_run",
        mode=str(cfg.get("mode") or "broker"),
        from_date=today,
        to_date=today,
        base_url=str(cfg.get("base_url") or "https://trade.vanquishtrader.com"),
        account=str(cfg.get("account") or ""),
        wl=str(cfg.get("wl") or "vanquishtrader"),
        time_zone=str(cfg.get("time_zone") or "America/New_York"),
        date_locale=str(cfg.get("date_locale") or "en-US"),
        report_locale=str(cfg.get("report_locale") or "en"),
        headless=bool(cfg.get("headless", True)),
        debug_capture=bool(cfg.get("debug_capture", True)),
        debug_only=False,
        username=str(cfg.get("username") or ""),
    )
    job = legacy._start_sync_job(
        selected_account_id=int(selected_account["id"]),
        title="Auto Sync Run",
        source_label="AUTO SYNC HTML",
        record_source="AUTO SYNC MANUAL RUN",
        mode=str(cfg.get("mode") or "broker"),
        username=str(cfg.get("username") or ""),
        password=auto_password,
        base_url=str(cfg.get("base_url") or "https://trade.vanquishtrader.com"),
        account=str(cfg.get("account") or ""),
        wl=str(cfg.get("wl") or "vanquishtrader"),
        time_zone=str(cfg.get("time_zone") or "America/New_York"),
        date_locale=str(cfg.get("date_locale") or "en-US"),
        report_locale=str(cfg.get("report_locale") or "en"),
        from_date=today,
        to_date=today,
        headless=bool(cfg.get("headless", True)),
        debug_capture=bool(cfg.get("debug_capture", True)),
        debug_only=False,
        requested=requested,
    )
    job_payload = job_response_payload(job, humanize_timestamp=legacy._humanize_et_timestamp)
    if str(job.get("status") or "").strip().lower() == "failed":
        message = str(job.get("message") or "Auto sync could not start.")
        if _wants_async_json():
            return jsonify({"ok": False, "message": message, "job": job_payload}), 503
        flash(message, "warn")
        return redirect(url_for("trades_upload_pdf", ws="live", job=job["id"]))
    if _wants_async_json():
        return jsonify({"ok": True, "job": job_payload})
    flash("Auto sync started. Live status will update below.", "success")
    return redirect(url_for("trades_upload_pdf", ws="live", job=job["id"]))


def trades_sync_job_status(job_id: str):
    job = legacy._get_bg_job((job_id or "").strip())
    if not job:
        return jsonify({"ok": False, "error": "job_not_found"}), 404
    return jsonify(
        {
            "ok": True,
            "job": job_response_payload(job, humanize_timestamp=legacy._humanize_et_timestamp),
            "sync": dashboard_live_sync_state(),
        }
    )


def trades_sync_job_cancel(job_id: str):
    key = (job_id or "").strip()
    job = legacy._get_bg_job(key)
    if not job:
        return jsonify({"ok": False, "error": "job_not_found"}), 404
    job = legacy._cancel_sync_job(key)
    flash("Sync job cancelled. Any late result from that run will be ignored.", "warn")
    if request.headers.get("Accept") == "application/json":
        return jsonify(
            {
                "ok": True,
                "job": job_response_payload(job, humanize_timestamp=legacy._humanize_et_timestamp),
            }
        )
    return redirect(url_for("trades_upload_pdf", ws="live", job=key))


def trades_sync_job_force_reset(job_id: str):
    key = (job_id or "").strip()
    job = legacy._get_bg_job(key)
    if not job:
        return jsonify({"ok": False, "error": "job_not_found"}), 404
    job = legacy._force_reset_sync_job(key)
    flash("Sync lane reset. The prior run is closed and the workspace is unlocked.", "warn")
    if request.headers.get("Accept") == "application/json":
        return jsonify(
            {
                "ok": True,
                "job": job_response_payload(job, humanize_timestamp=legacy._humanize_et_timestamp),
            }
        )
    return redirect(url_for("trades_upload_pdf", ws="live", job=key))


def ensure_auto_sync_worker_started(app) -> None:
    global _AUTO_SYNC_THREAD_STARTED, _AUTO_BACKUP_THREAD_STARTED
    legacy.ensure_sync_dispatcher_started(app)
    with _AUTO_SYNC_THREAD_LOCK:
        if not _AUTO_SYNC_THREAD_STARTED:
            t = threading.Thread(
                target=_auto_sync_worker, args=(app,), daemon=True, name="auto-sync-worker"
            )
            t.start()
            _AUTO_SYNC_THREAD_STARTED = True
    with _AUTO_BACKUP_THREAD_LOCK:
        if not _AUTO_BACKUP_THREAD_STARTED:
            t = threading.Thread(
                target=trades_ops._auto_backup_worker,
                args=(app,),
                daemon=True,
                name="auto-backup-worker",
            )
            t.start()
            _AUTO_BACKUP_THREAD_STARTED = True


def _auto_sync_worker(app) -> None:
    while True:
        try:
            cfg = legacy._load_auto_sync_config()
            if not cfg.get("enabled"):
                time.sleep(20)
                continue
            tz_name = str(cfg.get("time_zone") or "America/New_York")
            tz = ZoneInfo(tz_name)
            now_local = datetime.now(tz)
            if (not cfg.get("run_weekends")) and now_local.weekday() >= 5:
                time.sleep(30)
                continue
            hhmm = str(cfg.get("run_time_et") or "16:15")
            try:
                h, m = hhmm.split(":", 1)
                target_h = int(h)
                target_m = int(m)
            except Exception:
                target_h, target_m = 16, 15
            today = now_local.date().isoformat()
            if now_local.hour < target_h or (
                now_local.hour == target_h and now_local.minute < target_m
            ):
                time.sleep(20)
                continue
            if str(cfg.get("last_run_date") or "") == today:
                time.sleep(40)
                continue
            auto_password = legacy._get_auto_sync_password(cfg)
            if not cfg.get("username") or not auto_password or not cfg.get("account"):
                legacy._save_last_sync_status(
                    {
                        "status": "failed",
                        "stage": "auto_config",
                        "message": (
                            "Auto sync is enabled but username/keychain password/account "
                            "are not fully configured."
                        ),
                        "updated_at": legacy.now_iso(),
                    }
                )
                time.sleep(60)
                continue
            try:
                fd = os.open(
                    legacy._broker_auto_sync_lock_path(), os.O_CREAT | os.O_EXCL | os.O_WRONLY
                )
                os.close(fd)
            except FileExistsError:
                time.sleep(20)
                continue
            try:
                with app.app_context():
                    selected_account = legacy._selected_account()
                    if not selected_account:
                        legacy._save_last_sync_status(
                            {
                                "status": "failed",
                                "stage": "account_scope",
                                "message": "Please select an account before uploading trades.",
                                "updated_at": legacy.now_iso(),
                            }
                        )
                        time.sleep(60)
                        continue
                    started = time.time()
                    run = legacy._run_live_sync_once(
                        selected_account_id=int(selected_account["id"]),
                        mode=str(cfg.get("mode") or "broker"),
                        username=str(cfg.get("username") or ""),
                        password=auto_password,
                        base_url=str(cfg.get("base_url") or "https://trade.vanquishtrader.com"),
                        account=str(cfg.get("account") or ""),
                        wl=str(cfg.get("wl") or "vanquishtrader"),
                        time_zone=str(cfg.get("time_zone") or "America/New_York"),
                        date_locale=str(cfg.get("date_locale") or "en-US"),
                        report_locale=str(cfg.get("report_locale") or "en"),
                        from_date=today,
                        to_date=today,
                        headless=bool(cfg.get("headless", True)),
                        debug_capture=bool(cfg.get("debug_capture", True)),
                        debug_only=False,
                        source_label="AUTO SYNC HTML",
                    )
                    cfg["last_run_date"] = today
                    legacy._save_auto_sync_config(cfg)
                    duration_sec = round(max(0.0, time.time() - started), 2)
                    legacy._record_import_batch(
                        batch_id=str(run.get("batch_id") or ""),
                        source="AUTO SYNC SCHEDULER",
                        mode=str(cfg.get("mode") or "broker"),
                        report=run.get("report") if isinstance(run.get("report"), dict) else None,
                        status="success" if run.get("ok") else "failed",
                        message=str(run.get("message") or ""),
                    )
                    legacy._save_last_sync_status(
                        {
                            "status": "success" if run.get("ok") else "failed",
                            "stage": run.get("stage")
                            or ("import_complete" if run.get("ok") else "unknown"),
                            "message": run.get("message") or "",
                            "stage_help": legacy.SYNC_STAGE_HELP.get(
                                str(run.get("stage") or ""), ""
                            ),
                            "requested": {
                                "source": "scheduler",
                                "scheduled_for": f"{today} {target_h:02d}:{target_m:02d}",
                                "mode": cfg.get("mode", "broker"),
                            },
                            "sync_meta": run.get("sync_meta", {}),
                            "artifacts_rel": (run.get("artifacts_rel") or [])[:20],
                            "statement_file": (
                                legacy._debug_relative(run.get("statement_path", ""))
                                if run.get("statement_path")
                                else ""
                            ),
                            "duration_sec": duration_sec,
                            "updated_at": legacy.now_iso(),
                        }
                    )
            finally:
                try:
                    os.unlink(legacy._broker_auto_sync_lock_path())
                except OSError:
                    pass
            time.sleep(45)
        except Exception as e:  # pragma: no cover
            legacy._save_last_sync_status(
                {
                    "status": "failed",
                    "stage": "auto_worker",
                    "message": f"Auto sync worker error: {e}",
                    "updated_at": legacy.now_iso(),
                }
            )
            time.sleep(60)
