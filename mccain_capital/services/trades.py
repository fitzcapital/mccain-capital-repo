"""Trades domain service functions."""

from __future__ import annotations

import os
import sqlite3
import json
import base64
import hashlib
import inspect
import urllib
import shutil
import tempfile
import threading
import time
import queue
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from uuid import uuid4

from flask import (
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from werkzeug.utils import secure_filename

from mccain_capital.repositories import trades as repo
from mccain_capital.repositories import analytics as analytics_repo
from mccain_capital.repositories import strategies as strategies_repo
from mccain_capital import auth
from mccain_capital import runtime as app_runtime
from mccain_capital.migrations import run_migrations
from mccain_capital.runtime import (
    db,
    detect_paste_format,
    get_setting_float,
    latest_balance_overall,
    money,
    now_iso,
    normalize_opt_type,
    parse_date_any,
    parse_float,
    parse_int,
    today_iso,
)
from mccain_capital.services import trades_importing as importing
from mccain_capital.services import broker_equity
from mccain_capital.services import vanquish_live_sync
from mccain_capital.services.trades_backup import (
    _auto_backup_config_path,
    _auto_backup_config_paths,
    _auto_backup_dir,
    _auto_backup_worker,
    _integrity_health_snapshot,
    _list_saved_backups,
    _load_auto_backup_config,
    _normalize_backup_times,
    _restore_dry_run,
    _restore_from_backup_path,
    _run_backup_once,
    _save_auto_backup_config,
)
from mccain_capital.services.trades_notifications import (
    _alerts_actor,
    _bulk_update_alert_status,
    _emit_notification,
    _load_notify_history,
    _notify_history_paths,
    _parse_iso_epoch,
    _save_notify_history,
    _scan_anomaly_watch,
    _sorted_alerts,
    _update_alert_status,
)
from mccain_capital.services.background_jobs import BackgroundJobStore
from mccain_capital.services.ui import render_page, simple_msg
from mccain_capital.services.viewmodels import (
    backup_state_badges,
    sync_state_badges,
)

__all__ = [
    "urllib",
    "_auto_backup_config_paths",
    "_notify_history_paths",
]

# Compatibility aliases used by extracted route bodies.
fetch_trades = repo.fetch_trades
fetch_trade_reviews_map = repo.fetch_trade_reviews_map
trade_day_stats = repo.trade_day_stats
calc_consistency = repo.calc_consistency
week_total_net = repo.week_total_net

BROKER_KEYCHAIN_SERVICE = "mccain-capital.vanquish.auto-sync"
AUTO_SYNC_PASSWORD_FALLBACK = os.environ.get("AUTO_SYNC_PASSWORD_FALLBACK", "0") == "1"
SYNC_HISTORY_MAX = 300
IMPORT_HISTORY_MAX = 300
RECONCILE_GATE_ENABLED = os.environ.get("RECONCILE_GATE_ENABLED", "1") == "1"
RECONCILE_GATE_MAX_DELTA = float(os.environ.get("RECONCILE_GATE_MAX_DELTA", "1.0") or 1.0)
NOTIFY_WEBHOOK_URL = (os.environ.get("NOTIFY_WEBHOOK_URL") or "").strip()
NOTIFY_FAIL_STREAK = int(os.environ.get("NOTIFY_FAIL_STREAK", "3") or 3)
NOTIFY_WEBHOOK_SECRET = (os.environ.get("NOTIFY_WEBHOOK_SECRET") or "").strip()
NOTIFY_RETRY_ATTEMPTS = int(os.environ.get("NOTIFY_RETRY_ATTEMPTS", "3") or 3)
NOTIFY_RETRY_BACKOFF_SEC = float(os.environ.get("NOTIFY_RETRY_BACKOFF_SEC", "0.4") or 0.4)
NOTIFY_RETRY_BACKOFF_MULTIPLIER = float(
    os.environ.get("NOTIFY_RETRY_BACKOFF_MULTIPLIER", "2.0") or 2.0
)
NOTIFY_DEFAULT_DEDUPE_SECONDS = int(os.environ.get("NOTIFY_DEFAULT_DEDUPE_SECONDS", "300") or 300)
NOTIFY_DEDUPE_BY_EVENT = {
    "sync_fail_streak": int(os.environ.get("NOTIFY_DEDUPE_SYNC_FAIL_STREAK_SECONDS", "300") or 300),
    "reconcile_gate_block": int(
        os.environ.get("NOTIFY_DEDUPE_RECONCILE_GATE_BLOCK_SECONDS", "600") or 600
    ),
    "drift_recurrence": int(
        os.environ.get("NOTIFY_DEDUPE_DRIFT_RECURRENCE_SECONDS", "1800") or 1800
    ),
    "batch_rollback": int(os.environ.get("NOTIFY_DEDUPE_BATCH_ROLLBACK_SECONDS", "120") or 120),
    "anomaly_size_spike": int(
        os.environ.get("NOTIFY_DEDUPE_ANOMALY_SIZE_SPIKE_SECONDS", "900") or 900
    ),
    "anomaly_revenge_pattern": int(
        os.environ.get("NOTIFY_DEDUPE_ANOMALY_REVENGE_PATTERN_SECONDS", "900") or 900
    ),
    "anomaly_setup_underperformance": int(
        os.environ.get("NOTIFY_DEDUPE_ANOMALY_SETUP_UNDERPERF_SECONDS", "900") or 900
    ),
    "gamma_levels_ready": int(
        os.environ.get("NOTIFY_DEDUPE_GAMMA_LEVELS_READY_SECONDS", "21600") or 21600
    ),
}

_BG_JOB_STORES: Dict[str, BackgroundJobStore] = {}
_SYNC_CANCEL_EVENTS: Dict[str, threading.Event] = {}
_SYNC_CANCEL_LOCK = threading.Lock()
_SYNC_JOB_QUEUE: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue()
_SYNC_DISPATCH_THREAD_STARTED = False
_SYNC_DISPATCH_THREAD_LOCK = threading.Lock()
SYNC_JOB_STALE_SECONDS = int(os.environ.get("SYNC_JOB_STALE_SECONDS", "300") or 300)
SYNC_JOB_QUEUED_STALE_SECONDS = int(os.environ.get("SYNC_JOB_QUEUED_STALE_SECONDS", "120") or 120)
SYNC_JOB_UI_STAGE_STALE_SECONDS = int(os.environ.get("SYNC_JOB_UI_STAGE_STALE_SECONDS", "75") or 75)


class SyncJobCancelled(RuntimeError):
    """Raised when a sync job is cancelled by the user."""


def _upload_dir() -> str:
    return app_runtime.upload_root()


def _books_dir() -> str:
    return app_runtime.books_root()


def _upload_file(name: str) -> str:
    return app_runtime.upload_path(name)


def _broker_sync_config_path() -> str:
    return str(BROKER_SYNC_CONFIG_PATH or _upload_file(".vanquish_sync.json"))


def _broker_debug_dir() -> str:
    return str(BROKER_DEBUG_DIR or _upload_file("vanquish_debug"))


def _broker_sync_status_path() -> str:
    return str(BROKER_SYNC_STATUS_PATH or _upload_file(".vanquish_sync_last_run.json"))


def _broker_sync_history_path() -> str:
    return str(BROKER_SYNC_HISTORY_PATH or _upload_file(".vanquish_sync_history.json"))


def _broker_import_history_path() -> str:
    return str(BROKER_IMPORT_HISTORY_PATH or _upload_file(".vanquish_import_history.json"))


def _broker_notify_history_path() -> str:
    return str(BROKER_NOTIFY_HISTORY_PATH or _upload_file(".vanquish_notify_history.json"))


def _playbook_config_path() -> str:
    return str(PLAYBOOK_CONFIG_PATH or _upload_file(".playbook_rules.json"))


def _admin_audit_log_path() -> str:
    return str(ADMIN_AUDIT_LOG_PATH or _upload_file(".admin_audit_log.json"))


def _broker_auto_sync_config_path() -> str:
    return str(BROKER_AUTO_SYNC_CONFIG_PATH or _upload_file(".vanquish_auto_sync.json"))


def _broker_auto_sync_lock_path() -> str:
    return str(BROKER_AUTO_SYNC_LOCK_PATH or _upload_file(".vanquish_auto_sync.lock"))


def _bg_job_store() -> BackgroundJobStore:
    job_dir = str(BG_JOB_DIR or _upload_file(".bg_jobs"))
    store = _BG_JOB_STORES.get(job_dir)
    if store is None:
        store = BackgroundJobStore(job_dir, now_iso)
        _BG_JOB_STORES[job_dir] = store
    return store


# Optional path overrides used by targeted tests and local debugging.
BROKER_SYNC_CONFIG_PATH: Optional[str] = None
BROKER_DEBUG_DIR: Optional[str] = None
BROKER_SYNC_STATUS_PATH: Optional[str] = None
BROKER_SYNC_HISTORY_PATH: Optional[str] = None
BROKER_IMPORT_HISTORY_PATH: Optional[str] = None
BROKER_NOTIFY_HISTORY_PATH: Optional[str] = None
PLAYBOOK_CONFIG_PATH: Optional[str] = None
ADMIN_AUDIT_LOG_PATH: Optional[str] = None
BROKER_AUTO_SYNC_CONFIG_PATH: Optional[str] = None
BROKER_AUTO_SYNC_LOCK_PATH: Optional[str] = None
AUTO_BACKUP_CONFIG_PATH: Optional[str] = None
AUTO_BACKUP_DIR: Optional[str] = None
AUTO_BACKUP_LOCK_PATH: Optional[str] = None
BG_JOB_DIR: Optional[str] = None

_AUDIT_ACTION_META = {
    "backup_created": {"label": "Backup Created", "group": "backup"},
    "backup_failed": {"label": "Backup Failed", "group": "backup"},
    "backup_restored_from_center": {"label": "Backup Restored", "group": "restore"},
    "live_data_cleared": {"label": "Live Data Cleared", "group": "restore"},
    "manual_backup_restored": {"label": "Backup Restored", "group": "restore"},
    "manual_backup_downloaded": {"label": "Backup Downloaded", "group": "backup"},
    "backup_deleted": {"label": "Backup Deleted", "group": "backup"},
    "integrity_check_run": {"label": "Integrity Check", "group": "integrity"},
    "trades_rebuild_reviews": {"label": "Review Rebuild", "group": "review"},
    "trades_history_balance_updated": {"label": "History Balance Updated", "group": "config"},
    "trades_scope_balance_updated": {"label": "Active Scope Updated", "group": "config"},
    "rollback_import_batch": {"label": "Import Batch Rolled Back", "group": "rollback"},
    "dashboard_recompute_balances": {"label": "Balances Recomputed", "group": "recompute"},
    "auto_backup_config_saved": {"label": "Backup Settings Saved", "group": "config"},
    "ops_alert_ack": {"label": "Alert Acknowledged", "group": "alert"},
    "ops_alert_resolve": {"label": "Alert Resolved", "group": "alert"},
    "ops_alert_resolve_all": {"label": "All Alerts Resolved", "group": "alert"},
    "ops_alert_mute": {"label": "Alert Muted", "group": "alert"},
    "ops_alert_unmute": {"label": "Alert Unmuted", "group": "alert"},
}

SYNC_STAGE_HELP = {
    "queue_dispatch": "The worker accepted the request but has not started the broker session yet. If it sits here, treat it as a dispatch stall and retry once.",
    "system_resource": "System resources were not available while launching sync startup. Retry after load settles or reduce worker pressure.",
    "storage_io": "Sync could not write required files under uploads/debug storage. Check mounted volume permissions and free space.",
    "browser_boot": "The browser session did not start cleanly. Retry once, then inspect container resources and Playwright logs.",
    "open_login": "Broker login did not open cleanly. Check Base Origin and network.",
    "locate_username": "Could not find the username input. Broker UI likely changed.",
    "locate_password": "Could not find the password input. Broker login form likely changed.",
    "submit_login": "Login did not complete. Validate credentials or check for MFA/CAPTCHA.",
    "auth_required": "Broker returned a login page instead of statement HTML. Refresh credentials/session or use manual statement upload for this run.",
    "open_workspace_menu": "Could not open the app menu after login. Workspace may still be loading.",
    "open_statement_dialog": "Could not open Account Statement from the menu.",
    "configure_statement_period": "Could not set statement date range in the dialog.",
    "generate_statement": "Generate Statement did not complete as expected.",
    "capture_statement_html": "Statement page loaded but HTML capture/parse failed.",
    "stale": "The sync worker stopped updating. Retry the run and inspect logs if it stalls again.",
    "reset_required": "The sync lane was force-reset after a hang. Retry one clean run and inspect artifacts only if it stalls again.",
}

SYNC_STAGE_LABELS = {
    "start": "Dispatching sync request.",
    "queue_dispatch": "Dispatching sync worker.",
    "system_resource": "Waiting for startup resources.",
    "storage_io": "Writing sync artifacts.",
    "browser_boot": "Booting browser session.",
    "open_login": "Opening broker login.",
    "locate_username": "Finding username field.",
    "fill_username": "Entering username.",
    "locate_password": "Finding password field.",
    "submit_login": "Submitting broker login.",
    "auth_required": "Broker session required.",
    "open_workspace_menu": "Opening workspace menu.",
    "open_statement_dialog": "Opening statement dialog.",
    "configure_statement_period": "Setting statement date range.",
    "generate_statement": "Generating statement HTML.",
    "capture_statement_html": "Capturing statement HTML.",
    "stale": "Sync stalled before completion.",
    "reset_required": "Lane reset after hang.",
    "parse_statement_html": "Parsing statement rows.",
    "reconcile_gate": "Running reconcile guardrails.",
    "import_trades": "Importing trades.",
    "capture_account_metrics": "Capturing account metrics.",
    "import_complete": "Import complete.",
}


def _sync_stage_label(stage: str) -> str:
    key = str(stage or "").strip()
    return SYNC_STAGE_LABELS.get(key, key.replace("_", " ").strip().title() or "Working...")


def _is_sync_startup_stage(stage: str) -> bool:
    normalized = str(stage or "").strip().lower()
    return normalized in {
        "",
        "start",
        "queued",
        "queue_dispatch",
        "system_resource",
        "browser_boot",
        "open_login",
    }


def _create_bg_job(kind: str, title: str, requested: Dict[str, Any]) -> Dict[str, Any]:
    return _bg_job_store().create(kind, title, requested)


def _update_bg_job(job_id: str, **updates: Any) -> Dict[str, Any]:
    return _bg_job_store().update(job_id, **updates)


def _get_bg_job(job_id: str) -> Dict[str, Any]:
    return _reconcile_stale_sync_job(_bg_job_store().get(job_id))


def _stale_sync_job_message(stage: str) -> str:
    normalized = str(stage or "").strip().lower()
    stage_label = _sync_stage_label(stage or "start")
    if normalized in {"", "start", "queued", "queue_dispatch"}:
        return (
            "Sync startup stalled before broker login during worker dispatch. "
            "The lane was unlocked so you can start a fresh run."
        )
    if normalized in {"system_resource", "browser_boot", "open_login"}:
        return (
            f"Sync startup stalled during {stage_label}. "
            "The lane was unlocked so you can retry after load settles."
        )
    if normalized in {"open_workspace_menu", "open_statement_dialog"}:
        return (
            f"Broker UI stalled during {stage_label}. "
            "The lane was unlocked so you can retry or inspect the latest debug artifacts."
        )
    return (
        f"Sync job became stale before completion during {stage_label}. "
        "The worker likely stopped or the app restarted. Start a new sync run."
    )


def _sync_job_stale_after(status: str, stage: str) -> float:
    normalized_status = str(status or "").strip().lower()
    normalized_stage = str(stage or "").strip().lower()
    if normalized_status == "queued" or _is_sync_startup_stage(normalized_stage):
        return float(SYNC_JOB_QUEUED_STALE_SECONDS)
    if normalized_stage in {"open_workspace_menu", "open_statement_dialog"}:
        return float(SYNC_JOB_UI_STAGE_STALE_SECONDS)
    return float(SYNC_JOB_STALE_SECONDS)


def _mark_last_sync_status_stale(job: Dict[str, Any], *, message: str, duration_sec: float) -> None:
    raw = _read_last_sync_status()
    raw_job_id = str(raw.get("job_id") or "").strip()
    status = str(raw.get("status") or "").strip().lower()
    if raw_job_id != str(job.get("id") or "").strip() or status not in {"queued", "running"}:
        return
    _save_last_sync_status(
        {
            "job_id": raw_job_id,
            "status": "failed",
            "stage": "stale",
            "message": message,
            "stage_help": SYNC_STAGE_HELP.get("stale", ""),
            "requested": raw.get("requested") if isinstance(raw.get("requested"), dict) else {},
            "artifacts_rel": list(((job.get("summary") or {}).get("artifacts_rel") or []))[:20],
            "statement_file": str(((job.get("summary") or {}).get("statement_file") or "")).strip(),
            "duration_sec": duration_sec,
            "updated_at": now_iso(),
        }
    )


def _reconcile_stale_sync_job(job: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(job, dict) or not job:
        return {}
    if str(job.get("kind") or "").strip().lower() != "sync":
        return job
    status = str(job.get("status") or "").strip().lower()
    if status not in {"queued", "running"}:
        return job
    updated_epoch = _parse_iso_epoch(str(job.get("updated_at") or job.get("created_at") or ""))
    if updated_epoch is None:
        return job
    elapsed = time.time() - updated_epoch
    stale_after = _sync_job_stale_after(status, str(job.get("stage") or ""))
    if elapsed < stale_after:
        return job
    created_epoch = _parse_iso_epoch(str(job.get("created_at") or "")) or updated_epoch
    duration_sec = round(max(0.0, time.time() - created_epoch), 2)
    message = _stale_sync_job_message(str(job.get("stage") or "start"))
    summary = {
        "message": message,
        "warn_count": int(((job.get("summary") or {}).get("warn_count")) or 0),
        "error_count": max(1, int(((job.get("summary") or {}).get("error_count")) or 0)),
        "inserted": int(((job.get("summary") or {}).get("inserted")) or 0),
        "artifacts_rel": list(((job.get("summary") or {}).get("artifacts_rel") or []))[:20],
        "statement_file": str(((job.get("summary") or {}).get("statement_file") or "")).strip(),
    }
    stale = _update_bg_job(
        str(job.get("id") or ""),
        status="failed",
        stage="stale",
        message=message,
        duration_sec=duration_sec,
        summary=summary,
        result_summary=_build_action_result_summary(
            tone="danger",
            title="Live Sync Stale",
            happened=message,
            changed="No import was finalized from this stale run.",
            next_action="Retry the sync. If the problem repeats, inspect the latest debug artifacts and server logs.",
            metrics=[
                {"label": "Inserted", "value": str(summary["inserted"])},
                {"label": "Warnings", "value": str(summary["warn_count"])},
                {"label": "Errors", "value": str(summary["error_count"])},
            ],
            actions=[
                {
                    "label": "Open Live Sync",
                    "href": "/trades/upload/statement?ws=live",
                    "kind": "primary",
                }
            ],
        ),
    )
    _mark_last_sync_status_stale(stale or job, message=message, duration_sec=duration_sec)
    return stale or job


def _latest_active_sync_job() -> Dict[str, Any]:
    job_dir = str(BG_JOB_DIR or _upload_file(".bg_jobs"))
    try:
        names = sorted(os.listdir(job_dir))
    except OSError:
        return {}
    candidates: List[Dict[str, Any]] = []
    for name in names:
        if not name.endswith(".json"):
            continue
        job = _get_bg_job(name[:-5])
        if not job:
            continue
        if str(job.get("kind") or "").strip().lower() != "sync":
            continue
        if str(job.get("status") or "").strip().lower() not in {"queued", "running"}:
            continue
        candidates.append(job)
    if not candidates:
        return {}
    candidates.sort(
        key=lambda item: _parse_iso_epoch(
            str(item.get("updated_at") or item.get("created_at") or "")
        )
        or 0.0,
        reverse=True,
    )
    return candidates[0]


def _reconcile_sync_runtime_state() -> Dict[str, Any]:
    reconciled = 0
    active = {}
    job_dir = str(BG_JOB_DIR or _upload_file(".bg_jobs"))
    try:
        names = sorted(os.listdir(job_dir))
    except OSError:
        names = []
    for name in names:
        if not name.endswith(".json"):
            continue
        job = _get_bg_job(name[:-5])
        if not job:
            continue
        if str(job.get("kind") or "").strip().lower() != "sync":
            continue
        reconciled += 1
        if not active and str(job.get("status") or "").strip().lower() in {"queued", "running"}:
            active = job
    status = _load_last_sync_status()
    return {
        "reconciled_jobs": reconciled,
        "active_job": active,
        "last_status": status,
    }


def _sync_cancel_event(job_id: str) -> threading.Event:
    key = str(job_id or "").strip()
    with _SYNC_CANCEL_LOCK:
        event = _SYNC_CANCEL_EVENTS.get(key)
        if event is None:
            event = threading.Event()
            _SYNC_CANCEL_EVENTS[key] = event
        return event


def _clear_sync_cancel_event(job_id: str) -> None:
    key = str(job_id or "").strip()
    if not key:
        return
    with _SYNC_CANCEL_LOCK:
        _SYNC_CANCEL_EVENTS.pop(key, None)


def _sync_job_cancelled(job_id: str) -> bool:
    job = _get_bg_job(job_id)
    return str(job.get("status") or "").strip().lower() == "cancelled"


def _ensure_sync_job_active(job_id: str, cancel_event: Optional[threading.Event] = None) -> None:
    if cancel_event and cancel_event.is_set():
        raise SyncJobCancelled("Sync cancelled by user.")
    if _sync_job_cancelled(job_id):
        raise SyncJobCancelled("Sync cancelled by user.")


def _cancel_sync_job(job_id: str) -> Dict[str, Any]:
    job = _get_bg_job(job_id)
    if not job:
        return {}
    status = str(job.get("status") or "").strip().lower()
    if status in {"success", "failed", "debug_only", "cancelled"}:
        return job
    _sync_cancel_event(job_id).set()
    summary = {
        "message": "Sync cancelled. Late background output will be ignored.",
        "warn_count": 0,
        "error_count": 0,
        "inserted": 0,
        "artifacts_rel": list(((job.get("summary") or {}).get("artifacts_rel") or []))[:20],
        "statement_file": str(((job.get("summary") or {}).get("statement_file") or "")).strip(),
    }
    _save_last_sync_status(
        {
            "job_id": str(job.get("id") or ""),
            "status": "cancelled",
            "stage": "cancelled",
            "message": summary["message"],
            "requested": job.get("requested") or {},
            "artifacts_rel": summary["artifacts_rel"],
            "statement_file": summary["statement_file"],
            "updated_at": now_iso(),
        }
    )
    return _update_bg_job(
        job_id,
        status="cancelled",
        stage="cancelled",
        message=summary["message"],
        summary=summary,
        result_summary=_build_action_result_summary(
            tone="warning",
            title="Live Sync Cancelled",
            happened="The sync run was cancelled manually.",
            changed="No further result from this run will update the workspace card.",
            next_action="Retry once the broker page is responsive again.",
            actions=[
                {
                    "label": "Open Live Sync",
                    "href": "/trades/upload/statement?ws=live",
                    "kind": "primary",
                }
            ],
        ),
    )


def _force_reset_sync_job(job_id: str) -> Dict[str, Any]:
    job = _get_bg_job(job_id)
    if not job:
        return {}
    status = str(job.get("status") or "").strip().lower()
    stage = str(job.get("stage") or "").strip().lower()
    resettable_failed_startup = status == "failed" and stage == "system_resource"
    if status in {"success", "debug_only", "cancelled"} or (
        status == "failed" and not resettable_failed_startup
    ):
        return job
    _sync_cancel_event(job_id).set()
    vanquish_live_sync.reset_browser_boot_lane()
    summary = {
        "message": "Sync lane force-reset. Late output from the prior run will be ignored.",
        "warn_count": 0,
        "error_count": max(1, int(((job.get("summary") or {}).get("error_count")) or 0)),
        "inserted": int(((job.get("summary") or {}).get("inserted")) or 0),
        "artifacts_rel": list(((job.get("summary") or {}).get("artifacts_rel") or []))[:20],
        "statement_file": str(((job.get("summary") or {}).get("statement_file") or "")).strip(),
    }
    _save_last_sync_status(
        {
            "job_id": str(job.get("id") or ""),
            "status": "cancelled",
            "stage": "reset_required",
            "message": summary["message"],
            "stage_help": SYNC_STAGE_HELP.get("reset_required", ""),
            "requested": job.get("requested") or {},
            "artifacts_rel": summary["artifacts_rel"],
            "statement_file": summary["statement_file"],
            "updated_at": now_iso(),
        }
    )
    return _update_bg_job(
        job_id,
        status="cancelled",
        stage="reset_required",
        message=summary["message"],
        summary=summary,
        result_summary=_build_action_result_summary(
            tone="warning",
            title="Live Sync Lane Reset",
            happened="The active sync lock was cleared after the lane appeared hung.",
            changed="No further result from that run will update the workspace card.",
            next_action="Start one fresh sync run. If it hangs again, open the latest artifacts and inspect the login stage.",
            actions=[
                {
                    "label": "Open Live Sync",
                    "href": "/trades/upload/statement?ws=live",
                    "kind": "primary",
                }
            ],
        ),
    )


def _build_action_result_summary(
    *,
    tone: str,
    title: str,
    happened: str,
    changed: Optional[str] = None,
    warnings: Optional[List[str]] = None,
    next_action: str = "",
    metrics: Optional[List[Dict[str, str]]] = None,
    actions: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    return {
        "tone": str(tone or "info"),
        "title": str(title or "Action Summary"),
        "happened": str(happened or "").strip(),
        "changed": str(changed or "").strip(),
        "warnings": [str(x).strip() for x in (warnings or []) if str(x).strip()],
        "next_action": str(next_action or "").strip(),
        "metrics": [
            {"label": str(m.get("label") or "").strip(), "value": str(m.get("value") or "").strip()}
            for m in (metrics or [])
            if str(m.get("label") or "").strip()
        ],
        "actions": [
            {
                "label": str(a.get("label") or "").strip(),
                "href": str(a.get("href") or "").strip(),
                "kind": str(a.get("kind") or "").strip() or "default",
            }
            for a in (actions or [])
            if str(a.get("label") or "").strip() and str(a.get("href") or "").strip()
        ],
    }


def _render_action_result_summary(summary: Dict[str, Any]) -> str:
    return render_template("partials/action_result_summary.html", summary=summary)


def _job_response_payload(job: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(job or {})
    payload["created_at_human"] = _humanize_et_timestamp(str(payload.get("created_at") or ""))
    payload["updated_at_human"] = _humanize_et_timestamp(str(payload.get("updated_at") or ""))
    summary = payload.get("result_summary")
    if isinstance(summary, dict) and summary:
        payload["result_html"] = _render_action_result_summary(summary)
    else:
        payload["result_html"] = ""
    return payload


_AUTO_SYNC_THREAD_STARTED = False
_AUTO_SYNC_THREAD_LOCK = threading.Lock()
_AUTO_BACKUP_THREAD_STARTED = False
_AUTO_BACKUP_THREAD_LOCK = threading.Lock()
AUTO_RULE_BREAK_20_TAG = "no-cut-20-loss"


def _load_playbook_config() -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "enabled": False,
        "min_checklist_score": 0,
        "max_size_pct": 100.0,
        "blocked_time_blocks": [],
        "require_positive_setup_expectancy": False,
        "require_critical_checklist": False,
        "critical_items": ["Bias Confirmed", "Risk Defined", "Stop Planned"],
    }
    try:
        with open(_playbook_config_path(), "r", encoding="utf-8") as f:
            parsed = json.load(f)
            if isinstance(parsed, dict):
                cfg.update(parsed)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    cfg["enabled"] = bool(cfg.get("enabled"))
    cfg["min_checklist_score"] = max(0, min(100, int(cfg.get("min_checklist_score") or 0)))
    cfg["max_size_pct"] = max(1.0, min(100.0, float(cfg.get("max_size_pct") or 100.0)))
    raw_blocks = cfg.get("blocked_time_blocks")
    if isinstance(raw_blocks, list):
        cfg["blocked_time_blocks"] = [str(x).strip() for x in raw_blocks if str(x).strip()]
    else:
        cfg["blocked_time_blocks"] = []
    cfg["require_positive_setup_expectancy"] = bool(cfg.get("require_positive_setup_expectancy"))
    cfg["require_critical_checklist"] = bool(cfg.get("require_critical_checklist"))
    raw_items = cfg.get("critical_items")
    if isinstance(raw_items, list):
        items = [str(x).strip() for x in raw_items if str(x).strip()]
    else:
        items = []
    cfg["critical_items"] = items or ["Bias Confirmed", "Risk Defined", "Stop Planned"]
    return cfg


def _save_playbook_config(cfg: Dict[str, Any]) -> None:
    os.makedirs(_upload_dir(), exist_ok=True)
    with open(_playbook_config_path(), "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)


def _entry_time_block(entry_time: str) -> str:
    raw = (entry_time or "").strip()
    if not raw:
        return ""
    for fmt in ("%I:%M %p", "%H:%M"):
        try:
            dt = datetime.strptime(raw, fmt)
            h = dt.hour
            m = dt.minute
            if h == 9 and 30 <= m < 60:
                return "09:30-10:00"
            if 10 <= h < 11:
                return "10:00-11:00"
            if 11 <= h < 12:
                return "11:00-12:00"
            if 12 <= h < 13:
                return "12:00-13:00"
            if 13 <= h < 14:
                return "13:00-14:00"
            if 14 <= h < 15:
                return "14:00-15:00"
            if 15 <= h < 16:
                return "15:00-16:00"
            return f"{h:02d}:00-{(h+1)%24:02d}:00"
        except ValueError:
            continue
    return ""


def _setup_expectancy_map() -> Dict[str, float]:
    rows = analytics_repo.fetch_analytics_rows()
    grouped = analytics_repo.group_table(rows, "setup_tag")
    out: Dict[str, float] = {}
    for r in grouped:
        key = str(r.get("k") or "").strip()
        if not key:
            continue
        out[key] = float(r.get("expectancy") or 0.0)
    return out


def _merge_auto_rule_break_tags(
    *, entry_price: Optional[float], exit_price: Optional[float], existing_tags: str
) -> str:
    tags = [t.strip() for t in str(existing_tags or "").split(",") if t.strip()]
    tag_set = {t.lower(): t for t in tags}
    try:
        entry = float(entry_price) if entry_price is not None else None
        exit_ = float(exit_price) if exit_price is not None else None
    except (TypeError, ValueError):
        entry = None
        exit_ = None
    if entry and entry > 0 and exit_ is not None:
        loss_pct = ((exit_ - entry) / entry) * 100.0
        if loss_pct <= -20.0 and AUTO_RULE_BREAK_20_TAG.lower() not in tag_set:
            tags.append(AUTO_RULE_BREAK_20_TAG)
    dedup: List[str] = []
    seen: set[str] = set()
    for t in tags:
        k = t.lower()
        if not k or k in seen:
            continue
        dedup.append(t)
        seen.add(k)
    return ", ".join(dedup)


def _playbook_violations(
    *,
    cfg: Dict[str, Any],
    setup_tag: str,
    checklist_score: Optional[int],
    entry_time: str,
    total_spent: float,
    balance: float,
    critical_items_checked: Optional[List[str]] = None,
) -> List[str]:
    if not cfg.get("enabled"):
        return []
    violations: List[str] = []
    score = int(checklist_score or 0)
    min_score = int(cfg.get("min_checklist_score") or 0)
    if min_score > 0 and score < min_score:
        violations.append(f"Checklist score {score} is below minimum {min_score}.")
    block = _entry_time_block(entry_time)
    blocked = {str(x).strip() for x in (cfg.get("blocked_time_blocks") or []) if str(x).strip()}
    if block and block in blocked:
        violations.append(f"Time block {block} is blocked by playbook.")
    max_size_pct = float(cfg.get("max_size_pct") or 100.0)
    allowed = max(0.0, float(balance) * (max_size_pct / 100.0))
    if total_spent > allowed:
        violations.append(
            f"Position size {money(total_spent)} exceeds cap {money(allowed)} ({max_size_pct:.1f}% of balance)."
        )
    if cfg.get("require_positive_setup_expectancy"):
        setup = (setup_tag or "").strip()
        exp_map = _setup_expectancy_map()
        exp = float(exp_map.get(setup, 0.0))
        if exp <= 0:
            violations.append(
                f"Setup {setup or 'Unlabeled'} expectancy is not positive ({money(exp)})."
            )
    if cfg.get("require_critical_checklist"):
        required = [
            str(x).strip() for x in (cfg.get("critical_items") or []) if str(x).strip()
        ] or ["Bias Confirmed", "Risk Defined", "Stop Planned"]
        checked = {str(x).strip() for x in (critical_items_checked or []) if str(x).strip()}
        missing = [item for item in required if item not in checked]
        if missing:
            violations.append("Missing critical checklist items: " + ", ".join(missing[:5]) + ".")
    return violations


def _trade_gate_setting_key(day: str) -> str:
    return f"trade_gate::{str(day or '').strip()}"


def _load_trade_gate(day: str) -> Dict[str, Any]:
    raw = str(app_runtime.get_setting_value(_trade_gate_setting_key(day), "") or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _save_trade_gate(day: str, payload: Dict[str, Any]) -> None:
    app_runtime.set_setting_value(_trade_gate_setting_key(day), json.dumps(payload))


def _trade_gate_required(day: str) -> bool:
    return len(fetch_trades(d=day, q="")) == 0 and not bool(_load_trade_gate(day).get("passed"))


def _trade_gate_values(source: Dict[str, Any], saved: Dict[str, Any]) -> Dict[str, str]:
    return {
        "setup_type": str(source.get("gate_setup_type") or saved.get("setup_type") or "").strip(),
        "invalidation": str(
            source.get("gate_invalidation") or saved.get("invalidation") or ""
        ).strip(),
        "max_risk": str(source.get("gate_max_risk") or saved.get("max_risk") or "").strip(),
        "focus": str(source.get("gate_focus") or saved.get("focus") or "").strip(),
    }


def _trade_gate_checks(source: Dict[str, Any], saved: Dict[str, Any]) -> Dict[str, bool]:
    return {
        "market_ready": str(
            source.get("gate_market_ready") or ("1" if saved.get("market_ready") else "")
        ).strip()
        == "1",
        "macro_clear": str(
            source.get("gate_macro_clear") or ("1" if saved.get("macro_clear") else "")
        ).strip()
        == "1",
        "risk_confirmed": str(
            source.get("gate_risk_confirmed") or ("1" if saved.get("risk_confirmed") else "")
        ).strip()
        == "1",
    }


def _trade_gate_errors(values: Dict[str, str], checks: Dict[str, bool]) -> List[str]:
    errors: List[str] = []
    if not values.get("setup_type"):
        errors.append("Choose the setup you are actually trading.")
    if not values.get("invalidation"):
        errors.append("Write the invalidation before entry.")
    if not values.get("max_risk"):
        errors.append("Set the max risk for the trade.")
    if not checks.get("market_ready"):
        errors.append("Confirm market structure is aligned.")
    if not checks.get("macro_clear"):
        errors.append("Confirm you are clear of the macro window.")
    if not checks.get("risk_confirmed"):
        errors.append("Confirm size and max loss are defined before entry.")
    return errors


def _trade_gate_viewmodel(day: str, source: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    source = source or {}
    saved = _load_trade_gate(day)
    required = _trade_gate_required(day)
    passed = bool(saved.get("passed")) and not required
    values = _trade_gate_values(source, saved)
    checks = _trade_gate_checks(source, saved)
    return {
        "day": day,
        "required": required,
        "passed": passed,
        "values": values,
        "checks": checks,
        "passed_label": str(saved.get("passed_label") or ""),
        "summary": (
            f"{saved.get('setup_type', 'Setup')} · risk {saved.get('max_risk', '—')} · {saved.get('passed_label', '')}"
            if passed
            else "Complete this once before the first manual trade of the day."
        ),
    }


def _render_manual_trade_entry_form(
    *,
    pb_cfg: Dict[str, Any],
    values: Optional[Dict[str, Any]] = None,
    gate_error: str = "",
) -> str:
    values = values or {}
    trade_date = str(values.get("trade_date") or today_iso()).strip() or today_iso()
    strategy_options = [dict(r) for r in strategies_repo.fetch_strategies()]
    gate = _trade_gate_viewmodel(trade_date, values)
    content = render_template(
        "trades/manual_trade_entry.html",
        today=today_iso(),
        values=values,
        gate=gate,
        gate_error=gate_error,
        strategy_options=strategy_options,
        critical_items=pb_cfg.get("critical_items")
        or ["Bias Confirmed", "Risk Defined", "Stop Planned"],
        selected_critical_items=(
            [str(x).strip() for x in values.getlist("critical_item") if hasattr(values, "getlist")]
            if hasattr(values, "getlist")
            else [str(x).strip() for x in (values.get("critical_item") or []) if str(x).strip()]
        ),
    )
    return render_page(content, active="trades")


def _keyring_client():
    try:
        import keyring  # type: ignore

        try:
            backend = keyring.get_keyring()
            priority = float(getattr(backend, "priority", 0) or 0)
            if priority <= 0:
                return None
        except Exception:
            return None
        return keyring
    except Exception:
        return None


def _keychain_entry_name(username: str) -> str:
    u = (username or "").strip().lower()
    return f"vanquish::{u or 'default'}"


def _fallback_fernet():
    try:
        from cryptography.fernet import Fernet  # type: ignore
    except Exception:
        return None
    raw_key = (os.environ.get("AUTO_SYNC_PASSWORD_FALLBACK_KEY") or "").strip()
    if raw_key:
        try:
            return Fernet(raw_key.encode("utf-8"))
        except Exception:
            return None
    secret = app_runtime.load_or_create_secret_key().strip()
    if not secret:
        return None
    digest = hashlib.sha256(f"mccain-auto-sync::{secret}".encode("utf-8")).digest()
    try:
        return Fernet(base64.urlsafe_b64encode(digest))
    except Exception:
        return None


def _encrypt_fallback_password(raw: str) -> str:
    f = _fallback_fernet()
    if f is None:
        return ""
    try:
        return f.encrypt((raw or "").encode("utf-8")).decode("utf-8")
    except Exception:
        return ""


def _decrypt_fallback_password(token: str) -> str:
    if not token:
        return ""
    f = _fallback_fernet()
    if f is None:
        return ""
    try:
        return f.decrypt(token.encode("utf-8")).decode("utf-8")
    except Exception:
        return ""


def _get_auto_sync_password(cfg: Dict[str, Any]) -> str:
    username = str(cfg.get("username") or "")
    kr = _keyring_client()
    if kr is not None and username:
        try:
            pw = kr.get_password(BROKER_KEYCHAIN_SERVICE, _keychain_entry_name(username))
            if pw:
                return str(pw)
        except Exception:
            pass
    enc = str(cfg.get("password_enc") or "")
    if enc:
        dec = _decrypt_fallback_password(enc)
        if dec:
            return dec
    # Legacy fallback for existing installs.
    return str(cfg.get("password") or "")


def _set_auto_sync_password(username: str, password: str) -> bool:
    kr = _keyring_client()
    if kr is None:
        return False
    try:
        kr.set_password(BROKER_KEYCHAIN_SERVICE, _keychain_entry_name(username), password)
        return True
    except Exception:
        return False


def _clear_auto_sync_password(username: str) -> bool:
    kr = _keyring_client()
    if kr is None:
        return False
    try:
        kr.delete_password(BROKER_KEYCHAIN_SERVICE, _keychain_entry_name(username))
        return True
    except Exception:
        return False


def _load_broker_sync_config() -> Dict[str, str]:
    defaults = {
        "base_url": os.environ.get("VANQUISH_BASE_URL", "https://trade.vanquishtrader.com"),
        "wl": os.environ.get("VANQUISH_WL", "vanquishtrader"),
        "account": os.environ.get("VANQUISH_ACCOUNT", ""),
        "time_zone": os.environ.get("VANQUISH_TIME_ZONE", "America/New_York"),
        "date_locale": os.environ.get("VANQUISH_DATE_LOCALE", "en-US"),
        "report_locale": os.environ.get("VANQUISH_REPORT_LOCALE", "en"),
        "username": "",
        "password": "",
        "password_enc": "",
    }
    try:
        with open(_broker_sync_config_path(), "r", encoding="utf-8") as f:
            parsed = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        parsed = {}
    for key in defaults:
        val = parsed.get(key, defaults[key])
        defaults[key] = str(val).strip() if val is not None else defaults[key]
    defaults["keyring_available"] = _keyring_client() is not None
    defaults["password_stored"] = bool(_get_auto_sync_password(defaults))
    return defaults


def _safe_write_json(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp_path = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp_path, path)
        return
    except PermissionError:
        # Recover from stale bind-mounted files with incompatible ownership.
        try:
            os.remove(path)
        except OSError:
            pass
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass


def _save_broker_sync_config(data: Dict[str, str]) -> None:
    to_save = dict(data)
    to_save.pop("keyring_available", None)
    to_save.pop("password_stored", None)
    _safe_write_json(_broker_sync_config_path(), to_save)


def _humanize_et_timestamp(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.astimezone(ZoneInfo("America/New_York")).strftime("%b %d, %Y %I:%M %p ET")
    except Exception:
        return text


def _read_last_sync_status() -> Dict[str, Any]:
    try:
        with open(_broker_sync_status_path(), "r", encoding="utf-8") as f:
            parsed = json.load(f)
            return parsed if isinstance(parsed, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _load_last_sync_status() -> Dict[str, Any]:
    parsed = _read_last_sync_status()
    if not parsed:
        return {}
    raw_stage = str(parsed.get("stage") or "").strip().lower()
    if raw_stage in {"", "unknown"}:
        normalized_stage = _classify_sync_stage(
            str(parsed.get("message") or ""),
            raw_stage or "unknown",
        )
        if normalized_stage != (raw_stage or "unknown"):
            parsed["stage"] = normalized_stage
            parsed["stage_help"] = SYNC_STAGE_HELP.get(
                normalized_stage,
                str(parsed.get("stage_help") or ""),
            )
    status = str(parsed.get("status") or "").strip().lower()
    updated_epoch = _parse_iso_epoch(str(parsed.get("updated_at") or ""))
    if status in {"queued", "running"} and updated_epoch is not None:
        stale_after = _sync_job_stale_after(status, str(parsed.get("stage") or ""))
        if (time.time() - updated_epoch) >= stale_after:
            message = _stale_sync_job_message(str(parsed.get("stage") or "start"))
            parsed = {
                **parsed,
                "status": "failed",
                "stage": "stale",
                "message": message,
                "stage_help": SYNC_STAGE_HELP.get("stale", ""),
                "updated_at": now_iso(),
            }
            _save_last_sync_status(parsed)
    parsed["updated_at_human"] = _humanize_et_timestamp(str(parsed.get("updated_at") or ""))
    return parsed


def _reset_browser_boot_lane_after_resource_failure(payload: Dict[str, Any]) -> None:
    status = str(payload.get("status") or "").strip().lower()
    stage = str(payload.get("stage") or "").strip().lower()
    if status != "failed" or stage != "system_resource":
        return
    message = str(payload.get("message") or "").strip().lower()
    if not (
        "chromium" in message
        or "browser boot" in message
        or "startup resources" in message
        or "resource temporarily unavailable" in message
        or "can't start new thread" in message
        or "cannot start new thread" in message
        or "pthread_create" in message
    ):
        return
    vanquish_live_sync.reset_browser_boot_lane()


def _save_last_sync_status(payload: Dict[str, Any]) -> None:
    _reset_browser_boot_lane_after_resource_failure(payload)
    _safe_write_json(_broker_sync_status_path(), payload)
    status = str(payload.get("status") or "").strip().lower()
    if status not in {"success", "failed", "debug_only", "cancelled"}:
        return
    history = _load_sync_history()
    requested = payload.get("requested") if isinstance(payload.get("requested"), dict) else {}
    source = str(requested.get("source") or "live_manual").strip() or "live_manual"
    mode = str(requested.get("mode") or "").strip()
    history.append(
        {
            "updated_at": str(payload.get("updated_at") or now_iso()),
            "status": status,
            "stage": str(payload.get("stage") or ""),
            "message": str(payload.get("message") or ""),
            "source": source,
            "mode": mode,
            "requested": requested,
            "inserted": payload.get("inserted"),
            "duration_sec": (
                float(payload.get("duration_sec"))
                if payload.get("duration_sec") is not None
                else None
            ),
        }
    )
    if len(history) > SYNC_HISTORY_MAX:
        history = history[-SYNC_HISTORY_MAX:]
    _safe_write_json(_broker_sync_history_path(), history)
    if status == "failed":
        streak = 0
        for e in reversed(history):
            s = str(e.get("status") or "").lower()
            if s == "failed":
                streak += 1
            elif s in {"success", "debug_only"}:
                break
        if streak >= max(1, NOTIFY_FAIL_STREAK):
            state = _load_notify_history()
            last_streak = int(state.get("last_fail_streak_notified", 0) or 0)
            if streak > last_streak:
                _emit_notification(
                    "sync_fail_streak",
                    "Sync failure streak",
                    f"Sync has failed {streak} times in a row. Latest stage: {payload.get('stage') or 'unknown'}.",
                    {
                        "streak": streak,
                        "stage": payload.get("stage"),
                        "status": payload.get("status"),
                    },
                )
                state = _load_notify_history()
                state["last_fail_streak_notified"] = streak
                _save_notify_history(state)
    elif status in {"success", "debug_only"}:
        state = _load_notify_history()
        if state.get("last_fail_streak_notified"):
            state["last_fail_streak_notified"] = 0
            _save_notify_history(state)


def _load_sync_history() -> List[Dict[str, Any]]:
    try:
        with open(_broker_sync_history_path(), "r", encoding="utf-8") as f:
            parsed = json.load(f)
            return [x for x in parsed if isinstance(x, dict)] if isinstance(parsed, list) else []
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return []


def _load_admin_audit() -> List[Dict[str, Any]]:
    for path in _admin_audit_paths(for_read=True):
        try:
            with open(path, "r", encoding="utf-8") as f:
                parsed = json.load(f)
                return (
                    [x for x in parsed if isinstance(x, dict)] if isinstance(parsed, list) else []
                )
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            continue
    return []


def record_admin_audit(
    action: str, details: Optional[Dict[str, Any]] = None, actor: str = ""
) -> None:
    rows = _load_admin_audit()
    rows.append(
        {
            "at": now_iso(),
            "action": str(action or "").strip() or "unknown_action",
            "actor": str(actor or "").strip() or _alerts_actor(),
            "details": details or {},
        }
    )
    if len(rows) > 500:
        rows = rows[-500:]
    if not _save_admin_audit(rows):
        current_app.logger.error("Admin audit write failed; event kept in-memory only: %s", action)


def _save_admin_audit(rows: List[Dict[str, Any]]) -> bool:
    errors: List[str] = []
    for path in _admin_audit_paths(for_read=False):
        try:
            _safe_write_json(path, rows)
            return True
        except OSError as exc:
            errors.append(f"{path}: {exc}")
    if errors:
        current_app.logger.error("Admin audit write failed: %s", " | ".join(errors))
    return False


def _admin_audit_paths(for_read: bool = True) -> List[str]:
    fallback = os.path.join(tempfile.gettempdir(), "mccain-capital", ".admin_audit_log.json")
    ordered = (_admin_audit_log_path(), fallback)
    if for_read and os.path.isfile(fallback):
        ordered = (fallback, _admin_audit_log_path())
    paths: List[str] = []
    for path in ordered:
        p = os.path.abspath(str(path))
        if p and p not in paths:
            paths.append(p)
    return paths


def _audit_action_meta(action: str) -> Dict[str, str]:
    key = str(action or "").strip()
    meta = _AUDIT_ACTION_META.get(key)
    if meta:
        return dict(meta)
    return {
        "label": key.replace("_", " ").strip().title() or "Unknown Event",
        "group": "other",
    }


def _audit_summary_text(row: Dict[str, Any]) -> str:
    action = str(row.get("action") or "").strip()
    details = row.get("details") if isinstance(row.get("details"), dict) else {}
    if action in {"backup_created", "backup_restored_from_center", "manual_backup_restored"}:
        return str(details.get("file") or "Snapshot file updated.")
    if action == "live_data_cleared":
        return "Database and live uploads were cleared while backup archives were preserved."
    if action == "backup_deleted":
        return str(details.get("file") or "Backup file deleted.")
    if action == "backup_failed":
        return str(details.get("error") or "Backup failed.")
    if action == "integrity_check_run":
        return (
            f"Issues={int(details.get('issues') or 0)} · "
            f"Orphans={int(details.get('orphan_reviews') or 0)} · "
            f"Missing Balances={int(details.get('missing_balance') or 0)}"
        )
    if action == "trades_rebuild_reviews":
        return (
            f"Updated {int(details.get('rebuilt') or 0)} review(s), "
            f"skipped {int(details.get('skipped_existing') or 0)} existing."
        )
    if action in {"dashboard_recompute_balances", "rollback_import_batch"}:
        return json.dumps(details, separators=(", ", ": "))
    if action.startswith("ops_alert_"):
        return json.dumps(details, separators=(", ", ": "))
    if action == "auto_backup_config_saved":
        times = details.get("run_times_et") if isinstance(details.get("run_times_et"), list) else []
        return f"{int(details.get('frequency_hours') or 0)}h cadence · {', '.join(str(x) for x in times)}"
    return json.dumps(details, separators=(", ", ": ")) if details else "No extra details."


def _load_system_activity(limit: int, category: str = "all") -> List[Dict[str, Any]]:
    rows = list(reversed(_load_admin_audit()))
    selected = str(category or "all").strip().lower() or "all"
    if selected != "all":
        rows = [
            r
            for r in rows
            if _audit_action_meta(str(r.get("action") or "")).get("group") == selected
        ]
    out: List[Dict[str, Any]] = []
    for row in rows[: max(1, int(limit or 1))]:
        action = str(row.get("action") or "")
        meta = _audit_action_meta(action)
        out.append(
            {
                **row,
                "at_human": _humanize_et_timestamp(str(row.get("at") or "")),
                "label": meta["label"],
                "group": meta["group"],
                "summary": _audit_summary_text(row),
            }
        )
    return out


def _new_import_batch_id(prefix: str = "imp") -> str:
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{prefix}_{stamp}_{uuid4().hex[:10]}"


def _sync_reliability_summary(history: List[Dict[str, Any]], days: int = 30) -> Dict[str, Any]:
    now = datetime.now(ZoneInfo("America/New_York"))
    cutoff = now - timedelta(days=max(1, int(days)))
    recent: List[Dict[str, Any]] = []
    for e in history:
        raw = str(e.get("updated_at") or "").strip()
        if not raw:
            continue
        try:
            ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=ZoneInfo("America/New_York"))
        ts = ts.astimezone(ZoneInfo("America/New_York"))
        if ts >= cutoff:
            row = dict(e)
            row["_ts"] = ts
            recent.append(row)
    recent.sort(key=lambda x: x["_ts"])

    def category(entry: Dict[str, Any]) -> str:
        status = str(entry.get("status") or "").strip().lower()
        requested = entry.get("requested") if isinstance(entry.get("requested"), dict) else {}
        if status == "debug_only" or bool(requested.get("debug_only")):
            return "diagnostic_only"
        if status == "success":
            return "success"
        if status == "failed":
            return "failed"
        if status == "cancelled":
            return "cancelled"
        return "unknown"

    categories = [category(entry) for entry in recent]
    success = categories.count("success")
    failed = categories.count("failed")
    cancelled = categories.count("cancelled")
    diagnostic_only = categories.count("diagnostic_only")
    unknown = categories.count("unknown")
    import_attempts = success + failed
    attempts = len(recent)
    success_rate = (success / import_attempts * 100.0) if import_attempts else None
    durations = [
        float(e.get("duration_sec"))
        for e in recent
        if isinstance(e.get("duration_sec"), (int, float))
    ]
    avg_duration_sec = (sum(durations) / len(durations)) if durations else None
    fail_stage_counts: Dict[str, int] = {}
    for e in recent:
        if str(e.get("status")) != "failed":
            continue
        st = str(e.get("stage") or "unknown")
        fail_stage_counts[st] = fail_stage_counts.get(st, 0) + 1
    top_failure_stage = None
    top_failure_count = 0
    if fail_stage_counts:
        top_failure_stage, top_failure_count = sorted(
            fail_stage_counts.items(), key=lambda kv: kv[1], reverse=True
        )[0]
    by_source: Dict[str, Dict[str, int]] = {}
    for e in recent:
        src = str(e.get("source") or "unknown")
        bucket = by_source.setdefault(
            src,
            {
                "attempts": 0,
                "success": 0,
                "failed": 0,
                "cancelled": 0,
                "diagnostic_only": 0,
                "unknown": 0,
            },
        )
        bucket["attempts"] += 1
        bucket[category(e)] += 1
    return {
        "days": int(days),
        "attempts": attempts,
        "import_attempts": import_attempts,
        "success": success,
        "failed": failed,
        "cancelled": cancelled,
        "diagnostic_only": diagnostic_only,
        "unknown": unknown,
        "success_rate": success_rate,
        "avg_duration_sec": avg_duration_sec,
        "top_failure_stage": top_failure_stage,
        "top_failure_count": int(top_failure_count),
        "by_source": by_source,
        "recent": list(reversed(recent[-8:])),
    }


def _load_import_history() -> List[Dict[str, Any]]:
    try:
        with open(_broker_import_history_path(), "r", encoding="utf-8") as f:
            parsed = json.load(f)
            return [x for x in parsed if isinstance(x, dict)] if isinstance(parsed, list) else []
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return []


def _append_import_history(entry: Dict[str, Any]) -> None:
    history = _load_import_history()
    history.append(entry)
    if len(history) > IMPORT_HISTORY_MAX:
        history = history[-IMPORT_HISTORY_MAX:]
    _safe_write_json(_broker_import_history_path(), history)


def _record_import_batch(
    *,
    batch_id: str,
    source: str,
    mode: str,
    report: Optional[Dict[str, Any]],
    status: str = "success",
    message: str = "",
) -> None:
    rp = report or {}
    _append_import_history(
        {
            "updated_at": now_iso(),
            "batch_id": batch_id or "",
            "source": source,
            "mode": mode,
            "status": status,
            "message": message,
            "inserted_trades": int(rp.get("inserted_trades") or 0),
            "duplicates_skipped": int(rp.get("duplicates_skipped") or 0),
            "open_contracts": int(rp.get("open_contracts") or 0),
            "errors_count": int(rp.get("errors_count") or 0),
            "warnings_count": int(rp.get("warnings_count") or 0),
            "statement_ending_balance": rp.get("statement_ending_balance"),
            "ledger_ending_balance": rp.get("ledger_ending_balance"),
            "balance_delta": rp.get("balance_delta"),
            "rolled_back": False,
            "rolled_back_at": "",
        }
    )
    delta = rp.get("balance_delta")
    if isinstance(delta, (int, float)) and abs(float(delta)) > RECONCILE_GATE_MAX_DELTA:
        history = _load_import_history()
        now = datetime.now(ZoneInfo("America/New_York"))
        cutoff = now - timedelta(days=7)
        hits = 0
        for e in history:
            raw = str(e.get("updated_at") or "")
            try:
                ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except Exception:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=ZoneInfo("America/New_York"))
            ts = ts.astimezone(ZoneInfo("America/New_York"))
            if ts < cutoff:
                continue
            bd = e.get("balance_delta")
            if isinstance(bd, (int, float)) and abs(float(bd)) > RECONCILE_GATE_MAX_DELTA:
                hits += 1
        if hits >= 2:
            _emit_notification(
                "drift_recurrence",
                "Ledger drift recurrence",
                f"Detected {hits} high-delta import batches in the last 7 days.",
                {"hits": hits, "threshold": RECONCILE_GATE_MAX_DELTA, "batch_id": batch_id},
            )


def _reconcile_summary(import_history: List[Dict[str, Any]], days: int = 30) -> Dict[str, Any]:
    now = datetime.now(ZoneInfo("America/New_York"))
    cutoff = now - timedelta(days=max(1, int(days)))
    recent: List[Dict[str, Any]] = []
    for e in import_history:
        raw = str(e.get("updated_at") or "").strip()
        if not raw:
            continue
        try:
            ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=ZoneInfo("America/New_York"))
        ts = ts.astimezone(ZoneInfo("America/New_York"))
        if ts >= cutoff:
            row = dict(e)
            row["_ts"] = ts
            recent.append(row)
    recent.sort(key=lambda x: x["_ts"])
    batches = len(recent)
    inserted = sum(int(r.get("inserted_trades") or 0) for r in recent)
    unresolved = 0
    for r in recent:
        open_contracts = int(r.get("open_contracts") or 0)
        errors = int(r.get("errors_count") or 0)
        delta = r.get("balance_delta")
        delta_abs = abs(float(delta)) if isinstance(delta, (int, float)) else 0.0
        if open_contracts > 0 or errors > 0 or delta_abs > 1.0:
            unresolved += 1
    clean = max(0, batches - unresolved)
    clean_rate = (clean / batches * 100.0) if batches else 0.0
    return {
        "days": int(days),
        "batches": batches,
        "inserted": inserted,
        "unresolved": unresolved,
        "clean_rate": clean_rate,
        "recent": list(reversed(recent[-12:])),
    }


def _mark_import_batch_rolled_back(batch_id: str) -> None:
    if not batch_id:
        return
    history = _load_import_history()
    changed = False
    for e in history:
        if str(e.get("batch_id") or "") == batch_id:
            e["rolled_back"] = True
            e["rolled_back_at"] = now_iso()
            changed = True
    if changed:
        _safe_write_json(_broker_import_history_path(), history)


def rollback_import_batch() -> Any:
    if request.method != "POST":
        return redirect(url_for("trades_upload_pdf", ws="reconcile"))
    if not auth.auth_enabled():
        flash("Enable authentication to use rollback-by-batch.", "warn")
        return redirect(url_for("trades_upload_pdf", ws="reconcile"))
    if not auth.is_authenticated():
        abort(403)
    batch_id = (request.form.get("batch_id") or "").strip()
    if not batch_id:
        flash("Missing batch ID for rollback.", "warn")
        return redirect(url_for("trades_upload_pdf", ws="reconcile"))
    with db() as conn:
        rows = conn.execute(
            "SELECT id FROM trades WHERE import_batch_id = ?", (batch_id,)
        ).fetchall()
        trade_ids = [int(r["id"]) for r in rows if r["id"] is not None]
        if not trade_ids:
            _mark_import_batch_rolled_back(batch_id)
            flash(f"No trades found for batch {batch_id}.", "warn")
            return redirect(url_for("trades_upload_pdf", ws="reconcile"))
        marks = ",".join(["?"] * len(trade_ids))
        conn.execute(f"DELETE FROM trade_reviews WHERE trade_id IN ({marks})", trade_ids)
        deleted_trades = int(
            conn.execute("DELETE FROM trades WHERE import_batch_id = ?", (batch_id,)).rowcount or 0
        )
        conn.commit()
    starting = float(get_setting_float("starting_balance", 50000.0))
    repo.recompute_balances(starting_balance=starting)
    _mark_import_batch_rolled_back(batch_id)
    _emit_notification(
        "batch_rollback",
        "Import batch rolled back",
        f"Rolled back batch {batch_id} and deleted {deleted_trades} trade(s).",
        {"batch_id": batch_id, "deleted_trades": deleted_trades},
    )
    record_admin_audit(
        "rollback_import_batch",
        {"batch_id": batch_id, "deleted_trades": deleted_trades},
    )
    flash(f"Rolled back batch {batch_id} ({deleted_trades} trades).", "success")
    return redirect(url_for("trades_upload_pdf", ws="reconcile"))


def _reconcile_gate_result(report: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    rp = report or {}
    reasons: List[str] = []
    if int(rp.get("errors_count") or 0) > 0:
        reasons.append("Importer reported parse/matching errors.")
    if int(rp.get("open_contracts") or 0) > 0:
        reasons.append("Open contracts remain unmatched.")
    delta = rp.get("balance_delta")
    if isinstance(delta, (int, float)) and abs(float(delta)) > RECONCILE_GATE_MAX_DELTA:
        reasons.append(
            f"Ledger vs statement delta {money(delta)} exceeds threshold {money(RECONCILE_GATE_MAX_DELTA)}."
        )
    return {"blocked": bool(reasons), "reasons": reasons}


def _load_auto_sync_config() -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "enabled": False,
        "run_time_et": "16:15",
        "run_weekends": False,
        "mode": "broker",
        "username": "",
        "base_url": os.environ.get("VANQUISH_BASE_URL", "https://trade.vanquishtrader.com"),
        "wl": os.environ.get("VANQUISH_WL", "vanquishtrader"),
        "account": os.environ.get("VANQUISH_ACCOUNT", ""),
        "time_zone": os.environ.get("VANQUISH_TIME_ZONE", "America/New_York"),
        "date_locale": os.environ.get("VANQUISH_DATE_LOCALE", "en-US"),
        "report_locale": os.environ.get("VANQUISH_REPORT_LOCALE", "en"),
        "headless": True,
        "debug_capture": True,
        "last_run_date": "",
    }
    try:
        with open(_broker_auto_sync_config_path(), "r", encoding="utf-8") as f:
            parsed = json.load(f)
            if not isinstance(parsed, dict):
                return defaults
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return defaults
    merged = defaults.copy()
    merged.update(parsed)
    merged["enabled"] = bool(merged.get("enabled"))
    merged["run_weekends"] = bool(merged.get("run_weekends"))
    merged["headless"] = bool(merged.get("headless", True))
    merged["debug_capture"] = bool(merged.get("debug_capture", True))
    merged["run_time_et"] = str(merged.get("run_time_et") or "16:15")
    merged["password"] = str(merged.get("password") or "")
    merged["password_enc"] = str(merged.get("password_enc") or "")
    merged["keyring_available"] = _keyring_client() is not None
    merged["password_stored"] = bool(_get_auto_sync_password(merged))
    return merged


def _save_auto_sync_config(cfg: Dict[str, Any]) -> None:
    to_save = dict(cfg)
    to_save.pop("keyring_available", None)
    to_save.pop("password_stored", None)
    _safe_write_json(_broker_auto_sync_config_path(), to_save)


def _parse_sync_stage(message: str) -> str:
    text = (message or "").strip()
    prefix = "[stage:"
    if text.startswith(prefix):
        end = text.find("]")
        if end > len(prefix):
            return text[len(prefix) : end].strip()
    return "unknown"


def _strip_stage_prefix(message: str) -> str:
    text = (message or "").strip()
    if text.startswith("[stage:"):
        end = text.find("]")
        if end != -1:
            return text[end + 1 :].strip()
    return text


def _debug_relative(path: str) -> str:
    rel = os.path.relpath(path, _upload_dir())
    return rel.replace("\\", "/")


def _classify_sync_stage(raw_error: str, fallback_stage: str = "unknown") -> str:
    text = str(raw_error or "").strip().lower()
    if (
        "login page instead of statement html" in text
        or "broker login page instead of statement html" in text
        or "received login page" in text
    ):
        return "auth_required"
    if (
        "resource temporarily unavailable" in text
        or "[errno 11]" in text
        or "can't start new thread" in text
        or "cannot start new thread" in text
        or "pthread_create" in text
        or "startup resources are busy" in text
        or "another browser boot is still active" in text
    ):
        return "system_resource"
    if "permission denied" in text or "[errno 13]" in text:
        return "storage_io"
    if "target page, context or browser has been closed" in text:
        return "browser_boot"
    return fallback_stage


def _debug_safe_path(rel: str) -> str:
    clean = (rel or "").replace("\\", "/").lstrip("/")
    abs_path = os.path.abspath(os.path.join(_upload_dir(), clean))
    root = os.path.abspath(_upload_dir())
    if not abs_path.startswith(root + os.sep) and abs_path != root:
        raise ValueError("unsafe path")
    return abs_path


def _render_live_debug_result(
    *,
    folder_rel: str,
    artifacts_rel: List[str],
    warns: List[str],
    error: str = "",
):
    return render_page(
        render_template(
            "trades/live_sync_debug.html",
            folder_rel=folder_rel,
            artifacts_rel=artifacts_rel,
            warns=warns,
            error=error,
        ),
        active="trades",
    )


def _normalize_iso_date(raw: str, fallback: str) -> str:
    v = (raw or "").strip()
    if not v:
        return fallback
    try:
        return datetime.strptime(v, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return fallback


def _coerce_account_id(raw: Any) -> int | None:
    text = str(raw or "").strip()
    if not text or text.lower() in {"all", "none"}:
        return None
    try:
        return int(text)
    except Exception:
        return None


def _selected_account(raw: Any = None) -> Dict[str, Any] | None:
    account_id = _coerce_account_id(raw)
    if account_id:
        return repo.get_account(account_id)
    snapshot = repo.account_scope_snapshot()
    account_id = _coerce_account_id(snapshot.get("account_id"))
    if account_id:
        return repo.get_account(account_id)
    return None


def _require_import_account(raw: Any = None) -> Dict[str, Any] | None:
    account = _selected_account(raw)
    if account:
        return account
    flash("Please select an account before uploading trades.", "warn")
    return None


def _sync_account(raw: Any = None, broker_account_id: str = "") -> Dict[str, Any] | None:
    account = _selected_account(raw)
    if account:
        return account
    normalized = repo.normalize_broker_account_id(broker_account_id)
    if not normalized:
        return None
    for row in repo.list_accounts():
        if repo.normalize_broker_account_id(row.get("broker_account_id")) == normalized:
            return row
    return None


def _import_broker_paste_with_report(
    text: str,
    *,
    account_id: int,
    upload_id: int,
    ending_balance: Optional[float] = None,
    commit: bool = True,
    import_batch_id: str = "",
) -> Tuple[int, List[str], Dict[str, Any]]:
    func = importing.insert_trades_from_broker_paste_with_report
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        sig = None
    kwargs: Dict[str, Any] = {
        "ending_balance": ending_balance,
        "commit": commit,
        "import_batch_id": import_batch_id,
    }
    if sig is None:
        kwargs["account_id"] = account_id
        kwargs["upload_id"] = upload_id
    else:
        params = sig.parameters
        if "account_id" in params:
            kwargs["account_id"] = account_id
        if "upload_id" in params:
            kwargs["upload_id"] = upload_id
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            kwargs["account_id"] = account_id
            kwargs["upload_id"] = upload_id
    return func(text, **kwargs)


def _capture_account_metrics_for_sync(
    *,
    account_id: int,
    broker_account_id: str,
    headless: bool,
    debug_dir: Optional[str],
    progress_cb: Optional[Callable[[str, str], None]],
) -> Dict[str, Any]:
    if account_id <= 0:
        return {"metrics": None, "warns": ["Skipped account metrics: no selected account."]}
    metrics, warns, artifacts_abs, meta = vanquish_live_sync.fetch_account_metrics_via_dashboard(
        account=broker_account_id,
        headless=headless,
        debug_dir=debug_dir,
        progress_cb=progress_cb,
    )
    return {
        "metrics": metrics,
        "warns": warns,
        "artifacts_abs": artifacts_abs,
        "meta": meta,
    }


def _resolve_account_broker_equity(
    *,
    account_id: int,
    broker_account_id: str,
    metrics_result: Dict[str, Any],
    statement_ending_balance: float | None,
    statement_trusted: bool,
) -> Dict[str, Any]:
    result = broker_equity.resolve_refresh(
        account_id=account_id,
        requested_broker_account_id=broker_account_id,
        metrics=metrics_result.get("metrics"),
        metrics_meta=metrics_result.get("meta"),
        statement_balance=statement_ending_balance,
        statement_trusted=statement_trusted,
    )
    result["account_id"] = account_id
    if result.get("updated"):
        record_admin_audit(
            "broker_equity_automatic_update",
            {
                "account_id": account_id,
                "source": result.get("source"),
                "value": result.get("value"),
                "updated_at": result.get("updated_at"),
            },
        )
    return result


def _handle_statement_html_import(
    path: str,
    mode: str,
    source_label: str,
    *,
    account: Dict[str, Any],
    filename: str,
):
    paste_text, balance_val, warns = importing.parse_statement_html_to_broker_paste(path)

    if mode == "broker":
        if not paste_text:
            if balance_val is not None:
                batch_id = _new_import_batch_id("bal")
                upload_id = repo.create_upload(
                    account_id=int(account["id"]),
                    filename=filename,
                    source=source_label,
                    import_batch_id=batch_id,
                )
                importing.insert_balance_snapshot(
                    today_iso(),
                    balance_val,
                    account_id=int(account["id"]),
                    upload_id=upload_id,
                    raw_line=source_label,
                )
                repo.update_account_broker_equity_from_statement(
                    account_id=int(account["id"]),
                    broker_equity=balance_val,
                    source="statement",
                )
                ledger_balance = latest_balance_overall(
                    account_id=int(account["id"]),
                    starting_balance=float(account.get("starting_balance") or 0.0),
                )
                _record_import_batch(
                    batch_id=batch_id,
                    source=source_label,
                    mode="balance",
                    report={
                        "inserted_trades": 0,
                        "duplicates_skipped": 0,
                        "open_contracts": 0,
                        "errors_count": 0,
                        "warnings_count": len(warns or []),
                        "statement_ending_balance": balance_val,
                        "ledger_ending_balance": ledger_balance,
                        "balance_delta": (ledger_balance - float(balance_val)),
                    },
                    status="success",
                    message="No trade rows found; imported statement ending balance snapshot.",
                )
                return render_page(
                    render_template(
                        "trades/import_balance_snapshot_result.html",
                        warns=warns or [],
                        balance_val=balance_val,
                        money=money,
                    ),
                    active="trades",
                )
            return render_page(
                render_template(
                    "trades/import_no_trade_rows.html",
                    warns=warns or [],
                ),
                active="trades",
            )

        batch_id = _new_import_batch_id("stmt")
        upload_id = repo.create_upload(
            account_id=int(account["id"]),
            filename=filename,
            source=source_label,
            import_batch_id=batch_id,
        )
        _, _, pre_report = importing.insert_trades_from_broker_paste_with_report(
            paste_text,
            account_id=int(account["id"]),
            upload_id=upload_id,
            ending_balance=balance_val,
            commit=False,
            import_batch_id=batch_id,
        )
        if RECONCILE_GATE_ENABLED:
            gate = _reconcile_gate_result(pre_report)
            if gate["blocked"]:
                _record_import_batch(
                    batch_id=batch_id,
                    source=source_label,
                    mode="broker",
                    report=pre_report,
                    status="failed",
                    message="Reconciliation gate blocked import.",
                )
                _emit_notification(
                    "reconcile_gate_block",
                    "Reconcile gate blocked import",
                    f"{source_label} broker import blocked by reconcile gate.",
                    {
                        "batch_id": batch_id,
                        "source": source_label,
                        "reasons": gate["reasons"],
                        "balance_delta": pre_report.get("balance_delta"),
                    },
                )
                return render_page(
                    render_template(
                        "trades/import_reconcile_gate_blocked.html",
                        reasons=gate["reasons"],
                        reconciliation_html=_reconciliation_block(pre_report),
                    ),
                    active="trades",
                )

        inserted, errors, report = importing.insert_trades_from_broker_paste_with_report(
            paste_text,
            account_id=int(account["id"]),
            upload_id=upload_id,
            ending_balance=balance_val,
            commit=True,
            import_batch_id=batch_id,
        )
        if balance_val is not None:
            repo.update_account_broker_equity_from_statement(
                account_id=int(account["id"]),
                broker_equity=balance_val,
                source="statement",
            )
        _record_import_batch(
            batch_id=batch_id,
            source=source_label,
            mode="broker",
            report=report,
            status="success",
            message=f"Inserted {inserted} trade(s) from statement HTML.",
        )
        reconciliation_html = _reconciliation_block(report)
        msgs = (warns or []) + (errors or [])

        return render_page(
            render_template(
                "trades/import_html_result.html",
                inserted=inserted,
                msgs=msgs,
                reconciliation_html=reconciliation_html,
                review_day=today_iso(),
            ),
            active="trades",
        )

    if balance_val is None:
        return render_page(
            render_template(
                "trades/import_balance_missing.html",
                warns=warns or [],
            ),
            active="trades",
        )

    batch_id = _new_import_batch_id("bal")
    upload_id = repo.create_upload(
        account_id=int(account["id"]),
        filename=filename,
        source=source_label,
        import_batch_id=batch_id,
    )
    importing.insert_balance_snapshot(
        today_iso(),
        balance_val,
        account_id=int(account["id"]),
        upload_id=upload_id,
        raw_line=source_label,
    )
    repo.update_account_broker_equity_from_statement(
        account_id=int(account["id"]),
        broker_equity=balance_val,
        source="statement",
    )
    _record_import_batch(
        batch_id=batch_id,
        source=source_label,
        mode="balance",
        report={
            "inserted_trades": 0,
            "duplicates_skipped": 0,
            "open_contracts": 0,
            "errors_count": 0,
            "warnings_count": len(warns or []),
            "statement_ending_balance": balance_val,
            "ledger_ending_balance": latest_balance_overall(
                account_id=int(account["id"]),
                starting_balance=float(account.get("starting_balance") or 0.0),
            ),
            "balance_delta": (
                latest_balance_overall(
                    account_id=int(account["id"]),
                    starting_balance=float(account.get("starting_balance") or 0.0),
                )
                - float(balance_val)
            ),
        },
        status="success",
        message="Imported statement ending balance snapshot.",
    )
    return redirect(url_for("trades_page"))


def _reconciliation_block(report: Optional[dict]) -> str:
    if not report:
        return ""
    return render_template(
        "trades/_import_reconciliation.html",
        report=report,
        money=money,
    )


def trade_lockout_state(day_iso: str):
    rc = repo.get_risk_controls()
    return repo.trade_lockout_state(
        day_iso,
        daily_max_loss=float(rc.get("daily_max_loss", 0.0) or 0.0),
        enforce_lockout=int(rc.get("enforce_lockout", 0) or 0),
    )


def trades_update_balance_bases():
    if request.method != "POST":
        return redirect(url_for("trades_page"))
    if auth.auth_enabled() and not auth.is_authenticated():
        abort(403)

    d = (request.args.get("d") or "").strip()
    q = (request.args.get("q") or "").strip()
    mode = (request.form.get("mode") or "").strip().lower()

    if mode == "history":
        raw = (request.form.get("history_starting_balance") or "").strip()
        bal = parse_float(raw)
        if bal is None:
            flash("History balance not updated: enter a valid starting balance.", "warn")
            return redirect(url_for("trades_page", d=d, q=q))
        starting = float(bal)
        app_runtime.set_setting_value("starting_balance", f"{starting:.2f}")
        repo.recompute_balances(starting_balance=starting)
        record_admin_audit(
            "trades_history_balance_updated",
            {"starting_balance": starting},
            actor=auth.effective_username(),
        )
        flash(
            "History starting balance updated and stored row balances recomputed.",
            "success",
        )
        return redirect(url_for("trades_page", d=d, q=q))

    if mode == "scope":
        scope_enabled = request.form.get("scope_enabled") == "1"
        scope_start = _normalize_scope_start_date(request.form.get("scope_start_date") or "")
        scope_label = (request.form.get("scope_label") or "").strip()
        scope_balance_raw = (request.form.get("scope_starting_balance") or "").strip()

        if not scope_enabled:
            repo.clear_account_scope()
            record_admin_audit(
                "trades_scope_balance_updated",
                {"enabled": False},
                actor=auth.effective_username(),
            )
            flash(
                "Active account scope disabled. Trades page remains on all-history basis.",
                "success",
            )
            return redirect(url_for("trades_page", d=d, q=q))

        bal = parse_float(scope_balance_raw)
        try:
            if not scope_start:
                raise ValueError("invalid scope start date")
            if bal is None:
                raise ValueError("invalid scope balance")
            scope_balance = float(bal)
        except Exception:
            flash(
                "Active account not updated: use a valid start date and starting balance.", "warn"
            )
            return redirect(url_for("trades_page", d=d, q=q))

        repo.save_account_scope(scope_start, scope_balance, label=scope_label)
        record_admin_audit(
            "trades_scope_balance_updated",
            {
                "enabled": True,
                "start_date": scope_start,
                "starting_balance": scope_balance,
                "label": scope_label,
            },
            actor=auth.effective_username(),
        )
        flash("Active account basis updated for scoped dashboards and payouts.", "success")
        return redirect(url_for("trades_page", d=d, q=q))

    flash("Balance settings update ignored: invalid mode.", "warn")
    return redirect(url_for("trades_page", d=d, q=q))


def trades_page():
    from mccain_capital.services import trades_page as trades_page_svc

    return trades_page_svc.trades_page()


def get_trade(trade_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()


def trades_duplicate(trade_id: int):
    from mccain_capital.services import trades_mutations as trades_mutations_svc

    return trades_mutations_svc.trades_duplicate(trade_id)


def trades_delete(trade_id: int):
    from mccain_capital.services import trades_mutations as trades_mutations_svc

    return trades_mutations_svc.trades_delete(trade_id)


def trades_delete_many():
    from mccain_capital.services import trades_mutations as trades_mutations_svc

    return trades_mutations_svc.trades_delete_many()


def trades_copy_many():
    from mccain_capital.services import trades_mutations as trades_mutations_svc

    return trades_mutations_svc.trades_copy_many()


def trades_edit(trade_id: int):
    from mccain_capital.services import trades_forms as trades_forms_svc

    return trades_forms_svc.trades_edit(trade_id)


def trades_review(trade_id: int):
    from mccain_capital.services import trades_forms as trades_forms_svc

    return trades_forms_svc.trades_review(trade_id)


def trades_risk_controls():
    from mccain_capital.services import trades_forms as trades_forms_svc

    return trades_forms_svc.trades_risk_controls()


def trades_clear():
    from mccain_capital.services import trades_mutations as trades_mutations_svc

    return trades_mutations_svc.trades_clear()


def trades_paste():
    if request.method == "POST":
        guardrail = trade_lockout_state(today_iso())
        if guardrail["locked"]:
            return render_page(
                simple_msg(
                    f"Daily max-loss guardrail is active for {guardrail['day']}. "
                    f"Day net {money(guardrail['day_net'])} reached limit {money(guardrail['daily_max_loss'])}. "
                    "Unlock in Risk Controls to continue."
                ),
                active="trades",
            )
        text = request.form.get("text", "")
        fmt = detect_paste_format(text)
        selected_account = _require_import_account(request.form.get("selected_account_id"))
        if not selected_account:
            return redirect(url_for("trades_paste"))

        reconciliation_html = ""
        if fmt == "broker":
            batch_id = _new_import_batch_id("paste")
            upload_id = repo.create_upload(
                account_id=int(selected_account["id"]),
                filename=f"paste_{batch_id}.txt",
                source="PASTE TRADES",
                import_batch_id=batch_id,
            )
            inserted, errors, report = importing.insert_trades_from_broker_paste_with_report(
                text,
                account_id=int(selected_account["id"]),
                upload_id=upload_id,
                commit=True,
                import_batch_id=batch_id,
            )
            _record_import_batch(
                batch_id=batch_id,
                source="PASTE TRADES",
                mode="broker",
                report=report,
                status="success",
                message=f"Inserted {inserted} trade(s) via paste.",
            )
            reconciliation_html = _reconciliation_block(report)
        else:
            inserted, errors = importing.insert_trades_from_paste(text)

        content = render_template(
            "trades/paste_result.html",
            inserted=inserted,
            errors=errors,
            reconciliation_html=reconciliation_html,
        )
        return render_page(content, active="trades")

    example = "1/29\t9:35 AM\t9:37 AM\tSPX\tPUT\t6940\t$6.20\t$7.30\t3\t$1,860.00\t20\t30\t$4.96\t$8.06\t$374.10\t$2.10\t$330.00\t$327.90\t17.74%\t$50,924.40"
    content = render_template(
        "trades/paste_form.html",
        example=example,
        selected_account_id=str(_selected_account().get("id") if _selected_account() else ""),
    )
    return render_page(content, active="trades")


def trades_playbook():
    cfg = _load_playbook_config()
    if request.method == "POST":
        cfg["enabled"] = request.form.get("enabled") == "1"
        cfg["min_checklist_score"] = max(
            0, min(100, parse_int(request.form.get("min_checklist_score") or "0") or 0)
        )
        cfg["max_size_pct"] = max(
            1.0, min(100.0, parse_float(request.form.get("max_size_pct") or "100") or 100.0)
        )
        cfg["require_positive_setup_expectancy"] = (
            request.form.get("require_positive_setup_expectancy") == "1"
        )
        cfg["require_critical_checklist"] = request.form.get("require_critical_checklist") == "1"
        raw_blocks = (request.form.get("blocked_time_blocks") or "").strip()
        cfg["blocked_time_blocks"] = [x.strip() for x in raw_blocks.split(",") if x.strip()]
        raw_critical = (request.form.get("critical_items") or "").strip()
        cfg["critical_items"] = [x.strip() for x in raw_critical.split(",") if x.strip()] or [
            "Bias Confirmed",
            "Risk Defined",
            "Stop Planned",
        ]
        _save_playbook_config(cfg)
        record_admin_audit(
            "playbook_saved",
            {
                "enabled": cfg["enabled"],
                "min_checklist_score": cfg["min_checklist_score"],
                "max_size_pct": cfg["max_size_pct"],
                "blocked_time_blocks": cfg["blocked_time_blocks"],
                "require_positive_setup_expectancy": cfg["require_positive_setup_expectancy"],
                "require_critical_checklist": cfg["require_critical_checklist"],
                "critical_items": cfg["critical_items"],
            },
        )
        flash("Playbook rules saved.", "success")
        return redirect(url_for("trades_playbook"))

    setup_rows = analytics_repo.group_table(analytics_repo.fetch_analytics_rows(), "setup_tag")
    content = render_template(
        "trades/playbook.html",
        cfg=cfg,
        setup_rows=setup_rows,
        money=money,
    )
    return render_page(content, active="trades")


def trades_new_manual():
    pb_cfg = _load_playbook_config()
    if request.method == "POST":
        f = request.form
        trade_date = (f.get("trade_date") or today_iso()).strip()
        guardrail = trade_lockout_state(trade_date)
        if guardrail["locked"]:
            return render_page(
                simple_msg(
                    f"Daily max-loss lockout active for {trade_date}. "
                    f"Day net {money(guardrail['day_net'])} hit limit {money(guardrail['daily_max_loss'])}."
                ),
                active="trades",
            )
        entry_time = (f.get("entry_time") or "").strip()
        exit_time = (f.get("exit_time") or "").strip()
        ticker = (f.get("ticker") or "").strip().upper()
        opt_type = normalize_opt_type(f.get("opt_type") or "")
        strike = parse_float(f.get("strike") or "")
        contracts = parse_int(f.get("contracts") or "") or 0
        entry_price = parse_float(f.get("entry_price") or "")
        exit_price = parse_float(f.get("exit_price") or "")
        comm = parse_float(f.get("comm") or "") or 0.0
        strategy_label = (f.get("strategy_label") or f.get("setup_tag") or "").strip()
        session_tag = (f.get("session_tag") or "").strip()
        checklist_score_raw = (f.get("checklist_score") or "").strip()
        checklist_score = parse_int(checklist_score_raw) if checklist_score_raw else None
        critical_items_checked = [
            str(x).strip() for x in f.getlist("critical_item") if str(x).strip()
        ]
        gate_values = _trade_gate_values(f, _load_trade_gate(trade_date))
        gate_checks = _trade_gate_checks(f, _load_trade_gate(trade_date))

        if _trade_gate_required(trade_date):
            gate_errors = _trade_gate_errors(gate_values, gate_checks)
            if gate_errors:
                return _render_manual_trade_entry_form(
                    pb_cfg=pb_cfg,
                    values=f,
                    gate_error="Trade gate blocked first trade: " + " ".join(gate_errors),
                )

        if (
            not ticker
            or opt_type not in ("CALL", "PUT")
            or contracts <= 0
            or entry_price is None
            or exit_price is None
        ):
            return _render_manual_trade_entry_form(
                pb_cfg=pb_cfg,
                values=f,
                gate_error="Missing required fields (ticker/type/contracts/entry/exit).",
            )

        gross_pl = (exit_price - entry_price) * 100.0 * contracts
        net_pl = gross_pl - comm
        total_spent = entry_price * 100.0 * contracts
        result_pct = (net_pl / total_spent * 100.0) if total_spent > 0 else None
        balance = (latest_balance_overall() or 50000.0) + net_pl
        violations = _playbook_violations(
            cfg=pb_cfg,
            setup_tag=strategy_label,
            checklist_score=checklist_score,
            entry_time=entry_time,
            total_spent=float(total_spent),
            balance=float(latest_balance_overall() or 50000.0),
            critical_items_checked=critical_items_checked,
        )
        if violations:
            return render_page(
                simple_msg("Playbook blocked trade: " + " ".join(violations)),
                active="trades",
            )
        if _trade_gate_required(trade_date):
            _save_trade_gate(
                trade_date,
                {
                    **gate_values,
                    **gate_checks,
                    "passed": True,
                    "passed_at": now_iso(),
                    "passed_label": app_runtime.now_et().strftime("%b %d, %I:%M %p ET"),
                },
            )

        with db() as conn:
            conn.execute(
                """
                INSERT INTO trades (
                    trade_date, entry_time, exit_time, ticker, opt_type, strike,
                    entry_price, exit_price, contracts, total_spent,
                    comm, gross_pl, net_pl, result_pct, balance,
                    raw_line, created_at, trade_source
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    trade_date,
                    entry_time,
                    exit_time,
                    ticker,
                    opt_type,
                    strike,
                    entry_price,
                    exit_price,
                    contracts,
                    total_spent,
                    comm,
                    gross_pl,
                    net_pl,
                    result_pct,
                    balance,
                    "MANUAL ENTRY",
                    now_iso(),
                    "Manual Entry",
                ),
            )
            trade_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        if strategy_label or session_tag or checklist_score is not None:
            auto_tags = _merge_auto_rule_break_tags(
                entry_price=entry_price,
                exit_price=exit_price,
                existing_tags="",
            )
            repo.upsert_trade_review(
                trade_id=trade_id,
                strategy_label=strategy_label,
                setup_tag=strategy_label,
                session_tag=session_tag,
                checklist_score=checklist_score,
                rule_break_tags=auto_tags,
                review_note="",
            )
        flash("Trade saved.", "success")
        return redirect(url_for("trades_page", d=trade_date))
    return _render_manual_trade_entry_form(pb_cfg=pb_cfg, values={"trade_date": today_iso()})


def trades_paste_broker():
    if request.method == "POST":
        guardrail = trade_lockout_state(today_iso())
        if guardrail["locked"]:
            return render_page(
                simple_msg(
                    f"Daily max-loss guardrail is active for {guardrail['day']}. "
                    f"Day net {money(guardrail['day_net'])} reached limit {money(guardrail['daily_max_loss'])}."
                ),
                active="trades",
            )
        text = request.form.get("text", "")
        selected_account = _require_import_account(request.form.get("selected_account_id"))
        if not selected_account:
            return redirect(url_for("trades_paste_broker"))
        batch_id = _new_import_batch_id("brokerpaste")
        upload_id = repo.create_upload(
            account_id=int(selected_account["id"]),
            filename=f"broker_paste_{batch_id}.txt",
            source="BROKER PASTE",
            import_batch_id=batch_id,
        )
        inserted, errors, report = importing.insert_trades_from_broker_paste_with_report(
            text,
            account_id=int(selected_account["id"]),
            upload_id=upload_id,
            commit=True,
            import_batch_id=batch_id,
        )
        _record_import_batch(
            batch_id=batch_id,
            source="BROKER PASTE",
            mode="broker",
            report=report,
            status="success",
            message=f"Inserted {inserted} trade(s) via broker paste.",
        )
        reconciliation_html = _reconciliation_block(report)
        content = render_template(
            "trades/broker_paste_result.html",
            inserted=inserted,
            errors=errors,
            reconciliation_html=reconciliation_html,
            review_day=today_iso(),
        )
        return render_page(content, active="trades")

    content = render_template(
        "trades/broker_paste_form.html",
        selected_account_id=str(_selected_account().get("id") if _selected_account() else ""),
    )
    return render_page(content, active="trades")


def trades_upload_pdf():
    workspace = (request.args.get("ws") or "live").strip().lower()
    if workspace not in {"upload", "live", "reconcile"}:
        workspace = "live"
    account_editor_mode = (request.args.get("account_editor") or "").strip().lower()
    credentials_panel_mode = (request.args.get("credentials") or "").strip().lower()
    query_account_id = _coerce_account_id(request.args.get("account_id"))
    if query_account_id:
        if repo.get_account(query_account_id):
            repo.set_active_account(query_account_id)
    elif str(request.args.get("account_id") or "").strip().lower() == "all":
        repo.set_active_account(None)
    if request.method == "POST":
        form_intent = (request.form.get("intent") or "import").strip().lower()
        if form_intent == "save_account":
            prior_scope = repo.account_scope_snapshot()
            prior_account_id = _coerce_account_id(
                request.form.get("rollover_from_account_id") or prior_scope.get("account_id")
            )
            account_name = (request.form.get("account_name") or "").strip()
            broker_account_id = (request.form.get("broker_account_id") or "").strip()
            account_size = parse_float(request.form.get("account_size") or "")
            starting_balance = parse_float(request.form.get("starting_balance") or "")
            max_drawdown = parse_float(request.form.get("max_drawdown") or "")
            target_account_id = _coerce_account_id(request.form.get("selected_account_id"))
            if not account_name or starting_balance is None:
                flash("Account name and starting balance are required.", "warn")
                return redirect(url_for("trades_upload_pdf", ws=workspace))
            if target_account_id:
                repo.update_account(
                    target_account_id,
                    prop_firm=repo.DEFAULT_PROP_FIRM,
                    account_name=account_name,
                    broker_account_id=broker_account_id,
                    account_size=account_size,
                    starting_balance=float(starting_balance),
                    max_drawdown=max_drawdown,
                )
                repo.set_active_account(target_account_id)
                if repo.maybe_link_rollover_account(int(target_account_id), prior_account_id):
                    flash("Linked prior eval account for dashboard continuity.", "success")
                flash("Account updated.", "success")
            else:
                existing_account = repo.find_account_by_broker_account_id(broker_account_id)
                if existing_account:
                    created = int(existing_account["id"])
                    repo.update_account(
                        created,
                        prop_firm=repo.DEFAULT_PROP_FIRM,
                        account_name=account_name,
                        broker_account_id=broker_account_id,
                        account_size=account_size,
                        starting_balance=float(starting_balance),
                        max_drawdown=max_drawdown,
                    )
                    if repo.maybe_link_rollover_account(int(created), prior_account_id):
                        flash("Linked prior eval account for dashboard continuity.", "success")
                    flash("Account updated.", "success")
                else:
                    created = repo.create_account(
                        prop_firm=repo.DEFAULT_PROP_FIRM,
                        account_name=account_name,
                        broker_account_id=broker_account_id,
                        account_size=account_size,
                        starting_balance=float(starting_balance),
                        max_drawdown=max_drawdown,
                    )
                    flash("Account created.", "success")
                    if repo.maybe_link_rollover_account(int(created), prior_account_id):
                        flash("Linked prior eval account for dashboard continuity.", "success")
                repo.set_active_account(int(created))
            return redirect(url_for("trades_upload_pdf", ws=workspace, account_id=""))
        if form_intent == "archive_account":
            target_account_id = _coerce_account_id(request.form.get("selected_account_id"))
            target_account = repo.get_account(target_account_id) if target_account_id else None
            if not target_account:
                flash("Select an active account to archive.", "warn")
                return redirect(url_for("trades_upload_pdf", ws=workspace))
            repo.archive_account(int(target_account["id"]))
            remaining_accounts = repo.list_accounts()
            fallback_account = next(
                (
                    row
                    for row in remaining_accounts
                    if int(row.get("id") or 0) != int(target_account["id"])
                ),
                None,
            )
            (
                repo.set_active_account(int(fallback_account["id"]))
                if fallback_account
                else repo.set_active_account(None)
            )
            flash(f"Archived {target_account['account_name']}.", "success")
            return redirect(url_for("trades_upload_pdf", ws=workspace))
        if form_intent == "bulk_archive_accounts":
            seen_ids: set[int] = set()
            target_ids: List[int] = []
            for raw_id in request.form.getlist("account_ids"):
                account_id = _coerce_account_id(raw_id)
                if account_id and account_id not in seen_ids:
                    seen_ids.add(account_id)
                    target_ids.append(account_id)
            active_scope = repo.account_scope_snapshot()
            active_account_id = _coerce_account_id(active_scope.get("account_id"))
            archived_count = 0
            archived_ids: set[int] = set()
            for account_id in target_ids:
                target_account = repo.get_account(account_id)
                if not target_account:
                    continue
                repo.archive_account(int(target_account["id"]))
                archived_count += 1
                archived_ids.add(int(target_account["id"]))
            if not archived_count:
                flash("Select at least one account to archive.", "warn")
                return redirect(url_for("trades_upload_pdf", ws=workspace))
            if active_account_id in archived_ids:
                remaining_accounts = repo.list_accounts()
                fallback_account = next(iter(remaining_accounts), None)
                repo.set_active_account(int(fallback_account["id"]) if fallback_account else None)
            flash(
                f"Archived {archived_count} account{'s' if archived_count != 1 else ''}.",
                "success",
            )
            return redirect(url_for("trades_upload_pdf", ws=workspace))
        if form_intent == "restore_account":
            target_account_id = _coerce_account_id(request.form.get("selected_account_id"))
            if not target_account_id:
                flash("Select an archived account to restore.", "warn")
                return redirect(url_for("trades_upload_pdf", ws=workspace))
            repo.restore_account(target_account_id)
            repo.set_active_account(target_account_id)
            flash("Archived account restored.", "success")
            return redirect(url_for("trades_upload_pdf", ws=workspace))

        guardrail = trade_lockout_state(today_iso())
        if guardrail["locked"]:
            return render_page(
                simple_msg(
                    f"Daily max-loss guardrail is active for {guardrail['day']}. "
                    f"Day net {money(guardrail['day_net'])} reached limit {money(guardrail['daily_max_loss'])}."
                ),
                active="trades",
            )
        f = request.files.get("pdf")
        mode = (request.form.get("mode") or "broker").strip()  # broker | balance
        pasted_html = (request.form.get("statement_html") or "").strip()
        selected_account = _require_import_account(request.form.get("selected_account_id"))
        if not selected_account:
            return redirect(url_for("trades_upload_pdf", ws=workspace))

        if (not f or not f.filename) and not pasted_html:
            return render_page(simple_msg("Please upload a file."), active="trades")

        if pasted_html and (not f or not f.filename):
            filename = f"statement_paste_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            path = os.path.join(_upload_dir(), filename)
            os.makedirs(_upload_dir(), exist_ok=True)
            with open(path, "w", encoding="utf-8") as out:
                out.write(pasted_html)
            return _handle_statement_html_import(
                path,
                mode=mode,
                source_label="STATEMENT HTML PASTE",
                account=selected_account,
                filename=filename,
            )

        filename = secure_filename(f.filename)
        _, ext = os.path.splitext(filename.lower())

        if ext not in {".pdf", ".html", ".htm"}:
            return render_page(simple_msg("Please upload a .pdf or .html file."), active="trades")

        path = os.path.join(_upload_dir(), filename)
        f.save(path)

        # ✅ HTML path (no OCR)
        if ext in (".html", ".htm"):
            return _handle_statement_html_import(
                path,
                mode=mode,
                source_label="STATEMENT HTML UPLOAD",
                account=selected_account,
                filename=filename,
            )

        # --- PDF path (keep your OCR behavior for now) ---
        if mode == "broker":
            paste_text, ocr_warns = importing.ocr_pdf_to_broker_paste(path)
            if not paste_text:
                stitched = []
                try:
                    convert_from_path, pytesseract, _, _, _, dep_error = importing.load_ocr_deps()
                    if dep_error:
                        raise RuntimeError(dep_error)
                    pages = convert_from_path(path, dpi=250)
                    all_lines = []
                    for page_img in pages:
                        img = importing.prep_for_ocr(page_img)
                        txt = pytesseract.image_to_string(img, config="--oem 3 --psm 6")
                        all_lines.extend(
                            [
                                importing.normalize_ocr(ln)
                                for ln in txt.splitlines()
                                if importing.normalize_ocr(ln)
                            ]
                        )
                    stitched = importing.stitch_ocr_rows("\n".join(all_lines))
                except Exception as e:
                    ocr_warns = (ocr_warns or []) + [f"OCR debug error: {e}"]

                return render_page(
                    render_template(
                        "trades/import_ocr_rows_unparseable.html",
                        warns=ocr_warns,
                        dump="\n".join(stitched[:30]),
                    ),
                    active="trades",
                )

            batch_id = _new_import_batch_id("pdfocr")
            upload_id = repo.create_upload(
                account_id=int(selected_account["id"]),
                filename=filename,
                source="STATEMENT PDF OCR",
                import_batch_id=batch_id,
            )
            inserted, errors, report = importing.insert_trades_from_broker_paste_with_report(
                paste_text,
                account_id=int(selected_account["id"]),
                upload_id=upload_id,
                commit=True,
                import_batch_id=batch_id,
            )
            _record_import_batch(
                batch_id=str(report.get("import_batch_id") or ""),
                source="STATEMENT PDF OCR",
                mode="broker",
                report=report,
                status="success",
                message=f"Inserted {inserted} trade(s) via PDF OCR broker import.",
            )
            reconciliation_html = _reconciliation_block(report)
            msgs = (ocr_warns or []) + (errors or [])
            return render_page(
                render_template(
                    "trades/import_pdf_ocr_result.html",
                    inserted=inserted,
                    msgs=msgs,
                    reconciliation_html=reconciliation_html,
                ),
                active="trades",
            )

        # mode == balance (PDF OCR)
        text, warns = importing.ocr_pdf_to_text(path)
        bal = importing.extract_statement_balance(text)
        if bal is None:
            return render_page(
                render_template(
                    "trades/import_pdf_balance_missing.html",
                    dump=(text or "")[:1200],
                ),
                active="trades",
            )

        batch_id = _new_import_batch_id("pdfbal")
        upload_id = repo.create_upload(
            account_id=int(selected_account["id"]),
            filename=filename,
            source="STATEMENT PDF UPLOAD",
            import_batch_id=batch_id,
        )
        importing.insert_balance_snapshot(
            today_iso(),
            bal,
            account_id=int(selected_account["id"]),
            upload_id=upload_id,
            raw_line="STATEMENT PDF UPLOAD",
        )
        return redirect(url_for("trades_page"))

    # GET
    selected_account = _selected_account(request.args.get("account_id"))
    rollover_from_account_id = _coerce_account_id(request.args.get("rollover_from"))
    account_form_mode = (
        "new"
        if workspace == "live" and account_editor_mode == "new"
        else "edit" if selected_account else "new"
    )
    account_form_account = None if account_form_mode == "new" else selected_account
    accounts = repo.list_accounts()
    archived_accounts = [
        row
        for row in repo.list_accounts(include_archived=True)
        if int(row.get("archived") or 0) == 1
    ]
    broker_cfg = _load_broker_sync_config()
    broker_cfg["account_display"] = repo.display_broker_account_id(broker_cfg.get("account", ""))
    auto_sync_cfg = _load_auto_sync_config()
    auto_sync_cfg["account_display"] = repo.display_broker_account_id(
        auto_sync_cfg.get("account", "")
    )
    default_day = today_iso()
    sync_status = _load_last_sync_status()
    sync_history = _load_sync_history()
    sync_reliability = _sync_reliability_summary(sync_history, days=30)
    import_history = _load_import_history()
    reconcile_summary = _reconcile_summary(import_history, days=30)
    sync_job_id = (request.args.get("job") or "").strip()
    sync_job = _get_bg_job(sync_job_id) if sync_job_id else {}
    if not sync_job:
        sync_job = _latest_active_sync_job()
    from mccain_capital.services import trades_sync as sync_orchestration

    live_sync_state = sync_orchestration.dashboard_live_sync_state()
    content = render_template(
        "trades/upload_statement.html",
        workspace=workspace,
        broker_cfg=broker_cfg,
        auto_sync_cfg=auto_sync_cfg,
        auto_sync_password_fallback=AUTO_SYNC_PASSWORD_FALLBACK,
        default_day=default_day,
        sync_status=sync_status,
        sync_badges=sync_state_badges(
            sync_status,
            status_key="status",
            stage_key="stage",
            updated_key="updated_at_human",
        ),
        sync_reliability=sync_reliability,
        sync_job=sync_job,
        live_sync_state=live_sync_state,
        reconcile_summary=reconcile_summary,
        import_history=list(reversed(import_history[-40:])),
        sync_stage_help=SYNC_STAGE_HELP,
        sync_stage_labels=SYNC_STAGE_LABELS,
        accounts=accounts,
        archived_accounts=archived_accounts,
        selected_account=selected_account,
        ledger_equity=broker_equity.ledger_equity_view(selected_account),
        account_form_mode=account_form_mode,
        account_form_account=account_form_account,
        rollover_from_account_id=rollover_from_account_id,
        live_account_form_open=(workspace == "live" and account_editor_mode in {"edit", "new"}),
        live_credentials_form_open=(workspace == "live" and credentials_panel_mode == "edit"),
        money=money,
    )
    return render_page(content, active="trades")


def _run_live_sync_once(
    *,
    selected_account_id: int | None,
    mode: str,
    username: str,
    password: str,
    base_url: str,
    account: str,
    wl: str,
    time_zone: str,
    date_locale: str,
    report_locale: str,
    from_date: str,
    to_date: str,
    headless: bool,
    debug_capture: bool,
    debug_only: bool,
    source_label: str,
    progress_cb: Optional[Callable[[str, str], None]] = None,
    cancel_cb: Optional[Callable[[], None]] = None,
) -> Dict[str, Any]:
    selected_account = (
        repo.get_account(int(selected_account_id))
        if selected_account_id not in (None, "", 0)
        else None
    )
    account_id = int(selected_account["id"]) if selected_account else 0
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    debug_dir = (
        os.path.join(_broker_debug_dir(), f"live_{from_date}_{to_date}_{stamp}")
        if debug_capture
        else None
    )
    artifacts_rel: List[str] = []
    result: Dict[str, Any] = {"ok": False, "warns": [], "artifacts_rel": [], "message": ""}

    def ensure_active() -> None:
        if cancel_cb:
            cancel_cb()

    try:
        ensure_active()
        html_text, warns, artifacts_abs, sync_meta = (
            vanquish_live_sync.fetch_statement_html_via_login(
                base_origin=base_url,
                username=username,
                password=password,
                from_date=from_date,
                to_date=to_date,
                account=account,
                wl=wl,
                time_zone=time_zone,
                date_locale=date_locale,
                report_locale=report_locale,
                headless=headless,
                debug_dir=debug_dir,
                progress_cb=progress_cb,
            )
        )
        ensure_active()
        artifacts_rel = [_debug_relative(p) for p in artifacts_abs]
        result["warns"] = warns
        result["sync_meta"] = sync_meta
    except Exception as e:
        raw_error = str(e)
        failed_stage = _classify_sync_stage(raw_error, _parse_sync_stage(raw_error))
        clean_error = _strip_stage_prefix(raw_error)
        if debug_dir and os.path.isdir(debug_dir):
            artifacts_rel = [
                _debug_relative(os.path.join(debug_dir, n))
                for n in sorted(os.listdir(debug_dir))
                if os.path.isfile(os.path.join(debug_dir, n))
            ]
        result.update(
            {
                "ok": False,
                "stage": failed_stage,
                "message": clean_error,
                "artifacts_rel": artifacts_rel,
            }
        )
        return result

    os.makedirs(_upload_dir(), exist_ok=True)
    filename = f"vanquish_statement_live_{from_date}_{to_date}_{stamp}.html"
    path = os.path.join(_upload_dir(), filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_text)
    artifacts_rel = artifacts_rel + [_debug_relative(path)]
    ensure_active()

    if debug_only:
        result.update(
            {
                "ok": True,
                "message": "Debug capture completed. No import performed.",
                "artifacts_rel": artifacts_rel,
                "statement_path": path,
                "debug_only": True,
            }
        )
        return result

    if mode == "broker":
        batch_id = _new_import_batch_id("live")
        if progress_cb:
            progress_cb("parse_statement_html", "Parsing statement rows.")
        ensure_active()
        paste_text, balance_val, parse_warns = importing.parse_statement_html_to_broker_paste(path)
        warns_all = (result.get("warns") or []) + (parse_warns or [])
        date_range_fallback = any(
            "Could not set custom From/To" in str(w) for w in (result.get("warns") or [])
        )
        balance_for_snapshot = balance_val
        if date_range_fallback and balance_val is not None:
            # When broker UI keeps visible defaults, captured statement can span a wider range
            # than requested dates. Reconcile using ending balance would be misleading.
            balance_val = None
            warns_all.append(
                "Date-range fallback detected; skipped ending-balance reconcile for this run."
            )
        if not paste_text:
            if balance_for_snapshot is not None:
                balance_batch_id = _new_import_batch_id("livebal")
                upload_id = (
                    repo.create_upload(
                        account_id=account_id,
                        filename=filename,
                        source=source_label,
                        import_batch_id=balance_batch_id,
                    )
                    if account_id > 0
                    else 0
                )
                if account_id > 0:
                    importing.insert_balance_snapshot(
                        today_iso(),
                        balance_for_snapshot,
                        account_id=account_id,
                        upload_id=upload_id,
                        raw_line=source_label,
                    )
                ledger_balance = (
                    latest_balance_overall(
                        account_id=account_id,
                        starting_balance=float(selected_account.get("starting_balance") or 0.0),
                    )
                    if selected_account
                    else None
                )
                _record_import_batch(
                    batch_id=balance_batch_id,
                    source=source_label,
                    mode="balance",
                    report={
                        "inserted_trades": 0,
                        "duplicates_skipped": 0,
                        "open_contracts": 0,
                        "errors_count": 0,
                        "warnings_count": len(warns_all or []),
                        "statement_ending_balance": balance_for_snapshot,
                        "ledger_ending_balance": ledger_balance,
                        "balance_delta": (ledger_balance - float(balance_for_snapshot)),
                    },
                    status="success",
                    message="No trade rows found; imported statement ending balance snapshot.",
                )
                metrics_result = _capture_account_metrics_for_sync(
                    account_id=account_id,
                    broker_account_id=account,
                    headless=headless,
                    debug_dir=debug_dir,
                    progress_cb=progress_cb,
                )
                warns_all = warns_all + list(metrics_result.get("warns") or [])
                artifacts_rel = artifacts_rel + [
                    _debug_relative(p) for p in list(metrics_result.get("artifacts_abs") or [])
                ]
                equity_refresh = _resolve_account_broker_equity(
                    account_id=account_id,
                    broker_account_id=account,
                    metrics_result=metrics_result,
                    statement_ending_balance=balance_for_snapshot,
                    statement_trusted=not date_range_fallback,
                )
                result.update(
                    {
                        "ok": True,
                        "stage": "import_complete",
                        "message": f"{source_label}: no trade rows found; imported balance snapshot {money(balance_for_snapshot)}.",
                        "inserted": 0,
                        "warns": warns_all,
                        "artifacts_rel": artifacts_rel,
                        "statement_path": path,
                        "batch_id": balance_batch_id,
                        "account_metrics": metrics_result.get("metrics"),
                        "account_metrics_meta": metrics_result.get("meta"),
                        "equity_refresh": equity_refresh,
                    }
                )
                return result
            metrics_result = _capture_account_metrics_for_sync(
                account_id=account_id,
                broker_account_id=account,
                headless=headless,
                debug_dir=debug_dir,
                progress_cb=progress_cb,
            )
            warns_all = warns_all + list(metrics_result.get("warns") or [])
            equity_refresh = _resolve_account_broker_equity(
                account_id=account_id,
                broker_account_id=account,
                metrics_result=metrics_result,
                statement_ending_balance=None,
                statement_trusted=False,
            )
            result.update(
                {
                    "ok": True,
                    "stage": "import_complete",
                    "message": "No new trade rows found; equity refresh still attempted.",
                    "inserted": 0,
                    "warns": warns_all,
                    "artifacts_rel": artifacts_rel,
                    "statement_path": path,
                    "account_metrics": metrics_result.get("metrics"),
                    "account_metrics_meta": metrics_result.get("meta"),
                    "equity_refresh": equity_refresh,
                }
            )
            return result
        upload_id = (
            repo.create_upload(
                account_id=account_id,
                filename=filename,
                source=source_label,
                import_batch_id=batch_id,
            )
            if account_id > 0
            else 0
        )
        _, _, pre_report = _import_broker_paste_with_report(
            paste_text,
            account_id=account_id,
            upload_id=upload_id,
            ending_balance=balance_val,
            commit=False,
            import_batch_id=batch_id,
        )
        if RECONCILE_GATE_ENABLED:
            if progress_cb:
                progress_cb("reconcile_gate", "Running reconcile guardrails.")
            ensure_active()
            gate = _reconcile_gate_result(pre_report)
            if gate["blocked"]:
                _emit_notification(
                    "reconcile_gate_block",
                    "Reconcile gate blocked import",
                    f"{source_label} import blocked by reconcile gate.",
                    {
                        "batch_id": batch_id,
                        "source": source_label,
                        "reasons": gate["reasons"],
                        "balance_delta": pre_report.get("balance_delta"),
                    },
                )
                result.update(
                    {
                        "ok": False,
                        "stage": "reconcile_gate",
                        "message": "Reconciliation gate blocked import: "
                        + "; ".join(gate["reasons"]),
                        "report": pre_report,
                        "warns": warns_all,
                        "artifacts_rel": artifacts_rel,
                        "statement_path": path,
                    }
                )
                return result
        if progress_cb:
            progress_cb("import_trades", "Importing trades.")
        ensure_active()
        inserted, errors, report = _import_broker_paste_with_report(
            paste_text,
            account_id=account_id,
            upload_id=upload_id,
            ending_balance=balance_val,
            commit=True,
            import_batch_id=batch_id,
        )
        metrics_result = _capture_account_metrics_for_sync(
            account_id=account_id,
            broker_account_id=account,
            headless=headless,
            debug_dir=debug_dir,
            progress_cb=progress_cb,
        )
        warns_all = warns_all + list(metrics_result.get("warns") or [])
        artifacts_rel = artifacts_rel + [
            _debug_relative(p) for p in list(metrics_result.get("artifacts_abs") or [])
        ]
        equity_refresh = _resolve_account_broker_equity(
            account_id=account_id,
            broker_account_id=account,
            metrics_result=metrics_result,
            statement_ending_balance=balance_val,
            statement_trusted=not date_range_fallback,
        )
        msg = f"{source_label}: inserted {inserted} trade(s)."
        if errors:
            msg = f"{msg} Warnings: {len(errors)}."
        result.update(
            {
                "ok": True,
                "message": msg,
                "stage": "import_complete",
                "inserted": inserted,
                "errors": errors or [],
                "report": report or {},
                "warns": warns_all,
                "artifacts_rel": artifacts_rel,
                "statement_path": path,
                "batch_id": batch_id,
                "account_metrics": metrics_result.get("metrics"),
                "account_metrics_meta": metrics_result.get("meta"),
                "equity_refresh": equity_refresh,
            }
        )
        return result

    # balance mode
    batch_id = _new_import_batch_id("livebal")
    if progress_cb:
        progress_cb("parse_statement_html", "Parsing statement balance.")
    ensure_active()
    _, balance_val, parse_warns = importing.parse_statement_html_to_broker_paste(path)
    warns_all = (result.get("warns") or []) + (parse_warns or [])
    if balance_val is None:
        result.update(
            {
                "ok": False,
                "stage": "capture_statement_html",
                "message": "Statement balance not found in generated HTML.",
                "warns": warns_all,
                "artifacts_rel": artifacts_rel,
                "statement_path": path,
            }
        )
        return result
    upload_id = repo.create_upload(
        account_id=int(selected_account["id"]),
        filename=filename,
        source=source_label,
        import_batch_id=batch_id,
    )
    importing.insert_balance_snapshot(
        today_iso(),
        balance_val,
        account_id=int(selected_account["id"]),
        upload_id=upload_id,
        raw_line=source_label,
    )
    metrics_result = _capture_account_metrics_for_sync(
        account_id=int(selected_account["id"]),
        broker_account_id=account,
        headless=headless,
        debug_dir=debug_dir,
        progress_cb=progress_cb,
    )
    warns_all = warns_all + list(metrics_result.get("warns") or [])
    artifacts_rel = artifacts_rel + [
        _debug_relative(p) for p in list(metrics_result.get("artifacts_abs") or [])
    ]
    equity_refresh = _resolve_account_broker_equity(
        account_id=int(selected_account["id"]),
        broker_account_id=account,
        metrics_result=metrics_result,
        statement_ending_balance=balance_val,
        statement_trusted=True,
    )
    result.update(
        {
            "ok": True,
            "stage": "import_complete",
            "message": f"{source_label}: imported ending balance snapshot {money(balance_val)}.",
            "warns": warns_all,
            "artifacts_rel": artifacts_rel,
            "statement_path": path,
            "batch_id": batch_id,
            "account_metrics": metrics_result.get("metrics"),
            "account_metrics_meta": metrics_result.get("meta"),
            "equity_refresh": equity_refresh,
        }
    )
    return result


def _sync_requested_payload(
    *,
    source: str,
    mode: str,
    from_date: str,
    to_date: str,
    base_url: str,
    account: str,
    wl: str,
    time_zone: str,
    date_locale: str,
    report_locale: str,
    headless: bool,
    debug_capture: bool,
    debug_only: bool,
    username: str,
) -> Dict[str, Any]:
    return {
        "source": source,
        "mode": mode,
        "from_date": from_date,
        "to_date": to_date,
        "base_url": base_url,
        "account": account,
        "wl": wl,
        "time_zone": time_zone,
        "date_locale": date_locale,
        "report_locale": report_locale,
        "headless": headless,
        "debug_capture": debug_capture,
        "debug_only": debug_only,
        "username": username,
    }


def _execute_sync_job(
    *,
    app,
    job: Dict[str, Any],
    cancel_event: threading.Event,
    selected_account_id: int,
    title: str,
    source_label: str,
    record_source: str,
    mode: str,
    username: str,
    password: str,
    base_url: str,
    account: str,
    wl: str,
    time_zone: str,
    date_locale: str,
    report_locale: str,
    from_date: str,
    to_date: str,
    headless: bool,
    debug_capture: bool,
    debug_only: bool,
    requested: Dict[str, Any],
) -> None:
    started = time.time()

    def progress(stage: str, message: str) -> None:
        _ensure_sync_job_active(job["id"], cancel_event)
        stage_message = message or _sync_stage_label(stage)
        _update_bg_job(job["id"], status="running", stage=stage, message=stage_message)
        _save_last_sync_status(
            {
                "job_id": job["id"],
                "status": "running",
                "stage": stage,
                "message": stage_message,
                "stage_help": SYNC_STAGE_HELP.get(stage, ""),
                "requested": requested,
                "updated_at": now_iso(),
            }
        )

    try:
        with app.app_context():
            progress("queue_dispatch", "Sync worker picked up the job.")
            run = _run_live_sync_once(
                selected_account_id=selected_account_id,
                mode=mode,
                username=username,
                password=password,
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
                source_label=source_label,
                progress_cb=progress,
                cancel_cb=lambda: _ensure_sync_job_active(job["id"], cancel_event),
            )
            if cancel_event.is_set() or _sync_job_cancelled(job["id"]):
                return
            duration_sec = round(max(0.0, time.time() - started), 2)
            status = (
                "debug_only"
                if run.get("debug_only")
                else ("success" if run.get("ok") else "failed")
            )
            stage = str(
                run.get("stage")
                or ("capture_statement_html" if run.get("debug_only") else "")
                or ("import_complete" if run.get("ok") else "unknown")
            )
            summary = {
                "message": str(run.get("message") or ""),
                "warn_count": len(run.get("warns") or []),
                "error_count": len(run.get("errors") or []),
                "inserted": int(run.get("inserted") or 0),
                "account_metrics": run.get("account_metrics"),
                "account_metrics_meta": run.get("account_metrics_meta"),
                "equity_refresh": run.get("equity_refresh"),
                "artifacts_rel": (run.get("artifacts_rel") or [])[:20],
                "statement_file": (
                    _debug_relative(run.get("statement_path", ""))
                    if run.get("statement_path")
                    else ""
                ),
            }
            _record_import_batch(
                batch_id=str(run.get("batch_id") or ""),
                source=record_source,
                mode=mode,
                report=run.get("report") if isinstance(run.get("report"), dict) else None,
                status="success" if run.get("ok") else "failed",
                message=str(run.get("message") or ""),
            )
            _save_last_sync_status(
                {
                    "job_id": job["id"],
                    "status": status,
                    "stage": stage,
                    "message": run.get("message") or "",
                    "stage_help": SYNC_STAGE_HELP.get(stage, ""),
                    "requested": requested,
                    "sync_meta": run.get("sync_meta", {}),
                    "account_metrics": run.get("account_metrics"),
                    "account_metrics_meta": run.get("account_metrics_meta"),
                    "equity_refresh": run.get("equity_refresh"),
                    "artifacts_rel": summary["artifacts_rel"],
                    "statement_file": summary["statement_file"],
                    "duration_sec": duration_sec,
                    "updated_at": now_iso(),
                }
            )
            _update_bg_job(
                job["id"],
                status=status,
                stage=stage,
                message=summary["message"] or _sync_stage_label(stage),
                duration_sec=duration_sec,
                summary=summary,
                result_summary=_build_action_result_summary(
                    tone=(
                        "success"
                        if run.get("ok")
                        else ("warning" if run.get("debug_only") else "danger")
                    ),
                    title=(
                        "Live Sync Complete"
                        if run.get("ok")
                        else (
                            "Debug Capture Complete"
                            if run.get("debug_only")
                            else "Live Sync Failed"
                        )
                    ),
                    happened=str(run.get("message") or _sync_stage_label(stage)),
                    changed=(
                        f"Imported {int(run.get('inserted') or 0)} trade(s) into the execution log."
                        if run.get("ok")
                        else (
                            "No trade import was committed."
                            if not run.get("debug_only")
                            else "Captured artifacts only; no import was committed."
                        )
                    ),
                    warnings=[str(x) for x in (run.get("warns") or [])],
                    next_action=(
                        "Review imported trades, analyze the session, then debrief it while context is fresh."
                        if run.get("ok")
                        else "Open diagnostics or retry the sync after correcting the broker/session issue."
                    ),
                    metrics=[
                        {"label": "Inserted", "value": str(int(run.get("inserted") or 0))},
                        {"label": "Warnings", "value": str(len(run.get("warns") or []))},
                        {"label": "Errors", "value": str(len(run.get("errors") or []))},
                    ],
                    actions=(
                        [
                            {
                                "label": "Open Imported Trades",
                                "href": f"/trades?d={to_date}",
                                "kind": "primary",
                            },
                            {
                                "label": "Analyze Session",
                                "href": f"/analytics?tab=performance&start={from_date}&end={to_date}",
                            },
                            {
                                "label": "Journal This Session",
                                "href": f"/journal/new?d={to_date}&entry_type=trade_debrief&link_all_day=1",
                            },
                        ]
                        if run.get("ok")
                        else [
                            {
                                "label": "Open Live Sync",
                                "href": "/trades/upload/statement?ws=live",
                                "kind": "primary",
                            },
                            {
                                "label": "Open Reconcile",
                                "href": "/trades/upload/statement?ws=reconcile",
                            },
                        ]
                    ),
                ),
            )
    except SyncJobCancelled:
        return
    except Exception as e:  # pragma: no cover
        if cancel_event.is_set() or _sync_job_cancelled(job["id"]):
            return
        duration_sec = round(max(0.0, time.time() - started), 2)
        raw_message = str(e)
        stage = _classify_sync_stage(raw_message, "auto_worker")
        fail_message = _strip_stage_prefix(raw_message)
        _save_last_sync_status(
            {
                "job_id": job["id"],
                "status": "failed",
                "stage": stage,
                "message": fail_message,
                "stage_help": SYNC_STAGE_HELP.get(stage, ""),
                "requested": requested,
                "updated_at": now_iso(),
            }
        )
        _update_bg_job(
            job["id"],
            status="failed",
            stage=stage,
            message=fail_message,
            duration_sec=duration_sec,
            summary={
                "message": fail_message,
                "warn_count": 0,
                "error_count": 1,
                "inserted": 0,
                "artifacts_rel": [],
                "statement_file": "",
            },
            result_summary=_build_action_result_summary(
                tone="danger",
                title="Live Sync Failed",
                happened=fail_message,
                changed="No sync result was committed because the background worker terminated early.",
                next_action="Return to the Live Sync workspace, retry the run, and inspect any captured diagnostics.",
                actions=[
                    {
                        "label": "Open Live Sync",
                        "href": "/trades/upload/statement?ws=live",
                        "kind": "primary",
                    },
                    {"label": "Open Ops Alerts", "href": "/ops/alerts"},
                ],
            ),
        )
    finally:
        _clear_sync_cancel_event(job["id"])


def _sync_dispatch_loop(app) -> None:
    while True:
        item = _SYNC_JOB_QUEUE.get()
        if item is None:
            continue
        try:
            _execute_sync_job(app=app, **item)
        finally:
            _SYNC_JOB_QUEUE.task_done()


def ensure_sync_dispatcher_started(app) -> None:
    global _SYNC_DISPATCH_THREAD_STARTED
    with _SYNC_DISPATCH_THREAD_LOCK:
        if _SYNC_DISPATCH_THREAD_STARTED:
            return
        t = threading.Thread(
            target=_sync_dispatch_loop, args=(app,), daemon=True, name="sync-job-dispatcher"
        )
        t.start()
        _SYNC_DISPATCH_THREAD_STARTED = True


def _start_sync_job_thread(app, worker_payload: Dict[str, Any]) -> threading.Thread:
    t = threading.Thread(
        target=_execute_sync_job,
        kwargs={"app": app, **worker_payload},
        daemon=True,
        name=f"sync-job-{str(((worker_payload.get('job') or {}).get('id')) or 'worker')[:12]}",
    )
    t.start()
    return t


def _start_sync_job(
    *,
    selected_account_id: int | None,
    title: str,
    source_label: str,
    record_source: str,
    mode: str,
    username: str,
    password: str,
    base_url: str,
    account: str,
    wl: str,
    time_zone: str,
    date_locale: str,
    report_locale: str,
    from_date: str,
    to_date: str,
    headless: bool,
    debug_capture: bool,
    debug_only: bool,
    requested: Dict[str, Any],
) -> Dict[str, Any]:
    app = current_app._get_current_object()
    job = _create_bg_job("sync", title, requested)
    cancel_event = _sync_cancel_event(job["id"])
    worker_payload = {
        "job": job,
        "cancel_event": cancel_event,
        "selected_account_id": (
            int(selected_account_id) if selected_account_id not in (None, "", 0) else None
        ),
        "title": title,
        "source_label": source_label,
        "record_source": record_source,
        "mode": mode,
        "username": username,
        "password": password,
        "base_url": base_url,
        "account": account,
        "wl": wl,
        "time_zone": time_zone,
        "date_locale": date_locale,
        "report_locale": report_locale,
        "from_date": from_date,
        "to_date": to_date,
        "headless": headless,
        "debug_capture": debug_capture,
        "debug_only": debug_only,
        "requested": requested,
    }
    try:
        _start_sync_job_thread(app, worker_payload)
    except Exception as e:
        raw_message = str(e)
        fail_message = _strip_stage_prefix(raw_message)
        stage = _classify_sync_stage(raw_message, "queue_dispatch")
        if stage == "system_resource":
            fail_message = (
                "Chromium startup resources are busy. Another browser boot is still active."
            )
        _save_last_sync_status(
            {
                "job_id": job["id"],
                "status": "failed",
                "stage": stage,
                "message": fail_message,
                "stage_help": SYNC_STAGE_HELP.get(stage, ""),
                "requested": requested,
                "updated_at": now_iso(),
            }
        )
        _update_bg_job(
            job["id"],
            status="failed",
            stage=stage,
            message=fail_message,
            duration_sec=0.0,
            summary={
                "message": fail_message,
                "warn_count": 0,
                "error_count": 1,
                "inserted": 0,
                "artifacts_rel": [],
                "statement_file": "",
            },
            result_summary=_build_action_result_summary(
                tone="danger",
                title="Live Sync Failed",
                happened=fail_message,
                changed="No trade import was committed.",
                next_action=(
                    "Click Force Reset Lane, wait a few seconds, then retry once. "
                    "Use manual statement upload if startup remains busy."
                ),
                actions=[
                    {
                        "label": "Open Live Sync",
                        "href": "/trades/upload/statement?ws=live",
                        "kind": "primary",
                    },
                    {"label": "Open Ops Alerts", "href": "/ops/alerts"},
                ],
            ),
        )
        _clear_sync_cancel_event(job["id"])
        return _get_bg_job(job["id"])
    return job


def trades_sync_live():
    if request.method != "POST":
        return redirect(url_for("trades_upload_pdf"))
    wants_async = (request.args.get("async") or "").strip() == "1"

    mode = (request.form.get("mode") or "broker").strip()
    guardrail = trade_lockout_state(today_iso())
    if guardrail["locked"] and mode == "broker":
        message = (
            f"Daily max-loss guardrail is active for {guardrail['day']}. "
            f"Day net {money(guardrail['day_net'])} reached limit {money(guardrail['daily_max_loss'])}."
        )
        if wants_async:
            return jsonify({"ok": False, "message": message}), 409
        return render_page(simple_msg(message), active="trades")

    cfg = _load_broker_sync_config()
    remembered_username = str(cfg.get("username") or "").strip()
    username = (request.form.get("username") or "").strip() or remembered_username
    password = (request.form.get("password") or "").strip()
    remember_credentials = request.form.get("remember_credentials") == "1"
    clear_saved_credentials = request.form.get("clear_saved_credentials") == "1"
    base_url = (request.form.get("base_url") or "").strip() or cfg.get("base_url", "")
    account = repo.normalize_broker_account_id(
        (request.form.get("account") or "").strip() or cfg.get("account", "")
    )
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
    stored_password = _get_auto_sync_password({"username": username}) if username else ""

    if clear_saved_credentials and username:
        _clear_auto_sync_password(username)
        cfg["username"] = ""
        cfg["password"] = ""
        cfg["password_enc"] = ""
        cfg["password_stored"] = False
        _save_broker_sync_config(cfg)
        flash("Saved live sync credentials cleared.", "success")
        return redirect(url_for("trades_upload_pdf", ws="live"))

    active_job = _latest_active_sync_job()
    if active_job:
        message = "A live sync job is already active. Force Reset Lane before starting another run."
        if wants_async:
            return (
                jsonify(
                    {"ok": False, "message": message, "job": _job_response_payload(active_job)}
                ),
                409,
            )
        flash(message, "warn")
        return redirect(url_for("trades_upload_pdf", ws="live", job=active_job["id"]))

    password_for_run = password or stored_password

    if not username or not password_for_run:
        message = "Username and password are required for live login sync."
        if wants_async:
            return jsonify({"ok": False, "message": message}), 400
        return render_page(simple_msg(message), active="trades")
    if not base_url or not account:
        message = "Base origin and account are required for live login sync."
        if wants_async:
            return jsonify({"ok": False, "message": message}), 400
        return render_page(simple_msg(message), active="trades")

    from_date = _normalize_iso_date(request.form.get("from_date") or "", today_iso())
    to_date = _normalize_iso_date(request.form.get("to_date") or "", today_iso())
    if from_date > to_date:
        from_date, to_date = to_date, from_date
    selected_account = _require_import_account(request.form.get("selected_account_id"))
    if not selected_account:
        if wants_async:
            return (
                jsonify(
                    {"ok": False, "message": "Please select an account before uploading trades."}
                ),
                400,
            )
        return redirect(url_for("trades_upload_pdf", ws="live"))
    selected_broker_account = repo.normalize_broker_account_id(
        selected_account.get("display_broker_account_id")
        or repo.display_broker_account_id(selected_account.get("broker_account_id"))
        or selected_account.get("broker_account_id")
        or ""
    )
    if selected_broker_account:
        account = selected_broker_account

    requested = _sync_requested_payload(
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
            saved = _set_auto_sync_password(username, password)
            if not saved and AUTO_SYNC_PASSWORD_FALLBACK:
                cfg["password_enc"] = _encrypt_fallback_password(password)
                cfg["password"] = ""
                cfg["password_stored"] = True
            else:
                cfg.pop("password_enc", None)
                cfg["password_stored"] = saved
        _save_broker_sync_config(cfg)
    job = _start_sync_job(
        selected_account_id=int(selected_account["id"]),
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
    if wants_async:
        return jsonify({"ok": True, "job": _job_response_payload(job)})
    flash("Live sync started. Progress and result will update below.", "success")
    return redirect(url_for("trades_upload_pdf", ws="live", job=job["id"]))


def trades_sync_auto_config():
    if request.method != "POST":
        return redirect(url_for("trades_upload_pdf"))
    cfg = _load_auto_sync_config()
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
    cfg["account"] = repo.normalize_broker_account_id(request.form.get("auto_account") or "")
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
        if target_user and _clear_auto_sync_password(target_user):
            flash("Auto sync password cleared from OS keychain.", "success")
        elif target_user:
            flash("Could not clear keychain password (or it was not present).", "warn")
    elif new_password:
        if not username:
            cfg["password"] = ""
            cfg["password_enc"] = ""
            flash("Set username before saving password to keychain.", "warn")
        elif _set_auto_sync_password(username, new_password):
            cfg["password"] = ""
            cfg["password_enc"] = ""
            flash("Auto sync password stored in OS keychain.", "success")
        elif AUTO_SYNC_PASSWORD_FALLBACK:
            enc = _encrypt_fallback_password(new_password)
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
                    "OS keychain unavailable and fallback encryption is not ready. Set SECRET_KEY or AUTO_SYNC_PASSWORD_FALLBACK_KEY.",
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
        # Keep existing fallback secret unless explicitly cleared/replaced.
        cfg["password"] = str(cfg.get("password") or "")
        cfg["password_enc"] = str(cfg.get("password_enc") or "")
    _save_auto_sync_config(cfg)
    if cfg.get("enabled") and not _get_auto_sync_password(cfg):
        flash(
            "Auto sync is enabled but no keychain password is stored yet.",
            "warn",
        )
    flash("Auto sync schedule saved.", "success")
    return redirect(url_for("trades_upload_pdf", ws="live"))


def trades_sync_auto_run_now():
    cfg = _load_auto_sync_config()
    selected_account = _selected_account()
    if not selected_account:
        flash("Please select an account before uploading trades.", "warn")
        return redirect(url_for("trades_upload_pdf", ws="live"))
    auto_password = _get_auto_sync_password(cfg)
    if not cfg.get("username") or not auto_password:
        flash(
            "Auto sync credentials are missing. Save username and password in the Live Sync workspace first.",
            "warn",
        )
        return redirect(url_for("trades_upload_pdf", ws="live"))
    today = today_iso()
    requested = _sync_requested_payload(
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
    job = _start_sync_job(
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
    flash("Auto sync started. Live status will update below.", "success")
    return redirect(url_for("trades_upload_pdf", ws="live", job=job["id"]))


def trades_sync_job_status(job_id: str):
    job = _get_bg_job((job_id or "").strip())
    if not job:
        return jsonify({"ok": False, "error": "job_not_found"}), 404
    return jsonify({"ok": True, "job": _job_response_payload(job)})


def ensure_auto_sync_worker_started(app) -> None:
    global _AUTO_SYNC_THREAD_STARTED, _AUTO_BACKUP_THREAD_STARTED
    with _AUTO_SYNC_THREAD_LOCK:
        if _AUTO_SYNC_THREAD_STARTED:
            pass
        else:
            t = threading.Thread(
                target=_auto_sync_worker, args=(app,), daemon=True, name="auto-sync-worker"
            )
            t.start()
            _AUTO_SYNC_THREAD_STARTED = True
    with _AUTO_BACKUP_THREAD_LOCK:
        if _AUTO_BACKUP_THREAD_STARTED:
            return
        t = threading.Thread(
            target=_auto_backup_worker, args=(app,), daemon=True, name="auto-backup-worker"
        )
        t.start()
        _AUTO_BACKUP_THREAD_STARTED = True


def _auto_sync_worker(app) -> None:
    while True:
        try:
            cfg = _load_auto_sync_config()
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
            auto_password = _get_auto_sync_password(cfg)
            if not cfg.get("username") or not auto_password or not cfg.get("account"):
                _save_last_sync_status(
                    {
                        "status": "failed",
                        "stage": "auto_config",
                        "message": "Auto sync is enabled but username/keychain password/account are not fully configured.",
                        "updated_at": now_iso(),
                    }
                )
                time.sleep(60)
                continue
            try:
                fd = os.open(_broker_auto_sync_lock_path(), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
            except FileExistsError:
                time.sleep(20)
                continue
            try:
                with app.app_context():
                    selected_account = _selected_account()
                    if not selected_account:
                        _save_last_sync_status(
                            {
                                "status": "failed",
                                "stage": "account_scope",
                                "message": "Please select an account before uploading trades.",
                                "updated_at": now_iso(),
                            }
                        )
                        time.sleep(60)
                        continue
                    started = time.time()
                    run = _run_live_sync_once(
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
                    _save_auto_sync_config(cfg)
                    duration_sec = round(max(0.0, time.time() - started), 2)
                    _record_import_batch(
                        batch_id=str(run.get("batch_id") or ""),
                        source="AUTO SYNC SCHEDULER",
                        mode=str(cfg.get("mode") or "broker"),
                        report=run.get("report") if isinstance(run.get("report"), dict) else None,
                        status="success" if run.get("ok") else "failed",
                        message=str(run.get("message") or ""),
                    )
                    _save_last_sync_status(
                        {
                            "status": "success" if run.get("ok") else "failed",
                            "stage": run.get("stage")
                            or ("import_complete" if run.get("ok") else "unknown"),
                            "message": run.get("message") or "",
                            "stage_help": SYNC_STAGE_HELP.get(str(run.get("stage") or ""), ""),
                            "requested": {
                                "source": "scheduler",
                                "scheduled_for": f"{today} {target_h:02d}:{target_m:02d}",
                                "mode": cfg.get("mode", "broker"),
                            },
                            "sync_meta": run.get("sync_meta", {}),
                            "artifacts_rel": (run.get("artifacts_rel") or [])[:20],
                            "statement_file": (
                                _debug_relative(run.get("statement_path", ""))
                                if run.get("statement_path")
                                else ""
                            ),
                            "duration_sec": duration_sec,
                            "updated_at": now_iso(),
                        }
                    )
            finally:
                try:
                    os.unlink(_broker_auto_sync_lock_path())
                except OSError:
                    pass
            time.sleep(45)
        except Exception as e:  # pragma: no cover
            _save_last_sync_status(
                {
                    "status": "failed",
                    "stage": "auto_worker",
                    "message": f"Auto sync worker error: {e}",
                    "updated_at": now_iso(),
                }
            )
            time.sleep(60)


def trades_sync_debug_file(name: str):
    try:
        path = _debug_safe_path(name)
    except ValueError:
        abort(400)
    if not os.path.isfile(path):
        abort(404)
    return send_file(path, as_attachment=False)


def _require_ops_mutation_auth() -> None:
    if auth.auth_enabled() and not auth.is_authenticated():
        abort(403)


def _normalize_scope_start_date(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    parsed = parse_date_any(text)
    if parsed:
        return parsed
    # Browsers/locales can render date input as "MM / DD / YYYY".
    compact = text.replace(" ", "")
    parsed = parse_date_any(compact)
    return parsed or ""


def ops_alerts_page():
    _scan_anomaly_watch()
    state = _load_notify_history()
    status_filter = (request.args.get("status") or "active").strip().lower()
    if status_filter not in {"active", "all", "open", "acknowledged", "resolved", "muted"}:
        status_filter = "active"
    event_filter = (request.args.get("event") or "").strip()
    alerts = _sorted_alerts(state, status_filter=status_filter, event_filter=event_filter)
    auto_backup_cfg = _load_auto_backup_config()
    audit_rows = list(reversed(_load_admin_audit()[-30:]))
    muted = state.get("muted_by_event", {})
    if not isinstance(muted, dict):
        muted = {}
    event_types = sorted(
        {
            str(a.get("event_type") or "")
            for a in (state.get("alerts") if isinstance(state.get("alerts"), list) else [])
            if isinstance(a, dict) and str(a.get("event_type") or "")
        }
        | set(NOTIFY_DEDUPE_BY_EVENT.keys())
    )
    content = render_template(
        "ops/alerts.html",
        alerts=alerts,
        status_filter=status_filter,
        event_filter=event_filter,
        resolveable_count=len([a for a in alerts if str(a.get("status") or "open") != "resolved"]),
        muted=muted,
        event_types=event_types,
        auto_backup_cfg=auto_backup_cfg,
        audit_rows=audit_rows,
        active_count=len([a for a in _sorted_alerts(state, "active", "")]),
        open_count=len([a for a in _sorted_alerts(state, "open", "")]),
        ack_count=len([a for a in _sorted_alerts(state, "acknowledged", "")]),
        resolved_count=len([a for a in _sorted_alerts(state, "resolved", "")]),
    )
    return render_page(content, active="ops")


def ops_alert_ack():
    _require_ops_mutation_auth()
    alert_id = (request.form.get("alert_id") or "").strip()
    if not alert_id or not _update_alert_status(alert_id, "acknowledged"):
        flash("Alert not found.", "warn")
    else:
        record_admin_audit("ops_alert_ack", {"alert_id": alert_id})
        flash("Alert acknowledged.", "success")
    return redirect(url_for("ops_alerts_page"))


def ops_alert_resolve():
    _require_ops_mutation_auth()
    resolve_scope = (request.form.get("resolve_scope") or "").strip().lower()
    if resolve_scope == "visible":
        status_filter = (request.form.get("status_filter") or "active").strip().lower()
        event_filter = (request.form.get("event_filter") or "").strip()
        count = _bulk_update_alert_status(status_filter, event_filter, "resolved")
        if count <= 0:
            flash("No open alerts matched this view.", "warn")
        else:
            record_admin_audit(
                "ops_alert_resolve_all",
                {
                    "count": count,
                    "status_filter": status_filter,
                    "event_filter": event_filter,
                },
            )
            flash(f"Resolved {count} alerts in this view.", "success")
        if event_filter:
            return redirect(url_for("ops_alerts_page", status=status_filter, event=event_filter))
        return redirect(url_for("ops_alerts_page", status=status_filter))
    alert_id = (request.form.get("alert_id") or "").strip()
    if not alert_id or not _update_alert_status(alert_id, "resolved"):
        flash("Alert not found.", "warn")
    else:
        record_admin_audit("ops_alert_resolve", {"alert_id": alert_id})
        flash("Alert resolved.", "success")
    return redirect(url_for("ops_alerts_page"))


def ops_alert_mute():
    _require_ops_mutation_auth()
    event_type = (request.form.get("event_type") or "").strip()
    minutes = parse_int(request.form.get("minutes") or "0") or 0
    if not event_type:
        flash("Choose an event type to mute.", "warn")
        return redirect(url_for("ops_alerts_page"))
    state = _load_notify_history()
    muted = state.get("muted_by_event", {})
    if not isinstance(muted, dict):
        muted = {}
    if minutes <= 0:
        muted.pop(event_type, None)
        record_admin_audit("ops_alert_unmute", {"event_type": event_type})
        flash(f"Removed mute for {event_type}.", "success")
    else:
        minutes = min(10080, max(1, minutes))
        until = datetime.now(ZoneInfo("America/New_York")) + timedelta(minutes=minutes)
        muted[event_type] = until.isoformat(timespec="seconds")
        record_admin_audit(
            "ops_alert_mute",
            {"event_type": event_type, "minutes": minutes, "until": muted[event_type]},
        )
        flash(f"Muted {event_type} for {minutes} minutes.", "success")
    state["muted_by_event"] = muted
    _save_notify_history(state)
    return redirect(url_for("ops_alerts_page"))


def ops_backups_config():
    _require_ops_mutation_auth()
    cfg = _load_auto_backup_config()
    cfg["enabled"] = request.form.get("enabled") == "1"
    cfg["run_weekends"] = request.form.get("run_weekends") == "1"
    cfg["run_times_et"] = _normalize_backup_times(request.form.get("run_times_et") or "")
    cfg["frequency_hours"] = max(
        1, min(168, parse_int(request.form.get("frequency_hours") or "24") or 24)
    )
    cfg["keep_count"] = max(3, min(120, parse_int(request.form.get("keep_count") or "21") or 21))
    saved_cfg = _save_auto_backup_config(cfg)
    scope_enabled = request.form.get("account_scope_enabled") == "1"
    scope_start = _normalize_scope_start_date(request.form.get("account_scope_start") or "")
    scope_label = (request.form.get("account_scope_label") or "").strip()
    scope_balance_raw = request.form.get("account_scope_start_balance") or ""
    if scope_enabled:
        try:
            if not scope_start:
                raise ValueError("invalid account scope start date")
            scope_balance = parse_float(scope_balance_raw)
            if scope_balance is None:
                raise ValueError("invalid account scope balance")
            repo.save_account_scope(scope_start, float(scope_balance), label=scope_label)
        except Exception:
            flash("Account scope not saved: use a valid date and starting balance.", "warn")
            return redirect(url_for("ops_backups_page"))
    else:
        repo.clear_account_scope()
    record_admin_audit(
        "auto_backup_config_saved",
        {
            "enabled": cfg["enabled"],
            "run_weekends": cfg["run_weekends"],
            "run_times_et": cfg["run_times_et"],
            "frequency_hours": cfg["frequency_hours"],
            "keep_count": cfg["keep_count"],
            "account_scope_enabled": scope_enabled,
            "account_scope_start": scope_start if scope_enabled else "",
            "account_scope_label": scope_label if scope_enabled else "",
        },
    )
    if saved_cfg:
        flash("Backup settings and account scope saved.", "success")
    else:
        flash("Settings applied in this session, but backup config could not be persisted.", "warn")
    return redirect(url_for("ops_backups_page"))


def _start_backup_job(reason: str, actor: str) -> Dict[str, Any]:
    app = current_app._get_current_object()
    job = _create_bg_job("backup", "Backup Snapshot", {"reason": reason})

    def runner() -> None:
        started = time.time()
        try:
            _update_bg_job(
                job["id"],
                status="running",
                stage="create_archive",
                message="Creating backup archive from current app data.",
            )
            with app.app_context():
                out = _run_backup_once(reason=reason, actor=actor)
            if not out.get("ok"):
                raise RuntimeError(str(out.get("error") or "Backup failed."))
            summary = _build_action_result_summary(
                tone="success",
                title="Backup Created",
                happened=f"Saved snapshot {out.get('name')}.",
                changed="Database and uploads were archived into a new restore point.",
                next_action="Run a dry run before restoring, or keep auto backup enabled after major imports.",
                metrics=[
                    {"label": "Archive", "value": str(out.get("name") or "—")},
                    {"label": "Size", "value": f"{int(out.get('size_bytes') or 0)} bytes"},
                ],
            )
            _update_bg_job(
                job["id"],
                status="success",
                stage="complete",
                message=f"Backup created: {out.get('name')}",
                duration_sec=round(max(0.0, time.time() - started), 2),
                summary=out,
                result_summary=summary,
            )
        except Exception as e:  # pragma: no cover
            summary = _build_action_result_summary(
                tone="danger",
                title="Backup Failed",
                happened=f"Backup did not complete: {e}",
                changed="No new restore point was saved.",
                next_action="Check disk space and file permissions, then rerun the backup.",
            )
            _update_bg_job(
                job["id"],
                status="failed",
                stage="failed",
                message=f"Backup failed: {e}",
                duration_sec=round(max(0.0, time.time() - started), 2),
                summary={"ok": False, "error": str(e)},
                result_summary=summary,
            )

    threading.Thread(target=runner, daemon=True, name=f"backup-job-{job['id'][:8]}").start()
    return job


def _safe_backup_file_path(name: str) -> str:
    clean = (name or "").strip().replace("\\", "/").split("/")[-1]
    if not clean.endswith(".zip"):
        raise ValueError("invalid backup file")
    full = os.path.abspath(os.path.join(_auto_backup_dir(), clean))
    root = os.path.abspath(_auto_backup_dir())
    if not full.startswith(root + os.sep):
        raise ValueError("unsafe backup path")
    return full


def _uploaded_restore_dir() -> str:
    path = _upload_file(".restore_uploads")
    os.makedirs(path, exist_ok=True)
    return path


def _save_uploaded_restore_archive(upload: Any) -> str:
    filename = secure_filename(str(getattr(upload, "filename", "") or "").strip())
    if not filename or not filename.lower().endswith(".zip"):
        raise ValueError("Please upload a .zip backup file.")
    stamp = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d_%H%M%S")
    out_name = f"restore_upload_{stamp}_{uuid4().hex[:10]}_{filename}"
    out_path = os.path.join(_uploaded_restore_dir(), out_name)
    upload.save(out_path)
    return out_path


def ops_backups_page():
    cfg = _load_auto_backup_config()
    backups = _list_saved_backups()
    account_scope = repo.account_scope_snapshot()
    persistence = app_runtime.persistence_snapshot()
    dry_run_name = (request.args.get("dry_run") or "").strip()
    dry_run_report: Dict[str, Any] | None = None
    if dry_run_name:
        try:
            dry_path = _safe_backup_file_path(dry_run_name)
            if os.path.isfile(dry_path):
                dry_run_report = _restore_dry_run(dry_path)
        except Exception:
            dry_run_report = None
    audit_action = (request.args.get("audit_action") or "all").strip().lower()
    audit_limit_raw = parse_int(request.args.get("audit_limit") or "60") or 60
    audit_limit = max(20, min(300, int(audit_limit_raw or 60)))
    audit_rows = _load_system_activity(limit=audit_limit, category=audit_action)
    content = render_template(
        "ops/backups.html",
        cfg=cfg,
        backups=backups,
        backup_dir=_auto_backup_dir(),
        dry_run_name=dry_run_name,
        dry_run_report=dry_run_report,
        audit_rows=audit_rows,
        backup_badges=backup_state_badges(cfg, audit_rows),
        audit_action=audit_action,
        audit_limit=audit_limit,
        account_scope=account_scope,
        persistence=persistence,
    )
    return render_page(content, active="ops")


def ops_backups_run_now():
    _require_ops_mutation_auth()
    if (request.args.get("async") or "").strip() == "1":
        job = _start_backup_job(reason="manual_ops_run", actor=_alerts_actor())
        return jsonify({"ok": True, "job": _job_response_payload(job)})
    out = _run_backup_once(reason="manual_ops_run", actor=_alerts_actor())
    if out.get("ok"):
        flash(f"Backup created: {out.get('name')}", "success")
    else:
        flash(f"Backup failed: {out.get('error')}", "warn")
    return redirect(url_for("ops_backups_page"))


def ops_backups_download(name: str):
    try:
        path = _safe_backup_file_path(name)
    except ValueError:
        abort(400)
    if not os.path.isfile(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=os.path.basename(path))


def _clear_live_app_data(*, preserve_backups: bool = True) -> Dict[str, Any]:
    db_path = str(app_runtime.DB_PATH)
    upload_root = str(app_runtime.UPLOAD_DIR)
    books_root = _books_dir()
    preserved: set[str] = set()
    if preserve_backups:
        preserved.update(
            {
                os.path.abspath(_auto_backup_dir()),
                os.path.abspath(_auto_backup_config_path()),
                os.path.abspath(_admin_audit_log_path()),
            }
        )

    if os.path.exists(db_path):
        os.unlink(db_path)
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    run_migrations(db_path)

    if os.path.isdir(upload_root):
        for name in os.listdir(upload_root):
            path = os.path.abspath(os.path.join(upload_root, name))
            if path in preserved:
                continue
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=False)
            elif os.path.exists(path):
                os.unlink(path)
    os.makedirs(upload_root, exist_ok=True)

    if os.path.isdir(books_root):
        for name in os.listdir(books_root):
            path = os.path.join(books_root, name)
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=False)
            elif os.path.exists(path):
                os.unlink(path)
    os.makedirs(books_root, exist_ok=True)

    return {
        "db_path": db_path,
        "upload_root": upload_root,
        "books_root": books_root,
        "preserved_backups": preserve_backups,
    }


def _start_restore_job(
    path: str,
    actor: str,
    clear_first: bool = False,
    cleanup_source: bool = False,
) -> Dict[str, Any]:
    app = current_app._get_current_object()
    job = _create_bg_job(
        "restore",
        "Restore Backup",
        {"file": os.path.basename(path), "clear_first": bool(clear_first)},
    )

    def runner() -> None:
        started = time.time()
        try:
            _update_bg_job(
                job["id"],
                status="running",
                stage="validate_archive",
                message="Validating backup archive before restore.",
            )
            with app.app_context():
                _update_bg_job(
                    job["id"],
                    status="running",
                    stage="apply_restore",
                    message="Applying backup to database and uploads.",
                )
                if clear_first:
                    _clear_live_app_data(preserve_backups=True)
                    record_admin_audit(
                        "live_data_cleared",
                        {"mode": "before_restore", "file": os.path.basename(path)},
                        actor=actor,
                    )
                _restore_from_backup_path(path)
                record_admin_audit(
                    "backup_restored_from_center",
                    {"file": os.path.basename(path), "clear_first": bool(clear_first)},
                    actor=actor,
                )
            summary = _build_action_result_summary(
                tone="success",
                title="Restore Complete",
                happened=f"Restored from {os.path.basename(path)}.",
                changed=(
                    "Live data was cleared first, then the selected backup was applied."
                    if clear_first
                    else "Database rows were replaced and upload files were merged from the selected backup."
                ),
                next_action="Run Integrity Check next so ledger, reviews, and balances are verified on the restored state.",
                metrics=[
                    {"label": "Source", "value": os.path.basename(path)},
                ],
            )
            _update_bg_job(
                job["id"],
                status="success",
                stage="complete",
                message=f"Restored from {os.path.basename(path)}.",
                duration_sec=round(max(0.0, time.time() - started), 2),
                summary={"file": os.path.basename(path)},
                result_summary=summary,
            )
        except Exception as e:  # pragma: no cover
            summary = _build_action_result_summary(
                tone="danger",
                title="Restore Failed",
                happened=f"Restore did not complete: {e}",
                changed="The selected backup was not fully applied.",
                next_action="Review the backup file, then retry restore or run a dry run first.",
            )
            _update_bg_job(
                job["id"],
                status="failed",
                stage="failed",
                message=f"Restore failed: {e}",
                duration_sec=round(max(0.0, time.time() - started), 2),
                summary={"file": os.path.basename(path), "error": str(e)},
                result_summary=summary,
            )
        finally:
            if cleanup_source:
                try:
                    os.unlink(path)
                except OSError:
                    pass

    threading.Thread(target=runner, daemon=True, name=f"restore-job-{job['id'][:8]}").start()
    return job


def ops_backups_restore():
    _require_ops_mutation_auth()
    name = (request.form.get("name") or "").strip()
    clear_first = (request.form.get("clear_first") or "").strip() == "1"
    try:
        path = _safe_backup_file_path(name)
    except ValueError:
        flash("Invalid backup name.", "warn")
        return redirect(url_for("ops_backups_page"))
    if not os.path.isfile(path):
        flash("Backup not found.", "warn")
        return redirect(url_for("ops_backups_page"))
    if (request.args.get("async") or "").strip() == "1":
        job = _start_restore_job(path, actor=_alerts_actor(), clear_first=clear_first)
        return jsonify({"ok": True, "job": _job_response_payload(job)})
    try:
        if clear_first:
            _clear_live_app_data(preserve_backups=True)
            record_admin_audit(
                "live_data_cleared",
                {"mode": "before_restore", "file": os.path.basename(path)},
                actor=_alerts_actor(),
            )
        _restore_from_backup_path(path)
    except Exception as e:
        flash(f"Restore failed: {e}", "warn")
        return redirect(url_for("ops_backups_page"))
    record_admin_audit(
        "backup_restored_from_center",
        {"file": os.path.basename(path), "clear_first": bool(clear_first)},
    )
    flash(
        f"{'Cleared live data and r' if clear_first else 'R'}estored from {os.path.basename(path)}.",
        "success",
    )
    return redirect(url_for("ops_backups_page"))


def ops_backups_clear_live():
    _require_ops_mutation_auth()
    try:
        _clear_live_app_data(preserve_backups=True)
        record_admin_audit("live_data_cleared", {"mode": "manual_clear"})
        flash(
            "Live database, uploads, and books were cleared. Saved backups were preserved.",
            "success",
        )
    except Exception as e:
        flash(f"Clear failed: {e}", "warn")
    return redirect(url_for("ops_backups_page"))


def ops_job_status(job_id: str):
    _require_ops_mutation_auth()
    job = _get_bg_job((job_id or "").strip())
    if not job:
        return jsonify({"ok": False, "error": "job_not_found"}), 404
    return jsonify({"ok": True, "job": _job_response_payload(job)})


def ops_backups_restore_dry_run():
    _require_ops_mutation_auth()
    name = (request.form.get("name") or "").strip()
    if not name:
        flash("Pick a backup file for dry run.", "warn")
        return redirect(url_for("ops_backups_page"))
    return redirect(url_for("ops_backups_page", dry_run=name))


def ops_backups_delete():
    _require_ops_mutation_auth()
    name = (request.form.get("name") or "").strip()
    try:
        path = _safe_backup_file_path(name)
    except ValueError:
        flash("Invalid backup name.", "warn")
        return redirect(url_for("ops_backups_page"))
    if not os.path.isfile(path):
        flash("Backup not found.", "warn")
        return redirect(url_for("ops_backups_page"))
    try:
        os.unlink(path)
        record_admin_audit("backup_deleted", {"file": os.path.basename(path)})
        flash(f"Deleted backup {os.path.basename(path)}.", "success")
    except OSError as e:
        flash(f"Delete failed: {e}", "warn")
    return redirect(url_for("ops_backups_page"))


def _start_integrity_job() -> Dict[str, Any]:
    app = current_app._get_current_object()
    actor = _alerts_actor()
    job = _create_bg_job("integrity", "Integrity Check", {"source": "ops_integrity"})

    def runner() -> None:
        started = time.time()
        try:
            _update_bg_job(
                job["id"],
                status="running",
                stage="scan_ledger",
                message="Scanning ledger, reviews, and balances...",
            )
            with app.app_context():
                snap = _integrity_health_snapshot()
            _update_bg_job(
                job["id"],
                status="running",
                stage="build_summary",
                message="Building integrity summary...",
            )
            diag = snap.get("diag", {})
            summary_message = (
                "Integrity check: "
                f"issues={snap.get('issues', 0)} · "
                f"stale_bal={diag.get('stale_balance_rows', 0)} · "
                f"missing_setup={diag.get('missing_setup', 0)} · "
                f"missing_session={diag.get('missing_session', 0)} · "
                f"missing_scores={diag.get('missing_score', 0)} · "
                f"duplicates={diag.get('duplicate_candidates', 0)} · "
                f"orphan_reviews={snap.get('orphan_reviews', 0)} · "
                f"missing_balance={snap.get('missing_balance', 0)}"
            )
            summary_card = _build_action_result_summary(
                tone="success" if int(snap.get("issues", 0)) == 0 else "warning",
                title="Integrity Summary",
                happened=summary_message,
                changed="Ledger, review coverage, and stored balances were scanned against the current dataset.",
                next_action=(
                    "No action needed."
                    if int(snap.get("issues", 0)) == 0
                    else "Open Diagnostics next, then resolve the flagged rows before the next import."
                ),
                metrics=[
                    {"label": "Issues", "value": str(int(snap.get("issues") or 0))},
                    {"label": "Orphans", "value": str(int(snap.get("orphan_reviews") or 0))},
                    {
                        "label": "Missing Balance",
                        "value": str(int(snap.get("missing_balance") or 0)),
                    },
                ],
            )
            record_admin_audit(
                "integrity_check_run",
                {
                    "issues": int(snap.get("issues", 0)),
                    "orphan_reviews": int(snap.get("orphan_reviews", 0)),
                    "missing_balance": int(snap.get("missing_balance", 0)),
                },
                actor=actor,
            )
            _update_bg_job(
                job["id"],
                status="success" if int(snap.get("issues", 0)) == 0 else "warning",
                stage="complete",
                message=summary_message,
                duration_sec=round(max(0.0, time.time() - started), 2),
                summary=snap,
                result_summary=summary_card,
            )
        except Exception as e:  # pragma: no cover
            summary_card = _build_action_result_summary(
                tone="danger",
                title="Integrity Check Failed",
                happened=f"Integrity check failed: {e}",
                changed="The integrity pass did not finish, so the current dataset was not fully verified.",
                next_action="Retry the integrity pass. If it fails again, inspect diagnostics and logs before importing again.",
            )
            _update_bg_job(
                job["id"],
                status="failed",
                stage="failed",
                message=f"Integrity check failed: {e}",
                duration_sec=round(max(0.0, time.time() - started), 2),
                summary={"issues": 0},
                result_summary=summary_card,
            )

    threading.Thread(target=runner, daemon=True, name=f"integrity-job-{job['id'][:8]}").start()
    return job


def ops_integrity_run():
    _require_ops_mutation_auth()
    if (request.args.get("async") or "").strip() == "1":
        job = _start_integrity_job()
        return jsonify({"ok": True, "job": job})
    snap = _integrity_health_snapshot()
    diag = snap.get("diag", {})
    msg = (
        "Integrity check: "
        f"issues={snap.get('issues', 0)} · "
        f"stale_bal={diag.get('stale_balance_rows', 0)} · "
        f"missing_setup={diag.get('missing_setup', 0)} · "
        f"missing_session={diag.get('missing_session', 0)} · "
        f"missing_scores={diag.get('missing_score', 0)} · "
        f"duplicates={diag.get('duplicate_candidates', 0)} · "
        f"orphan_reviews={snap.get('orphan_reviews', 0)} · "
        f"missing_balance={snap.get('missing_balance', 0)}"
    )
    flash(msg, "success" if int(snap.get("issues", 0)) == 0 else "warn")
    record_admin_audit(
        "integrity_check_run",
        {
            "issues": int(snap.get("issues", 0)),
            "orphan_reviews": int(snap.get("orphan_reviews", 0)),
            "missing_balance": int(snap.get("missing_balance", 0)),
        },
        actor=_alerts_actor(),
    )
    return redirect(url_for("ops_backups_page"))


def ops_integrity_job_status(job_id: str):
    _require_ops_mutation_auth()
    job = _get_bg_job((job_id or "").strip())
    if not job or str(job.get("kind") or "") != "integrity":
        return jsonify({"ok": False, "error": "job_not_found"}), 404
    return jsonify({"ok": True, "job": _job_response_payload(job)})


def _fetch_trades_for_rebuild(start_date: str = "", end_date: str = "") -> List[Dict[str, Any]]:
    where: List[str] = []
    params: List[Any] = []
    if start_date:
        where.append("trade_date >= ?")
        params.append(start_date)
    if end_date:
        where.append("trade_date <= ?")
        params.append(end_date)
    sql = "SELECT * FROM trades"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY trade_date ASC, id ASC"
    with db() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def _run_review_rebuild(
    *,
    start_date: str,
    end_date: str,
    scope: str,
    preserve_manual: bool,
    actor: str,
) -> Dict[str, Any]:
    trades = _fetch_trades_for_rebuild(start_date=start_date, end_date=end_date)
    review_map = repo.fetch_trade_reviews_map(
        [int(t["id"]) for t in trades if t.get("id") is not None]
    )
    rebuilt = 0
    skipped_existing = 0

    for t in trades:
        tid = int(t["id"])
        existing = review_map.get(tid)
        if scope == "missing" and existing:
            skipped_existing += 1
            continue

        payload = importing._auto_review_payload(t)
        if preserve_manual and existing:
            payload["strategy_id"] = existing.get("strategy_id")
            payload["strategy_label"] = (
                existing.get("strategy_label")
                or existing.get("setup_tag")
                or payload.get("setup_tag", "")
            )
            payload["setup_tag"] = (existing.get("setup_tag") or "").strip() or payload["setup_tag"]
            payload["session_tag"] = (existing.get("session_tag") or "").strip() or payload[
                "session_tag"
            ]
            if existing.get("checklist_score") is not None:
                payload["checklist_score"] = int(existing["checklist_score"])
            payload["rule_break_tags"] = (existing.get("rule_break_tags") or "").strip() or payload[
                "rule_break_tags"
            ]
            payload["review_note"] = (existing.get("review_note") or "").strip() or payload[
                "review_note"
            ]
        payload["rule_break_tags"] = _merge_auto_rule_break_tags(
            entry_price=parse_float(str(t.get("entry_price") or "")),
            exit_price=parse_float(str(t.get("exit_price") or "")),
            existing_tags=payload.get("rule_break_tags", ""),
        )

        repo.upsert_trade_review(
            trade_id=tid,
            strategy_id=payload.get("strategy_id"),
            strategy_label=payload.get("strategy_label", "") or payload.get("setup_tag", ""),
            setup_tag=payload.get("setup_tag", ""),
            session_tag=payload.get("session_tag", ""),
            checklist_score=payload.get("checklist_score"),
            rule_break_tags=payload.get("rule_break_tags", ""),
            review_note=payload.get("review_note", ""),
        )
        rebuilt += 1

    record_admin_audit(
        "trades_rebuild_reviews",
        {
            "rebuilt": rebuilt,
            "skipped_existing": skipped_existing,
            "scope": scope,
            "preserve_manual": preserve_manual,
            "start_date": start_date,
            "end_date": end_date,
        },
        actor=actor,
    )
    return {
        "rebuilt": rebuilt,
        "skipped_existing": skipped_existing,
        "trade_count": len(trades),
        "scope": scope,
        "preserve_manual": preserve_manual,
    }


def _start_review_rebuild_job(
    *, start_date: str, end_date: str, scope: str, preserve_manual: bool, actor: str
) -> Dict[str, Any]:
    app = current_app._get_current_object()
    job = _create_bg_job(
        "review_rebuild",
        "Review Rebuild",
        {
            "start_date": start_date,
            "end_date": end_date,
            "scope": scope,
            "preserve_manual": preserve_manual,
        },
    )

    def runner() -> None:
        started = time.time()
        try:
            _update_bg_job(
                job["id"],
                status="running",
                stage="collect_scope",
                message="Collecting trades in rebuild scope.",
            )
            with app.app_context():
                _update_bg_job(
                    job["id"],
                    status="running",
                    stage="rebuild_reviews",
                    message="Rebuilding trade review metadata.",
                )
                out = _run_review_rebuild(
                    start_date=start_date,
                    end_date=end_date,
                    scope=scope,
                    preserve_manual=preserve_manual,
                    actor=actor,
                )
            summary = _build_action_result_summary(
                tone="success",
                title="Review Rebuild Complete",
                happened=(
                    f"Updated {int(out.get('rebuilt') or 0)} review(s) and skipped "
                    f"{int(out.get('skipped_existing') or 0)} existing review(s)."
                ),
                changed="Review metadata was regenerated from the current trade rows in scope.",
                next_action="Open Trades or Analytics to spot-check a few rebuilt rows before making more changes.",
                metrics=[
                    {"label": "Trades In Scope", "value": str(int(out.get("trade_count") or 0))},
                    {"label": "Updated", "value": str(int(out.get("rebuilt") or 0))},
                    {"label": "Skipped", "value": str(int(out.get("skipped_existing") or 0))},
                ],
                actions=[
                    {"label": "Open Trades", "href": "/trades", "kind": "primary"},
                    {"label": "Analyze Performance", "href": "/analytics?tab=performance"},
                ],
            )
            _update_bg_job(
                job["id"],
                status="success",
                stage="complete",
                message="Review rebuild completed.",
                duration_sec=round(max(0.0, time.time() - started), 2),
                summary=out,
                result_summary=summary,
            )
        except Exception as e:  # pragma: no cover
            summary = _build_action_result_summary(
                tone="danger",
                title="Review Rebuild Failed",
                happened=f"Review rebuild did not complete: {e}",
                changed="Trade review rows were not fully regenerated.",
                next_action="Retry with a smaller date range, then inspect the affected trades.",
                actions=[
                    {"label": "Open Trades", "href": "/trades", "kind": "primary"},
                    {"label": "Open Rebuild Workspace", "href": "/trades/reviews/rebuild"},
                ],
            )
            _update_bg_job(
                job["id"],
                status="failed",
                stage="failed",
                message=f"Review rebuild failed: {e}",
                duration_sec=round(max(0.0, time.time() - started), 2),
                summary={"error": str(e)},
                result_summary=summary,
            )

    threading.Thread(target=runner, daemon=True, name=f"review-rebuild-job-{job['id'][:8]}").start()
    return job


def trades_open_positions():
    as_of = (request.args.get("as_of") or "").strip()
    q = (request.args.get("q") or "").strip()
    rows = repo.fetch_open_positions(as_of=as_of, q=q)

    grouped: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        ticker = (r.get("ticker") or "—").strip() or "—"
        opt_type = (r.get("opt_type") or "—").strip() or "—"
        strike = r.get("strike")
        strike_label = (
            "—"
            if strike is None
            else str(int(strike)) if float(strike).is_integer() else f"{float(strike):.2f}"
        )
        key = f"{ticker} {opt_type} {strike_label}"
        g = grouped.setdefault(
            key,
            {
                "symbol": key,
                "trades": 0,
                "contracts": 0,
                "total_spent": 0.0,
                "latest_date": "",
            },
        )
        g["trades"] += 1
        g["contracts"] += int(r.get("contracts") or 0)
        g["total_spent"] += float(r.get("total_spent") or 0.0)
        g["latest_date"] = max(g["latest_date"], str(r.get("trade_date") or ""))

    grouped_rows = sorted(
        grouped.values(), key=lambda x: (x["latest_date"], x["symbol"]), reverse=True
    )
    total_contracts = sum(int(r["contracts"]) for r in grouped_rows)
    total_spent = sum(float(r["total_spent"]) for r in grouped_rows)

    content = render_template(
        "trades/open_positions.html",
        grouped_rows=grouped_rows,
        total_contracts=total_contracts,
        total_spent=total_spent,
        rows=rows,
        as_of=as_of,
        q=q,
        money=money,
    )
    return render_page(content, active="trades")


def trades_rebuild_reviews():
    start_date = (request.values.get("start_date") or "").strip()
    end_date = (request.values.get("end_date") or "").strip()
    scope = (request.values.get("scope") or "missing").strip().lower()
    if scope not in {"missing", "all"}:
        scope = "missing"
    preserve_manual = (request.values.get("preserve_manual") or "1") == "1"

    if request.method == "POST":
        if (request.args.get("async") or "").strip() == "1":
            job = _start_review_rebuild_job(
                start_date=start_date,
                end_date=end_date,
                scope=scope,
                preserve_manual=preserve_manual,
                actor=_alerts_actor(),
            )
            return jsonify({"ok": True, "job": _job_response_payload(job)})
        out = _run_review_rebuild(
            start_date=start_date,
            end_date=end_date,
            scope=scope,
            preserve_manual=preserve_manual,
            actor=_alerts_actor(),
        )
        flash(
            (
                f"Rebuild complete: updated {int(out.get('rebuilt') or 0)} review(s), "
                f"skipped {int(out.get('skipped_existing') or 0)} existing review(s)."
            ),
            "success",
        )
        return redirect(
            url_for(
                "trades_rebuild_reviews",
                start_date=start_date,
                end_date=end_date,
                scope=scope,
                preserve_manual="1" if preserve_manual else "0",
            )
        )

    preview = _fetch_trades_for_rebuild(start_date=start_date, end_date=end_date)
    preview_reviews = repo.fetch_trade_reviews_map(
        [int(t["id"]) for t in preview if t.get("id") is not None]
    )
    preview_missing = sum(1 for t in preview if int(t["id"]) not in preview_reviews)

    content = render_template(
        "trades/rebuild_reviews.html",
        preview=preview,
        preview_reviews=preview_reviews,
        preview_missing=preview_missing,
        start_date=start_date,
        end_date=end_date,
        scope=scope,
        preserve_manual=preserve_manual,
    )
    return render_page(content, active="trades")
