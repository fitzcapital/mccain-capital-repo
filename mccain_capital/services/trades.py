"""Trades domain service functions."""

from __future__ import annotations

import os
import sqlite3
import json
import base64
import hmac
import hashlib
import shutil
import tempfile
import urllib.request
import urllib.error
import threading
import time
import zipfile
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional
from zoneinfo import ZoneInfo
from uuid import uuid4

from flask import (
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    render_template_string,
    request,
    session,
    send_file,
    url_for,
)
from werkzeug.utils import secure_filename

from mccain_capital.repositories import trades as repo
from mccain_capital.repositories import analytics as analytics_repo
from mccain_capital import auth
from mccain_capital import runtime as app_runtime
from mccain_capital.runtime import (
    UPLOAD_DIR,
    db,
    detect_paste_format,
    get_setting_float,
    latest_balance_overall,
    money,
    next_trading_day_iso,
    now_iso,
    normalize_opt_type,
    parse_float,
    parse_int,
    pct,
    prev_trading_day_iso,
    today_iso,
)
from mccain_capital.services import trades_importing as importing
from mccain_capital.services import vanquish_live_sync
from mccain_capital.services.ui import render_page, simple_msg
from mccain_capital.services.viewmodels import trades_data_trust

# Compatibility aliases used by extracted route bodies.
fetch_trades = repo.fetch_trades
fetch_trade_reviews_map = repo.fetch_trade_reviews_map
trade_day_stats = repo.trade_day_stats
calc_consistency = repo.calc_consistency
week_total_net = repo.week_total_net

BROKER_SYNC_CONFIG_PATH = os.path.join(UPLOAD_DIR, ".vanquish_sync.json")
BROKER_DEBUG_DIR = os.path.join(UPLOAD_DIR, "vanquish_debug")
BROKER_SYNC_STATUS_PATH = os.path.join(UPLOAD_DIR, ".vanquish_sync_last_run.json")
BROKER_SYNC_HISTORY_PATH = os.path.join(UPLOAD_DIR, ".vanquish_sync_history.json")
BROKER_IMPORT_HISTORY_PATH = os.path.join(UPLOAD_DIR, ".vanquish_import_history.json")
BROKER_NOTIFY_HISTORY_PATH = os.path.join(UPLOAD_DIR, ".vanquish_notify_history.json")
PLAYBOOK_CONFIG_PATH = os.path.join(UPLOAD_DIR, ".playbook_rules.json")
ADMIN_AUDIT_LOG_PATH = os.path.join(UPLOAD_DIR, ".admin_audit_log.json")
BROKER_AUTO_SYNC_CONFIG_PATH = os.path.join(UPLOAD_DIR, ".vanquish_auto_sync.json")
BROKER_AUTO_SYNC_LOCK_PATH = os.path.join(UPLOAD_DIR, ".vanquish_auto_sync.lock")
AUTO_BACKUP_CONFIG_PATH = os.path.join(UPLOAD_DIR, ".auto_backup_config.json")
AUTO_BACKUP_DIR = os.path.join(UPLOAD_DIR, "backups")
AUTO_BACKUP_LOCK_PATH = os.path.join(UPLOAD_DIR, ".auto_backup.lock")
BG_JOB_DIR = os.path.join(UPLOAD_DIR, ".bg_jobs")
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
}

_BG_JOB_LOCK = threading.Lock()
_BG_JOBS: Dict[str, Dict[str, Any]] = {}
_BG_JOB_MAX = 40

_AUDIT_ACTION_META = {
    "backup_created": {"label": "Backup Created", "group": "backup"},
    "backup_failed": {"label": "Backup Failed", "group": "backup"},
    "backup_restored_from_center": {"label": "Backup Restored", "group": "restore"},
    "manual_backup_restored": {"label": "Backup Restored", "group": "restore"},
    "manual_backup_downloaded": {"label": "Backup Downloaded", "group": "backup"},
    "backup_deleted": {"label": "Backup Deleted", "group": "backup"},
    "integrity_check_run": {"label": "Integrity Check", "group": "integrity"},
    "trades_rebuild_reviews": {"label": "Review Rebuild", "group": "review"},
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
    "open_login": "Broker login page did not load cleanly. Check Base Origin and network.",
    "locate_username": "Could not find the username input. Broker UI likely changed.",
    "locate_password": "Could not find the password input. Broker login form likely changed.",
    "submit_login": "Login did not complete. Validate credentials or check for MFA/CAPTCHA.",
    "open_workspace_menu": "Could not open the app menu after login. Workspace may still be loading.",
    "open_statement_dialog": "Could not open Account Statement from the menu.",
    "configure_statement_period": "Could not set statement date range in the dialog.",
    "generate_statement": "Generate Statement did not complete as expected.",
    "capture_statement_html": "Statement page loaded but HTML capture/parse failed.",
}

SYNC_STAGE_LABELS = {
    "start": "Queued and preparing sync run.",
    "open_login": "Opening broker login page.",
    "locate_username": "Finding username field.",
    "fill_username": "Entering username.",
    "locate_password": "Finding password field.",
    "submit_login": "Submitting broker login.",
    "open_workspace_menu": "Opening workspace menu.",
    "open_statement_dialog": "Opening statement dialog.",
    "configure_statement_period": "Setting statement date range.",
    "generate_statement": "Generating statement HTML.",
    "capture_statement_html": "Capturing statement HTML.",
    "parse_statement_html": "Parsing statement rows.",
    "reconcile_gate": "Running reconcile guardrails.",
    "import_trades": "Importing trades.",
    "import_complete": "Import complete.",
}


def _sync_stage_label(stage: str) -> str:
    key = str(stage or "").strip()
    return SYNC_STAGE_LABELS.get(key, key.replace("_", " ").strip().title() or "Working...")


def _bg_job_path(job_id: str) -> str:
    return os.path.join(BG_JOB_DIR, f"{job_id}.json")


def _write_bg_job(job: Dict[str, Any]) -> None:
    os.makedirs(BG_JOB_DIR, exist_ok=True)
    job_id = str(job.get("id") or "").strip()
    if not job_id:
        return
    path = _bg_job_path(job_id)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(job, f, indent=2)
    os.replace(tmp_path, path)


def _read_bg_job(job_id: str) -> Dict[str, Any]:
    path = _bg_job_path((job_id or "").strip())
    try:
        with open(path, "r", encoding="utf-8") as f:
            parsed = json.load(f)
            return parsed if isinstance(parsed, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _trim_bg_jobs_locked() -> None:
    try:
        os.makedirs(BG_JOB_DIR, exist_ok=True)
        entries: List[tuple[str, float]] = []
        for name in os.listdir(BG_JOB_DIR):
            if not name.endswith(".json"):
                continue
            full = os.path.join(BG_JOB_DIR, name)
            if not os.path.isfile(full):
                continue
            entries.append((full, os.path.getmtime(full)))
        if len(entries) <= _BG_JOB_MAX:
            return
        entries.sort(key=lambda item: item[1])
        for full, _ in entries[: max(0, len(entries) - _BG_JOB_MAX)]:
            try:
                os.unlink(full)
            except OSError:
                pass
    except OSError:
        return


def _create_bg_job(kind: str, title: str, requested: Dict[str, Any]) -> Dict[str, Any]:
    stamp = now_iso()
    job = {
        "id": uuid4().hex,
        "kind": kind,
        "title": title,
        "status": "queued",
        "stage": "start",
        "message": "Queued and waiting to start.",
        "requested": requested,
        "created_at": stamp,
        "updated_at": stamp,
        "duration_sec": None,
        "summary": {},
    }
    with _BG_JOB_LOCK:
        _BG_JOBS[job["id"]] = job
        _write_bg_job(job)
        _trim_bg_jobs_locked()
        return dict(job)


def _update_bg_job(job_id: str, **updates: Any) -> Dict[str, Any]:
    with _BG_JOB_LOCK:
        existing = _BG_JOBS.get(job_id) or _read_bg_job(job_id)
        if not existing:
            return {}
        job = dict(existing)
        job.update(updates)
        job["updated_at"] = now_iso()
        _BG_JOBS[job_id] = job
        _write_bg_job(job)
        return dict(job)


def _get_bg_job(job_id: str) -> Dict[str, Any]:
    with _BG_JOB_LOCK:
        cached = _BG_JOBS.get(job_id)
        if cached:
            return dict(cached)
        disk = _read_bg_job(job_id)
        if disk:
            _BG_JOBS[job_id] = disk
        return dict(disk)


def _build_action_result_summary(
    *,
    tone: str,
    title: str,
    happened: str,
    changed: Optional[str] = None,
    warnings: Optional[List[str]] = None,
    next_action: str = "",
    metrics: Optional[List[Dict[str, str]]] = None,
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
        with open(PLAYBOOK_CONFIG_PATH, "r", encoding="utf-8") as f:
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
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    with open(PLAYBOOK_CONFIG_PATH, "w", encoding="utf-8") as f:
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
    secret = (os.environ.get("SECRET_KEY") or "dev-secret-key").strip()
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
    }
    try:
        with open(BROKER_SYNC_CONFIG_PATH, "r", encoding="utf-8") as f:
            parsed = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return defaults
    for key in defaults:
        val = parsed.get(key, defaults[key])
        defaults[key] = str(val).strip() if val is not None else defaults[key]
    return defaults


def _save_broker_sync_config(data: Dict[str, str]) -> None:
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    with open(BROKER_SYNC_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _humanize_et_timestamp(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.astimezone(ZoneInfo("America/New_York")).strftime("%b %d, %Y %I:%M %p ET")
    except Exception:
        return text


def _load_last_sync_status() -> Dict[str, Any]:
    try:
        with open(BROKER_SYNC_STATUS_PATH, "r", encoding="utf-8") as f:
            parsed = json.load(f)
            if not isinstance(parsed, dict):
                return {}
            parsed["updated_at_human"] = _humanize_et_timestamp(str(parsed.get("updated_at") or ""))
            return parsed
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _save_last_sync_status(payload: Dict[str, Any]) -> None:
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    with open(BROKER_SYNC_STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    status = str(payload.get("status") or "").strip().lower()
    if status not in {"success", "failed", "debug_only"}:
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
            "duration_sec": (
                float(payload.get("duration_sec"))
                if payload.get("duration_sec") is not None
                else None
            ),
        }
    )
    if len(history) > SYNC_HISTORY_MAX:
        history = history[-SYNC_HISTORY_MAX:]
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    with open(BROKER_SYNC_HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)
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
        with open(BROKER_SYNC_HISTORY_PATH, "r", encoding="utf-8") as f:
            parsed = json.load(f)
            return [x for x in parsed if isinstance(x, dict)] if isinstance(parsed, list) else []
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return []


def _load_notify_history() -> Dict[str, Any]:
    try:
        with open(BROKER_NOTIFY_HISTORY_PATH, "r", encoding="utf-8") as f:
            parsed = json.load(f)
            return parsed if isinstance(parsed, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _save_notify_history(state: Dict[str, Any]) -> None:
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    with open(BROKER_NOTIFY_HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def _load_admin_audit() -> List[Dict[str, Any]]:
    try:
        with open(ADMIN_AUDIT_LOG_PATH, "r", encoding="utf-8") as f:
            parsed = json.load(f)
            return [x for x in parsed if isinstance(x, dict)] if isinstance(parsed, list) else []
    except (FileNotFoundError, json.JSONDecodeError, OSError):
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
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    with open(ADMIN_AUDIT_LOG_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)


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


def _load_auto_backup_config() -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "enabled": False,
        "frequency_hours": 24,
        "run_times_et": ["16:30"],
        "run_weekends": False,
        "last_run_slot_key": "",
        "keep_count": 21,
        "last_run_at": "",
        "last_status": "",
        "last_message": "",
    }
    try:
        with open(AUTO_BACKUP_CONFIG_PATH, "r", encoding="utf-8") as f:
            parsed = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return cfg
    if isinstance(parsed, dict):
        cfg["enabled"] = bool(parsed.get("enabled"))
        cfg["frequency_hours"] = max(1, min(168, int(parsed.get("frequency_hours") or 24)))
        times = parsed.get("run_times_et")
        if isinstance(times, list):
            cfg["run_times_et"] = [str(x).strip() for x in times if str(x).strip()]
        elif isinstance(times, str):
            cfg["run_times_et"] = [x.strip() for x in times.split(",") if x.strip()]
        cfg["run_weekends"] = bool(parsed.get("run_weekends"))
        cfg["last_run_slot_key"] = str(parsed.get("last_run_slot_key") or "")
        cfg["keep_count"] = max(3, min(120, int(parsed.get("keep_count") or 21)))
        cfg["last_run_at"] = str(parsed.get("last_run_at") or "")
        cfg["last_status"] = str(parsed.get("last_status") or "")
        cfg["last_message"] = str(parsed.get("last_message") or "")
    if not cfg["run_times_et"]:
        cfg["run_times_et"] = ["16:30"]
    return cfg


def _save_auto_backup_config(cfg: Dict[str, Any]) -> None:
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    with open(AUTO_BACKUP_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)


def _create_backup_archive(reason: str, actor: str) -> Dict[str, Any]:
    stamp = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d_%H%M%S")
    os.makedirs(AUTO_BACKUP_DIR, exist_ok=True)
    name = f"mccain_backup_{stamp}_{secure_filename(reason or 'manual')}.zip"
    out_path = os.path.join(AUTO_BACKUP_DIR, name)
    db_path = str(app_runtime.DB_PATH)
    upload_root = str(app_runtime.UPLOAD_DIR)
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if os.path.exists(db_path):
            zf.write(db_path, arcname="data/journal.db")
        if os.path.isdir(upload_root):
            for root, _, files in os.walk(upload_root):
                for fn in files:
                    full = os.path.join(root, fn)
                    if os.path.abspath(full).startswith(os.path.abspath(AUTO_BACKUP_DIR) + os.sep):
                        continue
                    rel = os.path.relpath(full, upload_root)
                    zf.write(full, arcname=f"data/uploads/{rel}")
        zf.writestr(
            "data/meta.json",
            json.dumps(
                {
                    "exported_at": now_iso(),
                    "reason": reason,
                    "actor": actor,
                    "db_path": db_path,
                    "upload_dir": upload_root,
                    "app": "mccain-capital",
                },
                indent=2,
            ),
        )
    return {"path": out_path, "name": name, "size_bytes": os.path.getsize(out_path)}


def _prune_auto_backups(keep_count: int) -> None:
    if not os.path.isdir(AUTO_BACKUP_DIR):
        return
    files = [
        os.path.join(AUTO_BACKUP_DIR, n)
        for n in os.listdir(AUTO_BACKUP_DIR)
        if n.endswith(".zip") and os.path.isfile(os.path.join(AUTO_BACKUP_DIR, n))
    ]
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    for p in files[max(3, keep_count) :]:
        try:
            os.unlink(p)
        except OSError:
            pass


def _normalize_backup_times(raw: str) -> List[str]:
    out: List[str] = []
    for token in [x.strip() for x in (raw or "").split(",") if x.strip()]:
        try:
            dt = datetime.strptime(token, "%H:%M")
            out.append(dt.strftime("%H:%M"))
            continue
        except ValueError:
            pass
        try:
            dt = datetime.strptime(token, "%I:%M %p")
            out.append(dt.strftime("%H:%M"))
        except ValueError:
            continue
    dedup = sorted(set(out))
    return dedup or ["16:30"]


def _notify_dedupe_window_seconds(event_type: str) -> int:
    return max(0, int(NOTIFY_DEDUPE_BY_EVENT.get(event_type, NOTIFY_DEFAULT_DEDUPE_SECONDS)))


def _notification_fingerprint(
    event_type: str, title: str, message: str, extra: Optional[Dict[str, Any]]
) -> str:
    obj = {
        "event_type": event_type,
        "title": title,
        "message": message,
        "extra": extra or {},
    }
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _append_notification_history(state: Dict[str, Any], payload: Dict[str, Any]) -> None:
    sent = state.get("sent", [])
    if not isinstance(sent, list):
        sent = []
    sent.append(payload)
    if len(sent) > 200:
        sent = sent[-200:]
    state["sent"] = sent
    _save_notify_history(state)


def _parse_iso_epoch(raw: str) -> Optional[float]:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("America/New_York"))
    return float(dt.timestamp())


def _record_alert_event(state: Dict[str, Any], payload: Dict[str, Any]) -> None:
    alerts = state.get("alerts", [])
    if not isinstance(alerts, list):
        alerts = []
    fingerprint = _notification_fingerprint(
        str(payload.get("event_type") or ""),
        str(payload.get("title") or ""),
        str(payload.get("message") or ""),
        payload.get("extra") if isinstance(payload.get("extra"), dict) else None,
    )
    now = str(payload.get("ts") or now_iso())
    delivery = payload.get("delivery") if isinstance(payload.get("delivery"), dict) else {}
    existing = None
    for a in alerts:
        if (
            isinstance(a, dict)
            and str(a.get("fingerprint") or "") == fingerprint
            and str(a.get("status") or "open") != "resolved"
        ):
            existing = a
            break
    if existing is None:
        alert = {
            "id": f"al_{int(time.time())}_{fingerprint[:8]}",
            "fingerprint": fingerprint,
            "event_type": str(payload.get("event_type") or ""),
            "title": str(payload.get("title") or ""),
            "message": str(payload.get("message") or ""),
            "extra": payload.get("extra") if isinstance(payload.get("extra"), dict) else {},
            "status": "muted" if str(delivery.get("status") or "") == "muted" else "open",
            "count": 1,
            "first_seen_at": now,
            "last_seen_at": now,
            "last_delivery": delivery,
            "ack_by": "",
            "ack_at": "",
            "resolved_by": "",
            "resolved_at": "",
        }
        alerts.append(alert)
    else:
        existing["count"] = int(existing.get("count") or 0) + 1
        existing["last_seen_at"] = now
        existing["last_delivery"] = delivery
        if str(existing.get("status") or "") in {"acknowledged", "muted"} and str(
            delivery.get("status") or ""
        ) not in {"skipped_dedupe"}:
            existing["status"] = "open"
            existing["ack_by"] = ""
            existing["ack_at"] = ""
    if len(alerts) > 300:
        alerts = alerts[-300:]
    state["alerts"] = alerts


def _signed_headers(body: bytes, event_type: str, ts: str) -> Dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "X-McCain-Event": event_type,
        "X-McCain-Timestamp": ts,
    }
    if NOTIFY_WEBHOOK_SECRET:
        digest = hmac.new(NOTIFY_WEBHOOK_SECRET.encode("utf-8"), body, hashlib.sha256).hexdigest()
        headers["X-McCain-Signature"] = f"sha256={digest}"
    return headers


def _emit_notification(
    event_type: str, title: str, message: str, extra: Optional[Dict[str, Any]] = None
):
    state = _load_notify_history()
    now_epoch = time.time()
    fp = _notification_fingerprint(event_type, title, message, extra)
    window = _notify_dedupe_window_seconds(event_type)
    dedupe = state.get("dedupe", {})
    if not isinstance(dedupe, dict):
        dedupe = {}
    last_ts = dedupe.get(fp)
    if window > 0 and isinstance(last_ts, (int, float)) and (now_epoch - float(last_ts)) < window:
        dedupe_payload = {
            "event_type": event_type,
            "title": title,
            "message": message,
            "ts": now_iso(),
            "extra": extra or {},
            "delivery": {"status": "skipped_dedupe", "window_sec": window},
        }
        _record_alert_event(state, dedupe_payload)
        _append_notification_history(state, dedupe_payload)
        _save_notify_history(state)
        return

    payload: Dict[str, Any] = {
        "event_type": event_type,
        "title": title,
        "message": message,
        "ts": now_iso(),
    }
    if extra:
        payload["extra"] = extra
    dedupe[fp] = now_epoch
    if len(dedupe) > 600:
        keep = sorted(
            ((k, v) for k, v in dedupe.items() if isinstance(v, (int, float))),
            key=lambda kv: float(kv[1]),
            reverse=True,
        )[:500]
        dedupe = {k: v for k, v in keep}
    state["dedupe"] = dedupe
    muted = state.get("muted_by_event", {})
    if not isinstance(muted, dict):
        muted = {}
    muted_until = str(muted.get(event_type) or "")
    muted_until_epoch = _parse_iso_epoch(muted_until)
    if muted_until_epoch is not None and muted_until_epoch > now_epoch:
        payload["delivery"] = {"status": "muted", "muted_until": muted_until}
        _record_alert_event(state, payload)
        _append_notification_history(state, payload)
        _save_notify_history(state)
        return
    if not NOTIFY_WEBHOOK_URL:
        payload["delivery"] = {"status": "local_only"}
        _record_alert_event(state, payload)
        _append_notification_history(state, payload)
        _save_notify_history(state)
        return
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    attempts = max(1, int(NOTIFY_RETRY_ATTEMPTS))
    wait = max(0.0, float(NOTIFY_RETRY_BACKOFF_SEC))
    scale = max(1.0, float(NOTIFY_RETRY_BACKOFF_MULTIPLIER))
    last_error = ""
    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(
                NOTIFY_WEBHOOK_URL,
                data=body,
                headers=_signed_headers(body, event_type=event_type, ts=str(payload["ts"])),
                method="POST",
            )
            urllib.request.urlopen(req, timeout=4).read()
            payload["delivery"] = {"status": "delivered", "attempt": attempt}
            _record_alert_event(state, payload)
            _append_notification_history(state, payload)
            _save_notify_history(state)
            return
        except urllib.error.HTTPError as e:
            last_error = f"http_{e.code}"
            retryable = int(e.code) >= 500
        except (urllib.error.URLError, TimeoutError, ValueError):
            last_error = "transport_error"
            retryable = True
        if attempt >= attempts or not retryable:
            break
        if wait > 0:
            time.sleep(wait)
            wait *= scale
    payload["delivery"] = {"status": "failed", "attempts": attempts, "error": last_error}
    _record_alert_event(state, payload)
    _append_notification_history(state, payload)
    _save_notify_history(state)


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
    attempts = len(recent)
    success = len([e for e in recent if str(e.get("status")) == "success"])
    failed = len([e for e in recent if str(e.get("status")) == "failed"])
    success_rate = (success / attempts * 100.0) if attempts else 0.0
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
    if fail_stage_counts:
        top_failure_stage = sorted(fail_stage_counts.items(), key=lambda kv: kv[1], reverse=True)[
            0
        ][0]
    by_source: Dict[str, Dict[str, int]] = {}
    for e in recent:
        src = str(e.get("source") or "unknown")
        bucket = by_source.setdefault(src, {"attempts": 0, "success": 0, "failed": 0})
        bucket["attempts"] += 1
        if str(e.get("status")) == "success":
            bucket["success"] += 1
        elif str(e.get("status")) == "failed":
            bucket["failed"] += 1
    return {
        "days": int(days),
        "attempts": attempts,
        "success": success,
        "failed": failed,
        "success_rate": success_rate,
        "avg_duration_sec": avg_duration_sec,
        "top_failure_stage": top_failure_stage,
        "by_source": by_source,
        "recent": list(reversed(recent[-8:])),
    }


def _load_import_history() -> List[Dict[str, Any]]:
    try:
        with open(BROKER_IMPORT_HISTORY_PATH, "r", encoding="utf-8") as f:
            parsed = json.load(f)
            return [x for x in parsed if isinstance(x, dict)] if isinstance(parsed, list) else []
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return []


def _append_import_history(entry: Dict[str, Any]) -> None:
    history = _load_import_history()
    history.append(entry)
    if len(history) > IMPORT_HISTORY_MAX:
        history = history[-IMPORT_HISTORY_MAX:]
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    with open(BROKER_IMPORT_HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)


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
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        with open(BROKER_IMPORT_HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2)


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
        conn.execute("DELETE FROM trades WHERE import_batch_id = ?", (batch_id,))
    starting = float(get_setting_float("starting_balance", 50000.0))
    repo.recompute_balances(starting_balance=starting)
    _mark_import_batch_rolled_back(batch_id)
    _emit_notification(
        "batch_rollback",
        "Import batch rolled back",
        f"Rolled back batch {batch_id} and deleted {len(trade_ids)} trade(s).",
        {"batch_id": batch_id, "deleted_trades": len(trade_ids)},
    )
    record_admin_audit(
        "rollback_import_batch",
        {"batch_id": batch_id, "deleted_trades": len(trade_ids)},
    )
    flash(f"Rolled back batch {batch_id} ({len(trade_ids)} trades).", "success")
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
        with open(BROKER_AUTO_SYNC_CONFIG_PATH, "r", encoding="utf-8") as f:
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
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    to_save = dict(cfg)
    to_save.pop("keyring_available", None)
    to_save.pop("password_stored", None)
    with open(BROKER_AUTO_SYNC_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(to_save, f, indent=2)


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
    rel = os.path.relpath(path, UPLOAD_DIR)
    return rel.replace("\\", "/")


def _debug_safe_path(rel: str) -> str:
    clean = (rel or "").replace("\\", "/").lstrip("/")
    abs_path = os.path.abspath(os.path.join(UPLOAD_DIR, clean))
    root = os.path.abspath(UPLOAD_DIR)
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


def _handle_statement_html_import(path: str, mode: str, source_label: str):
    paste_text, balance_val, warns = importing.parse_statement_html_to_broker_paste(path)

    if mode == "broker":
        batch_id = _new_import_batch_id("stmt")
        if not paste_text:
            return render_page(
                render_template_string(
                    """
                    <div class="card"><div class="toolbar">
                      <div class="pill">⛔ HTML parsed, but no trade rows found</div>
                      <div class="hr"></div>
                      <div class="tiny metaBlue line16">
                        {% for m in warns %}• {{ m }}<br>{% endfor %}
                      </div>
                      <div class="hr"></div>
                      <a class="btn" href="/trades/upload/statement">Back</a>
                    </div></div>
                    """,
                    warns=warns or [],
                ),
                active="trades",
            )

        _, _, pre_report = importing.insert_trades_from_broker_paste_with_report(
            paste_text,
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
                    render_template_string(
                        """
                        <div class="card"><div class="toolbar">
                          <div class="pill">⛔ Reconciliation Gate Blocked Import</div>
                          <div class="stack10">This import was not committed.</div>
                          <div class="tiny metaRed line16">
                            {% for r in reasons %}• {{ r }}<br>{% endfor %}
                          </div>
                          <div class="hr"></div>
                          {{ reconciliation_html|safe }}
                          <div class="hr"></div>
                          <a class="btn" href="/trades/upload/statement?ws=reconcile">Open Reconcile Workspace</a>
                          <a class="btn" href="/trades/upload/statement">Back</a>
                        </div></div>
                        """,
                        reasons=gate["reasons"],
                        reconciliation_html=_reconciliation_block(pre_report),
                    ),
                    active="trades",
                )

        inserted, errors, report = importing.insert_trades_from_broker_paste_with_report(
            paste_text,
            ending_balance=balance_val,
            commit=True,
            import_batch_id=batch_id,
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
            render_template_string(
                """
                <div class="card"><div class="toolbar">
                  <div class="pill">🧾 HTML → Trades ✅</div>
                  <div class="stack10">Inserted <b>{{ inserted }}</b> trade{{ '' if inserted==1 else 's' }}.</div>
                  {% if msgs %}
                    <div class="hr"></div>
                    <div class="tiny metaBlue line16">
                      {% for m in msgs %}• {{ m }}<br>{% endfor %}
                    </div>
                  {% endif %}
                  {{ reconciliation_html|safe }}
                  <div class="hr"></div>
                  <a class="btn primary" href="/trades">Trades 📅</a>
                  <a class="btn" href="/trades/upload/statement">Upload Another</a>
                </div></div>
                """,
                inserted=inserted,
                msgs=msgs,
                reconciliation_html=reconciliation_html,
            ),
            active="trades",
        )

    if balance_val is None:
        return render_page(
            render_template_string(
                """
                <div class="card"><div class="toolbar">
                  <div class="pill">⛔ Balance not found in HTML</div>
                  <div class="hr"></div>
                  <div class="tiny metaBlue line16">
                    {% for m in warns %}• {{ m }}<br>{% endfor %}
                  </div>
                  <div class="hr"></div>
                  <a class="btn" href="/trades/upload/statement">Back</a>
                </div></div>
                """,
                warns=warns or [],
            ),
            active="trades",
        )

    importing.insert_balance_snapshot(today_iso(), balance_val, raw_line=source_label)
    _record_import_batch(
        batch_id=_new_import_batch_id("bal"),
        source=source_label,
        mode="balance",
        report={
            "inserted_trades": 0,
            "duplicates_skipped": 0,
            "open_contracts": 0,
            "errors_count": 0,
            "warnings_count": len(warns or []),
            "statement_ending_balance": balance_val,
            "ledger_ending_balance": latest_balance_overall(),
            "balance_delta": (latest_balance_overall() - float(balance_val)),
        },
        status="success",
        message="Imported statement ending balance snapshot.",
    )
    return redirect(url_for("trades_page"))


def _reconciliation_block(report: Optional[dict]) -> str:
    if not report:
        return ""
    return render_template_string(
        """
        <div class="hr"></div>
        <div class="pill">🧾 Import Reconciliation</div>
        <div class="hr"></div>
        <table>
          <tbody>
            <tr><td>Fills Parsed</td><td>{{ report.fills_parsed }}</td></tr>
            <tr><td>Round-Trips Paired</td><td>{{ report.pairs_completed }}</td></tr>
            <tr><td>Inserted Trades</td><td>{{ report.inserted_trades }}</td></tr>
            <tr><td>Duplicates Skipped</td><td>{{ report.duplicates_skipped }}</td></tr>
            <tr><td>Open Contracts Remaining</td><td>{{ report.open_contracts }}</td></tr>
            <tr><td>Errors / Warnings</td><td>{{ report.errors_count }} / {{ report.warnings_count }}</td></tr>
            <tr>
              <td>Statement Ending Balance</td>
              <td>
                {% if report.statement_ending_balance is not none %}{{ money(report.statement_ending_balance) }}{% else %}Not provided{% endif %}
              </td>
            </tr>
            <tr>
              <td>Ledger Ending Balance</td>
              <td>
                {% if report.ledger_ending_balance is not none %}{{ money(report.ledger_ending_balance) }}{% else %}Not available{% endif %}
              </td>
            </tr>
            <tr>
              <td>Balance Delta (Ledger - Statement)</td>
              <td>
                {% if report.balance_delta is not none %}{{ money(report.balance_delta) }}{% else %}Not computed{% endif %}
              </td>
            </tr>
          </tbody>
        </table>
      """,
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


def _derived_balance_map(as_of: Optional[str] = None) -> Dict[int, float]:
    starting = float(get_setting_float("starting_balance", 50000.0))
    with db() as conn:
        if as_of:
            rows = conn.execute(
                """
                SELECT id, net_pl
                FROM trades
                WHERE trade_date <= ? AND net_pl IS NOT NULL
                ORDER BY trade_date ASC, id ASC
                """,
                (as_of,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, net_pl
                FROM trades
                WHERE net_pl IS NOT NULL
                ORDER BY trade_date ASC, id ASC
                """
            ).fetchall()
    out: Dict[int, float] = {}
    bal = starting
    for r in rows:
        bal += float(r["net_pl"] or 0.0)
        out[int(r["id"])] = float(bal)
    return out


def trades_page():
    d = request.args.get("d", "")
    active_day = d or today_iso()

    prev_day = prev_trading_day_iso(active_day)
    next_day = next_trading_day_iso(active_day)

    q = request.args.get("q", "")
    page = max(1, parse_int(request.args.get("page") or "1") or 1)
    per = parse_int(request.args.get("per") or "50") or 50
    per = max(25, min(200, per))

    # ✅ Convert sqlite3.Row -> dict so Jinja can use .get() and ['key']
    raw_trades = fetch_trades(d=d, q=q)
    trades = [dict(r) for r in raw_trades]
    review_map = fetch_trade_reviews_map([int(t["id"]) for t in trades if t.get("id") is not None])
    for t in trades:
        rv = review_map.get(int(t["id"]), {})
        t["setup_tag"] = rv.get("setup_tag", "")
        t["session_tag"] = rv.get("session_tag", "")
        t["checklist_score"] = rv.get("checklist_score", None)
        t["rule_break_tags"] = rv.get("rule_break_tags", "")
    derived_balances = _derived_balance_map(as_of=active_day)
    for t in trades:
        trade_id = t.get("id")
        if trade_id in derived_balances:
            t["balance"] = derived_balances[trade_id]
    total_rows = len(trades)
    page_count = max(1, (total_rows + per - 1) // per)
    if page > page_count:
        page = page_count
    row_start = (page - 1) * per
    row_end = row_start + per
    page_trades = trades[row_start:row_end]

    stats = trade_day_stats(trades)  # likely dict
    cons = calc_consistency(trades)  # dict-like expected
    guardrail = trade_lockout_state(active_day)
    sync_status = _load_last_sync_status() or {}
    data_trust = trades_data_trust(
        sync_status, guardrail_locked=bool(guardrail.get("locked")), active_day=active_day
    )

    week_total = week_total_net(d or None)
    overall_bal = latest_balance_overall(as_of=active_day)
    running_balance = overall_bal
    with db() as conn:
        starting_balance = float(get_setting_float("starting_balance", 50000.0))
        y_start = f"{active_day[:4]}-01-01"
        ytd_row = conn.execute(
            """
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE trade_date >= ? AND trade_date <= ?
            """,
            (y_start, active_day),
        ).fetchone()
        prior_eod_row = conn.execute(
            """
            SELECT COALESCE(SUM(net_pl), 0) AS net, COUNT(*) AS cnt
            FROM trades
            WHERE trade_date < ? AND net_pl IS NOT NULL
            """,
            (active_day,),
        ).fetchone()
        all_time_row = conn.execute(
            """
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            """
        ).fetchone()
    ytd_net = float(ytd_row["net"] or 0.0)
    all_time_net = float(all_time_row["net"] or 0.0)
    prior_eod_balance = (
        starting_balance + float(prior_eod_row["net"] or 0.0)
        if prior_eod_row and int(prior_eod_row["cnt"] or 0) > 0
        else None
    )
    day_net = float(
        (stats["total"] if isinstance(stats, dict) else getattr(stats, "total", 0.0)) or 0.0
    )
    win_rate = float(
        (stats["win_rate"] if isinstance(stats, dict) else getattr(stats, "win_rate", 0.0)) or 0.0
    )
    trades_count = len(trades)
    avg_net = (day_net / trades_count) if trades_count else 0.0
    if trades_count == 0:
        execution_msg = (
            "No trades logged for the current filter. Start with one clean, rules-based setup."
        )
    elif win_rate >= 60 and day_net >= 0:
        execution_msg = "Execution quality is stable today. Keep sizing disciplined and avoid late-session forcing."
    elif day_net < 0:
        execution_msg = "P/L is under pressure. Prioritize A+ entries and reduce pace until process quality improves."
    else:
        execution_msg = (
            "Mixed session so far. Focus on setup clarity and post-trade review accuracy."
        )

    if guardrail.get("locked"):
        risk_msg = "Guardrail is locked. New trades should pause until next session or risk controls are adjusted."
    else:
        risk_msg = (
            f"Guardrail active with day net at {money(guardrail.get('day_net') or 0)}. "
            "Current risk posture is tradable."
        )

    next_action_msg = (
        "Tag every trade with setup/session and complete missing review scores before day end."
        if trades_count
        else "Import statement or add first trade, then complete setup/session review tags."
    )
    is_day_view = bool(d)
    primary_net_label = (
        f"💰 Day Net ({d})" if is_day_view else "💰 Filtered Net (All Visible Trades)"
    )
    primary_net_sub = (
        "Net for the selected trading day"
        if is_day_view
        else "Net across the current filter (all dates when no date is set)"
    )
    secondary_total_label = "📅 Week Total" if is_day_view else "🏁 All-Time Net"
    secondary_total_value = week_total if is_day_view else all_time_net

    content = render_template(
        "trades/index.html",
        trades=trades,
        page_trades=page_trades,
        total_rows=total_rows,
        page=page,
        page_count=page_count,
        per=per,
        d=d,
        q=q,
        stats=stats,
        cons=cons,  # ✅ THIS was missing and caused your crash
        week_total=week_total,
        running_balance=running_balance,
        ytd_net=ytd_net,
        all_time_net=all_time_net,
        prior_eod_balance=prior_eod_balance,
        money=money,
        pct=pct,
        prev_day=prev_day,
        next_day=next_day,
        day_net=day_net,
        win_rate=win_rate,
        trades_count=trades_count,
        avg_net=avg_net,
        execution_msg=execution_msg,
        risk_msg=risk_msg,
        next_action_msg=next_action_msg,
        guardrail=guardrail,
        data_trust=data_trust,
        primary_net_label=primary_net_label,
        primary_net_sub=primary_net_sub,
        secondary_total_label=secondary_total_label,
        secondary_total_value=secondary_total_value,
    )

    return render_page(content, active="trades")


def get_trade(trade_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()


def _parse_ids_from_request() -> List[int]:
    """Parse a list of trade ids from JSON or form data."""
    ids: Any = None
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        ids = payload.get("ids")
    if ids is None:
        ids = request.form.getlist("ids") or request.form.get("ids")

    if isinstance(ids, str):
        raw = [x.strip() for x in ids.split(",") if x.strip()]
    elif isinstance(ids, list):
        raw = ids
    else:
        raw = []

    clean: List[int] = []
    for x in raw:
        try:
            clean.append(int(x))
        except Exception:
            continue

    seen = set()
    out: List[int] = []
    for i in clean:
        if i not in seen:
            out.append(i)
            seen.add(i)
    return out


def _trades_table_columns(conn: sqlite3.Connection) -> List[str]:
    """Return the current trades table columns (sqlite)."""
    return [r["name"] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]


def trades_duplicate(trade_id: int):
    """Clone a trade row (useful for scaling in/out or repeating a similar fill)."""
    src = get_trade(trade_id)
    if not src:
        abort(404)

    net_pl = float(src["net_pl"] or 0.0)
    new_balance = (latest_balance_overall() or 50000.0) + net_pl

    with db() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent,
                stop_pct, target_pct, stop_price, take_profit,
                risk, comm, gross_pl, net_pl, result_pct, balance,
                raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                src["trade_date"],
                src["entry_time"] or "",
                src["exit_time"] or "",
                src["ticker"] or "",
                src["opt_type"] or "",
                src["strike"],
                src["entry_price"],
                src["exit_price"],
                src["contracts"],
                src["total_spent"],
                src["stop_pct"],
                src["target_pct"],
                src["stop_price"],
                src["take_profit"],
                src["risk"],
                src["comm"],
                src["gross_pl"],
                src["net_pl"],
                src["result_pct"],
                new_balance,
                f"DUPLICATE OF #{trade_id}",
                now_iso(),
            ),
        )

    d = request.args.get("d", "") or (src["trade_date"] or "")
    q = request.args.get("q", "")
    return redirect(url_for("trades_page", d=d, q=q))


def trades_delete(trade_id: int):
    with db() as conn:
        conn.execute("DELETE FROM trades WHERE id = ?", (trade_id,))
    d = request.args.get("d", "")
    q = request.args.get("q", "")
    return redirect(url_for("trades_page", d=d, q=q))


def trades_delete_many():
    ids = _parse_ids_from_request()
    if not ids:
        if request.is_json:
            return jsonify({"ok": True, "deleted": 0})
        flash("No trades selected.", "warning")
        return redirect(
            url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", ""))
        )

    placeholders = ",".join(["?"] * len(ids))
    with db() as conn:
        cur = conn.execute(f"DELETE FROM trades WHERE id IN ({placeholders})", ids)
        deleted = cur.rowcount if cur.rowcount is not None else 0

    if request.is_json:
        return jsonify({"ok": True, "deleted": int(deleted)})
    flash(f"Deleted {deleted} trade(s).", "success")
    return redirect(
        url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", ""))
    )


def trades_copy_many():
    ids = _parse_ids_from_request()
    target_date = None
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        target_date = payload.get("target_date")
    if not target_date:
        target_date = request.form.get("target_date")

    if not ids:
        if request.is_json:
            return jsonify({"ok": True, "copied": 0})
        flash("No trades selected.", "warning")
        return redirect(
            url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", ""))
        )

    try:
        datetime.strptime(str(target_date), "%Y-%m-%d")
    except Exception:
        if request.is_json:
            return jsonify({"ok": False, "error": "Invalid target_date. Use YYYY-MM-DD."}), 400
        flash("Invalid target date (use YYYY-MM-DD).", "danger")
        return redirect(
            url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", ""))
        )

    with db() as conn:
        cols = _trades_table_columns(conn)
        insert_cols = [c for c in cols if c != "id"]
        select_cols = ",".join([f"{c}" for c in insert_cols])
        placeholders = ",".join(["?"] * len(ids))
        rows = conn.execute(
            f"SELECT {select_cols} FROM trades WHERE id IN ({placeholders}) ORDER BY trade_date, id",
            ids,
        ).fetchall()

        copied = 0
        if rows:
            now_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            qmarks = ",".join(["?"] * len(insert_cols))
            insert_sql = f"INSERT INTO trades ({','.join(insert_cols)}) VALUES ({qmarks})"
            for r in rows:
                data = dict(r)
                data["trade_date"] = str(target_date)
                if "created_at" in data:
                    data["created_at"] = now_iso
                if "balance" in data:
                    data["balance"] = None
                values = [data.get(c) for c in insert_cols]
                conn.execute(insert_sql, values)
                copied += 1

    if request.is_json:
        return jsonify({"ok": True, "copied": copied})
    flash(f"Copied {copied} trade(s) to {target_date}.", "success")
    return redirect(url_for("trades_page", d=str(target_date), q=request.args.get("q", "")))


def trades_edit(trade_id: int):
    row = get_trade(trade_id)
    if not row:
        abort(404)

    d = request.args.get("d", "")
    q = request.args.get("q", "")

    if request.method == "POST":
        f = request.form

        trade_date = (f.get("trade_date") or today_iso()).strip()
        entry_time = (f.get("entry_time") or "").strip()
        exit_time = (f.get("exit_time") or "").strip()

        ticker = (f.get("ticker") or "").strip().upper()
        opt_type = normalize_opt_type(f.get("opt_type") or "")
        strike = parse_float(f.get("strike") or "")

        contracts = parse_int(f.get("contracts") or "") or 0
        entry_price = parse_float(f.get("entry_price") or "")
        exit_price = parse_float(f.get("exit_price") or "")
        comm = parse_float(f.get("comm") or "") or 0.0

        if (
            not ticker
            or opt_type not in ("CALL", "PUT")
            or contracts <= 0
            or entry_price is None
            or exit_price is None
        ):
            return render_page(
                simple_msg("Missing required fields (ticker/type/contracts/entry/exit)."),
                active="trades",
            )

        gross_pl = (exit_price - entry_price) * 100.0 * contracts
        net_pl = gross_pl - comm
        total_spent = entry_price * 100.0 * contracts
        result_pct = (net_pl / total_spent * 100.0) if total_spent > 0 else None

        with db() as conn:
            conn.execute(
                """
                UPDATE trades
                SET trade_date=?, entry_time=?, exit_time=?, ticker=?, opt_type=?, strike=?,
                    entry_price=?, exit_price=?, contracts=?, comm=?,
                    total_spent=?, gross_pl=?, net_pl=?, result_pct=?
                WHERE id=?
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
                    comm,
                    total_spent,
                    gross_pl,
                    net_pl,
                    result_pct,
                    trade_id,
                ),
            )

        repo.recompute_balances()
        return redirect(
            url_for("trades_page", d=d, q=q) if (d or q) else url_for("trades_page", d=trade_date)
        )

    t = dict(row)
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">✏️ Edit Trade #{{ t.id }}</div>
          <div class="hr"></div>

          <form method="post" action="/trades/edit/{{ t.id }}?d={{ d }}&q={{ q }}">
            <div class="row">
              <div><label>📆 Date</label><input type="date" name="trade_date" value="{{ t.trade_date }}"/></div>
              <div><label>⏱️ Entry Time</label><input name="entry_time" value="{{ t.entry_time or '' }}"/></div>
              <div><label>⏱️ Exit Time</label><input name="exit_time" value="{{ t.exit_time or '' }}"/></div>
            </div>

            <div class="row stack10">
              <div><label>🏷️ Ticker</label><input name="ticker" value="{{ t.ticker or '' }}"/></div>
              <div>
                <label>📌 Type</label>
                <select name="opt_type">
                  <option value="CALL" {% if (t.opt_type or '')=='CALL' %}selected{% endif %}>CALL</option>
                  <option value="PUT"  {% if (t.opt_type or '')=='PUT' %}selected{% endif %}>PUT</option>
                </select>
              </div>
              <div><label>❌ Strike</label><input name="strike" inputmode="decimal" value="{{ '' if t.strike is none else t.strike }}"/></div>
            </div>

            <div class="row stack10">
              <div><label>🧾 Contracts</label><input name="contracts" inputmode="numeric" value="{{ t.contracts or 1 }}"/></div>
              <div><label>💰 Entry</label><input name="entry_price" inputmode="decimal" value="{{ '' if t.entry_price is none else t.entry_price }}"/></div>
              <div><label>💰 Exit</label><input name="exit_price" inputmode="decimal" value="{{ '' if t.exit_price is none else t.exit_price }}"/></div>
            </div>

            <div class="row stack10">
              <div><label>💵 Fees (total)</label><input name="comm" inputmode="decimal" value="{{ t.comm or 0.70 }}"/></div>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save</button>
              <a class="btn" href="/trades?d={{ d }}&q={{ q }}">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        t=t,
        d=d,
        q=q,
    )
    return render_page(content, active="trades")


def trades_review(trade_id: int):
    row = get_trade(trade_id)
    if not row:
        abort(404)

    d = request.args.get("d", "")
    q = request.args.get("q", "")
    rv = repo.get_trade_review(trade_id) or {}

    if request.method == "POST":
        f = request.form
        setup_tag = (f.get("setup_tag") or "").strip()
        session_tag = (f.get("session_tag") or "").strip()
        score_raw = (f.get("checklist_score") or "").strip()
        checklist_score = parse_int(score_raw) if score_raw else None
        rule_break_tags = (f.get("rule_break_tags") or "").strip()
        rule_break_tags = _merge_auto_rule_break_tags(
            entry_price=parse_float(
                str(row["entry_price"]) if row["entry_price"] is not None else ""
            ),
            exit_price=parse_float(str(row["exit_price"]) if row["exit_price"] is not None else ""),
            existing_tags=rule_break_tags,
        )
        review_note = (f.get("review_note") or "").strip()
        repo.upsert_trade_review(
            trade_id=trade_id,
            setup_tag=setup_tag,
            session_tag=session_tag,
            checklist_score=checklist_score,
            rule_break_tags=rule_break_tags,
            review_note=review_note,
        )
        return redirect(url_for("trades_page", d=d, q=q) if (d or q) else url_for("trades_page"))

    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🧠 Trade Review #{{ t.id }}</div>
          <div class="tiny stack8">{{ t.trade_date }} · {{ t.ticker }} {{ t.opt_type }}</div>
          <div class="hr"></div>
          <form method="post" action="/trades/review/{{ t.id }}?d={{ d }}&q={{ q }}">
            <div class="row">
              <div><label>Setup Tag</label><input name="setup_tag" value="{{ rv.get('setup_tag','') }}" placeholder="FVG, ORB, Fade, Breakout"></div>
              <div>
                <label>Session Tag</label>
                <select name="session_tag">
                  {% set s = rv.get('session_tag','') %}
                  <option value="" {% if s=='' %}selected{% endif %}>—</option>
                  <option value="Open" {% if s=='Open' %}selected{% endif %}>Open</option>
                  <option value="Midday" {% if s=='Midday' %}selected{% endif %}>Midday</option>
                  <option value="Power Hour" {% if s=='Power Hour' %}selected{% endif %}>Power Hour</option>
                  <option value="After Hours" {% if s=='After Hours' %}selected{% endif %}>After Hours</option>
                </select>
              </div>
              <div><label>Checklist Score (0-100)</label><input name="checklist_score" inputmode="numeric" value="{{ '' if rv.get('checklist_score') is none else rv.get('checklist_score') }}"></div>
            </div>
            <div class="row stack10">
              <div>
                <label>Rule-Break Tags (comma separated)</label>
                <input name="rule_break_tags" value="{{ rv.get('rule_break_tags','') }}" placeholder="oversized, late entry, no stop, revenge trade">
              </div>
            </div>
            <div class="stack10">
              <label>Review Note</label>
              <textarea name="review_note" placeholder="What to repeat, what to remove next session">{{ rv.get('review_note','') }}</textarea>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save Review</button>
              <a class="btn" href="/trades?d={{ d }}&q={{ q }}">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        t=dict(row),
        rv=rv,
        d=d,
        q=q,
    )
    return render_page(content, active="trades")


def trades_risk_controls():
    if request.method == "POST":
        daily_max_loss = parse_float(request.form.get("daily_max_loss", "")) or 0.0
        enforce_lockout = 1 if request.form.get("enforce_lockout") == "1" else 0
        repo.save_risk_controls(daily_max_loss, enforce_lockout)
        return redirect(url_for("trades_risk_controls"))

    rc = repo.get_risk_controls()
    state = trade_lockout_state(today_iso())
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🛡️ Risk Controls</div>
          <div class="tiny stack8">
            Today's net: {{ money(state.day_net) }} · Max loss: {{ money(state.daily_max_loss) }} ·
            Status: {% if state.locked %}<b class="statusLock">LOCKED</b>{% else %}<b class="statusActive">ACTIVE</b>{% endif %}
          </div>
          <div class="hr"></div>
          <form method="post">
            <div class="row">
              <div><label>Daily Max Loss ($)</label><input name="daily_max_loss" inputmode="decimal" value="{{ rc.daily_max_loss }}"></div>
              <div>
                <label>Enforce Lockout</label>
                <select name="enforce_lockout">
                  <option value="0" {% if not rc.enforce_lockout %}selected{% endif %}>Off</option>
                  <option value="1" {% if rc.enforce_lockout %}selected{% endif %}>On</option>
                </select>
              </div>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">Save Controls</button>
              <a class="btn" href="/trades">Back to Trades</a>
            </div>
          </form>
        </div></div>
        """,
        rc=rc,
        state=state,
        money=money,
    )
    return render_page(content, active="trades")


def trades_clear():
    repo.clear_trades()
    return redirect(url_for("trades_page"))


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

        reconciliation_html = ""
        if fmt == "broker":
            batch_id = _new_import_batch_id("paste")
            inserted, errors, report = importing.insert_trades_from_broker_paste_with_report(
                text,
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

        content = render_template_string(
            """
            <div class="card"><div class="toolbar">
              <div class="pill">📋 Paste Trades</div>
              <div class="stack10">Inserted <b>{{ inserted }}</b> trade{{ '' if inserted==1 else 's' }} ✅</div>
              {% if errors %}
                <div class="hr"></div>
                <div class="tiny metaRed">
                  {% for e in errors %}• {{ e }}<br/>{% endfor %}
                </div>
              {% endif %}
              {{ reconciliation_html|safe }}
              <div class="hr"></div>
              <div class="rightActions">
                <a class="btn primary" href="/trades">Trades 📅</a>
                <a class="btn" href="/dashboard">Calendar 📊</a>
                <a class="btn" href="/calculator">Calculator 🧮</a>
                <a class="btn" href="/trades/paste">Paste More 🔁</a>
              </div>
            </div></div>
            """,
            inserted=inserted,
            errors=errors,
            reconciliation_html=reconciliation_html,
        )
        return render_page(content, active="trades")

    example = "1/29\t9:35 AM\t9:37 AM\tSPX\tPUT\t6940\t$6.20\t$7.30\t3\t$1,860.00\t20\t30\t$4.96\t$8.06\t$374.10\t$2.10\t$330.00\t$327.90\t17.74%\t$50,924.40"
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">📋 Paste Trades (tabs please ✅)</div>
          <div class="tiny stack10 line15">
            Pro tip: copy straight from your sheet/log, keep the tabs.
            <div class="hr"></div>
            Example:<br/><code class="preWrapMuted">{{ example }}</code>
          </div>
          <div class="hr"></div>
          <form method="post">
            <div class="stack12">
              <label>📎 Paste here</label>
              <textarea name="text" placeholder="Paste your trade rows here…"></textarea>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">🚀 Import</button>
              <a class="btn" href="/trades">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        example=example,
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
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">📘 Playbook Engine</div>
          <div class="tiny stack10 line16">Enforce pre-trade rules from your edge stats (size caps, blocked windows, quality floor).</div>
          <div class="tiny stack8 line16">Why this matters: rules prevent emotional drift and preserve consistency under pressure.</div>
          <div class="tiny stack8 line16">Next best action: keep this enabled, set a realistic checklist floor, then expand advanced controls only if needed.</div>
          <div class="hr"></div>
          <form method="post">
            <div class="row">
              <div><label><input type="checkbox" name="enabled" value="1" {% if cfg.enabled %}checked{% endif %}/> Enable playbook enforcement</label></div>
            </div>
            <div class="row">
              <div>
                <label>Minimum Checklist Score</label>
                <input type="number" min="0" max="100" name="min_checklist_score" value="{{ cfg.min_checklist_score }}" />
              </div>
              <div>
                <label>Max Size (% of balance by total spend)</label>
                <input type="number" min="1" max="100" step="0.1" name="max_size_pct" value="{{ cfg.max_size_pct }}" />
              </div>
            </div>
            <details class="syncDetails stack10">
              <summary>Advanced Rule Controls</summary>
              <div class="hr"></div>
              <div class="row">
                <div><label><input type="checkbox" name="require_positive_setup_expectancy" value="1" {% if cfg.require_positive_setup_expectancy %}checked{% endif %}/> Require positive setup expectancy</label></div>
                <div><label><input type="checkbox" name="require_critical_checklist" value="1" {% if cfg.require_critical_checklist %}checked{% endif %}/> Require critical checklist items</label></div>
              </div>
              <div class="row">
                <div class="fieldGrow2">
                  <label>Blocked Time Blocks (comma-separated)</label>
                  <input name="blocked_time_blocks" value="{{ cfg.blocked_time_blocks|join(', ') }}" placeholder="09:30-10:00, 15:00-16:00" />
                </div>
              </div>
              <div class="row">
                <div class="fieldGrow2">
                  <label>Critical Checklist Items (comma-separated)</label>
                  <input name="critical_items" value="{{ cfg.critical_items|join(', ') }}" placeholder="Bias Confirmed, Risk Defined, Stop Planned" />
                </div>
              </div>
            </details>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">Save Playbook</button>
              <a class="btn" href="/trades">Back Trades</a>
            </div>
          </form>
        </div></div>
        <div class="card"><div class="toolbar">
          <div class="pill">📈 Setup Expectancy Snapshot</div>
          <div class="tableWrap"><table class="tableDense">
            <thead><tr><th>Setup</th><th>Trades</th><th>Win Rate</th><th>Expectancy</th></tr></thead>
            <tbody>
            {% for r in setup_rows[:20] %}
              <tr>
                <td>{{ r.k or 'Unlabeled' }}</td>
                <td>{{ r.count }}</td>
                <td>{{ '%.1f'|format(r.win_rate) }}%</td>
                <td>{{ money(r.expectancy) }}</td>
              </tr>
            {% else %}
              <tr><td colspan="4">No setup data yet.</td></tr>
            {% endfor %}
            </tbody>
          </table></div>
        </div></div>
        """,
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
        setup_tag = (f.get("setup_tag") or "").strip()
        session_tag = (f.get("session_tag") or "").strip()
        checklist_score_raw = (f.get("checklist_score") or "").strip()
        checklist_score = parse_int(checklist_score_raw) if checklist_score_raw else None
        critical_items_checked = [
            str(x).strip() for x in f.getlist("critical_item") if str(x).strip()
        ]

        if (
            not ticker
            or opt_type not in ("CALL", "PUT")
            or contracts <= 0
            or entry_price is None
            or exit_price is None
        ):
            return render_page(
                simple_msg("Missing required fields (ticker/type/contracts/entry/exit)."),
                active="trades",
            )

        gross_pl = (exit_price - entry_price) * 100.0 * contracts
        net_pl = gross_pl - comm
        total_spent = entry_price * 100.0 * contracts
        result_pct = (net_pl / total_spent * 100.0) if total_spent > 0 else None
        balance = (latest_balance_overall() or 50000.0) + net_pl
        violations = _playbook_violations(
            cfg=pb_cfg,
            setup_tag=setup_tag,
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

        with db() as conn:
            conn.execute(
                """
                INSERT INTO trades (
                    trade_date, entry_time, exit_time, ticker, opt_type, strike,
                    entry_price, exit_price, contracts, total_spent,
                    comm, gross_pl, net_pl, result_pct, balance,
                    raw_line, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                ),
            )
            trade_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        if setup_tag or session_tag or checklist_score is not None:
            auto_tags = _merge_auto_rule_break_tags(
                entry_price=entry_price,
                exit_price=exit_price,
                existing_tags="",
            )
            repo.upsert_trade_review(
                trade_id=trade_id,
                setup_tag=setup_tag,
                session_tag=session_tag,
                checklist_score=checklist_score,
                rule_break_tags=auto_tags,
                review_note="",
            )
        return redirect(url_for("trades_page", d=trade_date))

    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">➕ Manual Trade Entry</div>
          <div class="hr"></div>
          <form method="post">
            <div class="row">
              <div><label>📆 Date</label><input type="date" name="trade_date" value="{{ today }}"/></div>
              <div><label>⏱️ Entry Time</label><input name="entry_time" placeholder="9:45 AM"/></div>
              <div><label>⏱️ Exit Time</label><input name="exit_time" placeholder="10:05 AM"/></div>
            </div>
            <div class="row stack10">
              <div><label>🏷️ Ticker</label><input name="ticker" placeholder="SPX"/></div>
              <div>
                <label>📌 Type</label>
                <select name="opt_type"><option>CALL</option><option>PUT</option></select>
              </div>
              <div><label>❌ Strike</label><input name="strike" inputmode="decimal" placeholder="6940"/></div>
            </div>
            <div class="row stack10">
              <div><label>🧾 Contracts</label><input name="contracts" inputmode="numeric" value="1"/></div>
              <div><label>💰 Entry</label><input name="entry_price" inputmode="decimal" placeholder="6.20"/></div>
              <div><label>💰 Exit</label><input name="exit_price" inputmode="decimal" placeholder="7.30"/></div>
            </div>
            <div class="row stack10">
              <div><label>🏷️ Setup Tag</label><input name="setup_tag" placeholder="Fitz 2-2 REV"/></div>
              <div><label>🕒 Session Tag</label><input name="session_tag" placeholder="AM / Midday / PM"/></div>
              <div><label>✅ Checklist Score</label><input name="checklist_score" inputmode="numeric" placeholder="0-100"/></div>
            </div>
            <div class="row stack10">
              <div class="fieldGrow2">
                <label>🧱 Critical Checklist Gate</label>
                <div class="tiny stack8 line16">
                  {% for item in critical_items %}
                    <label style="display:inline-flex; gap:8px; margin-right:14px; align-items:center;">
                      <input type="checkbox" name="critical_item" value="{{ item }}"> {{ item }}
                    </label>
                  {% endfor %}
                </div>
              </div>
            </div>
            <div class="row stack10">
              <div><label>💵 Commission/Fees (total)</label><input name="comm" inputmode="decimal" value="0.70"/></div>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save Trade</button>
              <a class="btn" href="/trades">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        today=today_iso(),
        critical_items=pb_cfg.get("critical_items")
        or ["Bias Confirmed", "Risk Defined", "Stop Planned"],
    )
    return render_page(content, active="trades")


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
        batch_id = _new_import_batch_id("brokerpaste")
        inserted, errors, report = importing.insert_trades_from_broker_paste_with_report(
            text,
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
        content = render_template_string(
            """
            <div class="card"><div class="toolbar">
              <div class="pill">🏦 Broker Paste Import</div>
              <div class="stack10">Inserted <b>{{ inserted }}</b> round-trip trade{{ '' if inserted==1 else 's' }} ✅</div>
              {% if errors %}
                <div class="hr"></div><div class="tiny metaRed">{% for e in errors %}• {{ e }}<br/>{% endfor %}</div>
              {% endif %}
              {{ reconciliation_html|safe }}
              <div class="hr"></div>
              <div class="rightActions">
                <a class="btn primary" href="/trades">Trades 📅</a>
                <a class="btn" href="/dashboard">Calendar 📊</a>
                <a class="btn" href="/trades/paste/broker">Paste More 🔁</a>
              </div>
            </div></div>
            """,
            inserted=inserted,
            errors=errors,
            reconciliation_html=reconciliation_html,
        )
        return render_page(content, active="trades")

    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🏦 Paste Broker Fills (BUY/SELL legs)</div>
          <div class="tiny stack10 line15">
            Paste the raw fills. This importer pairs BUY+SELL into one completed trade (FIFO). ✅
          </div>
          <div class="hr"></div>
          <form method="post">
            <div class="stack12">
              <label>📎 Paste here</label>
              <textarea name="text" placeholder="SPX JAN/30/26 6935 PUT | 1/30/26, 10:30 AM | SELL | 2 | 18.90 | 0.70"></textarea>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">🚀 Convert + Import</button>
              <a class="btn" href="/trades">← Back</a>
            </div>
          </form>
        </div></div>
        """
    )
    return render_page(content, active="trades")


def trades_upload_pdf():
    workspace = (request.args.get("ws") or "live").strip().lower()
    if workspace not in {"upload", "live", "reconcile"}:
        workspace = "live"
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
        f = request.files.get("pdf")
        mode = (request.form.get("mode") or "broker").strip()  # broker | balance

        if not f or not f.filename:
            return render_page(simple_msg("Please upload a file."), active="trades")

        filename = secure_filename(f.filename)
        _, ext = os.path.splitext(filename.lower())

        if ext not in {".pdf", ".html", ".htm"}:
            return render_page(simple_msg("Please upload a .pdf or .html file."), active="trades")

        path = os.path.join(UPLOAD_DIR, filename)
        f.save(path)

        # ✅ HTML path (no OCR)
        if ext in (".html", ".htm"):
            return _handle_statement_html_import(
                path, mode=mode, source_label="STATEMENT HTML UPLOAD"
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
                    render_template_string(
                        """
                        <div class="card"><div class="toolbar">
                          <div class="pill">⛔ OCR rows not parseable</div>
                          <div class="hr"></div>
                          <div class="tiny metaBlue line16">
                            {% for m in warns %}• {{ m }}<br>{% endfor %}
                          </div>
                          <div class="hr"></div>
                          <div class="tiny">Stitched rows (first 30):</div>
                          <pre class="preWrapMuted">{{ dump }}</pre>
                          <div class="hr"></div>
                          <a class="btn" href="/trades/upload/statement">Back</a>
                        <a class="btn" href="/trades/upload/statement">Upload Another</a>

                        </div></div>
                        """,
                        warns=ocr_warns,
                        dump="\n".join(stitched[:30]),
                    ),
                    active="trades",
                )

            inserted, errors, report = importing.insert_trades_from_broker_paste_with_report(
                paste_text,
                commit=True,
                import_batch_id=_new_import_batch_id("pdfocr"),
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
                render_template_string(
                    """
                    <div class="card"><div class="toolbar">
                      <div class="pill">📄 PDF → OCR → Trades ✅</div>
                      <div class="stack10">Inserted <b>{{ inserted }}</b> trade{{ '' if inserted==1 else 's' }}.</div>
                      {% if msgs %}
                        <div class="hr"></div>
                        <div class="tiny metaBlue line16">
                          {% for m in msgs %}• {{ m }}<br>{% endfor %}
                        </div>
                      {% endif %}
                      {{ reconciliation_html|safe }}
                      <div class="hr"></div>
                      <a class="btn primary" href="/trades">Trades 📅</a>
                     <a class="btn" href="/trades/upload/statement">Upload Another</a>
                    </div></div>
                    """,
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
                render_template_string(
                    """<div class="card"><div class="toolbar">
                       <div class="pill">⛔ Could not find ending balance</div>
                       <div class="hr"></div>
                       <div class="tiny">Dump (first 1200 chars):</div>
                       <pre class="preWrapMuted">{{ dump }}</pre>
                       <div class="hr"></div>
                       <a class="btn" href="/trades/upload/statement">Back</a>
                       </div></div>""",
                    dump=(text or "")[:1200],
                ),
                active="trades",
            )

        importing.insert_balance_snapshot(today_iso(), bal, raw_line="STATEMENT PDF UPLOAD")
        return redirect(url_for("trades_page"))

    # GET
    broker_cfg = _load_broker_sync_config()
    auto_sync_cfg = _load_auto_sync_config()
    default_day = today_iso()
    sync_status = _load_last_sync_status()
    sync_history = _load_sync_history()
    sync_reliability = _sync_reliability_summary(sync_history, days=30)
    import_history = _load_import_history()
    reconcile_summary = _reconcile_summary(import_history, days=30)
    sync_job_id = (request.args.get("job") or "").strip()
    sync_job = _get_bg_job(sync_job_id) if sync_job_id else {}
    content = render_template(
        "trades/upload_statement.html",
        workspace=workspace,
        broker_cfg=broker_cfg,
        auto_sync_cfg=auto_sync_cfg,
        auto_sync_password_fallback=AUTO_SYNC_PASSWORD_FALLBACK,
        default_day=default_day,
        sync_status=sync_status,
        sync_reliability=sync_reliability,
        sync_job=sync_job,
        reconcile_summary=reconcile_summary,
        import_history=list(reversed(import_history[-40:])),
        sync_stage_help=SYNC_STAGE_HELP,
        money=money,
    )
    return render_page(content, active="trades")


def _run_live_sync_once(
    *,
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
) -> Dict[str, Any]:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    debug_dir = (
        os.path.join(BROKER_DEBUG_DIR, f"live_{from_date}_{to_date}_{stamp}")
        if debug_capture
        else None
    )
    artifacts_rel: List[str] = []
    result: Dict[str, Any] = {"ok": False, "warns": [], "artifacts_rel": [], "message": ""}
    try:
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
        artifacts_rel = [_debug_relative(p) for p in artifacts_abs]
        result["warns"] = warns
        result["sync_meta"] = sync_meta
    except Exception as e:
        raw_error = str(e)
        failed_stage = _parse_sync_stage(raw_error)
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

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    filename = f"vanquish_statement_live_{from_date}_{to_date}_{stamp}.html"
    path = os.path.join(UPLOAD_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_text)
    artifacts_rel = artifacts_rel + [_debug_relative(path)]

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
        paste_text, balance_val, parse_warns = importing.parse_statement_html_to_broker_paste(path)
        warns_all = (result.get("warns") or []) + (parse_warns or [])
        date_range_fallback = any(
            "Could not set custom From/To" in str(w) for w in (result.get("warns") or [])
        )
        if date_range_fallback and balance_val is not None:
            # When broker UI keeps visible defaults, captured statement can span a wider range
            # than requested dates. Reconcile using ending balance would be misleading.
            balance_val = None
            warns_all.append(
                "Date-range fallback detected; skipped ending-balance reconcile for this run."
            )
        if not paste_text:
            result.update(
                {
                    "ok": False,
                    "stage": "capture_statement_html",
                    "message": "Parsed statement HTML but found no trade rows.",
                    "warns": warns_all,
                    "artifacts_rel": artifacts_rel,
                    "statement_path": path,
                }
            )
            return result
        _, _, pre_report = importing.insert_trades_from_broker_paste_with_report(
            paste_text,
            ending_balance=balance_val,
            commit=False,
            import_batch_id=batch_id,
        )
        if RECONCILE_GATE_ENABLED:
            if progress_cb:
                progress_cb("reconcile_gate", "Running reconcile guardrails.")
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
        inserted, errors, report = importing.insert_trades_from_broker_paste_with_report(
            paste_text,
            ending_balance=balance_val,
            commit=True,
            import_batch_id=batch_id,
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
            }
        )
        return result

    # balance mode
    batch_id = _new_import_batch_id("livebal")
    if progress_cb:
        progress_cb("parse_statement_html", "Parsing statement balance.")
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
    importing.insert_balance_snapshot(today_iso(), balance_val, raw_line=source_label)
    result.update(
        {
            "ok": True,
            "stage": "import_complete",
            "message": f"{source_label}: imported ending balance snapshot {money(balance_val)}.",
            "warns": warns_all,
            "artifacts_rel": artifacts_rel,
            "statement_path": path,
            "batch_id": batch_id,
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


def _start_sync_job(
    *,
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

    def runner() -> None:
        started = time.time()

        def progress(stage: str, message: str) -> None:
            stage_message = message or _sync_stage_label(stage)
            _update_bg_job(job["id"], status="running", stage=stage, message=stage_message)
            _save_last_sync_status(
                {
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
                progress("start", "Sync job started.")
                run = _run_live_sync_once(
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
                )
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
                        "status": status,
                        "stage": stage,
                        "message": run.get("message") or "",
                        "stage_help": SYNC_STAGE_HELP.get(stage, ""),
                        "requested": requested,
                        "sync_meta": run.get("sync_meta", {}),
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
                )
        except Exception as e:  # pragma: no cover
            duration_sec = round(max(0.0, time.time() - started), 2)
            fail_message = f"Background sync worker error: {e}"
            _save_last_sync_status(
                {
                    "status": "failed",
                    "stage": "auto_worker",
                    "message": fail_message,
                    "updated_at": now_iso(),
                }
            )
            _update_bg_job(
                job["id"],
                status="failed",
                stage="auto_worker",
                message=fail_message,
                duration_sec=duration_sec,
                summary={"message": fail_message, "warn_count": 0, "error_count": 1},
            )

    threading.Thread(target=runner, daemon=True, name=f"sync-job-{job['id'][:8]}").start()
    return job


def trades_sync_live():
    if request.method != "POST":
        return redirect(url_for("trades_upload_pdf"))

    mode = (request.form.get("mode") or "broker").strip()
    guardrail = trade_lockout_state(today_iso())
    if guardrail["locked"] and mode == "broker":
        return render_page(
            simple_msg(
                f"Daily max-loss guardrail is active for {guardrail['day']}. "
                f"Day net {money(guardrail['day_net'])} reached limit {money(guardrail['daily_max_loss'])}."
            ),
            active="trades",
        )

    cfg = _load_broker_sync_config()
    username = (request.form.get("username") or "").strip()
    password = (request.form.get("password") or "").strip()
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

    if not username or not password:
        return render_page(
            simple_msg("Username and password are required for live login sync."),
            active="trades",
        )
    if not base_url or not account:
        return render_page(
            simple_msg("Base origin and account are required for live login sync."),
            active="trades",
        )

    from_date = _normalize_iso_date(request.form.get("from_date") or "", today_iso())
    to_date = _normalize_iso_date(request.form.get("to_date") or "", today_iso())
    if from_date > to_date:
        from_date, to_date = to_date, from_date

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

    if remember_connection:
        cfg.update(
            {
                "base_url": base_url,
                "wl": wl,
                "account": account,
                "time_zone": time_zone,
                "date_locale": date_locale,
                "report_locale": report_locale,
            }
        )
        _save_broker_sync_config(cfg)
    job = _start_sync_job(
        title="Live Sync",
        source_label="LIVE LOGIN HTML",
        record_source="LIVE LOGIN HTML",
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
        requested=requested,
    )
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
                fd = os.open(BROKER_AUTO_SYNC_LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
            except FileExistsError:
                time.sleep(20)
                continue
            try:
                with app.app_context():
                    started = time.time()
                    run = _run_live_sync_once(
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
                    os.unlink(BROKER_AUTO_SYNC_LOCK_PATH)
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


def _auto_backup_worker(app) -> None:
    while True:
        try:
            cfg = _load_auto_backup_config()
            if not cfg.get("enabled"):
                time.sleep(30)
                continue
            now_local = datetime.now(ZoneInfo("America/New_York"))
            if (not cfg.get("run_weekends")) and now_local.weekday() >= 5:
                time.sleep(30)
                continue
            times = [str(x).strip() for x in (cfg.get("run_times_et") or []) if str(x).strip()]
            if not times:
                times = ["16:30"]
            now_slot = now_local.strftime("%H:%M")
            if now_slot not in times:
                time.sleep(30)
                continue
            slot_key = f"{now_local.date().isoformat()}@{now_slot}"
            if str(cfg.get("last_run_slot_key") or "") == slot_key:
                time.sleep(35)
                continue
            try:
                fd = os.open(AUTO_BACKUP_LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
            except FileExistsError:
                time.sleep(20)
                continue
            try:
                with app.app_context():
                    _run_backup_once(reason="scheduled_auto", actor="auto-backup-worker")
                    cfg = _load_auto_backup_config()
                    cfg["last_run_slot_key"] = slot_key
                    _save_auto_backup_config(cfg)
            finally:
                try:
                    os.unlink(AUTO_BACKUP_LOCK_PATH)
                except OSError:
                    pass
            time.sleep(20)
        except Exception:
            time.sleep(45)


def trades_sync_debug_file(name: str):
    try:
        path = _debug_safe_path(name)
    except ValueError:
        abort(400)
    if not os.path.isfile(path):
        abort(404)
    return send_file(path, as_attachment=False)


def _entry_minutes(raw: str) -> Optional[int]:
    value = (raw or "").strip()
    if not value:
        return None
    for fmt in ("%I:%M %p", "%H:%M"):
        try:
            dt = datetime.strptime(value, fmt)
            return int(dt.hour) * 60 + int(dt.minute)
        except ValueError:
            continue
    return None


def _scan_anomaly_watch() -> None:
    rows = analytics_repo.fetch_analytics_rows()
    if not rows:
        return
    rows = sorted(rows, key=lambda r: int(r.get("id") or 0))
    recent = rows[-48:]

    # Size spike: recent average spend is meaningfully above prior baseline.
    spends_recent = [
        float(r.get("total_spent") or 0.0)
        for r in recent[-6:]
        if float(r.get("total_spent") or 0.0) > 0
    ]
    spends_base = [
        float(r.get("total_spent") or 0.0)
        for r in recent[:-6]
        if float(r.get("total_spent") or 0.0) > 0
    ]
    if len(spends_recent) >= 4 and len(spends_base) >= 8:
        avg_recent = sum(spends_recent) / len(spends_recent)
        avg_base = sum(spends_base) / len(spends_base)
        if avg_base > 0 and avg_recent >= (avg_base * 1.7):
            _emit_notification(
                "anomaly_size_spike",
                "Anomaly Watch: size spike",
                f"Recent avg size {money(avg_recent)} is {avg_recent/avg_base:.2f}x baseline {money(avg_base)}.",
                {"avg_recent": round(avg_recent, 2), "avg_baseline": round(avg_base, 2)},
            )

    # Revenge pattern: loss followed by larger quick re-entry on same day.
    revenge_hits = 0
    for prev, curr in zip(recent[-18:-1], recent[-17:]):
        if str(prev.get("trade_date") or "") != str(curr.get("trade_date") or ""):
            continue
        if float(prev.get("net_pl") or 0.0) >= 0:
            continue
        prev_spend = float(prev.get("total_spent") or 0.0)
        curr_spend = float(curr.get("total_spent") or 0.0)
        if prev_spend <= 0 or curr_spend < (prev_spend * 1.3):
            continue
        prev_m = _entry_minutes(str(prev.get("entry_time") or ""))
        curr_m = _entry_minutes(str(curr.get("entry_time") or ""))
        if prev_m is None or curr_m is None:
            continue
        if 0 <= (curr_m - prev_m) <= 35:
            revenge_hits += 1
    if revenge_hits >= 2:
        _emit_notification(
            "anomaly_revenge_pattern",
            "Anomaly Watch: revenge-trade pattern",
            f"Detected {revenge_hits} quick size-up re-entries after losses in recent trades.",
            {"hits": revenge_hits},
        )

    # Setup underperformance: recent setup expectancy dropped versus historical baseline.
    by_setup_all: Dict[str, List[float]] = {}
    by_setup_recent: Dict[str, List[float]] = {}
    for r in rows:
        setup = str(r.get("setup_tag") or "").strip() or "Unlabeled"
        by_setup_all.setdefault(setup, []).append(float(r.get("net_pl") or 0.0))
    for r in recent[-12:]:
        setup = str(r.get("setup_tag") or "").strip() or "Unlabeled"
        by_setup_recent.setdefault(setup, []).append(float(r.get("net_pl") or 0.0))
    for setup, vals in by_setup_recent.items():
        all_vals = by_setup_all.get(setup) or []
        if len(vals) < 3 or len(all_vals) < 8:
            continue
        recent_exp = sum(vals) / len(vals)
        base_exp = sum(all_vals[: -len(vals)] or all_vals) / max(
            1, len(all_vals[: -len(vals)] or all_vals)
        )
        if base_exp >= 40.0 and recent_exp <= -40.0:
            _emit_notification(
                "anomaly_setup_underperformance",
                "Anomaly Watch: setup underperformance",
                f"{setup} shifted from {money(base_exp)} baseline expectancy to {money(recent_exp)} recently.",
                {
                    "setup": setup,
                    "recent_expectancy": round(recent_exp, 2),
                    "baseline_expectancy": round(base_exp, 2),
                },
            )
            break


def _alerts_actor() -> str:
    user = str(session.get("auth_user") or "").strip()
    if user:
        return user
    return str(auth.effective_username() or "local")


def _require_ops_mutation_auth() -> None:
    if auth.auth_enabled() and not auth.is_authenticated():
        abort(403)


def _sorted_alerts(
    state: Dict[str, Any], status_filter: str, event_filter: str
) -> List[Dict[str, Any]]:
    alerts = state.get("alerts", [])
    if not isinstance(alerts, list):
        alerts = []
    out: List[Dict[str, Any]] = []
    for a in alerts:
        if not isinstance(a, dict):
            continue
        status = str(a.get("status") or "open")
        event_type = str(a.get("event_type") or "")
        if status_filter == "active" and status == "resolved":
            continue
        if (
            status_filter in {"open", "acknowledged", "resolved", "muted"}
            and status != status_filter
        ):
            continue
        if event_filter and event_type != event_filter:
            continue
        out.append(a)
    out.sort(key=lambda x: _parse_iso_epoch(str(x.get("last_seen_at") or "")) or 0.0, reverse=True)
    return out


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
    content = render_template_string(
        """
        <div class="metricStrip">
          <div class="metric"><div class="label">Active Alerts</div><div class="value">{{ active_count }}</div></div>
          <div class="metric"><div class="label">Open</div><div class="value">{{ open_count }}</div></div>
          <div class="metric"><div class="label">Acknowledged</div><div class="value">{{ ack_count }}</div></div>
          <div class="metric"><div class="label">Resolved</div><div class="value">{{ resolved_count }}</div></div>
        </div>
        <div class="card"><div class="toolbar">
          <div class="pill">🧭 Ops Quick Access</div>
          <div class="leftActions">
            <a class="btn" href="/analytics?tab=diagnostics">🧪 Analytics Diagnostics</a>
            <a class="btn" href="/trades/upload/statement?ws=reconcile">🧮 Reconcile Batches</a>
            <a class="btn" href="/trades/upload/statement?ws=live">🤖 Live Sync Reliability</a>
            <a class="btn" href="/ops/backups">💾 Backup Center</a>
          </div>
        </div></div>
        <div class="card"><div class="toolbar">
          <div class="pill">🚨 Ops Alerts Inbox</div>
          <div class="tiny stack10 line15">Track sync/risk/integrity alerts with acknowledge, resolve, and mute controls.</div>
          <div class="tiny stack8 line16">Why this matters: unresolved alerts hide reliability risk and can distort performance review.</div>
          <div class="tiny stack8 line16">Next best action: clear open alerts first, then tune noise with advanced mute controls.</div>
          <div class="hr"></div>
          <div class="actionRow">
            <a class="btn {% if status_filter == 'active' %}primary{% endif %}" href="/ops/alerts?status=active">Active</a>
            <a class="btn {% if status_filter == 'open' %}primary{% endif %}" href="/ops/alerts?status=open">Open</a>
            <a class="btn {% if status_filter == 'acknowledged' %}primary{% endif %}" href="/ops/alerts?status=acknowledged">Acknowledged</a>
            <a class="btn {% if status_filter == 'resolved' %}primary{% endif %}" href="/ops/alerts?status=resolved">Resolved</a>
            <a class="btn {% if status_filter == 'all' %}primary{% endif %}" href="/ops/alerts?status=all">All</a>
            {% if resolveable_count %}
              <form method="post" action="/ops/alerts/resolve" style="display:inline">
                <input type="hidden" name="resolve_scope" value="visible">
                <input type="hidden" name="status_filter" value="{{ status_filter }}">
                <input type="hidden" name="event_filter" value="{{ event_filter }}">
                <button class="btn" type="submit">Resolve All ({{ resolveable_count }})</button>
              </form>
            {% endif %}
          </div>
          <details class="syncDetails stack10">
            <summary>Advanced Alert Controls</summary>
            <div class="hr"></div>
            <form method="post" action="/ops/alerts/mute" class="row">
              <div>
                <label>Mute Event Type</label>
                <select name="event_type">
                  {% for et in event_types %}
                  <option value="{{ et }}">{{ et }}</option>
                  {% endfor %}
                </select>
              </div>
              <div>
                <label>Minutes</label>
                <input type="number" min="0" max="10080" step="1" name="minutes" value="60" />
              </div>
              <div class="tiny stack10">
                {% if muted %}
                  {% for et, until in muted.items() %}
                    • {{ et }} muted until {{ until }}<br>
                  {% endfor %}
                {% else %}
                  No active mutes.
                {% endif %}
              </div>
              <div class="actionRow"><button class="btn" type="submit">Apply Mute</button></div>
            </form>
          </details>
          <div class="hr"></div>
          <div class="tableWrap"><table class="tableDense">
            <thead><tr><th>When</th><th>Event</th><th>Status</th><th>Count</th><th>Message</th><th>Action</th></tr></thead>
            <tbody>
            {% for a in alerts %}
              <tr>
                <td>{{ a.get('last_seen_at', '—') }}</td>
                <td><code>{{ a.get('event_type', '—') }}</code></td>
                <td>{{ (a.get('status') or 'open')|upper }}</td>
                <td>{{ a.get('count', 1) }}</td>
                <td class="tiny line16">{{ a.get('message', '') }}</td>
                <td>
                  {% if a.get('status') != 'resolved' %}
                    <form method="post" action="/ops/alerts/ack" style="display:inline">
                      <input type="hidden" name="alert_id" value="{{ a.get('id') }}">
                      <button class="btn" type="submit">Ack</button>
                    </form>
                    <form method="post" action="/ops/alerts/resolve" style="display:inline">
                      <input type="hidden" name="alert_id" value="{{ a.get('id') }}">
                      <button class="btn" type="submit">Resolve</button>
                    </form>
                  {% else %}
                    <span class="tiny">Resolved by {{ a.get('resolved_by') or '—' }}</span>
                  {% endif %}
                </td>
              </tr>
            {% else %}
              <tr><td colspan="6">No alerts in this view.</td></tr>
            {% endfor %}
            </tbody>
          </table></div>
        </div></div>
        <div class="card"><div class="toolbar">
          <div class="pill">💾 Auto Backup Settings</div>
          <div class="tiny stack10 line15">Schedule app backups to local storage with your own frequency.</div>
          <div class="tiny stack8 line16">Why this matters: recoverability depends on recent, verified backups.</div>
          <div class="tiny stack8 line16">Next best action: keep auto backup enabled and run one manual backup after major imports.</div>
          <div class="hr"></div>
          <form method="post" action="/ops/backups/config" class="row">
            <div><label><input type="checkbox" name="enabled" value="1" {% if auto_backup_cfg.get('enabled') %}checked{% endif %}/> Enable auto backups</label></div>
            <div class="tiny stack10">
              Last run: {{ auto_backup_cfg.get('last_run_at') or 'Never' }}<br>
              Status: {{ auto_backup_cfg.get('last_status') or 'n/a' }}<br>
              {{ auto_backup_cfg.get('last_message') or '' }}
            </div>
            <div class="actionRow"><button class="btn" type="submit">Save</button></div>
            <details class="syncDetails stack10 fieldGrow2">
              <summary>Advanced Schedule Controls</summary>
              <div class="row">
                <div>
                  <label>Frequency (hours)</label>
                  <input type="number" name="frequency_hours" min="1" max="168" step="1" value="{{ auto_backup_cfg.get('frequency_hours', 24) }}" />
                </div>
                <div>
                  <label>Keep Last (files)</label>
                  <input type="number" name="keep_count" min="3" max="120" step="1" value="{{ auto_backup_cfg.get('keep_count', 21) }}" />
                </div>
              </div>
            </details>
          </form>
          <div class="hr"></div>
          <form method="post" action="/ops/backups/run" class="rightActions" id="backup-run-form">
            <button class="btn primary" type="submit" id="backup-run-submit">Run Backup Now</button>
          </form>
          <div class="syncRunway" id="backup-job-runway" style="display:none;" aria-live="polite">
            <div class="syncRunwayTop">
              <span class="syncBadge" id="backup-job-badge">Running</span>
              <span class="syncStageLabel" id="backup-job-label">Creating backup archive from current app data.</span>
            </div>
            <div class="syncTrack"><div class="syncTrackBar" id="backup-job-track"></div></div>
            <div class="syncSteps">
              <span class="syncStep is-active" data-backup-step>Archive</span>
              <span class="syncStep" data-backup-step>Verify</span>
              <span class="syncStep" data-backup-step>Summary</span>
              <span class="syncStep" data-backup-step>Ready</span>
            </div>
          </div>
          <div id="ops-job-summary" style="display:none;"></div>
        </div></div>
        <div class="card"><div class="toolbar">
          <div class="pill">🧾 Admin Action Timeline</div>
          <div class="tiny stack8 line15">Audit log for rollback, recompute, backup, and alert state changes.</div>
          <div class="hr"></div>
          <div class="tableWrap"><table class="tableDense">
            <thead><tr><th>Time</th><th>Action</th><th>Actor</th><th>Details</th></tr></thead>
            <tbody>
            {% for r in audit_rows %}
              <tr>
                <td>{{ r.get('at', '—') }}</td>
                <td><code>{{ r.get('action', '—') }}</code></td>
                <td>{{ r.get('actor', '—') }}</td>
                <td class="tiny line16"><code>{{ r.get('details', {})|tojson }}</code></td>
              </tr>
            {% else %}
              <tr><td colspan="4">No admin audit events yet.</td></tr>
            {% endfor %}
            </tbody>
          </table></div>
        </div></div>
        """,
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


def _update_alert_status(alert_id: str, status: str) -> bool:
    state = _load_notify_history()
    alerts = state.get("alerts", [])
    if not isinstance(alerts, list):
        return False
    actor = _alerts_actor()
    updated = False
    for a in alerts:
        if not isinstance(a, dict):
            continue
        if str(a.get("id") or "") != alert_id:
            continue
        a["status"] = status
        if status == "acknowledged":
            a["ack_by"] = actor
            a["ack_at"] = now_iso()
        if status == "resolved":
            a["resolved_by"] = actor
            a["resolved_at"] = now_iso()
        updated = True
        break
    if updated:
        state["alerts"] = alerts
        _save_notify_history(state)
    return updated


def _bulk_update_alert_status(status_filter: str, event_filter: str, status: str) -> int:
    state = _load_notify_history()
    alerts = state.get("alerts", [])
    if not isinstance(alerts, list):
        return 0
    targets = _sorted_alerts(state, status_filter=status_filter, event_filter=event_filter)
    target_ids = {
        str(a.get("id") or "")
        for a in targets
        if isinstance(a, dict) and str(a.get("status") or "open") != status
    }
    if not target_ids:
        return 0
    actor = _alerts_actor()
    stamp = now_iso()
    updated = 0
    for a in alerts:
        if not isinstance(a, dict):
            continue
        if str(a.get("id") or "") not in target_ids:
            continue
        a["status"] = status
        if status == "acknowledged":
            a["ack_by"] = actor
            a["ack_at"] = stamp
        if status == "resolved":
            a["resolved_by"] = actor
            a["resolved_at"] = stamp
        updated += 1
    if updated:
        state["alerts"] = alerts
        _save_notify_history(state)
    return updated


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
    _save_auto_backup_config(cfg)
    record_admin_audit(
        "auto_backup_config_saved",
        {
            "enabled": cfg["enabled"],
            "run_weekends": cfg["run_weekends"],
            "run_times_et": cfg["run_times_et"],
            "frequency_hours": cfg["frequency_hours"],
            "keep_count": cfg["keep_count"],
        },
    )
    flash("Auto backup settings saved.", "success")
    return redirect(url_for("ops_backups_page"))


def _run_backup_once(reason: str, actor: str) -> Dict[str, Any]:
    cfg = _load_auto_backup_config()
    try:
        made = _create_backup_archive(reason=reason, actor=actor)
        _prune_auto_backups(int(cfg.get("keep_count") or 21))
        cfg["last_run_at"] = now_iso()
        cfg["last_status"] = "success"
        cfg["last_message"] = f"{made['name']} ({made['size_bytes']} bytes)"
        _save_auto_backup_config(cfg)
        record_admin_audit(
            "backup_created",
            {
                "reason": reason,
                "file": made["name"],
                "size_bytes": made["size_bytes"],
            },
            actor=actor,
        )
        return {"ok": True, **made}
    except Exception as e:
        cfg["last_run_at"] = now_iso()
        cfg["last_status"] = "failed"
        cfg["last_message"] = str(e)
        _save_auto_backup_config(cfg)
        _emit_notification("backup_failed", "Auto backup failed", str(e), {"reason": reason})
        record_admin_audit(
            "backup_failed",
            {"reason": reason, "error": str(e)},
            actor=actor,
        )
        return {"ok": False, "error": str(e)}


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
    full = os.path.abspath(os.path.join(AUTO_BACKUP_DIR, clean))
    root = os.path.abspath(AUTO_BACKUP_DIR)
    if not full.startswith(root + os.sep):
        raise ValueError("unsafe backup path")
    return full


def _count_db_rows(db_path: str) -> Dict[str, int]:
    out = {"trades": 0, "entries": 0, "trade_reviews": 0}
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        for key in list(out.keys()):
            try:
                row = conn.execute(f"SELECT COUNT(*) AS c FROM {key}").fetchone()
                out[key] = int(row["c"] if row else 0)
            except Exception:
                out[key] = 0
    except Exception:
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return out


def _backup_verification(path: str) -> Dict[str, Any]:
    score = 0
    issues: List[str] = []
    try:
        with zipfile.ZipFile(path, "r") as zf:
            names = zf.namelist()
            score += 30
            db_member = "data/journal.db"
            if db_member not in names:
                issues.append("missing data/journal.db")
                return {
                    "score": score,
                    "ok": False,
                    "label": "Missing DB",
                    "issues": issues,
                }
            score += 20
            fd, tmp_path = tempfile.mkstemp(prefix="backup_verify_", suffix=".db")
            os.close(fd)
            try:
                with zf.open(db_member) as src, open(tmp_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
                conn = sqlite3.connect(tmp_path)
                conn.row_factory = sqlite3.Row
                tables = {
                    str(r["name"] or "")
                    for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
                if "trades" in tables and "entries" in tables:
                    score += 25
                else:
                    issues.append("expected tables missing")
                # sample reads to verify DB can be queried
                conn.execute("SELECT COUNT(*) FROM trades").fetchone()
                conn.execute("SELECT COUNT(*) FROM entries").fetchone()
                score += 25
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
    except Exception as e:
        issues.append(str(e))
    ok = score >= 80 and not issues
    label = "Verified" if ok else ("Partial" if score >= 50 else "Failed")
    return {
        "score": score,
        "ok": ok,
        "label": label,
        "issues": issues[:2],
    }


def _restore_dry_run(path: str) -> Dict[str, Any]:
    now_counts = _count_db_rows(str(app_runtime.DB_PATH))
    backup_counts = {"trades": 0, "entries": 0, "trade_reviews": 0}
    upload_new = 0
    upload_overwrite = 0
    upload_bytes = 0
    upload_root = str(app_runtime.UPLOAD_DIR)
    existing_files: set[str] = set()
    for root, _, files in os.walk(upload_root):
        for name in files:
            rel = os.path.relpath(os.path.join(root, name), upload_root)
            existing_files.add(rel.replace("\\", "/"))

    with zipfile.ZipFile(path, "r") as zf:
        names = zf.namelist()
        db_member = "data/journal.db"
        if db_member in names:
            fd, tmp_path = tempfile.mkstemp(prefix="backup_dryrun_", suffix=".db")
            os.close(fd)
            try:
                with zf.open(db_member) as src, open(tmp_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
                backup_counts = _count_db_rows(tmp_path)
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
        for n in names:
            if not n.startswith("data/uploads/") or n.endswith("/"):
                continue
            rel = n[len("data/uploads/") :].replace("\\", "/")
            try:
                info = zf.getinfo(n)
                upload_bytes += int(info.file_size or 0)
            except Exception:
                pass
            if rel in existing_files:
                upload_overwrite += 1
            else:
                upload_new += 1

    return {
        "current_counts": now_counts,
        "backup_counts": backup_counts,
        "delta": {
            "trades": int(backup_counts["trades"] - now_counts["trades"]),
            "entries": int(backup_counts["entries"] - now_counts["entries"]),
            "trade_reviews": int(backup_counts["trade_reviews"] - now_counts["trade_reviews"]),
        },
        "uploads": {
            "new_files": upload_new,
            "overwritten_files": upload_overwrite,
            "payload_bytes": upload_bytes,
        },
    }


def _list_saved_backups() -> List[Dict[str, Any]]:
    if not os.path.isdir(AUTO_BACKUP_DIR):
        return []
    out: List[Dict[str, Any]] = []
    for n in os.listdir(AUTO_BACKUP_DIR):
        if not n.endswith(".zip"):
            continue
        p = os.path.join(AUTO_BACKUP_DIR, n)
        if not os.path.isfile(p):
            continue
        verify = _backup_verification(p)
        out.append(
            {
                "name": n,
                "size_bytes": os.path.getsize(p),
                "modified_at": datetime.fromtimestamp(os.path.getmtime(p)).isoformat(
                    timespec="seconds"
                ),
                "verify_score": int(verify.get("score") or 0),
                "verify_ok": bool(verify.get("ok")),
                "verify_label": str(verify.get("label") or "Unknown"),
                "verify_issues": verify.get("issues") or [],
            }
        )
    out.sort(key=lambda x: str(x.get("modified_at") or ""), reverse=True)
    return out


def ops_backups_page():
    cfg = _load_auto_backup_config()
    backups = _list_saved_backups()
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
        backup_dir=AUTO_BACKUP_DIR,
        dry_run_name=dry_run_name,
        dry_run_report=dry_run_report,
        audit_rows=audit_rows,
        audit_action=audit_action,
        audit_limit=audit_limit,
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


def _restore_from_backup_path(path: str) -> None:
    with zipfile.ZipFile(path, "r") as zf:
        names = zf.namelist()
        if not names:
            raise ValueError("Backup zip is empty.")
        allowed_prefixes = ("data/journal.db", "data/uploads/", "data/meta.json")
        for n in names:
            if n.startswith("/") or ".." in n:
                raise ValueError("Backup zip contains unsafe paths.")
            if not any(n == p or n.startswith(p) for p in allowed_prefixes):
                raise ValueError("Backup zip contains unsupported files.")
        db_member = "data/journal.db"
        if db_member in names:
            db_path = str(app_runtime.DB_PATH)
            os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
            db_dir = os.path.dirname(db_path) or "."
            fd, tmp_db = tempfile.mkstemp(prefix="restore_db_", suffix=".tmp", dir=db_dir)
            os.close(fd)
            try:
                with zf.open(db_member) as src, open(tmp_db, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
                os.replace(tmp_db, db_path)
            finally:
                if os.path.exists(tmp_db):
                    os.unlink(tmp_db)
        upload_root = str(app_runtime.UPLOAD_DIR)
        os.makedirs(upload_root, exist_ok=True)
        for n in names:
            if not n.startswith("data/uploads/") or n.endswith("/"):
                continue
            rel = n[len("data/uploads/") :]
            out_path = os.path.join(upload_root, rel)
            out_dir = os.path.dirname(out_path)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
            with zf.open(n) as src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst, length=1024 * 1024)


def _start_restore_job(path: str, actor: str) -> Dict[str, Any]:
    app = current_app._get_current_object()
    job = _create_bg_job(
        "restore",
        "Restore Backup",
        {"file": os.path.basename(path)},
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
                _restore_from_backup_path(path)
                record_admin_audit(
                    "backup_restored_from_center",
                    {"file": os.path.basename(path)},
                    actor=actor,
                )
            summary = _build_action_result_summary(
                tone="success",
                title="Restore Complete",
                happened=f"Restored from {os.path.basename(path)}.",
                changed="Database rows were replaced and upload files were merged from the selected backup.",
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

    threading.Thread(target=runner, daemon=True, name=f"restore-job-{job['id'][:8]}").start()
    return job


def ops_backups_restore():
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
    if (request.args.get("async") or "").strip() == "1":
        job = _start_restore_job(path, actor=_alerts_actor())
        return jsonify({"ok": True, "job": _job_response_payload(job)})
    try:
        _restore_from_backup_path(path)
    except Exception as e:
        flash(f"Restore failed: {e}", "warn")
        return redirect(url_for("ops_backups_page"))
    record_admin_audit("backup_restored_from_center", {"file": os.path.basename(path)})
    flash(f"Restored from {os.path.basename(path)}.", "success")
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


def _integrity_health_snapshot() -> Dict[str, Any]:
    rows = analytics_repo.fetch_analytics_rows()
    diag = analytics_repo.integrity_diagnostics(rows)
    with db() as conn:
        orphan_reviews = int(
            (
                conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM trade_reviews r
                    LEFT JOIN trades t ON t.id = r.trade_id
                    WHERE t.id IS NULL
                    """
                ).fetchone()
                or {"c": 0}
            )["c"]
        )
        missing_balance = int(
            (
                conn.execute("SELECT COUNT(*) AS c FROM trades WHERE balance IS NULL").fetchone()
                or {"c": 0}
            )["c"]
        )
    issues = int(
        diag.get("stale_balance_rows", 0)
        + diag.get("missing_setup", 0)
        + diag.get("missing_session", 0)
        + diag.get("missing_score", 0)
        + diag.get("duplicate_candidates", 0)
        + orphan_reviews
        + missing_balance
    )
    return {
        "issues": issues,
        "diag": diag,
        "orphan_reviews": orphan_reviews,
        "missing_balance": missing_balance,
    }


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

    content = render_template_string(
        """
        <div class="metricStrip">
          <div class="metric"><div class="label">Open Buckets</div><div class="value">{{ grouped_rows|length }}</div></div>
          <div class="metric"><div class="label">Open Contracts</div><div class="value">{{ total_contracts }}</div></div>
          <div class="metric"><div class="label">Capital In Open Lots</div><div class="value">{{ money(total_spent) }}</div></div>
          <div class="metric"><div class="label">Candidate Rows</div><div class="value">{{ rows|length }}</div></div>
        </div>

        <div class="card"><div class="toolbar">
          <div class="pill">📂 Open Positions (Unmatched / Incomplete)</div>
          <div class="tiny stack10 line15">Derived from trades missing close info (no exit time, no exit price, or no net P/L).</div>
          <div class="hr"></div>
          <form method="get" class="row">
            <div><label>As of Date</label><input type="date" name="as_of" value="{{ as_of }}"></div>
            <div class="fieldGrow2"><label>Filter</label><input name="q" value="{{ q }}" placeholder="SPX, CALL, raw note..."></div>
            <div class="actionRow">
              <button class="btn" type="submit">Apply</button>
              <a class="btn" href="/trades/open-positions">Reset</a>
              <a class="btn" href="/trades">Back Trades</a>
            </div>
          </form>
        </div></div>

        <div class="card stack12"><div class="toolbar">
          <div class="pill">🧾 Position Summary</div>
          <div class="hr"></div>
          <div class="tableWrap"><table class="tableDense">
            <thead><tr><th>Symbol</th><th>Open Trades</th><th>Contracts</th><th>Capital</th><th>Latest</th></tr></thead>
            <tbody>
            {% for r in grouped_rows %}
              <tr>
                <td>{{ r.symbol }}</td>
                <td>{{ r.trades }}</td>
                <td>{{ r.contracts }}</td>
                <td>{{ money(r.total_spent) }}</td>
                <td>{{ r.latest_date }}</td>
              </tr>
            {% endfor %}
            {% if grouped_rows|length == 0 %}
              <tr><td colspan="5">No open-position candidates found.</td></tr>
            {% endif %}
            </tbody>
          </table></div>
        </div></div>
        """,
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
