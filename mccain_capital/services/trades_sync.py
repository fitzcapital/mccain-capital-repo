"""Live sync and auto-sync orchestration for trades."""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import flash, jsonify, redirect, request, url_for

from mccain_capital.services import trades as legacy
from mccain_capital.services import trades_ops
from mccain_capital.services.job_presenters import job_response_payload

_AUTO_SYNC_THREAD_STARTED = False
_AUTO_SYNC_THREAD_LOCK = threading.Lock()
_AUTO_BACKUP_THREAD_STARTED = False
_AUTO_BACKUP_THREAD_LOCK = threading.Lock()


def trades_sync_live():
    if request.method != "POST":
        return redirect(url_for("trades_upload_pdf"))

    mode = (request.form.get("mode") or "broker").strip()
    guardrail = legacy.trade_lockout_state(legacy.today_iso())
    if guardrail["locked"] and mode == "broker":
        return legacy.render_page(
            legacy.simple_msg(
                f"Daily max-loss guardrail is active for {guardrail['day']}. "
                f"Day net {legacy.money(guardrail['day_net'])} reached limit "
                f"{legacy.money(guardrail['daily_max_loss'])}."
            ),
            active="trades",
        )

    cfg = legacy._load_broker_sync_config()
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
        return legacy.render_page(
            legacy.simple_msg("Username and password are required for live login sync."),
            active="trades",
        )
    if not base_url or not account:
        return legacy.render_page(
            legacy.simple_msg("Base origin and account are required for live login sync."),
            active="trades",
        )

    from_date = legacy._normalize_iso_date(request.form.get("from_date") or "", legacy.today_iso())
    to_date = legacy._normalize_iso_date(request.form.get("to_date") or "", legacy.today_iso())
    if from_date > to_date:
        from_date, to_date = to_date, from_date

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
        legacy._save_broker_sync_config(cfg)
    job = legacy._start_sync_job(
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
    cfg = legacy._load_auto_sync_config()
    auto_password = legacy._get_auto_sync_password(cfg)
    if not cfg.get("username") or not auto_password:
        flash(
            "Auto sync credentials are missing. Save username and password in the Live Sync workspace first.",
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
    job = legacy._get_bg_job((job_id or "").strip())
    if not job:
        return jsonify({"ok": False, "error": "job_not_found"}), 404
    return jsonify(
        {
            "ok": True,
            "job": job_response_payload(job, humanize_timestamp=legacy._humanize_et_timestamp),
        }
    )


def ensure_auto_sync_worker_started(app) -> None:
    global _AUTO_SYNC_THREAD_STARTED, _AUTO_BACKUP_THREAD_STARTED
    with _AUTO_SYNC_THREAD_LOCK:
        if not _AUTO_SYNC_THREAD_STARTED:
            t = threading.Thread(target=_auto_sync_worker, args=(app,), daemon=True, name="auto-sync-worker")
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
                fd = os.open(legacy.BROKER_AUTO_SYNC_LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
            except FileExistsError:
                time.sleep(20)
                continue
            try:
                with app.app_context():
                    started = time.time()
                    run = legacy._run_live_sync_once(
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
                            "stage_help": legacy.SYNC_STAGE_HELP.get(str(run.get("stage") or ""), ""),
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
                    os.unlink(legacy.BROKER_AUTO_SYNC_LOCK_PATH)
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
