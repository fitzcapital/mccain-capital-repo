"""Ops endpoints extracted from the trades service."""

from __future__ import annotations

import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import abort, flash, jsonify, redirect, request, send_file, url_for

from mccain_capital.services import trades as legacy
from mccain_capital.services.job_presenters import job_response_payload


def _auto_backup_worker(app) -> None:
    while True:
        try:
            cfg = legacy._load_auto_backup_config()
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
                fd = os.open(legacy.AUTO_BACKUP_LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
            except FileExistsError:
                time.sleep(20)
                continue
            try:
                with app.app_context():
                    legacy._run_backup_once(reason="scheduled_auto", actor="auto-backup-worker")
                    cfg = legacy._load_auto_backup_config()
                    cfg["last_run_slot_key"] = slot_key
                    legacy._save_auto_backup_config(cfg)
            finally:
                try:
                    os.unlink(legacy.AUTO_BACKUP_LOCK_PATH)
                except OSError:
                    pass
            time.sleep(20)
        except Exception:
            time.sleep(45)


def ops_backups_config():
    return legacy.ops_backups_config()


def ops_backups_page():
    return legacy.ops_backups_page()


def ops_backups_run_now():
    legacy._require_ops_mutation_auth()
    if (request.args.get("async") or "").strip() == "1":
        job = legacy._start_backup_job(reason="manual_ops_run", actor=legacy._alerts_actor())
        return jsonify(
            {
                "ok": True,
                "job": job_response_payload(job, humanize_timestamp=legacy._humanize_et_timestamp),
            }
        )
    out = legacy._run_backup_once(reason="manual_ops_run", actor=legacy._alerts_actor())
    if out.get("ok"):
        flash(f"Backup created: {out.get('name')}", "success")
    else:
        flash(f"Backup failed: {out.get('error')}", "warn")
    return redirect(url_for("ops_backups_page"))


def ops_backups_download(name: str):
    try:
        path = legacy._safe_backup_file_path(name)
    except ValueError:
        abort(400)
    if not os.path.isfile(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=os.path.basename(path))


def ops_backups_restore():
    legacy._require_ops_mutation_auth()
    name = (request.form.get("name") or "").strip()
    try:
        path = legacy._safe_backup_file_path(name)
    except ValueError:
        flash("Invalid backup name.", "warn")
        return redirect(url_for("ops_backups_page"))
    if not os.path.isfile(path):
        flash("Backup not found.", "warn")
        return redirect(url_for("ops_backups_page"))
    if (request.args.get("async") or "").strip() == "1":
        job = legacy._start_restore_job(path, actor=legacy._alerts_actor())
        return jsonify(
            {
                "ok": True,
                "job": job_response_payload(job, humanize_timestamp=legacy._humanize_et_timestamp),
            }
        )
    try:
        legacy._restore_from_backup_path(path)
    except Exception as e:
        flash(f"Restore failed: {e}", "warn")
        return redirect(url_for("ops_backups_page"))
    legacy.record_admin_audit("backup_restored_from_center", {"file": os.path.basename(path)})
    flash(f"Restored from {os.path.basename(path)}.", "success")
    return redirect(url_for("ops_backups_page"))


def ops_job_status(job_id: str):
    legacy._require_ops_mutation_auth()
    job = legacy._get_bg_job((job_id or "").strip())
    if not job:
        return jsonify({"ok": False, "error": "job_not_found"}), 404
    return jsonify(
        {
            "ok": True,
            "job": job_response_payload(job, humanize_timestamp=legacy._humanize_et_timestamp),
        }
    )


def ops_backups_restore_dry_run():
    legacy._require_ops_mutation_auth()
    name = (request.form.get("name") or "").strip()
    if not name:
        flash("Pick a backup file for dry run.", "warn")
        return redirect(url_for("ops_backups_page"))
    return redirect(url_for("ops_backups_page", dry_run=name))


def ops_backups_delete():
    legacy._require_ops_mutation_auth()
    name = (request.form.get("name") or "").strip()
    try:
        path = legacy._safe_backup_file_path(name)
    except ValueError:
        flash("Invalid backup name.", "warn")
        return redirect(url_for("ops_backups_page"))
    if not os.path.isfile(path):
        flash("Backup not found.", "warn")
        return redirect(url_for("ops_backups_page"))
    try:
        os.unlink(path)
        legacy.record_admin_audit("backup_deleted", {"file": os.path.basename(path)})
        flash(f"Deleted backup {os.path.basename(path)}.", "success")
    except OSError as e:
        flash(f"Delete failed: {e}", "warn")
    return redirect(url_for("ops_backups_page"))


def ops_integrity_run():
    legacy._require_ops_mutation_auth()
    if (request.args.get("async") or "").strip() == "1":
        job = legacy._start_integrity_job()
        return jsonify(
            {
                "ok": True,
                "job": job_response_payload(job, humanize_timestamp=legacy._humanize_et_timestamp),
            }
        )
    snap = legacy._integrity_health_snapshot()
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
    legacy.record_admin_audit(
        "integrity_check_run",
        {
            "issues": int(snap.get("issues", 0)),
            "orphan_reviews": int(snap.get("orphan_reviews", 0)),
            "missing_balance": int(snap.get("missing_balance", 0)),
        },
        actor=legacy._alerts_actor(),
    )
    return redirect(url_for("ops_backups_page"))


def ops_integrity_job_status(job_id: str):
    legacy._require_ops_mutation_auth()
    job = legacy._get_bg_job((job_id or "").strip())
    if not job or str(job.get("kind") or "") != "integrity":
        return jsonify({"ok": False, "error": "job_not_found"}), 404
    return jsonify(
        {
            "ok": True,
            "job": job_response_payload(job, humanize_timestamp=legacy._humanize_et_timestamp),
        }
    )
