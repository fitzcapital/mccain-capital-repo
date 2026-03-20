"""Backup, restore, and integrity helpers extracted from trades.py."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import threading
import time
import zipfile
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from flask import current_app
from werkzeug.utils import secure_filename

from mccain_capital import runtime as app_runtime
from mccain_capital.repositories import analytics as analytics_repo
from mccain_capital.runtime import db, now_iso


# ---------------------------------------------------------------------------
# Path override variables (set by tests / local debugging)
# ---------------------------------------------------------------------------
AUTO_BACKUP_CONFIG_PATH: Optional[str] = None
AUTO_BACKUP_DIR: Optional[str] = None
AUTO_BACKUP_LOCK_PATH: Optional[str] = None


# ---------------------------------------------------------------------------
# Internal utility: atomic JSON writer (copy of the shared helper in trades.py)
# ---------------------------------------------------------------------------
def _safe_write_json(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp_path = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp_path, path)
        return
    except PermissionError:
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


# ---------------------------------------------------------------------------
# Path accessors
# ---------------------------------------------------------------------------
def _auto_backup_config_path() -> str:
    # Also check trades module so monkeypatch in tests can override.
    try:
        import mccain_capital.services.trades as _trades_mod
        override = _trades_mod.__dict__.get("AUTO_BACKUP_CONFIG_PATH") or AUTO_BACKUP_CONFIG_PATH
    except ImportError:
        override = AUTO_BACKUP_CONFIG_PATH
    return str(override or app_runtime.upload_path(".auto_backup_config.json"))


def _auto_backup_dir() -> str:
    try:
        import mccain_capital.services.trades as _trades_mod
        override = _trades_mod.__dict__.get("AUTO_BACKUP_DIR") or AUTO_BACKUP_DIR
    except ImportError:
        override = AUTO_BACKUP_DIR
    return str(override or app_runtime.upload_path("backups"))


def _auto_backup_lock_path() -> str:
    try:
        import mccain_capital.services.trades as _trades_mod
        override = _trades_mod.__dict__.get("AUTO_BACKUP_LOCK_PATH") or AUTO_BACKUP_LOCK_PATH
    except ImportError:
        override = AUTO_BACKUP_LOCK_PATH
    return str(override or app_runtime.upload_path(".auto_backup.lock"))


def _auto_backup_config_paths(for_read: bool = True) -> List[str]:
    fallback = os.path.join(tempfile.gettempdir(), "mccain-capital", ".auto_backup_config.json")
    ordered = (_auto_backup_config_path(), fallback)
    if for_read and os.path.isfile(fallback):
        ordered = (fallback, _auto_backup_config_path())
    paths: List[str] = []
    for path in ordered:
        p = os.path.abspath(str(path))
        if p and p not in paths:
            paths.append(p)
    return paths


# ---------------------------------------------------------------------------
# Config load / save
# ---------------------------------------------------------------------------
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
    # Look up through trades module so monkeypatch on trades_svc works in tests.
    try:
        import mccain_capital.services.trades as _tm
        _paths_fn = _tm.__dict__.get("_auto_backup_config_paths") or _auto_backup_config_paths
    except ImportError:
        _paths_fn = _auto_backup_config_paths
    parsed: Any = None
    for path in _paths_fn(for_read=True):
        try:
            with open(path, "r", encoding="utf-8") as f:
                parsed = json.load(f)
            break
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            continue
    if parsed is None:
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


def _save_auto_backup_config(cfg: Dict[str, Any]) -> bool:
    # Look up function through trades module so monkeypatch on trades_svc works in tests.
    try:
        import mccain_capital.services.trades as _tm
        _paths_fn = _tm.__dict__.get("_auto_backup_config_paths") or _auto_backup_config_paths
        _write_fn = _tm.__dict__.get("_safe_write_json") or _safe_write_json
    except ImportError:
        _paths_fn = _auto_backup_config_paths
        _write_fn = _safe_write_json

    errors: List[str] = []
    for path in _paths_fn(for_read=False):
        try:
            _write_fn(path, cfg)
            return True
        except OSError as exc:
            errors.append(f"{path}: {exc}")
    if errors:
        current_app.logger.error("Auto backup config write failed: %s", " | ".join(errors))
    return False


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


# ---------------------------------------------------------------------------
# Archive creation and pruning
# ---------------------------------------------------------------------------
def _create_backup_archive(reason: str, actor: str) -> Dict[str, Any]:
    stamp = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d_%H%M%S")
    os.makedirs(_auto_backup_dir(), exist_ok=True)
    name = f"mccain_backup_{stamp}_{secure_filename(reason or 'manual')}.zip"
    out_path = os.path.join(_auto_backup_dir(), name)
    db_path = str(app_runtime.DB_PATH)
    upload_root = str(app_runtime.UPLOAD_DIR)
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if os.path.exists(db_path):
            zf.write(db_path, arcname="data/journal.db")
        if os.path.isdir(upload_root):
            for root, _, files in os.walk(upload_root):
                for fn in files:
                    full = os.path.join(root, fn)
                    if os.path.abspath(full).startswith(
                        os.path.abspath(_auto_backup_dir()) + os.sep
                    ):
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
    if not os.path.isdir(_auto_backup_dir()):
        return
    files = [
        os.path.join(_auto_backup_dir(), n)
        for n in os.listdir(_auto_backup_dir())
        if n.endswith(".zip") and os.path.isfile(os.path.join(_auto_backup_dir(), n))
    ]
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    for p in files[max(3, keep_count) :]:
        try:
            os.unlink(p)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Verification helpers
# ---------------------------------------------------------------------------
def _count_db_rows(db_path: str) -> Dict[str, int]:
    out = {"trades": 0, "entries": 0, "trade_reviews": 0}
    conn = None
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
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return out


def _backup_verification(path: str) -> Dict[str, Any]:
    score = 0
    issues: List[str] = []
    conn = None
    tmp_path = ""
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
                conn.execute("SELECT COUNT(*) FROM trades").fetchone()
                conn.execute("SELECT COUNT(*) FROM entries").fetchone()
                score += 25
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                if tmp_path and os.path.exists(tmp_path):
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


# ---------------------------------------------------------------------------
# List and dry-run
# ---------------------------------------------------------------------------
def _list_saved_backups() -> List[Dict[str, Any]]:
    if not os.path.isdir(_auto_backup_dir()):
        return []
    out: List[Dict[str, Any]] = []
    for n in os.listdir(_auto_backup_dir()):
        if not n.endswith(".zip"):
            continue
        p = os.path.join(_auto_backup_dir(), n)
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


# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Integrity snapshot
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Core run-once and worker
# ---------------------------------------------------------------------------
def _run_backup_once(reason: str, actor: str) -> Dict[str, Any]:
    # Lazy imports to avoid circular dependency with trades.py.
    from mccain_capital.services.trades import record_admin_audit, _emit_notification

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
                fd = os.open(_auto_backup_lock_path(), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
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
                    os.unlink(_auto_backup_lock_path())
                except OSError:
                    pass
            time.sleep(20)
        except Exception:
            time.sleep(45)
