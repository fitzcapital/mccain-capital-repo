"""Notification, alert, and anomaly helpers extracted from trades.py."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from flask import session

from mccain_capital import auth
from mccain_capital import runtime as app_runtime
from mccain_capital.repositories import analytics as analytics_repo
from mccain_capital.runtime import money, now_iso


# ---------------------------------------------------------------------------
# Default constants (may be overridden by tests via trades.NOTIFY_*)
# ---------------------------------------------------------------------------
NOTIFY_WEBHOOK_URL = (os.environ.get("NOTIFY_WEBHOOK_URL") or "").strip()
NOTIFY_FAIL_STREAK = int(os.environ.get("NOTIFY_FAIL_STREAK", "3") or 3)
NOTIFY_WEBHOOK_SECRET = (os.environ.get("NOTIFY_WEBHOOK_SECRET") or "").strip()
NOTIFY_RETRY_ATTEMPTS = int(os.environ.get("NOTIFY_RETRY_ATTEMPTS", "3") or 3)
NOTIFY_RETRY_BACKOFF_SEC = float(os.environ.get("NOTIFY_RETRY_BACKOFF_SEC", "0.4") or 0.4)
NOTIFY_RETRY_BACKOFF_MULTIPLIER = float(
    os.environ.get("NOTIFY_RETRY_BACKOFF_MULTIPLIER", "2.0") or 2.0
)
NOTIFY_DEFAULT_DEDUPE_SECONDS = int(os.environ.get("NOTIFY_DEFAULT_DEDUPE_SECONDS", "300") or 300)
NOTIFY_DEDUPE_BY_EVENT: Dict[str, int] = {
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


def _tm_dict() -> dict:
    """Return trades module __dict__ at call time (supports test monkeypatching)."""
    try:
        import mccain_capital.services.trades as _tm  # noqa: PLC0415

        return _tm.__dict__
    except ImportError:
        return {}


# ---------------------------------------------------------------------------
# History path helpers
# ---------------------------------------------------------------------------
def _notify_history_paths(for_read: bool = True) -> List[str]:
    # Check trades module for path override so tests can monkeypatch BROKER_NOTIFY_HISTORY_PATH.
    _d = _tm_dict()
    override_path = _d.get("BROKER_NOTIFY_HISTORY_PATH")
    if override_path:
        primary = str(override_path)
    else:
        primary = str(app_runtime.upload_path(".vanquish_notify_history.json"))

    fallback = os.path.join(
        tempfile.gettempdir(), "mccain-capital", ".vanquish_notify_history.json"
    )
    ordered = (primary, fallback)
    if for_read and os.path.isfile(fallback):
        ordered = (fallback, primary)
    paths: List[str] = []
    for path in ordered:
        p = os.path.abspath(str(path))
        if p and p not in paths:
            paths.append(p)
    return paths


def _load_notify_history() -> Dict[str, Any]:
    _d = _tm_dict()
    _paths_fn = _d.get("_notify_history_paths") or _notify_history_paths
    for path in _paths_fn(for_read=True):
        try:
            with open(path, "r", encoding="utf-8") as f:
                parsed = json.load(f)
                return parsed if isinstance(parsed, dict) else {}
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            continue
    return {}


def _save_notify_history(state: Dict[str, Any]) -> None:
    _d = _tm_dict()
    _paths_fn = _d.get("_notify_history_paths") or _notify_history_paths
    _write_fn = _d.get("_safe_write_json") or _safe_write_json_local

    errors: List[str] = []
    for path in _paths_fn(for_read=False):
        try:
            _write_fn(path, state)
            return
        except OSError as exc:
            errors.append(f"{path}: {exc}")
    if errors:
        raise OSError("Notify history write failed: " + " | ".join(errors))


def _safe_write_json_local(path: str, payload: Any) -> None:
    """Local copy of _safe_write_json for when the trades module is not available."""
    import threading  # noqa: PLC0415

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
# Dedupe / fingerprint helpers
# ---------------------------------------------------------------------------
def _notify_dedupe_window_seconds(event_type: str) -> int:
    _d = _tm_dict()
    dedupe_map = _d.get("NOTIFY_DEDUPE_BY_EVENT", NOTIFY_DEDUPE_BY_EVENT)
    default_sec = _d.get("NOTIFY_DEFAULT_DEDUPE_SECONDS", NOTIFY_DEFAULT_DEDUPE_SECONDS)
    return max(0, int(dedupe_map.get(event_type, default_sec)))


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


# ---------------------------------------------------------------------------
# Alert tracking
# ---------------------------------------------------------------------------
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


def _append_notification_history(state: Dict[str, Any], payload: Dict[str, Any]) -> None:
    sent = state.get("sent", [])
    if not isinstance(sent, list):
        sent = []
    sent.append(payload)
    if len(sent) > 200:
        sent = sent[-200:]
    state["sent"] = sent
    _save_notify_history(state)


# ---------------------------------------------------------------------------
# Webhook emission
# ---------------------------------------------------------------------------
def _signed_headers(body: bytes, event_type: str, ts: str) -> Dict[str, str]:
    _d = _tm_dict()
    webhook_secret = _d.get("NOTIFY_WEBHOOK_SECRET", NOTIFY_WEBHOOK_SECRET)
    headers = {
        "Content-Type": "application/json",
        "X-McCain-Event": event_type,
        "X-McCain-Timestamp": ts,
    }
    if webhook_secret:
        digest = hmac.new(str(webhook_secret).encode("utf-8"), body, hashlib.sha256).hexdigest()
        headers["X-McCain-Signature"] = f"sha256={digest}"
    return headers


def _emit_notification(
    event_type: str, title: str, message: str, extra: Optional[Dict[str, Any]] = None
) -> None:
    # Read all config at call time so test monkeypatching on trades_svc works.
    _d = _tm_dict()
    webhook_url = _d.get("NOTIFY_WEBHOOK_URL", NOTIFY_WEBHOOK_URL)
    retry_attempts = _d.get("NOTIFY_RETRY_ATTEMPTS", NOTIFY_RETRY_ATTEMPTS)
    backoff_sec = _d.get("NOTIFY_RETRY_BACKOFF_SEC", NOTIFY_RETRY_BACKOFF_SEC)
    backoff_mult = _d.get("NOTIFY_RETRY_BACKOFF_MULTIPLIER", NOTIFY_RETRY_BACKOFF_MULTIPLIER)

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
    if not webhook_url:
        payload["delivery"] = {"status": "local_only"}
        _record_alert_event(state, payload)
        _append_notification_history(state, payload)
        _save_notify_history(state)
        return
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    attempts = max(1, int(retry_attempts))
    wait = max(0.0, float(backoff_sec))
    scale = max(1.0, float(backoff_mult))
    last_error = ""
    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(
                str(webhook_url),
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


# ---------------------------------------------------------------------------
# Alert status helpers
# ---------------------------------------------------------------------------
def _alerts_actor() -> str:
    user = str(session.get("auth_user") or "").strip()
    if user:
        return user
    return str(auth.effective_username() or "local")


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


# ---------------------------------------------------------------------------
# Anomaly scanning
# ---------------------------------------------------------------------------
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
