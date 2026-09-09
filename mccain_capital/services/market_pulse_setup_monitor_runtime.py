"""Server-owned Market Pulse setup evaluation lifecycle."""

from __future__ import annotations

from datetime import datetime
import os
import sys
import threading
import time
from typing import Any, Callable

from flask import Flask

from mccain_capital import runtime as app_runtime

_LOCK = threading.Lock()
_STARTED = False
_THREAD: threading.Thread | None = None
_STOP = threading.Event()
_STATE: dict[str, Any] = {
    "enabled": False,
    "status": "disabled",
    "owner": "",
    "heartbeat_at": "",
    "last_attempt_at": "",
    "last_success_at": "",
    "last_evaluated_candle": "",
    "consecutive_failures": 0,
    "clock_status": "unknown",
    "last_error": "",
}


def _enabled(app: Flask) -> bool:
    configured = app.config.get("MARKET_PULSE_SERVER_SETUP_MONITOR_ENABLED")
    if configured is None:
        configured = os.environ.get("MARKET_PULSE_SERVER_SETUP_MONITOR_ENABLED", "1")
    value = str(configured).strip().lower()
    return value not in {"0", "false", "no", "off"}


def _interval(app: Flask) -> int:
    value = app.config.get(
        "MARKET_PULSE_SERVER_SETUP_MONITOR_INTERVAL_SECONDS",
        os.environ.get("MARKET_PULSE_SERVER_SETUP_MONITOR_INTERVAL_SECONDS", "15"),
    )
    return max(5, min(60, int(value or 15)))


def _clock_threshold(app: Flask) -> float:
    value = app.config.get(
        "MARKET_PULSE_SERVER_SETUP_CLOCK_DRIFT_SECONDS",
        os.environ.get("MARKET_PULSE_SERVER_SETUP_CLOCK_DRIFT_SECONDS", "15"),
    )
    return max(2.0, float(value or 15))


def _iso(now: datetime | None = None) -> str:
    return (now or app_runtime.now_et()).isoformat()


def get_server_setup_monitor_state(*, now: datetime | None = None) -> dict[str, Any]:
    with _LOCK:
        state = dict(_STATE)
    current = now or app_runtime.now_et()
    heartbeat = str(state.get("heartbeat_at") or "")
    try:
        parsed = datetime.fromisoformat(heartbeat)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=current.tzinfo)
        state["heartbeat_age_seconds"] = max(
            0, int((current - parsed.astimezone(current.tzinfo)).total_seconds())
        )
    except (TypeError, ValueError):
        state["heartbeat_age_seconds"] = None
    state["thread_alive"] = bool(_THREAD and _THREAD.is_alive())
    return state


def _update(**values: Any) -> None:
    with _LOCK:
        _STATE.update(values)


def run_server_setup_monitor_cycle(
    app: Flask,
    *,
    now: datetime | None = None,
    clock_discontinuous: bool = False,
    evaluator: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run one deterministic setup cycle without a browser request."""

    attempted_at = _iso(now)
    previous = get_server_setup_monitor_state(now=now)
    _update(last_attempt_at=attempted_at, heartbeat_at=attempted_at)
    with app.app_context():
        if evaluator is None:
            from mccain_capital.services.core import run_market_pulse_server_setup_monitor_cycle

            evaluator = run_market_pulse_server_setup_monitor_cycle
        result = evaluator(
            now_et=now,
            clock_discontinuous=clock_discontinuous,
            previous_candle=str(previous.get("last_evaluated_candle") or ""),
        )
    status = str(result.get("status") or "error")
    success = status in {"healthy", "sleeping", "unchanged"}
    _update(
        enabled=True,
        status=status,
        heartbeat_at=_iso(now),
        last_success_at=_iso(now) if success else _STATE.get("last_success_at", ""),
        last_evaluated_candle=str(
            result.get("last_evaluated_candle") or _STATE.get("last_evaluated_candle") or ""
        ),
        consecutive_failures=0 if success else int(_STATE.get("consecutive_failures") or 0) + 1,
        clock_status="recovering" if clock_discontinuous else "coherent",
        last_error="" if success else str(result.get("error") or status)[:160],
    )
    return result


def _worker_loop(app: Flask) -> None:
    interval = _interval(app)
    previous_wall = time.time()
    previous_mono = time.monotonic()
    while not _STOP.is_set():
        wall = time.time()
        mono = time.monotonic()
        discontinuous = abs((wall - previous_wall) - (mono - previous_mono)) > _clock_threshold(app)
        previous_wall, previous_mono = wall, mono
        try:
            result = run_server_setup_monitor_cycle(
                app,
                clock_discontinuous=discontinuous,
            )
            wait_seconds = interval if result.get("status") != "sleeping" else min(60, interval * 4)
        except Exception as exc:  # pragma: no cover - defensive runtime boundary
            _update(
                enabled=True,
                status="degraded",
                heartbeat_at=_iso(),
                consecutive_failures=int(_STATE.get("consecutive_failures") or 0) + 1,
                clock_status="recovering" if discontinuous else "coherent",
                last_error=str(exc)[:160],
            )
            wait_seconds = min(60, interval * max(1, int(_STATE["consecutive_failures"])))
        _STOP.wait(wait_seconds)


def start_server_setup_monitor_once(app: Flask, *, force: bool = False) -> bool:
    """Start one monitor thread per process; return whether a thread was started."""

    global _STARTED, _THREAD
    if (app.config.get("TESTING") or "pytest" in sys.modules) and not force:
        _update(enabled=False, status="disabled")
        return False
    if not _enabled(app):
        _update(enabled=False, status="disabled")
        return False
    with _LOCK:
        if _STARTED:
            return False
        _STARTED = True
        _STOP.clear()
        owner = f"pid:{os.getpid()}"
        _STATE.update(enabled=True, status="starting", owner=owner)
        _THREAD = threading.Thread(
            target=_worker_loop,
            args=(app,),
            name="market-pulse-setup-monitor",
            daemon=True,
        )
        _THREAD.start()
    return True


def reset_server_setup_monitor_for_tests() -> None:
    global _STARTED, _THREAD
    _STOP.set()
    thread = _THREAD
    if thread and thread.is_alive():
        thread.join(timeout=1)
    with _LOCK:
        _STARTED = False
        _THREAD = None
        _STATE.clear()
        _STATE.update(
            enabled=False,
            status="disabled",
            owner="",
            heartbeat_at="",
            last_attempt_at="",
            last_success_at="",
            last_evaluated_candle="",
            consecutive_failures=0,
            clock_status="unknown",
            last_error="",
        )
