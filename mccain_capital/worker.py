"""Dedicated background-worker entrypoint for the local Kubernetes deployment."""

from __future__ import annotations

import argparse
import fcntl
import os
from pathlib import Path
import signal
import sys
import time

from mccain_capital import create_app, runtime_role


DATA_DIR = Path(os.environ.get("PERSISTENT_DATA_DIR") or "/data")
LOCK_PATH = Path(os.environ.get("MCCAIN_WORKER_LOCK_PATH") or DATA_DIR / ".worker.lock")
HEARTBEAT_PATH = Path(
    os.environ.get("MCCAIN_WORKER_HEARTBEAT_PATH") or "/tmp/mccain-worker-heartbeat"
)
HEARTBEAT_SECONDS = max(5, int(os.environ.get("MCCAIN_WORKER_HEARTBEAT_SECONDS") or "10"))
MAX_HEARTBEAT_AGE_SECONDS = max(
    HEARTBEAT_SECONDS * 3,
    int(os.environ.get("MCCAIN_WORKER_MAX_HEARTBEAT_AGE_SECONDS") or "45"),
)


def heartbeat_is_current(*, now: float | None = None) -> bool:
    try:
        age = (now if now is not None else time.time()) - HEARTBEAT_PATH.stat().st_mtime
    except OSError:
        return False
    return 0 <= age <= MAX_HEARTBEAT_AGE_SECONDS


def _acquire_owner_lock():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    handle = LOCK_PATH.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise RuntimeError("another McCain Capital worker already owns the runtime lock") from None
    handle.seek(0)
    handle.truncate()
    handle.write(f"pid={os.getpid()}\n")
    handle.flush()
    return handle


def run() -> int:
    if runtime_role() != "worker":
        raise RuntimeError("worker entrypoint requires MCCAIN_RUNTIME_ROLE=worker")

    owner_lock = _acquire_owner_lock()
    app = create_app()

    from mccain_capital.services import trades_sync
    from mccain_capital.services.market_pulse_setup_monitor_runtime import (
        start_server_setup_monitor_once,
    )

    trades_sync.prepare_sync_runtime_state()
    trades_sync.ensure_auto_sync_worker_started(app)
    start_server_setup_monitor_once(app)

    stopping = False

    def _stop(_signum, _frame) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    try:
        while not stopping:
            HEARTBEAT_PATH.touch()
            time.sleep(HEARTBEAT_SECONDS)
    finally:
        HEARTBEAT_PATH.unlink(missing_ok=True)
        owner_lock.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="McCain Capital background worker")
    parser.add_argument("--check", action="store_true", help="check worker heartbeat freshness")
    args = parser.parse_args(argv)
    if args.check:
        return 0 if heartbeat_is_current() else 1
    try:
        return run()
    except RuntimeError as exc:
        print(f"[mccain-worker] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
