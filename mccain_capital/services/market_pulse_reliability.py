"""Durable canonical Market Pulse envelope helpers.

The app is local-only but runs multiple Gunicorn workers.  This module keeps
their last verified context monotonic without adding another infrastructure
dependency.
"""

from __future__ import annotations

import copy
from datetime import datetime
import fcntl
import json
import os
import tempfile
from typing import Any


REQUIRED_COMPONENTS = ("spot", "bars", "gamma")


def _parse_timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed


def validate_context_payload(payload: Any, *, ticker: str) -> tuple[bool, list[str]]:
    """Validate identity and coherence fields before sharing a context payload."""

    if not isinstance(payload, dict):
        return False, ["payload"]
    problems: list[str] = []
    expected = str(ticker or "SPX").upper()
    if str(payload.get("ticker") or "").upper() != expected:
        problems.append("symbol")
    freshness = payload.get("canonical_freshness")
    if not isinstance(freshness, dict):
        return False, problems + ["canonical_freshness"]
    if not str(freshness.get("generation_id") or ""):
        problems.append("generation")
    if str(freshness.get("symbol") or expected).upper() != expected:
        problems.append("symbol")
    if not str(freshness.get("session_id") or ""):
        problems.append("session")
    if _parse_timestamp(freshness.get("generated_at")) is None:
        problems.append("generated_at")
    components = freshness.get("components")
    if not isinstance(components, dict):
        problems.append("components")
    else:
        for name in REQUIRED_COMPONENTS:
            component = components.get(name)
            if not isinstance(component, dict) or not str(component.get("as_of") or ""):
                problems.append(name)
    return not problems, list(dict.fromkeys(problems))


def generation_sort_key(payload: Any) -> tuple[float, str]:
    freshness = payload.get("canonical_freshness") if isinstance(payload, dict) else {}
    generated = _parse_timestamp((freshness or {}).get("generated_at"))
    stamp = generated.timestamp() if generated is not None else float("-inf")
    return stamp, str((freshness or {}).get("generation_id") or "")


def load_shared_context(path: str, *, ticker: str) -> dict[str, Any] | None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            envelope = json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    payload = envelope.get("payload") if isinstance(envelope, dict) else None
    valid, _ = validate_context_payload(payload, ticker=ticker)
    return copy.deepcopy(payload) if valid else None


def promote_shared_context(path: str, payload: dict[str, Any], *, ticker: str) -> bool:
    """Atomically promote payload unless another worker already published newer data."""

    valid, _ = validate_context_payload(payload, ticker=ticker)
    if not valid:
        return False
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    lock_path = f"{path}.lock"
    with open(lock_path, "a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        current = load_shared_context(path, ticker=ticker)
        if current is not None and generation_sort_key(current) > generation_sort_key(payload):
            return False
        tmp_path = ""
        try:
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", dir=directory, prefix=".market-pulse-context.",
                suffix=".tmp", delete=False
            ) as handle:
                tmp_path = handle.name
                json.dump(
                    {"ticker": ticker, "payload": payload},
                    handle,
                    separators=(",", ":"),
                    default=str,
                )
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, path)
            return True
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass


def choose_newest_context(*payloads: Any, ticker: str) -> dict[str, Any] | None:
    valid_payloads = []
    for payload in payloads:
        valid, _ = validate_context_payload(payload, ticker=ticker)
        if valid:
            valid_payloads.append(payload)
    if not valid_payloads:
        return None
    return copy.deepcopy(max(valid_payloads, key=generation_sort_key))
