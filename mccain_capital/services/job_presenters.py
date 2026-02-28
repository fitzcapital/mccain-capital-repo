"""Presentation helpers for background job payloads."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from flask import render_template


def build_action_result_summary(
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


def render_action_result_summary(summary: Dict[str, Any]) -> str:
    return render_template("partials/action_result_summary.html", summary=summary)


def job_response_payload(
    job: Dict[str, Any],
    *,
    humanize_timestamp: Callable[[str], str],
) -> Dict[str, Any]:
    payload = dict(job or {})
    payload["created_at_human"] = humanize_timestamp(str(payload.get("created_at") or ""))
    payload["updated_at_human"] = humanize_timestamp(str(payload.get("updated_at") or ""))
    summary = payload.get("result_summary")
    if isinstance(summary, dict) and summary:
        payload["result_html"] = render_action_result_summary(summary)
    else:
        payload["result_html"] = ""
    return payload
