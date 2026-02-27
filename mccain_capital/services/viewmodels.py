"""Shared typed UI viewmodels for page rendering."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from mccain_capital.runtime import money


@dataclass(frozen=True)
class DataTrustViewModel:
    status_label: str
    stage_label: str
    updated_label: str
    tone: str
    message: str
    primary_href: Optional[str] = None
    primary_label: Optional[str] = None
    secondary_href: Optional[str] = None
    secondary_label: Optional[str] = None


def _status_line(
    raw_status: str, raw_stage: str, raw_updated: str, unknown_status: str = "unknown"
) -> tuple[str, str, str]:
    status_label = (raw_status or unknown_status).replace("_", " ").title()
    stage_label = raw_stage.replace("_", " ").title() if raw_stage else ""
    updated_label = raw_updated or ""
    return status_label, stage_label, updated_label


def dashboard_data_trust(sync_status: Mapping[str, Any], balance_integrity: Mapping[str, Any]) -> DataTrustViewModel:
    status_label, stage_label, updated_label = _status_line(
        str(sync_status.get("last_sync_status") or ""),
        str(sync_status.get("last_sync_stage") or ""),
        str(sync_status.get("last_sync_updated_human") or ""),
    )
    sync_state = str(sync_status.get("last_sync_status") or "").strip().lower()
    has_drift = bool(balance_integrity.get("has_drift"))
    drift_delta = float(balance_integrity.get("delta") or 0.0)
    if has_drift:
        return DataTrustViewModel(
            status_label=status_label,
            stage_label=stage_label,
            updated_label=updated_label,
            tone="critical",
            message=f"Ledger drift detected {money(drift_delta)} vs stored row balance.",
            primary_href="/trades/upload/statement?ws=reconcile",
            primary_label="🧮 Open Reconcile Workspace",
            secondary_href="/ops/alerts",
            secondary_label="🚨 View Ops Alerts",
        )
    if sync_state in {"failed", "error", "blocked"}:
        return DataTrustViewModel(
            status_label=status_label,
            stage_label=stage_label,
            updated_label=updated_label,
            tone="critical",
            message="Sync reported a failure or block. Review diagnostics before next import.",
            primary_href="/trades/upload/statement?ws=live",
            primary_label="🤖 Open Live Sync",
            secondary_href="/ops/alerts",
            secondary_label="🚨 View Ops Alerts",
        )
    return DataTrustViewModel(
        status_label=status_label,
        stage_label=stage_label,
        updated_label=updated_label,
        tone="healthy",
        message="Ledger and sync look healthy. Continue normal workflow.",
    )


def trades_data_trust(sync_status: Mapping[str, Any], *, guardrail_locked: bool, active_day: str) -> DataTrustViewModel:
    status_label, stage_label, updated_label = _status_line(
        str(sync_status.get("status") or ""),
        str(sync_status.get("stage") or ""),
        str(sync_status.get("updated_at_human") or ""),
    )
    sync_state = str(sync_status.get("status") or "").strip().lower()
    if guardrail_locked:
        return DataTrustViewModel(
            status_label=status_label,
            stage_label=stage_label,
            updated_label=updated_label,
            tone="critical",
            message=f"Guardrail is locked for {active_day}. New risk should pause until controls are reviewed.",
            primary_href="/trades/risk-controls",
            primary_label="⚙️ Review Risk Controls",
            secondary_href="/analytics?tab=performance",
            secondary_label="📈 Analyze Day",
        )
    if sync_state in {"failed", "error", "blocked"}:
        return DataTrustViewModel(
            status_label=status_label,
            stage_label=stage_label,
            updated_label=updated_label,
            tone="critical",
            message="Latest sync/import reported a failure or block. Fix source before adding more trades.",
            primary_href="/trades/upload/statement?ws=live",
            primary_label="🤖 Open Live Sync",
            secondary_href="/trades/upload/statement?ws=reconcile",
            secondary_label="🧮 Reconcile Workspace",
        )
    return DataTrustViewModel(
        status_label=status_label,
        stage_label=stage_label,
        updated_label=updated_label,
        tone="healthy",
        message="Sync and guardrails look stable. Continue logging and review tags for clean analytics.",
    )


def analytics_data_trust(sync_status: Mapping[str, Any], *, integrity_issue_count: int) -> DataTrustViewModel:
    status_label, stage_label, updated_label = _status_line(
        str(sync_status.get("last_sync_status") or ""),
        str(sync_status.get("last_sync_stage") or ""),
        str(sync_status.get("last_sync_updated_human") or ""),
    )
    sync_state = str(sync_status.get("last_sync_status") or "").strip().lower()
    if int(integrity_issue_count or 0) > 0:
        return DataTrustViewModel(
            status_label=status_label,
            stage_label=stage_label,
            updated_label=updated_label,
            tone="critical",
            message=f"{int(integrity_issue_count)} integrity flags in current analytics range.",
            primary_href="/analytics?tab=diagnostics",
            primary_label="🧪 Open Diagnostics",
            secondary_href="/trades/upload/statement?ws=reconcile",
            secondary_label="🧮 Reconcile Imports",
        )
    if sync_state in {"failed", "error", "blocked"}:
        return DataTrustViewModel(
            status_label=status_label,
            stage_label=stage_label,
            updated_label=updated_label,
            tone="critical",
            message="Sync reliability is degraded. Validate source import before drawing performance conclusions.",
            primary_href="/trades/upload/statement?ws=live",
            primary_label="🤖 Open Live Sync",
            secondary_href="/ops/alerts",
            secondary_label="🚨 View Ops Alerts",
        )
    return DataTrustViewModel(
        status_label=status_label,
        stage_label=stage_label,
        updated_label=updated_label,
        tone="healthy",
        message="Data quality checks are clean for this range. Safe to use analytics for decisioning.",
    )
