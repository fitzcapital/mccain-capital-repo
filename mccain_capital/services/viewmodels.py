"""Shared typed UI viewmodels for page rendering."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Mapping, Optional

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


@dataclass(frozen=True)
class StateBadgeViewModel:
    label: str
    value: str
    tone: str = "neutral"
    title: str = ""


def _status_line(
    raw_status: str, raw_stage: str, raw_updated: str, unknown_status: str = "unknown"
) -> tuple[str, str, str]:
    status_label = (raw_status or unknown_status).replace("_", " ").title()
    stage_label = raw_stage.replace("_", " ").title() if raw_stage else ""
    updated_label = raw_updated or ""
    return status_label, stage_label, updated_label


def _tone_for_status(raw_status: str) -> str:
    state = (raw_status or "").strip().lower()
    if state in {"success", "succeeded", "ok", "healthy", "debug_only"}:
        return "healthy"
    if state in {"failed", "error", "blocked"}:
        return "critical"
    if state in {"running", "queued", "pending", "started", "in_progress"}:
        return "caution"
    return "neutral"


def balance_state_badges(balance_integrity: Mapping[str, Any]) -> List[StateBadgeViewModel]:
    canonical = float(balance_integrity.get("canonical_balance") or 0.0)
    starting = float(balance_integrity.get("starting_balance") or 0.0)
    stored = balance_integrity.get("stored_balance")
    has_drift = bool(balance_integrity.get("has_drift"))
    delta = float(balance_integrity.get("delta") or 0.0)
    stored_value = "No snapshot"
    stored_tone = "neutral"
    stored_title = "No stored per-trade balance snapshot is available yet."
    if stored is not None:
        stored_value = "Drift " + money(delta) if has_drift else "In sync"
        stored_tone = "critical" if has_drift else "healthy"
        stored_title = (
            f"Stored row balance {'lags' if has_drift else 'matches'} the derived ledger."
        )
    return [
        StateBadgeViewModel(
            label="Source",
            value=str(balance_integrity.get("source_label") or "Derived ledger"),
            tone="healthy",
            title=str(balance_integrity.get("source_detail") or ""),
        ),
        StateBadgeViewModel(
            label="Start",
            value=money(starting),
            tone="neutral",
            title="Configured ledger starting balance.",
        ),
        StateBadgeViewModel(
            label="Now",
            value=money(canonical),
            tone="healthy",
            title="Canonical balance used across the app.",
        ),
        StateBadgeViewModel(
            label="Stored",
            value=stored_value,
            tone=stored_tone,
            title=stored_title,
        ),
    ]


def sync_state_badges(
    sync_status: Mapping[str, Any],
    *,
    status_key: str,
    stage_key: str,
    updated_key: str,
) -> List[StateBadgeViewModel]:
    raw_status = str(sync_status.get(status_key) or "")
    raw_stage = str(sync_status.get(stage_key) or "")
    raw_updated = str(sync_status.get(updated_key) or "")
    status_label, stage_label, updated_label = _status_line(raw_status, raw_stage, raw_updated)
    return [
        StateBadgeViewModel(
            label="Sync",
            value=status_label,
            tone=_tone_for_status(raw_status),
            title="Latest sync or import state.",
        ),
        StateBadgeViewModel(
            label="Stage",
            value=stage_label or "Idle",
            tone="neutral",
            title="Most recent sync step reached.",
        ),
        StateBadgeViewModel(
            label="Updated",
            value=updated_label or "No run",
            tone="neutral",
            title="Last recorded sync timestamp.",
        ),
    ]


def backup_state_badges(
    cfg: Mapping[str, Any], audit_rows: List[Mapping[str, Any]]
) -> List[StateBadgeViewModel]:
    last_backup_status = str(cfg.get("last_status") or "").strip()
    last_backup_label = (
        last_backup_status.replace("_", " ").title() if last_backup_status else "Never"
    )
    last_restore = next(
        (row for row in audit_rows if "restore" in str(row.get("label") or "").strip().lower()),
        None,
    )
    last_restore_label = "None yet"
    last_restore_tone = "neutral"
    last_restore_title = "No restore action recorded in the current activity window."
    if last_restore:
        restore_event = str(last_restore.get("label") or "restore").replace("_", " ").title()
        restore_at = str(last_restore.get("at_human") or "").strip()
        last_restore_label = restore_event if not restore_at else f"{restore_event} · {restore_at}"
        last_restore_tone = "caution"
        last_restore_title = str(
            last_restore.get("summary") or "Most recent restore-related action."
        )
    return [
        StateBadgeViewModel(
            label="Schedule",
            value="On" if bool(cfg.get("enabled")) else "Off",
            tone="healthy" if bool(cfg.get("enabled")) else "caution",
            title="Auto backup schedule state.",
        ),
        StateBadgeViewModel(
            label="Last Backup",
            value=last_backup_label,
            tone=_tone_for_status(last_backup_status),
            title=str(cfg.get("last_message") or "Most recent backup result."),
        ),
        StateBadgeViewModel(
            label="Last Restore",
            value=last_restore_label,
            tone=last_restore_tone,
            title=last_restore_title,
        ),
    ]


def dashboard_data_trust(
    sync_status: Mapping[str, Any], balance_integrity: Mapping[str, Any]
) -> DataTrustViewModel:
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


def trades_data_trust(
    sync_status: Mapping[str, Any], *, guardrail_locked: bool, active_day: str
) -> DataTrustViewModel:
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


def analytics_data_trust(
    sync_status: Mapping[str, Any], *, integrity_issue_count: int
) -> DataTrustViewModel:
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
