"""Shared typed UI viewmodels for page rendering."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Mapping, Optional

from mccain_capital.runtime import money


@dataclass(frozen=True)
class DataTrustViewModel:
    status_label: str
    stage_label: str
    updated_label: str
    tone: str
    message: str
    badges: List["StateBadgeViewModel"] = field(default_factory=list)
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


def _trust_badges(
    *,
    confidence: str,
    confidence_tone: str,
    source: str,
    source_tone: str,
    mode: str,
    mode_tone: str,
) -> List[StateBadgeViewModel]:
    return [
        StateBadgeViewModel(
            label="Confidence",
            value=confidence,
            tone=confidence_tone,
            title="How safe this surface is to trust right now.",
        ),
        StateBadgeViewModel(
            label="Source",
            value=source,
            tone=source_tone,
            title="Primary data or control system behind this read.",
        ),
        StateBadgeViewModel(
            label="Mode",
            value=mode,
            tone=mode_tone,
            title="The current recommended operating mode.",
        ),
    ]


def balance_state_badges(balance_integrity: Mapping[str, Any]) -> List[StateBadgeViewModel]:
    canonical = float(balance_integrity.get("canonical_balance") or 0.0)
    starting = float(balance_integrity.get("starting_balance") or 0.0)
    stored = balance_integrity.get("stored_balance")
    has_drift = bool(balance_integrity.get("has_drift"))
    delta = float(balance_integrity.get("delta") or 0.0)
    stored_value = "No snapshot"
    stored_tone = "neutral"
    stored_title = "No stored per-trade balance snapshot is available yet."
    stored_label = str(balance_integrity.get("stored_status_label") or "No snapshot")
    stored_tone = str(balance_integrity.get("stored_status_tone") or stored_tone)
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
            value=stored_value if stored is not None else stored_label,
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
            badges=_trust_badges(
                confidence="Compromised",
                confidence_tone="critical",
                source="Derived ledger",
                source_tone="neutral",
                mode="Reconcile now",
                mode_tone="critical",
            ),
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
            badges=_trust_badges(
                confidence="Degraded",
                confidence_tone="critical",
                source="Sync feed",
                source_tone="caution",
                mode="Review source",
                mode_tone="critical",
            ),
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
        badges=_trust_badges(
            confidence="High",
            confidence_tone="healthy",
            source="Ledger + sync",
            source_tone="healthy",
            mode="Normal workflow",
            mode_tone="healthy",
        ),
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
            badges=_trust_badges(
                confidence="Protected",
                confidence_tone="caution",
                source="Guardrail lock",
                source_tone="critical",
                mode="Review only",
                mode_tone="critical",
            ),
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
            badges=_trust_badges(
                confidence="Degraded",
                confidence_tone="critical",
                source="Import / sync",
                source_tone="caution",
                mode="Fix source",
                mode_tone="critical",
            ),
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
        badges=_trust_badges(
            confidence="Stable",
            confidence_tone="healthy",
            source="Execution + sync",
            source_tone="healthy",
            mode="Log and review",
            mode_tone="healthy",
        ),
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
            badges=_trust_badges(
                confidence="Compromised",
                confidence_tone="critical",
                source="Imported ledger",
                source_tone="neutral",
                mode="Repair first",
                mode_tone="critical",
            ),
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
            badges=_trust_badges(
                confidence="Mixed",
                confidence_tone="caution",
                source="Sync reliability",
                source_tone="caution",
                mode="Validate feed",
                mode_tone="critical",
            ),
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
        badges=_trust_badges(
            confidence="High",
            confidence_tone="healthy",
            source="Reviewed ledger",
            source_tone="healthy",
            mode="Read safely",
            mode_tone="healthy",
        ),
    )
