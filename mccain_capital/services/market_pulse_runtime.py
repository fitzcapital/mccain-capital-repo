"""Market Pulse refresh orchestration boundary.

This keeps the web layer from owning refresh orchestration details directly.
Development continues to use a simple in-process coordinator. Production can
switch to an external worker mode later without changing route logic.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Protocol

from mccain_capital.services.gamma_snapshot_models import RefreshMode


class MarketPulseRuntimeCoordinator(Protocol):
    mode: RefreshMode

    def ensure_started(self) -> None:
        """Start any local worker loops needed by the Market Pulse page."""

    def refresh_now(self, *, force_gamma: bool = False) -> Dict[str, Any]:
        """Refresh any synchronous dependencies exposed to the page."""


class InProcessMarketPulseRuntimeCoordinator:
    """Local/dev coordinator that keeps refresh loops inside the web process."""

    mode = RefreshMode.IN_PROCESS

    def ensure_started(self) -> None:
        from mccain_capital.services import gamma_map_service
        from mccain_capital.services import market_worker
        from mccain_capital.services import options_panel_service

        market_worker.start_market_worker_once()
        options_panel_service.start_options_worker_once()
        gamma_map_service.start_gamma_worker_once()

    def refresh_now(self, *, force_gamma: bool = False) -> Dict[str, Any]:
        from mccain_capital.services import gamma_map_service
        from mccain_capital.services import options_panel_service

        self.ensure_started()
        payload: Dict[str, Any] = {
            "gamma_snapshot": gamma_map_service.get_gamma_snapshot(),
            "options_snapshot": options_panel_service.get_options_snapshot(),
        }
        if force_gamma:
            try:
                payload["gamma_snapshot"] = gamma_map_service.run_gamma_refresh_once()
            except Exception:
                payload["gamma_snapshot"] = gamma_map_service.get_gamma_snapshot()
        return payload


class ExternalWorkerMarketPulseRuntimeCoordinator:
    """Production-ready boundary for future externalized refresh workers.

    Today this is intentionally a no-op adapter. It lets the web tier read the
    latest cached snapshots without owning worker startup. A queue/worker system
    can later implement this same interface.
    """

    mode = RefreshMode.EXTERNAL

    def ensure_started(self) -> None:
        return

    def refresh_now(self, *, force_gamma: bool = False) -> Dict[str, Any]:
        from mccain_capital.services import gamma_map_service
        from mccain_capital.services import options_panel_service

        return {
            "gamma_snapshot": gamma_map_service.get_gamma_snapshot(),
            "options_snapshot": options_panel_service.get_options_snapshot(),
        }


def get_market_pulse_runtime_coordinator() -> MarketPulseRuntimeCoordinator:
    mode = str(os.environ.get("MARKET_PULSE_REFRESH_MODE") or "in_process").strip().lower()
    if mode == RefreshMode.EXTERNAL.value:
        return ExternalWorkerMarketPulseRuntimeCoordinator()
    return InProcessMarketPulseRuntimeCoordinator()


def ensure_market_pulse_runtime_started() -> RefreshMode:
    coordinator = get_market_pulse_runtime_coordinator()
    coordinator.ensure_started()
    return coordinator.mode


def refresh_market_pulse_runtime(*, force_gamma: bool = False) -> Dict[str, Any]:
    coordinator = get_market_pulse_runtime_coordinator()
    payload = coordinator.refresh_now(force_gamma=force_gamma)
    payload["refresh_mode"] = coordinator.mode.value
    return payload
