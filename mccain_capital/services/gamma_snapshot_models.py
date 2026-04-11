"""Strict schema models for Market Pulse gamma snapshots.

These models validate the internal snapshot contract before values are cached or
rendered. The route/template layer can still consume compatibility aliases, but
the validated internal shape stays explicit and narrowly typed.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
import math
import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import ValidationError
from pydantic import field_validator
from pydantic import model_validator


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class SnapshotStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    STALE = "stale"
    INVALID = "invalid"


class RefreshMode(str, Enum):
    IN_PROCESS = "in_process"
    EXTERNAL = "external"


class GammaDiagnosticStatus(str, Enum):
    WAITING = "waiting"
    OK = "ok"
    ERROR = "error"
    STALE = "stale"
    INVALID = "invalid"


class GammaWarningState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    warnings: List[str] = Field(default_factory=list)
    stale_flags: List[str] = Field(default_factory=list)
    snapshot_status: SnapshotStatus
    snapshot_status_label: str
    snapshot_status_detail: str

    @field_validator("warnings", "stale_flags")
    @classmethod
    def _clean_list(cls, value: List[str]) -> List[str]:
        return [str(item).strip() for item in value if str(item).strip()]


class GammaSourceMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requested_expiries: List[str]
    included_expiries: List[str] = Field(default_factory=list)
    missing_expiries: List[str] = Field(default_factory=list)
    source_file_path: str = ""
    source_fetch_timestamp: str
    source_effective_timestamp: str
    source_effective_timestamp_source: Literal["exchange_native", "fetch_fallback", "unknown"]
    source_effective_timestamp_note: str = ""
    exchange_timestamp_available: bool = False

    @field_validator(
        "source_fetch_timestamp",
        "source_effective_timestamp",
    )
    @classmethod
    def _validate_timestamp(cls, value: str) -> str:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.isoformat()

    @field_validator("requested_expiries", "included_expiries", "missing_expiries")
    @classmethod
    def _validate_expiries(cls, value: List[str]) -> List[str]:
        clean = [str(item).strip() for item in value if str(item).strip()]
        for expiry in clean:
            if not _DATE_RE.match(expiry):
                raise ValueError(f"Invalid expiry {expiry!r}")
        return clean

    @model_validator(mode="after")
    def _validate_membership(self) -> "GammaSourceMetadata":
        requested = set(self.requested_expiries)
        if not requested:
            raise ValueError("requested_expiries must not be empty")
        if len(self.requested_expiries) > 2:
            raise ValueError("requested_expiries may not exceed 2 entries")
        if not set(self.included_expiries).issubset(requested):
            raise ValueError("included_expiries must be a subset of requested_expiries")
        if not set(self.missing_expiries).issubset(requested):
            raise ValueError("missing_expiries must be a subset of requested_expiries")
        return self


class GammaValidationResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total_rows_before_filter: int
    rows_dropped: int
    duplicate_raw_records: int
    nan_gamma_rows: int
    zero_open_interest_rows: int
    available_expiries: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    stale_flags: List[str] = Field(default_factory=list)

    @field_validator(
        "total_rows_before_filter",
        "rows_dropped",
        "duplicate_raw_records",
        "nan_gamma_rows",
        "zero_open_interest_rows",
    )
    @classmethod
    def _non_negative_int(cls, value: int) -> int:
        parsed = int(value)
        if parsed < 0:
            raise ValueError("count fields must be non-negative")
        return parsed

    @field_validator("available_expiries")
    @classmethod
    def _validate_available_expiries(cls, value: List[str]) -> List[str]:
        clean = [str(item).strip() for item in value if str(item).strip()]
        for expiry in clean:
            if not _DATE_RE.match(expiry):
                raise ValueError(f"Invalid expiry {expiry!r}")
        return clean

    @field_validator("warnings", "stale_flags")
    @classmethod
    def _clean_messages(cls, value: List[str]) -> List[str]:
        return [str(item).strip() for item in value if str(item).strip()]


class GroupedStrikeRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strike: float
    call_oi: float = 0.0
    put_oi: float = 0.0
    call_gex: float = 0.0
    put_gex: float = 0.0
    call_side_gex: float = 0.0
    put_side_gex: float = 0.0
    net_gex: float = 0.0
    gex: float = 0.0
    vex: float = 0.0
    dex: float = 0.0

    @field_validator(
        "strike",
        "call_oi",
        "put_oi",
        "call_gex",
        "put_gex",
        "call_side_gex",
        "put_side_gex",
        "net_gex",
        "gex",
        "vex",
        "dex",
    )
    @classmethod
    def _finite_number(cls, value: float) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("Grouped strike metrics must be finite")
        return parsed


class GammaSnapshotDiagnostics(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: GammaDiagnosticStatus
    contracts_seen: int = 0
    contracts_used: int = 0
    duplicate_raw_records: int = 0
    nan_gamma_rows: int = 0
    zero_open_interest_rows: int = 0
    rows_dropped: int = 0
    expirations: List[str] = Field(default_factory=list)
    refresh_ms: int = 0
    error: str = ""

    @field_validator(
        "contracts_seen",
        "contracts_used",
        "duplicate_raw_records",
        "nan_gamma_rows",
        "zero_open_interest_rows",
        "rows_dropped",
        "refresh_ms",
    )
    @classmethod
    def _non_negative_counts(cls, value: int) -> int:
        parsed = int(value)
        if parsed < 0:
            raise ValueError("diagnostic counts must be non-negative")
        return parsed

    @field_validator("expirations")
    @classmethod
    def _clean_expirations(cls, value: List[str]) -> List[str]:
        clean = [str(item).strip() for item in value if str(item).strip()]
        for expiry in clean:
            if not _DATE_RE.match(expiry):
                raise ValueError(f"Invalid expiry {expiry!r}")
        return clean


class GammaSnapshotNarrative(BaseModel):
    model_config = ConfigDict(extra="forbid")

    what_matters: str = ""
    trader_live_quote: str = ""
    trade_bias: str = ""
    environment_note: str = ""
    auto_read: List[str] = Field(default_factory=list)
    warning_badges: List[str] = Field(default_factory=list)

    @field_validator("auto_read", "warning_badges")
    @classmethod
    def _clean_strings(cls, value: List[str]) -> List[str]:
        return [str(item).strip() for item in value if str(item).strip()]


class GammaSnapshotPaths(BaseModel):
    model_config = ConfigDict(extra="forbid")

    csv: str = ""
    png: str = ""


class MarketPulseSnapshotModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    asof: str
    computed_at: str
    last_successful_compute: str
    source_metadata: GammaSourceMetadata
    warning_state: GammaWarningState
    spot: Optional[float] = None
    spot_price_used: Optional[float] = None
    spot_source: str = ""
    spot_source_label: str = ""
    spot_source_name: str = ""
    spot_source_timestamp: str = ""
    spot_is_fallback: bool = False
    fallback_reason: str = ""
    build_confidence: Literal["high", "reduced", "invalid"] = "invalid"
    build_status: Literal["live_valid", "fallback_valid", "invalid"] = "invalid"
    session_mode: str = ""
    contract_multiplier: int
    total_rows_before_filter: int
    total_rows_after_filter: int
    total_unique_strikes: int
    regime: str
    net_gex: float
    net_gex_total: float
    net_gamma_label: str
    gamma_flip_combined_basket: Optional[float] = None
    local_flip_aggregated_gamma: Optional[float] = None
    local_flip_found: bool = False
    local_flip_distance_from_spot: Optional[float] = None
    local_flip_window_used: Dict[str, Any] = Field(default_factory=dict)
    local_flip_expiries_used: List[str] = Field(default_factory=list)
    call_wall_aggregated_gamma: Optional[float] = None
    put_wall_aggregated_gamma: Optional[float] = None
    call_wall_gamma_per_point: Optional[float] = None
    put_wall_gamma_per_point: Optional[float] = None
    gamma_walls_top3: List[float] = Field(default_factory=list)
    top_positive_strikes: List[float] = Field(default_factory=list)
    top_negative_strikes: List[float] = Field(default_factory=list)
    top_3_positive_gamma_strikes: List[float] = Field(default_factory=list)
    top_3_negative_gamma_strikes: List[float] = Field(default_factory=list)
    nearest_positive_gamma_above_spot: Optional[float] = None
    nearest_negative_gamma_below_spot: Optional[float] = None
    gamma_range_estimate: Optional[float] = None
    gamma_range_high: Optional[float] = None
    gamma_range_low: Optional[float] = None
    next_call_wall_above: Optional[float] = None
    next_put_wall_below: Optional[float] = None
    void_zone: Dict[str, Optional[float]] = Field(default_factory=lambda: {"start": None, "end": None})
    bias: str
    cache_status: str
    refresh_mode: RefreshMode = RefreshMode.IN_PROCESS
    field_labels: Dict[str, str] = Field(default_factory=dict)
    paths: GammaSnapshotPaths = Field(default_factory=GammaSnapshotPaths)
    diagnostics: GammaSnapshotDiagnostics
    narrative: GammaSnapshotNarrative = Field(default_factory=GammaSnapshotNarrative)
    chart_json: Dict[str, Any] = Field(default_factory=dict)
    grouped_strike_rows: List[GroupedStrikeRow] = Field(default_factory=list)
    validation_result: GammaValidationResult

    @field_validator(
        "asof",
        "computed_at",
        "last_successful_compute",
        "spot_source_timestamp",
    )
    @classmethod
    def _validate_optional_timestamp(cls, value: str) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("timestamp fields must not be empty")
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.isoformat()

    @field_validator(
        "spot",
        "spot_price_used",
        "net_gex",
        "net_gex_total",
        "gamma_flip_combined_basket",
        "local_flip_aggregated_gamma",
        "local_flip_distance_from_spot",
        "call_wall_aggregated_gamma",
        "put_wall_aggregated_gamma",
        "call_wall_gamma_per_point",
        "put_wall_gamma_per_point",
        "nearest_positive_gamma_above_spot",
        "nearest_negative_gamma_below_spot",
        "gamma_range_estimate",
        "gamma_range_high",
        "gamma_range_low",
        "next_call_wall_above",
        "next_put_wall_below",
    )
    @classmethod
    def _finite_optional_float(cls, value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("critical numeric fields must be finite")
        return parsed

    @field_validator(
        "gamma_walls_top3",
        "top_positive_strikes",
        "top_negative_strikes",
        "top_3_positive_gamma_strikes",
        "top_3_negative_gamma_strikes",
    )
    @classmethod
    def _finite_float_list(cls, value: List[float]) -> List[float]:
        clean: List[float] = []
        for item in value:
            parsed = float(item)
            if not math.isfinite(parsed):
                raise ValueError("strike lists must contain only finite values")
            clean.append(parsed)
        return clean

    @model_validator(mode="after")
    def _validate_status_contract(self) -> "MarketPulseSnapshotModel":
        status = self.warning_state.snapshot_status
        requested_count = len(self.source_metadata.requested_expiries)
        included_count = len(self.source_metadata.included_expiries)
        if requested_count == 0:
            raise ValueError("snapshot must carry requested expiries")
        if status == SnapshotStatus.HEALTHY and included_count != requested_count:
            raise ValueError("healthy snapshot requires all requested expiries")
        if status == SnapshotStatus.DEGRADED and included_count >= requested_count:
            raise ValueError("degraded snapshot requires partial expiry coverage")
        if status == SnapshotStatus.DEGRADED and included_count <= 0:
            raise ValueError("degraded snapshot requires at least one included expiry")
        if status == SnapshotStatus.INVALID:
            if any(
                value is not None
                for value in (
                    self.gamma_flip_combined_basket,
                    self.local_flip_aggregated_gamma,
                    self.call_wall_aggregated_gamma,
                    self.put_wall_aggregated_gamma,
                )
            ):
                raise ValueError("invalid snapshot must not expose trusted gamma levels")
        else:
            if self.total_rows_after_filter <= 0:
                raise ValueError("non-invalid snapshot requires selected rows")
            if self.total_unique_strikes <= 0:
                raise ValueError("non-invalid snapshot requires grouped strikes")
        if self.local_flip_found and self.local_flip_aggregated_gamma is None:
            raise ValueError("local_flip_found requires local_flip_aggregated_gamma")
        if not self.local_flip_found and self.local_flip_distance_from_spot is not None:
            raise ValueError("local flip distance requires a discovered local flip")
        return self


def validate_market_pulse_snapshot(payload: Dict[str, Any]) -> MarketPulseSnapshotModel:
    return MarketPulseSnapshotModel.model_validate(payload)


def coerce_validated_snapshot(payload: Dict[str, Any]) -> Dict[str, Any]:
    return validate_market_pulse_snapshot(payload).model_dump(mode="json")


__all__ = [
    "GammaSourceMetadata",
    "GammaValidationResult",
    "GammaWarningState",
    "GammaSnapshotDiagnostics",
    "GammaSnapshotNarrative",
    "GammaSnapshotPaths",
    "GroupedStrikeRow",
    "MarketPulseSnapshotModel",
    "RefreshMode",
    "SnapshotStatus",
    "ValidationError",
    "coerce_validated_snapshot",
    "validate_market_pulse_snapshot",
]
