"""SPX gamma map engine (Tradier only)."""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
import json
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
import urllib.parse
import urllib.request

from mccain_capital import runtime as app_runtime
from mccain_capital.services import market_data_service

import numpy as np
import pandas as pd

try:
    import plotly.graph_objects as go
except Exception:  # pragma: no cover - fallback for offline test envs
    go = None

ACTIVE_POLL_SECONDS = 60
SHOULDER_POLL_SECONDS = 180
WEEKDAY_IDLE_POLL_SECONDS = 600
WEEKEND_POLL_SECONDS = 1800
MAX_SNAPSHOT_PAGES = 8
CSV_FILENAME = "gamma_data.csv"
PNG_FILENAME = "gamma_map.png"
DEFAULT_CONTRACT_MULTIPLIER = 100
DEFAULT_GEX_SCALER = 0.01
DEFAULT_GAMMA_REGIME_POSITIVE_THRESHOLD = 50_000_000.0
DEFAULT_GAMMA_REGIME_NEGATIVE_THRESHOLD = -50_000_000.0
SPOT_MISMATCH_POINTS_THRESHOLD = 5.0
SPOT_TIMESTAMP_DRIFT_SECONDS = 120
GAMMA_RANGE_LABEL = "Gamma Range Estimate (wall-based)"
EOD_GAMMA_NOTIFY_ENABLED = os.environ.get("EOD_GAMMA_NOTIFY_ENABLED", "1") == "1"
EOD_GAMMA_NOTIFY_TIME_ET = (os.environ.get("EOD_GAMMA_NOTIFY_TIME_ET") or "17:10").strip()
EOD_GAMMA_NOTIFY_END_ET = (os.environ.get("EOD_GAMMA_NOTIFY_END_ET") or "19:30").strip()

_LOCK = threading.Lock()
_STARTED = False
_CACHE: Dict[str, Any] = {
    "asof": "",
    "spot": None,
    "spot_price_used": None,
    "spot_source_name": "",
    "spot_source_timestamp": "",
    "source_timestamp": "",
    "computed_at": "",
    "last_successful_compute": "",
    "included_expiries": [],
    "requested_expiries": [],
    "contract_multiplier": DEFAULT_CONTRACT_MULTIPLIER,
    "total_rows_before_filter": 0,
    "total_rows_after_filter": 0,
    "total_unique_strikes": 0,
    "regime": "unavailable",
    "net_gex": 0.0,
    "net_gex_total": 0.0,
    "gamma_flip": None,
    "call_wall": None,
    "put_wall": None,
    "top_positive_strikes": [],
    "top_negative_strikes": [],
    "top_3_positive_gamma_strikes": [],
    "top_3_negative_gamma_strikes": [],
    "nearest_positive_gamma_above_spot": None,
    "nearest_negative_gamma_below_spot": None,
    "expected_move": None,
    "expected_move_high": None,
    "expected_move_low": None,
    "gamma_range_estimate": None,
    "gamma_range_high": None,
    "gamma_range_low": None,
    "next_call_wall_above": None,
    "next_put_wall_below": None,
    "call_wall_gamma_per_point": None,
    "put_wall_gamma_per_point": None,
    "gamma_walls_top3": [],
    "void_zone": {"start": None, "end": None},
    "bias": "insufficient_data",
    "warnings": [],
    "stale_flags": [],
    "cache_status": "cold",
    "source_file_path": "",
    "field_labels": {
        "spot": "Spot (raw market data)",
        "gamma_flip": "Gamma Flip (combined basket)",
        "call_wall": "Call Wall (aggregated strike gamma)",
        "put_wall": "Put Wall (aggregated strike gamma)",
        "net_gex": "Net GEX (selected basket)",
        "gamma_range_estimate": GAMMA_RANGE_LABEL,
    },
    "narrative": {
        "what_matters": "",
        "trader_live_quote": "",
        "trade_bias": "",
        "environment_note": "",
        "auto_read": [],
        "warning_badges": [],
    },
    "paths": {"csv": "", "png": ""},
    "diagnostics": {
        "status": "waiting",
        "contracts_seen": 0,
        "contracts_used": 0,
        "duplicate_raw_records": 0,
        "nan_gamma_rows": 0,
        "zero_open_interest_rows": 0,
        "rows_dropped": 0,
        "expirations": [],
        "refresh_ms": 0,
        "error": "",
    },
    "chart_json": {"gex": None, "vex": None},
}
_RUNTIME_STATE: Dict[str, Any] = {
    "last_attempted_at": "",
    "last_error": "",
    "last_refresh_ms": 0,
    "status": "waiting",
    "cache_status": "cold",
}

_COMPACT_TICKER = re.compile(r"^(?:O:)?(SPXW|SPX)(\d{2})(\d{2})(\d{2})([CP])(\d{8})$")


class _FallbackFigure:
    def __init__(self, title: str = "") -> None:
        self._title = title
        self._json = {"data": [], "layout": {"title": title}}

    def add_trace(self, *_args, **_kwargs) -> None:
        return

    def add_vline(self, *_args, **_kwargs) -> None:
        return

    def add_vrect(self, *_args, **_kwargs) -> None:
        return

    def update_layout(self, **kwargs) -> None:
        self._json["layout"].update(kwargs)

    def to_plotly_json(self) -> Dict[str, Any]:
        return self._json

    def write_image(self, _path: str, **_kwargs) -> None:
        raise RuntimeError("plotly/kaleido unavailable")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json_clone(value: Dict[str, Any]) -> Dict[str, Any]:
    return json.loads(json.dumps(value))


def _source_stale_threshold_seconds(now_et: Optional[datetime] = None) -> int:
    return max(300, int(_gamma_poll_seconds(now_et) * 2))


def _gamma_regime_thresholds() -> Tuple[float, float]:
    """Return the business thresholds for classifying the gamma regime.

    Values inside the band are treated as neutral:
    - positive if net_gex_total > positive_threshold
    - negative if net_gex_total < negative_threshold
    - neutral otherwise
    """

    positive_raw = (
        os.environ.get("GAMMA_REGIME_POSITIVE_THRESHOLD")
        or app_runtime.get_setting_value("gamma_regime_positive_threshold", "")
    )
    negative_raw = (
        os.environ.get("GAMMA_REGIME_NEGATIVE_THRESHOLD")
        or app_runtime.get_setting_value("gamma_regime_negative_threshold", "")
    )
    positive = float(_safe_float(positive_raw) or DEFAULT_GAMMA_REGIME_POSITIVE_THRESHOLD)
    negative = float(_safe_float(negative_raw) or DEFAULT_GAMMA_REGIME_NEGATIVE_THRESHOLD)
    if positive < 0:
        positive = abs(positive)
    if negative > 0:
        negative = -abs(negative)
    if negative >= positive:
        negative = -abs(positive)
    return (positive, negative)


def _parse_iso_timestamp(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=app_runtime.TZ)
        return dt.astimezone(app_runtime.TZ)
    except Exception:
        return None


def _gamma_notify_state_path() -> str:
    return app_runtime.upload_path(".gamma_notify_state.json")


def _load_gamma_notify_state() -> Dict[str, Any]:
    try:
        with open(_gamma_notify_state_path(), "r", encoding="utf-8") as fh:
            payload = json.load(fh)
            return payload if isinstance(payload, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _save_gamma_notify_state(payload: Dict[str, Any]) -> None:
    try:
        with open(_gamma_notify_state_path(), "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
    except OSError:
        return


def _parse_hhmm(raw: str, *, default: str) -> int:
    text = str(raw or "").strip() or default
    try:
        hour_text, minute_text = text.split(":", 1)
        hour = max(0, min(23, int(hour_text)))
        minute = max(0, min(59, int(minute_text)))
        return (hour * 60) + minute
    except Exception:
        hour_text, minute_text = default.split(":", 1)
        return (int(hour_text) * 60) + int(minute_text)


def _gamma_poll_seconds(now_et: Optional[datetime] = None) -> int:
    current = now_et.astimezone(app_runtime.TZ) if now_et else app_runtime.now_et()
    if current.weekday() >= 5:
        return WEEKEND_POLL_SECONDS

    minutes = current.hour * 60 + current.minute
    if 480 <= minutes <= 975:
        return ACTIVE_POLL_SECONDS
    if 360 <= minutes < 480 or 975 < minutes <= 1080:
        return SHOULDER_POLL_SECONDS
    return WEEKDAY_IDLE_POLL_SECONDS


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _safe_int(v: Any) -> int:
    try:
        return int(float(v or 0))
    except Exception:
        return 0


def _fmt_level(value: Any) -> str:
    level = _safe_float(value)
    if level is None:
        return "n/a"
    return f"{level:,.0f}"


def _massive_api_key() -> str:
    return (
        (os.environ.get("MASSIVE_API_KEY") or "").strip()
        or (os.environ.get("POLYGON_API_KEY") or "").strip()
        or str(app_runtime.get_setting_value("massive_api_key", "") or "").strip()
        or str(app_runtime.get_setting_value("polygon_api_key", "") or "").strip()
    )


def _tradier_api_key() -> str:
    return (os.environ.get("TRADIER_API_KEY") or "").strip() or str(
        app_runtime.get_setting_value("tradier_api_key", "") or ""
    ).strip()


def _tradier_json(path: str, params: Dict[str, Any]) -> Dict[str, Any] | None:
    key = _tradier_api_key()
    if not key:
        return None
    base = (os.environ.get("TRADIER_BASE_URL") or "https://api.tradier.com").strip().rstrip("/")
    url = base + path + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
            "User-Agent": "mccain-capital/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            parsed = json.loads(body)
            return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _massive_json(path: str, params: Dict[str, Any]) -> Dict[str, Any] | None:
    key = _massive_api_key()
    if not key:
        return None
    q = dict(params)
    q["apiKey"] = key
    url = "https://api.polygon.io" + path + "?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers={"User-Agent": "mccain-capital/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            parsed = json.loads(body)
            return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _parse_option_ticker(ticker: str) -> Dict[str, Any]:
    raw = str(ticker or "").strip().upper()
    m = _COMPACT_TICKER.match(raw)
    if not m:
        return {"root": "", "expiration": "", "cp": "", "strike": None}
    root = m.group(1)
    yy = int(m.group(2))
    mm = int(m.group(3))
    dd = int(m.group(4))
    cp = m.group(5)
    strike = int(m.group(6)) / 1000.0
    return {"root": root, "expiration": f"20{yy:02d}-{mm:02d}-{dd:02d}", "cp": cp, "strike": strike}


def _next_trading_day_iso(anchor_iso: str) -> str:
    return app_runtime.next_trading_day_iso(anchor_iso)


def fetch_spx_chain_for_expiries(expiries: List[str]) -> pd.DataFrame:
    tradier_first = _fetch_spx_chain_from_tradier({str(x) for x in expiries if str(x)})
    if not tradier_first.empty:
        tradier_first.attrs["contracts_seen"] = int(tradier_first.attrs.get("contracts_seen") or 0)
        return tradier_first
    return pd.DataFrame()


def _fetch_spx_chain_from_tradier(expiry_set: set[str]) -> pd.DataFrame:
    if not _tradier_api_key():
        return pd.DataFrame()
    rows: Dict[Tuple[str, float], Dict[str, Any]] = {}
    seen = 0
    for expiry in sorted(expiry_set):
        payload = _tradier_json(
            "/v1/markets/options/chains",
            {"symbol": "SPX", "expiration": expiry, "greeks": "true"},
        )
        options = ((payload or {}).get("options") or {}).get("option")
        if isinstance(options, dict):
            opts = [options]
        elif isinstance(options, list):
            opts = [o for o in options if isinstance(o, dict)]
        else:
            opts = []
        for item in opts:
            seen += 1
            expiration = str(item.get("expiration_date") or expiry)
            strike = _safe_float(item.get("strike"))
            if strike is None:
                continue
            cp_raw = str(item.get("option_type") or "").lower()
            cp = "call" if cp_raw.startswith("c") else "put" if cp_raw.startswith("p") else ""
            if cp not in {"call", "put"}:
                continue
            oi = float(_safe_float(item.get("open_interest")) or 0.0)
            greeks = item.get("greeks") if isinstance(item.get("greeks"), dict) else {}
            gamma = float(_safe_float(greeks.get("gamma")) or 0.0)
            vega = float(_safe_float(greeks.get("vega")) or 0.0)
            delta = float(_safe_float(greeks.get("delta")) or 0.0)

            key = (expiration, float(strike))
            bucket = rows.setdefault(
                key,
                {
                    "expiration": expiration,
                    "strike": float(strike),
                    "call_oi": 0.0,
                    "put_oi": 0.0,
                    "call_gamma": 0.0,
                    "put_gamma": 0.0,
                    "call_vega": 0.0,
                    "put_vega": 0.0,
                    "call_delta": 0.0,
                    "put_delta": 0.0,
                },
            )
            if cp == "call":
                bucket["call_oi"] = oi
                bucket["call_gamma"] = gamma
                bucket["call_vega"] = vega
                bucket["call_delta"] = delta
            else:
                bucket["put_oi"] = oi
                bucket["put_gamma"] = gamma
                bucket["put_vega"] = vega
                bucket["put_delta"] = abs(delta)
    df = pd.DataFrame(list(rows.values()))
    if not df.empty:
        df.attrs["contracts_seen"] = seen
    return df


def _parse_cboe_option_symbol(symbol: str) -> Dict[str, Any]:
    raw = str(symbol or "").strip().upper()
    m = re.match(r"^(SPXW?|SPX)(\d{2})(\d{2})(\d{2})([CP])(\d{8})$", raw)
    if not m:
        return {"root": "", "expiration": "", "cp": "", "strike": None}
    root = m.group(1)
    yy = int(m.group(2))
    mm = int(m.group(3))
    dd = int(m.group(4))
    cp = m.group(5)
    strike = int(m.group(6)) / 1000.0
    exp = f"20{yy:02d}-{mm:02d}-{dd:02d}"
    return {"root": root, "expiration": exp, "cp": cp, "strike": strike}


def _fetch_spx_chain_from_cboe(expiry_set: set[str]) -> pd.DataFrame:
    url = "https://cdn.cboe.com/api/global/delayed_quotes/options/_SPX.json"
    req = urllib.request.Request(url, headers={"User-Agent": "mccain-capital/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
    except Exception:
        return pd.DataFrame()

    options = (
        ((payload.get("data") or {}).get("options") or []) if isinstance(payload, dict) else []
    )
    rows: Dict[Tuple[str, float], Dict[str, Any]] = {}
    seen = 0
    for item in options:
        if not isinstance(item, dict):
            continue
        seen += 1
        parsed = _parse_cboe_option_symbol(str(item.get("option") or ""))
        expiration = str(parsed.get("expiration") or "")
        strike = _safe_float(parsed.get("strike"))
        cp = str(parsed.get("cp") or "")
        if expiration not in expiry_set or strike is None or cp not in {"C", "P"}:
            continue

        key = (expiration, float(strike))
        bucket = rows.setdefault(
            key,
            {
                "expiration": expiration,
                "strike": float(strike),
                "call_oi": 0.0,
                "put_oi": 0.0,
                "call_gamma": 0.0,
                "put_gamma": 0.0,
                "call_vega": 0.0,
                "put_vega": 0.0,
                "call_delta": 0.0,
                "put_delta": 0.0,
            },
        )

        oi = float(_safe_float(item.get("open_interest")) or 0.0)
        gamma = float(_safe_float(item.get("gamma")) or 0.0)
        vega = float(_safe_float(item.get("vega")) or 0.0)
        delta = float(_safe_float(item.get("delta")) or 0.0)
        if cp == "C":
            bucket["call_oi"] = oi
            bucket["call_gamma"] = gamma
            bucket["call_vega"] = vega
            bucket["call_delta"] = delta
        else:
            bucket["put_oi"] = oi
            bucket["put_gamma"] = gamma
            bucket["put_vega"] = vega
            bucket["put_delta"] = abs(delta)

    df = pd.DataFrame(list(rows.values()))
    if not df.empty:
        df.attrs["contracts_seen"] = seen
    return df


def load_gamma_source(expiries: Optional[List[str]] = None) -> Dict[str, Any]:
    basket = [str(x) for x in (expiries or []) if str(x).strip()]
    if not basket:
        today = app_runtime.today_iso()
        basket = [today, _next_trading_day_iso(today)]
    raw = fetch_spx_chain_for_expiries(basket)
    return {
        "raw": raw.copy(),
        "requested_expiries": basket,
        "source_timestamp": _now_iso(),
        "contracts_seen": int(raw.attrs.get("contracts_seen") or 0),
        "source_file_path": "",
    }


def validate_gamma_source(
    raw: pd.DataFrame,
    *,
    source_timestamp: str,
    spot_price: Optional[float],
) -> Dict[str, Any]:
    warnings: List[str] = []
    stale_flags: List[str] = []
    mandatory = [
        "strike",
        "expiration",
        "call_oi",
        "put_oi",
        "call_gamma",
        "put_gamma",
        "call_vega",
        "put_vega",
        "call_delta",
        "put_delta",
    ]
    if raw.empty:
        return {
            "rows": raw.copy(),
            "warnings": ["No gamma source rows were loaded."],
            "stale_flags": ["empty_source"],
            "total_rows_before_filter": 0,
            "rows_dropped": 0,
            "duplicate_raw_records": 0,
            "nan_gamma_rows": 0,
            "zero_open_interest_rows": 0,
            "available_expiries": [],
        }

    out = raw.copy()
    total_rows_before_filter = int(len(out.index))
    for col in mandatory:
        if col not in out.columns:
            out[col] = 0.0
            warnings.append(f"Missing source column {col}; defaulted to 0.")
            stale_flags.append(f"missing_column_{col}")

    duplicate_mask = out.duplicated(
        subset=mandatory,
        keep="first",
    )
    duplicate_raw_records = int(duplicate_mask.sum())
    if duplicate_raw_records:
        warnings.append(f"Dropped {duplicate_raw_records} duplicate raw record(s).")
        stale_flags.append("duplicate_raw_records")
        out = out.loc[~duplicate_mask].copy()

    rows_dropped = 0
    if "strike" in out.columns and "expiration" in out.columns:
        before_drop = int(len(out.index))
        out = out.dropna(subset=["strike", "expiration"]).copy()
        rows_dropped += max(0, before_drop - int(len(out.index)))

    nan_gamma_rows = 0
    zero_open_interest_rows = 0
    numeric_cols = [
        "strike",
        "call_oi",
        "put_oi",
        "call_gamma",
        "put_gamma",
        "call_vega",
        "put_vega",
        "call_delta",
        "put_delta",
    ]
    for col in numeric_cols:
        coerced = pd.to_numeric(out[col], errors="coerce")
        if col in {"call_gamma", "put_gamma"}:
            nan_gamma_rows += int((coerced.isna() & out[col].notna()).sum())
        out[col] = coerced.fillna(0.0)

    zero_open_interest_rows = int(((out["call_oi"] <= 0) & (out["put_oi"] <= 0)).sum())
    if nan_gamma_rows:
        warnings.append(f"Found {nan_gamma_rows} NaN gamma value(s); coerced to 0.")
        stale_flags.append("nan_gamma")
    if zero_open_interest_rows:
        warnings.append(f"Found {zero_open_interest_rows} zero-OI row(s).")
        stale_flags.append("zero_open_interest")
    if spot_price is None:
        warnings.append("Spot price was unavailable for gamma validation.")
        stale_flags.append("missing_spot_price")

    age_seconds = None
    try:
        source_dt = datetime.fromisoformat(source_timestamp.replace("Z", "+00:00")).astimezone(
            app_runtime.TZ
        )
        age_seconds = max(0, int((app_runtime.now_et() - source_dt).total_seconds()))
    except Exception:
        warnings.append("Source timestamp could not be parsed.")
        stale_flags.append("source_timestamp_invalid")
    if age_seconds is not None and age_seconds > _source_stale_threshold_seconds():
        warnings.append("Gamma source timestamp is stale.")
        stale_flags.append("stale_gamma_source")

    available_expiries = sorted(
        {
            str(expiry).strip()
            for expiry in out["expiration"].tolist()
            if str(expiry).strip()
        }
    )
    return {
        "rows": out.sort_values(["expiration", "strike"]).reset_index(drop=True),
        "warnings": warnings,
        "stale_flags": stale_flags,
        "total_rows_before_filter": total_rows_before_filter,
        "rows_dropped": int(rows_dropped),
        "duplicate_raw_records": duplicate_raw_records,
        "nan_gamma_rows": nan_gamma_rows,
        "zero_open_interest_rows": zero_open_interest_rows,
        "available_expiries": available_expiries,
    }


def select_expiry_basket(
    raw: pd.DataFrame,
    *,
    requested_expiries: Optional[List[str]] = None,
) -> Dict[str, Any]:
    basket = [str(x) for x in (requested_expiries or []) if str(x).strip()]
    if not basket:
        today = app_runtime.today_iso()
        basket = [today, _next_trading_day_iso(today)]
    available = {
        str(expiry).strip()
        for expiry in raw.get("expiration", pd.Series(dtype=str)).tolist()
        if str(expiry).strip()
    }
    included_expiries = [expiry for expiry in basket if expiry in available]
    missing_expiries = [expiry for expiry in basket if expiry not in available]
    filtered = raw[raw["expiration"].astype(str).isin(basket)].copy() if not raw.empty else raw.copy()
    warnings: List[str] = []
    stale_flags: List[str] = []
    if missing_expiries:
        warnings.append(f"Missing expiry data for {', '.join(missing_expiries)}.")
        stale_flags.append("missing_expiries")
    if filtered.empty:
        warnings.append("Selected expiry basket produced no valid rows.")
        stale_flags.append("empty_grouped_results")
    if not filtered.empty and {"expiration", "strike"}.issubset(filtered.columns):
        filtered = filtered.sort_values(["expiration", "strike"]).reset_index(drop=True)
    return {
        "rows": filtered,
        "requested_expiries": basket,
        "included_expiries": included_expiries,
        "missing_expiries": missing_expiries,
        "warnings": warnings,
        "stale_flags": stale_flags,
    }


def compute_row_gex(
    gamma: Any,
    open_interest: Any,
    spot_price: Any,
    option_type: str,
    *,
    contract_multiplier: int = DEFAULT_CONTRACT_MULTIPLIER,
    scaler: float = DEFAULT_GEX_SCALER,
) -> float:
    gamma_value = float(_safe_float(gamma) or 0.0)
    oi_value = float(_safe_float(open_interest) or 0.0)
    spot_value = float(_safe_float(spot_price) or 0.0)
    sign = 1.0 if str(option_type or "").lower().startswith("c") else -1.0
    return gamma_value * oi_value * float(contract_multiplier) * (spot_value**2) * float(scaler) * sign


def compute_exposures(
    df: pd.DataFrame,
    spot: float,
    *,
    contract_multiplier: int = DEFAULT_CONTRACT_MULTIPLIER,
    scaler: float = DEFAULT_GEX_SCALER,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    for col in (
        "call_oi",
        "put_oi",
        "call_gamma",
        "put_gamma",
        "call_vega",
        "put_vega",
        "call_delta",
        "put_delta",
    ):
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    out["call_gex"] = out.apply(
        lambda row: compute_row_gex(
            row.get("call_gamma"),
            row.get("call_oi"),
            spot,
            "call",
            contract_multiplier=contract_multiplier,
            scaler=scaler,
        ),
        axis=1,
    )
    out["put_gex"] = out.apply(
        lambda row: compute_row_gex(
            row.get("put_gamma"),
            row.get("put_oi"),
            spot,
            "put",
            contract_multiplier=contract_multiplier,
            scaler=scaler,
        ),
        axis=1,
    )
    out["call_side_gex"] = out["call_gex"]
    out["put_side_gex"] = out["put_gex"].abs()
    out["net_gex"] = out["call_gex"] + out["put_gex"]
    out["gex"] = out["net_gex"]
    out["vex"] = (out["call_oi"] * out["call_vega"] * float(contract_multiplier)) + (
        out["put_oi"] * out["put_vega"] * float(contract_multiplier)
    )
    out["dex"] = (out["call_oi"] * out["call_delta"] * float(contract_multiplier)) - (
        out["put_oi"] * out["put_delta"] * float(contract_multiplier)
    )
    return out.sort_values(["strike", "expiration"]).reset_index(drop=True)


def aggregate_gex_by_strike(expo_df: pd.DataFrame) -> pd.DataFrame:
    if expo_df.empty:
        return expo_df.copy()

    out = expo_df.copy()
    defaults = {
        "call_oi": 0.0,
        "put_oi": 0.0,
        "call_gex": 0.0,
        "put_gex": 0.0,
        "call_side_gex": 0.0,
        "put_side_gex": 0.0,
        "net_gex": 0.0,
        "gex": 0.0,
        "vex": 0.0,
        "dex": 0.0,
    }
    for col, default in defaults.items():
        if col not in out.columns:
            out[col] = default

    grouped = (
        out.groupby("strike", as_index=False)
        .agg(
            {
                "call_oi": "sum",
                "put_oi": "sum",
                "call_gex": "sum",
                "put_gex": "sum",
                "call_side_gex": "sum",
                "put_side_gex": "sum",
                "net_gex": "sum",
                "gex": "sum",
                "vex": "sum",
                "dex": "sum",
            }
        )
        .sort_values("strike", kind="mergesort")
        .reset_index(drop=True)
    )
    grouped["gex"] = grouped["net_gex"]
    return grouped


def compute_net_gex_total(grouped_df: pd.DataFrame) -> float:
    if grouped_df.empty:
        return 0.0
    return float(grouped_df["net_gex"].sum())


def classify_gamma_regime(
    net_gex_total: float,
    *,
    positive_threshold: Optional[float] = None,
    negative_threshold: Optional[float] = None,
) -> str:
    configured_positive, configured_negative = _gamma_regime_thresholds()
    upper = float(configured_positive if positive_threshold is None else positive_threshold)
    lower = float(configured_negative if negative_threshold is None else negative_threshold)
    if net_gex_total > upper:
        return "positive"
    if net_gex_total < lower:
        return "negative"
    return "neutral"


def _wall_candidates_by_value(
    grouped_df: pd.DataFrame,
    *,
    target_value: float,
    comparison: str,
) -> pd.DataFrame:
    if grouped_df.empty:
        return grouped_df.copy()
    values = grouped_df["net_gex"].astype(float)
    if comparison == "max":
        mask = np.isclose(values.to_numpy(), float(target_value))
    else:
        mask = np.isclose(values.to_numpy(), float(target_value))
    return grouped_df.loc[mask].copy()


def compute_call_wall(grouped_df: pd.DataFrame, spot: float) -> Optional[Dict[str, float]]:
    if grouped_df.empty:
        return None
    positive = grouped_df[grouped_df["net_gex"] > 0].copy()
    if positive.empty:
        return None
    max_value = float(positive["net_gex"].max())
    candidates = _wall_candidates_by_value(positive, target_value=max_value, comparison="max")
    ordered = candidates.sort_values(
        by=["strike"],
        kind="mergesort",
        ascending=True,
    )
    best = min(
        ordered.to_dict("records"),
        key=lambda row: (
            abs(float(row["strike"]) - float(spot)),
            -float(row["strike"]),
        ),
    )
    return {"strike": float(best["strike"]), "net_gex": float(best["net_gex"])}


def compute_put_wall(grouped_df: pd.DataFrame, spot: float) -> Optional[Dict[str, float]]:
    if grouped_df.empty:
        return None
    negative = grouped_df[grouped_df["net_gex"] < 0].copy()
    if negative.empty:
        return None
    min_value = float(negative["net_gex"].min())
    candidates = _wall_candidates_by_value(negative, target_value=min_value, comparison="min")
    ordered = candidates.sort_values(
        by=["strike"],
        kind="mergesort",
        ascending=True,
    )
    best = min(
        ordered.to_dict("records"),
        key=lambda row: (
            abs(float(row["strike"]) - float(spot)),
            float(row["strike"]),
        ),
    )
    return {"strike": float(best["strike"]), "net_gex": float(best["net_gex"])}


def _identify_void_zone(expo_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    if "net_gex" in expo_df.columns:
        neg = expo_df[expo_df["net_gex"] < 0].sort_values("strike")
    else:
        neg = expo_df[expo_df["gex"] < 0].sort_values("strike")
    if neg.empty:
        return {"start": None, "end": None}

    runs: List[List[float]] = []
    current: List[float] = []
    prev = None
    for _, row in neg.iterrows():
        strike = float(row["strike"])
        if prev is None or abs(strike - prev) <= 5.1:
            current.append(strike)
        else:
            if current:
                runs.append(current)
            current = [strike]
        prev = strike
    if current:
        runs.append(current)
    if not runs:
        return {"start": None, "end": None}

    best_run = runs[0]
    best_score = None
    for run in runs:
        seg = neg[neg["strike"].isin(run)]
        score = float(seg["net_gex"].sum())  # most negative wins
        if best_score is None or score < best_score:
            best_score = score
            best_run = run
    return {"start": float(min(best_run)), "end": float(max(best_run))}


def compute_gamma_flip(grouped_df: pd.DataFrame, spot: float) -> Optional[float]:
    if grouped_df.empty:
        return None
    ordered = grouped_df.sort_values("strike", kind="mergesort")
    xs = ordered["strike"].astype(float).to_numpy()
    ys = ordered["net_gex"].astype(float).to_numpy()

    zero_hits = [float(xs[idx]) for idx, value in enumerate(ys) if np.isclose(value, 0.0)]
    if zero_hits:
        return min(zero_hits, key=lambda strike: abs(float(strike) - float(spot)))

    crosses: List[float] = []
    for i in range(len(xs) - 1):
        x1, x2 = xs[i], xs[i + 1]
        y1, y2 = ys[i], ys[i + 1]
        if np.sign(y1) == np.sign(y2):
            continue
        denom = y2 - y1
        if denom == 0:
            continue
        cross = x1 + ((0.0 - y1) / denom) * (x2 - x1)
        crosses.append(float(cross))
    if not crosses:
        return None
    return min(crosses, key=lambda strike: abs(float(strike) - float(spot)))


def compute_secondary_gamma_levels(grouped_df: pd.DataFrame, spot: float) -> Dict[str, Any]:
    if grouped_df.empty:
        return {
            "top_3_positive_gamma_strikes": [],
            "top_3_negative_gamma_strikes": [],
            "nearest_positive_gamma_above_spot": None,
            "nearest_negative_gamma_below_spot": None,
            "next_call_wall_above": None,
            "next_put_wall_below": None,
        }

    positive = grouped_df[grouped_df["net_gex"] > 0].copy()
    negative = grouped_df[grouped_df["net_gex"] < 0].copy()
    positive = positive.sort_values(
        by=["net_gex", "strike"],
        ascending=[False, True],
        kind="mergesort",
    )
    negative = negative.sort_values(
        by=["net_gex", "strike"],
        ascending=[True, True],
        kind="mergesort",
    )

    top_positive = [float(v) for v in positive["strike"].head(3).tolist()]
    top_negative = [float(v) for v in negative["strike"].head(3).tolist()]
    above_spot = positive[positive["strike"] > float(spot)]
    below_spot = negative[negative["strike"] < float(spot)]
    return {
        "top_3_positive_gamma_strikes": top_positive,
        "top_3_negative_gamma_strikes": top_negative,
        "nearest_positive_gamma_above_spot": (
            float(above_spot["strike"].min()) if not above_spot.empty else None
        ),
        "nearest_negative_gamma_below_spot": (
            float(below_spot["strike"].max()) if not below_spot.empty else None
        ),
        "next_call_wall_above": float(above_spot["strike"].min()) if not above_spot.empty else None,
        "next_put_wall_below": float(below_spot["strike"].max()) if not below_spot.empty else None,
    }


def identify_levels(expo_df: pd.DataFrame, spot: float) -> Dict[str, Any]:
    aggregated = aggregate_gex_by_strike(expo_df)
    if aggregated.empty:
        return {
            "net_gex": 0.0,
            "gamma_walls_top3": [],
            "void_zone": {"start": None, "end": None},
            "gamma_flip": None,
            "call_wall": None,
            "put_wall": None,
            "call_wall_gamma_per_point": None,
            "put_wall_gamma_per_point": None,
            "top_3_negative_gamma_strikes": [],
            "nearest_positive_gamma_above_spot": None,
            "nearest_negative_gamma_below_spot": None,
            "next_call_wall_above": None,
            "next_put_wall_below": None,
        }

    net_gex = compute_net_gex_total(aggregated)
    call_wall_row = compute_call_wall(aggregated, float(spot))
    put_wall_row = compute_put_wall(aggregated, float(spot))
    secondary = compute_secondary_gamma_levels(aggregated, float(spot))
    call_wall = call_wall_row["strike"] if call_wall_row else None
    put_wall = put_wall_row["strike"] if put_wall_row else None
    call_wall_gamma_per_point = abs(float(call_wall_row["net_gex"])) if call_wall_row else None
    put_wall_gamma_per_point = abs(float(put_wall_row["net_gex"])) if put_wall_row else None

    return {
        "net_gex": net_gex,
        "net_gex_total": net_gex,
        "gamma_walls_top3": secondary.get("top_3_positive_gamma_strikes") or [],
        "void_zone": _identify_void_zone(aggregated),
        "gamma_flip": compute_gamma_flip(aggregated, float(spot)),
        "call_wall": call_wall,
        "put_wall": put_wall,
        "call_wall_gamma_per_point": call_wall_gamma_per_point,
        "put_wall_gamma_per_point": put_wall_gamma_per_point,
        "top_3_positive_gamma_strikes": secondary.get("top_3_positive_gamma_strikes") or [],
        "top_3_negative_gamma_strikes": secondary.get("top_3_negative_gamma_strikes") or [],
        "nearest_positive_gamma_above_spot": secondary.get("nearest_positive_gamma_above_spot"),
        "nearest_negative_gamma_below_spot": secondary.get("nearest_negative_gamma_below_spot"),
        "next_call_wall_above": secondary.get("next_call_wall_above"),
        "next_put_wall_below": secondary.get("next_put_wall_below"),
    }


def build_summary(levels: Dict[str, Any], spot: float, net_gex: float) -> Dict[str, Any]:
    regime = classify_gamma_regime(float(net_gex))
    gamma_flip = _safe_float(levels.get("gamma_flip"))

    if regime == "positive" and gamma_flip is not None and float(spot) >= gamma_flip:
        bias = "buy_dips_above_flip"
    elif regime == "negative" and gamma_flip is not None and float(spot) <= gamma_flip:
        bias = "sell_rips_below_flip"
    else:
        bias = "neutral_chop"

    return {
        "regime": regime,
        "bias": bias,
    }


def _derive_gamma_range_estimate(
    spot: Optional[float],
    call_wall: Optional[float],
    put_wall: Optional[float],
) -> Optional[float]:
    # This is intentionally a wall-distance estimate, not an options-implied
    # expected move. It measures the larger distance from spot to the current
    # aggregated call/put wall pair so the UI can communicate a simple
    # wall-based range proxy without implying a volatility formula.
    if spot is None or call_wall is None or put_wall is None:
        return None
    return float(max(abs(float(call_wall) - float(spot)), abs(float(spot) - float(put_wall))))


def _build_narrative(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    warnings = [str(item) for item in snapshot.get("warnings") or []]
    spot = _safe_float(snapshot.get("spot_price_used"))
    regime = str(snapshot.get("regime") or "unavailable")
    gamma_flip = _safe_float(snapshot.get("gamma_flip"))
    call_wall = _safe_float(snapshot.get("call_wall"))
    put_wall = _safe_float(snapshot.get("put_wall"))
    top_positive = list(snapshot.get("top_3_positive_gamma_strikes") or [])
    top_negative = list(snapshot.get("top_3_negative_gamma_strikes") or [])
    auto_read: List[str] = []
    warning_badges: List[str] = []

    if snapshot.get("stale_flags"):
        warning_badges.append("Warnings")
    if "missing_expiries" in (snapshot.get("stale_flags") or []):
        warning_badges.append("Missing next expiry")
    if gamma_flip is None:
        warning_badges.append("No valid gamma flip")
    if "stale_gamma_source" in (snapshot.get("stale_flags") or []):
        warning_badges.append("Source stale")
    if "spot_source_mismatch" in (snapshot.get("stale_flags") or []):
        warning_badges.append("Spot mismatch")

    if warnings:
        what_matters = "Gamma model is degraded. Use level interaction cautiously until warnings clear."
        trader_live_quote = "Data quality warning: combined-basket gamma is available, but one or more inputs are stale or incomplete."
        trade_bias = "Reduce confidence and anchor decisions to confirmed price response at the displayed levels."
        environment_note = "Warnings: " + " · ".join(warnings[:3])
        auto_read.append(environment_note)
        return {
            "what_matters": what_matters,
            "trader_live_quote": trader_live_quote,
            "trade_bias": trade_bias,
            "environment_note": environment_note,
            "auto_read": auto_read,
            "warning_badges": warning_badges,
        }

    if spot is None:
        return {
            "what_matters": "Spot price is unavailable, so gamma context is withheld.",
            "trader_live_quote": "No live SPX spot was available at compute time.",
            "trade_bias": "Do not rely on directional guidance without a valid spot anchor.",
            "environment_note": "Awaiting a valid spot price.",
            "auto_read": [
                "Spot price is missing, so no directional gamma narrative is issued.",
            ],
            "warning_badges": warning_badges or ["Warnings"],
        }

    distance_to_flip = None if gamma_flip is None else float(spot) - float(gamma_flip)
    distance_to_call = None if call_wall is None else float(call_wall) - float(spot)
    distance_to_put = None if put_wall is None else float(spot) - float(put_wall)

    if regime == "negative" and gamma_flip is not None and spot < gamma_flip:
        what_matters = (
            f"Negative gamma below the combined-basket flip ({gamma_flip:.0f}) keeps downside expansion risk active."
        )
        trader_live_quote = "Expect faster directional moves and weaker mean reversion until price reclaims the flip."
        trade_bias = "Sell failed rallies and demand clean acceptance back above the flip before trusting stabilization."
    elif regime == "negative" and gamma_flip is not None and spot > gamma_flip:
        what_matters = (
            f"Negative gamma with spot above the flip ({gamma_flip:.0f}) leaves squeeze conditions live if buyers hold acceptance."
        )
        trader_live_quote = "Upside can extend quickly, but failed acceptance above the flip can reverse sharply."
        trade_bias = "Respect upside squeeze risk while spot is above the flip; avoid shorting strength without rejection."
    elif regime == "positive" and gamma_flip is not None and spot >= gamma_flip:
        what_matters = (
            f"Positive gamma above the combined-basket flip ({gamma_flip:.0f}) favors a more contained, mean-reverting tape."
        )
        trader_live_quote = "Expect stronger pinning behavior unless price is accepted through a major wall."
        trade_bias = "Favor fade setups into stretched moves unless a wall breaks with clear acceptance."
    elif regime == "positive" and gamma_flip is not None and spot < gamma_flip:
        what_matters = (
            f"Positive gamma remains in place, but spot is below the flip ({gamma_flip:.0f}), so local downside can stay unstable until reclaimed."
        )
        trader_live_quote = "The tape can stabilize quickly if price regains the flip; until then, support tests matter more."
        trade_bias = "Stay selective until spot reclaims the flip or clearly responds at support."
    else:
        what_matters = "No valid combined-basket gamma flip was found in the aggregated strike map."
        trader_live_quote = "The engine is withholding regime-transition certainty because no sign change was present."
        trade_bias = "Trade the walls and raw price confirmation rather than assuming a flip-driven regime."
        warning_badges.append("No valid gamma flip")

    if distance_to_call is not None and distance_to_call >= 0 and distance_to_call <= 15:
        auto_read.append(
            f"Spot is within {distance_to_call:.1f} points of call wall {call_wall:.0f}; watch for resistance, pinning, or squeeze-through acceptance."
        )
    elif distance_to_put is not None and distance_to_put >= 0 and distance_to_put <= 15:
        auto_read.append(
            f"Spot is within {distance_to_put:.1f} points of put wall {put_wall:.0f}; watch for support response or breakdown acceleration."
        )
    elif gamma_flip is not None and abs(float(distance_to_flip or 0.0)) >= 25:
        auto_read.append(
            f"Spot is {abs(float(distance_to_flip)):.1f} points from the flip, so current regime posture is materially displaced from transition."
        )

    if top_positive:
        auto_read.append(
            "Top positive gamma strikes: " + ", ".join(f"{float(level):.0f}" for level in top_positive[:3])
        )
    if top_negative:
        auto_read.append(
            "Top negative gamma strikes: " + ", ".join(f"{float(level):.0f}" for level in top_negative[:3])
        )

    gamma_range_estimate = _safe_float(
        snapshot.get("gamma_range_estimate", snapshot.get("expected_move"))
    )
    environment_bits = [
        f"Included expiries: {', '.join(str(exp) for exp in snapshot.get('included_expiries') or []) or 'none'}."
    ]
    if gamma_range_estimate is not None:
        environment_bits.append(f"{GAMMA_RANGE_LABEL}: +/- {gamma_range_estimate:.1f} points.")
    environment_note = " ".join(environment_bits)
    return {
        "what_matters": what_matters,
        "trader_live_quote": trader_live_quote,
        "trade_bias": trade_bias,
        "environment_note": environment_note,
        "auto_read": auto_read[:4],
        "warning_badges": list(dict.fromkeys(warning_badges))[:4],
    }


def assemble_market_pulse_snapshot(
    *,
    raw: pd.DataFrame,
    requested_expiries: List[str],
    source_timestamp: str,
    source_file_path: str,
    spot_price: float,
    spot_source_name: str,
    spot_source_timestamp: str,
    contracts_seen: int,
    contract_multiplier: int = DEFAULT_CONTRACT_MULTIPLIER,
) -> Dict[str, Any]:
    validated = validate_gamma_source(raw, source_timestamp=source_timestamp, spot_price=spot_price)
    selected = select_expiry_basket(
        validated["rows"],
        requested_expiries=requested_expiries,
    )
    warnings = list(validated["warnings"]) + list(selected["warnings"])
    stale_flags = list(validated["stale_flags"]) + list(selected["stale_flags"])
    selected_rows = selected["rows"]
    if selected_rows.empty:
        warnings.append("Combined near-dated basket is empty after validation.")
        stale_flags.append("empty_grouped_results")

    expo = compute_exposures(
        selected_rows,
        float(spot_price),
        contract_multiplier=contract_multiplier,
    )
    grouped = aggregate_gex_by_strike(expo)
    levels = identify_levels(expo, float(spot_price))
    net_gex_total = compute_net_gex_total(grouped)
    summary = build_summary(levels, float(spot_price), net_gex_total)
    if levels.get("gamma_flip") is None:
        warnings.append("No valid gamma flip was found in the aggregated strike map.")
        stale_flags.append("no_valid_gamma_flip")
    if grouped.empty:
        warnings.append("Grouped strike table is empty after aggregation.")
        stale_flags.append("empty_grouped_results")

    gamma_range_estimate = _derive_gamma_range_estimate(
        float(spot_price),
        _safe_float(levels.get("call_wall")),
        _safe_float(levels.get("put_wall")),
    )
    computed_at = _now_iso()
    gex_fig = render_gex_chart(grouped, levels, float(spot_price))
    vex_fig = render_vex_chart(grouped, float(spot_price))
    paths = export_outputs(expo, gex_fig, vex_fig, app_runtime.UPLOAD_DIR)

    snapshot = {
        "asof": computed_at,
        "computed_at": computed_at,
        "last_successful_compute": computed_at,
        "source_timestamp": source_timestamp,
        "source_file_path": source_file_path or str(paths.get("csv") or ""),
        "spot": float(spot_price),
        "spot_price_used": float(spot_price),
        "spot_source_name": str(spot_source_name or ""),
        "spot_source_timestamp": str(spot_source_timestamp or ""),
        "requested_expiries": list(requested_expiries),
        "included_expiries": list(selected.get("included_expiries") or []),
        "contract_multiplier": int(contract_multiplier),
        "total_rows_before_filter": int(validated["total_rows_before_filter"]),
        "total_rows_after_filter": int(len(selected_rows.index)),
        "total_unique_strikes": int(grouped["strike"].nunique()) if not grouped.empty else 0,
        "regime": summary.get("regime"),
        "net_gex": float(net_gex_total),
        "net_gex_total": float(net_gex_total),
        "net_gamma_label": _abbrev_billions(float(net_gex_total)),
        "gamma_flip": levels.get("gamma_flip"),
        "call_wall": levels.get("call_wall"),
        "put_wall": levels.get("put_wall"),
        "call_wall_gamma_per_point": levels.get("call_wall_gamma_per_point"),
        "put_wall_gamma_per_point": levels.get("put_wall_gamma_per_point"),
        "gamma_walls_top3": list(levels.get("top_3_positive_gamma_strikes") or []),
        "top_positive_strikes": list(levels.get("top_3_positive_gamma_strikes") or []),
        "top_negative_strikes": list(levels.get("top_3_negative_gamma_strikes") or []),
        "top_3_positive_gamma_strikes": list(levels.get("top_3_positive_gamma_strikes") or []),
        "top_3_negative_gamma_strikes": list(levels.get("top_3_negative_gamma_strikes") or []),
        "nearest_positive_gamma_above_spot": levels.get("nearest_positive_gamma_above_spot"),
        "nearest_negative_gamma_below_spot": levels.get("nearest_negative_gamma_below_spot"),
        "gamma_range_estimate": gamma_range_estimate,
        "gamma_range_high": (float(spot_price) + gamma_range_estimate) if gamma_range_estimate is not None else None,
        "gamma_range_low": (float(spot_price) - gamma_range_estimate) if gamma_range_estimate is not None else None,
        "expected_move": gamma_range_estimate,
        "expected_move_high": (float(spot_price) + gamma_range_estimate) if gamma_range_estimate is not None else None,
        "expected_move_low": (float(spot_price) - gamma_range_estimate) if gamma_range_estimate is not None else None,
        "expected_move_up": (float(spot_price) + gamma_range_estimate) if gamma_range_estimate is not None else None,
        "expected_move_down": (float(spot_price) - gamma_range_estimate) if gamma_range_estimate is not None else None,
        "next_call_wall_above": levels.get("next_call_wall_above"),
        "next_put_wall_below": levels.get("next_put_wall_below"),
        "void_zone": levels.get("void_zone") or {"start": None, "end": None},
        "bias": summary.get("bias"),
        "warnings": list(dict.fromkeys(str(item) for item in warnings if str(item).strip())),
        "stale_flags": list(dict.fromkeys(str(item) for item in stale_flags if str(item).strip())),
        "cache_status": "fresh",
        "field_labels": _json_clone(_CACHE["field_labels"]),
        "paths": paths,
        "diagnostics": {
            "status": "ok",
            "contracts_seen": int(contracts_seen),
            "contracts_used": int(len(expo.index)),
            "duplicate_raw_records": int(validated["duplicate_raw_records"]),
            "nan_gamma_rows": int(validated["nan_gamma_rows"]),
            "zero_open_interest_rows": int(validated["zero_open_interest_rows"]),
            "rows_dropped": int(validated["rows_dropped"]),
            "expirations": list(selected.get("included_expiries") or []),
            "refresh_ms": 0,
            "error": "",
        },
        "chart_json": {
            "gex": gex_fig.to_plotly_json(),
            "vex": vex_fig.to_plotly_json(),
        },
    }
    snapshot["narrative"] = _build_narrative(snapshot)
    return snapshot


def should_refresh_snapshot(
    current_snapshot: Dict[str, Any],
    *,
    force_refresh: bool = False,
    now_et: Optional[datetime] = None,
) -> bool:
    if force_refresh:
        return True
    asof_raw = str(current_snapshot.get("computed_at") or current_snapshot.get("asof") or "").strip()
    if not asof_raw:
        return True
    try:
        asof_dt = datetime.fromisoformat(asof_raw.replace("Z", "+00:00")).astimezone(app_runtime.TZ)
    except Exception:
        return True
    current = now_et.astimezone(app_runtime.TZ) if now_et else app_runtime.now_et()
    age_seconds = max(0, int((current - asof_dt).total_seconds()))
    return age_seconds >= _gamma_poll_seconds(current)


def get_or_build_market_pulse_snapshot(force_refresh: bool = False) -> Dict[str, Any]:
    with _LOCK:
        current_snapshot = _json_clone(_CACHE)
    if not should_refresh_snapshot(current_snapshot, force_refresh=force_refresh):
        snapshot = _json_clone(current_snapshot)
        snapshot["cache_status"] = "cache_hit"
        snapshot["diagnostics"] = dict(snapshot.get("diagnostics") or {})
        snapshot["diagnostics"]["status"] = str(_RUNTIME_STATE.get("status") or snapshot["diagnostics"].get("status") or "ok")
        snapshot["diagnostics"]["error"] = str(_RUNTIME_STATE.get("last_error") or "")
        return snapshot

    started = time.time()
    source = load_gamma_source()
    spot_snapshot = market_data_service.get_price_snapshot("SPX")
    spot = _safe_float(spot_snapshot.get("value"))
    if spot is None:
        raise RuntimeError("SPX spot unavailable")

    candidate = assemble_market_pulse_snapshot(
        raw=source["raw"],
        requested_expiries=list(source["requested_expiries"] or []),
        source_timestamp=str(source["source_timestamp"] or _now_iso()),
        source_file_path=str(source.get("source_file_path") or ""),
        spot_price=float(spot),
        spot_source_name=str(spot_snapshot.get("source_name") or ""),
        spot_source_timestamp=str(spot_snapshot.get("source_timestamp") or ""),
        contracts_seen=int(source.get("contracts_seen") or 0),
        contract_multiplier=DEFAULT_CONTRACT_MULTIPLIER,
    )
    if candidate.get("total_rows_after_filter", 0) <= 0:
        raise RuntimeError("Gamma snapshot build produced no selected rows")
    if candidate.get("total_unique_strikes", 0) <= 0:
        raise RuntimeError("Gamma snapshot build produced no grouped strikes")
    if not list(candidate.get("included_expiries") or []):
        raise RuntimeError("Gamma snapshot build produced no included expiries")
    candidate["diagnostics"] = dict(candidate.get("diagnostics") or {})
    candidate["diagnostics"]["refresh_ms"] = int((time.time() - started) * 1000)
    candidate["cache_status"] = "fresh"
    return candidate


def render_gex_chart(expo_df: pd.DataFrame, levels: Dict[str, Any], spot: float) -> go.Figure:
    expo_df = aggregate_gex_by_strike(expo_df)
    fig = go.Figure() if go is not None else _FallbackFigure("GEX by Strike")
    if expo_df.empty:
        fig.update_layout(template="plotly_dark", title="GEX by Strike (no data)")
        return fig

    if go is not None:
        colors = ["#3ed08f" if float(v) >= 0 else "#ff6f7d" for v in expo_df["gex"].tolist()]
        fig.add_trace(
            go.Bar(
                x=expo_df["strike"].tolist(),
                y=expo_df["gex"].tolist(),
                marker_color=colors,
                name="GEX",
            )
        )
    else:
        fig.update_layout(title="SPX Gamma Exposure (GEX) by Strike")
        return fig
    fig.add_vline(x=float(spot), line_dash="dash", line_color="#9ec7ff", annotation_text="Spot")
    for lvl in levels.get("gamma_walls_top3") or []:
        fig.add_vline(x=float(lvl), line_color="#55efc4", line_width=1)
    flip = _safe_float(levels.get("gamma_flip"))
    if flip is not None:
        fig.add_vline(x=float(flip), line_color="#ffd166", line_width=2, annotation_text="Flip")
    void_zone = levels.get("void_zone") or {}
    vz_start = _safe_float(void_zone.get("start"))
    vz_end = _safe_float(void_zone.get("end"))
    if vz_start is not None and vz_end is not None and vz_end >= vz_start:
        fig.add_vrect(x0=vz_start, x1=vz_end, fillcolor="rgba(255,90,90,.12)", line_width=0)

    fig.update_layout(
        template="plotly_dark",
        title="SPX Gamma Exposure (GEX) by Strike",
        xaxis_title="Strike",
        yaxis_title="GEX",
        margin=dict(l=40, r=20, t=52, b=40),
        height=420,
    )
    return fig


def render_vex_chart(expo_df: pd.DataFrame, spot: float) -> go.Figure:
    expo_df = aggregate_gex_by_strike(expo_df)
    fig = go.Figure() if go is not None else _FallbackFigure("VEX by Strike")
    if expo_df.empty:
        fig.update_layout(template="plotly_dark", title="VEX by Strike (no data)")
        return fig

    if go is not None:
        fig.add_trace(
            go.Bar(
                x=expo_df["strike"].tolist(),
                y=expo_df["vex"].tolist(),
                marker_color="#6aaeff",
                name="VEX",
            )
        )
    else:
        fig.update_layout(title="SPX Vega Exposure (VEX) by Strike")
        return fig
    fig.add_vline(x=float(spot), line_dash="dash", line_color="#9ec7ff", annotation_text="Spot")
    fig.update_layout(
        template="plotly_dark",
        title="SPX Vega Exposure (VEX) by Strike",
        xaxis_title="Strike",
        yaxis_title="VEX",
        margin=dict(l=40, r=20, t=52, b=40),
        height=360,
    )
    return fig


def export_outputs(
    expo_df: pd.DataFrame, gex_fig: go.Figure, vex_fig: go.Figure, out_dir: str
) -> Dict[str, str]:
    os.makedirs(out_dir, exist_ok=True)
    csv_path = os.path.join(out_dir, CSV_FILENAME)
    png_path = os.path.join(out_dir, PNG_FILENAME)
    expo_df.to_csv(csv_path, index=False)

    try:
        gex_fig.write_image(png_path, width=1400, height=900, scale=2)
    except Exception:
        # If kaleido is missing/unavailable, keep the path stable but no crash.
        pass
    return {"csv": csv_path, "png": png_path}


def _abbrev_billions(value: float) -> str:
    abs_v = abs(float(value))
    sign = "+" if value > 0 else "-" if value < 0 else ""
    if abs_v >= 1_000_000_000:
        return f"{sign}{abs_v / 1_000_000_000:.2f}B"
    if abs_v >= 1_000_000:
        return f"{sign}{abs_v / 1_000_000:.2f}M"
    return f"{value:.0f}"


def _eod_gamma_notification_context(
    snapshot: Dict[str, Any], now_et: Optional[datetime] = None
) -> Dict[str, Any] | None:
    if not EOD_GAMMA_NOTIFY_ENABLED:
        return None
    current = now_et.astimezone(app_runtime.TZ) if now_et else app_runtime.now_et()
    if current.weekday() >= 5:
        return None
    minutes = current.hour * 60 + current.minute
    start_minutes = _parse_hhmm(EOD_GAMMA_NOTIFY_TIME_ET, default="17:10")
    end_minutes = _parse_hhmm(EOD_GAMMA_NOTIFY_END_ET, default="19:30")
    if minutes < start_minutes or minutes > end_minutes:
        return None

    diagnostics = snapshot.get("diagnostics") if isinstance(snapshot, dict) else {}
    expirations = diagnostics.get("expirations") if isinstance(diagnostics, dict) else []
    if not isinstance(expirations, list) or len(expirations) < 2:
        return None
    target_expiry = str(expirations[1] or "").strip()
    if not target_expiry:
        return None

    state = _load_gamma_notify_state()
    if str(state.get("last_target_expiry") or "") == target_expiry:
        return None

    return {
        "trade_date": current.date().isoformat(),
        "target_expiry": target_expiry,
        "window_start_et": EOD_GAMMA_NOTIFY_TIME_ET,
        "window_end_et": EOD_GAMMA_NOTIFY_END_ET,
    }


def _send_eod_gamma_notification(snapshot: Dict[str, Any], context: Dict[str, Any]) -> None:
    from mccain_capital.services import trades as trades_service

    target_expiry = str(context.get("target_expiry") or "").strip()
    flip = _fmt_level(snapshot.get("gamma_flip"))
    call_wall = _fmt_level(snapshot.get("call_wall"))
    put_wall = _fmt_level(snapshot.get("put_wall"))
    spot = _safe_float(snapshot.get("spot"))
    spot_text = f"{spot:,.2f}" if spot is not None else "n/a"
    message = (
        f"Preliminary SPX gamma levels for {target_expiry} are ready for charting after the "
        f"5:00 PM ET curb close. Spot {spot_text} · flip {flip} · call wall {call_wall} · "
        f"put wall {put_wall}. Recheck pre-open for the final open-interest-backed view."
    )
    trades_service._emit_notification(
        "gamma_levels_ready",
        "Next-Day SPX Gamma Ready",
        message,
        extra={
            "target_expiry": target_expiry,
            "asof": str(snapshot.get("asof") or ""),
            "spot": spot,
            "gamma_flip": _safe_float(snapshot.get("gamma_flip")),
            "call_wall": _safe_float(snapshot.get("call_wall")),
            "put_wall": _safe_float(snapshot.get("put_wall")),
        },
    )


def _maybe_emit_eod_gamma_notification(
    snapshot: Dict[str, Any], now_et: Optional[datetime] = None
) -> None:
    context = _eod_gamma_notification_context(snapshot, now_et=now_et)
    if not context:
        return
    _send_eod_gamma_notification(snapshot, context)
    _save_gamma_notify_state(
        {
            "last_trade_date": str(context.get("trade_date") or ""),
            "last_target_expiry": str(context.get("target_expiry") or ""),
            "last_notified_at": app_runtime.now_iso(),
        }
    )


def run_gamma_refresh_once() -> Dict[str, Any]:
    snapshot = get_or_build_market_pulse_snapshot(force_refresh=True)
    with _LOCK:
        _CACHE.clear()
        _CACHE.update(_json_clone(snapshot))
        _RUNTIME_STATE["last_attempted_at"] = str(snapshot.get("computed_at") or snapshot.get("asof") or "")
        _RUNTIME_STATE["last_error"] = ""
        _RUNTIME_STATE["last_refresh_ms"] = int(((snapshot.get("diagnostics") or {}).get("refresh_ms")) or 0)
        _RUNTIME_STATE["status"] = "ok"
        _RUNTIME_STATE["cache_status"] = str(snapshot.get("cache_status") or "fresh")
    _maybe_emit_eod_gamma_notification(snapshot)
    return _json_clone(snapshot)


def _worker_loop() -> None:
    while True:
        try:
            run_gamma_refresh_once()
        except Exception as exc:
            with _LOCK:
                _RUNTIME_STATE["last_attempted_at"] = _now_iso()
                _RUNTIME_STATE["last_error"] = str(exc)
                _RUNTIME_STATE["last_refresh_ms"] = int(_gamma_poll_seconds() * 1000)
                _RUNTIME_STATE["status"] = "error"
                _RUNTIME_STATE["cache_status"] = "last_good"
        time.sleep(_gamma_poll_seconds())


def start_gamma_worker_once() -> None:
    global _STARTED
    with _LOCK:
        if _STARTED:
            return
        _STARTED = True
    t = threading.Thread(target=_worker_loop, name="gamma-map-worker", daemon=True)
    t.start()


def get_gamma_snapshot() -> Dict[str, Any]:
    with _LOCK:
        snapshot = _json_clone(_CACHE)
        runtime_state = dict(_RUNTIME_STATE)
    snapshot["cache_status"] = str(runtime_state.get("cache_status") or snapshot.get("cache_status") or "cold")
    snapshot["diagnostics"] = dict(snapshot.get("diagnostics") or {})
    snapshot["diagnostics"]["status"] = str(runtime_state.get("status") or snapshot["diagnostics"].get("status") or "waiting")
    snapshot["diagnostics"]["error"] = str(runtime_state.get("last_error") or snapshot["diagnostics"].get("error") or "")
    if runtime_state.get("last_refresh_ms"):
        snapshot["diagnostics"]["refresh_ms"] = int(runtime_state.get("last_refresh_ms") or 0)
    if snapshot["diagnostics"].get("status") == "error" and runtime_state.get("last_error"):
        warnings = list(snapshot.get("warnings") or [])
        if "Last refresh failed; serving last good gamma snapshot." not in warnings:
            warnings.append("Last refresh failed; serving last good gamma snapshot.")
        snapshot["warnings"] = warnings
        stale_flags = list(snapshot.get("stale_flags") or [])
        if "last_refresh_failed" not in stale_flags:
            stale_flags.append("last_refresh_failed")
        snapshot["stale_flags"] = stale_flags
    return snapshot
