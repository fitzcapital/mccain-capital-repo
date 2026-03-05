"""SPX gamma map engine (Massive/Polygon-backed)."""

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

POLL_SECONDS = 900
MAX_SNAPSHOT_PAGES = 8
CSV_FILENAME = "gamma_data.csv"
PNG_FILENAME = "gamma_map.png"

_LOCK = threading.Lock()
_STARTED = False
_CACHE: Dict[str, Any] = {
    "asof": "",
    "spot": None,
    "regime": "unavailable",
    "net_gex": 0.0,
    "gamma_flip": None,
    "call_wall": None,
    "put_wall": None,
    "gamma_walls_top3": [],
    "void_zone": {"start": None, "end": None},
    "bias": "insufficient_data",
    "paths": {"csv": "", "png": ""},
    "diagnostics": {
        "status": "waiting",
        "contracts_seen": 0,
        "contracts_used": 0,
        "rows_dropped": 0,
        "expirations": [],
        "refresh_ms": 0,
        "error": "",
    },
    "chart_json": {"gex": None, "vex": None},
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


def _massive_api_key() -> str:
    return (
        (os.environ.get("MASSIVE_API_KEY") or "").strip()
        or (os.environ.get("POLYGON_API_KEY") or "").strip()
        or str(app_runtime.get_setting_value("massive_api_key", "") or "").strip()
        or str(app_runtime.get_setting_value("polygon_api_key", "") or "").strip()
    )


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
    expiry_set = {str(x) for x in expiries if str(x)}
    rows: Dict[Tuple[str, float], Dict[str, Any]] = {}
    seen = 0
    pages = 0
    cursor: Optional[str] = None

    while pages < MAX_SNAPSHOT_PAGES:
        params: Dict[str, Any] = {"limit": 1000}
        if cursor:
            params["cursor"] = cursor
        payload = _massive_json("/v3/snapshot/options/SPX", params)
        pages += 1
        if not isinstance(payload, dict):
            break
        results = payload.get("results")
        if not isinstance(results, list):
            break

        for row in results:
            if not isinstance(row, dict):
                continue
            seen += 1
            details = row.get("details") if isinstance(row.get("details"), dict) else {}
            ticker = str(details.get("ticker") or row.get("ticker") or "")
            parsed = _parse_option_ticker(ticker)

            expiration = str(details.get("expiration_date") or parsed.get("expiration") or "")
            if expiration not in expiry_set:
                continue

            strike = _safe_float(details.get("strike_price"))
            if strike is None:
                strike = _safe_float(parsed.get("strike"))
            if strike is None:
                continue

            cp_raw = str(details.get("contract_type") or "").lower()
            cp = (
                "call"
                if cp_raw.startswith("c")
                else (
                    "put"
                    if cp_raw.startswith("p")
                    else ("call" if parsed.get("cp") == "C" else "put")
                )
            )

            oi = _safe_int(row.get("open_interest") or details.get("open_interest"))
            greeks = row.get("greeks") if isinstance(row.get("greeks"), dict) else {}
            gamma = _safe_float(greeks.get("gamma")) or 0.0
            vega = _safe_float(greeks.get("vega")) or 0.0
            delta = _safe_float(greeks.get("delta")) or 0.0

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
                bucket["call_oi"] = float(oi)
                bucket["call_gamma"] = float(gamma)
                bucket["call_vega"] = float(vega)
                bucket["call_delta"] = float(delta)
            else:
                bucket["put_oi"] = float(oi)
                bucket["put_gamma"] = float(gamma)
                bucket["put_vega"] = float(vega)
                bucket["put_delta"] = float(delta)

        next_url = str(payload.get("next_url") or "")
        if not next_url:
            break
        parsed_url = urllib.parse.urlparse(next_url)
        cursor = urllib.parse.parse_qs(parsed_url.query).get("cursor", [None])[0]
        if not cursor:
            break

    df = pd.DataFrame(list(rows.values()))
    if df.empty:
        fallback = _fetch_spx_chain_from_cboe(expiry_set)
        if not fallback.empty:
            fallback.attrs["contracts_seen"] = int(fallback.attrs.get("contracts_seen") or 0)
            return fallback
    if df.empty:
        return df
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


def compute_exposures(df: pd.DataFrame, spot: float) -> pd.DataFrame:
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

    out["call_side_gex"] = out["call_oi"] * out["call_gamma"] * 100.0 * float(spot)
    out["put_side_gex"] = out["put_oi"] * out["put_gamma"] * 100.0 * float(spot)
    out["gex"] = out["call_side_gex"] - out["put_side_gex"]
    out["vex"] = (out["call_oi"] * out["call_vega"] * 100.0) + (
        out["put_oi"] * out["put_vega"] * 100.0
    )
    out["dex"] = (out["call_oi"] * out["call_delta"] * 100.0) - (
        out["put_oi"] * out["put_delta"] * 100.0
    )
    out = out.sort_values(["strike", "expiration"]).reset_index(drop=True)
    return out


def _identify_void_zone(expo_df: pd.DataFrame) -> Dict[str, Optional[float]]:
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
        score = float(seg["gex"].sum())  # most negative wins
        if best_score is None or score < best_score:
            best_score = score
            best_run = run
    return {"start": float(min(best_run)), "end": float(max(best_run))}


def _identify_gamma_flip(expo_df: pd.DataFrame, spot: float) -> Optional[float]:
    if expo_df.empty:
        return None
    ordered = expo_df.sort_values("strike")
    xs = ordered["strike"].astype(float).to_numpy()
    ys = ordered["gex"].astype(float).to_numpy()

    crosses: List[float] = []
    for i in range(len(xs) - 1):
        x1, x2 = xs[i], xs[i + 1]
        y1, y2 = ys[i], ys[i + 1]
        if y1 == 0:
            crosses.append(float(x1))
            continue
        if y1 * y2 < 0:
            denom = y2 - y1
            if denom == 0:
                crosses.append(float(x1))
            else:
                cross = x1 + ((0.0 - y1) * (x2 - x1) / denom)
                crosses.append(float(cross))
    if crosses:
        return min(crosses, key=lambda s: abs(float(s) - float(spot)))

    idx = int(np.argmin(np.abs(ys)))
    return float(xs[idx])


def identify_levels(expo_df: pd.DataFrame, spot: float) -> Dict[str, Any]:
    if expo_df.empty:
        return {
            "net_gex": 0.0,
            "gamma_walls_top3": [],
            "void_zone": {"start": None, "end": None},
            "gamma_flip": None,
            "call_wall": None,
            "put_wall": None,
        }

    net_gex = float(expo_df["gex"].sum())

    pos = expo_df[expo_df["gex"] > 0].sort_values("gex", ascending=False)
    gamma_walls = [float(v) for v in pos["strike"].head(3).tolist()]

    call_idx = int(expo_df["call_side_gex"].idxmax())
    put_idx = int(expo_df["put_side_gex"].idxmax())
    call_wall = float(expo_df.loc[call_idx, "strike"]) if len(expo_df.index) else None
    put_wall = float(expo_df.loc[put_idx, "strike"]) if len(expo_df.index) else None

    return {
        "net_gex": net_gex,
        "gamma_walls_top3": gamma_walls,
        "void_zone": _identify_void_zone(expo_df),
        "gamma_flip": _identify_gamma_flip(expo_df, float(spot)),
        "call_wall": call_wall,
        "put_wall": put_wall,
    }


def build_summary(levels: Dict[str, Any], spot: float, net_gex: float) -> Dict[str, Any]:
    regime = "positive" if net_gex > 0 else "negative" if net_gex < 0 else "neutral"
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


def render_gex_chart(expo_df: pd.DataFrame, levels: Dict[str, Any], spot: float) -> go.Figure:
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


def run_gamma_refresh_once() -> Dict[str, Any]:
    started = time.time()
    today = app_runtime.today_iso()
    expiries = [today, _next_trading_day_iso(today)]
    spot = _safe_float(market_data_service.get_price("SPX"))
    if spot is None:
        raise RuntimeError("SPX spot unavailable")

    raw = fetch_spx_chain_for_expiries(expiries)
    contracts_seen = int(raw.attrs.get("contracts_seen") or 0)
    if raw.empty:
        raise RuntimeError("No SPX contracts for 0DTE/1DTE")

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
    before = len(raw.index)
    for col in mandatory:
        if col not in raw.columns:
            raw[col] = 0.0
    raw = raw.dropna(subset=["strike", "expiration"]).copy()
    dropped = max(0, before - len(raw.index))

    expo = compute_exposures(raw, float(spot))
    levels = identify_levels(expo, float(spot))
    summary = build_summary(levels, float(spot), float(levels.get("net_gex") or 0.0))
    gex_fig = render_gex_chart(expo, levels, float(spot))
    vex_fig = render_vex_chart(expo, float(spot))
    paths = export_outputs(expo, gex_fig, vex_fig, app_runtime.UPLOAD_DIR)

    snapshot = {
        "asof": _now_iso(),
        "spot": float(spot),
        "regime": summary.get("regime"),
        "net_gex": float(levels.get("net_gex") or 0.0),
        "net_gamma_label": _abbrev_billions(float(levels.get("net_gex") or 0.0)),
        "gamma_flip": levels.get("gamma_flip"),
        "call_wall": levels.get("call_wall"),
        "put_wall": levels.get("put_wall"),
        "gamma_walls_top3": levels.get("gamma_walls_top3") or [],
        "void_zone": levels.get("void_zone") or {"start": None, "end": None},
        "bias": summary.get("bias"),
        "paths": paths,
        "diagnostics": {
            "status": "ok",
            "contracts_seen": contracts_seen,
            "contracts_used": int(len(expo.index)),
            "rows_dropped": int(dropped),
            "expirations": expiries,
            "refresh_ms": int((time.time() - started) * 1000),
            "error": "",
        },
        "chart_json": {
            "gex": gex_fig.to_plotly_json(),
            "vex": vex_fig.to_plotly_json(),
        },
    }
    with _LOCK:
        _CACHE.clear()
        _CACHE.update(snapshot)
    return json.loads(json.dumps(snapshot))


def _worker_loop() -> None:
    while True:
        try:
            run_gamma_refresh_once()
        except Exception as exc:
            with _LOCK:
                _CACHE["diagnostics"] = dict(_CACHE.get("diagnostics") or {})
                _CACHE["diagnostics"]["status"] = "error"
                _CACHE["diagnostics"]["error"] = str(exc)
                _CACHE["diagnostics"]["refresh_ms"] = int(POLL_SECONDS * 1000)
                if not _CACHE.get("asof"):
                    _CACHE["asof"] = _now_iso()
        time.sleep(POLL_SECONDS)


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
        return json.loads(json.dumps(_CACHE))
