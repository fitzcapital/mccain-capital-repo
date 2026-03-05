"""SPX-focused live options panel service (Massive/Polygon)."""

from __future__ import annotations

from datetime import date
from datetime import datetime
from datetime import timezone
import json
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional
import urllib.parse
import urllib.request

from mccain_capital import runtime as app_runtime
from mccain_capital.services import market_data_service

POLL_SECONDS = 10
MAX_CONTRACTS = 5
OPTION_SYMBOLS = ["SPX"]

_LOCK = threading.Lock()
_STARTED = False
_CACHE: Dict[str, Any] = {
    "asof": "",
    "symbols": {
        "SPX": {
            "underlying": {"price": None, "change_pct": None, "source": "massive"},
            "contracts": [],
            "trade_mode": {
                "active": False,
                "entry": None,
                "stop": None,
                "target": None,
                "dist_stop": None,
                "dist_target": None,
            },
        }
    },
}

_COMPACT_TICKER = re.compile(r"^(?:O:)?(SPXW|SPX)(\d{2})(\d{2})(\d{2})([CP])(\d{8})$")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _massive_key() -> str:
    return (
        (os.environ.get("MASSIVE_API_KEY") or "").strip()
        or (os.environ.get("POLYGON_API_KEY") or "").strip()
        or str(app_runtime.get_setting_value("massive_api_key", "") or "").strip()
        or str(app_runtime.get_setting_value("polygon_api_key", "") or "").strip()
    )


def _massive_json(path: str, params: Dict[str, Any]) -> Dict[str, Any] | None:
    key = _massive_key()
    if not key:
        return None
    q = dict(params)
    q["apiKey"] = key
    url = "https://api.polygon.io" + path + "?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers={"User-Agent": "mccain-capital/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            parsed = json.loads(body)
            return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


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


def _format_strike(v: float) -> str:
    if abs(v - int(v)) < 1e-9:
        return str(int(v))
    return f"{v:.2f}".rstrip("0").rstrip(".")


def parse_option_ticker(ticker: str) -> Dict[str, Any]:
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
    exp = f"20{yy:02d}-{mm:02d}-{dd:02d}"
    return {"root": root, "expiration": exp, "cp": cp, "strike": strike}


def format_contract_label(root: str, expiration: str, strike: float, cp: str) -> str:
    return f"{root} {expiration} {_format_strike(float(strike))}{cp}"


def liquidity_badge(spread: Optional[float], volume: int) -> str:
    s = float(spread or 9999.0)
    v = int(volume or 0)
    if s <= 0.75 and v >= 2000:
        return "Tight"
    if s <= 1.50 and v >= 500:
        return "OK"
    return "Wide"


def _extract_contract_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    details = row.get("details") if isinstance(row.get("details"), dict) else {}
    ticker = str(details.get("ticker") or row.get("ticker") or "").strip()
    parsed = parse_option_ticker(ticker)
    root = str(details.get("underlying_ticker") or parsed.get("root") or "").upper()
    if root not in {"SPX", "SPXW"}:
        root = str(parsed.get("root") or "")
    expiration = str(details.get("expiration_date") or parsed.get("expiration") or "")
    cp_raw = str(details.get("contract_type") or "").lower()
    cp = (
        "C"
        if cp_raw.startswith("c")
        else "P" if cp_raw.startswith("p") else str(parsed.get("cp") or "")
    )
    strike = _safe_float(details.get("strike_price"))
    if strike is None:
        strike = _safe_float(parsed.get("strike"))
    if not root or not expiration or not cp or strike is None:
        return None

    quote = row.get("last_quote") if isinstance(row.get("last_quote"), dict) else {}
    bid = _safe_float(quote.get("bid"))
    ask = _safe_float(quote.get("ask"))
    spread = (ask - bid) if (bid is not None and ask is not None) else None

    mid = None
    if bid is not None and ask is not None:
        mid = (bid + ask) / 2.0
    if mid is None:
        trade = row.get("last_trade") if isinstance(row.get("last_trade"), dict) else {}
        mid = _safe_float(trade.get("price"))

    greeks = row.get("greeks") if isinstance(row.get("greeks"), dict) else {}
    delta = _safe_float(greeks.get("delta"))

    day = row.get("day") if isinstance(row.get("day"), dict) else {}
    vol = _safe_int(day.get("volume") or row.get("volume"))
    oi = _safe_int(row.get("open_interest") or details.get("open_interest"))

    label = format_contract_label(root, expiration, float(strike), cp)
    liq = liquidity_badge(spread, vol)

    try:
        exp_d = datetime.strptime(expiration, "%Y-%m-%d").date()
        dte = (exp_d - date.today()).days
    except Exception:
        dte = 999

    liq_rank = {"Tight": 0, "OK": 1, "Wide": 2}.get(liq, 3)
    root_rank = 0 if (root == "SPXW" and dte <= 7) else 1

    return {
        "label": label,
        "mid": mid,
        "delta": delta,
        "vol": vol,
        "oi": oi,
        "spread": spread,
        "liq": liq,
        "_root_rank": root_rank,
        "_liq_rank": liq_rank,
        "_dte": dte,
    }


def _fetch_spx_contracts() -> List[Dict[str, Any]]:
    payload = _massive_json("/v3/snapshot/options/SPX", {"limit": 250})
    rows = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        rows = []

    contracts: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            c = _extract_contract_row(row)
            if c is not None and int(c.get("_dte") or 999) >= 0:
                contracts.append(c)

    if not contracts:
        return []

    # Prefer <=7 DTE, then SPXW root, then tight liquidity, then volume.
    window = [c for c in contracts if int(c.get("_dte") or 999) <= 7]
    work = window if window else contracts
    work.sort(
        key=lambda c: (
            int(c.get("_root_rank") or 9),
            int(c.get("_liq_rank") or 9),
            -int(c.get("vol") or 0),
            float(c.get("spread") or 9999.0),
        )
    )
    return [
        {
            "label": c.get("label"),
            "mid": c.get("mid"),
            "delta": c.get("delta"),
            "vol": c.get("vol"),
            "oi": c.get("oi"),
            "spread": c.get("spread"),
            "liq": c.get("liq"),
        }
        for c in work[:MAX_CONTRACTS]
    ]


def _trade_mode(price: Optional[float]) -> Dict[str, Any]:
    active = str(app_runtime.get_setting_value("options_trade_active", "0") or "0").strip() in {
        "1",
        "true",
        "yes",
        "on",
    }
    entry = _safe_float(app_runtime.get_setting_value("options_trade_entry", ""))
    stop = _safe_float(app_runtime.get_setting_value("options_trade_stop", ""))
    target = _safe_float(app_runtime.get_setting_value("options_trade_target", ""))
    dist_stop = (stop - price) if (price is not None and stop is not None) else None
    dist_target = (target - price) if (price is not None and target is not None) else None
    return {
        "active": active,
        "entry": entry,
        "stop": stop,
        "target": target,
        "dist_stop": dist_stop,
        "dist_target": dist_target,
    }


def _poll_once() -> None:
    under = market_data_service.get_watchlist(["SPX"]).get("SPX", {})
    price = _safe_float(under.get("price"))
    change_pct = _safe_float(under.get("pct_change"))
    contracts = _fetch_spx_contracts() if _massive_key() else []

    snap = {
        "asof": _now_iso(),
        "symbols": {
            "SPX": {
                "underlying": {"price": price, "change_pct": change_pct, "source": "massive"},
                "contracts": contracts,
                "trade_mode": _trade_mode(price),
            }
        },
    }
    with _LOCK:
        _CACHE.clear()
        _CACHE.update(snap)


def _worker_loop() -> None:
    while True:
        try:
            _poll_once()
        except Exception:
            pass
        time.sleep(POLL_SECONDS)


def start_options_worker_once() -> None:
    global _STARTED
    with _LOCK:
        if _STARTED:
            return
        _STARTED = True
    t = threading.Thread(target=_worker_loop, name="options-panel-worker", daemon=True)
    t.start()


def get_options_snapshot() -> Dict[str, Any]:
    with _LOCK:
        return json.loads(json.dumps(_CACHE))
