"""Shared runtime utilities and data-access helpers.

This module is intentionally independent of ``app_core`` so repositories/services
can import stable primitives without pulling the legacy monolith.
"""

from __future__ import annotations

import os
import re
import sqlite3
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/New_York")
DB_PATH = os.environ.get("DB_PATH", "journal.db")
BOOKS_DIR = os.environ.get("BOOKS_DIR", "books")
UPLOAD_DIR = os.environ.get("UPLOAD_DIR", "uploads")
BASE_MONTHLY_INCOME = float(os.environ.get("BASE_MONTHLY_INCOME", "7800"))
DEFAULT_PROTECT_BUFFER = float(os.environ.get("PAYOUT_PROTECT_BUFFER", "1000"))
PROFIT_BUFFER_LEVEL_50K = 52875.0
FIXED_LOSS_LIMIT_50K = 50375.0
_HEADER_HINTS = {
    "date",
    "entry",
    "exit",
    "ticker",
    "type",
    "strike",
    "contracts",
    "net",
    "p/l",
    "balance",
}


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def get_setting_value(key: str, default: Any = None) -> Any:
    conn = db()
    if not _table_exists(conn, "settings"):
        return default

    cols = [r[1] for r in conn.execute("PRAGMA table_info(settings)").fetchall()]
    key_col = next((c for c in ("key", "name", "setting") if c in cols), None)
    val_col = next((c for c in ("value", "val", "setting_value") if c in cols), None)
    if not key_col or not val_col:
        return default

    row = conn.execute(
        f'SELECT "{val_col}" FROM settings WHERE "{key_col}" = ? LIMIT 1',
        (key,),
    ).fetchone()
    return row[0] if row else default


def get_setting_float(key: str, default: float = 0.0) -> float:
    val = get_setting_value(key, None)
    if val is None:
        return float(default)
    try:
        return float(val)
    except Exception:
        return float(default)


def now_et() -> datetime:
    return datetime.now(TZ)


def now_iso() -> str:
    return now_et().isoformat(timespec="seconds")


def today_iso() -> str:
    return now_et().date().isoformat()


def prev_trading_day_iso(d_iso: str) -> str:
    d = datetime.strptime(d_iso, "%Y-%m-%d").date() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


def next_trading_day_iso(d_iso: str) -> str:
    d = datetime.strptime(d_iso, "%Y-%m-%d").date() + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d.isoformat()


def money(val: Any) -> str:
    if val is None or val == "":
        return ""
    try:
        n = float(val)
    except Exception:
        return ""
    sign = "-" if n < 0 else ""
    return f"{sign}${abs(n):,.2f}"


def pct(val: Any) -> str:
    if val is None or val == "":
        return ""
    try:
        n = float(val)
    except Exception:
        return ""
    return f"{n:.2f}%"


def parse_float(s: str) -> Optional[float]:
    s = (s or "").strip()
    if not s:
        return None
    s2 = s.replace("$", "").replace(",", "").replace("%", "").strip()
    try:
        return float(s2)
    except ValueError:
        return None


def parse_int(s: str) -> Optional[int]:
    s = (s or "").strip().replace(",", "")
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def parse_date_any(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass

    parts = re.split(r"[/-]", s)
    parts = [p for p in parts if p]
    if len(parts) == 2:
        try:
            m = int(parts[0])
            d = int(parts[1])
            y = now_et().year
            return date(y, m, d).isoformat()
        except Exception:
            return None
    return None


def month_bounds(d: date) -> Tuple[date, date]:
    first = d.replace(day=1)
    if d.month == 12:
        nxt = date(d.year + 1, 1, 1)
    else:
        nxt = date(d.year, d.month + 1, 1)
    return first, (nxt - timedelta(days=1))


def normalize_opt_type(s: str) -> str:
    s = (s or "").strip().upper()
    if s in ("CALL", "C"):
        return "CALL"
    if s in ("PUT", "P"):
        return "PUT"
    return s


def looks_like_header(line: str) -> bool:
    low = (line or "").lower()
    hits = sum(1 for h in _HEADER_HINTS if h in low)
    return hits >= 3


def split_row(line: str) -> List[str]:
    if "\t" in line:
        return [c.strip() for c in line.split("\t")]
    return [c.strip() for c in re.split(r"\s{2,}", line.strip())]


def detect_paste_format(text: str) -> str:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return "table"

    if (
        ("Instrument" in lines[0])
        and ("Transaction Time" in lines[0])
        and ("Direction" in lines[0])
    ):
        return "vanquish_statement"

    if looks_like_header(lines[0]):
        lines = lines[1:]
    if not lines:
        return "table"

    sample = lines[:3]
    broker_hits = 0
    for ln in sample:
        joined = " ".join(split_row(ln)).upper()
        if re.search(r"\b(BUY|SELL)\b", joined):
            broker_hits += 1
        if re.search(r"\b\d{1,2}/\d{1,2}/\d{2},\s*\d{1,2}:\d{2}\s*(AM|PM)\b", ln):
            broker_hits += 1
        if re.match(
            r"^[A-Z]{1,6}\s+[A-Z]{3}/\d{1,2}/\d{2}\s+\d+(\.\d+)?\s+(PUT|CALL)\b", ln.upper()
        ):
            broker_hits += 2

    return "broker" if broker_hits >= 3 else "table"


def _safe_col(col: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col):
        raise ValueError(f"Unsafe column name: {col}")
    return f'"{col}"'


def latest_balance_overall(as_of: Optional[str] = None) -> float:
    conn = db()
    starting = get_setting_float("starting_balance", 50000.0)

    cols = [r[1] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]

    def pick(existing: List[str], preferred: List[str]) -> Optional[str]:
        return next((c for c in preferred if c in existing), None)

    pnl_col = pick(cols, ["net_pl", "pnl", "profit_loss", "pl", "profit", "p_l", "net_pnl"])
    if not pnl_col:
        return float(starting)

    date_col = pick(cols, ["trade_date", "date", "day"])
    bal_col = pick(cols, ["balance", "running_balance", "equity", "account_balance"])

    # Prefer real trade-ledger balances over ACCT snapshot rows so stale snapshots
    # don't override true running balances.
    if bal_col:
        bal_q = _safe_col(bal_col)
        acct_filter = " AND COALESCE(ticker, '') <> 'ACCT'" if "ticker" in cols else ""
        if as_of and date_col:
            date_q = _safe_col(date_col)
            bal_row = conn.execute(
                f"""
                SELECT {bal_q}
                FROM trades
                WHERE {bal_q} IS NOT NULL AND {date_q} <= ?{acct_filter}
                ORDER BY {date_q} DESC, id DESC
                LIMIT 1
                """,
                (str(as_of),),
            ).fetchone()
        else:
            bal_row = conn.execute(
                f"""
                SELECT {bal_q}
                FROM trades
                WHERE {bal_q} IS NOT NULL{acct_filter}
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        if bal_row and bal_row[0] is not None:
            try:
                return float(bal_row[0])
            except Exception:
                pass

        # Fallback: if no real trade balances exist, allow ACCT snapshot balances.
        if as_of and date_col:
            date_q = _safe_col(date_col)
            bal_row = conn.execute(
                f"""
                SELECT {bal_q}
                FROM trades
                WHERE {bal_q} IS NOT NULL AND {date_q} <= ?
                ORDER BY {date_q} DESC, id DESC
                LIMIT 1
                """,
                (str(as_of),),
            ).fetchone()
        else:
            bal_row = conn.execute(
                f"""
                SELECT {bal_q}
                FROM trades
                WHERE {bal_q} IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        if bal_row and bal_row[0] is not None:
            try:
                return float(bal_row[0])
            except Exception:
                pass

    pnl_q = _safe_col(pnl_col)
    if as_of and date_col:
        date_q = _safe_col(date_col)
        row = conn.execute(
            f"SELECT COALESCE(SUM(CAST({pnl_q} AS REAL)), 0) FROM trades WHERE {date_q} <= ?",
            (str(as_of),),
        ).fetchone()
    else:
        row = conn.execute(f"SELECT COALESCE(SUM(CAST({pnl_q} AS REAL)), 0) FROM trades").fetchone()

    return float(starting + float(row[0] or 0.0))


def default_starting_balance() -> float:
    return latest_balance_overall() or 50000.0


def month_total_net(year: int, month: int) -> float:
    first = date(year, month, 1)
    nxt = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    with db() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            """,
            (first.isoformat(), nxt.isoformat()),
        ).fetchone()
    return float(row["net"] or 0.0)


def last_30d_total_net() -> float:
    end = now_et().date()
    start = end - timedelta(days=30)
    with db() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE trade_date >= ? AND trade_date <= ?
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchone()
    return float(row["net"] or 0.0)


def last_n_trading_day_totals(n: int = 20) -> List[float]:
    with db() as conn:
        rows = conn.execute(
            """
            SELECT trade_date, COALESCE(SUM(net_pl),0) AS net
            FROM trades
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 200
            """
        ).fetchall()

    out: List[float] = []
    for r in rows:
        try:
            d = datetime.strptime(r["trade_date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if d.weekday() >= 5:
            continue
        out.append(float(r["net"] or 0.0))
        if len(out) >= n:
            break
    return out


def projections_from_daily(
    daily_vals: List[float], base_balance: Optional[float]
) -> Dict[str, Any]:
    avg = (sum(daily_vals) / len(daily_vals)) if daily_vals else 0.0
    b0 = float(base_balance or 0.0)

    def proj(days: int) -> Dict[str, Any]:
        est = avg * days
        return {"days": days, "daily_avg": avg, "est_pnl": est, "est_balance": b0 + est}

    return {"avg": avg, "base_balance": b0, "p5": proj(5), "p10": proj(10), "p20": proj(20)}


def payout_summary(
    balance: Optional[float], protect_buffer: float = DEFAULT_PROTECT_BUFFER
) -> Dict[str, Any]:
    b = float(balance or 0.0)
    protect = float(protect_buffer or 0.0)
    buffer_reached = b >= PROFIT_BUFFER_LEVEL_50K
    max_request = max(0.0, b - FIXED_LOSS_LIMIT_50K) if buffer_reached else 0.0
    safe_floor = FIXED_LOSS_LIMIT_50K + protect
    safe_request = max(0.0, b - safe_floor) if buffer_reached else 0.0
    return {
        "balance": b,
        "buffer_reached": buffer_reached,
        "profit_buffer_level": PROFIT_BUFFER_LEVEL_50K,
        "fixed_loss_limit": FIXED_LOSS_LIMIT_50K,
        "protect_buffer": protect,
        "safe_floor": safe_floor,
        "max_request": max_request,
        "safe_request": safe_request,
    }
