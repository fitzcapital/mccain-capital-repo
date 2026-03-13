"""Shared runtime utilities and data-access helpers.

This module is intentionally independent of ``app_core`` so repositories/services
can import stable primitives without pulling the legacy monolith.
"""

from __future__ import annotations

import os
import re
import secrets
import shutil
import sqlite3
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/New_York")
PERSISTENT_DATA_DIR = os.environ.get("PERSISTENT_DATA_DIR", "persistent-data")
DB_PATH = os.environ.get("DB_PATH", os.path.join(PERSISTENT_DATA_DIR, "journal.db"))
BOOKS_DIR = os.environ.get("BOOKS_DIR", os.path.join(PERSISTENT_DATA_DIR, "books"))
UPLOAD_DIR = os.environ.get("UPLOAD_DIR", os.path.join(PERSISTENT_DATA_DIR, "uploads"))
BASE_MONTHLY_INCOME = float(os.environ.get("BASE_MONTHLY_INCOME", "7800"))
DEFAULT_PROTECT_BUFFER = float(os.environ.get("PAYOUT_PROTECT_BUFFER", "1000"))
PROFIT_BUFFER_LEVEL_50K = 52875.0
FIXED_LOSS_LIMIT_50K = 50375.0
SQLITE_BUSY_TIMEOUT_MS = max(1000, int(os.environ.get("SQLITE_BUSY_TIMEOUT_MS", "10000") or 10000))
SQLITE_SYNCHRONOUS = str(os.environ.get("SQLITE_SYNCHRONOUS", "NORMAL") or "NORMAL").upper()
SQLITE_JOURNAL_MODE = str(os.environ.get("SQLITE_JOURNAL_MODE", "WAL") or "WAL").upper()
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


def upload_root() -> str:
    return str(UPLOAD_DIR)


def books_root() -> str:
    return str(BOOKS_DIR)


def upload_path(*parts: str) -> str:
    root = upload_root()
    return os.path.join(root, *parts) if parts else root


def books_path(*parts: str) -> str:
    root = books_root()
    return os.path.join(root, *parts) if parts else root


def ensure_storage_dirs() -> None:
    db_dir = os.path.dirname(str(DB_PATH)) or "."
    os.makedirs(db_dir, exist_ok=True)
    os.makedirs(upload_root(), exist_ok=True)
    os.makedirs(books_root(), exist_ok=True)


def _secret_key_path() -> str:
    configured = str(os.environ.get("SECRET_KEY_FILE") or "").strip()
    if configured:
        return configured
    db_dir = os.path.dirname(os.path.abspath(str(DB_PATH))) or os.path.abspath(PERSISTENT_DATA_DIR)
    return os.path.join(db_dir, ".secret_key")


def secret_key_file_path() -> str:
    return _secret_key_path()


def load_or_create_secret_key() -> str:
    env_secret = str(os.environ.get("SECRET_KEY") or "").strip()
    if env_secret:
        return env_secret

    path = _secret_key_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            existing = handle.read().strip()
            if existing:
                return existing
    except FileNotFoundError:
        pass

    token = secrets.token_urlsafe(48)
    try:
        with open(path, "x", encoding="utf-8") as handle:
            handle.write(token)
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
        return token
    except FileExistsError:
        with open(path, "r", encoding="utf-8") as handle:
            return handle.read().strip()


def _apply_sqlite_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute(f"PRAGMA busy_timeout = {int(SQLITE_BUSY_TIMEOUT_MS)}")
    conn.execute("PRAGMA foreign_keys = ON")
    if str(DB_PATH) != ":memory:":
        journal_mode = SQLITE_JOURNAL_MODE if SQLITE_JOURNAL_MODE in {"WAL", "DELETE", "TRUNCATE"} else "WAL"
        synchronous = SQLITE_SYNCHRONOUS if SQLITE_SYNCHRONOUS in {"OFF", "NORMAL", "FULL", "EXTRA"} else "NORMAL"
        conn.execute(f"PRAGMA journal_mode = {journal_mode}")
        conn.execute(f"PRAGMA synchronous = {synchronous}")


def persistence_snapshot() -> Dict[str, Any]:
    ensure_storage_dirs()
    db_path = os.path.abspath(str(DB_PATH))
    upload_dir = os.path.abspath(upload_root())
    books_dir = os.path.abspath(books_root())
    secret_path = os.path.abspath(secret_key_file_path())
    snapshot: Dict[str, Any] = {
        "persistent_data_dir": os.path.abspath(str(PERSISTENT_DATA_DIR)),
        "db_path": db_path,
        "upload_dir": upload_dir,
        "books_dir": books_dir,
        "secret_key_file": secret_path,
        "db_exists": os.path.exists(db_path),
        "upload_dir_exists": os.path.isdir(upload_dir),
        "books_dir_exists": os.path.isdir(books_dir),
        "secret_key_exists": os.path.exists(secret_path),
        "sqlite_busy_timeout_ms": int(SQLITE_BUSY_TIMEOUT_MS),
        "sqlite_journal_mode": "",
        "sqlite_synchronous": SQLITE_SYNCHRONOUS,
        "disk_free_bytes": None,
        "disk_total_bytes": None,
    }
    try:
        usage = shutil.disk_usage(os.path.dirname(db_path) or ".")
        snapshot["disk_free_bytes"] = int(usage.free)
        snapshot["disk_total_bytes"] = int(usage.total)
    except OSError:
        pass
    try:
        with db() as conn:
            row = conn.execute("PRAGMA journal_mode").fetchone()
            if row:
                snapshot["sqlite_journal_mode"] = str(row[0] or "")
            row = conn.execute("PRAGMA synchronous").fetchone()
            if row:
                sync_map = {0: "OFF", 1: "NORMAL", 2: "FULL", 3: "EXTRA"}
                snapshot["sqlite_synchronous"] = sync_map.get(int(row[0]), str(row[0]))
    except sqlite3.DatabaseError:
        pass
    return snapshot


def db() -> sqlite3.Connection:
    ensure_storage_dirs()
    conn = sqlite3.connect(DB_PATH, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000.0)
    conn.row_factory = sqlite3.Row
    try:
        _apply_sqlite_pragmas(conn)
    except sqlite3.DatabaseError:
        pass
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def get_setting_value(key: str, default: Any = None) -> Any:
    with db() as conn:
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


def set_setting_value(key: str, value: Any) -> None:
    """Insert or update a setting in DB."""
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            """
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


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
    """
    Overall balance = starting_balance + cumulative net P/L.

    We intentionally derive this from net P/L instead of trusting stored per-row
    balance snapshots, which can be stale after imports/edits.
    """
    conn = db()
    starting = get_setting_float("starting_balance", 50000.0)

    cols = [r[1] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]

    def pick(existing: List[str], preferred: List[str]) -> Optional[str]:
        return next((c for c in preferred if c in existing), None)

    pnl_col = pick(cols, ["net_pl", "pnl", "profit_loss", "pl", "profit", "p_l", "net_pnl"])
    if not pnl_col:
        return float(starting)

    date_col = pick(cols, ["trade_date", "date", "day"])
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
