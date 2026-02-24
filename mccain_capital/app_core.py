"""
McCain Capital 🏛️ — Journal + Trades + Calendar + Calculator + Strategies + Books + Payouts
✅ FIXED: syntax errors, duplicate functions, missing helpers, bad indentation, "..." placeholders, missing balance parser,
         duplicate imports, conflicting parsers, and incomplete insert_balance_snapshot.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import tempfile
import calendar
import zipfile
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple, Callable
from flask import (
    Flask,
    abort,
    redirect,
    render_template,
    render_template_string,
    request,
    session,
    send_file,
    url_for,
    jsonify,
    flash,
)
from werkzeug.security import check_password_hash, generate_password_hash
from zoneinfo import ZoneInfo

BUILD_MARKER = "BUILD_2026-02-21_GOALS"

# ============================================================
# App config
# ============================================================
APP_TITLE = "McCain Capital 🏛️"
DB_PATH = os.environ.get("DB_PATH", "journal.db")
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

# ✅ Trading timezone (ET)
TZ = ZoneInfo("America/New_York")

# ✅ Books folder (no web-upload; you drop PDFs into this folder)
BOOKS_DIR = os.environ.get("BOOKS_DIR", "books")

# ✅ Calculator defaults (simple, not overkill)
MULTIPLIER = 100
DEFAULT_STOP_PCT = 20.0
DEFAULT_TARGET_PCT = 30.0
DEFAULT_FEE_PER_CONTRACT = 0.70  # per contract round-trip

UPLOAD_DIR = os.environ.get("UPLOAD_DIR", "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

ALLOWED_UPLOAD_EXTS = {".pdf", ".html", ".htm"}
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "25"))
APP_USERNAME = os.environ.get("APP_USERNAME", "owner")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")
APP_PASSWORD_HASH = os.environ.get("APP_PASSWORD_HASH", "")

_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_STATIC_DIR = os.path.join(_ROOT_DIR, "static")
_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

app = Flask(__name__, static_folder=_STATIC_DIR, static_url_path="/static", template_folder=_TEMPLATE_DIR)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_MB * 1024 * 1024


def auth_enabled() -> bool:
    return bool(_effective_password_hash())


def _effective_password_hash() -> str:
    db_hash = (get_setting_value("auth_password_hash", "") or "").strip()
    if db_hash:
        return db_hash
    if APP_PASSWORD_HASH:
        return APP_PASSWORD_HASH
    if APP_PASSWORD:
        return generate_password_hash(APP_PASSWORD)
    return ""


def _effective_username() -> str:
    db_user = (get_setting_value("auth_username", "") or "").strip()
    if db_user:
        return db_user
    return APP_USERNAME


def is_authenticated() -> bool:
    return bool(session.get("auth_ok")) and session.get("auth_user") == _effective_username()


# ============================================================
# DB helpers
# ============================================================
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        ).fetchone()
        return row is not None
    except Exception:
        return False


def get_setting_value(key: str, default=None):
    """Read a setting from the DB if a settings table exists; otherwise return default.

    Supports common schemas:
      - settings(key TEXT PRIMARY KEY, value TEXT)
      - settings(name TEXT PRIMARY KEY, value TEXT)
    """
    conn = db()
    if not _table_exists(conn, "settings"):
        return default

    # discover columns
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(settings)").fetchall()]
    except Exception:
        return default

    key_col = None
    for c in ("key", "name", "setting"):
        if c in cols:
            key_col = c
            break

    val_col = None
    for c in ("value", "val", "setting_value"):
        if c in cols:
            val_col = c
            break

    if not key_col or not val_col:
        return default

    try:
        row = conn.execute(
            f'SELECT "{val_col}" FROM settings WHERE "{key_col}" = ? LIMIT 1',
            (key,),
        ).fetchone()
        if not row:
            return default
        return row[0]
    except Exception:
        return default


def get_setting_float(key: str, default: float = 0.0) -> float:
    val = get_setting_value(key, None)
    if val is None:
        return float(default)
    try:
        return float(val)
    except Exception:
        return float(default)


def set_setting_value(key: str, value: str) -> None:
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


def init_db() -> None:
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
            """
            CREATE TABLE IF NOT EXISTS entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_date TEXT NOT NULL,
                market TEXT DEFAULT '',
                setup TEXT DEFAULT '',
                grade TEXT DEFAULT '',
                pnl REAL,
                mood TEXT DEFAULT '',
                notes TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_date ON entries(entry_date);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date TEXT NOT NULL,
                entry_time TEXT DEFAULT '',
                exit_time TEXT DEFAULT '',
                ticker TEXT DEFAULT '',
                opt_type TEXT DEFAULT '',
                strike REAL,
                entry_price REAL,
                exit_price REAL,
                contracts INTEGER,
                total_spent REAL,
                stop_pct REAL,
                target_pct REAL,
                stop_price REAL,
                take_profit REAL,
                risk REAL,
                comm REAL,
                gross_pl REAL,
                net_pl REAL,
                result_pct REAL,
                balance REAL,
                raw_line TEXT DEFAULT '',
                created_at TEXT NOT NULL
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(trade_date);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_ticker ON trades(ticker);")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER NOT NULL UNIQUE,
                setup_tag TEXT DEFAULT '',
                session_tag TEXT DEFAULT '',
                checklist_score INTEGER DEFAULT NULL,
                rule_break_tags TEXT DEFAULT '',
                review_note TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_reviews_trade_id ON trade_reviews(trade_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_reviews_setup ON trade_reviews(setup_tag);")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_controls (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                daily_max_loss REAL DEFAULT 0,
                enforce_lockout INTEGER DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO risk_controls (id, daily_max_loss, enforce_lockout, updated_at) VALUES (1, 0, 0, ?)",
            (now_iso(),),
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS strategies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_strategies_updated ON strategies(updated_at);")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_goals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                track_date TEXT NOT NULL UNIQUE,
                debt_paid REAL DEFAULT 0,
                debt_note TEXT DEFAULT '',
                upwork_proposals INTEGER DEFAULT 0,
                upwork_interviews INTEGER DEFAULT 0,
                upwork_hours REAL DEFAULT 0,
                upwork_earnings REAL DEFAULT 0,
                other_income REAL DEFAULT 0,
                notes TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_goals_date ON daily_goals(track_date);")


def prev_day_iso(d_iso: str) -> str:
    d = datetime.strptime(d_iso, "%Y-%m-%d").date()
    return (d - timedelta(days=1)).isoformat()


def next_day_iso(d_iso: str) -> str:
    d = datetime.strptime(d_iso, "%Y-%m-%d").date()
    return (d + timedelta(days=1)).isoformat()


def prev_trading_day_iso(d_iso: str) -> str:
    d = datetime.strptime(d_iso, "%Y-%m-%d").date() - timedelta(days=1)
    while d.weekday() >= 5:  # Sat/Sun
        d -= timedelta(days=1)
    return d.isoformat()


def next_trading_day_iso(d_iso: str) -> str:
    d = datetime.strptime(d_iso, "%Y-%m-%d").date() + timedelta(days=1)
    while d.weekday() >= 5:  # Sat/Sun
        d += timedelta(days=1)
    return d.isoformat()


# ============================================================
# Formatting helpers ✅ ($400.00 everywhere)
# ============================================================
def money(val: Any) -> str:
    if val is None or val == "":
        return ""
    try:
        n = float(val)
    except Exception:
        return ""
    sign = "-" if n < 0 else ""
    return f"{sign}${abs(n):,.2f}"


def default_starting_balance() -> float:
    return latest_balance_overall() or 50000.0


def money_compact(val: Any) -> str:
    if val is None or val == "":
        return ""
    try:
        n = float(val)
    except Exception:
        return ""
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 10000:
        return f"{sign}${n / 1000:.0f}k"
    if n >= 1000:
        return f"{sign}${n / 1000:.1f}k"
    if n >= 100:
        return f"{sign}${n:.0f}"
    return f"{sign}${n:.2f}"


def pct(val: Any) -> str:
    if val is None or val == "":
        return ""
    try:
        n = float(val)
    except Exception:
        return ""
    return f"{n:.2f}%"


# ============================================================
# Parse helpers
# ============================================================
_HEADER_HINTS = {"date", "entry", "exit", "ticker", "type", "strike", "contracts", "net", "p/l", "balance"}


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

    # allow "1/29" (assume current year)
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


def looks_like_header(line: str) -> bool:
    low = (line or "").lower()
    hits = sum(1 for h in _HEADER_HINTS if h in low)
    return hits >= 3


def split_row(line: str) -> List[str]:
    if "\t" in line:
        return [c.strip() for c in line.split("\t")]
    return [c.strip() for c in re.split(r"\s{2,}", line.strip())]


def normalize_opt_type(s: str) -> str:
    s = (s or "").strip().upper()
    if s in ("CALL", "C"):
        return "CALL"
    if s in ("PUT", "P"):
        return "PUT"
    return s


# ============================================================
# OCR helpers
# ============================================================
PLACEHOLDERS = {"-", "—", "–", "_", "—-", "—_"}


def normalize_ocr(s: str) -> str:
    return (s or "").replace("\u202f", " ").replace("\u00a0", " ").strip()


def clean_ocr_trade_row(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("—", "-").replace("–", "-").replace("_", "-").replace("|", " ")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\b(PUT|CALL)\s*[=+]\s+", r"\1 ", s, flags=re.IGNORECASE)
    return s


def _load_ocr_deps() -> Tuple[Optional[Callable[..., Any]], Optional[Any], Optional[Any], Optional[Any], Optional[Any], Optional[str]]:
    try:
        from pdf2image import convert_from_path as _convert_from_path
        import pytesseract as _pytesseract
        from PIL import Image as _Image, ImageEnhance as _ImageEnhance, ImageOps as _ImageOps
        return _convert_from_path, _pytesseract, _Image, _ImageEnhance, _ImageOps, None
    except Exception as e:
        return None, None, None, None, None, f"OCR dependencies missing/incompatible. Install pdf2image + pytesseract + Pillow. Error: {e}"


def _prep_for_ocr(img):
    _, _, _, ImageEnhance, ImageOps, _ = _load_ocr_deps()
    if ImageEnhance is None or ImageOps is None:
        return img
    img = img.convert("L")
    img = ImageOps.autocontrast(img)
    img = ImageEnhance.Sharpness(img).enhance(1.6)
    img = ImageEnhance.Contrast(img).enhance(1.4)
    return img


# ============================================================
# Vanquish OCR row stitching + parsing (single authoritative version)
# ============================================================
DT_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{2},\s*\d{1,2}:\d{2}\s*(?:AM|PM)\b", re.IGNORECASE)
SIDE_RE = re.compile(r"\b(BUY|SELL)\b", re.IGNORECASE)

TRADE_START_RE = re.compile(
    r"^(SPX|NDX|QQQ|SPY|ES|MES|NQ|MNQ)\s+[A-Z]{3}/\d{2}/\d{2}\s+\d+(?:\.\d+)?\s+(PUT|CALL)\b",
    re.IGNORECASE,
)

PREFIX_RE = re.compile(
    r"^(?P<instrument>(?:SPX|NDX|QQQ|SPY|ES|MES|NQ|MNQ)\s+[A-Z]{3}/\d{2}/\d{2}\s+\d+(?:\.\d+)?\s+(?:CALL|PUT))\s+"
    r"(?P<txcode>\d+:\d+)\s+"
    r"(?P<time>\d{1,2}/\d{1,2}/\d{2},\s*\d{1,2}:\d{2}\s*(?:AM|PM))\s+"
    r"(?P<side>BUY|SELL)\s+"
    r"(?P<size>\d+)\s+"
    r"(?P<price>\d+(?:\.\d+)?)\s+"
    r"(?P<orderid>\d+)\s+"
    r"(?P<ttype>[A-Z]+)\s+"
    r"(?P<tail>.+)$",
    re.IGNORECASE,
)


def is_complete_row(s: str) -> bool:
    s2 = normalize_ocr(s).upper()
    return bool(DT_RE.search(s2) and SIDE_RE.search(s2) and re.search(r"\b(PUT|CALL)\b", s2))


def stitch_ocr_rows(text: str) -> List[str]:
    out: List[str] = []
    buf = ""
    for raw in (text or "").splitlines():
        ln = normalize_ocr(raw)
        if not ln:
            continue
        buf = (buf + " " + ln).strip() if buf else ln
        if is_complete_row(buf):
            out.append(clean_ocr_trade_row(buf))
            buf = ""
    if buf.strip():
        out.append(clean_ocr_trade_row(buf.strip()))
    return out


def split_into_trade_lines(stitched_rows: List[str]) -> List[str]:
    """
    If OCR accidentally concatenates multiple trades in one line, split them.
    """
    out: List[str] = []
    for row in stitched_rows or []:
        if not row:
            continue
        s = " ".join(row.split())
        matches = list(TRADE_START_RE.finditer(s))
        if not matches:
            continue
        for i, m in enumerate(matches):
            start = m.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(s)
            chunk = s[start:end].strip()
            if chunk:
                out.append(clean_ocr_trade_row(chunk))
    return out


def _clean_money(tok: str) -> Optional[float]:
    if not tok or tok in PLACEHOLDERS:
        return None
    t = tok.replace(",", "")
    t = t.replace("—", "-").replace("–", "-")
    try:
        return float(t)
    except Exception:
        return None


def parse_vanquish_trade_line(line: str) -> Optional[Dict[str, Any]]:
    """
    Parses a SINGLE Vanquish transaction row (BUY leg or SELL leg).
    Returns dict used to convert into broker-paste format.
    """
    if not line:
        return None

    s = clean_ocr_trade_row(line)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace(" = ", " ").replace(" + ", " ")

    m = PREFIX_RE.match(s)
    if not m:
        return None

    d = {k: (v.strip() if isinstance(v, str) else v) for k, v in m.groupdict().items()}
    tail = d.pop("tail").split()

    # Tail mapping: cash_effect, realized_pl, commission, fee, total_cash_effect
    # OCR sometimes inserts bogus placeholder as first tail token.
    if len(tail) == 6 and tail[0] in PLACEHOLDERS:
        tail = tail[1:]
    if len(tail) < 5:
        return None

    cash_effect = _clean_money(tail[0])
    realized_pl = _clean_money(tail[1])
    commission = _clean_money(tail[2]) or 0.0
    fee = _clean_money(tail[3]) or 0.0
    total_cash = _clean_money(tail[4])

    d.update(
        {
            "instrument": d["instrument"].upper(),
            "txcode": d["txcode"],
            "time": normalize_ocr(d["time"]),
            "side": d["side"].upper(),
            "size": int(d["size"]),
            "price": float(d["price"]),
            "orderid": d["orderid"],
            "ttype": d["ttype"].upper(),
            "cash_effect": cash_effect,
            "realized_pl": realized_pl,
            "commission": float(commission),
            "fee": float(fee),
            "total_cash_effect": total_cash,
        }
    )
    return d


def vanquish_trades_to_broker_paste(trades: List[Dict[str, Any]]) -> str:
    """
    Convert Vanquish rows into a clean broker-paste line format:
    INSTRUMENT | dt | side | qty | price | fee
    """
    lines: List[str] = []
    for t in trades:
        dt = t.get("time", "")
        dt = re.sub(r"(\d{1,2}:\d{2})\s*(AM|PM)\b", r"\1 \2", dt, flags=re.IGNORECASE)
        fee = t.get("commission") or 0.70  # per-leg fallback
        lines.append(f"{t['instrument']} | {dt} | {t['side']} | {t['size']} | {t['price']} | {fee}")
    return "\n".join(lines)


# ============================================================
# HTML parsing helpers (robust column matching) ✅
# ============================================================
COL_ALIASES = {
    "Transaction Time": ["Transaction Time", "Time", "Date/Time", "Datetime", "TransactionTime"],
    "Direction": ["Direction", "Side", "Buy/Sell", "B/S", "Action"],
    "Instrument": ["Instrument", "Symbol", "Contract", "Description", "Instr"],
    "Size": ["Size", "Qty", "Quantity", "Contracts", "Contract(s)"],
    "Price": ["Price", "Fill Price", "Avg Price", "FillPrice", "Execution Price"],
    "Commission": ["Commission", "Comm", "Fees", "Fee", "Costs"],
    "Balance": ["Balance", "Account Value", "Net Liquidating Value", "Net Liq", "Ending Balance"],
}


def _pick_col(df, want: str) -> Optional[str]:
    """Return the first matching column name for a desired canonical column."""
    for name in COL_ALIASES.get(want, [want]):
        if name in df.columns:
            return name
    return None


# ============================================================
# HTML Statement parsing (Vanquish Account Statement) ✅
# ============================================================
def parse_statement_html_to_broker_paste(html_path: str) -> Tuple[str, Optional[float], List[str]]:
    """
    Reads Vanquish account statement HTML and returns:
      - broker paste lines: "instrument | dt | side | qty | price | fee"
      - balance (if found)
      - warnings
    """
    warnings: List[str] = []

    try:
        import pandas as pd  # local import so app can run without pandas until needed
    except Exception as e:
        return "", None, [f"pandas is required for HTML parsing (pip install pandas lxml). Error: {e}"]

    try:
        tables = pd.read_html(html_path)
    except Exception as e:
        return "", None, [f"Could not read HTML tables. Error: {e}"]

    if not tables:
        return "", None, ["No tables found in HTML statement."]

    # ---- Balance extraction (best effort across all tables) ----
    balance_val: Optional[float] = None
    try:
        # try any table that looks like key/value
        for tbl in tables[:4]:  # usually near the top, but keep it conservative
            if tbl.shape[1] < 2:
                continue
            for _, row in tbl.iterrows():
                k = str(row.iloc[0]).strip().lower()
                v = str(row.iloc[1]).strip()
                if k in ("balance", "ending balance", "account value", "net liquidating value", "net liq"):
                    maybe = parse_float(v)
                    if maybe is not None:
                        balance_val = maybe
                        break
            if balance_val is not None:
                break
    except Exception as e:
        warnings.append(f"Could not parse balance tables: {e}")

    # ---- Find the transactions table by required columns (with aliases) ----
    tx_tbl = None
    inst_c = time_c = side_c = qty_c = price_c = comm_c = None

    for cand in tables:
        inst_c = _pick_col(cand, "Instrument")
        time_c = _pick_col(cand, "Transaction Time")
        side_c = _pick_col(cand, "Direction")
        qty_c = _pick_col(cand, "Size")
        price_c = _pick_col(cand, "Price")
        comm_c = _pick_col(cand, "Commission")
        if all([inst_c, time_c, side_c, qty_c, price_c]):
            tx_tbl = cand
            break

    if tx_tbl is None:
        found_cols = [list(map(str, t.columns)) for t in tables[:3]]
        warnings.append("Could not locate a transactions table with expected columns.")
        warnings.append(f"Sample columns from first tables: {found_cols}")
        return "", balance_val, warnings

    lines: List[str] = []
    for _, r in tx_tbl.iterrows():
        try:
            instrument = str(r[inst_c]).strip().upper()
            dt = str(r[time_c]).replace("\u202f", " ").replace("\u00a0", " ").strip()
            side = str(r[side_c]).strip().upper()
            qty = parse_int(str(r[qty_c])) or 0
            price = parse_float(str(r[price_c]))

            # Prefer Commission/Fee column; fallback to DEFAULT_FEE_PER_CONTRACT
            fee = None
            if comm_c and comm_c in tx_tbl.columns:
                fee = parse_float(str(r.get(comm_c, "")))

            if fee is None:
                fee = DEFAULT_FEE_PER_CONTRACT

            if not instrument or side not in ("BUY", "SELL") or qty <= 0 or price is None:
                continue

            lines.append(f"{instrument} | {dt} | {side} | {qty} | {price} | {fee}")
        except Exception:
            continue

    if not lines:
        warnings.append("Found a transactions table but no usable transaction rows parsed.")

    return "\n".join(lines), balance_val, warnings


def ocr_pdf_to_broker_paste(pdf_path: str) -> Tuple[str, List[str]]:
    """
    OCR the PDF -> stitch lines -> parse Vanquish rows -> emit broker-paste lines.
    """
    warnings: List[str] = []

    convert_from_path, pytesseract, _, _, _, dep_error = _load_ocr_deps()
    if dep_error:
        return "", [dep_error]

    pages = convert_from_path(pdf_path, dpi=250)
    all_lines: List[str] = []
    for page_img in pages:
        img = _prep_for_ocr(page_img)
        ocr_text = pytesseract.image_to_string(img, config="--oem 3 --psm 6")
        raw_lines = [normalize_ocr(ln) for ln in ocr_text.splitlines() if normalize_ocr(ln)]
        all_lines.extend(raw_lines)

    if not all_lines:
        return "", ["OCR returned no text."]

    stitched = stitch_ocr_rows("\n".join(all_lines))
    stitched = split_into_trade_lines(stitched) or stitched

    trade_rows = [r for r in stitched if TRADE_START_RE.match(clean_ocr_trade_row(r))]
    if not trade_rows:
        warnings.append("No trade rows found (rows starting with SPX/NDX/QQQ/etc).")
        warnings.append(f"Stitched {len(stitched)} rows.")
        return "", warnings

    parsed: List[Dict[str, Any]] = []
    for ln in trade_rows:
        p = parse_vanquish_trade_line(ln)
        if p:
            parsed.append(p)

    if not parsed:
        warnings.append("Trade rows found but none parsed. Parser regex likely too strict.")
        warnings.append(f"Trade rows found: {len(trade_rows)}")
        warnings.append("Sample trade row:")
        warnings.append(trade_rows[0][:250])
        return "", warnings

    paste_text = vanquish_trades_to_broker_paste(parsed)
    return paste_text, warnings


def ocr_pdf_to_text(pdf_path: str) -> Tuple[str, List[str]]:
    warnings: List[str] = []
    convert_from_path, pytesseract, _, _, _, dep_error = _load_ocr_deps()
    if dep_error:
        return "", [dep_error]
    pages = convert_from_path(pdf_path, dpi=250)
    all_text: List[str] = []
    for page_img in pages:
        img = _prep_for_ocr(page_img)
        txt = pytesseract.image_to_string(img, config="--oem 3 --psm 6")
        all_text.append(txt)
    text = "\n".join(all_text).strip()
    if not text:
        warnings.append("OCR returned empty text.")
    return text, warnings


# ============================================================
# Statement balance extraction (FIXED: was missing)
# ============================================================
BALANCE_RE_LIST = [
    re.compile(r"\bEnding\s+Balance\b[^0-9$-]*\$?\s*([-–—]?\s*[\d,]+\.\d{2})", re.IGNORECASE),
    re.compile(r"\bNet\s+Liquidating\s+Value\b[^0-9$-]*\$?\s*([-–—]?\s*[\d,]+\.\d{2})", re.IGNORECASE),
    re.compile(r"\bAccount\s+Value\b[^0-9$-]*\$?\s*([-–—]?\s*[\d,]+\.\d{2})", re.IGNORECASE),
    re.compile(r"\bBalance\b[^0-9$-]*\$?\s*([-–—]?\s*[\d,]+\.\d{2})", re.IGNORECASE),
]


def extract_statement_balance(text: str) -> Optional[float]:
    """
    Pull an ending balance-ish number from statement OCR text.
    Tries multiple common label patterns.
    """
    t = (text or "").replace("\u202f", " ").replace("\u00a0", " ")
    for rx in BALANCE_RE_LIST:
        m = rx.search(t)
        if m:
            s = m.group(1).replace("—", "-").replace("–", "-").replace("$", "")
            s = re.sub(r"\s+", "", s)
            return parse_float(s)
    return None


# ============================================================
# Broker fills paste -> round-trip trades ✅
# ============================================================
MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def parse_broker_dt(s: str) -> Tuple[Optional[str], str]:
    s = (s or "").strip().replace("\u202f", " ").replace("\u00a0", " ")
    # Accept "1/30/26, 10:30 AM" or "1/30/26, 10:30AM"
    s = re.sub(r"(\d{1,2}:\d{2})\s*(AM|PM)\b", r"\1 \2", s, flags=re.IGNORECASE)
    try:
        dt = datetime.strptime(s, "%m/%d/%y, %I:%M %p")
        # portable time formatting
        hour = dt.strftime("%I").lstrip("0") or "0"
        return dt.date().isoformat(), f"{hour}:{dt.strftime('%M %p')}"
    except Exception:
        return None, ""


def parse_contract_desc(desc: str) -> Dict[str, Any]:
    """
    'SPX JAN/30/26 6935 PUT' -> ticker=SPX, expiry=2026-01-30, strike=6935, opt_type=PUT
    """
    desc = (desc or "").strip().replace("\u202f", " ").replace("\u00a0", " ")
    parts = desc.split()
    if len(parts) < 4:
        return {"ticker": "", "expiry": None, "strike": None, "opt_type": ""}

    ticker = parts[0].upper()
    exp_raw = parts[1].upper()
    exp_bits = exp_raw.split("/")
    expiry_iso = None
    try:
        mon = MONTH_MAP.get(exp_bits[0], None)
        day = int(exp_bits[1])
        yy = int(exp_bits[2])
        year = 2000 + yy if yy < 100 else yy
        if mon:
            expiry_iso = date(year, mon, day).isoformat()
    except Exception:
        expiry_iso = None

    strike = parse_float(parts[2])
    opt_type = normalize_opt_type(parts[3])
    return {"ticker": ticker, "expiry": expiry_iso, "strike": strike, "opt_type": opt_type}


BROKER_OCR_RE = re.compile(
    r"^(?P<desc>[A-Z]{1,6}\s+[A-Z]{3}/\d{1,2}/\d{2}\s+\d+(?:\.\d+)?\s+(?:PUT|CALL))\s+"
    r"(?P<dt>\d{1,2}/\d{1,2}/\d{2},\s*\d{1,2}:\d{2}\s*(?:AM|PM))\s+"
    r"(?P<side>BUY|SELL)\s+"
    r"(?P<qty>\d+)\s+"
    r"(?P<price>\d+(?:\.\d+)?)"
    r"(?:.*?\b(?P<fee>\d+(?:\.\d+)?)\b)?\s*$",
    re.IGNORECASE,
)


def parse_broker_line_any(ln: str) -> Optional[Dict[str, Any]]:
    raw = (ln or "").replace("\u202f", " ").replace("\u00a0", " ").strip()
    if not raw:
        return None

    # ✅ Handle "instrument | dt | side | qty | price | fee"
    if "|" in raw:
        parts = [p.strip() for p in raw.split("|") if p.strip()]
        if len(parts) >= 5:
            desc = parts[0].upper()
            dt = parts[1]
            side = parts[2].upper()
            qty = parse_int(parts[3]) or 0
            price = parse_float(parts[4])
            fee = 0.70
            if len(parts) >= 6:
                maybe_fee = parse_float(parts[5])
                if maybe_fee is not None and 0 <= maybe_fee <= 5:
                    fee = float(maybe_fee)
            if desc and dt and side in ("BUY", "SELL") and qty > 0 and price is not None:
                return {"desc": desc, "dt": dt, "side": side, "qty": qty, "price": float(price), "fee": float(fee)}

    # 1) OCR regex
    m = BROKER_OCR_RE.match(raw.upper())
    if m:
        fee = parse_float(m.group("fee") or "")
        fee = float(fee) if fee is not None and 0 <= fee <= 5 else 0.70
        return {
            "desc": m.group("desc"),
            "dt": m.group("dt"),
            "side": m.group("side").upper(),
            "qty": int(m.group("qty")),
            "price": float(m.group("price")),
            "fee": fee,
        }

    # 2) Tab/space paste fallback
    cols = split_row(raw)
    if len(cols) >= 6:
        desc = cols[0]
        dt = cols[2] if len(cols) > 2 else ""
        side = (cols[3] if len(cols) > 3 else "").strip().upper()
        qty = parse_int(cols[4] if len(cols) > 4 else "") or 0
        price = parse_float(cols[5] if len(cols) > 5 else "")

        fee = 0.70
        tail = cols[-6:] if len(cols) >= 6 else cols
        for token in reversed(tail):
            v = parse_float(token)
            if v is not None and 0 <= v <= 5:
                fee = float(v)
                break

        if desc and dt and side in ("BUY", "SELL") and qty > 0 and price is not None:
            return {"desc": desc, "dt": dt, "side": side, "qty": qty, "price": float(price), "fee": float(fee)}

    return None


def insert_trades_from_broker_paste(text: str, starting_balance: float = 50000.0) -> Tuple[int, List[str]]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return 0, ["Nothing to import."]

    created = now_iso()
    errors: List[str] = []
    warnings: List[str] = []

    # 1) Parse ALL fills first
    fills: List[Dict[str, Any]] = []
    for i, ln in enumerate(lines, start=1):
        parsed = parse_broker_line_any(ln)
        if not parsed:
            errors.append(f"Line {i}: could not parse broker row.")
            continue

        desc = parsed["desc"]
        trade_date, tm = parse_broker_dt(parsed["dt"])
        side = parsed["side"]
        qty = parsed["qty"]
        price = parsed["price"]
        fee = parsed["fee"]

        info = parse_contract_desc(desc)
        ticker = info["ticker"]
        strike = info["strike"]
        opt_type = info["opt_type"]
        expiry = info.get("expiry") or ""

        if not ticker or strike is None or not opt_type:
            errors.append(f"Line {i}: can't parse contract desc '{desc}'")
            continue
        if not trade_date or not tm:
            errors.append(f"Line {i}: can't parse datetime '{parsed['dt']}'")
            continue
        if qty <= 0 or price is None:
            errors.append(f"Line {i}: bad qty/price (qty={qty}, price={price})")
            continue
        if side not in ("BUY", "SELL"):
            errors.append(f"Line {i}: side must be BUY/SELL, got '{side}'")
            continue

        key = f"{ticker}|{expiry}|{strike}|{opt_type}"

        # build a sortable datetime
        try:
            dt_obj = datetime.strptime(f"{trade_date} {tm}", "%Y-%m-%d %I:%M %p")
        except Exception:
            dt_obj = None

        fills.append(
            {
                "key": key,
                "ticker": ticker,
                "expiry": expiry,
                "strike": strike,
                "opt_type": opt_type,
                "trade_date": trade_date,
                "tm": tm,
                "dt_obj": dt_obj,
                "side": side,
                "qty": int(qty),
                "price": float(price),
                "fee": float(fee),
                "raw_line": ln,
                "line_no": i,
            }
        )

    if not fills:
        return 0, (errors or ["No valid fills to import."])

    # 2) Sort fills oldest -> newest (fixes reverse-chronological statements)
    def side_rank(s: str) -> int:
        return 0 if s == "BUY" else 1  # if same timestamp, BUY first

    fills_sorted = sorted(
        fills,
        key=lambda f: (
            f["dt_obj"] if f["dt_obj"] else datetime.max,
            side_rank(f["side"]),
            f["line_no"],
        ),
    )

    # optional: detect reverse chrono input to inform you
    if fills_sorted and fills_sorted[0]["line_no"] != 1:
        warnings.append("Detected out-of-order fills; sorted by datetime before pairing. ✅")

    # 3) Pair using FIFO per contract key
    open_lots: Dict[str, List[Dict[str, Any]]] = {}
    completed: List[Dict[str, Any]] = []

    for f in fills_sorted:
        key = f["key"]
        side = f["side"]
        qty = f["qty"]
        price = f["price"]
        fee = f["fee"]

        if side == "BUY":
            open_lots.setdefault(key, []).append(
                {
                    "qty": qty,
                    "entry_price": price,
                    "entry_time": f["tm"],
                    "trade_date": f["trade_date"],
                    "fees": fee,
                }
            )
            continue

        # SELL
        if key not in open_lots or not open_lots[key]:
            errors.append(f"Line {f['line_no']}: SELL with no matching BUY open lot for {key}")
            continue

        remaining = qty
        while remaining > 0 and open_lots[key]:
            lot = open_lots[key][0]
            take = min(remaining, int(lot["qty"]))
            remaining -= take
            lot["qty"] -= take

            entry_price = float(lot["entry_price"])
            exit_price = float(price)

            gross_pl = (exit_price - entry_price) * 100.0 * take
            comm = float(lot["fees"]) + float(fee)
            net_pl = gross_pl - comm

            completed.append(
                {
                    "trade_date": lot["trade_date"],  # entry date
                    "entry_time": lot["entry_time"],
                    "exit_time": f["tm"],
                    "ticker": f["ticker"],
                    "opt_type": f["opt_type"],
                    "strike": f["strike"],
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "contracts": take,
                    "total_spent": entry_price * 100.0 * take,
                    "comm": comm,
                    "gross_pl": gross_pl,
                    "net_pl": net_pl,
                    "result_pct": (net_pl / (entry_price * 100.0 * take) * 100.0) if entry_price > 0 else None,
                    "raw_line": f["raw_line"],
                }
            )

            if lot["qty"] <= 0:
                open_lots[key].pop(0)

        if remaining > 0:
            errors.append(f"Line {f['line_no']}: SELL qty exceeds open BUY qty for {key} (extra {remaining})")

    # 4) Insert completed trades
    inserted = 0
    balance = float(starting_balance)

    with db() as conn:
        conn.execute("BEGIN")
        for tr in completed:
            balance += float(tr["net_pl"] or 0.0)
            cur = conn.execute(
                """
                INSERT INTO trades (
                    trade_date, entry_time, exit_time, ticker, opt_type, strike,
                    entry_price, exit_price, contracts, total_spent,
                    stop_pct, target_pct, stop_price, take_profit,
                    risk, comm, gross_pl, net_pl, result_pct, balance,
                    raw_line, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    tr["trade_date"],
                    tr["entry_time"],
                    tr["exit_time"],
                    tr["ticker"],
                    tr["opt_type"],
                    tr["strike"],
                    tr["entry_price"],
                    tr["exit_price"],
                    tr["contracts"],
                    tr["total_spent"],
                    None, None, None, None, None,
                    tr["comm"],
                    tr["gross_pl"],
                    tr["net_pl"],
                    tr["result_pct"],
                    balance,
                    tr["raw_line"],
                    created,
                ),
            )
            trade_id = int(cur.lastrowid)
            payload = _auto_review_payload(tr)
            conn.execute(
                """
                INSERT OR IGNORE INTO trade_reviews
                  (trade_id, setup_tag, session_tag, checklist_score, rule_break_tags, review_note, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade_id,
                    payload.get("setup_tag", ""),
                    payload.get("session_tag", ""),
                    payload.get("checklist_score", None),
                    payload.get("rule_break_tags", ""),
                    payload.get("review_note", ""),
                    created,
                    created,
                ),
            )
            inserted += 1
        conn.commit()

    open_count = sum(sum(lot["qty"] for lot in lots) for lots in open_lots.values() if lots)
    if open_count:
        warnings.append(f"Note: {open_count} contract(s) remain OPEN (unmatched BUY). That’s normal mid-position.")

    # return inserted + combined messages
    return inserted, (warnings + errors)


# ============================================================
# Journal CRUD
# ============================================================
def fetch_entries(q: str = "", d: str = "") -> List[sqlite3.Row]:
    q = (q or "").strip()
    d = (d or "").strip()

    sql = "SELECT * FROM entries"
    where = []
    params: List[Any] = []

    if d:
        where.append("entry_date = ?")
        params.append(d)

    if q:
        where.append("(notes LIKE ? OR market LIKE ? OR setup LIKE ? OR grade LIKE ? OR mood LIKE ?)")
        like = f"%{q}%"
        params.extend([like, like, like, like, like])

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY entry_date DESC, updated_at DESC"

    with db() as conn:
        return list(conn.execute(sql, params).fetchall())


def get_entry(entry_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM entries WHERE id = ?", (entry_id,)).fetchone()


def create_entry(data: Dict[str, Any]) -> int:
    created = now_iso()
    updated = created
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO entries (entry_date, market, setup, grade, pnl, mood, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data.get("entry_date") or today_iso(),
                (data.get("market") or "").strip(),
                (data.get("setup") or "").strip(),
                (data.get("grade") or "").strip(),
                data.get("pnl"),
                (data.get("mood") or "").strip(),
                (data.get("notes") or "").strip(),
                created,
                updated,
            ),
        )
        return int(cur.lastrowid)


def update_entry(entry_id: int, data: Dict[str, Any]) -> None:
    updated = now_iso()
    with db() as conn:
        conn.execute(
            """
            UPDATE entries
            SET entry_date = ?, market = ?, setup = ?, grade = ?, pnl = ?, mood = ?, notes = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                data.get("entry_date") or today_iso(),
                (data.get("market") or "").strip(),
                (data.get("setup") or "").strip(),
                (data.get("grade") or "").strip(),
                data.get("pnl"),
                (data.get("mood") or "").strip(),
                (data.get("notes") or "").strip(),
                updated,
                entry_id,
            ),
        )


def delete_entry(entry_id: int) -> None:
    with db() as conn:
        conn.execute("DELETE FROM entries WHERE id = ?", (entry_id,))


# ============================================================
# Trades (table paste)
# ============================================================
def insert_trades_from_paste(text: str) -> Tuple[int, List[str]]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return 0, ["Nothing to import."]

    if looks_like_header(lines[0]):
        lines = lines[1:]

    inserted = 0
    errors: List[str] = []
    created = now_iso()

    with db() as conn:
        conn.execute("BEGIN")
        for i, ln in enumerate(lines, start=1):
            cols = split_row(ln)

            if len(cols) < 10:
                errors.append(f"Line {i}: too few columns (got {len(cols)}). Use tab-delimited paste.")
                continue

            trade_date = parse_date_any(cols[0])
            if not trade_date:
                errors.append(f"Line {i}: bad date '{cols[0]}' (try 1/29 or 01/29/2026 or 2026-01-29).")
                continue

            def c(idx: int) -> str:
                return cols[idx] if idx < len(cols) else ""

            row = {
                "trade_date": trade_date,
                "entry_time": c(1),
                "exit_time": c(2),
                "ticker": c(3).upper(),
                "opt_type": normalize_opt_type(c(4)),
                "strike": parse_float(c(5)),
                "entry_price": parse_float(c(6)),
                "exit_price": parse_float(c(7)),
                "contracts": parse_int(c(8)),
                "total_spent": parse_float(c(9)),
                "stop_pct": parse_float(c(10)),
                "target_pct": parse_float(c(11)),
                "stop_price": parse_float(c(12)),
                "take_profit": parse_float(c(13)),
                "risk": parse_float(c(14)),
                "comm": parse_float(c(15)),
                "gross_pl": parse_float(c(16)),
                "net_pl": parse_float(c(17)),
                "result_pct": parse_float(c(18)),
                "balance": parse_float(c(19)),
                "raw_line": ln,
            }

            if not row["ticker"]:
                errors.append(f"Line {i}: missing ticker")
                continue

            if row["net_pl"] is None and row["gross_pl"] is not None:
                row["net_pl"] = row["gross_pl"]

            cur = conn.execute(
                """
                INSERT INTO trades (
                    trade_date, entry_time, exit_time, ticker, opt_type, strike,
                    entry_price, exit_price, contracts, total_spent,
                    stop_pct, target_pct, stop_price, take_profit,
                    risk, comm, gross_pl, net_pl, result_pct, balance,
                    raw_line, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row["trade_date"],
                    row["entry_time"],
                    row["exit_time"],
                    row["ticker"],
                    row["opt_type"],
                    row["strike"],
                    row["entry_price"],
                    row["exit_price"],
                    row["contracts"],
                    row["total_spent"],
                    row["stop_pct"],
                    row["target_pct"],
                    row["stop_price"],
                    row["take_profit"],
                    row["risk"],
                    row["comm"],
                    row["gross_pl"],
                    row["net_pl"],
                    row["result_pct"],
                    row["balance"],
                    row["raw_line"],
                    created,
                ),
            )
            trade_id = int(cur.lastrowid)
            payload = _auto_review_payload(row)
            conn.execute(
                """
                INSERT OR IGNORE INTO trade_reviews
                  (trade_id, setup_tag, session_tag, checklist_score, rule_break_tags, review_note, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade_id,
                    payload.get("setup_tag", ""),
                    payload.get("session_tag", ""),
                    payload.get("checklist_score", None),
                    payload.get("rule_break_tags", ""),
                    payload.get("review_note", ""),
                    created,
                    created,
                ),
            )
            inserted += 1

        conn.commit()

    return inserted, errors


def fetch_trades(d: str = "", q: str = "") -> List[sqlite3.Row]:
    from mccain_capital.repositories import trades as repo
    return repo.fetch_trades(d=d, q=q)


def get_risk_controls() -> Dict[str, Any]:
    from mccain_capital.repositories import trades as repo
    return repo.get_risk_controls()


def save_risk_controls(daily_max_loss: float, enforce_lockout: int) -> None:
    from mccain_capital.repositories import trades as repo
    repo.save_risk_controls(daily_max_loss=daily_max_loss, enforce_lockout=enforce_lockout)


def day_net_total(day_iso: str) -> float:
    with db() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(net_pl), 0) AS total FROM trades WHERE trade_date = ?",
            (day_iso,),
        ).fetchone()
    return float((row["total"] if row else 0.0) or 0.0)


def trade_lockout_state(day_iso: Optional[str] = None) -> Dict[str, Any]:
    day_iso = day_iso or today_iso()
    rc = get_risk_controls()
    day_net = day_net_total(day_iso)
    max_loss = abs(float(rc["daily_max_loss"] or 0.0))
    locked = bool(rc["enforce_lockout"]) and max_loss > 0 and day_net <= (-max_loss)
    return {
        "day": day_iso,
        "day_net": day_net,
        "daily_max_loss": max_loss,
        "enforce_lockout": int(rc["enforce_lockout"]),
        "locked": locked,
    }


def get_trade_review(trade_id: int) -> Optional[Dict[str, Any]]:
    from mccain_capital.repositories import trades as repo
    return repo.get_trade_review(trade_id=trade_id)


def upsert_trade_review(
    trade_id: int,
    setup_tag: str = "",
    session_tag: str = "",
    checklist_score: Optional[int] = None,
    rule_break_tags: str = "",
    review_note: str = "",
) -> None:
    from mccain_capital.repositories import trades as repo
    repo.upsert_trade_review(
        trade_id=trade_id,
        setup_tag=setup_tag,
        session_tag=session_tag,
        checklist_score=checklist_score,
        rule_break_tags=rule_break_tags,
        review_note=review_note,
    )


def fetch_trade_reviews_map(trade_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    from mccain_capital.repositories import trades as repo
    return repo.fetch_trade_reviews_map(trade_ids=trade_ids)


def _parse_ampm_time(s: str) -> Optional[datetime]:
    s = (s or "").strip().upper()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%I:%M %p")
    except Exception:
        return None


def _infer_session_tag(entry_time: str) -> str:
    t = _parse_ampm_time(entry_time)
    if not t:
        return "Unlabeled"
    h = t.hour
    if h < 11:
        return "Open"
    if h < 14:
        return "Midday"
    if h < 16:
        return "Power Hour"
    return "After Hours"


def _auto_review_payload(trade: Dict[str, Any]) -> Dict[str, Any]:
    """Heuristic checklist score for imported trades."""
    net = float(trade.get("net_pl") or 0.0)
    total_spent = float(trade.get("total_spent") or 0.0)
    comm = float(trade.get("comm") or 0.0)
    contracts = int(trade.get("contracts") or 0)
    rp = trade.get("result_pct")
    rp = float(rp) if rp is not None else None
    score = 78
    tags: List[str] = []

    # Outcome impact
    if net > 0:
        score += 8
    elif net < 0:
        score -= 10

    if rp is not None:
        if rp >= 20:
            score += 6
        elif rp <= -20:
            score -= 12

    # Friction/efficiency
    fee_ratio = (comm / total_spent) if total_spent > 0 else 0.0
    if fee_ratio > 0.02:
        score -= 8
        tags.append("high-fee-ratio")
    elif fee_ratio > 0.01:
        score -= 4

    # Hold quality proxy from timestamp spread
    entry_dt = _parse_ampm_time(str(trade.get("entry_time") or ""))
    exit_dt = _parse_ampm_time(str(trade.get("exit_time") or ""))
    if entry_dt and exit_dt:
        hold_min = int((exit_dt - entry_dt).total_seconds() // 60)
        if hold_min < 2:
            score -= 6
            tags.append("ultra-short-hold")
        elif hold_min > 120:
            score -= 3
            tags.append("extended-hold")

    if contracts >= 10:
        score -= 3
        tags.append("size-heavy")

    score = max(35, min(95, int(round(score))))
    return {
        "setup_tag": "Statement Import",
        "session_tag": _infer_session_tag(str(trade.get("entry_time") or "")),
        "checklist_score": score,
        "rule_break_tags": ", ".join(sorted(set(tags))),
        "review_note": "Auto-generated from imported statement data. Edit this review if needed.",
    }


def upsert_trade_review_if_missing(trade_id: int, payload: Dict[str, Any]) -> None:
    with db() as conn:
        row = conn.execute("SELECT 1 FROM trade_reviews WHERE trade_id = ? LIMIT 1", (trade_id,)).fetchone()
    if row:
        return
    upsert_trade_review(
        trade_id=trade_id,
        setup_tag=payload.get("setup_tag", ""),
        session_tag=payload.get("session_tag", ""),
        checklist_score=payload.get("checklist_score", None),
        rule_break_tags=payload.get("rule_break_tags", ""),
        review_note=payload.get("review_note", ""),
    )


def backfill_auto_reviews_for_unreviewed() -> int:
    with db() as conn:
        rows = conn.execute(
            """
            SELECT t.id, t.entry_time, t.exit_time, t.contracts, t.total_spent, t.comm, t.net_pl, t.result_pct
            FROM trades t
            LEFT JOIN trade_reviews r ON r.trade_id = t.id
            WHERE r.trade_id IS NULL
            ORDER BY t.id ASC
            """
        ).fetchall()
    count = 0
    for r in rows:
        payload = _auto_review_payload(dict(r))
        upsert_trade_review_if_missing(int(r["id"]), payload)
        count += 1
    return count


def latest_balance_overall(as_of: str | None = None) -> float:
    """Overall balance = starting_balance + sum(PnL up to `as_of` (inclusive).

    This is schema-tolerant:
      - picks the first existing PnL column from a preferred list
      - optionally filters by a trade date column if present
    """
    conn = db()

    starting = get_setting_float("starting_balance", 50000.0)

    # Introspect columns so we don't break on older DBs
    cols = []
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]
    except Exception:
        return float(starting)

    def _pick(existing: list[str], preferred: list[str]) -> str | None:
        for c in preferred:
            if c in existing:
                return c
        return None

    pnl_col = _pick(
        cols,
        [
            "net_pl",
            "pnl",
            "profit_loss",
            "pl",
            "profit",
            "p_l",
            "net_pnl",
        ],
    )
    if not pnl_col:
        return float(starting)

    date_col = _pick(cols, ["trade_date", "date", "day"])

    # Validate column names and quote them
    def _q(col: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col):
            raise ValueError(f"Unsafe column name: {col}")
        return f'"{col}"'

    try:
        pnl_q = _q(pnl_col)
        if as_of and date_col:
            date_q = _q(date_col)
            row = conn.execute(
                f"SELECT COALESCE(SUM(CAST({pnl_q} AS REAL)), 0) FROM trades WHERE {date_q} <= ?",
                (str(as_of),),
            ).fetchone()
        else:
            row = conn.execute(
                f"SELECT COALESCE(SUM(CAST({pnl_q} AS REAL)), 0) FROM trades"
            ).fetchone()
        total = float(row[0] or 0.0)
    except Exception:
        total = 0.0

    return float(starting + total)


def last_balance_in_list(trades: List[sqlite3.Row]) -> Optional[float]:
    for t in trades:
        b = t["balance"]
        if b is not None:
            try:
                return float(b)
            except Exception:
                return None
    return None


def trade_day_stats(trades: List[sqlite3.Row]) -> Dict[str, Any]:
    total = 0.0
    wins = 0
    losses = 0
    for t in trades:
        net = t["net_pl"]
        if net is None:
            continue
        total += float(net)
        if net > 0:
            wins += 1
        elif net < 0:
            losses += 1

    total_trades = wins + losses
    win_rate = (wins / total_trades * 100.0) if total_trades else 0.0
    wl_ratio = (wins / losses) if losses else (float(wins) if wins else 0.0)

    return {"total": total, "wins": wins, "losses": losses, "total_trades": total_trades, "win_rate": win_rate,
            "wl_ratio": wl_ratio}


def week_range_for(day_iso: Optional[str]) -> Tuple[str, str]:
    d = datetime.strptime(day_iso or today_iso(), "%Y-%m-%d").date()
    start = d - timedelta(days=d.weekday())  # Monday
    end = start + timedelta(days=7)
    return start.isoformat(), end.isoformat()


def week_total_net(day_iso: Optional[str]) -> float:
    start, end = week_range_for(day_iso)
    with db() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            """,
            (start, end),
        ).fetchone()
    return float(row["net"] or 0.0)


def month_heatmap(year: int, month: int) -> Dict[str, Any]:
    first = date(year, month, 1)
    nxt = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    days_in_month = (nxt - first).days

    with db() as conn:
        rows = conn.execute(
            """
            SELECT trade_date, COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            GROUP BY trade_date
            """,
            (first.isoformat(), nxt.isoformat()),
        ).fetchall()

        bal_rows = conn.execute(
            """
            SELECT trade_date, balance, id
            FROM trades
            WHERE trade_date >= ? AND trade_date < ? AND balance IS NOT NULL
            ORDER BY trade_date ASC, id ASC
            """,
            (first.isoformat(), nxt.isoformat()),
        ).fetchall()

    daily_net = {r["trade_date"]: float(r["net"] or 0) for r in rows}

    daily_balance: Dict[str, float] = {}
    for r in bal_rows:
        try:
            daily_balance[r["trade_date"]] = float(r["balance"])
        except Exception:
            pass

    # Sunday-start calendar grid
    start_weekday = (first.weekday() + 1) % 7  # Mon=0 -> Sun=0
    cells: List[Tuple[Optional[int], float, str, Optional[int]]] = []
    for _ in range(start_weekday):
        cells.append((None, 0.0, "", None))

    max_abs = 0.0
    for daynum in range(1, days_in_month + 1):
        iso = date(year, month, daynum).isoformat()
        net = daily_net.get(iso, 0.0)
        wd = date(year, month, daynum).weekday()
        max_abs = max(max_abs, abs(net))
        cells.append((daynum, net, iso, wd))

    while len(cells) % 7 != 0:
        cells.append((None, 0.0, "", None))

    weeks = [cells[i: i + 7] for i in range(0, len(cells), 7)]
    return {"year": year, "month": month, "weeks": weeks, "max_abs": max_abs or 1.0, "daily_balance": daily_balance}


def clear_trades() -> None:
    with db() as conn:
        conn.execute("DELETE FROM trades")


def allowed_file(filename: str) -> bool:
    name = (filename or "").lower().strip()
    _, ext = os.path.splitext(name)
    return ext in {".pdf", ".html", ".htm"}


def detect_paste_format(text: str) -> str:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return "table"

    # Vanquish statement table paste
    if ("Instrument" in lines[0]) and ("Transaction Time" in lines[0]) and ("Direction" in lines[0]):
        return "vanquish_statement"

    # existing logic...
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
        if re.match(r"^[A-Z]{1,6}\s+[A-Z]{3}/\d{1,2}/\d{2}\s+\d+(\.\d+)?\s+(PUT|CALL)\b", ln.upper()):
            broker_hits += 2

    return "broker" if broker_hits >= 3 else "table"


# ============================================================
# Projections (Mon–Fri only) ✅
# ============================================================
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


def safe_avg(vals: List[float]) -> float:
    return (sum(vals) / len(vals)) if vals else 0.0


def projections_from_daily(daily_vals: List[float], base_balance: Optional[float]) -> Dict[str, Any]:
    avg = safe_avg(daily_vals)
    b0 = float(base_balance or 0.0)

    def proj(days: int) -> Dict[str, Any]:
        est = avg * days
        return {"days": days, "daily_avg": avg, "est_pnl": est, "est_balance": b0 + est}

    return {"avg": avg, "base_balance": b0, "p5": proj(5), "p10": proj(10), "p20": proj(20)}


# ============================================================
# Calculator (simple + fast) ✅
# ============================================================
def calc_stop_takeprofit(entry: float, stop_pct: float, target_pct: float) -> Tuple[float, float]:
    stop_price = round(entry * (1 - stop_pct / 100.0), 2)
    tp_price = round(entry * (1 + target_pct / 100.0), 2)
    return stop_price, tp_price


def calc_risk_reward(entry: float, contracts: int, stop_price: float, tp_price: float, fee_per_contract: float) -> Dict[
    str, float]:
    fees = round(contracts * fee_per_contract, 2)
    risk_gross = (entry - stop_price) * MULTIPLIER * contracts
    reward_gross = (tp_price - entry) * MULTIPLIER * contracts
    risk_net = round(risk_gross + fees, 2)
    reward_net = round(reward_gross - fees, 2)
    rr = round((reward_net / risk_net), 2) if risk_net > 0 else 0.0
    return {"fees": fees, "risk_net": risk_net, "reward_net": reward_net, "rr": rr}


# ============================================================
# Strategies CRUD ✅
# ============================================================
def fetch_strategies() -> List[sqlite3.Row]:
    from mccain_capital.repositories import strategies as repo
    return repo.fetch_strategies()


def get_strategy(sid: int) -> Optional[sqlite3.Row]:
    from mccain_capital.repositories import strategies as repo
    return repo.get_strategy(sid=sid)


def create_strategy(title: str, body: str) -> int:
    from mccain_capital.repositories import strategies as repo
    return repo.create_strategy(title=title, body=body)


def update_strategy(sid: int, title: str, body: str) -> None:
    from mccain_capital.repositories import strategies as repo
    repo.update_strategy(sid=sid, title=title, body=body)


def delete_strategy(sid: int) -> None:
    from mccain_capital.repositories import strategies as repo
    repo.delete_strategy(sid=sid)


# ============================================================
# Books (folder-only, no web upload) ✅
# ============================================================
def safe_filename(name: str) -> str:
    from mccain_capital.repositories import books as repo
    return repo.safe_filename(name)


def list_books() -> List[Dict[str, str]]:
    from mccain_capital.repositories import books as repo
    return repo.list_books()


# ============================================================
# Balance snapshot insert (FIXED: incomplete before)
# ============================================================
def insert_balance_snapshot(trade_date: str, balance: float, raw_line: str = "") -> None:
    created = now_iso()
    with db() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent,
                stop_pct, target_pct, stop_price, take_profit,
                risk, comm, gross_pl, net_pl, result_pct, balance,
                raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                trade_date,
                "",
                "",
                "ACCT",
                "",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                float(balance),
                raw_line or "BALANCE SNAPSHOT",
                created,
            ),
        )


# ============================================================
# UI Template
# ============================================================

def render_page(content_html: str, *, active: str, title: str = APP_TITLE):
    logo_path = os.path.join(app.static_folder or "static", "logo.png")
    favicon_path = os.path.join(app.static_folder or "static", "favicon.ico")
    logo_exists = os.path.exists(logo_path)
    favicon_exists = os.path.exists(favicon_path)
    # Cache-bust static branding assets so icon/logo updates show immediately after deploy.
    try:
        static_v = str(int(max(os.path.getmtime(logo_path), os.path.getmtime(favicon_path))))
    except Exception:
        static_v = BUILD_MARKER
    return render_template(
        "base.html",
        title=title,
        logo_exists=logo_exists,
        favicon_exists=favicon_exists,
        static_v=static_v,
        auth_enabled=auth_enabled(),
        authenticated=is_authenticated(),
        auth_username=_effective_username(),
        content=content_html,
        active=active,
    )


def _simple_msg(msg: str) -> str:
    return render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">⚠️</div>
          <div style="margin-top:10px">{{ msg }}</div>
          <div class="hr"></div>
          <div class="rightActions">
            <a class="btn primary" href="/trades">Back</a>
          </div>
        </div></div>
        """,
        msg=msg,
    )


# ============================================================
# Routes – Home + favicon
# ============================================================
def setup_page():
    """First-run auth setup from the web UI."""
    if auth_enabled() and not is_authenticated():
        return redirect(url_for("login_page"))

    err = ""
    msg = ""
    default_user = _effective_username() if auth_enabled() else APP_USERNAME

    if request.method == "POST":
        user = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        confirm = request.form.get("confirm_password") or ""

        if len(user) < 3:
            err = "Username must be at least 3 characters."
        elif len(password) < 8:
            err = "Password must be at least 8 characters."
        elif password != confirm:
            err = "Passwords do not match."
        else:
            set_setting_value("auth_username", user)
            set_setting_value("auth_password_hash", generate_password_hash(password))
            session["auth_ok"] = True
            session["auth_user"] = user
            session.permanent = True
            msg = "Login credentials saved."
            return redirect(url_for("dashboard"))

    return render_page(
        render_template_string(
            """
            <div class="card"><div class="toolbar" style="max-width:560px;margin:18px auto;">
              <div class="pill">🔐 Setup Login</div>
              <div class="tiny" style="margin-top:10px;line-height:1.6">
                Create your app login. You can change it later from this same page.
              </div>
              {% if err %}<div class="hr"></div><div class="tiny" style="color:#ff8f8f">{{ err }}</div>{% endif %}
              {% if msg %}<div class="hr"></div><div class="tiny" style="color:#9fd6ff">{{ msg }}</div>{% endif %}
              <div class="hr"></div>
              <form method="post">
                <div class="row">
                  <div><label>Username</label><input name="username" value="{{ default_user }}" autocomplete="username" required></div>
                </div>
                <div class="row" style="margin-top:10px">
                  <div><label>Password</label><input type="password" name="password" autocomplete="new-password" required></div>
                </div>
                <div class="row" style="margin-top:10px">
                  <div><label>Confirm Password</label><input type="password" name="confirm_password" autocomplete="new-password" required></div>
                </div>
                <div class="hr"></div>
                <div class="rightActions">
                  <button class="btn primary" type="submit">Save Login</button>
                  <a class="btn" href="/dashboard">Cancel</a>
                </div>
              </form>
            </div></div>
            """,
            err=err,
            msg=msg,
            default_user=default_user,
        ),
        active="auth",
        title=f"{APP_TITLE} · Setup Login",
    )


def login_page():
    if not auth_enabled():
        return redirect(url_for("setup_page"))
    if is_authenticated():
        return redirect(url_for("dashboard"))

    err = ""
    if request.method == "POST":
        user = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        effective_user = _effective_username()
        if user == effective_user and check_password_hash(_effective_password_hash(), password):
            session["auth_ok"] = True
            session["auth_user"] = effective_user
            session.permanent = True
            nxt = (request.args.get("next") or request.form.get("next") or "").strip()
            if nxt.startswith("/") and not nxt.startswith("//"):
                return redirect(nxt)
            return redirect(url_for("dashboard"))
        err = "Invalid username or password."

    return render_page(
        render_template_string(
            """
            <div class="card"><div class="toolbar" style="max-width:520px;margin:18px auto;">
              <div class="pill">🔐 Secure Sign In</div>
              <div class="tiny" style="margin-top:10px">Private journal access is enabled.</div>
              {% if err %}<div class="hr"></div><div class="tiny" style="color:#ff8f8f">{{ err }}</div>{% endif %}
              <div class="hr"></div>
              <form method="post" action="{{ url_for('login_page', next=next_url) }}">
                <div class="row">
                  <div><label>Username</label><input name="username" autocomplete="username" required></div>
                </div>
                <div class="row" style="margin-top:10px">
                  <div><label>Password</label><input type="password" name="password" autocomplete="current-password" required></div>
                </div>
                <input type="hidden" name="next" value="{{ next_url }}">
                <div class="hr"></div>
                <div class="rightActions">
                  <button class="btn primary" type="submit">Sign In</button>
                </div>
              </form>
            </div></div>
            """,
            err=err,
            next_url=request.args.get("next", ""),
        ),
        active="auth",
        title=f"{APP_TITLE} · Login",
    )


def logout_page():
    session.clear()
    return redirect(url_for("login_page"))


def healthz():
    return jsonify({"status": "ok", "app": "mccain-capital", "build": BUILD_MARKER, "ts": now_iso()})


def home():
    return redirect(url_for("dashboard"))


def favicon():
    return send_file(os.path.join(app.static_folder or "static", "favicon.ico"))


# ============================================================
# Routes – Journal
# ============================================================
def _entry_form(mode: str, values: Dict[str, Any], entry_id: Optional[int] = None,
                errors: Optional[List[str]] = None) -> str:
    errors = errors or []
    action = "/new" if mode == "new" else f"/edit/{entry_id}"
    title = "➕ New Entry" if mode == "new" else f"✏️ Edit Entry #{entry_id}"
    return render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">{{ title }}</div>
          <div class="tiny" style="margin-top:10px; line-height:1.6">
            Document observations, execution, and lessons with clarity.
          </div>

          {% if errors %}
            <div class="hr"></div>
            <div class="tiny" style="color:#ff8f8f">{% for e in errors %}• {{ e }}<br/>{% endfor %}</div>
          {% endif %}

          <div class="hr"></div>
          <form method="post" action="{{ action }}">
            <div class="row">
              <div>
                <label>📆 Date</label>
                <input type="date" name="entry_date" value="{{ values.get('entry_date','') }}">
              </div>
              <div>
                <label>🏷️ Market</label>
                <input name="market" value="{{ values.get('market','') }}" placeholder="SPX / QQQ / NQ...">
              </div>
              <div>
                <label>📌 Setup</label>
                <input name="setup" value="{{ values.get('setup','') }}" placeholder="Midday CE Strike...">
              </div>
            </div>

            <div class="row" style="margin-top:10px">
              <div>
                <label>🧠 Grade</label>
                <input name="grade" value="{{ values.get('grade','') }}" placeholder="A / B / C...">
              </div>
              <div>
                <label>😶‍🌫️ Mood</label>
                <input name="mood" value="{{ values.get('mood','') }}" placeholder="Calm / anxious / revenge...">
              </div>
              <div>
                <label>💰 PnL</label>
                <input name="pnl" inputmode="decimal" value="{{ values.get('pnl','') }}" placeholder="e.g. 327.90">
              </div>
            </div>

            <div style="margin-top:12px">
              <label>📝 Notes</label>
              <textarea name="notes" placeholder="Capture context, execution, and improvement plan...">{{ values.get('notes','') }}</textarea>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save</button>
              <a class="btn" href="/journal">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        title=title,
        action=action,
        values=values,
        errors=errors,
    )


def journal_home():
    q = request.args.get("q", "")
    d = request.args.get("d", "")
    entries = fetch_entries(q=q, d=d)

    content = render_template_string(
        """
        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <form method="get" action="/journal" class="row">
              <div style="flex:2 1 260px">
                <label for="search">🔎 Search Journal 🧠</label>
                <input id="search" name="q" value="{{ q }}" placeholder="notes, setup, mood…" />
              </div>
              <div style="flex:1 1 160px">
                <label>📆 Date</label>
                <input type="date" name="d" value="{{ d }}" />
              </div>
              <div style="display:flex; gap:10px; flex-wrap:wrap">
                <button class="btn" type="submit">🧲 Filter</button>
                <a class="btn" href="/journal">♻️ Reset</a>
                <a class="btn primary" href="{{ url_for('new_entry') }}">➕ New Entry</a>
              </div>
            </form>
            <div class="hr"></div>
            <div class="meta">🧾 {{ entries|length }} entr{{ 'y' if entries|length==1 else 'ies' }} found</div>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">🎯 Daily Focus</div>
            <div style="margin-top:10px; color:var(--muted); line-height:1.5">
              <div>✅ Rules first (or it’s gambling 🎰).</div>
              <div>✅ Confirmation > Hope 👀</div>
              <div>✅ Size + stop respected 🛑</div>
              <div style="margin-top:10px">Journal: <b>what you saw</b> → <b>what you did</b> → <b>what you learned</b> 🧱</div>
            </div>
          </div></div>
        </div>

        <div class="grid">
          {% for e in entries %}
            <div class="card entry">
              <div class="entryTop">
                <div>
                  <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap">
                    <div class="pill">📆 {{ e['entry_date'] }}</div>
                    {% if e['market'] %}<div class="meta">🏷️ Market: <b>{{ e['market'] }}</b></div>{% endif %}
                    {% if e['setup'] %}<div class="meta">📌 Setup: <b>{{ e['setup'] }}</b></div>{% endif %}
                  </div>
                  <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:6px">
                    {% if e['grade'] %}<span class="meta">🧠 Grade: <b>{{ e['grade'] }}</b></span>{% endif %}
                    {% if e['mood'] %}<span class="meta">😶‍🌫️ Mood: <b>{{ e['mood'] }}</b></span>{% endif %}
                    {% if e['pnl'] is not none %}<span class="meta">💰 PnL: <b>{{ money(e['pnl']) }}</b></span>{% endif %}
                    <span class="meta">🕒 Updated: {{ e['updated_at'] }}</span>
                  </div>
                </div>

                <div class="rightActions">
                  <a class="btn" href="{{ url_for('edit_entry', entry_id=e['id']) }}">✏️ Edit</a>
                  <form id="del-e-{{ e['id'] }}" method="post" action="{{ url_for('delete_entry_route', entry_id=e['id']) }}" style="display:inline"></form>
                  <button class="btn danger" type="button" onclick="confirmDelete('del-e-{{ e['id'] }}')">🗑️</button>
                </div>
              </div>

              <div class="notes">{{ e['notes'] }}</div>
            </div>
          {% endfor %}

          {% if entries|length == 0 %}
            <div class="card entry"><div class="meta">No journal entries yet. Hit <b>New Entry</b>. 📝</div></div>
          {% endif %}
        </div>
        """,
        q=q,
        d=d,
        entries=entries,
        money=money,
    )
    return render_page(content, active="journal")


def new_entry():
    if request.method == "POST":
        f = request.form
        pnl = parse_float(f.get("pnl", ""))
        notes = (f.get("notes") or "").strip()
        if not notes:
            return render_page(_entry_form("new", dict(f), errors=["Notes is required."]), active="journal")

        entry_id = create_entry(
            {
                "entry_date": (f.get("entry_date") or today_iso()).strip(),
                "market": f.get("market"),
                "setup": f.get("setup"),
                "grade": f.get("grade"),
                "pnl": pnl,
                "mood": f.get("mood"),
                "notes": notes,
            }
        )
        return redirect(url_for("edit_entry", entry_id=entry_id))

    return render_page(_entry_form("new", {"entry_date": today_iso()}, errors=[]), active="journal")


def edit_entry(entry_id: int):
    row = get_entry(entry_id)
    if not row:
        abort(404)

    if request.method == "POST":
        f = request.form
        pnl = parse_float(f.get("pnl", ""))
        notes = (f.get("notes") or "").strip()
        if not notes:
            return render_page(_entry_form("edit", dict(f), entry_id=entry_id, errors=["Notes is required."]),
                               active="journal")

        update_entry(
            entry_id,
            {
                "entry_date": (f.get("entry_date") or today_iso()).strip(),
                "market": f.get("market"),
                "setup": f.get("setup"),
                "grade": f.get("grade"),
                "pnl": pnl,
                "mood": f.get("mood"),
                "notes": notes,
            },
        )
        return redirect(url_for("journal_home"))

    values = dict(row)
    if values.get("pnl") is None:
        values["pnl"] = ""
    return render_page(_entry_form("edit", values, entry_id=entry_id, errors=[]), active="journal")


def delete_entry_route(entry_id: int):
    delete_entry(entry_id)
    return redirect(url_for("journal_home"))


def latest_trade_day() -> Optional[date]:
    with db() as conn:
        row = conn.execute(
            """
            SELECT trade_date
            FROM trades
            WHERE trade_date IS NOT NULL AND trade_date != ''
            ORDER BY trade_date DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

    if not row:
        return None

    try:
        return datetime.strptime(row["trade_date"], "%Y-%m-%d").date()
    except Exception:
        return None


def fetch_trades_range(start_iso: str, end_iso: str) -> List[sqlite3.Row]:
    from mccain_capital.repositories import trades as repo
    return repo.fetch_trades_range(start_iso=start_iso, end_iso=end_iso)


def month_range(year: int, month: int) -> Tuple[str, str]:
    first = date(year, month, 1)
    nxt = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    return first.isoformat(), nxt.isoformat()


# ============================================================
# Routes – Trades
# ============================================================
def trades_page():
    """Compatibility delegator: runtime implementation lives in services.trades."""
    from mccain_capital.services import trades as svc
    return svc.trades_page()


def trades_duplicate(trade_id: int):
    """Clone a trade row (useful for scaling in/out or repeating a similar fill)."""
    src = get_trade(trade_id)
    if not src:
        abort(404)

    # Keep the same economics, but roll balance forward from latest balance
    net_pl = float(src["net_pl"] or 0.0)
    new_balance = (latest_balance_overall() or 50000.0) + net_pl

    with db() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent,
                stop_pct, target_pct, stop_price, take_profit,
                risk, comm, gross_pl, net_pl, result_pct, balance,
                raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                src["trade_date"],
                src["entry_time"] or "",
                src["exit_time"] or "",
                src["ticker"] or "",
                src["opt_type"] or "",
                src["strike"],
                src["entry_price"],
                src["exit_price"],
                src["contracts"],
                src["total_spent"],
                src["stop_pct"],
                src["target_pct"],
                src["stop_price"],
                src["take_profit"],
                src["risk"],
                src["comm"],
                src["gross_pl"],
                src["net_pl"],
                src["result_pct"],
                new_balance,
                f"DUPLICATE OF #{trade_id}",
                now_iso(),
            ),
        )

    d = request.args.get("d", "") or (src["trade_date"] or "")
    q = request.args.get("q", "")
    return redirect(url_for("trades_page", d=d, q=q))


def trades_delete(trade_id: int):
    with db() as conn:
        conn.execute("DELETE FROM trades WHERE id = ?", (trade_id,))
    # send them back where they were
    d = request.args.get("d", "")
    q = request.args.get("q", "")
    return redirect(url_for("trades_page", d=d, q=q))


def _parse_ids_from_request() -> List[int]:
    """Parse a list of trade ids from JSON or form data."""
    ids: Any = None
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        ids = payload.get("ids")
    if ids is None:
        # form submissions may send ids[]=1&ids[]=2, or ids="1,2"
        ids = request.form.getlist("ids") or request.form.get("ids")

    if isinstance(ids, str):
        raw = [x.strip() for x in ids.split(",") if x.strip()]
    elif isinstance(ids, list):
        raw = ids
    else:
        raw = []

    clean: List[int] = []
    for x in raw:
        try:
            clean.append(int(x))
        except Exception:
            continue
    # de-dupe while preserving order
    seen = set()
    out: List[int] = []
    for i in clean:
        if i not in seen:
            out.append(i)
            seen.add(i)
    return out


def _trades_table_columns(conn: sqlite3.Connection) -> List[str]:
    """Return the current trades table columns (sqlite)."""
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(trades)").fetchall()]
    return cols


def trades_delete_many():
    ids = _parse_ids_from_request()
    if not ids:
        if request.is_json:
            return jsonify({"ok": True, "deleted": 0})
        flash("No trades selected.", "warning")
        return redirect(url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", "")))

    placeholders = ",".join(["?"] * len(ids))
    with db() as conn:
        cur = conn.execute(f"DELETE FROM trades WHERE id IN ({placeholders})", ids)
        deleted = cur.rowcount if cur.rowcount is not None else 0

    if request.is_json:
        return jsonify({"ok": True, "deleted": int(deleted)})
    flash(f"Deleted {deleted} trade(s).", "success")
    return redirect(url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", "")))


def trades_copy_many():
    ids = _parse_ids_from_request()
    target_date = None
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        target_date = payload.get("target_date")
    if not target_date:
        target_date = request.form.get("target_date")

    if not ids:
        if request.is_json:
            return jsonify({"ok": True, "copied": 0})
        flash("No trades selected.", "warning")
        return redirect(url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", "")))

    # validate date
    try:
        datetime.strptime(str(target_date), "%Y-%m-%d")
    except Exception:
        if request.is_json:
            return jsonify({"ok": False, "error": "Invalid target_date. Use YYYY-MM-DD."}), 400
        flash("Invalid target date (use YYYY-MM-DD).", "danger")
        return redirect(url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", "")))

    with db() as conn:
        cols = _trades_table_columns(conn)
        # copy everything except id; we'll override trade_date and reset balance
        insert_cols = [c for c in cols if c != "id"]
        select_cols = ",".join([f"{c}" for c in insert_cols])
        placeholders = ",".join(["?"] * len(ids))
        rows = conn.execute(
            f"SELECT {select_cols} FROM trades WHERE id IN ({placeholders}) ORDER BY trade_date, id",
            ids,
        ).fetchall()

        now_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        copied = 0
        if rows:
            qmarks = ",".join(["?"] * len(insert_cols))
            insert_sql = f"INSERT INTO trades ({','.join(insert_cols)}) VALUES ({qmarks})"
            for r in rows:
                data = dict(r)
                data["trade_date"] = str(target_date)
                if "created_at" in data:
                    data["created_at"] = now_iso
                if "balance" in data:
                    data["balance"] = None
                values = [data.get(c) for c in insert_cols]
                conn.execute(insert_sql, values)
                copied += 1

    if request.is_json:
        return jsonify({"ok": True, "copied": copied})
    flash(f"Copied {copied} trade(s) to {target_date}.", "success")
    return redirect(url_for("trades_page", d=str(target_date), q=request.args.get("q", "")))


def get_trade(trade_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()


def update_trade(trade_id: int, data: Dict[str, Any]) -> None:
    with db() as conn:
        conn.execute(
            """
            UPDATE trades
            SET trade_date=?, entry_time=?, exit_time=?, ticker=?, opt_type=?, strike=?,
                entry_price=?, exit_price=?, contracts=?, comm=?,
                total_spent=?, gross_pl=?, net_pl=?, result_pct=?
            WHERE id=?
            """,
            (
                data["trade_date"],
                data["entry_time"],
                data["exit_time"],
                data["ticker"],
                data["opt_type"],
                data["strike"],
                data["entry_price"],
                data["exit_price"],
                data["contracts"],
                data["comm"],
                data["total_spent"],
                data["gross_pl"],
                data["net_pl"],
                data["result_pct"],
                trade_id,
            ),
        )


def trades_edit(trade_id: int):
    row = get_trade(trade_id)
    if not row:
        abort(404)

    # preserve where user came from
    d = request.args.get("d", "")
    q = request.args.get("q", "")

    if request.method == "POST":
        f = request.form

        trade_date = (f.get("trade_date") or today_iso()).strip()
        entry_time = (f.get("entry_time") or "").strip()
        exit_time = (f.get("exit_time") or "").strip()

        ticker = (f.get("ticker") or "").strip().upper()
        opt_type = normalize_opt_type(f.get("opt_type") or "")
        strike = parse_float(f.get("strike") or "")

        contracts = parse_int(f.get("contracts") or "") or 0
        entry_price = parse_float(f.get("entry_price") or "")
        exit_price = parse_float(f.get("exit_price") or "")
        comm = parse_float(f.get("comm") or "") or 0.0

        if not ticker or opt_type not in ("CALL", "PUT") or contracts <= 0 or entry_price is None or exit_price is None:
            return render_page(_simple_msg("Missing required fields (ticker/type/contracts/entry/exit)."),
                               active="trades")

        gross_pl = (exit_price - entry_price) * 100.0 * contracts
        net_pl = gross_pl - comm
        total_spent = entry_price * 100.0 * contracts
        result_pct = (net_pl / total_spent * 100.0) if total_spent > 0 else None

        update_trade(trade_id, {
            "trade_date": trade_date,
            "entry_time": entry_time,
            "exit_time": exit_time,
            "ticker": ticker,
            "opt_type": opt_type,
            "strike": strike,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "contracts": contracts,
            "comm": comm,
            "total_spent": total_spent,
            "gross_pl": gross_pl,
            "net_pl": net_pl,
            "result_pct": result_pct,
        })

        # IMPORTANT: balances are stored per row. Editing net_pl affects future balances.
        recompute_balances()

        return redirect(url_for("trades_page", d=d, q=q) if (d or q) else url_for("trades_page", d=trade_date))

    # GET
    t = dict(row)
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">✏️ Edit Trade #{{ t.id }}</div>
          <div class="hr"></div>

          <form method="post" action="/trades/edit/{{ t.id }}?d={{ d }}&q={{ q }}">
            <div class="row">
              <div><label>📆 Date</label><input type="date" name="trade_date" value="{{ t.trade_date }}"/></div>
              <div><label>⏱️ Entry Time</label><input name="entry_time" value="{{ t.entry_time or '' }}"/></div>
              <div><label>⏱️ Exit Time</label><input name="exit_time" value="{{ t.exit_time or '' }}"/></div>
            </div>

            <div class="row" style="margin-top:10px">
              <div><label>🏷️ Ticker</label><input name="ticker" value="{{ t.ticker or '' }}"/></div>
              <div>
                <label>📌 Type</label>
                <select name="opt_type">
                  <option value="CALL" {% if (t.opt_type or '')=='CALL' %}selected{% endif %}>CALL</option>
                  <option value="PUT"  {% if (t.opt_type or '')=='PUT' %}selected{% endif %}>PUT</option>
                </select>
              </div>
              <div><label>❌ Strike</label><input name="strike" inputmode="decimal" value="{{ '' if t.strike is none else t.strike }}"/></div>
            </div>

            <div class="row" style="margin-top:10px">
              <div><label>🧾 Contracts</label><input name="contracts" inputmode="numeric" value="{{ t.contracts or 1 }}"/></div>
              <div><label>💰 Entry</label><input name="entry_price" inputmode="decimal" value="{{ '' if t.entry_price is none else t.entry_price }}"/></div>
              <div><label>💰 Exit</label><input name="exit_price" inputmode="decimal" value="{{ '' if t.exit_price is none else t.exit_price }}"/></div>
            </div>

            <div class="row" style="margin-top:10px">
              <div><label>💵 Fees (total)</label><input name="comm" inputmode="decimal" value="{{ t.comm or 0.70 }}"/></div>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save</button>
              <a class="btn" href="/trades?d={{ d }}&q={{ q }}">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        t=t,
        d=d,
        q=q,
    )
    return render_page(content, active="trades")


def trades_review(trade_id: int):
    row = get_trade(trade_id)
    if not row:
        abort(404)

    d = request.args.get("d", "")
    q = request.args.get("q", "")
    rv = get_trade_review(trade_id) or {}

    if request.method == "POST":
        f = request.form
        setup_tag = (f.get("setup_tag") or "").strip()
        session_tag = (f.get("session_tag") or "").strip()
        score_raw = (f.get("checklist_score") or "").strip()
        checklist_score = parse_int(score_raw) if score_raw else None
        rule_break_tags = (f.get("rule_break_tags") or "").strip()
        review_note = (f.get("review_note") or "").strip()
        upsert_trade_review(
            trade_id=trade_id,
            setup_tag=setup_tag,
            session_tag=session_tag,
            checklist_score=checklist_score,
            rule_break_tags=rule_break_tags,
            review_note=review_note,
        )
        return redirect(url_for("trades_page", d=d, q=q) if (d or q) else url_for("trades_page"))

    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🧠 Trade Review #{{ t.id }}</div>
          <div class="tiny" style="margin-top:8px">{{ t.trade_date }} · {{ t.ticker }} {{ t.opt_type }}</div>
          <div class="hr"></div>
          <form method="post" action="/trades/review/{{ t.id }}?d={{ d }}&q={{ q }}">
            <div class="row">
              <div><label>Setup Tag</label><input name="setup_tag" value="{{ rv.get('setup_tag','') }}" placeholder="FVG, ORB, Fade, Breakout"></div>
              <div>
                <label>Session Tag</label>
                <select name="session_tag">
                  {% set s = rv.get('session_tag','') %}
                  <option value="" {% if s=='' %}selected{% endif %}>—</option>
                  <option value="Open" {% if s=='Open' %}selected{% endif %}>Open</option>
                  <option value="Midday" {% if s=='Midday' %}selected{% endif %}>Midday</option>
                  <option value="Power Hour" {% if s=='Power Hour' %}selected{% endif %}>Power Hour</option>
                  <option value="After Hours" {% if s=='After Hours' %}selected{% endif %}>After Hours</option>
                </select>
              </div>
              <div><label>Checklist Score (0-100)</label><input name="checklist_score" inputmode="numeric" value="{{ '' if rv.get('checklist_score') is none else rv.get('checklist_score') }}"></div>
            </div>
            <div class="row" style="margin-top:10px">
              <div>
                <label>Rule-Break Tags (comma separated)</label>
                <input name="rule_break_tags" value="{{ rv.get('rule_break_tags','') }}" placeholder="oversized, late entry, no stop, revenge trade">
              </div>
            </div>
            <div style="margin-top:10px">
              <label>Review Note</label>
              <textarea name="review_note" placeholder="What to repeat, what to remove next session">{{ rv.get('review_note','') }}</textarea>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save Review</button>
              <a class="btn" href="/trades?d={{ d }}&q={{ q }}">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        t=dict(row),
        rv=rv,
        d=d,
        q=q,
    )
    return render_page(content, active="trades")


def trades_risk_controls():
    if request.method == "POST":
        daily_max_loss = parse_float(request.form.get("daily_max_loss", "")) or 0.0
        enforce_lockout = 1 if request.form.get("enforce_lockout") == "1" else 0
        save_risk_controls(daily_max_loss, enforce_lockout)
        return redirect(url_for("trades_risk_controls"))

    rc = get_risk_controls()
    state = trade_lockout_state(today_iso())
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🛡️ Risk Controls</div>
          <div class="tiny" style="margin-top:8px">
            Today's net: {{ money(state.day_net) }} · Max loss: {{ money(state.daily_max_loss) }} ·
            Status: {% if state.locked %}<b style="color:#ff8f8f">LOCKED</b>{% else %}<b style="color:#7ee2ae">ACTIVE</b>{% endif %}
          </div>
          <div class="hr"></div>
          <form method="post">
            <div class="row">
              <div><label>Daily Max Loss ($)</label><input name="daily_max_loss" inputmode="decimal" value="{{ rc.daily_max_loss }}"></div>
              <div>
                <label>Enforce Lockout</label>
                <select name="enforce_lockout">
                  <option value="0" {% if not rc.enforce_lockout %}selected{% endif %}>Off</option>
                  <option value="1" {% if rc.enforce_lockout %}selected{% endif %}>On</option>
                </select>
              </div>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">Save Controls</button>
              <a class="btn" href="/trades">Back to Trades</a>
            </div>
          </form>
        </div></div>
        """,
        rc=rc,
        state=state,
        money=money,
    )
    return render_page(content, active="trades")


def analytics_page():
    backfill_auto_reviews_for_unreviewed()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT t.id, t.trade_date, t.entry_time, t.net_pl, r.setup_tag, r.session_tag, r.checklist_score, r.rule_break_tags
            FROM trades t
            LEFT JOIN trade_reviews r ON r.trade_id = t.id
            ORDER BY t.trade_date DESC, t.id DESC
            """
        ).fetchall()
    trades = [dict(r) for r in rows]

    def _bucket_hour(v: str) -> str:
        s = (v or "").strip().upper()
        m = re.match(r"^(\d{1,2}):(\d{2})\s*(AM|PM)$", s)
        if not m:
            return "Unknown"
        hh = int(m.group(1)) % 12
        if m.group(3) == "PM":
            hh += 12
        return f"{hh:02d}:00"

    def _group(key_fn):
        out: Dict[str, Dict[str, Any]] = {}
        for t in trades:
            k = key_fn(t)
            if not k:
                continue
            out.setdefault(k, {"count": 0, "wins": 0, "net": 0.0, "scores": []})
            out[k]["count"] += 1
            n = float(t.get("net_pl") or 0.0)
            out[k]["net"] += n
            if n > 0:
                out[k]["wins"] += 1
            sc = t.get("checklist_score")
            if sc is not None:
                out[k]["scores"].append(float(sc))
        table = []
        for k, v in out.items():
            c = v["count"] or 1
            table.append(
                {
                    "k": k,
                    "count": v["count"],
                    "net": v["net"],
                    "win_rate": (v["wins"] / c) * 100.0,
                    "avg_score": (sum(v["scores"]) / len(v["scores"])) if v["scores"] else None,
                }
            )
        table.sort(key=lambda x: x["net"], reverse=True)
        return table

    setup_rows = _group(lambda t: (t.get("setup_tag") or "").strip() or "Unlabeled")
    session_rows = _group(lambda t: (t.get("session_tag") or "").strip() or "Unlabeled")
    hour_rows = _group(lambda t: _bucket_hour(t.get("entry_time") or ""))

    content = render_template_string(
        """
        <div class="metricStrip">
          <div class="metric"><div class="label">Reviewed Trades</div><div class="value">{{ trades|length }}</div></div>
          <div class="metric"><div class="label">Setups Tracked</div><div class="value">{{ setup_rows|length }}</div></div>
          <div class="metric"><div class="label">Sessions Tracked</div><div class="value">{{ session_rows|length }}</div></div>
          <div class="metric"><div class="label">Hour Buckets</div><div class="value">{{ hour_rows|length }}</div></div>
        </div>

        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div class="pill">📌 Setup Analytics</div>
            <div class="hr"></div>
            <table>
              <thead><tr><th>Setup</th><th>Trades</th><th>Win Rate</th><th>Net</th><th>Avg Score</th></tr></thead>
              <tbody>
              {% for r in setup_rows %}
                <tr><td>{{ r.k }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.net) }}</td><td>{{ '%.1f'|format(r.avg_score) if r.avg_score is not none else '—' }}</td></tr>
              {% endfor %}
              </tbody>
            </table>
          </div></div>
          <div class="card"><div class="toolbar">
            <div class="pill">🕒 Session Analytics</div>
            <div class="hr"></div>
            <table>
              <thead><tr><th>Session</th><th>Trades</th><th>Win Rate</th><th>Net</th><th>Avg Score</th></tr></thead>
              <tbody>
              {% for r in session_rows %}
                <tr><td>{{ r.k }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.net) }}</td><td>{{ '%.1f'|format(r.avg_score) if r.avg_score is not none else '—' }}</td></tr>
              {% endfor %}
              </tbody>
            </table>
          </div></div>
        </div>

        <div class="card" style="margin-top:12px"><div class="toolbar">
          <div class="pill">⏱️ Time-of-Day Analytics</div>
          <div class="hr"></div>
          <table>
            <thead><tr><th>Hour</th><th>Trades</th><th>Win Rate</th><th>Net</th><th>Avg Score</th></tr></thead>
            <tbody>
            {% for r in hour_rows %}
              <tr><td>{{ r.k }}</td><td>{{ r.count }}</td><td>{{ '%.1f'|format(r.win_rate) }}%</td><td>{{ money(r.net) }}</td><td>{{ '%.1f'|format(r.avg_score) if r.avg_score is not none else '—' }}</td></tr>
            {% endfor %}
            </tbody>
          </table>
        </div></div>
        """,
        trades=trades,
        setup_rows=setup_rows,
        session_rows=session_rows,
        hour_rows=hour_rows,
        money=money,
    )
    return render_page(content, active="analytics", title=f"{APP_TITLE} · Analytics")


def recompute_balances(starting_balance: float = 50000.0) -> None:
    """
    Recompute running balances for all trades in chronological order.
    Call this after EDIT or DELETE of any trade.
    """
    with db() as conn:
        rows = conn.execute(
            """
            SELECT id, net_pl
            FROM trades
            ORDER BY trade_date ASC, id ASC
            """
        ).fetchall()

        bal = float(starting_balance)

        conn.execute("BEGIN")
        for r in rows:
            net = r["net_pl"]
            if net is not None:
                bal += float(net)
            conn.execute(
                "UPDATE trades SET balance = ? WHERE id = ?",
                (bal, r["id"])
            )
        conn.commit()


def _row_has(t, key: str) -> bool:
    # sqlite3.Row supports `key in row.keys()`
    try:
        return key in t.keys()
    except Exception:
        return False


def _val(t, key: str, default=None):
    # Safe access for sqlite3.Row or dict
    if t is None:
        return default
    try:
        if hasattr(t, "keys") and _row_has(t, key):
            v = t[key]
            return default if v is None else v
        if isinstance(t, dict):
            v = t.get(key, default)
            return default if v is None else v
    except Exception:
        pass
    return default


def calc_consistency(trades):
    """
    Vanquish consistency (per article wording):
      ratio = biggest winning trade / total PnL   (when total PnL > 0)
      ratio = biggest losing trade  / total PnL   (when total PnL < 0) using abs values
    Pass if ratio <= 0.30
    """
    if not trades:
        return {"ratio": None, "status": "—", "class": "", "biggest": 0.0, "denom": 0.0}

    net_vals = []
    for t in trades:
        v = _val(t, "net_pl", None)
        if v is None:
            continue
        try:
            net_vals.append(float(v))
        except Exception:
            continue

    if not net_vals:
        return {"ratio": None, "status": "—", "class": "", "biggest": 0.0, "denom": 0.0}

    total_pnl = sum(net_vals)

    winners = [v for v in net_vals if v > 0]
    losers = [v for v in net_vals if v < 0]

    if total_pnl > 0:
        biggest = max(winners) if winners else 0.0
        denom = total_pnl
        ratio = (biggest / denom) if denom else None
    elif total_pnl < 0:
        biggest = max(abs(v) for v in losers) if losers else 0.0
        denom = abs(total_pnl)
        ratio = (biggest / denom) if denom else None
    else:
        return {"ratio": None, "status": "—", "class": "", "biggest": 0.0, "denom": 0.0}

    ok = (ratio is not None) and (ratio <= 0.30)
    return {
        "ratio": ratio,
        "status": "✅ Pass" if ok else "🚫 Fail",
        "class": "glow-green" if ok else "glow-red",
        "biggest": biggest,
        "denom": denom,
    }


def trades_clear():
    clear_trades()
    return redirect(url_for("trades_page"))


def trades_paste():
    if request.method == "POST":
        guardrail = trade_lockout_state(today_iso())
        if guardrail["locked"]:
            return render_page(
                _simple_msg(
                    f"Daily max-loss guardrail is active for {guardrail['day']}. "
                    f"Day net {money(guardrail['day_net'])} reached limit {money(guardrail['daily_max_loss'])}. "
                    "Unlock in Risk Controls to continue."
                ),
                active="trades",
            )
        text = request.form.get("text", "")
        starting_balance = parse_float(request.form.get("starting_balance", "")) or default_starting_balance()
        fmt = detect_paste_format(text)

        if fmt == "broker":
            inserted, errors = insert_trades_from_broker_paste(text, starting_balance=starting_balance)
        else:
            inserted, errors = insert_trades_from_paste(text)

        content = render_template_string(
            """
            <div class="card"><div class="toolbar">
              <div class="pill">📋 Paste Trades</div>
              <div style="margin-top:10px">Inserted <b>{{ inserted }}</b> trade{{ '' if inserted==1 else 's' }} ✅</div>

              {% if errors %}
                <div class="hr"></div>
                <div class="tiny" style="color:#ff8f8f">
                  {% for e in errors %}• {{ e }}<br/>{% endfor %}
                </div>
              {% endif %}

              <div class="hr"></div>
              <div class="rightActions">
                <a class="btn primary" href="/trades">Trades 📅</a>
                <a class="btn" href="/dashboard">Calendar 📊</a>
                <a class="btn" href="/calculator">Calculator 🧮</a>
                <a class="btn" href="/trades/paste">Paste More 🔁</a>
              </div>
            </div></div>
            """,
            inserted=inserted,
            errors=errors,
        )
        return render_page(content, active="trades")

    example = "1/29\t9:35 AM\t9:37 AM\tSPX\tPUT\t6940\t$6.20\t$7.30\t3\t$1,860.00\t20\t30\t$4.96\t$8.06\t$374.10\t$2.10\t$330.00\t$327.90\t17.74%\t$50,924.40"

    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">📋 Paste Trades (tabs please ✅)</div>
          <div class="tiny" style="margin-top:10px; line-height:1.5">
            Pro tip: copy straight from your sheet/log, keep the tabs.
            <div class="hr"></div>
            Example:<br/><code style="font-size:12px; color:var(--muted)">{{ example }}</code>
          </div>

          <div class="hr"></div>
          <form method="post">
            <div class="row">
              <div>
                <label>🏁 Starting Balance (for Broker paste)</label>
                <input name="starting_balance" inputmode="decimal" value="50000" />
              </div>
            </div>

            <div style="margin-top:12px">
              <label>📎 Paste here</label>
              <textarea name="text" placeholder="Paste your trade rows here…"></textarea>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">🚀 Import</button>
              <a class="btn" href="/trades">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        example=example,
    )
    return render_page(content, active="trades")


def trades_new_manual():
    if request.method == "POST":
        f = request.form

        trade_date = (f.get("trade_date") or today_iso()).strip()
        guardrail = trade_lockout_state(trade_date)
        if guardrail["locked"]:
            return render_page(
                _simple_msg(
                    f"Daily max-loss lockout active for {trade_date}. "
                    f"Day net {money(guardrail['day_net'])} hit limit {money(guardrail['daily_max_loss'])}."
                ),
                active="trades",
            )
        entry_time = (f.get("entry_time") or "").strip()
        exit_time = (f.get("exit_time") or "").strip()

        ticker = (f.get("ticker") or "").strip().upper()
        opt_type = normalize_opt_type(f.get("opt_type") or "")
        strike = parse_float(f.get("strike") or "")

        contracts = parse_int(f.get("contracts") or "") or 0
        entry_price = parse_float(f.get("entry_price") or "")
        exit_price = parse_float(f.get("exit_price") or "")
        comm = parse_float(f.get("comm") or "") or 0.0

        if not ticker or opt_type not in ("CALL", "PUT") or contracts <= 0 or entry_price is None or exit_price is None:
            return render_page(_simple_msg("Missing required fields (ticker/type/contracts/entry/exit)."),
                               active="trades")

        gross_pl = (exit_price - entry_price) * 100.0 * contracts
        net_pl = gross_pl - comm
        total_spent = entry_price * 100.0 * contracts
        result_pct = (net_pl / total_spent * 100.0) if total_spent > 0 else None

        # balance rolls forward from latest
        balance = (latest_balance_overall() or 50000.0) + net_pl

        with db() as conn:
            conn.execute(
                """
                INSERT INTO trades (
                    trade_date, entry_time, exit_time, ticker, opt_type, strike,
                    entry_price, exit_price, contracts, total_spent,
                    comm, gross_pl, net_pl, result_pct, balance,
                    raw_line, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    trade_date, entry_time, exit_time, ticker, opt_type, strike,
                    entry_price, exit_price, contracts, total_spent,
                    comm, gross_pl, net_pl, result_pct, balance,
                    "MANUAL ENTRY",
                    now_iso(),
                ),
            )

        return redirect(url_for("trades_page", d=trade_date))

    # GET: simple form
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">➕ Manual Trade Entry</div>
          <div class="hr"></div>

          <form method="post">
            <div class="row">
              <div><label>📆 Date</label><input type="date" name="trade_date" value="{{ today }}"/></div>
              <div><label>⏱️ Entry Time</label><input name="entry_time" placeholder="9:45 AM"/></div>
              <div><label>⏱️ Exit Time</label><input name="exit_time" placeholder="10:05 AM"/></div>
            </div>

            <div class="row" style="margin-top:10px">
              <div><label>🏷️ Ticker</label><input name="ticker" placeholder="SPX"/></div>
              <div>
                <label>📌 Type</label>
                <select name="opt_type">
                  <option>CALL</option>
                  <option>PUT</option>
                </select>
              </div>
              <div><label>❌ Strike</label><input name="strike" inputmode="decimal" placeholder="6940"/></div>
            </div>

            <div class="row" style="margin-top:10px">
              <div><label>🧾 Contracts</label><input name="contracts" inputmode="numeric" value="1"/></div>
              <div><label>💰 Entry</label><input name="entry_price" inputmode="decimal" placeholder="6.20"/></div>
              <div><label>💰 Exit</label><input name="exit_price" inputmode="decimal" placeholder="7.30"/></div>
            </div>

            <div class="row" style="margin-top:10px">
              <div><label>💵 Commission/Fees (total)</label><input name="comm" inputmode="decimal" value="0.70"/></div>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save Trade</button>
              <a class="btn" href="/trades">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        today=today_iso(),
    )
    return render_page(content, active="trades")


def trades_paste_broker():
    if request.method == "POST":
        guardrail = trade_lockout_state(today_iso())
        if guardrail["locked"]:
            return render_page(
                _simple_msg(
                    f"Daily max-loss guardrail is active for {guardrail['day']}. "
                    f"Day net {money(guardrail['day_net'])} reached limit {money(guardrail['daily_max_loss'])}."
                ),
                active="trades",
            )
        text = request.form.get("text", "")
        starting_balance = parse_float(request.form.get("starting_balance", "")) or default_starting_balance()
        inserted, errors = insert_trades_from_broker_paste(text, starting_balance=starting_balance)

        content = render_template_string(
            """
            <div class="card"><div class="toolbar">
              <div class="pill">🏦 Broker Paste Import</div>
              <div style="margin-top:10px">Inserted <b>{{ inserted }}</b> round-trip trade{{ '' if inserted==1 else 's' }} ✅</div>

              {% if errors %}
                <div class="hr"></div>
                <div class="tiny" style="color:#ff8f8f">
                  {% for e in errors %}• {{ e }}<br/>{% endfor %}
                </div>
              {% endif %}

              <div class="hr"></div>
              <div class="rightActions">
                <a class="btn primary" href="/trades">Trades 📅</a>
                <a class="btn" href="/dashboard">Calendar 📊</a>
                <a class="btn" href="/trades/paste/broker">Paste More 🔁</a>
              </div>
            </div></div>
            """,
            inserted=inserted,
            errors=errors,
        )
        return render_page(content, active="trades")

    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🏦 Paste Broker Fills (BUY/SELL legs)</div>
          <div class="tiny" style="margin-top:10px; line-height:1.5">
            Paste the raw fills. This importer pairs BUY+SELL into one completed trade (FIFO). ✅
          </div>

          <div class="hr"></div>
          <form method="post">
            <div class="row">
              <div>
                <label>🏁 Starting Balance</label>
                <input name="starting_balance" inputmode="decimal" value="50000" />
              </div>
            </div>

            <div style="margin-top:12px">
              <label>📎 Paste here</label>
              <textarea name="text" placeholder="SPX JAN/30/26 6935 PUT | 1/30/26, 10:30 AM | SELL | 2 | 18.90 | 0.70"></textarea>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">🚀 Convert + Import</button>
              <a class="btn" href="/trades">← Back</a>
            </div>
          </form>
        </div></div>
        """
    )
    return render_page(content, active="trades")


# ============================================================
# PDF Upload (FIXED: broken indentation + double if)
# ============================================================
def trades_upload_pdf():
    """Compatibility delegator: runtime implementation lives in services.trades."""
    from mccain_capital.services import trades as svc
    return svc.trades_upload_pdf()


def parse_vanquish_statement_table_to_broker_paste(text: str) -> Tuple[str, List[str]]:
    """
    Converts your pasted Vanquish statement table (Instrument, Transaction Time, Direction, Size, Price, Commission...)
    into broker-paste lines that your broker importer already knows how to pair.
    """
    warnings: List[str] = []
    lines = [ln for ln in (text or "").splitlines() if ln.strip()]

    if not lines:
        return "", ["Nothing pasted."]

    # drop header row if present
    if "Instrument" in lines[0] and "Transaction Time" in lines[0]:
        lines = lines[1:]

    out: List[str] = []
    for i, ln in enumerate(lines, start=1):
        cols = split_row(ln)
        # Expected (at least): Instrument, TxCode, TxTime, Direction, Size, Price, ... Commission ...
        if len(cols) < 6:
            warnings.append(f"Line {i}: too few columns ({len(cols)}).")
            continue

        instrument = cols[0].strip().upper()
        dt = cols[2].strip().replace("\u202f", " ").replace("\u00a0", " ")
        side = cols[3].strip().upper()
        qty = parse_int(cols[4]) or 0
        price = parse_float(cols[5])

        # Commission column exists in your paste (index 10), but sometimes it may be missing
        fee = DEFAULT_FEE_PER_CONTRACT
        if len(cols) >= 11:
            maybe_fee = parse_float(cols[10])
            if maybe_fee is not None and 0 <= maybe_fee <= 5:
                fee = float(maybe_fee)

        if not instrument or side not in ("BUY", "SELL") or qty <= 0 or price is None:
            warnings.append(f"Line {i}: skipped (bad instrument/side/qty/price).")
            continue

        out.append(f"{instrument} | {dt} | {side} | {qty} | {price} | {fee}")

    return "\n".join(out), warnings


def ytd_total_net(year: int) -> float:
    start = date(year, 1, 1)
    end = date(year + 1, 1, 1)
    with db() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(net_pl), 0) AS net
            FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchone()
    return float(row["net"] or 0.0)


def month_trade_count(year: int, month: int) -> int:
    first = date(year, month, 1)
    nxt = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    with db() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            """,
            (first.isoformat(), nxt.isoformat()),
        ).fetchone()
    return int(row["c"] or 0)


def ytd_trade_count(year: int) -> int:
    start = date(year, 1, 1)
    end = date(year + 1, 1, 1)
    with db() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            """,
            (start.isoformat(), end.isoformat()),
        ).fetchone()
    return int(row["c"] or 0)


# ============================================================
# Dashboard – calendar + projections ✅
# ============================================================
def dashboard():
    # ✅ Default dashboard month = month of most recent trade (fallback to today)
    anchor = latest_trade_day() or now_et().date()

    y = int(request.args.get("y") or anchor.year)
    m = int(request.args.get("m") or anchor.month)
    m = max(1, min(12, m))

    heat = month_heatmap(y, m)

    prev_y, prev_m = (y, m - 1)
    next_y, next_m = (y, m + 1)
    if prev_m == 0:
        prev_m = 12
        prev_y -= 1
    if next_m == 13:
        next_m = 1
        next_y += 1

    month_name = date(y, m, 1).strftime("%B %Y")

    overall_balance = latest_balance_overall()

    # ✅ Week total should match the month view anchor
    week_anchor = anchor.isoformat() if (y == anchor.year and m == anchor.month) else date(y, m, 1).isoformat()
    this_week_total = week_total_net(week_anchor)

    mtd_net = month_total_net(y, m)
    ytd_net = ytd_total_net(y)

    mtd_trades = month_trade_count(y, m)
    ytd_trades = ytd_trade_count(y)

    daily20 = last_n_trading_day_totals(20)
    proj = projections_from_daily(daily20, overall_balance)

    # ✅ YTD ONLY: Consistency + threshold line
    # Ratio is assumed: biggest / denom (lower is better)
    CONSISTENCY_THRESHOLD = 0.30  # 30% line (adjust if your firm uses a different rule)

    y_start = date(y, 1, 1).isoformat()
    y_end = date(y + 1, 1, 1).isoformat()

    ytd_trades_list = fetch_trades_range(y_start, y_end) or []
    ytd_trades_list = [dict(r) for r in ytd_trades_list]  # sqlite3.Row -> dict for safety
    ytd_cons = calc_consistency(ytd_trades_list)
    today_rows = [dict(r) for r in fetch_trades(d=today_iso(), q="")]
    today_stats = trade_day_stats(today_rows)
    today_net = float(today_stats.get("total", 0.0))
    today_win_rate = float(today_stats.get("win_rate", 0.0))
    today_count = len(today_rows)

    content = render_template_string(
        """
        <div class="metricStrip">
          <div class="metric">
            <div class="label">Today Net</div>
            <div class="value">{{ money(today_net) }}</div>
            <div class="sub">Live from today’s journaled trades</div>
          </div>
          <div class="metric">
            <div class="label">Today Win Rate</div>
            <div class="value">{{ '%.1f'|format(today_win_rate) }}%</div>
            <div class="sub">Consistency starts with daily process</div>
          </div>
          <div class="metric">
            <div class="label">Trades Today</div>
            <div class="value">{{ today_count }}</div>
            <div class="sub">Focused > frequent</div>
          </div>
          <div class="metric">
            <div class="label">Current Balance</div>
            <div class="value">{{ money(overall_balance) }}</div>
            <div class="sub">Snapshot as of latest recorded trade</div>
          </div>
        </div>

        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div class="calendarHead">
              <div>
                <div class="pill">📊 P/L Calendar</div>
                <div class="tiny" style="margin-top:8px">Tap a weekday to open that day’s trades 🧲</div>
              </div>
              <div class="calendarNav">
                <a class="btn" href="/dashboard?y={{ prev_y }}&m={{ prev_m }}">⬅️ Prev</a>
                <a class="btn" href="/dashboard">🎯 This Month</a>
                <a class="btn" href="/dashboard?y={{ next_y }}&m={{ next_m }}">Next ➡️</a>
              </div>
            </div>

            <div class="hr"></div>
            <div class="statRow">
              <div class="stat"><div class="k">🗓️ Month</div><div class="v">{{ month_name }}</div></div>
              <div class="stat"><div class="k">🏦 Balance</div><div class="v">{{ money(overall_balance) }}</div></div>

              <div class="stat {% if this_week_total > 0 %}glow-green{% elif this_week_total < 0 %}glow-red{% endif %}">
                <div class="k">📅 Week Total</div><div class="v">{{ money(this_week_total) }}</div>
              </div>

              <div class="stat"><div class="k">🗓️ MTD Net</div><div class="v">{{ money(mtd_net) }}</div></div>
              <div class="stat"><div class="k">📆 YTD Net</div><div class="v">{{ money(ytd_net) }}</div></div>
              <div class="stat"><div class="k">🧾 Trades (MTD)</div><div class="v">{{ mtd_trades }}</div></div>
              <div class="stat"><div class="k">🧾 Trades (YTD)</div><div class="v">{{ ytd_trades }}</div></div>

              <!-- ✅ YTD ONLY Consistency + threshold marker line -->
              <div class="stat {{ ytd_cons.class }}">
                <div class="k">🎯 Consistency (YTD)</div>
                <div class="v">
                  {% if ytd_cons.ratio is none %}
                    —
                  {% else %}
                    {{ '%.1f'|format(ytd_cons.ratio * 100) }}%
                  {% endif %}
                </div>

                <div class="tiny" style="margin-top:6px">
                  Max: {{ money(ytd_cons.biggest) }} / {{ money(ytd_cons.denom) }}
                  &nbsp; • &nbsp; Line: {{ '%.0f'|format(cons_threshold * 100) }}%
                </div>

                {% if ytd_cons.ratio is not none %}
                  {% set bar_pct = (ytd_cons.ratio * 100) %}
                  {% if bar_pct < 0 %}{% set bar_pct = 0 %}{% endif %}
                  {% if bar_pct > 100 %}{% set bar_pct = 100 %}{% endif %}

                  {% set line_pct = (cons_threshold * 100) %}
                  {% if line_pct < 0 %}{% set line_pct = 0 %}{% endif %}
                  {% if line_pct > 100 %}{% set line_pct = 100 %}{% endif %}

                  <div style="margin-top:8px; position:relative; height:10px; border-radius:999px; background: rgba(255,255,255,.08); overflow:hidden;">
                    <!-- fill -->
                    <div style="height:100%; width: {{ bar_pct }}%; background: rgba(46, 204, 113, .55);"></div>

                    <!-- threshold marker line -->
                    <div title="Threshold"
                         style="position:absolute; left: {{ line_pct }}%; top:0; bottom:0; width:2px; background: rgba(255,255,255,.7);">
                    </div>

                    <!-- if failing, overlay red tint -->
                    {% if ytd_cons.ratio > cons_threshold %}
                      <div style="position:absolute; inset:0; background: rgba(231, 76, 60, .18);"></div>
                    {% endif %}
                  </div>

                  <div class="tiny" style="margin-top:6px">
                    {% if ytd_cons.ratio <= cons_threshold %}
                      ✅ Within threshold
                    {% else %}
                      ❌ Over threshold
                    {% endif %}
                  </div>
                {% endif %}
              </div>

              <div class="stat">
                <div class="k">🧮 Calculator</div>
                <div class="v"><a class="btn primary" style="padding:8px 10px" href="/calculator">Open</a></div>
              </div>
            </div>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">⚡ Quick Actions</div>
            <div class="hr"></div>
            <div class="leftActions">
              <a class="btn primary" href="/trades/upload/statement">📄 Upload Trades</a>
              <a class="btn primary" href="/trades">📅 Trades</a>
            </div>
            <div class="tiny" style="margin-top:10px;line-height:1.5">
              Weekdays only: Mon–Fri. Weekends don’t show $0.00.
            </div>
          </div></div>
        </div>

        <div class="card" style="margin-top:12px"><div class="toolbar">
          <div class="heat">
            <table>
              <thead>
                <tr><th>Sun</th><th>Mon</th><th>Tue</th><th>Wed</th><th>Thu</th><th>Fri</th><th>Sat</th></tr>
              </thead>
              <tbody>
              {% for wk in heat.weeks %}
                <tr>
                  {% for daynum, net, iso, wd in wk %}
                    {% if daynum is none %}
                      <td class="daycell" style="background: rgba(255,255,255,.02)"></td>
                    {% else %}
                      {% set max_abs = heat.max_abs if heat.max_abs else 1 %}
                      {% set intensity = ((net|abs) / max_abs) %}
                      {% set alpha = 0.10 + (0.45 * intensity) %}
                      {% if net > 0 %}
                        <td class="daycell" style="background: rgba(46, 204, 113, {{ alpha }});">
                      {% elif net < 0 %}
                        <td class="daycell" style="background: rgba(231, 76, 60, {{ alpha }});">
                      {% else %}
                        <td class="daycell" style="background: rgba(255,255,255,.03)">
                      {% endif %}
                          <div class="daynum">{{ daynum }}</div>

                          {% if wd is not none and wd < 5 %}
                            {% if net %}
                              <div class="daypnl">{{ money_compact(net) }}</div>
                            {% else %}
                              <div class="daypnl"></div>
                            {% endif %}
                            <a href="/trades?d={{ iso }}" style="position:absolute; inset:0;" aria-label="Open day"></a>
                          {% else %}
                            <div class="daypnl"></div>
                          {% endif %}
                        </td>
                    {% endif %}
                  {% endfor %}
                </tr>
              {% endfor %}
              </tbody>
            </table>
          </div>
        </div></div>

        <div class="card" style="margin-top:12px"><div class="toolbar">
          <div class="pill">📈 Projections (Trading Days: Mon–Fri)</div>
          <div class="tiny" style="margin-top:10px;line-height:1.5">
            Based on recent weekday daily totals (up to 20 days). Planning tool — not a promise.
          </div>

          <div class="hr"></div>
          <div class="statRow">
            <div class="stat"><div class="k">📊 Daily Avg (recent)</div><div class="v">{{ money(proj.avg) }}</div></div>
            <div class="stat"><div class="k">🏦 Base Balance</div><div class="v">{{ money(proj.base_balance) }}</div></div>

            <div class="stat {% if proj.p5.est_pnl > 0 %}glow-green{% elif proj.p5.est_pnl < 0 %}glow-red{% endif %}">
              <div class="k">5 Trading Days</div><div class="v">{{ money(proj.p5.est_pnl) }}</div>
              <div class="tiny">Est Bal: {{ money(proj.p5.est_balance) }}</div>
            </div>

            <div class="stat {% if proj.p10.est_pnl > 0 %}glow-green{% elif proj.p10.est_pnl < 0 %}glow-red{% endif %}">
              <div class="k">10 Trading Days</div><div class="v">{{ money(proj.p10.est_pnl) }}</div>
              <div class="tiny">Est Bal: {{ money(proj.p10.est_balance) }}</div>
            </div>

            <div class="stat {% if proj.p20.est_pnl > 0 %}glow-green{% elif proj.p20.est_pnl < 0 %}glow-red{% endif %}">
              <div class="k">20 Trading Days</div><div class="v">{{ money(proj.p20.est_pnl) }}</div>
              <div class="tiny">Est Bal: {{ money(proj.p20.est_balance) }}</div>
            </div>
          </div>
        </div></div>
        """,
        heat=heat,
        prev_y=prev_y,
        prev_m=prev_m,
        next_y=next_y,
        next_m=next_m,
        month_name=month_name,
        overall_balance=overall_balance,
        this_week_total=this_week_total,
        mtd_net=mtd_net,
        ytd_net=ytd_net,
        mtd_trades=mtd_trades,
        ytd_trades=ytd_trades,
        ytd_cons=ytd_cons,  # ✅ YTD ONLY
        cons_threshold=CONSISTENCY_THRESHOLD,  # ✅ threshold line
        today_net=today_net,
        today_win_rate=today_win_rate,
        today_count=today_count,
        proj=proj,
        money=money,
        money_compact=money_compact,
    )

    return render_page(content, active="dashboard")


# ============================================================
# Calculator page ✅
# ============================================================
def calculator():
    out = None
    err = None

    vals = {
        "entry": "",
        "contracts": "1",
        "stop_pct": str(DEFAULT_STOP_PCT),
        "target_pct": str(DEFAULT_TARGET_PCT),
        "fee_per_contract": str(DEFAULT_FEE_PER_CONTRACT),
    }

    if request.method == "POST":
        f = request.form
        vals["entry"] = (f.get("entry") or "").strip()
        vals["contracts"] = (f.get("contracts") or "1").strip()
        vals["stop_pct"] = (f.get("stop_pct") or str(DEFAULT_STOP_PCT)).strip()
        vals["target_pct"] = (f.get("target_pct") or str(DEFAULT_TARGET_PCT)).strip()
        vals["fee_per_contract"] = (f.get("fee_per_contract") or str(DEFAULT_FEE_PER_CONTRACT)).strip()

        entry = parse_float(vals["entry"])
        contracts = parse_int(vals["contracts"]) or 1
        stop_pct = parse_float(vals["stop_pct"]) or DEFAULT_STOP_PCT
        target_pct = parse_float(vals["target_pct"]) or DEFAULT_TARGET_PCT
        fee = parse_float(vals["fee_per_contract"]) or DEFAULT_FEE_PER_CONTRACT

        if not entry or entry <= 0:
            err = "Entry premium must be > 0."
        elif contracts <= 0:
            err = "Contracts must be >= 1."
        else:
            stop_price, tp_price = calc_stop_takeprofit(entry, stop_pct, target_pct)
            rr = calc_risk_reward(entry, contracts, stop_price, tp_price, fee)

            ladder = []
            for p in range(10, 101, 10):
                tpp = round(entry * (1 + p / 100.0), 2)
                rr2 = calc_risk_reward(entry, contracts, stop_price, tpp, fee)
                ladder.append({"pct": p, "tp": tpp, "net": rr2["reward_net"]})

            out = {
                "entry": entry,
                "contracts": contracts,
                "total_spend": round(entry * MULTIPLIER * contracts + (fee * contracts), 2),
                "stop_pct": stop_pct,
                "target_pct": target_pct,
                "fee": fee,
                "stop_price": stop_price,
                "tp_price": tp_price,
                **rr,
                "ladder": ladder,
            }

    content = render_template_string(
        """
        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div class="pill">🧮 Quick Stop + Take Profit</div>
            <div class="tiny" style="margin-top:10px; line-height:1.5">
              Fast. Clean. No overkill. Use this during the day. ✅
            </div>

            {% if err %}
              <div class="hr"></div>
              <div class="tiny" style="color:#ff8f8f">• {{ err }}</div>
            {% endif %}

            <div class="hr"></div>
            <form method="post">
              <div class="row">
                <div>
                  <label>💰 Entry Premium</label>
                  <input name="entry" inputmode="decimal" placeholder="e.g. 6.20" value="{{ vals.entry }}">
                </div>
                <div>
                  <label>🧾 Contracts</label>
                  <input name="contracts" inputmode="numeric" value="{{ vals.contracts }}">
                </div>
              </div>

              <div class="row" style="margin-top:10px">
                <div>
                  <label>🛑 Stop %</label>
                  <input name="stop_pct" inputmode="decimal" value="{{ vals.stop_pct }}">
                </div>
                <div>
                  <label>🎯 Target %</label>
                  <input name="target_pct" inputmode="decimal" value="{{ vals.target_pct }}">
                </div>
                <div>
                  <label>💵 Fee / Contract (round-trip)</label>
                  <input name="fee_per_contract" inputmode="decimal" value="{{ vals.fee_per_contract }}">
                </div>
              </div>

              <div class="hr"></div>
              <div class="rightActions">
                <button class="btn primary" type="submit">⚡ Calculate</button>
                <a class="btn" href="/dashboard">📊 Calendar</a>
                <a class="btn" href="/trades">📅 Trades</a>
              </div>
            </form>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">📌 Quick Notes</div>
            <div class="tiny" style="margin-top:10px; line-height:1.6">
              • Risk = (Entry → Stop) × 100 × contracts + fees<br>
              • Reward = (Target → Entry) × 100 × contracts − fees<br>
              • If you’re “hoping” instead of confirming… you’re gambling 😈
            </div>
          </div></div>
        </div>

        {% if out %}
          <div class="card" style="margin-top:12px"><div class="toolbar">
            <div class="pill">✅ Results</div>

            <div class="calcGrid">
              <div class="calcCard"><div class="k">🛑 Stop Price</div><div class="v">{{ money(out.stop_price) }}</div></div>
              <div class="calcCard"><div class="k">🎯 Target Price</div><div class="v">{{ money(out.tp_price) }}</div></div>
              <div class="calcCard"><div class="k">💵 Fees</div><div class="v">{{ money(out.fees) }}</div></div>
              <div class="calcCard"><div class="k">🧾 Total Contract Spend</div><div class="v">{{ money(out.total_spend) }}</div></div>
              <div class="calcCard"><div class="k">⚠️ Risk (Net)</div><div class="v">{{ money(out.risk_net) }}</div></div>
              <div class="calcCard"><div class="k">🎁 Reward (Net)</div><div class="v">{{ money(out.reward_net) }}</div></div>
              <div class="calcCard"><div class="k">⚖️ R:R</div><div class="v">{{ out.rr }}</div></div>
            </div>

            <div class="hr"></div>
            <div class="pill">📈 TP Ladder (Net)</div>
            <div class="hr"></div>

            <div style="overflow:auto">
              <table>
                <thead><tr><th>🎯 %</th><th>📈 TP Price</th><th>🎁 Reward Net</th></tr></thead>
                <tbody>
                  {% for r in out.ladder %}
                    <tr>
                      <td><b>+{{ r.pct }}%</b></td>
                      <td>{{ money(r.tp) }}</td>
                      <td><b>{{ money(r.net) }}</b></td>
                    </tr>
                  {% endfor %}
                </tbody>
              </table>
            </div>
          </div></div>
        {% endif %}
        """,
        out=out,
        err=err,
        vals=vals,
        money=money,
    )
    return render_page(content, active="calc")


# ============================================================
# Goals Tracker ✅ (Debt + Upwork + Income Projection)
# ============================================================
BASE_MONTHLY_INCOME = 7200.0  # Fitz baseline monthly income


def _month_bounds(d: date) -> Tuple[date, date]:
    first = d.replace(day=1)
    last_day = calendar.monthrange(d.year, d.month)[1]
    last = d.replace(day=last_day)
    return first, last


def upsert_daily_goal(track_date: str, payload: Dict[str, Any]) -> None:
    from mccain_capital.repositories import goals as repo
    repo.upsert_daily_goal(track_date=track_date, payload=payload)


def fetch_daily_goals(start_iso: str, end_iso: str) -> List[sqlite3.Row]:
    from mccain_capital.repositories import goals as repo
    return repo.fetch_daily_goals(start_iso=start_iso, end_iso=end_iso)


def fetch_daily_goal(track_date: str) -> Optional[sqlite3.Row]:
    from mccain_capital.repositories import goals as repo
    return repo.fetch_daily_goal(track_date=track_date)


def goals_tracker():
    """Compatibility delegator: runtime implementation lives in services.goals."""
    from mccain_capital.services import goals as svc
    return svc.goals_tracker()


# ============================================================
# Strategies pages ✅
# ============================================================
def _strategy_form(title: str, t: str, body: str, errors: List[str]) -> str:
    return render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">📌 {{ title }}</div>
          <div class="tiny" style="margin-top:10px; line-height:1.6">
            Keep it executable. If it’s too complex, you won’t follow it. ✅
          </div>

          {% if errors %}
            <div class="hr"></div>
            <div class="tiny" style="color:#ff8f8f">{% for e in errors %}• {{ e }}<br/>{% endfor %}</div>
          {% endif %}

          <div class="hr"></div>
          <form method="post">
            <div class="row">
              <div style="flex:2 1 320px">
                <label>Title</label>
                <input name="title" value="{{ t }}" placeholder="e.g. Fitz Midday CE Strike">
              </div>
            </div>

            <div style="margin-top:12px">
              <label>Body</label>
              <textarea name="body" placeholder="Entry trigger… Invalidation… Size… Stops… Targets…">{{ body }}</textarea>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">💾 Save</button>
              <a class="btn" href="/strategies">← Back</a>
            </div>
          </form>
        </div></div>
        """,
        title=title,
        t=t,
        body=body,
        errors=errors,
    )


def strategies_page():
    items = fetch_strategies()
    content = render_template_string(
        """
        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div class="pill">📌 Strategies</div>
            <div class="tiny" style="margin-top:10px; line-height:1.5">
              Build your playbook here. Add / edit anytime. ✅
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <a class="btn primary" href="/strategies/new">➕ New Strategy</a>
              <a class="btn" href="/dashboard">📊 Calendar</a>
            </div>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">Rules</div>
            <div class="tiny" style="margin-top:10px; line-height:1.6">
              • One page per setup<br>
              • Include: Entry trigger, invalidation, size rule, exit plan<br>
              • Keep it simple enough to execute under pressure
            </div>
          </div></div>
        </div>

        <div class="grid">
          {% for s in items %}
            <div class="card entry">
              <div class="entryTop">
                <div>
                  <div class="pill">📌 {{ s['title'] }}</div>
                  <div class="meta" style="margin-top:6px">🕒 Updated: {{ s['updated_at'] }}</div>
                </div>
                <div class="rightActions">
                  <a class="btn" href="/strategies/edit/{{ s['id'] }}">✏️ Edit</a>
                  <form id="del-s-{{ s['id'] }}" method="post" action="/strategies/delete/{{ s['id'] }}" style="display:inline"></form>
                  <button class="btn danger" type="button" onclick="confirmDelete('del-s-{{ s['id'] }}')">🗑️</button>
                </div>
              </div>
              <div class="notes">{{ s['body'] }}</div>
            </div>
          {% endfor %}

          {% if items|length == 0 %}
            <div class="card entry"><div class="meta">No strategies yet. Hit <b>New Strategy</b>. 📌</div></div>
          {% endif %}
        </div>
        """,
        items=items,
    )
    return render_page(content, active="strategies")


def strategies_new():
    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        body = (request.form.get("body") or "").strip()
        if not title or not body:
            return render_page(_strategy_form("New Strategy", title, body, ["Title and body required."]),
                               active="strategies")
        create_strategy(title, body)
        return redirect(url_for("strategies_page"))
    return render_page(_strategy_form("New Strategy", "", "", []), active="strategies")


def strategies_edit(sid: int):
    row = get_strategy(sid)
    if not row:
        abort(404)

    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        body = (request.form.get("body") or "").strip()
        if not title or not body:
            return render_page(_strategy_form("Edit Strategy", title, body, ["Title and body required."]),
                               active="strategies")
        update_strategy(sid, title, body)
        return redirect(url_for("strategies_page"))

    return render_page(_strategy_form("Edit Strategy", row["title"], row["body"], []), active="strategies")


def strategies_delete(sid: int):
    delete_strategy(sid)
    return redirect(url_for("strategies_page"))


# ============================================================
# Books ✅
# ============================================================
def books_page():
    books = list_books()
    content = render_template_string(
        """
        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div class="pill">📚 Trading Books</div>
            <div class="tiny" style="margin-top:10px; line-height:1.6">
              No web uploading. Drop PDFs into the <b>{{ books_dir }}</b> folder and refresh. ✅<br>
              Path example: <span class="kbd">./books</span>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <a class="btn" href="/dashboard">📊 Calendar</a>
              <a class="btn" href="/links">🔗 Links</a>
            </div>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">⭐ Current Favorites</div>
            <div class="tiny" style="margin-top:10px; line-height:1.6">
              • Trading in the Zone — Mark Douglas<br>
              • The Disciplined Trader — Mark Douglas<br>
              • Best Loser Wins — Tom Hougaard
            </div>
          </div></div>
        </div>

        <div class="card" style="margin-top:12px"><div class="toolbar">
          <div class="pill">📄 Library ({{ books|length }})</div>
          <div class="hr"></div>

          {% if books|length == 0 %}
            <div class="meta">No PDFs found in <b>{{ books_dir }}</b>.</div>
          {% else %}
            <div style="display:grid; gap:10px">
              {% for b in books %}
                <div class="card" style="border-radius:14px">
                  <div class="toolbar" style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap">
                    <div><b>{{ b.name }}</b></div>
                    <div class="rightActions">
                      <a class="btn primary" href="/books/open/{{ b.name }}">Open</a>
                    </div>
                  </div>
                </div>
              {% endfor %}
            </div>
          {% endif %}
        </div></div>
        """,
        books=books,
        books_dir=BOOKS_DIR,
    )
    return render_page(content, active="books")


def strat_page():
    """Dedicated reference page for The Strat trading framework."""
    content = r"""
    <style>
      .stratWrap{display:grid;gap:14px}
      .stratHero{
        padding:16px;
        border-radius:18px;
        border:1px solid rgba(0,229,255,.2);
        background:
          radial-gradient(800px 220px at 18% 0%, rgba(0,229,255,.14), transparent 65%),
          linear-gradient(180deg, rgba(0,229,255,.08), rgba(0,0,0,0) 70%),
          var(--panel);
        box-shadow:var(--shadow-soft);
      }
      .stratTitle{font-size:28px;font-weight:900;letter-spacing:.2px;line-height:1.15;margin:0}
      .stratSub{margin-top:8px;color:var(--muted);font-size:14px;line-height:1.6;max-width:980px}
      .stratPills{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
      .stratPill{
        display:inline-flex;align-items:center;gap:6px;padding:6px 10px;border-radius:999px;
        border:1px solid rgba(0,229,255,.28);background:rgba(0,229,255,.1);font-size:12px;color:var(--text)
      }
      .stratGrid3{display:grid;grid-template-columns:1fr;gap:12px}
      .stratGrid2{display:grid;grid-template-columns:1fr;gap:12px}
      @media (min-width: 980px){ .stratGrid3{grid-template-columns:repeat(3,minmax(0,1fr));} }
      @media (min-width: 860px){ .stratGrid2{grid-template-columns:repeat(2,minmax(0,1fr));} }
      .stratCard{
        padding:14px;
        border-radius:16px;
        border:1px solid rgba(0,229,255,.18);
        background: linear-gradient(180deg, rgba(0,229,255,.06), rgba(255,255,255,.01));
      }
      .stratCard h3{margin:0;font-size:18px;line-height:1.2}
      .stratCard .meta{margin-top:6px}
      .stratCard ul{margin:10px 0 0 18px;line-height:1.7;padding:0}
      .stratChecklistTop{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}
      .stratProgress{display:flex;align-items:center;gap:10px}
      .stratProgressBar{
        width:180px;height:10px;border-radius:999px;background:rgba(255,255,255,.08);overflow:hidden;
        border:1px solid rgba(255,255,255,.14)
      }
      .stratProgressFill{
        width:0%;height:100%;
        background:linear-gradient(90deg, rgba(0,229,255,.9), rgba(70,255,186,.95));
        transition:width .18s ease;
      }
      .stratProgressText{font-size:12px;color:var(--muted);font-weight:700;min-width:96px;text-align:right}
      .stratActions{display:flex;gap:10px;margin-top:12px;flex-wrap:wrap}
      .stratTableWrap{overflow:auto;margin-top:10px;-webkit-overflow-scrolling:touch}
      .stratTableWrap table{min-width:820px}
      .checkRow.checked{
        border-color: rgba(53,208,127,.45);
        background: rgba(53,208,127,.08);
      }
      @media (max-width: 720px){
        .stratWrap{gap:10px}
        .stratHero{
          padding:12px;
          border-radius:14px;
        }
        .stratTitle{
          font-size:21px;
          line-height:1.2;
          letter-spacing:.1px;
        }
        .stratSub{
          font-size:13px;
          line-height:1.5;
          margin-top:6px;
        }
        .stratPills{margin-top:10px;gap:6px}
        .stratPill{font-size:11px;padding:5px 8px}
        .stratCard{
          padding:12px;
          border-radius:14px;
        }
        .stratCard h3{font-size:16px}
        .stratCard ul{
          margin-top:8px;
          margin-left:16px;
          line-height:1.6;
        }
        .stratChecklistTop{
          display:grid;
          grid-template-columns:1fr;
          gap:8px;
        }
        .stratProgress{
          justify-content:space-between;
          width:100%;
        }
        .stratProgressBar{
          width:100%;
          max-width:none;
          flex:1 1 auto;
          min-width:0;
        }
        .stratProgressText{
          min-width:auto;
          text-align:right;
          padding-left:8px;
          font-size:11px;
          white-space:nowrap;
        }
        .checklist{
          gap:8px;
          max-width:none;
        }
        .checkRow{
          grid-template-columns:20px 1fr;
          gap:10px;
          padding:12px;
          border-radius:12px;
        }
        .checkRow input[type="checkbox"]{
          width:20px;
          height:20px;
          margin-top:1px;
        }
        .checkText{font-size:13px;line-height:1.45}
        .stratActions{
          display:grid;
          grid-template-columns:1fr 1fr;
          gap:8px;
        }
        .stratActions .btn{
          width:100%;
          padding:11px 10px;
          font-size:13px;
        }
        .stratTableWrap{
          margin-left:-4px;
          margin-right:-4px;
          padding:0 4px 2px;
        }
        .stratTableWrap table{
          min-width:680px;
          font-size:12px;
        }
      }
    </style>

    <div class="stratWrap">
      <section class="stratHero">
        <h2 class="stratTitle">🧠 The Strat Core Playbook</h2>
        <div class="stratSub">
          Quick reference for <b>candle types</b>, <b>combo patterns</b>, <b>universal truths</b>, and <b>stop structure</b>.
          Use this as your pre-trade quality gate before entry.
        </div>
        <div class="stratPills">
          <span class="stratPill">🕯️ Structure</span>
          <span class="stratPill">🔁 Patterns</span>
          <span class="stratPill">🧭 Context</span>
          <span class="stratPill">🛡️ Risk</span>
        </div>
      </section>

      <section class="stratGrid3">
        <article class="stratCard">
          <h3>🕯️ Candle Types</h3>
          <div class="meta">The 1-2-3 language</div>
          <ul>
            <li><b>1</b> = inside bar (range contraction)</li>
            <li><b>2</b> = directional break (higher high or lower low)</li>
            <li><b>3</b> = outside bar (breaks both sides)</li>
          </ul>
        </article>

        <article class="stratCard">
          <h3>🔁 Core Combos</h3>
          <div class="meta">Common setups</div>
          <ul>
            <li><b>2-1-2</b> continuation after pause</li>
            <li><b>3-1-2</b> volatility → pause → break</li>
            <li><b>2-2</b> reversal (your main trigger)</li>
          </ul>
        </article>

        <article class="stratCard">
          <h3>🧭 Timeframe Continuity</h3>
          <div class="meta">Context matters</div>
          <ul>
            <li>Trade <b>with</b> higher timeframe intent</li>
            <li>Expect cleaner moves when HTF aligns</li>
            <li>Be selective when HTF disagrees</li>
          </ul>
        </article>
      </section>

      <section class="stratGrid2">
        <article class="stratCard">
          <h3>🌎 Universal Truths</h3>
          <div class="meta">Keep these on your screen</div>
          <ul>
            <li><b>Location is king:</b> levels and liquidity drive decisions.</li>
            <li><b>Direction needs proof:</b> break plus follow-through beats hoping.</li>
            <li><b>Range = risk:</b> mid-range trades are hardest to manage.</li>
            <li><b>Losses are part of the plan:</b> define risk before entry.</li>
            <li><b>Your edge is repetition:</b> same process, same sizing.</li>
          </ul>
        </article>

        <article class="stratCard">
          <h3>🛡️ Stop Loss Structure</h3>
          <div class="meta">Simple, consistent, non-negotiable</div>
          <div style="margin-top:10px; line-height:1.7;">
            <div><b>Default rule:</b> stop goes beyond the level that invalidates the setup.</div>
            <div class="meta" style="margin-top:6px;">
              Beyond reversal-candle extreme, key level (PDH/PDL), or HTF swing.
            </div>
            <div style="margin-top:12px;"><b>Options risk cap:</b> keep premium risk in plan (e.g. 20-25%).</div>
            <div class="meta" style="margin-top:6px;">If your real stop exceeds cap, reduce size or pass.</div>
          </div>
        </article>
      </section>

      <section class="stratCard">
        <div class="stratChecklistTop">
          <div>
            <h3 style="margin:0">✅ Pre-Trade Checklist</h3>
            <div class="meta" style="margin-top:6px">Saved locally in your browser.</div>
          </div>
          <div class="stratProgress">
            <div class="stratProgressBar"><div id="stratProgressFill" class="stratProgressFill"></div></div>
            <div id="stratProgressText" class="stratProgressText">0/6 complete</div>
          </div>
        </div>

        <div class="checklist" style="margin-top:10px;">
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="level" />
            <div class="checkText"><b>Location:</b> at PDH/PDL, CDH/CDL, HTF swing, or VWAP zone</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="htf" />
            <div class="checkText"><b>HTF intent:</b> 45m/1h agrees (or explicit fade at major level)</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="structure" />
            <div class="checkText"><b>Structure:</b> 30m defines box, range, and pivots clearly</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="trigger" />
            <div class="checkText"><b>Trigger:</b> 15m 2-2 reversal with 5m expansion confirmation</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="risk" />
            <div class="checkText"><b>Risk:</b> stop defined, size fixed, premium cap respected</div>
          </label>
          <label class="checkRow">
            <input type="checkbox" class="strat-check" data-key="plan" />
            <div class="checkText"><b>Plan:</b> targets chosen and no revenge re-entry rule acknowledged</div>
          </label>
        </div>

        <div class="stratActions">
          <button class="btn" type="button" onclick="stratChecklistClear()">🧹 Clear</button>
          <button class="btn primary" type="button" onclick="window.location.href='/trades'">📒 Go to Trades</button>
        </div>
      </section>

      <section class="stratCard">
        <h3>🧩 Combo Quick Reference</h3>
        <div class="meta" style="margin-top:6px">Use this like a decision tree.</div>
        <div class="stratTableWrap">
          <table class="table">
          <thead>
            <tr>
              <th>Pattern</th>
              <th>Meaning</th>
              <th>What you want to see</th>
              <th>Invalidation / stop anchor</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><b>2-1-2</b></td>
              <td>Continuation after contraction</td>
              <td>Break → inside → break in same direction</td>
              <td>Beyond the <b>1</b> range / setup level</td>
            </tr>
            <tr>
              <td><b>3-1-2</b></td>
              <td>Expansion then decision</td>
              <td>Outside bar sets both sides → inside bar → break with intent</td>
              <td>Beyond the inside bar / opposite side of 3</td>
            </tr>
            <tr>
              <td><b>2-2</b></td>
              <td>Reversal / failed direction</td>
              <td>Push fails at key level → reverse break + follow-through</td>
              <td>Beyond the reversal extreme (the “failed direction” point)</td>
            </tr>
          </tbody>
        </table>
        </div>
      </section>
    </div>

    <script>
      (function initStratChecklist(){
        try{
          const key = "strat_checklist_v1";
          const saved = JSON.parse(localStorage.getItem(key) || "{}");
          const checks = Array.from(document.querySelectorAll(".strat-check"));
          const progressFill = document.getElementById("stratProgressFill");
          const progressText = document.getElementById("stratProgressText");

          function syncProgress(){
            const total = checks.length;
            const done = checks.filter(cb => cb.checked).length;
            const pct = total ? Math.round((done / total) * 100) : 0;
            if (progressFill) progressFill.style.width = pct + "%";
            if (progressText) progressText.textContent = done + "/" + total + " complete";
            checks.forEach(cb => {
              const row = cb.closest(".checkRow");
              if (row) row.classList.toggle("checked", cb.checked);
            });
          }

          checks.forEach(cb=>{
            const k = cb.getAttribute("data-key");
            cb.checked = !!saved[k];
            cb.addEventListener("change", ()=>{
              const next = JSON.parse(localStorage.getItem(key) || "{}");
              next[k] = cb.checked;
              localStorage.setItem(key, JSON.stringify(next));
              syncProgress();
            });
          });
          syncProgress();
          window.stratChecklistClear = function(){
            localStorage.removeItem(key);
            checks.forEach(cb => cb.checked = false);
            syncProgress();
          }
        }catch(e){
          // ignore localStorage issues
          window.stratChecklistClear = function(){
            document.querySelectorAll(".strat-check").forEach(cb => {
              cb.checked = false;
              const row = cb.closest(".checkRow");
              if (row) row.classList.remove("checked");
            });
            const progressFill = document.getElementById("stratProgressFill");
            const progressText = document.getElementById("stratProgressText");
            if (progressFill) progressFill.style.width = "0%";
            if (progressText) progressText.textContent = "0/6 complete";
          }
        }
      })();
    </script>
    """
    return render_page(content, active="strat", title="🧠 The Strat")


def books_open(name: str):
    fn = safe_filename(name)
    path = os.path.join(BOOKS_DIR, fn)
    if not os.path.exists(path) or not fn.lower().endswith(".pdf"):
        abort(404)
    return send_file(path, as_attachment=False)


# ============================================================
# Links ✅
# ============================================================
def links_page():
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">🔗 Trading Links</div>
          <div class="tiny" style="margin-top:10px; line-height:1.6">
            Open in new tab. ✅
          </div>
          <div class="hr"></div>
          <div class="rightActions">
            <a class="btn primary" href="https://trade.vanquishtrader.com/" target="_blank" rel="noopener">Vanquish Trader</a>
            <a class="btn" href="https://www.vanquishtrader.com/dashboard" target="_blank" rel="noopener">Prop Dashboard</a>
            <a class="btn" href="{{ url_for('chart') }}" target="_blank" rel="noopener">TradingView Charts</a>
          </div>
        </div></div>
        """
    )
    return render_page(content, active="links")


# ============================================================
# Export
# ============================================================
def export_all() -> Dict[str, Any]:
    with db() as conn:
        j = conn.execute("SELECT * FROM entries ORDER BY entry_date DESC, updated_at DESC").fetchall()
        t = conn.execute("SELECT * FROM trades ORDER BY trade_date DESC, id DESC").fetchall()
        s = conn.execute("SELECT * FROM strategies ORDER BY updated_at DESC").fetchall()
    return {
        "app": "mccain-capital-journal-trades",
        "version": 3,
        "exported_at": now_iso(),
        "entries": [dict(r) for r in j],
        "trades": [dict(r) for r in t],
        "strategies": [dict(r) for r in s],
    }


def export_json():
    payload = export_all()
    fd, out_path = tempfile.mkstemp(prefix="mccain_export_", suffix=".json")
    os.close(fd)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return send_file(out_path, as_attachment=True, download_name="mccain_capital_export.json")


def backup_data():
    """Download a zip backup containing DB + uploads."""
    stamp = now_et().strftime("%Y%m%d_%H%M%S")
    fd, out_path = tempfile.mkstemp(prefix="mccain_backup_", suffix=".zip")
    os.close(fd)
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if os.path.exists(DB_PATH):
            zf.write(DB_PATH, arcname="data/journal.db")

        if os.path.isdir(UPLOAD_DIR):
            for root, _, files in os.walk(UPLOAD_DIR):
                for name in files:
                    full = os.path.join(root, name)
                    rel = os.path.relpath(full, UPLOAD_DIR)
                    zf.write(full, arcname=f"data/uploads/{rel}")

        meta = {
            "exported_at": now_iso(),
            "db_path": DB_PATH,
            "upload_dir": UPLOAD_DIR,
            "app": "mccain-capital",
        }
        zf.writestr("data/meta.json", json.dumps(meta, ensure_ascii=False, indent=2))

    return send_file(
        out_path,
        as_attachment=True,
        download_name=f"mccain_capital_backup_{stamp}.zip",
        mimetype="application/zip",
    )


def restore_data():
    """Restore DB + uploads from a backup zip."""
    if request.method == "GET":
        content = render_template_string(
            """
            <div class="card"><div class="toolbar">
              <div class="pill">♻️ Restore Backup</div>
              <div class="tiny" style="margin-top:10px;line-height:1.6">
                Upload a backup zip created from <b>/admin/backup</b>.<br>
                This will replace <b>{{ db_path }}</b> and merge files into <b>{{ upload_dir }}</b>.
              </div>
              <div class="hr"></div>
              <form method="post" enctype="multipart/form-data">
                <label>Backup ZIP</label>
                <input type="file" name="backup_zip" accept=".zip,application/zip" required />
                <div class="hr"></div>
                <div class="rightActions">
                  <button class="btn danger" type="submit">Restore</button>
                  <a class="btn" href="/dashboard">Cancel</a>
                </div>
              </form>
            </div></div>
            """,
            db_path=DB_PATH,
            upload_dir=UPLOAD_DIR,
        )
        return render_page(content, active="dashboard")

    f = request.files.get("backup_zip")
    if not f or not f.filename:
        return render_page(_simple_msg("Please choose a backup zip file."), active="dashboard")

    try:
        with zipfile.ZipFile(f.stream) as zf:
            names = zf.namelist()
            if not names:
                return render_page(_simple_msg("Backup zip is empty."), active="dashboard")

            allowed_prefixes = ("data/journal.db", "data/uploads/", "data/meta.json")
            for n in names:
                if n.startswith("/") or ".." in n:
                    return render_page(_simple_msg("Backup zip contains unsafe paths."), active="dashboard")
                if not any(n == p or n.startswith(p) for p in allowed_prefixes):
                    return render_page(_simple_msg("Backup zip contains unsupported files."), active="dashboard")

            db_member = "data/journal.db"
            if db_member in names:
                os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
                with zf.open(db_member) as src, open(DB_PATH, "wb") as dst:
                    dst.write(src.read())

            os.makedirs(UPLOAD_DIR, exist_ok=True)
            for n in names:
                if not n.startswith("data/uploads/") or n.endswith("/"):
                    continue
                rel = n[len("data/uploads/"):]
                out_path = os.path.join(UPLOAD_DIR, rel)
                out_dir = os.path.dirname(out_path)
                if out_dir:
                    os.makedirs(out_dir, exist_ok=True)
                with zf.open(n) as src, open(out_path, "wb") as dst:
                    dst.write(src.read())

    except zipfile.BadZipFile:
        return render_page(_simple_msg("Invalid zip file."), active="dashboard")
    except Exception as e:
        return render_page(_simple_msg(f"Restore failed: {e}"), active="dashboard")

    return render_page(_simple_msg("Backup restore completed."), active="dashboard")


# ============================================================
# Payouts (kept simple; your existing numbers)
# ============================================================
PAYOUT_ACCT_SIZE = float(os.environ.get("PAYOUT_ACCT_SIZE", "50000"))
PROFIT_BUFFER_LEVEL_50K = 52875.0
FIXED_LOSS_LIMIT_50K = 50375.0
DEFAULT_PROTECT_BUFFER = float(os.environ.get("PAYOUT_PROTECT_BUFFER", "1000"))


def payout_summary(balance: Optional[float], protect_buffer: float = DEFAULT_PROTECT_BUFFER) -> Dict[str, Any]:
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


def payouts_page():
    """Compatibility delegator: runtime implementation lives in services.goals."""
    from mccain_capital.services import goals as svc
    return svc.payouts_page()


# ============================================================
# Boot
# ============================================================
# DB init moved to package startup (mccain_capital.create_app).


def chart():
    '''
    Full-screen TradingView chart embed.
    Use the chart UI "Compare / Add Symbol" to add additional tickers (overlays).

    Query params:
      - symbol (default: AMEX:SPY)
      - interval (default: 5)
    '''
    symbol = request.args.get("symbol", "AMEX:SPY")
    interval = request.args.get("interval", "5")

    page = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{APP_TITLE} — Chart</title>
  <style>
    html, body {{ height: 100%; margin: 0; }}
    body {{ background: #0b0f19; }}
    .wrap {{
      height: 100vh;
      display: grid;
      grid-template-rows: auto 1fr;
    }}
    .bar {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      padding: 10px 12px;
      border-bottom: 1px solid rgba(255,255,255,.10);
      color: rgba(255,255,255,.92);
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
    }}
    .bar a {{
      color: rgba(255,255,255,.92);
      text-decoration: none;
      padding: 8px 10px;
      border-radius: 10px;
      border: 1px solid rgba(255,255,255,.14);
      background: rgba(255,255,255,.08);
    }}
    .bar label {{ opacity: .85; font-size: 14px; }}
    .bar input {{
      width: 230px;
      padding: 8px 10px;
      border-radius: 10px;
      border: 1px solid rgba(255,255,255,.14);
      background: rgba(255,255,255,.06);
      color: rgba(255,255,255,.95);
      outline: none;
    }}
    .bar input.small {{ width: 90px; }}
    .bar button {{
      padding: 8px 12px;
      border-radius: 10px;
      border: 1px solid rgba(255,255,255,.14);
      background: rgba(255,255,255,.12);
      color: rgba(255,255,255,.95);
      cursor: pointer;
    }}
    .hint {{
      margin-left: auto;
      opacity: .75;
      font-size: 13px;
      white-space: nowrap;
    }}
    #tv_container {{ height: 100%; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="bar">
      <a href="/">🏠 Home</a>
      <label for="sym">Symbol</label>
      <input id="sym" value="{symbol}" placeholder="e.g. AMEX:SPY or SP:SPX" />
      <label for="intv">Interval</label>
      <input id="intv" class="small" value="{interval}" placeholder="1,5,15,60,D" />
      <button id="go">Load</button>
      <div class="hint">Tip: use <b>Compare / Add Symbol</b> inside the chart to add tickers.</div>
    </div>
    <div id="tv_container"></div>
  </div>

  <script src="https://s3.tradingview.com/tv.js"></script>
  <script>
    function loadChart(symbol, interval) {{
      const container = document.getElementById("tv_container");
      container.innerHTML = "";
      new TradingView.widget({{
        autosize: true,
        symbol: symbol,
        interval: interval,
        timezone: "America/New_York",
        theme: "dark",
        style: "1",
        locale: "en",
        enable_publishing: false,
        allow_symbol_change: true,
        container_id: "tv_container",
      }});
    }}

    document.getElementById("go").addEventListener("click", () => {{
      const s = (document.getElementById("sym").value || "AMEX:SPY").trim();
      const i = (document.getElementById("intv").value || "5").trim();
      const url = new URL(window.location.href);
      url.searchParams.set("symbol", s);
      url.searchParams.set("interval", i);
      window.location.href = url.toString();
    }});

    loadChart("{symbol}", "{interval}");
  </script>
</body>
</html>
"""
    return render_template_string(page)


if __name__ == "__main__":
    from mccain_capital.routes import register_all_routes

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "5001"))
    debug = os.environ.get("DEBUG", "0") == "1"
    if not getattr(app, "_routes_registered", False):
        register_all_routes(app)
        app._routes_registered = True
    init_db()
    app.run(host=host, port=port, debug=debug)
