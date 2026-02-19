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
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber
import pytesseract
from PIL import Image, ImageEnhance, ImageOps
from flask import (
    Flask,
    abort,
    redirect,
    render_template_string,
    request,
    send_file,
    url_for,
    jsonify,
    make_response,
    flash,
)
from pdf2image import convert_from_path
from werkzeug.utils import secure_filename
from zoneinfo import ZoneInfo

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

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_MB * 1024 * 1024


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


def _prep_for_ocr(img: Image.Image) -> Image.Image:
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

    # Lazy imports so HTML parsing can work without OCR deps installed
    try:
        from pdf2image import convert_from_path
        import pytesseract
    except Exception as e:
        return "", [f"OCR dependencies missing. Install pdf2image + pytesseract. Error: {e}"]

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
            inserted += 1
        conn.commit()

    open_count = sum(sum(l["qty"] for l in lots) for lots in open_lots.values() if lots)
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
            inserted += 1

        conn.commit()

    return inserted, errors


def fetch_trades(d: str = "", q: str = "") -> List[sqlite3.Row]:
    d = (d or "").strip()
    q = (q or "").strip()

    sql = "SELECT * FROM trades"
    where = []
    params: List[Any] = []

    if d:
        where.append("trade_date = ?")
        params.append(d)

    if q:
        where.append("(ticker LIKE ? OR opt_type LIKE ? OR raw_line LIKE ?)")
        like = f"%{q}%"
        params.extend([like, like, like])

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += " ORDER BY trade_date DESC, id DESC"

    with db() as conn:
        return list(conn.execute(sql, params).fetchall())


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
    with db() as conn:
        return list(conn.execute("SELECT * FROM strategies ORDER BY updated_at DESC").fetchall())


def get_strategy(sid: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM strategies WHERE id = ?", (sid,)).fetchone()


def create_strategy(title: str, body: str) -> int:
    created = now_iso()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO strategies (title, body, created_at, updated_at)
            VALUES (?,?,?,?)
            """,
            (title.strip(), body.strip(), created, created),
        )
        return int(cur.lastrowid)


def update_strategy(sid: int, title: str, body: str) -> None:
    updated = now_iso()
    with db() as conn:
        conn.execute(
            """
            UPDATE strategies
            SET title = ?, body = ?, updated_at = ?
            WHERE id = ?
            """,
            (title.strip(), body.strip(), updated, sid),
        )


def delete_strategy(sid: int) -> None:
    with db() as conn:
        conn.execute("DELETE FROM strategies WHERE id = ?", (sid,))


# ============================================================
# Books (folder-only, no web upload) ✅
# ============================================================
def safe_filename(name: str) -> str:
    name = (name or "").strip()
    name = name.replace("\\", "/").split("/")[-1]
    name = re.sub(r"[^a-zA-Z0-9._ -]+", "", name)
    return name


def list_books() -> List[Dict[str, str]]:
    os.makedirs(BOOKS_DIR, exist_ok=True)
    files = []
    for fn in sorted(os.listdir(BOOKS_DIR)):
        if fn.lower().endswith(".pdf"):
            files.append({"name": fn, "path": os.path.join(BOOKS_DIR, fn)})
    return files


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
# UI (HTML) — kept as-is except removed duplicate clock script
# ============================================================
BASE_HTML = r"""
<!doctype html>
<html lang="en">
<head>
    <link rel="icon" href="{{ url_for('static', filename='favicon.ico') }}">
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
  <title>{{ title }}</title>
  <style>
@media (max-width: 720px){
  .wrap{ padding: 14px 10px 50px; }
  .heat{ border-radius: 14px; }
  .heat table{ table-layout: fixed; width: 100%; }
  th{ font-size: 11px; padding: 8px 6px; }
  .daycell{ height: 56px; padding: 8px 8px; }
  .daynum{ font-size: 11px; opacity: .9; }
  .daypnl{
    position: absolute;
    bottom: 6px;
    left: 8px;
    font-weight: 900;
    font-size: 11px;
    line-height: 1;
    max-width: calc(100% - 16px);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .daybal{ display:none; }
}

    :root{
      --bg:#07070a;
      --panel:#0f0f16;
      --panel2:#0b0b10;
      --text:#e7e7f2;
      --muted:#a7a7bd;
      --gold:#d6b25e;
      --border:rgba(214,178,94,.18);
      --shadow: 0 12px 34px rgba(0,0,0,.55);
      --ring: 0 0 0 3px rgba(214,178,94,.22);
      --overlay: rgba(0,0,0,.55);
      --green: 46, 204, 113;
      --red: 231, 76, 60;
    }

    *{box-sizing:border-box}
    body{
      margin:0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Apple Color Emoji","Segoe UI Emoji";
      background: radial-gradient(1200px 800px at 20% 0%, rgba(214,178,94,.10), transparent 60%),
                  radial-gradient(1000px 700px at 95% 15%, rgba(214,178,94,.08), transparent 55%),
                  var(--bg);
      color:var(--text);
    }

    .wrap{max-width:1100px;margin:0 auto;padding:24px 16px 60px}
    .topbar{
      top: 0;
      z-index: 50;
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:12px;
      margin-bottom:14px;
      flex-wrap:wrap;
      padding:10px 0;
      backdrop-filter: blur(8px);
    }
    .brand{display:flex;align-items:center;gap:12px;justify-content:flex-start;min-width:0}
    .brandText{min-width:0}

    .logo{
      width:44px;height:44px;border-radius:999px;border:1px solid var(--border);
      background: linear-gradient(145deg, rgba(214,178,94,.08), rgba(0,0,0,0));
      display:grid;place-items:center;overflow:hidden;box-shadow:var(--shadow);
      flex:0 0 auto;
    }
    .logo img{width:100%;height:100%;object-fit:cover;display:block}
    .logo .fallback{font-weight:900;color:var(--gold);letter-spacing:.5px}

    h1{
      font-size:18px;margin:0;letter-spacing:.3px;
      text-align:left;
      white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
    }
    .sub{color:var(--muted);font-size:12px;margin-top:2px}

    .nav{display:flex;gap:10px;flex-wrap:wrap}

    .card{
      background: linear-gradient(180deg, rgba(214,178,94,.06), rgba(0,0,0,0) 70%), var(--panel);
      border:1px solid var(--border);
      border-radius:16px;
      box-shadow: var(--shadow);
    }
        .nav{display:flex;gap:10px;flex-wrap:wrap;align-items:center}

.btn.active{
  background: linear-gradient(180deg, rgba(214,178,94,.18), rgba(214,178,94,.05));
  border-color: rgba(214,178,94,.45);
}

.menuMore{ position: relative; display:inline-flex; }
.moreMenu{
  position:absolute;
  top: calc(100% + 10px);
  right: 0;
  width: 240px;
  padding: 10px;
  border-radius: 16px;
  border: 1px solid rgba(214,178,94,.18);
  background: linear-gradient(180deg, rgba(214,178,94,.06), rgba(0,0,0,0) 70%), var(--panel);
  box-shadow: var(--shadow);
  display:none;
  z-index: 120;
}
.moreMenu.open{ display:block; }
.moreMenu .btn{ width:100%; justify-content:flex-start; }
.moreMenu .hr{ margin: 8px 0; }

.moreBtn{ padding-left: 14px; padding-right: 14px; }


    .toolbar{padding:14px}

    label{font-size:12px;color:var(--muted)}
    input, select, textarea{
      width:100%;
      background: var(--panel2);
      border:1px solid rgba(255,255,255,.08);
      color:var(--text);
      padding:10px 12px;
      border-radius:12px;
      outline:none;
    }
    textarea{min-height:130px;resize:vertical}
    input:focus,select:focus,textarea:focus{box-shadow:var(--ring);border-color:rgba(214,178,94,.35)}

    .btn{
      display:inline-flex;align-items:center;justify-content:center;gap:8px;
      padding:10px 12px;border-radius:12px;border:1px solid rgba(255,255,255,.10);
      background: rgba(255,255,255,.03);color:var(--text);cursor:pointer;text-decoration:none;white-space:nowrap;
    }
    .btn:hover{border-color:rgba(214,178,94,.35)}
    .btn.primary{
      background: linear-gradient(180deg, rgba(214,178,94,.20), rgba(214,178,94,.05));
      border-color: rgba(214,178,94,.35);
    }
    .btn.danger{border-color: rgba(255,90,90,.35)}
    .btn.danger:hover{box-shadow:0 0 0 3px rgba(255,90,90,.10)}

    .pill{
      display:inline-flex;align-items:center;padding:4px 10px;border-radius:999px;
      border:1px solid rgba(214,178,94,.25);
      background: rgba(214,178,94,.08);
      color:var(--gold);
      font-size:12px;
    }

    .row{display:flex;gap:12px;flex-wrap:wrap;align-items:end}
    .row>div{flex:1 1 220px}

    .twoCol{display:grid;grid-template-columns:1fr;gap:12px}
    @media(min-width:880px){.twoCol{grid-template-columns:1.1fr .9fr}}

    .hr{height:1px;background: rgba(214,178,94,.12);margin:12px 0}
    .meta{color:var(--muted);font-size:12px}
    .grid{display:grid;grid-template-columns:1fr;gap:12px;margin-top:12px}

    .entry{padding:14px}
    .entryTop{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;flex-wrap:wrap}
    .notes{white-space:pre-wrap;line-height:1.45;margin-top:10px}

    table{
      width:100%;
      border-collapse:separate;
      border-spacing:0;
      overflow:hidden;
      border-radius:14px;
      border:1px solid rgba(214,178,94,.15)
    }
    th,td{
      padding:10px 10px;
      font-size:12px;
      border-bottom:1px solid rgba(214,178,94,.10)
    }
    th{
      color:var(--muted);
      text-align:left;
      background: rgba(0,0,0,.18)
    }
    tr:last-child td{border-bottom:none}

    .statRow{display:flex;gap:10px;flex-wrap:wrap;margin-top:10px}
    .stat{
      flex:1 1 160px;
      padding:12px;
      border-radius:14px;
      border:1px solid rgba(214,178,94,.15);
      background: rgba(0,0,0,.18);
    }
    .stat .k{font-size:12px;color:var(--muted)}
    .stat .v{font-size:18px;font-weight:900;margin-top:6px}

    .rightActions{display:flex;gap:10px;flex-wrap:wrap;justify-content:flex-end}

    .heat{width:100%;border:1px solid rgba(214,178,94,.15);border-radius:16px;overflow:hidden}
    .heat table{border:none;border-radius:0}
    .daycell{height:78px;vertical-align:top;padding:10px;position:relative}
    .daynum{font-size:12px;color:var(--muted)}
    .daypnl{position:absolute;bottom:10px;left:10px;font-weight:900}

    .tiny{font-size:12px;color:var(--muted)}
    .kbd{border:1px solid rgba(255,255,255,.14);padding:2px 6px;border-radius:6px;font-size:12px;color:var(--muted)}

    .topRight{display:flex;align-items:center;gap:10px;flex-wrap:wrap;justify-content:flex-end;}
    .clockPill{
      display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;
      border:1px solid rgba(214,178,94,.35);background: rgba(214,178,94,.08);color:var(--gold);
      font-size:12px;font-weight:800;white-space:nowrap;
    }
    .clockTime{color:var(--text);font-weight:900;letter-spacing:.2px}

    @keyframes pulseGreen {
      0%   { box-shadow: 0 0 0 rgba(var(--green), 0); }
      50%  { box-shadow: 0 0 22px rgba(var(--green), .55); }
      100% { box-shadow: 0 0 0 rgba(var(--green), 0); }
    }
    @keyframes pulseRed {
      0%   { box-shadow: 0 0 0 rgba(var(--red), 0); }
      50%  { box-shadow: 0 0 22px rgba(var(--red), .55); }
      100% { box-shadow: 0 0 0 rgba(var(--red), 0); }
    }
    .glow-green{
      border-color: rgba(var(--green), .55) !important;
      box-shadow: 0 0 16px rgba(var(--green), .45), inset 0 0 16px rgba(var(--green), .15);
      animation: pulseGreen 2.6s ease-in-out infinite;
    }
    .glow-red{
      border-color: rgba(var(--red), .55) !important;
      box-shadow: 0 0 16px rgba(var(--red), .45), inset 0 0 16px rgba(var(--red), .15);
      animation: pulseRed 2.6s ease-in-out infinite;
    }

    .hamburger{
      display:none;width:44px;height:44px;border-radius:12px;border:1px solid rgba(255,255,255,.10);
      background: rgba(255,255,255,.03);box-shadow: var(--shadow);
      align-items:center;justify-content:center;cursor:pointer;
    }
    .hamburger:hover{border-color:rgba(214,178,94,.35)}
    .hamburger svg{width:22px;height:22px;opacity:.95}

    .drawerOverlay{display:none;position:fixed; inset:0;background: rgba(0,0,0,.55);z-index: 80;}
    .drawer{
      position:fixed;top:0; right:-340px;height:100%;width:340px;max-width: 88vw;
      background: linear-gradient(180deg, rgba(214,178,94,.06), rgba(0,0,0,0) 70%), var(--panel);
      border-left: 1px solid rgba(214,178,94,.18);box-shadow: var(--shadow);
      z-index: 90;transition: right .22s ease;padding: 14px;overflow:auto;
    }
    .drawer.open{ right: 0; }
    .drawerOverlay.open{ display:block; }

    .drawerHead{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom: 10px;}
    .drawerTitle{font-weight:900;color: var(--text);font-size: 14px;letter-spacing: .2px;}
    .drawerClose{
      width:40px;height:40px;border-radius:12px;border:1px solid rgba(255,255,255,.10);
      background: rgba(255,255,255,.03);display:flex;align-items:center;justify-content:center;cursor:pointer;
    }
    .drawerClose:hover{border-color:rgba(214,178,94,.35)}
    @media (max-width: 640px){
  .drawerGrid{ grid-template-columns: 1fr; }
  .drawerGrid a{ padding: 14px; }
}

    .drawerGrid a{ justify-content:flex-start; padding:12px; }

    @media (max-width: 640px){
      .wrap{padding:14px 10px 60px}
      .nav{display:none}
      .hamburger{display:flex}
      .sub{display:none}
      .logo{width:34px;height:34px}
      h1{font-size:15px}
      .topbar{padding:6px 0; margin-bottom:10px}
      .glow-green,.glow-red{animation-duration:3.4s}
    }

    .calcGrid{
      display:grid;
      grid-template-columns: repeat(2, 1fr);
      gap:10px;
      margin-top:12px;
    }
    @media(min-width:900px){ .calcGrid{ grid-template-columns: repeat(3, 1fr); } }
    .calcCard{
      padding:12px;border-radius:14px;border:1px solid rgba(214,178,94,.15);
      background: rgba(0,0,0,.18);
    }
    .calcCard .k{font-size:12px;color:var(--muted)}
    .calcCard .v{font-size:18px;font-weight:900;margin-top:6px}

    .rowMenu{ position: relative; display:inline-flex; }
.rowMenuBtn{
  width:36px;height:36px;padding:0;
  border-radius:12px;
  display:inline-flex;align-items:center;justify-content:center;
}
.rowMenuPanel{
  position:absolute;
  right:0;
  top: calc(100% + 8px);
  min-width: 160px;
  padding: 8px;
  border-radius: 14px;
  border: 1px solid rgba(214,178,94,.18);
  background: linear-gradient(180deg, rgba(214,178,94,.06), rgba(0,0,0,0) 70%), var(--panel);
  box-shadow: var(--shadow);
  display:none;
  z-index: 200;
}
.rowMenuPanel.open{ display:block; }
.rowMenuPanel .btn{ width:100%; justify-content:flex-start; }

  
/* Strat checklist polish */
.checklist{
  display:grid;
  gap:10px;
  max-width: 900px;
}

.checkRow{
  display:grid;
  grid-template-columns: 22px 1fr;
  gap:12px;
  align-items:start;
  padding:12px 14px;
  border-radius:14px;
  border:1px solid rgba(255,255,255,.10);
  background: rgba(255,255,255,.03);
  cursor:pointer;
  transition: transform .08s ease, border-color .15s ease, background .15s ease;
}

.checkRow:hover{
  border-color: rgba(255,255,255,.18);
  background: rgba(255,255,255,.05);
  transform: translateY(-1px);
}

.checkRow input[type="checkbox"]{
  width:18px;
  height:18px;
  margin-top:2px;
  accent-color: #35d07f;
}

.checkRow:has(input:checked){
  border-color: rgba(53,208,127,.45);
  background: rgba(53,208,127,.08);
}

.checkText{
  line-height: 1.55;
}
</style>
</head>

<body>
  <div class="wrap">
    <div class="topbar">
      <div class="brand">
        <div class="logo">
          {% if logo_exists %}
            <img src="{{ url_for('static', filename='logo.png') }}" alt="logo"/>
          {% else %}
            <div class="fallback">MC</div>
          {% endif %}
        </div>
        <div class="brandText">
          <h1>{{ title }}</h1>
          <div class="sub">Journal • Trades • Calendar</div>
        </div>
      </div>

      <div class="topRight">
        <div id="modePill" class="clockPill">⏳ Loading…</div>
        <div class="clockPill">🕒 <span id="etClock" class="clockTime">--:--:--</span> ET</div>

        <div class="nav">
            <a class="btn {% if active=='dashboard' %}active{% endif %}" href="/dashboard">📊 Calendar</a>
            <a class="btn {% if active=='journal' %}active{% endif %}" href="/journal">📝 Journal</a>
            <a class="btn {% if active=='trades' %}active{% endif %}" href="/trades">📅 Trades</a>
            <a class="btn {% if active=='calc' %}active{% endif %}" href="/calculator">🧮 Calc</a>

  <div class="menuMore">
    <button class="btn moreBtn" type="button" onclick="toggleMoreMenu()" aria-haspopup="true" aria-expanded="false">
      More ▾
    </button>

    <div id="moreMenu" class="moreMenu" role="menu">
      <a class=\"btn {% if active=='strategies' %}active{% endif %}\" href=\"/strategies\">📌 Strategies</a>
      <a class="btn {% if active=='strat' %}active{% endif %}" href="/strat">🧠 The Strat</a>
      <a class=\"btn {% if active=='books' %}active{% endif %}\" href=\"/books\">📚 Books</a>
      <a class="btn {% if active=='payouts' %}active{% endif %}" href="/payouts">💸 Payouts</a>
      <div class="hr"></div>
      <a class="btn" href="{{ url_for('chart') }}" target="_blank" rel="noopener">📈 Charts</a>
      <a class="btn" href="https://trade.vanquishtrader.com/" target="_blank" rel="noopener">🏦 Prop Firm</a>
    </div>
  </div>
</div>


        <button class="hamburger" type="button" aria-label="Open menu" onclick="openDrawer()">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
               stroke-linecap="round" stroke-linejoin="round">
            <line x1="4" y1="6" x2="20" y2="6"></line>
            <line x1="4" y1="12" x2="20" y2="12"></line>
            <line x1="4" y1="18" x2="20" y2="18"></line>
          </svg>
        </button>
      </div>
    </div>

    <div id="drawerOverlay" class="drawerOverlay" onclick="closeDrawer()"></div>
    <aside id="drawer" class="drawer" aria-label="Menu">
      <div class="drawerHead">
        <div class="drawerTitle">Menu</div>
        <button class="drawerClose" type="button" aria-label="Close menu" onclick="closeDrawer()">✕</button>
      </div>

      <div class="tiny">Quick navigation ⚡</div>

      <div class="drawerGrid">
        <a class="btn {% if active=='dashboard' %}primary{% endif %}" href="/dashboard">📊 Calendar</a>
        <a class="btn {% if active=='journal' %}primary{% endif %}" href="/journal">📝 Journal</a>
        <a class="btn {% if active=='trades' %}primary{% endif %}" href="/trades">📅 Trades</a>
        <a class="btn {% if active=='payouts' %}primary{% endif %}" href="/payouts">💸 Payouts</a>
        <a class="btn {% if active=='calc' %}primary{% endif %}" href="/calculator">🧮 Calculator</a>
        <a class=\"btn {% if active=='strategies' %}primary{% endif %}\" href=\"/strategies\">📌 Strategies</a>
        <a class="btn {% if active=='strat' %}primary{% endif %}" href="/strat">🧠 The Strat</a>
        <a class=\"btn {% if active=='books' %}primary{% endif %}\" href=\"/books\">📚 Books</a>
        <a class="btn" href="{{ url_for('chart') }}" target="_blank" rel="noopener">📈 Charts</a>
        <a class="btn" href="https://trade.vanquishtrader.com/" target="_blank" rel="noopener">🏦 Prop Firm</a>
      </div>

      <div class="hr"></div>
      <div class="tiny">Tip: Be honest. The market punishes delusion 😈🔥</div>
    </aside>

    {{ content|safe }}

    <div style="margin-top:14px" class="tiny">
      Tip: Be honest. The market punishes delusion 😈🔥
      <span class="kbd">Ctrl</span> + <span class="kbd">K</span> focuses Search.
    </div>
  </div>
    <script>
  function toggleMoreMenu(){
    const m = document.getElementById("moreMenu");
    if(!m) return;
    m.classList.toggle("open");
  }
  window.toggleMoreMenu = toggleMoreMenu;

  window.addEventListener("click", (e)=>{
    const menu = document.getElementById("moreMenu");
    if(!menu) return;

    const isButton = e.target.closest && e.target.closest(".moreBtn");
    const isInside = e.target.closest && e.target.closest("#moreMenu");
    if(!isButton && !isInside){
      menu.classList.remove("open");
    }
  });

  window.addEventListener("keydown", (e)=>{
    if(e.key === "Escape"){
      const menu = document.getElementById("moreMenu");
      if(menu) menu.classList.remove("open");
    }
  });
</script>
<script>
function toggleRowMenu(tradeId, ev) {
  ev.preventDefault();
  ev.stopPropagation();

  const menu = document.getElementById(`rowMenu-${tradeId}`);
  const btn  = ev.currentTarget;

  // Close any other open menus
  document.querySelectorAll('.rowMoreMenu.open').forEach(m => {
    if (m !== menu) m.classList.remove('open');
  });

  // Toggle this menu
  menu.classList.toggle('open');

  if (menu.classList.contains('open')) {
    // Make it immune to overflow clipping
    menu.style.position = 'fixed';
    menu.style.zIndex = '999999';

    // Need it visible to measure height
    menu.style.visibility = 'hidden';
    menu.style.display = 'block';

    const btnRect = btn.getBoundingClientRect();
    const menuRect = menu.getBoundingClientRect();

    // Default: open downward
    let top = btnRect.bottom + 6;

    // If it would go off-screen, open upward
    if (top + menuRect.height > window.innerHeight - 8) {
      top = btnRect.top - menuRect.height - 6;
    }

    // Keep inside viewport horizontally
    let left = btnRect.right - menuRect.width;
    left = Math.max(8, Math.min(left, window.innerWidth - menuRect.width - 8));

    menu.style.top = `${top}px`;
    menu.style.left = `${left}px`;

    menu.style.visibility = 'visible';
  }
}

// Close menus when clicking elsewhere
document.addEventListener('click', () => {
  document.querySelectorAll('.rowMoreMenu.open').forEach(m => m.classList.remove('open'));
});

// Close on ESC
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    document.querySelectorAll('.rowMoreMenu.open').forEach(m => m.classList.remove('open'));
  }
});
</script>




  <script>
    window.addEventListener('keydown', (e)=>{
      if((e.ctrlKey || e.metaKey) && e.key.toLowerCase()==='k'){
        const s = document.getElementById('search');
        if(s){ e.preventDefault(); s.focus(); }
      }
      if(e.key === "Escape"){ closeDrawer(); }
    });

    function confirmDelete(formId){
      if(confirm("Delete this? This can't be undone.")){
        document.getElementById(formId).submit();
      }
    }
    window.confirmDelete = confirmDelete;

    function confirmClear(formId){
      if(confirm("Clear ALL trade data? This wipes the trades table.")){
        document.getElementById(formId).submit();
      }
    }
    window.confirmClear = confirmClear;

    function openDrawer(){
      document.getElementById('drawer').classList.add('open');
      document.getElementById('drawerOverlay').classList.add('open');
      document.body.style.overflow = "hidden";
    }
    function closeDrawer(){
      document.getElementById('drawer').classList.remove('open');
      document.getElementById('drawerOverlay').classList.remove('open');
      document.body.style.overflow = "";
    }
    window.openDrawer = openDrawer;
    window.closeDrawer = closeDrawer;

    function updateETClock(){
      try{
        const now = new Date();
        const timeStr = new Intl.DateTimeFormat("en-US", {
          timeZone: "America/New_York",
          hour: "2-digit", minute:"2-digit", second:"2-digit",
          hour12:true
        }).format(now);

        const weekday = new Intl.DateTimeFormat("en-US", {
          timeZone: "America/New_York",
          weekday: "short"
        }).format(now);

        const hour24 = Number(new Intl.DateTimeFormat("en-US", {
          timeZone: "America/New_York",
          hour: "2-digit",
          hour12: false
        }).format(now));

        const clock = document.getElementById("etClock");
        const mode = document.getElementById("modePill");
        if(clock) clock.textContent = timeStr;

        if(mode){
          const isWeekend = (weekday === "Sat" || weekday === "Sun");
          if(isWeekend) mode.textContent = "🧠 Weekend Mode";
          else if(hour24 >= 16) mode.textContent = "📚 Study Mode";
          else mode.textContent = "📈 Trading Day";
        }
      }catch(e){
        console.error(e);
      }
    }

    setInterval(updateETClock, 1000);
    updateETClock();
  </script>
</body>
</html>
"""


def render_page(content_html: str, *, active: str, title: str = APP_TITLE):
    logo_exists = os.path.exists(os.path.join(app.static_folder or "static", "logo.png"))
    favicon_exists = os.path.exists(os.path.join(app.static_folder or "static", "favicon.ico"))
    return render_template_string(
        BASE_HTML,
        title=title,
        logo_exists=logo_exists,
        favicon_exists=favicon_exists,
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
@app.route("/")
def home():
    return redirect(url_for("dashboard"))


@app.route("/favicon.ico")
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
            Write what you saw → what you did → what you learned 🧱
          </div>

          {% if errors %}
            <div class="hr"></div>
            <div class="tiny" style="color:#ffb3b3">{% for e in errors %}• {{ e }}<br/>{% endfor %}</div>
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
              <textarea name="notes" placeholder="Be honest...">{{ values.get('notes','') }}</textarea>
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


@app.route("/journal")
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


@app.route("/new", methods=["GET", "POST"])
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


@app.route("/edit/<int:entry_id>", methods=["GET", "POST"])
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


@app.route("/delete/<int:entry_id>", methods=["POST"])
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
    with db() as conn:
        return list(conn.execute(
            """
            SELECT * FROM trades
            WHERE trade_date >= ? AND trade_date < ?
            ORDER BY trade_date ASC, id ASC
            """,
            (start_iso, end_iso),
        ).fetchall())


def month_range(year: int, month: int) -> Tuple[str, str]:
    first = date(year, month, 1)
    nxt = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    return first.isoformat(), nxt.isoformat()


# ============================================================
# Routes – Trades
# ============================================================
@app.route("/trades")
def trades_page():
    d = request.args.get("d", "")
    active_day = d or today_iso()

    prev_day = prev_trading_day_iso(active_day)
    next_day = next_trading_day_iso(active_day)

    q = request.args.get("q", "")

    # ✅ Convert sqlite3.Row -> dict so Jinja can use .get() and ['key']
    raw_trades = fetch_trades(d=d, q=q)
    trades = [dict(r) for r in raw_trades]

    stats = trade_day_stats(trades)  # likely dict
    cons = calc_consistency(trades)  # dict-like expected

    week_total = week_total_net(d or None)
    bal_in_day = last_balance_in_list(trades)
    overall_bal = latest_balance_overall(as_of=active_day)
    display_balance = bal_in_day if bal_in_day is not None else overall_bal

    content = render_template_string(
        """
        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <form method="get" action="/trades" class="row">
              <div style="flex:2 1 260px">
                <label for="search">🔎 Search Trades 🎯</label>
                <input id="search" name="q" value="{{ q }}" placeholder="SPX, CALL, PUT…" />
              </div>
              <div style="flex:1 1 160px">
                <label>📆 Date</label>
                <input type="date" name="d" value="{{ d }}" />
              </div>
              <div style="display:flex; gap:10px; flex-wrap:wrap">
                <a class="btn" href="/trades?d={{ prev_day }}&q={{ q }}">⬅️ Prev</a>
                <a class="btn" href="/trades?d={{ next_day }}&q={{ q }}">Next ➡️</a>
                <button class="btn" type="submit">🧲 Filter</button>
                <a class="btn" href="/trades">♻️ Reset</a>
                <a class="btn primary" href="/trades/new">➕ Manual Add</a>
                <a class="btn primary" href="/trades/paste">📋 Table Paste</a>
                <a class="btn primary" href="/trades/upload/statement">📄 Upload Statement</a>
              </div>
            </form>

            <div class="hr"></div>
            <div class="statRow">
              <div class="stat">
                <div class="k">💰 Day Net (filtered)</div>
                <div class="v">{{ money(stats['total'] if stats is mapping else stats.total) }}</div>
              </div>

              <div class="stat {% if week_total > 0 %}glow-green{% elif week_total < 0 %}glow-red{% endif %}">
                <div class="k">📅 Week Total</div>
                <div class="v">{{ money(week_total) }}</div>
              </div>

              <div class="stat">
                <div class="k">🏦 Balance</div>
                <div class="v">{{ money(display_balance) }}</div>
              </div>

              <div class="stat">
                <div class="k">✅ Wins</div>
                <div class="v">{{ stats['wins'] if stats is mapping else stats.wins }}</div>
              </div>

              <div class="stat">
                <div class="k">❌ Losses</div>
                <div class="v">{{ stats['losses'] if stats is mapping else stats.losses }}</div>
              </div>

              <div class="stat">
                <div class="k">🎯 Win Rate</div>
                <div class="v">
                  {{ '%.1f'|format((stats['win_rate'] if stats is mapping else stats.win_rate)) }}%
                </div>
              </div>

              <div class="stat">
                <div class="k">⚖️ W/L Ratio</div>
                <div class="v">
                  {{ '%.2f'|format((stats['wl_ratio'] if stats is mapping else stats.wl_ratio)) }}
                </div>
              </div>

              <div class="stat {{ cons.class }}">
                <div class="k">🎯 Consistency</div>
                <div class="v">
                  {% if cons.ratio is none %}
                    —
                  {% else %}
                    {{ '%.1f'|format(cons.ratio * 100) }}%
                  {% endif %}
                </div>
                <div class="tiny">
                  Max: {{ money(cons.biggest) }} / {{ money(cons.denom) }}
                </div>
              </div>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <form id="clear-trades" method="post" action="/trades/clear" style="display:inline"></form>
              <button class="btn danger" type="button" onclick="confirmClear('clear-trades')">🧼 Clear</button>
              <a class="btn" href="/dashboard">📊 Calendar</a>
              <a class="btn" href="/calculator">🧮 Calculator</a>
            </div>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">🧠 Paste Format</div>
            <div class="tiny" style="margin-top:10px; line-height:1.5">
              Table paste = tab-delimited rows. Broker paste = "instrument | dt | side | qty | price | fee". ✅
            </div>
          </div></div>
        </div>

        <div class="card" style="margin-top:12px"><div class="toolbar">
          <div class="pill">🧾 Trades ({{ trades|length }})</div>
          <div class="hr"></div>

          <!-- Bulk actions: multi-select delete / copy -->
          <div class="row" style="align-items:center; flex-wrap:wrap; gap:10px; margin:10px 0 2px;">
            <label class="pill" style="cursor:pointer; gap:8px;">
              <input type="checkbox" id="selectAll" style="margin:0;" />
              Select all
            </label>
            <span class="meta" id="selectedCount">0 selected</span>
            <button class="btn danger" id="bulkDeleteBtn" disabled>🗑️ Delete selected</button>

            <span class="hr" style="flex:1; min-width:40px;"></span>

            <label class="meta" style="display:flex; align-items:center; gap:8px;">
              Copy to:
              <input type="date" id="bulkCopyDate" value="{{ d }}" style="max-width:180px;" />
            </label>
            <button class="btn" id="bulkCopyBtn" disabled>📋 Copy selected</button>
          </div>

          <div style="overflow:auto">
            <table>
              <thead>
                <tr>
                  <th style="width:42px"></th>
                  <th>📆 Date</th>
                  <th>⏱️ Time</th>
                  <th>🏷️</th>
                  <th>📌</th>
                  <th>❌ Strike</th>
                  <th>🧾 C</th>
                  <th>💳 Spend</th>
                  <th>💰 Entry</th>
                  <th>💰 Exit</th>
                  <th>🛑20% Risk</th>
                  <th>💵 Net</th>
                  <th>📊%</th>
                  <th>🏦 Bal</th>
                  <th style="width:90px; text-align:right;">Actions</th>

                </tr>
              </thead>
              <tbody>
                {% for t in trades %}
                <tr>
                  <td><input type="checkbox" class="tradeCheckbox" data-id="{{ t['id'] }}" aria-label="Select trade {{ t['id'] }}"></td>
                  <td>{{ t['trade_date'] }}</td>

                  <td>
                    {% set et = t.get('entry_time') %}
                    {% set xt = t.get('exit_time') %}
                    {% if et and xt %}
                      {{ et }} → {{ xt }}
                    {% elif et %}
                      {{ et }}
                    {% elif xt %}
                      {{ xt }}
                    {% else %}
                      —
                    {% endif %}
                  </td>

                  <td><b>{{ t['ticker'] }}</b></td>
                  <td>{{ t['opt_type'] }}</td>
                  <td>{{ '' if t['strike'] is none else t['strike'] }}</td>
                  <td>{{ '' if t['contracts'] is none else t['contracts'] }}</td>
                  <td>{{ money(t['total_spent']) }}</td>
                  <td>{{ money(t['entry_price']) }}</td>
                  <td>{{ money(t['exit_price']) }}</td>
                  <td><span class="cell-red">{{ money((t['total_spent'] or 0) * 0.20) }}</span></td>
                  <td>
  {% set n = t.get('net_pl') %}
  {% if n is none %}
    <span class="pl-zero">—</span>
  {% elif n > 0 %}
    <span class="pl-pos">{{ money(n) }}</span>
  {% elif n < 0 %}
    <span class="pl-neg">{{ money(n) }}</span>
  {% else %}
    <span class="pl-zero">{{ money(n) }}</span>
  {% endif %}
</td>
                  {% set rp = t.get('result_pct') %}
<td>
  {% if rp is none %}
    <span class="muted">–</span>
  {% elif rp < 10 %}
    <span class="cell-red">{{ pct(rp) }}</span>
  {% elif rp > 20 %}
    <span class="cell-green">{{ pct(rp) }}</span>
  {% elif rp > 15 %}
    <span class="cell-orange">{{ pct(rp) }}</span>
  {% else %}
    <span class="muted">{{ pct(rp) }}</span>
  {% endif %}
</td>

                  <td>{{ money(t['balance']) }}</td>
                  <td style="text-align:right; white-space:nowrap;">
                    <div class="rowActions" id="rowActions-{{ t['id'] }}">
                      <button type="button" class="rowMoreBtn" onclick="toggleRowMenu('{{ t['id'] }}', event)" aria-label="Trade actions">▾</button>
                      <div class="rowMoreMenu" id="rowMenu-{{ t['id'] }}">
                        <a class="rowMenuItem" href="/trades/edit/{{ t['id'] }}?d={{ d }}&q={{ q }}">✏️ Edit</a>
                        <form method="post" action="/trades/duplicate/{{ t['id'] }}?d={{ d }}&q={{ q }}" style="margin:0;">
  <button class="rowMenuItem" type="submit">📄 Duplicate</button>
</form>

                        <form id="del-t-{{ t['id'] }}" method="post"
                              action="/trades/delete/{{ t['id'] }}?d={{ d }}&q={{ q }}"
                              onsubmit="return confirm('Delete this trade?');"
                              style="margin:0;">
                          <button class="rowMenuItem danger" type="submit">🗑️ Delete</button>
                        </form>
                      </div>
                    </div>
                  </td>
                </tr>
              {% endfor %}

              {% if trades|length == 0 %}
                <tr><td colspan="12" class="meta">No trades yet. Click <b>📋 Paste</b> and feed the beast 😈</td></tr>
              {% endif %}
              </tbody>
            </table>
          </div>
        </div></div>

<style>
/* Net P/L coloring */
.pl-pos { color: rgb(var(--green)); font-weight: 900; }
.pl-neg { color: rgb(var(--red));   font-weight: 900; }
.pl-zero{ color: var(--muted);      font-weight: 900; }

/* cell emphasis */
.cell-red { color: #dc2626; font-weight: 700; }
.cell-orange { color: #f97316; font-weight: 700; }
.cell-green { color: #16a34a; font-weight: 700; }

/* optional: make it pop a bit more without coloring the whole row */
.cell-red, .cell-orange, .cell-green { white-space: nowrap; }

  .rowActions{ position:relative; display:inline-block; }
  .rowMoreBtn{
    background: transparent; border: 0; cursor: pointer;
    padding: 6px 10px; border-radius: 10px;
    color: inherit; font-weight: 800;
  }
  .rowMoreBtn:hover{ background: rgba(255,255,255,0.06); }
  .rowMoreMenu{
    position:absolute; right:0; top: calc(100% + 6px);
    min-width: 140px;
    max-width:140px;
    background: rgba(10,10,10,0.92);
    border: 1px solid rgba(255,255,255,0.14);
    border-radius: 14px;
    padding: 8px;
    box-shadow: 0 18px 40px rgba(0,0,0,0.45);
    display:none;
    z-index: 50;
    backdrop-filter: blur(10px);
  }
  .rowMoreMenu.open{ display:block; }
  .rowMenuItem{
    display:block;
    width: 100%;
    text-align:left;
    padding: 10px 10px;
    border-radius: 12px;
    text-decoration:none;
    background: transparent;
    border: 0;
    color: inherit;
    font-weight: 700;
    cursor:pointer;
  }
  .rowMenuItem:hover{ background: rgba(255,255,255,0.06); }
  .rowMenuItem.danger{ color: #ff6b6b; }

/* ===== FIX: Allow row action dropdowns to escape table/card ===== */

/* The card that wraps the trades table */
.tradesCard,
.tradesTableWrap,
.tableWrap,
.tableContainer {
  overflow: visible !important;
}

/* If you rely on horizontal scroll */
.tradesTableWrap {
  overflow-x: auto !important;
  overflow-y: visible !important;
}

/* Ensure table + cells do not clip */
table,
thead,
tbody,
tr,
td,
th {
  overflow: visible !important;
}

/* Anchor for dropdown */
.rowActions {
  position: relative;
  overflow: visible !important;
}

/* Dropdown menu itself */
.rowMoreMenu {
  position: absolute;
  right: 0;
  top: calc(100% + 6px);
  z-index: 999999;
}



</style>

<script>
  function closeAllRowMenus() {
  document.querySelectorAll('.rowMoreMenu.open').forEach(menu => closeRowMenu(menu));
}

function closeRowMenu(menu) {
  menu.classList.remove('open');
  menu.style.position = '';
  menu.style.left = '';
  menu.style.top = '';
  menu.style.visibility = '';
  const originId = menu.dataset.origin;
  if (originId) {
    const origin = document.getElementById(originId);
    if (origin) origin.appendChild(menu);
  }
}

function openRowMenu(tradeId, btn, menu) {
  // Remember where the menu lives so we can put it back
  const origin = btn.closest('.rowActions');
  if (origin && origin.id) menu.dataset.origin = origin.id;

  // Move to <body> so it isn't clipped by scroll/overflow containers
  document.body.appendChild(menu);
  menu.classList.add('open');

  // Position near the button (fixed)
  const rect = btn.getBoundingClientRect();

  // Measure after open so width is accurate
  menu.style.visibility = 'hidden';
  const w = menu.offsetWidth || 180;
  const h = menu.offsetHeight || 120;

  let left = rect.right - w;
  let top  = rect.bottom + 6;

  const pad = 8;
  left = Math.max(pad, Math.min(left, window.innerWidth - w - pad));
  top  = Math.max(pad, Math.min(top,  window.innerHeight - h - pad));

  menu.style.position = 'fixed';
  menu.style.left = left + 'px';
  menu.style.top = top + 'px';
  menu.style.visibility = '';
}

// Close on outside click / escape
document.addEventListener('click', function(e){
  if (e.target.closest('.rowMoreMenu') || e.target.closest('.rowMoreBtn')) return;
  closeAllRowMenus();
});
document.addEventListener('keydown', function(e){
  if (e.key === 'Escape') closeAllRowMenus();
});
window.addEventListener('resize', closeAllRowMenus);
window.addEventListener('scroll', closeAllRowMenus, true);

// -----------------------------
// Bulk select / delete / copy
// -----------------------------
const selCountEl = document.getElementById('selectedCount');
const bulkDeleteBtn = document.getElementById('bulkDeleteBtn');
const bulkCopyBtn = document.getElementById('bulkCopyBtn');
const bulkCopyDate = document.getElementById('bulkCopyDate');
const selectAll = document.getElementById('selectAll');

function getSelectedIds() {
  return Array.from(document.querySelectorAll('.tradeCheckbox:checked'))
    .map(cb => parseInt(cb.dataset.id || '', 10))
    .filter(n => Number.isFinite(n));
}

function refreshBulkUi() {
  const ids = getSelectedIds();
  if (selCountEl) selCountEl.textContent = ids.length + ' selected';
  if (bulkDeleteBtn) bulkDeleteBtn.disabled = ids.length === 0;
  if (bulkCopyBtn) bulkCopyBtn.disabled = ids.length === 0;

  if (selectAll) {
    const all = document.querySelectorAll('.tradeCheckbox');
    const checked = document.querySelectorAll('.tradeCheckbox:checked');
    const allCount = all.length;
    const checkedCount = checked.length;
    selectAll.checked = allCount > 0 && checkedCount === allCount;
    selectAll.indeterminate = checkedCount > 0 && checkedCount < allCount;
  }
}

if (selectAll) {
  selectAll.addEventListener('change', () => {
    const on = selectAll.checked;
    document.querySelectorAll('.tradeCheckbox').forEach(cb => { cb.checked = on; });
    refreshBulkUi();
  });
}
document.querySelectorAll('.tradeCheckbox').forEach(cb => cb.addEventListener('change', refreshBulkUi));
refreshBulkUi();

if (bulkDeleteBtn) {
  bulkDeleteBtn.addEventListener('click', async () => {
    const ids = getSelectedIds();
    if (!ids.length) return;
    if (!confirm(`Delete ${ids.length} selected trade(s)? This can't be undone.`)) return;
    const r = await fetch(`/trades/delete_many?d={{ d }}&q={{ q|urlencode }}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ids })
    });
    if (r.ok) {
      location.reload();
    } else {
      alert('Delete failed: ' + (await r.text()));
    }
  });
}

if (bulkCopyBtn) {
  bulkCopyBtn.addEventListener('click', async () => {
    const ids = getSelectedIds();
    if (!ids.length) return;
    const target_date = (bulkCopyDate && bulkCopyDate.value) ? bulkCopyDate.value : '{{ d }}';
    const r = await fetch(`/trades/copy_many?d={{ d }}&q={{ q|urlencode }}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ids, target_date })
    });
    if (r.ok) {
      // jump to the day you copied into
      window.location.href = `/trades?d=${encodeURIComponent(target_date)}`;
    } else {
      alert('Copy failed: ' + (await r.text()));
    }
  });
}
</script>
""",
        trades=trades,
        d=d,
        q=q,
        stats=stats,
        cons=cons,  # ✅ THIS was missing and caused your crash
        week_total=week_total,
        display_balance=display_balance,
        money=money,
        pct=pct,
        prev_day=prev_day,
        next_day=next_day,
    )

    return render_page(content, active="trades")


@app.route("/trades/duplicate/<int:trade_id>", methods=["POST"])
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


@app.route("/trades/delete/<int:trade_id>", methods=["POST"])
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


@app.post("/trades/delete_many")
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


@app.post("/trades/copy_many")
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


@app.route("/trades/edit/<int:trade_id>", methods=["GET", "POST"])
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


@app.route("/trades/clear", methods=["POST"])
def trades_clear():
    clear_trades()
    return redirect(url_for("trades_page"))


@app.route("/trades/paste", methods=["GET", "POST"])
def trades_paste():
    if request.method == "POST":
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
                <div class="tiny" style="color:#ffb3b3">
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


@app.route("/trades/new", methods=["GET", "POST"])
def trades_new_manual():
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


@app.route("/trades/paste/broker", methods=["GET", "POST"])
def trades_paste_broker():
    if request.method == "POST":
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
                <div class="tiny" style="color:#ffb3b3">
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
@app.route("/trades/upload/statement", methods=["GET", "POST"])
def trades_upload_pdf():
    if request.method == "POST":
        f = request.files.get("pdf")
        mode = (request.form.get("mode") or "broker").strip()  # broker | balance
        starting_balance = parse_float(request.form.get("starting_balance", "")) or 50000.0

        if not f or not f.filename:
            return render_page(_simple_msg("Please upload a file."), active="trades")

        filename = secure_filename(f.filename)
        _, ext = os.path.splitext(filename.lower())

        if ext not in {".pdf", ".html", ".htm"}:
            return render_page(_simple_msg("Please upload a .pdf or .html file."), active="trades")

        path = os.path.join(UPLOAD_DIR, filename)
        f.save(path)

        # ✅ HTML path (no OCR)
        if ext in (".html", ".htm"):
            paste_text, balance_val, warns = parse_statement_html_to_broker_paste(path)

            if mode == "broker":
                if not paste_text:
                    return render_page(
                        render_template_string(
                            """
                            <div class="card"><div class="toolbar">
                              <div class="pill">⛔ HTML parsed, but no trade rows found</div>
                              <div class="hr"></div>
                              <div class="tiny" style="color:#ffcf9c; line-height:1.6">
                                {% for m in warns %}• {{ m }}<br>{% endfor %}
                              </div>
                              <div class="hr"></div>
                              <a class="btn" href="/trades/upload/statement">Back</a>
                            </div></div>
                            """,
                            warns=warns or [],
                        ),
                        active="trades",
                    )

                inserted, errors = insert_trades_from_broker_paste(paste_text, starting_balance=starting_balance)
                msgs = (warns or []) + (errors or [])

                return render_page(
                    render_template_string(
                        """
                        <div class="card"><div class="toolbar">
                          <div class="pill">🧾 HTML → Trades ✅</div>
                          <div style="margin-top:10px">Inserted <b>{{ inserted }}</b> trade{{ '' if inserted==1 else 's' }}.</div>
                          {% if msgs %}
                            <div class="hr"></div>
                            <div class="tiny" style="color:#ffcf9c; line-height:1.6">
                              {% for m in msgs %}• {{ m }}<br>{% endfor %}
                            </div>
                          {% endif %}
                          <div class="hr"></div>
                          <a class="btn primary" href="/trades">Trades 📅</a>
                          <a class="btn" href="/trades/upload/statement">Upload Another</a>
                        </div></div>
                        """,
                        inserted=inserted,
                        msgs=msgs,
                    ),
                    active="trades",
                )

            # mode == "balance"
            if balance_val is None:
                return render_page(
                    render_template_string(
                        """
                        <div class="card"><div class="toolbar">
                          <div class="pill">⛔ Balance not found in HTML</div>
                          <div class="hr"></div>
                          <div class="tiny" style="color:#ffcf9c; line-height:1.6">
                            {% for m in warns %}• {{ m }}<br>{% endfor %}
                          </div>
                          <div class="hr"></div>
                          <a class="btn" href="/trades/upload/statement">Back</a>
                        </div></div>
                        """,
                        warns=warns or [],
                    ),
                    active="trades",
                )

            insert_balance_snapshot(today_iso(), balance_val, raw_line="STATEMENT HTML UPLOAD")
            return redirect(url_for("trades_page"))

        # --- PDF path (keep your OCR behavior for now) ---
        if mode == "broker":
            paste_text, ocr_warns = ocr_pdf_to_broker_paste(path)
            if not paste_text:
                stitched = []
                try:
                    pages = convert_from_path(path, dpi=250)
                    all_lines = []
                    for page_img in pages:
                        img = _prep_for_ocr(page_img)
                        txt = pytesseract.image_to_string(img, config="--oem 3 --psm 6")
                        all_lines.extend([normalize_ocr(ln) for ln in txt.splitlines() if normalize_ocr(ln)])
                    stitched = stitch_ocr_rows("\n".join(all_lines))
                except Exception as e:
                    ocr_warns = (ocr_warns or []) + [f"OCR debug error: {e}"]

                return render_page(
                    render_template_string(
                        """
                        <div class="card"><div class="toolbar">
                          <div class="pill">⛔ OCR rows not parseable</div>
                          <div class="hr"></div>
                          <div class="tiny" style="color:#ffcf9c; line-height:1.6">
                            {% for m in warns %}• {{ m }}<br>{% endfor %}
                          </div>
                          <div class="hr"></div>
                          <div class="tiny">Stitched rows (first 30):</div>
                          <pre style="white-space:pre-wrap; font-size:12px; color:var(--muted)">{{ dump }}</pre>
                          <div class="hr"></div>
                          <a class="btn" href="/trades/upload/statement">Back</a>
                        <a class="btn" href="/trades/upload/statement">Upload Another</a>

                        </div></div>
                        """,
                        warns=ocr_warns,
                        dump="\n".join(stitched[:30]),
                    ),
                    active="trades",
                )

            inserted, errors = insert_trades_from_broker_paste(paste_text, starting_balance=starting_balance)
            msgs = (ocr_warns or []) + (errors or [])
            return render_page(
                render_template_string(
                    """
                    <div class="card"><div class="toolbar">
                      <div class="pill">📄 PDF → OCR → Trades ✅</div>
                      <div style="margin-top:10px">Inserted <b>{{ inserted }}</b> trade{{ '' if inserted==1 else 's' }}.</div>
                      {% if msgs %}
                        <div class="hr"></div>
                        <div class="tiny" style="color:#ffcf9c; line-height:1.6">
                          {% for m in msgs %}• {{ m }}<br>{% endfor %}
                        </div>
                      {% endif %}
                      <div class="hr"></div>
                      <a class="btn primary" href="/trades">Trades 📅</a>
                     <a class="btn" href="/trades/upload/statement">Upload Another</a>
                    </div></div>
                    """,
                    inserted=inserted,
                    msgs=msgs,
                ),
                active="trades",
            )

        # mode == balance (PDF OCR)
        text, warns = ocr_pdf_to_text(path)
        bal = extract_statement_balance(text)
        if bal is None:
            return render_page(
                render_template_string(
                    """<div class="card"><div class="toolbar">
                       <div class="pill">⛔ Could not find ending balance</div>
                       <div class="hr"></div>
                       <div class="tiny">Dump (first 1200 chars):</div>
                       <pre style="white-space:pre-wrap; font-size:12px; color:var(--muted)">{{ dump }}</pre>
                       <div class="hr"></div>
                       <a class="btn" href="/trades/upload/statement">Back</a>
                       </div></div>""",
                    dump=(text or "")[:1200],
                ),
                active="trades",
            )

        insert_balance_snapshot(today_iso(), bal, raw_line="STATEMENT PDF UPLOAD")
        return redirect(url_for("trades_page"))

    # GET
    content = render_template_string(
        """
        <div class="card"><div class="toolbar">
          <div class="pill">📄 Upload Statement (PDF / HTML)</div>
          <div class="hr"></div>
          <form method="post" enctype="multipart/form-data">
            <div class="row">
              <div>
                <label>Mode</label>
                <select name="mode">
                  <option value="broker">🏦 Broker fills → trades</option>
                  <option value="balance">🏁 Statement → ending balance snapshot</option>
                </select>
              </div>
              <div>
                <label>🏁 Starting Balance (broker mode)</label>
                <input name="starting_balance" inputmode="decimal" value="50000" />
              </div>
            </div>

            <div style="margin-top:12px">
              <label>📎 File</label>
              <input type="file" name="pdf" accept="application/pdf,text/html" />
              <div class="tiny" style="margin-top:6px">Upload the Vanquish Account Statement HTML if you have it — it’s cleaner than OCR ✅</div>
            </div>

            <div class="hr"></div>
            <div class="rightActions">
              <button class="btn primary" type="submit">🚀 Process</button>
              <a class="btn" href="/trades">← Back</a>
            </div>
          </form>
        </div></div>
        """
    )
    return render_page(content, active="trades")


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
@app.route("/dashboard")
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

    content = render_template_string(
        """
        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap">
              <div>
                <div class="pill">📊 P/L Calendar</div>
                <div class="tiny" style="margin-top:8px">Tap a weekday to open that day’s trades 🧲</div>
              </div>
              <div class="rightActions">
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
        proj=proj,
        money=money,
        money_compact=money_compact,
    )

    return render_page(content, active="dashboard")


# ============================================================
# Calculator page ✅
# ============================================================
@app.route("/calculator", methods=["GET", "POST"])
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
              <div class="tiny" style="color:#ffb3b3">• {{ err }}</div>
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
            <div class="tiny" style="color:#ffb3b3">{% for e in errors %}• {{ e }}<br/>{% endfor %}</div>
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


@app.route("/strategies")
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


@app.route("/strategies/new", methods=["GET", "POST"])
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


@app.route("/strategies/edit/<int:sid>", methods=["GET", "POST"])
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


@app.route("/strategies/delete/<int:sid>", methods=["POST"])
def strategies_delete(sid: int):
    delete_strategy(sid)
    return redirect(url_for("strategies_page"))


# ============================================================
# Books ✅
# ============================================================
@app.route("/books")
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


@app.get("/strat")
def strat_page():
    """Dedicated reference page for The Strat trading framework."""
    content = r"""
    <div class="section-title">🧠 The Strat — Core Playbook</div>
    <div class="muted" style="margin-bottom:14px;">
      A quick-reference page for <b>candle types</b>, <b>combo patterns</b>, <b>universal truths</b>, and <b>stop loss structure</b>.
      Use it as a pre-trade checklist before you click buy/sell. ✅
    </div>

    <div class="grid grid-3" style="margin-bottom:16px;">
      <div class="card">
        <div class="h2">🕯️ Candle Types</div>
        <div class="muted">The 1-2-3 language</div>
        <ul style="margin:10px 0 0 18px; line-height:1.6;">
          <li><b>1</b> = inside bar (range contraction)</li>
          <li><b>2</b> = directional break (higher high or lower low)</li>
          <li><b>3</b> = outside bar (breaks both sides)</li>
        </ul>
      </div>

      <div class="card">
        <div class="h2">🔁 Core Combos</div>
        <div class="muted">Common setups</div>
        <ul style="margin:10px 0 0 18px; line-height:1.6;">
          <li><b>2-1-2</b> continuation after pause</li>
          <li><b>3-1-2</b> volatility → pause → break</li>
          <li><b>2-2</b> reversal (your main trigger)</li>
        </ul>
      </div>

      <div class="card">
        <div class="h2">🧭 Timeframe Continuity</div>
        <div class="muted">Context matters</div>
        <ul style="margin:10px 0 0 18px; line-height:1.6;">
          <li>Trade <b>with</b> higher timeframe intent</li>
          <li>Expect cleaner moves when HTF aligns</li>
          <li>Be selective when HTF disagrees</li>
        </ul>
      </div>
    </div>

    <div class="grid grid-2" style="margin-bottom:16px;">
      <div class="card">
        <div class="h2">🌎 Universal Truths</div>
        <div class="muted">Keep these on your screen</div>
        <ul style="margin:10px 0 0 18px; line-height:1.65;">
          <li><b>Location is king:</b> levels & liquidity drive decisions.</li>
          <li><b>Direction needs proof:</b> break + follow-through beats “hoping”.</li>
          <li><b>Range = risk:</b> mid-range trades are hardest to manage.</li>
          <li><b>Losses are part of the plan:</b> define risk before entry.</li>
          <li><b>Your edge is repetition:</b> same process, same sizing.</li>
        </ul>
      </div>

      <div class="card">
        <div class="h2">🛡️ Stop Loss Structure</div>
        <div class="muted">Simple, consistent, non-negotiable</div>

        <div style="margin-top:10px; line-height:1.6;">
          <div><b>Default rule:</b> stop goes beyond the level that invalidates the setup.</div>
          <div class="muted" style="margin-top:6px;">
            Examples: beyond the reversal candle extreme, beyond the key level (PDH/PDL), or beyond the HTF swing.
          </div>

          <div style="margin-top:12px;">
            <div><b>Options risk cap:</b> keep premium risk within your plan (e.g., 20–25%).</div>
            <div class="muted" style="margin-top:6px;">
              If your “real stop” requires more than your cap, reduce size or pass.
            </div>
          </div>
        </div>
      </div>
    </div>

    <div class="card" style="margin-bottom:16px;">
      <div class="h2">✅ Pre‑Trade Checklist</div>
      <div class="muted">Tick these before you enter. Saved locally in your browser.</div>

      <div class="checklist" style="margin-top:10px;">
        <label class="checkRow">
          <input type="checkbox" class="strat-check" data-key="level" />
          <div class="checkText"><b>Location:</b> at PDH/PDL, CDH/CDL, HTF swing, or VWAP zone</div>
        </label>

        <label class="checkRow">
          <input type="checkbox" class="strat-check" data-key="htf" />
          <div class="checkText"><b>HTF intent:</b> 45m/1h agrees (or I’m explicitly fading at a major level)</div>
        </label>

        <label class="checkRow">
          <input type="checkbox" class="strat-check" data-key="structure" />
          <div class="checkText"><b>Structure:</b> 30m defines the box / range / pivots clearly</div>
        </label>

        <label class="checkRow">
          <input type="checkbox" class="strat-check" data-key="trigger" />
          <div class="checkText"><b>Trigger:</b> 15m 2-2 reversal + 5m expansion confirmation</div>
        </label>

        <label class="checkRow">
          <input type="checkbox" class="strat-check" data-key="risk" />
          <div class="checkText"><b>Risk:</b> stop defined, position size fixed, premium cap respected</div>
        </label>

        <label class="checkRow">
          <input type="checkbox" class="strat-check" data-key="plan" />
          <div class="checkText"><b>Plan:</b> targets chosen (TP1/TP2), and “no re‑entry revenge” rule acknowledged</div>
        </label>
      </div>

      <div style="display:flex; gap:10px; margin-top:12px; flex-wrap:wrap;">
        <button class="btn" type="button" onclick="stratChecklistClear()">🧹 Clear</button>
        <button class="btn primary" type="button" onclick="window.location.href='/trades'">📒 Go to Trades</button>
      </div>
    </div>

    <div class="card">
      <div class="h2">🧩 Combo Quick Reference</div>
      <div class="muted">Use this like a decision tree.</div>

      <div style="overflow:auto; margin-top:10px;">
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
    </div>

    <script>
      (function initStratChecklist(){
        try{
          const key = "strat_checklist_v1";
          const saved = JSON.parse(localStorage.getItem(key) || "{}");
          document.querySelectorAll(".strat-check").forEach(cb=>{
            const k = cb.getAttribute("data-key");
            cb.checked = !!saved[k];
            cb.addEventListener("change", ()=>{
              const next = JSON.parse(localStorage.getItem(key) || "{}");
              next[k] = cb.checked;
              localStorage.setItem(key, JSON.stringify(next));
            });
          });
          window.stratChecklistClear = function(){
            localStorage.removeItem(key);
            document.querySelectorAll(".strat-check").forEach(cb=>cb.checked=false);
          }
        }catch(e){
          // ignore localStorage issues
          window.stratChecklistClear = function(){
            document.querySelectorAll(".strat-check").forEach(cb=>cb.checked=false);
          }
        }
      })();
    </script>
    """
    return render_page(content, active="strat", title="🧠 The Strat")

@app.route("/books/open/<path:name>")
def books_open(name: str):
    fn = safe_filename(name)
    path = os.path.join(BOOKS_DIR, fn)
    if not os.path.exists(path) or not fn.lower().endswith(".pdf"):
        abort(404)
    return send_file(path, as_attachment=False)


# ============================================================
# Links ✅
# ============================================================
@app.route("/links")
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


@app.route("/export.json")
def export_json():
    payload = export_all()
    fd, out_path = tempfile.mkstemp(prefix="mccain_export_", suffix=".json")
    os.close(fd)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return send_file(out_path, as_attachment=True, download_name="mccain_capital_export.json")


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


@app.route("/payouts", methods=["GET", "POST"])
def payouts_page():
    protect = DEFAULT_PROTECT_BUFFER
    biweekly_goal = 2000.0

    if request.method == "POST":
        protect = parse_float(request.form.get("protect_buffer", "")) or protect
        biweekly_goal = parse_float(request.form.get("biweekly_goal", "")) or biweekly_goal
    else:
        protect = parse_float(request.args.get("protect", "")) or protect
        biweekly_goal = parse_float(request.args.get("goal", "")) or biweekly_goal

    overall_balance = latest_balance_overall() or 0.0
    ps = payout_summary(overall_balance, protect)

    today = now_et().date()
    mtd = month_total_net(today.year, today.month)
    last30 = last_30d_total_net()

    daily20 = last_n_trading_day_totals(20)
    proj = projections_from_daily(daily20, overall_balance)

    can_take_biweekly_now = ps["safe_request"] >= biweekly_goal

    content = render_template_string(
        """
        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div class="pill">💸 Payouts</div>
            <div class="tiny" style="margin-top:10px; line-height:1.5">
              Safe payout = protects cushion above the fixed loss limit 🛡️
            </div>

            <div class="hr"></div>
            <form method="post" class="row">
              <div>
                <label>🛡️ Protect Buffer ($)</label>
                <input name="protect_buffer" inputmode="decimal" value="{{ protect }}" />
              </div>
              <div>
                <label>🎯 Bi-Weekly Goal ($)</label>
                <input name="biweekly_goal" inputmode="decimal" value="{{ biweekly_goal }}" />
              </div>
              <div style="display:flex; gap:10px; flex-wrap:wrap">
                <button class="btn primary" type="submit">🔄 Update</button>
                <a class="btn" href="/payouts">♻️ Reset</a>
              </div>
            </form>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">📌 Rule Snapshot (50K)</div>
            <div class="tiny" style="margin-top:10px; line-height:1.6">
              • Buffer reached at: <b>{{ money(ps.profit_buffer_level) }}</b><br>
              • Fixed loss limit after buffer: <b>{{ money(ps.fixed_loss_limit) }}</b><br>
              • Safe floor (loss limit + cushion): <b>{{ money(ps.safe_floor) }}</b><br>
              <div class="hr"></div>
              {% if ps.buffer_reached %}
                ✅ Buffer reached — payouts can be calculated.
              {% else %}
                ⛔ Buffer NOT reached — eligibility = $0.00 until you pass <b>{{ money(ps.profit_buffer_level) }}</b>.
              {% endif %}
            </div>
          </div></div>
        </div>

        <div class="card" style="margin-top:12px"><div class="toolbar">
          <div class="pill">📊 Current Totals</div>
          <div class="hr"></div>

          <div class="statRow">
            <div class="stat"><div class="k">🏦 Balance</div><div class="v">{{ money(ps.balance) }}</div></div>
            <div class="stat"><div class="k">🗓️ MTD Net</div><div class="v">{{ money(mtd) }}</div></div>
            <div class="stat"><div class="k">📆 Last 30 Days</div><div class="v">{{ money(last30) }}</div></div>

            <div class="stat {% if ps.safe_request > 0 %}glow-green{% endif %}">
              <div class="k">🛡️ Safe Withdraw (now)</div>
              <div class="v">{{ money(ps.safe_request) }}</div>
            </div>

            <div class="stat {% if ps.max_request > 0 %}glow-green{% endif %}">
              <div class="k">⚠️ Max Withdraw (no cushion)</div>
              <div class="v">{{ money(ps.max_request) }}</div>
            </div>

            <div class="stat {% if can_take_biweekly_now %}glow-green{% else %}glow-red{% endif %}">
              <div class="k">🎯 ${{ '%.0f'|format(biweekly_goal) }} Bi-Weekly?</div>
              <div class="v">{% if can_take_biweekly_now %}✅ Yes{% else %}⛔ Not yet{% endif %}</div>
              <div class="tiny">Needs Safe ≥ {{ money(biweekly_goal) }}</div>
            </div>
          </div>

          <div class="hr"></div>
          <div class="pill">📈 Projections</div>
          <div class="hr"></div>
          <div class="statRow">
            <div class="stat"><div class="k">📊 Daily Avg (recent)</div><div class="v">{{ money(proj.avg) }}</div></div>
            <div class="stat"><div class="k">5D Est Bal</div><div class="v">{{ money(proj.p5.est_balance) }}</div></div>
            <div class="stat"><div class="k">10D Est Bal</div><div class="v">{{ money(proj.p10.est_balance) }}</div></div>
            <div class="stat"><div class="k">20D Est Bal</div><div class="v">{{ money(proj.p20.est_balance) }}</div></div>
          </div>

          <div class="hr"></div>
          <div class="tiny">
            Don’t pull it too tight. One red day can nuke the account 😈
          </div>
        </div></div>
        """,
        ps=ps,
        protect=protect,
        biweekly_goal=biweekly_goal,
        mtd=mtd,
        last30=last30,
        proj=proj,
        can_take_biweekly_now=can_take_biweekly_now,
        money=money,
    )
    return render_page(content, active="payouts")


# ============================================================
# Boot
# ============================================================
init_db()


@app.route("/chart")
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
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "5001"))
    debug = os.environ.get("DEBUG", "0") == "1"
    app.run(host=host, port=port, debug=debug)
