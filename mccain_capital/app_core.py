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
import shutil
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
from werkzeug.exceptions import RequestEntityTooLarge
from zoneinfo import ZoneInfo
from mccain_capital.services.ui import get_system_status
from mccain_capital.services.viewmodels import dashboard_data_trust

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
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "250"))
APP_USERNAME = os.environ.get("APP_USERNAME", "owner")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")
APP_PASSWORD_HASH = os.environ.get("APP_PASSWORD_HASH", "")

_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_STATIC_DIR = os.path.join(_ROOT_DIR, "static")
_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

app = Flask(
    __name__, static_folder=_STATIC_DIR, static_url_path="/static", template_folder=_TEMPLATE_DIR
)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_MB * 1024 * 1024


@app.errorhandler(RequestEntityTooLarge)
def handle_request_entity_too_large(_e):
    return (
        render_page(
            _simple_msg(
                f"Upload too large. Max allowed is {MAX_UPLOAD_MB}MB. "
                "Use Backup Center restore or increase MAX_UPLOAD_MB."
            ),
            active="dashboard",
        ),
        413,
    )


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
    from mccain_capital.migrations import run_migrations

    run_migrations(DB_PATH)


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
    from mccain_capital import runtime as app_runtime

    return app_runtime.default_starting_balance()


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


def _load_ocr_deps() -> Tuple[
    Optional[Callable[..., Any]],
    Optional[Any],
    Optional[Any],
    Optional[Any],
    Optional[Any],
    Optional[str],
]:
    try:
        from pdf2image import convert_from_path as _convert_from_path
        import pytesseract as _pytesseract
        from PIL import Image as _Image, ImageEnhance as _ImageEnhance, ImageOps as _ImageOps

        return _convert_from_path, _pytesseract, _Image, _ImageEnhance, _ImageOps, None
    except Exception as e:
        return (
            None,
            None,
            None,
            None,
            None,
            f"OCR dependencies missing/incompatible. Install pdf2image + pytesseract + Pillow. Error: {e}",
        )


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
        return (
            "",
            None,
            [f"pandas is required for HTML parsing (pip install pandas lxml). Error: {e}"],
        )

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
                if k in (
                    "balance",
                    "ending balance",
                    "account value",
                    "net liquidating value",
                    "net liq",
                ):
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
    re.compile(
        r"\bNet\s+Liquidating\s+Value\b[^0-9$-]*\$?\s*([-–—]?\s*[\d,]+\.\d{2})", re.IGNORECASE
    ),
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
                return {
                    "desc": desc,
                    "dt": dt,
                    "side": side,
                    "qty": qty,
                    "price": float(price),
                    "fee": float(fee),
                }

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
            return {
                "desc": desc,
                "dt": dt,
                "side": side,
                "qty": qty,
                "price": float(price),
                "fee": float(fee),
            }

    return None


def insert_trades_from_broker_paste(
    text: str, starting_balance: float = 50000.0
) -> Tuple[int, List[str]]:
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
                    "result_pct": (
                        (net_pl / (entry_price * 100.0 * take) * 100.0) if entry_price > 0 else None
                    ),
                    "raw_line": f["raw_line"],
                }
            )

            if lot["qty"] <= 0:
                open_lots[key].pop(0)

        if remaining > 0:
            errors.append(
                f"Line {f['line_no']}: SELL qty exceeds open BUY qty for {key} (extra {remaining})"
            )

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
                    None,
                    None,
                    None,
                    None,
                    None,
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
        warnings.append(
            f"Note: {open_count} contract(s) remain OPEN (unmatched BUY). That’s normal mid-position."
        )

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
        where.append(
            "(notes LIKE ? OR market LIKE ? OR setup LIKE ? OR grade LIKE ? OR mood LIKE ?)"
        )
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
                errors.append(
                    f"Line {i}: too few columns (got {len(cols)}). Use tab-delimited paste."
                )
                continue

            trade_date = parse_date_any(cols[0])
            if not trade_date:
                errors.append(
                    f"Line {i}: bad date '{cols[0]}' (try 1/29 or 01/29/2026 or 2026-01-29)."
                )
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
        row = conn.execute(
            "SELECT 1 FROM trade_reviews WHERE trade_id = ? LIMIT 1", (trade_id,)
        ).fetchone()
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
    css_path = os.path.join(app.static_folder or "static", "css", "app.css")
    logo_exists = os.path.exists(logo_path)
    favicon_exists = os.path.exists(favicon_path)
    # Cache-bust static branding assets so icon/logo updates show immediately after deploy.
    try:
        mtimes = [
            os.path.getmtime(p) for p in (logo_path, favicon_path, css_path) if os.path.exists(p)
        ]
        static_v = str(int(max(mtimes))) if mtimes else BUILD_MARKER
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
        system_status=get_system_status(),
        content=content_html,
        active=active,
    )


def _simple_msg(msg: str) -> str:
    from mccain_capital.services.ui import simple_msg

    return simple_msg(msg)


# ============================================================
# Routes – Home + favicon
# ============================================================
def setup_page():
    from mccain_capital.services import auth as auth_svc

    return auth_svc.setup_page()


def login_page():
    from mccain_capital.services import auth as auth_svc

    return auth_svc.login_page()


def logout_page():
    from mccain_capital.services import auth as auth_svc

    return auth_svc.logout_page()


def healthz():
    return jsonify(
        {
            "status": "ok",
            "app": "mccain-capital",
            "build": BUILD_MARKER,
            "ts": now_iso(),
            "safe_mode": bool(app.config.get("SAFE_MODE")),
        }
    )


def home():
    return redirect(url_for("dashboard"))


def favicon():
    return send_file(os.path.join(app.static_folder or "static", "favicon.ico"))


# ============================================================
# Routes – Journal
# ============================================================
def _entry_form(
    mode: str,
    values: Dict[str, Any],
    entry_id: Optional[int] = None,
    errors: Optional[List[str]] = None,
) -> str:
    from mccain_capital.services import journal as journal_svc

    return journal_svc._entry_form(mode, values, entry_id=entry_id, errors=errors)


def journal_home():
    from mccain_capital.services import journal as journal_svc

    return journal_svc.journal_home()


def new_entry():
    from mccain_capital.services import journal as journal_svc

    return journal_svc.new_entry()


def edit_entry(entry_id: int):
    from mccain_capital.services import journal as journal_svc

    return journal_svc.edit_entry(entry_id)


def delete_entry_route(entry_id: int):
    from mccain_capital.services import journal as journal_svc

    return journal_svc.delete_entry_route(entry_id)


def latest_trade_day() -> Optional[date]:
    from mccain_capital.repositories import trades as trades_repo

    return trades_repo.latest_trade_day()


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
    from mccain_capital import runtime as app_runtime

    new_balance = (app_runtime.latest_balance_overall() or 50000.0) + net_pl

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
        return redirect(
            url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", ""))
        )

    placeholders = ",".join(["?"] * len(ids))
    with db() as conn:
        cur = conn.execute(f"DELETE FROM trades WHERE id IN ({placeholders})", ids)
        deleted = cur.rowcount if cur.rowcount is not None else 0

    if request.is_json:
        return jsonify({"ok": True, "deleted": int(deleted)})
    flash(f"Deleted {deleted} trade(s).", "success")
    return redirect(
        url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", ""))
    )


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
        return redirect(
            url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", ""))
        )

    # validate date
    try:
        datetime.strptime(str(target_date), "%Y-%m-%d")
    except Exception:
        if request.is_json:
            return jsonify({"ok": False, "error": "Invalid target_date. Use YYYY-MM-DD."}), 400
        flash("Invalid target date (use YYYY-MM-DD).", "danger")
        return redirect(
            url_for("trades_page", d=request.args.get("d", ""), q=request.args.get("q", ""))
        )

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
    from mccain_capital.services import trades as trades_svc

    return trades_svc.trades_edit(trade_id)


def trades_review(trade_id: int):
    from mccain_capital.services import trades as trades_svc

    return trades_svc.trades_review(trade_id)


def trades_risk_controls():
    from mccain_capital.services import trades as trades_svc

    return trades_svc.trades_risk_controls()


def analytics_page():
    from mccain_capital.services import analytics as analytics_svc

    return analytics_svc.analytics_page()


def recompute_balances(starting_balance: float = 50000.0) -> None:
    from mccain_capital.repositories import trades as trades_repo

    trades_repo.recompute_balances(starting_balance=starting_balance)


def calc_consistency(trades):
    from mccain_capital.repositories import trades as trades_repo

    return trades_repo.calc_consistency(trades)


def trades_clear():
    from mccain_capital.services import trades as trades_svc

    return trades_svc.trades_clear()


def trades_paste():
    from mccain_capital.services import trades as trades_svc

    return trades_svc.trades_paste()


def trades_new_manual():
    from mccain_capital.services import trades as trades_svc

    return trades_svc.trades_new_manual()


def trades_paste_broker():
    from mccain_capital.services import trades as trades_svc

    return trades_svc.trades_paste_broker()


# ============================================================
# PDF Upload (FIXED: broken indentation + double if)
# ============================================================
def trades_upload_pdf():
    """Compatibility delegator: runtime implementation lives in services.trades."""
    from mccain_capital.services import trades as svc

    return svc.trades_upload_pdf()


def parse_vanquish_statement_table_to_broker_paste(text: str) -> Tuple[str, List[str]]:
    from mccain_capital.services import trades_importing as importing

    return importing.parse_vanquish_statement_table_to_broker_paste(text)


# ============================================================
# Dashboard – calendar + projections ✅
# ============================================================
def dashboard():
    from mccain_capital.services import core as core_svc

    return core_svc.dashboard()


def dashboard_recompute_balances():
    from mccain_capital.services import core as core_svc

    return core_svc.dashboard_recompute_balances()


# ============================================================
# Calculator page ✅
# ============================================================
def calculator():
    from mccain_capital.services import core as core_svc

    return core_svc.calculator()


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
    from mccain_capital.services import strategies as svc

    return svc._strategy_form(title, t, body, errors)


def strategies_page():
    from mccain_capital.services import strategies as svc

    return svc.strategies_page()


def strategies_new():
    from mccain_capital.services import strategies as svc

    return svc.strategies_new()


def strategies_edit(sid: int):
    from mccain_capital.services import strategies as svc

    return svc.strategies_edit(sid)


def strategies_delete(sid: int):
    from mccain_capital.services import strategies as svc

    return svc.strategies_delete(sid)


# ============================================================
# Books ✅
# ============================================================
def books_page():
    from mccain_capital.services import books as svc

    return svc.books_page()


def strat_page():
    """Compatibility delegator: runtime implementation lives in services.strat."""
    from mccain_capital.services import strat as svc

    return svc.strat_page()


def books_open(name: str):
    from mccain_capital.services import books as svc

    return svc.books_open(name)


# ============================================================
# Links ✅
# ============================================================
def links_page():
    from mccain_capital.services import core as svc

    return svc.links_page()


# ============================================================
# Export
# ============================================================
def export_all() -> Dict[str, Any]:
    with db() as conn:
        j = conn.execute(
            "SELECT * FROM entries ORDER BY entry_date DESC, updated_at DESC"
        ).fetchall()
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
    from mccain_capital.services import core as svc

    return svc.backup_data()


def restore_data():
    from mccain_capital.services import core as svc

    return svc.restore_data()


# ============================================================
# Payouts (kept simple; your existing numbers)
# ============================================================
PAYOUT_ACCT_SIZE = float(os.environ.get("PAYOUT_ACCT_SIZE", "50000"))
PROFIT_BUFFER_LEVEL_50K = 52875.0
FIXED_LOSS_LIMIT_50K = 50375.0
DEFAULT_PROTECT_BUFFER = float(os.environ.get("PAYOUT_PROTECT_BUFFER", "1000"))


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
