"""Trade import and OCR parsing utilities.

This module owns statement parsing/OCR and trade import transformations without
depending on the legacy ``app_core`` module.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from mccain_capital.runtime import (
    db,
    looks_like_header,
    normalize_opt_type,
    now_iso,
    parse_date_any,
    parse_float,
    parse_int,
    split_row,
)

DEFAULT_FEE_PER_CONTRACT = 0.70

# OCR helpers
PLACEHOLDERS = {"-", "—", "–", "_", "—-", "—_"}


def normalize_ocr(s: str) -> str:
    return (s or "").replace("\u202f", " ").replace("\u00a0", " ").strip()


def clean_ocr_trade_row(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("—", "-").replace("–", "-").replace("_", "-").replace("|", " ")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\b(PUT|CALL)\s*[=+]\s+", r"\1 ", s, flags=re.IGNORECASE)
    return s


def load_ocr_deps() -> Tuple[
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


def prep_for_ocr(img):
    _, _, _, ImageEnhance, ImageOps, _ = load_ocr_deps()
    if ImageEnhance is None or ImageOps is None:
        return img
    img = img.convert("L")
    img = ImageOps.autocontrast(img)
    img = ImageEnhance.Sharpness(img).enhance(1.6)
    img = ImageEnhance.Contrast(img).enhance(1.4)
    return img


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


COL_ALIASES = {
    "Transaction Time": ["Transaction Time", "Time", "Date/Time", "Datetime", "TransactionTime"],
    "Direction": ["Direction", "Side", "Buy/Sell", "B/S", "Action"],
    "Instrument": ["Instrument", "Symbol", "Contract", "Description", "Instr"],
    "Size": ["Size", "Qty", "Quantity", "Contracts", "Contract(s)"],
    "Price": ["Price", "Fill Price", "Avg Price", "FillPrice", "Execution Price"],
    "Commission": ["Commission", "Comm", "Fees", "Fee", "Costs"],
    "Balance": ["Balance", "Account Value", "Net Liquidating Value", "Net Liq", "Ending Balance"],
}


BALANCE_RE_LIST = [
    re.compile(r"\bEnding\s+Balance\b[^0-9$-]*\$?\s*([-–—]?\s*[\d,]+\.\d{2})", re.IGNORECASE),
    re.compile(
        r"\bNet\s+Liquidating\s+Value\b[^0-9$-]*\$?\s*([-–—]?\s*[\d,]+\.\d{2})", re.IGNORECASE
    ),
    re.compile(r"\bAccount\s+Value\b[^0-9$-]*\$?\s*([-–—]?\s*[\d,]+\.\d{2})", re.IGNORECASE),
    re.compile(r"\bBalance\b[^0-9$-]*\$?\s*([-–—]?\s*[\d,]+\.\d{2})", re.IGNORECASE),
]


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


BROKER_OCR_RE = re.compile(
    r"^(?P<desc>[A-Z]{1,6}\s+[A-Z]{3}/\d{1,2}/\d{2}\s+\d+(?:\.\d+)?\s+(?:PUT|CALL))\s+"
    r"(?P<dt>\d{1,2}/\d{1,2}/\d{2},\s*\d{1,2}:\d{2}\s*(?:AM|PM))\s+"
    r"(?P<side>BUY|SELL)\s+"
    r"(?P<qty>\d+)\s+"
    r"(?P<price>\d+(?:\.\d+)?)"
    r"(?:.*?\b(?P<fee>\d+(?:\.\d+)?)\b)?\s*$",
    re.IGNORECASE,
)


def _pick_col(df, want: str) -> Optional[str]:
    for name in COL_ALIASES.get(want, [want]):
        if name in df.columns:
            return name
    return None


def _clean_money(tok: str) -> Optional[float]:
    if not tok or tok in PLACEHOLDERS:
        return None
    t = tok.replace(",", "")
    t = t.replace("—", "-").replace("–", "-")
    try:
        return float(t)
    except Exception:
        return None


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


def parse_vanquish_trade_line(line: str) -> Optional[Dict[str, Any]]:
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
    lines: List[str] = []
    for t in trades:
        dt = t.get("time", "")
        dt = re.sub(r"(\d{1,2}:\d{2})\s*(AM|PM)\b", r"\1 \2", dt, flags=re.IGNORECASE)
        fee = t.get("commission") or DEFAULT_FEE_PER_CONTRACT
        lines.append(f"{t['instrument']} | {dt} | {t['side']} | {t['size']} | {t['price']} | {fee}")
    return "\n".join(lines)


def parse_statement_html_to_broker_paste(html_path: str) -> Tuple[str, Optional[float], List[str]]:
    warnings: List[str] = []

    try:
        import pandas as pd
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

    balance_val: Optional[float] = None
    try:
        for tbl in tables[:4]:
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

    tx_tbl = None
    inst_c = time_c = side_c = qty_c = price_c = comm_c = bal_c = None
    for cand in tables:
        inst_c = _pick_col(cand, "Instrument")
        time_c = _pick_col(cand, "Transaction Time")
        side_c = _pick_col(cand, "Direction")
        qty_c = _pick_col(cand, "Size")
        price_c = _pick_col(cand, "Price")
        comm_c = _pick_col(cand, "Commission")
        bal_c = _pick_col(cand, "Balance")
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

            fee = None
            if comm_c and comm_c in tx_tbl.columns:
                fee = parse_float(str(r.get(comm_c, "")))
            if fee is None:
                fee = DEFAULT_FEE_PER_CONTRACT
            row_balance = (
                parse_float(str(r.get(bal_c, ""))) if bal_c and bal_c in tx_tbl.columns else None
            )

            if not instrument or side not in ("BUY", "SELL") or qty <= 0 or price is None:
                continue
            if row_balance is not None:
                lines.append(
                    f"{instrument} | {dt} | {side} | {qty} | {price} | {fee} | {row_balance}"
                )
            else:
                lines.append(f"{instrument} | {dt} | {side} | {qty} | {price} | {fee}")
        except Exception:
            continue

    if not lines:
        warnings.append("Found a transactions table but no usable transaction rows parsed.")

    return "\n".join(lines), balance_val, warnings


def ocr_pdf_to_broker_paste(pdf_path: str) -> Tuple[str, List[str]]:
    warnings: List[str] = []
    convert_from_path, pytesseract, _, _, _, dep_error = load_ocr_deps()
    if dep_error:
        return "", [dep_error]

    pages = convert_from_path(pdf_path, dpi=250)
    all_lines: List[str] = []
    for page_img in pages:
        img = prep_for_ocr(page_img)
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

    return vanquish_trades_to_broker_paste(parsed), warnings


def ocr_pdf_to_text(pdf_path: str) -> Tuple[str, List[str]]:
    warnings: List[str] = []
    convert_from_path, pytesseract, _, _, _, dep_error = load_ocr_deps()
    if dep_error:
        return "", [dep_error]
    pages = convert_from_path(pdf_path, dpi=250)
    all_text: List[str] = []
    for page_img in pages:
        img = prep_for_ocr(page_img)
        txt = pytesseract.image_to_string(img, config="--oem 3 --psm 6")
        all_text.append(txt)
    text = "\n".join(all_text).strip()
    if not text:
        warnings.append("OCR returned empty text.")
    return text, warnings


def extract_statement_balance(text: str) -> Optional[float]:
    t = (text or "").replace("\u202f", " ").replace("\u00a0", " ")
    for rx in BALANCE_RE_LIST:
        m = rx.search(t)
        if m:
            s = m.group(1).replace("—", "-").replace("–", "-").replace("$", "")
            s = re.sub(r"\s+", "", s)
            return parse_float(s)
    return None


def parse_broker_dt(s: str) -> Tuple[Optional[str], str]:
    s = (s or "").strip().replace("\u202f", " ").replace("\u00a0", " ")
    s = re.sub(r"(\d{1,2}:\d{2})\s*(AM|PM)\b", r"\1 \2", s, flags=re.IGNORECASE)
    try:
        dt = datetime.strptime(s, "%m/%d/%y, %I:%M %p")
        hour = dt.strftime("%I").lstrip("0") or "0"
        return dt.date().isoformat(), f"{hour}:{dt.strftime('%M %p')}"
    except Exception:
        return None, ""


def parse_contract_desc(desc: str) -> Dict[str, Any]:
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


def parse_broker_line_any(ln: str) -> Optional[Dict[str, Any]]:
    raw = (ln or "").replace("\u202f", " ").replace("\u00a0", " ").strip()
    if not raw:
        return None

    if "|" in raw:
        bits = [b.strip() for b in raw.split("|")]
        if len(bits) >= 5:
            desc = bits[0]
            dt = bits[1]
            side = bits[2].upper()
            qty = parse_int(bits[3]) or 0
            price = parse_float(bits[4])
            fee = parse_float(bits[5]) if len(bits) > 5 else DEFAULT_FEE_PER_CONTRACT
            fee = DEFAULT_FEE_PER_CONTRACT if fee is None else fee
            bal = parse_float(bits[6]) if len(bits) > 6 else None
            if desc and dt and side in ("BUY", "SELL") and qty > 0 and price is not None:
                return {
                    "desc": desc,
                    "dt": dt,
                    "side": side,
                    "qty": qty,
                    "price": float(price),
                    "fee": float(fee),
                    "balance": bal,
                }

    m = BROKER_OCR_RE.match(raw.upper())
    if m:
        fee = parse_float(m.group("fee") or "")
        fee = float(fee) if fee is not None and 0 <= fee <= 5 else DEFAULT_FEE_PER_CONTRACT
        return {
            "desc": m.group("desc"),
            "dt": m.group("dt"),
            "side": m.group("side").upper(),
            "qty": int(m.group("qty")),
            "price": float(m.group("price")),
            "fee": fee,
        }

    cols = split_row(raw)
    if len(cols) >= 6:
        desc = cols[0]
        dt = cols[2] if len(cols) > 2 else ""
        side = (cols[3] if len(cols) > 3 else "").strip().upper()
        qty = parse_int(cols[4] if len(cols) > 4 else "") or 0
        price = parse_float(cols[5] if len(cols) > 5 else "")

        fee = DEFAULT_FEE_PER_CONTRACT
        tail = cols[-6:] if len(cols) >= 6 else cols
        for token in reversed(tail):
            v = parse_float(token)
            if v is not None and 0 <= v <= 5:
                fee = float(v)
                break
        bal = None
        for token in reversed(cols):
            v = parse_float(token)
            if v is not None and abs(v) >= 1000:
                bal = float(v)
                break

        if desc and dt and side in ("BUY", "SELL") and qty > 0 and price is not None:
            return {
                "desc": desc,
                "dt": dt,
                "side": side,
                "qty": qty,
                "price": float(price),
                "fee": float(fee),
                "balance": bal,
            }

    return None


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
    net = float(trade.get("net_pl") or 0.0)
    total_spent = float(trade.get("total_spent") or 0.0)
    comm = float(trade.get("comm") or 0.0)
    contracts = int(trade.get("contracts") or 0)
    rp = trade.get("result_pct")
    rp = float(rp) if rp is not None else None
    score = 78
    tags: List[str] = []

    if net > 0:
        score += 8
    elif net < 0:
        score -= 10

    if rp is not None:
        if rp >= 20:
            score += 6
        elif rp <= -20:
            score -= 12

    fee_ratio = (comm / total_spent) if total_spent > 0 else 0.0
    if fee_ratio > 0.02:
        score -= 8
        tags.append("high-fee-ratio")
    elif fee_ratio > 0.01:
        score -= 4

    entry_dt = _parse_ampm_time(str(trade.get("entry_time") or ""))
    exit_dt = _parse_ampm_time(str(trade.get("exit_time") or ""))
    if entry_dt and exit_dt:
        hold_min = int((exit_dt - entry_dt).total_seconds() // 60)
        if hold_min > 120:
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


def insert_trades_from_broker_paste(
    text: str, ending_balance: Optional[float] = None
) -> Tuple[int, List[str]]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return 0, ["Nothing to import."]

    created = now_iso()
    errors: List[str] = []
    warnings: List[str] = []

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
                "balance": (
                    float(parsed["balance"]) if parsed.get("balance") is not None else None
                ),
                "raw_line": ln,
                "line_no": i,
            }
        )

    if not fills:
        return 0, (errors or ["No valid fills to import."])

    def side_rank(s: str) -> int:
        return 0 if s == "BUY" else 1

    fills_sorted = sorted(
        fills,
        key=lambda f: (
            f["dt_obj"] if f["dt_obj"] else datetime.max,
            side_rank(f["side"]),
            f["line_no"],
        ),
    )

    if fills_sorted and fills_sorted[0]["line_no"] != 1:
        warnings.append("Detected out-of-order fills; sorted by datetime before pairing. ✅")

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
                    "trade_date": lot["trade_date"],
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
                    "balance": f.get("balance"),
                    "raw_line": f["raw_line"],
                }
            )

            if lot["qty"] <= 0:
                open_lots[key].pop(0)

        if remaining > 0:
            errors.append(
                f"Line {f['line_no']}: SELL qty exceeds open BUY qty for {key} (extra {remaining})"
            )

    inserted = 0
    skipped_duplicates = 0

    def trade_identity(tr: Dict[str, Any]) -> tuple[Any, ...]:
        return (
            tr.get("trade_date"),
            tr.get("entry_time"),
            tr.get("exit_time"),
            tr.get("ticker"),
            tr.get("opt_type"),
            round(float(tr.get("strike") or 0.0), 4),
            round(float(tr.get("entry_price") or 0.0), 4),
            round(float(tr.get("exit_price") or 0.0), 4),
            int(tr.get("contracts") or 0),
            round(float(tr.get("comm") or 0.0), 4),
            round(float(tr.get("gross_pl") or 0.0), 4),
            round(float(tr.get("net_pl") or 0.0), 4),
            (tr.get("raw_line") or "").strip(),
        )

    def db_trade_identity(row: Any) -> tuple[Any, ...]:
        return (
            row["trade_date"],
            row["entry_time"],
            row["exit_time"],
            row["ticker"],
            row["opt_type"],
            round(float(row["strike"] or 0.0), 4),
            round(float(row["entry_price"] or 0.0), 4),
            round(float(row["exit_price"] or 0.0), 4),
            int(row["contracts"] or 0),
            round(float(row["comm"] or 0.0), 4),
            round(float(row["gross_pl"] or 0.0), 4),
            round(float(row["net_pl"] or 0.0), 4),
            (row["raw_line"] or "").strip(),
        )

    derived_start_balance: Optional[float] = None
    if ending_balance is not None and completed:
        total_net = sum(float(tr.get("net_pl") or 0.0) for tr in completed)
        derived_start_balance = float(ending_balance) - total_net

    balance = derived_start_balance
    if balance is None and completed:
        first_day = min(str(tr["trade_date"]) for tr in completed)
        with db() as conn:
            row = conn.execute(
                """
                SELECT balance
                FROM trades
                WHERE trade_date < ? AND balance IS NOT NULL
                ORDER BY trade_date DESC, id DESC
                LIMIT 1
                """,
                (first_day,),
            ).fetchone()
        balance = float(row["balance"]) if row and row["balance"] is not None else 50000.0

    with db() as conn:
        conn.execute("BEGIN")
        existing_rows = conn.execute(
            """
            SELECT trade_date, entry_time, exit_time, ticker, opt_type, strike,
                   entry_price, exit_price, contracts, comm, gross_pl, net_pl, raw_line
            FROM trades
            """
        ).fetchall()
        existing = {db_trade_identity(r) for r in existing_rows}
        for tr in completed:
            ident = trade_identity(tr)
            if ident in existing:
                skipped_duplicates += 1
                continue
            row_balance = tr.get("balance")
            if row_balance is None:
                balance = float(balance or 0.0) + float(tr["net_pl"] or 0.0)
                row_balance = balance
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
                    row_balance,
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
            existing.add(ident)
        conn.commit()

    open_count = sum(sum(lot["qty"] for lot in lots) for lots in open_lots.values() if lots)
    if open_count:
        warnings.append(
            f"Note: {open_count} contract(s) remain OPEN (unmatched BUY). That’s normal mid-position."
        )
    if skipped_duplicates:
        warnings.append(f"Skipped {skipped_duplicates} duplicate trade(s) already imported.")

    return inserted, (warnings + errors)


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
