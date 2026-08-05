"""Forward Pace projection planner and PDF export."""

from __future__ import annotations

import io
import json
import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable

from flask import jsonify, render_template, request, send_file

from mccain_capital.runtime import today_iso
from mccain_capital.services.ui import render_page

_STANDARD_DEDUCTION_2026 = {
    "single": 16100.0,
    "married_joint": 32200.0,
    "head_household": 24150.0,
}

_FEDERAL_BRACKETS_2026 = {
    "single": [
        (0, 0.10),
        (12400, 0.12),
        (50400, 0.22),
        (105700, 0.24),
        (201775, 0.32),
        (256225, 0.35),
        (640600, 0.37),
    ],
    "married_joint": [
        (0, 0.10),
        (24800, 0.12),
        (100800, 0.22),
        (211400, 0.24),
        (403550, 0.32),
        (512450, 0.35),
        (768700, 0.37),
    ],
    "head_household": [
        (0, 0.10),
        (17700, 0.12),
        (67450, 0.22),
        (105700, 0.24),
        (201775, 0.32),
        (256200, 0.35),
        (640600, 0.37),
    ],
}

# Planning-rate estimates by state. These are intentionally conservative effective-rate inputs,
# not a substitute for filing software or a CPA review.
_STATE_TAX_RATES = {
    "AL": 0.05,
    "AK": 0.0,
    "AZ": 0.025,
    "AR": 0.039,
    "CA": 0.093,
    "CO": 0.044,
    "CT": 0.055,
    "DC": 0.085,
    "DE": 0.066,
    "FL": 0.0,
    "GA": 0.0519,
    "HI": 0.0825,
    "IA": 0.038,
    "ID": 0.058,
    "IL": 0.0495,
    "IN": 0.03,
    "KS": 0.052,
    "KY": 0.04,
    "LA": 0.0425,
    "MA": 0.05,
    "MD": 0.0575,
    "ME": 0.0715,
    "MI": 0.0425,
    "MN": 0.0785,
    "MO": 0.048,
    "MS": 0.047,
    "MT": 0.059,
    "NC": 0.0425,
    "ND": 0.025,
    "NE": 0.055,
    "NH": 0.0,
    "NJ": 0.0637,
    "NM": 0.049,
    "NV": 0.0,
    "NY": 0.0645,
    "OH": 0.035,
    "OK": 0.0475,
    "OR": 0.0875,
    "PA": 0.0307,
    "RI": 0.0475,
    "SC": 0.064,
    "SD": 0.0,
    "TN": 0.0,
    "TX": 0.0,
    "UT": 0.0455,
    "VA": 0.0575,
    "VT": 0.066,
    "WA": 0.0,
    "WI": 0.053,
    "WV": 0.047,
    "WY": 0.0,
}

_STATES = [
    ("AL", "Alabama"),
    ("AK", "Alaska"),
    ("AZ", "Arizona"),
    ("AR", "Arkansas"),
    ("CA", "California"),
    ("CO", "Colorado"),
    ("CT", "Connecticut"),
    ("DC", "District of Columbia"),
    ("DE", "Delaware"),
    ("FL", "Florida"),
    ("GA", "Georgia"),
    ("HI", "Hawaii"),
    ("IA", "Iowa"),
    ("ID", "Idaho"),
    ("IL", "Illinois"),
    ("IN", "Indiana"),
    ("KS", "Kansas"),
    ("KY", "Kentucky"),
    ("LA", "Louisiana"),
    ("MA", "Massachusetts"),
    ("MD", "Maryland"),
    ("ME", "Maine"),
    ("MI", "Michigan"),
    ("MN", "Minnesota"),
    ("MO", "Missouri"),
    ("MS", "Mississippi"),
    ("MT", "Montana"),
    ("NC", "North Carolina"),
    ("ND", "North Dakota"),
    ("NE", "Nebraska"),
    ("NH", "New Hampshire"),
    ("NJ", "New Jersey"),
    ("NM", "New Mexico"),
    ("NV", "Nevada"),
    ("NY", "New York"),
    ("OH", "Ohio"),
    ("OK", "Oklahoma"),
    ("OR", "Oregon"),
    ("PA", "Pennsylvania"),
    ("RI", "Rhode Island"),
    ("SC", "South Carolina"),
    ("SD", "South Dakota"),
    ("TN", "Tennessee"),
    ("TX", "Texas"),
    ("UT", "Utah"),
    ("VA", "Virginia"),
    ("VT", "Vermont"),
    ("WA", "Washington"),
    ("WI", "Wisconsin"),
    ("WV", "West Virginia"),
    ("WY", "Wyoming"),
]


def forward_pace_page():
    content = render_template("forward_pace.html", today=today_iso(), states=_STATES)
    return render_page(content, active="forward-pace", title="McCain Capital · Forward Pace")


def api_projection():
    payload = request.get_json(silent=True) or {}
    return jsonify({"ok": True, "projection": build_projection(payload)})


def download_pdf():
    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    if "payload" in payload:
        payload = json.loads(payload.get("payload") or "{}")
    projection = build_projection(payload)
    pdf = _build_pdf(projection)
    return send_file(
        io.BytesIO(pdf),
        mimetype="application/pdf",
        as_attachment=True,
        download_name="forward-pace-projection.pdf",
    )


def build_projection(raw: Dict[str, Any]) -> Dict[str, Any]:
    base_balance = _money(raw.get("base_balance"), 50000)
    gross_payout = _money(raw.get("gross_payout"), 250)
    payouts_per_week = _bounded_int(raw.get("payouts_per_week"), 5, 1, 14)
    weeks = _bounded_int(raw.get("weeks"), 12, 1, 104)
    fixed_buffer = _money(raw.get("fixed_buffer"), 5000)
    buffer_rate = _bounded_float(raw.get("buffer_rate"), 10, 0, 90) / 100
    state = str(raw.get("state") or "GA").upper()
    filing_status = str(raw.get("filing_status") or "single")
    if filing_status not in _FEDERAL_BRACKETS_2026:
        filing_status = "single"
    start_date = _parse_date(raw.get("start_date"))

    weekly_gross = gross_payout * payouts_per_week
    annual_gross = weekly_gross * 52
    federal_annual = _federal_tax(annual_gross, filing_status)
    state_rate = _STATE_TAX_RATES.get(state, 0)
    state_annual = max(0.0, annual_gross * state_rate)
    weekly_federal = federal_annual / 52
    weekly_state = state_annual / 52
    weekly_tax = weekly_federal + weekly_state
    weekly_buffer = weekly_gross * buffer_rate
    weekly_net = max(0.0, weekly_gross - weekly_tax - weekly_buffer)

    schedule = []
    balance = base_balance
    net_total = 0.0
    gross_total = 0.0
    tax_total = 0.0
    buffer_total = fixed_buffer
    for idx in range(weeks):
        week_start = start_date + timedelta(days=idx * 7)
        week_end = week_start + timedelta(days=6)
        balance += weekly_net
        net_total += weekly_net
        gross_total += weekly_gross
        tax_total += weekly_tax
        buffer_total += weekly_buffer
        schedule.append(
            {
                "week": idx + 1,
                "start": week_start.isoformat(),
                "end": week_end.isoformat(),
                "gross": round(weekly_gross, 2),
                "federal_tax": round(weekly_federal, 2),
                "state_tax": round(weekly_state, 2),
                "buffer": round(weekly_buffer, 2),
                "net": round(weekly_net, 2),
                "projected_balance": round(balance, 2),
            }
        )

    return {
        "inputs": {
            "base_balance": round(base_balance, 2),
            "gross_payout": round(gross_payout, 2),
            "payouts_per_week": payouts_per_week,
            "weeks": weeks,
            "fixed_buffer": round(fixed_buffer, 2),
            "buffer_rate": round(buffer_rate * 100, 2),
            "state": state,
            "filing_status": filing_status,
            "start_date": start_date.isoformat(),
        },
        "tax": {
            "annual_gross": round(annual_gross, 2),
            "taxable_federal_income": round(
                max(0.0, annual_gross - _STANDARD_DEDUCTION_2026[filing_status]), 2
            ),
            "federal_annual": round(federal_annual, 2),
            "state_annual": round(state_annual, 2),
            "state_rate": round(state_rate * 100, 3),
            "effective_tax_rate": (
                round((federal_annual + state_annual) / annual_gross * 100, 2)
                if annual_gross
                else 0
            ),
        },
        "totals": {
            "gross": round(gross_total, 2),
            "tax": round(tax_total, 2),
            "buffer": round(buffer_total, 2),
            "net": round(net_total, 2),
            "projected_balance": round(balance, 2),
        },
        "weekly": {
            "gross": round(weekly_gross, 2),
            "federal_tax": round(weekly_federal, 2),
            "state_tax": round(weekly_state, 2),
            "buffer": round(weekly_buffer, 2),
            "net": round(weekly_net, 2),
        },
        "schedule": schedule,
        "notes": [
            "Planning estimate only; verify with tax software or a CPA before filing.",
            "State tax uses a conservative planning rate so projections work across every state.",
        ],
    }


def _federal_tax(annual_gross: float, filing_status: str) -> float:
    taxable = max(0.0, annual_gross - _STANDARD_DEDUCTION_2026[filing_status])
    brackets = _FEDERAL_BRACKETS_2026[filing_status]
    tax = 0.0
    for idx, (floor, rate) in enumerate(brackets):
        next_floor = brackets[idx + 1][0] if idx + 1 < len(brackets) else math.inf
        if taxable <= floor:
            break
        tax += (min(taxable, next_floor) - floor) * rate
    return max(0.0, tax)


def _build_pdf(projection: Dict[str, Any]) -> bytes:
    lines = [
        "McCain Capital - Forward Pace Projection",
        f"State: {projection['inputs']['state']}    Filing: {projection['inputs']['filing_status']}",
        f"Start: {projection['inputs']['start_date']}    Weeks: {projection['inputs']['weeks']}",
        "",
        f"Base Balance: {_fmt_money(projection['inputs']['base_balance'])}",
        f"Weekly Gross: {_fmt_money(projection['weekly']['gross'])}",
        f"Weekly Tax: {_fmt_money(projection['weekly']['federal_tax'] + projection['weekly']['state_tax'])}",
        f"Weekly Buffer: {_fmt_money(projection['weekly']['buffer'])}",
        f"Weekly Net Pace: {_fmt_money(projection['weekly']['net'])}",
        f"Projected Balance: {_fmt_money(projection['totals']['projected_balance'])}",
        "",
        "Weekly Schedule",
    ]
    for row in projection["schedule"]:
        lines.append(
            f"W{row['week']:02d} {row['start']} - Gross {_fmt_money(row['gross'])} | "
            f"Tax {_fmt_money(row['federal_tax'] + row['state_tax'])} | "
            f"Buffer {_fmt_money(row['buffer'])} | Net {_fmt_money(row['net'])} | "
            f"Balance {_fmt_money(row['projected_balance'])}"
        )
    lines.extend(["", *projection["notes"]])
    return _simple_pdf(lines)


def _simple_pdf(lines: Iterable[str]) -> bytes:
    escaped_lines = [_pdf_escape(line) for line in lines]
    chunks = [escaped_lines[idx : idx + 42] for idx in range(0, len(escaped_lines), 42)] or [[]]
    page_refs = [f"{4 + idx * 2} 0 R" for idx in range(len(chunks))]
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        f"<< /Type /Pages /Kids [{' '.join(page_refs)}] /Count {len(chunks)} >>".encode(),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    for idx, chunk in enumerate(chunks):
        content_ref = 5 + idx * 2
        text_ops = ["BT", "/F1 12 Tf", "50 760 Td", "16 TL"]
        for line in chunk:
            text_ops.append(f"({line}) Tj")
            text_ops.append("T*")
        text_ops.append("ET")
        stream = "\n".join(text_ops).encode("latin-1", "replace")
        objects.append(
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
                f"/Resources << /Font << /F1 3 0 R >> >> /Contents {content_ref} 0 R >>"
            ).encode()
        )
        objects.append(
            b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream"
        )
    out = io.BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets = [0]
    for idx, obj in enumerate(objects, start=1):
        offsets.append(out.tell())
        out.write(f"{idx} 0 obj\n".encode())
        out.write(obj)
        out.write(b"\nendobj\n")
    xref = out.tell()
    out.write(f"xref\n0 {len(objects) + 1}\n".encode())
    out.write(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        out.write(f"{offset:010d} 00000 n \n".encode())
    out.write(
        f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode()
    )
    return out.getvalue()


def _pdf_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")[:110]


def _parse_date(value: Any) -> date:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return date.fromisoformat(today_iso())


def _money(value: Any, default: float) -> float:
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return default


def _bounded_int(value: Any, default: int, low: int, high: int) -> int:
    try:
        return min(high, max(low, int(value)))
    except (TypeError, ValueError):
        return default


def _bounded_float(value: Any, default: float, low: float, high: float) -> float:
    try:
        return min(high, max(low, float(value)))
    except (TypeError, ValueError):
        return default


def _fmt_money(value: float) -> str:
    return f"${value:,.2f}"
