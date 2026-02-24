"""Trade import/ocr bridge.

This isolates the remaining legacy OCR/import pipeline while services continue to
migrate away from app_core.
"""

from mccain_capital import app_core as core

insert_trades_from_broker_paste = core.insert_trades_from_broker_paste
insert_trades_from_paste = core.insert_trades_from_paste
parse_statement_html_to_broker_paste = core.parse_statement_html_to_broker_paste
insert_balance_snapshot = core.insert_balance_snapshot
ocr_pdf_to_broker_paste = core.ocr_pdf_to_broker_paste
load_ocr_deps = core._load_ocr_deps
prep_for_ocr = core._prep_for_ocr
normalize_ocr = core.normalize_ocr
stitch_ocr_rows = core.stitch_ocr_rows
ocr_pdf_to_text = core.ocr_pdf_to_text
extract_statement_balance = core.extract_statement_balance
