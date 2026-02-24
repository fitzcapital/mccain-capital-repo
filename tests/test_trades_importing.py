"""Broker import behavior tests."""

from mccain_capital.runtime import db
from mccain_capital.services import trades_importing as importing


def test_parse_broker_line_with_balance_column():
    line = "SPX JAN/30/26 6935 PUT | 1/30/26, 10:30 AM | SELL | 2 | 18.90 | 0.70 | 50924.40"
    parsed = importing.parse_broker_line_any(line)
    assert parsed is not None
    assert parsed["side"] == "SELL"
    assert float(parsed["balance"]) == 50924.40


def test_broker_import_is_idempotent_and_uses_statement_ending_balance(app):
    text = "\n".join(
        [
            "SPX JAN/30/26 6935 PUT | 1/30/26, 10:00 AM | BUY | 1 | 10.00 | 0.70",
            "SPX JAN/30/26 6935 PUT | 1/30/26, 10:30 AM | SELL | 1 | 12.00 | 0.70",
        ]
    )

    inserted_1, msgs_1 = importing.insert_trades_from_broker_paste(text, ending_balance=50198.60)
    assert inserted_1 == 1
    assert all("duplicate" not in m.lower() for m in msgs_1)

    with db() as conn:
        row = conn.execute("SELECT balance FROM trades LIMIT 1").fetchone()
    assert row is not None
    assert round(float(row["balance"] or 0.0), 2) == 50198.60

    inserted_2, msgs_2 = importing.insert_trades_from_broker_paste(text, ending_balance=50198.60)
    assert inserted_2 == 0
    assert any("duplicate" in m.lower() for m in msgs_2)


def test_broker_import_reconciliation_report_fields(app):
    text = "\n".join(
        [
            "SPX JAN/30/26 6935 PUT | 1/30/26, 10:00 AM | BUY | 1 | 10.00 | 0.70",
            "SPX JAN/30/26 6935 PUT | 1/30/26, 10:30 AM | SELL | 1 | 12.00 | 0.70",
        ]
    )
    inserted, messages, report = importing.insert_trades_from_broker_paste_with_report(
        text, ending_balance=50198.60
    )
    assert inserted == 1
    assert isinstance(messages, list)
    assert report["fills_parsed"] == 2
    assert report["pairs_completed"] == 1
    assert report["inserted_trades"] == 1
    assert report["duplicates_skipped"] == 0
    assert report["open_contracts"] == 0
    assert report["statement_ending_balance"] == 50198.60
    assert report["ledger_ending_balance"] is not None
