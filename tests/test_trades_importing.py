"""Broker import behavior tests."""

from mccain_capital.runtime import db
from mccain_capital.services import trades_importing as importing


def test_parse_broker_line_with_balance_column():
    line = "SPX JAN/30/26 6935 PUT | 1/30/26, 10:30 AM | SELL | 2 | 18.90 | 0.70 | 50924.40"
    parsed = importing.parse_broker_line_any(line)
    assert parsed is not None
    assert parsed["side"] == "SELL"
    assert float(parsed["balance"]) == 50924.40


def test_parse_contract_desc_ignores_bracketed_session_marker():
    parsed = importing.parse_contract_desc("SPX APR/17/26 [AM] 7025 CALL")
    assert parsed["ticker"] == "SPX"
    assert parsed["expiry"] == "2026-04-17"
    assert float(parsed["strike"]) == 7025.0
    assert parsed["opt_type"] == "CALL"


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


def test_broker_import_dedupes_when_contract_format_changes(app):
    original = "\n".join(
        [
            "SPX APR/17/26 7025 CALL | 4/16/26, 11:00 AM | BUY | 3 | 22.10 | 1.05",
            "SPX APR/17/26 7025 CALL | 4/16/26, 11:03 AM | SELL | 3 | 23.00 | 1.05",
        ]
    )
    changed_format = "\n".join(
        [
            "SPX APR/17/26 [AM] 7025 CALL | 4/16/26, 11:00 AM | BUY | 3 | 22.10 | 1.05",
            "SPX APR/17/26 [AM] 7025 CALL | 4/16/26, 11:03 AM | SELL | 3 | 23.00 | 1.05",
        ]
    )

    inserted_1, msgs_1 = importing.insert_trades_from_broker_paste(original, ending_balance=50198.60)
    assert inserted_1 == 1
    assert all("duplicate" not in m.lower() for m in msgs_1)

    inserted_2, msgs_2 = importing.insert_trades_from_broker_paste(
        changed_format, ending_balance=50198.60
    )
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


def test_broker_import_preflight_commit_false_does_not_write(app):
    text = "\n".join(
        [
            "SPX JAN/30/26 6935 PUT | 1/30/26, 10:00 AM | BUY | 1 | 10.00 | 0.70",
            "SPX JAN/30/26 6935 PUT | 1/30/26, 10:30 AM | SELL | 1 | 12.00 | 0.70",
        ]
    )
    inserted, messages, report = importing.insert_trades_from_broker_paste_with_report(
        text, ending_balance=50198.60, commit=False
    )
    assert inserted == 1
    assert isinstance(messages, list)
    assert report["pairs_completed"] == 1

    with db() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM trades").fetchone()
    assert int(row["c"] or 0) == 0


def test_broker_import_supports_short_sell_then_buy_round_trip(app):
    text = "\n".join(
        [
            "SPX JAN/30/26 6935 PUT | 1/30/26, 10:00 AM | SELL | 1 | 12.00 | 0.70",
            "SPX JAN/30/26 6935 PUT | 1/30/26, 10:30 AM | BUY | 1 | 10.00 | 0.70",
        ]
    )

    inserted, messages, report = importing.insert_trades_from_broker_paste_with_report(
        text, ending_balance=50198.60
    )

    assert inserted == 1
    assert all("could not parse" not in message.lower() for message in messages)
    assert report["fills_parsed"] == 2
    assert report["pairs_completed"] == 1
    assert report["inserted_trades"] == 1
    assert report["open_contracts"] == 0
    assert report["errors_count"] == 0

    with db() as conn:
        row = conn.execute(
            """
            SELECT entry_price, exit_price, gross_pl, net_pl, total_spent
            FROM trades
            LIMIT 1
            """
        ).fetchone()
    assert row is not None
    assert round(float(row["entry_price"] or 0.0), 2) == 12.00
    assert round(float(row["exit_price"] or 0.0), 2) == 10.00
    assert round(float(row["gross_pl"] or 0.0), 2) == 200.00
    assert round(float(row["net_pl"] or 0.0), 2) == 198.60
    assert round(float(row["total_spent"] or 0.0), 2) == 1200.00


def test_auto_review_payload_adds_no_cut_20_loss_rule_break():
    payload = importing._auto_review_payload(
        {
            "entry_time": "10:00 AM",
            "net_pl": -30.0,
            "total_spent": 100.0,
            "comm": 0.7,
            "contracts": 1,
            "result_pct": -25.0,
        }
    )
    tags = str(payload.get("rule_break_tags") or "")
    assert "no-cut-20-loss" in tags
