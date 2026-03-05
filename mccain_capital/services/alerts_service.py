"""SQLite-backed market alert rules and fire logging."""

from __future__ import annotations

from typing import Any, Dict, List

from mccain_capital import runtime as app_runtime


def ensure_alert_tables() -> None:
    with app_runtime.db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                rule_type TEXT NOT NULL CHECK (rule_type IN ('above', 'below')),
                threshold REAL NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_alerts_symbol_enabled ON alerts(symbol, enabled);

            CREATE TABLE IF NOT EXISTS alert_fires (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                message TEXT NOT NULL,
                fired_at TEXT NOT NULL,
                UNIQUE(alert_id, fired_at)
            );
            CREATE INDEX IF NOT EXISTS idx_alert_fires_symbol_time ON alert_fires(symbol, fired_at DESC);
            """
        )

        count_row = conn.execute("SELECT COUNT(*) AS c FROM alerts").fetchone()
        count = int((count_row["c"] if count_row else 0) or 0)
        if count == 0:
            now = app_runtime.now_iso()
            for sym in ("SPY", "QQQ", "NVDA", "TSLA"):
                conn.execute(
                    """
                    INSERT INTO alerts(symbol, rule_type, threshold, enabled, created_at, updated_at)
                    VALUES (?, 'above', 0, 0, ?, ?)
                    """,
                    (sym, now, now),
                )


def fetch_enabled_alerts_for_symbol(symbol: str) -> List[Dict[str, Any]]:
    ensure_alert_tables()
    sym = str(symbol or "").strip().upper()
    with app_runtime.db() as conn:
        rows = conn.execute(
            """
            SELECT id, symbol, rule_type, threshold, enabled
            FROM alerts
            WHERE enabled = 1 AND UPPER(symbol) = ?
            ORDER BY id
            """,
            (sym,),
        ).fetchall()
    return [dict(r) for r in rows]


def _is_triggered(rule_type: str, threshold: float, price: float) -> bool:
    if rule_type == "above":
        return float(price) >= float(threshold)
    if rule_type == "below":
        return float(price) <= float(threshold)
    return False


def evaluate_alerts(symbol: str, price: float, state: Dict[str, Any]) -> List[str]:
    """Evaluate enabled rules with edge-trigger semantics.

    `state` tracks prior condition state by alert id.
    """
    ensure_alert_tables()
    if price is None:
        return []
    messages: List[str] = []
    sym = str(symbol or "").strip().upper()
    rules = fetch_enabled_alerts_for_symbol(sym)
    for rule in rules:
        alert_id = int(rule.get("id") or 0)
        if alert_id <= 0:
            continue
        rule_type = str(rule.get("rule_type") or "").strip().lower()
        threshold = float(rule.get("threshold") or 0.0)
        triggered = _is_triggered(rule_type, threshold, float(price))

        prev = bool(state.get(alert_id, False))
        if triggered and not prev:
            direction = "above" if rule_type == "above" else "below"
            msg = f"{sym} crossed {direction} {threshold:.2f} at {float(price):.2f}"
            fired_at = app_runtime.now_iso()
            with app_runtime.db() as conn:
                conn.execute(
                    """
                    INSERT INTO alert_fires(alert_id, symbol, price, message, fired_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (alert_id, sym, float(price), msg, fired_at),
                )
            messages.append(msg)
            state[alert_id] = True
        elif triggered and prev:
            state[alert_id] = True
        else:
            state[alert_id] = False

    return messages
