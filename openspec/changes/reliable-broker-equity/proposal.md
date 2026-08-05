## Why

Live Sync can successfully import broker fills while leaving Broker Equity blank because the
separate Vanquish account-metrics request requires an authenticated dashboard session and the
captured statement may not contain a trustworthy ending balance. The app needs a reliable,
source-labeled equity workflow that attempts automatic capture every time, never erases a valid
value after failure, and gives the user a direct manual recovery path.

## What Changes

- Attempt Broker Equity capture during every normal Live Sync, including runs with no new fills.
- Use an explicit source priority: authenticated broker metrics first, trustworthy statement ending
  balance second, and a preserved manual value as the final fallback.
- Never replace an existing broker or manual equity value when an automated refresh fails or
  returns incomplete/untrusted data.
- Keep broker-reported equity capture and provenance available to sync diagnostics without exposing a
  competing manual value in the primary account workflow.
- Distinguish trade-import success from equity-refresh success in job results and on the Broker
  Equity card; show the actionable failure reason instead of only `Ledger fallback active`.
- Keep Remaining Drawdown manual unless the broker returns a complete, account-matched metric.
- Add focused repository, service, endpoint, and UI contract coverage for automatic and manual
  equity updates.

### Non-goals

- Do not label ledger-derived equity as broker-reported data; present it directly as Ledger Equity.
- Do not require the user to mark individual bills, trades, or broker events as settled.
- Do not automate Google authentication or bypass Vanquish authentication requirements.
- Do not overwrite manually entered equity or drawdown after a failed refresh.
- Do not change account sizing, drawdown formulas, or unrelated Live Sync import behavior.

### Acceptance criteria

- Every completed normal sync records a separate equity-refresh result and source.
- A valid account-matched broker value updates Broker Equity and its timestamp.
- A trustworthy statement ending balance updates Broker Equity when broker metrics are unavailable.
- A failed or incomplete automated refresh preserves the last valid Broker Equity value.
- The primary receiving surfaces show one Ledger Equity value calculated as opening balance plus
  recorded net realized trade P&L.
- Broker capture failures and provenance remain in advanced sync diagnostics rather than producing
  missing-value warnings or manual Broker Equity controls in the main workflow.

## Capabilities

### New Capabilities

- `reliable-broker-equity`: Source-prioritized automatic equity capture, preservation rules,
  transparent status reporting, and manual Broker Equity recovery for the selected account.

### Modified Capabilities

None.

## Impact

- Live Sync orchestration and Vanquish metric/statement result handling under
  `mccain_capital/services/`.
- Account metric persistence under `mccain_capital/repositories/trades.py` without a required schema
  migration unless design review identifies missing provenance fields.
- Live Sync handlers/routes, `trades/upload_statement.html`, and supporting JavaScript/CSS.
- Focused tests for source priority, failed-refresh preservation, manual updates, job summaries, and
  rendered recovery guidance.
- External dependency remains Vanquish; broker dashboard metrics may require a seeded Google-backed
  session, while manual values remain local-only.
