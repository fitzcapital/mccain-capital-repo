## 1. Provenance and Safe Persistence

- [x] 1.1 Confirm the account schema and repository projections for equity source/timestamp; add the
  smallest nullable provenance migration only if current fields cannot represent the contract.
- [x] 1.2 Implement a presence-aware account metric update that preserves stored equity, drawdown,
  max loss, and peak when an automated response omits those fields.
- [x] 1.3 Implement a shared manual-equity service operation with finite non-negative validation,
  account checks, peak advancement, provenance, timestamp, and audit recording.
- [x] 1.4 Add repository and service tests for partial responses, failed-refresh preservation, source
  recording, manual validation, and peak behavior.

## 2. Equity Resolution in Live Sync

- [x] 2.1 Add a source-priority equity resolver for account-matched broker metrics, trustworthy
  statement ending balance, preserved stored value, and missing state.
- [x] 2.2 Run equity resolution once for every completed normal sync path, including imported fills,
  duplicate/no-new-fill results, and balance-only statements.
- [x] 2.3 Reject statement equity when date-range fallback or account mismatch makes it untrusted, and
  never substitute ledger-derived balance.
- [x] 2.4 Persist `equity_refresh` separately in sync job/status summaries with source, value,
  timestamp, attempted sources, and redacted actionable reason.
- [x] 2.5 Add focused sync tests covering broker priority, statement fallback, authentication failure,
  account mismatch, zero-fill runs, and successful imports with failed equity refresh.

## 3. Manual Recovery and Receiving-Surface Clarity

- [x] 3.1 Add an account-scoped Live Sync manual Broker Equity POST route/handler that reuses the shared
  update operation and returns to the selected live workspace.
- [x] 3.2 Add the manual equity input and save action to the Live Sync Review & Reference card without
  removing the existing manual Remaining Drawdown control.
- [x] 3.3 Render stored value, source, saved timestamp, latest automatic refresh outcome, and precise
  recovery guidance on the Broker Equity card.
- [x] 3.4 Keep import success and equity-refresh failure visually distinct in the Live Sync result and
  expose broker authentication guidance when metrics redirect to login/signup.
- [x] 3.5 Add handler, template, and UI contract tests for valid/invalid manual saves, preserved values,
  selected-account continuity, and missing/auth-required states.

## 4. Verification

- [x] 4.1 Run the focused trade feature, Live Sync lane, migration, and repository/service tests.
- [x] 4.2 Run Black, Ruff, JavaScript syntax checks for touched assets, and `git diff --check`.
- [x] 4.3 Rebuild the local Podman app, verify `/healthz`, and test the signed-in Live Sync receiving
  surface for automatic status, manual save, source label, timestamp, and refresh persistence.
- [x] 4.4 Run `openspec validate reliable-broker-equity --type change --strict --no-interactive` and
  record any required follow-up before sync/archive.

## 5. Estimated Ledger Equity

- [x] 5.1 Add an account-scoped Estimated Ledger Equity view model using opening balance plus recorded
  realized trade P&L, with an optional Broker Equity reconciliation delta.
- [x] 5.2 Render the estimate, calculation components, and delta on Live Sync and Dashboard without
  relabeling the estimate as Broker Equity.
- [x] 5.3 Add focused calculation and receiving-surface contract tests.
- [x] 5.4 Run focused tests, formatting/lint checks, strict OpenSpec validation, rebuild, health check,
  and signed-in browser verification.

## 6. Ledger Equity Simplification

- [x] 6.1 Rename the calculation and view model to Ledger Equity while preserving opening-balance plus
  recorded net-realized-P&L math.
- [x] 6.2 Remove primary Broker Equity cards, missing warnings, manual inputs, and reconciliation delta
  from Dashboard and Live Sync; retain broker capture in diagnostics.
- [x] 6.3 Update focused tests for the simplified receiving-surface contract.
- [x] 6.4 Run focused tests, formatting/lint checks, strict validation, rebuild, health check, and
  signed-in browser verification.
