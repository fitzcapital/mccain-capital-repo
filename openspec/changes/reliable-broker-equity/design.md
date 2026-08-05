## Context

Live Sync currently has two independent data paths:

1. statement/fill capture imports trades and may expose a statement ending balance; and
2. a post-import Vanquish dashboard capture attempts to parse broker equity and drawdown metrics.

The August 5 run demonstrated the split: seven trades imported, while the dashboard metric request
redirected to Vanquish signup and returned `auth_required`. The selected account therefore retained
manual drawdown but had no Broker Equity. The UI reduced this to `Ledger fallback active`, which
does not explain that trade import succeeded while equity refresh failed.

Existing account columns already hold broker equity, peak, remaining drawdown, max loss, and a
metrics timestamp. Existing dashboard endpoints also prove manual equity updates can be validated
and persisted. The smallest safe change is to consolidate update decisions in the trade-sync service,
reuse account storage and validation, and expose the manual action on Live Sync.

## Goals / Non-Goals

**Goals:**

- Attempt equity acquisition exactly once per completed normal Live Sync, regardless of fill count.
- Choose the best valid equity source deterministically and record the outcome separately from the
  trade-import outcome.
- Preserve the last valid equity and all unrelated manual risk metrics on failed or partial refreshes.
- Let the user update Broker Equity directly for the selected account on the Live Sync page.
- Make value, source, timestamp, failure reason, and recovery action visible at the receiving page.

**Non-Goals:**

- Automating or bypassing Vanquish/Google authentication.
- Treating ledger-derived balance as broker-reported equity.
- Reworking Remaining Drawdown formulas, account lifecycle, or trade reconciliation.
- Adding a general-purpose financial provenance framework.
- Making mobile layout a priority beyond preserving existing responsive behavior.

## Decisions

### 1. Use a single source-priority resolver after statement parsing and metric capture

The sync service will construct an equity-refresh result with the following priority:

1. `broker_dashboard`: valid numeric equity from an account-matched authenticated metrics response;
2. `statement`: valid ending balance only when the statement range is trustworthy and assigned to
   the selected account;
3. `preserved`: the last stored equity when neither automated source succeeds;
4. `missing`: no automated source and no stored value.

The resolver returns `status`, `source`, `value`, `updated`, `reason`, and `attempted_sources`. Job
summaries and the Live Sync view consume this same result.

Alternative considered: always copy current ledger balance into Broker Equity. Rejected because
ledger balance is calculated local state and is not evidence of broker equity.

### 2. Make account metric writes partial and non-destructive

Automated capture will update only fields actually present in a successful response. Missing equity,
peak, max-loss, or drawdown keys MUST preserve their stored values. Equity peak advances to the
greater of the existing peak and a newly accepted equity value unless the broker supplies a higher
valid peak.

Alternative considered: continue calling `update_account_broker_metrics` with `None` for absent
fields. Rejected because a partial response can erase a manual value.

### 3. Reuse one manual-equity service operation from Dashboard and Live Sync

Validation and persistence will move behind a shared service operation that accepts account ID,
equity, return URL, and request context. The existing Dashboard route remains compatible; Live Sync
gets a narrowly scoped POST route or handler that invokes the same operation and returns to
`/trades/upload/statement?ws=live&account_id=<id>`.

Manual save sets the equity and peak safely, preserves drawdown/max-loss, marks the source as
`manual`, updates the timestamp, and emits a user-visible success or validation message.

Alternative considered: duplicate the Dashboard handler in the trades service. Rejected because
the validation and peak rules could drift.

### 4. Separate import outcome from equity-refresh outcome

Successful fill import remains successful even when equity refresh fails. The background job stores
an `equity_refresh` object in its summary/status payload and warnings contain the actionable cause,
such as broker authentication required or statement ending balance unavailable. The primary Live
Sync status continues to describe the import, while the Broker Equity card describes the equity lane.

Alternative considered: fail the whole sync when equity is unavailable. Rejected because it would
misrepresent successfully imported trades and encourage duplicate reruns.

### 5. Keep broker source truth in advanced diagnostics

Sync records retain the stored broker value, source, timestamp, and failure reason for diagnostics.
The primary Dashboard and Live Sync surfaces do not expose a second equity card or manual Broker
Equity control because Ledger Equity is the operational source of truth.

### 6. Display transaction-derived Ledger Equity as the primary value

The selected account already exposes a transaction-derived balance calculated as account opening
balance plus recorded realized trade P&L. Live Sync and Dashboard will display that value as
`Ledger Equity`, along with its opening-balance and net-realized-P&L components. The supplied account
statement proves this calculation matches broker balance and equity when positions are closed. No
additional financial value is persisted and no reconciliation delta competes in the main workflow.

## Data Flow

1. Live Sync authenticates and captures the statement/fills.
2. The statement parser returns fills, optional ending balance, and trust warnings.
3. Live Sync attempts account metrics capture for the selected broker account.
4. The resolver validates account identity, numeric value, statement trust, and source priority.
5. A successful result writes only accepted account fields; failure writes no financial values.
6. The background job persists the equity-refresh outcome independently of import success.
7. The Live Sync page reads selected-account values plus the latest matching refresh outcome.
8. Manual save uses the shared update operation and returns to the same selected-account surface.
9. Receiving surfaces calculate and display Ledger Equity as opening balance plus recorded net
   realized trade P&L; broker values remain diagnostic-only.

## Compatibility and Migration

- Existing account values remain valid and require no destructive migration.
- If current schema lacks a reliable equity-source field, add a nullable source column through the
  existing migration system and backfill no inferred source; legacy populated values display as
  `Stored value` until the next successful automatic or manual update.
- Existing Dashboard manual-equity URLs and form behavior remain supported.
- Rollback removes the new resolver/UI wiring; stored equity values remain compatible.

## Risks / Trade-offs

- [Broker dashboard still requires Google-backed authentication] → Surface `auth_required`, retain
  the prior value, and keep the manual editor available.
- [Statement ending balance can describe the wrong period] → Accept it only when the requested date
  range was applied and the selected account matches; otherwise mark it untrusted.
- [Partial metric payload erases manual drawdown] → Use presence-aware repository updates and test
  each missing-field boundary.
- [Manual entry can be mistyped] → Require a finite non-negative amount, show account identity, and
  confirm the saved value/source/timestamp.
- [Import success can obscure equity failure] → Persist and render distinct import and equity states.
- [One shared timestamp covers multiple broker metrics in the current schema] → Prefer the smallest
  schema change; add equity-specific provenance only if implementation confirms the shared field
  cannot represent the required UI truthfully.

## Open Questions

- Implementation should confirm whether `broker_metrics_source` exists in the current migration
  schema but is absent from the repository projection, or whether one nullable provenance column is
  required.
- Vanquish may eventually expose equity in the statement response reliably; the resolver keeps this
  source isolated so that integration can improve without changing UI semantics.
