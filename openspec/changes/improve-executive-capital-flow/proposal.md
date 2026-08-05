## Why

The Executive Command Center currently judges treasury safety from the projected month-end BOA balance, which can hide an intramonth cash shortfall. A dated capital-flow projection is needed so the user can see whether scheduled inflows and outflows can be absorbed without breaching the required reserve.

## What Changes

- Project BOA and Current balances chronologically across every day of the selected month.
- Distinguish the hard floor, protected reserve, month-end target, projected intramonth low, and projected month-end close.
- Calculate immediate cushion, month-path cushion, funding gap, low-balance date, recovery date, and absorbable unexpected expense.
- Support build, secured, and recovery phases so a future reserve goal is not treated as an already-established floor.
- Activate the $10,000 protected floor only after the remaining projected path stays at or above it.
- Base treasury status and recommended action on the full balance path, not only the closing balance.
- Surface the assumptions and dated entries that determine the projection, including manual adjustments and fallback timing.
- Preserve manually entered projection controls and ledger state during recalculation.
- Generate paycheck funding windows from the confirmed July 31 biweekly anchor rather than resetting cycles at calendar-month boundaries.
- Treat the active paycheck cycle as settled when the user enters current as-of balances, without requiring per-bill status toggles.
- Use the confirmed $1,873.78 Current and $1,873.78 BOA regular split and an editable estimated $9,400 September 25 exception.
- Log explicit recalculation snapshots locally with their as-of balances, settled cycle, paycheck assumptions, projected low, projected close, and floor phase.
- Make the current calendar month the default Executive workspace and keep the entered BOA as-of balance consistent across every summary surface.
- Consolidate repeated Executive summaries into one desktop-first decision band and keep each workspace tab focused on unique information.
- Replace Roadmap and Year View duplication with one clearly labeled 12-month planning baseline that discloses repeated assumptions.
- Remove or relabel synthetic Executive metrics that are not supported by authoritative inputs, including the static CEO score, rough runway, and derived net-worth trend.
- Make cycle-managed ledger items read-only by default, keep manual status controls for exceptions, and make budget groups non-overlapping.
- Capture the account, date, description, amount, and adjustment type required for quick actions to affect the projection accurately.
- Add focused automated coverage for transaction ordering, floor breaches, recovery, transfers, and month-end totals.
- Non-goals: bank synchronization, automatic transaction imports, investment-performance forecasting, probabilistic income forecasting, changes to unrelated dashboard projections, and a mobile-specific redesign.

## Capabilities

### New Capabilities

- `executive-capital-flow`: Dated BOA and Current cash-flow projection, liquidity guardrails, absorption metrics, and path-based treasury status for the Executive Command Center.

### Modified Capabilities

None.

## Impact

- Affects the Executive Command Center projection service data, its client-side projection engine, projection controls, treasury summary cards, roadmap/month views, and focused tests.
- Uses the existing operating-plan deposits, bills, subscriptions, transfers, ledger statuses, manual adjustments, and user-entered opening balances.
- Uses a $4,000 temporary hard floor while building toward the $10,000 permanent floor goal.
- Assumes an active floor is an every-day minimum and the future floor goal remains a milestone until its activation condition is satisfied.
- Entries with explicit dates use those dates; entries without dates require a visible deterministic fallback assumption.
- No new external dependency, background job, database integration, or bank API is required.

### Acceptance Criteria

- Recalculation orders effective transactions by day and produces a daily running balance for BOA and Current.
- The displayed projected low and its date match the minimum BOA balance on that path.
- A hard-floor breach is reported even when the projected month-end close exceeds the floor or target.
- Absorbable unexpected expense never exceeds the month-path cushion above the hard floor and never displays as negative.
- The final daily balance equals the projected month-end close and reconciles to opening balances plus all effective inflows, outflows, and transfers.
- Manual control values and ledger statuses remain authoritative across recalculation.
- The current active paycheck cycle and its funded bills contribute zero forward impact; only future funding windows are projected.
- Regular pay events repeat every 14 days from July 31, 2026, and the September 25 exception is visibly labeled estimated until updated.
- Before activation, absorption is measured above the $4,000 temporary floor; after activation, it is measured above the $10,000 permanent floor.
- The $10,000 floor is reported as secured only when the remaining projected balance path never drops below it.
- The Executive workspace defaults to the current operating month and uses `Current BOA` terminology for the user-entered as-of account balance.
- Roadmap is the only annual projection surface and labels future values as a planning baseline rather than a bank-like forecast.
- Ledger cycle-managed rows do not require individual paid toggles, budget summary groups do not double-count the same obligation, and quick actions collect enough timing and account data to update the path.
- Desktop summaries do not repeat the same core balance and floor values in adjacent bands, and unsupported synthetic metrics are absent.
