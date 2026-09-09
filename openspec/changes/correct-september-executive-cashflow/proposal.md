## Why

The Executive cash-flow projection currently risks deducting September's first-cycle bills from a $4,500 balance that already reflects those payments. The page also needs the recurring Ragan & Ragan obligation represented so its forward cash balance remains trustworthy.

## What Changes

- Set September 2026's opening Current balance to $4,500 and label it as the balance after cycle-one bills.
- Exclude already-paid first-cycle bills from that September projection without suppressing either remaining September pay event.
- Project only the two remaining September checks and expenses due after the cycle-one baseline.
- Add Ragan & Ragan as a $150 recurring Current-account bill due on the 28th, beginning September 2026.
- Preserve normal full-month projection behavior for other months and future recurring bills.
- Add focused regression coverage for the new balance, settlement boundary, and recurring bill.
- Non-goals: changing bank data, historical ledger records, unrelated bills, or Market Pulse behavior.

## Capabilities

### New Capabilities

- `executive-cashflow-baselines`: Defines dated, post-settlement opening balances and effective-month recurring obligations for Executive cash-flow projections.

### Modified Capabilities


## Impact

- Executive dashboard month configuration in `mccain_capital/services/core.py`.
- Executive projection calculations in `static/js/executive_command_center.js`.
- Focused Executive dashboard tests; no database migration or new dependency.
- Financial assumptions: September begins with $4,500 after cycle-one bills are paid; the regular September check and larger September 25 check remain; Ragan & Ragan costs $150 on each month's 28th from September onward.
