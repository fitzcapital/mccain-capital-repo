## Context

The Executive projection builds a month from configured opening balances plus generated deposits, bills, and subscriptions. Automatic settlement currently applies only to the active calendar funding cycle, so it cannot represent a future month whose opening balance was supplied after a cycle had already been settled.

## Goals / Non-Goals

**Goals:**

- Represent September's $4,500 opening Current balance as post-first-cycle cash.
- Make the UI describe that value as the balance after cycle-one bills, not a live current balance.
- Give first-cycle expense entries zero forward impact without deleting them from the ledger view; first-cycle deposits remain projected.
- Include the two remaining September checks, including the larger September 25 check, in the forward projection.
- Add the $150 Ragan & Ragan obligation from September forward.
- Preserve existing month and manual-ledger behavior.

**Non-Goals:**

- Mutating bank or journal data.
- Inferring settlement from today's date.
- Reworking unrelated Executive dashboard projections.

## Decisions

1. Add an explicit `baselineSettledExpenseCycles` month field. The projection will mark bills, subscriptions, and other expense entries assigned to those cycles as settled while leaving deposits active. `effectiveAmount` will continue to make settled entries zero-impact. This reuses the existing settlement mechanism and keeps the entries visible for explanation.
2. Assign generated pay deposits and bill/subscription rows an explicit cycle number from their `Paycheck 1` / `First Half` or `Paycheck 2` / `Second Half` timing. Entries without a cycle classification remain part of the forward projection.
3. Configure September with `openingCurrent: 4500`, `openingBOA: 0`, and `baselineSettledExpenseCycles: [1]`. This treats $4,500 as cash after first-cycle bills while retaining both remaining September pay events.
4. Add Ragan & Ragan only when building September 2026 and later months. It is a Current-account, second-cycle bill due on day 28.
5. Present September's configured opening value as `Balance after cycle-one bills`. The label must not imply that this future baseline is today's live account balance.

Alternatives considered: settling the whole first cycle incorrectly removes the first remaining paycheck; shifting all pre-baseline due dates forward would still deduct completed expenses; deleting entries would hide the audit trail; date-driven settlement would make a configured future baseline dependent on the viewer's current date.

## Risks / Trade-offs

- [Cycle labels can be misclassified] -> Only explicit first/second-cycle timings are auto-settled; unclassified monthly items remain visible and included.
- [A manual override could conflict with the configured baseline] -> Existing local input overrides remain authoritative, while settlement metadata stays visible in ledger details.
- [Future bill changes could affect old projections] -> The effective-month check prevents Ragan & Ragan from appearing before September 2026.

## Migration Plan

No database migration is required. Deploy server configuration and JavaScript together. Rollback is the removal of the new month fields, recurring bill, and cycle classification.

## Open Questions

None. The user confirmed the $150 amount and that September's $4,500 follows completion of the first cycle.
