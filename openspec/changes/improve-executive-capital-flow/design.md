## Context

The Executive Command Center builds a ledger from operating-plan deposits, bills, subscriptions, generated transfers, advanced assumptions, and manual adjustments. Its projection currently applies ledger entries in construction order and evaluates treasury status from the final BOA balance. Although entries carry a `dueDay`, the projection does not retain a daily balance path, so it cannot reveal a temporary reserve breach or measure how much additional expense the month can absorb.

The existing interface also derives its protected floor from the target instead of honoring the configured month-specific floor and red line. The redesign must keep manual opening balances and ledger state authoritative, reconcile both BOA and Current, and avoid presenting estimates as bank-synchronized facts.

## Goals / Non-Goals

**Goals:**

- Produce a deterministic daily BOA and Current balance path from effective dated entries.
- Measure the intramonth low, funding gap, reserve cushion, recovery, and closing balances.
- Make path-based risk visible without removing the existing ledger and monthly planning workflow.
- Honor each month's configured hard floor, protected reserve, and target as distinct guardrails.
- Distinguish a temporary build-phase floor from a permanent floor goal and its activation state.
- Preserve existing manual projection controls, adjustments, and paid/skipped ledger behavior.

**Non-Goals:**

- Connecting to bank APIs or importing live transactions.
- Predicting uncertain income, trading returns, or transaction posting behavior probabilistically.
- Replacing the operating plan, ledger editor, or annual roadmap.
- Changing Dashboard Forward Pace or other unrelated financial projections.
- Spending this iteration on a mobile-specific redesign; the Executive workspace remains usable responsively, but desktop is the primary receiving surface.

## Decisions

### Build a chronological daily projection

The client projection engine will normalize each effective entry to an integer day within the selected month, sort entries by day, and calculate end-of-day BOA and Current balances from an explicit as-of day. Entries on the same day will be aggregated for daily risk measurement while remaining individually visible in the ledger.

This is preferred over a closing-balance-only calculation because liquidity risk depends on when money moves. It is also preferred over hourly or bank-posting simulation because the source plan only has day-level precision.

### Use deterministic fallback dates and disclose them

An explicit `dueDay` is authoritative. Existing deterministic timing rules will supply a fallback day for entries without one, but the UI will label those entries as estimated timing. Days outside the month will be clamped to the month's valid range. Stable entry identity will break same-day sort ties so recalculation is repeatable.

This retains compatibility with the existing plan while making missing timing visible. Blocking projection until every entry has an explicit date was rejected because it would make the current operating plan unusable.

### Separate guardrails from calculated outcomes

The month configuration will provide three independent values:

- **Hard floor:** absolute minimum permitted at any end-of-day point.
- **Protected reserve:** preferred minimum maintained throughout the month.
- **Month-end target:** desired BOA closing balance.

The calculated **projected low** is not a floor. Configured values will be honored directly rather than derived from the target.

### Activate the permanent floor only after it is supportable

The operating plan will expose a temporary hard floor, a permanent floor goal, and an activation month. August and pre-activation September use a $4,000 active hard floor. The $10,000 permanent floor becomes active only when the first day at or above $10,000 is followed by a remaining projected path whose minimum never falls below $10,000.

The projection will return the active hard floor, floor goal, phase, activation day when available, and gap to the floor goal. A previously secured floor that is no longer supported enters recovery phase and continues using $10,000 as the active floor. Browser state may preserve the secured milestone once achieved; the projected path remains the source for whether the current month is secured or recovering.

### Calculate absorption from the weakest point

The engine will return against the active hard floor:

- `immediateCushion = max(0, openingBOA - hardFloor)`
- `monthPathCushion = projectedLow - hardFloor`
- `fundingGap = max(0, hardFloor - projectedLow)`
- `absorbableUnexpectedExpense = max(0, monthPathCushion)`
- the first date of the projected low
- the first subsequent date that the balance returns to or above the protected reserve

Absorbable expense is based on the lowest projected BOA balance, not the closing balance. This deliberately provides a conservative liquidity figure.

### Derive status from the path before the close

Status precedence will be:

1. `Temporary Floor at Risk` when a build-phase projected low is below $4,000.
2. `Building Normally` when the build-phase path protects $4,000 but remains below the $10,000 floor goal.
3. `$10K Floor Secured` when the path reaches $10,000 and never falls below it afterward.
4. `Rebuilding Secured Floor` when a previously secured $10,000 floor is breached.

This prevents a strong close from hiding an earlier breach.

### Treat transfers as cash-neutral across combined accounts

BOA-to-Current and Current-to-BOA transfers change the two account paths in opposite directions on the same day. They do not count as income, expense, or combined-capital growth. The BOA path remains the source for treasury guardrails, while Current remains visible to expose bill-funding shortfalls.

### Preserve manual state and fail visibly

Recalculation will read current controls and existing ledger state without overwriting them. Invalid or non-finite inputs will not replace the last successful projection; the interface will identify the invalid field and retain the prior result. No runtime financial data migration is required.

### Treat the entered opening balance as current as-of cash

For the current month, the projection starts on today's calendar day. A ledger entry marked `Paid` or `Skipped` remains visible but has zero forward impact because the entered BOA and Current balances already include completed activity. A past-due entry still marked `Planned` or `Adjusted` remains an obligation and is applied on the as-of day. Future months start before day 1; past months use their final day.

This avoids subtracting first-check bills twice while ensuring an overdue unpaid bill does not disappear merely because its due date passed.

### Use continuous paycheck funding windows

Regular pay events repeat every 14 days from the confirmed July 31, 2026 anchor. Each funding cycle starts on a payday and ends immediately before the next payday, even when the window crosses a calendar-month boundary. The cycle containing the as-of date is automatically settled because the user enters balances after paying that cycle's obligations.

Regular events deposit $1,873.78 to Current and $1,873.78 to BOA. September 25 uses an estimated $4,700/$4,700 exception until the user replaces it. The projection will display the settled cycle, next pay event, regular split, and whether an event is estimated.

Manual `Adjusted` entries remain available for genuine exceptions, but normal use does not require bill-by-bill completion toggles.

### Fund Current just in time

The engine will calculate Current's end-of-day path from its as-of balance, remaining deposits, and remaining bills. When a day's remaining activity would make Current negative, it will create a BOA-to-Current transfer for only that day's shortfall. This replaces the single immediate transfer for the entire month's Current deficit and prevents an artificial early BOA low.

### Log explicit projection snapshots locally

Each Recalculate action will append a bounded local snapshot containing its timestamp, month, as-of balances, settled and next cycles, regular and exceptional paycheck assumptions, projected low and close, required Current funding, active floor, and phase. Automatic renders will not create history noise. The latest snapshots will be visible near the capital-flow path and remain local to the browser profile.

### Use one desktop decision hierarchy

The Executive page will expose one authoritative summary band containing Current BOA, projected low, active floor, absorbable cushion, projected close, and next paycheck. The duplicate month-header projection cards, KPI band, analytics KPI band, and repeated overview values will be consolidated rather than independently restyled. `Current Treasury` will be replaced with `Current BOA` wherever the value represents only the BOA account.

Daily Capital Flow remains the primary explanation of movement. Overview will summarize next cash, next obligation, the phase-aware recommendation, and projection assumptions without repeating the entire KPI band.

### Keep one annual planning baseline

Roadmap and Year View currently render nearly identical rolling projections. Roadmap will become the single `12-Month Planning Baseline`; Year View will be removed. Future rows will disclose that current configured income, bills, paycheck rules, and known exceptions repeat until the user changes them. A real projected zero must remain zero rather than falling back to a target value.

The permanent $10,000 objective will be labeled `Floor Goal` until activation and `Active Floor` only after it is secured. `Hard Floor` will not be used as an alias for an unactivated goal.

### Keep only defensible analytics

Treasury will retain four decision views: daily capital path, monthly inflow versus obligations, floor-goal progress, and the 12-month planning baseline. Static or weakly derived widgets such as CEO Score, approximate Cash Runway, synthetic Net Worth Trend, and static Capital Allocation will be removed until authoritative inputs exist.

### Make operating workspaces projection-aware

Budget groups will be mutually exclusive so the same subscription or lifestyle expense is not presented in more than one additive total. Ledger rows automatically covered by the settled funding cycle will display their automatic state without requiring a status toggle; manual overrides remain available for true exceptions. Calendar details will show account, funding cycle, automatic-settlement state, estimated timing, and projected-path impact.

Quick actions will collect the minimum data needed for deterministic projection impact: description, amount, account, effective date, and adjustment direction or expense type. Review will pair reflection prompts with an automatic opening-to-current/close comparison, projected low, unexpected adjustments, and projection delta.

### Treat payday cycles as the operating boundary

Calendar months remain the strategic Roadmap boundary, but Current-account obligations belong to the paycheck cycle that funds them. A payday cycle starts on its payday and ends immediately before the next payday, even when it crosses month-end. The August 28 cycle therefore owns Current obligations through September 10, including early-September rent.

Roadmap month rows will keep projected BOA close primary and split Current month-end cash into committed next-cycle carryover and genuinely free Current. A separate funding-cycle table will show the Current and BOA deposits, account-specific obligations, required BOA support for Current, and Current remainder for each upcoming cycle. This is classification and explanation of the existing dated projection; transfers do not become income and committed Current is not presented as absorbable cash.

A calendar-month boundary does not authorize a Current-to-BOA sweep. Cash remains in Current while obligations in its active payday cycle are outstanding; this removes the previous September 30 sweep that incorrectly moved money reserved through October 8 into BOA.

The Executive page will observe the local calendar date while it remains open and rerender when the date changes or the page returns to the foreground after a date change. A visible as-of label will state that this rollover is automatic. Funding-cycle and 12-month tables will use separate horizontal scroll regions so a wide table cannot displace the other table or clip the section heading.

Roadmap presentation will keep one title hierarchy, emphasize non-zero BOA support as the cycle's primary exception, label estimated paychecks as planning placeholders, and explain secured-floor status from the remaining BOA path. Operating priorities will use durable month-neutral language so a prior month name cannot remain visible after rollover.

Each non-zero BOA support cell will expose a desktop hover and keyboard-focus explanation. The explanation will use the same cycle inputs as the displayed total, show the subtraction formula, list the largest Current obligations in the cycle, and state that the transfer only prevents Current from falling below zero. This remains explanatory UI and does not introduce a second calculation path.

The shortfall preview will show the four largest obligations plus an `Other cycle obligations` subtotal so the visible preview reconciles to total Current bills. An inline disclosure will expand to every obligation and date in the cycle; opening it will increase the explanation area rather than clip the list.

The historical Verizon catch-up remains a one-time July Budget record and will not appear in Executive's recurring operating-plan bills. Executive will project only the regular $267 Verizon obligation with an explicit day-26 due date, preventing fallback timing and duplicate phone charges from distorting cycle shortfalls.

The recurring lifestyle plan uses a $450 `Food` obligation without a separate dates category. Funding-cycle rows summarize direct BOA obligations by name beneath the BOA-bills total. Chase is one $376 BOA obligation per month, assigned to the first monthly pay cycle, and remains visibly distinct from BOA support sent to Current.

## Risks / Trade-offs

- [Day-level timing can still differ from real bank posting] → Label the result as plan-based and identify estimated dates.
- [Same-day inflow and outflow ordering is unknown] → Measure end-of-day liquidity and aggregate same-day movement consistently rather than implying intraday precision.
- [A conservative absorption figure can understate usable cash] → Show both immediate cushion and month-path cushion with concise definitions.
- [A projected September path could briefly touch $10,000 and imply false security] → Require every remaining projected point after activation to stay at or above the goal.
- [Incorrect account assignment can distort BOA and Current paths] → Preserve account labels in the ledger and add reconciliation tests for transfers and account-specific entries.
- [Existing saved browser state may contain old assumptions] → Normalize inputs defensively and preserve the last successful result when validation fails.
- [A cycle assumption could hide an unpaid exception] → Keep manual adjustments and status overrides for exceptions while making automatic settlement the default.

## Migration Plan

1. Correct month configuration mapping so configured floors and red lines are passed through unchanged.
2. Extend the existing projection result with the daily path and liquidity metrics while retaining current closing totals.
3. Update the Executive Command Center summaries and labels to consume the new fields.
4. Add focused Python and JavaScript tests, then verify the rendered signed-in workflow locally.
5. Roll back by restoring the previous projection renderer and calculation; no stored financial records require conversion.

## Open Questions

- Whether a later iteration should support an optional intraday ordering override for same-day events.
- Whether the user eventually wants a configurable stress reserve in addition to the protected reserve and hard floor.
