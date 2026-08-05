## 1. Correct Projection Inputs

- [x] 1.1 Pass each operating month's configured hard floor, protected reserve, and target through the Executive service without deriving floors from the target.
- [x] 1.2 Mark explicit versus estimated transaction timing in the projection ledger data and clamp effective dates to the selected month.
- [x] 1.3 Add focused service/template tests confirming August guardrails and projection source assumptions are rendered correctly.

## 2. Build the Daily Capital-Flow Engine

- [x] 2.1 Normalize and deterministically order effective ledger entries by selected-month day while preserving ledger status and manual adjustments.
- [x] 2.2 Calculate reconciled end-of-day BOA and Current paths, treating cross-account transfers as equal opposite movements.
- [x] 2.3 Calculate projected low and date, immediate cushion, month-path cushion, funding gap, absorbable unexpected expense, and recovery date.
- [x] 2.4 Derive status and recommended action using projected-low precedence before the month-end target.
- [x] 2.5 Reject invalid projection controls without replacing the last successful projection or manual values.
- [x] 2.6 Start current-month projections on the as-of day, exclude paid/skipped entries from forward impact, and carry overdue planned entries into the as-of day.
- [x] 2.7 Calculate build, secured, and recovery phases with a $4,000 temporary floor and a $10,000 permanent floor goal.
- [x] 2.8 Activate the permanent floor only when every remaining projected balance after the first $10,000 point stays at or above $10,000.
- [x] 2.9 Persist a successfully secured permanent floor and measure later recovery gaps against $10,000.
- [x] 2.10 Generate continuous 14-day funding cycles from July 31 and automatically settle the cycle containing the current as-of balance.
- [x] 2.11 Replace regular paycheck allocations with $1,873.78 per account and model September 25 as an editable estimated $4,700/$4,700 exception.
- [x] 2.12 Replace the immediate whole-month Current float with dated just-in-time BOA transfers that prevent Current from becoming negative.
- [x] 2.13 Log bounded local snapshots only on explicit recalculation with balances, funding-cycle assumptions, projection outcomes, and floor phase.

## 3. Update the Executive Command Center

- [x] 3.1 Replace ambiguous floor labels with separate hard floor, protected reserve, projected low, and month-end target labels.
- [x] 3.2 Add concise liquidity cards for low date, funding gap or absorbable expense, recovery, and projected close.
- [x] 3.3 Add a compact daily capital-flow path or timeline that makes balance fluctuations and guardrail crossings visible.
- [x] 3.4 Disclose plan-based inputs and visibly identify entries using estimated timing.
- [x] 3.5 Ensure recalculation, ledger edits, month switching, and roadmap summaries consume the same projection result.
- [x] 3.6 Display the as-of starting point and distinguish completed ledger activity from remaining forecast impact.
- [x] 3.7 Display active floor, permanent floor goal, phase, activation date, goal gap, and phase-appropriate absorption language.
- [x] 3.8 Display the settled funding cycle, next paycheck date, confirmed regular allocation, and estimated-event labeling without requiring bill toggles.
- [x] 3.9 Display recent explainable projection snapshots for the selected month.

## 4. Verify Financial Behavior

- [x] 4.1 Add JavaScript tests for chronological ordering, same-day determinism, short-month clamping, and missing-date fallbacks.
- [x] 4.2 Add JavaScript tests for temporary hard-floor breach, protected-reserve breach, recovery, and a healthy path below target.
- [x] 4.3 Add reconciliation tests for BOA, Current, transfers, paid/skipped entries, manual adjustments, and projected close.
- [x] 4.4 Add tests ensuring absorbable expense is conservative and never negative and invalid inputs preserve the last successful result.
- [x] 4.5 Run focused pytest coverage, JavaScript syntax/tests, formatting or lint checks, and strict OpenSpec validation.
- [x] 4.6 Rebuild the local application and verify the signed-in Executive Command Center receiving surface with the user-confirmed $4,400 current BOA scenario.
- [x] 4.7 Add regression coverage proving paid first-check bills are not deducted again from a current BOA balance and overdue planned bills remain projected.
- [x] 4.8 Add regression coverage for touching but not securing $10,000, successful activation, secured absorption, and recovery phase.
- [x] 4.9 Add regression coverage for cross-month cycles, automatic cycle settlement, new regular allocations, the September exception, and just-in-time Current funding.
- [x] 4.10 Add regression coverage for explicit-only bounded projection logging and visible material assumptions.

## 5. Simplify the Desktop Executive Workspace

- [x] 5.1 Default the Executive workspace to the current operating month, keep the entered BOA balance authoritative, and replace BOA-only `Current Treasury` labels with `Current BOA`.
- [x] 5.2 Consolidate repeated month, KPI, analytics, and Overview metrics into one desktop decision band and use the projection's phase-aware recommendation everywhere.
- [x] 5.3 Replace Roadmap and Year View with one assumptions-labeled 12-month planning baseline, preserve legitimate zero projections, and correct build-phase floor-goal terminology.
- [x] 5.4 Reduce Treasury to authoritative capital-path, inflow/obligation, floor-goal, and planning-baseline views; remove unsupported CEO score, runway, net-worth, and static allocation widgets.
- [x] 5.5 Make budget groups non-overlapping and make automatically settled cycle ledger rows read-only while retaining explicit exception overrides.
- [x] 5.6 Enrich calendar details and quick actions with account, funding-cycle, settlement, timing, date, description, direction, and projection-impact context.
- [x] 5.7 Add an automatic projection comparison to Review and remove fabricated priority statuses or replace them with stored, editable state.
- [x] 5.8 Add focused regression coverage, run targeted validation, rebuild the local app, and verify the signed-in desktop Executive receiving surface.
- [x] 5.9 Replace oversized Roadmap cards with an August-forward desktop accounting table that separates BOA, Current, net new cash, Current-to-BOA transfers, combined close, and the estimated September paycheck.
- [x] 5.10 Classify Current month-end carryover by its cross-month payday cycle, make BOA close primary, and add an upcoming funding-cycle table with account-specific obligations, BOA support, and Current remainder.
- [x] 5.11 Refresh the as-of date and active cycle automatically across local-day rollover, disclose the tracked date, and isolate Roadmap table overflow so headings and first columns remain visible.
- [x] 5.12 Remove stale month-specific priority copy, simplify Roadmap hierarchy, emphasize required BOA support, strengthen estimate labeling, and explain secured-floor status from the remaining path.
- [x] 5.13 Add a hover/focus BOA-shortfall explanation using the cycle formula and largest Current obligations, with focused regression and desktop verification.
- [x] 5.14 Remove the completed Verizon catch-up from recurring Executive obligations, set regular Verizon to day 26, and verify funding-cycle shortfalls update without duplicate phone charges.
- [x] 5.15 Reconcile the shortfall preview with an Other obligations subtotal and expandable complete cycle list, including unclipped desktop verification.
- [x] 5.16 Reduce the recurring Food plan to $450, remove the dates label, and identify direct BOA obligations such as Chase beneath each cycle's BOA-bills total.
- [x] 5.17 Correct Chase to a $376 BOA obligation in every biweekly paycheck cycle and align all policy labels and projection tests.
- [x] 5.18 Correct the Chase interpretation to one $376 monthly BOA payment in the first monthly pay cycle and remove the duplicate second-cycle projection.
